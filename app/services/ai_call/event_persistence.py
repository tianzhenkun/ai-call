from __future__ import annotations

import asyncio
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.config.setting import settings
from app.core.logger import log
from app.services.ai_call.event_store import AiCallEvent, InMemoryEventStore
from app.services.ai_call.livekit_egress import LiveKitEgressManager
from app.services.ai_call.record_service import AiCallRecordService
from app.services.ai_call.recording_service import AiCallRecordingService

TERMINAL_RECORD_EVENT_TYPES = frozenset({"session_completed", "session_failed", "model_error"})


@dataclass(slots=True)
class QueuedAiCallEvent:
    event: AiCallEvent
    attempts: int = 0


class AiCallEventPersistenceWorker:
    """后台持久化关键通话事件，避免数据库 I/O 进入实时通话路径。"""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        queue_max_size: int = 5000,
        batch_size: int = 100,
        flush_interval_seconds: float = 0.2,
        max_retries: int = 2,
    ) -> None:
        self.session_factory = session_factory
        self.batch_size = max(1, batch_size)
        self.flush_interval_seconds = max(0.05, flush_interval_seconds)
        self.max_retries = max(0, max_retries)
        self.queue: asyncio.Queue[QueuedAiCallEvent] = asyncio.Queue(maxsize=queue_max_size)
        self.dropped_count = 0
        self.failed_count = 0
        self.persisted_count = 0
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._attached_stores: dict[int, InMemoryEventStore] = {}

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping.clear()
        self._task = asyncio.create_task(
            self._run(),
            name="ai-call-event-persistence-worker",
        )

    async def stop(self, timeout_seconds: float = 3.0) -> None:
        self._stopping.set()
        if self._task is None:
            return
        try:
            await asyncio.wait_for(self.queue.join(), timeout=timeout_seconds)
        except TimeoutError:
            log.warning("AI Call 事件持久化队列关闭超时，仍有事件未落库")
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    def attach_event_store(self, event_store: InMemoryEventStore) -> None:
        store_id = id(event_store)
        if store_id in self._attached_stores:
            return
        event_store.add_listener(self.enqueue)
        self._attached_stores[store_id] = event_store

    def detach_all(self) -> None:
        for event_store in self._attached_stores.values():
            event_store.remove_listener(self.enqueue)
        self._attached_stores.clear()

    def enqueue(self, event: AiCallEvent) -> None:
        if not AiCallRecordService.should_persist_event(event):
            return
        try:
            # 热路径只做无等待入队；队列满时宁可丢事件，也不阻塞实时音频。
            self.queue.put_nowait(QueuedAiCallEvent(event=event))
        except asyncio.QueueFull:
            self.dropped_count += 1
            log.warning(
                "AI Call 事件持久化队列已满，丢弃事件: callId={}, eventId={}, type={}",
                event.call_id,
                event.event_id,
                event.type,
            )

    async def flush_pending(self) -> None:
        await self.queue.join()

    async def _run(self) -> None:
        while not self._stopping.is_set() or not self.queue.empty():
            batch = await self._next_batch()
            if not batch:
                continue
            await self._persist_batch(batch)

    async def _next_batch(self) -> list[QueuedAiCallEvent]:
        try:
            first = await asyncio.wait_for(
                self.queue.get(),
                timeout=self.flush_interval_seconds,
            )
        except TimeoutError:
            return []
        batch = [first]
        while len(batch) < self.batch_size:
            try:
                batch.append(self.queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return batch

    async def _persist_batch(self, batch: list[QueuedAiCallEvent]) -> None:
        events = [item.event for item in batch]
        try:
            terminal_call_ids: set[str] = set()
            async with self.session_factory() as db:
                async with db.begin():
                    service = AiCallRecordService(AiCallRecordRepository(db))
                    persisted = await service.mirror_runtime_events(events)
                    terminal_call_ids = await self._mirror_terminal_records(service, events)
                    self.persisted_count += len(persisted)
            if terminal_call_ids:
                await self._stop_terminal_recordings(terminal_call_ids)
        except Exception as exc:
            self.failed_count += len(batch)
            log.error(
                "AI Call 事件后台落库失败: count={}, errorType={}, message={}",
                len(batch),
                type(exc).__name__,
                str(exc),
            )
            await self._retry_batch(batch)
        finally:
            for _item in batch:
                self.queue.task_done()

    async def _retry_batch(self, batch: list[QueuedAiCallEvent]) -> None:
        retry_items = [
            QueuedAiCallEvent(event=item.event, attempts=item.attempts + 1)
            for item in batch
            if item.attempts < self.max_retries
        ]
        if not retry_items:
            return
        await asyncio.sleep(min(0.2 * retry_items[0].attempts, 1.0))
        for item in retry_items:
            try:
                self.queue.put_nowait(item)
            except asyncio.QueueFull:
                self.dropped_count += 1
                log.warning(
                    "AI Call 事件重试入队失败，队列已满: callId={}, eventId={}, type={}",
                    item.event.call_id,
                    item.event.event_id,
                    item.event.type,
                )

    async def _mirror_terminal_records(
        self,
        service: AiCallRecordService,
        events: list[AiCallEvent],
    ) -> set[str]:
        terminal_by_call: dict[str, AiCallEvent] = {}
        for event in events:
            if event.type in TERMINAL_RECORD_EVENT_TYPES:
                terminal_by_call[event.call_id] = event
        for event in terminal_by_call.values():
            if event.type == "session_completed":
                await service.complete_session(
                    event.call_id,
                    end_reason=self._end_reason(event, default="unknown"),
                    ended_at=event.timestamp,
                )
                continue
            await service.fail_session(
                event.call_id,
                end_reason=self._end_reason(event, default=event.type),
                failure_stage=self._failure_stage(event),
                failure_message=self._failure_message(event),
                ended_at=event.timestamp,
            )
        return set(terminal_by_call)

    async def _stop_terminal_recordings(self, call_ids: set[str]) -> None:
        for call_id in sorted(call_ids):
            try:
                async with self.session_factory() as db:
                    async with db.begin():
                        repository = AiCallRecordRepository(db)
                        recording_service = self._build_recording_service(repository)
                        await recording_service.stop_for_session(call_id)
                        ready_for_asr = await recording_service.is_ready_for_offline_asr(call_id)
                    from app.api.v1.ai_call.service import enqueue_ai_call_offline_asr

                    if ready_for_asr:
                        enqueue_ai_call_offline_asr(call_id)
            except Exception as exc:
                log.warning(
                    "AI Call 终态录音后台停止失败: callId={}, errorType={}, message={}",
                    call_id,
                    type(exc).__name__,
                    str(exc),
                )

    def _build_recording_service(
        self,
        repository: AiCallRecordRepository,
    ) -> AiCallRecordingService:
        manager: LiveKitEgressManager | None = None
        if settings.AI_CALL_RECORDING_ENABLED:
            manager = LiveKitEgressManager(
                livekit_url=settings.LIVEKIT_URL,
                api_key=settings.LIVEKIT_API_KEY,
                api_secret=settings.LIVEKIT_API_SECRET,
                timeout_seconds=settings.AI_CALL_EGRESS_TIMEOUT_SECONDS,
                stop_timeout_seconds=settings.AI_CALL_EGRESS_STOP_TIMEOUT_SECONDS,
                object_prefix=settings.AI_CALL_RECORDING_OBJECT_PREFIX,
                file_type=settings.AI_CALL_RECORDING_FORMAT,
                participant_file_type=settings.AI_CALL_PARTICIPANT_RECORDING_FORMAT,
            )
        return AiCallRecordingService(
            repository,
            enabled=settings.AI_CALL_RECORDING_ENABLED,
            egress_manager=manager,
            participant_recording_enabled=settings.AI_CALL_PARTICIPANT_RECORDING_ENABLED,
            verify_deadline_seconds=settings.AI_CALL_RECORDING_VERIFY_DEADLINE_SECONDS,
            stop_session_factory=self.session_factory,
        )

    @staticmethod
    def _end_reason(event: AiCallEvent, *, default: str) -> str:
        value = (
            event.payload.get("endReason")
            or event.payload.get("end_reason")
            or event.payload.get("reason")
        )
        return str(value) if value else default

    @staticmethod
    def _failure_stage(event: AiCallEvent) -> str | None:
        value = event.payload.get("failureStage") or event.payload.get("failure_stage")
        if value:
            return str(value)
        if event.type == "model_error":
            return "model"
        if event.type == "session_failed":
            return "runtime"
        return None

    @staticmethod
    def _failure_message(event: AiCallEvent) -> str | None:
        value = event.payload.get("failureMessage") or event.payload.get("failure_message")
        if value:
            return str(value)
        error = event.payload.get("error")
        if isinstance(error, dict):
            message = error.get("message") or error.get("code")
            return str(message) if message else None
        if isinstance(error, str):
            return error
        message = event.payload.get("message")
        return str(message) if message else None
