from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.logger import log
from app.services.ai_call.dialogue_service import (
    AiCallDialogueRuntimeStore,
    DialogueSegmentSnapshot,
)
from app.services.ai_call.event_store import InMemoryEventStore
from app.services.ai_call.runtime_control.dialogue_repository import (
    OwnerDialogueFence,
    OwnerDialogueRepository,
    OwnerDialogueSegment,
)

RepositoryFactory = Callable[[AsyncSession], OwnerDialogueRepository]


@dataclass(slots=True)
class _CallState:
    fence: OwnerDialogueFence
    pending_count: int = 0
    persisted_count: int = 0
    dropped_count: int = 0
    failed: bool = False
    failure_code: str | None = None
    drained: asyncio.Event = field(default_factory=asyncio.Event)

    def __post_init__(self) -> None:
        self.drained.set()


@dataclass(frozen=True, slots=True)
class _QueuedDialogue:
    state: _CallState
    segment: OwnerDialogueSegment
    attempts: int = 0


@dataclass(frozen=True, slots=True)
class OwnerDialogueFinalizeResult:
    status: str
    persisted_count: int
    dropped_count: int


class OwnerRuntimeDialogueBridge:
    """Non-blocking event-to-PostgreSQL bridge for fenced Runtime Owners."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        queue_max_size: int = 5_000,
        batch_size: int = 50,
        flush_interval_seconds: float = 0.2,
        max_retries: int = 2,
        repository_factory: RepositoryFactory = OwnerDialogueRepository,
    ) -> None:
        if queue_max_size <= 0:
            raise ValueError("dialogue queue max size must be positive")
        self._session_factory = session_factory
        self._repository_factory = repository_factory
        self._batch_size = max(1, batch_size)
        self._flush_interval_seconds = max(0.01, flush_interval_seconds)
        self._max_retries = max(0, max_retries)
        self._runtime_store = AiCallDialogueRuntimeStore()
        self._runtime_store.add_persist_listener(self._enqueue)
        self._queue: asyncio.Queue[_QueuedDialogue] = asyncio.Queue(
            maxsize=queue_max_size
        )
        self._states: dict[str, _CallState] = {}
        self._stopping = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    def attach_event_store(self, event_store: InMemoryEventStore) -> None:
        self._runtime_store.attach_event_store(event_store)

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping.clear()
        self._task = asyncio.create_task(
            self._run(),
            name="ai-call-owner-dialogue-bridge",
        )

    async def stop(self, timeout_seconds: float = 3.0) -> None:
        self._runtime_store.detach_all()
        self._stopping.set()
        if self._task is None:
            return
        try:
            await asyncio.wait_for(self._queue.join(), timeout=timeout_seconds)
        except TimeoutError:
            log.warning("Owner Runtime 对话持久化队列关闭超时")
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    async def bind_call(self, fence: OwnerDialogueFence) -> bool:
        async with self._session_factory.begin() as session:
            next_segment_no = await self._repository_factory(session).next_segment_no(
                fence
            )
        if next_segment_no is None:
            return False
        previous = self._states.get(fence.call_id)
        if previous is not None and previous.fence == fence:
            return True
        if previous is not None:
            previous.failed = True
            previous.failure_code = previous.failure_code or "owner_replaced"
        state = _CallState(fence=fence)
        self._states[fence.call_id] = state
        self._runtime_store.initialize_call(fence.call_id, next_segment_no)
        return True

    async def finalize_call(
        self,
        fence: OwnerDialogueFence,
        *,
        ended_at: datetime,
        timeout_seconds: float = 3.0,
    ) -> OwnerDialogueFinalizeResult:
        state = self._states.get(fence.call_id)
        if state is None or state.fence != fence:
            return OwnerDialogueFinalizeResult("pending", 0, 0)
        self._runtime_store.finalize_call(fence.call_id, ended_at)
        try:
            await asyncio.wait_for(state.drained.wait(), timeout=timeout_seconds)
        except TimeoutError:
            state.failed = True
            state.failure_code = state.failure_code or "drain_timeout"

        status = "uncertain" if state.failed else "complete"
        async with self._session_factory.begin() as session:
            finalized = await self._repository_factory(session).finalize(
                fence,
                status=status,
                error=state.failure_code,
            )
        if not finalized:
            status = "pending"
        if self._states.get(fence.call_id) is state:
            self._states.pop(fence.call_id, None)
            self._runtime_store.discard_call(fence.call_id)
        return OwnerDialogueFinalizeResult(
            status=status,
            persisted_count=state.persisted_count,
            dropped_count=state.dropped_count,
        )

    async def mark_uncertain(
        self,
        fence: OwnerDialogueFence,
        *,
        error: str,
    ) -> bool:
        async with self._session_factory.begin() as session:
            return await self._repository_factory(session).finalize(
                fence,
                status="uncertain",
                error=error,
            )

    def _enqueue(self, snapshot: DialogueSegmentSnapshot) -> None:
        state = self._states.get(snapshot.call_id)
        if state is None:
            return
        queued = _QueuedDialogue(
            state=state,
            segment=OwnerDialogueSegment(
                segment_no=snapshot.segment_no,
                speaker_type=snapshot.speaker_type,
                speaker_identity=snapshot.speaker_identity,
                source=snapshot.source,
                source_segment_id=snapshot.source_segment_id,
                text=snapshot.text,
                segment_status=snapshot.segment_status,
                started_at=snapshot.started_at,
                ended_at=snapshot.ended_at,
                duration_ms=snapshot.duration_ms,
                audio_start_ms=snapshot.audio_start_ms,
                audio_end_ms=snapshot.audio_end_ms,
                failure_stage=snapshot.failure_stage,
                failure_message=snapshot.failure_message,
            ),
        )
        try:
            self._queue.put_nowait(queued)
        except asyncio.QueueFull:
            state.failed = True
            state.failure_code = state.failure_code or "queue_full"
            state.dropped_count += 1
            log.warning(
                "Owner Runtime 对话队列已满: callId={}, segmentNo={}",
                snapshot.call_id,
                snapshot.segment_no,
            )
            return
        state.pending_count += 1
        state.drained.clear()

    async def _run(self) -> None:
        while not self._stopping.is_set() or not self._queue.empty():
            batch = await self._next_batch()
            if not batch:
                continue
            await self._persist_batch(batch)

    async def _next_batch(self) -> list[_QueuedDialogue]:
        try:
            first = await asyncio.wait_for(
                self._queue.get(),
                timeout=self._flush_interval_seconds,
            )
        except TimeoutError:
            return []
        batch = [first]
        while len(batch) < self._batch_size:
            try:
                batch.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return batch

    async def _persist_batch(self, batch: list[_QueuedDialogue]) -> None:
        groups: dict[tuple[int, OwnerDialogueFence], list[_QueuedDialogue]] = {}
        for item in batch:
            groups.setdefault((id(item.state), item.state.fence), []).append(item)
        for items in groups.values():
            await self._persist_group(items)
        for _item in batch:
            self._queue.task_done()

    async def _persist_group(self, items: list[_QueuedDialogue]) -> None:
        state = items[0].state
        try:
            async with self._session_factory.begin() as session:
                result = await self._repository_factory(session).persist_batch(
                    state.fence,
                    [item.segment for item in items],
                )
        except Exception as exc:
            log.error(
                "Owner Runtime 对话批次落库失败: callId={}, errorType={}",
                state.fence.call_id,
                type(exc).__name__,
            )
            for item in items:
                self._retry_or_fail(item, "database_error")
            return
        if not result.accepted:
            for item in items:
                self._fail_item(item, "owner_fence_rejected")
            return
        state.persisted_count += result.persisted_count
        for item in items:
            self._settle_item(item)

    def _retry_or_fail(self, item: _QueuedDialogue, failure_code: str) -> None:
        if item.attempts >= self._max_retries:
            self._fail_item(item, failure_code)
            return
        try:
            self._queue.put_nowait(
                _QueuedDialogue(
                    state=item.state,
                    segment=item.segment,
                    attempts=item.attempts + 1,
                )
            )
        except asyncio.QueueFull:
            item.state.dropped_count += 1
            self._fail_item(item, "queue_full")

    def _fail_item(self, item: _QueuedDialogue, failure_code: str) -> None:
        item.state.failed = True
        item.state.failure_code = item.state.failure_code or failure_code
        self._settle_item(item)

    @staticmethod
    def _settle_item(item: _QueuedDialogue) -> None:
        item.state.pending_count = max(0, item.state.pending_count - 1)
        if item.state.pending_count == 0:
            item.state.drained.set()
