from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import AiCallHandoffModel
from app.core.logger import log
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.handoff_service import (
    HANDOFF_STATUS_ACCEPTED,
    HANDOFF_STATUS_REQUESTED,
    AiCallHandoffService,
)
from app.services.ai_call.orchestrator import AiCallOrchestrator
from app.services.ai_call.record_service import AiCallRecordService
from app.services.ai_call.recording_service import AiCallRecordingService
from app.services.ai_call.session_registry import utc_now


class SystemPromptPlayerProtocol(Protocol):
    async def play(
        self,
        *,
        call_id: str,
        room_name: str,
        audio_path: str | Path,
    ) -> None: ...


RecordingServiceFactory = Callable[[AiCallRecordRepository], AiCallRecordingService | None]


class AiCallHandoffExceptionManager:
    """B3.1 转人工失败/超时后的提示音和自动结束协调器。"""

    def __init__(
        self,
        *,
        orchestrator: AiCallOrchestrator,
        session_factory: async_sessionmaker[AsyncSession],
        recording_service_factory: RecordingServiceFactory | None = None,
        system_prompt_player: SystemPromptPlayerProtocol | None = None,
        timeout_seconds: int = 30,
        exception_close_enabled: bool = True,
        waiting_prompt_audio_path: str | Path | None = None,
        waiting_tone_enabled: bool = False,
        waiting_tone_audio_path: str | Path | None = None,
        waiting_tone_interval_seconds: float = 0.0,
        unavailable_prompt_audio_path: str | Path | None = None,
    ) -> None:
        self.orchestrator = orchestrator
        self.session_factory = session_factory
        self.recording_service_factory = recording_service_factory
        self.system_prompt_player = system_prompt_player
        self.timeout_seconds = max(1, timeout_seconds)
        self.exception_close_enabled = exception_close_enabled
        self.waiting_prompt_audio_path = (
            Path(waiting_prompt_audio_path).expanduser() if waiting_prompt_audio_path else None
        )
        self.waiting_tone_enabled = waiting_tone_enabled
        self.waiting_tone_audio_path = (
            Path(waiting_tone_audio_path).expanduser() if waiting_tone_audio_path else None
        )
        self.waiting_tone_interval_seconds = max(0.0, waiting_tone_interval_seconds)
        self.unavailable_prompt_audio_path = (
            Path(unavailable_prompt_audio_path).expanduser()
            if unavailable_prompt_audio_path
            else None
        )
        self._timeout_tasks: dict[str, asyncio.Task] = {}
        self._closure_tasks: dict[str, asyncio.Task] = {}
        self._waiting_tone_tasks: dict[str, asyncio.Task] = {}

    def schedule_timeout(self, handoff: AiCallHandoffModel) -> None:
        if not self.exception_close_enabled or handoff.expires_at is None:
            return
        if handoff.status not in {HANDOFF_STATUS_REQUESTED, HANDOFF_STATUS_ACCEPTED}:
            return
        handoff_id = handoff.handoff_id
        self.cancel_timeout(handoff_id)
        delay_seconds = max(0.0, (self._ensure_utc(handoff.expires_at) - utc_now()).total_seconds())
        try:
            task = asyncio.create_task(
                self._timeout_worker(
                    handoff_id=handoff_id,
                    call_id=handoff.call_id,
                    room_name=handoff.room_name,
                    delay_seconds=delay_seconds,
                )
            )
        except RuntimeError as exc:
            log.warning("转人工超时任务启动失败: handoffId={}, message={}", handoff_id, str(exc))
            return
        self._timeout_tasks[handoff_id] = task
        task.add_done_callback(
            lambda done, current=task: self._forget_task(
                self._timeout_tasks, handoff_id, current, done
            )
        )
        self._record_handoff_event(
            call_id=handoff.call_id,
            event_type="handoff_timeout_task_started",
            handoff_id=handoff_id,
            handoff_status=handoff.status,
            payload={"expiresAt": self._ensure_utc(handoff.expires_at).isoformat()},
        )

    def start_waiting_tone(self, handoff: AiCallHandoffModel) -> None:
        has_waiting_tone = self.waiting_tone_enabled and self.waiting_tone_audio_path is not None
        if self.system_prompt_player is None or (
            self.waiting_prompt_audio_path is None and not has_waiting_tone
        ):
            return
        if handoff.status not in {HANDOFF_STATUS_REQUESTED, HANDOFF_STATUS_ACCEPTED}:
            return
        existing = self._waiting_tone_tasks.get(handoff.handoff_id)
        if existing is not None and not existing.done():
            return
        try:
            task = asyncio.create_task(
                self._waiting_tone_worker(
                    handoff_id=handoff.handoff_id,
                    call_id=handoff.call_id,
                    room_name=handoff.room_name,
                    handoff_status=handoff.status,
                )
            )
        except RuntimeError as exc:
            self._record_handoff_event(
                call_id=handoff.call_id,
                event_type="handoff_waiting_tone_failed",
                handoff_id=handoff.handoff_id,
                handoff_status=handoff.status,
                payload={
                    "stage": "start_task",
                    "errorType": type(exc).__name__,
                    "message": str(exc),
                },
                source="system",
            )
            return
        self._waiting_tone_tasks[handoff.handoff_id] = task
        task.add_done_callback(
            lambda done, current=task: self._forget_task(
                self._waiting_tone_tasks,
                handoff.handoff_id,
                current,
                done,
            )
        )

    def stop_waiting_tone(
        self,
        handoff_id: str,
        *,
        call_id: str | None = None,
        handoff_status: str | None = None,
        reason: str = "state_changed",
    ) -> None:
        task = self._waiting_tone_tasks.pop(handoff_id, None)
        if task is None:
            return
        task.cancel()
        if call_id and handoff_status:
            self._record_handoff_event(
                call_id=call_id,
                event_type="handoff_waiting_tone_stopped",
                handoff_id=handoff_id,
                handoff_status=handoff_status,
                payload={"reason": reason},
                source="system",
            )

    def cancel_timeout(
        self,
        handoff_id: str,
        *,
        call_id: str | None = None,
        handoff_status: str | None = None,
        reason: str = "state_changed",
    ) -> None:
        task = self._timeout_tasks.pop(handoff_id, None)
        if task is None:
            return
        task.cancel()
        if call_id and handoff_status:
            self._record_handoff_event(
                call_id=call_id,
                event_type="handoff_timeout_task_canceled",
                handoff_id=handoff_id,
                handoff_status=handoff_status,
                payload={"reason": reason},
            )

    def trigger_exception_close(self, handoff: AiCallHandoffModel, *, call_end_reason: str) -> None:
        if not self.exception_close_enabled:
            return
        self.cancel_timeout(
            handoff.handoff_id,
            call_id=handoff.call_id,
            handoff_status=handoff.status,
            reason=call_end_reason,
        )
        self.stop_waiting_tone(
            handoff.handoff_id,
            call_id=handoff.call_id,
            handoff_status=handoff.status,
            reason=call_end_reason,
        )
        if self._closure_tasks.get(handoff.handoff_id) is not None:
            return
        self._record_handoff_event(
            call_id=handoff.call_id,
            event_type="handoff_auto_end_scheduled",
            handoff_id=handoff.handoff_id,
            handoff_status=handoff.status,
            payload={"reason": call_end_reason},
        )
        try:
            task = asyncio.create_task(
                self._close_after_exception(
                    handoff_id=handoff.handoff_id,
                    call_id=handoff.call_id,
                    room_name=handoff.room_name,
                    handoff_status=handoff.status,
                    call_end_reason=call_end_reason,
                )
            )
        except RuntimeError as exc:
            log.warning(
                "转人工异常自动结束任务启动失败: handoffId={}, message={}",
                handoff.handoff_id,
                str(exc),
            )
            return
        self._closure_tasks[handoff.handoff_id] = task
        task.add_done_callback(
            lambda done, current=task: self._forget_task(
                self._closure_tasks, handoff.handoff_id, current, done
            )
        )

    async def shutdown(self) -> None:
        tasks = (
            list(self._timeout_tasks.values())
            + list(self._closure_tasks.values())
            + list(self._waiting_tone_tasks.values())
        )
        self._timeout_tasks.clear()
        self._closure_tasks.clear()
        self._waiting_tone_tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _timeout_worker(
        self,
        *,
        handoff_id: str,
        call_id: str,
        room_name: str,
        delay_seconds: float,
    ) -> None:
        try:
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
            expired = await self._expire_handoff_if_due(handoff_id)
            if expired is None:
                return
            self._record_handoff_event(
                call_id=expired.call_id,
                event_type="handoff_expired",
                handoff_id=expired.handoff_id,
                handoff_status=expired.status,
                payload={"reason": expired.end_reason},
            )
            self._timeout_tasks.pop(handoff_id, None)
            self.trigger_exception_close(expired, call_end_reason="handoff_timeout")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning(
                "转人工超时任务执行失败: handoffId={}, errorType={}, message={}",
                handoff_id,
                type(exc).__name__,
                str(exc),
            )

    async def _waiting_tone_worker(
        self,
        *,
        handoff_id: str,
        call_id: str,
        room_name: str,
        handoff_status: str,
    ) -> None:
        assert self.system_prompt_player is not None
        try:
            if self.waiting_prompt_audio_path is not None:
                await self._play_prompt_audio(
                    call_id=call_id,
                    room_name=room_name,
                    handoff_id=handoff_id,
                    handoff_status=handoff_status,
                    audio_path=self.waiting_prompt_audio_path,
                    event_prefix="handoff_prompt",
                )
            if not self.waiting_tone_enabled or self.waiting_tone_audio_path is None:
                return
            self._record_handoff_event(
                call_id=call_id,
                event_type="handoff_waiting_tone_started",
                handoff_id=handoff_id,
                handoff_status=handoff_status,
                payload={"audioFile": self.waiting_tone_audio_path.name},
                source="system",
            )
            while True:
                try:
                    await self.system_prompt_player.play(
                        call_id=call_id,
                        room_name=room_name,
                        audio_path=self.waiting_tone_audio_path,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._record_handoff_event(
                        call_id=call_id,
                        event_type="handoff_waiting_tone_failed",
                        handoff_id=handoff_id,
                        handoff_status=handoff_status,
                        payload={
                            "stage": "publish_audio",
                            "errorType": type(exc).__name__,
                            "message": str(exc),
                        },
                        source="system",
                    )
                    return
                if self.waiting_tone_interval_seconds > 0:
                    await asyncio.sleep(self.waiting_tone_interval_seconds)
        except asyncio.CancelledError:
            raise

    async def _expire_handoff_if_due(self, handoff_id: str) -> AiCallHandoffModel | None:
        async with self.session_factory() as db:
            async with db.begin():
                repository = AiCallRecordRepository(db)
                handoff = await repository.get_handoff_by_id(handoff_id)
                if handoff is None:
                    return None
                if handoff.status not in {HANDOFF_STATUS_REQUESTED, HANDOFF_STATUS_ACCEPTED}:
                    return None
                if handoff.expires_at is None or self._ensure_utc(handoff.expires_at) > utc_now():
                    return None
                handoff_service = AiCallHandoffService(
                    repository,
                    request_timeout_seconds=self.timeout_seconds,
                )
                return await handoff_service.expire_request(handoff_id)

    async def _close_after_exception(
        self,
        *,
        handoff_id: str,
        call_id: str,
        room_name: str,
        handoff_status: str,
        call_end_reason: str,
    ) -> None:
        await self._play_unavailable_prompt(
            call_id=call_id,
            room_name=room_name,
            handoff_id=handoff_id,
            handoff_status=handoff_status,
        )
        async with self.session_factory() as db:
            async with db.begin():
                repository = AiCallRecordRepository(db)
                recording_service = (
                    self.recording_service_factory(repository)
                    if self.recording_service_factory is not None
                    else None
                )
                if recording_service is not None:
                    await recording_service.stop_for_session(call_id)
                try:
                    await self.orchestrator.end_session(
                        call_id,
                        end_reason=call_end_reason,
                    )
                except AiCallError as exc:
                    log.warning(
                        "转人工异常自动结束运行态会话失败: callId={}, errorId={}, message={}",
                        call_id,
                        exc.error_id,
                        exc.msg,
                    )
                record_service = AiCallRecordService(repository)
                await record_service.complete_session(call_id, end_reason=call_end_reason)

        self._record_handoff_event(
            call_id=call_id,
            event_type="handoff_auto_ended",
            handoff_id=handoff_id,
            handoff_status=handoff_status,
            payload={"reason": call_end_reason},
        )

    async def _play_unavailable_prompt(
        self,
        *,
        call_id: str,
        room_name: str,
        handoff_id: str,
        handoff_status: str,
    ) -> None:
        if self.unavailable_prompt_audio_path is None:
            return
        await self._play_prompt_audio(
            call_id=call_id,
            room_name=room_name,
            handoff_id=handoff_id,
            handoff_status=handoff_status,
            audio_path=self.unavailable_prompt_audio_path,
            event_prefix="handoff_unavailable_prompt",
        )

    async def _play_prompt_audio(
        self,
        *,
        call_id: str,
        room_name: str,
        handoff_id: str,
        handoff_status: str,
        audio_path: Path,
        event_prefix: str,
    ) -> None:
        if self.system_prompt_player is None:
            return
        self._record_handoff_event(
            call_id=call_id,
            event_type=f"{event_prefix}_started",
            handoff_id=handoff_id,
            handoff_status=handoff_status,
            payload={"audioFile": audio_path.name},
            source="system",
        )
        try:
            await self.system_prompt_player.play(
                call_id=call_id,
                room_name=room_name,
                audio_path=audio_path,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._record_handoff_event(
                call_id=call_id,
                event_type=f"{event_prefix}_failed",
                handoff_id=handoff_id,
                handoff_status=handoff_status,
                payload={
                    "audioFile": audio_path.name,
                    "errorType": type(exc).__name__,
                    "message": str(exc),
                },
                source="system",
            )
            return
        self._record_handoff_event(
            call_id=call_id,
            event_type=f"{event_prefix}_done",
            handoff_id=handoff_id,
            handoff_status=handoff_status,
            payload={"audioFile": audio_path.name},
            source="system",
        )

    def _record_handoff_event(
        self,
        *,
        call_id: str,
        event_type: str,
        handoff_id: str,
        handoff_status: str,
        payload: dict[str, Any] | None = None,
        source: str = "handoff",
    ) -> None:
        try:
            self.orchestrator.record_handoff_event(
                call_id=call_id,
                event_type=event_type,
                handoff_id=handoff_id,
                handoff_status=handoff_status,
                payload=payload,
                source=source,
            )
        except AiCallError:
            return
        except Exception:
            return

    @staticmethod
    def _forget_task(
        registry: dict[str, asyncio.Task],
        key: str,
        expected_task: asyncio.Task,
        done_task: asyncio.Task,
    ) -> None:
        if registry.get(key) is expected_task:
            registry.pop(key, None)
        if done_task.cancelled():
            return
        with log.catch(reraise=False):
            _ = done_task.exception()

    @staticmethod
    def _ensure_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
