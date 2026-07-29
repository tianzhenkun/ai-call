from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import AiCallHandoffModel
from app.core.logger import log
from app.services.ai_call.agent_console_service import AiCallAgentConsoleService
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.handoff_service import (
    HANDOFF_STATUS_ACCEPTED,
    HANDOFF_STATUS_REQUESTED,
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

DEFAULT_WAITING_PROMPT_TEXT = "正在为您转接人工客服，请稍候。"
DEFAULT_BUSY_WAITING_PROMPT_TEXT = "当前人工坐席繁忙，正在为您排队转接，请稍候。"
DEFAULT_UNAVAILABLE_PROMPT_TEXT = "当前暂时没有人工接入，我先帮您记录需求，稍后安排顾问联系您。"
DEFAULT_NO_ONLINE_AGENT_PROMPT_TEXT = (
    "当前暂无人工坐席在线，我先为您记录需求，稍后安排工作人员联系您。"
)
DEFAULT_BUSY_TIMEOUT_PROMPT_TEXT = "当前人工坐席繁忙，暂未接通，我先为您记录需求。"
DEFAULT_SERVICE_UNAVAILABLE_PROMPT_TEXT = "人工转接服务暂时不可用，我先为您记录需求。"


@dataclass(frozen=True, slots=True)
class HandoffPrompt:
    audio_path: Path | None
    text: str


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
        waiting_prompt_text: str | None = DEFAULT_WAITING_PROMPT_TEXT,
        busy_waiting_prompt_audio_path: str | Path | None = None,
        busy_waiting_prompt_text: str | None = DEFAULT_BUSY_WAITING_PROMPT_TEXT,
        waiting_tone_enabled: bool = False,
        waiting_tone_audio_path: str | Path | None = None,
        waiting_tone_interval_seconds: float = 0.0,
        unavailable_prompt_audio_path: str | Path | None = None,
        unavailable_prompt_text: str | None = DEFAULT_UNAVAILABLE_PROMPT_TEXT,
        no_online_agent_prompt_audio_path: str | Path | None = None,
        no_online_agent_prompt_text: str | None = DEFAULT_NO_ONLINE_AGENT_PROMPT_TEXT,
        busy_timeout_prompt_audio_path: str | Path | None = None,
        busy_timeout_prompt_text: str | None = DEFAULT_BUSY_TIMEOUT_PROMPT_TEXT,
        service_unavailable_prompt_audio_path: str | Path | None = None,
        service_unavailable_prompt_text: str | None = (
            DEFAULT_SERVICE_UNAVAILABLE_PROMPT_TEXT
        ),
    ) -> None:
        self.orchestrator = orchestrator
        self.session_factory = session_factory
        self.recording_service_factory = recording_service_factory
        self.system_prompt_player = system_prompt_player
        self.timeout_seconds = max(1, timeout_seconds)
        self.exception_close_enabled = exception_close_enabled
        default_waiting_prompt = self._build_prompt(
            waiting_prompt_audio_path,
            waiting_prompt_text,
        )
        busy_waiting_prompt = (
            self._build_prompt(
                busy_waiting_prompt_audio_path,
                busy_waiting_prompt_text,
            )
            if busy_waiting_prompt_audio_path
            else default_waiting_prompt
        )
        self.waiting_prompts = {
            "default": default_waiting_prompt,
            "available": default_waiting_prompt,
            "busy": busy_waiting_prompt,
        }
        self.waiting_tone_enabled = waiting_tone_enabled
        self.waiting_tone_audio_path = (
            Path(waiting_tone_audio_path).expanduser() if waiting_tone_audio_path else None
        )
        self.waiting_tone_interval_seconds = max(0.0, waiting_tone_interval_seconds)
        default_exception_prompt = self._build_prompt(
            unavailable_prompt_audio_path,
            unavailable_prompt_text,
        )
        self.exception_prompts = {
            "default": default_exception_prompt,
            "no_online_agent": self._build_prompt(
                no_online_agent_prompt_audio_path,
                no_online_agent_prompt_text,
            )
            if no_online_agent_prompt_audio_path
            else default_exception_prompt,
            "handoff_timeout": self._build_prompt(
                busy_timeout_prompt_audio_path,
                busy_timeout_prompt_text,
            )
            if busy_timeout_prompt_audio_path
            else default_exception_prompt,
            "handoff_service_unavailable": self._build_prompt(
                service_unavailable_prompt_audio_path,
                service_unavailable_prompt_text,
            )
            if service_unavailable_prompt_audio_path
            else default_exception_prompt,
        }
        self._timeout_tasks: dict[str, asyncio.Task] = {}
        self._closure_tasks: dict[str, asyncio.Task] = {}
        self._waiting_tone_tasks: dict[str, asyncio.Task] = {}

    def schedule_timeout(self, handoff: AiCallHandoffModel) -> None:
        if not self.exception_close_enabled or handoff.expires_at is None:
            return
        if handoff.status not in {
            HANDOFF_STATUS_REQUESTED,
            HANDOFF_STATUS_ACCEPTED,
            "reconnecting",
        }:
            return
        deadlines = [handoff.expires_at]
        if handoff.status == HANDOFF_STATUS_ACCEPTED:
            deadlines.append(handoff.claim_expires_at)
        elif handoff.status == "reconnecting":
            deadlines = [handoff.reconnect_expires_at]
        deadline = min(
            (self._ensure_utc(value) for value in deadlines if value is not None),
            default=None,
        )
        if deadline is None:
            return
        handoff_id = handoff.handoff_id
        self.cancel_timeout(handoff_id)
        delay_seconds = max(0.0, (deadline - utc_now()).total_seconds())
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
            payload={"deadlineAt": deadline.isoformat()},
        )

    def start_waiting_tone(
        self,
        handoff: AiCallHandoffModel,
        *,
        prompt_kind: str = "default",
    ) -> None:
        prompt = self.waiting_prompts.get(
            prompt_kind,
            self.waiting_prompts["default"],
        )
        has_waiting_tone = self.waiting_tone_enabled and self.waiting_tone_audio_path is not None
        if self.system_prompt_player is None or (
            prompt.audio_path is None and not has_waiting_tone
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
                    prompt=prompt,
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
            reconciled = await self._expire_handoff_if_due(handoff_id)
            if reconciled is None:
                return
            if reconciled.status in {HANDOFF_STATUS_REQUESTED, HANDOFF_STATUS_ACCEPTED}:
                self._timeout_tasks.pop(handoff_id, None)
                self.schedule_timeout(reconciled)
                return
            if reconciled.status == "failed":
                self._record_handoff_event(
                    call_id=reconciled.call_id,
                    event_type="handoff_reconnect_timeout",
                    handoff_id=reconciled.handoff_id,
                    handoff_status=reconciled.status,
                    payload={"reason": reconciled.end_reason},
                )
                return
            if reconciled.status != "expired":
                return
            self._record_handoff_event(
                call_id=reconciled.call_id,
                event_type="handoff_expired",
                handoff_id=reconciled.handoff_id,
                handoff_status=reconciled.status,
                payload={"reason": reconciled.end_reason},
            )
            self._timeout_tasks.pop(handoff_id, None)
            self.trigger_exception_close(reconciled, call_end_reason="handoff_timeout")
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
        prompt: HandoffPrompt,
    ) -> None:
        assert self.system_prompt_player is not None
        try:
            if prompt.audio_path is not None:
                await self._play_prompt_audio(
                    call_id=call_id,
                    room_name=room_name,
                    handoff_id=handoff_id,
                    handoff_status=handoff_status,
                    audio_path=prompt.audio_path,
                    event_prefix="handoff_prompt",
                    event_payload=self._prompt_event_payload(prompt),
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
                return await AiCallAgentConsoleService(db).reconcile_handoff_timeout(
                    handoff.tenant_id,
                    handoff.handoff_id,
                    now=utc_now(),
                )

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
            call_end_reason=call_end_reason,
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
                runtime_close_mode = await self._close_runtime_after_exception(
                    call_id=call_id,
                    room_name=room_name,
                    handoff_id=handoff_id,
                    handoff_status=handoff_status,
                    call_end_reason=call_end_reason,
                )
                if runtime_close_mode is None:
                    return
                record_service = AiCallRecordService(repository)
                await record_service.complete_session(call_id, end_reason=call_end_reason)

        payload = {"reason": call_end_reason}
        if runtime_close_mode != "orchestrator":
            payload["runtimeCloseMode"] = runtime_close_mode
        self._record_handoff_event(
            call_id=call_id,
            event_type="handoff_auto_ended",
            handoff_id=handoff_id,
            handoff_status=handoff_status,
            payload=payload,
        )
        if runtime_close_mode != "orchestrator":
            self.orchestrator.event_store.append(
                call_id=call_id,
                type="session_completed",
                source="orchestrator",
                payload={"endReason": call_end_reason},
            )

    async def _close_runtime_after_exception(
        self,
        *,
        call_id: str,
        room_name: str,
        handoff_id: str,
        handoff_status: str,
        call_end_reason: str,
    ) -> str | None:
        try:
            await self.orchestrator.end_session(
                call_id,
                end_reason=call_end_reason,
            )
            return "orchestrator"
        except AiCallError as exc:
            log.warning(
                "转人工异常自动结束运行态会话失败: callId={}, errorId={}, message={}",
                call_id,
                exc.error_id,
                exc.msg,
            )
            if exc.error_id != "session_not_found":
                self._record_runtime_close_failed(
                    call_id=call_id,
                    handoff_id=handoff_id,
                    handoff_status=handoff_status,
                    call_end_reason=call_end_reason,
                    stage="orchestrator_end",
                    error=exc,
                )
                return None
            if await self._close_runtime_by_room_name(
                call_id=call_id,
                room_name=room_name,
                handoff_id=handoff_id,
                handoff_status=handoff_status,
                call_end_reason=call_end_reason,
                original_error=exc,
            ):
                return "room_name_fallback"
            return None

    async def _close_runtime_by_room_name(
        self,
        *,
        call_id: str,
        room_name: str,
        handoff_id: str,
        handoff_status: str,
        call_end_reason: str,
        original_error: AiCallError,
    ) -> bool:
        payload: dict[str, Any] = {
            "reason": call_end_reason,
            "fallback": "room_name",
            "roomName": room_name,
            "runtimeErrorId": original_error.error_id,
            "runtimeErrorMessage": original_error.msg,
        }
        try:
            await self.orchestrator.agent_runner.stop(call_id)
        except Exception as exc:
            payload["agentStopErrorType"] = type(exc).__name__
            payload["agentStopErrorMessage"] = str(exc)
        try:
            await self.orchestrator.livekit_room_manager.delete_room(room_name)
        except Exception as exc:
            log.warning(
                "转人工异常按房间名关闭运行态失败: callId={}, roomName={}, errorType={}, message={}",
                call_id,
                room_name,
                type(exc).__name__,
                str(exc),
            )
            self._record_runtime_close_failed(
                call_id=call_id,
                handoff_id=handoff_id,
                handoff_status=handoff_status,
                call_end_reason=call_end_reason,
                stage="room_name_fallback",
                error=exc,
                original_error=original_error,
            )
            return False
        self._record_handoff_event(
            call_id=call_id,
            event_type="handoff_runtime_close_fallback",
            handoff_id=handoff_id,
            handoff_status=handoff_status,
            payload=payload,
        )
        return True

    def _record_runtime_close_failed(
        self,
        *,
        call_id: str,
        handoff_id: str,
        handoff_status: str,
        call_end_reason: str,
        stage: str,
        error: Exception,
        original_error: AiCallError | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "reason": call_end_reason,
            "stage": stage,
            "errorType": type(error).__name__,
            "message": str(error),
        }
        if isinstance(error, AiCallError):
            payload["errorId"] = error.error_id
            payload["message"] = error.msg
        if original_error is not None:
            payload["originalErrorId"] = original_error.error_id
            payload["originalMessage"] = original_error.msg
        self._record_handoff_event(
            call_id=call_id,
            event_type="handoff_auto_end_runtime_failed",
            handoff_id=handoff_id,
            handoff_status=handoff_status,
            payload=payload,
        )

    async def _play_unavailable_prompt(
        self,
        *,
        call_id: str,
        room_name: str,
        handoff_id: str,
        handoff_status: str,
        call_end_reason: str = "default",
    ) -> None:
        prompt = self.exception_prompts.get(
            call_end_reason,
            self.exception_prompts["default"],
        )
        if prompt.audio_path is None:
            return
        await self._play_prompt_audio(
            call_id=call_id,
            room_name=room_name,
            handoff_id=handoff_id,
            handoff_status=handoff_status,
            audio_path=prompt.audio_path,
            event_prefix="handoff_unavailable_prompt",
            event_payload=self._prompt_event_payload(prompt),
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
        event_payload: dict[str, Any] | None = None,
    ) -> None:
        if self.system_prompt_player is None:
            return
        base_payload = {"audioFile": audio_path.name}
        if event_payload:
            base_payload.update(event_payload)
        self._record_handoff_event(
            call_id=call_id,
            event_type=f"{event_prefix}_started",
            handoff_id=handoff_id,
            handoff_status=handoff_status,
            payload=base_payload,
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
                    **base_payload,
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
            payload=base_payload,
            source="system",
        )

    @staticmethod
    def _prompt_event_payload(prompt: HandoffPrompt) -> dict[str, Any]:
        if not prompt.text:
            return {}
        return {"promptText": prompt.text}

    @classmethod
    def _build_prompt(
        cls,
        audio_path: str | Path | None,
        text: str | None,
    ) -> HandoffPrompt:
        return HandoffPrompt(
            audio_path=Path(audio_path).expanduser() if audio_path else None,
            text=cls._strip_prompt_text(text) or "",
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

    @staticmethod
    def _strip_prompt_text(value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        return stripped or None
