from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import AsyncIterator, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from app.services.ai_call.audio_bridge import PcmAudioBridge, PcmAudioFrame
from app.services.ai_call.event_store import InMemoryEventStore
from app.services.ai_call.metrics import CallMetrics
from app.services.ai_call.prompt_config import (
    CALL_END_TOOL_INSTRUCTIONS,
    HANDOFF_CAPABILITY_INSTRUCTIONS,
)
from app.services.ai_call.providers.aliyun_qwen_realtime import (
    DEFAULT_REALTIME_TOOLS,
    QwenRealtimeSessionConfig,
)
from app.services.ai_call.providers.base import ProviderEvent
from app.services.ai_call.session_registry import (
    CallSession,
    CallSessionStatus,
    InMemorySessionRegistry,
)


class RealtimeProviderProtocol(Protocol):
    async def connect(self) -> None: ...

    async def update_session(self, config: QwenRealtimeSessionConfig) -> None: ...

    async def send_audio(self, pcm_frame: bytes) -> None: ...

    async def create_response(self, input_text: str | None = None) -> None: ...

    async def submit_tool_result(self, tool_call_id: str, output: str) -> None: ...

    async def cancel_response(self) -> None: ...

    async def clear_input_audio(self) -> None: ...

    def receive_events(self) -> AsyncIterator[ProviderEvent]: ...

    async def close(self) -> None: ...


class AudioPublisherProtocol(Protocol):
    async def publish_audio(self, call_id: str, frame: PcmAudioFrame) -> None: ...

    async def stop_audio(self, call_id: str) -> None: ...


class RoomAudioTransportProtocol(AudioPublisherProtocol, Protocol):
    async def start(self, session: CallSession) -> None: ...

    def receive_audio_frames(self, call_id: str) -> AsyncIterator[PcmAudioFrame]: ...

    async def close(self, call_id: str) -> None: ...


ProviderFactory = Callable[[CallSession], RealtimeProviderProtocol]
CallEndScheduler = Callable[[str, str], None]


CALL_END_REASON_MAPPING = {
    "customer_end": "customer_end",
    "task_completed": "normal_completed",
}

HANDOFF_REASON_VALUES = {"customer_request", "business_escalation"}


@dataclass(slots=True)
class PendingUserTurn:
    started_at: datetime | None = None
    stopped_at: datetime | None = None
    transcript_parts: list[str] = field(default_factory=list)
    response_requested: bool = False
    interrupt_candidate: bool = False
    interrupt_confirmed: bool = False
    interrupt_ignored: bool = False
    interrupt_trigger_at: datetime | None = None
    interrupt_reason: str = "user_speech_started_during_ai_audio"

    @property
    def transcript(self) -> str:
        return "".join(self.transcript_parts).strip()


@dataclass(slots=True)
class ResponseLifecycle:
    active: bool = False
    cancel_pending: bool = False
    pending_create: bool = False
    pending_input_text: str | None = None


@dataclass(slots=True)
class PendingCallEnd:
    tool_call_id: str
    tool_reason: str
    end_reason: str
    final_response_started: bool = False
    scheduled: bool = False


class RealtimeCallAgentRunner:
    def __init__(
        self,
        provider_factory: ProviderFactory,
        registry: InMemorySessionRegistry,
        event_store: InMemoryEventStore,
        metrics_by_call_id: dict[str, CallMetrics] | None = None,
        audio_bridge: PcmAudioBridge | None = None,
        audio_publisher: AudioPublisherProtocol | None = None,
        audio_transport: RoomAudioTransportProtocol | None = None,
        ai_speaking_tail_grace_seconds: float = 0.6,
        browser_interrupt_recent_audio_seconds: float = 1.5,
        browser_interrupt_audio_suppress_seconds: float = 1.5,
        user_turn_stability_delay_seconds: float = 0.35,
        handoff_prompt_constraint_enabled: bool = False,
        call_end_scheduler: CallEndScheduler | None = None,
    ) -> None:
        self.provider_factory = provider_factory
        self.registry = registry
        self.event_store = event_store
        self.metrics_by_call_id = metrics_by_call_id if metrics_by_call_id is not None else {}
        self.audio_bridge = audio_bridge or PcmAudioBridge()
        self.audio_transport = audio_transport
        self.audio_publisher = audio_publisher or audio_transport
        self.ai_speaking_tail_grace_seconds = ai_speaking_tail_grace_seconds
        self.browser_interrupt_recent_audio_seconds = browser_interrupt_recent_audio_seconds
        self.browser_interrupt_audio_suppress_seconds = max(
            0.0,
            browser_interrupt_audio_suppress_seconds,
        )
        self.user_turn_stability_delay_seconds = max(0.0, user_turn_stability_delay_seconds)
        self.handoff_prompt_constraint_enabled = handoff_prompt_constraint_enabled
        self.call_end_scheduler = call_end_scheduler
        self._providers: dict[str, RealtimeProviderProtocol] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._audio_tasks: dict[str, asyncio.Task[None]] = {}
        self._playout_tasks: dict[str, asyncio.Task[None]] = {}
        self._turn_response_tasks: dict[str, asyncio.Task[None]] = {}
        self._last_ai_audio_published_at: dict[str, datetime] = {}
        self._browser_interrupt_audio_suppressed_until: dict[str, datetime] = {}
        self._pending_user_turns: dict[str, PendingUserTurn] = {}
        self._response_lifecycles: dict[str, ResponseLifecycle] = {}
        self._pending_call_ends: dict[str, PendingCallEnd] = {}

    async def start(self, session: CallSession) -> None:
        provider = self.provider_factory(session)
        self._providers[session.call_id] = provider
        await provider.connect()
        await provider.update_session(self._session_config(session))
        self._tasks[session.call_id] = asyncio.create_task(
            self._consume_provider_events(session.call_id, provider)
        )
        if self.audio_transport is not None:
            await self.audio_transport.start(session)
            self._audio_tasks[session.call_id] = asyncio.create_task(
                self._consume_room_audio(session.call_id, self.audio_transport)
            )

    async def stop(self, call_id: str) -> None:
        await self._cancel_playout_task(call_id)
        await self._cancel_turn_response_task(call_id)

        audio_task = self._audio_tasks.pop(call_id, None)
        if audio_task is not None and not audio_task.done():
            audio_task.cancel()
            try:
                await audio_task
            except asyncio.CancelledError:
                pass
        if self.audio_transport is not None:
            await self.audio_transport.close(call_id)

        task = self._tasks.pop(call_id, None)
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        provider = self._providers.pop(call_id, None)
        if provider is not None:
            await provider.close()
        self._last_ai_audio_published_at.pop(call_id, None)
        self._browser_interrupt_audio_suppressed_until.pop(call_id, None)
        self._pending_user_turns.pop(call_id, None)
        self._response_lifecycles.pop(call_id, None)
        self._pending_call_ends.pop(call_id, None)

    async def suspend_for_handoff(self, call_id: str) -> None:
        await self._cancel_playout_task(call_id)
        if self.audio_publisher is not None:
            try:
                await self.audio_publisher.stop_audio(call_id)
            except Exception as exc:
                self._append_event(
                    call_id,
                    "handoff_prompt_cleanup_failed",
                    "agent",
                    {
                        "step": "stop_audio",
                        "errorType": type(exc).__name__,
                        "message": str(exc),
                    },
                )

        provider = self._providers.get(call_id)
        if provider is not None:
            with suppress(Exception):
                await provider.cancel_response()
            with suppress(Exception):
                await provider.clear_input_audio()
            self._clear_response_lifecycle(call_id)
        await self.stop(call_id)

    async def wait(self, call_id: str) -> None:
        for task in (self._tasks.get(call_id), self._audio_tasks.get(call_id)):
            if task is None:
                continue
            await task
        turn_response_task = self._turn_response_tasks.get(call_id)
        if turn_response_task is not None:
            await turn_response_task

    async def send_audio_frame(self, call_id: str, frame: PcmAudioFrame) -> None:
        provider = self._providers[call_id]
        for chunk in self.audio_bridge.iter_qwen_input_chunks(frame):
            await provider.send_audio(chunk)

    async def start_opening(self, call_id: str) -> None:
        provider = self._providers[call_id]
        session = self.registry.get(call_id)
        opening_message = str(self._config_value(session.effective_config, "opening_message", ""))
        input_text = f"请主动说出开场白：{opening_message}" if opening_message else None
        await self._request_response(call_id, provider, input_text=input_text)

    async def _consume_room_audio(
        self,
        call_id: str,
        audio_transport: RoomAudioTransportProtocol,
    ) -> None:
        try:
            async for frame in audio_transport.receive_audio_frames(call_id):
                await self.send_audio_frame(call_id, frame)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._fail_running_session(
                call_id,
                end_reason="audio_transport_error",
                failure_stage="audio_transport",
                failure_message=f"通话音频传输异常: {exc}",
            )

    async def _consume_provider_events(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
    ) -> None:
        try:
            async for provider_event in provider.receive_events():
                event_timestamp = self._append_event(
                    call_id,
                    provider_event.type,
                    "provider",
                    self._event_payload(provider_event.type, provider_event.payload),
                )
                if provider_event.type == "user_speech_started":
                    await self._handle_user_speech_started(call_id, provider, event_timestamp)
                elif provider_event.type == "user_speech_stopped":
                    await self._handle_user_speech_stopped(call_id, provider, event_timestamp)
                elif provider_event.type in {"user_transcript_delta", "user_transcript_done"}:
                    await self._handle_user_transcript(
                        call_id,
                        provider,
                        provider_event,
                        event_timestamp,
                    )
                elif provider_event.type == "tool_call_done":
                    await self._handle_tool_call_done(call_id, provider, provider_event)
                else:
                    await self._apply_provider_event(
                        call_id,
                        provider,
                        provider_event.type,
                        event_timestamp,
                        provider_event.payload,
                    )
                if provider_event.type == "model_audio_delta":
                    await self._publish_model_audio_delta(call_id, provider_event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._fail_running_session(
                call_id,
                end_reason="model_error",
                failure_stage="provider_event_stream",
                failure_message=f"模型事件流异常: {exc}",
            )

    async def record_browser_speech_candidate(
        self,
        call_id: str,
        trigger_timestamp: datetime,
    ) -> bool:
        if call_id not in self._providers:
            return False
        session = self.registry.get(call_id)
        if session.status in {
            CallSessionStatus.COMPLETED,
            CallSessionStatus.FAILED,
        }:
            return False
        if session.status in {
            CallSessionStatus.CONNECTED,
            CallSessionStatus.AI_THINKING,
        } and self._has_recent_ai_audio(call_id, trigger_timestamp):
            # 浏览器侧 VAD 可能晚于服务端状态变更到达；近期 AI 音频仍按可打断处理。
            self.registry.transition(call_id, CallSessionStatus.AI_SPEAKING)
            session = self.registry.get(call_id)
        if session.status != CallSessionStatus.AI_SPEAKING:
            return False

        turn = self._pending_turn(call_id, reset_if_finished=True)
        if turn.started_at is None:
            turn.started_at = trigger_timestamp
        self._mark_interrupt_candidate(
            call_id=call_id,
            turn=turn,
            trigger_timestamp=trigger_timestamp,
            source="browser",
            reason="browser_user_speech_started_during_ai_audio",
        )
        self._browser_interrupt_audio_suppressed_until[call_id] = datetime.now(
            timezone.utc
        ) + timedelta(seconds=self.browser_interrupt_audio_suppress_seconds)
        await self._cancel_playout_task(call_id)
        if self.audio_publisher is not None:
            try:
                await self.audio_publisher.stop_audio(call_id)
            except Exception as exc:
                self._append_event(
                    call_id,
                    "interrupt_cleanup_failed",
                    "agent",
                    {
                        "step": "browser_stop_audio",
                        "errorType": type(exc).__name__,
                        "message": str(exc),
                    },
                )
        return True

    async def _apply_provider_event(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        event_type: str,
        timestamp: datetime,
        payload: dict[str, Any],
    ) -> None:
        session = self.registry.get(call_id)
        metrics = self.metrics_by_call_id.setdefault(call_id, CallMetrics())

        if event_type == "model_session_started" and session.status == CallSessionStatus.READY:
            self.registry.transition(call_id, CallSessionStatus.CONNECTED)
        elif event_type == "model_response_started":
            self._mark_response_started(call_id)
        elif event_type == "user_speech_started" and session.status == CallSessionStatus.CONNECTED:
            self.registry.transition(call_id, CallSessionStatus.USER_SPEAKING)
        elif (
            event_type == "user_speech_stopped"
            and session.status == CallSessionStatus.USER_SPEAKING
        ):
            metrics.mark_user_speech_stopped(timestamp)
            self.registry.transition(call_id, CallSessionStatus.AI_THINKING)
        elif event_type == "model_audio_delta" and session.status in {
            CallSessionStatus.CONNECTED,
            CallSessionStatus.AI_THINKING,
        }:
            self._cancel_playout_task_nowait(call_id)
            metrics.mark_model_audio_delta(timestamp)
            self.registry.transition(call_id, CallSessionStatus.AI_SPEAKING)
        elif (
            event_type == "model_response_done" and session.status == CallSessionStatus.AI_SPEAKING
        ):
            self._complete_ai_speaking_after_playout(call_id)
            await self._complete_response_and_flush_pending(call_id, provider)
        elif event_type == "model_response_done":
            await self._complete_response_and_flush_pending(call_id, provider)
        elif event_type == "model_error":
            self._fail_running_session(
                call_id,
                end_reason="model_error",
                failure_stage="model",
                failure_message=self._failure_message(payload) or "模型调用失败",
            )

        self.registry.get(call_id).metrics = metrics.snapshot()

    async def _handle_user_speech_started(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        timestamp: datetime,
    ) -> None:
        session = self.registry.get(call_id)
        turn = self._pending_turn(call_id, reset_if_finished=True)
        self._cancel_turn_response_task_nowait(call_id)
        if turn.stopped_at is not None and not turn.response_requested:
            turn.stopped_at = None
        turn.started_at = timestamp

        if session.status in {
            CallSessionStatus.CONNECTED,
            CallSessionStatus.AI_THINKING,
        } and self._has_recent_ai_audio(call_id, timestamp):
            self.registry.transition(call_id, CallSessionStatus.AI_SPEAKING)
            session = self.registry.get(call_id)

        if session.status == CallSessionStatus.CONNECTED:
            self.registry.transition(call_id, CallSessionStatus.USER_SPEAKING)
            return

        if session.status != CallSessionStatus.AI_SPEAKING:
            return

        self._mark_interrupt_candidate(
            call_id=call_id,
            turn=turn,
            trigger_timestamp=timestamp,
            source="provider",
            reason="user_speech_started_during_ai_audio",
        )
        await self._maybe_confirm_interrupt_from_turn(call_id, provider, timestamp)

    async def _handle_user_speech_stopped(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        timestamp: datetime,
    ) -> None:
        turn = self._pending_turn(call_id)
        turn.stopped_at = timestamp
        await self._maybe_schedule_response_from_turn(call_id, provider, timestamp)
        if not turn.transcript and not turn.response_requested:
            self._ignore_empty_turn(call_id, turn, "no_valid_transcript")

    async def _handle_user_transcript(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        provider_event: ProviderEvent,
        timestamp: datetime,
    ) -> None:
        text = self._transcript_text(provider_event)
        if not text:
            return
        turn = self._pending_turn(call_id)
        if provider_event.type == "user_transcript_done" or (
            provider_event.type == "user_transcript_delta"
            and ("text" in provider_event.payload or "stash" in provider_event.payload)
        ):
            turn.transcript_parts = [text]
        else:
            turn.transcript_parts.append(text)
        await self._maybe_confirm_interrupt_from_turn(call_id, provider, timestamp)
        await self._maybe_schedule_response_from_turn(call_id, provider, timestamp)

    async def _handle_tool_call_done(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        provider_event: ProviderEvent,
    ) -> None:
        payload = provider_event.payload
        name = self._payload_string(payload, "name")
        if name == "request_handoff":
            await self._handle_handoff_tool_done(call_id, provider, provider_event)
            return
        if name != "schedule_call_end":
            return

        tool_call_id = self._payload_string(payload, "call_id", "callId")
        arguments = self._tool_call_arguments(payload)
        tool_reason = arguments.get("reason")
        end_reason = (
            CALL_END_REASON_MAPPING.get(tool_reason) if isinstance(tool_reason, str) else None
        )
        if not tool_call_id or end_reason is None or not isinstance(tool_reason, str):
            self._append_event(
                call_id,
                "call_end_tool_ignored",
                "agent",
                {
                    "reason": "invalid_tool_arguments",
                    "toolCallId": tool_call_id,
                },
            )
            return

        if call_id not in self._pending_call_ends:
            self._pending_call_ends[call_id] = PendingCallEnd(
                tool_call_id=tool_call_id,
                tool_reason=tool_reason,
                end_reason=end_reason,
            )
            self._append_event(
                call_id,
                "call_end_tool_requested",
                "agent",
                {
                    "toolCallId": tool_call_id,
                    "toolReason": tool_reason,
                    "endReason": end_reason,
                },
            )

        try:
            await provider.submit_tool_result(
                tool_call_id,
                "已记录。请用一句简短礼貌的话结束通话，不要继续提出新问题。",
            )
        except Exception as exc:
            self._append_event(
                call_id,
                "agent_error",
                "agent",
                {
                    "message": f"提交结束通话工具结果失败: {exc}",
                    "toolCallId": tool_call_id,
                },
            )

    async def _handle_handoff_tool_done(
        self,
        call_id: str,
        _provider: RealtimeProviderProtocol,
        provider_event: ProviderEvent,
    ) -> None:
        payload = provider_event.payload
        tool_call_id = self._payload_string(payload, "call_id", "callId")
        arguments = self._tool_call_arguments(payload)
        reason = arguments.get("reason")
        if not tool_call_id or not isinstance(reason, str) or reason not in HANDOFF_REASON_VALUES:
            self._append_event(
                call_id,
                "handoff_tool_ignored",
                "agent",
                {
                    "reason": "invalid_tool_arguments",
                    "toolCallId": tool_call_id,
                },
            )
            return

        self._append_event(
            call_id,
            "handoff_tool_requested",
            "agent",
            {
                "toolCallId": tool_call_id,
                "reason": reason,
            },
        )

    @staticmethod
    def _payload_string(payload: dict[str, Any], *keys: str) -> str | None:
        for key in keys:
            value = payload.get(key)
            if isinstance(value, str) and value:
                return value
        item = payload.get("item")
        if isinstance(item, dict):
            for key in keys:
                value = item.get(key)
                if isinstance(value, str) and value:
                    return value
        return None

    @staticmethod
    def _tool_call_arguments(payload: dict[str, Any]) -> dict[str, Any]:
        arguments = payload.get("arguments")
        if isinstance(arguments, dict):
            return arguments
        if not isinstance(arguments, str) or not arguments.strip():
            return {}
        try:
            parsed = json.loads(arguments)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _pending_turn(self, call_id: str, reset_if_finished: bool = False) -> PendingUserTurn:
        turn = self._pending_user_turns.get(call_id)
        if turn is None or (
            reset_if_finished
            and (turn.response_requested or (turn.stopped_at is not None and not turn.transcript))
        ):
            turn = PendingUserTurn()
            self._pending_user_turns[call_id] = turn
        return turn

    def _mark_interrupt_candidate(
        self,
        call_id: str,
        turn: PendingUserTurn,
        trigger_timestamp: datetime,
        source: str,
        reason: str,
    ) -> None:
        if not turn.interrupt_candidate:
            self._append_event(
                call_id,
                "interrupt_candidate",
                "agent",
                {"source": source, "reason": reason},
            )
        turn.interrupt_candidate = True
        turn.interrupt_ignored = False
        turn.interrupt_trigger_at = trigger_timestamp
        turn.interrupt_reason = reason

    async def _maybe_confirm_interrupt_from_turn(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        timestamp: datetime,
    ) -> None:
        turn = self._pending_turn(call_id)
        # 只在已有有效文字时确认打断；短噪声、回声和无转写输入停留在候选阶段。
        if (
            turn.interrupt_candidate
            and not turn.interrupt_confirmed
            and self._is_stale_browser_interrupt_candidate(turn, timestamp)
        ):
            self._ignore_empty_turn(call_id, turn, "browser_candidate_expired")
            return
        if not turn.interrupt_candidate or turn.interrupt_confirmed or not turn.transcript:
            return
        await self._confirm_interrupt(
            call_id,
            provider,
            turn.interrupt_trigger_at or timestamp,
            reason=turn.interrupt_reason,
            clear_input_audio=False,
        )
        turn.interrupt_confirmed = True

    async def _maybe_schedule_response_from_turn(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        timestamp: datetime,
    ) -> None:
        turn = self._pending_turn(call_id)
        if turn.response_requested or turn.stopped_at is None or not turn.transcript:
            return
        await self._maybe_confirm_interrupt_from_turn(call_id, provider, timestamp)
        if self.user_turn_stability_delay_seconds <= 0:
            await self._request_response_from_turn(call_id, provider, turn)
            return
        # 给用户话尾一个很短的稳定窗口，避免断句或补字触发多次 response.create。
        existing_task = self._turn_response_tasks.get(call_id)
        if existing_task is not None and not existing_task.done():
            return
        stopped_at = turn.stopped_at
        self._turn_response_tasks[call_id] = asyncio.create_task(
            self._request_response_after_turn_stability(
                call_id,
                provider,
                turn,
                stopped_at,
            ),
            name=f"ai-call-turn-response-{call_id}",
        )

    async def _request_response_after_turn_stability(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        turn: PendingUserTurn,
        stopped_at: datetime,
    ) -> None:
        try:
            await asyncio.sleep(self.user_turn_stability_delay_seconds)
            if self._pending_user_turns.get(call_id) is not turn:
                return
            if turn.stopped_at != stopped_at or turn.response_requested or not turn.transcript:
                return
            if self.registry.get(call_id).status in {
                CallSessionStatus.COMPLETED,
                CallSessionStatus.FAILED,
            }:
                return
            await self._request_response_from_turn(call_id, provider, turn)
        except asyncio.CancelledError:
            raise
        finally:
            if self._turn_response_tasks.get(call_id) is asyncio.current_task():
                self._turn_response_tasks.pop(call_id, None)

    async def _request_response_from_turn(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        turn: PendingUserTurn,
    ) -> None:
        if turn.response_requested or turn.stopped_at is None or not turn.transcript:
            return

        metrics = self.metrics_by_call_id.setdefault(call_id, CallMetrics())
        metrics.mark_user_speech_stopped(turn.stopped_at)
        session = self.registry.get(call_id)
        if session.status in {
            CallSessionStatus.USER_SPEAKING,
            CallSessionStatus.CONNECTED,
            CallSessionStatus.INTERRUPTED,
        }:
            self.registry.transition(call_id, CallSessionStatus.AI_THINKING)
        await self._request_response(call_id, provider)
        turn.response_requested = True
        session.metrics = metrics.snapshot()

    def _ignore_empty_turn(
        self,
        call_id: str,
        turn: PendingUserTurn,
        reason: str,
    ) -> None:
        if turn.interrupt_candidate and not turn.interrupt_ignored:
            self._append_event(
                call_id,
                "interrupt_ignored",
                "agent",
                {"reason": reason},
            )
            turn.interrupt_candidate = False
            turn.interrupt_ignored = True
        session = self.registry.get(call_id)
        if session.status == CallSessionStatus.USER_SPEAKING:
            self.registry.transition(call_id, CallSessionStatus.WAITING)
            self.registry.transition(call_id, CallSessionStatus.CONNECTED)

    def _append_event(
        self,
        call_id: str,
        event_type: str,
        source: str,
        payload: dict[str, Any] | None = None,
    ) -> datetime:
        event = self.event_store.append(
            call_id=call_id,
            type=event_type,
            source=source,
            payload=payload,
            timestamp=datetime.now(timezone.utc),
        )
        self.registry.get(call_id).last_event_at = event.timestamp
        return event.timestamp

    def _event_payload(self, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        event_payload = dict(payload)
        session = event_payload.get("session")
        if isinstance(session, dict) and isinstance(session.get("instructions"), str):
            session_payload = dict(session)
            session_payload["instructions"] = "<redacted>"
            event_payload["session"] = session_payload
        if event_type == "model_audio_delta":
            # 原始音频 delta 只用于实时播放，事件列表只保留体积信息。
            delta = event_payload.get("delta")
            if isinstance(delta, str):
                event_payload["delta"] = "<redacted_audio_delta>"
                event_payload["deltaBytes"] = self._base64_decoded_size(delta)
        return event_payload

    @staticmethod
    def _base64_decoded_size(value: str) -> int | None:
        try:
            return len(base64.b64decode(value))
        except Exception:
            return None

    async def _publish_model_audio_delta(
        self,
        call_id: str,
        provider_event: ProviderEvent,
    ) -> None:
        # 供应商在打断后仍可能吐缓存音频，发布前用状态闸门拦截。
        if self.registry.get(call_id).status != CallSessionStatus.AI_SPEAKING:
            return
        if self._is_browser_audio_suppressed(call_id):
            return
        if self.audio_publisher is None:
            return
        delta = provider_event.payload.get("delta")
        if not isinstance(delta, str) or not delta:
            return

        frame = self.audio_bridge.decode_qwen_output_delta(delta)
        for playout_frame in self.audio_bridge.iter_output_playout_frames(frame):
            if self.registry.get(call_id).status != CallSessionStatus.AI_SPEAKING:
                return
            await self.audio_publisher.publish_audio(call_id, playout_frame)
            if self.registry.get(call_id).status != CallSessionStatus.AI_SPEAKING:
                await self.audio_publisher.stop_audio(call_id)
                return
            event_timestamp = self._append_event(
                call_id,
                "ai_audio_published",
                "agent",
                {
                    "sampleRateHz": playout_frame.sample_rate_hz,
                    "bytes": len(playout_frame.data),
                },
            )
            metrics = self.metrics_by_call_id.setdefault(call_id, CallMetrics())
            metrics.mark_audio_published(event_timestamp)
            self._last_ai_audio_published_at[call_id] = event_timestamp
            self.registry.get(call_id).metrics = metrics.snapshot()

    def _has_recent_ai_audio(self, call_id: str, trigger_timestamp: datetime) -> bool:
        last_published_at = self._last_ai_audio_published_at.get(call_id)
        if last_published_at is None:
            return False
        elapsed_seconds = (trigger_timestamp - last_published_at).total_seconds()
        return 0 <= elapsed_seconds <= self.browser_interrupt_recent_audio_seconds

    def _is_browser_audio_suppressed(self, call_id: str) -> bool:
        suppressed_until = self._browser_interrupt_audio_suppressed_until.get(call_id)
        if suppressed_until is None:
            return False
        if datetime.now(timezone.utc) < suppressed_until:
            return True
        self._browser_interrupt_audio_suppressed_until.pop(call_id, None)
        return False

    def _is_stale_browser_interrupt_candidate(
        self,
        turn: PendingUserTurn,
        timestamp: datetime,
    ) -> bool:
        if turn.interrupt_reason != "browser_user_speech_started_during_ai_audio":
            return False
        if turn.interrupt_trigger_at is None:
            return False
        max_age_seconds = max(
            self.browser_interrupt_audio_suppress_seconds,
            self.browser_interrupt_recent_audio_seconds,
        )
        return (timestamp - turn.interrupt_trigger_at).total_seconds() > max_age_seconds

    @staticmethod
    def _transcript_text(provider_event: ProviderEvent) -> str:
        payload = provider_event.payload
        if provider_event.type == "user_transcript_done":
            value = payload.get("transcript")
        elif provider_event.type == "user_transcript_delta":
            text = payload.get("text")
            stash = payload.get("stash")
            if isinstance(text, str) or isinstance(stash, str):
                return f"{text or ''}{stash or ''}".strip()
            value = payload.get("delta")
        else:
            value = payload.get("delta")
        return value.strip() if isinstance(value, str) else ""

    async def _confirm_interrupt(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        trigger_timestamp: datetime,
        reason: str = "user_speech_started_during_ai_audio",
        clear_input_audio: bool = False,
    ) -> None:
        await self._cancel_playout_task(call_id)

        metrics = self.metrics_by_call_id.setdefault(call_id, CallMetrics())
        metrics.mark_interrupt_confirmed(trigger_timestamp)
        self.registry.transition(call_id, CallSessionStatus.INTERRUPTED)

        cleanup_errors: list[dict[str, str]] = []
        if self.audio_publisher is not None:
            try:
                await self.audio_publisher.stop_audio(call_id)
            except Exception as exc:
                cleanup_errors.append({
                    "step": "stop_audio",
                    "errorType": type(exc).__name__,
                    "message": str(exc),
                })
        response_lifecycle = self._response_lifecycle(call_id)
        try:
            if response_lifecycle.active:
                response_lifecycle.cancel_pending = True
            await provider.cancel_response()
        except Exception as exc:
            response_lifecycle.cancel_pending = False
            cleanup_errors.append({
                "step": "cancel_response",
                "errorType": type(exc).__name__,
                "message": str(exc),
            })
        if clear_input_audio:
            try:
                await provider.clear_input_audio()
            except Exception as exc:
                cleanup_errors.append({
                    "step": "clear_input_audio",
                    "errorType": type(exc).__name__,
                    "message": str(exc),
                })

        event_timestamp = self._append_event(
            call_id,
            "interrupt_confirmed",
            "agent",
            {"reason": reason},
        )
        self._browser_interrupt_audio_suppressed_until.pop(call_id, None)
        for cleanup_error in cleanup_errors:
            self._append_event(
                call_id,
                "interrupt_cleanup_failed",
                "agent",
                cleanup_error,
            )
        metrics.mark_ai_audio_stopped(event_timestamp)
        if self.registry.get(call_id).status == CallSessionStatus.INTERRUPTED:
            self.registry.transition(call_id, CallSessionStatus.USER_SPEAKING)
        self.registry.get(call_id).metrics = metrics.snapshot()

    async def _request_response(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        *,
        input_text: str | None = None,
    ) -> bool:
        lifecycle = self._response_lifecycle(call_id)
        if lifecycle.active or lifecycle.cancel_pending:
            lifecycle.pending_create = True
            if input_text:
                lifecycle.pending_input_text = input_text
            return False
        try:
            await provider.create_response(input_text)
        except Exception as exc:
            self._fail_running_session(
                call_id,
                end_reason="model_error",
                failure_stage="model_response_create",
                failure_message=f"创建模型响应失败: {exc}",
            )
            return False
        lifecycle.active = True
        lifecycle.cancel_pending = False
        lifecycle.pending_create = False
        lifecycle.pending_input_text = None
        return True

    def _mark_response_started(self, call_id: str) -> None:
        lifecycle = self._response_lifecycle(call_id)
        lifecycle.active = True
        pending_call_end = self._pending_call_ends.get(call_id)
        if pending_call_end is not None:
            pending_call_end.final_response_started = True

    async def _complete_response_and_flush_pending(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
    ) -> None:
        lifecycle = self._response_lifecycle(call_id)
        lifecycle.active = False
        lifecycle.cancel_pending = False
        if not lifecycle.pending_create:
            self._schedule_pending_call_end_nowait(call_id)
            return
        if self.registry.get(call_id).status in {
            CallSessionStatus.COMPLETED,
            CallSessionStatus.FAILED,
        }:
            lifecycle.pending_create = False
            lifecycle.pending_input_text = None
            return
        input_text = lifecycle.pending_input_text
        lifecycle.pending_create = False
        lifecycle.pending_input_text = None
        await self._request_response(call_id, provider, input_text=input_text)

    def _schedule_pending_call_end_nowait(self, call_id: str) -> None:
        pending_call_end = self._pending_call_ends.get(call_id)
        if pending_call_end is None or pending_call_end.scheduled:
            return
        if not pending_call_end.final_response_started:
            return
        if self.registry.get(call_id).status in {
            CallSessionStatus.AI_SPEAKING,
            CallSessionStatus.USER_SPEAKING,
            CallSessionStatus.INTERRUPTED,
        }:
            return
        if self.call_end_scheduler is None:
            return

        pending_call_end.scheduled = True
        self._append_event(
            call_id,
            "call_end_scheduled",
            "agent",
            {
                "toolCallId": pending_call_end.tool_call_id,
                "toolReason": pending_call_end.tool_reason,
                "endReason": pending_call_end.end_reason,
            },
        )
        try:
            self.call_end_scheduler(call_id, pending_call_end.end_reason)
        except Exception as exc:
            pending_call_end.scheduled = False
            self._append_event(
                call_id,
                "agent_error",
                "agent",
                {
                    "message": f"调度结束通话失败: {exc}",
                    "toolCallId": pending_call_end.tool_call_id,
                },
            )

    def _clear_response_lifecycle(self, call_id: str) -> None:
        lifecycle = self._response_lifecycle(call_id)
        lifecycle.active = False
        lifecycle.cancel_pending = False
        lifecycle.pending_create = False
        lifecycle.pending_input_text = None

    def _fail_running_session(
        self,
        call_id: str,
        *,
        end_reason: str,
        failure_stage: str,
        failure_message: str,
    ) -> None:
        self._clear_response_lifecycle(call_id)
        session = self.registry.get(call_id)
        if session.status in {CallSessionStatus.COMPLETED, CallSessionStatus.FAILED}:
            return
        self.registry.transition(call_id, CallSessionStatus.FAILED)
        self._append_event(
            call_id,
            "session_failed",
            "agent",
            {
                "endReason": end_reason,
                "failureStage": failure_stage,
                "failureMessage": failure_message,
            },
        )
        if self.call_end_scheduler is None:
            return
        try:
            self.call_end_scheduler(call_id, end_reason)
        except Exception as exc:
            self._append_event(
                call_id,
                "agent_error",
                "agent",
                {"message": f"调度异常结束通话失败: {exc}", "endReason": end_reason},
            )

    def _response_lifecycle(self, call_id: str) -> ResponseLifecycle:
        lifecycle = self._response_lifecycles.get(call_id)
        if lifecycle is None:
            lifecycle = ResponseLifecycle()
            self._response_lifecycles[call_id] = lifecycle
        return lifecycle

    def _complete_ai_speaking_after_playout(self, call_id: str) -> None:
        wait_for_playout = getattr(self.audio_publisher, "wait_for_playout", None)
        if wait_for_playout is None:
            self.registry.transition(call_id, CallSessionStatus.CONNECTED)
            self._schedule_pending_call_end_nowait(call_id)
            return

        self._cancel_playout_task_nowait(call_id)
        self._playout_tasks[call_id] = asyncio.create_task(
            self._wait_for_playout_and_mark_connected(call_id, wait_for_playout)
        )

    async def _wait_for_playout_and_mark_connected(
        self,
        call_id: str,
        wait_for_playout: Any,
    ) -> None:
        try:
            await wait_for_playout(call_id)
            if self.ai_speaking_tail_grace_seconds > 0:
                await asyncio.sleep(self.ai_speaking_tail_grace_seconds)
            session = self.registry.get(call_id)
            if session.status == CallSessionStatus.AI_SPEAKING:
                self.registry.transition(call_id, CallSessionStatus.CONNECTED)
            self._schedule_pending_call_end_nowait(call_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._append_event(
                call_id,
                "agent_error",
                "agent",
                {"message": f"等待 AI 音频播放结束失败: {exc}"},
            )
        finally:
            if self._playout_tasks.get(call_id) is asyncio.current_task():
                self._playout_tasks.pop(call_id, None)

    def _cancel_playout_task_nowait(self, call_id: str) -> None:
        task = self._playout_tasks.pop(call_id, None)
        if task is not None and not task.done():
            task.cancel()

    async def _cancel_playout_task(self, call_id: str) -> None:
        task = self._playout_tasks.pop(call_id, None)
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    def _cancel_turn_response_task_nowait(self, call_id: str) -> None:
        task = self._turn_response_tasks.pop(call_id, None)
        if task is not None and not task.done():
            task.cancel()

    async def _cancel_turn_response_task(self, call_id: str) -> None:
        task = self._turn_response_tasks.pop(call_id, None)
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    def _session_config(self, session: CallSession) -> QwenRealtimeSessionConfig:
        effective_instructions = self._config_value(session.effective_config, "instructions", None)
        if effective_instructions is None:
            instructions = str(self._config_value(session.effective_config, "prompt", ""))
            if self.handoff_prompt_constraint_enabled:
                instructions = self._with_handoff_capability_instructions(instructions)
            opening_message = str(
                self._config_value(session.effective_config, "opening_message", "")
            ).strip()
            if opening_message:
                instructions = (
                    f"{instructions}\n\n"
                    f"通话开始后，系统会触发你主动开场。请先自然说出这句开场白：{opening_message}"
                )
        else:
            instructions = str(effective_instructions)
        instructions = self._with_call_end_tool_instructions(instructions)
        return QwenRealtimeSessionConfig(
            voice=str(self._config_value(session.effective_config, "voice", "Tina")),
            instructions=instructions,
            vad_type=str(self._config_value(session.effective_config, "vad_type", "server_vad")),
            vad_threshold=float(self._config_value(session.effective_config, "vad_threshold", 0.5)),
            vad_silence_duration_ms=int(
                self._config_value(session.effective_config, "vad_silence_duration_ms", 800)
            ),
            tools=list(DEFAULT_REALTIME_TOOLS),
        )

    @staticmethod
    def _with_handoff_capability_instructions(instructions: str) -> str:
        clean_instructions = instructions.strip()
        if not clean_instructions:
            return HANDOFF_CAPABILITY_INSTRUCTIONS
        return f"{HANDOFF_CAPABILITY_INSTRUCTIONS}\n\n业务话术：\n{clean_instructions}"

    @staticmethod
    def _with_call_end_tool_instructions(instructions: str) -> str:
        clean_instructions = instructions.strip()
        if not clean_instructions:
            return CALL_END_TOOL_INSTRUCTIONS
        return f"{clean_instructions}\n\n{CALL_END_TOOL_INSTRUCTIONS}"

    @staticmethod
    def _config_value(config: Any, key: str, default: Any) -> Any:
        if isinstance(config, dict):
            return config.get(key, default)
        return getattr(config, key, default)

    @staticmethod
    def _failure_message(payload: dict[str, Any]) -> str | None:
        error = payload.get("error")
        if isinstance(error, dict):
            message = error.get("message") or error.get("code")
            return str(message) if message else None
        if isinstance(error, str):
            return error
        message = payload.get("message")
        return str(message) if message else None
