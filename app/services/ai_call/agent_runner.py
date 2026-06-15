from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from app.services.ai_call.audio_bridge import PcmAudioBridge, PcmAudioFrame
from app.services.ai_call.event_store import InMemoryEventStore
from app.services.ai_call.metrics import CallMetrics
from app.services.ai_call.providers.aliyun_qwen_realtime import QwenRealtimeSessionConfig
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

_CJK_PATTERN = re.compile(r"[\u4e00-\u9fff]")
_LATIN_WORD_PATTERN = re.compile(r"[a-z0-9]+", re.IGNORECASE)
_WEAK_BACKCHANNELS = {
    "ah",
    "eh",
    "er",
    "hm",
    "hmm",
    "mhm",
    "mm",
    "oh",
    "ok",
    "okay",
    "uh",
    "uhh",
    "um",
    "umm",
    "yeah",
    "yep",
    "啊",
    "啊啊",
    "呃",
    "呃呃",
    "嗯",
    "嗯嗯",
    "哦",
    "哦哦",
    "喔",
    "额",
    "额额",
}
_STRONG_SINGLE_WORD_INTERRUPTS = {
    "continue",
    "help",
    "stop",
    "wait",
    "停",
    "等",
    "继续",
}
_EARLY_BROWSER_SPEECH_INTERRUPT_REASON = (
    "browser_user_speech_started_before_ai_audio"
)
_DURING_AI_AUDIO_BROWSER_SPEECH_INTERRUPT_REASON = (
    "browser_user_speech_started_during_ai_audio"
)


@dataclass(slots=True)
class _PendingInterrupt:
    trigger_timestamp: datetime
    reason: str


@dataclass(slots=True)
class _PendingManualResponse:
    transcript: str
    queued_at: datetime
    cancel_requested: bool = False


class NullRealtimeAgentRunner:
    """Phase A 第一切片占位 Agent；真实音频接入在后续切片替换。"""

    async def start(self, session: CallSession) -> None:
        _ = session

    async def start_opening(self, call_id: str) -> None:
        _ = call_id

    async def stop(self, call_id: str) -> None:
        _ = call_id


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
        pending_interrupt_max_age_seconds: float = 4.0,
        pending_interrupt_upgrade_seconds: float = 0.5,
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
        self.pending_interrupt_max_age_seconds = pending_interrupt_max_age_seconds
        self.pending_interrupt_upgrade_seconds = pending_interrupt_upgrade_seconds
        self._providers: dict[str, RealtimeProviderProtocol] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._audio_tasks: dict[str, asyncio.Task[None]] = {}
        self._playout_tasks: dict[str, asyncio.Task[None]] = {}
        self._last_ai_audio_published_at: dict[str, datetime] = {}
        self._pending_browser_interrupts: dict[str, _PendingInterrupt] = {}
        self._pending_interrupt_upgrade_tasks: dict[str, asyncio.Task[None]] = {}
        self._pending_manual_responses: dict[str, _PendingManualResponse] = {}
        self._discard_model_audio_until_response_started: set[str] = set()
        self._discard_model_audio_until_response_done: set[str] = set()
        self._manual_response_inflight_call_ids: set[str] = set()
        self._opening_response_pending_call_ids: set[str] = set()

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
        self._cancel_pending_interrupt_upgrade_task_nowait(call_id)

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
        self._pending_browser_interrupts.pop(call_id, None)
        self._pending_manual_responses.pop(call_id, None)
        self._discard_model_audio_until_response_started.discard(call_id)
        self._discard_model_audio_until_response_done.discard(call_id)
        self._manual_response_inflight_call_ids.discard(call_id)
        self._opening_response_pending_call_ids.discard(call_id)

    async def wait(self, call_id: str) -> None:
        for task in (self._tasks.get(call_id), self._audio_tasks.get(call_id)):
            if task is None:
                continue
            await task

    async def send_audio_frame(self, call_id: str, frame: PcmAudioFrame) -> None:
        provider = self._providers[call_id]
        for chunk in self.audio_bridge.iter_qwen_input_chunks(frame):
            await provider.send_audio(chunk)

    async def start_opening(self, call_id: str) -> None:
        provider = self._providers[call_id]
        session = self.registry.get(call_id)
        opening_message = str(
            self._config_value(session.effective_config, "opening_message", "")
        )
        input_text = f"请主动说出开场白：{opening_message}" if opening_message else None
        self._opening_response_pending_call_ids.add(call_id)
        await provider.create_response(input_text)

    async def _consume_room_audio(
        self,
        call_id: str,
        audio_transport: RoomAudioTransportProtocol,
    ) -> None:
        async for frame in audio_transport.receive_audio_frames(call_id):
            await self.send_audio_frame(call_id, frame)

    async def _consume_provider_events(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
    ) -> None:
        async for provider_event in provider.receive_events():
            event_timestamp = self._append_event(
                call_id,
                provider_event.type,
                "provider",
                self._event_payload(provider_event.payload),
            )
            if provider_event.type == "model_response_started":
                self._discard_model_audio_until_response_started.discard(call_id)
            if provider_event.type in {"model_response_done", "model_error"}:
                self._manual_response_inflight_call_ids.discard(call_id)
                self._discard_model_audio_until_response_done.discard(call_id)
            if await self._maybe_upgrade_pending_browser_interrupt_before_ai_audio(
                call_id,
                provider,
                provider_event.type,
                event_timestamp,
            ):
                continue
            if self._should_discard_model_audio(call_id, provider_event.type):
                continue
            if self._has_pending_browser_interrupt(call_id) and (
                provider_event.type == "user_speech_started"
            ):
                pass
            elif self._is_interrupt_event(call_id, provider_event.type):
                await self._confirm_interrupt(call_id, provider, event_timestamp)
            elif await self._maybe_cancel_inflight_response_on_user_speech_started(
                call_id,
                provider,
                provider_event.type,
            ):
                pass
            elif self._has_pending_browser_interrupt(call_id) and (
                provider_event.type in {"user_transcript_delta", "user_transcript_done"}
            ):
                await self._maybe_confirm_pending_browser_interrupt(
                    call_id,
                    provider,
                    provider_event,
                    event_timestamp,
                )
            else:
                self._apply_provider_event(call_id, provider_event.type, event_timestamp)
            if provider_event.type == "user_transcript_done":
                await self._maybe_request_manual_response(
                    call_id,
                    provider,
                    provider_event,
                    event_timestamp,
                )
            if provider_event.type == "model_audio_delta":
                await self._publish_model_audio_delta(call_id, provider_event)
            if provider_event.type == "model_response_done":
                await self._maybe_request_queued_manual_response(
                    call_id,
                    provider,
                    event_timestamp,
                )
            elif provider_event.type == "model_error":
                self._pending_manual_responses.pop(call_id, None)

    async def confirm_browser_interrupt(
        self,
        call_id: str,
        trigger_timestamp: datetime,
    ) -> bool:
        session = self.registry.get(call_id)
        provider = self._providers.get(call_id)
        if provider is None:
            return False
        pending = self._pending_browser_interrupts.get(call_id)
        if pending is not None:
            if self._is_pending_interrupt_expired(pending, trigger_timestamp):
                self._clear_pending_browser_interrupt(call_id)
                self._append_event(
                    call_id,
                    "interrupt_ignored",
                    "agent",
                    {"reason": "pending_interrupt_expired"},
                )
            elif self._should_upgrade_pending_browser_interrupt(
                pending,
                trigger_timestamp,
            ):
                self._clear_pending_browser_interrupt(call_id)
                await self._confirm_interrupt(
                    call_id,
                    provider,
                    trigger_timestamp,
                    reason="browser_user_speech_repeated_during_ai_audio",
                )
                return True
            else:
                return True
        interrupt_reason = _DURING_AI_AUDIO_BROWSER_SPEECH_INTERRUPT_REASON
        if session.status != CallSessionStatus.AI_SPEAKING:
            if session.status not in {
                CallSessionStatus.CONNECTED,
                CallSessionStatus.AI_THINKING,
            }:
                return False
            if not self._has_recent_ai_audio(call_id, trigger_timestamp):
                interrupt_reason = _EARLY_BROWSER_SPEECH_INTERRUPT_REASON
        self._mark_pending_browser_interrupt(
            call_id,
            trigger_timestamp,
            reason=interrupt_reason,
        )
        return True

    def _apply_provider_event(
        self,
        call_id: str,
        event_type: str,
        timestamp: datetime,
    ) -> None:
        session = self.registry.get(call_id)
        metrics = self.metrics_by_call_id.setdefault(call_id, CallMetrics())

        if event_type == "model_session_started" and session.status == CallSessionStatus.READY:
            self.registry.transition(call_id, CallSessionStatus.CONNECTED)
        elif event_type == "user_speech_started" and session.status == CallSessionStatus.CONNECTED:
            self.registry.transition(call_id, CallSessionStatus.USER_SPEAKING)
        elif event_type == "user_speech_stopped" and session.status == CallSessionStatus.USER_SPEAKING:
            metrics.mark_user_speech_stopped(timestamp)
            self.registry.transition(call_id, CallSessionStatus.AI_THINKING)
        elif event_type == "model_audio_delta" and session.status in {
            CallSessionStatus.CONNECTED,
            CallSessionStatus.AI_THINKING,
        }:
            self._cancel_playout_task_nowait(call_id)
            metrics.mark_model_audio_delta(timestamp)
            self.registry.transition(call_id, CallSessionStatus.AI_SPEAKING)
        elif event_type == "model_response_done" and session.status == CallSessionStatus.AI_SPEAKING:
            self._complete_ai_speaking_after_playout(call_id)
        elif event_type == "model_error":
            self.registry.transition(call_id, CallSessionStatus.FAILED)

        self.registry.get(call_id).metrics = metrics.snapshot()

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

    def _event_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        event_payload = dict(payload)
        session = event_payload.get("session")
        if isinstance(session, dict) and isinstance(session.get("instructions"), str):
            session_payload = dict(session)
            session_payload["instructions"] = "<redacted>"
            event_payload["session"] = session_payload
        return event_payload

    async def _publish_model_audio_delta(
        self,
        call_id: str,
        provider_event: ProviderEvent,
    ) -> None:
        if self.registry.get(call_id).status != CallSessionStatus.AI_SPEAKING:
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

    def _mark_pending_browser_interrupt(
        self,
        call_id: str,
        trigger_timestamp: datetime,
        reason: str,
    ) -> None:
        if call_id in self._pending_browser_interrupts:
            return
        self._pending_browser_interrupts[call_id] = _PendingInterrupt(
            trigger_timestamp=trigger_timestamp,
            reason=reason,
        )
        self._append_event(
            call_id,
            "interrupt_pending",
            "agent",
            {"reason": reason},
        )
        if reason == _DURING_AI_AUDIO_BROWSER_SPEECH_INTERRUPT_REASON:
            self._schedule_pending_interrupt_upgrade(call_id)

    def _has_pending_browser_interrupt(self, call_id: str) -> bool:
        return call_id in self._pending_browser_interrupts

    def _clear_pending_browser_interrupt(self, call_id: str) -> _PendingInterrupt | None:
        self._cancel_pending_interrupt_upgrade_task_nowait(call_id)
        return self._pending_browser_interrupts.pop(call_id, None)

    def _schedule_pending_interrupt_upgrade(self, call_id: str) -> None:
        self._cancel_pending_interrupt_upgrade_task_nowait(call_id)
        self._pending_interrupt_upgrade_tasks[call_id] = asyncio.create_task(
            self._upgrade_pending_interrupt_after_delay(call_id)
        )

    def _cancel_pending_interrupt_upgrade_task_nowait(self, call_id: str) -> None:
        task = self._pending_interrupt_upgrade_tasks.pop(call_id, None)
        if task is None or task.done() or task is asyncio.current_task():
            return
        task.cancel()

    async def _upgrade_pending_interrupt_after_delay(self, call_id: str) -> None:
        try:
            await asyncio.sleep(self.pending_interrupt_upgrade_seconds)
            pending = self._pending_browser_interrupts.get(call_id)
            provider = self._providers.get(call_id)
            if (
                pending is None
                or provider is None
                or pending.reason != _DURING_AI_AUDIO_BROWSER_SPEECH_INTERRUPT_REASON
            ):
                return
            session = self.registry.get(call_id)
            if session.status not in {
                CallSessionStatus.AI_SPEAKING,
                CallSessionStatus.CONNECTED,
                CallSessionStatus.AI_THINKING,
            }:
                self._pending_browser_interrupts.pop(call_id, None)
                return
            if self._is_pending_interrupt_expired(
                pending,
                datetime.now(timezone.utc),
            ):
                self._pending_browser_interrupts.pop(call_id, None)
                self._append_event(
                    call_id,
                    "interrupt_ignored",
                    "agent",
                    {"reason": "pending_interrupt_expired"},
                )
                return
            self._pending_browser_interrupts.pop(call_id, None)
            await self._confirm_interrupt(
                call_id,
                provider,
                pending.trigger_timestamp,
                reason="browser_user_speech_timeout_during_ai_audio",
            )
        except asyncio.CancelledError:
            raise
        finally:
            if self._pending_interrupt_upgrade_tasks.get(call_id) is asyncio.current_task():
                self._pending_interrupt_upgrade_tasks.pop(call_id, None)

    def _is_interrupt_event(self, call_id: str, event_type: str) -> bool:
        session = self.registry.get(call_id)
        return (
            event_type == "user_speech_started"
            and session.status == CallSessionStatus.AI_SPEAKING
        )

    def _should_discard_model_audio(self, call_id: str, event_type: str) -> bool:
        return (
            event_type == "model_audio_delta"
            and (
                call_id in self._discard_model_audio_until_response_started
                or call_id in self._discard_model_audio_until_response_done
            )
        )

    async def _maybe_cancel_inflight_response_on_user_speech_started(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        event_type: str,
    ) -> bool:
        if event_type != "user_speech_started":
            return False
        session = self.registry.get(call_id)
        if (
            session.status != CallSessionStatus.AI_THINKING
            or call_id not in self._manual_response_inflight_call_ids
        ):
            return False

        self.registry.transition(call_id, CallSessionStatus.USER_SPEAKING)
        self._discard_model_audio_until_response_done.add(call_id)
        await self._cancel_inflight_response_for_queued_manual_response(call_id, provider)
        return True

    async def _maybe_confirm_pending_browser_interrupt(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        provider_event: ProviderEvent,
        event_timestamp: datetime,
    ) -> None:
        pending = self._pending_browser_interrupts.get(call_id)
        if pending is None:
            return
        if self._is_pending_interrupt_expired(pending, event_timestamp):
            self._clear_pending_browser_interrupt(call_id)
            self._append_event(
                call_id,
                "interrupt_ignored",
                "agent",
                {"reason": "pending_interrupt_expired"},
            )
            return

        transcript = self._transcript_text(provider_event.payload)
        if not transcript:
            return
        if self._is_weak_backchannel(transcript):
            if provider_event.type == "user_transcript_done":
                self._clear_pending_browser_interrupt(call_id)
                self._append_event(
                    call_id,
                    "interrupt_ignored",
                    "agent",
                    {
                        "reason": "weak_backchannel",
                        "transcript": transcript,
                    },
                )
            return
        if provider_event.type != "user_transcript_done" and not self._is_meaningful_partial(
            transcript
            ):
            return

        self._clear_pending_browser_interrupt(call_id)
        await self._confirm_interrupt(
            call_id,
            provider,
            pending.trigger_timestamp,
            reason="browser_user_speech_confirmed_during_ai_audio",
            next_status=CallSessionStatus.AI_THINKING,
        )

    async def _maybe_upgrade_pending_browser_interrupt_before_ai_audio(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        event_type: str,
        event_timestamp: datetime,
    ) -> bool:
        if event_type != "model_audio_delta":
            return False
        pending = self._pending_browser_interrupts.get(call_id)
        if pending is None:
            return False
        if self._is_pending_interrupt_expired(pending, event_timestamp):
            self._clear_pending_browser_interrupt(call_id)
            self._append_event(
                call_id,
                "interrupt_ignored",
                "agent",
                {"reason": "pending_interrupt_expired"},
            )
            return False
        if pending.reason == _EARLY_BROWSER_SPEECH_INTERRUPT_REASON:
            self._clear_pending_browser_interrupt(call_id)
            await self._confirm_interrupt(
                call_id,
                provider,
                pending.trigger_timestamp,
                reason=_EARLY_BROWSER_SPEECH_INTERRUPT_REASON,
            )
            return True
        if not self._should_upgrade_pending_browser_interrupt(pending, event_timestamp):
            return False

        self._clear_pending_browser_interrupt(call_id)
        await self._confirm_interrupt(
            call_id,
            provider,
            pending.trigger_timestamp,
            reason="browser_user_speech_timeout_during_ai_audio",
        )
        return True

    def _is_pending_interrupt_expired(
        self,
        pending: _PendingInterrupt,
        timestamp: datetime,
    ) -> bool:
        elapsed_seconds = (timestamp - pending.trigger_timestamp).total_seconds()
        return elapsed_seconds > self.pending_interrupt_max_age_seconds

    def _should_upgrade_pending_browser_interrupt(
        self,
        pending: _PendingInterrupt,
        timestamp: datetime,
    ) -> bool:
        elapsed_seconds = (timestamp - pending.trigger_timestamp).total_seconds()
        return elapsed_seconds >= self.pending_interrupt_upgrade_seconds

    def _transcript_text(self, payload: dict[str, Any]) -> str:
        for key in ("transcript", "stash", "text"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    def _is_meaningful_partial(self, transcript: str) -> bool:
        normalized = self._normalized_transcript(transcript)
        if normalized in _STRONG_SINGLE_WORD_INTERRUPTS:
            return True
        if len(_CJK_PATTERN.findall(transcript)) >= 2:
            return True
        return len(_LATIN_WORD_PATTERN.findall(transcript.lower())) >= 2

    def _is_weak_backchannel(self, transcript: str) -> bool:
        normalized = self._normalized_transcript(transcript)
        return normalized in _WEAK_BACKCHANNELS

    async def _maybe_request_manual_response(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        provider_event: ProviderEvent,
        event_timestamp: datetime,
    ) -> None:
        transcript = self._transcript_text(provider_event.payload)
        session = self.registry.get(call_id)
        if not transcript:
            return
        is_opening_response = call_id in self._opening_response_pending_call_ids
        if is_opening_response:
            self._opening_response_pending_call_ids.discard(call_id)
        if self._is_weak_backchannel(transcript) and not is_opening_response:
            if session.status in {CallSessionStatus.USER_SPEAKING, CallSessionStatus.AI_THINKING}:
                self.registry.transition(call_id, CallSessionStatus.CONNECTED)
            return
        if call_id in self._manual_response_inflight_call_ids:
            await self._queue_manual_response_after_inflight(
                call_id,
                provider,
                transcript,
                event_timestamp,
            )
            return
        if session.status not in {
            CallSessionStatus.CONNECTED,
            CallSessionStatus.USER_SPEAKING,
            CallSessionStatus.AI_THINKING,
        }:
            return

        await self._request_manual_response(
            call_id,
            provider,
            event_timestamp,
            reason="user_transcript_done",
        )

    async def _queue_manual_response_after_inflight(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        transcript: str,
        event_timestamp: datetime,
    ) -> None:
        previous = self._pending_manual_responses.get(call_id)
        self._pending_manual_responses[call_id] = _PendingManualResponse(
            transcript=transcript,
            queued_at=event_timestamp,
            cancel_requested=previous.cancel_requested
            if previous
            else call_id in self._discard_model_audio_until_response_done,
        )
        self._append_event(
            call_id,
            "manual_response_queued",
            "agent",
            {
                "reason": "response_request_inflight",
                "transcript": transcript,
            },
        )
        pending = self._pending_manual_responses[call_id]
        if pending.cancel_requested:
            return
        pending.cancel_requested = True
        if self.registry.get(call_id).status != CallSessionStatus.AI_SPEAKING:
            await self._cancel_inflight_response_for_queued_manual_response(
                call_id,
                provider,
            )
            return
        await self._confirm_interrupt(
            call_id,
            provider,
            event_timestamp,
            reason="user_transcript_done_during_inflight_response",
            next_status=CallSessionStatus.AI_THINKING,
            release_manual_response_inflight=False,
        )

    async def _cancel_inflight_response_for_queued_manual_response(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
    ) -> None:
        try:
            await provider.cancel_response()
        except Exception as exc:
            self._append_event(
                call_id,
                "manual_response_cancel_failed",
                "agent",
                {
                    "errorType": type(exc).__name__,
                    "message": str(exc),
                },
            )
            return
        self._append_event(
            call_id,
            "manual_response_cancel_requested",
            "agent",
            {"reason": "queued_user_transcript_done"},
        )

    async def _maybe_request_queued_manual_response(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        event_timestamp: datetime,
    ) -> None:
        pending = self._pending_manual_responses.pop(call_id, None)
        if pending is None:
            return
        session = self.registry.get(call_id)
        if session.status not in {
            CallSessionStatus.CONNECTED,
            CallSessionStatus.USER_SPEAKING,
            CallSessionStatus.AI_THINKING,
        }:
            return
        await self._request_manual_response(
            call_id,
            provider,
            max(event_timestamp, pending.queued_at),
            reason="queued_user_transcript_done",
        )

    async def _request_manual_response(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        event_timestamp: datetime,
        reason: str,
    ) -> None:
        session = self.registry.get(call_id)
        if session.status != CallSessionStatus.AI_THINKING:
            self.registry.transition(call_id, CallSessionStatus.AI_THINKING)
        metrics = self.metrics_by_call_id.setdefault(call_id, CallMetrics())
        metrics.mark_model_response_requested(event_timestamp)
        session.metrics = metrics.snapshot()
        self._manual_response_inflight_call_ids.add(call_id)
        try:
            await provider.create_response()
        except Exception:
            self._manual_response_inflight_call_ids.discard(call_id)
            raise
        requested_at = self._append_event(
            call_id,
            "manual_response_requested",
            "agent",
            {"reason": reason},
        )
        metrics.mark_model_response_requested(requested_at)
        self.registry.get(call_id).metrics = metrics.snapshot()

    def _normalized_transcript(self, transcript: str) -> str:
        return "".join(
            char.lower()
            for char in transcript
            if char.isalnum() or _CJK_PATTERN.match(char)
        )

    async def _confirm_interrupt(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        trigger_timestamp: datetime,
        reason: str = "user_speech_started_during_ai_audio",
        next_status: CallSessionStatus = CallSessionStatus.USER_SPEAKING,
        release_manual_response_inflight: bool = False,
    ) -> None:
        await self._cancel_playout_task(call_id)

        metrics = self.metrics_by_call_id.setdefault(call_id, CallMetrics())
        metrics.mark_interrupt_confirmed(trigger_timestamp)
        session = self.registry.get(call_id)
        if session.status != CallSessionStatus.AI_SPEAKING:
            if session.status not in {CallSessionStatus.CONNECTED, CallSessionStatus.AI_THINKING}:
                return
            self.registry.transition(call_id, CallSessionStatus.AI_SPEAKING)
        self.registry.transition(call_id, CallSessionStatus.INTERRUPTED)
        self._discard_model_audio_until_response_started.add(call_id)
        if release_manual_response_inflight:
            self._manual_response_inflight_call_ids.discard(call_id)

        cleanup_errors: list[dict[str, str]] = []
        if self.audio_publisher is not None:
            try:
                await self.audio_publisher.stop_audio(call_id)
            except Exception as exc:
                cleanup_errors.append(
                    {
                        "step": "stop_audio",
                        "errorType": type(exc).__name__,
                        "message": str(exc),
                    }
                )
        try:
            await provider.cancel_response()
        except Exception as exc:
            cleanup_errors.append(
                {
                    "step": "cancel_response",
                    "errorType": type(exc).__name__,
                    "message": str(exc),
                }
            )
        try:
            await provider.clear_input_audio()
        except Exception as exc:
            cleanup_errors.append(
                {
                    "step": "clear_input_audio",
                    "errorType": type(exc).__name__,
                    "message": str(exc),
                }
            )

        event_timestamp = self._append_event(
            call_id,
            "interrupt_confirmed",
            "agent",
            {"reason": reason},
        )
        for cleanup_error in cleanup_errors:
            self._append_event(
                call_id,
                "interrupt_cleanup_failed",
                "agent",
                cleanup_error,
            )
        metrics.mark_ai_audio_stopped(event_timestamp)
        if self.registry.get(call_id).status == CallSessionStatus.INTERRUPTED:
            self.registry.transition(call_id, next_status)
        self.registry.get(call_id).metrics = metrics.snapshot()

    def _complete_ai_speaking_after_playout(self, call_id: str) -> None:
        wait_for_playout = getattr(self.audio_publisher, "wait_for_playout", None)
        if wait_for_playout is None:
            self.registry.transition(call_id, CallSessionStatus.CONNECTED)
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

    def _session_config(self, session: CallSession) -> QwenRealtimeSessionConfig:
        instructions = str(self._config_value(session.effective_config, "prompt", ""))
        if self._config_value(session.effective_config, "opening_enabled", False):
            opening_message = str(
                self._config_value(session.effective_config, "opening_message", "")
            )
            if opening_message:
                instructions = (
                    f"{instructions}\n\n"
                    f"通话开始后，系统会触发你主动开场。请先自然说出这句开场白：{opening_message}"
                )
        return QwenRealtimeSessionConfig(
            voice=str(self._config_value(session.effective_config, "voice", "Tina")),
            instructions=instructions,
            vad_type=str(self._config_value(session.effective_config, "vad_type", "server_vad")),
            vad_threshold=float(self._config_value(session.effective_config, "vad_threshold", 0.5)),
            vad_silence_duration_ms=int(
                self._config_value(session.effective_config, "vad_silence_duration_ms", 800)
            ),
            input_audio_transcription_model=str(
                self._config_value(
                    session.effective_config,
                    "input_transcription_model",
                    "qwen3-asr-flash-realtime",
                )
            ),
            input_audio_transcription_language=str(
                self._config_value(
                    session.effective_config,
                    "input_transcription_language",
                    "zh",
                )
            ),
        )

    @staticmethod
    def _config_value(config: Any, key: str, default: Any) -> Any:
        if isinstance(config, dict):
            return config.get(key, default)
        return getattr(config, key, default)
