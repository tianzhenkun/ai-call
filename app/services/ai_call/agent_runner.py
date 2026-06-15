from __future__ import annotations

import asyncio
import base64
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
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
        self._providers: dict[str, RealtimeProviderProtocol] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._audio_tasks: dict[str, asyncio.Task[None]] = {}
        self._playout_tasks: dict[str, asyncio.Task[None]] = {}
        self._last_ai_audio_published_at: dict[str, datetime] = {}
        self._pending_user_turns: dict[str, PendingUserTurn] = {}

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
        self._pending_user_turns.pop(call_id, None)

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
            else:
                self._apply_provider_event(call_id, provider_event.type, event_timestamp)
            if provider_event.type == "model_audio_delta":
                await self._publish_model_audio_delta(call_id, provider_event)

    async def record_browser_speech_candidate(
        self,
        call_id: str,
        trigger_timestamp: datetime,
    ) -> bool:
        session = self.registry.get(call_id)
        if session.status != CallSessionStatus.AI_SPEAKING and not self._has_recent_ai_audio(
            call_id,
            trigger_timestamp,
        ):
            return False
        turn = self._pending_turn(call_id)
        self._mark_interrupt_candidate(
            call_id=call_id,
            turn=turn,
            trigger_timestamp=trigger_timestamp,
            source="browser",
            reason="browser_user_speech_started_during_ai_audio",
        )
        return False

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
            event_type == "model_response_done"
            and session.status == CallSessionStatus.AI_SPEAKING
        ):
            self._complete_ai_speaking_after_playout(call_id)
        elif event_type == "model_error":
            self.registry.transition(call_id, CallSessionStatus.FAILED)

        self.registry.get(call_id).metrics = metrics.snapshot()

    async def _handle_user_speech_started(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        timestamp: datetime,
    ) -> None:
        session = self.registry.get(call_id)
        turn = self._pending_turn(call_id, reset_if_finished=True)
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
        await self._maybe_request_response_from_turn(call_id, provider, timestamp)
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
            and (
                "text" in provider_event.payload
                or "stash" in provider_event.payload
            )
        ):
            turn.transcript_parts = [text]
        else:
            turn.transcript_parts.append(text)
        await self._maybe_confirm_interrupt_from_turn(call_id, provider, timestamp)
        await self._maybe_request_response_from_turn(call_id, provider, timestamp)

    def _pending_turn(self, call_id: str, reset_if_finished: bool = False) -> PendingUserTurn:
        turn = self._pending_user_turns.get(call_id)
        if turn is None or (
            reset_if_finished
            and (
                turn.response_requested
                or (turn.stopped_at is not None and not turn.transcript)
            )
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

    async def _maybe_request_response_from_turn(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        timestamp: datetime,
    ) -> None:
        turn = self._pending_turn(call_id)
        if turn.response_requested or turn.stopped_at is None or not turn.transcript:
            return
        await self._maybe_confirm_interrupt_from_turn(call_id, provider, timestamp)

        metrics = self.metrics_by_call_id.setdefault(call_id, CallMetrics())
        metrics.mark_user_speech_stopped(turn.stopped_at)
        session = self.registry.get(call_id)
        if session.status in {
            CallSessionStatus.USER_SPEAKING,
            CallSessionStatus.CONNECTED,
            CallSessionStatus.INTERRUPTED,
        }:
            self.registry.transition(call_id, CallSessionStatus.AI_THINKING)
        await provider.create_response()
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
        if clear_input_audio:
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
            self.registry.transition(call_id, CallSessionStatus.USER_SPEAKING)
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
        )

    @staticmethod
    def _config_value(config: Any, key: str, default: Any) -> Any:
        if isinstance(config, dict):
            return config.get(key, default)
        return getattr(config, key, default)
