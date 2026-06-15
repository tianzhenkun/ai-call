from __future__ import annotations

import hashlib
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from fastapi import status

from app.config.setting import Settings
from app.services.ai_call.agent_runner import RealtimeCallAgentRunner
from app.services.ai_call.event_store import AiCallEvent, InMemoryEventStore
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.livekit_audio_transport import LiveKitRoomAudioTransport
from app.services.ai_call.livekit_room import BrowserRoomToken, LiveKitRoomManager
from app.services.ai_call.metrics import CallMetrics
from app.services.ai_call.providers.aliyun_qwen_realtime import AliyunQwenRealtimeProvider
from app.services.ai_call.session_registry import (
    RUNNING_STATUSES,
    CallSession,
    CallSessionStatus,
    InMemorySessionRegistry,
    utc_now,
)
from app.utils.id_util import generate_snowflake_id


class LiveKitRoomManagerProtocol(Protocol):
    async def create_room(self, room_name: str) -> None: ...

    def issue_browser_token(self, room_name: str, participant_identity: str) -> BrowserRoomToken:
        ...

    async def delete_room(self, room_name: str) -> None: ...


class RealtimeAgentRunnerProtocol(Protocol):
    async def start(self, session: CallSession) -> None: ...

    async def start_opening(self, call_id: str) -> None: ...

    async def confirm_browser_interrupt(
        self,
        call_id: str,
        trigger_timestamp: datetime,
    ) -> bool: ...

    async def stop(self, call_id: str) -> None: ...


@dataclass(frozen=True, slots=True)
class AiCallRuntimeConfig:
    livekit_url: str
    livekit_api_key: str
    livekit_api_secret: str
    browser_token_ttl_seconds: int
    dashscope_api_key: str
    dashscope_realtime_url: str
    qwen_realtime_model: str
    qwen_realtime_voice: str
    default_prompt: str
    opening_enabled: bool
    opening_message: str
    web_audio_echo_cancellation: bool
    web_audio_noise_suppression: bool
    web_audio_auto_gain_control: bool
    vad_type: str
    vad_threshold: float
    vad_silence_duration_ms: int
    qwen_realtime_input_transcription_model: str = "qwen3-asr-flash-realtime"
    qwen_realtime_input_transcription_language: str = "zh"

    @classmethod
    def from_settings(cls, settings: Settings) -> AiCallRuntimeConfig:
        return cls(
            livekit_url=settings.LIVEKIT_URL,
            livekit_api_key=settings.LIVEKIT_API_KEY,
            livekit_api_secret=settings.LIVEKIT_API_SECRET,
            browser_token_ttl_seconds=settings.LIVEKIT_BROWSER_TOKEN_TTL_SECONDS,
            dashscope_api_key=settings.DASHSCOPE_API_KEY,
            dashscope_realtime_url=settings.DASHSCOPE_REALTIME_URL,
            qwen_realtime_model=settings.QWEN_REALTIME_MODEL,
            qwen_realtime_voice=settings.QWEN_REALTIME_VOICE,
            qwen_realtime_input_transcription_model=(
                settings.QWEN_REALTIME_INPUT_TRANSCRIPTION_MODEL
            ),
            qwen_realtime_input_transcription_language=(
                settings.QWEN_REALTIME_INPUT_TRANSCRIPTION_LANGUAGE
            ),
            default_prompt=settings.AI_CALL_DEFAULT_PROMPT,
            opening_enabled=settings.AI_CALL_OPENING_ENABLED,
            opening_message=settings.AI_CALL_OPENING_MESSAGE,
            web_audio_echo_cancellation=settings.WEB_AUDIO_ECHO_CANCELLATION,
            web_audio_noise_suppression=settings.WEB_AUDIO_NOISE_SUPPRESSION,
            web_audio_auto_gain_control=settings.WEB_AUDIO_AUTO_GAIN_CONTROL,
            vad_type=settings.QWEN_REALTIME_TURN_DETECTION_TYPE,
            vad_threshold=settings.QWEN_REALTIME_VAD_THRESHOLD,
            vad_silence_duration_ms=settings.QWEN_REALTIME_VAD_SILENCE_DURATION_MS,
        )

    def ensure_ready(self) -> None:
        if not self.livekit_url or not self.livekit_api_key or not self.livekit_api_secret:
            raise AiCallError(
                error_id="missing_livekit_config",
                msg="LiveKit 配置缺失",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        if not self.dashscope_api_key:
            raise AiCallError(
                error_id="missing_model_config",
                msg="模型配置缺失",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )


@dataclass(frozen=True, slots=True)
class EffectiveConfig:
    model: str
    voice: str
    prompt: str = field(repr=False)
    prompt_hash: str
    opening_enabled: bool
    opening_message: str = field(repr=False)
    opening_message_hash: str
    vad_type: str
    vad_threshold: float
    vad_silence_duration_ms: int
    input_transcription_model: str = "qwen3-asr-flash-realtime"
    input_transcription_language: str = "zh"


@dataclass(frozen=True, slots=True)
class WebAudioConstraints:
    echo_cancellation: bool
    noise_suppression: bool
    auto_gain_control: bool


@dataclass(frozen=True, slots=True)
class CreateSessionResult:
    call_id: str
    room_name: str
    livekit_url: str
    participant_token: str
    participant_identity: str
    status: CallSessionStatus
    effective_config: EffectiveConfig
    web_audio_constraints: WebAudioConstraints


@dataclass(frozen=True, slots=True)
class ReissueTokenResult:
    call_id: str
    room_name: str
    livekit_url: str
    participant_token: str
    participant_identity: str
    expires_in_seconds: int


@dataclass(frozen=True, slots=True)
class SessionStatusResult:
    call_id: str
    status: CallSessionStatus
    room_name: str
    effective_config: Any
    started_at: datetime
    last_event_at: datetime
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EventListResult:
    rows: list[AiCallEvent]
    total: int


@dataclass(frozen=True, slots=True)
class EndSessionResult:
    call_id: str
    status: CallSessionStatus


@dataclass(frozen=True, slots=True)
class BrowserEventReportResult:
    event_id: str
    call_id: str
    type: str
    timestamp: datetime
    source: str
    payload: dict[str, Any]


class AiCallOrchestrator:
    def __init__(
        self,
        config: AiCallRuntimeConfig,
        livekit_room_manager: LiveKitRoomManagerProtocol | None = None,
        agent_runner: RealtimeAgentRunnerProtocol | None = None,
        registry: InMemorySessionRegistry | None = None,
        event_store: InMemoryEventStore | None = None,
        metrics_by_call_id: dict[str, CallMetrics] | None = None,
    ) -> None:
        self.config = config
        self.registry = registry or InMemorySessionRegistry()
        self.event_store = event_store or InMemoryEventStore()
        self.metrics_by_call_id = metrics_by_call_id if metrics_by_call_id is not None else {}
        self.livekit_room_manager = livekit_room_manager or LiveKitRoomManager(
            livekit_url=config.livekit_url,
            api_key=config.livekit_api_key,
            api_secret=config.livekit_api_secret,
            browser_token_ttl_seconds=config.browser_token_ttl_seconds,
        )
        self.agent_runner = agent_runner or self._build_default_agent_runner()

    @classmethod
    def from_settings(cls, settings: Settings) -> AiCallOrchestrator:
        return cls(config=AiCallRuntimeConfig.from_settings(settings))

    def _build_default_agent_runner(self) -> RealtimeAgentRunnerProtocol:
        audio_transport = LiveKitRoomAudioTransport(
            livekit_url=self.config.livekit_url,
            api_key=self.config.livekit_api_key,
            api_secret=self.config.livekit_api_secret,
        )
        return RealtimeCallAgentRunner(
            provider_factory=lambda _session: AliyunQwenRealtimeProvider(
                realtime_url=self.config.dashscope_realtime_url,
                api_key=self.config.dashscope_api_key,
                model=self.config.qwen_realtime_model,
            ),
            registry=self.registry,
            event_store=self.event_store,
            metrics_by_call_id=self.metrics_by_call_id,
            audio_transport=audio_transport,
        )

    async def create_web_session(
        self,
        voice: str | None,
        prompt: str | None,
    ) -> CreateSessionResult:
        self.config.ensure_ready()
        call_id = self._new_call_id()
        room_name = f"ai-call-{call_id}"
        participant_identity = f"browser-{call_id}"
        effective_config = self._build_effective_config(
            voice=voice,
            prompt=prompt,
        )
        session = CallSession(
            call_id=call_id,
            room_name=room_name,
            participant_identity=participant_identity,
            status=CallSessionStatus.CREATED,
            effective_config=effective_config,
        )
        self.registry.add(session)
        self.metrics_by_call_id[call_id] = CallMetrics()
        self._append_event(call_id, "session_created", "orchestrator")

        self.registry.transition(call_id, CallSessionStatus.PREPARING)
        self._append_event(call_id, "session_preparing", "orchestrator")

        await self.livekit_room_manager.create_room(room_name)
        self._append_event(call_id, "room_created", "livekit", {"roomName": room_name})

        token = self.livekit_room_manager.issue_browser_token(room_name, participant_identity)
        self._append_event(
            call_id,
            "browser_token_issued",
            "livekit",
            {
                "participantIdentity": participant_identity,
                "expiresInSeconds": token.expires_in_seconds,
            },
        )

        try:
            await self.agent_runner.start(session)
        except Exception as exc:
            await self._handle_agent_start_failed(call_id, room_name, exc)
        self._append_event(call_id, "agent_started", "agent")

        self.registry.transition(call_id, CallSessionStatus.READY)
        self._append_event(call_id, "session_ready", "orchestrator")

        return CreateSessionResult(
            call_id=call_id,
            room_name=room_name,
            livekit_url=token.livekit_url,
            participant_token=token.participant_token,
            participant_identity=token.participant_identity,
            status=CallSessionStatus.READY,
            effective_config=effective_config,
            web_audio_constraints=self._web_audio_constraints(),
        )

    async def _handle_agent_start_failed(
        self,
        call_id: str,
        room_name: str,
        exc: Exception,
    ) -> None:
        self._append_event(
            call_id,
            "agent_start_failed",
            "agent",
            {"errorType": type(exc).__name__},
        )
        if self.registry.get(call_id).status != CallSessionStatus.FAILED:
            self.registry.transition(call_id, CallSessionStatus.FAILED)
        self._append_event(
            call_id,
            "session_failed",
            "orchestrator",
            {"reason": "agent_start_failed"},
        )
        with suppress(Exception):
            await self.agent_runner.stop(call_id)
        with suppress(Exception):
            await self.livekit_room_manager.delete_room(room_name)
        raise AiCallError(
            error_id="agent_start_failed",
            msg="Agent 启动失败",
            status_code=status.HTTP_502_BAD_GATEWAY,
        ) from exc

    async def reissue_browser_token(self, call_id: str) -> ReissueTokenResult:
        session = self.registry.get(call_id)
        if session.status not in RUNNING_STATUSES:
            raise AiCallError(
                error_id="invalid_session_state",
                msg="当前会话状态不允许该操作",
                status_code=status.HTTP_409_CONFLICT,
            )
        token = self.livekit_room_manager.issue_browser_token(
            session.room_name,
            session.participant_identity,
        )
        self._append_event(
            call_id,
            "browser_token_issued",
            "livekit",
            {
                "participantIdentity": session.participant_identity,
                "expiresInSeconds": token.expires_in_seconds,
                "reason": "reissue",
            },
        )
        return ReissueTokenResult(
            call_id=call_id,
            room_name=session.room_name,
            livekit_url=token.livekit_url,
            participant_token=token.participant_token,
            participant_identity=token.participant_identity,
            expires_in_seconds=token.expires_in_seconds,
        )

    async def get_session(self, call_id: str) -> SessionStatusResult:
        session = self.registry.get(call_id)
        return SessionStatusResult(
            call_id=session.call_id,
            status=session.status,
            room_name=session.room_name,
            effective_config=session.effective_config,
            started_at=session.started_at,
            last_event_at=session.last_event_at,
            metrics=session.metrics,
        )

    async def list_events(
        self,
        call_id: str,
        limit: int = 200,
        after_event_id: str | None = None,
    ) -> EventListResult:
        self.registry.get(call_id)
        rows = self.event_store.list(
            call_id=call_id,
            limit=limit,
            after_event_id=after_event_id,
        )
        return EventListResult(rows=rows, total=len(rows))

    async def end_session(self, call_id: str) -> EndSessionResult:
        session = self.registry.get(call_id)
        if session.status == CallSessionStatus.COMPLETED:
            return EndSessionResult(call_id=call_id, status=CallSessionStatus.COMPLETED)
        if session.status == CallSessionStatus.FAILED:
            await self.agent_runner.stop(call_id)
            await self.livekit_room_manager.delete_room(session.room_name)
            return EndSessionResult(call_id=call_id, status=CallSessionStatus.FAILED)

        self.registry.transition(call_id, CallSessionStatus.ENDING)
        self._append_event(call_id, "session_ending", "orchestrator")

        await self.agent_runner.stop(call_id)
        await self.livekit_room_manager.delete_room(session.room_name)

        self.registry.transition(call_id, CallSessionStatus.COMPLETED)
        self._append_event(call_id, "session_completed", "orchestrator")
        return EndSessionResult(call_id=call_id, status=CallSessionStatus.COMPLETED)

    async def report_browser_event(
        self,
        call_id: str,
        event_type: str,
        timestamp: datetime | None = None,
        payload: dict[str, Any] | None = None,
    ) -> BrowserEventReportResult:
        session = self.registry.get(call_id)
        if session.status not in RUNNING_STATUSES:
            raise AiCallError(
                error_id="invalid_session_state",
                msg="当前会话状态不允许该操作",
                status_code=status.HTTP_409_CONFLICT,
            )
        reported_at = timestamp or utc_now()
        should_start_opening = False
        should_confirm_browser_interrupt = False
        if event_type == "browser_first_audio":
            metrics = self.metrics_by_call_id.setdefault(call_id, CallMetrics())
            metrics.mark_browser_first_audio(reported_at)
            session.metrics = metrics.snapshot()
        elif event_type == "browser_ready":
            if session.status == CallSessionStatus.READY:
                self.registry.transition(call_id, CallSessionStatus.CONNECTED)
                should_start_opening = bool(
                    self._config_value(session.effective_config, "opening_enabled", False)
                )
        elif event_type == "browser_user_speech_started":
            should_confirm_browser_interrupt = True
        elif event_type == "browser_remote_audio_track_state":
            pass
        else:
            raise AiCallError(
                error_id="unsupported_browser_event",
                msg="不支持的浏览器事件",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        event_payload = {
            "reportedAt": reported_at.isoformat(),
            **self._sanitize_browser_event_payload(payload or {}),
        }
        event = self.event_store.append(
            call_id=call_id,
            type=event_type,
            source="browser",
            payload=event_payload,
        )
        session.last_event_at = event.timestamp
        if should_confirm_browser_interrupt:
            await self.agent_runner.confirm_browser_interrupt(call_id, reported_at)
        if should_start_opening:
            opening_started_at = self._append_event(
                call_id,
                "opening_started",
                "agent",
                {
                    "openingMessageHash": self._config_value(
                        session.effective_config,
                        "opening_message_hash",
                        "",
                    )
                },
            )
            metrics = self.metrics_by_call_id.setdefault(call_id, CallMetrics())
            metrics.mark_model_response_requested(opening_started_at)
            session.metrics = metrics.snapshot()
            await self.agent_runner.start_opening(call_id)
        return BrowserEventReportResult(
            event_id=event.event_id,
            call_id=event.call_id,
            type=event.type,
            timestamp=event.timestamp,
            source=event.source,
            payload=event.payload,
        )

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
        )
        session = self.registry.get(call_id)
        session.last_event_at = event.timestamp
        return event.timestamp

    def _build_effective_config(self, voice: str | None, prompt: str | None) -> EffectiveConfig:
        resolved_voice = voice or self.config.qwen_realtime_voice
        resolved_prompt = prompt or self.config.default_prompt
        return EffectiveConfig(
            model=self.config.qwen_realtime_model,
            voice=resolved_voice,
            input_transcription_model=self.config.qwen_realtime_input_transcription_model,
            input_transcription_language=self.config.qwen_realtime_input_transcription_language,
            prompt=resolved_prompt,
            prompt_hash=self._hash_text(resolved_prompt),
            opening_enabled=self.config.opening_enabled,
            opening_message=self.config.opening_message,
            opening_message_hash=self._hash_text(self.config.opening_message),
            vad_type=self.config.vad_type,
            vad_threshold=self.config.vad_threshold,
            vad_silence_duration_ms=self.config.vad_silence_duration_ms,
        )

    def _web_audio_constraints(self) -> WebAudioConstraints:
        return WebAudioConstraints(
            echo_cancellation=self.config.web_audio_echo_cancellation,
            noise_suppression=self.config.web_audio_noise_suppression,
            auto_gain_control=self.config.web_audio_auto_gain_control,
        )

    @staticmethod
    def _hash_text(text: str) -> str:
        return f"sha256:{hashlib.sha256(text.encode('utf-8')).hexdigest()}"

    @staticmethod
    def _config_value(config: Any, key: str, default: Any) -> Any:
        if isinstance(config, dict):
            return config.get(key, default)
        return getattr(config, key, default)

    @staticmethod
    def _sanitize_browser_event_payload(payload: dict[str, Any]) -> dict[str, Any]:
        sanitized: dict[str, Any] = {}
        for key, value in payload.items():
            if not isinstance(key, str) or key == "reportedAt":
                continue
            if len(sanitized) >= 20:
                break
            if isinstance(value, str):
                sanitized[key[:80]] = value[:200]
            elif isinstance(value, int | float | bool) or value is None:
                sanitized[key[:80]] = value
        return sanitized

    @staticmethod
    def _new_call_id() -> str:
        return f"call_{generate_snowflake_id()}"
