from __future__ import annotations

import asyncio
import hashlib
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from fastapi import status

from app.config.setting import Settings
from app.core.logger import log
from app.services.ai_call.agent_runner import (
    NO_BARGE_USER_TURN_STABILITY_DELAY_SECONDS,
    RealtimeCallAgentRunner,
)
from app.services.ai_call.event_store import AiCallEvent, InMemoryEventStore
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.livekit_audio_transport import LiveKitRoomAudioTransport
from app.services.ai_call.livekit_room import BrowserRoomToken, LiveKitRoomManager
from app.services.ai_call.metrics import CallMetrics
from app.services.ai_call.prompt_config import PromptEffectiveConfig
from app.services.ai_call.providers.aliyun_qwen_realtime import AliyunQwenRealtimeProvider
from app.services.ai_call.session_registry import (
    ALLOWED_TRANSITIONS,
    RUNNING_STATUSES,
    CallSession,
    CallSessionStatus,
    InMemorySessionRegistry,
    utc_now,
)
from app.services.ai_call.sip_barge_in import SipBargeInConfig, WebRtcVadAdapter
from app.services.ai_call.sip_vad_shadow import (
    FsmnVadSidecarClient,
    MultiSipVadShadowDetector,
    QueuedSipVadShadowDetector,
    SipFrameVadShadowDetector,
    SipVadShadowDetectorProtocol,
    UnavailableSipVadShadowDetector,
)
from app.utils.id_util import generate_snowflake_id

END_CLEANUP_TIMEOUT_SECONDS = 1.0
BROWSER_READY_TIMEOUT_SECONDS = 90.0
TERMINAL_BROWSER_EVENT_TYPES = frozenset(
    {
        "browser_disconnect",
        "browser_first_audio",
        "browser_audio_input_diagnostics",
        "browser_user_speech_started",
        "browser_user_speech_segment",
    }
)


class LiveKitRoomManagerProtocol(Protocol):
    async def create_room(self, room_name: str) -> None: ...

    def issue_browser_token(
        self, room_name: str, participant_identity: str
    ) -> BrowserRoomToken: ...

    def issue_handoff_token(
        self,
        room_name: str,
        participant_identity: str,
        expires_in_seconds: int | None = None,
    ) -> BrowserRoomToken: ...

    async def delete_room(self, room_name: str) -> None: ...


class RealtimeAgentRunnerProtocol(Protocol):
    async def start(self, session: CallSession) -> None: ...

    async def start_opening(self, call_id: str) -> None: ...

    async def record_browser_speech_candidate(
        self,
        call_id: str,
        trigger_timestamp: datetime,
    ) -> bool: ...

    async def record_browser_speech_segment(
        self,
        call_id: str,
        trigger_timestamp: datetime,
        payload: dict[str, Any],
    ) -> bool: ...

    async def suspend_for_handoff(self, call_id: str) -> None: ...

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
    opening_message: str
    web_audio_echo_cancellation: bool
    web_audio_noise_suppression: bool
    web_audio_auto_gain_control: bool
    vad_type: str
    vad_threshold: float
    vad_silence_duration_ms: int
    user_turn_stability_delay_seconds: float = 0.35
    no_barge_user_turn_stability_delay_seconds: float = (
        NO_BARGE_USER_TURN_STABILITY_DELAY_SECONDS
    )
    handoff_prompt_constraint_enabled: bool = True
    barge_in_enabled: bool = True
    sip_barge_in_enabled: bool = True
    sip_barge_in_min_rms_dbfs: float = -35.0
    sip_barge_in_min_speech_duration_ms: int = 220
    sip_barge_in_hold_timeout_seconds: float = 5.0
    sip_barge_in_fast_stop_enabled: bool = False
    sip_barge_in_config: SipBargeInConfig = field(default_factory=SipBargeInConfig)
    sip_barge_in_recovery_silence_ms: int = 600
    sip_barge_in_recovery_max_per_turn: int = 1
    sip_vad_shadow_enabled: bool = False
    sip_vad_shadow_detector: str = "webrtc"
    sip_vad_shadow_fsmn_model: str = "iic/speech_fsmn_vad_zh-cn-16k-common-pytorch"
    sip_vad_shadow_fsmn_endpoint: str = ""
    sip_vad_shadow_fsmn_timeout_seconds: float = 0.2
    sip_vad_shadow_queue_size: int = 50

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
            default_prompt=settings.AI_CALL_DEFAULT_PROMPT,
            opening_message=settings.AI_CALL_OPENING_MESSAGE,
            web_audio_echo_cancellation=settings.WEB_AUDIO_ECHO_CANCELLATION,
            web_audio_noise_suppression=settings.WEB_AUDIO_NOISE_SUPPRESSION,
            web_audio_auto_gain_control=settings.WEB_AUDIO_AUTO_GAIN_CONTROL,
            vad_type=settings.QWEN_REALTIME_TURN_DETECTION_TYPE,
            vad_threshold=settings.QWEN_REALTIME_VAD_THRESHOLD,
            vad_silence_duration_ms=settings.QWEN_REALTIME_VAD_SILENCE_DURATION_MS,
            user_turn_stability_delay_seconds=(settings.AI_CALL_USER_TURN_STABILITY_DELAY_SECONDS),
            handoff_prompt_constraint_enabled=(settings.AI_CALL_HANDOFF_PROMPT_CONSTRAINT_ENABLED),
            barge_in_enabled=settings.AI_CALL_BARGE_IN_ENABLED,
            sip_barge_in_enabled=settings.AI_CALL_SIP_BARGE_IN_ENABLED,
            sip_barge_in_min_rms_dbfs=settings.AI_CALL_SIP_BARGE_IN_MIN_RMS_DBFS,
            sip_barge_in_min_speech_duration_ms=(
                settings.AI_CALL_SIP_BARGE_IN_MIN_SPEECH_DURATION_MS
            ),
            sip_barge_in_hold_timeout_seconds=(
                settings.AI_CALL_SIP_BARGE_IN_HOLD_TIMEOUT_SECONDS
            ),
            sip_barge_in_fast_stop_enabled=settings.AI_CALL_SIP_BARGE_IN_FAST_STOP_ENABLED,
            sip_barge_in_config=SipBargeInConfig(
                rms_threshold_dbfs=settings.AI_CALL_SIP_BARGE_IN_RMS_THRESHOLD_DBFS,
                snr_threshold_db=settings.AI_CALL_SIP_BARGE_IN_SNR_THRESHOLD_DB,
                vad_voiced_duration_ms=settings.AI_CALL_SIP_BARGE_IN_VAD_VOICED_DURATION_MS,
                candidate_min_duration_ms=(
                    settings.AI_CALL_SIP_BARGE_IN_CANDIDATE_MIN_DURATION_MS
                ),
                pre_stop_min_duration_ms=(
                    settings.AI_CALL_SIP_BARGE_IN_PRE_STOP_MIN_DURATION_MS
                ),
                short_speech_min_duration_ms=(
                    settings.AI_CALL_SIP_BARGE_IN_SHORT_SPEECH_MIN_DURATION_MS
                ),
                impulse_noise_max_duration_ms=(
                    settings.AI_CALL_SIP_BARGE_IN_IMPULSE_NOISE_MAX_DURATION_MS
                ),
                clean_window_ms=settings.AI_CALL_SIP_BARGE_IN_CLEAN_WINDOW_MS,
                max_hold_ms=settings.AI_CALL_SIP_BARGE_IN_MAX_HOLD_MS,
                echo_tail_window_ms=settings.AI_CALL_SIP_BARGE_IN_ECHO_TAIL_WINDOW_MS,
            ),
            sip_barge_in_recovery_silence_ms=(
                settings.AI_CALL_SIP_BARGE_IN_RECOVERY_SILENCE_MS
            ),
            sip_barge_in_recovery_max_per_turn=(
                settings.AI_CALL_SIP_BARGE_IN_RECOVERY_MAX_PER_TURN
            ),
            sip_vad_shadow_enabled=settings.AI_CALL_SIP_VAD_SHADOW_ENABLED,
            sip_vad_shadow_detector=settings.AI_CALL_SIP_VAD_SHADOW_DETECTOR,
            sip_vad_shadow_fsmn_model=settings.AI_CALL_SIP_VAD_SHADOW_FSMN_MODEL,
            sip_vad_shadow_fsmn_endpoint=settings.AI_CALL_SIP_VAD_SHADOW_FSMN_ENDPOINT,
            sip_vad_shadow_fsmn_timeout_seconds=(
                settings.AI_CALL_SIP_VAD_SHADOW_FSMN_TIMEOUT_SECONDS
            ),
            sip_vad_shadow_queue_size=settings.AI_CALL_SIP_VAD_SHADOW_QUEUE_SIZE,
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
    opening_message: str = field(repr=False)
    opening_message_hash: str
    prompt_source_key: str
    vad_type: str
    vad_threshold: float
    vad_silence_duration_ms: int
    barge_in_enabled: bool = False
    instructions: str | None = field(default=None, repr=False)


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
class CreateSipRoomSessionResult:
    call_id: str
    room_name: str
    participant_identity: str
    status: CallSessionStatus
    effective_config: EffectiveConfig


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


@dataclass(frozen=True, slots=True)
class HandoffTokenResult:
    call_id: str
    handoff_id: str
    room_name: str
    livekit_url: str
    participant_token: str
    participant_identity: str
    expires_in_seconds: int


class AiCallOrchestrator:
    def __init__(
        self,
        config: AiCallRuntimeConfig,
        livekit_room_manager: LiveKitRoomManagerProtocol | None = None,
        agent_runner: RealtimeAgentRunnerProtocol | None = None,
        registry: InMemorySessionRegistry | None = None,
        event_store: InMemoryEventStore | None = None,
        metrics_by_call_id: dict[str, CallMetrics] | None = None,
        end_cleanup_timeout_seconds: float = END_CLEANUP_TIMEOUT_SECONDS,
        browser_ready_timeout_seconds: float = BROWSER_READY_TIMEOUT_SECONDS,
    ) -> None:
        self.config = config
        self.registry = registry or InMemorySessionRegistry()
        self.event_store = event_store or InMemoryEventStore()
        self.metrics_by_call_id = metrics_by_call_id if metrics_by_call_id is not None else {}
        self.end_cleanup_timeout_seconds = max(0.001, end_cleanup_timeout_seconds)
        self.browser_ready_timeout_seconds = max(
            0.001,
            browser_ready_timeout_seconds,
        )
        self.livekit_room_manager = livekit_room_manager or LiveKitRoomManager(
            livekit_url=config.livekit_url,
            api_key=config.livekit_api_key,
            api_secret=config.livekit_api_secret,
            browser_token_ttl_seconds=config.browser_token_ttl_seconds,
        )
        self._auto_end_tasks: set[asyncio.Task[None]] = set()
        self._browser_ready_timeout_handles: dict[str, asyncio.TimerHandle] = {}
        self._browser_ready_timeout_tasks: set[asyncio.Task[None]] = set()
        self._room_create_attempts: set[str] = set()
        self._agent_start_attempts: set[str] = set()
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
            user_turn_stability_delay_seconds=(self.config.user_turn_stability_delay_seconds),
            no_barge_user_turn_stability_delay_seconds=(
                self.config.no_barge_user_turn_stability_delay_seconds
            ),
            handoff_prompt_constraint_enabled=(self.config.handoff_prompt_constraint_enabled),
            sip_barge_in_enabled=self.config.sip_barge_in_enabled,
            sip_barge_in_min_rms_dbfs=self.config.sip_barge_in_min_rms_dbfs,
            sip_barge_in_min_speech_duration_ms=self.config.sip_barge_in_min_speech_duration_ms,
            sip_barge_in_hold_timeout_seconds=self.config.sip_barge_in_hold_timeout_seconds,
            sip_barge_in_fast_stop_enabled=self.config.sip_barge_in_fast_stop_enabled,
            sip_barge_in_config=self.config.sip_barge_in_config,
            sip_barge_in_recovery_silence_ms=self.config.sip_barge_in_recovery_silence_ms,
            sip_barge_in_recovery_max_per_turn=self.config.sip_barge_in_recovery_max_per_turn,
            sip_vad_shadow_enabled=self.config.sip_vad_shadow_enabled,
            sip_vad_shadow_detector=self._build_sip_vad_shadow_detector(),
            call_end_scheduler=self._schedule_auto_end_session,
        )

    def _build_sip_vad_shadow_detector(self) -> SipVadShadowDetectorProtocol | None:
        if not self.config.sip_vad_shadow_enabled:
            return None
        detector = self.config.sip_vad_shadow_detector.strip().lower()
        if detector == "webrtc":
            return SipFrameVadShadowDetector(
                vad=WebRtcVadAdapter(),
                detector_name="webrtc_shadow",
            )
        if detector == "fsmn":
            return self._build_fsmn_sip_vad_shadow_detector()
        if detector == "webrtc+fsmn":
            return MultiSipVadShadowDetector([
                SipFrameVadShadowDetector(
                    vad=WebRtcVadAdapter(),
                    detector_name="webrtc_shadow",
                ),
                self._build_fsmn_sip_vad_shadow_detector(),
            ])
        return UnavailableSipVadShadowDetector(
            detector_name=f"{detector}_shadow",
            reason=f"Unsupported SIP VAD shadow detector: {detector}",
        )

    def _build_fsmn_sip_vad_shadow_detector(self) -> SipVadShadowDetectorProtocol:
        endpoint = self.config.sip_vad_shadow_fsmn_endpoint.strip()
        if endpoint:
            return QueuedSipVadShadowDetector(
                client=FsmnVadSidecarClient(
                    endpoint=endpoint,
                    model=self.config.sip_vad_shadow_fsmn_model,
                    timeout_seconds=self.config.sip_vad_shadow_fsmn_timeout_seconds,
                ),
                detector_name="fsmn_shadow",
                max_queue_size=self.config.sip_vad_shadow_queue_size,
            )
        return UnavailableSipVadShadowDetector(
            detector_name="fsmn_shadow",
            reason=(
                "FSMN realtime SIP VAD shadow detector is not wired yet; "
                f"model={self.config.sip_vad_shadow_fsmn_model}"
            ),
        )

    async def create_web_session(
        self,
        voice: str | None,
        prompt: str | None,
        call_id: str | None = None,
        prompt_effective_config: PromptEffectiveConfig | None = None,
    ) -> CreateSessionResult:
        self.config.ensure_ready()
        call_id = call_id or self._new_call_id()
        room_name = f"ai-call-{call_id}"
        participant_identity = f"browser-{call_id}"
        effective_config = self._build_effective_config(
            voice=voice,
            prompt=prompt,
            prompt_effective_config=prompt_effective_config,
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
        self._append_event(
            call_id,
            "session_created",
            "orchestrator",
            {
                "promptHash": effective_config.prompt_hash,
                "openingMessageHash": effective_config.opening_message_hash,
                "promptSourceKey": effective_config.prompt_source_key,
            },
        )

        self.registry.transition(call_id, CallSessionStatus.PREPARING)
        self._append_event(call_id, "session_preparing", "orchestrator")
        self._room_create_attempts.add(call_id)

        try:
            await self.livekit_room_manager.create_room(room_name)
        except AiCallError as exc:
            self._append_event(
                call_id,
                "session_create_failed",
                "orchestrator",
                {
                    "errorId": "room_create_failed",
                    "failureStage": "room_create",
                },
            )
            raise AiCallError(
                error_id="room_create_failed",
                msg="会话创建失败",
                status_code=exc.status_code,
            ) from None
        except Exception:
            self._append_event(
                call_id,
                "session_create_failed",
                "orchestrator",
                {
                    "errorId": "room_create_failed",
                    "failureStage": "room_create",
                },
            )
            raise AiCallError(
                error_id="room_create_failed",
                msg="LiveKit Room 创建失败",
                status_code=status.HTTP_502_BAD_GATEWAY,
            ) from None
        self._append_event(call_id, "room_created", "livekit", {"roomName": room_name})

        try:
            token = self.livekit_room_manager.issue_browser_token(
                room_name,
                participant_identity,
            )
        except Exception:
            self._append_event(
                call_id,
                "session_create_failed",
                "orchestrator",
                {
                    "errorId": "browser_token_issue_failed",
                    "failureStage": "browser_token",
                },
            )
            raise AiCallError(
                error_id="browser_token_issue_failed",
                msg="浏览器连接凭证签发失败",
                status_code=status.HTTP_502_BAD_GATEWAY,
            ) from None
        self._append_event(
            call_id,
            "browser_token_issued",
            "livekit",
            {
                "participantIdentity": participant_identity,
                "expiresInSeconds": token.expires_in_seconds,
            },
        )

        self._agent_start_attempts.add(call_id)
        try:
            await self.agent_runner.start(session)
        except Exception as exc:
            await self._handle_agent_start_failed(call_id, room_name, exc)
        self._append_event(
            call_id,
            "agent_started",
            "agent",
            self._agent_runner_runtime_diagnostics(),
        )

        self.registry.transition(call_id, CallSessionStatus.READY)
        self._append_event(call_id, "session_ready", "orchestrator")
        self._schedule_browser_ready_watchdog(call_id)

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

    async def create_sip_session(
        self,
        voice: str | None,
        prompt: str | None,
        call_id: str | None = None,
        prompt_effective_config: PromptEffectiveConfig | None = None,
    ) -> CreateSipRoomSessionResult:
        self.config.ensure_ready()
        call_id = call_id or self._new_call_id()
        room_name = f"ai-call-{call_id}"
        participant_identity = f"sip-{call_id}"
        effective_config = self._build_effective_config(
            voice=voice,
            prompt=prompt,
            prompt_effective_config=prompt_effective_config,
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
        self._append_event(
            call_id,
            "session_created",
            "orchestrator",
            {
                "entryType": "sip_outbound",
                "promptHash": effective_config.prompt_hash,
                "openingMessageHash": effective_config.opening_message_hash,
                "promptSourceKey": effective_config.prompt_source_key,
            },
        )

        self.registry.transition(call_id, CallSessionStatus.PREPARING)
        self._append_event(call_id, "session_preparing", "orchestrator")
        self._room_create_attempts.add(call_id)

        try:
            await self.livekit_room_manager.create_room(room_name)
        except AiCallError as exc:
            self._append_event(
                call_id,
                "session_create_failed",
                "orchestrator",
                {
                    "errorId": "room_create_failed",
                    "failureStage": "room_create",
                },
            )
            raise AiCallError(
                error_id="room_create_failed",
                msg="会话创建失败",
                status_code=exc.status_code,
            ) from None
        except Exception:
            self._append_event(
                call_id,
                "session_create_failed",
                "orchestrator",
                {
                    "errorId": "room_create_failed",
                    "failureStage": "room_create",
                },
            )
            raise AiCallError(
                error_id="room_create_failed",
                msg="LiveKit Room 创建失败",
                status_code=status.HTTP_502_BAD_GATEWAY,
            ) from None
        self._append_event(call_id, "room_created", "livekit", {"roomName": room_name})

        self._agent_start_attempts.add(call_id)
        try:
            await self.agent_runner.start(session)
        except Exception as exc:
            await self._handle_agent_start_failed(call_id, room_name, exc)
        self._append_event(
            call_id,
            "agent_started",
            "agent",
            self._agent_runner_runtime_diagnostics(),
        )

        self.registry.transition(call_id, CallSessionStatus.READY)
        self._append_event(call_id, "session_ready", "orchestrator")

        return CreateSipRoomSessionResult(
            call_id=call_id,
            room_name=room_name,
            participant_identity=participant_identity,
            status=CallSessionStatus.READY,
            effective_config=effective_config,
        )

    async def _handle_agent_start_failed(
        self,
        call_id: str,
        room_name: str,
        exc: Exception,
    ) -> None:
        error_payload: dict[str, Any] = {
            "errorType": type(exc).__name__,
        }
        if exc.__cause__ is not None:
            error_payload["causeType"] = type(exc.__cause__).__name__
        log.error(
            "AI Call Agent 启动失败 call_id={} room_name={} error_type={}",
            call_id,
            room_name,
            error_payload["errorType"],
        )
        self._append_event(
            call_id,
            "agent_start_failed",
            "agent",
            error_payload,
        )
        try:
            await self._run_end_cleanup_step(
                call_id,
                step="agent_stop",
                awaitable=self.agent_runner.stop(call_id),
                record_success=False,
            )
            await self._run_end_cleanup_step(
                call_id,
                step="delete_room",
                awaitable=self.livekit_room_manager.delete_room(room_name),
                record_success=False,
            )
        except AiCallError:
            raise AiCallError(
                error_id="agent_start_failed",
                msg="Agent 启动失败",
                status_code=status.HTTP_502_BAD_GATEWAY,
            ) from None
        self._append_failed_terminal_event(
            call_id,
            end_reason="agent_start_failed",
            failure_stage="agent_start",
            failure_message="Agent 启动失败",
        )
        raise AiCallError(
            error_id="agent_start_failed",
            msg="Agent 启动失败",
            status_code=status.HTTP_502_BAD_GATEWAY,
        ) from None

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

    async def suspend_for_handoff(
        self,
        *,
        call_id: str,
        handoff_id: str,
        request_source: str,
        request_reason: str | None,
    ) -> None:
        session = self.registry.get(call_id)
        if session.status not in RUNNING_STATUSES:
            raise AiCallError(
                error_id="invalid_session_state",
                msg="当前会话状态不允许该操作",
                status_code=status.HTTP_409_CONFLICT,
            )
        self._append_event(
            call_id,
            "handoff_requested",
            "handoff",
            {
                "handoffId": handoff_id,
                "status": "requested",
                "source": request_source,
                "reason": request_reason,
            },
        )
        await self.agent_runner.suspend_for_handoff(call_id)
        self._move_session_to_waiting_if_possible(call_id)
        self._append_event(
            call_id,
            "agent_suspended_for_handoff",
            "agent",
            {
                "handoffId": handoff_id,
                "reason": "handoff_requested",
            },
        )

    def issue_handoff_token(
        self,
        *,
        call_id: str,
        handoff_id: str,
        human_agent_identity: str,
    ) -> HandoffTokenResult:
        session = self.registry.get(call_id)
        if session.status not in RUNNING_STATUSES:
            raise AiCallError(
                error_id="invalid_session_state",
                msg="当前会话状态不允许该操作",
                status_code=status.HTTP_409_CONFLICT,
            )
        participant_identity = f"human-agent-{handoff_id}"
        token = self.livekit_room_manager.issue_handoff_token(
            session.room_name,
            participant_identity,
            self.config.browser_token_ttl_seconds,
        )
        self._append_event(
            call_id,
            "handoff_accepted",
            "handoff",
            {
                "handoffId": handoff_id,
                "status": "accepted",
                "humanAgentIdentity": human_agent_identity,
                "participantIdentity": participant_identity,
                "expiresInSeconds": token.expires_in_seconds,
            },
        )
        return HandoffTokenResult(
            call_id=call_id,
            handoff_id=handoff_id,
            room_name=session.room_name,
            livekit_url=token.livekit_url,
            participant_token=token.participant_token,
            participant_identity=token.participant_identity,
            expires_in_seconds=token.expires_in_seconds,
        )

    def record_handoff_event(
        self,
        *,
        call_id: str,
        event_type: str,
        handoff_id: str,
        handoff_status: str,
        payload: dict[str, Any] | None = None,
        source: str = "handoff",
    ) -> None:
        event_payload = {
            "handoffId": handoff_id,
            "status": handoff_status,
        }
        if payload:
            event_payload.update(payload)
        self._append_event(call_id, event_type, source, event_payload)

    def record_sip_event(
        self,
        *,
        call_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
        source: str = "sip",
    ) -> None:
        self._append_event(call_id, event_type, source, payload)

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

    async def end_session(
        self,
        call_id: str,
        *,
        end_reason: str = "web_user_end",
    ) -> EndSessionResult:
        self._cancel_browser_ready_watchdog(call_id)
        session = self.registry.get(call_id)
        if session.status == CallSessionStatus.COMPLETED:
            return EndSessionResult(call_id=call_id, status=CallSessionStatus.COMPLETED)
        if session.status == CallSessionStatus.ENDING:
            return EndSessionResult(call_id=call_id, status=CallSessionStatus.ENDING)
        if session.status == CallSessionStatus.FAILED:
            await self._run_end_cleanup_step(
                call_id,
                step="agent_stop",
                awaitable=self.agent_runner.stop(call_id),
                strict=False,
            )
            await self._run_end_cleanup_step(
                call_id,
                step="delete_room",
                awaitable=self.livekit_room_manager.delete_room(session.room_name),
                strict=False,
            )
            return EndSessionResult(call_id=call_id, status=CallSessionStatus.FAILED)

        self.registry.transition(call_id, CallSessionStatus.ENDING)
        self._append_event(call_id, "session_ending", "orchestrator")
        await self._run_end_cleanup_step(
            call_id,
            step="agent_stop",
            awaitable=self.agent_runner.stop(call_id),
            strict=False,
        )
        await self._run_end_cleanup_step(
            call_id,
            step="delete_room",
            awaitable=self.livekit_room_manager.delete_room(session.room_name),
            strict=False,
        )
        self.registry.transition(call_id, CallSessionStatus.COMPLETED)
        self._append_event(
            call_id,
            "session_completed",
            "orchestrator",
            {"endReason": end_reason},
        )
        return EndSessionResult(call_id=call_id, status=CallSessionStatus.COMPLETED)

    async def abort_session(
        self,
        call_id: str,
        *,
        end_reason: str = "session_aborted",
    ) -> EndSessionResult:
        self._cancel_browser_ready_watchdog(call_id)
        session = self.registry.get(call_id)
        if session.status == CallSessionStatus.COMPLETED:
            return EndSessionResult(call_id=call_id, status=CallSessionStatus.COMPLETED)
        events = self.event_store.list_all(call_id)
        event_types = {event.type for event in events}
        should_stop_agent = bool(
            call_id in self._agent_start_attempts or "agent_started" in event_types
        )
        should_delete_room = bool(
            call_id in self._room_create_attempts or "room_created" in event_types
        )
        completed_steps = {
            event.payload.get("step")
            for event in events
            if event.type == "session_cleanup_completed"
        }

        if session.status in RUNNING_STATUSES and session.status != CallSessionStatus.ENDING:
            self.registry.transition(call_id, CallSessionStatus.ENDING)
            self._append_event(call_id, "session_ending", "orchestrator")

        if should_stop_agent and "agent_stop" not in completed_steps:
            await self._run_end_cleanup_step(
                call_id,
                step="agent_stop",
                awaitable=self.agent_runner.stop(call_id),
            )
        if should_delete_room and "delete_room" not in completed_steps:
            await self._run_end_cleanup_step(
                call_id,
                step="delete_room",
                awaitable=self.livekit_room_manager.delete_room(session.room_name),
            )

        if session.status in {
            CallSessionStatus.CREATED,
            CallSessionStatus.PREPARING,
        }:
            self.registry.transition(call_id, CallSessionStatus.FAILED)
            self._append_event(
                call_id,
                "session_failed",
                "orchestrator",
                {
                    "endReason": end_reason,
                    "failureStage": "session_create",
                    "failureMessage": "会话创建未完成",
                },
            )
            return EndSessionResult(call_id=call_id, status=CallSessionStatus.FAILED)
        if session.status == CallSessionStatus.FAILED:
            return EndSessionResult(call_id=call_id, status=CallSessionStatus.FAILED)
        if session.status != CallSessionStatus.ENDING:
            raise AiCallError(
                error_id="invalid_session_state",
                msg="当前会话状态不允许该操作",
                status_code=status.HTTP_409_CONFLICT,
            )
        self.registry.transition(call_id, CallSessionStatus.COMPLETED)
        self._append_event(
            call_id,
            "session_completed",
            "orchestrator",
            {"endReason": end_reason},
        )
        return EndSessionResult(call_id=call_id, status=CallSessionStatus.COMPLETED)

    async def _run_end_cleanup_step(
        self,
        call_id: str,
        *,
        step: str,
        awaitable,
        strict: bool = True,
        record_success: bool = True,
    ) -> None:
        try:
            await asyncio.wait_for(awaitable, timeout=self.end_cleanup_timeout_seconds)
        except TimeoutError:
            self._append_event(
                call_id,
                "session_cleanup_timeout",
                "orchestrator",
                {
                    "step": step,
                    "timeoutSeconds": self.end_cleanup_timeout_seconds,
                },
            )
            if not strict:
                return
            raise AiCallError(
                error_id="session_cleanup_timeout",
                msg="会话资源清理超时，请稍后重试",
                status_code=status.HTTP_502_BAD_GATEWAY,
            ) from None
        except Exception as exc:
            self._append_event(
                call_id,
                "session_cleanup_failed",
                "orchestrator",
                {
                    "step": step,
                    "errorType": type(exc).__name__,
                },
            )
            if not strict:
                return
            raise AiCallError(
                error_id="session_cleanup_failed",
                msg="会话资源清理失败，请稍后重试",
                status_code=status.HTTP_502_BAD_GATEWAY,
            ) from None
        if record_success:
            self._append_event(
                call_id,
                "session_cleanup_completed",
                "orchestrator",
                {"step": step},
            )

    def dispose_session(self, call_id: str) -> None:
        self._cancel_browser_ready_watchdog(call_id)
        self.registry._sessions.pop(call_id, None)
        self._room_create_attempts.discard(call_id)
        self._agent_start_attempts.discard(call_id)
        self.event_store._events = [
            event for event in self.event_store._events if event.call_id != call_id
        ]
        self.metrics_by_call_id.pop(call_id, None)

    async def shutdown(self) -> None:
        for handle in tuple(self._browser_ready_timeout_handles.values()):
            handle.cancel()
        self._browser_ready_timeout_handles.clear()
        tasks = tuple(self._browser_ready_timeout_tasks | self._auto_end_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._browser_ready_timeout_tasks.clear()
        self._auto_end_tasks.clear()
        for call_id in tuple(self.registry._sessions):
            with suppress(Exception):
                await self.abort_session(
                    call_id,
                    end_reason="orchestrator_shutdown",
                )
            self.dispose_session(call_id)

    def _schedule_browser_ready_watchdog(self, call_id: str) -> None:
        self._cancel_browser_ready_watchdog(call_id)
        loop = asyncio.get_running_loop()
        self._browser_ready_timeout_handles[call_id] = loop.call_later(
            self.browser_ready_timeout_seconds,
            self._start_browser_ready_timeout,
            call_id,
        )

    def _cancel_browser_ready_watchdog(self, call_id: str) -> None:
        handle = self._browser_ready_timeout_handles.pop(call_id, None)
        handle and handle.cancel()

    def _start_browser_ready_timeout(self, call_id: str) -> None:
        self._browser_ready_timeout_handles.pop(call_id, None)
        task = asyncio.create_task(
            self._expire_browser_ready_session(call_id),
            name=f"ai-call-browser-ready-timeout-{call_id}",
        )
        self._browser_ready_timeout_tasks.add(task)
        task.add_done_callback(self._consume_browser_ready_timeout_task)

    def _consume_browser_ready_timeout_task(
        self,
        task: asyncio.Task[None],
    ) -> None:
        self._browser_ready_timeout_tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            prefix = "ai-call-browser-ready-timeout-"
            call_id = task.get_name()[len(prefix) :]
            with suppress(Exception):
                self._append_event(
                    call_id,
                    "browser_ready_timeout_failed",
                    "orchestrator",
                    {
                        "errorType": type(exc).__name__,
                    },
                )

    async def _expire_browser_ready_session(self, call_id: str) -> None:
        session = self.registry.get(call_id)
        if session.status != CallSessionStatus.READY:
            return
        self._append_event(
            call_id,
            "browser_ready_timeout",
            "orchestrator",
            {"timeoutSeconds": self.browser_ready_timeout_seconds},
        )
        await self.end_session(call_id, end_reason="browser_ready_timeout")

    def _schedule_auto_end_session(self, call_id: str, end_reason: str) -> None:
        task = asyncio.create_task(
            self._auto_end_session(call_id, end_reason),
            name=f"ai-call-auto-end-{call_id}",
        )
        self._auto_end_tasks.add(task)
        task.add_done_callback(self._consume_auto_end_task)

    def _consume_auto_end_task(self, task: asyncio.Task[None]) -> None:
        self._auto_end_tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            call_id = self._auto_end_task_call_id(task)
            if call_id is None:
                return
            with suppress(Exception):
                self._append_event(
                    call_id,
                    "call_end_auto_failed",
                    "orchestrator",
                    {
                        "errorType": type(exc).__name__,
                    },
                )

    async def _auto_end_session(self, call_id: str, end_reason: str) -> None:
        try:
            await self.end_session(call_id, end_reason=end_reason)
        except AiCallError as exc:
            with suppress(Exception):
                self._append_event(
                    call_id,
                    "call_end_auto_failed",
                    "orchestrator",
                    {
                        "errorId": exc.error_id,
                        "endReason": end_reason,
                    },
                )

    @staticmethod
    def _auto_end_task_call_id(task: asyncio.Task[None]) -> str | None:
        prefix = "ai-call-auto-end-"
        name = task.get_name()
        if not name.startswith(prefix):
            return None
        return name[len(prefix) :]

    async def report_browser_event(
        self,
        call_id: str,
        event_type: str,
        timestamp: datetime | None = None,
        payload: dict[str, Any] | None = None,
    ) -> BrowserEventReportResult:
        session = self.registry.get(call_id)
        reported_at = timestamp or utc_now()
        event_payload = dict(payload or {})
        event_payload["reportedAt"] = reported_at.isoformat()
        if session.status not in RUNNING_STATUSES:
            if event_type in TERMINAL_BROWSER_EVENT_TYPES and session.status in {
                CallSessionStatus.COMPLETED,
                CallSessionStatus.FAILED,
            }:
                event_payload["terminalSessionStatus"] = session.status.value
                event = self.event_store.append(
                    call_id=call_id,
                    type=event_type,
                    source="browser",
                    payload=event_payload,
                )
                session.last_event_at = event.timestamp
                return BrowserEventReportResult(
                    event_id=event.event_id,
                    call_id=event.call_id,
                    type=event.type,
                    timestamp=event.timestamp,
                    source=event.source,
                    payload=event.payload,
                )
            raise AiCallError(
                error_id="invalid_session_state",
                msg="当前会话状态不允许该操作",
                status_code=status.HTTP_409_CONFLICT,
            )
        should_start_opening = False
        should_record_browser_speech_candidate = False
        should_record_browser_speech_segment = False
        if event_type == "browser_disconnect":
            event = self.event_store.append(
                call_id=call_id,
                type=event_type,
                source="browser",
                payload=event_payload,
            )
            session.last_event_at = event.timestamp
            await self.end_session(call_id, end_reason="browser_disconnect")
            return BrowserEventReportResult(
                event_id=event.event_id,
                call_id=event.call_id,
                type=event.type,
                timestamp=event.timestamp,
                source=event.source,
                payload=event.payload,
            )
        if event_type == "browser_first_audio":
            metrics = self.metrics_by_call_id.setdefault(call_id, CallMetrics())
            metrics.mark_browser_first_audio(reported_at)
            session.metrics = metrics.snapshot()
        elif event_type == "browser_ready":
            self._cancel_browser_ready_watchdog(call_id)
            if session.status == CallSessionStatus.READY:
                self.registry.transition(call_id, CallSessionStatus.CONNECTED)
                should_start_opening = bool(
                    str(self._config_value(session.effective_config, "opening_message", "")).strip()
                )
        elif event_type == "browser_audio_input_diagnostics":
            pass
        elif event_type == "browser_user_speech_started":
            should_record_browser_speech_candidate = True
        elif event_type == "browser_user_speech_segment":
            should_record_browser_speech_segment = True
        else:
            raise AiCallError(
                error_id="unsupported_browser_event",
                msg="不支持的浏览器事件",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        event = self.event_store.append(
            call_id=call_id,
            type=event_type,
            source="browser",
            payload=event_payload,
        )
        session.last_event_at = event.timestamp
        if should_record_browser_speech_candidate:
            await self.agent_runner.record_browser_speech_candidate(call_id, event.timestamp)
        if should_record_browser_speech_segment:
            await self.agent_runner.record_browser_speech_segment(
                call_id,
                event.timestamp,
                event.payload,
            )
        if should_start_opening:
            await self._start_opening_for_session(session)
        return BrowserEventReportResult(
            event_id=event.event_id,
            call_id=event.call_id,
            type=event.type,
            timestamp=event.timestamp,
            source=event.source,
            payload=event.payload,
        )

    async def start_opening(self, call_id: str) -> None:
        session = self.registry.get(call_id)
        if session.status not in RUNNING_STATUSES:
            raise AiCallError(
                error_id="invalid_session_state",
                msg="当前会话状态不允许该操作",
                status_code=status.HTTP_409_CONFLICT,
            )
        if session.status == CallSessionStatus.READY:
            self.registry.transition(call_id, CallSessionStatus.CONNECTED)
        if not str(self._config_value(session.effective_config, "opening_message", "")).strip():
            return
        await self._start_opening_for_session(session)

    async def _start_opening_for_session(self, session: CallSession) -> None:
        call_id = session.call_id
        if any(event.type == "opening_started" for event in self.event_store.list_all(call_id)):
            return
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

    def _agent_runner_runtime_diagnostics(self) -> dict[str, Any]:
        diagnostics = getattr(self.agent_runner, "runtime_diagnostics", None)
        if not callable(diagnostics):
            return {}
        try:
            payload = diagnostics()
        except Exception as exc:
            return {
                "diagnosticsVersion": "runtime-diagnostics-error",
                "diagnosticsErrorType": type(exc).__name__,
            }
        return payload if isinstance(payload, dict) else {}

    def _append_failed_terminal_event(
        self,
        call_id: str,
        *,
        end_reason: str,
        failure_stage: str,
        failure_message: str,
    ) -> None:
        if self.registry.get(call_id).status != CallSessionStatus.FAILED:
            self.registry.transition(call_id, CallSessionStatus.FAILED)
        self._append_event(
            call_id,
            "session_failed",
            "orchestrator",
            {
                "endReason": end_reason,
                "failureStage": failure_stage,
                "failureMessage": failure_message,
            },
        )

    def _move_session_to_waiting_if_possible(self, call_id: str) -> None:
        session = self.registry.get(call_id)
        if session.status == CallSessionStatus.WAITING:
            return
        if CallSessionStatus.WAITING not in ALLOWED_TRANSITIONS.get(session.status, set()):
            return
        self.registry.transition(call_id, CallSessionStatus.WAITING)

    def _build_effective_config(
        self,
        voice: str | None,
        prompt: str | None,
        prompt_effective_config: PromptEffectiveConfig | None = None,
    ) -> EffectiveConfig:
        resolved_voice = voice or self.config.qwen_realtime_voice
        if prompt_effective_config is not None:
            if not prompt_effective_config.instructions.strip():
                raise AiCallError(
                    error_id="prompt_empty",
                    msg="提示词不能为空",
                    status_code=status.HTTP_400_BAD_REQUEST,
                )
            if not prompt_effective_config.opening_message.strip():
                raise AiCallError(
                    error_id="opening_message_empty",
                    msg="开场白不能为空",
                    status_code=status.HTTP_400_BAD_REQUEST,
                )
            return EffectiveConfig(
                model=self.config.qwen_realtime_model,
                voice=resolved_voice,
                prompt=prompt_effective_config.instructions,
                prompt_hash=prompt_effective_config.prompt_hash,
                opening_message=prompt_effective_config.opening_message,
                opening_message_hash=prompt_effective_config.opening_message_hash,
                prompt_source_key=prompt_effective_config.prompt_source_key,
                vad_type=self.config.vad_type,
                vad_threshold=self.config.vad_threshold,
                vad_silence_duration_ms=self.config.vad_silence_duration_ms,
                barge_in_enabled=self.config.barge_in_enabled,
                instructions=prompt_effective_config.instructions,
            )

        resolved_prompt = (prompt or self.config.default_prompt).strip()
        resolved_opening_message = self.config.opening_message.strip()
        if not resolved_prompt:
            raise AiCallError(
                error_id="prompt_empty",
                msg="提示词不能为空",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        if not resolved_opening_message:
            raise AiCallError(
                error_id="opening_message_empty",
                msg="开场白不能为空",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        return EffectiveConfig(
            model=self.config.qwen_realtime_model,
            voice=resolved_voice,
            prompt=resolved_prompt,
            prompt_hash=self._hash_text(resolved_prompt),
            opening_message=resolved_opening_message,
            opening_message_hash=self._hash_text(resolved_opening_message),
            prompt_source_key="debug.prompt" if prompt else "default",
            vad_type=self.config.vad_type,
            vad_threshold=self.config.vad_threshold,
            vad_silence_duration_ms=self.config.vad_silence_duration_ms,
            barge_in_enabled=self.config.barge_in_enabled,
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
    def _failure_stage_for_end_reason(end_reason: str) -> str:
        return {
            "agent_start_failed": "agent_start",
            "room_create_failed": "room_create",
            "provider_connect_failed": "provider_connect",
        }.get(end_reason, end_reason)

    @staticmethod
    def _new_call_id() -> str:
        return f"call_{generate_snowflake_id()}"
