from __future__ import annotations

import asyncio
import base64
import hashlib
import json
from collections.abc import AsyncIterator, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal, Protocol

from app.services.ai_call.audio_bridge import PcmAudioBridge, PcmAudioFrame
from app.services.ai_call.call_end_decision_service import (
    CallEndDecision,
    RuleBasedCallEndDecisionService,
)
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
from app.services.ai_call.sip_barge_in import (
    EnergyVoiceActivityDetector,
    SipBargeInConfig,
    SipBargeInDetector,
    SipBargeInObservation,
    VoiceActivityDetectorProtocol,
    WebRtcVadAdapter,
)
from app.services.ai_call.transcript_trust import (
    decide_realtime_transcript_trust,
    is_realtime_transcript_semantically_rejected,
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
BUSINESS_HANDOFF_CONFIRMATION_TOOL_RESULT = (
    "系统尚未开始转人工。请先询问用户是否确认需要转人工，不得说正在转接、马上接入或已经接通。"
)
CALL_END_FINAL_RESPONSE_TOOL_RESULT = "已记录。请用一句简短礼貌的话结束通话，不要继续提出新问题。"
CALL_END_NO_EXTRA_RESPONSE_TOOL_RESULT = "已记录。系统将结束通话，不要再生成额外回复。"
BROWSER_INTERRUPT_REASONS = {
    "browser_speech_segment_candidate_during_ai_audio",
    "browser_speech_segment_strong_during_ai_audio",
    "browser_user_speech_started_during_ai_audio",
    "browser_user_speech_started_during_ai_response",
}
BROWSER_INTERRUPT_PROVIDER_UPGRADE_GRACE_SECONDS = 2.2
BROWSER_INTERRUPT_STRONG_CANDIDATE_WINDOW_SECONDS = 3.0
BROWSER_INTERRUPT_STRONG_CANDIDATE_COUNT = 2
BROWSER_SPEECH_SEGMENT_MIN_STOP_DURATION_MS = 320
BROWSER_SPEECH_SEGMENT_MIN_STOP_SNR_DB = 10.0
BROWSER_SPEECH_SEGMENT_MIN_STOP_HOT_FRAMES = 8
BROWSER_SPEECH_SEGMENT_SHORT_STOP_DURATION_MS = 300
BROWSER_SPEECH_SEGMENT_SHORT_STOP_SNR_DB = 20.0
BROWSER_SPEECH_SEGMENT_SHORT_STOP_HOT_FRAMES = 5
BROWSER_AUDIO_HOLD_TIMEOUT_SECONDS = 0.75
BROWSER_AUDIO_HOLD_MIN_DURATION_MS = 280
BROWSER_AUDIO_HOLD_MIN_SNR_DB = 20.0
BROWSER_AUDIO_HOLD_MIN_HOT_FRAMES = 7
BROWSER_AUDIO_HOLD_MIN_RMS_DBFS = -30.0
BROWSER_AUDIO_HOLD_LOW_SNR_MIN_DURATION_MS = 400
BROWSER_AUDIO_HOLD_LOW_SNR_MIN_SNR_DB = 17.5
BROWSER_AUDIO_HOLD_LOW_SNR_MIN_HOT_FRAMES = 9
SIP_POST_SPEECH_TAIL_GUARD_SECONDS = 1.8
SIP_DEFERRED_PRE_STOP_MAX_AGE_SECONDS = 1.0
SIP_PROVIDER_CONFIRM_MIN_DURATION_MS = 360
SIP_TURN_CLUSTER_MAX_GAP_SECONDS = 1.4
SIP_TURN_CLUSTER_MIN_BURSTS = 2
SIP_TURN_CLUSTER_MIN_VOICED_MS = 360
SIP_TURN_CLUSTER_MIN_RMS_RANGE_DB = 3.0
SIP_TURN_CLUSTER_MIN_SNR_DB = 20.0
SIP_SINGLE_SHORT_MIN_RMS_DBFS = -20.0
SIP_SINGLE_SHORT_MAX_RMS_DBFS = -12.0
SIP_SINGLE_SHORT_MIN_SNR_DB = 30.0
PROVIDER_CANCEL_RACE_GRACE_SECONDS = 2.0
BROWSER_AUDIO_HOLD_LOW_SNR_MIN_RMS_DBFS = -22.0
BROWSER_AUDIO_HOLD_NEAR_SPEECH_MIN_DURATION_MS = 400
BROWSER_AUDIO_HOLD_NEAR_SPEECH_MIN_SNR_DB = 17.5
BROWSER_AUDIO_HOLD_NEAR_SPEECH_MIN_HOT_FRAMES = 8
BROWSER_AUDIO_HOLD_NEAR_SPEECH_MIN_RMS_DBFS = -18.0
BROWSER_AUDIO_HOLD_DOUBLE_TALK_MIN_DURATION_MS = 480
BROWSER_AUDIO_HOLD_DOUBLE_TALK_MIN_SNR_DB = 20.0
BROWSER_AUDIO_HOLD_DOUBLE_TALK_MIN_HOT_FRAMES = 10
BROWSER_AUDIO_HOLD_DOUBLE_TALK_MIN_RMS_DBFS = -24.0
BROWSER_AUDIO_HOLD_DOUBLE_TALK_MAX_REMOTE_DOMINANCE_DB = 10.0
BROWSER_PRE_STOP_MIN_DURATION_MS = 480
BROWSER_PRE_STOP_MIN_SNR_DB = 24.0
BROWSER_PRE_STOP_MIN_HOT_FRAMES = 10
BROWSER_PRE_STOP_MIN_RMS_DBFS = -30.0
BROWSER_PRE_STOP_ECHO_REJECT_MARGIN_DB = 6.0
SIP_BARGE_IN_INTERRUPT_REASON = "sip_uplink_speech_during_ai_audio"
AGENT_RUNNER_DIAGNOSTICS_VERSION = "interrupt-diagnostics-v1"
InterruptSource = Literal["browser", "provider"]
InterruptAction = Literal["ignore", "candidate", "hold_audio", "pre_stop", "stop_only", "confirm"]


def _agent_runner_runtime_diagnostics() -> dict[str, object]:
    source_path = Path(__file__).resolve()
    diagnostics: dict[str, object] = {
        "diagnosticsVersion": AGENT_RUNNER_DIAGNOSTICS_VERSION,
        "runnerModule": __name__,
        "runnerSourceFile": str(source_path),
    }
    try:
        source = source_path.read_bytes()
        stat = source_path.stat()
    except OSError as exc:
        diagnostics["runnerSourceError"] = f"{type(exc).__name__}: {exc}"
        return diagnostics

    diagnostics.update({
        "runnerSourceHash": f"sha256:{hashlib.sha256(source).hexdigest()}",
        "runnerSourceMtime": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
        "runnerSourceSize": stat.st_size,
    })
    return diagnostics


AGENT_RUNNER_RUNTIME_DIAGNOSTICS = _agent_runner_runtime_diagnostics()


@dataclass(frozen=True, slots=True)
class InterruptDecisionContext:
    source: InterruptSource
    session_status: CallSessionStatus
    has_recent_ai_audio: bool = False
    has_active_model_response: bool = False
    has_interrupt_candidate: bool = False
    candidate_reason: str | None = None
    candidate_stale: bool = False
    has_valid_transcript: bool = False
    browser_segment_phase: str | None = None
    browser_segment_duration_ms: int | None = None
    browser_segment_snr_db: float | None = None
    browser_segment_hot_frame_count: int | None = None
    browser_segment_rms_dbfs: float | None = None
    browser_segment_remote_audio_active: bool | None = None
    browser_segment_remote_audio_rms_dbfs: float | None = None


@dataclass(frozen=True, slots=True)
class InterruptDecision:
    action: InterruptAction
    reason: str


class InterruptDecisionPolicy:
    @staticmethod
    def _is_interruptible_ai_audio(context: InterruptDecisionContext) -> bool:
        if context.session_status == CallSessionStatus.AI_SPEAKING:
            return True
        return context.session_status in {
            CallSessionStatus.CONNECTED,
            CallSessionStatus.AI_THINKING,
        } and (context.has_recent_ai_audio or context.has_active_model_response)

    def decide_speech_started(self, context: InterruptDecisionContext) -> InterruptDecision:
        if context.has_interrupt_candidate and context.candidate_stale:
            return InterruptDecision("ignore", "browser_candidate_expired")

        if context.source == "browser":
            if context.session_status == CallSessionStatus.AI_SPEAKING:
                return InterruptDecision(
                    "candidate",
                    "browser_user_speech_started_during_ai_audio",
                )
            if context.session_status in {
                CallSessionStatus.CONNECTED,
                CallSessionStatus.AI_THINKING,
            } and (context.has_recent_ai_audio or context.has_active_model_response):
                reason = (
                    "browser_user_speech_started_during_ai_audio"
                    if context.has_recent_ai_audio
                    else "browser_user_speech_started_during_ai_response"
                )
                return InterruptDecision("candidate", reason)
            return InterruptDecision("ignore", "not_interrupt")

        if context.session_status == CallSessionStatus.AI_SPEAKING:
            return InterruptDecision("stop_only", "user_speech_started_during_ai_audio")
        if context.session_status in {
            CallSessionStatus.CONNECTED,
            CallSessionStatus.AI_THINKING,
        } and (context.has_recent_ai_audio or context.has_active_model_response):
            return InterruptDecision("stop_only", "user_speech_started_during_ai_audio")
        return InterruptDecision("ignore", "not_interrupt")

    def decide_browser_speech_segment(
        self,
        context: InterruptDecisionContext,
    ) -> InterruptDecision:
        if not self._is_interruptible_ai_audio(context):
            return InterruptDecision("ignore", "not_interrupt")
        if self._is_strong_browser_speech_segment(context):
            return InterruptDecision("candidate", "browser_speech_segment_strong_during_ai_audio")
        return InterruptDecision("candidate", "browser_speech_segment_candidate_during_ai_audio")

    def decide_transcript(self, context: InterruptDecisionContext) -> InterruptDecision:
        if context.has_interrupt_candidate and context.candidate_stale:
            return InterruptDecision("ignore", "browser_candidate_expired")
        if context.has_interrupt_candidate and context.has_valid_transcript:
            return InterruptDecision(
                "confirm",
                context.candidate_reason or "user_speech_started_during_ai_audio",
            )
        return InterruptDecision("ignore", "not_interrupt")

    def decide_browser_audio_hold(self, context: InterruptDecisionContext) -> InterruptDecision:
        if not self._is_audio_hold_browser_speech_segment(context):
            return InterruptDecision("ignore", "browser_audio_hold_not_eligible")
        if self._is_remote_audio_dominating(
            context,
            margin_db=BROWSER_PRE_STOP_ECHO_REJECT_MARGIN_DB,
        ) and not self._is_sustained_double_talk_browser_segment(context):
            return InterruptDecision("ignore", "remote_audio_dominates")
        return InterruptDecision("hold_audio", "browser_audio_hold_medium_speech_segment")

    def decide_browser_pre_stop(self, context: InterruptDecisionContext) -> InterruptDecision:
        if not self._is_pre_stop_browser_speech_segment(context):
            return InterruptDecision("ignore", self._browser_pre_stop_skip_reason(context))
        if self._is_remote_audio_dominating(
            context,
            margin_db=BROWSER_PRE_STOP_ECHO_REJECT_MARGIN_DB,
        ):
            return InterruptDecision("ignore", "remote_audio_dominates")
        return InterruptDecision("pre_stop", "browser_pre_stop_strong_speech_segment")

    @staticmethod
    def _is_strong_browser_speech_segment(context: InterruptDecisionContext) -> bool:
        phase_allows_stop = context.browser_segment_phase in {"started", "updated", "ended"}
        if (
            (context.browser_segment_duration_ms or 0)
            >= BROWSER_SPEECH_SEGMENT_MIN_STOP_DURATION_MS
            and (context.browser_segment_snr_db or 0.0) >= BROWSER_SPEECH_SEGMENT_MIN_STOP_SNR_DB
            and (context.browser_segment_hot_frame_count or 0)
            >= BROWSER_SPEECH_SEGMENT_MIN_STOP_HOT_FRAMES
            and phase_allows_stop
        ):
            return True
        return (
            (context.browser_segment_duration_ms or 0)
            >= BROWSER_SPEECH_SEGMENT_SHORT_STOP_DURATION_MS
            and (context.browser_segment_snr_db or 0.0)
            >= BROWSER_SPEECH_SEGMENT_SHORT_STOP_SNR_DB
            and (context.browser_segment_hot_frame_count or 0)
            >= BROWSER_SPEECH_SEGMENT_SHORT_STOP_HOT_FRAMES
            and context.browser_segment_phase in {"updated", "ended"}
        )

    @staticmethod
    def _is_audio_hold_browser_speech_segment(context: InterruptDecisionContext) -> bool:
        duration_ms = context.browser_segment_duration_ms or 0
        snr_db = context.browser_segment_snr_db or 0.0
        hot_frames = context.browser_segment_hot_frame_count or 0
        rms_dbfs = context.browser_segment_rms_dbfs
        if context.browser_segment_phase not in {"updated", "ended"} or rms_dbfs is None:
            return False
        standard_hold = (
            duration_ms >= BROWSER_AUDIO_HOLD_MIN_DURATION_MS
            and snr_db >= BROWSER_AUDIO_HOLD_MIN_SNR_DB
            and hot_frames >= BROWSER_AUDIO_HOLD_MIN_HOT_FRAMES
            and rms_dbfs >= BROWSER_AUDIO_HOLD_MIN_RMS_DBFS
        )
        low_snr_sustained_hold = (
            duration_ms >= BROWSER_AUDIO_HOLD_LOW_SNR_MIN_DURATION_MS
            and snr_db >= BROWSER_AUDIO_HOLD_LOW_SNR_MIN_SNR_DB
            and hot_frames >= BROWSER_AUDIO_HOLD_LOW_SNR_MIN_HOT_FRAMES
            and rms_dbfs >= BROWSER_AUDIO_HOLD_LOW_SNR_MIN_RMS_DBFS
        )
        near_speech_hold = (
            duration_ms >= BROWSER_AUDIO_HOLD_NEAR_SPEECH_MIN_DURATION_MS
            and snr_db >= BROWSER_AUDIO_HOLD_NEAR_SPEECH_MIN_SNR_DB
            and hot_frames >= BROWSER_AUDIO_HOLD_NEAR_SPEECH_MIN_HOT_FRAMES
            and rms_dbfs >= BROWSER_AUDIO_HOLD_NEAR_SPEECH_MIN_RMS_DBFS
        )
        return standard_hold or low_snr_sustained_hold or near_speech_hold

    @staticmethod
    def _is_sustained_double_talk_browser_segment(context: InterruptDecisionContext) -> bool:
        local_rms = context.browser_segment_rms_dbfs
        remote_rms = context.browser_segment_remote_audio_rms_dbfs
        if local_rms is None or remote_rms is None:
            return False
        remote_dominance_db = remote_rms - local_rms
        return (
            (context.browser_segment_duration_ms or 0)
            >= BROWSER_AUDIO_HOLD_DOUBLE_TALK_MIN_DURATION_MS
            and (context.browser_segment_snr_db or 0.0)
            >= BROWSER_AUDIO_HOLD_DOUBLE_TALK_MIN_SNR_DB
            and (context.browser_segment_hot_frame_count or 0)
            >= BROWSER_AUDIO_HOLD_DOUBLE_TALK_MIN_HOT_FRAMES
            and local_rms >= BROWSER_AUDIO_HOLD_DOUBLE_TALK_MIN_RMS_DBFS
            and remote_dominance_db <= BROWSER_AUDIO_HOLD_DOUBLE_TALK_MAX_REMOTE_DOMINANCE_DB
        )

    @staticmethod
    def _is_pre_stop_browser_speech_segment(context: InterruptDecisionContext) -> bool:
        return (
            context.browser_segment_phase in {"updated", "ended"}
            and (context.browser_segment_duration_ms or 0) >= BROWSER_PRE_STOP_MIN_DURATION_MS
            and (context.browser_segment_snr_db or 0.0) >= BROWSER_PRE_STOP_MIN_SNR_DB
            and (context.browser_segment_hot_frame_count or 0)
            >= BROWSER_PRE_STOP_MIN_HOT_FRAMES
            and context.browser_segment_rms_dbfs is not None
            and context.browser_segment_rms_dbfs >= BROWSER_PRE_STOP_MIN_RMS_DBFS
        )

    @staticmethod
    def _browser_pre_stop_skip_reason(context: InterruptDecisionContext) -> str:
        if context.browser_segment_phase not in {"updated", "ended"}:
            return "phase_not_final_enough"
        if (context.browser_segment_duration_ms or 0) < BROWSER_PRE_STOP_MIN_DURATION_MS:
            return "below_min_duration"
        if (context.browser_segment_snr_db or 0.0) < BROWSER_PRE_STOP_MIN_SNR_DB:
            return "below_min_snr"
        if (context.browser_segment_hot_frame_count or 0) < BROWSER_PRE_STOP_MIN_HOT_FRAMES:
            return "below_min_hot_frames"
        if context.browser_segment_rms_dbfs is None:
            return "missing_rms"
        if context.browser_segment_rms_dbfs < BROWSER_PRE_STOP_MIN_RMS_DBFS:
            return "below_min_rms"
        return "not_eligible"

    @staticmethod
    def _is_remote_audio_dominating(
        context: InterruptDecisionContext,
        *,
        margin_db: float,
    ) -> bool:
        if context.browser_segment_remote_audio_active is not True:
            return False
        local_rms = context.browser_segment_rms_dbfs
        remote_rms = context.browser_segment_remote_audio_rms_dbfs
        if local_rms is None or remote_rms is None:
            return False
        return local_rms + margin_db < remote_rms


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
    browser_candidate_first_at: datetime | None = None
    browser_candidate_count: int = 0
    browser_candidate_promoted: bool = False
    browser_audio_hold_requested: bool = False
    browser_audio_hold_confirmed: bool = False
    browser_audio_hold_expires_at: datetime | None = None
    browser_pre_stop_requested: bool = False
    browser_pre_stop_confirmed: bool = False
    browser_pre_stop_expires_at: datetime | None = None
    sip_barge_in_requested: bool = False
    sip_barge_in_confirmed: bool = False
    sip_barge_in_confirmed_by: str | None = None
    sip_barge_in_expires_at: datetime | None = None
    sip_pre_stop_requested: bool = False
    sip_pre_stop_deferred: bool = False
    sip_pre_stop_at: datetime | None = None
    sip_candidate_class: str | None = None
    sip_candidate_response_id: str | None = None
    sip_candidate_generation: int | None = None
    sip_provider_speech_confirmable: bool = False
    sip_interrupt_rejected: bool = False
    sip_recovery_count: int = 0
    sip_turn_cluster_response_id: str | None = None
    sip_turn_cluster_first_at: datetime | None = None
    sip_turn_cluster_last_at: datetime | None = None
    sip_turn_cluster_burst_count: int = 0
    sip_turn_cluster_voiced_ms: int = 0
    sip_turn_cluster_min_rms_dbfs: float | None = None
    sip_turn_cluster_max_rms_dbfs: float | None = None
    sip_turn_cluster_max_snr_db: float | None = None
    browser_segment_phase: str | None = None
    browser_segment_duration_ms: int | None = None
    browser_segment_snr_db: float | None = None
    browser_segment_hot_frame_count: int | None = None
    browser_segment_rms_dbfs: float | None = None
    browser_segment_remote_audio_active: bool | None = None
    browser_segment_remote_audio_rms_dbfs: float | None = None

    @property
    def transcript(self) -> str:
        return "".join(self.transcript_parts).strip()


@dataclass(slots=True)
class ResponseLifecycle:
    active: bool = False
    cancel_pending: bool = False
    cancel_race_ignore_until: datetime | None = None
    pending_create: bool = False
    pending_input_text: str | None = None
    response_generation: int = 0


@dataclass(slots=True)
class PlaybackGuard:
    generation: int = 0
    current_response_id: str | None = None
    current_response_generation: int = 0
    current_response_audio_published: bool = False
    cancelled_response_ids: set[str] = field(default_factory=set)
    cancel_requested: bool = False
    audio_stop_requested: bool = False
    user_speech_active: bool = False
    awaiting_response_start_after_interrupt: bool = False
    suppress_audio_until: datetime | None = None


@dataclass(slots=True)
class PendingCallEnd:
    tool_call_id: str
    tool_reason: str
    end_reason: str
    final_response_started: bool = False
    scheduled: bool = False


@dataclass(slots=True)
class PendingCallEndIntent:
    transcript: str
    reason: str
    summary: str
    source: str
    confidence: float


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
        browser_audio_hold_timeout_seconds: float = BROWSER_AUDIO_HOLD_TIMEOUT_SECONDS,
        browser_pre_stop_timeout_seconds: float = BROWSER_INTERRUPT_PROVIDER_UPGRADE_GRACE_SECONDS,
        sip_barge_in_enabled: bool = True,
        sip_barge_in_min_rms_dbfs: float = -35.0,
        sip_barge_in_min_speech_duration_ms: int = 220,
        sip_barge_in_hold_timeout_seconds: float = 5.0,
        sip_barge_in_fast_stop_enabled: bool = False,
        sip_barge_in_config: SipBargeInConfig | None = None,
        sip_barge_in_vad: VoiceActivityDetectorProtocol | None = None,
        sip_barge_in_recovery_silence_ms: int = 600,
        sip_barge_in_recovery_max_per_turn: int = 1,
        user_turn_stability_delay_seconds: float = 0.35,
        handoff_prompt_constraint_enabled: bool = False,
        call_end_decision_service: RuleBasedCallEndDecisionService | None = None,
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
        self.browser_audio_hold_timeout_seconds = max(0.0, browser_audio_hold_timeout_seconds)
        self.browser_pre_stop_timeout_seconds = max(0.0, browser_pre_stop_timeout_seconds)
        self.sip_barge_in_enabled = sip_barge_in_enabled
        self.sip_barge_in_min_rms_dbfs = sip_barge_in_min_rms_dbfs
        self.sip_barge_in_min_speech_duration_ms = max(20, sip_barge_in_min_speech_duration_ms)
        self.sip_barge_in_hold_timeout_seconds = max(0.0, sip_barge_in_hold_timeout_seconds)
        self.sip_barge_in_fast_stop_enabled = sip_barge_in_fast_stop_enabled
        self.sip_barge_in_config = sip_barge_in_config or SipBargeInConfig(
            rms_threshold_dbfs=sip_barge_in_min_rms_dbfs,
            candidate_min_duration_ms=max(20, sip_barge_in_min_speech_duration_ms),
        )
        self.sip_barge_in_recovery_silence_ms = max(0, sip_barge_in_recovery_silence_ms)
        self.sip_barge_in_recovery_max_per_turn = max(0, sip_barge_in_recovery_max_per_turn)
        self.user_turn_stability_delay_seconds = max(0.0, user_turn_stability_delay_seconds)
        self.handoff_prompt_constraint_enabled = handoff_prompt_constraint_enabled
        self.call_end_decision_service = (
            call_end_decision_service or RuleBasedCallEndDecisionService()
        )
        self.call_end_scheduler = call_end_scheduler
        self._interrupt_policy = InterruptDecisionPolicy()
        self._sip_barge_in_vad = sip_barge_in_vad or (
            WebRtcVadAdapter()
            if self.sip_barge_in_fast_stop_enabled
            else EnergyVoiceActivityDetector()
        )
        self._sip_barge_in_detector = (
            SipBargeInDetector(
                config=self.sip_barge_in_config,
                vad=self._sip_barge_in_vad,
            )
            if self.sip_barge_in_enabled
            else None
        )
        self._providers: dict[str, RealtimeProviderProtocol] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._audio_tasks: dict[str, asyncio.Task[None]] = {}
        self._playout_tasks: dict[str, asyncio.Task[None]] = {}
        self._turn_response_tasks: dict[str, asyncio.Task[None]] = {}
        self._last_ai_audio_published_at: dict[str, datetime] = {}
        self._last_sip_provider_speech_stopped_at: dict[str, datetime] = {}
        self._last_sip_rejected_noise_response_id: dict[str, str | None] = {}
        self._pending_user_turns: dict[str, PendingUserTurn] = {}
        self._response_lifecycles: dict[str, ResponseLifecycle] = {}
        self._playback_guards: dict[str, PlaybackGuard] = {}
        self._pending_call_ends: dict[str, PendingCallEnd] = {}
        self._pending_call_end_intents: dict[str, PendingCallEndIntent] = {}
        self._browser_audio_hold_tasks: dict[str, asyncio.Task[None]] = {}
        self._browser_pre_stop_tasks: dict[str, asyncio.Task[None]] = {}
        self._sip_barge_in_tasks: dict[str, asyncio.Task[None]] = {}
        self._sip_clean_window_tasks: dict[str, asyncio.Task[None]] = {}
        self._sip_recovery_tasks: dict[str, asyncio.Task[None]] = {}

    def runtime_diagnostics(self) -> dict[str, object]:
        return dict(AGENT_RUNNER_RUNTIME_DIAGNOSTICS)

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
        await self._cancel_browser_audio_hold_task(call_id)
        await self._cancel_browser_pre_stop_task(call_id)
        await self._cancel_sip_barge_in_task(call_id)
        await self._cancel_sip_clean_window_task(call_id)
        await self._cancel_sip_recovery_task(call_id)

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
        self._last_sip_provider_speech_stopped_at.pop(call_id, None)
        self._last_sip_rejected_noise_response_id.pop(call_id, None)
        self._pending_user_turns.pop(call_id, None)
        self._response_lifecycles.pop(call_id, None)
        self._playback_guards.pop(call_id, None)
        self._pending_call_ends.pop(call_id, None)
        self._pending_call_end_intents.pop(call_id, None)
        if self._sip_barge_in_detector is not None:
            self._sip_barge_in_detector.reset(call_id)

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
        await self._maybe_handle_sip_barge_in_audio(call_id, provider, frame)
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

    async def _maybe_handle_sip_barge_in_audio(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        frame: PcmAudioFrame,
    ) -> None:
        detector = self._sip_barge_in_detector
        if detector is None:
            return

        session = self.registry.get(call_id)
        if not self._is_sip_participant(session):
            detector.reset(call_id)
            return
        if not self._is_barge_in_enabled_for_session(session):
            detector.reset(call_id)
            return

        now = datetime.now(timezone.utc)
        interruptible = self._is_sip_barge_in_interruptible(call_id, session, now)
        observation = detector.observe(
            call_id,
            frame,
            now=now,
            interruptible=interruptible,
        )
        if observation.candidate_class == "impulse_noise":
            payload = self._sip_barge_in_event_payload(call_id, observation)
            payload["reason"] = "impulse_noise"
            self._append_event(call_id, "sip_impulse_noise_ignored", "agent", payload)
            return
        if not observation.candidate:
            await self._maybe_upgrade_deferred_sip_pre_stop(
                call_id=call_id,
                provider=provider,
                trigger_timestamp=now,
                observation=observation,
            )
            return

        await self._handle_sip_barge_in_candidate(
            call_id=call_id,
            provider=provider,
            trigger_timestamp=now,
            observation=observation,
        )

    async def _handle_sip_barge_in_candidate(
        self,
        *,
        call_id: str,
        provider: RealtimeProviderProtocol,
        trigger_timestamp: datetime,
        observation: SipBargeInObservation,
    ) -> None:
        session = self.registry.get(call_id)
        if session.status in {
            CallSessionStatus.CONNECTED,
            CallSessionStatus.AI_THINKING,
        }:
            self.registry.transition(call_id, CallSessionStatus.AI_SPEAKING)
            session = self.registry.get(call_id)
        if session.status != CallSessionStatus.AI_SPEAKING:
            return

        turn = self._pending_turn(call_id, reset_if_finished=True)
        self._record_sip_turn_cluster_observation(call_id, turn, trigger_timestamp, observation)
        if not self._has_sip_turn_cluster_pre_stop_evidence(turn):
            self._expire_stale_deferred_sip_candidate_if_needed(
                call_id=call_id,
                turn=turn,
                trigger_timestamp=trigger_timestamp,
                observation=observation,
            )
        if turn.started_at is None:
            turn.started_at = trigger_timestamp
        candidate_created = self._mark_interrupt_candidate(
            call_id=call_id,
            turn=turn,
            trigger_timestamp=trigger_timestamp,
            source="sip",
            reason=SIP_BARGE_IN_INTERRUPT_REASON,
        )
        if candidate_created and not turn.sip_barge_in_requested:
            guard = self._playback_guard(call_id)
            turn.sip_barge_in_requested = True
            turn.sip_candidate_class = observation.candidate_class
            turn.sip_candidate_response_id = guard.current_response_id
            turn.sip_candidate_generation = guard.generation
            self._append_event(
                call_id,
                "sip_interrupt_candidate",
                "agent",
                self._sip_barge_in_event_payload(call_id, observation),
            )

        self._extend_sip_barge_in(call_id, turn, trigger_timestamp)
        if self.sip_barge_in_fast_stop_enabled:
            await self._maybe_pre_stop_sip_barge_in_candidate(
                call_id=call_id,
                provider=provider,
                turn=turn,
                trigger_timestamp=trigger_timestamp,
                observation=observation,
            )

    def _is_sip_barge_in_interruptible(
        self,
        call_id: str,
        session: CallSession,
        timestamp: datetime,
    ) -> bool:
        if session.status == CallSessionStatus.AI_SPEAKING:
            return True
        return session.status in {
            CallSessionStatus.CONNECTED,
            CallSessionStatus.AI_THINKING,
        } and (
            self._has_recent_ai_audio(call_id, timestamp)
            or self._has_active_model_response(call_id)
        )

    @staticmethod
    def _is_sip_participant(session: CallSession) -> bool:
        return session.participant_identity.startswith("sip-")

    def _is_barge_in_enabled_for_session(self, session: CallSession) -> bool:
        return bool(self._config_value(session.effective_config, "barge_in_enabled", True))

    def _sip_barge_in_event_payload(
        self,
        call_id: str,
        observation: SipBargeInObservation,
    ) -> dict[str, Any]:
        guard = self._playback_guard(call_id)
        payload: dict[str, Any] = {
            "reason": SIP_BARGE_IN_INTERRUPT_REASON,
            "speechDurationMs": observation.speech_duration_ms,
            "frameDurationMs": observation.frame_duration_ms,
            "minSpeechDurationMs": self.sip_barge_in_min_speech_duration_ms,
            "minRmsDbfs": self.sip_barge_in_min_rms_dbfs,
            "holdTimeoutSeconds": self.sip_barge_in_hold_timeout_seconds,
        }
        if observation.rms_dbfs is not None:
            payload["rmsDbfs"] = round(observation.rms_dbfs, 2)
        if observation.noise_floor_dbfs is not None:
            payload["noiseFloorDbfs"] = round(observation.noise_floor_dbfs, 2)
        if observation.snr_db is not None:
            payload["snrDb"] = round(observation.snr_db, 2)
        if observation.peak_dbfs is not None:
            payload["peakDbfs"] = round(observation.peak_dbfs, 2)
        payload["vadVoicedMs"] = observation.vad_voiced_ms
        payload["candidateDurationMs"] = observation.candidate_duration_ms
        payload["candidateClass"] = observation.candidate_class
        if guard.current_response_id:
            payload["responseId"] = guard.current_response_id
        payload["generation"] = guard.generation
        if self._sip_barge_in_detector is not None:
            diagnostics = self._sip_barge_in_detector.latest_observation_payload(call_id)
            for key in (
                "wallClockSpeechMs",
                "maxVoicedFrameGapMs",
                "rmsRangeDb",
                "rmsDirectionChanges",
                "largeRmsJumpCount",
                "speechQualityRejection",
            ):
                if key in diagnostics:
                    payload[key] = diagnostics[key]
        return payload

    async def _maybe_upgrade_deferred_sip_pre_stop(
        self,
        *,
        call_id: str,
        provider: RealtimeProviderProtocol,
        trigger_timestamp: datetime,
        observation: SipBargeInObservation,
    ) -> None:
        if not self.sip_barge_in_fast_stop_enabled:
            return
        if not observation.active or observation.candidate_class is None:
            return
        turn = self._pending_user_turns.get(call_id)
        if (
            turn is None
            or not turn.sip_barge_in_requested
            or turn.sip_pre_stop_requested
            or turn.sip_interrupt_rejected
        ):
            return
        if self._expire_sip_candidate_response_mismatch_if_needed(
            call_id=call_id,
            turn=turn,
            trigger_timestamp=trigger_timestamp,
            observation=observation,
        ):
            return
        if self._expire_stale_deferred_sip_candidate_if_needed(
            call_id=call_id,
            turn=turn,
            trigger_timestamp=trigger_timestamp,
            observation=observation,
        ):
            return
        await self._maybe_pre_stop_sip_barge_in_candidate(
            call_id=call_id,
            provider=provider,
            turn=turn,
            trigger_timestamp=trigger_timestamp,
            observation=observation,
        )

    async def _maybe_pre_stop_sip_barge_in_candidate(
        self,
        *,
        call_id: str,
        provider: RealtimeProviderProtocol,
        turn: PendingUserTurn,
        trigger_timestamp: datetime,
        observation: SipBargeInObservation,
    ) -> None:
        if self._expire_sip_candidate_response_mismatch_if_needed(
            call_id=call_id,
            turn=turn,
            trigger_timestamp=trigger_timestamp,
            observation=observation,
        ):
            return
        if (
            observation.candidate_class == "stable_speech_candidate"
            and self._is_within_sip_post_speech_tail_guard(call_id, trigger_timestamp)
        ):
            self._defer_sip_pre_stop(
                call_id=call_id,
                turn=turn,
                observation=observation,
                reason="awaiting_post_speech_tail_guard",
                required_duration_ms=self.sip_barge_in_config.pre_stop_min_duration_ms,
            )
            return
        if (
            observation.candidate_class == "stable_speech_candidate"
            and self._is_within_sip_rejected_noise_same_response_guard(call_id)
        ):
            self._defer_sip_pre_stop(
                call_id=call_id,
                turn=turn,
                observation=observation,
                reason="awaiting_rejected_noise_tail_guard",
                required_duration_ms=self.sip_barge_in_config.pre_stop_min_duration_ms,
            )
            return
        if (
            observation.candidate_class == "stable_speech_candidate"
            and self._has_sip_turn_cluster_pre_stop_evidence(turn)
        ):
            await self._pre_stop_sip_barge_in_candidate(
                call_id=call_id,
                provider=provider,
                turn=turn,
                trigger_timestamp=trigger_timestamp,
                observation=observation,
            )
            return
        if (
            self._expire_stale_deferred_sip_candidate_if_needed(
                call_id=call_id,
                turn=turn,
                trigger_timestamp=trigger_timestamp,
                observation=observation,
            )
        ):
            return

        required_duration_ms = self._sip_required_pre_stop_duration_ms(observation)
        detector = self._sip_barge_in_detector
        if (
            observation.candidate_class == "stable_speech_candidate"
            and detector is not None
            and detector.has_fast_pre_stop_local_speech(call_id)
        ):
            required_duration_ms = min(required_duration_ms, observation.candidate_duration_ms)
        if observation.candidate_duration_ms >= required_duration_ms:
            if (
                observation.candidate_class == "stable_speech_candidate"
                and detector is not None
                and not detector.has_pre_stop_local_speech(call_id)
                and not detector.has_fast_pre_stop_local_speech(call_id)
            ):
                self._defer_sip_pre_stop(
                    call_id=call_id,
                    turn=turn,
                    observation=observation,
                    reason="awaiting_speech_quality",
                    required_duration_ms=required_duration_ms,
                )
                return
            await self._pre_stop_sip_barge_in_candidate(
                call_id=call_id,
                provider=provider,
                turn=turn,
                trigger_timestamp=trigger_timestamp,
                observation=observation,
            )
            return
        self._defer_sip_pre_stop(
            call_id=call_id,
            turn=turn,
            observation=observation,
            reason="awaiting_pre_stop_authority",
            required_duration_ms=required_duration_ms,
        )

    def _defer_sip_pre_stop(
        self,
        *,
        call_id: str,
        turn: PendingUserTurn,
        observation: SipBargeInObservation,
        reason: str,
        required_duration_ms: int,
    ) -> None:
        if turn.sip_pre_stop_deferred:
            return
        turn.sip_pre_stop_deferred = True
        payload = self._sip_barge_in_event_payload(call_id, observation)
        payload.update({
            "reason": reason,
            "requiredPreStopDurationMs": required_duration_ms,
        })
        self._append_event(call_id, "sip_pre_stop_deferred", "agent", payload)

    def _record_sip_turn_cluster_observation(
        self,
        call_id: str,
        turn: PendingUserTurn,
        timestamp: datetime,
        observation: SipBargeInObservation,
    ) -> None:
        guard = self._playback_guard(call_id)
        response_id = guard.current_response_id
        if response_id != turn.sip_turn_cluster_response_id:
            self._reset_sip_turn_cluster(turn, response_id=response_id)
        if observation.candidate_class != "stable_speech_candidate":
            return
        if observation.rms_dbfs is None or observation.snr_db is None:
            self._reset_sip_turn_cluster(turn, response_id=response_id)
            return
        if observation.candidate_duration_ms < self.sip_barge_in_config.candidate_min_duration_ms:
            return
        if self._sip_observation_quality_rejection(call_id) is not None:
            self._reset_sip_turn_cluster(turn, response_id=response_id)
            return

        if (
            turn.sip_turn_cluster_last_at is not None
            and (timestamp - turn.sip_turn_cluster_last_at).total_seconds()
            > SIP_TURN_CLUSTER_MAX_GAP_SECONDS
        ):
            self._reset_sip_turn_cluster(turn, response_id=response_id)

        if turn.sip_turn_cluster_first_at is None:
            turn.sip_turn_cluster_first_at = timestamp
        turn.sip_turn_cluster_last_at = timestamp
        turn.sip_turn_cluster_burst_count += 1
        turn.sip_turn_cluster_voiced_ms += observation.vad_voiced_ms
        turn.sip_turn_cluster_min_rms_dbfs = (
            observation.rms_dbfs
            if turn.sip_turn_cluster_min_rms_dbfs is None
            else min(turn.sip_turn_cluster_min_rms_dbfs, observation.rms_dbfs)
        )
        turn.sip_turn_cluster_max_rms_dbfs = (
            observation.rms_dbfs
            if turn.sip_turn_cluster_max_rms_dbfs is None
            else max(turn.sip_turn_cluster_max_rms_dbfs, observation.rms_dbfs)
        )
        turn.sip_turn_cluster_max_snr_db = (
            observation.snr_db
            if turn.sip_turn_cluster_max_snr_db is None
            else max(turn.sip_turn_cluster_max_snr_db, observation.snr_db)
        )

    @staticmethod
    def _reset_sip_turn_cluster(
        turn: PendingUserTurn,
        *,
        response_id: str | None,
    ) -> None:
        turn.sip_turn_cluster_response_id = response_id
        turn.sip_turn_cluster_first_at = None
        turn.sip_turn_cluster_last_at = None
        turn.sip_turn_cluster_burst_count = 0
        turn.sip_turn_cluster_voiced_ms = 0
        turn.sip_turn_cluster_min_rms_dbfs = None
        turn.sip_turn_cluster_max_rms_dbfs = None
        turn.sip_turn_cluster_max_snr_db = None

    def _has_sip_turn_cluster_pre_stop_evidence(self, turn: PendingUserTurn) -> bool:
        if turn.sip_turn_cluster_burst_count < SIP_TURN_CLUSTER_MIN_BURSTS:
            return False
        if turn.sip_turn_cluster_voiced_ms < SIP_TURN_CLUSTER_MIN_VOICED_MS:
            return False
        if (
            turn.sip_turn_cluster_max_snr_db is None
            or turn.sip_turn_cluster_max_snr_db < SIP_TURN_CLUSTER_MIN_SNR_DB
        ):
            return False
        if (
            turn.sip_turn_cluster_min_rms_dbfs is None
            or turn.sip_turn_cluster_max_rms_dbfs is None
        ):
            return False
        return (
            turn.sip_turn_cluster_max_rms_dbfs - turn.sip_turn_cluster_min_rms_dbfs
            >= SIP_TURN_CLUSTER_MIN_RMS_RANGE_DB
        )

    def _has_sip_single_short_pre_stop_evidence(
        self,
        call_id: str,
        observation: SipBargeInObservation,
    ) -> bool:
        if observation.candidate_class != "stable_speech_candidate":
            return False
        if observation.rms_dbfs is None or observation.snr_db is None:
            return False
        if observation.candidate_duration_ms < self.sip_barge_in_config.candidate_min_duration_ms:
            return False
        if observation.vad_voiced_ms < self.sip_barge_in_config.vad_voiced_duration_ms:
            return False
        detector = self._sip_barge_in_detector
        if detector is None:
            return False
        return detector.has_single_short_pre_stop_local_speech(
            call_id,
            min_rms_dbfs=SIP_SINGLE_SHORT_MIN_RMS_DBFS,
            max_rms_dbfs=SIP_SINGLE_SHORT_MAX_RMS_DBFS,
            min_snr_db=SIP_SINGLE_SHORT_MIN_SNR_DB,
        )

    def _sip_turn_cluster_payload(self, turn: PendingUserTurn) -> dict[str, Any]:
        if turn.sip_turn_cluster_burst_count <= 0:
            return {}
        rms_range_db = None
        if (
            turn.sip_turn_cluster_min_rms_dbfs is not None
            and turn.sip_turn_cluster_max_rms_dbfs is not None
        ):
            rms_range_db = round(
                turn.sip_turn_cluster_max_rms_dbfs - turn.sip_turn_cluster_min_rms_dbfs,
                2,
            )
        wall_ms = None
        if (
            turn.sip_turn_cluster_first_at is not None
            and turn.sip_turn_cluster_last_at is not None
        ):
            wall_ms = round(
                max(
                    0.0,
                    (
                        turn.sip_turn_cluster_last_at - turn.sip_turn_cluster_first_at
                    ).total_seconds()
                    * 1000,
                )
            )
        return {
            "sipTurnClusterBurstCount": turn.sip_turn_cluster_burst_count,
            "sipTurnClusterVoicedMs": turn.sip_turn_cluster_voiced_ms,
            "sipTurnClusterWallMs": wall_ms,
            "sipTurnClusterRmsRangeDb": rms_range_db,
            "sipTurnClusterMaxSnrDb": (
                round(turn.sip_turn_cluster_max_snr_db, 2)
                if turn.sip_turn_cluster_max_snr_db is not None
                else None
            ),
        }

    def _sip_single_short_payload(
        self,
        call_id: str,
        observation: SipBargeInObservation,
    ) -> dict[str, Any]:
        if not self._has_sip_single_short_pre_stop_evidence(call_id, observation):
            return {}
        return {"sipShortSpeechEvidence": "single_high_confidence_burst"}

    def _sip_observation_quality_rejection(self, call_id: str) -> str | None:
        detector = self._sip_barge_in_detector
        if detector is None:
            return None
        value = detector.latest_observation_payload(call_id).get("speechQualityRejection")
        return value if isinstance(value, str) and value else None

    def _is_within_sip_post_speech_tail_guard(
        self,
        call_id: str,
        timestamp: datetime,
    ) -> bool:
        last_stopped_at = self._last_sip_provider_speech_stopped_at.get(call_id)
        if last_stopped_at is None:
            return False
        elapsed_seconds = (timestamp - last_stopped_at).total_seconds()
        return 0 <= elapsed_seconds <= SIP_POST_SPEECH_TAIL_GUARD_SECONDS

    def _is_within_sip_rejected_noise_same_response_guard(self, call_id: str) -> bool:
        rejected_response_id = self._last_sip_rejected_noise_response_id.get(call_id)
        if rejected_response_id is None:
            return False
        guard = self._playback_guard(call_id)
        return guard.current_response_id == rejected_response_id

    def _expire_sip_candidate_response_mismatch_if_needed(
        self,
        *,
        call_id: str,
        turn: PendingUserTurn,
        trigger_timestamp: datetime,
        observation: SipBargeInObservation,
    ) -> bool:
        if (
            not turn.sip_barge_in_requested
            or turn.sip_pre_stop_requested
            or turn.sip_interrupt_rejected
            or turn.sip_barge_in_confirmed
        ):
            return False
        guard = self._playback_guard(call_id)
        if (
            turn.sip_candidate_response_id == guard.current_response_id
            and (
                turn.sip_candidate_generation is None
                or turn.sip_candidate_generation == guard.generation
            )
        ):
            return False

        elapsed_ms: int | None = None
        if turn.interrupt_trigger_at is not None:
            elapsed_ms = round(
                max(0.0, (trigger_timestamp - turn.interrupt_trigger_at).total_seconds() * 1000)
            )
        payload = self._sip_barge_in_event_payload(call_id, observation)
        payload.update({
            "reason": "candidate_response_mismatch",
            "elapsedMs": elapsed_ms,
            "candidateResponseId": turn.sip_candidate_response_id,
            "currentResponseId": guard.current_response_id,
            "candidateGeneration": turn.sip_candidate_generation,
            "currentGeneration": guard.generation,
        })
        self._append_event(call_id, "sip_interrupt_candidate_expired", "agent", payload)
        self._cancel_sip_barge_in_task_nowait(call_id)
        turn.sip_barge_in_requested = False
        turn.sip_barge_in_confirmed = False
        turn.sip_barge_in_confirmed_by = None
        turn.sip_barge_in_expires_at = None
        turn.sip_pre_stop_deferred = False
        turn.sip_candidate_class = None
        turn.sip_candidate_response_id = None
        turn.sip_candidate_generation = None
        turn.sip_provider_speech_confirmable = False
        self._ignore_empty_turn(call_id, turn, "sip_candidate_response_mismatch")
        turn.started_at = None
        turn.interrupt_trigger_at = None
        return True

    @staticmethod
    def _is_stale_deferred_sip_candidate(
        turn: PendingUserTurn,
        timestamp: datetime,
    ) -> bool:
        if not turn.sip_pre_stop_deferred or turn.interrupt_trigger_at is None:
            return False
        elapsed_seconds = (timestamp - turn.interrupt_trigger_at).total_seconds()
        return elapsed_seconds > SIP_DEFERRED_PRE_STOP_MAX_AGE_SECONDS

    def _expire_stale_deferred_sip_candidate_if_needed(
        self,
        *,
        call_id: str,
        turn: PendingUserTurn,
        trigger_timestamp: datetime,
        observation: SipBargeInObservation,
    ) -> bool:
        if (
            not turn.sip_barge_in_requested
            or turn.sip_pre_stop_requested
            or turn.sip_interrupt_rejected
            or turn.sip_barge_in_confirmed
            or not self._is_stale_deferred_sip_candidate(turn, trigger_timestamp)
        ):
            return False

        elapsed_ms: int | None = None
        if turn.interrupt_trigger_at is not None:
            elapsed_ms = round(
                max(0.0, (trigger_timestamp - turn.interrupt_trigger_at).total_seconds() * 1000)
            )
        payload = self._sip_barge_in_event_payload(call_id, observation)
        payload.update({
            "reason": "stale_deferred_pre_stop_candidate",
            "elapsedMs": elapsed_ms,
            "maxDeferredCandidateAgeMs": round(SIP_DEFERRED_PRE_STOP_MAX_AGE_SECONDS * 1000),
        })
        self._append_event(call_id, "sip_interrupt_candidate_expired", "agent", payload)
        self._cancel_sip_barge_in_task_nowait(call_id)
        turn.sip_barge_in_requested = False
        turn.sip_barge_in_confirmed = False
        turn.sip_barge_in_confirmed_by = None
        turn.sip_barge_in_expires_at = None
        turn.sip_pre_stop_deferred = False
        turn.sip_candidate_class = None
        turn.sip_candidate_response_id = None
        turn.sip_candidate_generation = None
        turn.sip_provider_speech_confirmable = False
        self._ignore_empty_turn(call_id, turn, "stale_deferred_sip_candidate")
        turn.started_at = None
        turn.interrupt_trigger_at = None
        return True

    def _sip_required_pre_stop_duration_ms(self, observation: SipBargeInObservation) -> int:
        if observation.candidate_class == "strong_short_speech_candidate":
            return max(
                self.sip_barge_in_config.short_speech_min_duration_ms,
                observation.candidate_duration_ms,
            )
        return max(
            self.sip_barge_in_config.candidate_min_duration_ms,
            self.sip_barge_in_config.pre_stop_min_duration_ms,
        )

    async def _pre_stop_sip_barge_in_candidate(
        self,
        *,
        call_id: str,
        provider: RealtimeProviderProtocol,
        turn: PendingUserTurn,
        trigger_timestamp: datetime,
        observation: SipBargeInObservation,
    ) -> None:
        if turn.sip_pre_stop_requested:
            return
        turn.sip_pre_stop_requested = True
        turn.sip_provider_speech_confirmable = self._is_sip_provider_confirmable_local_speech(
            call_id,
            observation,
        )
        turn.sip_pre_stop_at = datetime.now(timezone.utc)

        await self._invalidate_audio_for_interrupt_candidate(
            call_id=call_id,
            provider=provider,
            trigger_timestamp=trigger_timestamp,
            source="sip",
            reason=SIP_BARGE_IN_INTERRUPT_REASON,
            cancel_provider_response=False,
        )

        guard = self._playback_guard(call_id)
        candidate_to_stop_ms = round(
            max(0.0, (turn.sip_pre_stop_at - trigger_timestamp).total_seconds() * 1000)
        )
        payload = self._sip_barge_in_event_payload(call_id, observation)
        payload.update({
            "candidateToStopMs": candidate_to_stop_ms,
            "responseId": guard.current_response_id,
            "generation": guard.generation,
        })
        payload.update(self._sip_turn_cluster_payload(turn))
        payload.update(self._sip_single_short_payload(call_id, observation))
        self._append_event(call_id, "sip_pre_stop", "agent", payload)
        self._schedule_sip_clean_window_decision(call_id, turn, provider)

    def _is_sip_provider_confirmable_local_speech(
        self,
        call_id: str,
        observation: SipBargeInObservation,
    ) -> bool:
        if observation.candidate_class == "strong_short_speech_candidate":
            return True
        if observation.candidate_class != "stable_speech_candidate":
            return False
        if self._has_sip_single_short_pre_stop_evidence(call_id, observation):
            return True
        if observation.candidate_duration_ms < SIP_PROVIDER_CONFIRM_MIN_DURATION_MS:
            return False
        detector = self._sip_barge_in_detector
        return detector.has_pre_stop_local_speech(call_id) if detector is not None else False

    @staticmethod
    def _can_confirm_sip_barge_in_from_provider(turn: PendingUserTurn) -> bool:
        return turn.sip_pre_stop_requested and turn.sip_provider_speech_confirmable

    def _schedule_sip_clean_window_decision(
        self,
        call_id: str,
        turn: PendingUserTurn,
        provider: RealtimeProviderProtocol,
    ) -> None:
        self._cancel_sip_clean_window_task_nowait(call_id)
        decision_window_ms = min(
            self.sip_barge_in_config.clean_window_ms,
            self.sip_barge_in_config.max_hold_ms,
        )
        delay_seconds = max(0.0, decision_window_ms / 1000)
        self._sip_clean_window_tasks[call_id] = asyncio.create_task(
            self._decide_sip_clean_window_after(call_id, turn, provider, delay_seconds),
            name=f"ai-call-sip-clean-window-{call_id}",
        )

    async def _decide_sip_clean_window_after(
        self,
        call_id: str,
        turn: PendingUserTurn,
        provider: RealtimeProviderProtocol,
        delay_seconds: float,
    ) -> None:
        try:
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
            if self._pending_user_turns.get(call_id) is not turn:
                return
            if (
                not turn.sip_pre_stop_requested
                or turn.sip_barge_in_confirmed
                or turn.interrupt_confirmed
                or turn.sip_interrupt_rejected
            ):
                return

            detector = self._sip_barge_in_detector
            confirmable_local_speech = (
                detector.has_confirmable_local_speech(call_id) if detector else False
            )
            if (
                confirmable_local_speech
                and turn.sip_candidate_class == "stable_speech_candidate"
            ):
                await self._confirm_sip_clean_window(call_id, turn, provider)
                return
            remaining_hold_seconds = self._sip_protected_hold_remaining_seconds(turn)
            if remaining_hold_seconds > 0:
                self._sip_clean_window_tasks[call_id] = asyncio.create_task(
                    self._decide_sip_clean_window_after(
                        call_id,
                        turn,
                        provider,
                        remaining_hold_seconds,
                    ),
                    name=f"ai-call-sip-protected-hold-{call_id}",
                )
                return

            reason = "rejected_echo_or_tail" if detector is None else "rejected_noise"
            self._reject_sip_clean_window(call_id, turn, provider=provider, reason=reason)
        except asyncio.CancelledError:
            raise
        finally:
            if self._sip_clean_window_tasks.get(call_id) is asyncio.current_task():
                self._sip_clean_window_tasks.pop(call_id, None)

    async def _confirm_sip_clean_window(
        self,
        call_id: str,
        turn: PendingUserTurn,
        provider: RealtimeProviderProtocol,
    ) -> None:
        confirmed_at = datetime.now(timezone.utc)
        reason = SIP_BARGE_IN_INTERRUPT_REASON
        self._confirm_sip_barge_in(
            call_id,
            turn,
            confirmed_by="sip_clean_window",
            reason=reason,
        )
        payload = self._sip_clean_window_payload(call_id, turn, decision="confirmed", reason=reason)
        self._append_event(call_id, "sip_interrupt_confirmed", "agent", payload)
        await self._confirm_interrupt(
            call_id,
            provider,
            turn.interrupt_trigger_at or confirmed_at,
            reason=reason,
            clear_input_audio=False,
        )
        turn.interrupt_confirmed = True

    def _reject_sip_clean_window(
        self,
        call_id: str,
        turn: PendingUserTurn,
        *,
        provider: RealtimeProviderProtocol,
        reason: str,
    ) -> None:
        turn.sip_interrupt_rejected = True
        turn.sip_barge_in_requested = False
        turn.sip_barge_in_expires_at = None
        turn.sip_candidate_response_id = None
        turn.sip_candidate_generation = None
        guard = self._playback_guard(call_id)
        if reason == "rejected_noise":
            self._last_sip_rejected_noise_response_id[call_id] = guard.current_response_id
        # A rejected SIP pre-stop is a closed provisional turn. Mark it finished
        # so later local SIP speech can create a fresh candidate and pre-stop.
        turn.response_requested = True
        self._cancel_sip_barge_in_task_nowait(call_id)
        payload = self._sip_clean_window_payload(call_id, turn, decision="rejected", reason=reason)
        self._append_event(call_id, "sip_interrupt_rejected", "agent", payload)
        self._schedule_sip_recovery(call_id, turn, provider, reason=reason)

    def _schedule_sip_recovery(
        self,
        call_id: str,
        turn: PendingUserTurn,
        provider: RealtimeProviderProtocol,
        *,
        reason: str,
    ) -> None:
        if turn.sip_recovery_count >= self.sip_barge_in_recovery_max_per_turn:
            return
        self._cancel_sip_recovery_task_nowait(call_id)
        delay_seconds = max(0.0, self.sip_barge_in_recovery_silence_ms / 1000)
        self._sip_recovery_tasks[call_id] = asyncio.create_task(
            self._start_sip_recovery_after(call_id, turn, provider, reason, delay_seconds),
            name=f"ai-call-sip-recovery-{call_id}",
        )

    async def _start_sip_recovery_after(
        self,
        call_id: str,
        turn: PendingUserTurn,
        provider: RealtimeProviderProtocol,
        reason: str,
        delay_seconds: float,
    ) -> None:
        try:
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
            if self._pending_user_turns.get(call_id) is not turn:
                return
            if turn.interrupt_confirmed or not turn.sip_interrupt_rejected:
                return
            turn.sip_recovery_count += 1
            self._append_sip_recovery_started_event(call_id, turn, reason=reason)
            await self._request_response(call_id, provider, input_text=None)
        except asyncio.CancelledError:
            raise
        finally:
            if self._sip_recovery_tasks.get(call_id) is asyncio.current_task():
                self._sip_recovery_tasks.pop(call_id, None)

    def _append_sip_recovery_started_event(
        self,
        call_id: str,
        turn: PendingUserTurn,
        *,
        reason: str,
    ) -> None:
        guard = self._playback_guard(call_id)
        guard.suppress_audio_until = None
        self._append_event(
            call_id,
            "sip_recovery_started",
            "agent",
            {
                "reason": reason,
                "recoveryCount": turn.sip_recovery_count,
                "maxRecoveryCount": self.sip_barge_in_recovery_max_per_turn,
                "recoverySilenceMs": self.sip_barge_in_recovery_silence_ms,
                "generation": guard.generation,
                "responseId": guard.current_response_id,
            },
        )

    def _sip_protected_hold_remaining_seconds(self, turn: PendingUserTurn) -> float:
        if turn.sip_pre_stop_at is None:
            return 0.0
        elapsed_ms = (datetime.now(timezone.utc) - turn.sip_pre_stop_at).total_seconds() * 1000
        remaining_ms = self.sip_barge_in_config.max_hold_ms - elapsed_ms
        return max(0.0, remaining_ms / 1000)

    def _sip_clean_window_payload(
        self,
        call_id: str,
        turn: PendingUserTurn,
        *,
        decision: str,
        reason: str,
    ) -> dict[str, Any]:
        guard = self._playback_guard(call_id)
        now = datetime.now(timezone.utc)
        pre_stop_to_decision_ms = None
        if turn.sip_pre_stop_at is not None:
            pre_stop_to_decision_ms = round(
                max(0.0, (now - turn.sip_pre_stop_at).total_seconds() * 1000)
            )
        payload: dict[str, Any] = {
            "reason": reason,
            "decision": decision,
            "candidateClass": turn.sip_candidate_class,
            "cleanWindowMs": self.sip_barge_in_config.clean_window_ms,
            "preStopToDecisionMs": pre_stop_to_decision_ms,
            "responseId": guard.current_response_id,
            "generation": guard.generation,
        }
        if self._sip_barge_in_detector is not None:
            payload.update(self._sip_barge_in_detector.latest_observation_payload(call_id))
        return payload

    async def _consume_provider_events(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
    ) -> None:
        try:
            async for provider_event in provider.receive_events():
                event_payload = self._event_payload(provider_event.type, provider_event.payload)
                handler_event = provider_event
                if provider_event.type == "user_transcript_done":
                    trust_decision = self._decide_realtime_transcript_trust(
                        call_id,
                        provider_event,
                    )
                    trust_payload = trust_decision.as_payload()
                    event_payload.update(trust_payload)
                    handler_event = ProviderEvent(
                        type=provider_event.type,
                        payload={**provider_event.payload, **trust_payload},
                    )
                event_timestamp = self._append_event(
                    call_id,
                    provider_event.type,
                    "provider",
                    event_payload,
                )
                if handler_event.type == "user_speech_started":
                    await self._handle_user_speech_started(call_id, provider, event_timestamp)
                elif handler_event.type == "user_speech_stopped":
                    await self._handle_user_speech_stopped(call_id, provider, event_timestamp)
                elif handler_event.type in {"user_transcript_delta", "user_transcript_done"}:
                    await self._handle_user_transcript(
                        call_id,
                        provider,
                        handler_event,
                        event_timestamp,
                    )
                elif handler_event.type == "tool_call_done":
                    await self._handle_tool_call_done(call_id, provider, handler_event)
                else:
                    await self._apply_provider_event(
                        call_id,
                        provider,
                        handler_event.type,
                        event_timestamp,
                        handler_event.payload,
                    )
                if handler_event.type == "model_audio_delta":
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
        if not self._is_barge_in_enabled_for_session(session):
            return False
        decision = self._interrupt_policy.decide_speech_started(
            InterruptDecisionContext(
                source="browser",
                session_status=session.status,
                has_recent_ai_audio=self._has_recent_ai_audio(call_id, trigger_timestamp),
                has_active_model_response=self._has_active_model_response(call_id),
            )
        )
        if decision.action != "candidate":
            return False
        if session.status in {
            CallSessionStatus.CONNECTED,
            CallSessionStatus.AI_THINKING,
        }:
            # 浏览器侧 VAD 可能晚于服务端状态变更到达；近期 AI 音频或已创建但未出声的
            # response 都按可打断处理。
            self.registry.transition(call_id, CallSessionStatus.AI_SPEAKING)
            session = self.registry.get(call_id)
        if session.status != CallSessionStatus.AI_SPEAKING:
            return False

        turn = self._pending_turn(call_id, reset_if_finished=True)
        if turn.started_at is None:
            turn.started_at = trigger_timestamp
        candidate_created = self._mark_interrupt_candidate(
            call_id=call_id,
            turn=turn,
            trigger_timestamp=trigger_timestamp,
            source="browser",
            reason=decision.reason,
        )
        if candidate_created:
            self._append_event(
                call_id,
                "browser_interrupt_candidate_deferred",
                "agent",
                {"reason": decision.reason},
            )
        self._record_browser_candidate_observation(turn, trigger_timestamp)
        if self._should_promote_browser_candidate(turn):
            turn.browser_candidate_promoted = True
            promoted_reason = "repeated_browser_user_speech_started_during_ai_audio"
            self._append_event(
                call_id,
                "browser_interrupt_candidate_promoted",
                "agent",
                {
                    "reason": promoted_reason,
                    "candidateCount": turn.browser_candidate_count,
                    "windowSeconds": BROWSER_INTERRUPT_STRONG_CANDIDATE_WINDOW_SECONDS,
                },
            )
        return True

    async def record_browser_speech_segment(
        self,
        call_id: str,
        trigger_timestamp: datetime,
        payload: dict[str, Any],
    ) -> bool:
        if call_id not in self._providers:
            return False
        session = self.registry.get(call_id)
        if session.status in {
            CallSessionStatus.COMPLETED,
            CallSessionStatus.FAILED,
        }:
            return False
        if not self._is_barge_in_enabled_for_session(session):
            return False

        context = self._browser_segment_decision_context(
            call_id=call_id,
            session=session,
            trigger_timestamp=trigger_timestamp,
            payload=payload,
        )
        decision = self._interrupt_policy.decide_browser_speech_segment(context)
        if decision.action != "candidate":
            return False
        if session.status in {
            CallSessionStatus.CONNECTED,
            CallSessionStatus.AI_THINKING,
        }:
            self.registry.transition(call_id, CallSessionStatus.AI_SPEAKING)
            session = self.registry.get(call_id)
        if session.status != CallSessionStatus.AI_SPEAKING:
            return False

        turn = self._pending_turn(call_id, reset_if_finished=True)
        if turn.started_at is None:
            turn.started_at = trigger_timestamp
        self._record_browser_segment_evidence(turn, payload)
        candidate_created = self._mark_interrupt_candidate(
            call_id=call_id,
            turn=turn,
            trigger_timestamp=trigger_timestamp,
            source="browser",
            reason=decision.reason,
        )
        if candidate_created:
            self._append_event(
                call_id,
                "browser_interrupt_candidate_deferred",
                "agent",
                self._browser_segment_event_payload(decision.reason, payload),
            )
        if (
            decision.reason == "browser_speech_segment_strong_during_ai_audio"
            and not turn.browser_candidate_promoted
        ):
            turn.browser_candidate_promoted = True
            self._append_event(
                call_id,
                "browser_interrupt_candidate_promoted",
                "agent",
                self._browser_segment_event_payload(decision.reason, payload),
            )
        if turn.browser_pre_stop_requested and not turn.browser_pre_stop_confirmed:
            self._extend_browser_pre_stop(call_id, turn, trigger_timestamp)
        elif decision.reason == "browser_speech_segment_strong_during_ai_audio":
            await self._maybe_pre_stop_browser_candidate(
                call_id=call_id,
                turn=turn,
                trigger_timestamp=trigger_timestamp,
                payload=payload,
            )
        if not turn.browser_pre_stop_requested and not turn.browser_pre_stop_confirmed:
            await self._maybe_audio_hold_browser_candidate(
                call_id=call_id,
                turn=turn,
                trigger_timestamp=trigger_timestamp,
                payload=payload,
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
            if session.status == CallSessionStatus.READY:
                session = self.registry.transition(call_id, CallSessionStatus.CONNECTED)
            self._mark_response_started(call_id, payload)
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
            if not self._ignore_provider_cancel_race_error(call_id, payload, timestamp):
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
        if session.status == CallSessionStatus.READY:
            session = self.registry.transition(call_id, CallSessionStatus.CONNECTED)
        if not self._is_barge_in_enabled_for_session(session):
            await self._apply_provider_event(
                call_id,
                provider,
                "user_speech_started",
                timestamp,
                {},
            )
            return
        if (
            self.sip_barge_in_fast_stop_enabled
            and self._is_sip_participant(session)
            and self._is_within_sip_rejected_noise_same_response_guard(call_id)
        ):
            self._append_event(
                call_id,
                "sip_provider_speech_started_deferred",
                "agent",
                {
                    "reason": "rejected_noise_tail_guard",
                    "confirmedBy": "provider_speech_started",
                    "preStopRequested": False,
                    "providerConfirmable": False,
                },
            )
            return
        self._playback_guard(call_id).user_speech_active = True
        turn = self._pending_turn(call_id, reset_if_finished=True)
        self._cancel_turn_response_task_nowait(call_id)
        if turn.stopped_at is not None and not turn.response_requested:
            turn.stopped_at = None
        turn.started_at = timestamp
        decision = self._interrupt_policy.decide_speech_started(
            InterruptDecisionContext(
                source="provider",
                session_status=session.status,
                has_recent_ai_audio=self._has_recent_ai_audio(call_id, timestamp),
                has_active_model_response=self._has_active_model_response(call_id),
                has_interrupt_candidate=turn.interrupt_candidate,
                candidate_reason=turn.interrupt_reason if turn.interrupt_candidate else None,
                candidate_stale=self._is_stale_browser_interrupt_candidate(turn, timestamp),
            )
        )
        if decision.action == "ignore" and decision.reason == "browser_candidate_expired":
            can_upgrade_current_provider_speech = (
                self._is_recent_enough_to_upgrade_from_browser_candidate(turn, timestamp)
                and (
                    session.status == CallSessionStatus.AI_SPEAKING
                    or self._has_recent_ai_audio(call_id, timestamp)
                    or self._has_active_model_response(call_id)
                )
            )
            self._ignore_empty_turn(call_id, turn, decision.reason)
            turn = PendingUserTurn(started_at=timestamp)
            self._pending_user_turns[call_id] = turn
            if can_upgrade_current_provider_speech:
                decision = self._interrupt_policy.decide_speech_started(
                    InterruptDecisionContext(
                        source="provider",
                        session_status=session.status,
                        has_recent_ai_audio=self._has_recent_ai_audio(call_id, timestamp),
                        has_active_model_response=self._has_active_model_response(call_id),
                    )
                )
            else:
                self._restore_after_ignored_interrupt_candidate(call_id)
                return

        if decision.action == "stop_only" and session.status in {
            CallSessionStatus.CONNECTED,
            CallSessionStatus.AI_THINKING,
        }:
            self.registry.transition(call_id, CallSessionStatus.AI_SPEAKING)
            session = self.registry.get(call_id)

        if decision.action == "ignore" and session.status == CallSessionStatus.CONNECTED:
            self.registry.transition(call_id, CallSessionStatus.USER_SPEAKING)
            return

        if decision.action != "stop_only" or session.status != CallSessionStatus.AI_SPEAKING:
            return

        self._mark_interrupt_candidate(
            call_id=call_id,
            turn=turn,
            trigger_timestamp=timestamp,
            source="provider",
            reason=decision.reason,
        )
        if (
            self.sip_barge_in_fast_stop_enabled
            and turn.sip_barge_in_requested
            and not self._can_confirm_sip_barge_in_from_provider(turn)
        ):
            self._append_event(
                call_id,
                "sip_provider_speech_started_deferred",
                "agent",
                {
                    "reason": "awaiting_turn_taking_evidence",
                    "confirmedBy": "provider_speech_started",
                    "candidateClass": turn.sip_candidate_class,
                    "preStopRequested": turn.sip_pre_stop_requested,
                    "providerConfirmable": turn.sip_provider_speech_confirmable,
                },
            )
            return
        self._confirm_sip_barge_in(
            call_id,
            turn,
            confirmed_by="provider_speech_started",
            reason=decision.reason,
        )
        self._confirm_browser_audio_hold(
            call_id,
            turn,
            confirmed_by="provider_speech_started",
            reason=decision.reason,
        )
        self._confirm_browser_pre_stop(
            call_id,
            turn,
            confirmed_by="provider_speech_started",
            reason=decision.reason,
        )
        guard = self._playback_guard(call_id)
        if not guard.cancel_requested:
            await self._invalidate_audio_for_interrupt_candidate(
                call_id=call_id,
                provider=provider,
                trigger_timestamp=timestamp,
                source="provider",
                reason=decision.reason,
            )
        await self._maybe_confirm_interrupt_from_turn(call_id, provider, timestamp)

    async def _handle_user_speech_stopped(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        timestamp: datetime,
    ) -> None:
        if self._is_sip_participant(self.registry.get(call_id)):
            self._last_sip_provider_speech_stopped_at[call_id] = timestamp
        self._playback_guard(call_id).user_speech_active = False
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
        if is_realtime_transcript_semantically_rejected(provider_event.payload):
            self._append_event(
                call_id,
                "user_transcript_semantic_rejected",
                "agent",
                {
                    "reason": str(
                        provider_event.payload.get("semanticRejectReason")
                        or "low_confidence_transcript"
                    ),
                    "transcriptPreview": self._text_preview(text),
                    "transcriptTrust": provider_event.payload.get("transcriptTrust"),
                    "semanticAction": provider_event.payload.get("semanticAction"),
                    "commitDecision": provider_event.payload.get("commitDecision"),
                },
            )
            return
        call_end_decision: CallEndDecision | None = None
        if provider_event.type == "user_transcript_done":
            call_end_decision = self.call_end_decision_service.decide(text)
        if call_end_decision is not None and call_end_decision.action == "explicit_end":
            self._record_call_end_intent(call_id, text, call_end_decision)
        else:
            self._pending_call_end_intents.pop(call_id, None)
            self._interrupt_pending_call_end(call_id, "user_transcript_after_call_end_tool")
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

    def _record_call_end_intent(
        self,
        call_id: str,
        transcript: str,
        decision: CallEndDecision,
    ) -> None:
        self._pending_call_end_intents[call_id] = PendingCallEndIntent(
            transcript=transcript,
            reason=decision.reason,
            summary=decision.summary,
            source=decision.source,
            confidence=decision.confidence,
        )
        self._append_event(
            call_id,
            "call_end_intent_detected",
            "agent",
            {
                "reason": decision.reason,
                "summary": decision.summary,
                "confidence": decision.confidence,
                "classifierSource": decision.source,
                "transcriptPreview": self._text_preview(transcript),
            },
        )

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

        self._pending_call_end_intents.pop(call_id, None)
        final_audio_already_spoken = self._has_current_response_audio(call_id)
        if call_id not in self._pending_call_ends:
            self._pending_call_ends[call_id] = PendingCallEnd(
                tool_call_id=tool_call_id,
                tool_reason=tool_reason,
                end_reason=end_reason,
                final_response_started=final_audio_already_spoken,
            )
            self._append_event(
                call_id,
                "call_end_tool_requested",
                "agent",
                {
                    "toolCallId": tool_call_id,
                    "toolReason": tool_reason,
                    "endReason": end_reason,
                    "finalAudioAlreadySpoken": final_audio_already_spoken,
                },
            )
        pending_call_end = self._pending_call_ends[call_id]
        should_create_final_response = not pending_call_end.final_response_started

        try:
            await provider.submit_tool_result(
                tool_call_id,
                (
                    CALL_END_FINAL_RESPONSE_TOOL_RESULT
                    if should_create_final_response
                    else CALL_END_NO_EXTRA_RESPONSE_TOOL_RESULT
                ),
            )
            if should_create_final_response:
                self._queue_response_create(call_id)
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

    def _interrupt_pending_call_end(self, call_id: str, reason: str) -> None:
        pending_call_end = self._pending_call_ends.pop(call_id, None)
        if pending_call_end is None or pending_call_end.scheduled:
            return
        self._append_event(
            call_id,
            "call_end_interrupted",
            "agent",
            {
                "reason": reason,
                "toolCallId": pending_call_end.tool_call_id,
                "toolReason": pending_call_end.tool_reason,
                "endReason": pending_call_end.end_reason,
            },
        )

    async def _handle_handoff_tool_done(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
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
        if reason != "business_escalation":
            return

        try:
            await provider.submit_tool_result(
                tool_call_id,
                BUSINESS_HANDOFF_CONFIRMATION_TOOL_RESULT,
            )
            self._queue_response_create(call_id)
        except Exception as exc:
            self._append_event(
                call_id,
                "agent_error",
                "agent",
                {
                    "message": f"提交转人工确认工具结果失败: {exc}",
                    "toolCallId": tool_call_id,
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
    ) -> bool:
        candidate_created = not turn.interrupt_candidate
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
        return candidate_created

    def _record_browser_candidate_observation(
        self,
        turn: PendingUserTurn,
        trigger_timestamp: datetime,
    ) -> None:
        first_at = turn.browser_candidate_first_at
        if first_at is None:
            turn.browser_candidate_first_at = trigger_timestamp
            turn.browser_candidate_count = 1
            return
        elapsed_seconds = (trigger_timestamp - first_at).total_seconds()
        if elapsed_seconds < 0 or elapsed_seconds > BROWSER_INTERRUPT_STRONG_CANDIDATE_WINDOW_SECONDS:
            turn.browser_candidate_first_at = trigger_timestamp
            turn.browser_candidate_count = 1
            return
        turn.browser_candidate_count += 1

    @staticmethod
    def _should_promote_browser_candidate(turn: PendingUserTurn) -> bool:
        return (
            not turn.browser_candidate_promoted
            and turn.browser_candidate_count >= BROWSER_INTERRUPT_STRONG_CANDIDATE_COUNT
        )

    @staticmethod
    def _browser_segment_event_payload(reason: str, payload: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {"reason": reason}
        for key in (
            "segmentId",
            "phase",
            "durationMs",
            "rmsDbfs",
            "noiseFloorDbfs",
            "snrDb",
            "hotFrameCount",
            "remoteAudioActive",
            "remoteAudioRmsDbfs",
        ):
            if key in payload:
                result[key] = payload[key]
        return result

    def _record_browser_segment_evidence(
        self,
        turn: PendingUserTurn,
        payload: dict[str, Any],
    ) -> None:
        duration_ms = self._payload_int(payload, "durationMs")
        current_duration_ms = turn.browser_segment_duration_ms or -1
        if duration_ms is not None and duration_ms < current_duration_ms:
            return
        turn.browser_segment_phase = self._payload_str(payload, "phase")
        turn.browser_segment_duration_ms = duration_ms
        turn.browser_segment_snr_db = self._payload_float(payload, "snrDb")
        turn.browser_segment_hot_frame_count = self._payload_int(payload, "hotFrameCount")
        turn.browser_segment_rms_dbfs = self._payload_float(payload, "rmsDbfs")
        turn.browser_segment_remote_audio_active = self._payload_bool(
            payload,
            "remoteAudioActive",
        )
        turn.browser_segment_remote_audio_rms_dbfs = self._payload_float(
            payload,
            "remoteAudioRmsDbfs",
        )

    def _browser_segment_decision_context(
        self,
        *,
        call_id: str,
        session: CallSession,
        trigger_timestamp: datetime,
        payload: dict[str, Any],
    ) -> InterruptDecisionContext:
        return InterruptDecisionContext(
            source="browser",
            session_status=session.status,
            has_recent_ai_audio=self._has_recent_ai_audio(call_id, trigger_timestamp),
            has_active_model_response=self._has_active_model_response(call_id),
            browser_segment_phase=self._payload_str(payload, "phase"),
            browser_segment_duration_ms=self._payload_int(payload, "durationMs"),
            browser_segment_snr_db=self._payload_float(payload, "snrDb"),
            browser_segment_hot_frame_count=self._payload_int(payload, "hotFrameCount"),
            browser_segment_rms_dbfs=self._payload_float(payload, "rmsDbfs"),
            browser_segment_remote_audio_active=self._payload_bool(payload, "remoteAudioActive"),
            browser_segment_remote_audio_rms_dbfs=self._payload_float(
                payload,
                "remoteAudioRmsDbfs",
            ),
        )

    async def _maybe_pre_stop_browser_candidate(
        self,
        *,
        call_id: str,
        turn: PendingUserTurn,
        trigger_timestamp: datetime,
        payload: dict[str, Any],
    ) -> None:
        if turn.browser_pre_stop_requested or turn.browser_pre_stop_confirmed:
            return
        decision = self._interrupt_policy.decide_browser_pre_stop(
            self._browser_segment_decision_context(
                call_id=call_id,
                session=self.registry.get(call_id),
                trigger_timestamp=trigger_timestamp,
                payload=payload,
            )
        )
        if decision.action != "pre_stop":
            if decision.reason == "remote_audio_dominates":
                self._append_event(
                    call_id,
                    "browser_pre_stop_rejected_echo",
                    "agent",
                    {
                        **self._browser_segment_event_payload(
                            "browser_pre_stop_rejected_echo",
                            payload,
                        ),
                        "rejectionReason": decision.reason,
                    },
                )
                return
            self._append_event(
                call_id,
                "browser_pre_stop_skipped",
                "agent",
                self._browser_pre_stop_skip_payload(payload, skip_reason=decision.reason),
            )
            return

        turn.browser_pre_stop_requested = True
        self._extend_browser_pre_stop(call_id, turn, trigger_timestamp)
        guard = self._playback_guard(call_id)
        event_payload = self._browser_segment_event_payload(decision.reason, payload)
        event_payload.update({
            "responseId": guard.current_response_id,
            "timeoutSeconds": self.browser_pre_stop_timeout_seconds,
        })
        self._append_event(call_id, "browser_pre_stop_requested", "agent", event_payload)

        await self._cancel_playout_task(call_id)
        stop_audio_succeeded = self.audio_publisher is None
        cleanup_errors: list[dict[str, str]] = []
        if self.audio_publisher is not None:
            try:
                await self.audio_publisher.stop_audio(call_id)
                stop_audio_succeeded = True
                guard.audio_stop_requested = True
            except Exception as exc:
                cleanup_errors.append({
                    "step": "stop_audio",
                    "errorType": type(exc).__name__,
                    "message": str(exc),
                })

        completed_payload = dict(event_payload)
        completed_payload["stopAudioSucceeded"] = stop_audio_succeeded
        self._append_event(call_id, "browser_pre_stop_completed", "agent", completed_payload)
        for cleanup_error in cleanup_errors:
            self._append_event(call_id, "interrupt_cleanup_failed", "agent", cleanup_error)

    async def _maybe_audio_hold_browser_candidate(
        self,
        *,
        call_id: str,
        turn: PendingUserTurn,
        trigger_timestamp: datetime,
        payload: dict[str, Any],
    ) -> None:
        if turn.browser_audio_hold_requested or turn.browser_audio_hold_confirmed:
            return
        decision = self._interrupt_policy.decide_browser_audio_hold(
            self._browser_segment_decision_context(
                call_id=call_id,
                session=self.registry.get(call_id),
                trigger_timestamp=trigger_timestamp,
                payload=payload,
            )
        )
        if decision.action != "hold_audio":
            if decision.reason == "remote_audio_dominates":
                self._append_event(
                    call_id,
                    "browser_audio_hold_rejected_echo",
                    "agent",
                    {
                        **self._browser_segment_event_payload(
                            "browser_audio_hold_rejected_echo",
                            payload,
                        ),
                        "rejectionReason": decision.reason,
                    },
                )
            return

        turn.browser_audio_hold_requested = True
        self._extend_browser_audio_hold(call_id, turn, trigger_timestamp)
        guard = self._playback_guard(call_id)
        event_payload = self._browser_segment_event_payload(decision.reason, payload)
        event_payload.update({
            "responseId": guard.current_response_id,
            "timeoutSeconds": self.browser_audio_hold_timeout_seconds,
        })
        self._append_event(call_id, "browser_audio_hold_requested", "agent", event_payload)

        await self._cancel_playout_task(call_id)
        stop_audio_succeeded = self.audio_publisher is None
        cleanup_errors: list[dict[str, str]] = []
        if self.audio_publisher is not None:
            try:
                await self.audio_publisher.stop_audio(call_id)
                stop_audio_succeeded = True
                guard.audio_stop_requested = True
            except Exception as exc:
                cleanup_errors.append({
                    "step": "stop_audio",
                    "errorType": type(exc).__name__,
                    "message": str(exc),
                })

        completed_payload = dict(event_payload)
        completed_payload["stopAudioSucceeded"] = stop_audio_succeeded
        self._append_event(call_id, "browser_audio_hold_completed", "agent", completed_payload)
        for cleanup_error in cleanup_errors:
            self._append_event(call_id, "interrupt_cleanup_failed", "agent", cleanup_error)

    def _extend_browser_audio_hold(
        self,
        call_id: str,
        turn: PendingUserTurn,
        trigger_timestamp: datetime,
    ) -> None:
        timeout = self.browser_audio_hold_timeout_seconds
        turn.browser_audio_hold_expires_at = trigger_timestamp + timedelta(seconds=timeout)
        guard = self._playback_guard(call_id)
        suppress_until = datetime.now(timezone.utc) + timedelta(seconds=timeout)
        if guard.suppress_audio_until is None or suppress_until > guard.suppress_audio_until:
            guard.suppress_audio_until = suppress_until
        self._schedule_browser_audio_hold_expiry(call_id, turn)

    def _confirm_browser_audio_hold(
        self,
        call_id: str,
        turn: PendingUserTurn,
        *,
        confirmed_by: str,
        reason: str,
    ) -> None:
        if not turn.browser_audio_hold_requested or turn.browser_audio_hold_confirmed:
            return
        turn.browser_audio_hold_confirmed = True
        self._cancel_browser_audio_hold_task_nowait(call_id)
        self._append_event(
            call_id,
            "browser_audio_hold_confirmed",
            "agent",
            {
                "confirmedBy": confirmed_by,
                "reason": reason,
                "expiresAt": (
                    turn.browser_audio_hold_expires_at.isoformat()
                    if turn.browser_audio_hold_expires_at is not None
                    else None
                ),
            },
        )

    def _extend_browser_pre_stop(
        self,
        call_id: str,
        turn: PendingUserTurn,
        trigger_timestamp: datetime,
    ) -> None:
        timeout = self.browser_pre_stop_timeout_seconds
        turn.browser_pre_stop_expires_at = trigger_timestamp + timedelta(seconds=timeout)
        guard = self._playback_guard(call_id)
        suppress_until = datetime.now(timezone.utc) + timedelta(seconds=timeout)
        if guard.suppress_audio_until is None or suppress_until > guard.suppress_audio_until:
            guard.suppress_audio_until = suppress_until
        self._schedule_browser_pre_stop_expiry(call_id, turn)

    def _browser_pre_stop_skip_payload(
        self,
        payload: dict[str, Any],
        *,
        skip_reason: str | None = None,
    ) -> dict[str, Any]:
        event_payload = self._browser_segment_event_payload(
            "browser_pre_stop_not_eligible",
            payload,
        )
        event_payload.update({
            "skipReason": skip_reason or self._browser_pre_stop_skip_reason(payload),
            "minDurationMs": BROWSER_PRE_STOP_MIN_DURATION_MS,
            "minSnrDb": BROWSER_PRE_STOP_MIN_SNR_DB,
            "minHotFrameCount": BROWSER_PRE_STOP_MIN_HOT_FRAMES,
            "minRmsDbfs": BROWSER_PRE_STOP_MIN_RMS_DBFS,
        })
        return event_payload

    def _browser_pre_stop_skip_reason(self, payload: dict[str, Any]) -> str:
        phase = self._payload_str(payload, "phase")
        duration_ms = self._payload_int(payload, "durationMs") or 0
        snr_db = self._payload_float(payload, "snrDb") or 0.0
        hot_frames = self._payload_int(payload, "hotFrameCount") or 0
        rms_dbfs = self._payload_float(payload, "rmsDbfs")
        if phase not in {"updated", "ended"}:
            return "phase_not_final_enough"
        if duration_ms < BROWSER_PRE_STOP_MIN_DURATION_MS:
            return "below_min_duration"
        if snr_db < BROWSER_PRE_STOP_MIN_SNR_DB:
            return "below_min_snr"
        if hot_frames < BROWSER_PRE_STOP_MIN_HOT_FRAMES:
            return "below_min_hot_frames"
        if rms_dbfs is None:
            return "missing_rms"
        if rms_dbfs < BROWSER_PRE_STOP_MIN_RMS_DBFS:
            return "below_min_rms"
        return "not_eligible"

    def _confirm_browser_pre_stop(
        self,
        call_id: str,
        turn: PendingUserTurn,
        *,
        confirmed_by: str,
        reason: str,
    ) -> None:
        if not turn.browser_pre_stop_requested or turn.browser_pre_stop_confirmed:
            return
        turn.browser_pre_stop_confirmed = True
        self._cancel_browser_pre_stop_task_nowait(call_id)
        self._append_event(
            call_id,
            "browser_pre_stop_confirmed",
            "agent",
            {
                "confirmedBy": confirmed_by,
                "reason": reason,
                "expiresAt": (
                    turn.browser_pre_stop_expires_at.isoformat()
                    if turn.browser_pre_stop_expires_at is not None
                    else None
                ),
            },
        )

    def _extend_sip_barge_in(
        self,
        call_id: str,
        turn: PendingUserTurn,
        trigger_timestamp: datetime,
    ) -> None:
        timeout = self.sip_barge_in_hold_timeout_seconds
        turn.sip_barge_in_expires_at = trigger_timestamp + timedelta(seconds=timeout)
        self._schedule_sip_barge_in_expiry(call_id, turn)

    def _confirm_sip_barge_in(
        self,
        call_id: str,
        turn: PendingUserTurn,
        *,
        confirmed_by: str,
        reason: str,
    ) -> None:
        if not turn.sip_barge_in_requested or turn.sip_barge_in_confirmed:
            return
        turn.sip_barge_in_confirmed = True
        turn.sip_barge_in_confirmed_by = confirmed_by
        self._cancel_sip_barge_in_task_nowait(call_id)
        self._append_event(
            call_id,
            "sip_interrupt_candidate_confirmed",
            "agent",
            {
                "confirmedBy": confirmed_by,
                "reason": reason,
                "expiresAt": (
                    turn.sip_barge_in_expires_at.isoformat()
                    if turn.sip_barge_in_expires_at is not None
                    else None
                ),
            },
        )

    def _schedule_sip_barge_in_expiry(
        self,
        call_id: str,
        turn: PendingUserTurn,
    ) -> None:
        expires_at = turn.sip_barge_in_expires_at
        if expires_at is None:
            return
        self._cancel_sip_barge_in_task_nowait(call_id)
        self._sip_barge_in_tasks[call_id] = asyncio.create_task(
            self._expire_sip_barge_in_at(call_id, turn, expires_at),
            name=f"ai-call-sip-barge-in-expiry-{call_id}",
        )

    async def _expire_sip_barge_in_at(
        self,
        call_id: str,
        turn: PendingUserTurn,
        expires_at: datetime,
    ) -> None:
        try:
            delay = max(0.0, (expires_at - datetime.now(timezone.utc)).total_seconds())
            if delay > 0:
                await asyncio.sleep(delay)
            if self._pending_user_turns.get(call_id) is not turn:
                return
            if (
                not turn.sip_barge_in_requested
                or turn.sip_barge_in_confirmed
                or turn.interrupt_confirmed
                or turn.sip_pre_stop_requested
                or turn.sip_barge_in_expires_at != expires_at
            ):
                return
            self._append_event(
                call_id,
                "sip_interrupt_candidate_expired",
                "agent",
                {
                    "reason": "sip_barge_in_expired",
                    "expiresAt": expires_at.isoformat(),
                },
            )
            guard = self._playback_guard(call_id)
            turn.sip_barge_in_requested = False
            turn.sip_barge_in_confirmed = False
            turn.sip_barge_in_confirmed_by = None
            turn.sip_barge_in_expires_at = None
            turn.sip_pre_stop_deferred = False
            turn.sip_candidate_class = None
            turn.sip_candidate_response_id = None
            turn.sip_candidate_generation = None
            turn.sip_provider_speech_confirmable = False
            turn.started_at = None
            turn.interrupt_trigger_at = None
            self._reset_sip_turn_cluster(turn, response_id=guard.current_response_id)
            if self._sip_barge_in_detector is not None:
                self._sip_barge_in_detector.reset_activity(call_id)
            guard.user_speech_active = False
            if guard.suppress_audio_until is not None and datetime.now(timezone.utc) >= (
                guard.suppress_audio_until
            ):
                guard.suppress_audio_until = None
            guard.audio_stop_requested = False
            self._ignore_empty_turn(call_id, turn, "sip_barge_in_expired")
            self._restore_after_expired_sip_barge_in(call_id)
        except asyncio.CancelledError:
            raise
        finally:
            if self._sip_barge_in_tasks.get(call_id) is asyncio.current_task():
                self._sip_barge_in_tasks.pop(call_id, None)

    def _restore_after_expired_sip_barge_in(self, call_id: str) -> None:
        if self._has_active_model_response(call_id):
            return
        session = self.registry.get(call_id)
        if session.status == CallSessionStatus.AI_SPEAKING:
            self.registry.transition(call_id, CallSessionStatus.CONNECTED)
        elif session.status == CallSessionStatus.INTERRUPTED:
            self.registry.transition(call_id, CallSessionStatus.WAITING)
            self.registry.transition(call_id, CallSessionStatus.CONNECTED)

    def _schedule_browser_pre_stop_expiry(
        self,
        call_id: str,
        turn: PendingUserTurn,
    ) -> None:
        expires_at = turn.browser_pre_stop_expires_at
        if expires_at is None:
            return
        self._cancel_browser_pre_stop_task_nowait(call_id)
        self._browser_pre_stop_tasks[call_id] = asyncio.create_task(
            self._expire_browser_pre_stop_at(call_id, turn, expires_at),
            name=f"ai-call-browser-pre-stop-expiry-{call_id}",
        )

    def _schedule_browser_audio_hold_expiry(
        self,
        call_id: str,
        turn: PendingUserTurn,
    ) -> None:
        expires_at = turn.browser_audio_hold_expires_at
        if expires_at is None:
            return
        self._cancel_browser_audio_hold_task_nowait(call_id)
        self._browser_audio_hold_tasks[call_id] = asyncio.create_task(
            self._expire_browser_audio_hold_at(call_id, turn, expires_at),
            name=f"ai-call-browser-audio-hold-expiry-{call_id}",
        )

    async def _expire_browser_audio_hold_at(
        self,
        call_id: str,
        turn: PendingUserTurn,
        expires_at: datetime,
    ) -> None:
        try:
            delay = max(0.0, (expires_at - datetime.now(timezone.utc)).total_seconds())
            if delay > 0:
                await asyncio.sleep(delay)
            if self._pending_user_turns.get(call_id) is not turn:
                return
            if (
                not turn.browser_audio_hold_requested
                or turn.browser_audio_hold_confirmed
                or turn.browser_pre_stop_requested
                or turn.interrupt_confirmed
                or turn.browser_audio_hold_expires_at != expires_at
            ):
                return
            self._append_event(
                call_id,
                "browser_audio_hold_expired",
                "agent",
                {
                    "reason": "browser_audio_hold_expired",
                    "expiresAt": expires_at.isoformat(),
                },
            )
            turn.browser_audio_hold_requested = False
            guard = self._playback_guard(call_id)
            if guard.suppress_audio_until is not None and datetime.now(timezone.utc) >= (
                guard.suppress_audio_until
            ):
                guard.suppress_audio_until = None
            guard.audio_stop_requested = False
            self._ignore_empty_turn(call_id, turn, "browser_audio_hold_expired")
            self._restore_after_expired_browser_audio_hold(call_id)
        except asyncio.CancelledError:
            raise
        finally:
            if self._browser_audio_hold_tasks.get(call_id) is asyncio.current_task():
                self._browser_audio_hold_tasks.pop(call_id, None)

    def _restore_after_expired_browser_audio_hold(self, call_id: str) -> None:
        if self._has_active_model_response(call_id):
            return
        session = self.registry.get(call_id)
        if session.status == CallSessionStatus.AI_SPEAKING:
            self.registry.transition(call_id, CallSessionStatus.CONNECTED)
        elif session.status == CallSessionStatus.INTERRUPTED:
            self.registry.transition(call_id, CallSessionStatus.WAITING)
            self.registry.transition(call_id, CallSessionStatus.CONNECTED)
        self._schedule_pending_call_end_nowait(call_id)

    async def _expire_browser_pre_stop_at(
        self,
        call_id: str,
        turn: PendingUserTurn,
        expires_at: datetime,
    ) -> None:
        try:
            delay = max(0.0, (expires_at - datetime.now(timezone.utc)).total_seconds())
            if delay > 0:
                await asyncio.sleep(delay)
            if self._pending_user_turns.get(call_id) is not turn:
                return
            if (
                not turn.browser_pre_stop_requested
                or turn.browser_pre_stop_confirmed
                or turn.interrupt_confirmed
                or turn.browser_pre_stop_expires_at != expires_at
            ):
                return
            self._append_event(
                call_id,
                "browser_pre_stop_expired",
                "agent",
                {
                    "reason": "browser_pre_stop_expired",
                    "expiresAt": expires_at.isoformat(),
                },
            )
            turn.browser_pre_stop_requested = False
            guard = self._playback_guard(call_id)
            if guard.suppress_audio_until is not None and datetime.now(timezone.utc) >= (
                guard.suppress_audio_until
            ):
                guard.suppress_audio_until = None
            guard.audio_stop_requested = False
            self._ignore_empty_turn(call_id, turn, "browser_pre_stop_expired")
        except asyncio.CancelledError:
            raise
        finally:
            if self._browser_pre_stop_tasks.get(call_id) is asyncio.current_task():
                self._browser_pre_stop_tasks.pop(call_id, None)

    def _cancel_browser_audio_hold_task_nowait(self, call_id: str) -> None:
        task = self._browser_audio_hold_tasks.pop(call_id, None)
        if task is not None and not task.done():
            task.cancel()

    async def _cancel_browser_audio_hold_task(self, call_id: str) -> None:
        task = self._browser_audio_hold_tasks.pop(call_id, None)
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    def _cancel_browser_pre_stop_task_nowait(self, call_id: str) -> None:
        task = self._browser_pre_stop_tasks.pop(call_id, None)
        if task is not None and not task.done():
            task.cancel()

    async def _cancel_browser_pre_stop_task(self, call_id: str) -> None:
        task = self._browser_pre_stop_tasks.pop(call_id, None)
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    def _cancel_sip_barge_in_task_nowait(self, call_id: str) -> None:
        task = self._sip_barge_in_tasks.pop(call_id, None)
        if task is not None and not task.done():
            task.cancel()

    async def _cancel_sip_barge_in_task(self, call_id: str) -> None:
        task = self._sip_barge_in_tasks.pop(call_id, None)
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    def _cancel_sip_clean_window_task_nowait(self, call_id: str) -> None:
        task = self._sip_clean_window_tasks.pop(call_id, None)
        if task is not None and not task.done():
            task.cancel()

    async def _cancel_sip_clean_window_task(self, call_id: str) -> None:
        task = self._sip_clean_window_tasks.pop(call_id, None)
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    def _cancel_sip_recovery_task_nowait(self, call_id: str) -> None:
        task = self._sip_recovery_tasks.pop(call_id, None)
        if task is not None and not task.done():
            task.cancel()

    async def _cancel_sip_recovery_task(self, call_id: str) -> None:
        task = self._sip_recovery_tasks.pop(call_id, None)
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @staticmethod
    def _payload_str(payload: dict[str, Any], key: str) -> str | None:
        value = payload.get(key)
        return value if isinstance(value, str) else None

    @staticmethod
    def _payload_int(payload: dict[str, Any], key: str) -> int | None:
        value = payload.get(key)
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return round(value)
        if isinstance(value, str):
            try:
                return round(float(value))
            except ValueError:
                return None
        return None

    @staticmethod
    def _payload_float(payload: dict[str, Any], key: str) -> float | None:
        value = payload.get(key)
        if isinstance(value, bool):
            return None
        if isinstance(value, int | float):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                return None
        return None

    @staticmethod
    def _payload_bool(payload: dict[str, Any], key: str) -> bool | None:
        value = payload.get(key)
        return value if isinstance(value, bool) else None

    async def _invalidate_audio_for_interrupt_candidate(
        self,
        *,
        call_id: str,
        provider: RealtimeProviderProtocol,
        trigger_timestamp: datetime,
        source: str,
        reason: str,
        cancel_provider_response: bool = True,
    ) -> None:
        guard = self._playback_guard(call_id)
        previous_generation = guard.generation
        guard.generation += 1
        guard.suppress_audio_until = datetime.now(timezone.utc) + timedelta(
            seconds=self.browser_interrupt_audio_suppress_seconds
        )
        guard.awaiting_response_start_after_interrupt = True
        if guard.current_response_id:
            guard.cancelled_response_ids.add(guard.current_response_id)

        self._append_event(
            call_id,
            "response_generation_invalidated",
            "agent",
            {
                "source": source,
                "reason": reason,
                "triggeredAt": trigger_timestamp.isoformat(),
                "previousGeneration": previous_generation,
                "generation": guard.generation,
                "responseId": guard.current_response_id,
            },
        )
        self._append_event(
            call_id,
            "interrupt_audio_stop_requested",
            "agent",
            {
                "source": source,
                "reason": reason,
                "generation": guard.generation,
                "responseId": guard.current_response_id,
            },
        )

        await self._cancel_playout_task(call_id)
        cleanup_errors = await self._stop_audio_playout_queue(
            call_id,
            source=source,
            reason=reason,
        )

        lifecycle = self._response_lifecycle(call_id)
        if cancel_provider_response and not guard.cancel_requested and not lifecycle.cancel_pending:
            should_wait_for_response_done = lifecycle.active
            try:
                if should_wait_for_response_done:
                    lifecycle.cancel_pending = True
                    self._mark_provider_cancel_race_window(lifecycle)
                await provider.cancel_response()
                guard.cancel_requested = True
            except Exception as exc:
                if should_wait_for_response_done:
                    lifecycle.cancel_pending = False
                cleanup_errors.append({
                    "step": "cancel_response",
                    "errorType": type(exc).__name__,
                    "message": str(exc),
                })

        self._append_event(
            call_id,
            "interrupt_audio_stop_completed",
            "agent",
            {
                "source": source,
                "reason": reason,
                "generation": guard.generation,
                "responseId": guard.current_response_id,
            },
        )
        for cleanup_error in cleanup_errors:
            self._append_event(
                call_id,
                "interrupt_cleanup_failed",
                "agent",
                cleanup_error,
            )

    async def _maybe_confirm_interrupt_from_turn(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        timestamp: datetime,
    ) -> None:
        turn = self._pending_turn(call_id)
        session = self.registry.get(call_id)
        has_active_model_response = self._has_active_model_response(call_id)
        has_recent_ai_audio = self._has_recent_ai_audio(call_id, timestamp)
        # 只在已有有效文字时确认打断；短噪声、回声和无转写输入停留在候选阶段。
        decision = self._interrupt_policy.decide_transcript(
            InterruptDecisionContext(
                source="provider",
                session_status=session.status,
                has_recent_ai_audio=has_recent_ai_audio,
                has_active_model_response=has_active_model_response,
                has_interrupt_candidate=turn.interrupt_candidate,
                candidate_reason=turn.interrupt_reason if turn.interrupt_candidate else None,
                candidate_stale=(
                    not turn.interrupt_confirmed
                    and self._is_stale_browser_interrupt_candidate(turn, timestamp)
                ),
                has_valid_transcript=bool(turn.transcript),
            )
        )
        if decision.action == "ignore" and decision.reason == "browser_candidate_expired":
            self._ignore_empty_turn(call_id, turn, decision.reason)
            return
        if (
            decision.action == "confirm"
            and turn.sip_barge_in_requested
            and turn.interrupt_reason == SIP_BARGE_IN_INTERRUPT_REASON
            and session.status != CallSessionStatus.AI_SPEAKING
            and not has_active_model_response
        ):
            self._cancel_sip_barge_in_task_nowait(call_id)
            turn.sip_barge_in_requested = False
            turn.sip_barge_in_expires_at = None
            turn.sip_candidate_response_id = None
            turn.sip_candidate_generation = None
            self._ignore_empty_turn(call_id, turn, "not_interrupt")
            return
        if turn.interrupt_confirmed or decision.action != "confirm":
            return
        self._confirm_sip_barge_in(
            call_id,
            turn,
            confirmed_by="transcript",
            reason=decision.reason,
        )
        self._confirm_browser_audio_hold(
            call_id,
            turn,
            confirmed_by="transcript",
            reason=decision.reason,
        )
        self._confirm_browser_pre_stop(
            call_id,
            turn,
            confirmed_by="transcript",
            reason=decision.reason,
        )
        guard = self._playback_guard(call_id)
        if not guard.cancel_requested:
            await self._invalidate_audio_for_interrupt_candidate(
                call_id=call_id,
                provider=provider,
                trigger_timestamp=turn.interrupt_trigger_at or timestamp,
                source="agent",
                reason=decision.reason,
            )
        await self._confirm_interrupt(
            call_id,
            provider,
            turn.interrupt_trigger_at or timestamp,
            reason=decision.reason,
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
        drop_reason = self._audio_drop_reason(call_id, provider_event)
        if drop_reason is not None:
            self._append_stale_audio_dropped(call_id, provider_event, drop_reason)
            return
        if self.audio_publisher is None:
            return
        delta = provider_event.payload.get("delta")
        if not isinstance(delta, str) or not delta:
            return

        frame = self.audio_bridge.decode_qwen_output_delta(delta)
        for playout_frame in self.audio_bridge.iter_output_playout_frames(frame):
            drop_reason = self._audio_drop_reason(call_id, provider_event)
            if drop_reason is not None:
                self._append_stale_audio_dropped(call_id, provider_event, drop_reason)
                return
            await self.audio_publisher.publish_audio(call_id, playout_frame)
            drop_reason = self._audio_drop_reason(call_id, provider_event)
            if drop_reason is not None:
                self._append_stale_audio_dropped(call_id, provider_event, drop_reason)
                cleanup_errors = await self._stop_audio_playout_queue(
                    call_id,
                    source="agent",
                    reason=f"stale_audio_after_publish:{drop_reason}",
                    force=True,
                )
                for cleanup_error in cleanup_errors:
                    self._append_event(
                        call_id,
                        "interrupt_cleanup_failed",
                        "agent",
                        cleanup_error,
                    )
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
            self._playback_guard(call_id).current_response_audio_published = True
            self.registry.get(call_id).metrics = metrics.snapshot()

    def _audio_drop_reason(self, call_id: str, provider_event: ProviderEvent) -> str | None:
        guard = self._playback_guard(call_id)
        response_id = self._response_id_from_payload(provider_event.payload)
        if response_id and response_id in guard.cancelled_response_ids:
            return "cancelled_response"
        if guard.awaiting_response_start_after_interrupt and (
            not guard.current_response_id or response_id != guard.current_response_id
        ):
            return "awaiting_response_start_after_interrupt"
        if guard.current_response_generation != guard.generation:
            return "stale_generation"
        if response_id and guard.current_response_id and response_id != guard.current_response_id:
            return "non_current_response"
        if guard.user_speech_active:
            return "user_speech_active"
        if self._is_audio_suppressed(call_id):
            return "suppressed_after_interrupt"
        if self.registry.get(call_id).status != CallSessionStatus.AI_SPEAKING:
            return "session_not_ai_speaking"
        return None

    def _append_stale_audio_dropped(
        self,
        call_id: str,
        provider_event: ProviderEvent,
        reason: str,
    ) -> None:
        guard = self._playback_guard(call_id)
        delta = provider_event.payload.get("delta")
        self._append_event(
            call_id,
            "stale_audio_dropped",
            "agent",
            {
                "reason": reason,
                "responseId": self._response_id_from_payload(provider_event.payload),
                "currentResponseId": guard.current_response_id,
                "generation": guard.generation,
                "currentResponseGeneration": guard.current_response_generation,
                "deltaBytes": self._base64_decoded_size(delta) if isinstance(delta, str) else None,
            },
        )

    async def _stop_audio_playout_queue(
        self,
        call_id: str,
        *,
        source: str,
        reason: str,
        force: bool = False,
    ) -> list[dict[str, str]]:
        guard = self._playback_guard(call_id)
        if self.audio_publisher is None:
            return []
        if guard.audio_stop_requested and not force:
            return []
        try:
            await self.audio_publisher.stop_audio(call_id)
        except Exception as exc:
            return [
                {
                    "step": "stop_audio",
                    "errorType": type(exc).__name__,
                    "message": str(exc),
                }
            ]

        guard.audio_stop_requested = True
        self._append_event(
            call_id,
            "playout_queue_flushed",
            "agent",
            {
                "source": source,
                "reason": reason,
                "generation": guard.generation,
                "responseId": guard.current_response_id,
                "forced": force,
            },
        )
        return []

    def _has_recent_ai_audio(self, call_id: str, trigger_timestamp: datetime) -> bool:
        last_published_at = self._last_ai_audio_published_at.get(call_id)
        if last_published_at is None:
            return False
        elapsed_seconds = (trigger_timestamp - last_published_at).total_seconds()
        return 0 <= elapsed_seconds <= self.browser_interrupt_recent_audio_seconds

    def _is_audio_suppressed(self, call_id: str) -> bool:
        guard = self._playback_guard(call_id)
        suppressed_until = guard.suppress_audio_until
        if suppressed_until is None:
            return False
        if datetime.now(timezone.utc) < suppressed_until:
            return True
        guard.suppress_audio_until = None
        return False

    def _is_stale_browser_interrupt_candidate(
        self,
        turn: PendingUserTurn,
        timestamp: datetime,
    ) -> bool:
        if turn.interrupt_reason not in BROWSER_INTERRUPT_REASONS:
            return False
        if turn.interrupt_trigger_at is None:
            return False
        if turn.browser_pre_stop_requested and turn.browser_pre_stop_expires_at is not None:
            return timestamp > turn.browser_pre_stop_expires_at
        max_age_seconds = max(
            self.browser_interrupt_audio_suppress_seconds,
            self.browser_interrupt_recent_audio_seconds,
        )
        return (timestamp - turn.interrupt_trigger_at).total_seconds() > max_age_seconds

    def _is_recent_enough_to_upgrade_from_browser_candidate(
        self,
        turn: PendingUserTurn,
        timestamp: datetime,
    ) -> bool:
        if turn.interrupt_reason not in BROWSER_INTERRUPT_REASONS:
            return False
        if turn.interrupt_trigger_at is None:
            return False
        elapsed_seconds = (timestamp - turn.interrupt_trigger_at).total_seconds()
        return 0 <= elapsed_seconds <= BROWSER_INTERRUPT_PROVIDER_UPGRADE_GRACE_SECONDS

    def _restore_after_ignored_interrupt_candidate(self, call_id: str) -> None:
        guard = self._playback_guard(call_id)
        guard.user_speech_active = False
        session = self.registry.get(call_id)
        if session.status == CallSessionStatus.AI_SPEAKING:
            self.registry.transition(call_id, CallSessionStatus.CONNECTED)
        elif session.status == CallSessionStatus.INTERRUPTED:
            self.registry.transition(call_id, CallSessionStatus.WAITING)
            self.registry.transition(call_id, CallSessionStatus.CONNECTED)

    def _has_active_model_response(self, call_id: str) -> bool:
        lifecycle = self._response_lifecycle(call_id)
        return lifecycle.active or lifecycle.cancel_pending

    def _has_current_response_audio(self, call_id: str) -> bool:
        return self._response_lifecycle(call_id).active and (
            self._playback_guard(call_id).current_response_audio_published
        )

    def _decide_realtime_transcript_trust(
        self,
        call_id: str,
        provider_event: ProviderEvent,
    ):
        now = datetime.now(timezone.utc)
        session = self.registry.get(call_id)
        turn = self._pending_turn(call_id)
        during_ai_audio = (
            session.status == CallSessionStatus.AI_SPEAKING
            or self._has_recent_ai_audio(call_id, now)
            or self._has_active_model_response(call_id)
        )
        return decide_realtime_transcript_trust(
            self._transcript_text(provider_event),
            during_ai_audio=during_ai_audio,
            has_interrupt_candidate=turn.interrupt_candidate,
            has_reliable_user_audio=self._has_reliable_short_transcript_audio_evidence(turn),
            payload=provider_event.payload,
        )

    @staticmethod
    def _has_reliable_short_transcript_audio_evidence(turn: PendingUserTurn) -> bool:
        if turn.sip_barge_in_confirmed_by in {"sip_clean_window", "transcript"}:
            return True
        if turn.sip_barge_in_confirmed and turn.sip_provider_speech_confirmable:
            return True
        if turn.browser_audio_hold_confirmed or turn.browser_pre_stop_confirmed:
            return True
        if turn.browser_segment_phase not in {"updated", "ended"}:
            return False
        duration_ms = turn.browser_segment_duration_ms or 0
        snr_db = turn.browser_segment_snr_db or 0.0
        hot_frames = turn.browser_segment_hot_frame_count or 0
        rms_dbfs = turn.browser_segment_rms_dbfs
        if rms_dbfs is None:
            return False
        if (
            turn.browser_segment_remote_audio_active is True
            and turn.browser_segment_remote_audio_rms_dbfs is not None
            and rms_dbfs + BROWSER_PRE_STOP_ECHO_REJECT_MARGIN_DB
            < turn.browser_segment_remote_audio_rms_dbfs
        ):
            return False
        audio_hold_reliable = (
            duration_ms >= BROWSER_AUDIO_HOLD_MIN_DURATION_MS
            and snr_db >= BROWSER_AUDIO_HOLD_MIN_SNR_DB
            and hot_frames >= BROWSER_AUDIO_HOLD_MIN_HOT_FRAMES
            and rms_dbfs >= BROWSER_AUDIO_HOLD_MIN_RMS_DBFS
        )
        strong_short_segment_reliable = (
            duration_ms >= BROWSER_SPEECH_SEGMENT_SHORT_STOP_DURATION_MS
            and snr_db >= BROWSER_SPEECH_SEGMENT_SHORT_STOP_SNR_DB
            and hot_frames >= BROWSER_SPEECH_SEGMENT_SHORT_STOP_HOT_FRAMES
            and rms_dbfs >= BROWSER_AUDIO_HOLD_MIN_RMS_DBFS
        )
        return audio_hold_reliable or strong_short_segment_reliable

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
        self._transition_to_interrupted_for_confirmed_interrupt(call_id)

        cleanup_errors: list[dict[str, str]] = []
        guard = self._playback_guard(call_id)
        cleanup_errors.extend(
            await self._stop_audio_playout_queue(
                call_id,
                source="agent",
                reason=reason,
            )
        )
        response_lifecycle = self._response_lifecycle(call_id)
        try:
            if guard.cancel_requested or response_lifecycle.cancel_pending:
                pass
            else:
                if response_lifecycle.active:
                    response_lifecycle.cancel_pending = True
                    self._mark_provider_cancel_race_window(response_lifecycle)
                await provider.cancel_response()
                guard.cancel_requested = True
        except Exception as exc:
            if response_lifecycle.active:
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
        self._playback_guard(call_id).suppress_audio_until = None
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

    def _transition_to_interrupted_for_confirmed_interrupt(self, call_id: str) -> None:
        session = self.registry.get(call_id)
        if session.status == CallSessionStatus.INTERRUPTED:
            return
        if session.status in {
            CallSessionStatus.CONNECTED,
            CallSessionStatus.AI_THINKING,
        } and self._has_active_model_response(call_id):
            self.registry.transition(call_id, CallSessionStatus.AI_SPEAKING)
        self.registry.transition(call_id, CallSessionStatus.INTERRUPTED)

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
        lifecycle.cancel_race_ignore_until = None
        lifecycle.pending_create = False
        lifecycle.pending_input_text = None
        lifecycle.response_generation = self._playback_guard(call_id).generation
        guard = self._playback_guard(call_id)
        guard.cancel_requested = False
        guard.audio_stop_requested = False
        guard.current_response_id = None
        guard.current_response_generation = lifecycle.response_generation
        return True

    def _queue_response_create(self, call_id: str, input_text: str | None = None) -> None:
        lifecycle = self._response_lifecycle(call_id)
        lifecycle.pending_create = True
        if input_text:
            lifecycle.pending_input_text = input_text

    def _mark_response_started(self, call_id: str, payload: dict[str, Any]) -> None:
        lifecycle = self._response_lifecycle(call_id)
        lifecycle.active = True
        guard = self._playback_guard(call_id)
        response_id = self._response_id_from_payload(payload)
        guard.current_response_id = response_id
        guard.current_response_generation = lifecycle.response_generation
        guard.current_response_audio_published = False
        if response_id and guard.current_response_generation != guard.generation:
            guard.cancelled_response_ids.add(response_id)
            self._append_event(
                call_id,
                "response_generation_invalidated",
                "agent",
                {
                    "source": "provider",
                    "reason": "stale_response_started",
                    "generation": guard.generation,
                    "responseGeneration": guard.current_response_generation,
                    "responseId": response_id,
                },
            )
        else:
            guard.awaiting_response_start_after_interrupt = False
            self._promote_stopped_turn_for_model_response(call_id)
        pending_call_end = self._pending_call_ends.get(call_id)
        if pending_call_end is not None:
            pending_call_end.final_response_started = True

    def _promote_stopped_turn_for_model_response(self, call_id: str) -> None:
        guard = self._playback_guard(call_id)
        if guard.user_speech_active:
            return
        session = self.registry.get(call_id)
        if session.status != CallSessionStatus.USER_SPEAKING:
            return
        self.registry.transition(call_id, CallSessionStatus.AI_THINKING)
        turn = self._pending_user_turns.get(call_id)
        if turn is not None and turn.stopped_at is not None and not turn.response_requested:
            turn.response_requested = True
            self._cancel_turn_response_task_nowait(call_id)

    async def _complete_response_and_flush_pending(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
    ) -> None:
        lifecycle = self._response_lifecycle(call_id)
        guard = self._playback_guard(call_id)
        cancel_was_pending = lifecycle.cancel_pending or guard.cancel_requested
        lifecycle.active = False
        lifecycle.cancel_pending = False
        guard.cancel_requested = False
        if cancel_was_pending:
            self._mark_provider_cancel_race_window(lifecycle)
        if not lifecycle.pending_create:
            if await self._maybe_recover_sip_confirmed_without_transcript(call_id, provider):
                return
            self._promote_missing_call_end_tool(call_id)
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

    async def _maybe_recover_sip_confirmed_without_transcript(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
    ) -> bool:
        turn = self._pending_user_turns.get(call_id)
        if turn is None:
            return False
        if (
            not turn.sip_barge_in_confirmed
            or turn.sip_barge_in_confirmed_by != "sip_clean_window"
            or turn.transcript
            or turn.response_requested
            or turn.sip_interrupt_rejected
        ):
            return False
        if turn.sip_recovery_count >= self.sip_barge_in_recovery_max_per_turn:
            return False
        turn.sip_recovery_count += 1
        turn.response_requested = True
        self._append_sip_recovery_started_event(
            call_id,
            turn,
            reason="sip_confirmed_without_transcript",
        )
        await self._request_response(call_id, provider, input_text=None)
        return True

    def _promote_missing_call_end_tool(self, call_id: str) -> None:
        if call_id in self._pending_call_ends:
            return
        intent = self._pending_call_end_intents.pop(call_id, None)
        if intent is None:
            return

        tool_call_id = f"local_call_end_intent:{intent.reason}"
        self._pending_call_ends[call_id] = PendingCallEnd(
            tool_call_id=tool_call_id,
            tool_reason="customer_end",
            end_reason="customer_end",
            final_response_started=True,
        )
        self._append_event(
            call_id,
            "call_end_tool_missing",
            "agent",
            {
                "toolCallId": tool_call_id,
                "toolReason": "customer_end",
                "endReason": "customer_end",
                "intentReason": intent.reason,
                "intentSummary": intent.summary,
                "classifierSource": intent.source,
                "confidence": intent.confidence,
                "transcriptPreview": self._text_preview(intent.transcript),
            },
        )

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

    @staticmethod
    def _mark_provider_cancel_race_window(lifecycle: ResponseLifecycle) -> None:
        lifecycle.cancel_race_ignore_until = datetime.now(timezone.utc) + timedelta(
            seconds=PROVIDER_CANCEL_RACE_GRACE_SECONDS
        )

    def _ignore_provider_cancel_race_error(
        self,
        call_id: str,
        payload: dict[str, Any],
        timestamp: datetime,
    ) -> bool:
        message = self._failure_message(payload) or ""
        if "none active response" not in message.lower():
            return False
        lifecycle = self._response_lifecycle(call_id)
        guard = self._playback_guard(call_id)
        race_window_active = (
            lifecycle.cancel_race_ignore_until is not None
            and timestamp <= lifecycle.cancel_race_ignore_until
        )
        if not (lifecycle.cancel_pending or guard.cancel_requested or race_window_active):
            return False

        lifecycle.active = False
        lifecycle.cancel_pending = False
        guard.cancel_requested = False
        self._append_event(
            call_id,
            "model_cancel_race_ignored",
            "agent",
            {
                "reason": "provider_cancel_no_active_response",
                "message": message,
                "responseId": guard.current_response_id,
                "raceWindowUntil": (
                    lifecycle.cancel_race_ignore_until.isoformat()
                    if lifecycle.cancel_race_ignore_until is not None
                    else None
                ),
            },
        )
        return True

    def _clear_response_lifecycle(self, call_id: str) -> None:
        lifecycle = self._response_lifecycle(call_id)
        lifecycle.active = False
        lifecycle.cancel_pending = False
        lifecycle.cancel_race_ignore_until = None
        lifecycle.pending_create = False
        lifecycle.pending_input_text = None
        lifecycle.response_generation = self._playback_guard(call_id).generation

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

    @staticmethod
    def _text_preview(text: str, limit: int = 120) -> str:
        stripped = " ".join(text.split())
        return stripped if len(stripped) <= limit else f"{stripped[:limit]}..."

    def _response_lifecycle(self, call_id: str) -> ResponseLifecycle:
        lifecycle = self._response_lifecycles.get(call_id)
        if lifecycle is None:
            lifecycle = ResponseLifecycle()
            self._response_lifecycles[call_id] = lifecycle
        return lifecycle

    def _playback_guard(self, call_id: str) -> PlaybackGuard:
        guard = self._playback_guards.get(call_id)
        if guard is None:
            guard = PlaybackGuard()
            self._playback_guards[call_id] = guard
        return guard

    @staticmethod
    def _response_id_from_payload(payload: dict[str, Any]) -> str | None:
        for key in ("response_id", "responseId"):
            value = payload.get(key)
            if isinstance(value, str) and value:
                return value
        response = payload.get("response")
        if isinstance(response, dict):
            value = response.get("id")
            if isinstance(value, str) and value:
                return value
        return None

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
