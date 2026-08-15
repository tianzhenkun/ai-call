from __future__ import annotations

import asyncio
import base64
import hashlib
import json
from collections.abc import AsyncIterator, Callable
from contextlib import suppress
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal, Protocol

from app.services.ai_call.audio_bridge import PcmAudioBridge, PcmAudioFrame
from app.services.ai_call.call_end_decision_service import (
    CallEndDecision,
    RuleBasedCallEndDecisionService,
)
from app.services.ai_call.dialogue_merge import normalize_dialogue_text
from app.services.ai_call.event_store import InMemoryEventStore
from app.services.ai_call.handoff_trigger_service import (
    RuleBasedHandoffIntentClassifier,
)
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
from app.services.ai_call.sip_vad_shadow import (
    SipVadShadowDetectorProtocol,
    SipVadShadowObservation,
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

AUDIO_PLAYOUT_MAX_RESPONSE_DURATION_MS = 60_000
AUDIO_PLAYOUT_HIGH_WATERMARK_PERCENT = 80
CALL_POLICY_WRAP_UP_SECONDS = 240
CALL_POLICY_FINAL_RESPONSE_SECONDS = 290
CALL_POLICY_SAFETY_END_SECONDS = 300
CALL_POLICY_MAX_CUSTOMER_TURNS = 15
CALL_POLICY_SILENCE_SECONDS = 8
CALL_POLICY_MAX_SILENCE_PROMPTS = 3
CALL_POLICY_WRAP_UP_INPUT = "通话即将达到时长上限，请从当前话题自然收尾，不要开启新问题。"
CALL_POLICY_FINAL_INPUT = (
    "请只说：感谢您的时间，今天先沟通到这里，祝您生活愉快，再见。"
    "不要添加其他内容，不要再提出问题。"
)
CALL_POLICY_SILENCE_INPUTS = (
    "客户暂未回应，请简短询问一次是否还在听，不要重复之前的长内容。",
    "客户仍未回应，请最后确认一次是否方便继续沟通，保持一句话。",
)


CALL_END_REASON_MAPPING = {
    "customer_end": "customer_end",
    "task_completed": "normal_completed",
    "policy_limit": "policy_limit",
}

HANDOFF_REASON_VALUES = {"customer_request", "business_escalation"}
BUSINESS_HANDOFF_CONFIRMATION_TOOL_RESULT = (
    "系统尚未开始转人工。请先询问用户是否确认需要转人工，不得说正在转接、马上接入或已经接通。"
)
CUSTOMER_HANDOFF_REJECTED_TOOL_RESULT = (
    "未确认用户明确要求转人工。请继续回应用户刚才的话，不要转人工。"
)
CUSTOMER_HANDOFF_CONFIRMATION_TOOL_RESULT = (
    "用户的转人工表达不完整。请只询问：您是希望转接人工客服吗？"
    "不得声称坐席繁忙、暂无人工接入或正在转接。"
)
PARTIAL_HANDOFF_INTENT_VALUES = frozenset({"转", "人工", "客服", "真人"})
CALL_END_FINAL_RESPONSE_TOOL_RESULT = "已记录。请用一句简短礼貌的话结束通话，不要继续提出新问题。"
CALL_END_NO_EXTRA_RESPONSE_TOOL_RESULT = "已记录。系统将结束通话，不要再生成额外回复。"
CALL_END_REJECTED_TOOL_RESULT = "未确认用户要求结束通话。请继续按用户刚才的话推进对话，不要结束通话。"
CALL_END_NO_TERMINAL_SIGNAL_REJECTED_TOOL_RESULT = (
    "未确认客户要结束通话。请继续回应客户刚才的问题或做一个必要澄清。"
)
TASK_COMPLETED_REJECTED_TOOL_RESULT = (
    "未确认客户已同意后续联系或演示。请继续澄清下一步，不要结束通话。"
)
CALL_END_FINAL_RESPONSE_TOOL_RESULTS_BY_REASON = {
    "customer_end": (
        "请直接回复：“好的，那我先不打扰您了，祝您工作顺利。”"
        "不要添加其他内容，不要再提出问题。"
    ),
    "task_completed": (
        "请直接回复：“好的，相关信息我已经记录，感谢您的时间，再见。”"
        "不要添加其他内容，不要再提出问题。"
    ),
    "policy_limit": CALL_POLICY_FINAL_INPUT,
}
CALL_END_USER_TURN_GRACE_SECONDS = 1.0
AI_QUESTION_ANSWER_WINDOW_SECONDS = 3.0
NO_BARGE_USER_TURN_STABILITY_DELAY_SECONDS = 1.2
CALL_END_ACKNOWLEDGEMENT_TEXTS = frozenset({
    "好",
    "好的",
    "好的好的",
    "好知道了",
    "知道了",
    "嗯",
    "嗯嗯",
    "嗯好的",
    "行",
    "可以",
})
TASK_COMPLETED_AMBIGUOUS_TEXTS = CALL_END_ACKNOWLEDGEMENT_TEXTS | frozenset({
    "方便",
    "知道",
    "知道了",
    "可以知道了",
    "是",
    "是的",
    "对",
    "对的",
})
TASK_COMPLETED_NEGATIVE_PATTERNS = (
    "不用联系",
    "不要联系",
    "别联系",
    "先不联系",
    "不需要联系",
    "不用顾问",
    "不要顾问",
    "不用沟通",
    "不要沟通",
    "不安排",
    "不要安排",
)
TASK_COMPLETED_NEXT_STEP_PATTERNS = (
    "顾问联系",
    "联系我",
    "后续联系",
    "稍后联系",
    "回头联系",
    "电话联系",
    "线上沟通",
    "线上会议",
    "安排沟通",
    "约个",
    "约一下",
    "约时间",
    "安排演示",
    "产品演示",
    "看看演示",
    "发资料",
    "把资料发",
    "加微信",
    "留电话",
)
NO_BARGE_FOLLOWUP_ACKNOWLEDGEMENT_TEXTS = frozenset({
    "嗯",
    "嗯嗯",
    "好",
    "好的",
    "行",
    "可以",
    "方便",
    "你好",
    "有",
    "有的",
    "对",
    "对的",
    "是",
})
NO_BARGE_FOLLOWUP_SUBSTANTIVE_HINTS = frozenset({
    "吗",
    "么",
    "呢",
    "怎么",
    "什么",
    "准",
    "不准",
    "测试",
    "平台",
    "demo",
    "演示",
    "上传",
    "效果",
    "价格",
    "试用",
    "合同",
    "审核",
    "流程",
    "联系",
    "顾问",
})
CALL_END_TERMINAL_TAIL_MAX_CHARS = 6
CALL_END_STRONG_CONTINUATION_TEXTS = frozenset({
    "等会",
    "等一下",
    "等等",
    "先等",
    "别挂",
    "不要挂",
    "先别挂",
})
CALL_END_STRONG_CONTINUATION_PATTERNS = (
    "还有",
    "不对",
    "不是",
    "问一下",
)
CALL_END_TERMINAL_TIME_HINT_PATTERNS = (
    "今天",
    "明天",
    "后天",
    "上午",
    "中午",
    "下午",
    "晚上",
    "周一",
    "周二",
    "周三",
    "周四",
    "周五",
    "周六",
    "周日",
    "星期",
)
PHONE_RESPONSE_BREVITY_INSTRUCTIONS = (
    "电话单轮回复约束：\n"
    "- 每次回复尽量控制在 10-15 秒内。\n"
    "- 默认不超过 2 句话，优先一句结论加一个问题。\n"
    "- 不要一次性展开多步骤长篇说明；用户要求详细说明时，也要分轮讲，每轮只讲一个重点。"
)
FINAL_ROLE_BOUNDARY_INSTRUCTIONS = (
    "最终角色边界复核：\n"
    "- 你正在以 AI 助手或业务专员身份和客户通话，只能说产品方应该说的话。\n"
    "- 不得用“我们这边”“我这边”模拟客户说话，不得替客户补充客户公司的背景、痛点、需求或疑问。\n"
    "- 客户短句只能按字面理解，不得把“好的”“有”“嗯”扩写成客户已确认痛点、需求或态度。\n"
    "- 如果客户只说“方便”“你好”“嗯”等简短确认，应继续用助手身份介绍一个产品价值点或追问一个业务问题。"
)
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
SIP_TURN_CLUSTER_LOCAL_ONLY_MAX_WALL_MS = 350
SIP_FAST_LOCAL_MIN_DURATION_MS = 480
SIP_DEFERRED_EPISODE_MAX_GAP_SECONDS = 2.6
SIP_DEFERRED_EPISODE_MAX_WALL_MS = 7000
SIP_DEFERRED_EPISODE_SAME_BURST_GAP_SECONDS = 0.35
SIP_DEFERRED_EPISODE_MIN_BURSTS = 3
SIP_DEFERRED_EPISODE_MIN_VOICED_MS = 540
SIP_DEFERRED_EPISODE_MIN_RMS_RANGE_DB = 4.0
SIP_DEFERRED_EPISODE_MIN_SNR_DB = 18.0
SIP_DEFERRED_EPISODE_COMPACT_MIN_BURSTS = 2
SIP_DEFERRED_EPISODE_COMPACT_MIN_VOICED_MS = 360
SIP_DEFERRED_EPISODE_COMPACT_MAX_WALL_MS = 2600
SIP_DEFERRED_EPISODE_COMPACT_MAX_GAP_MS = 2600
SIP_DEFERRED_EPISODE_COMPACT_MIN_RMS_RANGE_DB = 6.0
SIP_DEFERRED_EPISODE_COMPACT_MIN_SNR_DB = 15.0
SIP_DEFERRED_EPISODE_AI_RECEDED_COMPACT_MIN_VOICED_MS = 400
SIP_DEFERRED_EPISODE_AI_RECEDED_COMPACT_MIN_RMS_RANGE_DB = 5.5
SIP_DEFERRED_EPISODE_AI_RECEDED_COMPACT_MIN_SNR_DB = 16.0
SIP_DEFERRED_TURN_MAX_GAP_SECONDS = 12.0
SIP_DEFERRED_TURN_MAX_WALL_MS = 20000
SIP_DEFERRED_TURN_ECHO_GUARD_MIN_WALL_MS = 3000
SIP_DEFERRED_TURN_MIN_RMS_RANGE_DB = 6.0
SIP_DEFERRED_TURN_MIN_SNR_DB = 10.0
SIP_REJECTED_PRE_STOP_PROVIDER_CONFIRM_GRACE_SECONDS = 1.0
SIP_ECHO_GUARDED_TURN_MAX_GAP_SECONDS = 12.0
SIP_ECHO_GUARDED_TURN_MAX_WALL_MS = 16000
SIP_ECHO_GUARDED_TURN_SAME_BURST_GAP_SECONDS = 0.35
SIP_ECHO_GUARDED_TURN_MIN_BURSTS = 2
SIP_ECHO_GUARDED_TURN_MIN_VOICED_MS = 420
SIP_ECHO_GUARDED_TURN_MIN_RMS_RANGE_DB = 4.0
SIP_ECHO_GUARDED_TURN_MIN_SNR_DB = 15.0
SIP_ECHO_GUARDED_LOCAL_MIN_RMS_RANGE_DB = 6.0
SIP_ECHO_GUARDED_LOCAL_MIN_SNR_DB = 18.0
SIP_ECHO_GUARDED_LOCAL_MAX_LARGE_JUMPS = 3
SIP_ECHO_GUARDED_LOCAL_HIGH_NOISE_MIN_SNR_DB = 20.0
SIP_ECHO_GUARDED_LOCAL_HIGH_NOISE_MIN_DIRECTION_CHANGES = 2
SIP_ECHO_GUARDED_LOCAL_DEFERRED_MIN_BURSTS = 2
SIP_ECHO_GUARDED_LOCAL_DEFERRED_MIN_VOICED_MS = 420
SIP_ECHO_GUARDED_LOCAL_DEFERRED_MAX_WALL_MS = 2600
SIP_ECHO_GUARDED_LOCAL_DEFERRED_MAX_GAP_MS = 2600
SIP_ECHO_GUARDED_LOCAL_DEFERRED_MAX_AI_DOMINANCE_DB = 1.0
SIP_ECHO_GUARDED_LOCAL_DEFERRED_PRE_STOP_MIN_UPLINK_ABOVE_AI_DB = 3.0
SIP_ECHO_GUARDED_COMPACT_SHORT_PHRASE_MIN_UPLINK_ABOVE_AI_DB = 2.0
SIP_TURN_CLUSTER_RECOVERABLE_QUALITY_REJECTIONS = frozenset({
    "clipped_hot_onset",
    "short_hot_onset_drop",
})
SIP_TURN_EVIDENCE_IGNORED_QUALITY_REJECTIONS = frozenset({
    "rise_fall_tail_envelope",
})
SIP_SINGLE_SHORT_MIN_RMS_DBFS = -20.0
SIP_SINGLE_SHORT_MAX_RMS_DBFS = -12.0
SIP_SINGLE_SHORT_MIN_SNR_DB = 16.0
SIP_SINGLE_SHORT_MAX_DIRECTION_CHANGES = 4
SIP_ELEVATED_NOISE_FLOOR_DBFS = -38.0
SIP_BORDERLINE_ELEVATED_NOISE_FLOOR_DBFS = SIP_ELEVATED_NOISE_FLOOR_DBFS - 3.0
SIP_UNSTABLE_LOCAL_ENVELOPE_MIN_RMS_RANGE_DB = 12.0
SIP_UNSTABLE_LOCAL_ENVELOPE_MIN_DIRECTION_CHANGES = 4
SIP_UNSTABLE_LOCAL_ENVELOPE_MIN_LARGE_JUMPS = 4
SIP_ELEVATED_NOISE_MARGINAL_TURN_MIN_SNR_DB = 18.0
SIP_ELEVATED_NOISE_SPARSE_TURN_MIN_ANCHOR_SNR_OFFSET_DB = 10.0
SIP_ELEVATED_NOISE_SPARSE_TURN_MIN_CURRENT_SNR_OFFSET_DB = 4.0
SIP_ELEVATED_NOISE_SPARSE_TURN_MODULATED_MIN_SNR_DB = 16.0
SIP_ELEVATED_NOISE_SPARSE_TURN_MODULATED_MIN_DIRECTION_CHANGES = 2
SIP_CLEAR_SHORT_MODULATED_MIN_RMS_RANGE_DB = 6.0
SIP_CLEAR_SHORT_MODULATED_MIN_SNR_DB = 12.0
SIP_CLEAR_SHORT_MODULATED_MAX_LARGE_JUMPS = 1
SIP_CLEAR_SHORT_NOISE_LOW_SNR_MAX_DB = SIP_SINGLE_SHORT_MIN_SNR_DB
SIP_CLEAR_SHORT_NOISE_LOUD_MIN_SNR_DB = 20.0
SIP_CLEAR_SHORT_NOISE_LOUD_MIN_RMS_DBFS = -24.0
SIP_CLEAR_SHORT_NOISE_LOUD_MIN_DIRECTION_CHANGES = 3
SIP_ELEVATED_NOISE_CLEAR_SHORT_MIN_RMS_RANGE_DB = 8.0
SIP_ELEVATED_NOISE_CLEAR_SHORT_MIN_SNR_DB = 13.0
SIP_ELEVATED_NOISE_CLEAR_SHORT_MAX_SNR_DB = 18.0
SIP_ELEVATED_NOISE_CLEAR_SHORT_MAX_RMS_DBFS = -20.5
SIP_AI_PLAYBACK_ECHO_UPLINK_MARGIN_DB = 6.0
SIP_REALTIME_SHADOW_PRE_STOP_MIN_WINDOW_MS = 360
SIP_REALTIME_SHADOW_PRE_STOP_MAX_WINDOW_MS = 1800
SIP_REALTIME_SHADOW_CONTEXT_MAX_WINDOW_MS = 4200
SIP_REALTIME_SHADOW_EVIDENCE_MAX_AGE_SECONDS = 1.4
SIP_REALTIME_SHADOW_MIN_LOCAL_SNR_DB = 17.5
SIP_SHORT_RECOVERY_INPUT_TEXT = (
    "请用一句简短自然的话继续刚才未完成的问题或说明，不要重复整段内容。"
)
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


class ProviderTransportError(RuntimeError):
    pass


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
    transcript_merge_start_index: int | None = None
    no_barge_overlap_stopped_during_ai_response: bool = False
    no_barge_unstarted_response_deferred: bool = False
    current_speech_semantic_rejected: bool = False
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
    sip_ai_playback_echo_deferred: bool = False
    sip_pre_stop_at: datetime | None = None
    sip_candidate_class: str | None = None
    sip_candidate_response_id: str | None = None
    sip_candidate_generation: int | None = None
    sip_single_short_pre_stop_evidence: bool = False
    sip_provider_speech_confirmable: bool = False
    sip_interrupt_rejected: bool = False
    sip_interrupt_rejected_at: datetime | None = None
    sip_recovery_count: int = 0
    sip_turn_cluster_response_id: str | None = None
    sip_turn_cluster_first_at: datetime | None = None
    sip_turn_cluster_last_at: datetime | None = None
    sip_turn_cluster_burst_count: int = 0
    sip_turn_cluster_voiced_ms: int = 0
    sip_turn_cluster_shadow_burst_count: int = 0
    sip_turn_cluster_shadow_voiced_ms: int = 0
    sip_turn_cluster_shadow_detector: str | None = None
    sip_turn_cluster_shadow_window_ms: int | None = None
    sip_recent_shadow_response_id: str | None = None
    sip_recent_shadow_at: datetime | None = None
    sip_recent_shadow_evidence: str | None = None
    sip_recent_shadow_detector: str | None = None
    sip_recent_shadow_window_ms: int | None = None
    sip_turn_cluster_min_rms_dbfs: float | None = None
    sip_turn_cluster_max_rms_dbfs: float | None = None
    sip_turn_cluster_max_snr_db: float | None = None
    sip_turn_cluster_max_rms_range_db: float | None = None
    sip_deferred_episode_response_id: str | None = None
    sip_deferred_episode_generation: int | None = None
    sip_deferred_episode_first_at: datetime | None = None
    sip_deferred_episode_last_at: datetime | None = None
    sip_deferred_episode_burst_count: int = 0
    sip_deferred_episode_voiced_ms: int = 0
    sip_deferred_episode_current_burst_voiced_ms: int = 0
    sip_deferred_episode_min_rms_dbfs: float | None = None
    sip_deferred_episode_max_rms_dbfs: float | None = None
    sip_deferred_episode_max_snr_db: float | None = None
    sip_deferred_episode_max_rms_range_db: float | None = None
    sip_deferred_episode_max_gap_ms: int | None = None
    sip_echo_guarded_turn_response_id: str | None = None
    sip_echo_guarded_turn_generation: int | None = None
    sip_echo_guarded_turn_first_at: datetime | None = None
    sip_echo_guarded_turn_last_at: datetime | None = None
    sip_echo_guarded_turn_burst_count: int = 0
    sip_echo_guarded_turn_voiced_ms: int = 0
    sip_echo_guarded_turn_current_burst_voiced_ms: int = 0
    sip_echo_guarded_turn_min_rms_dbfs: float | None = None
    sip_echo_guarded_turn_max_rms_dbfs: float | None = None
    sip_echo_guarded_turn_max_snr_db: float | None = None
    sip_echo_guarded_turn_max_rms_range_db: float | None = None
    browser_segment_phase: str | None = None
    browser_segment_duration_ms: int | None = None
    browser_segment_snr_db: float | None = None
    browser_segment_hot_frame_count: int | None = None
    browser_segment_rms_dbfs: float | None = None
    browser_segment_remote_audio_active: bool | None = None
    browser_segment_remote_audio_rms_dbfs: float | None = None
    browser_segment_observed_at: datetime | None = None
    browser_segment_ended_at: datetime | None = None
    call_end_acknowledged: bool = False

    @property
    def transcript(self) -> str:
        return "".join(self.transcript_parts).strip()


@dataclass(frozen=True, slots=True)
class SipPreStopAuthorityDecision:
    action: Literal["pre_stop", "defer"]
    reason: str
    required_duration_ms: int
    authority: str = "local_speech"
    evidence: str | None = None
    extra_payload: dict[str, Any] = field(default_factory=dict)

    @property
    def allowed(self) -> bool:
        return self.action == "pre_stop"


@dataclass(slots=True)
class ResponseLifecycle:
    active: bool = False
    active_started_at: datetime | None = None
    cancel_pending: bool = False
    cancel_race_ignore_until: datetime | None = None
    pending_create: bool = False
    pending_input_text: str | None = None
    pending_response_is_opening: bool = False
    current_response_is_opening: bool = False
    response_generation: int = 0


@dataclass(slots=True)
class PlaybackGuard:
    generation: int = 0
    current_response_id: str | None = None
    current_response_generation: int = 0
    current_response_audio_published: bool = False
    cancelled_response_ids: set[str] = field(default_factory=set)
    overflowed_responses: set[tuple[str | None, int]] = field(default_factory=set)
    cancel_requested: bool = False
    audio_stop_requested: bool = False
    user_speech_active: bool = False
    awaiting_response_start_after_interrupt: bool = False
    suppress_audio_until: datetime | None = None


@dataclass(frozen=True, slots=True)
class AudioPlayoutDelta:
    delta: str
    pcm_bytes: int
    frame_count: int
    response_id: str | None
    response_generation: int


@dataclass(slots=True)
class AudioPlayoutQueueStats:
    queued_frames: int = 0
    queued_bytes: int = 0
    high_watermark_response_keys: set[tuple[str | None, int]] = field(
        default_factory=set
    )


@dataclass(slots=True)
class PendingCallEnd:
    tool_call_id: str
    tool_reason: str
    end_reason: str
    final_response_started: bool = False
    scheduled: bool = False
    local_explicit_intent: bool = False


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
        sip_vad_shadow_enabled: bool = False,
        sip_vad_shadow_detector: SipVadShadowDetectorProtocol | None = None,
        user_turn_stability_delay_seconds: float = 0.35,
        no_barge_user_turn_stability_delay_seconds: float = 0.0,
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
        self.sip_vad_shadow_enabled = sip_vad_shadow_enabled
        self._sip_vad_shadow_detector = sip_vad_shadow_detector
        self._sip_vad_shadow_failed_call_ids: set[str] = set()
        self.user_turn_stability_delay_seconds = max(0.0, user_turn_stability_delay_seconds)
        self.no_barge_user_turn_stability_delay_seconds = max(
            0.0,
            no_barge_user_turn_stability_delay_seconds,
        )
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
        self._audio_playout_queues: dict[str, asyncio.Queue[AudioPlayoutDelta]] = {}
        self._audio_playout_queue_stats: dict[str, AudioPlayoutQueueStats] = {}
        self._audio_playout_tasks: dict[str, asyncio.Task[None]] = {}
        self._audio_playout_overflow_tasks: dict[str, asyncio.Task[None]] = {}
        self._playout_tasks: dict[str, asyncio.Task[None]] = {}
        self._turn_response_tasks: dict[str, asyncio.Task[None]] = {}
        self._last_ai_audio_published_at: dict[str, datetime] = {}
        self._last_ai_audio_rms_dbfs: dict[str, float] = {}
        self._last_sip_local_speech_active_at: dict[str, datetime] = {}
        self._last_sip_provider_speech_stopped_at: dict[str, datetime] = {}
        self._pending_user_turns: dict[str, PendingUserTurn] = {}
        self._response_lifecycles: dict[str, ResponseLifecycle] = {}
        self._playback_guards: dict[str, PlaybackGuard] = {}
        self._pending_call_ends: dict[str, PendingCallEnd] = {}
        self._pending_call_end_intents: dict[str, PendingCallEndIntent] = {}
        self._pending_call_end_defer_tasks: dict[str, asyncio.Task[None]] = {}
        self._call_policy_tasks: dict[str, asyncio.Task[None]] = {}
        self._silence_watchdog_tasks: dict[str, asyncio.Task[None]] = {}
        self._silence_prompt_counts: dict[str, int] = {}
        self._customer_turn_counts: dict[str, int] = {}
        self._browser_audio_hold_tasks: dict[str, asyncio.Task[None]] = {}
        self._browser_pre_stop_tasks: dict[str, asyncio.Task[None]] = {}
        self._sip_barge_in_tasks: dict[str, asyncio.Task[None]] = {}
        self._sip_clean_window_tasks: dict[str, asyncio.Task[None]] = {}
        self._sip_recovery_tasks: dict[str, asyncio.Task[None]] = {}
        self._provider_transport_diagnostics: dict[str, dict[str, Any]] = {}
        self._last_ai_question_completed_at: dict[str, datetime] = {}

    def runtime_diagnostics(self) -> dict[str, object]:
        return dict(AGENT_RUNNER_RUNTIME_DIAGNOSTICS)

    async def start(self, session: CallSession) -> None:
        provider = self.provider_factory(session)
        self._providers[session.call_id] = provider
        state = self._provider_transport_state(session.call_id)
        state.update({
            "providerClass": f"{type(provider).__module__}.{type(provider).__name__}",
            "providerCreatedAt": self._utcnow_text(),
        })
        await provider.connect()
        state["providerConnectedAt"] = self._utcnow_text()
        session_config = self._session_config(session)
        await provider.update_session(session_config)
        state.update({
            "providerSessionUpdatedAt": self._utcnow_text(),
            "sessionVoice": session_config.voice,
            "sessionVadType": session_config.vad_type,
            "sessionVadThreshold": session_config.vad_threshold,
            "sessionVadSilenceDurationMs": session_config.vad_silence_duration_ms,
        })
        self._tasks[session.call_id] = asyncio.create_task(
            self._consume_provider_events(session.call_id, provider)
        )
        if self.audio_transport is not None:
            await self.audio_transport.start(session)
            self._audio_tasks[session.call_id] = asyncio.create_task(
                self._consume_room_audio(session.call_id, self.audio_transport)
            )

    async def stop(self, call_id: str) -> None:
        await self._cancel_call_policy_task(call_id)
        await self._cancel_silence_watchdog(call_id)
        await self._cancel_playout_task(call_id)
        await self._cancel_turn_response_task(call_id)
        await self._cancel_pending_call_end_defer_task(call_id)
        await self._cancel_browser_audio_hold_task(call_id)
        await self._cancel_browser_pre_stop_task(call_id)
        await self._cancel_sip_barge_in_task(call_id)
        await self._cancel_sip_clean_window_task(call_id)
        await self._cancel_sip_recovery_task(call_id)
        await self._cancel_audio_playout_overflow_task(call_id)
        guard = self._playback_guards.get(call_id)
        playout_queue = self._audio_playout_queues.get(call_id)
        playout_task = self._audio_playout_tasks.get(call_id)
        has_audio_playout = (
            (
                guard is not None
                and guard.current_response_audio_published
                and not guard.audio_stop_requested
            )
            or (playout_queue is not None and not playout_queue.empty())
            or (playout_task is not None and not playout_task.done())
        )
        if has_audio_playout:
            cleanup_errors = await self._stop_audio_playout_queue(
                call_id,
                source="agent",
                reason="session_stop",
                force=True,
            )
            for cleanup_error in cleanup_errors:
                self._append_event(
                    call_id,
                    "agent_cleanup_failed",
                    "agent",
                    cleanup_error,
                )
        else:
            self._clear_audio_playout_queue(call_id)
            await self._cancel_audio_playout_worker(call_id)

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
        self._last_ai_audio_rms_dbfs.pop(call_id, None)
        self._last_sip_local_speech_active_at.pop(call_id, None)
        self._last_sip_provider_speech_stopped_at.pop(call_id, None)
        self._sip_vad_shadow_failed_call_ids.discard(call_id)
        self._pending_user_turns.pop(call_id, None)
        self._response_lifecycles.pop(call_id, None)
        self._playback_guards.pop(call_id, None)
        self._pending_call_ends.pop(call_id, None)
        self._pending_call_end_intents.pop(call_id, None)
        self._silence_prompt_counts.pop(call_id, None)
        self._customer_turn_counts.pop(call_id, None)
        self._provider_transport_diagnostics.pop(call_id, None)
        self._last_ai_question_completed_at.pop(call_id, None)
        self._audio_playout_queues.pop(call_id, None)
        self._audio_playout_queue_stats.pop(call_id, None)
        if self._sip_barge_in_detector is not None:
            self._sip_barge_in_detector.reset(call_id)
        if self._sip_vad_shadow_detector is not None:
            self._sip_vad_shadow_detector.reset(call_id)

    async def suspend_for_handoff(self, call_id: str) -> None:
        await self._cancel_playout_task(call_id)
        cleanup_errors = await self._stop_audio_playout_queue(
            call_id,
            source="handoff",
            reason="suspend_for_handoff",
            force=True,
        )
        for cleanup_error in cleanup_errors:
            self._append_event(
                call_id,
                "handoff_prompt_cleanup_failed",
                "agent",
                cleanup_error,
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
        audio_playout_task = self._audio_playout_tasks.get(call_id)
        if audio_playout_task is not None:
            await audio_playout_task
        overflow_task = self._audio_playout_overflow_tasks.get(call_id)
        if overflow_task is not None:
            await overflow_task
        playout_task = self._playout_tasks.get(call_id)
        if playout_task is not None:
            await playout_task

    async def send_audio_frame(self, call_id: str, frame: PcmAudioFrame) -> None:
        provider = self._providers[call_id]
        await self._maybe_handle_sip_barge_in_audio(call_id, provider, frame)
        for chunk in self.audio_bridge.iter_qwen_input_chunks(frame):
            try:
                await provider.send_audio(chunk)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise ProviderTransportError(str(exc)) from exc

    async def start_opening(self, call_id: str) -> None:
        provider = self._providers[call_id]
        session = self.registry.get(call_id)
        opening_message = str(self._config_value(session.effective_config, "opening_message", ""))
        input_text = f"请主动说出开场白：{opening_message}" if opening_message else None
        await self._request_response(
            call_id,
            provider,
            input_text=input_text,
            opening_response=True,
        )
        if self._is_sip_participant(session):
            self._start_call_policy_task(call_id)

    def _start_call_policy_task(self, call_id: str) -> None:
        task = self._call_policy_tasks.get(call_id)
        if task is not None and not task.done():
            return
        self._call_policy_tasks[call_id] = asyncio.create_task(
            self._run_call_policy(call_id),
            name=f"ai-call-policy-{call_id}",
        )

    async def _run_call_policy(self, call_id: str) -> None:
        try:
            await asyncio.sleep(CALL_POLICY_WRAP_UP_SECONDS)
            if not self._call_policy_is_running(call_id):
                return
            provider = self._providers.get(call_id)
            if provider is None:
                return
            self._append_event(
                call_id,
                "call_policy_wrap_up_requested",
                "agent",
                {"elapsedSeconds": CALL_POLICY_WRAP_UP_SECONDS},
            )
            await self._request_response(
                call_id,
                provider,
                input_text=CALL_POLICY_WRAP_UP_INPUT,
            )

            await asyncio.sleep(
                CALL_POLICY_FINAL_RESPONSE_SECONDS - CALL_POLICY_WRAP_UP_SECONDS
            )
            if not self._call_policy_is_running(call_id):
                return
            await self._begin_policy_call_end(
                call_id,
                provider,
                end_reason="policy_duration_limit",
            )

            await asyncio.sleep(
                CALL_POLICY_SAFETY_END_SECONDS - CALL_POLICY_FINAL_RESPONSE_SECONDS
            )
            pending = self._pending_call_ends.get(call_id)
            if (
                not self._call_policy_is_running(call_id)
                or pending is None
                or pending.scheduled
                or self.call_end_scheduler is None
            ):
                return
            pending.scheduled = True
            self._append_event(
                call_id,
                "call_policy_safety_end",
                "agent",
                {"elapsedSeconds": CALL_POLICY_SAFETY_END_SECONDS},
            )
            self.call_end_scheduler(call_id, pending.end_reason)
        except asyncio.CancelledError:
            raise
        finally:
            if self._call_policy_tasks.get(call_id) is asyncio.current_task():
                self._call_policy_tasks.pop(call_id, None)

    def _call_policy_is_running(self, call_id: str) -> bool:
        try:
            status = self.registry.get(call_id).status
        except Exception:
            return False
        return status not in {
            CallSessionStatus.ENDING,
            CallSessionStatus.COMPLETED,
            CallSessionStatus.FAILED,
        }

    async def _begin_policy_call_end(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        *,
        end_reason: str,
    ) -> bool:
        if call_id in self._pending_call_ends or not self._call_policy_is_running(call_id):
            return False
        self._pending_call_ends[call_id] = PendingCallEnd(
            tool_call_id=f"local_policy:{end_reason}",
            tool_reason="policy_limit",
            end_reason=end_reason,
        )
        self._cancel_silence_watchdog_nowait(call_id)
        self._append_event(
            call_id,
            "call_policy_end_requested",
            "agent",
            {"endReason": end_reason},
        )
        await self._request_response(
            call_id,
            provider,
            input_text=CALL_POLICY_FINAL_INPUT,
        )
        return True

    def _arm_silence_watchdog(self, call_id: str) -> None:
        session = self.registry.get(call_id)
        if (
            not self._is_sip_participant(session)
            or call_id in self._pending_call_ends
            or not self._call_policy_is_running(call_id)
        ):
            return
        self._cancel_silence_watchdog_nowait(call_id)
        self._silence_watchdog_tasks[call_id] = asyncio.create_task(
            self._handle_silence_timeout(call_id),
            name=f"ai-call-silence-{call_id}",
        )

    async def _handle_silence_timeout(self, call_id: str) -> None:
        try:
            await asyncio.sleep(CALL_POLICY_SILENCE_SECONDS)
            session = self.registry.get(call_id)
            if (
                not self._call_policy_is_running(call_id)
                or self._playback_guard(call_id).user_speech_active
                or session.status == CallSessionStatus.USER_SPEAKING
            ):
                return
            provider = self._providers.get(call_id)
            if provider is None:
                return
            prompt_count = self._silence_prompt_counts.get(call_id, 0) + 1
            self._silence_prompt_counts[call_id] = prompt_count
            self._append_event(
                call_id,
                "call_policy_silence_timeout",
                "agent",
                {"count": prompt_count},
            )
            if prompt_count >= CALL_POLICY_MAX_SILENCE_PROMPTS:
                await self._begin_policy_call_end(
                    call_id,
                    provider,
                    end_reason="policy_no_response",
                )
                return
            await self._request_response(
                call_id,
                provider,
                input_text=CALL_POLICY_SILENCE_INPUTS[prompt_count - 1],
            )
        except asyncio.CancelledError:
            raise
        finally:
            if self._silence_watchdog_tasks.get(call_id) is asyncio.current_task():
                self._silence_watchdog_tasks.pop(call_id, None)

    def _cancel_silence_watchdog_nowait(self, call_id: str) -> None:
        task = self._silence_watchdog_tasks.pop(call_id, None)
        if task is not None and not task.done() and task is not asyncio.current_task():
            task.cancel()

    async def _cancel_silence_watchdog(self, call_id: str) -> None:
        task = self._silence_watchdog_tasks.pop(call_id, None)
        if task is None or task.done() or task is asyncio.current_task():
            return
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def _cancel_call_policy_task(self, call_id: str) -> None:
        task = self._call_policy_tasks.pop(call_id, None)
        if task is None or task.done() or task is asyncio.current_task():
            return
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def _consume_room_audio(
        self,
        call_id: str,
        audio_transport: RoomAudioTransportProtocol,
    ) -> None:
        try:
            async for frame in audio_transport.receive_audio_frames(call_id):
                self._record_provider_audio_send_attempt(call_id, frame)
                try:
                    await self.send_audio_frame(call_id, frame)
                except asyncio.CancelledError:
                    raise
                except ProviderTransportError as exc:
                    self._record_provider_audio_send_error(call_id, exc)
                    self._fail_running_session(
                        call_id,
                        end_reason="provider_transport_error",
                        failure_stage="provider_audio_send",
                        failure_message=f"模型音频上行传输异常: {exc}",
                        extra_payload={
                            "providerTransport": self._provider_transport_snapshot(
                                call_id,
                                error_source="provider_audio_send",
                            ),
                        },
                    )
                    return
                self._record_provider_audio_send_success(call_id)
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
        session = self.registry.get(call_id)
        if not self._is_sip_participant(session):
            self._reset_sip_audio_observers(call_id)
            return
        if not self._is_barge_in_enabled_for_session(session):
            self._reset_sip_audio_observers(call_id)
            return

        now = datetime.now(timezone.utc)
        interruptible = self._is_sip_barge_in_interruptible(call_id, session, now)
        shadow_observations = self._maybe_observe_sip_vad_shadow(
            call_id=call_id,
            session=session,
            frame=frame,
            now=now,
            interruptible=interruptible,
        )

        detector = self._sip_barge_in_detector
        if detector is None:
            return
        observation = detector.observe(
            call_id,
            frame,
            now=now,
            interruptible=interruptible,
        )
        self._record_sip_response_release_activity(
            call_id=call_id,
            timestamp=now,
            observation=observation,
            shadow_observations=shadow_observations,
        )
        if observation.candidate_class == "impulse_noise":
            payload = self._sip_barge_in_event_payload(call_id, observation)
            payload["reason"] = "impulse_noise"
            self._append_event(call_id, "sip_impulse_noise_ignored", "agent", payload)
            return
        if interruptible:
            self._record_sip_recent_shadow_pre_stop_evidence(
                call_id=call_id,
                trigger_timestamp=now,
                shadow_observations=shadow_observations,
            )
            self._record_sip_turn_cluster_shadow_observations(
                call_id=call_id,
                trigger_timestamp=now,
                shadow_observations=shadow_observations,
            )
            if (
                not observation.candidate
                and await self._maybe_create_shadow_assisted_sip_candidate(
                    call_id=call_id,
                    provider=provider,
                    trigger_timestamp=now,
                    observation=observation,
                    shadow_observations=shadow_observations,
                )
            ):
                return
        if not observation.candidate:
            if await self._maybe_upgrade_deferred_sip_pre_stop_from_shadow(
                call_id=call_id,
                provider=provider,
                trigger_timestamp=now,
                observation=observation,
                shadow_observations=shadow_observations,
            ):
                return
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
            shadow_observations=shadow_observations,
        )
        await self._maybe_upgrade_deferred_sip_pre_stop_from_shadow(
            call_id=call_id,
            provider=provider,
            trigger_timestamp=now,
            observation=observation,
            shadow_observations=shadow_observations,
        )

    def _reset_sip_audio_observers(self, call_id: str) -> None:
        if self._sip_barge_in_detector is not None:
            self._sip_barge_in_detector.reset(call_id)
        if self._sip_vad_shadow_detector is not None:
            self._sip_vad_shadow_detector.reset(call_id)
        self._sip_vad_shadow_failed_call_ids.discard(call_id)
        self._last_sip_local_speech_active_at.pop(call_id, None)
        turn = self._pending_user_turns.get(call_id)
        if turn is not None:
            self._reset_sip_recent_shadow_evidence(turn, response_id=None)

    def _record_sip_response_release_activity(
        self,
        *,
        call_id: str,
        timestamp: datetime,
        observation: SipBargeInObservation,
        shadow_observations: list[SipVadShadowObservation],
    ) -> None:
        if observation.active or any(item.active for item in shadow_observations):
            self._last_sip_local_speech_active_at[call_id] = timestamp

    def _sip_response_release_delay_seconds(self, call_id: str) -> float:
        session = self.registry.get(call_id)
        if (
            not self.sip_barge_in_fast_stop_enabled
            or not self._is_sip_participant(session)
            or not self._is_barge_in_enabled_for_session(session)
        ):
            return 0.0
        last_active_at = self._last_sip_local_speech_active_at.get(call_id)
        if last_active_at is None:
            return 0.0
        clean_window_seconds = max(0.0, self.sip_barge_in_config.clean_window_ms / 1000)
        elapsed_seconds = (datetime.now(timezone.utc) - last_active_at).total_seconds()
        return max(0.0, clean_window_seconds - elapsed_seconds)

    def _defer_sip_response_release_if_needed(
        self,
        call_id: str,
        *,
        input_text: str | None = None,
    ) -> bool:
        delay_seconds = self._sip_response_release_delay_seconds(call_id)
        if delay_seconds <= 0:
            return False
        lifecycle = self._response_lifecycle(call_id)
        lifecycle.pending_create = True
        if input_text:
            lifecycle.pending_input_text = input_text
        self._append_event(
            call_id,
            "sip_response_release_deferred",
            "agent",
            {
                "reason": "local_speech_active",
                "delayMs": int(round(delay_seconds * 1000)),
                "cleanWindowMs": self.sip_barge_in_config.clean_window_ms,
                "lastLocalSpeechAt": self._last_sip_local_speech_active_at[
                    call_id
                ].isoformat(),
            },
        )
        self._schedule_sip_pending_response_release(call_id, delay_seconds)
        return True

    def _schedule_sip_pending_response_release(
        self,
        call_id: str,
        delay_seconds: float,
    ) -> None:
        existing_task = self._turn_response_tasks.get(call_id)
        if (
            existing_task is not None
            and not existing_task.done()
            and existing_task is not asyncio.current_task()
        ):
            return
        provider = self._providers.get(call_id)
        if provider is None:
            return
        self._turn_response_tasks[call_id] = asyncio.create_task(
            self._release_sip_pending_response_after_clean_window(
                call_id,
                provider,
                max(0.0, delay_seconds),
            ),
            name=f"ai-call-sip-response-release-{call_id}",
        )

    async def _release_sip_pending_response_after_clean_window(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        delay_seconds: float,
    ) -> None:
        try:
            while True:
                if delay_seconds > 0:
                    await asyncio.sleep(delay_seconds)
                if self.registry.get(call_id).status in {
                    CallSessionStatus.ENDING,
                    CallSessionStatus.COMPLETED,
                    CallSessionStatus.FAILED,
                }:
                    return
                lifecycle = self._response_lifecycle(call_id)
                if not lifecycle.pending_create or lifecycle.active or lifecycle.cancel_pending:
                    return
                delay_seconds = self._sip_response_release_delay_seconds(call_id)
                if delay_seconds > 0:
                    continue
                input_text = lifecycle.pending_input_text
                opening_response = lifecycle.pending_response_is_opening
                lifecycle.pending_create = False
                lifecycle.pending_input_text = None
                lifecycle.pending_response_is_opening = False
                session = self.registry.get(call_id)
                if session.status in {
                    CallSessionStatus.USER_SPEAKING,
                    CallSessionStatus.CONNECTED,
                    CallSessionStatus.INTERRUPTED,
                }:
                    self.registry.transition(call_id, CallSessionStatus.AI_THINKING)
                await self._request_response(
                    call_id,
                    provider,
                    input_text=input_text,
                    opening_response=opening_response,
                )
                return
        except asyncio.CancelledError:
            raise
        finally:
            if self._turn_response_tasks.get(call_id) is asyncio.current_task():
                self._turn_response_tasks.pop(call_id, None)

    def _maybe_observe_sip_vad_shadow(
        self,
        *,
        call_id: str,
        session: CallSession,
        frame: PcmAudioFrame,
        now: datetime,
        interruptible: bool,
    ) -> list[SipVadShadowObservation]:
        detector = self._sip_vad_shadow_detector
        if (
            not self.sip_vad_shadow_enabled
            or detector is None
            or call_id in self._sip_vad_shadow_failed_call_ids
        ):
            return []
        try:
            result = detector.observe(
                call_id,
                frame,
                now=now,
                interruptible=interruptible,
            )
        except Exception as exc:
            self._sip_vad_shadow_failed_call_ids.add(call_id)
            self._append_event(
                call_id,
                "sip_vad_shadow_error",
                "agent",
                {
                    "reason": "sip_vad_shadow_detector_error",
                    "detector": getattr(detector, "detector_name", type(detector).__name__),
                    "errorType": type(exc).__name__,
                    "message": str(exc),
                },
            )
            return []
        observations = (
            [result]
            if isinstance(result, SipVadShadowObservation)
            else list(result)
        )
        for observation in observations:
            if observation.error_type is not None:
                self._append_event(
                    call_id,
                    "sip_vad_shadow_error",
                    "agent",
                    {
                        "reason": "sip_vad_shadow_detector_error",
                        "detector": observation.detector,
                        "errorType": observation.error_type,
                        "message": observation.error_message or "",
                    },
                )
                continue
            if observation.started:
                self._append_event(
                    call_id,
                    "sip_vad_shadow_started",
                    "agent",
                    self._sip_vad_shadow_event_payload(
                        session=session,
                        observation=observation,
                        interruptible=interruptible,
                    ),
                )
            if observation.ended:
                self._append_event(
                    call_id,
                    "sip_vad_shadow_ended",
                    "agent",
                    self._sip_vad_shadow_event_payload(
                        session=session,
                        observation=observation,
                        interruptible=interruptible,
                    ),
                )
        return observations

    def _sip_vad_shadow_event_payload(
        self,
        *,
        session: CallSession,
        observation: SipVadShadowObservation,
        interruptible: bool,
    ) -> dict[str, Any]:
        guard = self._playback_guard(session.call_id)
        return {
            "reason": "sip_vad_shadow",
            "detector": observation.detector,
            "active": observation.active,
            "durationMs": observation.duration_ms,
            "frameDurationMs": observation.frame_duration_ms,
            "confidence": observation.confidence,
            "analyzed": observation.analyzed,
            "bufferDurationMs": observation.buffer_duration_ms,
            "windowStartMs": observation.window_start_ms,
            "windowEndMs": observation.window_end_ms,
            "detectionLagMs": observation.detection_lag_ms,
            "speechEndLagMs": observation.speech_end_lag_ms,
            "interruptible": interruptible,
            "sessionStatus": session.status.value,
            "responseId": guard.current_response_id,
            "generation": guard.generation,
        }

    async def _handle_sip_barge_in_candidate(
        self,
        *,
        call_id: str,
        provider: RealtimeProviderProtocol,
        trigger_timestamp: datetime,
        observation: SipBargeInObservation,
        extra_payload: dict[str, Any] | None = None,
        allow_pre_stop: bool = True,
        shadow_observations: list[SipVadShadowObservation] | None = None,
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
        self._expire_stale_deferred_sip_candidate_if_needed(
            call_id=call_id,
            turn=turn,
            trigger_timestamp=trigger_timestamp,
            observation=observation,
        )
        self._record_sip_turn_cluster_observation(call_id, turn, trigger_timestamp, observation)
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
            payload = self._sip_barge_in_event_payload(call_id, observation)
            if extra_payload:
                payload.update(extra_payload)
            self._append_event(
                call_id,
                "sip_interrupt_candidate",
                "agent",
                payload,
            )

        self._extend_sip_barge_in(call_id, turn, trigger_timestamp)
        if self._has_sip_single_short_pre_stop_evidence(call_id, observation):
            turn.sip_single_short_pre_stop_evidence = True
        if self.sip_barge_in_fast_stop_enabled and allow_pre_stop:
            await self._maybe_pre_stop_sip_barge_in_candidate(
                call_id=call_id,
                provider=provider,
                turn=turn,
                trigger_timestamp=trigger_timestamp,
                observation=observation,
                shadow_observations=shadow_observations,
            )
        elif self.sip_barge_in_fast_stop_enabled:
            self._defer_sip_pre_stop(
                call_id=call_id,
                turn=turn,
                observation=observation,
                reason="awaiting_pre_stop_authority",
                required_duration_ms=self._sip_required_pre_stop_duration_ms(observation),
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

    def _decide_sip_pre_stop_authority(
        self,
        *,
        call_id: str,
        turn: PendingUserTurn,
        trigger_timestamp: datetime,
        observation: SipBargeInObservation,
        shadow_observations: list[SipVadShadowObservation] | None = None,
    ) -> SipPreStopAuthorityDecision:
        required_duration_ms = self._sip_required_pre_stop_duration_ms(observation)
        detector = self._sip_barge_in_detector
        shadow_observations = shadow_observations or []
        shadow_evidence = self._sip_realtime_shadow_pre_stop_evidence(
            shadow_observations
        ) or self._sip_recent_shadow_pre_stop_evidence(
            call_id=call_id,
            turn=turn,
            trigger_timestamp=trigger_timestamp,
        )
        local_shadow_evidence = (
            self._sip_realtime_shadow_local_pre_stop_evidence(
                call_id=call_id,
                turn=turn,
                observation=observation,
            )
            if shadow_evidence is not None
            else None
        )
        has_shadow_local_speech = (
            shadow_evidence is not None and local_shadow_evidence is not None
        )
        has_shadow_local_duration_bypass = (
            has_shadow_local_speech
            and local_shadow_evidence.get("sipVadShadowLocalEvidence")
            == "local_modulated_candidate"
            and observation.candidate_duration_ms >= required_duration_ms
        )
        has_shadow_turn_cluster_duration_bypass = (
            has_shadow_local_speech
            and local_shadow_evidence.get("sipVadShadowLocalEvidence")
            == "shadow_turn_cluster"
            and observation.candidate_duration_ms >= required_duration_ms
            and turn.sip_turn_cluster_voiced_ms
            >= max(
                SIP_TURN_CLUSTER_MIN_VOICED_MS * 2,
                self.sip_barge_in_config.pre_stop_min_duration_ms * 2,
            )
        )
        has_single_short = self._has_sip_single_short_pre_stop_evidence(
            call_id,
            observation,
        )
        has_clear_short = self._has_sip_clear_short_modulated_pre_stop_evidence(
            call_id,
            observation,
        )
        continuous_shadow_context_evidence = (
            None
            if shadow_evidence is not None
            else self._sip_realtime_continuous_shadow_context_evidence(
                shadow_observations
            )
        )
        continuous_shadow_context_local_evidence = (
            self._sip_continuous_shadow_context_local_pre_stop_evidence(
                has_clear_short=has_clear_short,
            )
            if continuous_shadow_context_evidence is not None
            else None
        )
        has_fast_local = (
            observation.candidate_class
            in {"stable_speech_candidate", "strong_short_speech_candidate"}
            and detector is not None
            and detector.has_fast_pre_stop_local_speech(call_id)
            and self._has_sip_fast_local_pre_stop_authority(
                observation=observation
            )
        )
        has_stable_local = (
            observation.candidate_class == "stable_speech_candidate"
            and detector is not None
            and detector.has_pre_stop_local_speech(call_id)
        )
        quality_rejection = self._sip_observation_quality_rejection(call_id)
        shadow_local_quality_rejection = (
            self._sip_shadow_local_quality_rejection(
                call_id=call_id,
                observation=observation,
                local_shadow_evidence=local_shadow_evidence,
            )
            if local_shadow_evidence is not None
            else None
        )
        cluster_payload = self._sip_turn_cluster_pre_stop_extra_payload(
            call_id,
            turn,
            observation=observation,
            quality_rejection=quality_rejection,
        )
        has_cluster = (
            observation.candidate_class == "stable_speech_candidate"
            and cluster_payload is not None
        )
        deferred_episode_payload = self._sip_deferred_episode_pre_stop_extra_payload(
            call_id,
            turn,
            observation=observation,
            quality_rejection=quality_rejection,
        )
        has_deferred_episode = (
            observation.candidate_class == "stable_speech_candidate"
            and deferred_episode_payload is not None
        )
        has_elevated_noise_short_burst_risk = (
            self._has_sip_elevated_noise_short_burst_risk(observation)
        )
        has_elevated_noise_local_only_risk = self._has_sip_elevated_noise_local_only_risk(
            call_id,
            observation,
        )
        clear_short_noise_risk = (
            self._sip_clear_short_noise_risk(call_id, observation)
            if has_clear_short
            else None
        )
        has_strong_stable_local = (
            has_stable_local
            and observation.candidate_duration_ms
            >= max(
                self.sip_barge_in_config.pre_stop_min_duration_ms,
                self.sip_barge_in_config.candidate_min_duration_ms * 2,
            )
            and self._has_sip_modulated_pre_stop_local_speech(call_id, observation)
            and turn.sip_deferred_episode_max_rms_dbfs is not None
            and turn.sip_deferred_episode_max_rms_dbfs >= SIP_SINGLE_SHORT_MIN_RMS_DBFS
        )
        short_evidence_payload: dict[str, Any] = {}
        if has_single_short:
            short_evidence_payload["sipShortSpeechEvidence"] = "single_high_confidence_burst"
        elif has_clear_short:
            short_evidence_payload["sipShortSpeechEvidence"] = "clear_short_modulated_burst"
        elif has_fast_local and observation.candidate_class == "strong_short_speech_candidate":
            short_evidence_payload["sipShortSpeechEvidence"] = "strong_short_local_speech"

        fast_local_required_duration_ms = (
            min(required_duration_ms, observation.candidate_duration_ms)
            if has_fast_local and observation.candidate_class == "strong_short_speech_candidate"
            else required_duration_ms
        )
        opening_pre_stop_guard_active = self._is_sip_opening_pre_stop_guard_active(
            call_id=call_id,
            trigger_timestamp=trigger_timestamp,
        )

        if not self._has_sip_pre_stop_playback_target(
            call_id=call_id,
            trigger_timestamp=trigger_timestamp,
        ):
            playback_target_payload = dict(short_evidence_payload)
            if shadow_evidence is not None:
                playback_target_payload.update(shadow_evidence)
            if continuous_shadow_context_evidence is not None:
                playback_target_payload.update(continuous_shadow_context_evidence)
            if local_shadow_evidence is not None:
                playback_target_payload.update(local_shadow_evidence)
            if continuous_shadow_context_local_evidence is not None:
                playback_target_payload.update(continuous_shadow_context_local_evidence)
            return SipPreStopAuthorityDecision(
                action="defer",
                reason="awaiting_ai_playback_target",
                required_duration_ms=required_duration_ms,
                extra_payload=playback_target_payload,
            )

        if observation.candidate_class == "stable_speech_candidate":
            if self._is_within_sip_post_speech_tail_guard(call_id, trigger_timestamp):
                return SipPreStopAuthorityDecision(
                    action="defer",
                    reason="awaiting_post_speech_tail_guard",
                    required_duration_ms=self.sip_barge_in_config.pre_stop_min_duration_ms,
                    extra_payload=short_evidence_payload,
                )

        is_ai_playback_echo_like = self._is_sip_ai_playback_echo_like(
            call_id=call_id,
            observation=observation,
            trigger_timestamp=trigger_timestamp,
        )
        if is_ai_playback_echo_like:
            self._record_sip_echo_guarded_turn_observation(
                call_id=call_id,
                turn=turn,
                timestamp=trigger_timestamp,
                observation=observation,
            )
            echo_guarded_turn_payload = (
                self._sip_echo_guarded_turn_pre_stop_extra_payload(
                    turn,
                    observation=observation,
                    quality_rejection=quality_rejection,
                )
            )
            if echo_guarded_turn_payload is not None:
                if self._is_sip_opening_pre_stop_guard_active(
                    call_id=call_id,
                    trigger_timestamp=trigger_timestamp,
                ):
                    return SipPreStopAuthorityDecision(
                        action="defer",
                        reason="awaiting_opening_pre_stop_authority",
                        required_duration_ms=required_duration_ms,
                        extra_payload=echo_guarded_turn_payload,
                    )
                if self._has_sip_echo_guarded_turn_noise_risk(call_id, observation):
                    payload = dict(echo_guarded_turn_payload)
                    payload["sipEchoGuardedTurnNoiseRisk"] = (
                        "low_current_snr_echo_guarded_turn"
                    )
                    return SipPreStopAuthorityDecision(
                        action="defer",
                        reason="awaiting_ai_playback_echo_guard",
                        required_duration_ms=required_duration_ms,
                        extra_payload=payload,
                    )
                return SipPreStopAuthorityDecision(
                    action="pre_stop",
                    reason=SIP_BARGE_IN_INTERRUPT_REASON,
                    required_duration_ms=required_duration_ms,
                    evidence="echo_guarded_turn_evidence",
                    extra_payload=echo_guarded_turn_payload,
                )
            echo_guarded_local_payload = (
                self._sip_echo_guarded_local_pre_stop_extra_payload(
                    call_id=call_id,
                    turn=turn,
                    observation=observation,
                    quality_rejection=quality_rejection,
                )
            )
            if echo_guarded_local_payload is not None:
                if self._is_sip_opening_pre_stop_guard_active(
                    call_id=call_id,
                    trigger_timestamp=trigger_timestamp,
                ):
                    return SipPreStopAuthorityDecision(
                        action="defer",
                        reason="awaiting_opening_pre_stop_authority",
                        required_duration_ms=required_duration_ms,
                        extra_payload=echo_guarded_local_payload,
                    )
                uplink_above_ai_db = echo_guarded_local_payload.get(
                    "sipUplinkAboveAiPlaybackDb"
                )
                if (
                    echo_guarded_local_payload.get("sipEchoGuardedLocalEvidence")
                    == "deferred_episode_micro_confirmed"
                    and observation.noise_floor_dbfs is not None
                    and isinstance(uplink_above_ai_db, int | float)
                    and uplink_above_ai_db
                    >= SIP_ECHO_GUARDED_LOCAL_DEFERRED_PRE_STOP_MIN_UPLINK_ABOVE_AI_DB
                ):
                    pre_stop_payload = dict(echo_guarded_local_payload)
                    pre_stop_payload["sipAiPlaybackEchoGuardEscapedBy"] = (
                        "deferred_episode_micro_confirmed"
                    )
                    pre_stop_payload["sipEchoGuardedLocalMinUplinkAboveAiPlaybackDb"] = (
                        SIP_ECHO_GUARDED_LOCAL_DEFERRED_PRE_STOP_MIN_UPLINK_ABOVE_AI_DB
                    )
                    return SipPreStopAuthorityDecision(
                        action="pre_stop",
                        reason=SIP_BARGE_IN_INTERRUPT_REASON,
                        required_duration_ms=required_duration_ms,
                        evidence="echo_guarded_local_speech",
                        extra_payload=pre_stop_payload,
                    )
                return SipPreStopAuthorityDecision(
                    action="defer",
                    reason="awaiting_ai_playback_echo_guard",
                    required_duration_ms=required_duration_ms,
                    extra_payload=echo_guarded_local_payload,
                )
            echo_guard_deferred_episode_payload = (
                self._sip_deferred_episode_echo_guard_pre_stop_extra_payload(
                    deferred_episode_payload
                )
            )
            if echo_guard_deferred_episode_payload is None:
                echo_guard_deferred_episode_payload = (
                    self._sip_echo_guarded_compact_deferred_episode_pre_stop_extra_payload(
                        call_id=call_id,
                        turn=turn,
                        observation=observation,
                        quality_rejection=quality_rejection,
                    )
                )
            if echo_guard_deferred_episode_payload is not None:
                if self._is_sip_opening_pre_stop_guard_active(
                    call_id=call_id,
                    trigger_timestamp=trigger_timestamp,
                ):
                    return SipPreStopAuthorityDecision(
                        action="defer",
                        reason="awaiting_opening_pre_stop_authority",
                        required_duration_ms=required_duration_ms,
                        extra_payload=echo_guard_deferred_episode_payload,
                    )
                compact_short_phrase_payload = (
                    self._sip_echo_guarded_compact_short_phrase_pre_stop_extra_payload(
                        call_id=call_id,
                        payload=echo_guard_deferred_episode_payload,
                        observation=observation,
                    )
                )
                if compact_short_phrase_payload is not None:
                    return SipPreStopAuthorityDecision(
                        action="pre_stop",
                        reason=SIP_BARGE_IN_INTERRUPT_REASON,
                        required_duration_ms=min(
                            required_duration_ms,
                            observation.candidate_duration_ms,
                        ),
                        evidence="echo_guarded_compact_short_phrase",
                        extra_payload=compact_short_phrase_payload,
                    )
                if self._should_defer_sip_compact_deferred_episode(
                    echo_guard_deferred_episode_payload,
                    call_id=call_id,
                    turn=turn,
                    observation=observation,
                    required_duration_ms=required_duration_ms,
                    echo_guarded=True,
                ):
                    return SipPreStopAuthorityDecision(
                        action="defer",
                        reason="awaiting_ai_playback_echo_guard",
                        required_duration_ms=required_duration_ms,
                        extra_payload=echo_guard_deferred_episode_payload,
                    )
                deferred_episode_evidence = (
                    "deferred_multi_candidate_turn"
                    if echo_guard_deferred_episode_payload.get("sipDeferredEpisodeEvidence")
                    == "sparse_multi_candidate_turn"
                    else "deferred_speech_episode"
                )
                return SipPreStopAuthorityDecision(
                    action="pre_stop",
                    reason=SIP_BARGE_IN_INTERRUPT_REASON,
                    required_duration_ms=required_duration_ms,
                    evidence=deferred_episode_evidence,
                    extra_payload=echo_guard_deferred_episode_payload,
                )
            return SipPreStopAuthorityDecision(
                action="defer",
                reason="awaiting_ai_playback_echo_guard",
                required_duration_ms=required_duration_ms,
                extra_payload=short_evidence_payload,
            )

        if (
            quality_rejection is not None
            and quality_rejection not in SIP_TURN_CLUSTER_RECOVERABLE_QUALITY_REJECTIONS
        ):
            quality_payload = dict(short_evidence_payload)
            quality_payload["speechQualityRejection"] = quality_rejection
            return SipPreStopAuthorityDecision(
                action="defer",
                reason="awaiting_speech_like_continuity",
                required_duration_ms=required_duration_ms,
                extra_payload=quality_payload,
            )

        if (
            continuous_shadow_context_evidence is not None
            and continuous_shadow_context_local_evidence is not None
        ):
            payload = {
                **continuous_shadow_context_evidence,
                **continuous_shadow_context_local_evidence,
            }
            if observation.candidate_duration_ms < required_duration_ms:
                return SipPreStopAuthorityDecision(
                    action="defer",
                    reason="awaiting_pre_stop_authority",
                    required_duration_ms=required_duration_ms,
                    extra_payload=payload,
                )
            if shadow_local_quality_rejection is not None:
                payload["speechQualityRejection"] = shadow_local_quality_rejection
                return SipPreStopAuthorityDecision(
                    action="defer",
                    reason="awaiting_speech_like_continuity",
                    required_duration_ms=required_duration_ms,
                    extra_payload=payload,
                )
            evidence = self._sip_pre_stop_shadow_authority_evidence(payload)
            return SipPreStopAuthorityDecision(
                action="pre_stop",
                reason=SIP_BARGE_IN_INTERRUPT_REASON,
                required_duration_ms=min(
                    required_duration_ms,
                    observation.candidate_duration_ms,
                ),
                evidence=evidence,
                extra_payload=payload,
            )

        if has_shadow_local_duration_bypass or has_shadow_turn_cluster_duration_bypass:
            payload = {**shadow_evidence, **local_shadow_evidence}
            if shadow_local_quality_rejection is not None:
                payload["speechQualityRejection"] = shadow_local_quality_rejection
                return SipPreStopAuthorityDecision(
                    action="defer",
                    reason="awaiting_speech_like_continuity",
                    required_duration_ms=required_duration_ms,
                    extra_payload=payload,
                )
            evidence = self._sip_pre_stop_shadow_authority_evidence(payload)
            return SipPreStopAuthorityDecision(
                action="pre_stop",
                reason=SIP_BARGE_IN_INTERRUPT_REASON,
                required_duration_ms=min(
                    required_duration_ms,
                    observation.candidate_duration_ms,
                ),
                evidence=evidence,
                extra_payload=payload,
                )

        if has_deferred_episode and opening_pre_stop_guard_active:
            payload = dict(deferred_episode_payload)
            payload["sipOpeningGuardedEvidence"] = "deferred_episode"
            return SipPreStopAuthorityDecision(
                action="defer",
                reason="awaiting_opening_pre_stop_authority",
                required_duration_ms=required_duration_ms,
                extra_payload=payload,
            )

        if has_deferred_episode:
            if self._has_sip_deferred_episode_noise_risk(
                call_id=call_id,
                turn=turn,
                observation=observation,
                payload=deferred_episode_payload,
            ):
                payload = dict(deferred_episode_payload)
                payload["sipDeferredEpisodeNoiseRisk"] = (
                    "low_confidence_or_elevated_noise_deferred_turn"
                )
                return SipPreStopAuthorityDecision(
                    action="defer",
                    reason="awaiting_pre_stop_authority",
                    required_duration_ms=required_duration_ms,
                    extra_payload=payload,
                )
            if self._should_defer_sip_compact_deferred_episode(
                deferred_episode_payload,
                call_id=call_id,
                turn=turn,
                observation=observation,
                required_duration_ms=required_duration_ms,
                echo_guarded=False,
            ):
                return SipPreStopAuthorityDecision(
                    action="defer",
                    reason="awaiting_pre_stop_authority",
                    required_duration_ms=required_duration_ms,
                    extra_payload=deferred_episode_payload,
                )
            deferred_episode_evidence = (
                "deferred_multi_candidate_turn"
                if deferred_episode_payload.get("sipDeferredEpisodeEvidence")
                == "sparse_multi_candidate_turn"
                else "deferred_speech_episode"
            )
            return SipPreStopAuthorityDecision(
                action="pre_stop",
                reason=SIP_BARGE_IN_INTERRUPT_REASON,
                required_duration_ms=required_duration_ms,
                evidence=deferred_episode_evidence,
                extra_payload=deferred_episode_payload,
            )

        if has_fast_local and opening_pre_stop_guard_active:
            return SipPreStopAuthorityDecision(
                action="defer",
                reason="awaiting_opening_pre_stop_authority",
                required_duration_ms=fast_local_required_duration_ms,
                extra_payload=short_evidence_payload,
            )

        if has_fast_local and (
            has_elevated_noise_short_burst_risk
            or has_elevated_noise_local_only_risk
        ):
            return SipPreStopAuthorityDecision(
                action="defer",
                reason="awaiting_authorized_pre_stop_evidence",
                required_duration_ms=fast_local_required_duration_ms,
                extra_payload=short_evidence_payload,
            )

        if has_fast_local:
            evidence = (
                "strong_short_local_speech"
                if observation.candidate_class == "strong_short_speech_candidate"
                else "stable_local_speech"
            )
            return SipPreStopAuthorityDecision(
                action="pre_stop",
                reason=SIP_BARGE_IN_INTERRUPT_REASON,
                required_duration_ms=fast_local_required_duration_ms,
                evidence=evidence,
                extra_payload=short_evidence_payload,
            )

        if has_clear_short and opening_pre_stop_guard_active:
            return SipPreStopAuthorityDecision(
                action="defer",
                reason="awaiting_opening_pre_stop_authority",
                required_duration_ms=observation.candidate_duration_ms,
                extra_payload=short_evidence_payload,
            )

        if has_clear_short and self._has_sip_elevated_noise_clear_short_pre_stop_evidence(
            call_id,
            observation,
        ):
            payload = dict(short_evidence_payload)
            payload["sipElevatedNoiseClearShortEvidence"] = True
            return SipPreStopAuthorityDecision(
                action="pre_stop",
                reason=SIP_BARGE_IN_INTERRUPT_REASON,
                required_duration_ms=observation.candidate_duration_ms,
                evidence="elevated_noise_clear_short_modulated_burst",
                extra_payload=payload,
            )

        if has_clear_short and (
            has_elevated_noise_short_burst_risk
            or has_elevated_noise_local_only_risk
        ):
            return SipPreStopAuthorityDecision(
                action="defer",
                reason="awaiting_authorized_pre_stop_evidence",
                required_duration_ms=observation.candidate_duration_ms,
                extra_payload=short_evidence_payload,
            )

        if has_clear_short and clear_short_noise_risk is not None:
            payload = dict(short_evidence_payload)
            payload["sipClearShortNoiseRisk"] = clear_short_noise_risk
            return SipPreStopAuthorityDecision(
                action="defer",
                reason="awaiting_authorized_pre_stop_evidence",
                required_duration_ms=observation.candidate_duration_ms,
                extra_payload=payload,
            )

        if has_clear_short:
            return SipPreStopAuthorityDecision(
                action="pre_stop",
                reason=SIP_BARGE_IN_INTERRUPT_REASON,
                required_duration_ms=observation.candidate_duration_ms,
                evidence="clear_short_modulated_burst",
                extra_payload=short_evidence_payload,
            )

        if observation.candidate_duration_ms < required_duration_ms:
            return SipPreStopAuthorityDecision(
                action="defer",
                reason="awaiting_pre_stop_authority",
                required_duration_ms=required_duration_ms,
                extra_payload=short_evidence_payload,
            )

        if has_shadow_local_speech:
            payload = {**shadow_evidence, **local_shadow_evidence}
            if shadow_local_quality_rejection is not None:
                payload["speechQualityRejection"] = shadow_local_quality_rejection
                return SipPreStopAuthorityDecision(
                    action="defer",
                    reason="awaiting_speech_like_continuity",
                    required_duration_ms=required_duration_ms,
                    extra_payload=payload,
                )
            evidence = self._sip_pre_stop_shadow_authority_evidence(payload)
            return SipPreStopAuthorityDecision(
                action="pre_stop",
                reason=SIP_BARGE_IN_INTERRUPT_REASON,
                required_duration_ms=min(
                    required_duration_ms,
                    observation.candidate_duration_ms,
                ),
                evidence=evidence,
                extra_payload=payload,
            )

        if (
            has_cluster
            and opening_pre_stop_guard_active
            and cluster_payload is not None
            and cluster_payload.get("sipVadShadowLocalEvidence") is None
        ):
            payload = dict(cluster_payload)
            payload["sipOpeningGuardedEvidence"] = "turn_cluster"
            return SipPreStopAuthorityDecision(
                action="defer",
                reason="awaiting_opening_pre_stop_authority",
                required_duration_ms=required_duration_ms,
                extra_payload=payload,
            )

        if has_cluster:
            return SipPreStopAuthorityDecision(
                action="pre_stop",
                reason=SIP_BARGE_IN_INTERRUPT_REASON,
                required_duration_ms=required_duration_ms,
                evidence="turn_cluster",
                extra_payload=cluster_payload,
            )

        if has_strong_stable_local and quality_rejection is None and opening_pre_stop_guard_active:
            payload = dict(short_evidence_payload)
            payload["sipOpeningGuardedEvidence"] = "stable_local_speech"
            return SipPreStopAuthorityDecision(
                action="defer",
                reason="awaiting_opening_pre_stop_authority",
                required_duration_ms=required_duration_ms,
                extra_payload=payload,
            )

        if has_strong_stable_local and quality_rejection is None:
            if has_elevated_noise_local_only_risk:
                return SipPreStopAuthorityDecision(
                    action="defer",
                    reason="awaiting_authorized_pre_stop_evidence",
                    required_duration_ms=required_duration_ms,
                    extra_payload=short_evidence_payload,
                )
            return SipPreStopAuthorityDecision(
                action="pre_stop",
                reason=SIP_BARGE_IN_INTERRUPT_REASON,
                required_duration_ms=required_duration_ms,
                evidence="stable_local_speech",
                extra_payload=short_evidence_payload,
            )

        if (
            observation.candidate_class == "stable_speech_candidate"
            and detector is not None
            and not detector.has_pre_stop_local_speech(call_id)
        ):
            return SipPreStopAuthorityDecision(
                action="defer",
                reason="awaiting_speech_quality",
                required_duration_ms=required_duration_ms,
                extra_payload=short_evidence_payload,
            )

        return SipPreStopAuthorityDecision(
            action="defer",
            reason="awaiting_authorized_pre_stop_evidence",
            required_duration_ms=required_duration_ms,
            extra_payload=short_evidence_payload,
        )

    @staticmethod
    def _is_sip_compact_deferred_episode_payload(payload: dict[str, Any] | None) -> bool:
        if payload is None:
            return False
        return payload.get("sipDeferredEpisodeEvidence") in {
            "ai_receded_compact_two_burst_turn",
            "compact_two_burst_turn",
            "elevated_noise_compact_two_burst_turn",
        }

    def _has_sip_deferred_episode_noise_risk(
        self,
        *,
        call_id: str,
        turn: PendingUserTurn,
        observation: SipBargeInObservation,
        payload: dict[str, Any] | None,
    ) -> bool:
        if payload is None:
            return False
        if observation.rms_dbfs is None or observation.snr_db is None:
            return False
        detector = self._sip_barge_in_detector
        diagnostics = detector.latest_observation_payload(call_id) if detector is not None else {}
        has_sparse_speech_anchor = (
            turn.sip_deferred_episode_max_rms_dbfs is not None
            and turn.sip_deferred_episode_max_rms_dbfs
            >= SIP_SINGLE_SHORT_MIN_RMS_DBFS + 1.5
            and turn.sip_deferred_episode_max_snr_db is not None
            and turn.sip_deferred_episode_max_snr_db
            >= SIP_SINGLE_SHORT_MIN_SNR_DB + 0.5
        )
        large_jumps = diagnostics.get("largeRmsJumpCount")
        if (
            observation.rms_dbfs <= -24.0
            and observation.snr_db < SIP_SINGLE_SHORT_MIN_SNR_DB
            and isinstance(large_jumps, int)
            and large_jumps >= 2
            and not has_sparse_speech_anchor
        ):
            return True
        if payload.get("sipDeferredEpisodeEvidence") != "sparse_multi_candidate_turn":
            return False
        if not self._has_sip_marginal_elevated_noise_turn_risk(observation):
            return False
        return not has_sparse_speech_anchor

    def _should_defer_sip_compact_deferred_episode(
        self,
        payload: dict[str, Any] | None,
        *,
        call_id: str,
        turn: PendingUserTurn,
        observation: SipBargeInObservation,
        required_duration_ms: int,
        echo_guarded: bool,
    ) -> bool:
        if not self._is_sip_compact_deferred_episode_payload(payload):
            return False
        elevated_noise_compact = (
            payload is not None
            and payload.get("sipDeferredEpisodeEvidence")
            == "elevated_noise_compact_two_burst_turn"
        )
        ai_receded_compact = (
            payload is not None
            and payload.get("sipDeferredEpisodeEvidence")
            == "ai_receded_compact_two_burst_turn"
        )
        if ai_receded_compact:
            if self._has_sip_marginal_elevated_noise_turn_risk(observation):
                wall_ms = payload.get("sipDeferredEpisodeWallMs")
                detector = self._sip_barge_in_detector
                diagnostics = (
                    detector.latest_observation_payload(call_id)
                    if detector is not None
                    else {}
                )
                direction_changes = diagnostics.get("rmsDirectionChanges")
                has_short_tail = (
                    isinstance(wall_ms, int)
                    and wall_ms < round(SIP_TURN_CLUSTER_MAX_GAP_SECONDS * 1000)
                )
                has_over_modulated_tail = (
                    isinstance(direction_changes, int)
                    and direction_changes > SIP_SINGLE_SHORT_MAX_DIRECTION_CHANGES
                )
                if has_short_tail or has_over_modulated_tail:
                    payload["sipAiRecededCompactTurnNoiseRisk"] = (
                        "short_or_over_modulated_tail_under_elevated_noise"
                    )
                    return True
            return echo_guarded and observation.candidate_duration_ms < required_duration_ms
        if not self._has_sip_strong_compact_deferred_episode_anchor(
            turn,
            elevated_noise=elevated_noise_compact,
        ):
            return True
        return echo_guarded and observation.candidate_duration_ms < required_duration_ms

    def _sip_echo_guarded_compact_short_phrase_pre_stop_extra_payload(
        self,
        *,
        call_id: str,
        payload: dict[str, Any],
        observation: SipBargeInObservation,
    ) -> dict[str, Any] | None:
        if (
            payload.get("sipDeferredEpisodeEvidence")
            != "elevated_noise_compact_two_burst_turn"
        ):
            return None
        if (
            payload.get("sipElevatedNoiseCompactTurnEvidence")
            != "strong_current_modulated_two_burst"
        ):
            return None
        if observation.rms_dbfs is None:
            return None
        if observation.rms_dbfs < SIP_SINGLE_SHORT_MIN_RMS_DBFS + 4.0:
            return None
        ai_rms_dbfs = self._last_ai_audio_rms_dbfs.get(call_id)
        if ai_rms_dbfs is None:
            return None
        uplink_above_ai_db = observation.rms_dbfs - ai_rms_dbfs
        if (
            uplink_above_ai_db
            < SIP_ECHO_GUARDED_COMPACT_SHORT_PHRASE_MIN_UPLINK_ABOVE_AI_DB
        ):
            return None
        pre_stop_payload = dict(payload)
        pre_stop_payload["sipEchoGuardedCompactShortPhraseEvidence"] = (
            "loud_modulated_two_burst"
        )
        pre_stop_payload["sipUplinkAboveAiPlaybackDb"] = round(uplink_above_ai_db, 2)
        pre_stop_payload[
            "sipEchoGuardedCompactShortPhraseMinUplinkAboveAiPlaybackDb"
        ] = SIP_ECHO_GUARDED_COMPACT_SHORT_PHRASE_MIN_UPLINK_ABOVE_AI_DB
        return pre_stop_payload

    @staticmethod
    def _has_sip_strong_compact_deferred_episode_anchor(
        turn: PendingUserTurn,
        *,
        elevated_noise: bool,
    ) -> bool:
        min_rms_dbfs = SIP_SINGLE_SHORT_MIN_RMS_DBFS + (4.0 if elevated_noise else 0.0)
        return (
            turn.sip_deferred_episode_max_rms_dbfs is not None
            and turn.sip_deferred_episode_max_rms_dbfs >= min_rms_dbfs
        )

    def _has_sip_pre_stop_playback_target(
        self,
        *,
        call_id: str,
        trigger_timestamp: datetime,
    ) -> bool:
        guard = self._playback_guard(call_id)
        return (
            bool(guard.current_response_id)
            and (
                guard.current_response_audio_published
                or self._has_recent_ai_audio(call_id, trigger_timestamp)
            )
        )

    def _is_sip_opening_pre_stop_guard_active(
        self,
        *,
        call_id: str,
        trigger_timestamp: datetime,
    ) -> bool:
        if self._response_lifecycle(call_id).current_response_is_opening:
            return True

        opening_started_at: datetime | None = None
        for event in self.event_store.list_all(call_id):
            if event.timestamp > trigger_timestamp:
                continue
            if event.type == "opening_started":
                opening_started_at = event.timestamp
                continue
            if event.type == "model_response_done" and opening_started_at is not None:
                return False
        return opening_started_at is not None

    @staticmethod
    def _sip_pre_stop_shadow_authority_evidence(payload: dict[str, Any]) -> str:
        shadow_evidence = payload.get("sipVadShadowEvidence")
        local_evidence = payload.get("sipVadShadowLocalEvidence")
        if (
            shadow_evidence == "realtime_webrtc_shadow"
            and local_evidence == "local_modulated_candidate"
        ):
            return "realtime_webrtc_shadow_local_modulation"
        if (
            shadow_evidence == "realtime_webrtc_shadow_continuous_context"
            and local_evidence == "local_modulated_candidate"
        ):
            return "continuous_webrtc_shadow_local_modulation"
        if (
            shadow_evidence == "realtime_fsmn_shadow"
            and local_evidence == "local_modulated_candidate"
        ):
            return "realtime_fsmn_shadow_local_modulation"
        if local_evidence == "shadow_turn_cluster":
            return "shadow_turn_cluster"
        return "shadow_local_speech"

    @staticmethod
    def _sip_pre_stop_authority_payload(
        decision: SipPreStopAuthorityDecision,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"sipPreStopAuthority": decision.authority}
        if decision.evidence:
            payload["sipPreStopAuthorityEvidence"] = decision.evidence
        payload.update(decision.extra_payload)
        return payload

    async def _maybe_pre_stop_sip_barge_in_candidate(
        self,
        *,
        call_id: str,
        provider: RealtimeProviderProtocol,
        turn: PendingUserTurn,
        trigger_timestamp: datetime,
        observation: SipBargeInObservation,
        shadow_observations: list[SipVadShadowObservation] | None = None,
    ) -> None:
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

        self._record_sip_deferred_episode_observation(
            call_id=call_id,
            turn=turn,
            timestamp=trigger_timestamp,
            observation=observation,
        )
        decision = self._decide_sip_pre_stop_authority(
            call_id=call_id,
            turn=turn,
            trigger_timestamp=trigger_timestamp,
            observation=observation,
            shadow_observations=shadow_observations,
        )
        if not decision.allowed:
            self._defer_sip_pre_stop_from_authority(
                call_id=call_id,
                turn=turn,
                observation=observation,
                trigger_timestamp=trigger_timestamp,
                decision=decision,
            )
            return

        await self._pre_stop_sip_barge_in_candidate(
            call_id=call_id,
            provider=provider,
            turn=turn,
            trigger_timestamp=trigger_timestamp,
            observation=observation,
            extra_payload=self._sip_pre_stop_authority_payload(decision),
        )

    async def _maybe_create_shadow_assisted_sip_candidate(
        self,
        *,
        call_id: str,
        provider: RealtimeProviderProtocol,
        trigger_timestamp: datetime,
        observation: SipBargeInObservation,
        shadow_observations: list[SipVadShadowObservation],
    ) -> bool:
        if not self.sip_barge_in_fast_stop_enabled:
            return False
        turn = self._pending_user_turns.get(call_id)
        if (
            turn is not None
            and (
                turn.sip_barge_in_requested
                or turn.sip_pre_stop_requested
                or turn.sip_interrupt_rejected
            )
        ):
            return False
        shadow_evidence = self._sip_realtime_shadow_pre_stop_evidence(shadow_observations)
        if shadow_evidence is None:
            return False
        local_evidence = self._sip_shadow_assisted_candidate_local_evidence(
            call_id=call_id,
            observation=observation,
        )
        if local_evidence is None:
            return False
        assisted_observation = replace(
            observation,
            candidate=True,
            candidate_class="stable_speech_candidate",
            reason=SIP_BARGE_IN_INTERRUPT_REASON,
        )
        await self._handle_sip_barge_in_candidate(
            call_id=call_id,
            provider=provider,
            trigger_timestamp=trigger_timestamp,
            observation=assisted_observation,
            extra_payload={
                **shadow_evidence,
                **local_evidence,
                "sipVadShadowCandidateEvidence": shadow_evidence["sipVadShadowEvidence"],
            },
            allow_pre_stop=False,
            shadow_observations=shadow_observations,
        )
        return True

    async def _maybe_upgrade_deferred_sip_pre_stop_from_shadow(
        self,
        *,
        call_id: str,
        provider: RealtimeProviderProtocol,
        trigger_timestamp: datetime,
        observation: SipBargeInObservation,
        shadow_observations: list[SipVadShadowObservation],
    ) -> bool:
        if not self.sip_barge_in_fast_stop_enabled:
            return False
        turn = self._pending_user_turns.get(call_id)
        if (
            turn is None
            or not turn.sip_barge_in_requested
            or not turn.sip_pre_stop_deferred
            or turn.sip_pre_stop_requested
            or turn.sip_interrupt_rejected
        ):
            return False
        if observation.candidate_duration_ms < self.sip_barge_in_config.candidate_min_duration_ms:
            return False
        if self._expire_sip_candidate_response_mismatch_if_needed(
            call_id=call_id,
            turn=turn,
            trigger_timestamp=trigger_timestamp,
            observation=observation,
        ):
            return False
        if self._expire_stale_deferred_sip_candidate_if_needed(
            call_id=call_id,
            turn=turn,
            trigger_timestamp=trigger_timestamp,
                observation=observation,
            ):
            return False
        decision = self._decide_sip_pre_stop_authority(
            call_id=call_id,
            turn=turn,
            trigger_timestamp=trigger_timestamp,
            observation=observation,
            shadow_observations=shadow_observations,
        )
        if not decision.allowed:
            return False
        await self._pre_stop_sip_barge_in_candidate(
            call_id=call_id,
            provider=provider,
            turn=turn,
            trigger_timestamp=trigger_timestamp,
            observation=observation,
            extra_payload=self._sip_pre_stop_authority_payload(decision),
        )
        return True

    def _sip_realtime_shadow_pre_stop_evidence(
        self,
        shadow_observations: list[SipVadShadowObservation],
    ) -> dict[str, Any] | None:
        for observation in shadow_observations:
            evidence = self._sip_realtime_shadow_pre_stop_observation_evidence(observation)
            if evidence is not None:
                return evidence
        return None

    def _sip_realtime_continuous_shadow_context_evidence(
        self,
        shadow_observations: list[SipVadShadowObservation],
    ) -> dict[str, Any] | None:
        for observation in shadow_observations:
            evidence = self._sip_realtime_continuous_shadow_context_observation_evidence(
                observation
            )
            if evidence is not None:
                return evidence
        return None

    def _sip_realtime_continuous_shadow_context_observation_evidence(
        self,
        observation: SipVadShadowObservation,
    ) -> dict[str, Any] | None:
        if observation.error_type is not None or not observation.active:
            return None
        if observation.detector != "webrtc_shadow":
            return None
        window_ms = max(0, observation.duration_ms)
        if window_ms <= SIP_REALTIME_SHADOW_PRE_STOP_MAX_WINDOW_MS:
            return None
        if window_ms > SIP_REALTIME_SHADOW_CONTEXT_MAX_WINDOW_MS:
            return None
        payload = self._sip_shadow_evidence_payload(
            observation,
            window_ms=window_ms,
        )
        payload["sipVadShadowEvidence"] = "realtime_webrtc_shadow_continuous_context"
        return payload

    def _sip_realtime_shadow_turn_cluster_evidence(
        self,
        shadow_observations: list[SipVadShadowObservation],
    ) -> dict[str, Any] | None:
        for observation in shadow_observations:
            evidence = self._sip_analyzed_shadow_observation_evidence(observation)
            if evidence is not None:
                return evidence
        return None

    def _sip_realtime_shadow_pre_stop_observation_evidence(
        self,
        observation: SipVadShadowObservation,
    ) -> dict[str, Any] | None:
        analyzed_evidence = self._sip_analyzed_shadow_observation_evidence(observation)
        if analyzed_evidence is not None:
            return analyzed_evidence
        if observation.error_type is not None or not observation.active:
            return None
        if observation.detector != "webrtc_shadow":
            return None
        window_ms = max(0, observation.duration_ms)
        if window_ms < SIP_REALTIME_SHADOW_PRE_STOP_MIN_WINDOW_MS:
            return None
        if window_ms > SIP_REALTIME_SHADOW_PRE_STOP_MAX_WINDOW_MS:
            return None
        return self._sip_shadow_evidence_payload(
            observation,
            window_ms=window_ms,
        )

    def _record_sip_recent_shadow_pre_stop_evidence(
        self,
        *,
        call_id: str,
        trigger_timestamp: datetime,
        shadow_observations: list[SipVadShadowObservation],
    ) -> None:
        shadow_evidence = self._sip_realtime_shadow_pre_stop_evidence(shadow_observations)
        if shadow_evidence is None:
            return
        turn = self._pending_turn(call_id, reset_if_finished=True)
        guard = self._playback_guard(call_id)
        response_id = guard.current_response_id
        if response_id != turn.sip_recent_shadow_response_id:
            self._reset_sip_recent_shadow_evidence(turn, response_id=response_id)
        turn.sip_recent_shadow_response_id = response_id
        turn.sip_recent_shadow_at = trigger_timestamp
        turn.sip_recent_shadow_evidence = shadow_evidence["sipVadShadowEvidence"]
        detector = shadow_evidence.get("sipVadShadowDetector")
        turn.sip_recent_shadow_detector = detector if isinstance(detector, str) else None
        window_ms = shadow_evidence.get("sipVadShadowWindowMs")
        turn.sip_recent_shadow_window_ms = window_ms if isinstance(window_ms, int) else None

    @staticmethod
    def _reset_sip_recent_shadow_evidence(
        turn: PendingUserTurn,
        *,
        response_id: str | None,
    ) -> None:
        turn.sip_recent_shadow_response_id = response_id
        turn.sip_recent_shadow_at = None
        turn.sip_recent_shadow_evidence = None
        turn.sip_recent_shadow_detector = None
        turn.sip_recent_shadow_window_ms = None

    def _sip_recent_shadow_pre_stop_evidence(
        self,
        *,
        call_id: str,
        turn: PendingUserTurn,
        trigger_timestamp: datetime,
    ) -> dict[str, Any] | None:
        if turn.sip_recent_shadow_at is None or turn.sip_recent_shadow_evidence is None:
            return None
        guard = self._playback_guard(call_id)
        if guard.current_response_id != turn.sip_recent_shadow_response_id:
            self._reset_sip_recent_shadow_evidence(
                turn,
                response_id=guard.current_response_id,
            )
            return None
        age_seconds = (trigger_timestamp - turn.sip_recent_shadow_at).total_seconds()
        if age_seconds < 0 or age_seconds > SIP_REALTIME_SHADOW_EVIDENCE_MAX_AGE_SECONDS:
            self._reset_sip_recent_shadow_evidence(
                turn,
                response_id=turn.sip_recent_shadow_response_id,
            )
            return None
        payload: dict[str, Any] = {
            "sipVadShadowEvidence": turn.sip_recent_shadow_evidence,
        }
        if turn.sip_recent_shadow_detector:
            payload["sipVadShadowDetector"] = turn.sip_recent_shadow_detector
        if turn.sip_recent_shadow_window_ms is not None:
            payload["sipVadShadowWindowMs"] = turn.sip_recent_shadow_window_ms
        return payload

    @staticmethod
    def _sip_continuous_shadow_context_local_pre_stop_evidence(
        *,
        has_clear_short: bool,
    ) -> dict[str, Any] | None:
        if not has_clear_short:
            return None
        return {"sipVadShadowLocalEvidence": "local_modulated_candidate"}

    @staticmethod
    def _sip_analyzed_shadow_observation_evidence(
        observation: SipVadShadowObservation,
    ) -> dict[str, Any] | None:
        if observation.error_type is not None:
            return None
        if not (observation.active and observation.started and observation.analyzed):
            return None
        window_ms = None
        if observation.window_start_ms is not None and observation.window_end_ms is not None:
            window_ms = max(0, observation.window_end_ms - observation.window_start_ms)
        if window_ms is None:
            window_ms = observation.duration_ms
        if window_ms < SIP_REALTIME_SHADOW_PRE_STOP_MIN_WINDOW_MS:
            return None
        if window_ms > SIP_REALTIME_SHADOW_PRE_STOP_MAX_WINDOW_MS:
            return None
        return RealtimeCallAgentRunner._sip_shadow_evidence_payload(
            observation,
            window_ms=window_ms,
        )

    @staticmethod
    def _sip_shadow_evidence_payload(
        observation: SipVadShadowObservation,
        *,
        window_ms: int,
    ) -> dict[str, Any]:
        evidence = (
            "realtime_webrtc_shadow"
            if observation.detector == "webrtc_shadow"
            else "realtime_fsmn_shadow"
        )
        return {
            "sipVadShadowEvidence": evidence,
            "sipVadShadowDetector": observation.detector,
            "sipVadShadowWindowMs": window_ms,
        }

    def _sip_realtime_shadow_local_pre_stop_evidence(
        self,
        *,
        call_id: str,
        turn: PendingUserTurn,
        observation: SipBargeInObservation,
    ) -> dict[str, Any] | None:
        if observation.candidate_class != "stable_speech_candidate":
            return None
        if observation.rms_dbfs is None or observation.snr_db is None:
            return None
        if observation.vad_voiced_ms < self.sip_barge_in_config.vad_voiced_duration_ms:
            return None
        cluster_payload = self._sip_turn_cluster_pre_stop_extra_payload(
            call_id,
            turn,
            observation=observation,
            quality_rejection=self._sip_observation_quality_rejection(call_id),
        )
        if (
            cluster_payload is not None
            and cluster_payload.get("sipVadShadowLocalEvidence") == "shadow_turn_cluster"
            and observation.snr_db >= self.sip_barge_in_config.snr_threshold_db
        ):
            return {"sipVadShadowLocalEvidence": "shadow_turn_cluster"}
        if not self._has_sip_shadow_backed_local_modulation(call_id, observation):
            return None
        return {"sipVadShadowLocalEvidence": "local_modulated_candidate"}

    def _has_sip_shadow_backed_local_modulation(
        self,
        call_id: str,
        observation: SipBargeInObservation,
    ) -> bool:
        if observation.candidate_class != "stable_speech_candidate":
            return False
        if observation.snr_db is None:
            return False
        min_duration_ms = (
            self.sip_barge_in_config.candidate_min_duration_ms
            + observation.frame_duration_ms
        )
        if observation.candidate_duration_ms < min_duration_ms:
            return False
        detector = self._sip_barge_in_detector
        if detector is None:
            return False
        diagnostics = detector.latest_observation_payload(call_id)
        if diagnostics.get("speechQualityRejection") is not None:
            return False
        rms_range_db = diagnostics.get("rmsRangeDb")
        direction_changes = diagnostics.get("rmsDirectionChanges")
        if not isinstance(rms_range_db, int | float):
            return False
        if not isinstance(direction_changes, int):
            return False
        return (
            rms_range_db >= self.sip_barge_in_config.turn_taking_min_range_db
            and direction_changes >= 1
            and observation.snr_db
            >= max(
                self.sip_barge_in_config.snr_threshold_db,
                SIP_REALTIME_SHADOW_MIN_LOCAL_SNR_DB,
            )
        )

    def _sip_shadow_local_quality_rejection(
        self,
        *,
        call_id: str,
        observation: SipBargeInObservation,
        local_shadow_evidence: dict[str, Any],
    ) -> str | None:
        if local_shadow_evidence.get("sipVadShadowLocalEvidence") != "local_modulated_candidate":
            return None
        if self._has_sip_unstable_local_envelope_risk(call_id, observation):
            return "unstable_local_envelope"
        return None

    def _has_sip_unstable_local_envelope_risk(
        self,
        call_id: str,
        observation: SipBargeInObservation,
    ) -> bool:
        if observation.candidate_class != "stable_speech_candidate":
            return False
        if observation.candidate_duration_ms >= SIP_FAST_LOCAL_MIN_DURATION_MS:
            return False
        detector = self._sip_barge_in_detector
        if detector is None:
            return False
        diagnostics = detector.latest_observation_payload(call_id)
        rms_range_db = diagnostics.get("rmsRangeDb")
        direction_changes = diagnostics.get("rmsDirectionChanges")
        large_jumps = diagnostics.get("largeRmsJumpCount")
        if not isinstance(rms_range_db, int | float):
            return False
        if not isinstance(direction_changes, int):
            return False
        if not isinstance(large_jumps, int):
            return False
        return (
            rms_range_db
            >= max(
                self.sip_barge_in_config.non_speech_envelope_min_range_db,
                SIP_UNSTABLE_LOCAL_ENVELOPE_MIN_RMS_RANGE_DB,
            )
            and direction_changes >= SIP_UNSTABLE_LOCAL_ENVELOPE_MIN_DIRECTION_CHANGES
            and large_jumps >= SIP_UNSTABLE_LOCAL_ENVELOPE_MIN_LARGE_JUMPS
        )

    def _sip_shadow_assisted_candidate_local_evidence(
        self,
        *,
        call_id: str,
        observation: SipBargeInObservation,
    ) -> dict[str, Any] | None:
        if not observation.active or observation.candidate:
            return None
        if observation.rms_dbfs is None or observation.snr_db is None:
            return None
        if observation.vad_voiced_ms < self.sip_barge_in_config.vad_voiced_duration_ms:
            return None
        if observation.snr_db < max(
            self.sip_barge_in_config.snr_threshold_db,
            SIP_REALTIME_SHADOW_MIN_LOCAL_SNR_DB,
        ):
            return None
        detector = self._sip_barge_in_detector
        if detector is None:
            return None
        diagnostics = detector.latest_observation_payload(call_id)
        if diagnostics.get("speechQualityRejection") is not None:
            return None
        rms_range_db = diagnostics.get("rmsRangeDb")
        direction_changes = diagnostics.get("rmsDirectionChanges")
        if not isinstance(rms_range_db, int | float):
            return None
        if not isinstance(direction_changes, int):
            return None
        if rms_range_db < self.sip_barge_in_config.turn_taking_min_range_db:
            return None
        if direction_changes < 1:
            return None
        return {"sipVadShadowLocalEvidence": "local_modulated_candidate"}

    def _defer_sip_pre_stop(
        self,
        *,
        call_id: str,
        turn: PendingUserTurn,
        observation: SipBargeInObservation,
        reason: str,
        required_duration_ms: int,
        extra_payload: dict[str, Any] | None = None,
    ) -> None:
        if turn.sip_pre_stop_deferred:
            return
        turn.sip_pre_stop_deferred = True
        payload = self._sip_barge_in_event_payload(call_id, observation)
        payload.update({
            "reason": reason,
            "requiredPreStopDurationMs": required_duration_ms,
        })
        if extra_payload:
            payload.update(extra_payload)
        self._append_event(call_id, "sip_pre_stop_deferred", "agent", payload)

    def _defer_sip_pre_stop_from_authority(
        self,
        *,
        call_id: str,
        turn: PendingUserTurn,
        observation: SipBargeInObservation,
        trigger_timestamp: datetime,
        decision: SipPreStopAuthorityDecision,
    ) -> None:
        extra_payload = self._sip_pre_stop_authority_payload(decision)
        if decision.reason == "awaiting_ai_playback_echo_guard":
            self._defer_sip_ai_playback_echo_pre_stop(
                call_id=call_id,
                turn=turn,
                observation=observation,
                trigger_timestamp=trigger_timestamp,
                extra_payload=extra_payload,
            )
            return
        self._defer_sip_pre_stop(
            call_id=call_id,
            turn=turn,
            observation=observation,
            reason=decision.reason,
            required_duration_ms=decision.required_duration_ms,
            extra_payload=extra_payload,
        )

    def _is_sip_ai_playback_echo_like(
        self,
        *,
        call_id: str,
        observation: SipBargeInObservation,
        trigger_timestamp: datetime,
    ) -> bool:
        if observation.candidate_class != "stable_speech_candidate":
            return False
        if observation.rms_dbfs is None:
            return False
        if not self._has_recent_ai_audio(call_id, trigger_timestamp):
            return False
        ai_rms_dbfs = self._last_ai_audio_rms_dbfs.get(call_id)
        if ai_rms_dbfs is None or ai_rms_dbfs < self.sip_barge_in_config.rms_threshold_dbfs:
            return False
        if self._has_sip_single_short_pre_stop_evidence(call_id, observation):
            return False
        if self._has_sip_clear_short_modulated_pre_stop_evidence(call_id, observation):
            return False
        if self._has_sip_modulated_pre_stop_local_speech(call_id, observation):
            return False
        return observation.rms_dbfs <= ai_rms_dbfs + SIP_AI_PLAYBACK_ECHO_UPLINK_MARGIN_DB

    def _has_sip_modulated_pre_stop_local_speech(
        self,
        call_id: str,
        observation: SipBargeInObservation,
    ) -> bool:
        if observation.candidate_class != "stable_speech_candidate":
            return False
        if observation.snr_db is None:
            return False
        if observation.candidate_duration_ms < self.sip_barge_in_config.pre_stop_min_duration_ms:
            return False
        detector = self._sip_barge_in_detector
        if detector is None:
            return False
        diagnostics = detector.latest_observation_payload(call_id)
        if diagnostics.get("speechQualityRejection") is not None:
            return False
        rms_range_db = diagnostics.get("rmsRangeDb")
        direction_changes = diagnostics.get("rmsDirectionChanges")
        if not isinstance(rms_range_db, int | float):
            return False
        if not isinstance(direction_changes, int):
            return False
        return (
            rms_range_db >= self.sip_barge_in_config.turn_taking_min_range_db
            and direction_changes >= 1
            and observation.snr_db >= max(self.sip_barge_in_config.snr_threshold_db, 20.0)
        )

    def _defer_sip_ai_playback_echo_pre_stop(
        self,
        *,
        call_id: str,
        turn: PendingUserTurn,
        observation: SipBargeInObservation,
        trigger_timestamp: datetime,
        extra_payload: dict[str, Any] | None = None,
    ) -> None:
        turn.sip_pre_stop_deferred = True
        if turn.sip_ai_playback_echo_deferred:
            return
        turn.sip_ai_playback_echo_deferred = True
        payload = self._sip_barge_in_event_payload(call_id, observation)
        ai_rms_dbfs = self._last_ai_audio_rms_dbfs.get(call_id)
        last_published_at = self._last_ai_audio_published_at.get(call_id)
        playback_age_ms = None
        if last_published_at is not None:
            playback_age_ms = round(
                max(0.0, (trigger_timestamp - last_published_at).total_seconds() * 1000)
            )
        payload.update({
            "reason": "awaiting_ai_playback_echo_guard",
            "aiPlaybackRmsDbfs": round(ai_rms_dbfs, 2) if ai_rms_dbfs is not None else None,
            "aiPlaybackAgeMs": playback_age_ms,
            "uplinkAboveAiPlaybackDb": (
                round(observation.rms_dbfs - ai_rms_dbfs, 2)
                if ai_rms_dbfs is not None and observation.rms_dbfs is not None
                else None
            ),
            "allowedUplinkAboveAiPlaybackDb": SIP_AI_PLAYBACK_ECHO_UPLINK_MARGIN_DB,
        })
        if extra_payload:
            payload.update(extra_payload)
        self._append_event(call_id, "sip_ai_playback_echo_deferred", "agent", payload)

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
        quality_rejection = self._sip_observation_quality_rejection(call_id)
        if (
            quality_rejection is not None
            and quality_rejection not in SIP_TURN_CLUSTER_RECOVERABLE_QUALITY_REJECTIONS
        ):
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
        detector = self._sip_barge_in_detector
        diagnostics = detector.latest_observation_payload(call_id) if detector is not None else {}
        max_snr_db = diagnostics.get("maxSnrDb")
        if isinstance(max_snr_db, int | float):
            turn.sip_turn_cluster_max_snr_db = (
                float(max_snr_db)
                if turn.sip_turn_cluster_max_snr_db is None
                else max(turn.sip_turn_cluster_max_snr_db, float(max_snr_db))
            )
        rms_range_db = diagnostics.get("rmsRangeDb")
        if isinstance(rms_range_db, int | float):
            turn.sip_turn_cluster_max_rms_range_db = (
                float(rms_range_db)
                if turn.sip_turn_cluster_max_rms_range_db is None
                else max(turn.sip_turn_cluster_max_rms_range_db, float(rms_range_db))
            )

    def _record_sip_turn_cluster_shadow_observations(
        self,
        *,
        call_id: str,
        trigger_timestamp: datetime,
        shadow_observations: list[SipVadShadowObservation],
    ) -> None:
        shadow_evidence = self._sip_realtime_shadow_turn_cluster_evidence(shadow_observations)
        if shadow_evidence is None:
            return
        window_ms = shadow_evidence.get("sipVadShadowWindowMs")
        if not isinstance(window_ms, int):
            return
        turn = self._pending_turn(call_id, reset_if_finished=True)
        guard = self._playback_guard(call_id)
        response_id = guard.current_response_id
        if response_id != turn.sip_turn_cluster_response_id:
            self._reset_sip_turn_cluster(turn, response_id=response_id)
        if (
            turn.sip_turn_cluster_last_at is not None
            and (trigger_timestamp - turn.sip_turn_cluster_last_at).total_seconds()
            > SIP_TURN_CLUSTER_MAX_GAP_SECONDS
        ):
            self._reset_sip_turn_cluster(turn, response_id=response_id)

        if turn.sip_turn_cluster_first_at is None:
            turn.sip_turn_cluster_first_at = trigger_timestamp
        turn.sip_turn_cluster_last_at = trigger_timestamp
        turn.sip_turn_cluster_burst_count += 1
        turn.sip_turn_cluster_shadow_burst_count += 1
        turn.sip_turn_cluster_voiced_ms += window_ms
        turn.sip_turn_cluster_shadow_voiced_ms += window_ms
        detector = shadow_evidence.get("sipVadShadowDetector")
        if isinstance(detector, str):
            turn.sip_turn_cluster_shadow_detector = detector
        turn.sip_turn_cluster_shadow_window_ms = window_ms

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
        turn.sip_turn_cluster_shadow_burst_count = 0
        turn.sip_turn_cluster_shadow_voiced_ms = 0
        turn.sip_turn_cluster_shadow_detector = None
        turn.sip_turn_cluster_shadow_window_ms = None
        turn.sip_turn_cluster_min_rms_dbfs = None
        turn.sip_turn_cluster_max_rms_dbfs = None
        turn.sip_turn_cluster_max_snr_db = None
        turn.sip_turn_cluster_max_rms_range_db = None

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
            self._sip_turn_cluster_rms_range_db(turn) is None
            or self._sip_turn_cluster_rms_range_db(turn) < SIP_TURN_CLUSTER_MIN_RMS_RANGE_DB
        ):
            return False
        return True

    def _sip_turn_cluster_pre_stop_extra_payload(
        self,
        call_id: str,
        turn: PendingUserTurn,
        *,
        observation: SipBargeInObservation,
        quality_rejection: str | None,
    ) -> dict[str, Any] | None:
        if not self._has_sip_turn_cluster_pre_stop_evidence(turn):
            return None
        if quality_rejection is None:
            if turn.sip_turn_cluster_shadow_burst_count <= 0:
                if not (
                    self._has_compact_local_only_sip_turn_cluster(turn)
                    and self._has_current_sip_turn_cluster_anchor(
                        call_id,
                        observation,
                    )
                ):
                    return None
            return self._sip_turn_cluster_shadow_payload(turn)
        if (
            turn.sip_turn_cluster_shadow_burst_count > 0
            and quality_rejection in SIP_TURN_CLUSTER_RECOVERABLE_QUALITY_REJECTIONS
        ):
            return self._sip_turn_cluster_shadow_payload(turn)
        return None

    def _has_compact_local_only_sip_turn_cluster(self, turn: PendingUserTurn) -> bool:
        wall_ms = self._sip_turn_cluster_wall_ms(turn)
        return (
            wall_ms is not None
            and wall_ms <= SIP_TURN_CLUSTER_LOCAL_ONLY_MAX_WALL_MS
        )

    def _has_current_sip_turn_cluster_anchor(
        self,
        call_id: str,
        observation: SipBargeInObservation,
    ) -> bool:
        if observation.snr_db is None or observation.snr_db < SIP_TURN_CLUSTER_MIN_SNR_DB:
            return False
        detector = self._sip_barge_in_detector
        if detector is None:
            return False
        diagnostics = detector.latest_observation_payload(call_id)
        if diagnostics.get("speechQualityRejection") is not None:
            return False
        rms_range_db = diagnostics.get("rmsRangeDb")
        if not isinstance(rms_range_db, int | float):
            return False
        return rms_range_db >= SIP_TURN_CLUSTER_MIN_RMS_RANGE_DB

    def _has_sip_fast_local_pre_stop_authority(
        self,
        *,
        observation: SipBargeInObservation,
    ) -> bool:
        if observation.candidate_class == "strong_short_speech_candidate":
            return (
                observation.candidate_duration_ms
                >= self.sip_barge_in_config.short_speech_min_duration_ms
            )
        return observation.candidate_duration_ms >= SIP_FAST_LOCAL_MIN_DURATION_MS

    @staticmethod
    def _sip_turn_cluster_rms_range_db(turn: PendingUserTurn) -> float | None:
        rms_range_db = turn.sip_turn_cluster_max_rms_range_db
        if (
            turn.sip_turn_cluster_min_rms_dbfs is not None
            and turn.sip_turn_cluster_max_rms_dbfs is not None
        ):
            observed_range_db = (
                turn.sip_turn_cluster_max_rms_dbfs - turn.sip_turn_cluster_min_rms_dbfs
            )
            rms_range_db = (
                observed_range_db
                if rms_range_db is None
                else max(rms_range_db, observed_range_db)
            )
        return rms_range_db

    @staticmethod
    def _sip_turn_cluster_wall_ms(turn: PendingUserTurn) -> int | None:
        if (
            turn.sip_turn_cluster_first_at is None
            or turn.sip_turn_cluster_last_at is None
        ):
            return None
        return round(
            max(
                0.0,
                (
                    turn.sip_turn_cluster_last_at - turn.sip_turn_cluster_first_at
                ).total_seconds()
                * 1000,
            )
        )

    @staticmethod
    def _sip_turn_cluster_shadow_payload(turn: PendingUserTurn) -> dict[str, Any]:
        if turn.sip_turn_cluster_shadow_burst_count <= 0:
            return {}
        payload: dict[str, Any] = {
            "sipVadShadowEvidence": "realtime_fsmn_shadow",
            "sipVadShadowLocalEvidence": "shadow_turn_cluster",
        }
        if turn.sip_turn_cluster_shadow_detector:
            payload["sipVadShadowDetector"] = turn.sip_turn_cluster_shadow_detector
        if turn.sip_turn_cluster_shadow_window_ms is not None:
            payload["sipVadShadowWindowMs"] = turn.sip_turn_cluster_shadow_window_ms
        return payload

    def _record_sip_deferred_episode_observation(
        self,
        *,
        call_id: str,
        turn: PendingUserTurn,
        timestamp: datetime,
        observation: SipBargeInObservation,
    ) -> None:
        guard = self._playback_guard(call_id)
        response_id = guard.current_response_id
        generation = guard.generation
        if (
            response_id != turn.sip_deferred_episode_response_id
            or generation != turn.sip_deferred_episode_generation
        ):
            self._reset_sip_deferred_episode(
                turn,
                response_id=response_id,
                generation=generation,
            )
        if not self._has_sip_pre_stop_playback_target(
            call_id=call_id,
            trigger_timestamp=timestamp,
        ):
            return
        if observation.candidate_class != "stable_speech_candidate":
            return
        if observation.rms_dbfs is None or observation.snr_db is None:
            return
        if observation.candidate_duration_ms < self.sip_barge_in_config.candidate_min_duration_ms:
            return
        quality_rejection = self._sip_observation_quality_rejection(call_id)
        if (
            quality_rejection is not None
            and quality_rejection not in SIP_TURN_CLUSTER_RECOVERABLE_QUALITY_REJECTIONS
        ):
            if quality_rejection in SIP_TURN_EVIDENCE_IGNORED_QUALITY_REJECTIONS:
                return
            self._reset_sip_deferred_episode(
                turn,
                response_id=response_id,
                generation=generation,
            )
            return

        gap_seconds: float | None = None
        gap_ms: int | None = None
        if turn.sip_deferred_episode_last_at is not None:
            gap_seconds = (timestamp - turn.sip_deferred_episode_last_at).total_seconds()
            gap_ms = round(max(0.0, gap_seconds * 1000))

        if (
            gap_seconds is not None
            and gap_seconds > SIP_DEFERRED_TURN_MAX_GAP_SECONDS
        ):
            self._reset_sip_deferred_episode(
                turn,
                response_id=response_id,
                generation=generation,
            )
            gap_ms = None
        if (
            turn.sip_deferred_episode_first_at is not None
            and (timestamp - turn.sip_deferred_episode_first_at).total_seconds() * 1000
            > SIP_DEFERRED_TURN_MAX_WALL_MS
        ):
            self._reset_sip_deferred_episode(
                turn,
                response_id=response_id,
                generation=generation,
            )
            gap_ms = None

        starts_new_burst = (
            turn.sip_deferred_episode_last_at is None
            or (timestamp - turn.sip_deferred_episode_last_at).total_seconds()
            > SIP_DEFERRED_EPISODE_SAME_BURST_GAP_SECONDS
        )
        if starts_new_burst:
            if turn.sip_deferred_episode_first_at is None:
                turn.sip_deferred_episode_first_at = timestamp
            turn.sip_deferred_episode_burst_count += 1
            turn.sip_deferred_episode_current_burst_voiced_ms = 0
            if gap_ms is not None:
                turn.sip_deferred_episode_max_gap_ms = (
                    gap_ms
                    if turn.sip_deferred_episode_max_gap_ms is None
                    else max(turn.sip_deferred_episode_max_gap_ms, gap_ms)
                )

        turn.sip_deferred_episode_last_at = timestamp
        added_voiced_ms = max(
            0,
            observation.vad_voiced_ms
            - turn.sip_deferred_episode_current_burst_voiced_ms,
        )
        turn.sip_deferred_episode_voiced_ms += added_voiced_ms
        turn.sip_deferred_episode_current_burst_voiced_ms = max(
            turn.sip_deferred_episode_current_burst_voiced_ms,
            observation.vad_voiced_ms,
        )
        turn.sip_deferred_episode_min_rms_dbfs = (
            observation.rms_dbfs
            if turn.sip_deferred_episode_min_rms_dbfs is None
            else min(turn.sip_deferred_episode_min_rms_dbfs, observation.rms_dbfs)
        )
        turn.sip_deferred_episode_max_rms_dbfs = (
            observation.rms_dbfs
            if turn.sip_deferred_episode_max_rms_dbfs is None
            else max(turn.sip_deferred_episode_max_rms_dbfs, observation.rms_dbfs)
        )
        turn.sip_deferred_episode_max_snr_db = (
            observation.snr_db
            if turn.sip_deferred_episode_max_snr_db is None
            else max(turn.sip_deferred_episode_max_snr_db, observation.snr_db)
        )
        detector = self._sip_barge_in_detector
        diagnostics = detector.latest_observation_payload(call_id) if detector is not None else {}
        max_snr_db = diagnostics.get("maxSnrDb")
        if isinstance(max_snr_db, int | float):
            turn.sip_deferred_episode_max_snr_db = (
                float(max_snr_db)
                if turn.sip_deferred_episode_max_snr_db is None
                else max(turn.sip_deferred_episode_max_snr_db, float(max_snr_db))
            )
        rms_range_db = diagnostics.get("rmsRangeDb")
        if isinstance(rms_range_db, int | float):
            turn.sip_deferred_episode_max_rms_range_db = (
                float(rms_range_db)
                if turn.sip_deferred_episode_max_rms_range_db is None
                else max(turn.sip_deferred_episode_max_rms_range_db, float(rms_range_db))
            )

    @staticmethod
    def _reset_sip_deferred_episode(
        turn: PendingUserTurn,
        *,
        response_id: str | None,
        generation: int | None,
    ) -> None:
        turn.sip_deferred_episode_response_id = response_id
        turn.sip_deferred_episode_generation = generation
        turn.sip_deferred_episode_first_at = None
        turn.sip_deferred_episode_last_at = None
        turn.sip_deferred_episode_burst_count = 0
        turn.sip_deferred_episode_voiced_ms = 0
        turn.sip_deferred_episode_current_burst_voiced_ms = 0
        turn.sip_deferred_episode_min_rms_dbfs = None
        turn.sip_deferred_episode_max_rms_dbfs = None
        turn.sip_deferred_episode_max_snr_db = None
        turn.sip_deferred_episode_max_rms_range_db = None
        turn.sip_deferred_episode_max_gap_ms = None

    def _sip_deferred_episode_pre_stop_extra_payload(
        self,
        call_id: str,
        turn: PendingUserTurn,
        *,
        observation: SipBargeInObservation,
        quality_rejection: str | None,
    ) -> dict[str, Any] | None:
        if (
            quality_rejection is not None
            and quality_rejection not in SIP_TURN_CLUSTER_RECOVERABLE_QUALITY_REJECTIONS
        ):
            return None
        if (
            turn.sip_deferred_episode_burst_count
            < SIP_DEFERRED_EPISODE_COMPACT_MIN_BURSTS
        ):
            return None
        if (
            turn.sip_deferred_episode_voiced_ms
            < SIP_DEFERRED_EPISODE_COMPACT_MIN_VOICED_MS
        ):
            return None
        if (
            turn.sip_deferred_episode_max_snr_db is None
            or turn.sip_deferred_episode_max_snr_db
            < SIP_DEFERRED_TURN_MIN_SNR_DB
        ):
            return None
        rms_range_db = self._sip_deferred_episode_rms_range_db(turn)
        if (
            rms_range_db is None
            or rms_range_db < SIP_DEFERRED_EPISODE_MIN_RMS_RANGE_DB
        ):
            return None
        wall_ms = self._sip_deferred_episode_wall_ms(turn)
        if wall_ms is None:
            return None
        max_gap_ms = turn.sip_deferred_episode_max_gap_ms or 0
        elevated_noise_compact_turn = (
            self._has_sip_elevated_noise_compact_deferred_turn_evidence(
                call_id,
                turn,
                observation,
                rms_range_db=rms_range_db,
                wall_ms=wall_ms,
                max_gap_ms=max_gap_ms,
            )
        )
        ai_receded_compact_turn = (
            self._has_sip_ai_receded_compact_deferred_turn_evidence(
                call_id,
                turn,
                observation,
                rms_range_db=rms_range_db,
                wall_ms=wall_ms,
                max_gap_ms=max_gap_ms,
            )
        )
        compact_two_burst_turn = (
            turn.sip_deferred_episode_burst_count
            >= SIP_DEFERRED_EPISODE_COMPACT_MIN_BURSTS
            and turn.sip_deferred_episode_voiced_ms
            >= SIP_DEFERRED_EPISODE_COMPACT_MIN_VOICED_MS
            and wall_ms <= SIP_DEFERRED_EPISODE_COMPACT_MAX_WALL_MS
            and max_gap_ms <= SIP_DEFERRED_EPISODE_COMPACT_MAX_GAP_MS
            and turn.sip_deferred_episode_max_snr_db
            >= max(
                SIP_DEFERRED_EPISODE_COMPACT_MIN_SNR_DB,
                self.sip_barge_in_config.snr_threshold_db,
            )
            and rms_range_db
            >= max(
                SIP_DEFERRED_EPISODE_COMPACT_MIN_RMS_RANGE_DB,
                self.sip_barge_in_config.turn_taking_min_range_db + 2.0,
            )
            and (
                not self._has_sip_elevated_noise_short_burst_risk(observation)
                or elevated_noise_compact_turn
            )
        )
        compact_episode = (
            turn.sip_deferred_episode_burst_count >= SIP_DEFERRED_EPISODE_MIN_BURSTS
            and turn.sip_deferred_episode_voiced_ms >= SIP_DEFERRED_EPISODE_MIN_VOICED_MS
            and wall_ms <= SIP_DEFERRED_EPISODE_MAX_WALL_MS
            and max_gap_ms <= round(SIP_DEFERRED_EPISODE_MAX_GAP_SECONDS * 1000)
            and turn.sip_deferred_episode_max_snr_db >= SIP_DEFERRED_EPISODE_MIN_SNR_DB
            and rms_range_db >= SIP_DEFERRED_EPISODE_MIN_RMS_RANGE_DB
        )
        elevated_noise_sparse_turn = (
            self._has_sip_elevated_noise_sparse_deferred_turn_evidence(
                call_id,
                turn,
                observation,
                rms_range_db=rms_range_db,
                wall_ms=wall_ms,
                max_gap_ms=max_gap_ms,
            )
        )
        high_noise_sparse_risk = self._has_sip_high_noise_sparse_deferred_episode_risk(
            observation,
            max_gap_ms=max_gap_ms,
        )
        sparse_turn = (
            turn.sip_deferred_episode_burst_count >= SIP_DEFERRED_EPISODE_MIN_BURSTS
            and turn.sip_deferred_episode_voiced_ms >= SIP_DEFERRED_EPISODE_MIN_VOICED_MS
            and wall_ms <= SIP_DEFERRED_TURN_MAX_WALL_MS
            and max_gap_ms <= round(SIP_DEFERRED_TURN_MAX_GAP_SECONDS * 1000)
            and rms_range_db >= SIP_DEFERRED_TURN_MIN_RMS_RANGE_DB
            and turn.sip_deferred_episode_max_snr_db
            >= max(
                SIP_DEFERRED_TURN_MIN_SNR_DB,
                self.sip_barge_in_config.snr_threshold_db,
            )
            and (
                not self._has_sip_elevated_noise_short_burst_risk(observation)
                or elevated_noise_sparse_turn
            )
            and (not high_noise_sparse_risk or elevated_noise_sparse_turn)
        )
        if not (
            compact_two_burst_turn
            or ai_receded_compact_turn
            or compact_episode
            or sparse_turn
        ):
            return None
        payload = {
            "sipDeferredEpisodeBurstCount": turn.sip_deferred_episode_burst_count,
            "sipDeferredEpisodeVoicedMs": turn.sip_deferred_episode_voiced_ms,
            "sipDeferredEpisodeWallMs": wall_ms,
            "sipDeferredEpisodeMaxGapMs": max_gap_ms,
            "sipDeferredEpisodeRmsRangeDb": round(rms_range_db, 2),
            "sipDeferredEpisodeMaxSnrDb": round(turn.sip_deferred_episode_max_snr_db, 2),
        }
        if compact_two_burst_turn and not compact_episode:
            payload["sipDeferredEpisodeEvidence"] = (
                "elevated_noise_compact_two_burst_turn"
                if elevated_noise_compact_turn
                else "compact_two_burst_turn"
            )
        elif ai_receded_compact_turn and not compact_episode:
            payload["sipDeferredEpisodeEvidence"] = "ai_receded_compact_two_burst_turn"
        elif sparse_turn and not compact_episode:
            payload["sipDeferredEpisodeEvidence"] = "sparse_multi_candidate_turn"
        if elevated_noise_compact_turn and compact_two_burst_turn:
            payload["sipElevatedNoiseCompactTurnEvidence"] = (
                "strong_current_modulated_two_burst"
            )
        if ai_receded_compact_turn:
            payload["sipAiRecededCompactTurnEvidence"] = (
                "two_burst_modulated_tail_after_echo_guard"
            )
        if elevated_noise_sparse_turn and sparse_turn:
            payload["sipElevatedNoiseSparseTurnEvidence"] = (
                "strong_anchor_current_modulation"
            )
        return payload

    def _has_sip_ai_receded_compact_deferred_turn_evidence(
        self,
        call_id: str,
        turn: PendingUserTurn,
        observation: SipBargeInObservation,
        *,
        rms_range_db: float,
        wall_ms: int,
        max_gap_ms: int,
    ) -> bool:
        if (
            turn.sip_deferred_episode_burst_count
            < SIP_DEFERRED_EPISODE_COMPACT_MIN_BURSTS
            or turn.sip_deferred_episode_voiced_ms
            < SIP_DEFERRED_EPISODE_AI_RECEDED_COMPACT_MIN_VOICED_MS
        ):
            return False
        if wall_ms > SIP_DEFERRED_EPISODE_COMPACT_MAX_WALL_MS:
            return False
        if max_gap_ms > SIP_DEFERRED_EPISODE_COMPACT_MAX_GAP_MS:
            return False
        if observation.rms_dbfs is None or observation.snr_db is None:
            return False
        ai_rms_dbfs = self._last_ai_audio_rms_dbfs.get(call_id)
        if ai_rms_dbfs is None:
            return False
        if observation.rms_dbfs - ai_rms_dbfs < SIP_AI_PLAYBACK_ECHO_UPLINK_MARGIN_DB:
            return False
        min_current_duration_ms = max(
            self.sip_barge_in_config.candidate_min_duration_ms,
            self.sip_barge_in_config.pre_stop_min_duration_ms
            - observation.frame_duration_ms,
        )
        if observation.candidate_duration_ms < min_current_duration_ms:
            return False
        if (
            turn.sip_deferred_episode_max_snr_db is None
            or turn.sip_deferred_episode_max_snr_db
            < max(
                SIP_DEFERRED_EPISODE_AI_RECEDED_COMPACT_MIN_SNR_DB,
                self.sip_barge_in_config.snr_threshold_db + 6.0,
            )
        ):
            return False
        if observation.snr_db < max(
            SIP_CLEAR_SHORT_MODULATED_MIN_SNR_DB,
            self.sip_barge_in_config.snr_threshold_db + 2.0,
        ):
            return False
        if (
            turn.sip_deferred_episode_max_rms_dbfs is None
            or turn.sip_deferred_episode_max_rms_dbfs
            < self.sip_barge_in_config.turn_taking_high_confidence_rms_dbfs
        ):
            return False
        if rms_range_db < SIP_DEFERRED_EPISODE_AI_RECEDED_COMPACT_MIN_RMS_RANGE_DB:
            return False
        detector = self._sip_barge_in_detector
        diagnostics = detector.latest_observation_payload(call_id) if detector is not None else {}
        if diagnostics.get("speechQualityRejection") is not None:
            return False
        direction_changes = diagnostics.get("rmsDirectionChanges")
        large_jumps = diagnostics.get("largeRmsJumpCount")
        if not isinstance(direction_changes, int) or direction_changes < 3:
            return False
        if (
            not isinstance(large_jumps, int)
            or large_jumps > SIP_CLEAR_SHORT_MODULATED_MAX_LARGE_JUMPS
        ):
            return False
        return not self._has_sip_unstable_local_envelope_risk(call_id, observation)

    def _has_sip_elevated_noise_compact_deferred_turn_evidence(
        self,
        call_id: str,
        turn: PendingUserTurn,
        observation: SipBargeInObservation,
        *,
        rms_range_db: float,
        wall_ms: int,
        max_gap_ms: int,
    ) -> bool:
        if not self._has_sip_elevated_noise_short_burst_risk(observation):
            return False
        if observation.snr_db is None or observation.rms_dbfs is None:
            return False
        if wall_ms > SIP_DEFERRED_EPISODE_COMPACT_MAX_WALL_MS:
            return False
        if max_gap_ms > SIP_DEFERRED_EPISODE_COMPACT_MAX_GAP_MS:
            return False
        if (
            turn.sip_deferred_episode_burst_count
            < SIP_DEFERRED_EPISODE_COMPACT_MIN_BURSTS
            or turn.sip_deferred_episode_voiced_ms
            < SIP_DEFERRED_EPISODE_COMPACT_MIN_VOICED_MS
        ):
            return False
        if (
            turn.sip_deferred_episode_max_snr_db is None
            or turn.sip_deferred_episode_max_snr_db
            < max(
                SIP_ELEVATED_NOISE_MARGINAL_TURN_MIN_SNR_DB,
                self.sip_barge_in_config.snr_threshold_db + 8.0,
            )
        ):
            return False
        if observation.snr_db < max(
            SIP_ELEVATED_NOISE_MARGINAL_TURN_MIN_SNR_DB,
            self.sip_barge_in_config.snr_threshold_db + 8.0,
        ):
            return False
        if rms_range_db < max(
            SIP_DEFERRED_EPISODE_COMPACT_MIN_RMS_RANGE_DB,
            self.sip_barge_in_config.turn_taking_min_range_db + 2.0,
        ):
            return False
        detector = self._sip_barge_in_detector
        diagnostics = detector.latest_observation_payload(call_id) if detector is not None else {}
        if diagnostics.get("speechQualityRejection") is not None:
            return False
        current_rms_range_db = diagnostics.get("rmsRangeDb")
        direction_changes = diagnostics.get("rmsDirectionChanges")
        large_jumps = diagnostics.get("largeRmsJumpCount")
        if (
            not isinstance(current_rms_range_db, int | float)
            or current_rms_range_db < SIP_DEFERRED_EPISODE_COMPACT_MIN_RMS_RANGE_DB
        ):
            return False
        if not isinstance(direction_changes, int) or direction_changes < 2:
            return False
        return (
            isinstance(large_jumps, int)
            and large_jumps <= SIP_CLEAR_SHORT_MODULATED_MAX_LARGE_JUMPS
        )

    def _has_sip_elevated_noise_sparse_deferred_turn_evidence(
        self,
        call_id: str,
        turn: PendingUserTurn,
        observation: SipBargeInObservation,
        *,
        rms_range_db: float,
        wall_ms: int,
        max_gap_ms: int,
    ) -> bool:
        if observation.noise_floor_dbfs is None or observation.snr_db is None:
            return False
        if observation.noise_floor_dbfs < SIP_ELEVATED_NOISE_FLOOR_DBFS:
            return False
        if wall_ms < SIP_DEFERRED_TURN_ECHO_GUARD_MIN_WALL_MS:
            return False
        if wall_ms > min(SIP_DEFERRED_TURN_MAX_WALL_MS, SIP_ECHO_GUARDED_TURN_MAX_WALL_MS):
            return False
        if max_gap_ms > round(SIP_DEFERRED_TURN_MAX_GAP_SECONDS * 1000):
            return False
        if (
            turn.sip_deferred_episode_burst_count < SIP_DEFERRED_EPISODE_MIN_BURSTS
            or turn.sip_deferred_episode_voiced_ms < SIP_DEFERRED_EPISODE_MIN_VOICED_MS
        ):
            return False
        min_anchor_snr_db = max(
            SIP_ELEVATED_NOISE_MARGINAL_TURN_MIN_SNR_DB,
            self.sip_barge_in_config.snr_threshold_db + 8.0,
        )
        min_modulated_snr_db = max(
            SIP_ELEVATED_NOISE_SPARSE_TURN_MODULATED_MIN_SNR_DB,
            self.sip_barge_in_config.snr_threshold_db + 6.0,
        )
        max_snr_db = turn.sip_deferred_episode_max_snr_db
        if max_snr_db is None:
            return False
        has_strong_anchor = max_snr_db >= min_anchor_snr_db
        has_modulated_sparse_anchor = (
            max_snr_db >= min_modulated_snr_db
            and observation.snr_db >= min_modulated_snr_db
        )
        if not (has_strong_anchor or has_modulated_sparse_anchor):
            return False
        if observation.snr_db < max(
            SIP_DEFERRED_TURN_MIN_SNR_DB,
            self.sip_barge_in_config.snr_threshold_db,
        ):
            return False
        if rms_range_db < max(
            SIP_DEFERRED_TURN_MIN_RMS_RANGE_DB,
            self.sip_barge_in_config.turn_taking_min_range_db + 2.0,
        ):
            return False
        detector = self._sip_barge_in_detector
        diagnostics = detector.latest_observation_payload(call_id) if detector is not None else {}
        if diagnostics.get("speechQualityRejection") is not None:
            return False
        if self._has_sip_unstable_local_envelope_risk(call_id, observation):
            return False
        current_rms_range_db = diagnostics.get("rmsRangeDb")
        direction_changes = diagnostics.get("rmsDirectionChanges")
        large_jumps = diagnostics.get("largeRmsJumpCount")
        if (
            not isinstance(current_rms_range_db, int | float)
            or current_rms_range_db < SIP_DEFERRED_EPISODE_COMPACT_MIN_RMS_RANGE_DB
        ):
            return False
        min_direction_changes = (
            SIP_ELEVATED_NOISE_SPARSE_TURN_MODULATED_MIN_DIRECTION_CHANGES
            if not has_strong_anchor
            else 1
        )
        if not isinstance(direction_changes, int) or direction_changes < min_direction_changes:
            return False
        return (
            isinstance(large_jumps, int)
            and large_jumps <= SIP_CLEAR_SHORT_MODULATED_MAX_LARGE_JUMPS
        )

    @staticmethod
    def _sip_deferred_episode_echo_guard_pre_stop_extra_payload(
        payload: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if payload is None:
            return None
        wall_ms = payload.get("sipDeferredEpisodeWallMs")
        burst_count = payload.get("sipDeferredEpisodeBurstCount")
        if not isinstance(wall_ms, int):
            return None
        if not isinstance(burst_count, int):
            return None
        evidence = payload.get("sipDeferredEpisodeEvidence")
        if evidence in {
            "compact_two_burst_turn",
            "elevated_noise_compact_two_burst_turn",
        }:
            echo_guard_payload = dict(payload)
            echo_guard_payload["sipAiPlaybackEchoGuardEscapedBy"] = evidence
            return echo_guard_payload
        if burst_count < SIP_DEFERRED_EPISODE_MIN_BURSTS:
            return None
        if wall_ms < SIP_DEFERRED_TURN_ECHO_GUARD_MIN_WALL_MS:
            return None
        echo_guard_payload = dict(payload)
        echo_guard_payload["sipAiPlaybackEchoGuardEscapedBy"] = (
            payload.get("sipDeferredEpisodeEvidence") or "deferred_speech_episode"
        )
        return echo_guard_payload

    def _sip_echo_guarded_compact_deferred_episode_pre_stop_extra_payload(
        self,
        *,
        call_id: str,
        turn: PendingUserTurn,
        observation: SipBargeInObservation,
        quality_rejection: str | None,
    ) -> dict[str, Any] | None:
        if (
            quality_rejection is not None
            and quality_rejection not in SIP_TURN_CLUSTER_RECOVERABLE_QUALITY_REJECTIONS
        ):
            return None
        guard = self._playback_guard(call_id)
        if (
            turn.sip_deferred_episode_response_id != guard.current_response_id
            or turn.sip_deferred_episode_generation != guard.generation
        ):
            return None
        if (
            turn.sip_deferred_episode_burst_count
            < SIP_DEFERRED_EPISODE_COMPACT_MIN_BURSTS
        ):
            return None
        if (
            turn.sip_deferred_episode_voiced_ms
            < SIP_DEFERRED_EPISODE_COMPACT_MIN_VOICED_MS
        ):
            return None
        wall_ms = self._sip_deferred_episode_wall_ms(turn)
        if wall_ms is None or wall_ms > SIP_DEFERRED_EPISODE_COMPACT_MAX_WALL_MS:
            return None
        max_gap_ms = turn.sip_deferred_episode_max_gap_ms or 0
        if max_gap_ms > SIP_DEFERRED_EPISODE_COMPACT_MAX_GAP_MS:
            return None
        if (
            turn.sip_deferred_episode_max_snr_db is None
            or turn.sip_deferred_episode_max_snr_db
            < max(
                SIP_DEFERRED_EPISODE_COMPACT_MIN_SNR_DB,
                self.sip_barge_in_config.snr_threshold_db + 5.0,
            )
        ):
            return None
        if observation.snr_db is None or observation.rms_dbfs is None:
            return None
        if observation.snr_db < max(
            SIP_ECHO_GUARDED_LOCAL_MIN_SNR_DB,
            self.sip_barge_in_config.snr_threshold_db + 5.0,
        ):
            return None
        rms_range_db = self._sip_deferred_episode_rms_range_db(turn)
        if (
            rms_range_db is None
            or rms_range_db
            < max(
                SIP_DEFERRED_EPISODE_COMPACT_MIN_RMS_RANGE_DB,
                self.sip_barge_in_config.turn_taking_min_range_db + 2.0,
            )
        ):
            return None
        detector = self._sip_barge_in_detector
        diagnostics = detector.latest_observation_payload(call_id) if detector is not None else {}
        if diagnostics.get("speechQualityRejection") is not None:
            return None
        direction_changes = diagnostics.get("rmsDirectionChanges")
        large_jumps = diagnostics.get("largeRmsJumpCount")
        if not isinstance(direction_changes, int) or direction_changes < 1:
            return None
        if (
            not isinstance(large_jumps, int)
            or large_jumps > SIP_CLEAR_SHORT_MODULATED_MAX_LARGE_JUMPS
        ):
            return None
        ai_rms_dbfs = self._last_ai_audio_rms_dbfs.get(call_id)
        if ai_rms_dbfs is None:
            return None
        uplink_above_ai_db = observation.rms_dbfs - ai_rms_dbfs
        if uplink_above_ai_db < -SIP_ECHO_GUARDED_LOCAL_DEFERRED_MAX_AI_DOMINANCE_DB:
            return None
        return {
            "sipDeferredEpisodeEvidence": "compact_two_burst_turn",
            "sipDeferredEpisodeBurstCount": turn.sip_deferred_episode_burst_count,
            "sipDeferredEpisodeVoicedMs": turn.sip_deferred_episode_voiced_ms,
            "sipDeferredEpisodeWallMs": wall_ms,
            "sipDeferredEpisodeMaxGapMs": max_gap_ms,
            "sipDeferredEpisodeRmsRangeDb": round(rms_range_db, 2),
            "sipDeferredEpisodeMaxSnrDb": round(turn.sip_deferred_episode_max_snr_db, 2),
            "sipEchoGuardedDeferredEpisodeEvidence": "compact_two_burst_turn",
            "sipEchoGuardedLocalRmsDirectionChanges": direction_changes,
            "sipEchoGuardedLocalLargeRmsJumpCount": large_jumps,
            "sipUplinkAboveAiPlaybackDb": round(uplink_above_ai_db, 2),
            "sipAiPlaybackEchoGuardEscapedBy": "compact_two_burst_turn",
        }

    @staticmethod
    def _sip_deferred_episode_rms_range_db(turn: PendingUserTurn) -> float | None:
        rms_range_db = turn.sip_deferred_episode_max_rms_range_db
        if (
            turn.sip_deferred_episode_min_rms_dbfs is not None
            and turn.sip_deferred_episode_max_rms_dbfs is not None
        ):
            observed_range_db = (
                turn.sip_deferred_episode_max_rms_dbfs
                - turn.sip_deferred_episode_min_rms_dbfs
            )
            rms_range_db = (
                observed_range_db
                if rms_range_db is None
                else max(rms_range_db, observed_range_db)
            )
        return rms_range_db

    @staticmethod
    def _sip_deferred_episode_wall_ms(turn: PendingUserTurn) -> int | None:
        if (
            turn.sip_deferred_episode_first_at is None
            or turn.sip_deferred_episode_last_at is None
        ):
            return None
        return round(
            max(
                0.0,
                (
                    turn.sip_deferred_episode_last_at
                    - turn.sip_deferred_episode_first_at
                ).total_seconds()
                * 1000,
            )
        )

    def _record_sip_echo_guarded_turn_observation(
        self,
        *,
        call_id: str,
        turn: PendingUserTurn,
        timestamp: datetime,
        observation: SipBargeInObservation,
    ) -> None:
        guard = self._playback_guard(call_id)
        response_id = guard.current_response_id
        generation = guard.generation
        if (
            response_id != turn.sip_echo_guarded_turn_response_id
            or generation != turn.sip_echo_guarded_turn_generation
        ):
            self._reset_sip_echo_guarded_turn(
                turn,
                response_id=response_id,
                generation=generation,
            )
        if observation.candidate_class != "stable_speech_candidate":
            return
        if observation.rms_dbfs is None or observation.snr_db is None:
            return
        if observation.candidate_duration_ms < self.sip_barge_in_config.candidate_min_duration_ms:
            return
        quality_rejection = self._sip_observation_quality_rejection(call_id)
        if (
            quality_rejection is not None
            and quality_rejection not in SIP_TURN_CLUSTER_RECOVERABLE_QUALITY_REJECTIONS
        ):
            if quality_rejection in SIP_TURN_EVIDENCE_IGNORED_QUALITY_REJECTIONS:
                return
            self._reset_sip_echo_guarded_turn(
                turn,
                response_id=response_id,
                generation=generation,
            )
            return

        if (
            turn.sip_echo_guarded_turn_last_at is not None
            and (timestamp - turn.sip_echo_guarded_turn_last_at).total_seconds()
            > SIP_ECHO_GUARDED_TURN_MAX_GAP_SECONDS
        ):
            self._reset_sip_echo_guarded_turn(
                turn,
                response_id=response_id,
                generation=generation,
            )
        if (
            turn.sip_echo_guarded_turn_first_at is not None
            and (
                timestamp - turn.sip_echo_guarded_turn_first_at
            ).total_seconds()
            * 1000
            > SIP_ECHO_GUARDED_TURN_MAX_WALL_MS
        ):
            self._reset_sip_echo_guarded_turn(
                turn,
                response_id=response_id,
                generation=generation,
            )

        starts_new_burst = (
            turn.sip_echo_guarded_turn_last_at is None
            or (timestamp - turn.sip_echo_guarded_turn_last_at).total_seconds()
            > SIP_ECHO_GUARDED_TURN_SAME_BURST_GAP_SECONDS
        )
        if starts_new_burst:
            if turn.sip_echo_guarded_turn_first_at is None:
                turn.sip_echo_guarded_turn_first_at = timestamp
            turn.sip_echo_guarded_turn_burst_count += 1
            turn.sip_echo_guarded_turn_current_burst_voiced_ms = 0

        turn.sip_echo_guarded_turn_last_at = timestamp
        added_voiced_ms = max(
            0,
            observation.vad_voiced_ms
            - turn.sip_echo_guarded_turn_current_burst_voiced_ms,
        )
        turn.sip_echo_guarded_turn_voiced_ms += added_voiced_ms
        turn.sip_echo_guarded_turn_current_burst_voiced_ms = max(
            turn.sip_echo_guarded_turn_current_burst_voiced_ms,
            observation.vad_voiced_ms,
        )
        turn.sip_echo_guarded_turn_min_rms_dbfs = (
            observation.rms_dbfs
            if turn.sip_echo_guarded_turn_min_rms_dbfs is None
            else min(turn.sip_echo_guarded_turn_min_rms_dbfs, observation.rms_dbfs)
        )
        turn.sip_echo_guarded_turn_max_rms_dbfs = (
            observation.rms_dbfs
            if turn.sip_echo_guarded_turn_max_rms_dbfs is None
            else max(turn.sip_echo_guarded_turn_max_rms_dbfs, observation.rms_dbfs)
        )
        turn.sip_echo_guarded_turn_max_snr_db = (
            observation.snr_db
            if turn.sip_echo_guarded_turn_max_snr_db is None
            else max(turn.sip_echo_guarded_turn_max_snr_db, observation.snr_db)
        )
        detector = self._sip_barge_in_detector
        diagnostics = detector.latest_observation_payload(call_id) if detector is not None else {}
        max_snr_db = diagnostics.get("maxSnrDb")
        if isinstance(max_snr_db, int | float):
            turn.sip_echo_guarded_turn_max_snr_db = (
                float(max_snr_db)
                if turn.sip_echo_guarded_turn_max_snr_db is None
                else max(turn.sip_echo_guarded_turn_max_snr_db, float(max_snr_db))
            )
        rms_range_db = diagnostics.get("rmsRangeDb")
        if isinstance(rms_range_db, int | float):
            turn.sip_echo_guarded_turn_max_rms_range_db = (
                float(rms_range_db)
                if turn.sip_echo_guarded_turn_max_rms_range_db is None
                else max(turn.sip_echo_guarded_turn_max_rms_range_db, float(rms_range_db))
            )

    @staticmethod
    def _reset_sip_echo_guarded_turn(
        turn: PendingUserTurn,
        *,
        response_id: str | None,
        generation: int | None,
    ) -> None:
        turn.sip_echo_guarded_turn_response_id = response_id
        turn.sip_echo_guarded_turn_generation = generation
        turn.sip_echo_guarded_turn_first_at = None
        turn.sip_echo_guarded_turn_last_at = None
        turn.sip_echo_guarded_turn_burst_count = 0
        turn.sip_echo_guarded_turn_voiced_ms = 0
        turn.sip_echo_guarded_turn_current_burst_voiced_ms = 0
        turn.sip_echo_guarded_turn_min_rms_dbfs = None
        turn.sip_echo_guarded_turn_max_rms_dbfs = None
        turn.sip_echo_guarded_turn_max_snr_db = None
        turn.sip_echo_guarded_turn_max_rms_range_db = None

    def _sip_echo_guarded_turn_pre_stop_extra_payload(
        self,
        turn: PendingUserTurn,
        *,
        observation: SipBargeInObservation,
        quality_rejection: str | None,
    ) -> dict[str, Any] | None:
        if (
            quality_rejection is not None
            and quality_rejection not in SIP_TURN_CLUSTER_RECOVERABLE_QUALITY_REJECTIONS
        ):
            return None
        if observation.candidate_duration_ms < self.sip_barge_in_config.pre_stop_min_duration_ms:
            return None
        if turn.sip_echo_guarded_turn_burst_count < SIP_ECHO_GUARDED_TURN_MIN_BURSTS:
            return None
        if turn.sip_echo_guarded_turn_voiced_ms < SIP_ECHO_GUARDED_TURN_MIN_VOICED_MS:
            return None
        min_snr_db = SIP_ECHO_GUARDED_TURN_MIN_SNR_DB
        if observation.noise_floor_dbfs is None:
            min_snr_db = max(min_snr_db, SIP_TURN_CLUSTER_MIN_SNR_DB)
        if (
            turn.sip_echo_guarded_turn_max_snr_db is None
            or turn.sip_echo_guarded_turn_max_snr_db < min_snr_db
        ):
            return None
        if self._has_sip_marginal_elevated_noise_turn_risk(observation):
            return None
        rms_range_db = self._sip_echo_guarded_turn_rms_range_db(turn)
        if (
            rms_range_db is None
            or rms_range_db < SIP_ECHO_GUARDED_TURN_MIN_RMS_RANGE_DB
        ):
            return None
        wall_ms = self._sip_echo_guarded_turn_wall_ms(turn)
        if wall_ms is None or wall_ms > SIP_ECHO_GUARDED_TURN_MAX_WALL_MS:
            return None
        return {
            "sipEchoGuardedTurnBurstCount": turn.sip_echo_guarded_turn_burst_count,
            "sipEchoGuardedTurnVoicedMs": turn.sip_echo_guarded_turn_voiced_ms,
            "sipEchoGuardedTurnWallMs": wall_ms,
            "sipEchoGuardedTurnRmsRangeDb": round(rms_range_db, 2),
            "sipEchoGuardedTurnMaxSnrDb": round(turn.sip_echo_guarded_turn_max_snr_db, 2),
        }

    def _has_sip_echo_guarded_turn_noise_risk(
        self,
        call_id: str,
        observation: SipBargeInObservation,
    ) -> bool:
        if observation.rms_dbfs is None or observation.snr_db is None:
            return False
        if observation.rms_dbfs > -30.0:
            return False
        if observation.snr_db >= SIP_ECHO_GUARDED_TURN_MIN_SNR_DB:
            return False
        detector = self._sip_barge_in_detector
        diagnostics = detector.latest_observation_payload(call_id) if detector is not None else {}
        direction_changes = diagnostics.get("rmsDirectionChanges")
        return isinstance(direction_changes, int) and direction_changes >= 2

    def _sip_echo_guarded_local_pre_stop_extra_payload(
        self,
        *,
        call_id: str,
        turn: PendingUserTurn,
        observation: SipBargeInObservation,
        quality_rejection: str | None,
    ) -> dict[str, Any] | None:
        if (
            quality_rejection is not None
            and quality_rejection not in SIP_TURN_CLUSTER_RECOVERABLE_QUALITY_REJECTIONS
        ):
            return None
        if observation.candidate_duration_ms < self.sip_barge_in_config.pre_stop_min_duration_ms:
            return None
        if turn.sip_echo_guarded_turn_voiced_ms < self.sip_barge_in_config.pre_stop_min_duration_ms:
            return None
        detector = self._sip_barge_in_detector
        diagnostics = detector.latest_observation_payload(call_id) if detector is not None else {}
        direction_changes = diagnostics.get("rmsDirectionChanges")
        large_jumps = diagnostics.get("largeRmsJumpCount")
        if not isinstance(direction_changes, int) or direction_changes < 1:
            return None
        if (
            not isinstance(large_jumps, int)
            or large_jumps > SIP_ECHO_GUARDED_LOCAL_MAX_LARGE_JUMPS
        ):
            return None
        rms_range_db = self._sip_echo_guarded_turn_rms_range_db(turn)
        min_rms_range_db = max(
            SIP_ECHO_GUARDED_LOCAL_MIN_RMS_RANGE_DB,
            self.sip_barge_in_config.turn_taking_min_range_db + 2.0,
        )
        min_snr_db = max(
            SIP_ECHO_GUARDED_LOCAL_MIN_SNR_DB,
            self.sip_barge_in_config.snr_threshold_db + 8.0,
        )
        deferred_episode_payload = (
            self._sip_echo_guarded_local_deferred_episode_extra_payload(
                call_id=call_id,
                turn=turn,
                observation=observation,
                rms_range_db=rms_range_db,
                direction_changes=direction_changes,
                large_jumps=large_jumps,
            )
        )
        if (
            turn.sip_echo_guarded_turn_max_snr_db is None
            or turn.sip_echo_guarded_turn_max_snr_db < min_snr_db
        ):
            return deferred_episode_payload
        if rms_range_db is None or rms_range_db < min_rms_range_db:
            return deferred_episode_payload
        has_prior_single_short = turn.sip_single_short_pre_stop_evidence
        has_compact_modulation = large_jumps <= SIP_CLEAR_SHORT_MODULATED_MAX_LARGE_JUMPS
        if not (has_prior_single_short or has_compact_modulation):
            return deferred_episode_payload
        if (
            self._has_sip_elevated_noise_short_burst_risk(observation)
            and not self._has_sip_echo_guarded_elevated_noise_micro_confirm(
                turn=turn,
                max_snr_db=turn.sip_echo_guarded_turn_max_snr_db,
                rms_range_db=rms_range_db,
                direction_changes=direction_changes,
                large_jumps=large_jumps,
            )
        ):
            return deferred_episode_payload
        wall_ms = self._sip_echo_guarded_turn_wall_ms(turn)
        if wall_ms is None or wall_ms > SIP_ECHO_GUARDED_TURN_MAX_WALL_MS:
            return None
        local_evidence = (
            "single_short_micro_confirmed"
            if has_prior_single_short
            else "compact_modulated_micro_confirmed"
        )
        payload: dict[str, Any] = {
            "sipEchoGuardedLocalEvidence": local_evidence,
            "sipEchoGuardedTurnBurstCount": turn.sip_echo_guarded_turn_burst_count,
            "sipEchoGuardedTurnVoicedMs": turn.sip_echo_guarded_turn_voiced_ms,
            "sipEchoGuardedTurnWallMs": wall_ms,
            "sipEchoGuardedTurnRmsRangeDb": round(rms_range_db, 2),
            "sipEchoGuardedTurnMaxSnrDb": round(turn.sip_echo_guarded_turn_max_snr_db, 2),
            "sipEchoGuardedLocalRmsDirectionChanges": direction_changes,
            "sipEchoGuardedLocalLargeRmsJumpCount": large_jumps,
        }
        return payload

    def _has_sip_echo_guarded_elevated_noise_micro_confirm(
        self,
        *,
        turn: PendingUserTurn,
        max_snr_db: float,
        rms_range_db: float,
        direction_changes: int,
        large_jumps: int,
    ) -> bool:
        return (
            turn.sip_single_short_pre_stop_evidence
            and max_snr_db
            >= max(
                SIP_ECHO_GUARDED_LOCAL_HIGH_NOISE_MIN_SNR_DB,
                self.sip_barge_in_config.snr_threshold_db + 10.0,
            )
            and rms_range_db >= SIP_ECHO_GUARDED_LOCAL_MIN_RMS_RANGE_DB
            and direction_changes
            >= SIP_ECHO_GUARDED_LOCAL_HIGH_NOISE_MIN_DIRECTION_CHANGES
            and large_jumps <= SIP_CLEAR_SHORT_MODULATED_MAX_LARGE_JUMPS
        )

    def _sip_echo_guarded_local_deferred_episode_extra_payload(
        self,
        *,
        call_id: str,
        turn: PendingUserTurn,
        observation: SipBargeInObservation,
        rms_range_db: float | None,
        direction_changes: int,
        large_jumps: int,
    ) -> dict[str, Any] | None:
        guard = self._playback_guard(call_id)
        if (
            turn.sip_deferred_episode_response_id != guard.current_response_id
            or turn.sip_deferred_episode_generation != guard.generation
        ):
            return None
        if turn.sip_deferred_episode_burst_count < SIP_ECHO_GUARDED_LOCAL_DEFERRED_MIN_BURSTS:
            return None
        if turn.sip_deferred_episode_voiced_ms < SIP_ECHO_GUARDED_LOCAL_DEFERRED_MIN_VOICED_MS:
            return None
        if observation.snr_db is None or observation.rms_dbfs is None:
            return None
        if observation.snr_db < max(self.sip_barge_in_config.snr_threshold_db + 3.0, 13.0):
            return None
        if (
            turn.sip_deferred_episode_max_snr_db is None
            or turn.sip_deferred_episode_max_snr_db
            < max(SIP_ECHO_GUARDED_TURN_MIN_SNR_DB, self.sip_barge_in_config.snr_threshold_db + 5.0)
        ):
            return None
        episode_rms_range_db = self._sip_deferred_episode_rms_range_db(turn)
        combined_rms_range_db = max(
            value
            for value in (rms_range_db, episode_rms_range_db)
            if value is not None
        ) if rms_range_db is not None or episode_rms_range_db is not None else None
        if (
            combined_rms_range_db is None
            or combined_rms_range_db < SIP_ECHO_GUARDED_LOCAL_MIN_RMS_RANGE_DB
        ):
            return None
        if direction_changes < 2:
            return None
        if large_jumps > SIP_CLEAR_SHORT_MODULATED_MAX_LARGE_JUMPS:
            return None
        wall_ms = self._sip_deferred_episode_wall_ms(turn)
        if wall_ms is None or wall_ms > SIP_ECHO_GUARDED_LOCAL_DEFERRED_MAX_WALL_MS:
            return None
        max_gap_ms = turn.sip_deferred_episode_max_gap_ms or 0
        if max_gap_ms > SIP_ECHO_GUARDED_LOCAL_DEFERRED_MAX_GAP_MS:
            return None
        ai_rms_dbfs = self._last_ai_audio_rms_dbfs.get(call_id)
        if ai_rms_dbfs is None:
            return None
        uplink_above_ai_db = observation.rms_dbfs - ai_rms_dbfs
        if uplink_above_ai_db < -SIP_ECHO_GUARDED_LOCAL_DEFERRED_MAX_AI_DOMINANCE_DB:
            return None
        return {
            "sipEchoGuardedLocalEvidence": "deferred_episode_micro_confirmed",
            "sipEchoGuardedTurnBurstCount": turn.sip_echo_guarded_turn_burst_count,
            "sipEchoGuardedTurnVoicedMs": turn.sip_echo_guarded_turn_voiced_ms,
            "sipEchoGuardedTurnWallMs": self._sip_echo_guarded_turn_wall_ms(turn),
            "sipEchoGuardedTurnRmsRangeDb": round(rms_range_db, 2)
            if rms_range_db is not None
            else None,
            "sipEchoGuardedTurnMaxSnrDb": round(turn.sip_echo_guarded_turn_max_snr_db, 2)
            if turn.sip_echo_guarded_turn_max_snr_db is not None
            else None,
            "sipEchoGuardedLocalRmsDirectionChanges": direction_changes,
            "sipEchoGuardedLocalLargeRmsJumpCount": large_jumps,
            "sipDeferredEpisodeBurstCount": turn.sip_deferred_episode_burst_count,
            "sipDeferredEpisodeVoicedMs": turn.sip_deferred_episode_voiced_ms,
            "sipDeferredEpisodeWallMs": wall_ms,
            "sipDeferredEpisodeMaxGapMs": max_gap_ms,
            "sipDeferredEpisodeRmsRangeDb": round(combined_rms_range_db, 2),
            "sipDeferredEpisodeMaxSnrDb": round(turn.sip_deferred_episode_max_snr_db, 2),
            "sipUplinkAboveAiPlaybackDb": round(uplink_above_ai_db, 2),
        }

    @staticmethod
    def _sip_echo_guarded_turn_rms_range_db(turn: PendingUserTurn) -> float | None:
        rms_range_db = turn.sip_echo_guarded_turn_max_rms_range_db
        if (
            turn.sip_echo_guarded_turn_min_rms_dbfs is not None
            and turn.sip_echo_guarded_turn_max_rms_dbfs is not None
        ):
            observed_range_db = (
                turn.sip_echo_guarded_turn_max_rms_dbfs
                - turn.sip_echo_guarded_turn_min_rms_dbfs
            )
            rms_range_db = (
                observed_range_db
                if rms_range_db is None
                else max(rms_range_db, observed_range_db)
            )
        return rms_range_db

    @staticmethod
    def _sip_echo_guarded_turn_wall_ms(turn: PendingUserTurn) -> int | None:
        if (
            turn.sip_echo_guarded_turn_first_at is None
            or turn.sip_echo_guarded_turn_last_at is None
        ):
            return None
        return round(
            max(
                0.0,
                (
                    turn.sip_echo_guarded_turn_last_at
                    - turn.sip_echo_guarded_turn_first_at
                ).total_seconds()
                * 1000,
            )
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
        diagnostics = detector.latest_observation_payload(call_id)
        direction_changes = diagnostics.get("rmsDirectionChanges")
        if (
            not isinstance(direction_changes, int)
            or direction_changes > SIP_SINGLE_SHORT_MAX_DIRECTION_CHANGES
        ):
            return False
        return detector.has_single_short_pre_stop_local_speech(
            call_id,
            min_rms_dbfs=SIP_SINGLE_SHORT_MIN_RMS_DBFS,
            max_rms_dbfs=SIP_SINGLE_SHORT_MAX_RMS_DBFS,
            min_snr_db=SIP_SINGLE_SHORT_MIN_SNR_DB,
        )

    def _has_sip_clear_short_modulated_pre_stop_evidence(
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
        if observation.candidate_duration_ms >= self.sip_barge_in_config.pre_stop_min_duration_ms:
            return False
        if observation.vad_voiced_ms < self.sip_barge_in_config.vad_voiced_duration_ms:
            return False
        min_snr_db = max(
            self.sip_barge_in_config.snr_threshold_db + 2.0,
            SIP_CLEAR_SHORT_MODULATED_MIN_SNR_DB,
        )
        if observation.snr_db < min_snr_db:
            return False
        if observation.rms_dbfs >= SIP_SINGLE_SHORT_MIN_RMS_DBFS:
            return False
        detector = self._sip_barge_in_detector
        if detector is None:
            return False
        diagnostics = detector.latest_observation_payload(call_id)
        if diagnostics.get("speechQualityRejection") is not None:
            return False
        rms_range_db = diagnostics.get("rmsRangeDb")
        direction_changes = diagnostics.get("rmsDirectionChanges")
        large_jumps = diagnostics.get("largeRmsJumpCount")
        if not isinstance(rms_range_db, int | float):
            return False
        if not isinstance(direction_changes, int):
            return False
        if not isinstance(large_jumps, int):
            return False
        if direction_changes > SIP_SINGLE_SHORT_MAX_DIRECTION_CHANGES:
            return False
        min_rms_range_db = max(
            self.sip_barge_in_config.turn_taking_min_range_db + 2.0,
            SIP_CLEAR_SHORT_MODULATED_MIN_RMS_RANGE_DB,
        )
        if rms_range_db < min_rms_range_db or direction_changes < 1:
            return False
        if large_jumps <= SIP_CLEAR_SHORT_MODULATED_MAX_LARGE_JUMPS:
            return True
        return (
            large_jumps == SIP_CLEAR_SHORT_MODULATED_MAX_LARGE_JUMPS + 1
            and direction_changes >= 2
            and observation.rms_dbfs <= -24.0
            and observation.snr_db < SIP_SINGLE_SHORT_MIN_SNR_DB + 6.0
            and rms_range_db >= min_rms_range_db + 2.0
        )

    def _has_sip_elevated_noise_clear_short_pre_stop_evidence(
        self,
        call_id: str,
        observation: SipBargeInObservation,
    ) -> bool:
        if not self._has_sip_elevated_noise_short_burst_risk(observation):
            return False
        if not self._has_sip_clear_short_modulated_pre_stop_evidence(call_id, observation):
            return False
        if observation.rms_dbfs is None or observation.snr_db is None:
            return False
        if observation.rms_dbfs > SIP_ELEVATED_NOISE_CLEAR_SHORT_MAX_RMS_DBFS:
            return False
        if not (
            SIP_ELEVATED_NOISE_CLEAR_SHORT_MIN_SNR_DB
            <= observation.snr_db
            <= SIP_ELEVATED_NOISE_CLEAR_SHORT_MAX_SNR_DB
        ):
            return False
        detector = self._sip_barge_in_detector
        if detector is None:
            return False
        diagnostics = detector.latest_observation_payload(call_id)
        rms_range_db = diagnostics.get("rmsRangeDb")
        direction_changes = diagnostics.get("rmsDirectionChanges")
        large_jumps = diagnostics.get("largeRmsJumpCount")
        return (
            isinstance(rms_range_db, int | float)
            and isinstance(direction_changes, int)
            and isinstance(large_jumps, int)
            and rms_range_db >= SIP_ELEVATED_NOISE_CLEAR_SHORT_MIN_RMS_RANGE_DB
            and direction_changes >= 1
            and large_jumps <= SIP_CLEAR_SHORT_MODULATED_MAX_LARGE_JUMPS
        )

    def _sip_clear_short_noise_risk(
        self,
        call_id: str,
        observation: SipBargeInObservation,
    ) -> str | None:
        if observation.rms_dbfs is None or observation.snr_db is None:
            return None
        detector = self._sip_barge_in_detector
        if detector is None:
            return None
        diagnostics = detector.latest_observation_payload(call_id)
        direction_changes = diagnostics.get("rmsDirectionChanges")
        large_jumps = diagnostics.get("largeRmsJumpCount")
        if not isinstance(direction_changes, int) or not isinstance(large_jumps, int):
            return None
        if (
            large_jumps > SIP_CLEAR_SHORT_MODULATED_MAX_LARGE_JUMPS
            and observation.snr_db < SIP_CLEAR_SHORT_NOISE_LOW_SNR_MAX_DB
        ):
            return "choppy_low_snr_short_burst"
        if (
            observation.rms_dbfs <= -24.0
            and observation.snr_db < SIP_CLEAR_SHORT_NOISE_LOUD_MIN_SNR_DB - 2.0
        ):
            return "low_energy_short_burst"
        if (
            observation.snr_db >= SIP_CLEAR_SHORT_NOISE_LOUD_MIN_SNR_DB
            and observation.rms_dbfs >= SIP_CLEAR_SHORT_NOISE_LOUD_MIN_RMS_DBFS
            and direction_changes < SIP_CLEAR_SHORT_NOISE_LOUD_MIN_DIRECTION_CHANGES
        ):
            return "loud_low_modulation_short_burst"
        if (
            observation.noise_floor_dbfs is not None
            and observation.noise_floor_dbfs >= SIP_BORDERLINE_ELEVATED_NOISE_FLOOR_DBFS
            and observation.snr_db < SIP_ELEVATED_NOISE_MARGINAL_TURN_MIN_SNR_DB + 1.0
            and observation.rms_dbfs >= SIP_CLEAR_SHORT_NOISE_LOUD_MIN_RMS_DBFS
            and not self._has_sip_elevated_noise_clear_short_pre_stop_evidence(
                call_id,
                observation,
            )
        ):
            return "borderline_high_noise_short_burst"
        return None

    def _has_sip_elevated_noise_short_burst_risk(
        self,
        observation: SipBargeInObservation,
    ) -> bool:
        if not self._has_sip_elevated_noise_floor(observation):
            return False
        noise_adaptive_min_duration_ms = max(
            self.sip_barge_in_config.pre_stop_min_duration_ms,
            self.sip_barge_in_config.candidate_min_duration_ms * 2,
        )
        return observation.candidate_duration_ms < noise_adaptive_min_duration_ms

    @staticmethod
    def _has_sip_elevated_noise_floor(
        observation: SipBargeInObservation,
    ) -> bool:
        return (
            observation.candidate_class == "stable_speech_candidate"
            and observation.noise_floor_dbfs is not None
            and observation.noise_floor_dbfs >= SIP_ELEVATED_NOISE_FLOOR_DBFS
        )

    def _has_sip_elevated_noise_local_only_risk(
        self,
        call_id: str,
        observation: SipBargeInObservation,
    ) -> bool:
        if not self._has_sip_elevated_noise_floor(observation):
            return False
        detector = self._sip_barge_in_detector
        diagnostics = detector.latest_observation_payload(call_id) if detector is not None else {}
        rms_range_db = diagnostics.get("rmsRangeDb")
        direction_changes = diagnostics.get("rmsDirectionChanges")
        if not isinstance(rms_range_db, int | float):
            return True
        if not isinstance(direction_changes, int):
            return True
        return (
            rms_range_db
            < max(
                SIP_CLEAR_SHORT_MODULATED_MIN_RMS_RANGE_DB,
                self.sip_barge_in_config.turn_taking_min_range_db,
            )
            or direction_changes < 1
        )

    def _has_sip_marginal_elevated_noise_turn_risk(
        self,
        observation: SipBargeInObservation,
    ) -> bool:
        if observation.candidate_class != "stable_speech_candidate":
            return False
        if observation.noise_floor_dbfs is None or observation.snr_db is None:
            return False
        if observation.noise_floor_dbfs < SIP_ELEVATED_NOISE_FLOOR_DBFS:
            return False
        if observation.candidate_duration_ms >= SIP_FAST_LOCAL_MIN_DURATION_MS:
            return False
        min_snr_db = max(
            self.sip_barge_in_config.snr_threshold_db + 8.0,
            SIP_ELEVATED_NOISE_MARGINAL_TURN_MIN_SNR_DB,
        )
        return observation.snr_db < min_snr_db

    def _has_sip_high_noise_sparse_deferred_episode_risk(
        self,
        observation: SipBargeInObservation,
        *,
        max_gap_ms: int,
    ) -> bool:
        if not self._has_sip_marginal_elevated_noise_turn_risk(observation):
            return False
        return max_gap_ms > SIP_ECHO_GUARDED_LOCAL_DEFERRED_MAX_GAP_MS

    def _sip_turn_cluster_payload(self, turn: PendingUserTurn) -> dict[str, Any]:
        if turn.sip_turn_cluster_burst_count <= 0:
            return {}
        rms_range_db = self._sip_turn_cluster_rms_range_db(turn)
        wall_ms = self._sip_turn_cluster_wall_ms(turn)
        return {
            "sipTurnClusterBurstCount": turn.sip_turn_cluster_burst_count,
            "sipTurnClusterVoicedMs": turn.sip_turn_cluster_voiced_ms,
            "sipTurnClusterShadowBurstCount": turn.sip_turn_cluster_shadow_burst_count,
            "sipTurnClusterShadowVoicedMs": turn.sip_turn_cluster_shadow_voiced_ms,
            "sipTurnClusterWallMs": wall_ms,
            "sipTurnClusterRmsRangeDb": round(rms_range_db, 2)
            if rms_range_db is not None
            else None,
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
        turn.sip_ai_playback_echo_deferred = False
        turn.sip_candidate_class = None
        turn.sip_candidate_response_id = None
        turn.sip_candidate_generation = None
        turn.sip_single_short_pre_stop_evidence = False
        self._reset_sip_recent_shadow_evidence(
            turn,
            response_id=guard.current_response_id,
        )
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
        turn.sip_ai_playback_echo_deferred = False
        turn.sip_candidate_class = None
        turn.sip_candidate_response_id = None
        turn.sip_candidate_generation = None
        turn.sip_single_short_pre_stop_evidence = False
        guard = self._playback_guard(call_id)
        self._reset_sip_recent_shadow_evidence(
            turn,
            response_id=guard.current_response_id,
        )
        turn.sip_provider_speech_confirmable = False
        self._ignore_empty_turn(call_id, turn, "stale_deferred_sip_candidate")
        turn.started_at = None
        turn.interrupt_trigger_at = None
        return True

    def _sip_required_pre_stop_duration_ms(self, observation: SipBargeInObservation) -> int:
        base_duration_ms = max(
            self.sip_barge_in_config.candidate_min_duration_ms,
            self.sip_barge_in_config.pre_stop_min_duration_ms,
        )
        if observation.candidate_class == "strong_short_speech_candidate":
            return max(
                base_duration_ms,
                self.sip_barge_in_config.short_speech_min_duration_ms,
            )
        return base_duration_ms

    async def _pre_stop_sip_barge_in_candidate(
        self,
        *,
        call_id: str,
        provider: RealtimeProviderProtocol,
        turn: PendingUserTurn,
        trigger_timestamp: datetime,
        observation: SipBargeInObservation,
        extra_payload: dict[str, Any] | None = None,
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
        if extra_payload:
            payload.update(extra_payload)
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
        turn.sip_pre_stop_deferred = False
        turn.sip_ai_playback_echo_deferred = False
        turn.sip_candidate_response_id = None
        turn.sip_candidate_generation = None
        turn.sip_single_short_pre_stop_evidence = False
        guard = self._playback_guard(call_id)
        self._reset_sip_recent_shadow_evidence(
            turn,
            response_id=guard.current_response_id,
        )
        # A rejected SIP pre-stop is a closed provisional turn. Mark it finished
        # so later local SIP speech can create a fresh candidate and pre-stop.
        turn.response_requested = True
        self._cancel_sip_barge_in_task_nowait(call_id)
        payload = self._sip_clean_window_payload(call_id, turn, decision="rejected", reason=reason)
        turn.sip_interrupt_rejected_at = self._append_event(
            call_id,
            "sip_interrupt_rejected",
            "agent",
            payload,
        )
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
            await self._request_response(
                call_id,
                provider,
                input_text=SIP_SHORT_RECOVERY_INPUT_TEXT,
            )
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
                self._record_provider_event(call_id, provider_event.type, event_timestamp)
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
            self._record_provider_event_stream_error(call_id, exc)
            self._fail_running_session(
                call_id,
                end_reason="provider_transport_error",
                failure_stage="provider_event_stream",
                failure_message=f"模型事件流传输异常: {exc}",
                extra_payload={
                    "providerTransport": self._provider_transport_snapshot(
                        call_id,
                        error_source="provider_event_stream",
                    ),
                },
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
            CallSessionStatus.ENDING,
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
            CallSessionStatus.ENDING,
            CallSessionStatus.COMPLETED,
            CallSessionStatus.FAILED,
        }:
            return False
        turn = self._pending_turn(call_id, reset_if_finished=True)
        if turn.started_at is None:
            turn.started_at = trigger_timestamp
        self._record_browser_segment_evidence(turn, payload, trigger_timestamp)
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
            self._mark_response_started(call_id, payload, timestamp)
        elif event_type == "user_speech_started" and session.status == CallSessionStatus.CONNECTED:
            self.registry.transition(call_id, CallSessionStatus.USER_SPEAKING)
        elif (
            event_type == "user_speech_stopped"
            and session.status == CallSessionStatus.USER_SPEAKING
        ):
            metrics.mark_user_speech_stopped(timestamp)
            self.registry.transition(call_id, CallSessionStatus.AI_THINKING)
        elif event_type == "model_audio_delta":
            if (
                session.status == CallSessionStatus.USER_SPEAKING
                and not self._is_barge_in_enabled_for_session(session)
            ):
                session = self.registry.transition(call_id, CallSessionStatus.AI_THINKING)
            if session.status in {
                CallSessionStatus.CONNECTED,
                CallSessionStatus.AI_THINKING,
            }:
                self._cancel_playout_task_nowait(call_id)
                metrics.mark_model_audio_delta(timestamp)
                self.registry.transition(call_id, CallSessionStatus.AI_SPEAKING)
        elif (
            event_type == "model_response_done" and session.status == CallSessionStatus.AI_SPEAKING
        ):
            self._mark_ai_question_completed(call_id, payload, timestamp)
            self._complete_ai_speaking_after_playout(call_id)
            await self._complete_response_and_flush_pending(call_id, provider)
        elif event_type == "model_response_done":
            self._mark_ai_question_completed(call_id, payload, timestamp)
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
        self._cancel_silence_watchdog_nowait(call_id)
        self._silence_prompt_counts[call_id] = 0
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
        previous_turn = self._pending_user_turns.get(call_id)
        guard = self._playback_guard(call_id)
        guard.user_speech_active = True
        turn = self._pending_turn(call_id, reset_if_finished=True)
        self._cancel_turn_response_task_nowait(call_id)
        if turn.stopped_at is not None and not turn.response_requested:
            turn.stopped_at = None
        turn.started_at = timestamp
        if self._should_confirm_recent_rejected_sip_pre_stop_from_provider(
            session,
            previous_turn,
            timestamp,
        ):
            await self._confirm_recent_rejected_sip_pre_stop_from_provider(
                call_id=call_id,
                provider=provider,
                turn=previous_turn,
                timestamp=timestamp,
            )
            return
        if self._is_redundant_sip_provider_speech_started_while_response_pending(
            call_id,
            session,
            previous_turn,
        ):
            guard.user_speech_active = False
            self._append_event(
                call_id,
                "sip_provider_speech_started_ignored",
                "agent",
                {
                    "reason": "awaiting_response_start_after_interrupt",
                    "generation": guard.generation,
                    "responseGeneration": self._response_lifecycle(
                        call_id,
                    ).response_generation,
                },
            )
            return
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

    def _should_confirm_recent_rejected_sip_pre_stop_from_provider(
        self,
        session: CallSession,
        turn: PendingUserTurn | None,
        timestamp: datetime,
    ) -> bool:
        if turn is None or not self._is_sip_participant(session):
            return False
        if (
            not turn.sip_interrupt_rejected
            or not turn.sip_pre_stop_requested
            or turn.sip_pre_stop_at is None
            or turn.sip_interrupt_rejected_at is None
        ):
            return False
        elapsed_seconds = (timestamp - turn.sip_interrupt_rejected_at).total_seconds()
        return 0 <= elapsed_seconds <= SIP_REJECTED_PRE_STOP_PROVIDER_CONFIRM_GRACE_SECONDS

    async def _confirm_recent_rejected_sip_pre_stop_from_provider(
        self,
        *,
        call_id: str,
        provider: RealtimeProviderProtocol,
        turn: PendingUserTurn,
        timestamp: datetime,
    ) -> None:
        self._pending_user_turns[call_id] = turn
        self._cancel_sip_recovery_task_nowait(call_id)
        recovery_pending = self._clear_pending_sip_recovery_response(call_id)
        turn.response_requested = False
        turn.stopped_at = None
        turn.started_at = timestamp
        turn.sip_interrupt_rejected = False
        turn.sip_barge_in_requested = True
        turn.sip_barge_in_expires_at = timestamp + timedelta(
            seconds=self.sip_barge_in_hold_timeout_seconds,
        )
        rejected_at = turn.sip_interrupt_rejected_at
        elapsed_ms = (
            round(max(0.0, (timestamp - rejected_at).total_seconds() * 1000))
            if rejected_at is not None
            else None
        )
        self._append_event(
            call_id,
            "sip_rejected_pre_stop_late_provider_confirmed",
            "agent",
            {
                "reason": "provider_speech_started_after_rejected_pre_stop",
                "confirmedBy": "provider_speech_started",
                "rejectedToProviderSpeechMs": elapsed_ms,
                "providerConfirmGraceMs": round(
                    SIP_REJECTED_PRE_STOP_PROVIDER_CONFIRM_GRACE_SECONDS * 1000
                ),
                "recoveryPendingCleared": recovery_pending,
            },
        )
        self._confirm_sip_barge_in(
            call_id,
            turn,
            confirmed_by="provider_speech_started",
            reason="user_speech_started_during_ai_audio",
        )
        await self._confirm_interrupt(
            call_id,
            provider,
            timestamp,
            reason="user_speech_started_during_ai_audio",
        )
        turn.interrupt_confirmed = True

    def _clear_pending_sip_recovery_response(self, call_id: str) -> bool:
        lifecycle = self._response_lifecycle(call_id)
        if (
            not lifecycle.pending_create
            or lifecycle.pending_input_text != SIP_SHORT_RECOVERY_INPUT_TEXT
        ):
            return False
        lifecycle.pending_create = False
        lifecycle.pending_input_text = None
        lifecycle.pending_response_is_opening = False
        return True

    def _is_redundant_sip_provider_speech_started_while_response_pending(
        self,
        call_id: str,
        session: CallSession,
        previous_turn: PendingUserTurn | None,
    ) -> bool:
        if not self._is_sip_participant(session):
            return False
        if previous_turn is not None and not previous_turn.response_requested:
            return False
        guard = self._playback_guard(call_id)
        lifecycle = self._response_lifecycle(call_id)
        return (
            guard.awaiting_response_start_after_interrupt
            and lifecycle.active
            and not lifecycle.cancel_pending
            and not guard.cancel_requested
            and guard.current_response_id is None
            and not guard.current_response_audio_published
        )

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
        if turn.current_speech_semantic_rejected:
            turn.current_speech_semantic_rejected = False
            if (
                turn.no_barge_unstarted_response_deferred
                and turn.transcript
                and not turn.response_requested
            ):
                turn.stopped_at = timestamp
                turn.transcript_merge_start_index = None
                self._append_event(
                    call_id,
                    "no_barge_deferred_response_recovered_after_rejected_overlap",
                    "agent",
                    {"reason": "semantic_rejected_short_overlap"},
                )
                await self._maybe_schedule_response_from_turn(call_id, provider, timestamp)
                return
            turn.stopped_at = None
            if not turn.transcript and not turn.response_requested:
                self._ignore_empty_turn(call_id, turn, "semantic_rejected_transcript")
            return
        turn.stopped_at = timestamp
        session = self.registry.get(call_id)
        if (
            not self._is_barge_in_enabled_for_session(session)
            and self._has_active_model_response(call_id)
        ):
            turn.no_barge_overlap_stopped_during_ai_response = True
        if turn.call_end_acknowledged:
            self._complete_acknowledged_call_end_turn(call_id)
            return
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
            self._mark_current_no_barge_speech_semantically_rejected(call_id)
            return
        call_end_decision: CallEndDecision | None = None
        if provider_event.type == "user_transcript_done":
            call_end_decision = self.call_end_decision_service.decide(text)
        if self._acknowledge_pending_call_end_if_closing_ack(call_id, text):
            return
        if self._ignore_no_barge_call_end_tail_transcript(call_id, text):
            return
        if call_end_decision is not None and call_end_decision.action == "explicit_end":
            self._record_call_end_intent(call_id, text, call_end_decision)
        else:
            self._pending_call_end_intents.pop(call_id, None)
            self._interrupt_pending_call_end(call_id, "user_transcript_after_call_end_tool")
        turn = self._pending_turn(call_id)
        should_queue_no_barge_followup = (
            provider_event.type == "user_transcript_done"
            and self._should_queue_no_barge_followup_response(call_id, turn, text)
        )
        if provider_event.type == "user_transcript_done" or (
            provider_event.type == "user_transcript_delta"
            and ("text" in provider_event.payload or "stash" in provider_event.payload)
        ):
            self._replace_or_merge_turn_transcript(turn, text)
        else:
            turn.transcript_parts.append(text)
        if should_queue_no_barge_followup:
            self._queue_no_barge_followup_response(call_id, text)
        await self._maybe_confirm_interrupt_from_turn(call_id, provider, timestamp)
        if (
            provider_event.type == "user_transcript_done"
            and self._record_customer_turn(call_id) >= CALL_POLICY_MAX_CUSTOMER_TURNS
        ):
            await self._begin_policy_call_end(
                call_id,
                provider,
                end_reason="policy_turn_limit",
            )
            return
        await self._maybe_schedule_response_from_turn(call_id, provider, timestamp)

    def _record_customer_turn(self, call_id: str) -> int:
        count = self._customer_turn_counts.get(call_id, 0) + 1
        self._customer_turn_counts[call_id] = count
        self._append_event(
            call_id,
            "call_policy_customer_turn",
            "agent",
            {"count": count, "limit": CALL_POLICY_MAX_CUSTOMER_TURNS},
        )
        return count

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

    def _mark_current_no_barge_speech_semantically_rejected(self, call_id: str) -> None:
        session = self.registry.get(call_id)
        if self._is_barge_in_enabled_for_session(session):
            return
        turn = self._pending_user_turns.get(call_id)
        if turn is None or turn.response_requested:
            return
        turn.current_speech_semantic_rejected = True
        turn.stopped_at = None
        self._cancel_turn_response_task_nowait(call_id)

    def _ignore_no_barge_call_end_tail_transcript(self, call_id: str, text: str) -> bool:
        session = self.registry.get(call_id)
        if self._is_barge_in_enabled_for_session(session):
            return False
        pending_call_end = self._pending_call_ends.get(call_id)
        if pending_call_end is None or not pending_call_end.final_response_started:
            return False
        if (
            not self._has_active_model_response(call_id)
            and session.status != CallSessionStatus.AI_SPEAKING
        ):
            return False
        call_end_decision = self.call_end_decision_service.decide(text)
        tail_reason: str | None = None
        if call_end_decision.action == "explicit_end":
            tail_reason = "explicit_customer_end_tail"
        elif self._is_terminal_call_end_tail_fragment(
            pending_call_end,
            text,
            call_end_decision,
        ):
            tail_reason = "terminal_tail_fragment_after_customer_end"
        else:
            return False
        self._pending_call_end_intents.pop(call_id, None)
        turn = self._pending_turn(call_id)
        turn.transcript_parts = [text]
        turn.stopped_at = None
        turn.response_requested = True
        self._cancel_turn_response_task_nowait(call_id)
        self._append_event(
            call_id,
            "call_end_tail_ignored",
            "agent",
            {
                "toolCallId": pending_call_end.tool_call_id,
                "toolReason": pending_call_end.tool_reason,
                "endReason": pending_call_end.end_reason,
                "reason": tail_reason,
                "transcriptPreview": self._text_preview(text),
            },
        )
        return True

    def _is_terminal_call_end_tail_fragment(
        self,
        pending_call_end: PendingCallEnd,
        text: str,
        decision: CallEndDecision,
    ) -> bool:
        if pending_call_end.tool_reason != "customer_end":
            return False
        if decision.action == "not_end":
            return False
        stripped = text.strip()
        if stripped.endswith(("?", "？")):
            return False
        normalized = self._normalize_call_end_acknowledgement(text)
        if not normalized:
            return False
        if normalized in CALL_END_STRONG_CONTINUATION_TEXTS:
            return False
        if any(pattern in normalized for pattern in CALL_END_STRONG_CONTINUATION_PATTERNS):
            return False
        if len(normalized) > CALL_END_TERMINAL_TAIL_MAX_CHARS:
            return False
        return any(pattern in normalized for pattern in CALL_END_TERMINAL_TIME_HINT_PATTERNS)

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

        ignore_payload = self._call_end_tool_ignore_payload(call_id, tool_reason)
        if ignore_payload is None and not self._accepts_call_end_tool(call_id, tool_reason):
            ignore_payload = {
                "reason": self._call_end_tool_rejection_reason(tool_reason),
            }
        if ignore_payload is not None:
            ignored_payload = {
                "toolCallId": tool_call_id,
                "toolReason": tool_reason,
                "endReason": end_reason,
                **ignore_payload,
            }
            self._append_event(
                call_id,
                "call_end_tool_ignored",
                "agent",
                ignored_payload,
            )
            try:
                await provider.submit_tool_result(
                    tool_call_id,
                    self._call_end_rejected_tool_result(
                        tool_reason,
                        rejection_reason=ignore_payload["reason"],
                    ),
                )
                self._queue_response_create(call_id)
            except Exception as exc:
                self._append_event(
                    call_id,
                    "agent_error",
                    "agent",
                    {
                        "message": f"提交结束通话工具拒绝结果失败: {exc}",
                        "toolCallId": tool_call_id,
                    },
                )
            return

        has_local_customer_end_intent = (
            tool_reason == "customer_end" and call_id in self._pending_call_end_intents
        )
        self._pending_call_end_intents.pop(call_id, None)
        final_audio_already_spoken = self._has_current_response_audio(call_id)
        if call_id not in self._pending_call_ends:
            self._pending_call_ends[call_id] = PendingCallEnd(
                tool_call_id=tool_call_id,
                tool_reason=tool_reason,
                end_reason=end_reason,
                final_response_started=final_audio_already_spoken,
                local_explicit_intent=has_local_customer_end_intent,
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
                    "localExplicitIntent": has_local_customer_end_intent,
                },
            )
        pending_call_end = self._pending_call_ends[call_id]
        should_create_final_response = not pending_call_end.final_response_started

        try:
            await provider.submit_tool_result(
                tool_call_id,
                (
                    self._call_end_final_response_tool_result(tool_reason)
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

    @staticmethod
    def _call_end_final_response_tool_result(tool_reason: str) -> str:
        return CALL_END_FINAL_RESPONSE_TOOL_RESULTS_BY_REASON.get(
            tool_reason,
            CALL_END_FINAL_RESPONSE_TOOL_RESULT,
        )

    def _accepts_call_end_tool(self, call_id: str, tool_reason: str) -> bool:
        if tool_reason == "policy_limit":
            return self._customer_turn_counts.get(call_id, 0) >= 3
        if tool_reason == "task_completed":
            return self._accepts_task_completed_tool(call_id)
        if tool_reason != "customer_end":
            return False
        if call_id in self._pending_call_ends:
            return True
        return call_id in self._pending_call_end_intents

    @staticmethod
    def _call_end_tool_rejection_reason(tool_reason: str) -> str:
        if tool_reason == "policy_limit":
            return "policy_limit_before_three_customer_turns"
        if tool_reason == "task_completed":
            return "task_completed_without_next_step_signal"
        return "customer_end_without_terminal_user_signal"

    @staticmethod
    def _call_end_rejected_tool_result(
        tool_reason: str,
        *,
        rejection_reason: str,
    ) -> str:
        if tool_reason == "task_completed":
            return TASK_COMPLETED_REJECTED_TOOL_RESULT
        if tool_reason == "policy_limit":
            return "尚未达到连续三轮沟通策略上限，请继续回应当前话题。"
        if rejection_reason == "customer_end_without_terminal_user_signal":
            return CALL_END_NO_TERMINAL_SIGNAL_REJECTED_TOOL_RESULT
        return CALL_END_REJECTED_TOOL_RESULT

    def _accepts_task_completed_tool(self, call_id: str) -> bool:
        if call_id in self._pending_call_ends:
            return True
        turn = self._pending_user_turns.get(call_id)
        if turn is None:
            return False
        normalized = self._normalize_call_end_acknowledgement(turn.transcript)
        if not normalized or normalized in TASK_COMPLETED_AMBIGUOUS_TEXTS:
            return False
        if any(pattern in normalized for pattern in TASK_COMPLETED_NEGATIVE_PATTERNS):
            return False
        return any(pattern in normalized for pattern in TASK_COMPLETED_NEXT_STEP_PATTERNS)

    def _call_end_tool_ignore_payload(
        self,
        call_id: str,
        tool_reason: str,
    ) -> dict[str, Any] | None:
        if tool_reason != "customer_end":
            return None
        if call_id in self._pending_call_end_intents:
            return None
        turn = self._pending_user_turns.get(call_id)
        transcript = turn.transcript if turn is not None else ""
        if not transcript:
            return None
        decision = self.call_end_decision_service.decide(transcript)
        if decision.action == "explicit_end":
            return None
        normalized = self._normalize_call_end_acknowledgement(transcript)
        reason = (
            "customer_end_without_explicit_customer_intent"
            if normalized in CALL_END_ACKNOWLEDGEMENT_TEXTS
            else "customer_end_without_terminal_user_signal"
        )
        return {
            "reason": reason,
            "localDecisionAction": decision.action,
            "localDecisionReason": decision.reason,
            "localDecisionConfidence": decision.confidence,
            "transcriptPreview": self._text_preview(transcript),
        }

    def _interrupt_pending_call_end(self, call_id: str, reason: str) -> None:
        pending_call_end = self._pending_call_ends.pop(call_id, None)
        self._cancel_pending_call_end_defer_task_nowait(call_id)
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

        if reason == "customer_request":
            transcript = self._pending_turn(call_id).transcript
            decision = await RuleBasedHandoffIntentClassifier().classify(
                transcript=transcript
            )
            if not decision.matched or decision.reason != "customer_request":
                if (
                    self._normalize_handoff_intent_fragment(transcript)
                    in PARTIAL_HANDOFF_INTENT_VALUES
                ):
                    self._append_event(
                        call_id,
                        "handoff_tool_requested",
                        "agent",
                        {
                            "toolCallId": tool_call_id,
                            "reason": reason,
                            "confirmationRequired": True,
                            "transcriptPreview": self._text_preview(transcript),
                        },
                    )
                    try:
                        await provider.submit_tool_result(
                            tool_call_id,
                            CUSTOMER_HANDOFF_CONFIRMATION_TOOL_RESULT,
                        )
                        self._queue_response_create(call_id)
                    except Exception as exc:
                        self._append_event(
                            call_id,
                            "agent_error",
                            "agent",
                            {
                                "message": f"提交转人工确认结果失败: {exc}",
                                "toolCallId": tool_call_id,
                            },
                        )
                    return
                self._append_event(
                    call_id,
                    "handoff_tool_ignored",
                    "agent",
                    {
                        "reason": "customer_request_without_explicit_intent",
                        "toolCallId": tool_call_id,
                        "localDecisionReason": decision.reason,
                        "localDecisionConfidence": decision.confidence,
                        "transcriptPreview": self._text_preview(transcript),
                    },
                )
                try:
                    await provider.submit_tool_result(
                        tool_call_id,
                        CUSTOMER_HANDOFF_REJECTED_TOOL_RESULT,
                    )
                    self._queue_response_create(call_id)
                except Exception as exc:
                    self._append_event(
                        call_id,
                        "agent_error",
                        "agent",
                        {
                            "message": f"提交转人工工具拒绝结果失败: {exc}",
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

    @staticmethod
    def _normalize_handoff_intent_fragment(text: str) -> str:
        return "".join(
            char.lower()
            for char in text.strip()
            if char not in " \t\r\n，。！？,.!?；;：:、"
        )

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
        observed_at: datetime | None = None,
    ) -> None:
        duration_ms = self._payload_int(payload, "durationMs")
        current_duration_ms = turn.browser_segment_duration_ms or -1
        if duration_ms is not None and duration_ms < current_duration_ms:
            return
        phase = self._payload_str(payload, "phase")
        turn.browser_segment_phase = phase
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
        if observed_at is not None:
            turn.browser_segment_observed_at = observed_at
            if phase == "ended":
                turn.browser_segment_ended_at = observed_at

    @staticmethod
    def _replace_or_merge_turn_transcript(turn: PendingUserTurn, text: str) -> None:
        merge_start = turn.transcript_merge_start_index
        if merge_start is None:
            turn.transcript_parts = [text]
            return
        merge_start = max(0, min(merge_start, len(turn.transcript_parts)))
        turn.transcript_parts[merge_start:] = [text]
        turn.transcript_merge_start_index = None

    def _should_queue_no_barge_followup_response(
        self,
        call_id: str,
        turn: PendingUserTurn,
        text: str,
    ) -> bool:
        session = self.registry.get(call_id)
        return (
            not self._is_barge_in_enabled_for_session(session)
            and self._has_active_model_response(call_id)
            and turn.response_requested
            and turn.transcript_merge_start_index is not None
            and self._is_substantive_no_barge_followup(text)
        )

    @staticmethod
    def _is_substantive_no_barge_followup(text: str) -> bool:
        normalized = normalize_dialogue_text(text)
        if not normalized or normalized in NO_BARGE_FOLLOWUP_ACKNOWLEDGEMENT_TEXTS:
            return False
        if len(normalized) <= 2:
            return False
        if len(normalized) >= 6:
            return True
        return any(hint in normalized for hint in NO_BARGE_FOLLOWUP_SUBSTANTIVE_HINTS)

    def _queue_no_barge_followup_response(self, call_id: str, text: str) -> None:
        lifecycle = self._response_lifecycle(call_id)
        if lifecycle.pending_create:
            return
        self._queue_response_create(call_id)
        self._append_event(
            call_id,
            "no_barge_overlap_followup_response_queued",
            "agent",
            {
                "reason": "substantive_followup_during_ai_response",
                "transcriptPreview": self._text_preview(text),
            },
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
        cleanup_errors = await self._stop_audio_playout_queue(
            call_id,
            source="browser",
            reason=decision.reason,
        )
        stop_audio_succeeded = not cleanup_errors

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
        cleanup_errors = await self._stop_audio_playout_queue(
            call_id,
            source="browser",
            reason=decision.reason,
        )
        stop_audio_succeeded = not cleanup_errors

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
            turn.sip_ai_playback_echo_deferred = False
            turn.sip_candidate_class = None
            turn.sip_candidate_response_id = None
            turn.sip_candidate_generation = None
            turn.sip_single_short_pre_stop_evidence = False
            self._reset_sip_recent_shadow_evidence(
                turn,
                response_id=guard.current_response_id,
            )
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
            turn.sip_pre_stop_deferred = False
            turn.sip_ai_playback_echo_deferred = False
            turn.sip_candidate_response_id = None
            turn.sip_candidate_generation = None
            turn.sip_single_short_pre_stop_evidence = False
            guard = self._playback_guard(call_id)
            self._reset_sip_recent_shadow_evidence(
                turn,
                response_id=guard.current_response_id,
            )
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
        if (
            turn.response_requested
            or turn.stopped_at is None
            or not turn.transcript
            or turn.transcript_merge_start_index is not None
        ):
            return
        if self._should_defer_no_barge_response_until_model_done(call_id):
            return
        await self._maybe_confirm_interrupt_from_turn(call_id, provider, timestamp)
        stability_delay_seconds = self._turn_response_stability_delay_seconds(call_id)
        if await self._maybe_schedule_explicit_call_end_without_response(
            call_id,
            provider,
            turn,
            timestamp,
        ):
            return
        if stability_delay_seconds <= 0:
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
                stability_delay_seconds,
            ),
            name=f"ai-call-turn-response-{call_id}",
        )

    def _turn_response_stability_delay_seconds(self, call_id: str) -> float:
        delay_seconds = self.user_turn_stability_delay_seconds
        session = self.registry.get(call_id)
        if not self._is_barge_in_enabled_for_session(session):
            delay_seconds = max(delay_seconds, self.no_barge_user_turn_stability_delay_seconds)
        return delay_seconds

    async def _request_response_after_turn_stability(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        turn: PendingUserTurn,
        stopped_at: datetime,
        stability_delay_seconds: float,
    ) -> None:
        try:
            await asyncio.sleep(stability_delay_seconds)
            if self._pending_user_turns.get(call_id) is not turn:
                return
            if (
                turn.stopped_at != stopped_at
                or turn.response_requested
                or not turn.transcript
                or turn.transcript_merge_start_index is not None
            ):
                return
            if self.registry.get(call_id).status in {
                CallSessionStatus.ENDING,
                CallSessionStatus.COMPLETED,
                CallSessionStatus.FAILED,
            }:
                return
            if self._should_defer_no_barge_response_until_model_done(call_id):
                return
            await self._request_response_from_turn(call_id, provider, turn)
        except asyncio.CancelledError:
            raise
        finally:
            if self._turn_response_tasks.get(call_id) is asyncio.current_task():
                self._turn_response_tasks.pop(call_id, None)

    async def _maybe_schedule_explicit_call_end_without_response(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        turn: PendingUserTurn,
        timestamp: datetime,
    ) -> bool:
        session = self.registry.get(call_id)
        if not self._is_barge_in_enabled_for_session(session):
            return False
        if call_id not in self._pending_call_end_intents:
            return False
        self._promote_missing_call_end_tool(call_id)
        if call_id not in self._pending_call_ends:
            return False
        turn.response_requested = True
        self._cancel_turn_response_task_nowait(call_id)

        await self._stop_ai_for_explicit_call_end(call_id, provider, timestamp)
        self._move_running_session_to_connected_for_call_end(call_id)

        self._schedule_pending_call_end_nowait(call_id)
        return True

    async def _stop_ai_for_explicit_call_end(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        timestamp: datetime,
    ) -> None:
        if (
            self.registry.get(call_id).status != CallSessionStatus.AI_SPEAKING
            and not self._has_active_model_response(call_id)
        ):
            return
        await self._invalidate_audio_for_interrupt_candidate(
            call_id=call_id,
            provider=provider,
            trigger_timestamp=timestamp,
            source="agent",
            reason="explicit_customer_end",
        )
        guard = self._playback_guard(call_id)
        guard.awaiting_response_start_after_interrupt = False
        guard.suppress_audio_until = None

    def _move_running_session_to_connected_for_call_end(self, call_id: str) -> None:
        session = self.registry.get(call_id)
        if session.status == CallSessionStatus.CONNECTED:
            return
        if session.status in {
            CallSessionStatus.USER_SPEAKING,
            CallSessionStatus.AI_THINKING,
            CallSessionStatus.AI_SPEAKING,
            CallSessionStatus.INTERRUPTED,
        }:
            self.registry.transition(call_id, CallSessionStatus.WAITING)
            self.registry.transition(call_id, CallSessionStatus.CONNECTED)
        elif session.status == CallSessionStatus.WAITING:
            self.registry.transition(call_id, CallSessionStatus.CONNECTED)

    async def _request_response_from_turn(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
        turn: PendingUserTurn,
    ) -> None:
        if (
            turn.response_requested
            or turn.stopped_at is None
            or not turn.transcript
            or turn.transcript_merge_start_index is not None
        ):
            return
        if self._should_defer_no_barge_response_until_model_done(call_id):
            return
        if self._should_wait_for_no_barge_user_speech(call_id):
            return
        if self._defer_sip_response_release_if_needed(call_id):
            turn.response_requested = True
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
        self._clear_sip_provisional_turn_state(call_id, turn)
        session = self.registry.get(call_id)
        if session.status == CallSessionStatus.USER_SPEAKING:
            self.registry.transition(call_id, CallSessionStatus.WAITING)
            self.registry.transition(call_id, CallSessionStatus.CONNECTED)
        self._schedule_pending_call_end_nowait(call_id)

    def _complete_acknowledged_call_end_turn(self, call_id: str) -> None:
        session = self.registry.get(call_id)
        if session.status == CallSessionStatus.USER_SPEAKING:
            self.registry.transition(call_id, CallSessionStatus.WAITING)
            self.registry.transition(call_id, CallSessionStatus.CONNECTED)
        self._schedule_pending_call_end_nowait(call_id)

    def _clear_sip_provisional_turn_state(
        self,
        call_id: str,
        turn: PendingUserTurn,
    ) -> None:
        self._cancel_sip_barge_in_task_nowait(call_id)
        self._cancel_sip_clean_window_task_nowait(call_id)
        turn.sip_barge_in_requested = False
        turn.sip_barge_in_confirmed = False
        turn.sip_barge_in_confirmed_by = None
        turn.sip_barge_in_expires_at = None
        turn.sip_pre_stop_requested = False
        turn.sip_pre_stop_deferred = False
        turn.sip_ai_playback_echo_deferred = False
        turn.sip_pre_stop_at = None
        turn.sip_candidate_class = None
        turn.sip_candidate_response_id = None
        turn.sip_candidate_generation = None
        turn.sip_single_short_pre_stop_evidence = False
        guard = self._playback_guard(call_id)
        self._reset_sip_recent_shadow_evidence(
            turn,
            response_id=guard.current_response_id,
        )
        turn.sip_provider_speech_confirmable = False
        turn.sip_interrupt_rejected = False
        turn.sip_interrupt_rejected_at = None

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

    @staticmethod
    def _utcnow_text() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _provider_transport_state(self, call_id: str) -> dict[str, Any]:
        return self._provider_transport_diagnostics.setdefault(call_id, {})

    def _record_provider_event(
        self,
        call_id: str,
        event_type: str,
        event_timestamp: datetime,
    ) -> None:
        state = self._provider_transport_state(call_id)
        state["lastProviderEventType"] = event_type
        state["lastProviderEventAt"] = event_timestamp.isoformat()

    def _record_provider_audio_send_attempt(
        self,
        call_id: str,
        frame: PcmAudioFrame,
    ) -> None:
        state = self._provider_transport_state(call_id)
        state["lastProviderAudioSendAttemptAt"] = self._utcnow_text()
        state["lastProviderAudioFrameBytes"] = len(frame.data)
        state["lastProviderAudioFrameSampleRateHz"] = frame.sample_rate_hz
        state["lastProviderAudioFrameChannels"] = frame.channels
        state["lastProviderAudioFrameSampleWidthBytes"] = frame.sample_width_bytes

    def _record_provider_audio_send_success(self, call_id: str) -> None:
        self._provider_transport_state(call_id)["lastProviderAudioSendSucceededAt"] = (
            self._utcnow_text()
        )

    def _record_provider_audio_send_error(
        self,
        call_id: str,
        exc: Exception,
    ) -> None:
        cause = exc.__cause__ if isinstance(exc, ProviderTransportError) and exc.__cause__ else exc
        state = self._provider_transport_state(call_id)
        state["lastProviderAudioSendErrorAt"] = self._utcnow_text()
        state["lastProviderAudioSendErrorMessage"] = str(exc)
        state["lastProviderAudioSendErrorType"] = type(cause).__name__

    def _record_provider_event_stream_error(
        self,
        call_id: str,
        exc: Exception,
    ) -> None:
        state = self._provider_transport_state(call_id)
        state["lastProviderEventStreamErrorAt"] = self._utcnow_text()
        state["lastProviderEventStreamErrorMessage"] = str(exc)
        state["lastProviderEventStreamErrorType"] = type(exc).__name__

    def _provider_transport_snapshot(
        self,
        call_id: str,
        *,
        error_source: str,
    ) -> dict[str, Any]:
        snapshot = dict(self._provider_transport_state(call_id))
        snapshot["errorSource"] = error_source
        return snapshot

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

        guard = self._playback_guard(call_id)
        response_id = self._response_id_from_payload(provider_event.payload)
        response_generation = guard.current_response_generation
        pcm_bytes, frame_count = self._audio_playout_delta_shape(delta)
        if pcm_bytes <= 0 or frame_count <= 0:
            return
        queued_delta = AudioPlayoutDelta(
            delta=delta,
            pcm_bytes=pcm_bytes,
            frame_count=frame_count,
            response_id=response_id,
            response_generation=response_generation,
        )
        queue = self._audio_playout_queue(call_id)
        stats = self._audio_playout_queue_stats_for_call(call_id)
        if stats.queued_bytes + pcm_bytes > self._audio_playout_max_pcm_bytes():
            self._fail_audio_playout_response_nowait(call_id, queued_delta)
            return
        try:
            queue.put_nowait(queued_delta)
        except asyncio.QueueFull:
            self._fail_audio_playout_response_nowait(call_id, queued_delta)
            return
        self._record_audio_playout_enqueued(call_id, queued_delta)
        self._ensure_audio_playout_worker(call_id)
        # 只让出事件循环调度 worker，不等待 capture_frame 或实际播放进度。
        await asyncio.sleep(0)

    def _audio_playout_delta_shape(self, delta: str) -> tuple[int, int]:
        padding = 2 if delta.endswith("==") else 1 if delta.endswith("=") else 0
        pcm_bytes = max(0, len(delta) * 3 // 4 - padding)
        frame_bytes = self._audio_playout_frame_bytes()
        frame_count = (pcm_bytes + frame_bytes - 1) // frame_bytes
        return pcm_bytes, frame_count

    def _audio_playout_frame_bytes(self) -> int:
        samples = (
            self.audio_bridge.qwen_output_sample_rate_hz
            * self.audio_bridge.output_frame_duration_ms
            // 1000
        )
        return max(1, samples * self.audio_bridge.sample_width_bytes)

    def _audio_playout_queue_capacity(self) -> int:
        frame_duration_ms = max(1, self.audio_bridge.output_frame_duration_ms)
        return max(
            1,
            (
                AUDIO_PLAYOUT_MAX_RESPONSE_DURATION_MS
                + frame_duration_ms
                - 1
            )
            // frame_duration_ms,
        )

    def _audio_playout_max_pcm_bytes(self) -> int:
        return (
            self.audio_bridge.qwen_output_sample_rate_hz
            * self.audio_bridge.sample_width_bytes
            * AUDIO_PLAYOUT_MAX_RESPONSE_DURATION_MS
            // 1000
        )

    def _audio_playout_queue_item_capacity(self) -> int:
        # 单个合法 mono PCM delta 至少包含一个 sample；实际容量由 PCM 字节预算约束。
        return max(
            1,
            self._audio_playout_max_pcm_bytes()
            // self.audio_bridge.sample_width_bytes,
        )

    def _audio_playout_frame_count_for_bytes(self, pcm_bytes: int) -> int:
        frame_bytes = self._audio_playout_frame_bytes()
        return max(0, (pcm_bytes + frame_bytes - 1) // frame_bytes)

    def _audio_playout_high_watermark(self) -> int:
        capacity = self._audio_playout_queue_capacity()
        return max(
            1,
            (
                capacity * AUDIO_PLAYOUT_HIGH_WATERMARK_PERCENT
                + 99
            )
            // 100,
        )

    def _audio_playout_queue(self, call_id: str) -> asyncio.Queue[AudioPlayoutDelta]:
        queue = self._audio_playout_queues.get(call_id)
        if queue is None:
            queue = asyncio.Queue(maxsize=self._audio_playout_queue_item_capacity())
            self._audio_playout_queues[call_id] = queue
        return queue

    def _audio_playout_queue_stats_for_call(
        self,
        call_id: str,
    ) -> AudioPlayoutQueueStats:
        return self._audio_playout_queue_stats.setdefault(
            call_id,
            AudioPlayoutQueueStats(),
        )

    def _record_audio_playout_enqueued(
        self,
        call_id: str,
        queued_delta: AudioPlayoutDelta,
    ) -> None:
        queue = self._audio_playout_queue(call_id)
        stats = self._audio_playout_queue_stats_for_call(call_id)
        stats.queued_bytes += queued_delta.pcm_bytes
        stats.queued_frames = self._audio_playout_frame_count_for_bytes(
            stats.queued_bytes
        )
        self._set_audio_playout_queue_depth(call_id, stats.queued_frames)

        response_key = (
            queued_delta.response_id,
            queued_delta.response_generation,
        )
        high_watermark = self._audio_playout_high_watermark()
        if (
            stats.queued_frames >= high_watermark
            and response_key not in stats.high_watermark_response_keys
        ):
            stats.high_watermark_response_keys.add(response_key)
            self._append_event(
                call_id,
                "audio_playout_queue_watermark",
                "agent",
                self._audio_playout_queue_payload(
                    call_id,
                    response_id=queued_delta.response_id,
                    response_generation=queued_delta.response_generation,
                    queue_depth_deltas=queue.qsize(),
                    queued_frames=stats.queued_frames,
                    queued_bytes=stats.queued_bytes,
                ),
            )

    def _record_audio_playout_dequeued(
        self,
        call_id: str,
        queued_delta: AudioPlayoutDelta,
    ) -> None:
        stats = self._audio_playout_queue_stats_for_call(call_id)
        stats.queued_bytes = max(0, stats.queued_bytes - queued_delta.pcm_bytes)
        stats.queued_frames = self._audio_playout_frame_count_for_bytes(
            stats.queued_bytes
        )
        self._set_audio_playout_queue_depth(call_id, stats.queued_frames)

    def _set_audio_playout_queue_depth(self, call_id: str, depth: int) -> None:
        metrics = self.metrics_by_call_id.setdefault(call_id, CallMetrics())
        metrics.audio_queue_depth = max(0, depth)
        self.registry.get(call_id).metrics = metrics.snapshot()

    def _audio_playout_queue_payload(
        self,
        call_id: str,
        *,
        response_id: str | None,
        response_generation: int,
        queue_depth_deltas: int,
        queued_frames: int,
        queued_bytes: int,
    ) -> dict[str, Any]:
        frame_duration_ms = max(1, self.audio_bridge.output_frame_duration_ms)
        return {
            "responseId": response_id,
            "generation": self._playback_guard(call_id).generation,
            "responseGeneration": response_generation,
            "queueUnit": "model_audio_delta",
            "queueDepthDeltas": queue_depth_deltas,
            "queuedFrames": queued_frames,
            "queuedBytes": queued_bytes,
            "queuedDurationMs": queued_frames * frame_duration_ms,
            "capacityFrames": self._audio_playout_queue_capacity(),
            "capacityBytes": self._audio_playout_max_pcm_bytes(),
            "capacityDurationMs": AUDIO_PLAYOUT_MAX_RESPONSE_DURATION_MS,
            "highWatermarkFrames": self._audio_playout_high_watermark(),
            "highWatermarkPercent": AUDIO_PLAYOUT_HIGH_WATERMARK_PERCENT,
        }

    def _ensure_audio_playout_worker(self, call_id: str) -> None:
        task = self._audio_playout_tasks.get(call_id)
        if task is not None and not task.done():
            return
        self._audio_playout_tasks[call_id] = asyncio.create_task(
            self._run_audio_playout_worker(call_id),
            name=f"ai-call-audio-playout-{call_id}",
        )

    async def _run_audio_playout_worker(self, call_id: str) -> None:
        queue = self._audio_playout_queue(call_id)
        try:
            while True:
                try:
                    queued_delta = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                self._record_audio_playout_dequeued(call_id, queued_delta)
                try:
                    drop_reason = self._queued_audio_drop_reason(call_id, queued_delta)
                    if drop_reason is not None:
                        self._append_queued_audio_dropped(
                            call_id,
                            queued_delta,
                            drop_reason,
                        )
                        continue
                    try:
                        decoded = self.audio_bridge.decode_qwen_output_delta(
                            queued_delta.delta
                        )
                        playout_frames = list(
                            self.audio_bridge.iter_output_playout_frames(decoded)
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        self._fail_running_session(
                            call_id,
                            end_reason="audio_transport_error",
                            failure_stage="audio_playout",
                            failure_message=f"AI 音频解码异常: {exc}",
                        )
                        return
                    for frame_index, playout_frame in enumerate(playout_frames):
                        drop_reason = self._queued_audio_drop_reason(
                            call_id,
                            queued_delta,
                        )
                        if drop_reason is not None:
                            self._append_queued_audio_dropped(
                                call_id,
                                queued_delta,
                                drop_reason,
                                frames=playout_frames[frame_index:],
                            )
                            return
                        try:
                            await self.audio_publisher.publish_audio(
                                call_id,
                                playout_frame,
                            )
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            drop_reason = self._queued_audio_drop_reason(
                                call_id,
                                queued_delta,
                            )
                            if drop_reason is not None:
                                self._append_queued_audio_dropped(
                                    call_id,
                                    queued_delta,
                                    drop_reason,
                                    frames=playout_frames[frame_index:],
                                )
                                return
                            self._fail_running_session(
                                call_id,
                                end_reason="audio_transport_error",
                                failure_stage="audio_playout",
                                failure_message=f"AI 音频播放异常: {exc}",
                            )
                            return
                        drop_reason = self._queued_audio_drop_reason(
                            call_id,
                            queued_delta,
                        )
                        if drop_reason is not None:
                            self._append_queued_audio_dropped(
                                call_id,
                                queued_delta,
                                drop_reason,
                                frames=playout_frames[frame_index:],
                            )
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
                        self._record_audio_playout_published(call_id, playout_frame)
                finally:
                    queue.task_done()
        finally:
            if self._audio_playout_tasks.get(call_id) is asyncio.current_task():
                self._audio_playout_tasks.pop(call_id, None)

    def _fail_audio_playout_response_nowait(
        self,
        call_id: str,
        overflow_delta: AudioPlayoutDelta,
    ) -> None:
        guard = self._playback_guard(call_id)
        response_key = (
            overflow_delta.response_id,
            overflow_delta.response_generation,
        )
        if response_key in guard.overflowed_responses:
            return
        guard.overflowed_responses.add(response_key)
        if overflow_delta.response_id:
            guard.cancelled_response_ids.add(overflow_delta.response_id)

        queue = self._audio_playout_queue(call_id)
        queue_depth_deltas = queue.qsize()
        queued_frames = self._audio_playout_queue_stats_for_call(call_id).queued_frames
        queued_bytes = self._audio_playout_queue_stats_for_call(call_id).queued_bytes
        cleared_frames, cleared_bytes = self._clear_audio_playout_queue(call_id)
        worker = self._audio_playout_tasks.get(call_id)
        if worker is not None and not worker.done():
            worker.cancel()

        dropped_bytes = cleared_bytes + overflow_delta.pcm_bytes
        dropped_frames = self._audio_playout_frame_count_for_bytes(dropped_bytes)
        payload = self._audio_playout_queue_payload(
            call_id,
            response_id=overflow_delta.response_id,
            response_generation=overflow_delta.response_generation,
            queue_depth_deltas=queue_depth_deltas,
            queued_frames=queued_frames,
            queued_bytes=queued_bytes,
        )
        payload.update({
            "overflowResponseId": overflow_delta.response_id,
            "overflowDeltaFrames": overflow_delta.frame_count,
            "overflowDeltaBytes": overflow_delta.pcm_bytes,
            "droppedFrames": dropped_frames,
            "droppedBytes": dropped_bytes,
            "strategy": "cancel_response_and_clear_playout",
        })
        self._append_event(
            call_id,
            "audio_playout_queue_full",
            "agent",
            payload,
        )
        self._audio_playout_overflow_tasks[call_id] = asyncio.create_task(
            self._handle_audio_playout_overflow(
                call_id=call_id,
                overflow_delta=overflow_delta,
                cleared_frames=cleared_frames,
                cleared_bytes=cleared_bytes,
                dropped_frames=dropped_frames,
                dropped_bytes=dropped_bytes,
            ),
            name=f"ai-call-audio-overflow-{call_id}",
        )

    async def _handle_audio_playout_overflow(
        self,
        *,
        call_id: str,
        overflow_delta: AudioPlayoutDelta,
        cleared_frames: int,
        cleared_bytes: int,
        dropped_frames: int,
        dropped_bytes: int,
    ) -> None:
        cleanup_errors: list[dict[str, str]] = []
        try:
            await self._cancel_audio_playout_worker(call_id)
            if self.audio_publisher is not None:
                try:
                    await self.audio_publisher.stop_audio(call_id)
                except Exception as exc:
                    cleanup_errors.append({
                        "step": "stop_audio",
                        "errorType": type(exc).__name__,
                        "message": str(exc),
                    })
            guard = self._playback_guard(call_id)
            guard.audio_stop_requested = True
            self._append_event(
                call_id,
                "playout_queue_flushed",
                "agent",
                {
                    "source": "agent",
                    "reason": "audio_playout_queue_overflow",
                    "generation": guard.generation,
                    "responseId": overflow_delta.response_id,
                    "forced": True,
                    "clearedFrames": cleared_frames,
                    "clearedBytes": cleared_bytes,
                },
            )

            provider = self._providers.get(call_id)
            if provider is not None:
                try:
                    await provider.cancel_response()
                    guard.cancel_requested = True
                except Exception as exc:
                    cleanup_errors.append({
                        "step": "cancel_response",
                        "errorType": type(exc).__name__,
                        "message": str(exc),
                    })
            self._append_event(
                call_id,
                "audio_playout_response_failed",
                "agent",
                {
                    "reason": "audio_playout_queue_overflow",
                    "responseId": overflow_delta.response_id,
                    "generation": guard.generation,
                    "responseGeneration": overflow_delta.response_generation,
                    "droppedFrames": dropped_frames,
                    "droppedBytes": dropped_bytes,
                    "cleanupErrors": cleanup_errors,
                },
            )
        finally:
            if self._audio_playout_overflow_tasks.get(call_id) is asyncio.current_task():
                self._audio_playout_overflow_tasks.pop(call_id, None)

    def _record_audio_playout_published(
        self,
        call_id: str,
        frame: PcmAudioFrame,
    ) -> None:
        event_timestamp = self._append_event(
            call_id,
            "ai_audio_published",
            "agent",
            {
                "sampleRateHz": frame.sample_rate_hz,
                "bytes": len(frame.data),
            },
        )
        metrics = self.metrics_by_call_id.setdefault(call_id, CallMetrics())
        metrics.mark_audio_published(event_timestamp)
        self._last_ai_audio_published_at[call_id] = event_timestamp
        ai_rms_dbfs = SipBargeInDetector._pcm16_rms_dbfs(frame)
        if ai_rms_dbfs is not None:
            self._last_ai_audio_rms_dbfs[call_id] = ai_rms_dbfs
        self._playback_guard(call_id).current_response_audio_published = True
        self.registry.get(call_id).metrics = metrics.snapshot()

    def _audio_drop_reason(self, call_id: str, provider_event: ProviderEvent) -> str | None:
        guard = self._playback_guard(call_id)
        return self._audio_identity_drop_reason(
            call_id,
            response_id=self._response_id_from_payload(provider_event.payload),
            response_generation=guard.current_response_generation,
        )

    def _queued_audio_drop_reason(
        self,
        call_id: str,
        queued_delta: AudioPlayoutDelta,
    ) -> str | None:
        return self._audio_identity_drop_reason(
            call_id,
            response_id=queued_delta.response_id,
            response_generation=queued_delta.response_generation,
        )

    def _audio_identity_drop_reason(
        self,
        call_id: str,
        *,
        response_id: str | None,
        response_generation: int,
    ) -> str | None:
        guard = self._playback_guard(call_id)
        if (response_id, response_generation) in guard.overflowed_responses:
            return "audio_playout_queue_overflow"
        if response_id and response_id in guard.cancelled_response_ids:
            return "cancelled_response"
        if guard.awaiting_response_start_after_interrupt and (
            not guard.current_response_id or response_id != guard.current_response_id
        ):
            return "awaiting_response_start_after_interrupt"
        if response_generation != guard.generation:
            return "stale_generation"
        if response_id and guard.current_response_id and response_id != guard.current_response_id:
            return "non_current_response"
        if guard.user_speech_active and self._is_barge_in_enabled_for_session(
            self.registry.get(call_id)
        ):
            return "user_speech_active"
        if self._is_audio_suppressed(call_id):
            return "suppressed_after_interrupt"
        if self.registry.get(call_id).status != CallSessionStatus.AI_SPEAKING:
            return "session_not_ai_speaking"
        return None

    def _append_queued_audio_dropped(
        self,
        call_id: str,
        queued_delta: AudioPlayoutDelta,
        reason: str,
        *,
        frames: list[PcmAudioFrame] | None = None,
    ) -> None:
        guard = self._playback_guard(call_id)
        dropped_frames = (
            len(frames)
            if frames is not None
            else queued_delta.frame_count
        )
        dropped_bytes = (
            sum(len(frame.data) for frame in frames)
            if frames is not None
            else queued_delta.pcm_bytes
        )
        self._append_event(
            call_id,
            "stale_audio_dropped",
            "agent",
            {
                "reason": reason,
                "responseId": queued_delta.response_id,
                "currentResponseId": guard.current_response_id,
                "generation": guard.generation,
                "currentResponseGeneration": guard.current_response_generation,
                "deltaFrames": dropped_frames,
                "deltaBytes": dropped_bytes,
            },
        )

    def _append_stale_audio_dropped(
        self,
        call_id: str,
        provider_event: ProviderEvent,
        reason: str,
    ) -> None:
        guard = self._playback_guard(call_id)
        delta = provider_event.payload.get("delta")
        delta_bytes = self._base64_decoded_size(delta) if isinstance(delta, str) else None
        delta_frames = None
        if isinstance(delta, str):
            _, delta_frames = self._audio_playout_delta_shape(delta)
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
                "deltaFrames": delta_frames,
                "deltaBytes": delta_bytes,
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
        cleared_frames, cleared_bytes = self._clear_audio_playout_queue(call_id)
        await self._cancel_audio_playout_worker(call_id)
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
                "clearedFrames": cleared_frames,
                "clearedBytes": cleared_bytes,
            },
        )
        return []

    def _clear_audio_playout_queue(self, call_id: str) -> tuple[int, int]:
        queue = self._audio_playout_queues.get(call_id)
        if queue is None:
            return 0, 0
        cleared_bytes = 0
        while True:
            try:
                queued_delta = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            cleared_bytes += queued_delta.pcm_bytes
            queue.task_done()
        cleared_frames = self._audio_playout_frame_count_for_bytes(cleared_bytes)
        stats = self._audio_playout_queue_stats_for_call(call_id)
        stats.queued_frames = 0
        stats.queued_bytes = 0
        self._set_audio_playout_queue_depth(call_id, 0)
        return cleared_frames, cleared_bytes

    async def _cancel_audio_playout_worker(self, call_id: str) -> None:
        task = self._audio_playout_tasks.get(call_id)
        if task is None or task.done() or task is asyncio.current_task():
            return
        self._audio_playout_tasks.pop(call_id, None)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def _cancel_audio_playout_overflow_task(self, call_id: str) -> None:
        task = self._audio_playout_overflow_tasks.pop(call_id, None)
        if task is None or task.done() or task is asyncio.current_task():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

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

    def _should_continue_no_barge_overlap_turn(self, call_id: str) -> bool:
        session = self.registry.get(call_id)
        if self._is_barge_in_enabled_for_session(session):
            return False
        if not self._has_active_model_response(call_id):
            return False
        turn = self._pending_user_turns.get(call_id)
        return turn is not None and turn.response_requested and bool(turn.transcript)

    def _split_unmaterialized_no_barge_overlap_turn_if_needed(self, call_id: str) -> None:
        session = self.registry.get(call_id)
        if self._is_barge_in_enabled_for_session(session):
            return
        guard = self._playback_guard(call_id)
        if not guard.user_speech_active:
            return
        turn = self._pending_user_turns.get(call_id)
        if (
            turn is None
            or not turn.response_requested
            or turn.transcript_merge_start_index is None
        ):
            return
        merge_start = max(0, min(turn.transcript_merge_start_index, len(turn.transcript_parts)))
        if len(turn.transcript_parts) != merge_start:
            return
        self._pending_user_turns[call_id] = PendingUserTurn(started_at=turn.started_at)
        self._append_event(
            call_id,
            "no_barge_overlap_turn_split_after_ai_done",
            "agent",
            {"reason": "transcript_arrived_after_ai_response_done"},
        )

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
        if self._turn_completed_before_active_response(call_id, turn):
            during_ai_audio = False
        elif self._is_short_answer_to_recent_ai_question(call_id, provider_event):
            during_ai_audio = False
        else:
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

    def _turn_completed_before_active_response(
        self,
        call_id: str,
        turn: PendingUserTurn,
    ) -> bool:
        completed_at = turn.stopped_at
        if (
            turn.browser_segment_ended_at is not None
            and self._has_reliable_short_transcript_audio_evidence(turn)
            and (completed_at is None or turn.browser_segment_ended_at < completed_at)
        ):
            completed_at = turn.browser_segment_ended_at
        if completed_at is None:
            return False
        active_started_at = self._response_lifecycle(call_id).active_started_at
        if active_started_at is None:
            return False
        return completed_at <= active_started_at

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

    def _is_short_answer_to_recent_ai_question(
        self,
        call_id: str,
        provider_event: ProviderEvent,
    ) -> bool:
        if self._has_active_model_response(call_id):
            return False
        question_completed_at = self._last_ai_question_completed_at.get(call_id)
        if question_completed_at is None:
            return False
        elapsed_seconds = (datetime.now(timezone.utc) - question_completed_at).total_seconds()
        if elapsed_seconds < 0 or elapsed_seconds > AI_QUESTION_ANSWER_WINDOW_SECONDS:
            return False
        text = self._transcript_text(provider_event)
        normalized = "".join(ch for ch in text if ch not in " \t\r\n，,。.!！?？")
        return 0 < len(normalized) <= 2

    def _mark_ai_question_completed(
        self,
        call_id: str,
        payload: dict[str, Any],
        timestamp: datetime,
    ) -> None:
        text = self._model_response_transcript(payload)
        if self._looks_like_ai_question(text):
            self._last_ai_question_completed_at[call_id] = timestamp
        else:
            self._last_ai_question_completed_at.pop(call_id, None)

    @classmethod
    def _model_response_transcript(cls, payload: dict[str, Any]) -> str:
        response = payload.get("response")
        if not isinstance(response, dict):
            return ""
        parts: list[str] = []
        output = response.get("output")
        if isinstance(output, list):
            for item in output:
                if not isinstance(item, dict):
                    continue
                content = item.get("content")
                if not isinstance(content, list):
                    continue
                for part in content:
                    if not isinstance(part, dict):
                        continue
                    value = part.get("transcript") or part.get("text")
                    if isinstance(value, str) and value:
                        parts.append(value)
        return "".join(parts).strip()

    @staticmethod
    def _looks_like_ai_question(text: str) -> bool:
        return bool(text.strip().endswith(("?", "？")))

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
        opening_response: bool = False,
    ) -> bool:
        lifecycle = self._response_lifecycle(call_id)
        if self.registry.get(call_id).status in {
            CallSessionStatus.ENDING,
            CallSessionStatus.COMPLETED,
            CallSessionStatus.FAILED,
        }:
            lifecycle.pending_create = False
            lifecycle.pending_input_text = None
            lifecycle.pending_response_is_opening = False
            return False
        if lifecycle.active or lifecycle.cancel_pending:
            lifecycle.pending_create = True
            if input_text:
                lifecycle.pending_input_text = input_text
            if opening_response:
                lifecycle.pending_response_is_opening = True
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
        if self.registry.get(call_id).status in {
            CallSessionStatus.ENDING,
            CallSessionStatus.COMPLETED,
            CallSessionStatus.FAILED,
        }:
            self._clear_response_lifecycle(call_id)
            return False
        lifecycle.active = True
        lifecycle.active_started_at = datetime.now(timezone.utc)
        lifecycle.cancel_pending = False
        lifecycle.cancel_race_ignore_until = None
        lifecycle.pending_create = False
        lifecycle.pending_input_text = None
        lifecycle.pending_response_is_opening = False
        lifecycle.current_response_is_opening = opening_response
        lifecycle.response_generation = self._playback_guard(call_id).generation
        guard = self._playback_guard(call_id)
        guard.cancel_requested = False
        guard.audio_stop_requested = False
        guard.current_response_id = None
        guard.current_response_generation = lifecycle.response_generation
        guard.current_response_audio_published = False
        return True

    def _should_defer_no_barge_response_until_model_done(self, call_id: str) -> bool:
        session = self.registry.get(call_id)
        return (
            not self._is_barge_in_enabled_for_session(session)
            and self._has_active_model_response(call_id)
        )

    def _should_wait_for_no_barge_user_speech(self, call_id: str) -> bool:
        session = self.registry.get(call_id)
        return (
            not self._is_barge_in_enabled_for_session(session)
            and self._playback_guard(call_id).user_speech_active
        )

    def _queue_response_create(self, call_id: str, input_text: str | None = None) -> None:
        lifecycle = self._response_lifecycle(call_id)
        lifecycle.pending_create = True
        if input_text:
            lifecycle.pending_input_text = input_text

    def _mark_response_started(
        self,
        call_id: str,
        payload: dict[str, Any],
        timestamp: datetime | None = None,
    ) -> None:
        timestamp = timestamp or datetime.now(timezone.utc)
        lifecycle = self._response_lifecycle(call_id)
        lifecycle.active = True
        lifecycle.active_started_at = timestamp
        guard = self._playback_guard(call_id)
        response_id = self._response_id_from_payload(payload)
        guard.current_response_id = response_id
        guard.current_response_generation = lifecycle.response_generation
        guard.current_response_audio_published = False
        if self._defer_unstarted_no_barge_response_for_active_user(call_id, response_id):
            return
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

    def _defer_unstarted_no_barge_response_for_active_user(
        self,
        call_id: str,
        response_id: str | None,
    ) -> bool:
        session = self.registry.get(call_id)
        guard = self._playback_guard(call_id)
        if self._is_barge_in_enabled_for_session(session) or not guard.user_speech_active:
            return False
        turn = self._pending_user_turns.get(call_id)
        if turn is None:
            turn = self._pending_turn(call_id)
        if response_id:
            guard.cancelled_response_ids.add(response_id)
        turn.no_barge_unstarted_response_deferred = True
        turn.response_requested = False
        self._append_event(
            call_id,
            "no_barge_unstarted_response_deferred",
            "agent",
            {
                "reason": "user_resumed_before_response_audio",
                "responseId": response_id,
            },
        )
        return True

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
        lifecycle.active_started_at = None
        lifecycle.cancel_pending = False
        lifecycle.current_response_is_opening = False
        guard.cancel_requested = False
        if cancel_was_pending:
            self._mark_provider_cancel_race_window(lifecycle)
        if self.registry.get(call_id).status in {
            CallSessionStatus.ENDING,
            CallSessionStatus.COMPLETED,
            CallSessionStatus.FAILED,
        }:
            lifecycle.pending_create = False
            lifecycle.pending_input_text = None
            lifecycle.pending_response_is_opening = False
            return
        if not lifecycle.pending_create:
            if await self._maybe_recover_sip_confirmed_without_transcript(call_id, provider):
                return
            self._promote_missing_call_end_tool(call_id)
            self._schedule_pending_call_end_nowait(call_id)
            return
        if self._drop_unmaterialized_no_barge_pending_response_if_needed(call_id):
            return
        if self._defer_sip_response_release_if_needed(
            call_id,
            input_text=lifecycle.pending_input_text,
        ):
            return
        input_text = lifecycle.pending_input_text
        opening_response = lifecycle.pending_response_is_opening
        lifecycle.pending_create = False
        lifecycle.pending_input_text = None
        lifecycle.pending_response_is_opening = False
        await self._request_response(
            call_id,
            provider,
            input_text=input_text,
            opening_response=opening_response,
        )

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
        await self._request_response(
            call_id,
            provider,
            input_text=SIP_SHORT_RECOVERY_INPUT_TEXT,
        )
        return True

    def _drop_unmaterialized_no_barge_pending_response_if_needed(self, call_id: str) -> bool:
        session = self.registry.get(call_id)
        if self._is_barge_in_enabled_for_session(session):
            return False
        lifecycle = self._response_lifecycle(call_id)
        if not lifecycle.pending_create or lifecycle.pending_input_text:
            return False
        turn = self._pending_user_turns.get(call_id)
        if (
            turn is None
            or not turn.response_requested
            or turn.transcript_merge_start_index is None
        ):
            return False
        merge_start = max(0, min(turn.transcript_merge_start_index, len(turn.transcript_parts)))
        if len(turn.transcript_parts) > merge_start:
            return False
        lifecycle.pending_create = False
        lifecycle.pending_input_text = None
        self._append_event(
            call_id,
            "no_barge_unmaterialized_pending_response_dropped",
            "agent",
            {"reason": "response_already_requested_for_turn"},
        )
        return True

    async def _maybe_schedule_deferred_no_barge_turn(
        self,
        call_id: str,
        provider: RealtimeProviderProtocol,
    ) -> bool:
        session = self.registry.get(call_id)
        if self._is_barge_in_enabled_for_session(session):
            return False
        turn = self._pending_user_turns.get(call_id)
        if (
            turn is None
            or turn.response_requested
            or turn.stopped_at is None
            or not turn.transcript
            or turn.transcript_merge_start_index is not None
        ):
            return False
        if (
            turn.no_barge_overlap_stopped_during_ai_response
            and not turn.no_barge_unstarted_response_deferred
        ):
            self._append_event(
                call_id,
                "no_barge_overlap_waiting_for_followup",
                "agent",
                {"reason": "overlap_turn_finished_before_ai_done"},
            )
            return False
        await self._maybe_schedule_response_from_turn(
            call_id,
            provider,
            datetime.now(timezone.utc),
        )
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
            local_explicit_intent=True,
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
            CallSessionStatus.ENDING,
            CallSessionStatus.COMPLETED,
            CallSessionStatus.FAILED,
        }:
            return
        if pending_call_end.local_explicit_intent and self.registry.get(call_id).status not in {
            CallSessionStatus.COMPLETED,
            CallSessionStatus.FAILED,
        }:
            self._move_running_session_to_connected_for_call_end(call_id)
        if self.registry.get(call_id).status in {
            CallSessionStatus.AI_SPEAKING,
            CallSessionStatus.USER_SPEAKING,
            CallSessionStatus.INTERRUPTED,
        }:
            return
        if self.call_end_scheduler is None:
            return
        defer_reason = self._call_end_user_turn_deferral_reason(call_id)
        if defer_reason is not None:
            self._defer_pending_call_end_for_user_turn(
                call_id,
                pending_call_end,
                reason=defer_reason,
            )
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

    def _call_end_user_turn_deferral_reason(self, call_id: str) -> str | None:
        session = self.registry.get(call_id)
        if self._is_barge_in_enabled_for_session(session):
            return None
        if self._playback_guard(call_id).user_speech_active:
            return "user_speech_active"
        turn = self._pending_user_turns.get(call_id)
        if turn is None or turn.response_requested:
            return None
        if turn.call_end_acknowledged:
            return None
        if turn.transcript:
            return "user_transcript_pending_response"
        if turn.started_at is None:
            return None
        if turn.stopped_at is None:
            return "user_speech_waiting_for_stop"
        grace = self._call_end_user_turn_grace_seconds()
        if datetime.now(timezone.utc) - turn.stopped_at <= timedelta(seconds=grace):
            return "user_speech_waiting_for_transcript"
        return None

    def _defer_pending_call_end_for_user_turn(
        self,
        call_id: str,
        pending_call_end: PendingCallEnd,
        *,
        reason: str,
    ) -> None:
        existing_task = self._pending_call_end_defer_tasks.get(call_id)
        if existing_task is not None and not existing_task.done():
            return
        delay_seconds = self._call_end_user_turn_grace_seconds()
        if not pending_call_end.deferred_for_user_turn:
            pending_call_end.deferred_for_user_turn = True
            self._append_event(
                call_id,
                "call_end_deferred_for_user_turn",
                "agent",
                {
                    "reason": reason,
                    "retryAfterSeconds": delay_seconds,
                    "toolCallId": pending_call_end.tool_call_id,
                    "toolReason": pending_call_end.tool_reason,
                    "endReason": pending_call_end.end_reason,
                },
            )
        self._pending_call_end_defer_tasks[call_id] = asyncio.create_task(
            self._retry_pending_call_end_after_user_turn_grace(
                call_id,
                pending_call_end,
                delay_seconds,
            ),
            name=f"ai-call-end-user-turn-grace-{call_id}",
        )

    async def _retry_pending_call_end_after_user_turn_grace(
        self,
        call_id: str,
        pending_call_end: PendingCallEnd,
        delay_seconds: float,
    ) -> None:
        try:
            await asyncio.sleep(delay_seconds)
            if self._pending_call_ends.get(call_id) is not pending_call_end:
                return
            if pending_call_end.scheduled:
                return
            if self._pending_call_end_defer_tasks.get(call_id) is asyncio.current_task():
                self._pending_call_end_defer_tasks.pop(call_id, None)
            self._schedule_pending_call_end_nowait(call_id)
        except asyncio.CancelledError:
            raise
        finally:
            if self._pending_call_end_defer_tasks.get(call_id) is asyncio.current_task():
                self._pending_call_end_defer_tasks.pop(call_id, None)

    def _call_end_user_turn_grace_seconds(self) -> float:
        return max(
            CALL_END_USER_TURN_GRACE_SECONDS,
            self.user_turn_stability_delay_seconds,
        )

    def _acknowledge_pending_call_end_if_closing_ack(
        self,
        call_id: str,
        transcript: str,
    ) -> bool:
        pending_call_end = self._pending_call_ends.get(call_id)
        if pending_call_end is None:
            return False
        normalized = self._normalize_call_end_acknowledgement(transcript)
        if normalized not in CALL_END_ACKNOWLEDGEMENT_TEXTS:
            return False
        self._pending_call_end_intents.pop(call_id, None)
        turn = self._pending_turn(call_id)
        turn.transcript_parts = [transcript]
        turn.call_end_acknowledged = True
        self._append_event(
            call_id,
            "call_end_acknowledged",
            "agent",
            {
                "toolCallId": pending_call_end.tool_call_id,
                "toolReason": pending_call_end.tool_reason,
                "endReason": pending_call_end.end_reason,
                "transcriptPreview": self._text_preview(transcript),
            },
        )
        return True

    @staticmethod
    def _normalize_call_end_acknowledgement(text: str) -> str:
        return "".join(
            char.lower() for char in text.strip() if char not in " \t\r\n，。！？,.!?；;：:、"
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
        lifecycle.pending_response_is_opening = False
        lifecycle.current_response_is_opening = False
        lifecycle.response_generation = self._playback_guard(call_id).generation

    def _fail_running_session(
        self,
        call_id: str,
        *,
        end_reason: str,
        failure_stage: str,
        failure_message: str,
        extra_payload: dict[str, Any] | None = None,
    ) -> None:
        self._clear_response_lifecycle(call_id)
        session = self.registry.get(call_id)
        if session.status in {
            CallSessionStatus.ENDING,
            CallSessionStatus.COMPLETED,
            CallSessionStatus.FAILED,
        }:
            return
        self.registry.transition(call_id, CallSessionStatus.FAILED)
        payload: dict[str, Any] = {
            "endReason": end_reason,
            "failureStage": failure_stage,
            "failureMessage": failure_message,
        }
        if extra_payload:
            payload.update(extra_payload)
        self._append_event(
            call_id,
            "session_failed",
            "agent",
            payload,
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
        audio_playout_task = self._audio_playout_tasks.get(call_id)
        if wait_for_playout is None and (
            audio_playout_task is None or audio_playout_task.done()
        ):
            self.registry.transition(call_id, CallSessionStatus.CONNECTED)
            self._schedule_pending_call_end_nowait(call_id)
            self._arm_silence_watchdog(call_id)
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
            audio_playout_task = self._audio_playout_tasks.get(call_id)
            if audio_playout_task is not None and audio_playout_task is not asyncio.current_task():
                await audio_playout_task
            if wait_for_playout is not None:
                await wait_for_playout(call_id)
            if self.ai_speaking_tail_grace_seconds > 0:
                await asyncio.sleep(self.ai_speaking_tail_grace_seconds)
            session = self.registry.get(call_id)
            if session.status == CallSessionStatus.AI_SPEAKING:
                self.registry.transition(call_id, CallSessionStatus.CONNECTED)
            self._schedule_pending_call_end_nowait(call_id)
            self._arm_silence_watchdog(call_id)
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

    def _cancel_pending_call_end_defer_task_nowait(self, call_id: str) -> None:
        task = self._pending_call_end_defer_tasks.pop(call_id, None)
        if task is not None and not task.done():
            task.cancel()

    async def _cancel_pending_call_end_defer_task(self, call_id: str) -> None:
        task = self._pending_call_end_defer_tasks.pop(call_id, None)
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
        instructions = self._with_phone_response_brevity_instructions(instructions)
        instructions = self._with_call_end_tool_instructions(instructions)
        instructions = self._with_final_role_boundary_instructions(instructions)
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
    def _with_phone_response_brevity_instructions(instructions: str) -> str:
        clean_instructions = instructions.strip()
        if PHONE_RESPONSE_BREVITY_INSTRUCTIONS in clean_instructions:
            return clean_instructions
        if not clean_instructions:
            return PHONE_RESPONSE_BREVITY_INSTRUCTIONS
        return f"{clean_instructions}\n\n{PHONE_RESPONSE_BREVITY_INSTRUCTIONS}"

    @staticmethod
    def _with_final_role_boundary_instructions(instructions: str) -> str:
        clean_instructions = instructions.strip()
        if FINAL_ROLE_BOUNDARY_INSTRUCTIONS in clean_instructions:
            return clean_instructions
        if not clean_instructions:
            return FINAL_ROLE_BOUNDARY_INSTRUCTIONS
        return f"{clean_instructions}\n\n{FINAL_ROLE_BOUNDARY_INSTRUCTIONS}"

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
