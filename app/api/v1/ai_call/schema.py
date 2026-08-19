from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic.alias_generators import to_camel

from app.services.ai_call.session_registry import CallSessionStatus


class AiCallBaseSchema(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
        alias_generator=to_camel,
        populate_by_name=True,
        use_enum_values=True,
    )


class CreateWebSessionRequest(AiCallBaseSchema):
    model_config = ConfigDict(extra="forbid")

    voice: str | None = Field(default=None, description="Qwen Realtime voice 参数")
    business_id: str | None = Field(default=None, description="上游业务ID")
    scene_code: str = Field(description="业务场景编码")
    business_params: dict[str, Any] = Field(
        default_factory=dict,
        description="业务侧上下文参数",
    )


class CreateSipSessionRequest(AiCallBaseSchema):
    model_config = ConfigDict(extra="forbid")

    callee_phone_number: str = Field(
        min_length=3,
        max_length=32,
        description="动态被叫号码，服务端线路配置负责主叫和 trunk 能力",
    )
    voice: str | None = Field(default=None, description="Qwen Realtime voice 参数")
    business_id: str | None = Field(default=None, description="上游业务ID")
    scene_code: str = Field(description="业务场景编码")
    business_params: dict[str, Any] = Field(
        default_factory=dict,
        description="业务侧上下文参数",
    )
    ringing_timeout_seconds: int | None = Field(
        default=None,
        ge=1,
        le=120,
        description="SIP 振铃等待秒数",
    )


class BrowserEventReportRequest(AiCallBaseSchema):
    type: str = Field(description="浏览器事件类型")
    timestamp: datetime | None = Field(default=None, description="浏览器侧事件时间")
    diagnostics_version: str | None = Field(default=None, description="浏览器诊断版本")
    source: str | None = Field(default=None, description="浏览器诊断来源")
    track_label: str | None = Field(default=None, description="麦克风轨道标签")
    track_state: dict[str, Any] | None = Field(default=None, description="麦克风轨道状态")
    requested_constraints: dict[str, Any] | None = Field(
        default=None,
        description="页面请求的音频约束",
    )
    track_constraints: dict[str, Any] | None = Field(
        default=None,
        description="浏览器返回的音频轨道约束",
    )
    track_settings: dict[str, Any] | None = Field(
        default=None,
        description="浏览器实际生效的音频轨道设置",
    )
    audio_context: dict[str, Any] | None = Field(
        default=None,
        description="本地音频分析上下文诊断",
    )
    segment_id: str | None = Field(default=None, description="浏览器语音段 ID")
    phase: Literal["started", "updated", "ended"] | None = Field(
        default=None,
        description="浏览器语音段阶段",
    )
    duration_ms: int | None = Field(default=None, ge=0, description="语音段持续毫秒数")
    rms_dbfs: float | None = Field(default=None, description="麦克风 RMS dBFS")
    noise_floor_dbfs: float | None = Field(default=None, description="本地底噪 dBFS")
    snr_db: float | None = Field(default=None, description="语音段相对底噪的信噪比")
    hot_frame_count: int | None = Field(default=None, ge=0, description="连续命中语音帧数")
    remote_audio_active: bool | None = Field(default=None, description="远端 AI 音频是否活跃")
    remote_audio_rms_dbfs: float | None = Field(default=None, description="远端音频 RMS dBFS")


class EffectiveConfigOut(AiCallBaseSchema):
    model: str
    voice: str
    prompt_hash: str
    opening_message_hash: str
    prompt_source_key: str
    barge_in_enabled: bool = False
    vad_type: str
    vad_threshold: float
    vad_silence_duration_ms: int


class WebAudioConstraintsOut(AiCallBaseSchema):
    echo_cancellation: bool
    noise_suppression: bool
    auto_gain_control: bool


class CreateSessionOut(AiCallBaseSchema):
    call_id: str
    room_name: str
    livekit_url: str
    participant_token: str
    participant_identity: str
    status: CallSessionStatus
    effective_config: EffectiveConfigOut
    web_audio_constraints: WebAudioConstraintsOut


class CreateSipSessionOut(AiCallBaseSchema):
    call_id: str
    room_name: str
    participant_identity: str
    status: CallSessionStatus
    effective_config: EffectiveConfigOut
    sip_call_id: str | None = None
    sip_trunk_id: str | None = None
    sip_call_status: str | None = None


class RuntimeStartCallRequest(AiCallBaseSchema):
    model_config = ConfigDict(extra="forbid")

    entry_type: Literal["web", "direct_sip"]
    idempotency_key: str = Field(min_length=1, max_length=128)
    payload: dict[str, Any] = Field(default_factory=dict)
    business_type: str | None = Field(default=None, max_length=32)
    business_id: str | None = Field(default=None, max_length=64)
    scene_code: str | None = Field(default=None, max_length=64)
    prompt_source_key: str | None = Field(default=None, max_length=64)
    allocation_deadline_at: datetime | None = None
    callee_phone_number: str | None = Field(default=None, min_length=5, max_length=32)


class RuntimeStartCallOut(AiCallBaseSchema):
    acceptance_status: Literal["ACCEPTED"] = "ACCEPTED"
    command_id: str
    call_id: str
    command_seq: str
    command_type: str
    status: str


class RuntimeDirectSipStartCallOut(RuntimeStartCallOut):
    callee_phone_number_masked: str


class RuntimeCommandOut(AiCallBaseSchema):
    command_id: str
    call_id: str
    command_seq: str
    command_type: str
    status: str
    result: dict[str, Any] | None = None
    error_message: str | None = None
    created_at: datetime
    claimed_at: datetime | None = None
    finished_at: datetime | None = None


class RuntimeEndCallRequest(AiCallBaseSchema):
    model_config = ConfigDict(extra="forbid")

    dedupe_key: str = Field(min_length=1, max_length=160)
    end_reason: str = Field(default="user_requested", min_length=1, max_length=64)


class RuntimeEndCallOut(AiCallBaseSchema):
    acceptance_status: Literal["ACCEPTED"] = "ACCEPTED"
    call_id: str
    command_id: str
    command_seq: str
    command_status: str


class RuntimeBootstrapOut(AiCallBaseSchema):
    call_id: str
    entry_type: str
    phase: Literal["starting", "ready", "ending", "terminal"]
    room_name: str
    participant_identity: str | None = None
    runtime_fencing_token: int
    agent_media_ready_at: datetime | None = None
    terminal_requested_at: datetime | None = None
    token_available: bool = False
    status: str
    resource_cleanup_status: str
    resource_cleanup_error: str | None = None
    failure_stage: str | None = None
    failure_message: str | None = None


class TokenOut(AiCallBaseSchema):
    call_id: str
    room_name: str
    livekit_url: str
    participant_token: str
    participant_identity: str
    expires_in_seconds: int


class SessionStatusOut(AiCallBaseSchema):
    call_id: str
    status: CallSessionStatus
    room_name: str
    effective_config: EffectiveConfigOut
    started_at: datetime
    last_event_at: datetime
    metrics: dict


class EventOut(AiCallBaseSchema):
    event_id: str
    call_id: str
    type: str
    timestamp: datetime
    source: str
    payload: dict


class EventListOut(AiCallBaseSchema):
    rows: list[EventOut]
    total: int


class EndSessionOut(AiCallBaseSchema):
    call_id: str
    status: CallSessionStatus


class CreateHandoffRequest(AiCallBaseSchema):
    source: str = Field(default="operator", description="转人工请求来源")
    reason: str | None = Field(default=None, max_length=64, description="转人工请求原因")
    request_message: str | None = Field(
        default=None,
        max_length=500,
        description="转人工请求摘要",
    )


class AcceptHandoffRequest(AiCallBaseSchema):
    human_agent_identity: str = Field(
        min_length=1,
        max_length=128,
        description="接管人工身份",
    )


class HandoffAgentStatusRequest(AiCallBaseSchema):
    status: Literal["online", "offline"] = Field(description="坐席人工可用状态")
    skill_group: str | None = Field(
        default=None,
        max_length=64,
        description="坐席技能组，当前默认 default",
    )


class FinishHandoffRequest(AiCallBaseSchema):
    reason: str | None = Field(default=None, max_length=64, description="终态原因")


class FailHandoffRequest(AiCallBaseSchema):
    failure_stage: str = Field(min_length=1, max_length=64, description="失败阶段")
    failure_message: str | None = Field(default=None, max_length=500, description="失败摘要")


class HandoffOut(AiCallBaseSchema):
    id: str
    handoff_id: str
    call_id: str
    room_name: str
    status: str
    request_source: str
    request_reason: str | None = None
    request_message: str | None = None
    human_agent_identity: str | None = None
    requested_at: datetime
    accepted_at: datetime | None = None
    connected_at: datetime | None = None
    ended_at: datetime | None = None
    expires_at: datetime | None = None
    end_reason: str | None = None
    failure_stage: str | None = None
    failure_message: str | None = None


class RuntimeHandoffCommandOut(AiCallBaseSchema):
    acceptance_status: Literal["ACCEPTED"] = "ACCEPTED"
    handoff_id: str
    call_id: str
    handoff_status: str
    command_id: str
    command_seq: str
    command_status: str


class HandoffTokenOut(AiCallBaseSchema):
    call_id: str
    handoff_id: str
    room_name: str
    livekit_url: str
    participant_token: str
    participant_identity: str
    expires_in_seconds: int


class AcceptHandoffOut(AiCallBaseSchema):
    handoff: HandoffOut
    seat_token: HandoffTokenOut


class HandoffAgentOut(AiCallBaseSchema):
    id: str | None = None
    human_agent_identity: str
    skill_group: str
    status: str
    active_handoff_id: str | None = None
    last_seen_at: datetime | None = None
    status_updated_at: datetime | None = None


class HandoffListOut(AiCallBaseSchema):
    rows: list[HandoffOut]
    total: int


class RecordOut(AiCallBaseSchema):
    id: str
    call_id: str
    business_type: str | None = None
    business_id: str | None = None
    scene_code: str | None = None
    prompt_source_key: str | None = None
    entry_type: str
    room_name: str | None = None
    participant_identity: str | None = None
    status: str
    end_reason: str | None = None
    failure_stage: str | None = None
    failure_message: str | None = None
    started_at: datetime
    answered_at: datetime | None = None
    ended_at: datetime | None = None
    duration_ms: int | None = None
    quality_score_status: Literal[
        "pending",
        "processing",
        "completed",
        "failed",
        "not_applicable",
    ] | None = None
    quality_score: int | None = None
    quality_review_result: Literal["excellent", "good", "pass", "fail"] | None = None


class RecordEventOut(AiCallBaseSchema):
    id: str
    event_id: str
    call_id: str
    event_type: str
    source: str
    event_time: datetime
    payload: dict


class RecordExecutionConfigOut(AiCallBaseSchema):
    prompt_profile_id: str | None = None
    prompt_name: str | None = None
    scene_code: str | None = None
    voice: str | None = None
    voice_name: str | None = None
    rule_name: str | None = None


class RecordAfterCallWorkOut(AiCallBaseSchema):
    agent_identity: str
    disposition_code: str
    summary: str | None = None
    needs_follow_up: bool
    submitted_at: datetime


class RecordFollowUpOut(AiCallBaseSchema):
    id: str
    status: str
    reason: str
    source_call_id: str
    source_record: RecordOut | None = None
    callback_records: list[RecordOut] = Field(default_factory=list)


class RecordDetailOut(AiCallBaseSchema):
    record: RecordOut
    last_event: RecordEventOut | None = None
    execution_config: RecordExecutionConfigOut | None = None
    after_call_work: RecordAfterCallWorkOut | None = None
    follow_up: RecordFollowUpOut | None = None


class RecordEventListOut(AiCallBaseSchema):
    rows: list[RecordEventOut]
    total: int


class InterruptSummaryOut(AiCallBaseSchema):
    call_id: str
    interrupt_candidate_count: int
    interrupt_confirmed_count: int
    candidate_not_confirmed_count: int
    browser_to_provider_ms: int | None = None
    provider_to_confirmed_ms: int | None = None
    browser_to_confirmed_ms: int | None = None
    stale_audio_dropped_count: int
    stale_audio_dropped_bytes: int
    playout_flush_count: int
    duplicate_end_request: bool
    agent_start_failed: bool
    verdict: str
    issues: list[str]


class SemanticAnalysisOut(AiCallBaseSchema):
    id: str | None = None
    call_id: str
    scene_code: str | None = None
    analysis_scene_code: str
    analysis_status: str
    analysis_result: dict[str, Any] | None = None
    analysis_error: str | None = None
    analysis_retry_count: int
    analysis_started_at: datetime | None = None
    analysis_finished_at: datetime | None = None
    transcript_hash: str | None = None
    transcript_snapshot: dict[str, Any] | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class QualityScoreOut(AiCallBaseSchema):
    id: str
    call_id: str
    status: Literal["pending", "processing", "completed", "failed"]
    score: int | None = None
    reason: str | None = None
    error_message: str | None = None
    model_version: str
    retry_count: int
    started_at: datetime | None = None
    finished_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class QualityReviewOut(AiCallBaseSchema):
    id: str
    call_id: str
    quality_result: Literal["excellent", "good", "pass", "fail"]
    quality_reason: str | None = None
    reviewed_by: str
    reviewed_by_name: str | None = None
    reviewed_at: datetime


class QualityDetailOut(AiCallBaseSchema):
    score: QualityScoreOut | None = None
    review: QualityReviewOut | None = None


class QualityReviewRequest(AiCallBaseSchema):
    model_config = ConfigDict(extra="forbid")

    quality_result: Literal["excellent", "good", "pass", "fail"]
    quality_reason: str | None = None


class RecordingTrackOut(AiCallBaseSchema):
    id: str
    call_id: str
    room_name: str
    track_role: str
    participant_identity: str
    handoff_id: str | None = None
    status: str
    egress_id: str | None = None
    oss_id: str | None = None
    object_name: str | None = None
    play_url: str | None = None
    started_at: datetime
    ended_at: datetime | None = None
    duration_ms: int | None = None
    failure_stage: str | None = None
    failure_message: str | None = None
    stop_requested_at: datetime | None = None
    verify_attempts: int | None = None
    next_verify_at: datetime | None = None
    verify_deadline_at: datetime | None = None
    last_verify_at: datetime | None = None
    last_verify_error: str | None = None


class AsrJobOut(AiCallBaseSchema):
    id: str
    call_id: str
    track_id: str
    track_role: str
    participant_identity: str
    provider: str
    model: str
    status: str
    task_id: str | None = None
    source_url: str | None = None
    transcription_url: str | None = None
    submitted_at: datetime | None = None
    completed_at: datetime | None = None
    segment_count: int | None = None
    failure_stage: str | None = None
    failure_message: str | None = None


class RecordingOut(AiCallBaseSchema):
    id: str
    call_id: str
    room_name: str
    status: str
    egress_id: str | None = None
    oss_id: str | None = None
    object_name: str | None = None
    play_url: str | None = None
    started_at: datetime
    ended_at: datetime | None = None
    duration_ms: int | None = None
    failure_stage: str | None = None
    failure_message: str | None = None
    stop_requested_at: datetime | None = None
    verify_attempts: int | None = None
    next_verify_at: datetime | None = None
    verify_deadline_at: datetime | None = None
    last_verify_at: datetime | None = None
    last_verify_error: str | None = None
    tracks: list[RecordingTrackOut] = Field(default_factory=list)
    asr_jobs: list[AsrJobOut] = Field(default_factory=list)


class DialogueSegmentOut(AiCallBaseSchema):
    id: str | None = None
    call_id: str
    segment_no: int
    speaker_type: str
    speaker_identity: str | None = None
    source: str
    source_segment_id: str
    text: str
    segment_status: str
    started_at: datetime | None = None
    ended_at: datetime | None = None
    duration_ms: int | None = None
    audio_start_ms: int | None = None
    audio_end_ms: int | None = None
    failure_stage: str | None = None
    failure_message: str | None = None


class DialogueSegmentListOut(AiCallBaseSchema):
    rows: list[DialogueSegmentOut]
    total: int


PROMPT_PROVIDER_STATIC_PROFILE = "static_profile"


class PromptProfileBaseRequest(AiCallBaseSchema):
    model_config = ConfigDict(extra="forbid")

    scene_code: str = Field(min_length=1, max_length=64, description="业务场景编码")
    name: str = Field(min_length=1, max_length=100, description="配置名称")
    provider_key: str = Field(
        default=PROMPT_PROVIDER_STATIC_PROFILE,
        min_length=1,
        max_length=64,
        description="提示词来源模式",
    )
    prompt_text: str | None = Field(default=None, description="固定提示词")
    opening_message: str | None = Field(default=None, max_length=1000, description="固定开场白")

    @model_validator(mode="after")
    def validate_static_content(self) -> "PromptProfileBaseRequest":
        if (
            self.provider_key == PROMPT_PROVIDER_STATIC_PROFILE
            and not (self.prompt_text or "").strip()
        ):
            raise ValueError("固定提示词不能为空")
        if (
            self.provider_key == PROMPT_PROVIDER_STATIC_PROFILE
            and not (self.opening_message or "").strip()
        ):
            raise ValueError("固定开场白不能为空")
        return self


class PromptProfileCreateRequest(PromptProfileBaseRequest):
    pass


class PromptProfileUpdateRequest(PromptProfileBaseRequest):
    knowledge_version_snapshot_hash: str | None = Field(
        default=None,
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
        description="从知识草稿应用时的来源版本快照",
    )


class PromptProfileOut(AiCallBaseSchema):
    id: str
    scene_code: str
    name: str
    provider_key: str
    prompt_text: str | None = None
    opening_message: str | None = None
    created_at: datetime
    updated_at: datetime


class VoiceProfileCreateRequest(AiCallBaseSchema):
    model_config = ConfigDict(extra="forbid")

    voice: str = Field(min_length=1, max_length=128, description="百炼返回的 voice 参数")
    display_name: str = Field(min_length=1, max_length=100, description="音色展示名")
    gender: str = Field(default="未知", max_length=16, description="音色性别")
    target_model: str | None = Field(
        default=None,
        max_length=64,
        description="适用 Qwen Omni Realtime 模型，默认使用当前运行模型",
    )
    description: str | None = Field(default=None, max_length=500, description="音色说明")
    sort_order: int = Field(default=1000, ge=0, le=999999, description="排序")
    remark: str | None = Field(default=None, max_length=500, description="备注")


class VoiceProfileOut(AiCallBaseSchema):
    id: str
    voice: str
    display_name: str
    voice_type: str
    gender: str
    target_model: str
    description: str | None = None
    sort_order: int
    remark: str | None = None
    created_at: datetime
    updated_at: datetime


class PromptComponentOut(AiCallBaseSchema):
    component_key: str
    name: str
    content: str


class PromptCommonConfigUpdateRequest(AiCallBaseSchema):
    model_config = ConfigDict(extra="forbid")

    content: str = Field(default="", max_length=20_000)


class PromptCommonConfigOut(AiCallBaseSchema):
    content: str
    updated_at: datetime | None = None


class PromptProfilePreviewRequest(AiCallBaseSchema):
    model_config = ConfigDict(extra="forbid")

    business_id: str | None = Field(default=None, description="业务ID")
    scene_code: str = Field(description="业务场景编码")
    business_params: dict[str, Any] = Field(default_factory=dict, description="业务侧上下文参数")


class PromptProfilePreviewOut(AiCallBaseSchema):
    instructions: str
    opening_message: str
    prompt_hash: str
    opening_message_hash: str
    prompt_source_key: str
    barge_in_enabled: bool = False


class ProductInfoSourceOut(AiCallBaseSchema):
    claim: str
    chunk_id: str
    version_id: str
    version_no: int
    source_filename: str
    page_no: int | None = None
    section_path: str | None = None
    start_ms: int | None = None
    end_ms: int | None = None
    excerpt: str


class ProductInfoConflictOut(AiCallBaseSchema):
    topic: str
    description: str
    source_chunk_ids: list[str]


class ProductInfoExtractOut(AiCallBaseSchema):
    draft_text: str
    sources: list[ProductInfoSourceOut]
    conflicts: list[ProductInfoConflictOut]
    source_version_ids: list[str]
    version_snapshot_hash: str
