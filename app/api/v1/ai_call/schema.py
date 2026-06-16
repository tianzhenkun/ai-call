from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field
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
    voice: str | None = Field(default=None, description="Qwen Realtime voice 参数")
    prompt: str | None = Field(default=None, description="本通会话的模型指令")
    business_type: str | None = Field(default=None, description="上游业务类型")
    business_id: str | None = Field(default=None, description="上游业务ID")


class BrowserEventReportRequest(AiCallBaseSchema):
    type: str = Field(description="浏览器事件类型")
    timestamp: datetime | None = Field(default=None, description="浏览器侧事件时间")


class EffectiveConfigOut(AiCallBaseSchema):
    model: str
    voice: str
    prompt_hash: str
    opening_enabled: bool
    opening_message_hash: str
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


class RecordOut(AiCallBaseSchema):
    id: str
    call_id: str
    business_type: str | None = None
    business_id: str | None = None
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


class RecordEventOut(AiCallBaseSchema):
    id: str
    event_id: str
    call_id: str
    event_type: str
    source: str
    event_time: datetime
    payload: dict


class RecordDetailOut(AiCallBaseSchema):
    record: RecordOut
    last_event: RecordEventOut | None = None


class RecordEventListOut(AiCallBaseSchema):
    rows: list[RecordEventOut]
    total: int
