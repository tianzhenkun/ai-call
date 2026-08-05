from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator


class AgentProfileCreateIn(BaseModel):
    user_id: int = Field(gt=0)
    agent_identity: str = Field(min_length=1, max_length=128)
    enabled: bool = False

    @field_validator("agent_identity")
    @classmethod
    def normalize_agent_identity(cls, value: str) -> str:
        return value.strip()


class AgentProfileUpdateIn(BaseModel):
    enabled: bool


class AgentSceneScopesIn(BaseModel):
    scene_codes: list[str]

    @field_validator("scene_codes")
    @classmethod
    def normalize_scene_codes(cls, values: list[str]) -> list[str]:
        normalized = sorted({value.strip() for value in values if value.strip()})
        if len(normalized) != len(values):
            raise ValueError("场景编码不能为空或重复")
        return normalized


class AgentProfileOut(BaseModel):
    id: str
    tenant_id: str
    agent_identity: str
    user_id: str
    enabled: bool
    scene_codes: list[str] = Field(default_factory=list)


class AgentBootstrapOut(BaseModel):
    profile: AgentProfileOut


class AgentPresenceOnlineIn(BaseModel):
    console_session_id: UUID
    device_preflight_passed: bool


class AgentPresenceSessionIn(BaseModel):
    console_session_id: UUID


class AgentHandoffClaimIn(BaseModel):
    console_session_id: UUID


class AgentMediaReadyIn(BaseModel):
    console_session_id: UUID
    participant_identity: str = Field(min_length=1, max_length=255)


DispositionCode = Literal[
    "resolved",
    "follow_up_required",
    "customer_refused",
    "invalid_contact",
    "other",
]
FollowUpContactChannel = Literal[
    "system_callback",
    "manual_phone",
    "wechat",
    "email",
    "other",
]
FollowUpAttemptResult = Literal[
    "connected",
    "no_answer",
    "busy",
    "rejected",
    "invalid_contact",
    "technical_failure",
]
FollowUpClosedReason = Literal[
    "customer_refused",
    "invalid_contact",
    "created_by_error",
    "no_longer_needed",
    "other",
]
FollowUpNextAction = Literal["continue", "complete", "close"]


class AfterCallWorkIn(BaseModel):
    disposition_code: DispositionCode
    needs_follow_up: bool
    summary: str | None = Field(default=None, max_length=4000)
    customer_callback_at: datetime | None = None

    @field_validator("summary")
    @classmethod
    def normalize_optional_summary(cls, value: str | None) -> str | None:
        normalized = value.strip() if value else ""
        return normalized or None

    @model_validator(mode="after")
    def validate_callback_time(self):
        if self.customer_callback_at is not None and not self.needs_follow_up:
            raise ValueError("只有需要跟进时才能记录客户预约时间")
        return self


class FollowUpAttemptIn(BaseModel):
    contact_channel: FollowUpContactChannel
    attempt_result: FollowUpAttemptResult
    related_call_id: str | None = Field(default=None, min_length=1, max_length=64)
    ring_duration_seconds: int | None = Field(default=None, ge=0)
    error_message: str | None = Field(default=None, max_length=500)
    remark: str | None = Field(default=None, max_length=500)
    customer_callback_at: datetime | None = None

    @field_validator("error_message", "remark")
    @classmethod
    def normalize_optional_text(cls, value: str | None) -> str | None:
        normalized = value.strip() if value else ""
        return normalized or None

    @model_validator(mode="after")
    def validate_result_details(self):
        if self.attempt_result == "technical_failure" and not self.error_message:
            raise ValueError("技术失败必须填写错误摘要")
        if self.customer_callback_at is not None and self.attempt_result != "connected":
            raise ValueError("只有客户已接通并明确预约时才能记录回访时间")
        return self


class FollowUpHandlingResultIn(BaseModel):
    call_id: str | None = Field(default=None, min_length=1, max_length=64)
    contact_channel: Literal["manual_phone", "wechat", "email", "other"] | None = None
    contact_result: FollowUpAttemptResult
    remark: str = Field(min_length=1, max_length=500)
    next_action: FollowUpNextAction
    next_follow_up_at: datetime | None = None
    closed_reason: FollowUpClosedReason | None = None

    @field_validator("call_id")
    @classmethod
    def normalize_call_id(cls, value: str | None) -> str | None:
        normalized = value.strip() if value else ""
        return normalized or None

    @field_validator("remark")
    @classmethod
    def normalize_remark(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("处理备注不能为空")
        return normalized

    @model_validator(mode="after")
    def validate_next_action(self):
        if self.call_id is None and self.contact_channel is None:
            raise ValueError("非电话回拨必须选择联系渠道")
        if self.call_id is None and self.contact_channel == "manual_phone":
            raise ValueError("人工回拨必须关联 callId")
        if self.next_action == "continue" and self.next_follow_up_at is None:
            raise ValueError("继续跟进必须填写下次跟进时间")
        if self.next_action == "complete" and self.contact_result != "connected":
            raise ValueError("只有已接通的联系结果可以办结任务")
        if self.next_action == "close" and self.closed_reason is None:
            raise ValueError("终止跟进必须填写终止原因")
        if self.contact_result in {"no_answer", "busy", "technical_failure"}:
            if self.next_action != "continue":
                raise ValueError("当前联系结果只能继续跟进")
        if self.contact_result in {"rejected", "invalid_contact"}:
            if self.next_action not in {"continue", "close"}:
                raise ValueError("当前联系结果不能办结任务")
        if self.next_action != "continue" and self.next_follow_up_at is not None:
            raise ValueError("只有继续跟进可以填写下次跟进时间")
        if self.next_action != "close" and self.closed_reason is not None:
            raise ValueError("只有终止跟进可以填写终止原因")
        return self


class FollowUpCallIn(BaseModel):
    console_session_id: UUID


class FollowUpCloseIn(BaseModel):
    closed_reason: FollowUpClosedReason
    closed_remark: str | None = Field(default=None, max_length=500)

    @field_validator("closed_remark")
    @classmethod
    def normalize_closed_remark(cls, value: str | None) -> str | None:
        normalized = value.strip() if value else ""
        return normalized or None

    @model_validator(mode="after")
    def validate_other_reason(self):
        if (
            self.closed_reason in {"created_by_error", "no_longer_needed", "other"}
            and not self.closed_remark
        ):
            raise ValueError("当前关闭原因必须填写说明")
        return self


class AgentAdminActionIn(BaseModel):
    confirmed: bool
    reason: str | None = Field(default=None, max_length=500)
