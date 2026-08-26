from __future__ import annotations

from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_serializer,
    model_validator,
)
from pydantic.alias_generators import to_camel


class OutboundSchema(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        extra="forbid",
    )


class BatchValidationRequest(OutboundSchema):
    task_name: str = Field(min_length=1, max_length=50)
    task_mode: Literal["batch"]
    answer_mode: Literal["linphone"] = "linphone"
    prompt_profile_id: str | None = None
    scene_code: str = Field(min_length=1, max_length=64)
    voice: str = Field(min_length=1, max_length=128)
    rule_id: str = Field(min_length=1, max_length=64)
    execution_mode: Literal["immediate", "scheduled"]
    scheduled_at: str | None = None

    @field_validator("task_name", "scene_code", "voice", "rule_id")
    @classmethod
    def strip_required_text(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("字段不能为空")
        return stripped

    @model_validator(mode="after")
    def validate_schedule(self) -> BatchValidationRequest:
        if self.execution_mode == "scheduled" and not (self.scheduled_at or "").strip():
            raise ValueError("定时执行必须提供 scheduledAt")
        if self.execution_mode == "immediate":
            self.scheduled_at = None
        return self


class ValidationResultOut(OutboundSchema):
    validation_id: str
    status: Literal["VALIDATING", "PASSED", "FAILED", "SYSTEM_ERROR"]
    valid_target_count: int = 0
    issue_count: int = 0
    issue_stats: dict[str, int] = Field(default_factory=dict)
    error_message: str | None = None
    accepted: bool = False
    retryable: bool = False
    retry_action: Literal["REUPLOAD", "RETRY_VALIDATION"] | None = None

    @model_serializer(mode="wrap")
    def serialize_result(self, serializer):
        result = serializer(self)
        if self.retry_action is None:
            result.pop("retryAction", None)
        return result


class ValidationIssueOut(OutboundSchema):
    issue_id: str
    row_number: int
    phone_number: str | None = None
    customer_name: str | None = None
    reasons: list[str] = Field(default_factory=list)
    duplicate_row_numbers: list[int] = Field(default_factory=list)
