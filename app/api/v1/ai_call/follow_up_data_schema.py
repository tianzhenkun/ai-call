from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

FollowUpClassification = Literal[
    "interested",
    "nurturing",
    "low_value",
    "converted",
]
LowValueReason = Literal[
    "explicit_rejection",
    "no_current_need",
    "customer_mismatch",
    "non_target_customer",
    "invalid_contact",
    "other",
]


class FollowUpDataClassificationIn(BaseModel):
    classification: FollowUpClassification
    reason: str = Field(min_length=1, max_length=500)
    conclusion: str | None = Field(default=None, max_length=4000)
    low_value_reason: LowValueReason | None = None
    expected_version: int = Field(ge=1)

    @field_validator("reason")
    @classmethod
    def normalize_reason(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("调整原因不能为空")
        return normalized

    @field_validator("conclusion")
    @classmethod
    def normalize_conclusion(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("沟通结论不能为空")
        return normalized

    @model_validator(mode="after")
    def validate_low_value_reason(self):
        if self.classification == "low_value" and self.low_value_reason is None:
            raise ValueError("选择低价值时必须填写低价值原因")
        if self.classification != "low_value" and self.low_value_reason is not None:
            raise ValueError("只有低价值分类可以填写低价值原因")
        return self


class FollowUpDataScheduleIn(BaseModel):
    follow_up_reason: str = Field(min_length=1, max_length=500)
    next_follow_up_at: datetime
    expected_version: int = Field(ge=1)

    @field_validator("follow_up_reason")
    @classmethod
    def normalize_reason(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("回访原因不能为空")
        return normalized

    @field_validator("next_follow_up_at")
    @classmethod
    def validate_follow_up_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("计划回访时间必须包含时区")
        return value
