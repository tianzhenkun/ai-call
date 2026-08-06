from __future__ import annotations

import re
from typing import Literal

from pydantic import Field, field_validator, model_validator

from .schema import OutboundSchema

TIME_PATTERN = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
SUPPORTED_RETRY_RESULTS = {"no_answer", "busy", "call_failed"}
MAX_RETRY_COUNT = 5


class CallWindow(OutboundSchema):
    start_time: str
    end_time: str

    @model_validator(mode="after")
    def validate_window(self) -> CallWindow:
        if not TIME_PATTERN.fullmatch(self.start_time) or not TIME_PATTERN.fullmatch(self.end_time):
            raise ValueError("呼叫时段必须使用 HH:mm 格式")
        if self.start_time >= self.end_time:
            raise ValueError("呼叫时段开始时间必须早于结束时间")
        return self


class CallRuleIn(OutboundSchema):
    rule_name: str = Field(min_length=1, max_length=100)
    enabled: bool
    call_windows: list[CallWindow] = Field(min_length=1)
    retry_count: int = Field(ge=0, le=MAX_RETRY_COUNT)
    retry_intervals_minutes: list[int]
    retryable_results: list[str]

    @field_validator("rule_name")
    @classmethod
    def strip_rule_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("规则名称不能为空")
        return value

    @model_validator(mode="after")
    def validate_rule(self) -> CallRuleIn:
        windows = sorted(self.call_windows, key=lambda item: item.start_time)
        if any(
            windows[index - 1].end_time > windows[index].start_time
            for index in range(1, len(windows))
        ):
            raise ValueError("呼叫时段不能重叠")
        if len(self.retry_intervals_minutes) != self.retry_count:
            raise ValueError("重试间隔数量必须与重试次数一致")
        if any(
            not isinstance(interval, int) or interval <= 0
            for interval in self.retry_intervals_minutes
        ):
            raise ValueError("重试间隔必须为正整数")
        if any(result not in SUPPORTED_RETRY_RESULTS for result in self.retryable_results):
            raise ValueError("存在不支持的可重试结果")
        return self


class CallRuleOut(CallRuleIn):
    rule_id: str
    updated_at: str


class RetryableResultMeta(OutboundSchema):
    value: Literal["no_answer", "busy", "call_failed"]
    label: str


class CallRuleMetadataOut(OutboundSchema):
    max_retry_count: int
    retryable_results: list[RetryableResultMeta]


class TaskConfigRequest(OutboundSchema):
    task_name: str = Field(min_length=1, max_length=50)
    task_mode: Literal["single", "batch"]
    answer_mode: Literal["linphone", "web"] = "linphone"
    prompt_profile_id: str | None = None
    scene_code: str = Field(min_length=1, max_length=64)
    voice: str = Field(min_length=1, max_length=128)
    rule_id: str = Field(min_length=1, max_length=64)
    execution_mode: Literal["immediate", "scheduled"]
    scheduled_at: str | None = None
    phone_number: str | None = None
    customer_name: str | None = Field(default=None, max_length=255)

    @field_validator("task_name", "scene_code", "voice", "rule_id")
    @classmethod
    def strip_required_text(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("字段不能为空")
        return value

    @field_validator("phone_number", "customer_name")
    @classmethod
    def strip_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return value.strip() or None

    @model_validator(mode="after")
    def validate_task_config(self) -> TaskConfigRequest:
        if self.execution_mode == "scheduled" and not (self.scheduled_at or "").strip():
            raise ValueError("定时执行必须提供 scheduledAt")
        if self.execution_mode == "immediate":
            self.scheduled_at = None
        if self.task_mode == "batch":
            if self.answer_mode == "web":
                raise ValueError("名单外呼暂不支持 Web 接听")
            self.phone_number = None
            self.customer_name = None
        elif self.answer_mode == "linphone" and not self.phone_number:
            raise ValueError("Linphone 接听任务必须提供 phoneNumber")
        elif self.answer_mode == "web":
            self.phone_number = None
        return self

    def config_dict(self) -> dict:
        result = self.model_dump(mode="json", by_alias=True)
        if self.task_mode == "batch" or self.answer_mode == "web":
            result.pop("phoneNumber", None)
        if self.task_mode == "batch":
            result.pop("customerName", None)
        return result


class SingleValidationRequest(TaskConfigRequest):
    task_mode: Literal["single"]


class CreateTaskRequest(TaskConfigRequest):
    validation_id: str = Field(min_length=1, max_length=64)

    def config_dict(self) -> dict:
        result = super().config_dict()
        result.pop("validationId", None)
        return result


class UpdateTaskScheduleRequest(OutboundSchema):
    task_name: str = Field(min_length=1, max_length=50)
    scheduled_at: str = Field(min_length=1, max_length=32)

    @field_validator("task_name", "scheduled_at")
    @classmethod
    def strip_text(cls, value: str) -> str:
        return value.strip()


class AcceptedCommandOut(OutboundSchema):
    accepted: bool = True


class CreateTaskResultOut(AcceptedCommandOut):
    task_id: str


class OutboundTaskLineSnapshotOut(OutboundSchema):
    line_id: str
    line_code: str
    line_name: str


class OutboundTaskOut(OutboundSchema):
    task_id: str
    task_name: str
    task_mode: Literal["single", "batch"]
    answer_mode: Literal["linphone", "web"]
    status: str
    total_targets: int
    completed_targets: int
    connected_targets: int
    failed_targets: int
    attempt_dialer_types: list[str] = Field(default_factory=list)
    execution_mode: Literal["immediate", "scheduled"]
    scheduled_at: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    prompt_profile_id: str | None = None
    prompt_name: str
    scene_code: str
    voice: str
    voice_name: str | None = None
    voice_type: str | None = None
    voice_target_model: str | None = None
    rule_id: str
    rule_name: str
    rule_summary: str
    line_id: str | None = None
    line_name: str | None = None
    line_snapshot: OutboundTaskLineSnapshotOut | None = None
    created_by_name: str | None = None
    created_at: str
    updated_at: str
    error_message: str | None = None


class OutboundTargetOut(OutboundSchema):
    target_id: str
    task_id: str
    customer_name: str | None = None
    phone_number: str | None = None
    status: str
    attempt_count: int
    latest_result: str | None = None
    latest_dialer_type: str | None = None
    updated_at: str
