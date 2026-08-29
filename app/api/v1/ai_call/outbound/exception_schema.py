from __future__ import annotations

from typing import Literal

from pydantic import Field

from .schema import OutboundSchema

ExceptionCategory = Literal[
    "no_answer",
    "rejected",
    "early_hangup",
    "invalid_number",
]
ExceptionDisplayStatus = Literal[
    "PENDING",
    "WAITING",
    "CALLING",
    "CONNECTED",
    "MAXED",
    "UNAVAILABLE",
    "STOPPED",
]


class ExceptionPolicyIn(OutboundSchema):
    interval_days: int = Field(ge=1, le=365)
    max_retry_count: int = Field(ge=1, le=5)


class ExceptionPolicyOut(OutboundSchema):
    category: ExceptionCategory
    interval_days: int
    max_retry_count: int
    retryable: bool


class ExceptionActiveBatchOut(OutboundSchema):
    batch_id: str
    target_count: int
    completed_count: int
    created_by: str
    created_by_name: str | None = None
    started_at: str


class ExceptionSummaryCardOut(OutboundSchema):
    category: ExceptionCategory
    total_count: int = 0
    pending_count: int = 0
    maxed_out_count: int = 0
    policy: ExceptionPolicyOut | None = None
    active_batch: ExceptionActiveBatchOut | None = None
    can_start: bool = False
    disabled_reason: str | None = None


class ExceptionSummaryOut(OutboundSchema):
    cards: list[ExceptionSummaryCardOut]


class ExceptionBatchOut(OutboundSchema):
    accepted: bool = True
    batch_id: str
    category: ExceptionCategory
    status: Literal["RUNNING", "COMPLETED"]
    target_count: int
    interval_days: int
    max_retry_count: int
    created_by: str
    created_by_name: str | None = None
    started_at: str


class ExceptionTargetOut(OutboundSchema):
    target_id: str
    customer_name: str | None = None
    phone_number: str | None = None
    task_id: str
    task_name: str
    category: ExceptionCategory
    source_result: str
    original_attempt_count: int
    retry_count: int
    max_retry_count: int
    status: ExceptionDisplayStatus
    next_attempt_at: str | None = None
    last_attempt_at: str | None = None
    last_result: str | None = None
    call_id: str | None = None
