from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import Field, field_validator

from .rule_task_schema import AcceptedCommandOut
from .schema import OutboundSchema


class LinphoneTestScenario(str, Enum):
    AI_ONLY = "ai_only"
    HANDOFF = "handoff"


class LinphoneTestRunIn(OutboundSchema):
    scenario: LinphoneTestScenario


class LinphoneTestCapabilityOut(OutboundSchema):
    enabled: bool
    eligible: bool
    reasons: list[str] = Field(default_factory=list)
    available_agent_count: int = 0
    active_call_id: str | None = None
    can_end_active_call: bool = False


class LinphoneTestAcceptedOut(AcceptedCommandOut):
    task_id: str
    attempt_id: str
    call_id: str

    @field_validator("task_id", "attempt_id", mode="before")
    @classmethod
    def stringify_bigint(cls, value: object) -> str:
        return str(value)


class LinphoneTestStatusOut(OutboundSchema):
    task_id: str
    target_id: str
    attempt_id: str
    call_id: str
    target_status: str
    attempt_status: str
    call_status: str | None = None
    handoff_status: str | None = None
    phase: Literal[
        "dialing",
        "ai_call",
        "waiting_handoff",
        "human_call",
        "completed",
        "failed",
    ]
    elapsed_seconds: int
    end_reason: str | None = None
    error_message: str | None = None
    can_end_active_call: bool

    @field_validator("task_id", "target_id", "attempt_id", mode="before")
    @classmethod
    def stringify_bigint(cls, value: object) -> str:
        return str(value)
