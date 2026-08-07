from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, StrictBool, StringConstraints, field_validator
from pydantic.alias_generators import to_camel

VoiceStatus = Literal[
    "CREATING",
    "ENABLED",
    "DISABLED",
    "CREATE_FAILED",
    "DELETING",
    "DELETE_FAILED",
    "DELETED",
]

VoiceAvailabilityStatus = Literal["ENABLED", "DISABLED"]


def _stringify_positive_bigint(value: object) -> str:
    if isinstance(value, bool):
        raise ValueError("id 必须是正 signed bigint")
    if isinstance(value, int):
        number = value
    elif isinstance(value, str) and value.isascii() and value.isdigit():
        number = int(value, 10)
    else:
        raise ValueError("id 必须是十进制整数")
    if not 1 <= number <= 2**63 - 1:
        raise ValueError("id 超出正 signed bigint 范围")
    return str(number)


class VoiceEnrollmentRequest(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        extra="forbid",
    )

    display_name: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=100),
    ]
    gender: Literal["未知", "女声", "男声"]
    language: Literal["zh"]
    transcript: (
        Annotated[
            str,
            StringConstraints(strip_whitespace=True, max_length=2000),
        ]
        | None
    ) = None
    consent_confirmed: StrictBool

    @field_validator("transcript", mode="before")
    @classmethod
    def normalize_transcript(cls, value: object) -> object:
        if isinstance(value, str):
            normalized = value.strip()
            return normalized or None
        return value


class VoiceAvailabilityRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: VoiceAvailabilityStatus


class VoiceEnrollmentAcceptedOut(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        extra="forbid",
    )

    voice_profile_id: str
    enrollment_id: str
    status: Literal["CREATING"]
    display_name: str

    @field_validator("voice_profile_id", "enrollment_id", mode="before")
    @classmethod
    def stringify_id(cls, value: object) -> str:
        return _stringify_positive_bigint(value)


class VoiceProfileOut(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
        alias_generator=to_camel,
        populate_by_name=True,
    )

    id: str
    scope: Literal["GLOBAL", "TENANT"]
    voice: str | None
    display_name: str
    voice_type: str
    gender: str
    language: str | None
    target_model: str
    status: VoiceStatus
    error_message: str | None
    can_preview: bool
    can_delete: bool
    created_at: datetime
    updated_at: datetime

    @field_validator("id", mode="before")
    @classmethod
    def stringify_id(cls, value: object) -> str:
        return _stringify_positive_bigint(value)
