from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator
from pydantic.alias_generators import to_camel

VoiceStatus = Literal[
    "CREATING",
    "ENABLED",
    "CREATE_FAILED",
    "DELETING",
    "DELETE_FAILED",
    "DELETED",
]


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
        return str(value)
