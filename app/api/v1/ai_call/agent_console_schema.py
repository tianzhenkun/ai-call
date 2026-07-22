from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, Field, field_validator


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
