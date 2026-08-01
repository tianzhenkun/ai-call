from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from sqlalchemy import and_, exists, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import AiCallRecordModel
from app.services.ai_call.livekit_room import BrowserRoomToken
from app.services.ai_call.runtime_control.models import (
    AiCallRuntimeEffectModel,
    AiCallRuntimeWorkerModel,
)

_JOINABLE_RECORD_STATUSES = frozenset(
    {
        "ready",
        "connected",
        "user_speaking",
        "ai_thinking",
        "ai_speaking",
        "interrupted",
        "waiting",
    }
)
_TERMINAL_RECORD_STATUSES = frozenset({"ending", "completed", "failed"})


class RuntimeTokenNotFoundError(LookupError):
    error_code = "CALL_NOT_FOUND"


class RuntimeTokenGateError(RuntimeError):
    def __init__(self, error_code: str, message: str) -> None:
        super().__init__(message)
        self.error_code = error_code


@dataclass(frozen=True, slots=True)
class RuntimeTokenGateSnapshot:
    call_id: str
    room_name: str
    participant_identity: str
    runtime_fencing_token: int


@dataclass(frozen=True, slots=True)
class RuntimeIssuedToken:
    call_id: str
    room_name: str
    livekit_url: str
    participant_token: str
    participant_identity: str
    expires_in_seconds: int


class RuntimeTokenGateReader(Protocol):
    async def authorize(
        self,
        *,
        tenant_id: str,
        call_id: str,
    ) -> RuntimeTokenGateSnapshot: ...


class RuntimeBrowserTokenIssuer(Protocol):
    def issue_browser_token(
        self,
        room_name: str,
        participant_identity: str,
        *,
        metadata: dict[str, str],
    ) -> BrowserRoomToken: ...


def evaluate_runtime_token_gate(
    record: object,
    *,
    owner_available: bool,
    room_applied: bool,
    agent_applied: bool,
) -> RuntimeTokenGateSnapshot:
    if getattr(record, "runtime_control_mode", None) != "owner_command_v1":
        raise RuntimeTokenGateError(
            "CALL_NOT_READY",
            "该通话不属于 owner runtime，不能签发新模式 Token",
        )
    status = str(getattr(record, "status", ""))
    if (
        getattr(record, "terminal_requested_at", None) is not None
        or status in _TERMINAL_RECORD_STATUSES
    ):
        raise RuntimeTokenGateError("CALL_ENDING", "通话正在结束或已经结束")
    if not owner_available:
        raise RuntimeTokenGateError("OWNER_UNAVAILABLE", "Runtime Owner 当前不可用")

    fencing_token = int(getattr(record, "runtime_fencing_token", 0))
    participant_identity = str(getattr(record, "participant_identity", "") or "")
    ready = bool(
        getattr(record, "entry_type", None) == "web"
        and status in _JOINABLE_RECORD_STATUSES
        and fencing_token > 0
        and getattr(record, "room_name", None)
        and room_applied
        and agent_applied
        and getattr(record, "agent_media_ready_at", None) is not None
        and getattr(record, "agent_resource_generation", None) == fencing_token
        and getattr(record, "agent_participant_identity", None)
        and participant_identity
    )
    if not ready:
        raise RuntimeTokenGateError("CALL_NOT_READY", "通话媒体尚未达到 Token 签发条件")

    return RuntimeTokenGateSnapshot(
        call_id=str(record.call_id),
        room_name=str(record.room_name),
        participant_identity=participant_identity,
        runtime_fencing_token=fencing_token,
    )


class RuntimeTokenGateRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def authorize(
        self,
        *,
        tenant_id: str,
        call_id: str,
    ) -> RuntimeTokenGateSnapshot:
        owner_available = and_(
            AiCallRecordModel.runtime_owner_id.is_not(None),
            AiCallRecordModel.runtime_fencing_token > 0,
            AiCallRecordModel.runtime_lease_expires_at.is_not(None),
            AiCallRecordModel.runtime_lease_expires_at > func.clock_timestamp(),
            exists().where(
                AiCallRuntimeWorkerModel.worker_id
                == AiCallRecordModel.runtime_owner_id,
                AiCallRuntimeWorkerModel.status.in_({"READY", "DRAINING"}),
                AiCallRuntimeWorkerModel.lease_expires_at > func.clock_timestamp(),
            ),
        )
        room_applied = exists().where(
            AiCallRuntimeEffectModel.tenant_id == AiCallRecordModel.tenant_id,
            AiCallRuntimeEffectModel.call_id == AiCallRecordModel.call_id,
            AiCallRuntimeEffectModel.effect_type == "CREATE_ROOM",
            AiCallRuntimeEffectModel.status == "APPLIED",
            AiCallRuntimeEffectModel.resource_generation
            == AiCallRecordModel.runtime_fencing_token,
        )
        agent_applied = exists().where(
            AiCallRuntimeEffectModel.tenant_id == AiCallRecordModel.tenant_id,
            AiCallRuntimeEffectModel.call_id == AiCallRecordModel.call_id,
            AiCallRuntimeEffectModel.effect_type == "ATTACH_AGENT_PARTICIPANT",
            AiCallRuntimeEffectModel.status == "APPLIED",
            AiCallRuntimeEffectModel.resource_generation
            == AiCallRecordModel.runtime_fencing_token,
        )
        row = (
            await self._session.execute(
                select(
                    AiCallRecordModel,
                    owner_available.label("owner_available"),
                    room_applied.label("room_applied"),
                    agent_applied.label("agent_applied"),
                ).where(
                    AiCallRecordModel.tenant_id == tenant_id,
                    AiCallRecordModel.call_id == call_id,
                )
            )
        ).one_or_none()
        if row is None:
            raise RuntimeTokenNotFoundError(f"call {call_id} 不存在")
        record, owner_ok, room_ok, agent_ok = row
        return evaluate_runtime_token_gate(
            record,
            owner_available=bool(owner_ok),
            room_applied=bool(room_ok),
            agent_applied=bool(agent_ok),
        )


class RuntimeTokenService:
    def __init__(
        self,
        *,
        repository: RuntimeTokenGateReader,
        room_manager: RuntimeBrowserTokenIssuer,
    ) -> None:
        self._repository = repository
        self._room_manager = room_manager

    async def issue_browser_token(
        self,
        *,
        tenant_id: str,
        call_id: str,
    ) -> RuntimeIssuedToken:
        gate = await self._repository.authorize(
            tenant_id=tenant_id,
            call_id=call_id,
        )
        token = self._room_manager.issue_browser_token(
            gate.room_name,
            gate.participant_identity,
            metadata={
                "call_id": gate.call_id,
                "resource_generation": str(gate.runtime_fencing_token),
                "participant_identity": gate.participant_identity,
            },
        )
        return RuntimeIssuedToken(
            call_id=gate.call_id,
            room_name=gate.room_name,
            livekit_url=token.livekit_url,
            participant_token=token.participant_token,
            participant_identity=token.participant_identity,
            expires_in_seconds=token.expires_in_seconds,
        )
