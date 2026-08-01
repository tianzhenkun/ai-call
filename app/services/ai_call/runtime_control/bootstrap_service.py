from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import AiCallRecordModel
from app.services.ai_call.runtime_control.models import AiCallRuntimeEffectModel
from app.services.ai_call.runtime_control.timing import read_database_time

RuntimeBootstrapPhase = Literal["starting", "ready", "ending", "terminal"]


class RuntimeBootstrapNotFoundError(LookupError):
    pass


class RuntimeBootstrapLegacyError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class RuntimeBootstrapSnapshot:
    call_id: str
    entry_type: str
    phase: RuntimeBootstrapPhase
    room_name: str
    participant_identity: str | None
    runtime_fencing_token: int
    agent_media_ready_at: datetime | None
    terminal_requested_at: datetime | None
    token_available: bool


def _value(value: object) -> object:
    return getattr(value, "value", value)


def build_runtime_bootstrap_snapshot(
    record: object,
    effects: list[object],
    *,
    now: datetime,
) -> RuntimeBootstrapSnapshot:
    """Project a DB snapshot; this function never mints a participant token."""
    if getattr(record, "runtime_control_mode", None) != "owner_command_v1":
        raise RuntimeBootstrapLegacyError(
            "该通话仍由 legacy_local 承载，不能从 owner runtime bootstrap"
        )

    terminal_requested_at = getattr(record, "terminal_requested_at", None)
    status = _value(getattr(record, "status", None))
    cleanup_status = _value(getattr(record, "resource_cleanup_status", None))
    if terminal_requested_at is not None:
        phase: RuntimeBootstrapPhase = (
            "terminal"
            if status in {"completed", "failed"} or cleanup_status == "clean"
            else "ending"
        )
        return RuntimeBootstrapSnapshot(
            call_id=str(record.call_id),
            entry_type=str(record.entry_type),
            phase=phase,
            room_name=str(record.room_name),
            participant_identity=getattr(record, "agent_participant_identity", None),
            runtime_fencing_token=int(getattr(record, "runtime_fencing_token", 0)),
            agent_media_ready_at=getattr(record, "agent_media_ready_at", None),
            terminal_requested_at=terminal_requested_at,
            token_available=False,
        )

    fencing_token = int(getattr(record, "runtime_fencing_token", 0))
    owner_valid = bool(
        getattr(record, "runtime_owner_id", None)
        and fencing_token > 0
        and getattr(record, "runtime_lease_expires_at", None) is not None
        and record.runtime_lease_expires_at > now
    )
    applied_by_type = {
        str(getattr(effect, "effect_type", "")): effect
        for effect in effects
        if _value(getattr(effect, "status", None)) == "APPLIED"
    }
    room_applied = applied_by_type.get("CREATE_ROOM")
    agent_applied = applied_by_type.get("ATTACH_AGENT_PARTICIPANT")
    media_ready = (
        getattr(record, "agent_media_ready_at", None) is not None
        and getattr(record, "agent_resource_generation", None) == fencing_token
        and bool(getattr(record, "agent_participant_identity", None))
    )
    ready = bool(
        owner_valid
        and room_applied is not None
        and agent_applied is not None
        and getattr(room_applied, "resource_generation", None) == fencing_token
        and getattr(agent_applied, "resource_generation", None) == fencing_token
        and media_ready
    )
    return RuntimeBootstrapSnapshot(
        call_id=str(record.call_id),
        entry_type=str(record.entry_type),
        phase="ready" if ready else "starting",
        room_name=str(record.room_name),
        participant_identity=getattr(record, "agent_participant_identity", None),
        runtime_fencing_token=fencing_token,
        agent_media_ready_at=getattr(record, "agent_media_ready_at", None),
        terminal_requested_at=None,
        token_available=False,
    )


class RuntimeBootstrapService:
    """读取 owner runtime 的启动闸门；不连接 LiveKit，也不签发 Token。"""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, *, tenant_id: str, call_id: str) -> RuntimeBootstrapSnapshot:
        record = await self._session.scalar(
            select(AiCallRecordModel).where(
                AiCallRecordModel.tenant_id == tenant_id,
                AiCallRecordModel.call_id == call_id,
            )
        )
        if record is None:
            raise RuntimeBootstrapNotFoundError(f"call {call_id} 不存在")
        if record.runtime_control_mode != "owner_command_v1":
            raise RuntimeBootstrapLegacyError(
                f"call {call_id} 仍由 legacy_local 承载"
            )
        now = await read_database_time(self._session)
        effects = list(
            (
                await self._session.scalars(
                    select(AiCallRuntimeEffectModel)
                    .where(
                        AiCallRuntimeEffectModel.tenant_id == tenant_id,
                        AiCallRuntimeEffectModel.call_id == call_id,
                    )
                    .order_by(AiCallRuntimeEffectModel.id)
                )
            ).all()
        )
        return build_runtime_bootstrap_snapshot(record, effects, now=now)
