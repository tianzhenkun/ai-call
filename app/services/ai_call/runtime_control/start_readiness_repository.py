from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import AiCallRecordModel
from app.services.ai_call.runtime_control.command_repository import CommandClaim
from app.services.ai_call.runtime_control.effect_repository import (
    CREATE_EFFECT_TYPES,
    EffectSpec,
)
from app.services.ai_call.runtime_control.models import AiCallRuntimeEffectModel
from app.services.ai_call.runtime_control.owner_repository import OwnerLease
from app.services.ai_call.runtime_control.timing import read_database_time


class StartReadinessRejected(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class StubStartReadiness:
    applied_effect_count: int
    agent_participant_identity: str
    agent_participant_sid: str
    agent_audio_track_sid: str


def _value(value: object) -> object:
    return getattr(value, "value", value)


def build_stub_start_readiness(
    *,
    call_id: str,
    fencing_token: int,
    specs: Sequence[EffectSpec],
    effects: Sequence[object],
) -> StubStartReadiness | None:
    if not specs:
        return None
    effects_by_key = {
        str(getattr(effect, "idempotency_key", "")): effect for effect in effects
    }
    matched: dict[str, object] = {}
    for spec in specs:
        effect = effects_by_key.get(spec.idempotency_key)
        if (
            effect is None
            or getattr(effect, "effect_type", None) != spec.effect_type
            or getattr(effect, "resource_generation", None) != fencing_token
            or _value(getattr(effect, "status", None)) != "APPLIED"
            or (
                spec.effect_type in CREATE_EFFECT_TYPES
                and not getattr(effect, "provider_reference", None)
            )
        ):
            return None
        matched[spec.effect_type] = effect

    room = matched.get("CREATE_ROOM")
    agent = matched.get("ATTACH_AGENT_PARTICIPANT")
    if room is None or agent is None:
        return None
    agent_reference = str(getattr(agent, "provider_reference", "") or "")
    if not agent_reference:
        return None
    return StubStartReadiness(
        applied_effect_count=len(specs),
        agent_participant_identity=f"agent-{call_id}-g{fencing_token}",
        agent_participant_sid=agent_reference,
        agent_audio_track_sid=f"stub-track-{call_id}-g{fencing_token}",
    )


class RuntimeStartReadinessRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def inspect_applied_effects(
        self,
        command_claim: CommandClaim,
        owner_lease: OwnerLease,
        specs: Sequence[EffectSpec],
    ) -> StubStartReadiness | None:
        if command_claim.command_type != "START_CALL":
            raise StartReadinessRejected("readiness inspection requires START_CALL")
        if not specs:
            return None
        idempotency_keys = {spec.idempotency_key for spec in specs}
        effects = list(
            (
                await self._session.scalars(
                    select(AiCallRuntimeEffectModel).where(
                        AiCallRuntimeEffectModel.tenant_id
                        == command_claim.tenant_id,
                        AiCallRuntimeEffectModel.call_id == command_claim.call_id,
                        AiCallRuntimeEffectModel.command_id == command_claim.command_id,
                        AiCallRuntimeEffectModel.idempotency_key.in_(idempotency_keys),
                    )
                )
            ).all()
        )
        return build_stub_start_readiness(
            call_id=command_claim.call_id,
            fencing_token=owner_lease.fencing_token,
            specs=specs,
            effects=effects,
        )

    async def persist_stub_ready(
        self,
        command_claim: CommandClaim,
        owner_lease: OwnerLease,
        readiness: StubStartReadiness,
    ) -> bool:
        record = await self._session.scalar(
            select(AiCallRecordModel)
            .where(
                AiCallRecordModel.tenant_id == command_claim.tenant_id,
                AiCallRecordModel.call_id == command_claim.call_id,
            )
            .with_for_update()
        )
        now = await read_database_time(self._session)
        if (
            record is None
            or command_claim.command_type != "START_CALL"
            or command_claim.processing_owner_id != owner_lease.owner_id
            or command_claim.processing_fencing_token != owner_lease.fencing_token
            or record.runtime_control_mode != "owner_command_v1"
            or record.runtime_owner_id != owner_lease.owner_id
            or record.runtime_fencing_token != owner_lease.fencing_token
            or record.runtime_lease_expires_at is None
            or record.runtime_lease_expires_at <= now
            or record.terminal_requested_at is not None
        ):
            raise StartReadinessRejected(
                "current Owner/fencing/lease no longer authorizes readiness"
            )
        if record.entry_type not in {"web", "direct_sip", "outbound"}:
            return False
        if record.status not in {"preparing", "ready"}:
            raise StartReadinessRejected(
                f"record status {record.status} does not allow readiness"
            )
        if (
            record.agent_resource_generation is not None
            and record.agent_resource_generation != owner_lease.fencing_token
        ):
            raise StartReadinessRejected("agent readiness generation conflicts")

        record.status = "ready"
        record.agent_participant_identity = readiness.agent_participant_identity
        record.agent_participant_sid = readiness.agent_participant_sid
        record.agent_audio_track_sid = readiness.agent_audio_track_sid
        record.agent_resource_generation = owner_lease.fencing_token
        if record.agent_media_ready_at is None:
            record.agent_media_ready_at = now
        await self._session.flush()
        return True
