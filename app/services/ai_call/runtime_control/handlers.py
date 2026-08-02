from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.model import AiCallRecordModel
from app.services.ai_call.runtime_control.command_repository import (
    CommandClaim,
    CommandDecision,
    RuntimeCommandRepository,
)
from app.services.ai_call.runtime_control.effect_repository import (
    EffectSpec,
    ProviderObservationKind,
    RuntimeEffectRepository,
)
from app.services.ai_call.runtime_control.owner_repository import (
    OwnerLease,
    RecoveryOwnerRepository,
)
from app.services.ai_call.runtime_control.provider_stub import ScriptedProviderStub
from app.services.ai_call.runtime_control.start_readiness_repository import (
    RuntimeStartReadinessRepository,
)
from app.services.ai_call.runtime_control.timing import read_database_time
from app.services.ai_call.runtime_control.types import CommandStatus


@dataclass(frozen=True, slots=True)
class StartHandlerResult:
    command_completed: bool
    applied_effect_count: int


@dataclass(frozen=True, slots=True)
class EndHandlerResult:
    logical_end_completed: bool
    resource_cleanup_status: str
    processed_effect_count: int


EffectRepositoryFactory = Callable[[AsyncSession], RuntimeEffectRepository]
CommandRepositoryFactory = Callable[[AsyncSession], RuntimeCommandRepository]
RecoveryOwnerRepositoryFactory = Callable[[AsyncSession], RecoveryOwnerRepository]
StartReadinessRepositoryFactory = Callable[
    [AsyncSession], RuntimeStartReadinessRepository
]


def _connected_duration_ms(answered_at: datetime, ended_at: datetime) -> int:
    if answered_at.tzinfo is None and ended_at.tzinfo is not None:
        answered_at = answered_at.replace(tzinfo=ended_at.tzinfo)
    elif answered_at.tzinfo is not None and ended_at.tzinfo is None:
        ended_at = ended_at.replace(tzinfo=answered_at.tzinfo)
    return max(0, int((ended_at - answered_at).total_seconds() * 1_000))


class StartCallHandler:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        provider: ScriptedProviderStub,
        *,
        effect_repository_factory: EffectRepositoryFactory = RuntimeEffectRepository,
        command_repository_factory: CommandRepositoryFactory = RuntimeCommandRepository,
        readiness_repository_factory: StartReadinessRepositoryFactory = (
            RuntimeStartReadinessRepository
        ),
    ) -> None:
        self._session_factory = session_factory
        self._provider = provider
        self._effect_repository_factory = effect_repository_factory
        self._command_repository_factory = command_repository_factory
        self._readiness_repository_factory = readiness_repository_factory

    async def handle(
        self,
        command_claim: CommandClaim,
        owner_lease: OwnerLease,
        effect_specs: Sequence[EffectSpec],
    ) -> StartHandlerResult:
        async with self._session_factory.begin() as session:
            repository = self._effect_repository_factory(session)
            for spec in effect_specs:
                await repository.register(command_claim, spec)

        applied_count = 0
        for _ in range(max(1, len(effect_specs))):
            async with self._session_factory.begin() as session:
                effect_claim = await self._effect_repository_factory(session).claim_next(
                    owner_lease
                )
            if effect_claim is None:
                break
            observation = await self._provider.apply(effect_claim)
            async with self._session_factory.begin() as session:
                submitted = await self._effect_repository_factory(session).submit(
                    effect_claim,
                    observation,
                )
            if not submitted:
                break
            if observation.kind == ProviderObservationKind.RESOURCE_PRESENT:
                applied_count += 1

        async with self._session_factory.begin() as session:
            readiness_repository = self._readiness_repository_factory(session)
            readiness = await readiness_repository.inspect_applied_effects(
                command_claim,
                owner_lease,
                effect_specs,
            )
            succeeded = readiness is not None
            persisted_count = (
                readiness.applied_effect_count if readiness is not None else applied_count
            )
            completed = await self._command_repository_factory(session).complete(
                command_claim,
                CommandDecision(
                    status=(
                        CommandStatus.SUCCEEDED
                        if succeeded
                        else CommandStatus.RETRY_WAIT
                    ),
                    result={"applied_effect_count": persisted_count},
                ),
            )
            if completed and readiness is not None:
                await readiness_repository.persist_stub_ready(
                    command_claim,
                    owner_lease,
                    readiness,
                )
        return StartHandlerResult(
            command_completed=completed and succeeded,
            applied_effect_count=persisted_count,
        )


class EndCallHandler:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        provider: ScriptedProviderStub,
        *,
        effect_repository_factory: EffectRepositoryFactory = RuntimeEffectRepository,
        command_repository_factory: CommandRepositoryFactory = RuntimeCommandRepository,
        recovery_owner_repository_factory: RecoveryOwnerRepositoryFactory = (
            RecoveryOwnerRepository
        ),
        max_effect_attempts: int = 32,
        attention_retry_after: timedelta = timedelta(seconds=30),
    ) -> None:
        self._session_factory = session_factory
        self._provider = provider
        self._effect_repository_factory = effect_repository_factory
        self._command_repository_factory = command_repository_factory
        self._recovery_owner_repository_factory = recovery_owner_repository_factory
        self._max_effect_attempts = max_effect_attempts
        if attention_retry_after.total_seconds() <= 0:
            raise ValueError("attention_retry_after must be positive")
        self._attention_retry_after = attention_retry_after

    async def handle(
        self,
        command_claim: CommandClaim,
        owner_lease: OwnerLease,
    ) -> EndHandlerResult:
        async with self._session_factory.begin() as session:
            await self._effect_repository_factory(session).register_end_graph(command_claim)

        processed_count = 0
        for _ in range(self._max_effect_attempts):
            async with self._session_factory.begin() as session:
                effect_claim = await self._effect_repository_factory(session).claim_next(
                    owner_lease
                )
            if effect_claim is None:
                break
            observation = await self._provider.apply(effect_claim)
            async with self._session_factory.begin() as session:
                submitted = await self._effect_repository_factory(session).submit(
                    effect_claim,
                    observation,
                )
            if not submitted:
                break
            processed_count += 1

        async with self._session_factory.begin() as session:
            command_repository = self._command_repository_factory(session)
            logical_completed = await command_repository.complete(
                command_claim,
                CommandDecision(status=CommandStatus.SUCCEEDED),
            )
            if logical_completed:
                record = await session.scalar(
                    select(AiCallRecordModel)
                    .where(
                        AiCallRecordModel.tenant_id == command_claim.tenant_id,
                        AiCallRecordModel.call_id == command_claim.call_id,
                    )
                    .with_for_update()
                )
                if record is not None:
                    ended_at = await read_database_time(session)
                    record.status = "completed"
                    record.ended_at = ended_at
                    if record.answered_at is not None:
                        record.duration_ms = _connected_duration_ms(
                            record.answered_at,
                            ended_at,
                        )
            clean = (
                await self._effect_repository_factory(session).mark_cleanup_clean(
                    owner_lease
                )
                if logical_completed
                else False
            )
        cleanup_status = "clean" if clean else "reconciling"
        if logical_completed and not clean:
            async with self._session_factory.begin() as session:
                parked = await self._recovery_owner_repository_factory(session).park_attention(
                    owner_lease,
                    self._attention_retry_after,
                )
            if parked:
                cleanup_status = "attention_required"
        return EndHandlerResult(
            logical_end_completed=logical_completed,
            resource_cleanup_status=cleanup_status,
            processed_effect_count=processed_count,
        )
