from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

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
from app.services.ai_call.runtime_control.owner_repository import OwnerLease
from app.services.ai_call.runtime_control.provider_stub import ScriptedProviderStub
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


class StartCallHandler:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        provider: ScriptedProviderStub,
        *,
        effect_repository_factory: EffectRepositoryFactory = RuntimeEffectRepository,
        command_repository_factory: CommandRepositoryFactory = RuntimeCommandRepository,
    ) -> None:
        self._session_factory = session_factory
        self._provider = provider
        self._effect_repository_factory = effect_repository_factory
        self._command_repository_factory = command_repository_factory

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

        succeeded = applied_count == len(effect_specs)
        async with self._session_factory.begin() as session:
            completed = await self._command_repository_factory(session).complete(
                command_claim,
                CommandDecision(
                    status=(
                        CommandStatus.SUCCEEDED
                        if succeeded
                        else CommandStatus.RETRY_WAIT
                    ),
                    result={"applied_effect_count": applied_count},
                ),
            )
        return StartHandlerResult(
            command_completed=completed and succeeded,
            applied_effect_count=applied_count,
        )


class EndCallHandler:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        provider: ScriptedProviderStub,
        *,
        effect_repository_factory: EffectRepositoryFactory = RuntimeEffectRepository,
        command_repository_factory: CommandRepositoryFactory = RuntimeCommandRepository,
        max_effect_attempts: int = 32,
    ) -> None:
        self._session_factory = session_factory
        self._provider = provider
        self._effect_repository_factory = effect_repository_factory
        self._command_repository_factory = command_repository_factory
        self._max_effect_attempts = max_effect_attempts

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
                    record.status = "completed"
                    record.ended_at = await read_database_time(session)
            clean = (
                await self._effect_repository_factory(session).mark_cleanup_clean(
                    owner_lease
                )
                if logical_completed
                else False
            )
        return EndHandlerResult(
            logical_end_completed=logical_completed,
            resource_cleanup_status="clean" if clean else "reconciling",
            processed_effect_count=processed_count,
        )
