from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from datetime import datetime
from enum import StrEnum

from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.model import AiCallRecordModel
from app.services.ai_call.runtime_control.command_repository import (
    EndCallIntent,
    RuntimeCommandRepository,
)
from app.services.ai_call.runtime_control.effect_repository import CREATE_EFFECT_TYPES
from app.services.ai_call.runtime_control.models import (
    AiCallRuntimeCommandModel,
    AiCallRuntimeEffectModel,
    AiCallRuntimeWorkerModel,
    AiCallSipLineReservationModel,
)
from app.services.ai_call.runtime_control.timing import read_database_time
from app.services.ai_call.runtime_control.types import EffectStatus
from app.utils.id_util import generate_snowflake_id


class StartupReconcileDecision(StrEnum):
    NO_RESOURCE = "no_resource"
    RESOURCE_PRESENT = "resource_present"
    UNKNOWN = "unknown"


def decide_startup_reconcile(
    effects: Iterable[tuple[str, str | None]],
) -> StartupReconcileDecision:
    observations = tuple(effects)
    if any(status == EffectStatus.APPLIED for status, _error in observations):
        return StartupReconcileDecision.RESOURCE_PRESENT
    if observations and all(
        status == EffectStatus.FAILED and error == "no_resource"
        for status, error in observations
    ):
        return StartupReconcileDecision.NO_RESOURCE
    return StartupReconcileDecision.UNKNOWN


def startup_reconcile_due(
    deadline_at: datetime | None,
    now: datetime,
) -> bool:
    return deadline_at is not None and now >= deadline_at


class StartupReconcileService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        batch_size: int = 32,
        id_generator: Callable[[], int] = generate_snowflake_id,
        database_clock: Callable[[AsyncSession], Awaitable[datetime]] = read_database_time,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        self._session_factory = session_factory
        self._batch_size = batch_size
        self._id_generator = id_generator
        self._database_clock = database_clock

    async def run_once(self) -> int:
        async with self._session_factory() as session:
            now = await self._database_clock(session)
            candidates = (
                await session.execute(
                    select(AiCallRecordModel.tenant_id, AiCallRecordModel.call_id)
                    .where(
                        AiCallRecordModel.runtime_control_mode == "owner_command_v1",
                        AiCallRecordModel.status == "preparing",
                        AiCallRecordModel.terminal_requested_at.is_(None),
                        AiCallRecordModel.startup_reconcile_deadline_at <= now,
                    )
                    .order_by(AiCallRecordModel.startup_reconcile_deadline_at)
                    .limit(self._batch_size)
                )
            ).all()

        resolved = 0
        for tenant_id, call_id in candidates:
            async with self._session_factory.begin() as session:
                if await self._resolve_one(session, str(tenant_id), call_id):
                    resolved += 1
        return resolved

    async def _resolve_one(
        self,
        session: AsyncSession,
        tenant_id: str,
        call_id: str,
    ) -> bool:
        record = await session.scalar(
            select(AiCallRecordModel)
            .where(
                AiCallRecordModel.tenant_id == tenant_id,
                AiCallRecordModel.call_id == call_id,
            )
            .with_for_update()
        )
        if (
            record is None
            or record.status != "preparing"
            or record.terminal_requested_at is not None
        ):
            return False

        worker = None
        if record.runtime_owner_id is not None:
            worker = await session.scalar(
                select(AiCallRuntimeWorkerModel)
                .where(AiCallRuntimeWorkerModel.worker_id == record.runtime_owner_id)
                .with_for_update()
            )
        command = await session.scalar(
            select(AiCallRuntimeCommandModel)
            .where(
                AiCallRuntimeCommandModel.tenant_id == tenant_id,
                AiCallRuntimeCommandModel.call_id == call_id,
                AiCallRuntimeCommandModel.command_type == "START_CALL",
            )
            .with_for_update()
        )
        if command is None or command.status in {
            "DEAD",
            "SUCCEEDED",
            "SUPERSEDED",
            "CANCELED",
        }:
            return False

        effects = list(
            (
                await session.scalars(
                    select(AiCallRuntimeEffectModel)
                    .where(
                        AiCallRuntimeEffectModel.tenant_id == tenant_id,
                        AiCallRuntimeEffectModel.call_id == call_id,
                        AiCallRuntimeEffectModel.effect_type.in_(CREATE_EFFECT_TYPES),
                    )
                    .with_for_update()
                )
            ).all()
        )
        decision = decide_startup_reconcile(
            (effect.status, effect.error_message) for effect in effects
        )
        reservation_present = bool(
            await session.scalar(
                select(
                    exists().where(
                        AiCallSipLineReservationModel.tenant_id == tenant_id,
                        AiCallSipLineReservationModel.call_id == call_id,
                        AiCallSipLineReservationModel.status != "RELEASED",
                    )
                )
            )
        )
        now = await self._database_clock(session)
        if not startup_reconcile_due(record.startup_reconcile_deadline_at, now):
            return False
        if reservation_present and decision == StartupReconcileDecision.NO_RESOURCE:
            decision = StartupReconcileDecision.RESOURCE_PRESENT
        if decision == StartupReconcileDecision.NO_RESOURCE:
            self._release_owner(record, worker, now)
            command.status = "DEAD"
            command.target_owner_id = None
            command.next_retry_at = None
            command.processing_owner_id = None
            command.processing_fencing_token = None
            command.processing_token = None
            command.processing_expires_at = None
            command.error_message = "START_NOT_CREATED"
            command.finished_at = now
            command.updated_at = now
            record.last_applied_command_seq = max(
                record.last_applied_command_seq,
                command.command_seq,
            )
            record.status = "failed"
            record.failure_stage = "startup_reconcile"
            record.failure_message = "START_NOT_CREATED"
            record.ended_at = now
            record.resource_cleanup_status = "clean"
            record.resource_cleanup_error = None
            record.resource_cleanup_next_retry_at = None
            record.resource_cleanup_completed_at = now
            await session.flush()
            return True

        await RuntimeCommandRepository(session).request_end(
            EndCallIntent(
                tenant_id=tenant_id,
                call_id=call_id,
                source="startup_reconcile",
                end_reason=f"startup_uncertain_{decision.value}",
                dedupe_key=f"startup-reconcile:{call_id}",
                evidence={"decision": decision.value},
            )
        )
        record.failure_stage = "startup_reconcile"
        record.failure_message = f"START_UNCERTAIN:{decision.value}"
        await session.flush()
        return True

    @staticmethod
    def _release_owner(
        record: AiCallRecordModel,
        worker: AiCallRuntimeWorkerModel | None,
        now: datetime,
    ) -> None:
        if worker is not None:
            if record.runtime_capacity_class == "active" and worker.active_call_count > 0:
                worker.active_call_count -= 1
            elif (
                record.runtime_capacity_class == "cleanup"
                and worker.active_cleanup_count > 0
            ):
                worker.active_cleanup_count -= 1
            worker.updated_at = now
        record.runtime_owner_id = None
        record.runtime_lease_expires_at = None
        record.runtime_heartbeat_at = now
        record.runtime_capacity_class = "none"
