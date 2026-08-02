from __future__ import annotations

import json
import secrets
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import exists, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import AiCallRecordModel
from app.services.ai_call.runtime_control.models import (
    AiCallRuntimeCommandModel,
    AiCallRuntimeEffectModel,
    AiCallRuntimeWorkerModel,
    AiCallSipLineReservationModel,
)
from app.services.ai_call.runtime_control.postgres_wakeup import publish_control_wakeup
from app.services.ai_call.runtime_control.timing import read_database_time
from app.services.ai_call.runtime_control.types import CommandStatus, EffectStatus
from app.utils.id_util import generate_snowflake_id

if TYPE_CHECKING:
    from app.api.v1.ai_call.outbound.rule_task_model import (
        AiCallOutboundAttemptModel,
        AiCallOutboundTargetModel,
        AiCallOutboundTaskModel,
    )
    from app.api.v1.ai_call.outbound.sip_line_model import AiCallSipLineModel


@dataclass(frozen=True, slots=True)
class WorkerRegistration:
    deployment_instance_id: str
    startup_id: UUID
    capacity: int
    cleanup_capacity: int


@dataclass(frozen=True, slots=True)
class WorkerLease:
    worker_id: str
    lease_expires_at: datetime


@dataclass(frozen=True, slots=True)
class OwnerLease:
    tenant_id: str
    call_id: str
    owner_id: str
    fencing_token: int
    lease_expires_at: datetime
    capacity_class: str


@dataclass(frozen=True, slots=True)
class OutboundStartRefs:
    task_id: int
    target_id: int
    attempt_id: int
    line_id: int


def build_worker_id(deployment_instance_id: str, startup_id: UUID) -> str:
    deployment_identity = deployment_instance_id.strip()
    if not deployment_identity:
        raise ValueError("deployment_instance_id must not be empty")
    worker_id = f"{deployment_identity}:{startup_id}"
    if len(worker_id) > 128:
        raise ValueError("worker_id exceeds 128 characters")
    return worker_id


class WorkerRegistryRepository:
    def __init__(
        self,
        session: AsyncSession,
        *,
        lease_ttl: timedelta = timedelta(seconds=30),
        database_clock: Callable[[AsyncSession], Awaitable[datetime]] = read_database_time,
    ) -> None:
        self._session = session
        self._lease_ttl = lease_ttl
        self._database_clock = database_clock

    async def register(self, registration: WorkerRegistration) -> WorkerLease:
        if registration.capacity < 0 or registration.cleanup_capacity < 0:
            raise ValueError("worker capacities must be non-negative")

        now = await self._database_clock(self._session)
        expires_at = now + self._lease_ttl
        worker_id = build_worker_id(
            registration.deployment_instance_id,
            registration.startup_id,
        )
        worker = await self._session.scalar(
            select(AiCallRuntimeWorkerModel)
            .where(AiCallRuntimeWorkerModel.worker_id == worker_id)
            .with_for_update()
        )
        if worker is None:
            worker = AiCallRuntimeWorkerModel(
                worker_id=worker_id,
                status="READY",
                capacity=registration.capacity,
                cleanup_capacity=registration.cleanup_capacity,
                heartbeat_at=now,
                lease_expires_at=expires_at,
                created_at=now,
                updated_at=now,
            )
            self._session.add(worker)
        else:
            worker.status = "READY"
            worker.capacity = registration.capacity
            worker.cleanup_capacity = registration.cleanup_capacity
            worker.heartbeat_at = now
            worker.lease_expires_at = expires_at
            worker.updated_at = now
        await self._session.flush()
        return WorkerLease(worker_id=worker_id, lease_expires_at=expires_at)

    async def heartbeat(self, lease: WorkerLease) -> bool:
        now = await self._database_clock(self._session)
        result = await self._session.execute(
            update(AiCallRuntimeWorkerModel)
            .where(
                AiCallRuntimeWorkerModel.worker_id == lease.worker_id,
                AiCallRuntimeWorkerModel.status.in_({"READY", "DRAINING"}),
                AiCallRuntimeWorkerModel.lease_expires_at > now,
            )
            .values(
                heartbeat_at=now,
                lease_expires_at=now + self._lease_ttl,
                updated_at=now,
            )
        )
        return result.rowcount == 1


class DispatcherOwnerRepository:
    def __init__(
        self,
        session: AsyncSession,
        *,
        lease_ttl: timedelta = timedelta(seconds=15),
        database_clock: Callable[[AsyncSession], Awaitable[datetime]] = read_database_time,
        id_generator: Callable[[], int] = generate_snowflake_id,
        reservation_token_generator: Callable[[], str] = (lambda: secrets.token_urlsafe(24)),
    ) -> None:
        self._session = session
        self._lease_ttl = lease_ttl
        self._database_clock = database_clock
        self._id_generator = id_generator
        self._reservation_token_generator = reservation_token_generator

    async def assign_initial_owner(
        self,
        tenant_id: str,
        call_id: str,
    ) -> OwnerLease | None:
        now = await self._database_clock(self._session)
        candidate_ids = list(
            (
                await self._session.scalars(
                    select(AiCallRuntimeWorkerModel.worker_id)
                    .where(
                        AiCallRuntimeWorkerModel.status == "READY",
                        AiCallRuntimeWorkerModel.lease_expires_at > now,
                        AiCallRuntimeWorkerModel.active_call_count
                        < AiCallRuntimeWorkerModel.capacity,
                    )
                    .order_by(
                        AiCallRuntimeWorkerModel.active_call_count,
                        AiCallRuntimeWorkerModel.worker_id,
                    )
                )
            ).all()
        )
        if not candidate_ids:
            return None

        candidate_start_command = await self._read_start_command(tenant_id, call_id)
        if candidate_start_command is None:
            return None
        candidate_payload_json = candidate_start_command.payload_json
        outbound_refs = parse_outbound_start_refs(candidate_payload_json)
        outbound_chain = None
        if outbound_refs is not None:
            outbound_chain = await self._lock_outbound_chain(
                tenant_id,
                outbound_refs,
            )
            if outbound_chain is None:
                return None

        record = await self._lock_record(tenant_id, call_id)
        if record is None or not self._initial_assignment_allowed(record):
            return None
        is_outbound = getattr(record, "entry_type", None) == "outbound"
        if is_outbound:
            if outbound_refs is None or outbound_chain is None:
                return None
            if not _outbound_chain_matches(
                outbound_chain,
                record=record,
                refs=outbound_refs,
                tenant_id=tenant_id,
                call_id=call_id,
            ):
                return None
        elif outbound_refs is not None:
            return None
        if await self._has_provider_resource(tenant_id, call_id):
            return None

        line_id = (
            outbound_refs.line_id
            if outbound_refs is not None
            else _start_command_line_id(candidate_start_command.payload_json)
        )
        if line_id is _INVALID_LINE_ID:
            return None
        line = None
        if line_id is not None:
            line = await self._lock_sip_line(tenant_id, line_id)
            if line is None or line.deleted or not line.enabled or line.max_concurrency <= 0:
                return None
            active_reservation_count = await self._session.scalar(
                select(func.count())
                .select_from(AiCallSipLineReservationModel)
                .where(
                    AiCallSipLineReservationModel.tenant_id == tenant_id,
                    AiCallSipLineReservationModel.line_id == line_id,
                    AiCallSipLineReservationModel.status.in_({
                        "RESERVED",
                        "ACTIVE",
                        "RECONCILE_REQUIRED",
                    }),
                )
            )
            if active_reservation_count >= line.max_concurrency:
                return None

        for worker_id in candidate_ids:
            worker = await self._lock_worker(worker_id)
            if worker is None:
                continue
            start_command = await self._lock_command(tenant_id, call_id, "START_CALL")
            if start_command is None or start_command.status != CommandStatus.PENDING:
                return None
            if outbound_refs is not None:
                locked_refs = parse_outbound_start_refs(start_command.payload_json)
                if (
                    locked_refs != outbound_refs
                    or start_command.payload_json != candidate_payload_json
                ):
                    return None

            now = await self._database_clock(self._session)
            if not _worker_has_active_capacity(worker, now):
                continue

            expires_at = now + self._lease_ttl
            record.runtime_owner_id = worker.worker_id
            record.runtime_fencing_token += 1
            record.runtime_lease_expires_at = expires_at
            record.runtime_heartbeat_at = now
            record.runtime_capacity_class = "active"
            worker.active_call_count += 1
            worker.updated_at = now
            start_command.target_owner_id = worker.worker_id
            start_command.expected_fencing_token = record.runtime_fencing_token
            start_command.updated_at = now
            if outbound_chain is not None:
                _task, _target, attempt = outbound_chain
                attempt.status = "STARTING"
                attempt.updated_at = now
            if line is not None:
                self._session.add(
                    AiCallSipLineReservationModel(
                        id=self._id_generator(),
                        tenant_id=tenant_id,
                        line_id=line.id,
                        call_id=call_id,
                        attempt_id=(
                            outbound_refs.attempt_id if outbound_refs is not None else None
                        ),
                        status="RESERVED",
                        reservation_token=self._reservation_token_generator(),
                        fencing_token=record.runtime_fencing_token,
                        acquired_at=now,
                        reconcile_after=None,
                        released_at=None,
                        error_message=None,
                        created_at=now,
                        updated_at=now,
                    )
                )
            await self._session.flush()
            await publish_control_wakeup(self._session)
            return _owner_lease(record)
        return None

    @staticmethod
    def _initial_assignment_allowed(record: AiCallRecordModel) -> bool:
        return (
            record.runtime_control_mode == "owner_command_v1"
            and record.runtime_owner_id is None
            and record.runtime_fencing_token == 0
            and record.runtime_capacity_class == "none"
            and record.terminal_requested_at is None
        )

    async def _has_provider_resource(self, tenant_id: str, call_id: str) -> bool:
        effect_exists = await self._session.scalar(
            select(
                exists().where(
                    AiCallRuntimeEffectModel.tenant_id == tenant_id,
                    AiCallRuntimeEffectModel.call_id == call_id,
                )
            )
        )
        if effect_exists:
            return True
        return bool(
            await self._session.scalar(
                select(
                    exists().where(
                        AiCallSipLineReservationModel.tenant_id == tenant_id,
                        AiCallSipLineReservationModel.call_id == call_id,
                        AiCallSipLineReservationModel.status != "RELEASED",
                    )
                )
            )
        )

    async def _lock_record(self, tenant_id: str, call_id: str) -> AiCallRecordModel | None:
        return await self._session.scalar(
            select(AiCallRecordModel)
            .where(
                AiCallRecordModel.tenant_id == tenant_id,
                AiCallRecordModel.call_id == call_id,
            )
            .with_for_update()
        )

    async def _lock_worker(self, worker_id: str) -> AiCallRuntimeWorkerModel | None:
        return await self._session.scalar(
            select(AiCallRuntimeWorkerModel)
            .where(AiCallRuntimeWorkerModel.worker_id == worker_id)
            .with_for_update()
        )

    async def _lock_command(
        self,
        tenant_id: str,
        call_id: str,
        command_type: str,
    ) -> AiCallRuntimeCommandModel | None:
        return await self._session.scalar(
            select(AiCallRuntimeCommandModel)
            .where(
                AiCallRuntimeCommandModel.tenant_id == tenant_id,
                AiCallRuntimeCommandModel.call_id == call_id,
                AiCallRuntimeCommandModel.command_type == command_type,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )

    async def _read_start_command(
        self,
        tenant_id: str,
        call_id: str,
    ) -> AiCallRuntimeCommandModel | None:
        return await self._session.scalar(
            select(AiCallRuntimeCommandModel).where(
                AiCallRuntimeCommandModel.tenant_id == tenant_id,
                AiCallRuntimeCommandModel.call_id == call_id,
                AiCallRuntimeCommandModel.command_type == "START_CALL",
            )
        )

    async def _lock_sip_line(
        self,
        tenant_id: str,
        line_id: int,
    ) -> AiCallSipLineModel | None:
        from app.api.v1.ai_call.outbound.sip_line_model import AiCallSipLineModel

        return await self._session.scalar(
            select(AiCallSipLineModel)
            .where(
                AiCallSipLineModel.tenant_id == tenant_id,
                AiCallSipLineModel.id == line_id,
            )
            .with_for_update()
        )

    async def _lock_outbound_chain(
        self,
        tenant_id: str,
        refs: OutboundStartRefs,
    ) -> (
        tuple[
            AiCallOutboundTaskModel,
            AiCallOutboundTargetModel,
            AiCallOutboundAttemptModel,
        ]
        | None
    ):
        from app.api.v1.ai_call.outbound.rule_task_model import (
            AiCallOutboundAttemptModel,
            AiCallOutboundTargetModel,
            AiCallOutboundTaskModel,
        )

        task = await self._session.scalar(
            select(AiCallOutboundTaskModel)
            .where(
                AiCallOutboundTaskModel.tenant_id == tenant_id,
                AiCallOutboundTaskModel.id == refs.task_id,
            )
            .with_for_update()
        )
        if task is None:
            return None
        target = await self._session.scalar(
            select(AiCallOutboundTargetModel)
            .where(
                AiCallOutboundTargetModel.tenant_id == tenant_id,
                AiCallOutboundTargetModel.task_id == refs.task_id,
                AiCallOutboundTargetModel.id == refs.target_id,
            )
            .with_for_update()
        )
        if target is None:
            return None
        attempt = await self._session.scalar(
            select(AiCallOutboundAttemptModel)
            .where(
                AiCallOutboundAttemptModel.tenant_id == tenant_id,
                AiCallOutboundAttemptModel.task_id == refs.task_id,
                AiCallOutboundAttemptModel.target_id == refs.target_id,
                AiCallOutboundAttemptModel.id == refs.attempt_id,
            )
            .with_for_update()
        )
        if attempt is None:
            return None
        return task, target, attempt


class RuntimeOwnerRepository:
    def __init__(
        self,
        session: AsyncSession,
        *,
        lease_ttl: timedelta = timedelta(seconds=15),
        database_clock: Callable[[AsyncSession], Awaitable[datetime]] = read_database_time,
    ) -> None:
        self._session = session
        self._lease_ttl = lease_ttl
        self._database_clock = database_clock

    async def renew(self, lease: OwnerLease) -> OwnerLease | None:
        record = await self._session.scalar(
            select(AiCallRecordModel)
            .where(
                AiCallRecordModel.tenant_id == lease.tenant_id,
                AiCallRecordModel.call_id == lease.call_id,
            )
            .with_for_update()
        )
        worker = await self._session.scalar(
            select(AiCallRuntimeWorkerModel)
            .where(AiCallRuntimeWorkerModel.worker_id == lease.owner_id)
            .with_for_update()
        )
        now = await self._database_clock(self._session)
        if (
            record is None
            or worker is None
            or record.runtime_owner_id != lease.owner_id
            or record.runtime_fencing_token != lease.fencing_token
            or record.runtime_lease_expires_at != lease.lease_expires_at
            or record.runtime_lease_expires_at is None
            or record.runtime_lease_expires_at <= now
            or worker.status not in {"READY", "DRAINING"}
            or worker.lease_expires_at <= now
        ):
            return None
        expires_at = now + self._lease_ttl
        record.runtime_lease_expires_at = expires_at
        record.runtime_heartbeat_at = now
        await self._session.flush()
        return OwnerLease(
            tenant_id=lease.tenant_id,
            call_id=lease.call_id,
            owner_id=lease.owner_id,
            fencing_token=lease.fencing_token,
            lease_expires_at=expires_at,
            capacity_class=record.runtime_capacity_class,
        )

    async def validate(self, lease: OwnerLease) -> bool:
        now = await self._database_clock(self._session)
        return bool(
            await self._session.scalar(
                select(
                    exists().where(
                        AiCallRecordModel.tenant_id == lease.tenant_id,
                        AiCallRecordModel.call_id == lease.call_id,
                        AiCallRecordModel.runtime_owner_id == lease.owner_id,
                        AiCallRecordModel.runtime_fencing_token == lease.fencing_token,
                        AiCallRecordModel.runtime_lease_expires_at == lease.lease_expires_at,
                        AiCallRecordModel.runtime_lease_expires_at > now,
                        exists().where(
                            AiCallRuntimeWorkerModel.worker_id == lease.owner_id,
                            AiCallRuntimeWorkerModel.status.in_({"READY", "DRAINING"}),
                            AiCallRuntimeWorkerModel.lease_expires_at > now,
                        ),
                    )
                )
            )
        )


class RecoveryOwnerRepository:
    def __init__(
        self,
        session: AsyncSession,
        *,
        lease_ttl: timedelta = timedelta(seconds=15),
        database_clock: Callable[[AsyncSession], Awaitable[datetime]] = read_database_time,
    ) -> None:
        self._session = session
        self._lease_ttl = lease_ttl
        self._database_clock = database_clock

    async def assign_cleanup_owner(
        self,
        tenant_id: str,
        call_id: str,
    ) -> OwnerLease | None:
        now = await self._database_clock(self._session)
        candidate_id = await self._session.scalar(
            select(AiCallRuntimeWorkerModel.worker_id)
            .where(
                AiCallRuntimeWorkerModel.status == "READY",
                AiCallRuntimeWorkerModel.lease_expires_at > now,
                AiCallRuntimeWorkerModel.active_cleanup_count
                < AiCallRuntimeWorkerModel.cleanup_capacity,
            )
            .order_by(
                AiCallRuntimeWorkerModel.active_cleanup_count,
                AiCallRuntimeWorkerModel.worker_id,
            )
            .limit(1)
        )
        if candidate_id is None:
            return None

        record = await self._session.scalar(
            select(AiCallRecordModel)
            .where(
                AiCallRecordModel.tenant_id == tenant_id,
                AiCallRecordModel.call_id == call_id,
            )
            .with_for_update()
        )
        if record is None:
            return None

        worker_ids = sorted({
            worker_id
            for worker_id in (record.runtime_owner_id, candidate_id)
            if worker_id is not None
        })
        locked_workers = {
            worker.worker_id: worker
            for worker in (
                await self._session.scalars(
                    select(AiCallRuntimeWorkerModel)
                    .where(AiCallRuntimeWorkerModel.worker_id.in_(worker_ids))
                    .order_by(AiCallRuntimeWorkerModel.worker_id)
                    .with_for_update()
                )
            ).all()
        }
        target_worker = locked_workers.get(candidate_id)
        if target_worker is None:
            return None

        end_command = await self._session.scalar(
            select(AiCallRuntimeCommandModel)
            .where(
                AiCallRuntimeCommandModel.tenant_id == tenant_id,
                AiCallRuntimeCommandModel.call_id == call_id,
                AiCallRuntimeCommandModel.command_type == "END_CALL",
            )
            .with_for_update()
        )
        unfinished_effect = await self._session.scalar(
            select(
                exists().where(
                    AiCallRuntimeEffectModel.tenant_id == tenant_id,
                    AiCallRuntimeEffectModel.call_id == call_id,
                    AiCallRuntimeEffectModel.status != EffectStatus.APPLIED,
                )
            )
        )
        if end_command is None and not unfinished_effect:
            return None

        reservations = list(
            (
                await self._session.scalars(
                    select(AiCallSipLineReservationModel)
                    .where(
                        AiCallSipLineReservationModel.tenant_id == tenant_id,
                        AiCallSipLineReservationModel.call_id == call_id,
                        AiCallSipLineReservationModel.status != "RELEASED",
                    )
                    .order_by(AiCallSipLineReservationModel.id)
                    .with_for_update()
                )
            ).all()
        )

        now = await self._database_clock(self._session)
        if not _cleanup_assignment_allowed(record, now) or not _worker_has_cleanup_capacity(
            target_worker, now
        ):
            return None

        old_worker = (
            locked_workers.get(record.runtime_owner_id)
            if record.runtime_owner_id is not None
            else None
        )
        _release_worker_capacity(old_worker, record.runtime_capacity_class)

        expires_at = now + self._lease_ttl
        record.runtime_owner_id = target_worker.worker_id
        record.runtime_fencing_token += 1
        record.runtime_lease_expires_at = expires_at
        record.runtime_heartbeat_at = now
        record.runtime_capacity_class = "cleanup"
        record.resource_cleanup_status = "reconciling"
        record.resource_cleanup_next_retry_at = None
        for reservation in reservations:
            reservation.fencing_token = record.runtime_fencing_token
            reservation.updated_at = now
        target_worker.active_cleanup_count += 1
        target_worker.updated_at = now
        if old_worker is not None:
            old_worker.updated_at = now
        if end_command is not None:
            end_command.target_owner_id = target_worker.worker_id
            end_command.expected_fencing_token = record.runtime_fencing_token
            end_command.updated_at = now
        await self._session.flush()
        return _owner_lease(record)

    async def park_attention(
        self,
        lease: OwnerLease,
        retry_after: timedelta,
    ) -> bool:
        if retry_after.total_seconds() <= 0:
            raise ValueError("retry_after must be positive")
        record = await self._session.scalar(
            select(AiCallRecordModel)
            .where(
                AiCallRecordModel.tenant_id == lease.tenant_id,
                AiCallRecordModel.call_id == lease.call_id,
            )
            .with_for_update()
        )
        if (
            record is None
            or record.runtime_owner_id != lease.owner_id
            or record.runtime_fencing_token != lease.fencing_token
            or record.runtime_lease_expires_at != lease.lease_expires_at
            or record.runtime_lease_expires_at is None
            or record.terminal_requested_at is None
            or record.runtime_capacity_class not in {"active", "cleanup"}
        ):
            return False

        worker = await self._session.scalar(
            select(AiCallRuntimeWorkerModel)
            .where(AiCallRuntimeWorkerModel.worker_id == lease.owner_id)
            .with_for_update()
        )
        if worker is None:
            return False

        end_command = await self._session.scalar(
            select(AiCallRuntimeCommandModel)
            .where(
                AiCallRuntimeCommandModel.tenant_id == lease.tenant_id,
                AiCallRuntimeCommandModel.call_id == lease.call_id,
                AiCallRuntimeCommandModel.command_type == "END_CALL",
            )
            .with_for_update()
        )
        reconciling_effect_exists = await self._session.scalar(
            select(
                exists().where(
                    AiCallRuntimeEffectModel.tenant_id == lease.tenant_id,
                    AiCallRuntimeEffectModel.call_id == lease.call_id,
                    AiCallRuntimeEffectModel.status == EffectStatus.RECONCILE_REQUIRED,
                    AiCallRuntimeEffectModel.processing_token.is_(None),
                    AiCallRuntimeEffectModel.processing_owner_id.is_(None),
                    AiCallRuntimeEffectModel.processing_expires_at.is_(None),
                )
            )
        )
        unsafe_effect_exists = await self._session.scalar(
            select(
                exists().where(
                    AiCallRuntimeEffectModel.tenant_id == lease.tenant_id,
                    AiCallRuntimeEffectModel.call_id == lease.call_id,
                    or_(
                        AiCallRuntimeEffectModel.status.not_in({
                            EffectStatus.APPLIED,
                            EffectStatus.FAILED,
                            EffectStatus.RECONCILE_REQUIRED,
                        }),
                        AiCallRuntimeEffectModel.processing_owner_id.is_not(None),
                        AiCallRuntimeEffectModel.processing_token.is_not(None),
                        AiCallRuntimeEffectModel.processing_expires_at.is_not(None),
                    ),
                )
            )
        )
        if not reconciling_effect_exists or unsafe_effect_exists:
            return False

        now = await self._database_clock(self._session)
        if record.runtime_lease_expires_at <= now:
            return False

        _release_worker_capacity(worker, record.runtime_capacity_class)
        worker.updated_at = now
        record.runtime_owner_id = None
        record.runtime_lease_expires_at = None
        record.runtime_heartbeat_at = now
        record.runtime_capacity_class = "attention"
        record.resource_cleanup_status = "attention_required"
        record.resource_cleanup_next_retry_at = now + retry_after
        if end_command is not None:
            end_command.target_owner_id = None
            end_command.updated_at = now
        await self._session.flush()
        return True


class OwnerFailClosedWatchdog:
    def __init__(
        self,
        *,
        lease_ttl_seconds: float,
        safety_margin_seconds: float,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds must be positive")
        if safety_margin_seconds < 0 or safety_margin_seconds >= lease_ttl_seconds:
            raise ValueError("safety margin must be non-negative and smaller than lease TTL")
        self._hard_window_seconds = lease_ttl_seconds - safety_margin_seconds
        self._monotonic_clock = monotonic_clock
        self._hard_deadline: float | None = None
        self._tripped = False

    def observe_renewal(self, *, renewal_started_monotonic: float | None = None) -> None:
        if self._tripped:
            return
        if self._hard_deadline is not None and self._monotonic_clock() >= self._hard_deadline:
            self._tripped = True
            return
        started_at = (
            self._monotonic_clock()
            if renewal_started_monotonic is None
            else renewal_started_monotonic
        )
        self._hard_deadline = started_at + self._hard_window_seconds

    def creation_allowed(self) -> bool:
        if self._tripped or self._hard_deadline is None:
            return False
        if self._monotonic_clock() >= self._hard_deadline:
            self._tripped = True
            return False
        return True

    def must_stop_media(self) -> bool:
        return not self.creation_allowed()

    def seconds_until_hard_deadline(self) -> float:
        if not self.creation_allowed() or self._hard_deadline is None:
            return 0.0
        return max(0.0, self._hard_deadline - self._monotonic_clock())

    def trip(self) -> None:
        self._tripped = True


_INVALID_LINE_ID = object()

_OUTBOUND_START_PAYLOAD_KEYS = frozenset({
    "attempt_id",
    "attempt_no",
    "line_code",
    "line_id",
    "prompt_profile_id",
    "scene_code",
    "target_id",
    "task_id",
    "voice",
})


def parse_outbound_start_refs(payload_json: str | None) -> OutboundStartRefs | None:
    if payload_json is None:
        return None
    try:
        payload = json.loads(payload_json)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict) or set(payload) != _OUTBOUND_START_PAYLOAD_KEYS:
        return None
    if type(payload["attempt_no"]) is not int or payload["attempt_no"] <= 0:
        return None
    if not all(
        isinstance(payload[key], str) and payload[key].strip()
        for key in ("line_code", "scene_code", "voice")
    ):
        return None
    prompt_profile_id = payload["prompt_profile_id"]
    if prompt_profile_id is not None and (
        not isinstance(prompt_profile_id, str) or not prompt_profile_id.strip()
    ):
        return None
    parsed_ids = {
        key: _canonical_positive_decimal(payload[key])
        for key in ("task_id", "target_id", "attempt_id", "line_id")
    }
    if any(value is None for value in parsed_ids.values()):
        return None
    return OutboundStartRefs(
        task_id=parsed_ids["task_id"],
        target_id=parsed_ids["target_id"],
        attempt_id=parsed_ids["attempt_id"],
        line_id=parsed_ids["line_id"],
    )


def _canonical_positive_decimal(value: object) -> int | None:
    if (
        not isinstance(value, str)
        or not value.isascii()
        or not value.isdigit()
        or value.startswith("0")
    ):
        return None
    parsed = int(value)
    return parsed if parsed > 0 else None


def _outbound_chain_matches(
    chain: tuple[
        AiCallOutboundTaskModel,
        AiCallOutboundTargetModel,
        AiCallOutboundAttemptModel,
    ],
    *,
    record: AiCallRecordModel,
    refs: OutboundStartRefs,
    tenant_id: str,
    call_id: str,
) -> bool:
    task, target, attempt = chain
    return (
        task.tenant_id == tenant_id
        and task.id == refs.task_id
        and task.status == "RUNNING"
        and task.line_id == refs.line_id
        and target.tenant_id == tenant_id
        and target.task_id == refs.task_id
        and target.id == refs.target_id
        and target.status == "DIALING"
        and attempt.tenant_id == tenant_id
        and attempt.task_id == refs.task_id
        and attempt.target_id == refs.target_id
        and attempt.id == refs.attempt_id
        and attempt.call_id == call_id
        and attempt.status == "QUEUED"
        and attempt.line_id == refs.line_id
        and attempt.attempt_no == target.attempt_count
        and record.entry_type == "outbound"
        and record.business_type == "outbound_attempt"
        and record.business_id == str(refs.attempt_id)
    )


def _start_command_line_id(payload_json: str | None) -> int | None | object:
    if payload_json is None:
        return None
    try:
        payload = json.loads(payload_json)
    except (TypeError, ValueError):
        return _INVALID_LINE_ID
    if not isinstance(payload, dict) or "line_id" not in payload:
        return None
    value = payload["line_id"]
    if isinstance(value, bool):
        return _INVALID_LINE_ID
    try:
        line_id = int(value)
    except (TypeError, ValueError, OverflowError):
        return _INVALID_LINE_ID
    return line_id if line_id > 0 else _INVALID_LINE_ID


def _worker_has_active_capacity(
    worker: AiCallRuntimeWorkerModel,
    now: datetime,
) -> bool:
    return (
        worker.status == "READY"
        and worker.lease_expires_at > now
        and worker.active_call_count < worker.capacity
    )


def _worker_has_cleanup_capacity(
    worker: AiCallRuntimeWorkerModel,
    now: datetime,
) -> bool:
    return (
        worker.status == "READY"
        and worker.lease_expires_at > now
        and worker.active_cleanup_count < worker.cleanup_capacity
    )


def _cleanup_assignment_allowed(record: AiCallRecordModel, now: datetime) -> bool:
    if record.terminal_requested_at is None:
        return False
    if record.runtime_owner_id is not None:
        return (
            record.runtime_lease_expires_at is not None
            and record.runtime_lease_expires_at <= now
            and record.runtime_capacity_class in {"active", "cleanup"}
        )
    if record.runtime_capacity_class == "attention":
        return (
            record.resource_cleanup_status == "attention_required"
            and record.resource_cleanup_next_retry_at is not None
            and record.resource_cleanup_next_retry_at <= now
        )
    return record.runtime_capacity_class == "none"


def _release_worker_capacity(
    worker: AiCallRuntimeWorkerModel | None,
    capacity_class: str,
) -> None:
    if worker is None:
        return
    if capacity_class == "active" and worker.active_call_count > 0:
        worker.active_call_count -= 1
    elif capacity_class == "cleanup" and worker.active_cleanup_count > 0:
        worker.active_cleanup_count -= 1


def _owner_lease(record: AiCallRecordModel) -> OwnerLease:
    if record.runtime_owner_id is None or record.runtime_lease_expires_at is None:
        raise RuntimeError("record does not hold an owner lease")
    return OwnerLease(
        tenant_id=str(record.tenant_id),
        call_id=record.call_id,
        owner_id=record.runtime_owner_id,
        fencing_token=record.runtime_fencing_token,
        lease_expires_at=record.runtime_lease_expires_at,
        capacity_class=record.runtime_capacity_class,
    )
