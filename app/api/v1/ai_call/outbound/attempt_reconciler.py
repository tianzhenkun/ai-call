from __future__ import annotations

import asyncio
import secrets
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.model import AiCallRecordModel
from app.core.logger import log
from app.services.ai_call.runtime_control.models import (
    AiCallRuntimeCommandModel,
    AiCallRuntimeEffectModel,
    AiCallSipLineReservationModel,
)
from app.services.ai_call.runtime_control.timing import read_database_time
from app.services.ai_call.runtime_control.types import CommandStatus, EffectStatus

from .attempt_projection import (
    apply_terminal_projection,
    refresh_task_counters,
    terminal_attempt_decision,
)
from .media_evidence import has_persisted_media_evidence
from .rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)


@dataclass(frozen=True, slots=True)
class AttemptReconcileClaim:
    tenant_id: str
    task_id: int
    target_id: int
    attempt_id: int
    call_id: str
    reconcile_owner_id: str
    reconcile_token: str
    reconcile_expires_at: datetime


@dataclass(frozen=True, slots=True)
class AttemptProjectionResult:
    attempt_id: int
    previous_status: str
    status: str


class OutboundAttemptReconciler:
    """只根据数据库事实单调投影外呼 Attempt，不执行 Provider 副作用。"""

    def __init__(
        self,
        session: AsyncSession,
        *,
        worker_id: str,
        lease_ttl: timedelta = timedelta(seconds=30),
        retry_after: timedelta = timedelta(seconds=1),
        token_generator: Callable[[], str] = lambda: secrets.token_urlsafe(24),
        database_clock: Callable[[AsyncSession], Awaitable[datetime]] = read_database_time,
    ) -> None:
        if not worker_id.strip():
            raise ValueError("worker_id must not be empty")
        if lease_ttl.total_seconds() <= 0:
            raise ValueError("lease_ttl must be positive")
        if retry_after.total_seconds() <= 0:
            raise ValueError("retry_after must be positive")
        self._session = session
        self._worker_id = worker_id
        self._lease_ttl = lease_ttl
        self._retry_after = retry_after
        self._token_generator = token_generator
        self._database_clock = database_clock

    async def claim_next(self) -> AttemptReconcileClaim | None:
        now = await self._database_clock(self._session)
        attempt = await self._session.scalar(
            select(AiCallOutboundAttemptModel)
            .where(
                AiCallOutboundAttemptModel.status.in_(
                    {"QUEUED", "STARTING", "DIALING", "IN_CALL"}
                ),
                or_(
                    AiCallOutboundAttemptModel.reconcile_after.is_(None),
                    AiCallOutboundAttemptModel.reconcile_after <= now,
                ),
                or_(
                    and_(
                        AiCallOutboundAttemptModel.reconcile_owner_id.is_(None),
                        AiCallOutboundAttemptModel.reconcile_token.is_(None),
                        AiCallOutboundAttemptModel.reconcile_expires_at.is_(None),
                    ),
                    AiCallOutboundAttemptModel.reconcile_expires_at <= now,
                ),
            )
            .order_by(
                AiCallOutboundAttemptModel.reconcile_after,
                AiCallOutboundAttemptModel.created_at,
                AiCallOutboundAttemptModel.id,
            )
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if attempt is None:
            return None
        token = self._token_generator()
        expires_at = now + self._lease_ttl
        attempt.reconcile_owner_id = self._worker_id
        attempt.reconcile_token = token
        attempt.reconcile_expires_at = expires_at
        attempt.reconcile_after = None
        attempt.reconcile_attempt_count += 1
        attempt.updated_at = now
        await self._session.flush()
        return AttemptReconcileClaim(
            tenant_id=attempt.tenant_id,
            task_id=attempt.task_id,
            target_id=attempt.target_id,
            attempt_id=attempt.id,
            call_id=attempt.call_id,
            reconcile_owner_id=self._worker_id,
            reconcile_token=token,
            reconcile_expires_at=expires_at,
        )

    async def submit(
        self,
        claim: AttemptReconcileClaim,
    ) -> AttemptProjectionResult | None:
        task = await self._session.scalar(
            select(AiCallOutboundTaskModel)
            .where(
                AiCallOutboundTaskModel.tenant_id == claim.tenant_id,
                AiCallOutboundTaskModel.id == claim.task_id,
            )
            .with_for_update()
        )
        if task is None:
            return None
        target = await self._session.scalar(
            select(AiCallOutboundTargetModel)
            .where(
                AiCallOutboundTargetModel.tenant_id == claim.tenant_id,
                AiCallOutboundTargetModel.task_id == claim.task_id,
                AiCallOutboundTargetModel.id == claim.target_id,
            )
            .with_for_update()
        )
        if target is None:
            return None
        attempt = await self._session.scalar(
            select(AiCallOutboundAttemptModel)
            .where(
                AiCallOutboundAttemptModel.tenant_id == claim.tenant_id,
                AiCallOutboundAttemptModel.task_id == claim.task_id,
                AiCallOutboundAttemptModel.target_id == claim.target_id,
                AiCallOutboundAttemptModel.id == claim.attempt_id,
                AiCallOutboundAttemptModel.call_id == claim.call_id,
            )
            .with_for_update()
        )
        if attempt is None:
            return None
        now = await self._database_clock(self._session)
        if not _claim_is_current(attempt, claim, now):
            return None

        record = await self._session.scalar(
            select(AiCallRecordModel)
            .where(
                AiCallRecordModel.tenant_id == claim.tenant_id,
                AiCallRecordModel.call_id == claim.call_id,
            )
            .with_for_update()
        )
        command = await self._session.scalar(
            select(AiCallRuntimeCommandModel)
            .where(
                AiCallRuntimeCommandModel.tenant_id == claim.tenant_id,
                AiCallRuntimeCommandModel.call_id == claim.call_id,
                AiCallRuntimeCommandModel.command_type == "START_CALL",
            )
            .with_for_update()
        )
        reservation = await self._session.scalar(
            select(AiCallSipLineReservationModel)
            .where(
                AiCallSipLineReservationModel.tenant_id == claim.tenant_id,
                AiCallSipLineReservationModel.call_id == claim.call_id,
            )
            .with_for_update()
        )
        effect = None
        if command is not None:
            effect = await self._session.scalar(
                select(AiCallRuntimeEffectModel)
                .where(
                    AiCallRuntimeEffectModel.tenant_id == claim.tenant_id,
                    AiCallRuntimeEffectModel.call_id == claim.call_id,
                    AiCallRuntimeEffectModel.command_id == command.id,
                    AiCallRuntimeEffectModel.effect_type
                    == "CREATE_SIP_PARTICIPANT",
                )
                .with_for_update()
            )

        previous_status = attempt.status
        graph_valid = _projection_graph_matches(task, target, attempt)
        terminal_projected = False
        if graph_valid and _terminal_facts_complete(record=record, command=command):
            media_connected = bool(
                record is not None
                and record.answered_at is not None
                and await has_persisted_media_evidence(self._session, claim.call_id)
            )
            decision = terminal_attempt_decision(
                record,
                media_connected=media_connected,
            )
            if decision is not None:
                apply_terminal_projection(
                    task=task,
                    target=target,
                    attempt=attempt,
                    record=record,
                    decision=decision,
                    now=now,
                )
                await self._session.flush()
                await refresh_task_counters(self._session, task, now)
                terminal_projected = True
        if graph_valid and not terminal_projected:
            if _dialing_facts_complete(
                attempt=attempt,
                record=record,
                command=command,
                reservation=reservation,
                effect=effect,
            ):
                attempt.status = "DIALING"
            elif attempt.status == "QUEUED" and _starting_facts_complete(
                attempt=attempt,
                record=record,
                reservation=reservation,
                now=now,
            ):
                attempt.status = "STARTING"

        attempt.reconcile_owner_id = None
        attempt.reconcile_token = None
        attempt.reconcile_expires_at = None
        attempt.reconcile_after = (
            None
            if terminal_projected
            else now + self._retry_after
        )
        attempt.updated_at = now
        await self._session.flush()
        return AttemptProjectionResult(
            attempt_id=attempt.id,
            previous_status=previous_status,
            status=attempt.status,
        )


class OutboundAttemptReconcileWorker:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        worker_id: str,
        batch_size: int = 20,
        poll_interval_seconds: float = 1.0,
    ) -> None:
        self._session_factory = session_factory
        self._worker_id = worker_id
        self._batch_size = max(1, batch_size)
        self._poll_interval_seconds = max(0.05, poll_interval_seconds)
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stopping.set()
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def run_once(self) -> int:
        processed = 0
        for _ in range(self._batch_size):
            async with self._session_factory.begin() as session:
                claim = await OutboundAttemptReconciler(
                    session,
                    worker_id=self._worker_id,
                ).claim_next()
            if claim is None:
                break
            async with self._session_factory.begin() as session:
                result = await OutboundAttemptReconciler(
                    session,
                    worker_id=self._worker_id,
                ).submit(claim)
            if result is None:
                break
            processed += 1
        return processed

    async def _run(self) -> None:
        while not self._stopping.is_set():
            try:
                await self.run_once()
            except Exception:
                log.exception("AI Call 外呼 Attempt 投影轮询失败")
            try:
                await asyncio.wait_for(
                    self._stopping.wait(),
                    timeout=self._poll_interval_seconds,
                )
            except TimeoutError:
                continue


def _claim_is_current(
    attempt: AiCallOutboundAttemptModel,
    claim: AttemptReconcileClaim,
    now: datetime,
) -> bool:
    return (
        attempt.reconcile_owner_id == claim.reconcile_owner_id
        and attempt.reconcile_token == claim.reconcile_token
        and attempt.reconcile_expires_at == claim.reconcile_expires_at
        and attempt.reconcile_expires_at is not None
        and attempt.reconcile_expires_at > now
        and attempt.status in {"QUEUED", "STARTING", "DIALING", "IN_CALL"}
    )


def _terminal_facts_complete(
    *,
    record: AiCallRecordModel | None,
    command: AiCallRuntimeCommandModel | None,
) -> bool:
    return (
        record is not None
        and record.status in {"completed", "failed"}
        and record.ended_at is not None
        and command is not None
        and command.status
        in {
            CommandStatus.SUCCEEDED,
            CommandStatus.DEAD,
            CommandStatus.SUPERSEDED,
        }
    )


def _projection_graph_matches(
    task: AiCallOutboundTaskModel,
    target: AiCallOutboundTargetModel,
    attempt: AiCallOutboundAttemptModel,
) -> bool:
    return (
        target.tenant_id == task.tenant_id == attempt.tenant_id
        and target.task_id == task.id == attempt.task_id
        and attempt.target_id == target.id
        and target.status in {"DIALING", "IN_CALL"}
        and attempt.attempt_no == target.attempt_count
        and task.line_id == attempt.line_id
    )


def _starting_facts_complete(
    *,
    attempt: AiCallOutboundAttemptModel,
    record: AiCallRecordModel | None,
    reservation: AiCallSipLineReservationModel | None,
    now: datetime,
) -> bool:
    return (
        record is not None
        and record.entry_type == "outbound"
        and record.business_type == "outbound_attempt"
        and record.business_id == str(attempt.id)
        and record.runtime_owner_id is not None
        and record.runtime_fencing_token > 0
        and record.runtime_lease_expires_at is not None
        and record.runtime_lease_expires_at > now
        and record.runtime_capacity_class == "active"
        and reservation is not None
        and reservation.attempt_id == attempt.id
        and reservation.line_id == attempt.line_id
        and reservation.status == "RESERVED"
        and reservation.fencing_token == record.runtime_fencing_token
    )


def _dialing_facts_complete(
    *,
    attempt: AiCallOutboundAttemptModel,
    record: AiCallRecordModel | None,
    command: AiCallRuntimeCommandModel | None,
    reservation: AiCallSipLineReservationModel | None,
    effect: AiCallRuntimeEffectModel | None,
) -> bool:
    return (
        record is not None
        and record.entry_type == "outbound"
        and record.business_type == "outbound_attempt"
        and record.business_id == str(attempt.id)
        and record.status == "ready"
        and command is not None
        and command.status == CommandStatus.SUCCEEDED
        and reservation is not None
        and reservation.attempt_id == attempt.id
        and reservation.line_id == attempt.line_id
        and reservation.status == "ACTIVE"
        and reservation.fencing_token == record.runtime_fencing_token
        and effect is not None
        and effect.status == EffectStatus.APPLIED
        and effect.fencing_token == reservation.fencing_token
        and effect.resource_generation == reservation.fencing_token
        and command.expected_fencing_token == reservation.fencing_token
        and bool(effect.provider_reference)
    )
