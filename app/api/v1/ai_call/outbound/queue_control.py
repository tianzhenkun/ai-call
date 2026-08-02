from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import AiCallRecordModel
from app.services.ai_call.runtime_control.timing import read_database_time

from .rule_task_model import AiCallOutboundAttemptModel


@dataclass(frozen=True, slots=True)
class OutboundQueueLimits:
    per_tenant: int = 100
    per_task: int = 20
    per_line: int = 50

    def __post_init__(self) -> None:
        if min(self.per_tenant, self.per_task, self.per_line) <= 0:
            raise ValueError("outbound queue limits must be positive")


DEFAULT_OUTBOUND_QUEUE_LIMITS = OutboundQueueLimits()


@dataclass(frozen=True, slots=True)
class OutboundQueueSnapshot:
    tenant_queued: int
    task_queued: int
    line_queued: int
    oldest_wait_seconds: float
    allocation_timeout_count: int
    limits: OutboundQueueLimits

    @property
    def has_capacity(self) -> bool:
        return (
            self.tenant_queued < self.limits.per_tenant
            and self.task_queued < self.limits.per_task
            and self.line_queued < self.limits.per_line
        )


class OutboundQueueRepository:
    """在创建新 Attempt 的事务中串行化并重算 DB-only 排队容量。"""

    def __init__(
        self,
        session: AsyncSession,
        *,
        limits: OutboundQueueLimits = DEFAULT_OUTBOUND_QUEUE_LIMITS,
        database_clock: Callable[
            [AsyncSession], Awaitable[datetime]
        ]
        | None = None,
    ) -> None:
        self._session = session
        self._limits = limits
        self._database_clock = database_clock

    async def lock_and_snapshot(
        self,
        *,
        tenant_id: str,
        task_id: int,
        line_id: int,
    ) -> OutboundQueueSnapshot:
        await self._lock_queue_scopes(tenant_id=tenant_id, line_id=line_id)
        now = await self._read_database_time()
        queued = (
            AiCallOutboundAttemptModel.dialer_type == "owner_runtime",
            AiCallOutboundAttemptModel.status == "QUEUED",
        )
        tenant_queued = await self._count(*queued, AiCallOutboundAttemptModel.tenant_id == tenant_id)
        task_queued = await self._count(
            *queued,
            AiCallOutboundAttemptModel.tenant_id == tenant_id,
            AiCallOutboundAttemptModel.task_id == task_id,
        )
        line_queued = await self._count(
            *queued,
            AiCallOutboundAttemptModel.tenant_id == tenant_id,
            AiCallOutboundAttemptModel.line_id == line_id,
        )
        oldest_queued_at = await self._session.scalar(
            select(func.min(AiCallOutboundAttemptModel.created_at)).where(
                *queued,
                AiCallOutboundAttemptModel.tenant_id == tenant_id,
            )
        )
        timeout_count = int(
            await self._session.scalar(
                select(func.count(AiCallOutboundAttemptModel.id))
                .select_from(AiCallOutboundAttemptModel)
                .join(
                    AiCallRecordModel,
                    (
                        AiCallRecordModel.tenant_id
                        == AiCallOutboundAttemptModel.tenant_id
                    )
                    & (
                        AiCallRecordModel.call_id
                        == AiCallOutboundAttemptModel.call_id
                    ),
                )
                .where(
                    AiCallOutboundAttemptModel.tenant_id == tenant_id,
                    AiCallRecordModel.failure_stage == "allocation",
                    AiCallRecordModel.failure_message == "ALLOCATION_TIMEOUT",
                )
            )
            or 0
        )
        return OutboundQueueSnapshot(
            tenant_queued=tenant_queued,
            task_queued=task_queued,
            line_queued=line_queued,
            oldest_wait_seconds=_elapsed_seconds(oldest_queued_at, now),
            allocation_timeout_count=timeout_count,
            limits=self._limits,
        )

    async def _lock_queue_scopes(self, *, tenant_id: str, line_id: int) -> None:
        if self._session.get_bind().dialect.name != "postgresql":
            return
        for lock_key in (
            f"ai-call:outbound-queue:tenant:{tenant_id}",
            f"ai-call:outbound-queue:tenant:{tenant_id}:line:{line_id}",
        ):
            await self._session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
                {"lock_key": lock_key},
            )

    async def _read_database_time(self) -> datetime:
        if self._database_clock is not None:
            return await self._database_clock(self._session)
        if self._session.get_bind().dialect.name == "postgresql":
            return await read_database_time(self._session)
        value = await self._session.scalar(select(func.current_timestamp()))
        if not isinstance(value, datetime):
            raise RuntimeError("database did not return a datetime")
        return value

    async def _count(self, *conditions: object) -> int:
        return int(
            await self._session.scalar(
                select(func.count(AiCallOutboundAttemptModel.id)).where(*conditions)
            )
            or 0
        )


def _elapsed_seconds(started_at: datetime | None, now: datetime) -> float:
    if started_at is None:
        return 0.0
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return max(0.0, (now - started_at).total_seconds())
