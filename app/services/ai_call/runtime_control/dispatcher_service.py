from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime

from sqlalchemy import String, cast, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.model import AiCallRecordModel
from app.api.v1.ai_call.outbound.rule_task_model import AiCallOutboundAttemptModel
from app.core.logger import log
from app.services.ai_call.runtime_control.command_repository import (
    RuntimeCommandRepository,
)
from app.services.ai_call.runtime_control.models import AiCallRuntimeCommandModel
from app.services.ai_call.runtime_control.owner_repository import (
    DispatcherOwnerRepository,
)
from app.services.ai_call.runtime_control.postgres_wakeup import WakeupListener
from app.services.ai_call.runtime_control.timing import read_database_time
from app.services.ai_call.runtime_control.types import CommandStatus


class _AllocationDeadlineElapsed(RuntimeError):
    pass


class DispatcherControlService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        batch_size: int = 32,
        scan_interval_seconds: float = 1.0,
        wakeup_listener: WakeupListener | None = None,
        database_clock: Callable[[AsyncSession], Awaitable[datetime]] = read_database_time,
    ) -> None:
        self._session_factory = session_factory
        self._batch_size = batch_size
        self._scan_interval_seconds = scan_interval_seconds
        self._wakeup_listener = wakeup_listener
        self._database_clock = database_clock
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def run_once(self) -> int:
        async with self._session_factory() as session:
            lane_key = func.coalesce(
                cast(AiCallOutboundAttemptModel.line_id, String),
                AiCallRecordModel.entry_type,
            )
            lane_rank = func.row_number().over(
                partition_by=(AiCallRecordModel.tenant_id, lane_key),
                order_by=(AiCallRecordModel.started_at, AiCallRecordModel.id),
            )
            ranked_candidates = (
                select(
                    AiCallRecordModel.tenant_id.label("tenant_id"),
                    AiCallRecordModel.call_id.label("call_id"),
                    AiCallRecordModel.started_at.label("started_at"),
                    AiCallRecordModel.id.label("record_id"),
                    lane_rank.label("lane_rank"),
                )
                .select_from(AiCallRecordModel)
                .join(
                    AiCallRuntimeCommandModel,
                    (
                        AiCallRuntimeCommandModel.tenant_id
                        == AiCallRecordModel.tenant_id
                    )
                    & (
                        AiCallRuntimeCommandModel.call_id
                        == AiCallRecordModel.call_id
                    )
                    & (AiCallRuntimeCommandModel.command_type == "START_CALL")
                    & (
                        AiCallRuntimeCommandModel.status.in_(
                            {
                                CommandStatus.PENDING,
                                CommandStatus.RETRY_WAIT,
                            }
                        )
                    ),
                )
                .outerjoin(
                    AiCallOutboundAttemptModel,
                    (
                        AiCallOutboundAttemptModel.tenant_id
                        == AiCallRecordModel.tenant_id
                    )
                    & (
                        AiCallOutboundAttemptModel.call_id
                        == AiCallRecordModel.call_id
                    ),
                )
                .where(
                    AiCallRecordModel.runtime_control_mode == "owner_command_v1",
                    AiCallRecordModel.runtime_owner_id.is_(None),
                    AiCallRecordModel.runtime_fencing_token == 0,
                    AiCallRecordModel.runtime_capacity_class == "none",
                    AiCallRecordModel.terminal_requested_at.is_(None),
                )
                .subquery()
            )
            candidates = list(
                (
                    await session.execute(
                        select(
                            ranked_candidates.c.tenant_id,
                            ranked_candidates.c.call_id,
                        )
                        .order_by(
                            ranked_candidates.c.lane_rank,
                            ranked_candidates.c.started_at,
                            ranked_candidates.c.record_id,
                        )
                        .limit(self._batch_size)
                    )
                ).all()
            )
        assigned = 0
        for tenant_id, call_id in candidates:
            async with self._session_factory.begin() as session:
                command_repository = RuntimeCommandRepository(
                    session,
                    database_clock=self._database_clock,
                )
                if await command_repository.expire_unallocated_start(
                    str(tenant_id), call_id
                ):
                    assigned += 1
                    continue

            try:
                async with self._session_factory.begin() as session:
                    lease = await DispatcherOwnerRepository(
                        session,
                        database_clock=self._database_clock,
                    ).assign_initial_owner(str(tenant_id), call_id)
                    if lease is not None and await self._allocation_deadline_elapsed(
                        session,
                        str(tenant_id),
                        call_id,
                    ):
                        raise _AllocationDeadlineElapsed
                    if lease is not None:
                        assigned += 1
            except _AllocationDeadlineElapsed:
                async with self._session_factory.begin() as session:
                    expired = await RuntimeCommandRepository(
                        session,
                        database_clock=self._database_clock,
                    ).expire_unallocated_start(str(tenant_id), call_id)
                if expired:
                    assigned += 1
        return assigned

    async def _allocation_deadline_elapsed(
        self,
        session: AsyncSession,
        tenant_id: str,
        call_id: str,
    ) -> bool:
        allocation_deadline_at = await session.scalar(
            select(AiCallRuntimeCommandModel.allocation_deadline_at).where(
                AiCallRuntimeCommandModel.tenant_id == tenant_id,
                AiCallRuntimeCommandModel.call_id == call_id,
                AiCallRuntimeCommandModel.command_type == "START_CALL",
            )
        )
        if allocation_deadline_at is None:
            return False
        return allocation_deadline_at <= await self._database_clock(session)

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stop_event.clear()
        if self._wakeup_listener is not None:
            try:
                await self._wakeup_listener.start()
            except Exception as exc:
                log.error(f"AI Call Dispatcher PostgreSQL 唤醒监听启动失败: {exc!s}")
        self._task = asyncio.create_task(
            self._run_loop(), name="ai-call-runtime-dispatcher"
        )

    async def stop(self) -> None:
        self._stop_event.set()
        task = self._task
        self._task = None
        if task is not None:
            await task
        if self._wakeup_listener is not None:
            try:
                await self._wakeup_listener.stop()
            except Exception as exc:
                log.error(f"AI Call Dispatcher PostgreSQL 唤醒监听关闭失败: {exc!s}")

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error(f"AI Call Dispatcher DB-only 扫描失败: {exc!s}")
            await self._wait_for_next_scan()

    async def _wait_for_next_scan(self) -> None:
        if self._wakeup_listener is not None:
            try:
                await self._wakeup_listener.wait(
                    timeout_seconds=self._scan_interval_seconds,
                    stop_event=self._stop_event,
                )
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error(f"AI Call Dispatcher PostgreSQL 唤醒等待失败: {exc!s}")
        try:
            await asyncio.wait_for(
                self._stop_event.wait(),
                timeout=self._scan_interval_seconds,
            )
        except TimeoutError:
            return
