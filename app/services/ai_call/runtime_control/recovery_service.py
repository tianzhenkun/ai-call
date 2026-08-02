from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.model import AiCallRecordModel
from app.core.logger import log
from app.services.ai_call.runtime_control.dialogue_repository import (
    OwnerDialogueFence,
    OwnerDialogueRepository,
)
from app.services.ai_call.runtime_control.models import AiCallRuntimeCommandModel
from app.services.ai_call.runtime_control.owner_repository import (
    RecoveryOwnerRepository,
)
from app.services.ai_call.runtime_control.startup_recovery import (
    StartupReconcileService,
)
from app.services.ai_call.runtime_control.timing import read_database_time
from app.services.ai_call.runtime_control.types import CommandStatus


class RecoveryControlService:
    """DB-only recovery scanner for expired and parked cleanup owners."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        batch_size: int = 32,
        scan_interval_seconds: float = 0.5,
        database_clock: Callable[[AsyncSession], Awaitable[datetime]] = read_database_time,
        dialogue_repository_factory: Callable[
            [AsyncSession], OwnerDialogueRepository
        ]
        | None = None,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if scan_interval_seconds <= 0:
            raise ValueError("scan_interval_seconds must be positive")
        self._session_factory = session_factory
        self._batch_size = batch_size
        self._scan_interval_seconds = scan_interval_seconds
        self._database_clock = database_clock
        self._dialogue_repository_factory = dialogue_repository_factory or (
            lambda session: OwnerDialogueRepository(
                session,
                database_clock=database_clock,
            )
        )
        self._startup_reconcile = StartupReconcileService(
            session_factory,
            batch_size=batch_size,
            database_clock=database_clock,
        )
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def run_once(self) -> int:
        async with self._session_factory() as session:
            now = await self._database_clock(session)
            candidates = (
                await session.execute(
                    select(
                        AiCallRecordModel.tenant_id,
                        AiCallRecordModel.call_id,
                    )
                    .where(
                        AiCallRecordModel.runtime_control_mode == "owner_command_v1",
                        AiCallRecordModel.terminal_requested_at.is_not(None),
                        or_(
                            and_(
                                AiCallRecordModel.runtime_owner_id.is_not(None),
                                AiCallRecordModel.runtime_lease_expires_at <= now,
                                AiCallRecordModel.runtime_capacity_class.in_(
                                    {"active", "cleanup"}
                                ),
                            ),
                            and_(
                                AiCallRecordModel.runtime_capacity_class == "attention",
                                AiCallRecordModel.resource_cleanup_status
                                == "attention_required",
                                AiCallRecordModel.resource_cleanup_next_retry_at
                                <= now,
                            ),
                        ),
                    )
                    .order_by(AiCallRecordModel.started_at, AiCallRecordModel.id)
                    .limit(self._batch_size)
                )
            ).all()
            start_candidates = (
                await session.execute(
                    select(
                        AiCallRecordModel.tenant_id,
                        AiCallRecordModel.call_id,
                    )
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
                        & (
                            AiCallRuntimeCommandModel.command_type
                            == "START_CALL"
                        ),
                    )
                    .where(
                        AiCallRecordModel.runtime_control_mode
                        == "owner_command_v1",
                        AiCallRecordModel.status == "preparing",
                        AiCallRecordModel.terminal_requested_at.is_(None),
                        AiCallRecordModel.runtime_owner_id.is_not(None),
                        AiCallRecordModel.runtime_lease_expires_at <= now,
                        AiCallRecordModel.runtime_capacity_class == "active",
                        or_(
                            AiCallRuntimeCommandModel.status
                            == CommandStatus.PENDING,
                            and_(
                                AiCallRuntimeCommandModel.status
                                == CommandStatus.RETRY_WAIT,
                                AiCallRuntimeCommandModel.next_retry_at.is_not(
                                    None
                                ),
                                AiCallRuntimeCommandModel.next_retry_at <= now,
                            ),
                        ),
                    )
                    .order_by(AiCallRecordModel.started_at, AiCallRecordModel.id)
                    .limit(self._batch_size)
                )
            ).all()

        assigned = 0
        for tenant_id, call_id in start_candidates:
            async with self._session_factory.begin() as session:
                lease = await RecoveryOwnerRepository(session).assign_start_owner(
                    str(tenant_id),
                    call_id,
                )
                if lease is not None:
                    assigned += 1
        for tenant_id, call_id in candidates:
            async with self._session_factory.begin() as session:
                lease = await RecoveryOwnerRepository(session).assign_cleanup_owner(
                    str(tenant_id),
                    call_id,
                )
                if lease is not None:
                    await self._dialogue_repository_factory(session).finalize(
                        OwnerDialogueFence(
                            tenant_id=lease.tenant_id,
                            call_id=lease.call_id,
                            owner_id=lease.owner_id,
                            fencing_token=lease.fencing_token,
                        ),
                        status="uncertain",
                        error="recovery_owner_takeover",
                    )
                    assigned += 1
        return assigned + await self._startup_reconcile.run_once()

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(
            self._run_loop(),
            name="ai-call-runtime-recovery",
        )

    async def stop(self) -> None:
        self._stop_event.set()
        task = self._task
        self._task = None
        if task is not None:
            await task

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error(f"AI Call DB-only Recovery 扫描失败: {exc!s}")
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._scan_interval_seconds,
                )
            except TimeoutError:
                continue
