from __future__ import annotations

import asyncio

from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.model import AiCallRecordModel
from app.core.logger import log
from app.services.ai_call.runtime_control.models import AiCallRuntimeCommandModel
from app.services.ai_call.runtime_control.owner_repository import (
    DispatcherOwnerRepository,
)
from app.services.ai_call.runtime_control.types import CommandStatus


class DispatcherControlService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        batch_size: int = 32,
        scan_interval_seconds: float = 1.0,
    ) -> None:
        self._session_factory = session_factory
        self._batch_size = batch_size
        self._scan_interval_seconds = scan_interval_seconds
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def run_once(self) -> int:
        async with self._session_factory() as session:
            call_ids = list(
                (
                    await session.scalars(
                        select(AiCallRecordModel.call_id)
                        .where(
                            AiCallRecordModel.runtime_control_mode
                            == "owner_command_v1",
                            AiCallRecordModel.runtime_owner_id.is_(None),
                            AiCallRecordModel.runtime_fencing_token == 0,
                            AiCallRecordModel.runtime_capacity_class == "none",
                            AiCallRecordModel.terminal_requested_at.is_(None),
                            exists().where(
                                AiCallRuntimeCommandModel.tenant_id
                                == AiCallRecordModel.tenant_id,
                                AiCallRuntimeCommandModel.call_id
                                == AiCallRecordModel.call_id,
                                AiCallRuntimeCommandModel.command_type == "START_CALL",
                                AiCallRuntimeCommandModel.status
                                == CommandStatus.PENDING,
                            ),
                        )
                        .order_by(AiCallRecordModel.started_at, AiCallRecordModel.id)
                        .limit(self._batch_size)
                    )
                ).all()
            )
        assigned = 0
        for call_id in call_ids:
            async with self._session_factory.begin() as session:
                record = await session.scalar(
                    select(AiCallRecordModel.tenant_id).where(
                        AiCallRecordModel.call_id == call_id
                    )
                )
                if record is None:
                    continue
                lease = await DispatcherOwnerRepository(
                    session
                ).assign_initial_owner(str(record), call_id)
                if lease is not None:
                    assigned += 1
        return assigned

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(
            self._run_loop(), name="ai-call-runtime-dispatcher"
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
                log.error(f"AI Call Dispatcher DB-only 扫描失败: {exc!s}")
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._scan_interval_seconds,
                )
            except TimeoutError:
                continue
