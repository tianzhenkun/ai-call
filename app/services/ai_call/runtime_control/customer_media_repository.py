from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import AiCallRecordModel
from app.services.ai_call.runtime_control.postgres_wakeup import (
    publish_control_wakeup,
)
from app.services.ai_call.runtime_control.timing import read_database_time


class OwnerCustomerMediaRepository:
    """Persist Owner-mode customer media readiness without provider I/O."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def mark_browser_ready(self, *, tenant_id: str, call_id: str) -> bool:
        record = await self._session.scalar(
            select(AiCallRecordModel)
            .where(
                AiCallRecordModel.tenant_id == tenant_id,
                AiCallRecordModel.call_id == call_id,
            )
            .with_for_update()
        )
        if (
            record is None
            or record.runtime_control_mode != "owner_command_v1"
            or record.terminal_requested_at is not None
            or str(record.status).lower() in {"ending", "completed", "failed"}
        ):
            return False

        bind = self._session.get_bind()
        now = (
            await read_database_time(self._session)
            if bind.dialect.name == "postgresql"
            else datetime.now(timezone.utc)
        )
        record.answered_at = record.answered_at or now
        record.status = "connected"
        await self._session.flush()
        await publish_control_wakeup(self._session)
        return True
