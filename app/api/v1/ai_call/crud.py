from __future__ import annotations

from datetime import datetime

from sqlalchemy import Select, asc, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import AiCallEventModel, AiCallRecordModel
from app.utils.id_util import generate_snowflake_id


class AiCallRecordRepository:
    """AI Call B1 专用持久化仓储。"""

    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def create_record(
        self,
        *,
        call_id: str,
        business_type: str | None,
        business_id: str | None,
        entry_type: str,
        room_name: str,
        participant_identity: str,
        status: str,
        started_at: datetime,
    ) -> AiCallRecordModel:
        record = AiCallRecordModel(
            id=generate_snowflake_id(),
            call_id=call_id,
            business_type=business_type,
            business_id=business_id,
            entry_type=entry_type,
            room_name=room_name,
            participant_identity=participant_identity,
            status=status,
            started_at=started_at,
        )
        self.db.add(record)
        await self.db.flush()
        await self.db.refresh(record)
        return record

    async def get_record(self, call_id: str) -> AiCallRecordModel | None:
        result = await self.db.execute(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id == call_id)
        )
        return result.scalar_one_or_none()

    async def update_record(self, call_id: str, **values) -> AiCallRecordModel | None:
        record = await self.get_record(call_id)
        if record is None:
            return None
        for key, value in values.items():
            if hasattr(record, key):
                setattr(record, key, value)
        await self.db.flush()
        await self.db.refresh(record)
        return record

    async def list_records(
        self,
        *,
        call_id: str | None = None,
        business_type: str | None = None,
        business_id: str | None = None,
        status: str | None = None,
        entry_type: str | None = None,
        started_at_begin: datetime | None = None,
        started_at_end: datetime | None = None,
        page_num: int = 1,
        page_size: int = 10,
    ) -> tuple[list[AiCallRecordModel], int]:
        stmt = self._record_filters(
            select(AiCallRecordModel),
            call_id=call_id,
            business_type=business_type,
            business_id=business_id,
            status=status,
            entry_type=entry_type,
            started_at_begin=started_at_begin,
            started_at_end=started_at_end,
        )
        count_stmt = self._record_filters(
            select(func.count()).select_from(AiCallRecordModel),
            call_id=call_id,
            business_type=business_type,
            business_id=business_id,
            status=status,
            entry_type=entry_type,
            started_at_begin=started_at_begin,
            started_at_end=started_at_end,
        )
        total = int((await self.db.execute(count_stmt)).scalar_one())
        safe_page_num = max(1, page_num)
        safe_page_size = max(1, min(page_size, 1000))
        stmt = (
            stmt.order_by(desc(AiCallRecordModel.started_at), desc(AiCallRecordModel.id))
            .offset((safe_page_num - 1) * safe_page_size)
            .limit(safe_page_size)
        )
        rows = (await self.db.execute(stmt)).scalars().all()
        return list(rows), total

    async def append_event(
        self,
        *,
        event_id: str,
        call_id: str,
        event_type: str,
        source: str,
        event_time: datetime,
        payload_json: str | None,
    ) -> AiCallEventModel:
        existing = await self.get_event_by_event_id(event_id)
        if existing is not None:
            return existing
        event = AiCallEventModel(
            id=generate_snowflake_id(),
            event_id=event_id,
            call_id=call_id,
            event_type=event_type,
            source=source,
            event_time=event_time,
            payload_json=payload_json,
        )
        self.db.add(event)
        await self.db.flush()
        await self.db.refresh(event)
        return event

    async def get_event_by_event_id(self, event_id: str) -> AiCallEventModel | None:
        result = await self.db.execute(
            select(AiCallEventModel).where(AiCallEventModel.event_id == event_id)
        )
        return result.scalar_one_or_none()

    async def list_events(
        self,
        *,
        call_id: str,
        limit: int = 200,
        after_event_id: str | None = None,
        event_type: str | None = None,
        source: str | None = None,
    ) -> list[AiCallEventModel]:
        safe_limit = max(1, min(limit, 1000))
        stmt = select(AiCallEventModel).where(AiCallEventModel.call_id == call_id)
        if after_event_id:
            after_event = await self.get_event_by_event_id(after_event_id)
            if after_event is None or after_event.call_id != call_id:
                return []
            stmt = stmt.where(AiCallEventModel.id > after_event.id)
        if event_type:
            stmt = stmt.where(AiCallEventModel.event_type == event_type)
        if source:
            stmt = stmt.where(AiCallEventModel.source == source)
        result = await self.db.execute(
            stmt.order_by(asc(AiCallEventModel.id)).limit(safe_limit)
        )
        return list(result.scalars().all())

    async def get_last_event(self, call_id: str) -> AiCallEventModel | None:
        result = await self.db.execute(
            select(AiCallEventModel)
            .where(AiCallEventModel.call_id == call_id)
            .order_by(desc(AiCallEventModel.id))
            .limit(1)
        )
        return result.scalar_one_or_none()

    @staticmethod
    def _record_filters(stmt: Select, **filters) -> Select:
        if filters.get("call_id"):
            stmt = stmt.where(AiCallRecordModel.call_id == filters["call_id"])
        if filters.get("business_type"):
            stmt = stmt.where(AiCallRecordModel.business_type == filters["business_type"])
        if filters.get("business_id"):
            stmt = stmt.where(AiCallRecordModel.business_id == filters["business_id"])
        if filters.get("status"):
            stmt = stmt.where(AiCallRecordModel.status == filters["status"])
        if filters.get("entry_type"):
            stmt = stmt.where(AiCallRecordModel.entry_type == filters["entry_type"])
        if filters.get("started_at_begin"):
            stmt = stmt.where(AiCallRecordModel.started_at >= filters["started_at_begin"])
        if filters.get("started_at_end"):
            stmt = stmt.where(AiCallRecordModel.started_at <= filters["started_at_end"])
        return stmt
