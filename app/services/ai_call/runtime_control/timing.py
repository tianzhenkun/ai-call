from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession


async def read_database_time(session: AsyncSession) -> datetime:
    """Return one authoritative PostgreSQL wall-clock value for the transaction step."""
    value = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(value, datetime):
        raise RuntimeError("PostgreSQL clock_timestamp() did not return a datetime")
    return value
