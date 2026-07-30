from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def begin_sqlite_immediate_write(db: AsyncSession) -> bool:
    """在新鲜 SQLite 会话上抢占写事务，其他数据库保持原事务协议。"""

    get_bind = getattr(db, "get_bind", None)
    if get_bind is None or get_bind().dialect.name != "sqlite":
        return False
    if db.in_transaction():
        raise RuntimeError("SQLite 写流程必须使用尚未开启事务的新鲜 AsyncSession")
    await db.execute(text("BEGIN IMMEDIATE"))
    return True
