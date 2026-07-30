from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

_OWNED_TRANSACTION_KEY = "ai_call.sqlite_immediate_transaction"


async def begin_sqlite_immediate_write(db: AsyncSession) -> bool:
    """在 SQLite 会话当前逻辑事务的首条 SQL 上抢占写事务。"""

    get_bind = getattr(db, "get_bind", None)
    if get_bind is None or get_bind().dialect.name != "sqlite":
        return False

    sync_session = db.sync_session
    transaction = sync_session.get_transaction()
    if (
        transaction is not None
        and transaction.is_active
        and sync_session.info.get(_OWNED_TRANSACTION_KEY) is transaction
    ):
        return True
    if transaction is not None and getattr(transaction, "_connections", None):
        raise RuntimeError("SQLite 写流程必须在当前逻辑事务执行首条 SQL 前进入")

    try:
        await db.execute(text("BEGIN IMMEDIATE"))
    except OperationalError as exc:
        if "cannot start a transaction within a transaction" in str(exc.orig).lower():
            raise RuntimeError("SQLite 写流程必须在当前逻辑事务执行首条 SQL 前进入") from None
        raise

    transaction = sync_session.get_transaction()
    if transaction is None:
        raise RuntimeError("SQLite BEGIN IMMEDIATE 未建立活动事务")
    sync_session.info[_OWNED_TRANSACTION_KEY] = transaction
    return True
