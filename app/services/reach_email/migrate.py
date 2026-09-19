"""创建邮件表并补齐新增字段：ENVIRONMENT=dev python -m app.services.reach_email.migrate --apply。

默认只输出表名，不打印连接字符串或执行数据库变更。
"""

import argparse
import asyncio
from sqlalchemy import inspect, text

from app.services.reach_email.models import TABLES


def migrate_tables(conn):
    for table in TABLES:
        table.create(conn, checkfirst=True)
    columns = {column["name"] for column in inspect(conn).get_columns("reach_email_lead")}
    if "last_read_reply_at" not in columns:
        conn.execute(text("ALTER TABLE reach_email_lead ADD COLUMN last_read_reply_at TIMESTAMP"))
    columns = {column["name"] for column in inspect(conn).get_columns("reach_email_message")}
    if "delivery_report" not in columns:
        conn.execute(text("ALTER TABLE reach_email_message ADD COLUMN delivery_report TEXT"))


async def apply():
    from app.core.database import async_engine

    async with async_engine.begin() as conn:
        await conn.run_sync(migrate_tables)
    await async_engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    print("\n".join(table.name for table in TABLES))
    if args.apply:
        asyncio.run(apply())
