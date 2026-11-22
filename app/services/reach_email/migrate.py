"""创建邮件表并补齐新增字段：ENVIRONMENT=dev python -m app.services.reach_email.migrate --apply。

默认只输出表名，不打印连接字符串或执行数据库变更。
"""

import argparse
import asyncio
import json

from sqlalchemy import Index, MetaData, Table, UniqueConstraint, inspect, select, text, update

from app.services.reach_email.models import TABLES
from app.services.reach_email.providers import mailbox_service


def migrate_account_services(conn):
    """先回填服务身份，再替换旧地址唯一约束；保留原记录 ID 和全部授权字段。"""
    columns = {column['name'] for column in inspect(conn).get_columns('reach_email_account')}
    if 'service_key' not in columns:
        conn.execute(text('ALTER TABLE reach_email_account ADD COLUMN service_key VARCHAR(253)'))
    account = Table('reach_email_account', MetaData(), autoload_with=conn)
    for row in conn.execute(select(account.c.id, account.c.config).where(account.c.service_key.is_(None))).mappings():
        conn.execute(update(account).where(account.c.id == row['id']).values(
            service_key=mailbox_service(json.loads(row['config']))))
    old = next((constraint for constraint in account.constraints
        if isinstance(constraint, UniqueConstraint)
        and list(constraint.columns.keys()) == ['tenant_id', 'owner_id', 'email']), None)
    if conn.dialect.name == 'sqlite' and old:
        # SQLite 无法直接删除表内唯一约束，事务内复制表并还原原有索引。
        replacement = account.to_metadata(MetaData(), name='reach_email_account_service_migration')
        replacement.constraints = {c for c in replacement.constraints
            if not isinstance(c, UniqueConstraint) or list(c.columns.keys()) != ['tenant_id', 'owner_id', 'email']}
        replacement.append_constraint(UniqueConstraint('tenant_id', 'owner_id', 'email', 'service_key',
            name='uq_reach_email_account_service'))
        replacement.c.service_key.nullable = False
        replacement.indexes.clear()
        replacement.create(conn)
        conn.execute(replacement.insert().from_select(list(account.c.keys()), select(account)))
        account.drop(conn)
        conn.execute(text('ALTER TABLE reach_email_account_service_migration RENAME TO reach_email_account'))
        for index in account.indexes:
            index.create(conn)
        return
    if conn.dialect.name == 'postgresql':
        conn.execute(text('ALTER TABLE reach_email_account ALTER COLUMN service_key SET NOT NULL'))
    elif conn.dialect.name == 'mysql':
        conn.execute(text('ALTER TABLE reach_email_account MODIFY service_key VARCHAR(253) NOT NULL'))
    if not any(isinstance(c, UniqueConstraint) and c.name == 'uq_reach_email_account_service'
               for c in account.constraints):
        Index('uq_reach_email_account_service', account.c.tenant_id, account.c.owner_id, account.c.email,
              account.c.service_key, unique=True).create(conn, checkfirst=True)
    if old:
        from sqlalchemy.schema import DropConstraint

        conn.execute(DropConstraint(old))


def migrate_tables(conn):
    # sqlite3 的默认事务模式不会为 DDL 开启事务，显式 BEGIN 保证中断后可完整回滚。
    if conn.dialect.name == 'sqlite' and not conn.connection.driver_connection.in_transaction:
        conn.exec_driver_sql('BEGIN')
    for table in TABLES:
        table.create(conn, checkfirst=True)
    columns = {column["name"] for column in inspect(conn).get_columns("reach_email_account")}
    for column in ("smtp_test_result", "imap_test_result"):
        if column not in columns:
            conn.execute(text(f"ALTER TABLE reach_email_account ADD COLUMN {column} TEXT"))
    if "oauth_status" not in columns:
        conn.execute(text("ALTER TABLE reach_email_account ADD COLUMN oauth_status VARCHAR(20)"))
    migrate_account_services(conn)
    columns = {column["name"] for column in inspect(conn).get_columns("reach_email_task")}
    if "sender_allocation_state" not in columns:
        conn.execute(text("ALTER TABLE reach_email_task ADD COLUMN sender_allocation_state TEXT"))
    columns = {column["name"] for column in inspect(conn).get_columns("reach_email_lead")}
    if "last_read_reply_at" not in columns:
        conn.execute(text("ALTER TABLE reach_email_lead ADD COLUMN last_read_reply_at TIMESTAMP"))
    columns = {column["name"] for column in inspect(conn).get_columns("reach_email_message")}
    if "delivery_report" not in columns:
        conn.execute(text("ALTER TABLE reach_email_message ADD COLUMN delivery_report TEXT"))
    if "send_diagnostic" not in columns:
        conn.execute(text("ALTER TABLE reach_email_message ADD COLUMN send_diagnostic TEXT"))
    if "provider_message_id" not in columns:
        conn.execute(text("ALTER TABLE reach_email_message ADD COLUMN provider_message_id VARCHAR(512)"))
    columns = {column["name"] for column in inspect(conn).get_columns("reach_email_ai_job")}
    if "attempts" not in columns:
        conn.execute(text("ALTER TABLE reach_email_ai_job ADD COLUMN attempts TEXT"))


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
