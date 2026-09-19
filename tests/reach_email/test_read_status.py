import asyncio
from datetime import timedelta, timezone

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.reach_email.controller import mark_conversation_read
from app.services.reach_email.migrate import migrate_tables
from app.services.reach_email.models import Lead, Task, now
from app.services.reach_email.schema import ReadInput
from app.services.reach_email.service import EmailService


def test_reply_read_state_preserves_new_replies_and_owner_scope():
    async def run():
        engine = create_async_engine("sqlite+aiosqlite://")
        async with engine.begin() as conn:
            await conn.run_sync(migrate_tables)
        async with async_sessionmaker(engine, expire_on_commit=False)() as db:
            service = EmailService(db, "tenant", "owner", "")
            task = service.add(Task, name="任务", import_id="import", settings="{}", content="{}")
            lead = service.add(Lead, task_id=task.id, email="test@example.com", values="{}")
            await db.flush()
            assert (await service.lead_json(lead))["replyStatus"] == "none"
            first = now()
            lead.last_reply_at = first
            await db.flush()
            assert (await service.lead_json(lead))["replyStatus"] == "unread"
            observed = ReadInput(lastReplyAt=first.replace(tzinfo=timezone.utc))
            with pytest.raises(ValueError, match="不存在"):
                await mark_conversation_read(lead.id, observed, EmailService(db, "tenant", "other", ""))
            await mark_conversation_read(lead.id, observed, service)
            await db.refresh(lead)
            assert (await service.lead_json(lead))["replyStatus"] == "read"
            lead.last_reply_at = first + timedelta(seconds=1)
            await db.flush()
            await mark_conversation_read(lead.id, observed, service)
            await db.refresh(lead)
            assert (await service.lead_json(lead))["replyStatus"] == "unread"
            assert lead.last_read_reply_at == first
            await mark_conversation_read(lead.id, ReadInput(lastReplyAt=lead.last_reply_at), service)
            await db.refresh(lead)
            assert (await service.lead_json(lead))["replyStatus"] == "read"
        await engine.dispose()
    asyncio.run(run())


def test_read_column_migration_is_additive_and_repeatable():
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE reach_email_lead (id TEXT PRIMARY KEY)"))
        conn.execute(text("INSERT INTO reach_email_lead (id) VALUES ('existing')"))
        migrate_tables(conn)
        migrate_tables(conn)
        assert "last_read_reply_at" in {column["name"] for column in inspect(conn).get_columns("reach_email_lead")}
        assert conn.scalar(text("SELECT id FROM reach_email_lead")) == "existing"
    engine.dispose()
