"""Optional verification against the local PostgreSQL, always rolled back."""

import asyncio
import os
from io import BytesIO

import pytest
from cryptography.fernet import Fernet
from openpyxl import Workbook
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app.services.reach_email.schema import ContentInput, TaskInput
from app.services.reach_email.service import EmailService


@pytest.mark.skipif(
    not os.getenv("REACH_EMAIL_TEST_DATABASE_URL"), reason="local PostgreSQL target not supplied"
)
def test_postgres_task_roundtrip_rollback(valid_email_dns, task_settings):
    async def run():
        engine = create_async_engine(os.environ["REACH_EMAIL_TEST_DATABASE_URL"], echo=False)
        async with engine.connect() as conn:
            transaction = await conn.begin()
            try:
                async with AsyncSession(bind=conn, expire_on_commit=False) as db:
                    service = EmailService(
                        db,
                        "email-verification",
                        "email-verification",
                        Fernet.generate_key().decode(),
                    )
                    book = Workbook()
                    book.active.append(["客户姓名", "邮箱", "需求描述"])
                    book.active.append(["", "controlled@example.com", "测试"])
                    output = BytesIO()
                    book.save(output)
                    imported = await service.import_file("名单.xlsx", output.getvalue())
                    task = await service.create_task(
                        TaskInput(name="验证事务回滚", importId=imported["id"], settings=task_settings)
                    )
                    task = await service.save_content(
                        task["id"],
                        ContentInput(version=1, subject="hello", html="{{客户姓名}} {{需求描述}}"),
                    )
                    preview = await service.preflight(task["id"])
                    assert preview["warnings"] == [
                        {"email": "controlled@example.com", "variables": ["客户姓名"]}
                    ]
                    assert (await service.task(task["id"])).version == 2
                    await db.flush()
            finally:
                if transaction.is_active:
                    await transaction.rollback()
        await engine.dispose()

    asyncio.run(run())


@pytest.mark.skipif(
    not os.getenv("REACH_EMAIL_TEST_DATABASE_URL"), reason="local PostgreSQL target not supplied"
)
def test_postgres_stop_and_claim_do_not_deadlock():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from sqlalchemy import delete
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from app.services.reach_email.models import Attempt, Lead, Message, Task
    from app.services.reach_email.worker import EmailWorker

    async def run():
        engine = create_async_engine(os.environ["REACH_EMAIL_TEST_DATABASE_URL"], echo=False)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        identity = uuid4().hex
        worker = EmailWorker(sessions, SimpleNamespace(), AsyncMock(), AsyncMock())
        # Isolate task/message lock testing from the actual global worker lease.
        worker.require_lease = AsyncMock()
        pending = None
        try:
            async with sessions() as db, db.begin():
                db.add(
                    Task(
                        id=identity,
                        tenant_id=identity,
                        owner_id=identity,
                        name="lock-test",
                        import_id=identity,
                        settings="{}",
                        status="running",
                    )
                )
                db.add(
                    Lead(
                        id=identity,
                        tenant_id=identity,
                        owner_id=identity,
                        task_id=identity,
                        email="controlled@example.com",
                        values="{}",
                    )
                )
                db.add(
                    Message(
                        id=identity,
                        tenant_id=identity,
                        owner_id=identity,
                        task_id=identity,
                        lead_id=identity,
                        dedup_key=identity,
                        message_id="<test@example.com>",
                        to_email="controlled@example.com",
                        subject="test",
                        html="test",
                    )
                )
            async with sessions() as stopper:
                service = EmailService(stopper, identity, identity, "")
                task = await service.task(identity, lock=True)
                pending = asyncio.create_task(worker.claim(identity))
                await asyncio.sleep(0.15)
                await asyncio.wait_for(
                    service.transition(identity, task.version, "stop"), timeout=2
                )
                await stopper.commit()
                assert await asyncio.wait_for(pending, timeout=2) is None
            async with sessions() as db:
                assert (await db.get(Task, identity)).ended_reason == "stopped"
                assert (await db.get(Message, identity)).status == "cancelled"
        finally:
            if pending and not pending.done():
                pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)
            async with sessions() as db, db.begin():
                for model in (Attempt, Message, Lead, Task):
                    await db.execute(delete(model).where(model.tenant_id == identity))
            await engine.dispose()

    asyncio.run(run())
