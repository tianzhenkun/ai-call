import asyncio
from datetime import timedelta

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.services.reach_email.health import worker_health
from app.services.reach_email.models import TABLES, Message, WorkerLease, now
from app.services.reach_email.service import EmailService


def test_health_expiry_backlog_and_owner_scope():
    async def check():
        engine = create_async_engine('sqlite+aiosqlite://')
        async with engine.begin() as conn:
            for table in TABLES:
                await conn.run_sync(table.create)
        async with async_sessionmaker(engine, expire_on_commit=False)() as db:
            s = EmailService(db, 't', 'o', '')
            assert (await worker_health(db, s.query(Message)))['status'] == 'offline'
            lease = WorkerLease(id='email', token='test', expires_at=now() + timedelta(seconds=120))
            db.add(lease)
            s.add(Message, task_id='task', lead_id='lead', dedup_key='late', message_id='<late@test>', subject='test', html='test',
                  to_email='test@example.com', due_at=now() - timedelta(minutes=6))
            other = EmailService(db, 'other', 'other', '')
            other.add(Message, task_id='task', lead_id='lead', dedup_key='other', message_id='<other@test>', subject='test', html='test',
                      to_email='other@example.com', due_at=now() - timedelta(minutes=6))
            await db.flush()
            assert await worker_health(db, s.query(Message)) == {
                'workerOnline': True, 'overdueCount': 1, 'status': 'backlog'}
            row = await db.scalar(s.query(Message))
            row.due_at = now() + timedelta(hours=1)
            await db.flush()
            assert (await worker_health(db, s.query(Message)))['status'] == 'ok'
            lease.expires_at = now() - timedelta(seconds=1)
            await db.flush()
            assert (await worker_health(db, s.query(Message)))['status'] == 'offline'
        await engine.dispose()
    asyncio.run(check())
