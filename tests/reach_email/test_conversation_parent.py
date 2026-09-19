import asyncio
import json

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.reach_email.controller import conversation
from app.services.reach_email.models import TABLES, Lead, Message, Task
from app.services.reach_email.service import EmailService


def test_parent_is_scoped_to_conversation_and_available_across_pages():
    async def run():
        engine = create_async_engine('sqlite+aiosqlite://')
        async with engine.begin() as conn:
            for table in TABLES:
                await conn.run_sync(lambda sync, t=table: t.create(sync))
        async with async_sessionmaker(engine, expire_on_commit=False)() as db:
            service = EmailService(db, 'tenant', 'owner', '')
            task = service.add(Task, name='test', import_id='import', settings='{}', content='{}')
            lead = service.add(Lead, task_id=task.id, email='test@example.com', values='{}')
            parent = service.add(Message, task_id=task.id, lead_id=lead.id, message_id='<original@test>', dedup_key='parent', to_email=lead.email, subject='原邮件', html='正文')
            await db.flush()
            reply = service.add(Message, task_id=task.id, lead_id=lead.id, direction='inbound', message_id='<reply@test>', in_reply_to=parent.message_id, dedup_key='reply', to_email='sender@example.com', subject='回复', html='回复正文')
            await db.flush()
            parent.attempt_count = 1
            parent.from_email = 'sender@example.com'
            reply.from_email = lead.email
            reply.kind = 'reply'
            await db.flush()
            response = await conversation(lead.id, service, 1, 1)
            item = json.loads(response.body)['data']['messages'][0]
            assert item['id'] == reply.id
            assert item['replyTo']['id'] == parent.id
            assert item['replyTo']['deliveryStatus'] == 'delivered'
            parent.lead_id = 'different-conversation'
            await db.flush()
            response = await conversation(lead.id, service, 1, 1)
            assert json.loads(response.body)['data']['messages'][0]['replyTo'] is None
        await engine.dispose()
    asyncio.run(run())
