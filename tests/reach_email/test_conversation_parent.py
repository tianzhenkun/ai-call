import asyncio
import json

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.reach_email.controller import conversation
from app.services.reach_email.models import TABLES, Lead, Message, Task
from app.services.reach_email.service import EmailService


@pytest.mark.parametrize('reference_kind', ['exact', 'prefixed', 'references', 'prefixed_references', 'headerless'])
def test_parent_is_scoped_to_conversation_and_available_across_pages(reference_kind):
    async def run():
        engine = create_async_engine('sqlite+aiosqlite://')
        async with engine.begin() as conn:
            for table in TABLES:
                await conn.run_sync(lambda sync, t=table: t.create(sync))
        async with async_sessionmaker(engine, expire_on_commit=False)() as db:
            service = EmailService(db, 'tenant', 'owner', '')
            task = service.add(Task, name='test', import_id='import', settings='{}', content='{}')
            lead = service.add(Lead, task_id=task.id, email='test@example.com', values='{}')
            token = 'a44188e1c0e246dbb3670a7c005336d5'
            parent = service.add(Message, task_id=task.id, lead_id=lead.id, message_id=f'<{token}@reach.local>', dedup_key='parent', to_email=lead.email, subject='原邮件', html='正文', status='accepted')
            await db.flush()
            reply = service.add(Message, task_id=task.id, lead_id=lead.id, direction='inbound', message_id='<reply@test>', in_reply_to=parent.message_id, dedup_key='reply', to_email='sender@example.com', subject='回复', html='回复正文')
            await db.flush()
            parent.attempt_count = 1
            parent.from_email = 'sender@example.com'
            reply.from_email = lead.email
            reply.kind = 'reply'
            reply.subject = f'Re: 原邮件 [REACH:{token}]'
            if reference_kind.startswith('prefixed'):
                reply.in_reply_to = f'<431FAAAD64166961+{token}@reach.local>'
            if 'references' in reference_kind:
                reply.references = f'<older@test> {reply.in_reply_to}'
                reply.in_reply_to = ''
            if reference_kind == 'headerless':
                reply.in_reply_to = ''
            await db.flush()
            response = await conversation(lead.id, service, 1, 1)
            item = json.loads(response.body)['data']['messages'][0]
            assert item['id'] == reply.id
            assert item['replyTo']['id'] == parent.id
            assert item['replyTo']['deliveryStatus'] == 'delivered'
            assert item['inReplyTo'] == reply.in_reply_to
            for field in ('tenant_id', 'owner_id', 'account_id', 'task_id', 'lead_id'):
                previous = getattr(parent, field)
                setattr(parent, field, 'different')
                await db.flush()
                response = await conversation(lead.id, service, 1, 1)
                assert json.loads(response.body)['data']['messages'][0]['replyTo'] is None
                setattr(parent, field, previous)
        await engine.dispose()
    asyncio.run(run())
