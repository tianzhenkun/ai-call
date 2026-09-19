import asyncio
import hashlib
import json

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.services.reach_email.models import TABLES, AIJob, Lead, Message, Task
from app.services.reach_email.schema import ReplyAIInput
from app.services.reach_email.service import EmailService, dump


def test_reply_ai_uses_selected_inbound_and_scoped_context_without_sending():
    async def run():
        engine = create_async_engine('sqlite+aiosqlite://')
        async with engine.begin() as conn:
            for table in TABLES:
                await conn.run_sync(lambda sync, t=table: t.create(sync))
        async with async_sessionmaker(engine, expire_on_commit=False)() as db:
            s = EmailService(db, 'tenant', 'owner', '')
            task = s.add(Task, name='task', status='ended', import_id='import', settings='{}', content='{}')
            lead = s.add(Lead, task_id=task.id, email='test@example.com', values='{}')
            mail = s.add(Message, task_id=task.id, lead_id=lead.id, direction='inbound', kind='reply', dedup_key='incoming', message_id='<incoming@test>', subject='报价问题', html='<p>请介绍产品</p>', to_email='sender@example.com')
            await db.flush()
            data = ReplyAIInput(action='generate', inboundId=mail.id, instruction='简洁回答')
            result = await s.reply_ai_job(lead.id, data)
            job = await db.get(AIJob, result['id'])
            payload = json.loads(job.payload)
            assert payload['context']['replyTo']['content'] == mail.html
            assert payload['context']['mode'] == 'reply'
            assert payload['allowed_variables'] == []
            old_hash = hashlib.sha256(dump(dict(version=task.version, **payload)).encode()).hexdigest()
            assert job.source_hash != old_hash
            assert (await s.reply_ai_job(lead.id, data))['id'] == job.id
            with pytest.raises(ValueError, match='不存在'):
                await EmailService(db, 'other', 'owner', '').reply_ai_job(lead.id, data)
            mail.lead_id = 'other-conversation'
            await db.flush()
            with pytest.raises(ValueError, match='来信'):
                await s.reply_ai_job(lead.id, data)
            assert len((await db.scalars(s.query(Message))).all()) == 1
        await engine.dispose()
    asyncio.run(run())
