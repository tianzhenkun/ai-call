import asyncio
import hashlib
import json
from datetime import datetime, timedelta

import httpx
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.services.reach_email.ai import EmailAI
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


@pytest.mark.parametrize('selected_reply', [True, False])
def test_reply_ai_sends_complete_history_and_long_bodies_to_model(selected_reply):
    async def run():
        engine = create_async_engine('sqlite+aiosqlite://')
        async with engine.begin() as conn:
            for table in TABLES:
                await conn.run_sync(table.create)
        async with async_sessionmaker(engine, expire_on_commit=False)() as db:
            service = EmailService(db, 'tenant', 'owner', '')
            task = service.add(Task, name='task', status='ended', import_id='import', settings='{}', content='{}')
            lead = service.add(Lead, task_id=task.id, email='test@example.com', values='{}')
            start = datetime(2026, 10, 14)
            history = []
            for index in range(13):
                history.append(service.add(
                    Message, task_id=task.id, lead_id=lead.id,
                    direction='inbound' if index % 2 == 0 else 'outbound',
                    kind='reply', status='received' if index % 2 == 0 else 'accepted',
                    dedup_key=f'mail-{index}', message_id=f'<mail-{index}@example.test>',
                    subject=f'往来 {index}', html='<p>' + '完整正文' * 6000 + f'末尾条件 {index}</p>',
                    to_email='test@example.com', created_at=start + timedelta(minutes=index),
                ))
            target = history[-1]
            later = service.add(
                Message, task_id=task.id, lead_id=lead.id, direction='inbound', kind='reply',
                dedup_key='later', message_id='<later@example.test>', subject='后续来信', html='后续条件',
                to_email='test@example.com', created_at=target.created_at + timedelta(minutes=1),
            )
            for index, overrides in enumerate([
                {'tenant_id': 'other'}, {'owner_id': 'other'}, {'lead_id': 'other'},
                {'task_id': 'other'}, {'kind': 'auto_reply'},
                {'direction': 'outbound', 'status': 'failed'},
                {'direction': 'outbound', 'status': 'queued'},
            ]):
                values = {
                    'task_id': task.id, 'lead_id': lead.id, 'direction': 'inbound',
                    'kind': 'reply', 'status': 'received', 'dedup_key': f'excluded-{index}',
                    'message_id': f'<excluded-{index}@example.test>', 'subject': '不属于上下文',
                    'html': '不应传入模型', 'to_email': 'test@example.com', 'created_at': start,
                }
                tenant = overrides.pop('tenant_id', 'tenant')
                owner = overrides.pop('owner_id', 'owner')
                EmailService(db, tenant, owner, '').add(Message, **(values | overrides))
            await db.flush()
            result = await service.reply_ai_job(lead.id, ReplyAIInput(
                action='generate', inboundId=target.id if selected_reply else None,
            ))
            job = await db.get(AIJob, result['id'])
            payload = json.loads(job.payload)
            expected = history if selected_reply else [*history, later]

            def handler(request):
                request_payload = json.loads(request.content)
                assert 'max_tokens' not in request_payload
                context = json.loads(request_payload['messages'][1]['content'])['context']
                assert context['messages'] == [
                    {'direction': mail.direction, 'subject': mail.subject, 'content': mail.html}
                    for mail in expected
                ]
                assert context['replyTo'] == (
                    {'subject': target.subject, 'content': target.html} if selected_reply else None
                )
                return httpx.Response(200, json={'choices': [{'message': {'content': json.dumps({
                    'subject': 'Re: 往来', 'content': '<p>待审阅回复</p>',
                })}, 'finish_reason': 'stop'}]})

            ai = EmailAI('https://example.test/v1', 'email-model', 'test-key', httpx.MockTransport(handler))
            assert (await ai.run(**payload))['content'] == '<p>待审阅回复</p>'
            assert job.status == 'queued'
        await engine.dispose()
    asyncio.run(run())
