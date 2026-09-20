import asyncio
import json

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.reach_email.controller import conversation
from app.services.reach_email.migrate import migrate_tables
from app.services.reach_email.models import Account, AIJob, Lead, Message, Task, now
from app.services.reach_email.schema import ReplyAIInput, ReplyInput
from app.services.reach_email.service import EmailService, dump


def test_manual_followup_requires_accepted_first_and_renders_customer_variables():
    async def run():
        engine = create_async_engine('sqlite+aiosqlite://')
        async with engine.begin() as conn:
            await conn.run_sync(migrate_tables)
        async with async_sessionmaker(engine, expire_on_commit=False)() as db:
            s = EmailService(db, 't', 'o', '')
            account = s.add(Account, name='发件人', email='sender@example.com', config='{}', secret='')
            task = s.add(Task, name='任务', import_id='i', settings='{}', status='ended')
            await db.flush()
            lead = s.add(Lead, task_id=task.id, account_id=account.id, email='customer@example.com',
                         values=dump({'客户姓名': '<客户>', '职务': ''}))
            await db.flush()
            first = s.add(Message, task_id=task.id, lead_id=lead.id, account_id=account.id,
                          from_email=account.email, to_email=lead.email, subject='邀请', html='正文',
                          message_id='<first@example.com>', dedup_key='initial', status='queued')
            await db.flush()
            lead.first_message_id = first.id
            await db.flush()
            data = ReplyInput(subject='跟进', html='<p>{{客户姓名}}您好</p>', requestId='send')
            for status, reason in [('queued', '排队'), ('sending', '发送中'), ('retryable', '重试'), ('failed', '失败'), ('unknown', '不确定')]:
                first.status = status
                await db.flush()
                detail = json.loads((await conversation(lead.id, s, 1, 1)).body)['data']
                assert not detail['canSend'] and reason in detail['sendBlockedReason']
                with pytest.raises(ValueError, match=reason):
                    await s.reply(lead.id, data, 'manual_follow_up')
            first.status, first.sent_at = 'accepted', now()
            await db.flush()
            detail = json.loads((await conversation(lead.id, s, 1, 1)).body)['data']
            assert detail['canSend'] and detail['firstMessage']['id'] == first.id
            assert detail['firstMessage']['deliveryStatus'] == 'unconfirmed'
            assert detail['variables'] == {'客户姓名': '<客户>', '职务': ''}
            for html, reason in [('<p>{{职务}}</p>', '职务'), ('<p>{{未定义}}</p>', '变量'), ('<p><br></p>', '正文')]:
                with pytest.raises(ValueError, match=reason):
                    await s.reply(lead.id, data.model_copy(update={'html': html}), 'manual_follow_up')
            result = await s.reply(lead.id, data, 'manual_follow_up')
            assert result['html'] == '<p>&lt;客户&gt;您好</p>'
            assert result['fromEmail'] == account.email
            assert result['inReplyTo'] == first.message_id
            assert result['status'] == 'queued'
            assert (await s.reply(lead.id, data, 'manual_follow_up'))['id'] == result['id']
            inbound = s.add(Message, task_id=task.id, lead_id=lead.id, direction='inbound', kind='reply',
                            from_email=lead.email, to_email=account.email, subject='客户回复', html='请介绍产品',
                            message_id='<reply@example.com>', dedup_key='inbound', in_reply_to=first.message_id)
            await db.flush()
            reply_data = data.model_copy(update={'inbound_id': inbound.id, 'request_id': 'reply'})
            reply = await s.reply(lead.id, reply_data, 'reply')
            assert reply['kind'] == 'reply' and reply['inReplyTo'] == inbound.message_id
            assert reply['html'] == '<p>&lt;客户&gt;您好</p>'
            detail = json.loads((await conversation(lead.id, s, 1, 1)).body)['data']
            assert detail['messages'][0]['id'] == reply['id']
            assert detail['latestInbound']['id'] == inbound.id
            inbound.kind = 'auto_reply'
            await db.flush()
            with pytest.raises(ValueError, match='会话'):
                await s.reply(lead.id, reply_data.model_copy(update={'request_id': 'auto'}), 'reply')
            detail = json.loads((await conversation(lead.id, s, 1, 1)).body)['data']
            assert detail['latestInbound'] is None
            job = await s.reply_ai_job(lead.id, ReplyAIInput(action='generate'))
            payload = json.loads((await db.get(AIJob, job['id'])).payload)
            assert payload['allowed_variables'] == ['客户姓名', '职务']
            assert payload['context']['replyTo'] is None
            assert payload['context']['messages'] == [{'direction': 'outbound', 'subject': first.subject, 'content': first.html}]
            account.enabled = False
            await db.flush()
            detail = json.loads((await conversation(lead.id, s, 1, 1)).body)['data']
            assert not detail['canSend'] and '停用' in detail['sendBlockedReason']
        await engine.dispose()
    asyncio.run(run())
