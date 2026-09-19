import asyncio
import json
from datetime import datetime
from types import SimpleNamespace

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.reach_email.controller import recipient_summary, summary
from app.services.reach_email.migrate import migrate_tables
from app.services.reach_email.models import Message, Task
from app.services.reach_email.reporting import summarize
from app.services.reach_email.service import EmailService


def mail(identifier, **changes):
    values = dict(id=identifier, message_id=f'<{identifier}>', account_id='a', task_id='t',
        lead_id='l', direction='outbound', attempt_count=1, from_email='a@example.com',
        to_email='lead@example.com', status='accepted', kind='initial', delivery_report=None,
        in_reply_to='', references='', subject='test')
    values.update(changes)
    return SimpleNamespace(**values)


def test_evidence_is_per_message_deduplicated_and_recipient_scoped():
    reply = dict(direction='inbound', kind='reply', from_email='lead@example.com', to_email='a@example.com', in_reply_to='<one>')
    report = lambda action, status, recipient='lead@example.com': json.dumps({
        'messageId': '<two>', 'recipients': [{'recipient': recipient, 'action': action, 'status': status}]})
    rows = [mail('one'), mail('two'), mail('three', from_email='b@example.com', account_id='b'),
        mail('r1', **reply), mail('r2', **reply),
        mail('dsn', direction='inbound', kind='receipt', delivery_report=report('relayed', '2.0.0'))]
    result = summarize(rows)
    assert result['senderEmails'] == ['a@example.com', 'b@example.com']
    assert (result['sentCount'], result['deliveredCount'], result['repliedCount'], result['unconfirmedCount']) == (3, 1, 2, 2)
    rows[-1].delivery_report = report('delivered', '2.0.0', 'other@example.com')
    assert summarize(rows)['deliveredCount'] == 1
    rows[-1].delivery_report = report('delivered', '2.0.0')
    assert summarize(rows)['deliveredCount'] == 2
    rows[-1].delivery_report = report('failed', '5.1.1')
    assert summarize(rows)['failedCount'] == 1
    rows[3].in_reply_to = '<unknown>'
    rows[4].account_id = 'other-account'
    assert summarize(rows)['deliveredCount'] == 0
    rows[3].in_reply_to = '<one>'
    rows.append(mail('duplicate', message_id='<one>'))
    assert summarize(rows)['deliveredCount'] == 0


def test_auto_reply_confirms_only_its_original_without_counting_as_customer_reply():
    automatic = mail('auto', direction='inbound', kind='auto_reply',
        from_email='lead@example.com', to_email='a@example.com', in_reply_to='<one>')
    rows = [mail('one'), mail('two'), automatic]
    result = summarize(rows)
    assert (result['deliveredCount'], result['repliedCount'], result['unconfirmedCount']) == (1, 0, 1)
    automatic.in_reply_to = '<unknown>'
    assert summarize(rows)['deliveredCount'] == 0
    automatic.in_reply_to = '<one>'
    automatic.from_email = 'different@example.com'
    assert summarize(rows)['deliveredCount'] == 0
    automatic.from_email = 'lead@example.com'
    automatic.kind = 'bounce'
    assert summarize(rows)['deliveredCount'] == 0


def test_task_summary_pagination_search_and_owner_scope():
    async def run():
        engine = create_async_engine('sqlite+aiosqlite://')
        async with engine.begin() as conn:
            await conn.run_sync(migrate_tables)
        async with async_sessionmaker(engine, expire_on_commit=False)() as db:
            service = EmailService(db, 'tenant', 'owner', '')
            task = service.add(Task, name='task', import_id='i', settings='{}', content='{}')
            for i, sender in enumerate(['a@example.com', 'b@example.com']):
                service.add(Message, task_id=task.id, from_email=sender, to_email='lead@example.com',
                    dedup_key=str(i), message_id=f'<{i}>', subject='test', html='test', attempt_count=1)
            other = EmailService(db, 'other', 'owner', '')
            other.add(Task, name='private', import_id='i', settings='{}', content='{}')
            await db.flush()
            data = json.loads((await summary(service, 1, 1, '', 'b@example.com')).body)['data']
            assert data['total'] == 1
            assert data['items'][0]['sentCount'] == 2
            assert len(data['items'][0]['senders']) == 2
            recipients = json.loads((await recipient_summary(service, task.id, 'a@example.com', 1, 10)).body)['data']
            assert recipients['total'] == 1
            assert recipients['items'][0]['recipientEmail'] == 'lead@example.com'
            assert recipients['items'][0]['sentCount'] == 1
            first = json.loads((await recipient_summary(service, task.id, '', 1, 1)).body)['data']
            second = json.loads((await recipient_summary(service, task.id, '', 2, 1)).body)['data']
            assert first['total'] == second['total'] == 2
            assert first['items'][0]['senderEmail'] == 'a@example.com'
            assert second['items'][0]['senderEmail'] == 'b@example.com'
            assert first['items'][0]['recipientEmail'] == second['items'][0]['recipientEmail'] == 'lead@example.com'
            assert first['items'][0]['sentCount'] == second['items'][0]['sentCount'] == 1
            assert json.loads((await summary(other, 1, 20, task.id, '')).body)['data']['items'] == []
        await engine.dispose()
    asyncio.run(run())


def test_recipient_totals_match_sender_and_keep_other_recipients_separate():
    rows = [mail('one'), mail('two', to_email='other@example.com', lead_id='other'),
        mail('reply', direction='inbound', kind='reply', from_email='lead@example.com', to_email='a@example.com', in_reply_to='<one>')]
    recipients = summarize(rows, sender_email='a@example.com')
    assert len(recipients) == 2
    assert recipients[0]['deliveredCount'] == recipients[0]['repliedCount'] == 1
    assert recipients[1]['deliveredCount'] == recipients[1]['repliedCount'] == 0
    sender = summarize(rows)['senders'][0]
    for key in ('sentCount', 'deliveredCount', 'failedCount', 'unconfirmedCount', 'repliedCount'):
        assert sum(r[key] for r in recipients) == sender[key]


def test_recipient_first_sent_time_uses_initial_acceptance_per_sender_and_recipient():
    async def run():
        engine = create_async_engine('sqlite+aiosqlite://')
        async with engine.begin() as conn:
            await conn.run_sync(migrate_tables)
        async with async_sessionmaker(engine, expire_on_commit=False)() as db:
            service = EmailService(db, 'tenant', 'owner', '')
            task = service.add(Task, name='times', import_id='i', settings='{}', content='{}')
            cases = [
                ('initial', 'a@example.com', 'lead@example.com', 'accepted', 10, None),
                ('initial', 'a@example.com', 'lead@example.com', 'failed', 9, None),
                ('follow_up', 'a@example.com', 'lead@example.com', 'accepted', 8, None),
                ('reply', 'a@example.com', 'lead@example.com', 'accepted', 7, None),
                ('initial', 'a@example.com', 'lead@example.com', 'accepted', 6, 6),
                ('initial', 'b@example.com', 'lead@example.com', 'accepted', 11, None),
                ('initial', 'a@example.com', 'pending@example.com', 'retryable', None, None),
            ]
            for index, (kind, sender, recipient, status, hour, resolved) in enumerate(cases):
                service.add(Message, task_id=task.id, from_email=sender, to_email=recipient,
                    kind=kind, status=status, dedup_key=str(index), message_id=f'<{index}>',
                    subject='test', html='test', attempt_count=1,
                    sent_at=datetime(2026, 9, 19, hour) if hour else None,
                    resolved_at=datetime(2026, 9, 19, resolved) if resolved else None,
                    created_at=datetime(2026, 9, 18))
            other = EmailService(db, 'other', 'owner', '')
            other.add(Message, task_id=task.id, from_email='a@example.com', to_email='lead@example.com',
                kind='initial', status='accepted', dedup_key='other', message_id='<other>',
                subject='test', html='test', attempt_count=1, sent_at=datetime(2026, 9, 18))
            await db.flush()
            result = json.loads((await recipient_summary(service, task.id)).body)['data']['items']
            actual = {(row['senderEmail'], row['recipientEmail']): row['firstSentAt'] for row in result}
            assert actual == {
                ('a@example.com', 'lead@example.com'): '2026-09-19T09:00:00+00:00',
                ('a@example.com', 'pending@example.com'): None,
                ('b@example.com', 'lead@example.com'): '2026-09-19T11:00:00+00:00',
            }
        await engine.dispose()
    asyncio.run(run())
