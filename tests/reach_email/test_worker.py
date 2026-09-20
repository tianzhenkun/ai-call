import asyncio
import json
from contextlib import asynccontextmanager
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.services.reach_email.models import (
    TABLES,
    Account,
    AIJob,
    Attempt,
    Lead,
    Message,
    Task,
    WorkerLease,
    now,
)
from app.services.reach_email.worker import EmailWorker


def test_worker_initializes_oss_before_building_store(monkeypatch):
    from cryptography.fernet import Fernet

    from app.api.v1.system.oss.service import OssService
    from app.services.ai_call import knowledge
    from app.services.reach_email import worker as module

    init = AsyncMock()
    engine = SimpleNamespace(dispose=AsyncMock())
    monkeypatch.setattr(OssService, 'init_active_config', init)
    monkeypatch.setattr(module, 'configured_secret', lambda *args: Fernet.generate_key().decode())
    monkeypatch.setattr(module, 'create_async_engine', lambda *args, **kwargs: engine)
    monkeypatch.setattr(module, 'async_sessionmaker', lambda *args, **kwargs: None)
    monkeypatch.setattr(module, 'EmailWorker', lambda *args, **kwargs: SimpleNamespace(acquire=AsyncMock(return_value=False)))

    def store(settings):
        assert init.await_count == 1
        return object()

    monkeypatch.setattr(knowledge, 'build_knowledge_store', store)
    with pytest.raises(RuntimeError, match='EMAIL_WORKER_ALREADY_RUNNING'):
        asyncio.run(module.main())
    engine.dispose.assert_awaited_once()


def test_manual_sync_does_not_overwrite_a_newer_cursor():
    async def check():
        async with setup() as (worker, sessions):
            async def fetched(*args, **kwargs):
                async with sessions() as db, db.begin():
                    account = await db.get(Account, 'a')
                    account.uidvalidity, account.last_uid = 'current', 10
                return {'uidvalidity': 'current', 'last_uid': 5, 'messages': [], 'drained': True}
            with patch('app.services.reach_email.worker.asyncio.to_thread', side_effect=fetched):
                with pytest.raises(ValueError, match='另一轮同步'):
                    await worker.sync_account('a', require_worker_lease=False)
            async with sessions() as db:
                assert (await db.get(Account, 'a')).last_uid == 10
    asyncio.run(check())


@asynccontextmanager
async def setup():
    engine = create_async_engine('sqlite+aiosqlite://')
    async with engine.begin() as conn:
        for table in TABLES:
            await conn.run_sync(table.create)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    cipher = SimpleNamespace(decrypt=lambda value: {})
    worker = EmailWorker(sessions, cipher, AsyncMock(), AsyncMock())
    async with sessions() as db, db.begin():
        db.add(Account(id='a', tenant_id='t', owner_id='o', name='Test', email='sender@example.com',
            config='{}', secret='test', smtp_status='ok', imap_status='ok', interval_seconds=1))
        db.add(Task(id='t', tenant_id='t', owner_id='o', name='Test', import_id='i', status='running',
            settings=json.dumps({'followUpEnabled': True, 'followUpCount': 2, 'followUpIntervalDays': 1})))
        db.add(Lead(id='l', tenant_id='t', owner_id='o', task_id='t', email='lead@example.com',
            values='{}', first_message_id='m'))
        db.add(Message(id='m', tenant_id='t', owner_id='o', task_id='t', lead_id='l',
            dedup_key='initial:l', message_id='<first@example.com>', to_email='lead@example.com',
            subject='Hello', html='<p>hello</p>'))
    await worker.acquire()
    try:
        yield worker, sessions
    finally:
        await engine.dispose()


def test_send_reserves_attempt_and_binds_account_and_followup():
    async def check():
        async with setup() as (worker, sessions):
            with patch('app.services.reach_email.worker.transport.send_message', return_value={'status': 'accepted'}) as send:
                await worker.send_one('m')
            assert send.call_count == 1
            async with sessions() as db:
                msg, lead = await db.get(Message, 'm'), await db.get(Lead, 'l')
                assert msg.status == 'accepted' and msg.attempt_count == 1
                assert lead.account_id == 'a' and lead.next_follow_up_at > now()
                assert (await db.scalar(select(Attempt))).status == 'accepted'
    asyncio.run(check())


@pytest.mark.parametrize('blocked_by', ['interval', 'hourly', 'daily'])
def test_weighted_assignment_precedes_rate_limits_and_survives_waiting(blocked_by):
    async def check():
        async with setup() as (worker, sessions):
            stamp = now()
            ids = ['m', *[f'm{i}' for i in range(1, 6)]]
            async with sessions() as db, db.begin():
                (await db.get(Account, 'a')).interval_seconds = 60
                heavy = Account(id='b', tenant_id='t', owner_id='o', name='Heavy',
                    email='heavy@example.com', config='{}', secret='test',
                    smtp_status='ok', imap_status='ok', weight=2, interval_seconds=60)
                db.add(heavy)
                if blocked_by == 'interval':
                    heavy.next_send_at = stamp + timedelta(minutes=5)
                else:
                    setattr(heavy, f'{blocked_by}_limit', 1)
                    db.add(Attempt(tenant_id='t', owner_id='o', message_id='older',
                        account_id='b', task_id='t', status='accepted', created_at=stamp))
                for i in range(1, 6):
                    db.add(Lead(id=f'l{i}', tenant_id='t', owner_id='o', task_id='t',
                        email=f'lead{i}@example.com', values='{}', first_message_id=f'm{i}'))
                    db.add(Message(id=f'm{i}', tenant_id='t', owner_id='o', task_id='t',
                        lead_id=f'l{i}', dedup_key=f'initial:l{i}', message_id=f'<m{i}>',
                        to_email=f'lead{i}@example.com', subject='Hello', html='<p>hello</p>'))
            with patch('app.services.reach_email.worker.transport.send_message', return_value={'status': 'accepted'}) as send:
                for identity in ids:
                    await worker.send_one(identity)
                assert send.call_count == 1
                async with sessions() as db:
                    rows = (await db.scalars(select(Message).order_by(Message.id))).all()
                    assignments = {m.id: m.account_id for m in rows}
                    assert list(assignments.values()).count('a') == 2
                    assert list(assignments.values()).count('b') == 4
                    for m in rows:
                        assert (await db.get(Lead, m.lead_id)).account_id == m.account_id
                        if m.status == 'queued':
                            assert m.attempt_count == 0 and m.due_at > stamp
                # 账号暂时停用、worker 换实例及重复扫描均不得改派或再次消耗权重。
                worker = EmailWorker(sessions, worker.cipher, worker.store, worker.ai)
                async with sessions() as db, db.begin():
                    (await db.get(Account, 'b')).enabled = False
                    (await db.get(Account, 'a')).next_send_at = None
                    for m in (await db.scalars(select(Message).where(Message.status == 'queued'))).all():
                        m.due_at = stamp
                # 模拟重启后的租约接管，不触碰真实运行数据库。
                async with sessions() as db, db.begin():
                    (await db.get(WorkerLease, 'email')).expires_at = stamp - timedelta(seconds=1)
                assert await worker.acquire()
                for identity in ids:
                    await worker.send_one(identity)
                assert send.call_count == 2
                async with sessions() as db:
                    assert {m.id: m.account_id for m in (await db.scalars(select(Message))).all()} == assignments
                    assert (await db.get(Account, 'a')).current_weight == 0
                    assert (await db.get(Account, 'b')).current_weight == 0
                async with sessions() as db, db.begin():
                    heavy = await db.get(Account, 'b')
                    heavy.enabled, heavy.hourly_limit, heavy.daily_limit = True, 100, 100
                for _ in range(4):
                    async with sessions() as db, db.begin():
                        (await db.get(Account, 'b')).next_send_at = None
                        for m in (await db.scalars(select(Message).where(Message.status == 'queued'))).all():
                            m.due_at = stamp
                    for identity in ids:
                        await worker.send_one(identity)
                assert send.call_count == 6
                async with sessions() as db:
                    assert all(m.status == 'accepted' and m.attempt_count == 1
                        for m in (await db.scalars(select(Message))).all())
    asyncio.run(check())


def test_second_worker_cannot_claim_and_crash_becomes_unknown():
    async def check():
        async with setup() as (worker, sessions):
            other = EmailWorker(sessions, worker.cipher, worker.store, worker.ai)
            assert not await other.acquire()
            await worker.claim('m')
            async with sessions() as db, db.begin():
                msg = await db.get(Message, 'm')
                msg.lease_until = now() - timedelta(seconds=1)
            await worker.recover_and_schedule()
            assert await worker.claim('m') is None
            async with sessions() as db:
                assert (await db.get(Message, 'm')).status == 'unknown'
                assert (await db.scalar(select(Attempt))).status == 'unknown'
    asyncio.run(check())


def test_limits_are_rolling_24_hours_and_scheduled_tasks_wait():
    async def check():
        async with setup() as (worker, sessions):
            async with sessions() as db, db.begin():
                account = await db.get(Account, 'a')
                account.daily_limit = 1
                db.add(Attempt(tenant_id='t', owner_id='o', message_id='older', account_id='a',
                    task_id='t', created_at=now() - timedelta(hours=23), status='accepted'))
            assert await worker.claim('m') is None
            async with sessions() as db, db.begin():
                (await db.get(Account, 'a')).daily_limit = 100
                task = await db.get(Task, 't')
                task.status, task.scheduled_at = 'scheduled', now() + timedelta(hours=1)
            assert await worker.claim('m') is None
    asyncio.run(check())


def test_followup_copies_first_snapshot_after_fresh_sync_only():
    async def check():
        async with setup() as (worker, sessions):
            async with sessions() as db, db.begin():
                first, lead = await db.get(Message, 'm'), await db.get(Lead, 'l')
                first.status, first.account_id = 'accepted', 'a'
                first.attachment_ids = '["attachment"]'
                lead.account_id, lead.next_follow_up_at = 'a', now() - timedelta(days=1)
                lead.classification = 'low_value'
            await worker.followups()
            async with sessions() as db:
                assert len((await db.scalars(select(Message))).all()) == 1
            worker.sync_ok.add('a')
            await worker.followups()
            await worker.followups()
            async with sessions() as db:
                followup = await db.scalar(select(Message).where(Message.kind == 'follow_up'))
                assert followup.html == '<p>hello</p>' and followup.attachment_ids == '["attachment"]'
                assert followup.in_reply_to == '<first@example.com>' and followup.account_id == 'a'
                assert len((await db.scalars(select(Message))).all()) == 2
    asyncio.run(check())


def incoming(**changes):
    return dict(uid=1, message_id='<reply>', in_reply_to='<first@example.com>', references='',
        from_email='lead@example.com', to_emails=['sender@example.com'], subject='Re: Hello',
        html='<p>reply</p>', text='reply', date='', attachments=[], auto_reply=False, bounce=False,
        permanent_bounce=False, **changes)


def test_sync_deduplicates_and_reply_stops_automatic_followups():
    async def check():
        async with setup() as (worker, sessions):
            async with sessions() as db, db.begin():
                (await db.get(Message, 'm')).account_id = 'a'
                (await db.get(Lead, 'l')).account_id = 'a'
            result = {'uidvalidity': '1', 'last_uid': 1, 'messages': [incoming()]}
            with patch('app.services.reach_email.worker.transport.sync_messages', return_value=result):
                await worker.sync_account('a')
                await worker.sync_account('a')
            async with sessions() as db:
                assert (await db.get(Lead, 'l')).stopped_reason == 'replied'
                assert (await db.get(Message, 'm')).status == 'cancelled'
                assert (await db.get(Account, 'a')).last_uid == 1
                assert len((await db.scalars(select(Message))).all()) == 2
    asyncio.run(check())


def test_inbound_wrong_sender_is_preserved_unmatched():
    async def check():
        async with setup() as (worker, sessions):
            async with sessions() as db, db.begin():
                (await db.get(Message, 'm')).account_id = 'a'
                (await db.get(Lead, 'l')).account_id = 'a'
            item = incoming()
            item['from_email'] = 'stranger@example.com'
            with patch('app.services.reach_email.worker.transport.sync_messages', return_value={
                'uidvalidity': '1', 'last_uid': 1, 'messages': [item]}):
                await worker.sync_account('a')
            async with sessions() as db:
                reply = await db.scalar(select(Message).where(Message.direction == 'inbound'))
                assert reply.lead_id is None and reply.task_id is None
                assert (await db.get(Lead, 'l')).stopped_reason is None
    asyncio.run(check())


def test_followup_waits_for_its_sync_round_without_rescheduling():
    async def check():
        async with setup() as (worker, sessions):
            async with sessions() as db, db.begin():
                message = await db.get(Message, 'm')
                message.kind, message.account_id = 'follow_up', 'a'
                due = message.due_at
                (await db.get(Lead, 'l')).account_id = 'a'
            worker.account_cursor = 'a'
            with patch('app.services.reach_email.worker.transport.sync_messages', return_value={
                'uidvalidity': '1', 'last_uid': 0, 'messages': [], 'drained': True
            }), patch('app.services.reach_email.worker.transport.send_message', return_value={'status': 'accepted'}) as send:
                await worker.run_once()
                async with sessions() as db:
                    assert (await db.get(Message, 'm')).due_at == due
                send.assert_not_called()
                await worker.run_once()
                send.assert_called_once()
    asyncio.run(check())


@pytest.mark.parametrize('mismatch', [None, 'sender', 'recipient', 'account', 'owner', 'tenant', 'unaccepted', 'two_tokens', 'headers'])
def test_headerless_reply_uses_exact_marker_and_scoped_participants(mismatch):
    async def check():
        async with setup() as (worker, sessions):
            token = '0123456789abcdef0123456789abcdef'
            async with sessions() as db, db.begin():
                source = await db.get(Message, 'm')
                source.account_id, source.status = 'a', 'accepted'
                source.message_id = f'<{token}@reach.local>'
                (await db.get(Lead, 'l')).account_id = 'a'
                if mismatch == 'account':
                    source.account_id = 'other'
                if mismatch == 'owner':
                    source.owner_id = 'other'
                if mismatch == 'tenant':
                    source.tenant_id = 'other'
                if mismatch == 'unaccepted':
                    source.status = 'failed'
            item = incoming()
            item.update(in_reply_to='', references='', subject=f'Re: Hello [REACH:{token}]')
            if mismatch == 'sender':
                item['from_email'] = 'stranger@example.com'
            if mismatch == 'recipient':
                item['to_emails'] = ['other@example.com']
            if mismatch == 'two_tokens':
                item['subject'] += ' [REACH:fedcba9876543210fedcba9876543210]'
            if mismatch == 'headers':
                item['in_reply_to'] = '<different@example.com>'
            with patch('app.services.reach_email.worker.transport.sync_messages', return_value={
                'uidvalidity': '1', 'last_uid': 1, 'messages': [item]}):
                await worker.sync_account('a')
            async with sessions() as db:
                reply = await db.scalar(select(Message).where(Message.direction == 'inbound'))
                assert (reply.task_id, reply.lead_id) == (('t', 'l') if mismatch is None else (None, None))
    asyncio.run(check())


@pytest.mark.parametrize('oversized', ['count', 'size'])
def test_auto_reply_does_not_stop_and_oversized_attachments_do_not_advance_cursor(oversized):
    async def check():
        async with setup() as (worker, sessions):
            async with sessions() as db, db.begin():
                (await db.get(Message, 'm')).account_id = 'a'
                (await db.get(Lead, 'l')).account_id = 'a'
            item = incoming()
            item['auto_reply'] = True
            result = {'uidvalidity': '1', 'last_uid': 1, 'messages': [item]}
            with patch('app.services.reach_email.worker.transport.sync_messages', return_value=result):
                await worker.sync_account('a')
                item['attachments'] = ([{'content': b'x'}] * 6 if oversized == 'count'
                                       else [{'content': b'x' * (8 * 1024 * 1024 + 1)}])
                result['last_uid'] = 2
                try:
                    await worker.sync_account('a')
                    raise AssertionError('must reject excessive attachments')
                except ValueError as exc:
                    assert str(exc) == 'INBOUND_ATTACHMENT_LIMIT'
            async with sessions() as db:
                assert (await db.get(Lead, 'l')).stopped_reason is None
                assert (await db.get(Account, 'a')).last_uid == 1
    asyncio.run(check())


def test_ai_result_does_not_replace_new_version():
    async def check():
        async with setup() as (worker, sessions):
            async with sessions() as db, db.begin():
                db.add(AIJob(id='job', tenant_id='t', owner_id='o', task_id='t', action='translate',
                    source_hash='hash', source_version=0, payload='{"action":"translate"}'))
            worker.ai.run.return_value = {'reviewSubject': '你好', 'reviewContent': '你好'}
            await worker.ai_jobs()
            async with sessions() as db:
                job = await db.get(AIJob, 'job')
                assert job.status == 'stale' and job.result is None
                assert (await db.get(Task, 't')).content == '{}'
    asyncio.run(check())


def test_manual_followup_allowed_after_task_end():
    async def check():
        async with setup() as (worker, sessions):
            async with sessions() as db, db.begin():
                task, msg = await db.get(Task, 't'), await db.get(Message, 'm')
                task.status = 'ended'
                msg.kind, msg.dedup_key = 'follow_up', 'manual:l:request'
                (await db.get(Lead, 'l')).stopped_reason = 'replied'
            assert await worker.claim('m') is not None
    asyncio.run(check())


@pytest.mark.parametrize('kind,manual,allowed', [
    ('reply', True, True), ('initial', False, False),
    ('follow_up', False, False), ('follow_up', True, False),
])
def test_only_manual_reply_bypasses_task_quota(kind, manual, allowed):
    async def check():
        async with setup() as (worker, sessions):
            worker.sync_ok.add('a')
            async with sessions() as db, db.begin():
                (await db.get(Task, 't')).daily_limit = 1
                msg = await db.get(Message, 'm')
                msg.kind, msg.account_id = kind, 'a'
                msg.dedup_key = 'manual:l:reply' if manual else 'initial:l'
                (await db.get(Lead, 'l')).account_id = 'a'
                db.add(Attempt(tenant_id='t', owner_id='o', message_id='old', account_id='a',
                    task_id='t', created_at=now() - timedelta(hours=2), status='accepted'))
            assert (await worker.claim('m') is not None) == allowed
    asyncio.run(check())


def test_manual_replies_count_only_toward_account_quota():
    async def check():
        async with setup() as (worker, sessions):
            first_at = now() - timedelta(hours=2)
            async with sessions() as db, db.begin():
                msg = await db.get(Message, 'm')
                msg.kind, msg.dedup_key = 'reply', 'manual:l:reply'
                db.add(Attempt(tenant_id='t', owner_id='o', message_id='m', account_id='a',
                    task_id='t', created_at=first_at - timedelta(hours=1), status='accepted'))
                db.add(Attempt(tenant_id='t', owner_id='o', message_id='initial', account_id='a',
                    task_id='t', created_at=first_at, status='accepted'))
            async with sessions() as db:
                row = await db.get(Message, 'm')
                since = now() - timedelta(hours=24)
                assert await worker.count_attempts(db, row, since, task_id='t') == 1
                assert await worker.count_attempts(db, row, since, account_id='a') == 2
                assert await worker.quota_release(db, row, 24, task_id='t') == first_at + timedelta(hours=24, seconds=1)
    asyncio.run(check())


@pytest.mark.parametrize('blocked_by', ['hourly', 'daily', 'interval', 'disabled', 'suppressed'])
def test_manual_reply_keeps_account_and_suppression_guards(blocked_by):
    from app.services.reach_email.models import Suppression

    async def check():
        async with setup() as (worker, sessions):
            async with sessions() as db, db.begin():
                account, msg = await db.get(Account, 'a'), await db.get(Message, 'm')
                msg.kind, msg.dedup_key, msg.account_id = 'reply', 'manual:l:reply', 'a'
                if blocked_by in ('hourly', 'daily'):
                    setattr(account, f'{blocked_by}_limit', 1)
                    db.add(Attempt(tenant_id='t', owner_id='o', message_id='old', account_id='a',
                        task_id='t', created_at=now() - timedelta(minutes=5), status='accepted'))
                elif blocked_by == 'interval':
                    account.next_send_at = now() + timedelta(minutes=5)
                elif blocked_by == 'disabled':
                    account.enabled = False
                else:
                    db.add(Suppression(tenant_id='t', owner_id='o', email=msg.to_email, reason='manual'))
            assert await worker.claim('m') is None
            async with sessions() as db:
                assert (await db.get(Message, 'm')).attempt_count == 0
    asyncio.run(check())


def test_blocked_first_batch_does_not_starve_other_task():
    async def check():
        async with setup() as (worker, sessions):
            worker.batch_size = 10
            async with sessions() as db, db.begin():
                (await db.get(Task, 't')).daily_limit = 1
                db.add(Attempt(tenant_id='t', owner_id='o', message_id='old', account_id='a',
                    task_id='t', created_at=now() - timedelta(hours=2), status='accepted'))
                for index in range(9):
                    db.add(Message(id=f'blocked{index}', tenant_id='t', owner_id='o', task_id='t', lead_id='l',
                        dedup_key=f'initial:{index}', message_id=f'<blocked{index}>', to_email='lead@example.com',
                        subject='blocked', html='blocked', due_at=now() - timedelta(days=1)))
                db.add(Task(id='task_b', tenant_id='t', owner_id='o', name='B', import_id='i',
                    status='running', settings='{}'))
                db.add(Lead(id='lead_b', tenant_id='t', owner_id='o', task_id='task_b', email='other@example.com', values='{}'))
                db.add(Message(id='message_b', tenant_id='t', owner_id='o', task_id='task_b', lead_id='lead_b',
                    dedup_key='initial:b', message_id='<b>', to_email='other@example.com', subject='B', html='B',
                    due_at=now() + timedelta(microseconds=1)))
            result = {'uidvalidity': '1', 'last_uid': 0, 'drained': True, 'messages': []}
            with patch('app.services.reach_email.worker.transport.sync_messages', return_value=result), \
                 patch('app.services.reach_email.worker.transport.send_message', return_value={'status': 'accepted'}) as send:
                await worker.run_once()
                assert send.call_count == 0
                await worker.run_once()
                assert send.call_count == 1
            async with sessions() as db:
                assert (await db.get(Message, 'message_b')).status == 'accepted'
                assert (await db.get(Message, 'm')).due_at > now() + timedelta(hours=20)
    asyncio.run(check())


def test_backlog_blocks_followup_until_later_reply_is_processed():
    async def check():
        async with setup() as (worker, sessions):
            async with sessions() as db, db.begin():
                first, lead = await db.get(Message, 'm'), await db.get(Lead, 'l')
                first.status, first.account_id = 'accepted', 'a'
                lead.account_id, lead.next_follow_up_at = 'a', now() - timedelta(days=1)
            first_batch = {'uidvalidity': '1', 'last_uid': 1, 'drained': False, 'messages': []}
            second_batch = {'uidvalidity': '1', 'last_uid': 2, 'drained': True, 'messages': [incoming()]}
            with patch('app.services.reach_email.worker.transport.sync_messages', side_effect=[first_batch, second_batch]):
                await worker.sync_account('a')
                await worker.followups()
                assert 'a' not in worker.sync_ok
                async with sessions() as db:
                    assert len((await db.scalars(select(Message))).all()) == 1
                await worker.sync_account('a')
                await worker.followups()
            async with sessions() as db:
                assert (await db.get(Lead, 'l')).stopped_reason == 'replied'
                assert not await db.scalar(select(Message.id).where(Message.kind == 'follow_up'))
    asyncio.run(check())


def test_dsn_requires_original_message_and_failed_recipient():
    async def check():
        async with setup() as (worker, sessions):
            async with sessions() as db, db.begin():
                (await db.get(Message, 'm')).account_id = 'a'
                (await db.get(Lead, 'l')).account_id = 'a'
            item = incoming()
            item.update(from_email='postmaster@example.com', bounce=True, permanent_bounce=True,
                        in_reply_to='', original_message_id='<first@example.com>',
                        failed_recipients=['different@example.com'])
            result = {'uidvalidity': '1', 'last_uid': 1, 'drained': True, 'messages': [item]}
            with patch('app.services.reach_email.worker.transport.sync_messages', return_value=result):
                await worker.sync_account('a')
                async with sessions() as db:
                    assert (await db.get(Lead, 'l')).stopped_reason is None
                item['failed_recipients'] = ['lead@example.com']
                await worker.sync_account('a')
            async with sessions() as db:
                assert (await db.get(Lead, 'l')).stopped_reason == 'permanent_bounce'
    asyncio.run(check())


def test_positive_dsn_is_persisted_without_becoming_a_customer_reply():
    async def check():
        async with setup() as (worker, sessions):
            async with sessions() as db, db.begin():
                message = await db.get(Message, 'm')
                message.account_id, message.attempt_count, message.from_email = 'a', 1, 'sender@example.com'
                (await db.get(Lead, 'l')).account_id = 'a'
            item = incoming()
            item.update(from_email='postmaster@example.com', bounce=True, in_reply_to='',
                permanent_bounce=True, failed_recipients=['other@example.com'],
                original_message_id='<first@example.com>', delivery_reports=[{
                    'recipient': 'lead@example.com', 'action': 'delivered', 'status': '2.0.0'}])
            result = {'uidvalidity': '1', 'last_uid': 1, 'drained': True, 'messages': [item]}
            with patch('app.services.reach_email.worker.transport.sync_messages', return_value=result):
                await worker.sync_account('a')
            async with sessions() as db:
                lead = await db.get(Lead, 'l')
                assert lead.last_reply_at is None and lead.stopped_reason is None
                receipt = await db.scalar(select(Message).where(Message.delivery_report.is_not(None)))
                assert receipt.task_id == 't' and json.loads(receipt.delivery_report)['messageId'] == '<first@example.com>'
                from app.services.reach_email.reporting import summarize
                stats = summarize((await db.scalars(select(Message))).all())
                assert stats['deliveredCount'] == 1 and stats['repliedCount'] == 0
    asyncio.run(check())


def test_mixed_per_message_failures_finish_task_as_completed():
    async def check():
        async with setup() as (worker, sessions):
            async with sessions() as db, db.begin():
                (await db.get(Message, 'm')).status = 'failed'
            await worker.finish_tasks()
            async with sessions() as db:
                task = await db.get(Task, 't')
                assert task.status == 'ended' and task.ended_reason == 'completed'
    asyncio.run(check())


def test_disable_during_attachment_load_prevents_smtp():
    async def check():
        async with setup() as (worker, sessions):
            async def attachments(*args):
                async with sessions() as db, db.begin():
                    account = await db.get(Account, 'a')
                    account.enabled = False
                    account.version += 1
                return []
            worker.load_attachments = attachments
            with patch('app.services.reach_email.worker.transport.send_message') as send:
                await worker.send_one('m')
            send.assert_not_called()
            async with sessions() as db:
                message = await db.get(Message, 'm')
                assert message.status == 'retryable' and message.error_code == 'ACCOUNT_CHANGED'
    asyncio.run(check())


@pytest.mark.parametrize('ids,expected', [
    (['a'] * 6, 'ATTACHMENT_COUNT_LIMIT'),
    (['a', 'a'], 'ATTACHMENT_DUPLICATE'),
    (['missing'], 'ATTACHMENT_INVALID'),
])
def test_attachment_prepare_failure_preserves_specific_safe_error(ids, expected):
    async def check():
        async with setup() as (worker, sessions):
            async with sessions() as db, db.begin():
                (await db.get(Message, 'm')).attachment_ids = json.dumps(ids)
            with patch('app.services.reach_email.worker.transport.send_message') as send:
                await worker.send_one('m')
            send.assert_not_called()
            async with sessions() as db:
                message = await db.get(Message, 'm')
                assert message.status == 'failed' and message.error_code == expected
    asyncio.run(check())


def test_attachment_prepare_failure_never_exposes_storage_error():
    from app.services.reach_email.models import Attachment

    async def check():
        async with setup() as (worker, sessions):
            async with sessions() as db, db.begin():
                db.add(Attachment(id='file', tenant_id='t', owner_id='o', name='file.pdf', size=5,
                                  content_type='application/pdf', object_key='key', sha256='unused'))
                (await db.get(Message, 'm')).attachment_ids = '["file"]'
            worker.store.open.side_effect = ValueError('password=private-storage-secret')
            with patch('app.services.reach_email.worker.transport.send_message') as send:
                await worker.send_one('m')
            send.assert_not_called()
            async with sessions() as db:
                message = await db.get(Message, 'm')
                assert message.status == 'failed' and message.error_code == 'PREPARE_SEND_FAILED'
    asyncio.run(check())


def test_stop_during_attachment_load_releases_unsent_quota():
    async def check():
        async with setup() as (worker, sessions):
            async def attachments(*args):
                async with sessions() as db, db.begin():
                    (await db.get(Task, 't')).status = 'ended'
                return []
            worker.load_attachments = attachments
            with patch('app.services.reach_email.worker.transport.send_message') as send:
                await worker.send_one('m')
            send.assert_not_called()
            async with sessions() as db:
                assert not await db.scalar(select(Attempt.id))
                assert (await db.get(Message, 'm')).attempt_count == 0
                assert (await db.get(Account, 'a')).next_send_at is None
    asyncio.run(check())


def test_claim_locks_task_before_message_and_rechecks_changed_status():
    from sqlalchemy import event

    async def check():
        async with setup() as (worker, sessions):
            engine = sessions.kw['bind']
            locked = []

            def before_execute(conn, statement, *args):
                if getattr(statement, '_for_update_arg', None) is None:
                    return
                table = statement.get_final_froms()[0].name
                locked.append(table)
                if table == Task.__tablename__:
                    conn.exec_driver_sql("UPDATE reach_email_message SET status='cancelled' WHERE id='m'")

            event.listen(engine.sync_engine, 'before_execute', before_execute)
            try:
                assert await worker.claim('m') is None
            finally:
                event.remove(engine.sync_engine, 'before_execute', before_execute)
            assert locked.index(Task.__tablename__) < locked.index(Message.__tablename__)
            async with sessions() as db:
                assert (await db.get(Message, 'm')).status == 'cancelled'
                assert not await db.scalar(select(Attempt.id))
    asyncio.run(check())


def test_finish_refreshes_locked_task_and_preserves_stop():
    from sqlalchemy import event

    async def check():
        async with setup() as (worker, sessions):
            engine = sessions.kw['bind']
            async with sessions() as db, db.begin():
                (await db.get(Message, 'm')).status = 'failed'

            def before_execute(conn, statement, *args):
                if (getattr(statement, '_for_update_arg', None) is not None
                        and statement.get_final_froms()[0].name == Task.__tablename__):
                    conn.exec_driver_sql("UPDATE reach_email_task SET status='ended', ended_reason='stopped' WHERE id='t'")

            event.listen(engine.sync_engine, 'before_execute', before_execute)
            try:
                await worker.finish_tasks()
            finally:
                event.remove(engine.sync_engine, 'before_execute', before_execute)
            async with sessions() as db:
                assert (await db.get(Task, 't')).ended_reason == 'stopped'
    asyncio.run(check())


def test_followup_limit_applies_after_healthy_account_filter():
    async def check():
        async with setup() as (worker, sessions):
            async with sessions() as db, db.begin():
                first, lead = await db.get(Message, 'm'), await db.get(Lead, 'l')
                first.status, first.account_id = 'accepted', 'a'
                lead.account_id, lead.next_follow_up_at = 'a', now() - timedelta(hours=1)
                for index in range(10):
                    db.add(Lead(id=f'bad{index}', tenant_id='t', owner_id='o', task_id='t',
                        email=f'bad{index}@example.com', values='{}', account_id='unhealthy',
                        next_follow_up_at=now() - timedelta(days=2)))
            worker.sync_ok.add('a')
            await worker.followups()
            async with sessions() as db:
                assert await db.scalar(select(Message.id).where(Message.kind == 'follow_up', Message.lead_id == 'l'))
    asyncio.run(check())


def test_permanent_bounce_suppresses_other_tasks_but_not_other_owner():
    from app.services.reach_email.models import Suppression

    async def check():
        async with setup() as (worker, sessions):
            async with sessions() as db, db.begin():
                (await db.get(Message, 'm')).account_id = 'a'
                (await db.get(Lead, 'l')).account_id = 'a'
                for suffix, owner in [('same', 'o'), ('other', 'other-owner')]:
                    db.add(Task(id=suffix, tenant_id='t', owner_id=owner, name=suffix, import_id='i', settings='{}'))
                    db.add(Lead(id=suffix, tenant_id='t', owner_id=owner, task_id=suffix,
                        email='lead@example.com', values='{}', next_follow_up_at=now()))
                    db.add(Message(id=suffix, tenant_id='t', owner_id=owner, task_id=suffix, lead_id=suffix,
                        dedup_key=f'manual:{suffix}', kind='reply', message_id=f'<{suffix}>',
                        to_email='lead@example.com', subject='Hello', html='Hello'))
            item = incoming()
            item.update(from_email='postmaster@example.com', bounce=True, permanent_bounce=True,
                        original_message_id='<first@example.com>', failed_recipients=['lead@example.com'])
            with patch('app.services.reach_email.worker.transport.sync_messages', return_value={
                'uidvalidity': '1', 'last_uid': 1, 'drained': True, 'messages': [item]}):
                await worker.sync_account('a')
                await worker.sync_account('a')
            async with sessions() as db:
                suppression = (await db.scalars(select(Suppression))).all()
                assert len(suppression) == 1 and suppression[0].reason == 'permanent_bounce'
                assert (await db.get(Message, 'same')).status == 'cancelled'
                assert (await db.get(Lead, 'same')).stopped_reason == 'permanent_bounce'
                assert (await db.get(Lead, 'same')).next_follow_up_at is None
                assert (await db.get(Message, 'other')).status == 'queued'
                assert (await db.get(Lead, 'other')).stopped_reason is None
    asyncio.run(check())
