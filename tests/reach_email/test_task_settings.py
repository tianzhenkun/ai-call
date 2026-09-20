"""设置变更使用隔离 SQLite；发送仅模拟 SMTP 返回，不访问真实邮箱。"""

import asyncio
from datetime import timedelta
from unittest.mock import patch

import pytest
from sqlalchemy import select
from test_worker import setup

from app.services.reach_email.models import Attempt, Lead, Message, Task, now
from app.services.reach_email.schema import ContentInput, SettingsInput, TaskSettings
from app.services.reach_email.service import EmailService


async def change_settings(sessions, settings, **changes):
    async with sessions() as db, db.begin():
        task = await db.get(Task, 't')
        return await EmailService(db, 't', 'o', '').update_settings('t', SettingsInput(
            version=task.version, settings=TaskSettings(**{**settings, **changes})))


@pytest.mark.parametrize('status', ['unstarted', 'scheduled', 'running'])
def test_settings_edit_preserves_frozen_content_and_checks_version(status, task_settings):
    async def check():
        async with setup() as (_, sessions):
            async with sessions() as db, db.begin():
                task = await db.get(Task, 't')
                task.status, task.content = status, '{"subject":"原主题","signature":"原签名"}'
            result = await change_settings(sessions, task_settings, dailyLimit=7)
            assert result['settings']['dailyLimit'] == 7 and result['version'] == 2
            assert result['status'] == status and result['content']['signature'] == '原签名'
            async with sessions() as db:
                service = EmailService(db, 't', 'o', '')
                assert (await db.get(Task, 't')).daily_limit == 7
                with pytest.raises(ValueError, match='邮件设置已被更新，请关闭弹窗后重新打开再修改'):
                    await service.update_settings('t', SettingsInput(version=1, settings=TaskSettings(**task_settings)))
                with pytest.raises(ValueError, match='无权访问'):
                    await EmailService(db, 'other', 'o', '').update_settings('t', SettingsInput(version=2, settings=TaskSettings(**task_settings)))
                if status != 'unstarted':
                    with pytest.raises(ValueError, match='不能编辑'):
                        await service.save_content('t', ContentInput(version=2, signature='替换'))
    asyncio.run(check())


def test_ended_settings_are_read_only(task_settings):
    async def check():
        async with setup() as (_, sessions):
            async with sessions() as db, db.begin():
                (await db.get(Task, 't')).status = 'ended'
            with pytest.raises(ValueError, match='已结束.*不能修改'):
                await change_settings(sessions, task_settings)
    asyncio.run(check())


@pytest.mark.parametrize('reason', ['completed', 'stopped', 'failed'])
def test_stop_with_stale_version_returns_existing_ended_result(reason):
    async def check():
        async with setup() as (_, sessions):
            async with sessions() as db, db.begin():
                task = await db.get(Task, 't')
                task.status, task.ended_reason, task.version = 'ended', reason, 9
            async with sessions() as db:
                result = await EmailService(db, 't', 'o', '').transition('t', 1, 'stop')
                assert (result['status'], result['endedReason'], result['version']) == ('ended', reason, 9)
                assert (await db.get(Message, 'm')).status == 'queued'
    asyncio.run(check())


@pytest.mark.parametrize('status,action,error', [
    ('running', 'stop', '任务状态或设置已更新，请刷新后重新停止'),
    ('running', 'withdraw', '任务已开始执行，不能撤回'),
    ('ended', 'withdraw', '任务已结束，不能撤回'),
    ('scheduled', 'withdraw', '任务状态或设置已更新，请刷新后重新撤回'),
])
def test_transition_conflicts_explain_current_state(status, action, error):
    async def check():
        async with setup() as (_, sessions):
            async with sessions() as db, db.begin():
                task = await db.get(Task, 't')
                task.status, task.version = status, 2
            async with sessions() as db:
                with pytest.raises(ValueError, match=error):
                    await EmailService(db, 't', 'o', '').transition('t', 1, action)
    asyncio.run(check())


def test_followup_changes_reuse_cancelled_message_and_ignore_manual_history(task_settings):
    async def check():
        async with setup() as (worker, sessions):
            stamp = now() - timedelta(days=3)
            async with sessions() as db, db.begin():
                first, lead = await db.get(Message, 'm'), await db.get(Lead, 'l')
                first.status, first.sent_at, first.account_id = 'accepted', stamp, 'a'
                lead.account_id, lead.last_sent_at, lead.next_follow_up_at = 'a', now(), stamp
                db.add(Message(id='manual', tenant_id='t', owner_id='o', task_id='t', lead_id='l',
                    account_id='a', dedup_key='manual:l', message_id='<manual>', kind='follow_up',
                    status='accepted', sent_at=now(), to_email=lead.email, subject='人工', html='人工'))
            worker.sync_ok.add('a')
            await worker.followups()
            async with sessions() as db:
                follow = await db.scalar(select(Message).where(Message.dedup_key == 'followup:l:1'))
                identity, message_id = follow.id, follow.message_id
            enabled = {**task_settings, 'followUpEnabled': True, 'followUpCount': 2, 'followUpIntervalDays': 5}
            await change_settings(sessions, enabled)
            async with sessions() as db:
                assert (await db.get(Message, identity)).due_at == stamp + timedelta(days=5)
            await change_settings(sessions, enabled, followUpEnabled=False)
            async with sessions() as db:
                assert (await db.get(Message, identity)).status == 'cancelled'
                assert (await db.get(Lead, 'l')).next_follow_up_at is None
                assert (await db.get(Message, 'manual')).status == 'accepted'
            await change_settings(sessions, enabled, followUpIntervalDays=1)
            await worker.followups()
            async with sessions() as db:
                follow = await db.get(Message, identity)
                assert follow.status == 'queued' and follow.message_id == message_id
                assert follow.due_at == stamp + timedelta(days=1)
                assert len((await db.scalars(select(Message))).all()) == 3
            await change_settings(sessions, enabled, followUpCount=0)
            async with sessions() as db:
                assert (await db.get(Message, identity)).status == 'cancelled'
            await change_settings(sessions, enabled, followUpCount=1, followUpIntervalDays=1)
            with patch('app.services.reach_email.worker.transport.send_message', return_value={'status': 'accepted'}) as send:
                await worker.send_one(identity)
                assert send.call_count == 1
            async with sessions() as db:
                assert (await db.get(Lead, 'l')).follow_up_count == 1
                assert (await db.get(Lead, 'l')).next_follow_up_at is None
                assert (await db.get(Message, 'm')).html == '<p>hello</p>'
    asyncio.run(check())


def test_settings_quota_applies_to_next_claim_without_changing_manual_reply(task_settings):
    async def check():
        async with setup() as (worker, sessions):
            async with sessions() as db, db.begin():
                db.add(Attempt(tenant_id='t', owner_id='o', message_id='prior', account_id='a', task_id='t', status='accepted'))
                db.add(Message(id='manual', tenant_id='t', owner_id='o', task_id='t', lead_id='l',
                    account_id='a', dedup_key='manual:reply', message_id='<manual>', kind='reply',
                    to_email='lead@example.com', subject='人工', html='人工'))
            await change_settings(sessions, task_settings, dailyLimit=1)
            assert await worker.claim('m') is None
            assert await worker.claim('manual') is not None
            async with sessions() as db:
                assert (await db.get(Message, 'm')).attempt_count == 0
    asyncio.run(check())


def test_increasing_daily_limit_wakes_only_messages_waiting_for_task_quota(task_settings):
    async def check():
        async with setup() as (worker, sessions):
            async with sessions() as db, db.begin():
                db.add(Attempt(tenant_id='t', owner_id='o', message_id='prior', account_id='a', task_id='t', status='accepted'))
            await change_settings(sessions, task_settings, dailyLimit=1)
            assert await worker.claim('m') is None
            await change_settings(sessions, task_settings, dailyLimit=2)
            assert await worker.claim('m') is not None
    asyncio.run(check())


def test_followup_reschedule_preserves_retry_backoff(task_settings):
    async def check():
        async with setup() as (worker, sessions):
            stamp = now() - timedelta(days=3)
            async with sessions() as db, db.begin():
                first, lead = await db.get(Message, 'm'), await db.get(Lead, 'l')
                first.status, first.sent_at, first.account_id = 'accepted', stamp, 'a'
                lead.account_id, lead.next_follow_up_at = 'a', stamp
            worker.sync_ok.add('a')
            await worker.followups()
            async with sessions() as db:
                identity = await db.scalar(select(Message.id).where(Message.kind == 'follow_up'))
            with patch('app.services.reach_email.worker.transport.send_message', return_value={'status': 'retryable'}):
                await worker.send_one(identity)
            async with sessions() as db:
                retry_at = (await db.get(Message, identity)).due_at
            await change_settings(sessions, {**task_settings, 'followUpEnabled': False})
            await change_settings(sessions, {**task_settings, 'followUpEnabled': True, 'followUpIntervalDays': 2})
            async with sessions() as db:
                message = await db.get(Message, identity)
                assert message.status == 'retryable' and message.due_at >= retry_at
            assert await worker.claim(identity) is None
    asyncio.run(check())


def test_enabling_followups_paginates_and_preserves_manual_queue(task_settings):
    async def check():
        async with setup() as (_, sessions):
            stamp = now() - timedelta(days=4)
            async with sessions() as db, db.begin():
                for index in range(201):
                    key = f'lead-{index:03}'
                    db.add(Lead(id=key, tenant_id='t', owner_id='o', task_id='t',
                        email=f'{key}@example.com', values='{}', first_message_id=key, account_id='a',
                        stopped_reason='replied' if index == 0 else None))
                    db.add(Message(id=key, tenant_id='t', owner_id='o', task_id='t', lead_id=key,
                        dedup_key=f'initial:{key}', message_id=f'<{key}>', to_email=f'{key}@example.com',
                        subject='首封', html='首封', status='accepted', sent_at=stamp))
                db.add(Message(id='manual', tenant_id='t', owner_id='o', task_id='t', lead_id='lead-200',
                    dedup_key='manual:queued', message_id='<manual>', kind='follow_up',
                    to_email='lead-200@example.com', subject='人工', html='人工', due_at=stamp))
            await change_settings(sessions, task_settings, followUpEnabled=True, followUpIntervalDays=2)
            async with sessions() as db:
                leads = (await db.scalars(select(Lead).where(Lead.id.like('lead-%')))).all()
                assert len(leads) == 201
                assert all(lead.next_follow_up_at == (None if lead.id == 'lead-000' else stamp + timedelta(days=2)) for lead in leads)
                manual = await db.get(Message, 'manual')
                assert manual.status == 'queued' and manual.due_at == stamp
    asyncio.run(check())


@pytest.mark.parametrize('status,error,attempts', [
    ('failed', 'RETRY_LIMIT_REACHED', 3), ('cancelled', 'CONTACT_REPLIED', 0),
    ('unknown', 'SEND_INTERRUPTED', 1), ('sending', None, 1),
])
def test_settings_never_revive_terminal_or_uncertain_followups(status, error, attempts, task_settings):
    async def check():
        async with setup() as (_, sessions):
            stamp = now() - timedelta(days=4)
            async with sessions() as db, db.begin():
                first = await db.get(Message, 'm')
                first.status, first.sent_at = 'accepted', stamp
                db.add(Message(id='follow', tenant_id='t', owner_id='o', task_id='t', lead_id='l',
                    dedup_key='followup:l:1', message_id='<follow>', kind='follow_up',
                    to_email='lead@example.com', subject='跟进', html='跟进', status=status,
                    error_code=error, attempt_count=attempts, due_at=stamp))
            await change_settings(sessions, task_settings, followUpEnabled=False)
            await change_settings(sessions, task_settings, followUpEnabled=True)
            async with sessions() as db:
                message = await db.get(Message, 'follow')
                assert (message.status, message.error_code, message.attempt_count, message.due_at) == (status, error, attempts, stamp)
                assert (await db.get(Lead, 'l')).next_follow_up_at is None
    asyncio.run(check())


@pytest.mark.parametrize('outcome,change,expected', [
    ('accepted', {'followUpEnabled': False}, 'accepted'),
    ('retryable', {'followUpEnabled': False}, 'cancelled'),
    ('retryable', {'followUpCount': 0}, 'cancelled'),
    ('retryable', {'followUpIntervalDays': 7}, 'retryable'),
])
def test_inflight_followup_result_obeys_latest_settings(outcome, change, expected, task_settings):
    async def check():
        async with setup() as (worker, sessions):
            stamp = now() - timedelta(days=3)
            async with sessions() as db, db.begin():
                first, lead = await db.get(Message, 'm'), await db.get(Lead, 'l')
                first.status, first.sent_at, first.account_id = 'accepted', stamp, 'a'
                lead.account_id, lead.next_follow_up_at = 'a', stamp
            worker.sync_ok.add('a')
            await worker.followups()
            async with sessions() as db:
                identity = await db.scalar(select(Message.id).where(Message.kind == 'follow_up'))

            async def send(*args, **kwargs):
                await change_settings(sessions, {**task_settings, 'followUpEnabled': True,
                    'followUpCount': 2, 'followUpIntervalDays': 1}, **change)
                async with sessions() as db:
                    assert (await db.get(Message, identity)).status == 'sending'
                return {'status': outcome}
            with patch('app.services.reach_email.worker.asyncio.to_thread', side_effect=send):
                await worker.send_one(identity)
            async with sessions() as db:
                message = await db.get(Message, identity)
                assert message.status == expected
                if 'followUpIntervalDays' in change:
                    assert message.due_at == stamp + timedelta(days=7)
                assert (await db.get(Lead, 'l')).next_follow_up_at is None
    asyncio.run(check())


@pytest.mark.parametrize('outcome', ['accepted', 'retryable'])
def test_settings_change_during_smtp_preserves_inflight_and_uses_new_followup_rules(outcome, task_settings):
    async def check():
        async with setup() as (worker, sessions):
            async def send(*args, **kwargs):
                await change_settings(sessions, task_settings, followUpEnabled=False)
                async with sessions() as db:
                    assert (await db.get(Message, 'm')).status == 'sending'
                return {'status': outcome}
            with patch('app.services.reach_email.worker.asyncio.to_thread', side_effect=send):
                await worker.send_one('m')
            async with sessions() as db:
                assert (await db.get(Message, 'm')).status == outcome
                assert (await db.get(Lead, 'l')).next_follow_up_at is None
    asyncio.run(check())
