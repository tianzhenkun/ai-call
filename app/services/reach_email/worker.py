"""Independent, single-consumer email worker: python -m app.services.reach_email.worker."""
from __future__ import annotations

import asyncio
import hashlib
import html
import json
import logging
import re
from datetime import timedelta

from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.services.reach_email import transport
from app.services.reach_email.ai import EmailAI
from app.services.reach_email.models import (
    Account,
    AIJob,
    Attachment,
    Attempt,
    Lead,
    Message,
    Suppression,
    Task,
    WorkerLease,
    new_id,
    now,
)
from app.services.reach_email.security import CredentialCipher, configured_secret

log = logging.getLogger(__name__)
LEASE_SECONDS = 120
ACTIVE = ('queued', 'retryable', 'sending', 'unknown')


def scope(model, row):
    return (model.tenant_id == row.tenant_id, model.owner_id == row.owner_id)


def owned(model, row, identity):
    return select(model).where(model.id == identity, *scope(model, row))


class EmailWorker:
    def __init__(self, sessions, cipher, store, ai, *, batch_size=10):
        self.sessions, self.cipher, self.store, self.ai = sessions, cipher, store, ai
        self.token = new_id()
        self.batch_size = max(1, min(batch_size, 50))
        self.sync_ok = set()
        self.account_cursor = ''

    async def acquire(self):
        stamp = now()
        async with self.sessions() as db:
            result = await db.execute(update(WorkerLease).where(
                WorkerLease.id == 'email', or_(WorkerLease.expires_at < stamp,
                                               WorkerLease.token == self.token)
            ).values(token=self.token, expires_at=stamp + timedelta(seconds=LEASE_SECONDS)))
            if result.rowcount:
                await db.commit()
                return True
            if await db.get(WorkerLease, 'email'):
                return False
            db.add(WorkerLease(id='email', token=self.token,
                               expires_at=stamp + timedelta(seconds=LEASE_SECONDS)))
            try:
                await db.commit()
                return True
            except IntegrityError:
                await db.rollback()
                return False

    async def require_lease(self, db):
        lease = await db.scalar(select(WorkerLease).where(WorkerLease.id == 'email').with_for_update())
        if not lease or lease.token != self.token or lease.expires_at <= now():
            raise RuntimeError('EMAIL_WORKER_LEASE_LOST')

    async def heartbeat(self):
        while True:
            await asyncio.sleep(15)
            try:
                async with self.sessions() as db:
                    stamp = now()
                    result = await db.execute(update(WorkerLease).where(
                        WorkerLease.id == 'email', WorkerLease.token == self.token,
                        WorkerLease.expires_at > stamp
                    ).values(expires_at=stamp + timedelta(seconds=LEASE_SECONDS)))
                    await db.commit()
                    if not result.rowcount:
                        raise RuntimeError('EMAIL_WORKER_LEASE_LOST')
            except DBAPIError:
                log.warning('EMAIL_WORKER_HEARTBEAT_RETRY')

    def config(self, account):
        return {**json.loads(account.config), **self.cipher.decrypt(account.secret), 'email': account.email}

    async def recover_and_schedule(self):
        async with self.sessions() as db, db.begin():
            await self.require_lease(db)
            stamp = now()
            expired = list((await db.scalars(select(Message).where(
                Message.status == 'sending', Message.lease_until < stamp))).all())
            for message in expired:
                message.status, message.error_code = 'unknown', 'WORKER_INTERRUPTED'
                await db.execute(update(Attempt).where(Attempt.message_id == message.id,
                    *scope(Attempt, message), Attempt.status == 'sending').values(status='unknown'))
            await db.execute(update(AIJob).where(AIJob.status == 'running', AIJob.lease_until < stamp)
                             .values(status='failed', error_code='WORKER_INTERRUPTED'))
            await db.execute(update(Task).where(Task.status == 'scheduled', Task.scheduled_at <= stamp)
                             .values(status='running', version=Task.version + 1))

    def attempt_query(self, row, since, **filters):
        conditions = [*scope(Attempt, row), Attempt.created_at >= since]
        conditions.extend(getattr(Attempt, key) == value for key, value in filters.items())
        if 'task_id' in filters:
            # 人工回复只占用邮箱额度；任务计数与额度释放时间使用相同口径。
            replies = select(Message.id).where(*scope(Message, row),
                Message.kind == 'reply', Message.dedup_key.like('manual:%'))
            conditions.append(Attempt.message_id.not_in(replies))
        return select(Attempt).where(*conditions)

    async def count_attempts(self, db, row, since, **filters):
        query = self.attempt_query(row, since, **filters)
        return await db.scalar(query.with_only_columns(func.count(), maintain_column_froms=True))

    async def quota_release(self, db, row, hours, **filters):
        query = self.attempt_query(row, now() - timedelta(hours=hours), **filters)
        first = await db.scalar(query.with_only_columns(func.min(Attempt.created_at)))
        return (first or now()) + timedelta(hours=hours, seconds=1)

    async def choose_account(self, db, message, lead):
        stamp = now()
        query = select(Account).where(*scope(Account, message), Account.enabled.is_(True),
            Account.smtp_status == 'ok', Account.imap_status == 'ok').order_by(Account.id).with_for_update()
        fixed = message.account_id or (lead.account_id if lead else None)
        if fixed:
            query = query.where(Account.id == fixed)
        accounts = (await db.scalars(query)).all()
        if not accounts:
            message.due_at = stamp + timedelta(seconds=60)
            return None
        if fixed:
            selected = accounts[0]
        else:
            # 先固定分配结果，再等待间隔和额度；重试、跟进不能重复参与权重轮转。
            for account in accounts:
                account.current_weight += account.weight
            selected = max(accounts, key=lambda a: (a.current_weight, a.id))
            selected.current_weight -= sum(a.weight for a in accounts)
            message.account_id = selected.id
            message.from_email = selected.email
            if lead:
                lead.account_id = selected.id
        available_at = max(stamp, selected.next_send_at or stamp)
        if await self.count_attempts(db, selected, stamp - timedelta(hours=1), account_id=selected.id) >= selected.hourly_limit:
            available_at = max(available_at, await self.quota_release(db, selected, 1, account_id=selected.id))
        if await self.count_attempts(db, selected, stamp - timedelta(hours=24), account_id=selected.id) >= selected.daily_limit:
            available_at = max(available_at, await self.quota_release(db, selected, 24, account_id=selected.id))
        if available_at > stamp:
            message.due_at = available_at
            return None
        return selected

    async def load_attachments(self, db, message):
        result = []
        identities = json.loads(message.attachment_ids)
        if len(identities) > 5:
            raise ValueError('ATTACHMENT_LIMIT')
        for identity in identities:
            attachment = await db.scalar(owned(Attachment, message, identity))
            if not attachment or attachment.size > 8 * 1024 * 1024:
                raise ValueError('ATTACHMENT_INVALID')
            source = await self.store.open(attachment.object_key)
            data = bytearray()
            async for chunk in source.body:
                data.extend(chunk)
                if len(data) > attachment.size:
                    raise ValueError('ATTACHMENT_CHANGED')
            if len(data) != attachment.size or hashlib.sha256(data).hexdigest() != attachment.sha256:
                raise ValueError('ATTACHMENT_CHANGED')
            result.append({'filename': attachment.name, 'content': bytes(data),
                           'content_type': attachment.content_type})
        if sum(len(item['content']) for item in result) > 8 * 1024 * 1024:
            raise ValueError('ATTACHMENT_LIMIT')
        return result

    async def claim(self, identity):
        async with self.sessions() as db, db.begin():
            await self.require_lease(db)
            peek = await db.get(Message, identity)
            if peek is None:
                return None
            task_id = peek.task_id
            task = await db.scalar(owned(Task, peek, task_id).with_for_update()
                                   .execution_options(populate_existing=True))
            message = await db.scalar(owned(Message, peek, identity).with_for_update()
                                      .execution_options(populate_existing=True))
            if (not message or message.task_id != task_id
                    or message.status not in ('queued', 'retryable') or message.due_at > now()):
                return None
            lead = await db.scalar(owned(Lead, message, message.lead_id))
            if not task or not lead:
                message.status, message.error_code = 'failed', 'CONVERSATION_MISSING'
                return None
            suppressed = await db.scalar(select(Suppression.id).where(
                *scope(Suppression, message), Suppression.email == message.to_email))
            if suppressed or (not message.dedup_key.startswith('manual:') and (lead.stopped_reason or task.status == 'ended')):
                message.status, message.error_code = 'cancelled', 'CONTACT_STOPPED'
                return None
            if not message.dedup_key.startswith('manual:') and task.status != 'running':
                return None
            if message.kind == 'follow_up' and not message.dedup_key.startswith('manual:') and lead.account_id not in self.sync_ok:
                message.due_at = now() + timedelta(seconds=30)
                return None
            day = now() - timedelta(hours=24)
            manual_reply = message.kind == 'reply' and message.dedup_key.startswith('manual:')
            if not manual_reply and await self.count_attempts(db, message, day, task_id=task.id) >= task.daily_limit:
                message.due_at = await self.quota_release(db, message, 24, task_id=task.id)
                return None
            account = await self.choose_account(db, message, lead)
            if account is None:
                return None
            try:
                config = self.config(account)
            except Exception:
                message.status, message.error_code = 'failed', 'ACCOUNT_CREDENTIALS_INVALID'
                return None
            message.account_id = account.id
            lead.account_id = account.id
            message.from_email = account.email
            message.status = 'sending'
            message.lease_token = self.token
            message.lease_until = now() + timedelta(seconds=LEASE_SECONDS)
            message.attempt_count += 1
            account.next_send_at = now() + timedelta(seconds=account.interval_seconds)
            attempt = Attempt(id=new_id(), tenant_id=message.tenant_id, owner_id=message.owner_id,
                              message_id=message.id, account_id=account.id, task_id=task.id)
            db.add(attempt)
            await db.flush()
            return message, attempt.id, config, account.version

    async def send_one(self, identity):
        claimed = await self.claim(identity)
        if not claimed:
            return
        message, attempt_id, config, account_version = claimed
        submitted = False
        try:
            async with self.sessions() as db:
                attachments = await self.load_attachments(db, message)
                await self.require_lease(db)
                task = await db.scalar(owned(Task, message, message.task_id))
                lead = await db.scalar(owned(Lead, message, message.lead_id))
                account = await db.scalar(owned(Account, message, message.account_id))
                blocked = await db.scalar(select(Suppression.id).where(
                    *scope(Suppression, message), Suppression.email == message.to_email))
                if (not account or not account.enabled or account.version != account_version
                        or account.smtp_status != 'ok' or account.imap_status != 'ok'):
                    outcome = {'status': 'retryable', 'errorCode': 'ACCOUNT_CHANGED'}
                elif blocked or (not message.dedup_key.startswith('manual:') and (task.status != 'running' or lead.stopped_reason)):
                    outcome = {'status': 'cancelled', 'errorCode': 'CONTACT_STOPPED'}
                else:
                    outcome = None
            if outcome is None:
                submitted = True
                outcome = await asyncio.to_thread(transport.send_message, config,
                    recipient=message.to_email, subject=message.subject, html=message.html,
                    attachments=attachments, message_id=message.message_id,
                    in_reply_to=message.in_reply_to, references=message.references)
        except Exception:
            outcome = {'status': 'unknown' if submitted else 'failed',
                       'errorCode': 'SEND_INTERRUPTED' if submitted else 'PREPARE_SEND_FAILED'}
        async with self.sessions() as db, db.begin():
            await self.require_lease(db)
            current = await db.scalar(owned(Message, message, message.id).with_for_update())
            if current.status != 'sending' or current.lease_token != self.token:
                return
            current.status, current.error_code = outcome['status'], outcome.get('errorCode')
            current.lease_until = None
            attempt = await db.scalar(owned(Attempt, message, attempt_id))
            if submitted:
                attempt.status = current.status
            else:
                # No SMTP call occurred: release the reservation, never an unknown submission.
                await db.delete(attempt)
                current.attempt_count -= 1
                reserved_account = await db.scalar(owned(Account, message, message.account_id))
                if reserved_account:
                    reserved_account.next_send_at = None
            lead = await db.scalar(owned(Lead, message, message.lead_id))
            task = await db.scalar(owned(Task, message, message.task_id))
            if current.status == 'accepted':
                current.sent_at = lead.last_sent_at = now()
                if current.kind == 'follow_up' and not current.dedup_key.startswith('manual:'):
                    lead.follow_up_count += 1
                if not current.dedup_key.startswith('manual:'):
                    settings = json.loads(task.settings)
                    if (settings.get('followUpEnabled') and not lead.stopped_reason
                            and task.status == 'running'
                            and lead.follow_up_count < settings.get('followUpCount', 0)):
                        lead.next_follow_up_at = now() + timedelta(days=settings.get('followUpIntervalDays', 2))
                    else:
                        lead.next_follow_up_at = None
            elif current.status == 'retryable':
                if current.attempt_count >= 3:
                    current.status = 'failed'
                    current.error_code = 'RETRY_LIMIT_REACHED'
                else:
                    current.due_at = now() + timedelta(minutes=2 ** current.attempt_count)
            if current.status in ('failed', 'cancelled') and not current.dedup_key.startswith('manual:'):
                lead.next_follow_up_at = None

    async def followups(self):
        if not self.sync_ok:
            return
        async with self.sessions() as db, db.begin():
            await self.require_lease(db)
            leads = (await db.scalars(select(Lead).where(Lead.next_follow_up_at <= now(),
                Lead.stopped_reason.is_(None), Lead.account_id.in_(self.sync_ok))
                .order_by(Lead.next_follow_up_at).limit(self.batch_size))).all()
            for lead in leads:
                task = await db.scalar(owned(Task, lead, lead.task_id))
                first = await db.scalar(owned(Message, lead, lead.first_message_id))
                if not task or task.status != 'running' or not first or first.status != 'accepted':
                    continue
                settings = json.loads(task.settings)
                if not settings.get('followUpEnabled') or lead.follow_up_count >= settings.get('followUpCount', 0):
                    lead.next_follow_up_at = None
                    continue
                key = f'followup:{lead.id}:{lead.follow_up_count + 1}'
                if await db.scalar(select(Message.id).where(*scope(Message, lead), Message.dedup_key == key)):
                    continue
                db.add(Message(id=new_id(), tenant_id=lead.tenant_id, owner_id=lead.owner_id,
                    task_id=task.id, lead_id=lead.id, account_id=lead.account_id,
                    dedup_key=key, message_id=f'<{new_id()}@reach.local>', kind='follow_up',
                    in_reply_to=first.message_id, references=first.message_id,
                    from_email=first.from_email, to_email=lead.email, subject=first.subject,
                    html=first.html, attachment_ids=first.attachment_ids))
                lead.next_follow_up_at = None

    async def finish_tasks(self):
        async with self.sessions() as db, db.begin():
            await self.require_lease(db)
            candidates = (await db.scalars(select(Task).where(Task.status == 'running'))).all()
            for candidate in candidates:
                task = await db.scalar(owned(Task, candidate, candidate.id).with_for_update()
                                       .execution_options(populate_existing=True))
                if task is None or task.status != 'running':
                    continue
                active = await db.scalar(select(Message.id).where(*scope(Message, task),
                    Message.task_id == task.id, ~Message.dedup_key.like('manual:%'), Message.status.in_(ACTIVE)).limit(1))
                future = await db.scalar(select(Lead.id).where(*scope(Lead, task), Lead.task_id == task.id,
                    Lead.next_follow_up_at.is_not(None), Lead.stopped_reason.is_(None)).limit(1))
                if active or future:
                    continue
                task.status, task.ended_reason = 'ended', 'completed'
                task.version += 1

    async def ai_jobs(self):
        async with self.sessions() as db:
            identities = (await db.scalars(select(AIJob.id).where(AIJob.status == 'queued')
                .order_by(AIJob.created_at).limit(self.batch_size))).all()
        for identity in identities:
            async with self.sessions() as db, db.begin():
                await self.require_lease(db)
                job = await db.get(AIJob, identity)
                if job.status != 'queued':
                    continue
                job.status, job.lease_until = 'running', now() + timedelta(seconds=LEASE_SECONDS)
                payload = json.loads(job.payload)
                tenant_id, owner_id = job.tenant_id, job.owner_id
            try:
                result = await self.ai.run(**payload)
                error = None
            except Exception as exc:
                result = None
                error = str(exc) if re.fullmatch(r'EMAIL_AI_[A-Z_]+', str(exc)) else 'EMAIL_AI_FAILED'
            async with self.sessions() as db, db.begin():
                await self.require_lease(db)
                job = await db.scalar(select(AIJob).where(AIJob.id == identity,
                    AIJob.tenant_id == tenant_id, AIJob.owner_id == owner_id))
                if job.status != 'running':
                    continue
                task = await db.scalar(owned(Task, job, job.task_id))
                if not task or task.version != job.source_version:
                    job.status, job.error_code = 'stale', 'SOURCE_VERSION_CHANGED'
                else:
                    job.status = 'failed' if error else 'ready'
                    job.error_code = error
                    job.result = json.dumps(result, ensure_ascii=False) if result is not None else None
                job.lease_until = None

    async def sync_account(self, account_id, *, require_worker_lease=True):
        async with self.sessions() as db:
            account = await db.get(Account, account_id)
            if not account or not account.enabled:
                raise ValueError('邮箱不存在或未启用')
            config = self.config(account)
            version = account.version
        result = await asyncio.to_thread(transport.sync_messages, config,
            uidvalidity=account.uidvalidity, last_uid=account.last_uid, limit=self.batch_size)
        async with self.sessions() as db, db.begin():
            if require_worker_lease:
                await self.require_lease(db)
            current = await db.scalar(owned(Account, account, account.id).with_for_update())
            if not current or not current.enabled or current.version != version:
                raise ValueError('邮箱配置已变更，请重新同步')
            # 手动同步与后台同步共享游标比较，防止较早的网络结果覆盖新游标。
            if (current.uidvalidity, current.last_uid) != (account.uidvalidity, account.last_uid):
                raise ValueError('该邮箱已完成另一轮同步，请刷新后重试')
            processed = 0
            for incoming in result['messages']:
                attachments = incoming.get('attachments', [])
                if len(attachments) > 5 or sum(len(a['content']) for a in attachments) > 8 * 1024 * 1024:
                    raise ValueError('INBOUND_ATTACHMENT_LIMIT')
                fingerprint = hashlib.sha256(json.dumps({k: v for k, v in incoming.items()
                    if k not in ('uid', 'attachments')}, sort_keys=True).encode())
                for item in attachments:
                    fingerprint.update(item['content'])
                key = f'inbound:{account.id}:{fingerprint.hexdigest()}'
                if await db.scalar(select(Message.id).where(*scope(Message, account), Message.dedup_key == key)):
                    continue
                references = re.findall(r'<[^<>\s]+>', incoming.get('in_reply_to', '') + ' ' + incoming.get('references', ''))
                if incoming.get('bounce'):
                    references += re.findall(r'<[^<>\s]+>', incoming.get('original_message_id', ''))
                source = None
                marker_fallback = False
                if not incoming.get('in_reply_to') and not incoming.get('references') and not incoming.get('bounce'):
                    tokens = set(re.findall(r'\[REACH:([0-9a-f]{32})\]', incoming.get('subject', '')))
                    recipients = {address.lower() for address in incoming.get('to_emails', [])}
                    if len(tokens) == 1 and account.email.lower() in recipients:
                        references = [f'<{next(iter(tokens))}@reach.local>']
                        marker_fallback = True
                if references:
                    sources = (await db.scalars(select(Message).where(*scope(Message, account),
                        Message.account_id == account.id, Message.direction == 'outbound',
                        Message.message_id.in_(references)))).all()
                    matched = []
                    for candidate in sources:
                        if marker_fallback and candidate.status != 'accepted':
                            continue
                        lead = await db.scalar(owned(Lead, account, candidate.lead_id))
                        direct_reply = lead and lead.email.lower() == incoming['from_email'].lower()
                        dsn_reply = (lead and incoming.get('bounce')
                            and candidate.message_id == incoming.get('original_message_id')
                            and lead.email.lower() in (incoming.get('failed_recipients', []) +
                                [report['recipient'] for report in incoming.get('delivery_reports', [])]))
                        if lead and lead.task_id == candidate.task_id and lead.account_id == account.id and (direct_reply or dsn_reply):
                            matched.append((candidate, lead))
                    if len({lead.id for _, lead in matched}) == 1:
                        source, lead = matched[0]
                attachment_ids = []
                from app.services.reach_email.service import EmailService

                attachment_service = EmailService(db, account.tenant_id, account.owner_id, '', self.store)
                for item in attachments:
                    saved = await attachment_service.save_attachment(
                        item['filename'], item['content'], item['content_type'])
                    attachment_ids.append(saved['id'])
                db.add(Message(id=new_id(), tenant_id=account.tenant_id, owner_id=account.owner_id,
                    task_id=source.task_id if source else None, lead_id=source.lead_id if source else None,
                    account_id=account.id, direction='inbound', kind=('bounce' if incoming.get('permanent_bounce') else 'receipt') if incoming.get('bounce') else ('auto_reply' if incoming.get('auto_reply') else 'reply'),
                    delivery_report=json.dumps({'messageId': incoming.get('original_message_id', ''),
                        'recipients': incoming.get('delivery_reports', [])}) if incoming.get('bounce') else None,
                    status='received', dedup_key=key, message_id=incoming.get('message_id', '')[:512],
                    in_reply_to=incoming.get('in_reply_to', '')[:512], references=incoming.get('references', ''),
                    from_email=incoming['from_email'][:254], to_email=account.email,
                    subject=incoming.get('subject', '')[:512],
                    html=incoming.get('html') or '<p>' + html.escape(incoming.get('text', '')).replace('\n', '<br>') + '</p>',
                    attachment_ids=json.dumps(attachment_ids)))
                processed += 1
                permanent_bounce = source and incoming.get('permanent_bounce') and source.to_email.lower() in incoming.get('failed_recipients', [])
                if source and (permanent_bounce or
                               not incoming.get('bounce') and not incoming.get('auto_reply')):
                    if permanent_bounce:
                        suppression = await db.scalar(select(Suppression).where(
                            *scope(Suppression, account), Suppression.email == lead.email))
                        if suppression is None:
                            db.add(Suppression(id=new_id(), tenant_id=account.tenant_id,
                                owner_id=account.owner_id, email=lead.email, reason='permanent_bounce'))
                        # Keep the same Message -> Lead write order as API suppression.
                        await db.execute(update(Message).where(*scope(Message, account),
                            Message.to_email == lead.email, Message.direction == 'outbound',
                            Message.status.in_(('queued', 'retryable')))
                            .values(status='cancelled', error_code='PERMANENT_BOUNCE'))
                        await db.execute(update(Lead).where(*scope(Lead, account), Lead.email == lead.email)
                            .values(next_follow_up_at=None, stopped_reason='permanent_bounce'))
                    else:
                        await db.execute(update(Message).where(*scope(Message, account),
                            Message.lead_id == lead.id, Message.direction == 'outbound',
                            ~Message.dedup_key.like('manual:%'), Message.status.in_(('queued', 'retryable')))
                            .values(status='cancelled', error_code='CONTACT_REPLIED'))
                        lead.stopped_reason = 'replied'
                        lead.next_follow_up_at = None
                        lead.last_reply_at = now()
            current.uidvalidity, current.last_uid, current.last_sync_at = result['uidvalidity'], result['last_uid'], now()
        if result.get('drained') is True:
            self.sync_ok.add(account.id)
        return {'processedCount': processed, 'hasMore': not result.get('drained', False)}

    async def run_once(self):
        if not await self.acquire():
            return False
        await self.recover_and_schedule()
        self.sync_ok.clear()
        async with self.sessions() as db:
            accounts = (await db.scalars(select(Account.id).where(Account.enabled.is_(True),
                Account.imap_status == 'ok', Account.id > self.account_cursor).order_by(Account.id)
                .limit(self.batch_size))).all()
        self.account_cursor = accounts[-1] if accounts else ''
        for identity in accounts:
            try:
                await self.sync_account(identity)
            except Exception:
                log.warning('EMAIL_SYNC_FAILED account=%s', identity)
        await self.followups()
        async with self.sessions() as db:
            identities = (await db.scalars(select(Message.id).where(Message.direction == 'outbound',
                Message.status.in_(('queued', 'retryable')), Message.due_at <= now(),
                # 未轮到该账号同步的跟进留在队列，避免反复改期错过可发送轮次。
                or_(Message.kind != 'follow_up', Message.dedup_key.like('manual:%'),
                    Message.account_id.in_(self.sync_ok)))
                .order_by(Message.due_at, Message.id).limit(self.batch_size))).all()
        for identity in identities:
            await self.send_one(identity)
        await self.ai_jobs()
        await self.finish_tasks()
        return True


async def main():
    from app.api.v1.system.oss.service import OssService
    from app.config.setting import settings
    from app.services.ai_call.knowledge import build_knowledge_store

    cipher = CredentialCipher(configured_secret(settings.REACH_EMAIL_ENCRYPTION_KEY,
        settings.REACH_EMAIL_ENCRYPTION_KEY_FILE, 'REACH_EMAIL_ENCRYPTION_KEY'))
    ai = EmailAI(settings.REACH_EMAIL_LLM_BASE_URL, settings.REACH_EMAIL_LLM_MODEL,
        configured_secret(settings.REACH_EMAIL_LLM_API_KEY,
                          settings.REACH_EMAIL_LLM_API_KEY_FILE, 'REACH_EMAIL_LLM_API_KEY'))
    # 独立进程不经过 API lifespan，必须先加载同库的 OSS 配置。
    await OssService.init_active_config()
    store = build_knowledge_store(settings)
    options = {} if settings.DATABASE_TYPE == 'sqlite' else {'pool_size': 2, 'max_overflow': 0}
    engine = create_async_engine(settings.ASYNC_DB_URI, pool_pre_ping=True, **options)
    worker = EmailWorker(async_sessionmaker(engine, expire_on_commit=False), cipher, store, ai)
    heartbeat = None
    try:
        if not await worker.acquire():
            raise RuntimeError('EMAIL_WORKER_ALREADY_RUNNING')
        heartbeat = asyncio.create_task(worker.heartbeat())
        while True:
            if heartbeat.done():
                await heartbeat
            try:
                await worker.run_once()
            except DBAPIError:
                # Transactions roll back; interrupted submissions recover as unknown.
                log.warning('EMAIL_WORKER_DATABASE_RETRY')
            await asyncio.sleep(5)
    finally:
        if heartbeat:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
        await engine.dispose()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
