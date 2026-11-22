"""Independent, single-consumer email worker: python -m app.services.reach_email.worker."""
from __future__ import annotations

import asyncio
import hashlib
import html
import json
import logging
import re
import traceback
from datetime import timedelta
from time import monotonic, perf_counter

from sqlalchemy import func, or_, select, text, update
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.config.setting import settings as app_settings
from app.services.ai_call.credit_metering import (
    CreditAdmissionDenied,
    CreditMeteringClient,
    CreditMeteringRejected,
    CreditMeteringUnavailable,
)
from app.services.reach_email import transport
from app.services.reach_email.ai import EmailAI
from app.services.reach_email.limits import (
    ATTACHMENT_ERROR_CODES,
    MAX_ATTACHMENT_BYTES,
    MAX_ATTACHMENT_COUNT,
    MAX_INBOUND_ATTACHMENT_BYTES,
)
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
from app.services.reach_email.outlook import OutlookError, connection_config
from app.services.reach_email.reporting import canonical_reply_reference
from app.services.reach_email.scheduling import (
    TASK_WAIT_REASONS,
    WAIT_LABELS,
    account_send_time,
    attempt_query,
    followup_send_time,
    task_send_time,
)
from app.services.reach_email.security import CredentialCipher, configured_secret
from app.services.reach_email.send_limits import LIMIT_PAUSED, clear_confirmed_limits, record_limit

log = logging.getLogger(__name__)
LEASE_SECONDS = 120
ACTIVE = ('queued', 'retryable', 'sending', 'unknown')


def scope(model, row):
    return (model.tenant_id == row.tenant_id, model.owner_id == row.owner_id)


def owned(model, row, identity):
    return select(model).where(model.id == identity, *scope(model, row))


def same_sender_accounts(account):
    return select(Account.id).where(*scope(Account, account),
        or_(Account.email == account.email, Account.id == account.id))


async def match_reply(db, account, incoming):
    """同步和历史补关联使用同一套编号、参与人及归属校验。"""
    references = re.findall(r'<[^<>\s]+>', incoming.get('in_reply_to', '') + ' ' + incoming.get('references', ''))
    if incoming.get('bounce'):
        references += re.findall(r'<[^<>\s]+>', incoming.get('original_message_id', ''))
    marker_fallback = False
    if not incoming.get('bounce'):
        recipients = {address.lower() for address in incoming.get('to_emails', [])}
        marker = canonical_reply_reference(incoming.get('subject', ''), references,
            headerless=not incoming.get('in_reply_to') and not incoming.get('references'))
        if marker:
            references.append(marker)
            marker_fallback = True
    if references:
        sources = (await db.scalars(select(Message).where(*scope(Message, account),
            Message.account_id.in_(same_sender_accounts(account)), Message.direction == 'outbound',
            or_(Message.message_id.in_(references), Message.provider_message_id.in_(references))))).all()
        matched = []
        for candidate in sources:
            # 跨服务回复必须仍指向原发件地址，不能仅凭同一个 Message-ID 关联。
            if candidate.account_id != account.id and (
                candidate.from_email.lower() != account.email.lower()
                or account.email.lower() not in {email.lower() for email in incoming.get('to_emails', [])}
            ):
                continue
            if marker_fallback and (candidate.status != 'accepted' or not recipients.intersection(
                    {account.email.lower(), candidate.from_email.lower()})):
                continue
            lead = await db.scalar(owned(Lead, account, candidate.lead_id))
            direct_reply = lead and lead.email.lower() == incoming['from_email'].lower()
            dsn_reply = (lead and incoming.get('bounce')
                and incoming.get('original_message_id') in (candidate.message_id, candidate.provider_message_id)
                and lead.email.lower() in (incoming.get('failed_recipients', []) +
                    [report['recipient'] for report in incoming.get('delivery_reports', [])]))
            if lead and lead.task_id == candidate.task_id and lead.account_id == candidate.account_id and (direct_reply or dsn_reply):
                matched.append((candidate, lead))
        if len({lead.id for _, lead in matched}) == 1:
            return matched[0]
    return None, None


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
        config = {**json.loads(account.config), 'email': account.email}
        if config.get('provider_type') == 'outlook_personal':
            return config
        return {**config, **self.cipher.decrypt(account.secret)}

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
        return attempt_query(row, since, **filters)

    async def count_attempts(self, db, row, since, **filters):
        query = self.attempt_query(row, since, **filters)
        return await db.scalar(query.with_only_columns(func.count(), maintain_column_froms=True))

    async def quota_release(self, db, row, hours, **filters):
        query = self.attempt_query(row, now() - timedelta(hours=hours), **filters)
        first = await db.scalar(query.with_only_columns(func.min(Attempt.created_at)))
        return (first or now()) + timedelta(hours=hours, seconds=1)

    async def choose_account(self, db, message, lead, task):
        stamp = now()
        pool = json.loads(task.settings).get('senderAccounts')
        query = select(Account).where(*scope(Account, message))
        fixed = message.account_id or (lead.account_id if lead else None)
        if fixed:
            query = query.where(Account.id == fixed)
        elif pool is None:
            # 旧的执行中任务继续使用原邮箱池，新任务必须显式配置任务邮箱池。
            query = query.where(Account.enabled.is_(True), Account.smtp_status == 'ok', Account.imap_status == 'ok')
        else:
            query = query.where(Account.id.in_([item['accountId'] for item in pool]))
        # 不同任务选择同地址的不同服务时，按统一顺序锁住整组后再核对额度。
        locked = (await db.scalars(select(Account).where(*scope(Account, message),
            Account.email.in_(query.with_only_columns(Account.email)))
            .order_by(Account.id).with_for_update())).all()
        accounts = [a for a in locked if (a.id == fixed if fixed else
            a.id in {item['accountId'] for item in pool} if pool is not None else
            a.enabled and a.smtp_status == 'ok' and a.imap_status == 'ok')]
        if not accounts or (not fixed and pool is not None and len(accounts) != len(pool)):
            message.due_at = stamp + timedelta(seconds=60)
            message.error_code = 'TASK_SENDER_UNAVAILABLE'
            return None
        if fixed:
            selected = accounts[0]
        else:
            # 先固定分配结果，再等待间隔和额度；重试、跟进不能重复参与权重轮转。
            weights = ({item['accountId']: item['weight'] for item in pool} if pool is not None
                       else {a.id: a.weight for a in accounts})
            previous = (json.loads(task.sender_allocation_state or '{}') if pool is not None
                        else {a.id: a.current_weight for a in accounts})
            current = {a.id: previous.get(a.id, 0) + weights[a.id] for a in accounts}
            selected = max(accounts, key=lambda a: (current[a.id], a.id))
            current[selected.id] -= sum(weights.values())
            if pool is not None:
                task.sender_allocation_state = json.dumps(current)
            else:
                for account in accounts:
                    account.current_weight = current[account.id]
            message.account_id = selected.id
            message.from_email = selected.email
            if lead:
                lead.account_id = selected.id
        available_at, reason = await account_send_time(db, selected, stamp, message.to_email, message.id)
        if available_at is None:
            message.due_at = stamp + timedelta(seconds=60)
            message.error_code = reason
            return None
        if message.error_code in WAIT_LABELS:
            message.error_code = None
        if available_at > stamp:
            message.due_at = available_at
            message.error_code = reason
            return None
        return selected

    async def refresh_store(self, db):
        from app.config.setting import settings
        from app.services.ai_call.knowledge import PlatformOssKnowledgeStore

        if not isinstance(self.store, PlatformOssKnowledgeStore):
            return
        # 独立 worker 常驻运行，附件读写前重新读取配置，避免继续连接已迁移的存储。
        result = await db.execute(text("SELECT * FROM sys_oss_config WHERE status = '0' LIMIT 1"))
        config = result.mappings().first()
        if config is None:
            raise RuntimeError('EMAIL_STORAGE_CONFIG_MISSING')
        if dict(config) != self.store.config:
            self.store = PlatformOssKnowledgeStore(dict(config), prefix=settings.AI_CALL_KNOWLEDGE_OSS_PREFIX)

    async def load_attachments(self, db, message):
        result = []
        identities = json.loads(message.attachment_ids)
        if len(identities) > MAX_ATTACHMENT_COUNT:
            raise ValueError('ATTACHMENT_COUNT_LIMIT')
        if len(set(identities)) != len(identities):
            raise ValueError('ATTACHMENT_DUPLICATE')
        if identities:
            await self.refresh_store(db)
        for identity in identities:
            attachment = await db.scalar(owned(Attachment, message, identity))
            if not attachment:
                raise ValueError('ATTACHMENT_INVALID')
            if attachment.size <= 0:
                raise ValueError('ATTACHMENT_EMPTY')
            if attachment.size > MAX_ATTACHMENT_BYTES:
                raise ValueError('ATTACHMENT_TOO_LARGE')
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
        if sum(len(item['content']) for item in result) > MAX_ATTACHMENT_BYTES:
            raise ValueError('ATTACHMENT_TOTAL_TOO_LARGE')
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
            if message.kind == 'follow_up' and not message.dedup_key.startswith('manual:'):
                settings = json.loads(task.settings)
                if not settings.get('followUpEnabled') or lead.follow_up_count >= settings.get('followUpCount', 0):
                    message.status, message.error_code = 'cancelled', 'FOLLOWUP_SETTINGS_CHANGED'
                    lead.next_follow_up_at = None
                    return None
                # 人工恢复或设置变化后，仍从最近成功发送重新校验跟进间隔。
                followup_at = await followup_send_time(db, message, settings)
                if followup_at and followup_at > now():
                    message.due_at = followup_at
                    return None
            if message.kind == 'follow_up' and not message.dedup_key.startswith('manual:') and lead.account_id not in self.sync_ok:
                message.due_at = now() + timedelta(seconds=30)
                return None
            stamp = now()
            manual_reply = message.kind == 'reply' and message.dedup_key.startswith('manual:')
            task_at, task_reason = (stamp, None) if manual_reply else await task_send_time(db, task, stamp)
            if task_reason == 'TASK_DAILY_LIMIT':
                message.due_at, message.error_code = task_at, task_reason
                return None
            if message.error_code in TASK_WAIT_REASONS:
                message.error_code = None
            account = await self.choose_account(db, message, lead, task)
            if account is None and message.error_code == LIMIT_PAUSED:
                return None
            # 先固定邮箱归属，再取任务与邮箱允许时间中更晚的一个，不为赶进度改派。
            if task_at > stamp:
                if account is not None or task_at >= message.due_at:
                    message.due_at, message.error_code = task_at, task_reason
                return None
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
            credit_enabled = app_settings.REACH_EMAIL_CREDIT_ENABLED
            if credit_enabled:
                from app.services.reach_email.credit import prepare_email_admission
                await prepare_email_admission(db, message)
            interval = await db.scalar(select(func.max(Account.interval_seconds)).where(
                *scope(Account, account), Account.email == account.email))
            account.next_send_at = now() + timedelta(seconds=interval)
            attempt = Attempt(id=new_id(), tenant_id=message.tenant_id, owner_id=message.owner_id,
                              message_id=message.id, account_id=account.id, task_id=task.id,
                              created_at=now())
            db.add(attempt)
            await db.flush()
            return message, attempt.id, config, account.version, credit_enabled

    async def send_one(self, identity):
        claimed = await self.claim(identity)
        if not claimed:
            return
        message, attempt_id, config, account_version, credit_enabled = claimed
        submitted = False
        started = perf_counter()
        stage = 'load_attachments'
        try:
            async with self.sessions() as db:
                attachments = await self.load_attachments(db, message)
                stage = 'eligibility_check'
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
                if config.get('provider_type') == 'outlook_personal':
                    stage = 'authorize'
                    config = await connection_config(self.sessions, self.cipher, message.account_id,
                        message.tenant_id, message.owner_id, account_version)
                    async with self.sessions() as db:
                        await self.require_lease(db)
                        account = await db.scalar(owned(Account, message, message.account_id))
                        current = await db.scalar(owned(Message, message, message.id))
                        if (not account or not account.enabled or account.version != account_version
                            or account.oauth_status != 'connected' or not current
                            or current.status != 'sending' or current.lease_token != self.token
                            or current.lease_until <= now()):
                            raise OutlookError('ACCOUNT_CHANGED', '邮箱或发送任务已变更，请重试。')
                if credit_enabled:
                    from app.services.reach_email.credit import require_email_admission
                    stage = 'credit_admission'
                    await require_email_admission(self.sessions, message_id=message.id,
                        tenant_id=message.tenant_id, owner_id=message.owner_id,
                        client=CreditMeteringClient.from_settings())
                    async with self.sessions() as db:
                        await self.require_lease(db)
                        current = await db.scalar(owned(Message, message, message.id))
                        if (current is None or current.status != 'sending'
                                or current.lease_token != self.token or current.lease_until <= now()):
                            raise CreditAdmissionDenied('EMAIL_SEND_LEASE_LOST', '邮件发送租约已失效')
                stage = 'graph_submit' if config.get('send_transport') == 'graph' else 'smtp_submit'
                if config.get('send_transport') == 'graph':
                    # 附件准备可能较久，租约到期前必须停止提交，避免另一 worker 接管后重复发送。
                    config['submission_deadline'] = monotonic() + (message.lease_until - now()).total_seconds() - 5
                submitted = True
                outcome = await asyncio.to_thread(transport.send_message, config,
                    recipient=message.to_email, subject=message.subject, html=message.html,
                    attachments=attachments, message_id=message.message_id,
                    in_reply_to=message.in_reply_to, references=message.references)
                submitted = outcome.get('submitted', submitted)
        except OutlookError as exc:
            outcome = {'status': 'retryable', 'errorCode': exc.code, 'diagnostic': exc.diagnostic()}
        except (CreditAdmissionDenied, CreditMeteringRejected, CreditMeteringUnavailable) as exc:
            log.warning('EMAIL_CREDIT_ADMISSION_FAILED message=%s error_type=%s',
                        message.id, type(exc).__name__)
            retryable_credit = not isinstance(exc, CreditAdmissionDenied) or exc.reason_code in {
                'INSUFFICIENT_START_BALANCE', 'INSUFFICIENT_BALANCE', 'ARREARS_BLOCKED',
            }
            outcome = {'status': 'retryable' if retryable_credit else 'failed',
                       'errorCode': getattr(exc, 'reason_code', 'CREDIT_UNAVAILABLE')}
        except Exception as exc:
            # 异常文本和完整堆栈可能带凭据，仅保留诊断元数据及函数位置。
            log.error('EMAIL_SEND_FAILED message=%s stage=%s elapsed_ms=%d error_type=%s '
                      'storage_error_type=%s storage_http_status=%s frames=%s',
                      message.id, stage, (perf_counter() - started) * 1000, type(exc).__name__,
                      getattr(exc, 'storage_error_type', None), getattr(exc, 'storage_http_status', None),
                      '>'.join(f'{frame.name}:{frame.lineno}' for frame in traceback.extract_tb(exc.__traceback__)))
            outcome = {'status': 'unknown' if submitted else 'failed',
                       'errorCode': ('SEND_INTERRUPTED' if submitted else
                                     str(exc) if isinstance(exc, ValueError) and str(exc) in ATTACHMENT_ERROR_CODES
                                     else 'PREPARE_SEND_FAILED')}
        async with self.sessions() as db, db.begin():
            await self.require_lease(db)
            task = await db.scalar(owned(Task, message, message.task_id).with_for_update())
            current = await db.scalar(owned(Message, message, message.id).with_for_update())
            if current.status != 'sending' or current.lease_token != self.token:
                return
            current.status, current.error_code = outcome['status'], outcome.get('errorCode')
            current.send_diagnostic = json.dumps(outcome['diagnostic'], ensure_ascii=False) if outcome.get('diagnostic') else None
            if outcome.get('providerMessageId'):
                current.provider_message_id = outcome['providerMessageId']
            current.lease_until = None
            attempt = await db.scalar(owned(Attempt, message, attempt_id))
            finished_at = now()
            if submitted:
                attempt.status = current.status
                attempt.updated_at = finished_at
            else:
                # 尚未提交发送，释放发送次数和节奏预留；结果不明时不能释放。
                await db.delete(attempt)
                current.attempt_count -= 1
                reserved_account = await db.scalar(owned(Account, message, message.account_id))
                if reserved_account:
                    reserved_account.next_send_at = None
            lead = await db.scalar(owned(Lead, message, message.lead_id))
            account = await db.scalar(owned(Account, message, message.account_id).with_for_update())
            if current.status == 'accepted':
                await clear_confirmed_limits(db, account, current, finished_at, smtp_config=config)
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
                if outcome.get('diagnostic', {}).get('category') == 'rate_limit':
                    if submitted:
                        attempt.status = 'rate_limited'
                    await record_limit(db, account, current, outcome, finished_at, smtp_config=config)
                else:
                    # 限流等待不消耗普通临时故障的重试预算；实际尝试仍计入发送节奏。
                    retries = await db.scalar(select(func.count()).select_from(Attempt).where(
                        *scope(Attempt, current), Attempt.message_id == current.id, Attempt.status == 'retryable'))
                    if retries >= 3:
                        current.status = 'failed'
                        current.error_code = 'RETRY_LIMIT_REACHED'
                    else:
                        current.due_at = finished_at + timedelta(minutes=2 ** max(retries, 1))
                if current.kind == 'follow_up' and not current.dedup_key.startswith('manual:'):
                    settings = json.loads(task.settings)
                    if not settings.get('followUpEnabled') or lead.follow_up_count >= settings.get('followUpCount', 0):
                        current.status, current.error_code = 'cancelled', 'FOLLOWUP_SETTINGS_CHANGED'
                    else:
                        followup_at = await followup_send_time(db, current, settings)
                        if followup_at:
                            current.due_at = max(current.due_at, followup_at)
            if (outcome.get('diagnostic', {}).get('category') == 'authentication'
                    and current.status == 'failed' and account.version == account_version):
                account.smtp_status = 'failed'
                account.smtp_test_result = json.dumps({
                    'status': 'failed', 'stage': 'authenticate', 'errorCode': outcome.get('errorCode', 'AUTH_FAILED'),
                    'message': outcome['diagnostic']['message'], 'serverCode': str(outcome['diagnostic'].get('serverCode') or ''),
                    'transport': config.get('send_transport', 'smtp'),
                    'suggestion': '请更新授权凭据或客户端权限，然后重新测试连接。',
                    'checkedAt': finished_at.isoformat() + '+00:00',
                }, ensure_ascii=False)
            if current.status in ('failed', 'cancelled') and not current.dedup_key.startswith('manual:'):
                lead.next_follow_up_at = None
            from app.services.reach_email.credit import record_email_outcome
            await record_email_outcome(db, current)

    async def followups(self):
        if not self.sync_ok:
            return
        async with self.sessions() as db, db.begin():
            await self.require_lease(db)
            leads = (await db.scalars(select(Lead).where(Lead.next_follow_up_at <= now(),
                Lead.stopped_reason.is_(None), Lead.account_id.in_(self.sync_ok))
                .order_by(Lead.next_follow_up_at).limit(self.batch_size))).all()
            for candidate in leads:
                task = await db.scalar(owned(Task, candidate, candidate.task_id).with_for_update()
                                       .execution_options(populate_existing=True))
                lead = await db.scalar(owned(Lead, candidate, candidate.id).with_for_update()
                                       .execution_options(populate_existing=True))
                if not lead or lead.stopped_reason or not lead.next_follow_up_at or lead.next_follow_up_at > now():
                    continue
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
                    in_reply_to=first.provider_message_id or first.message_id,
                    references=first.provider_message_id or first.message_id,
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

            async def record_attempt(attempt, identity=identity, tenant_id=tenant_id, owner_id=owner_id):
                record = dict(attempt)
                raw = record.pop('raw_output', None)
                if raw is not None:
                    record['raw_output_encrypted'] = self.cipher.encrypt({'content': raw})
                async with self.sessions() as db, db.begin():
                    await self.require_lease(db)
                    current = await db.scalar(select(AIJob).where(AIJob.id == identity,
                        AIJob.tenant_id == tenant_id, AIJob.owner_id == owner_id).with_for_update())
                    if current is None or current.status != 'running':
                        raise ValueError('EMAIL_AI_JOB_NOT_RUNNING')
                    # 人工重试复用作业，已产生费用的调用记录必须继续保留。
                    attempts = json.loads(current.attempts or '[]')
                    attempts.append(record)
                    current.attempts = json.dumps(attempts, ensure_ascii=False)
                    current.lease_until = now() + timedelta(seconds=LEASE_SECONDS)

            try:
                result = await self.ai.run(**payload, on_attempt=record_attempt)
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

    async def apply_bounce(self, db, account, row, incoming, *, apply=True):
        original = incoming.get('original_message_id')
        trace = incoming.get('trace_message_id')
        rewritten = re.fullmatch(r'<[0-9A-F]{16}\+([0-9a-f]{32})@reach\.local>', original or '')
        if rewritten and trace == f'<{rewritten.group(1)}@reach.local>':
            # 已观察到的腾讯前缀必须与正文编号一致，仍按会话唯一发送规则核对。
            original = None
        recipients = {r['recipient'].lower() for r in incoming.get('delivery_reports', [])}
        recipients.update(address.lower() for address in incoming.get('failed_recipients', []))
        if not (original or trace):
            return None
        sources = (await db.scalars(select(Message).where(*scope(Message, account),
            Message.account_id.in_(same_sender_accounts(account)), Message.direction == 'outbound',
            or_(Message.message_id == (original or trace), Message.provider_message_id == (original or trace)),
            func.lower(Message.to_email).in_(recipients)))).all()
        if len(sources) != 1:
            return None
        source = sources[0]
        if source.account_id != account.id and source.from_email.lower() != account.email.lower():
            return None
        # 改址后，收到发往历史发件地址的回执仍按原邮件快照核对。
        if not {account.email.lower(), source.from_email.lower()}.intersection(
                email.lower() for email in incoming.get('to_emails', [])):
            return None
        lead = await db.scalar(owned(Lead, account, source.lead_id))
        if not lead or lead.task_id != source.task_id or lead.account_id != source.account_id or lead.email.lower() != source.to_email.lower():
            return None
        if not original:
            # 会话编号被后续邮件复用；仅有一次发送尝试时才能据此确认原邮件。
            attempts = await db.scalar(select(func.count()).select_from(Message).where(
                *scope(Message, account), Message.lead_id == lead.id,
                Message.direction == 'outbound', Message.attempt_count > 0))
            if attempts != 1 or source.attempt_count < 1:
                return None
        if not apply:
            return source
        failures = [r for r in incoming.get('delivery_reports', [])
            if r['recipient'].lower() == source.to_email.lower()
            and r.get('action') == 'failed' and r.get('status', '').startswith('5.')]
        row.task_id, row.lead_id = source.task_id, source.lead_id
        row.kind = 'bounce' if failures else 'receipt'
        row.delivery_report = json.dumps({'messageId': source.message_id,
            'recipients': incoming.get('delivery_reports', [])})
        if not failures:
            return source
        invalid_address = any(r['status'] == '5.1.1' for r in failures)
        if invalid_address:
            suppression = await db.scalar(select(Suppression).where(
                *scope(Suppression, account), Suppression.email == lead.email))
            if suppression is None:
                db.add(Suppression(id=new_id(), tenant_id=account.tenant_id,
                    owner_id=account.owner_id, email=lead.email, reason='permanent_bounce'))
        # 地址不存在跨任务停发；认证、策略等永久拒收只停止当前会话的自动跟进。
        await db.execute(update(Message).where(*scope(Message, account),
            Message.direction == 'outbound', Message.status.in_(('queued', 'retryable')),
            Message.to_email == lead.email if invalid_address else Message.lead_id == lead.id,
            True if invalid_address else ~Message.dedup_key.like('manual:%'))
            .values(status='cancelled', error_code='PERMANENT_BOUNCE'))
        await db.execute(update(Lead).where(*scope(Lead, account),
            Lead.email == lead.email if invalid_address else Lead.id == lead.id)
            .values(next_follow_up_at=None, stopped_reason='permanent_bounce'))
        from app.services.reach_email.credit import request_email_refund
        await request_email_refund(db, source)
        return source

    async def reconcile_bounces(self, account_id, message_ids, *, apply=False):
        """仅回放指定的已保存退信；默认预览，不访问邮箱或重置 IMAP 游标。"""
        if not message_ids or len(message_ids) > 50:
            raise ValueError('请指定 1 至 50 条退信记录')
        async with self.sessions() as db, db.begin():
            account_query = select(Account).where(Account.id == account_id)
            account = await db.scalar(account_query.with_for_update() if apply else account_query)
            if not account:
                raise ValueError('邮箱不存在')
            results = []
            for identity in dict.fromkeys(message_ids):
                query = owned(Message, account, identity).where(
                    Message.account_id == account.id, Message.direction == 'inbound')
                row = await db.scalar(query.with_for_update() if apply else query)
                if not row:
                    raise ValueError('退信记录不存在或不属于指定邮箱')
                parsed = transport.tencent_bounce(row.from_email, row.subject, row.html)
                source = await self.apply_bounce(db, account, row,
                    dict(parsed, to_emails=[row.to_email]), apply=apply) if parsed else None
                results.append(dict(id=row.id, recognized=bool(parsed), matched=source is not None,
                    sourceId=source.id if source else None, applied=apply and source is not None))
            return results

    async def sync_account(self, account_id, *, require_worker_lease=True):
        async with self.sessions() as db:
            account = await db.get(Account, account_id)
            if not account or not account.enabled:
                raise ValueError('邮箱不存在或未启用')
            config = self.config(account)
            version = account.version
        if config.get('provider_type') == 'outlook_personal':
            config = await connection_config(self.sessions, self.cipher, account.id,
                account.tenant_id, account.owner_id, version)
        result = await asyncio.to_thread(transport.sync_messages, config,
            uidvalidity=account.uidvalidity, last_uid=account.last_uid, limit=self.batch_size)
        async with self.sessions() as db, db.begin():
            if require_worker_lease:
                await self.require_lease(db)
            siblings = (await db.scalars(select(Account).where(Account.id.in_(same_sender_accounts(account)))
                .order_by(Account.id).with_for_update())).all()
            current = next((row for row in siblings if row.id == account.id), None)
            if not current or not current.enabled or current.version != version:
                raise ValueError('邮箱配置已变更，请重新同步')
            # 手动同步与后台同步共享游标比较，防止较早的网络结果覆盖新游标。
            if (current.uidvalidity, current.last_uid) != (account.uidvalidity, account.last_uid):
                raise ValueError('该邮箱已完成另一轮同步，请刷新后重试')
            processed = 0
            for incoming in result['messages']:
                attachments = incoming.get('attachments', [])
                if len(attachments) > MAX_ATTACHMENT_COUNT or sum(len(a['content']) for a in attachments) > MAX_INBOUND_ATTACHMENT_BYTES:
                    raise ValueError('INBOUND_ATTACHMENT_LIMIT')
                fingerprint = hashlib.sha256(json.dumps({k: v for k, v in incoming.items()
                    if k not in ('uid', 'attachments')}, sort_keys=True).encode())
                for item in attachments:
                    fingerprint.update(item['content'])
                key = f'inbound:{account.id}:{fingerprint.hexdigest()}'
                sibling_keys = [f'inbound:{row.id}:{fingerprint.hexdigest()}' for row in siblings]
                existing = await db.scalar(select(Message).where(*scope(Message, account),
                    Message.dedup_key.in_(sibling_keys)).order_by(Message.created_at, Message.id).limit(1))
                if existing and existing.lead_id:
                    continue
                received_html = incoming.get('html') or '<p>' + html.escape(incoming.get('text', '')).replace('\n', '<br>') + '</p>'
                if incoming.get('bounce') and incoming.get('message_id'):
                    reports = (await db.scalars(select(Message).where(*scope(Message, account),
                        Message.account_id.in_([row.id for row in siblings]), Message.direction == 'inbound',
                        Message.message_id == incoming['message_id'],
                        Message.from_email == incoming['from_email'], Message.subject == incoming.get('subject', ''),
                        Message.html == received_html))).all()
                    if reports:
                        if len(reports) == 1:
                            await self.apply_bounce(db, account, reports[0], incoming)
                        continue
                source, lead = await match_reply(db, account, incoming)
                if existing and not source and not incoming.get('bounce'):
                    continue
                attachment_ids = []
                from app.services.reach_email.service import EmailService

                if attachments and not existing:
                    await self.refresh_store(db)
                attachment_service = EmailService(db, account.tenant_id, account.owner_id, '', self.store)
                for item in attachments if not existing else []:
                    saved = await attachment_service.save_attachment(
                        item['filename'], item['content'], item['content_type'])
                    attachment_ids.append(saved['id'])
                received = existing or Message(id=new_id(), tenant_id=account.tenant_id, owner_id=account.owner_id,
                    task_id=source.task_id if source else None, lead_id=source.lead_id if source else None,
                    account_id=account.id, direction='inbound', kind=('bounce' if incoming.get('permanent_bounce') else 'receipt') if incoming.get('bounce') else ('auto_reply' if incoming.get('auto_reply') else 'reply'),
                    delivery_report=json.dumps({'messageId': incoming.get('original_message_id', ''),
                        'recipients': incoming.get('delivery_reports', [])}) if incoming.get('bounce') else None,
                    status='received', dedup_key=key, message_id=incoming.get('message_id', '')[:512],
                    in_reply_to=incoming.get('in_reply_to', '')[:512], references=incoming.get('references', ''),
                    from_email=incoming['from_email'][:254],
                    to_email=(source.from_email if source and source.from_email.lower() in {
                        email.lower() for email in incoming.get('to_emails', [])} else account.email),
                    subject=incoming.get('subject', '')[:512],
                    html=received_html,
                    attachment_ids=json.dumps(attachment_ids))
                if not existing:
                    db.add(received)
                    processed += 1
                elif source:
                    received.task_id, received.lead_id = source.task_id, source.lead_id
                if incoming.get('bounce'):
                    await self.apply_bounce(db, account, received, incoming)
                elif source and not incoming.get('auto_reply'):
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
