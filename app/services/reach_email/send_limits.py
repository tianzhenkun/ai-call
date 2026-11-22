"""明确限流后的持久化等待；调度、页面和人工恢复共用范围匹配。"""

import hashlib
import json
from datetime import timedelta, timezone

from sqlalchemy import case, delete, func, or_, select, update

from app.services.reach_email.models import Account, Message, SendRestriction, new_id

LIMIT_WAIT = 'PROVIDER_LIMIT_WAIT'
LIMIT_PAUSED = 'PROVIDER_LIMIT_PAUSED'


def restriction_keys(account, recipient=None, message_id=None, *, smtp_config=None):
    config = smtp_config if smtp_config is not None else json.loads(account.config)
    host = config.get('smtp_host', '').lower().rstrip('.')
    domain = account.email.rsplit('@', 1)[-1].lower()
    values = {
        'account': (account.id, account.id),
        'enterprise': (f'{host}|{domain}', domain),
        'server': (host, host),
    }
    if recipient:
        recipient = recipient.lower()
        values.update(recipient=(f'{account.id}|{recipient}', recipient),
            recipient_domain=(f"{account.id}|{recipient.rsplit('@', 1)[-1]}", recipient.rsplit('@', 1)[-1]))
    if message_id:
        values['message'] = (message_id, message_id)
    return {scope: (hashlib.sha256(f'{scope}|{key}'.encode()).hexdigest(), target)
        for scope, (key, target) in values.items()}


async def restrictions(db, account, recipient=None, message_id=None, *, include_partial=False):
    keys = [key for key, _ in restriction_keys(account, recipient, message_id).values()]
    match = SendRestriction.scope_key.in_(keys)
    if include_partial:
        match = or_(match, (SendRestriction.account_id == account.id)
            & SendRestriction.scope.in_(('recipient', 'recipient_domain', 'message')))
    return (await db.scalars(select(SendRestriction).where(
        SendRestriction.tenant_id == account.tenant_id,
        SendRestriction.owner_id == account.owner_id, match,
    ).order_by(SendRestriction.created_at, SendRestriction.id))).all()


def restriction_time(rows, stamp):
    if any(row.retry_at is None for row in rows):
        return None, LIMIT_PAUSED
    until = max((row.retry_at for row in rows), default=stamp)
    return (until, LIMIT_WAIT) if until > stamp else (stamp, None)


def restriction_json(row):
    diagnostic = json.loads(row.diagnostic)
    label = {
        'account': '该邮箱全部任务',
        'enterprise': f'企业域 {row.target}（本用户配置的匹配邮箱）',
        'server': f'邮件服务器 {row.target}（本用户配置的匹配邮箱）',
        'recipient': f'收件人 {row.target}',
        'recipient_domain': f'收件域 {row.target}',
        'message': '单封邮件（服务商未明确限制范围）',
    }[row.scope]
    return {'id': row.id, 'scope': row.scope, 'scopeLabel': label, 'reason': diagnostic['message'],
        'errorCode': diagnostic['errorCode'], 'serverCode': diagnostic.get('serverCode'),
        'retryAt': row.retry_at.replace(tzinfo=timezone.utc).isoformat() if row.retry_at else None,
        'paused': row.retry_at is None, 'failureCount': row.failure_count}


def pending_due(model, due, recipient):
    """在数据库内计算局部限制后的排期，避免为展示加载整份收件名单。"""
    matches = [
        (SendRestriction.scope == 'recipient') & (recipient == SendRestriction.target),
        (SendRestriction.scope == 'recipient_domain') & recipient.endswith('@' + SendRestriction.target),
    ]
    if model is Message:
        matches.append((SendRestriction.scope == 'message') & (model.id == SendRestriction.target))
    query = select(SendRestriction).where(
        SendRestriction.tenant_id == model.tenant_id,
        SendRestriction.owner_id == model.owner_id,
        SendRestriction.account_id == model.account_id, or_(*matches),
    ).correlate(model)
    paused = query.where(SendRestriction.retry_at.is_(None)).exists()
    retry_at = query.with_only_columns(func.max(SendRestriction.retry_at)).scalar_subquery()
    return case((paused, None), (retry_at > due, retry_at), else_=due)


async def pending_condition(db, row):
    ids = [row.account_id]
    if row.scope in ('enterprise', 'server'):
        ids = []
        accounts = await db.stream_scalars(select(Account).where(
            Account.tenant_id == row.tenant_id, Account.owner_id == row.owner_id,
        ).execution_options(yield_per=100))
        async for account in accounts:
            if restriction_keys(account)[row.scope][0] == row.scope_key:
                ids.append(account.id)
    condition = (Message.tenant_id == row.tenant_id) & (Message.owner_id == row.owner_id)
    condition &= Message.account_id.in_(ids) & Message.status.in_(('queued', 'retryable'))
    if row.scope == 'recipient':
        condition &= Message.to_email == row.target
    elif row.scope == 'recipient_domain':
        condition &= Message.to_email.endswith('@' + row.target, autoescape=True)
    elif row.scope == 'message':
        condition &= Message.id == row.target
    return condition


async def record_limit(db, account, message, outcome, stamp, *, smtp_config=None):
    diagnostic = {**outcome['diagnostic'], 'errorCode': outcome['errorCode']}
    scope = diagnostic['scope']
    key, target = restriction_keys(account, message.to_email, message.id, smtp_config=smtp_config)[scope]
    row = await db.scalar(select(SendRestriction).where(
        SendRestriction.tenant_id == account.tenant_id,
        SendRestriction.owner_id == account.owner_id, SendRestriction.scope_key == key,
    ).with_for_update())
    if row is None:
        row = SendRestriction(id=new_id(), tenant_id=account.tenant_id, owner_id=account.owner_id,
            account_id=account.id, scope=scope, scope_key=key, target=target, failure_count=0)
        db.add(row)
    row.failure_count += 1
    row.diagnostic = json.dumps(diagnostic, ensure_ascii=False)
    delay = diagnostic.get('retryAfterSeconds') or (900 if row.failure_count == 1 else 3600)
    if outcome['errorCode'] == 'PROVIDER_DAILY_LIMIT':
        delay = max(delay, 86400)
    row.retry_at = stamp + timedelta(seconds=delay) if row.failure_count < 3 and not diagnostic.get('manualReview') else None
    due = row.retry_at or stamp + timedelta(minutes=5)
    code = LIMIT_WAIT if row.retry_at else LIMIT_PAUSED
    message.due_at, message.error_code = due, code
    await db.flush()
    # 已绑定同范围的待发邮件一并等待；新入队邮件仍由统一调度入口校验。
    await db.execute(update(Message).where(await pending_condition(db, row), Message.due_at <= due)
        .values(due_at=due, error_code=code))
    return row


async def clear_confirmed_limits(db, account, message, stamp, *, smtp_config=None):
    keys = [key for key, _ in restriction_keys(account, message.to_email, message.id, smtp_config=smtp_config).values()]
    # 只清除本次已成功验证且到期的范围，不因另一收件人成功解除其余限制。
    await db.execute(delete(SendRestriction).where(
        SendRestriction.tenant_id == account.tenant_id,
        SendRestriction.owner_id == account.owner_id,
        SendRestriction.scope_key.in_(keys), SendRestriction.retry_at <= stamp,
    ))
