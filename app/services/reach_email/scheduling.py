"""调度与页面共用发送时间计算；查询只读取本租户、本用户的持久化记录。"""

from datetime import timedelta

from sqlalchemy import and_, func, select

from app.services.reach_email.models import Account, Attempt, Message
from app.services.reach_email.send_limits import (
    LIMIT_PAUSED,
    LIMIT_WAIT,
    restriction_time,
    restrictions,
)

TASK_WAIT_REASONS = ('TASK_DAILY_LIMIT', 'TASK_SEND_INTERVAL')
WAIT_LABELS = {
    'TASK_DAILY_LIMIT': '已达任务 24 小时发送上限',
    'TASK_SEND_INTERVAL': '等待均匀发送间隔',
    'ACCOUNT_SEND_INTERVAL': '等待邮箱发送间隔',
    'ACCOUNT_HOURLY_LIMIT': '已达邮箱每小时发送上限',
    'ACCOUNT_DAILY_LIMIT': '已达邮箱 24 小时发送上限',
    'TASK_SENDER_UNAVAILABLE': '发件邮箱不可用',
    LIMIT_WAIT: '服务商限流，等待重试',
    LIMIT_PAUSED: '服务商持续限流，已暂停，请处理后恢复',
    'PROVIDER_PUBLISHED_LIMIT': '已达服务商公开额度对应的平台控制上限',
}


def manual_reply():
    return and_(Message.kind == 'reply', Message.dedup_key.like('manual:%'))


def attempt_query(row, since, **filters):
    conditions = [Attempt.tenant_id == row.tenant_id, Attempt.owner_id == row.owner_id,
                  Attempt.created_at >= since]
    conditions.extend(Attempt.account_id.in_(value) if key == 'account_ids'
                      else getattr(Attempt, key) == value for key, value in filters.items())
    if 'task_id' in filters:
        replies = select(Message.id).where(Message.tenant_id == row.tenant_id,
            Message.owner_id == row.owner_id, manual_reply())
        conditions.append(Attempt.message_id.not_in(replies))
    return select(Attempt).where(*conditions)


async def quota_time(db, row, stamp, hours, limit, **filters):
    query = attempt_query(row, stamp - timedelta(hours=hours), **filters)
    # 调低限额后可能已有超额记录，需等足够多的记录移出窗口，而非仅等最早一封。
    boundary = await db.scalar(query.with_only_columns(Attempt.created_at)
        .order_by(Attempt.created_at.desc()).offset(limit - 1).limit(1))
    return boundary + timedelta(hours=hours, seconds=1) if boundary else stamp


async def task_send_time(db, task, stamp):
    query = attempt_query(task, stamp - timedelta(hours=24), task_id=task.id)
    last = await db.scalar(query.with_only_columns(func.max(Attempt.created_at)))
    paced = last + timedelta(seconds=86400 / task.daily_limit) if last else stamp
    quota = await quota_time(db, task, stamp, 24, task.daily_limit, task_id=task.id)
    if quota > stamp and quota >= paced:
        return quota, 'TASK_DAILY_LIMIT'
    if paced > stamp:
        return paced, 'TASK_SEND_INTERVAL'
    return stamp, None


async def followup_send_time(db, message, settings):
    last = await db.scalar(select(func.max(Message.sent_at)).where(
        Message.tenant_id == message.tenant_id, Message.owner_id == message.owner_id,
        Message.lead_id == message.lead_id, Message.direction == 'outbound',
        Message.status == 'accepted', ~Message.dedup_key.like('manual:%')))
    return last + timedelta(days=settings.get('followUpIntervalDays', 2)) if last else None


async def account_send_time(db, account, stamp, recipient=None, message_id=None):
    if not account.enabled or account.smtp_status != 'ok' or account.imap_status != 'ok':
        return None, 'TASK_SENDER_UNAVAILABLE'
    limit_at, limit_reason = restriction_time(await restrictions(db, account, recipient, message_id), stamp)
    if limit_at is None:
        return None, limit_reason
    siblings = (await db.scalars(select(Account).where(Account.tenant_id == account.tenant_id,
        Account.owner_id == account.owner_id, Account.email == account.email))).all()
    available = max([stamp, *(row.next_send_at or stamp for row in siblings)])
    # 同地址共享平台额度，停用旧连接也不能清空已消耗的窗口。
    reason = 'ACCOUNT_SEND_INTERVAL' if available > stamp else None
    if limit_at > available:
        available, reason = limit_at, limit_reason
    for hours, limit, code in (
        (1, min(row.hourly_limit for row in siblings), 'ACCOUNT_HOURLY_LIMIT'),
        (24, min(row.daily_limit for row in siblings), 'ACCOUNT_DAILY_LIMIT'),
    ):
        release = await quota_time(db, account, stamp, hours, limit, account_ids=[row.id for row in siblings])
        if release > available:
            available, reason = release, code
    from app.services.reach_email.providers import published_daily_cap

    cap = published_daily_cap(account)
    if cap is not None and cap < account.daily_limit:
        release = await quota_time(db, account, stamp, 24, cap, account_id=account.id)
        if release > available:
            available, reason = release, 'PROVIDER_PUBLISHED_LIMIT'
    return available, reason
