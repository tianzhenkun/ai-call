"""邮件消费者心跳与到期队列积压检查，不返回邮件内容或账号信息。"""
from datetime import timedelta

from sqlalchemy import func, select

from app.services.reach_email.models import Message, WorkerLease, now


async def worker_health(db, message_query=None):
    stamp = now()
    lease = await db.get(WorkerLease, 'email', populate_existing=True)
    online = bool(lease and lease.expires_at > stamp)
    query = message_query if message_query is not None else select(Message)
    overdue = query.where(
        Message.direction == 'outbound',
        Message.status.in_(('queued', 'retryable')),
        Message.due_at < stamp - timedelta(minutes=5),
    ).subquery()
    count = await db.scalar(select(func.count()).select_from(overdue))
    return {'workerOnline': online, 'overdueCount': count,
            'status': 'offline' if not online else 'backlog' if count else 'ok'}


async def main():
    import json
    from app.core.database import async_db_session, async_engine

    try:
        async with async_db_session() as db:
            result = await worker_health(db)
        print(json.dumps(result), flush=True)
        return 0 if result['status'] == 'ok' else 1
    finally:
        await async_engine.dispose()


if __name__ == '__main__':
    import asyncio
    raise SystemExit(asyncio.run(main()))
