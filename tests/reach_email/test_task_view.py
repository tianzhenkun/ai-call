import asyncio
import json
from io import BytesIO

import pytest
from openpyxl import load_workbook
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.reach_email.controller import email_worker_health, task_recipients_download
from app.services.reach_email.migrate import migrate_tables
from app.services.reach_email.models import Import, Task
from app.services.reach_email.service import EmailService, dump


def test_activity_and_recipient_export_respect_owner_scope():
    async def check():
        engine = create_async_engine('sqlite+aiosqlite://')
        async with engine.begin() as conn:
            await conn.run_sync(migrate_tables)
        async with async_sessionmaker(engine, expire_on_commit=False)() as db:
            service = EmailService(db, 'tenant', 'owner', '')
            other = EmailService(db, 'other', 'owner', '')
            source = service.add(Import, filename='名单.xlsx', report=dump({
                'columns': ['邮箱', '客户姓名', '自定义列'],
                'records': [{'values': {'邮箱': 'customer@example.com', '客户姓名': '=1+1', '自定义列': '核对'}}],
            }))
            await db.flush()
            task = service.add(Task, name='任务', import_id=source.id, settings='{}', status='ended')
            other.add(Task, name='其他租户', import_id='other', settings='{}', status='running')
            await db.flush()
            for status, expected in [('ended', 0), ('unstarted', 0), ('scheduled', 1), ('running', 1)]:
                task.status = status
                await db.flush()
                data = json.loads((await email_worker_health(service)).body)['data']
                assert data['activeTaskCount'] == expected
            response = await task_recipients_download(task.id, service)
            book = load_workbook(BytesIO(response.body))
            assert list(book.active.values) == [('邮箱', '客户姓名', '自定义列'), ('customer@example.com', '=1+1', '核对')]
            assert book.active['B2'].data_type == 's'
            book.close()
            with pytest.raises(ValueError, match='无权访问'):
                await task_recipients_download(task.id, other)
        await engine.dispose()
    asyncio.run(check())
