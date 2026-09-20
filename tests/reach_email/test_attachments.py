"""附件边界使用内存数据库和内存文件存储，不连接真实邮箱或 OSS。"""
import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from fastapi import UploadFile
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.services.reach_email import transport
from app.services.reach_email.models import Attachment
from app.services.reach_email.service import EmailService
from app.services.reach_email.worker import EmailWorker


class MemoryStore:
    def __init__(self):
        self.files = {}

    async def put(self, key, source, **kwargs):
        payload = source.read()
        self.files[key] = payload
        return SimpleNamespace(sha256=hashlib.sha256(payload).hexdigest())

    async def open(self, key):
        async def body():
            yield self.files[key]
        return SimpleNamespace(body=body())


@asynccontextmanager
async def attachment_service():
    engine = create_async_engine('sqlite+aiosqlite://')
    async with engine.begin() as conn:
        await conn.run_sync(Attachment.__table__.create)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as db:
            yield EmailService(db, 't', 'o', '', MemoryStore())
    finally:
        await engine.dispose()


@pytest.mark.parametrize('size', [9 * 1024 * 1024, 15 * 1024 * 1024])
def test_upload_save_and_worker_preserve_full_attachment_up_to_15_mb(size, monkeypatch):
    from app.api.v1.reach_email.controller import attachment
    from app.services.ai_call import knowledge

    async def check():
        async with attachment_service() as service:
            monkeypatch.setattr(knowledge, 'build_knowledge_store', lambda settings: service.store)
            payload = b'%PDF-' + b'x' * (size - 5)
            response = await attachment(UploadFile(filename='附件.pdf', file=BytesIO(payload)), service)
            saved = json.loads(response.body)['data']
            assert saved['size'] == size
            assert len(await service.attachments([saved['id']])) == 1
            message = SimpleNamespace(tenant_id='t', owner_id='o', attachment_ids=json.dumps([saved['id']]))
            worker = EmailWorker(None, None, service.store, None)
            loaded = await worker.load_attachments(service.db, message)
            assert loaded[0]['content'] == payload
    asyncio.run(check())


@pytest.mark.parametrize('name,payload,expected', [
    ('large.pdf', b'%PDF-' + b'x' * (15 * 1024 * 1024 - 4), '单个附件不能超过 15 MB'),
    ('empty.pdf', b'', '附件不能为空文件'),
    ('bad.exe', b'x', '不支持 .exe 附件'),
    ('picture.bmp', b'x', '不支持 .bmp 附件'),
    ('a' * 252 + '.pdf', b'%PDF-', '附件文件名不能超过 255 个字符'),
    ('fake.pdf', b'not a pdf', 'PDF 文件格式不正确'),
], ids=['oversized', 'empty', 'unsupported', 'bmp', 'long-name', 'invalid-signature'])
def test_upload_errors_explain_the_individual_constraint(name, payload, expected):
    async def check():
        with pytest.raises(ValueError, match=expected):
            await EmailService(None, 't', 'o', '').save_attachment(name, payload, None)
    asyncio.run(check())


def test_upload_rejects_oversized_file_without_reading_all_bytes(monkeypatch):
    from app.api.v1.reach_email.controller import attachment
    from app.services.ai_call import knowledge

    async def check():
        async with attachment_service() as service:
            monkeypatch.setattr(knowledge, 'build_knowledge_store', lambda settings: service.store)
            source = BytesIO(b'%PDF-' + b'x' * (16 * 1024 * 1024))
            with pytest.raises(ValueError, match='单个附件不能超过 15 MB'):
                await attachment(UploadFile(filename='large.pdf', file=source), service)
            assert source.tell() == 15 * 1024 * 1024 + 1
            assert not service.store.files
    asyncio.run(check())


@pytest.mark.parametrize('ids,expected', [
    (['a', 'b', 'c', 'd', 'e', 'f'], '最多 5 个附件，当前选择了 6 个'),
    (['a', 'a'], '不能重复添加同一附件'),
])
def test_attachment_count_and_duplicate_are_distinct(ids, expected):
    with pytest.raises(ValueError, match=expected):
        asyncio.run(EmailService(None, 't', 'o', '').attachments(ids))


def test_combined_limit_and_ownership_are_checked_when_reusing_files():
    async def check():
        async with attachment_service() as service:
            files = [await service.save_attachment(f'{i}.pdf', b'%PDF-' + b'x' * (8 * 1024 * 1024 - 5), None)
                     for i in range(2)]
            with pytest.raises(ValueError, match='附件合计不能超过 15 MB'):
                await service.attachments([file['id'] for file in files])
            worker = EmailWorker(None, None, service.store, None)
            message = SimpleNamespace(tenant_id='t', owner_id='o', attachment_ids=json.dumps([file['id'] for file in files]))
            with pytest.raises(ValueError, match='ATTACHMENT_TOTAL_TOO_LARGE'):
                await worker.load_attachments(service.db, message)
            with pytest.raises(ValueError, match='无权访问'):
                await EmailService(service.db, 'another', 'o', '').attachments([files[0]['id']])
    asyncio.run(check())


@pytest.mark.parametrize('sizes,error', [
    ([15 * 1024 * 1024], None),
    ([15 * 1024 * 1024 + 1], 'ATTACHMENT_TOO_LARGE'),
    ([8 * 1024 * 1024, 8 * 1024 * 1024], 'ATTACHMENT_TOTAL_TOO_LARGE'),
    ([1] * 6, 'ATTACHMENT_COUNT_LIMIT'),
    ([0], 'ATTACHMENT_EMPTY'),
])
def test_smtp_checks_raw_limits_before_connecting_and_encodes_15_mb(sizes, error):
    smtp = Mock()
    smtp.has_extn.return_value = False
    smtp.mail.return_value = smtp.rcpt.return_value = smtp.data.return_value = (250, b'ok')
    with patch.object(transport, '_connect', return_value=smtp) as connect:
        result = transport.send_message({'email': 'sender@example.com'}, recipient='lead@example.com',
            subject='test', html='test', message_id='<test>', attachments=[
                {'filename': f'{i}.pdf', 'content': b'x' * size, 'content_type': 'application/pdf'}
                for i, size in enumerate(sizes)])
    assert result == {'status': 'failed' if error else 'accepted', 'errorCode': error}
    if error:
        connect.assert_not_called()
    else:
        assert 20 * 1024 * 1024 < len(smtp.data.call_args.args[0]) < 25 * 1024 * 1024


def test_smtp_rejects_encoded_message_over_25_mb_before_connecting():
    with patch.object(transport.EmailMessage, 'as_bytes', return_value=b'x' * (25 * 1024 * 1024 + 1)), \
            patch.object(transport, '_connect') as connect:
        result = transport.send_message({'email': 'sender@example.com'}, recipient='lead@example.com',
            subject='test', html='test', message_id='<test>', attachments=[])
    assert result == {'status': 'failed', 'errorCode': 'MESSAGE_TOO_LARGE'}
    connect.assert_not_called()


def test_imap_keeps_16_mb_fetch_limit():
    conn = Mock()
    conn.select.return_value = ('OK', [])
    conn.response.return_value = ('UIDVALIDITY', [b'one'])
    conn.uid.side_effect = [('OK', [b'1']), ('OK', [(b'1', b'x' * (16 * 1024 * 1024 + 1))])]
    with patch.object(transport, '_connect', return_value=conn):
        with pytest.raises(ValueError, match='IMAP_MESSAGE_TOO_LARGE'):
            transport.sync_messages({})
    conn.uid.assert_any_call('fetch', '1', '(BODY.PEEK[]<0.16777217>)')
