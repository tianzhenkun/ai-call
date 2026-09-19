"""账号授权契约：隔离数据库，网络连接以受控返回替代，不代表真实邮件验收。"""

import asyncio
import json

import pytest
from cryptography.fernet import Fernet
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.services.reach_email import transport
from app.services.reach_email.models import TABLES, Account, Message
from app.services.reach_email.schema import AccountInput
from app.services.reach_email.security import CredentialCipher
from app.services.reach_email.service import EmailService
from app.services.reach_email.worker import EmailWorker


def test_presets_reject_override_unknown_provider_and_header_injection():
    data = AccountInput(providerType="netease_163", email="sender@163.com", smtpPassword="test",
                        smtp_host="127.0.0.1", smtpPort=25, imapPassword="ignored")
    assert data.smtp_host == "smtp.163.com" and data.smtp_port == 465
    assert data.imap_username == "sender@163.com" and data.imap_password == "test"
    with pytest.raises(ValueError, match="邮箱类型"):
        AccountInput(providerType="unknown", email="sender@163.com")
    with pytest.raises(ValueError, match="发件人"):
        AccountInput(providerType="gmail", email="sender@gmail.com", fromName="name\r\nBcc: bad")


def test_authorization_preserves_secret_and_protocol_status_and_protects_history(monkeypatch):
    async def run():
        engine = create_async_engine("sqlite+aiosqlite://")
        async with engine.begin() as conn:
            for table in TABLES:
                await conn.run_sync(lambda sync, t=table: t.create(sync))
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        key = Fernet.generate_key().decode()
        async with sessions() as db:
            s = EmailService(db, "tenant", "owner", key)
            data = AccountInput(providerType="qq_personal", email="sender@qq.com", smtpPassword="test-secret")
            saved = await s.save_account(data)
            account = await db.get(Account, saved['id'])
            account.smtp_status = account.imap_status = 'ok'
            updated = await s.save_account(AccountInput(providerType="qq_personal", email=data.email,
                                                       fromName="测试发件人"), account.id)
            assert updated['smtpStatus'] == updated['imapStatus'] == 'ok'
            assert updated['fromName'] == '测试发件人'
            assert 'test-secret' not in str(updated)
            assert CredentialCipher(key).decrypt(account.secret)['imap_password'] == 'test-secret'
            config = json.loads(account.config)
            config.pop('provider_type')
            account.config = json.dumps(config)
            assert (await s.account_json(account))['providerType'] == 'qq_personal'
            custom = AccountInput(email=data.email, **dict(config, provider_type='custom', imap_host='imap.other.com'))
            updated = await s.save_account(custom, account.id)
            assert updated['smtpStatus'] == 'ok' and updated['imapStatus'] == 'untested'
            with pytest.raises(ValueError, match="无权"):
                await EmailService(db, 'other', 'owner', key).delete_account(account.id)
            await db.commit()
            identity = account.id
        worker = EmailWorker(sessions, CredentialCipher(key), None, None)
        monkeypatch.setattr(transport, 'sync_messages', lambda *a, **kw: {
            'uidvalidity': '1', 'last_uid': 2, 'messages': [], 'drained': True})
        result = await worker.sync_account(identity, require_worker_lease=False)
        assert result == {'processedCount': 0, 'hasMore': False}
        async with sessions() as db:
            s = EmailService(db, 'tenant', 'owner', key)
            account = await db.get(Account, identity)
            assert account.last_uid == 2 and account.last_sync_at is not None
            s.add(Message, account_id=identity, dedup_key='history', message_id='<1@example.com>',
                  to_email='recipient@example.com', subject='test', html='test')
            await db.flush()
            with pytest.raises(ValueError, match='已有邮件记录'):
                await s.delete_account(identity)
            other = await s.save_account(AccountInput(providerType='gmail', email='other@gmail.com', smtpPassword='test'))
            await s.delete_account(other['id'])
            await db.flush()
            assert await db.get(Account, other['id']) is None
        await engine.dispose()
    asyncio.run(run())


def test_protocol_probe_only_connects_requested_protocol(monkeypatch):
    from unittest.mock import Mock

    connected = []
    conn = Mock()
    conn.select.return_value = ('OK', [])

    def connect(config, protocol):
        connected.append(protocol)
        return conn
    monkeypatch.setattr(transport, '_connect', connect)
    monkeypatch.setattr(transport, '_close', lambda conn: None)
    assert transport.test_account({}, ('imap',)) == {'imap': {'status': 'ok', 'errorCode': None}}
    assert connected == ['imap']
