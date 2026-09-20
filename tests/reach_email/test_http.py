"""No running server, real database, model or mailbox is used by these tests."""

import os
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

os.environ.setdefault("ENVIRONMENT", "dev")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.reach_email.controller import EmailRouter, service
from app.common.response import ErrorResponse
from app.core.dependencies import get_current_user
from app.core.exceptions import CustomException


@pytest.mark.parametrize('method,path', [('post', '/email/tasks'), ('put', '/email/tasks/example')])
@pytest.mark.parametrize('change,expected', [
    ({'companyWebsite': 'www.example.com'}, '公司官网须填写完整的 http 或 https 地址'),
    ({'companyWebsite': 'https://lingchen-ai'}, '公司官网须填写包含完整域名的地址，例如 https://example.com'),
    ({'companyDescription': ''}, '请输入公司简介'),
    ({'dailyLimit': 0}, '本任务 24 小时发送上限不能小于 1'),
])
def test_task_validation_explains_the_field_without_echoing_input(method, path, change, expected):
    application = app()

    async def fake_service():
        yield object()

    application.dependency_overrides[service] = fake_service
    response = getattr(TestClient(application), method)(path, json={
        'name': '任务', 'importId': 'i', 'version': 1,
        'settings': {'companyName': 'private-company-marker', 'companyWebsite': 'https://example.com',
                     'companyDescription': 'private-description-marker', **change},
    })
    assert response.status_code == 422
    assert expected in response.json()['msg']
    assert 'private-' not in response.text


@pytest.mark.parametrize('path,method,scheduled', [
    ('/email/tasks/example/preflight', 'get', None),
    ('/email/tasks/example/start', 'post', None),
    ('/email/tasks/example/start', 'post', '2099-01-01T00:00:00Z'),
])
def test_saved_invalid_settings_have_safe_errors_during_execution(monkeypatch, path, method, scheduled):
    from app.api.v1.reach_email import controller

    application = app()
    rolled_back = []

    class DB:
        async def rollback(self):
            rolled_back.append(True)

    @asynccontextmanager
    async def session():
        yield DB()

    async def user():
        return object()

    @application.middleware('http')
    async def identity(request, call_next):
        request.scope.update(platform_tenant_id='t', user_id='u', product_code='reach')
        return await call_next(request)

    async def task(self, identifier, lock=False):
        return SimpleNamespace(status='unstarted', version=1, start_key=None, content='{}',
                               settings='{"companyName":"private-company-marker","companyWebsite":"www.example.com","companyDescription":"private-description-marker"}')

    application.dependency_overrides[get_current_user] = user
    monkeypatch.setattr(controller, 'async_db_session', session)
    monkeypatch.setattr(controller, 'encryption_key', lambda: '')
    monkeypatch.setattr(controller.EmailService, 'task', task)
    kwargs = {'json': {'version': 1, 'requestId': 'safe-check', 'scheduledAt': scheduled}} if method == 'post' else {}
    response = getattr(TestClient(application), method)(path, **kwargs)
    assert response.status_code == 422
    assert '公司官网须填写完整的 http 或 https 地址' in response.json()['msg']
    assert 'private-' not in response.text
    assert 'input_value' not in response.text
    assert rolled_back


def app():
    result = FastAPI()
    result.include_router(EmailRouter)

    @result.exception_handler(CustomException)
    async def handle(request, exc):
        return ErrorResponse(msg=exc.msg, status_code=exc.status_code)

    return result


@pytest.mark.parametrize('method,path,payload', [
    ('put', '/email/tasks/example/content', {'version': 1, 'subject': 'test', 'html': 'test'}),
    ('post', '/email/conversations/example/reply', {'requestId': 'test', 'subject': 'test', 'html': 'test'}),
])
def test_attachment_count_validation_explains_limit_and_actual_count(method, path, payload):
    application = app()

    async def fake_service():
        yield object()

    application.dependency_overrides[service] = fake_service
    response = getattr(TestClient(application), method)(path, json={
        **payload, 'attachmentIds': [f'private-file-{i}' for i in range(6)],
    })
    assert response.status_code == 422
    assert '最多 5 个附件，当前选择了 6 个' in response.json()['msg']
    assert 'private-file-' not in response.text


def test_settings_route_accepts_only_settings_and_version(task_settings):
    application = app()

    class SettingsService:
        async def update_settings(self, identifier, data):
            return {'id': identifier, 'version': data.version + 1,
                    'settings': data.settings.model_dump(by_alias=True)}

    async def settings_service():
        yield SettingsService()

    application.dependency_overrides[service] = settings_service
    client = TestClient(application)
    payload = {'version': 1, 'settings': task_settings}
    response = client.put('/email/tasks/example/settings', json=payload)
    assert response.status_code == 200
    assert response.json()['data']['version'] == 2
    assert response.json()['data']['settings']['companyName'] == task_settings['companyName']
    for extra in ({'content': {'subject': '禁止修改'}}, {'name': '禁止改名'}):
        assert client.put('/email/tasks/example/settings', json={**payload, **extra}).status_code == 422
    assert client.put('/email/tasks/example/settings', json={'settings': task_settings}).status_code == 422


def test_validation_never_echoes_email_passwords():
    application = app()

    async def fake_service():
        yield object()

    application.dependency_overrides[service] = fake_service
    response = TestClient(application).post(
        "/email/accounts",
        json={
            "smtpPassword": "fake-private-smtp-secret",
            "imapPassword": "fake-private-imap-secret",
            "fake-private-field-name": "fake-private-value",
            "smtpPort": -1,
        },
    )
    assert response.status_code == 422
    assert "fake-private" not in response.text
    assert response.json().get("data") is None
    for label in ('SMTP 服务器', 'IMAP 服务器', 'SMTP 用户名', 'IMAP 用户名'):
        assert label in response.json()['msg']


def test_development_auth_cannot_bypass_actual_platform_identity():
    application = app()

    async def legacy_user():
        return object()

    application.dependency_overrides[get_current_user] = legacy_user
    for path in ('/email/tasks', '/email/worker-health'):
        response = TestClient(application).get(path)
        assert response.status_code == 401
        assert "有效" in response.text
