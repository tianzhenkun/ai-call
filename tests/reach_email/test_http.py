"""No running server, real database, model or mailbox is used by these tests."""

import os

os.environ.setdefault("ENVIRONMENT", "dev")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.reach_email.controller import EmailRouter, service
from app.common.response import ErrorResponse
from app.core.dependencies import get_current_user
from app.core.exceptions import CustomException


def app():
    result = FastAPI()
    result.include_router(EmailRouter)

    @result.exception_handler(CustomException)
    async def handle(request, exc):
        return ErrorResponse(msg=exc.msg, status_code=exc.status_code)

    return result


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
            "smtpPort": -1,
        },
    )
    assert response.status_code == 422
    assert "fake-private" not in response.text
    assert response.json().get("data") is None


def test_development_auth_cannot_bypass_actual_platform_identity():
    application = app()

    async def legacy_user():
        return object()

    application.dependency_overrides[get_current_user] = legacy_user
    for path in ('/email/tasks', '/email/worker-health'):
        response = TestClient(application).get(path)
        assert response.status_code == 401
        assert "有效" in response.text
