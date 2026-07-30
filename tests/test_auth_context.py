import asyncio
import json
import time
from types import SimpleNamespace

import jwt
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.system.auth.schema import AuthSchema
from app.config.setting import settings
from app.core.dependencies import get_current_user, get_voice_manager
from app.core.exceptions import CustomException


def _build_ruoyi_token(user_id: int, username: str, tenant_id: str) -> str:
    return jwt.encode(
        {
            "userId": user_id,
            "userName": username,
            "tenantId": tenant_id,
            "exp": int(time.time()) + 3600,
        },
        key=settings.SECRET_KEY,
        algorithm=settings.ALGORITHM,
    )


def test_get_current_user_prefers_bearer_token_when_jwt_disabled(monkeypatch) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", False)
    request = SimpleNamespace(scope={})
    token = _build_ruoyi_token(123, "tenant-user", "367705")

    auth = asyncio.run(
        get_current_user(
            request=request,
            db=AsyncSession(),
            redis=None,
            token=token,
        )
    )

    assert request.scope["user_id"] == 123
    assert request.scope["tenant_id"] == "367705"
    assert auth.user.user_id == 123
    assert auth.user.tenant_id == "367705"


def test_get_current_user_parses_permissions_from_jwt_subject(monkeypatch) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", True)
    monkeypatch.setattr(
        "app.core.dependencies.decode_access_token",
        lambda _token: SimpleNamespace(
            is_refresh=False,
            sub=json.dumps({
                "user_id": 123,
                "user_name": "tenant-user",
                "tenant_id": "367705",
                "dept_id": 9,
                "permissions": [
                    "ai_call:voice:manage",
                    "ai_call:voice:manage",
                    "",
                    7,
                ],
            }),
        ),
    )
    request = SimpleNamespace(scope={})

    auth = asyncio.run(
        get_current_user(
            request=request,
            db=AsyncSession(),
            redis=None,
            token="Bearer opaque-token",
        )
    )

    assert auth.permissions == frozenset({"ai_call:voice:manage"})


def test_voice_manager_defaults_to_deny_when_jwt_enabled(monkeypatch) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", True)
    auth = AuthSchema(db=AsyncSession(), permissions=frozenset())

    with pytest.raises(CustomException) as error:
        asyncio.run(get_voice_manager(auth))

    assert error.value.status_code == 403
    assert error.value.code == 10403


def test_voice_manager_preserves_development_fallback(monkeypatch) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", False)
    auth = AuthSchema(db=AsyncSession(), permissions=frozenset())

    assert asyncio.run(get_voice_manager(auth)) is auth
