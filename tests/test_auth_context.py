import asyncio
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import jwt
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.system.auth.schema import AuthSchema
from app.api.v1.system.user.model import UserModel
from app.config.setting import settings
from app.core import dependencies
from app.core.dependencies import get_current_user, get_voice_manager
from app.core.exceptions import CustomException
from app.core.security import decode_access_token


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


@pytest.mark.anyio
async def test_get_current_user_dev_fallback_keeps_business_session_unused(
    monkeypatch,
) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", False)
    request = SimpleNamespace(scope={})
    user = UserModel(
        user_id=7,
        tenant_id="000000",
        user_name="local-admin",
        nick_name="本地管理员",
    )
    business_db = AsyncMock(spec=AsyncSession)
    business_db.execute.return_value = SimpleNamespace(
        scalar_one_or_none=lambda: user,
    )
    lookup_db = AsyncMock(spec=AsyncSession)
    lookup_db.execute.return_value = SimpleNamespace(
        scalar_one_or_none=lambda: user,
    )

    class LookupSessionContext:
        async def __aenter__(self):
            return lookup_db

        async def __aexit__(self, _exc_type, _exc, _traceback):
            return False

    monkeypatch.setattr(
        dependencies,
        "async_db_session",
        lambda: LookupSessionContext(),
    )

    auth = await get_current_user(
        request=request,
        db=business_db,
        redis=None,
        token=None,
    )

    business_db.execute.assert_not_awaited()
    lookup_db.execute.assert_awaited_once()
    assert auth.db is business_db
    assert auth.user is user
    assert request.scope["tenant_id"] == "000000"


def test_decode_access_token_logs_signature_failure_without_token(monkeypatch) -> None:
    token = jwt.encode(
        {
            "userId": 123,
            "userName": "tenant-user",
            "tenantId": "367705",
        },
        key="a-different-signing-key",
        algorithm=settings.ALGORITHM,
    )

    with (
        patch("app.core.security.logger.warning") as warning,
        pytest.raises(CustomException),
    ):
        decode_access_token(token)

    warning.assert_called_once_with("JWT认证失败: invalid_signature")
    assert token not in str(warning.call_args)


def test_decode_access_token_preserves_ruoyi_permissions() -> None:
    token = jwt.encode(
        {
            "userId": 123,
            "userName": "tenant-user",
            "tenantId": "367705",
            "permissions": ["ai_call:voice:manage", "*:*:*"],
            "exp": int(time.time()) + 3600,
        },
        key=settings.SECRET_KEY,
        algorithm=settings.ALGORITHM,
    )

    payload = decode_access_token(token)
    subject = json.loads(payload.sub)

    assert subject["permissions"] == ["ai_call:voice:manage", "*:*:*"]


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


def test_voice_manager_allows_ruoyi_super_admin_permission(monkeypatch) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", True)
    auth = AuthSchema(db=AsyncSession(), permissions=frozenset({"*:*:*"}))

    assert asyncio.run(get_voice_manager(auth)) is auth


def test_voice_manager_preserves_development_fallback(monkeypatch) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", False)
    auth = AuthSchema(db=AsyncSession(), permissions=frozenset())

    assert asyncio.run(get_voice_manager(auth)) is auth
