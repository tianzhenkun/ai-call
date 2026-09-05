import asyncio
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import jwt
import pytest
from lingchen_sdk.auth import AuthContext, VerifiedToken
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.system.auth.schema import AuthSchema
from app.api.v1.system.user.model import UserModel
from app.config.setting import settings
from app.core import dependencies
from app.core.dependencies import (
    get_ai_call_console,
    get_ai_call_manager,
    get_ai_call_statistics_viewer,
    get_current_user,
    get_knowledge_manager,
    get_knowledge_viewer,
    get_prompt_manager,
    get_voice_manager,
    redis_getter,
)
from app.core.exceptions import CustomException
from app.core.security import decode_access_token


def _build_ruoyi_token(user_id: int, username: str, tenant_id: str) -> str:
    return jwt.encode(
        {
            "userId": user_id,
            "userName": username,
            "tenantId": tenant_id,
            "clientid": "e5cd7e4891bf95d1d19206ce24a7b32e",
            "productCode": "reach",
            "portalScope": "PRODUCT",
            "exp": int(time.time()) + 3600,
        },
        key=settings.SECRET_KEY,
        algorithm=settings.ALGORITHM,
    )


@pytest.mark.anyio
async def test_redis_getter_returns_none_when_redis_is_disabled(monkeypatch) -> None:
    monkeypatch.setattr(settings, "REDIS_ENABLE", False)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))

    assert await redis_getter(request) is None


@pytest.mark.anyio
async def test_redis_getter_rejects_missing_enabled_connection(monkeypatch) -> None:
    monkeypatch.setattr(settings, "REDIS_ENABLE", True)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))

    with pytest.raises(RuntimeError, match="连接尚未初始化"):
        await redis_getter(request)


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


def test_platform_tenant_can_reuse_explicit_legacy_data_partition(monkeypatch) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", True)
    monkeypatch.setattr(settings, "AI_CALL_PLATFORM_TENANT_ID", "960001")
    monkeypatch.setattr(settings, "AI_CALL_LEGACY_DATA_TENANT_ID", "000000")
    monkeypatch.setattr(
        "app.core.dependencies._verify_platform_token",
        lambda _token, _client_id: VerifiedToken(
            context=AuthContext(
                user_id="123",
                username="tenant-user",
                tenant_id="960001",
                dept_id="9",
                client_id="e5cd7e4891bf95d1d19206ce24a7b32e",
                product_code="reach",
                portal_scope="PRODUCT",
            ),
            claims={"permissions": ["*:*:*"]},
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

    assert request.scope["platform_tenant_id"] == "960001"
    assert request.scope["tenant_id"] == "000000"
    assert auth.user.tenant_id == "000000"


def test_legacy_data_partition_mapping_does_not_capture_other_tenants(
    monkeypatch,
) -> None:
    monkeypatch.setattr(settings, "AI_CALL_PLATFORM_TENANT_ID", "960001")
    monkeypatch.setattr(settings, "AI_CALL_LEGACY_DATA_TENANT_ID", "000000")

    assert dependencies._resolve_data_tenant_id("tenant-other") == "tenant-other"


@pytest.mark.parametrize(
    ("claim", "value"),
    (
        ("productCode", "geo"),
        ("portalScope", "PLATFORM"),
        ("clientid", "other-client"),
    ),
)
def test_get_current_user_rejects_token_for_other_platform_entry(
    monkeypatch,
    claim: str,
    value: str,
) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", True)
    payload = {
        "userId": 123,
        "userName": "tenant-user",
        "tenantId": "367705",
        "clientid": "e5cd7e4891bf95d1d19206ce24a7b32e",
        "productCode": "reach",
        "portalScope": "PRODUCT",
        "permissions": ["ai_call:agent:manage"],
        "exp": int(time.time()) + 3600,
    }
    payload[claim] = value
    token = jwt.encode(payload, key=settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    request = SimpleNamespace(
        headers={"clientid": "e5cd7e4891bf95d1d19206ce24a7b32e"},
        scope={},
    )

    with pytest.raises(CustomException) as error:
        asyncio.run(
            get_current_user(
                request=request,
                db=AsyncSession(),
                redis=None,
                token=token,
            )
        )

    assert error.value.status_code == 401


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
        "app.core.dependencies._verify_platform_token",
        lambda _token, _client_id: VerifiedToken(
            context=AuthContext(
                user_id="123",
                username="tenant-user",
                tenant_id="367705",
                dept_id="9",
                client_id="e5cd7e4891bf95d1d19206ce24a7b32e",
                product_code="reach",
                portal_scope="PRODUCT",
            ),
            claims={
                "permissions": [
                    "ai_call:voice:manage",
                    "ai_call:voice:manage",
                    "",
                    7,
                ],
            },
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


def test_voice_manager_allows_superuser_without_explicit_permissions(monkeypatch) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", True)
    superuser = UserModel(
        user_id=1,
        tenant_id="000000",
        user_name="admin",
        nick_name="超级管理员",
    )
    auth = AuthSchema(db=AsyncSession(), user=superuser, permissions=frozenset())

    assert asyncio.run(get_voice_manager(auth)) is auth


def test_voice_manager_preserves_development_fallback(monkeypatch) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", False)
    auth = AuthSchema(db=AsyncSession(), permissions=frozenset())

    assert asyncio.run(get_voice_manager(auth)) is auth


@pytest.mark.parametrize(
    ("dependency", "permission"),
    (
        (get_ai_call_manager, "ai_call:agent:manage"),
        (get_ai_call_console, "ai_call:agent:console"),
        (get_ai_call_statistics_viewer, "ai_call:statistics:view"),
        (get_prompt_manager, "ai_call:prompt:manage"),
    ),
)
def test_ai_call_agent_permissions_default_to_deny(
    monkeypatch,
    dependency,
    permission: str,
) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", True)
    auth = AuthSchema(db=AsyncSession(), permissions=frozenset())

    with pytest.raises(CustomException) as error:
        asyncio.run(dependency(auth))

    assert error.value.status_code == 403
    assert error.value.code == 10403
    allowed = AuthSchema(db=AsyncSession(), permissions=frozenset({permission}))
    assert asyncio.run(dependency(allowed)) is allowed


@pytest.mark.parametrize(
    "dependency",
    (
        get_ai_call_manager,
        get_ai_call_console,
        get_ai_call_statistics_viewer,
        get_prompt_manager,
    ),
)
def test_ai_call_agent_permissions_allow_superuser(monkeypatch, dependency) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", True)
    superuser = UserModel(
        user_id=1,
        tenant_id="000000",
        user_name="admin",
        nick_name="超级管理员",
    )
    auth = AuthSchema(db=AsyncSession(), user=superuser, permissions=frozenset())

    assert asyncio.run(dependency(auth)) is auth


def test_knowledge_permissions_separate_view_and_manage(monkeypatch) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", True)
    denied = AuthSchema(db=AsyncSession(), permissions=frozenset())
    viewer = AuthSchema(
        db=AsyncSession(),
        permissions=frozenset({"ai_call:knowledge:view"}),
    )
    manager = AuthSchema(
        db=AsyncSession(),
        permissions=frozenset({"ai_call:knowledge:manage"}),
    )

    with pytest.raises(CustomException) as forbidden:
        asyncio.run(get_knowledge_viewer(denied))
    assert forbidden.value.status_code == 403
    assert asyncio.run(get_knowledge_viewer(viewer)) is viewer
    assert asyncio.run(get_knowledge_viewer(manager)) is manager

    with pytest.raises(CustomException) as forbidden:
        asyncio.run(get_knowledge_manager(viewer))
    assert forbidden.value.status_code == 403
    assert asyncio.run(get_knowledge_manager(manager)) is manager
