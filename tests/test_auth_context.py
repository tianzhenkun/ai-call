import asyncio
import time
from types import SimpleNamespace

import jwt
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.setting import settings
from app.core.dependencies import get_current_user


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
