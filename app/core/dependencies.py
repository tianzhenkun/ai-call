from collections.abc import AsyncGenerator

from fastapi import Depends, Query, Request
from lingchen_sdk.auth import PlatformAuthError, PlatformTokenVerifier, VerifiedToken
from redis.asyncio.client import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.system.auth.schema import AuthSchema
from app.api.v1.system.user.model import UserModel
from app.config.setting import settings
from app.core.database import async_db_session
from app.core.exceptions import CustomException
from app.core.security import OAuth2Schema

REACH_PRODUCT_CODE = "reach"
REACH_PORTAL_SCOPE = "PRODUCT"


def _subject_permissions(user_info: dict) -> frozenset[str]:
    raw_permissions = user_info.get("permissions")
    if not isinstance(raw_permissions, list):
        return frozenset()
    return frozenset(
        permission.strip()
        for permission in raw_permissions
        if isinstance(permission, str) and permission.strip()
    )


def _allowed_client_ids() -> tuple[str, ...]:
    return tuple(
        client_id.strip()
        for client_id in settings.PLATFORM_AUTH_ALLOWED_CLIENT_IDS.split(",")
        if client_id.strip()
    )


def _resolve_data_tenant_id(platform_tenant_id: str) -> str:
    """将指定的平台租户映射到存量 Reach 数据分区。"""

    expected_platform_tenant_id = settings.AI_CALL_PLATFORM_TENANT_ID.strip()
    legacy_data_tenant_id = settings.AI_CALL_LEGACY_DATA_TENANT_ID.strip()
    if (
        expected_platform_tenant_id
        and legacy_data_tenant_id
        and platform_tenant_id == expected_platform_tenant_id
    ):
        return legacy_data_tenant_id
    return platform_tenant_id


def _verify_platform_token(token: str, client_id: str | None = None) -> VerifiedToken:
    authorization = token if token.lower().startswith("bearer ") else f"Bearer {token}"
    verifier = PlatformTokenVerifier(
        key=settings.PLATFORM_JWT_SECRET or settings.SECRET_KEY,
        algorithms=(settings.ALGORITHM,),
        leeway_seconds=settings.JWT_LEEWAY_SECONDS,
        audience=settings.JWT_AUDIENCE or None,
        issuer=settings.JWT_ISSUER or None,
        allowed_client_ids=_allowed_client_ids(),
        allowed_product_codes=(REACH_PRODUCT_CODE,),
        allowed_portal_scopes=(REACH_PORTAL_SCOPE,),
    )
    try:
        return verifier.verify_authorization(authorization, client_id=client_id)
    except PlatformAuthError as exc:
        raise CustomException(msg=exc.message, code=10401, status_code=401) from exc


def _auth_from_verified_token(
    verified: VerifiedToken,
    db: AsyncSession,
    request: Request | None = None,
) -> AuthSchema:
    context = verified.context
    platform_tenant_id = context.tenant_id
    data_tenant_id = _resolve_data_tenant_id(platform_tenant_id)
    try:
        user_id = int(context.user_id)
        dept_id = int(context.dept_id) if context.dept_id is not None else None
    except ValueError as exc:
        raise CustomException(
            msg="认证缺少用户身份，请重新登录",
            code=10401,
            status_code=401,
        ) from exc

    user = UserModel()
    user.user_id = user_id
    user.user_name = context.username
    user.nick_name = context.username
    user.tenant_id = data_tenant_id
    user.dept_id = dept_id

    if request is not None:
        request.scope.update({
            "user_id": user_id,
            "user_username": context.username,
            "username": context.username,
            "nickname": context.username,
            "tenant_id": data_tenant_id,
            "platform_tenant_id": platform_tenant_id,
            "dept_id": dept_id,
            "client_id": context.client_id,
            "product_code": context.product_code,
            "portal_scope": context.portal_scope,
        })

    return AuthSchema(
        db=db,
        check_data_scope=False,
        permissions=_subject_permissions(dict(verified.claims)),
        user=user,
    )


async def _get_development_fallback_user() -> UserModel | None:
    """使用独立只读会话查询本地开发兜底用户。"""

    async with async_db_session() as lookup_db:
        result = await lookup_db.execute(select(UserModel).limit(1))
        return result.scalar_one_or_none()


async def db_getter() -> AsyncGenerator[AsyncSession, None]:
    """获取数据库会话连接

    返回:
    - AsyncSession: 数据库会话连接
    """
    async with async_db_session() as session:
        async with session.begin():
            yield session


async def redis_getter(request: Request) -> Redis | None:
    """获取Redis连接

    参数:
    - request (Request): 请求对象

    返回:
    - Redis: Redis连接
    """
    redis = getattr(request.app.state, "redis", None)
    if settings.AI_CALL_STANDALONE_ENABLE:
        return redis
    if settings.REDIS_ENABLE and redis is None:
        raise RuntimeError("Redis 已启用，但应用连接尚未初始化")
    return redis


async def get_current_user(
    request: Request,
    db: AsyncSession = Depends(db_getter),
    redis: Redis | None = Depends(redis_getter),
    token: str | None = Depends(OAuth2Schema),
) -> AuthSchema:
    """获取当前用户

    参数:
    - request (Request): 请求对象
    - db (AsyncSession): 数据库会话
    - redis (Redis): Redis连接
    - token (str): 访问令牌

    返回:
    - AuthSchema: 认证信息模型
    """
    # 1. 未启用 JWT 且请求没有携带 token 时，才使用本地开发兜底用户。
    #    如果请求已经带了 Authorization，应优先解析真实租户上下文。
    if not settings.JWT_ENABLE and not token:
        # 关闭数据权限过滤
        auth = AuthSchema(db=db, check_data_scope=False)
        # 认证查询不能占用后续业务写入需要抢占的 SQLite 事务。
        user = await _get_development_fallback_user()
        if user:
            auth.user = user
            # 设置上下文
            request.scope["user_id"] = user.id
            request.scope["user_username"] = user.username
            request.scope["tenant_id"] = getattr(user, "tenant_id", None)
            request.scope["dept_id"] = getattr(user, "dept_id", None)
        return auth

    if not token:
        raise CustomException(msg="认证已失效", code=10401, status_code=401)

    request_headers = getattr(request, "headers", {})
    verified = _verify_platform_token(token, request_headers.get("clientid"))
    return _auth_from_verified_token(verified, db, request)


async def get_current_user_ws(
    token: str = Query(..., description="认证token"),
    db: AsyncSession = Depends(db_getter),
    redis: Redis | None = Depends(redis_getter),
) -> AuthSchema:
    """获取当前用户（WebSocket专用，从查询参数获取token）

    参数:
    - token (str): 认证token
    - db (AsyncSession): 数据库会话
    - redis (Redis): Redis连接

    返回:
    - AuthSchema: 认证信息模型
    """
    return await _verify_token(token, db, redis)


async def _verify_token(
    token: str,
    db: AsyncSession,
    redis: Redis | None,
) -> AuthSchema:
    """验证token并返回用户信息

    参数:
    - token (str): 认证token
    - db (AsyncSession): 数据库会话
    - redis (Redis): Redis连接

    返回:
    - AuthSchema: 认证信息模型
    """
    # 1. 未启用 JWT 且没有传入 token 时，才使用本地开发兜底用户。
    if not settings.JWT_ENABLE and not token:
        auth = AuthSchema(db=db, check_data_scope=False)
        user = await _get_development_fallback_user()
        if user:
            auth.user = user
        return auth

    if not token:
        raise CustomException(msg="认证已失效", code=10401, status_code=401)

    verified = _verify_platform_token(token)
    return _auth_from_verified_token(verified, db)


async def get_voice_manager(
    auth: AuthSchema = Depends(get_current_user),
) -> AuthSchema:
    if auth.user and auth.user.is_superuser:
        return auth

    allowed_permissions = {"ai_call:voice:manage", "*:*:*"}
    if settings.JWT_ENABLE and auth.permissions.isdisjoint(allowed_permissions):
        raise CustomException(
            msg="无权限操作",
            code=10403,
            status_code=403,
        )
    return auth


async def get_ai_call_manager(
    auth: AuthSchema = Depends(get_current_user),
) -> AuthSchema:
    if auth.user and auth.user.is_superuser:
        return auth

    allowed_permissions = {"ai_call:agent:manage", "*:*:*"}
    if settings.JWT_ENABLE and auth.permissions.isdisjoint(allowed_permissions):
        raise CustomException(
            msg="无权限操作",
            code=10403,
            status_code=403,
        )
    return auth


async def get_ai_call_statistics_viewer(
    auth: AuthSchema = Depends(get_current_user),
) -> AuthSchema:
    if auth.user and auth.user.is_superuser:
        return auth

    allowed_permissions = {"ai_call:statistics:view", "*:*:*"}
    if settings.JWT_ENABLE and auth.permissions.isdisjoint(allowed_permissions):
        raise CustomException(
            msg="无权限操作",
            code=10403,
            status_code=403,
        )
    return auth


async def get_ai_call_console(
    auth: AuthSchema = Depends(get_current_user),
) -> AuthSchema:
    if auth.user and auth.user.is_superuser:
        return auth

    allowed_permissions = {"ai_call:agent:console", "*:*:*"}
    if settings.JWT_ENABLE and auth.permissions.isdisjoint(allowed_permissions):
        raise CustomException(
            msg="无权限操作",
            code=10403,
            status_code=403,
        )
    return auth


async def get_knowledge_viewer(
    auth: AuthSchema = Depends(get_current_user),
) -> AuthSchema:
    if auth.user and auth.user.is_superuser:
        return auth

    allowed_permissions = {
        "ai_call:knowledge:view",
        "ai_call:knowledge:manage",
        "*:*:*",
    }
    if settings.JWT_ENABLE and auth.permissions.isdisjoint(allowed_permissions):
        raise CustomException(msg="无权限操作", code=10403, status_code=403)
    return auth


async def get_knowledge_manager(
    auth: AuthSchema = Depends(get_current_user),
) -> AuthSchema:
    if auth.user and auth.user.is_superuser:
        return auth

    allowed_permissions = {"ai_call:knowledge:manage", "*:*:*"}
    if settings.JWT_ENABLE and auth.permissions.isdisjoint(allowed_permissions):
        raise CustomException(msg="无权限操作", code=10403, status_code=403)
    return auth


async def get_prompt_manager(
    auth: AuthSchema = Depends(get_current_user),
) -> AuthSchema:
    if auth.user and getattr(auth.user, "is_superuser", False):
        return auth

    allowed_permissions = {"ai_call:prompt:manage", "*:*:*"}
    if settings.JWT_ENABLE and auth.permissions.isdisjoint(allowed_permissions):
        raise CustomException(msg="无权限操作", code=10403, status_code=403)
    return auth


class AuthPermission:
    """权限验证类"""

    def __init__(
        self,
        permissions: list[str] | None = None,
        check_data_scope: bool = True,
    ) -> None:
        """
        初始化权限验证

        参数:
        - permissions (list[str] | None): 权限标识列表。
        - check_data_scope (bool): 是否启用严格模式校验。
        """
        self.permissions = permissions or []
        self.check_data_scope = check_data_scope

    async def __call__(self, auth: AuthSchema = Depends(get_current_user)) -> AuthSchema:
        """
        调用权限验证

        参数:
        - auth (AuthSchema): 认证信息对象。

        返回:
        - AuthSchema: 认证信息对象。
        """
        auth.check_data_scope = self.check_data_scope

        # 超级管理员直接通过
        if auth.user and auth.user.is_superuser:
            return auth

        # 如果未启用JWT认证，默认放行
        if not settings.JWT_ENABLE:
            return auth

        # 只要用户存在即可
        if not auth.user:
            raise CustomException(msg="无权限操作", code=10403, status_code=403)

        return auth
