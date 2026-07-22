import json
from collections.abc import AsyncGenerator

from fastapi import Depends, Query, Request
from redis.asyncio.client import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.system.auth.schema import AuthSchema
from app.api.v1.system.user.model import UserModel
from app.config.setting import settings
from app.core.database import async_db_session
from app.core.exceptions import CustomException
from app.core.security import OAuth2Schema, decode_access_token


async def db_getter() -> AsyncGenerator[AsyncSession, None]:
    """获取数据库会话连接

    返回:
    - AsyncSession: 数据库会话连接
    """
    async with async_db_session() as session:
        async with session.begin():
            yield session


async def redis_getter(request: Request) -> Redis:
    """获取Redis连接

    参数:
    - request (Request): 请求对象

    返回:
    - Redis: Redis连接
    """
    return request.app.state.redis


async def get_current_user(
    request: Request,
    db: AsyncSession = Depends(db_getter),
    redis: Redis = Depends(redis_getter),
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
        # 尝试查询数据库第一个用户作为模拟用户
        stmt = select(UserModel).limit(1)
        result = await db.execute(stmt)
        user = result.scalar_one_or_none()
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

    # 处理Bearer token
    if token.startswith("Bearer"):
        token = token.split(" ")[1]

    payload = decode_access_token(token)
    if not payload or not hasattr(payload, "is_refresh") or payload.is_refresh:
        raise CustomException(msg="非法凭证", code=10401, status_code=401)

    online_user_info = payload.sub
    # 解析用户信息
    user_info = json.loads(online_user_info)  # 确保是字典类型

    # 获取用户基本信息
    user_id = user_info.get("user_id")
    username = user_info.get("user_name")
    tenant_id = str(user_info.get("tenant_id") or "").strip()  # 获取租户ID
    dept_id = user_info.get("dept_id")  # 获取部门ID

    if not user_id or not username:
        raise CustomException(msg="认证已失效", code=10401, status_code=401)
    if not tenant_id:
        raise CustomException(msg="租户上下文缺失，请重新登录", code=10401, status_code=401)

    # 创建一个简单的用户对象（不查询数据库）
    user = UserModel()
    user.user_id = user_id
    user.user_name = username
    user.nick_name = user_info.get("name", username)
    user.tenant_id = tenant_id  # 设置租户ID
    user.dept_id = dept_id  # 设置部门ID

    # 设置请求上下文
    request.scope["user_id"] = user_id
    request.scope["user_username"] = username
    request.scope["username"] = username
    request.scope["nickname"] = user.nick_name
    request.scope["tenant_id"] = tenant_id
    request.scope["dept_id"] = dept_id

    # 创建认证对象
    auth = AuthSchema(db=db, check_data_scope=False)
    auth.user = user
    return auth


async def get_current_user_ws(
    token: str = Query(..., description="认证token"),
    db: AsyncSession = Depends(db_getter),
    redis: Redis = Depends(redis_getter),
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
    redis: Redis,
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
        stmt = select(UserModel).limit(1)
        result = await db.execute(stmt)
        user = result.scalar_one_or_none()
        if user:
            auth.user = user
        return auth

    if not token:
        raise CustomException(msg="认证已失效", code=10401, status_code=401)

    # 处理Bearer token（如果通过查询参数传递时包含Bearer前缀）
    if token.startswith("Bearer"):
        token = token.split(" ")[1]

    payload = decode_access_token(token)
    if not payload or not hasattr(payload, "is_refresh") or payload.is_refresh:
        raise CustomException(msg="非法凭证", code=10401, status_code=401)

    online_user_info = payload.sub
    # 解析用户信息
    user_info = json.loads(online_user_info)  # 确保是字典类型

    # 获取用户基本信息
    user_id = user_info.get("user_id")
    username = user_info.get("user_name")
    tenant_id = str(user_info.get("tenant_id") or "").strip()  # 获取租户ID
    dept_id = user_info.get("dept_id")  # 获取部门ID

    if not user_id or not username:
        raise CustomException(msg="认证已失效", code=10401, status_code=401)
    if not tenant_id:
        raise CustomException(msg="租户上下文缺失，请重新登录", code=10401, status_code=401)

    # 创建一个简单的用户对象（不查询数据库）
    user = UserModel()
    user.user_id = user_id
    user.user_name = username
    user.nick_name = user_info.get("name", username)
    user.tenant_id = tenant_id
    user.dept_id = dept_id

    auth = AuthSchema(db=db, check_data_scope=False)
    auth.user = user
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
