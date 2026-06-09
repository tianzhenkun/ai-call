from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from redis.asyncio.client import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.common.response import ErrorResponse, SuccessResponse
from app.config.setting import settings
from app.core.dependencies import db_getter, get_current_user, redis_getter
from app.core.logger import log
from app.core.router_class import OperationLogRoute
from app.core.security import CustomOAuth2PasswordRequestForm

from .schema import (
    AuthSchema,
    AutoLoginTokenSchema,
    AutoLoginUserSchema,
    CaptchaOutSchema,
    JWTOutSchema,
    LogoutPayloadSchema,
    RefreshTokenPayloadSchema,
)
from .service import AutoLoginService, CaptchaService, LoginService

AuthRouter = APIRouter(route_class=OperationLogRoute, prefix="/auth", tags=["认证授权"])


@AuthRouter.post(
    "/login",
    summary="登录",
    description="登录",
    response_model=JWTOutSchema,
)
async def login_for_access_token_controller(
    request: Request,
    redis: Annotated[Redis, Depends(redis_getter)],
    login_form: Annotated[CustomOAuth2PasswordRequestForm, Depends()],
    db: Annotated[AsyncSession, Depends(db_getter)],
) -> JSONResponse | dict:
    """
    用户登录

    参数:
    - request (Request): FastAPI请求对象
    - login_form (CustomOAuth2PasswordRequestForm): 登录表单数据
    - db (AsyncSession): 数据库会话对象

    返回:
    - JWTOutSchema: 包含访问令牌和刷新令牌的响应模型

    异常:
    - CustomException: 认证失败时抛出异常。
    """
    login_token = await LoginService.authenticate_user_service(
        request=request, redis=redis, login_form=login_form, db=db
    )

    log.info(f"用户{login_form.username}登录成功")

    # 如果是文档请求，则不记录日志:http://localhost:8000/api/v1/docs
    if settings.DOCS_URL in request.headers.get("referer", ""):
        return login_token.model_dump()
    return SuccessResponse(data=login_token.model_dump(), msg="登录成功")


@AuthRouter.post(
    "/token/refresh",
    summary="刷新token",
    description="刷新token",
    response_model=JWTOutSchema,
    dependencies=[Depends(get_current_user)],
)
async def get_new_token_controller(
    request: Request,
    payload: RefreshTokenPayloadSchema,
    db: Annotated[AsyncSession, Depends(db_getter)],
    redis: Annotated[Redis, Depends(redis_getter)],
) -> JSONResponse:
    """
    刷新token

    参数:
    - request (Request): FastAPI请求对象
    - payload (RefreshTokenPayloadSchema): 刷新令牌负载模型

    返回:
    - JWTOutSchema: 包含新的访问令牌和刷新令牌的响应模型

    异常:
    - CustomException: 刷新令牌失败时抛出异常。
    """
    # 解析当前的访问Token以获取用户名
    new_token = await LoginService.refresh_token_service(
        db=db, request=request, redis=redis, refresh_token=payload
    )
    token_dict = new_token.model_dump()
    log.info(f"刷新token成功: {token_dict}")
    return SuccessResponse(data=token_dict, msg="刷新成功")


@AuthRouter.get(
    "/captcha/get",
    summary="获取验证码",
    description="获取登录验证码",
    response_model=CaptchaOutSchema,
)
async def get_captcha_for_login_controller(
    redis: Annotated[Redis, Depends(redis_getter)],
) -> JSONResponse:
    """
    获取登录验证码

    参数:
    - redis (Redis): Redis客户端对象

    返回:
    - CaptchaOutSchema: 包含验证码图片和key的响应模型

    异常:
    - CustomException: 获取验证码失败时抛出异常。
    """
    # 获取验证码
    captcha = await CaptchaService.get_captcha_service(redis=redis)
    log.info("获取验证码成功")
    return SuccessResponse(data=captcha, msg="获取验证码成功")


@AuthRouter.post(
    "/logout",
    summary="退出登录",
    description="退出登录",
    dependencies=[Depends(get_current_user)],
)
async def logout_controller(
    payload: LogoutPayloadSchema,
    redis: Annotated[Redis, Depends(redis_getter)],
) -> JSONResponse:
    """
    退出登录

    参数:
    - payload (LogoutPayloadSchema): 退出登录负载模型
    - redis (Redis): Redis客户端对象

    返回:
    - JSONResponse: 包含退出登录结果的响应模型

    异常:
    - CustomException: 退出登录失败时抛出异常。
    """
    if await LoginService.logout_service(redis=redis, token=payload):
        log.info("退出成功")
        return SuccessResponse(msg="退出成功")
    return ErrorResponse(msg="退出失败")


@AuthRouter.get(
    "/auto-login/users",
    summary="获取免登录用户列表",
    description="获取可用于免登录快速登录的用户列表",
    response_model=list[AutoLoginUserSchema],
)
async def get_auto_login_users_controller(
    db: Annotated[AsyncSession, Depends(db_getter)],
) -> JSONResponse:
    """
    获取免登录用户列表

    参数:
    - db (AsyncSession): 数据库会话对象

    返回:
    - list[AutoLoginUserSchema]: 免登录用户列表
    """
    users = await AutoLoginService.get_auto_login_users_service(db=db)
    return SuccessResponse(data=users, msg="获取成功")


@AuthRouter.post(
    "/auto-login/token",
    summary="获取免登录Token",
    description="根据用户ID生成免登录Token",
    response_model=AutoLoginTokenSchema,
)
async def get_auto_login_token_controller(
    redis: Annotated[Redis, Depends(redis_getter)],
    db: Annotated[AsyncSession, Depends(db_getter)],
    user_id: int,
) -> JSONResponse:
    """
    获取免登录Token

    参数:
    - redis (Redis): Redis客户端对象
    - db (AsyncSession): 数据库会话对象
    - user_id (int): 用户ID

    返回:
    - AutoLoginTokenSchema: 免登录Token和用户信息
    """
    result = await AutoLoginService.create_auto_login_token_service(
        redis=redis, db=db, user_id=user_id
    )
    return SuccessResponse(data=result, msg="获取成功")


@AuthRouter.post(
    "/auto-login",
    summary="免登录",
    description="使用免登录Token快速登录",
    response_model=JWTOutSchema,
)
async def auto_login_controller(
    request: Request,
    redis: Annotated[Redis, Depends(redis_getter)],
    db: Annotated[AsyncSession, Depends(db_getter)],
    token: str,
) -> JSONResponse:
    """
    免登录

    参数:
    - request (Request): FastAPI请求对象
    - redis (Redis): Redis客户端对象
    - db (AsyncSession): 数据库会话对象
    - token (str): 免登录Token

    返回:
    - JWTOutSchema: JWT令牌信息
    """
    login_token = await AutoLoginService.auto_login_service(
        request=request, redis=redis, db=db, token=token
    )
    log.info("用户免登录成功")
    return SuccessResponse(data=login_token.model_dump(), msg="登录成功")


@AuthRouter.get(
    "/test/token/verify",
    summary="验证Token并获取用户信息",
    description="验证Token并从上下文中获取用户信息",
)
async def verify_token(request: Request, auth: AuthSchema = Depends(get_current_user)):
    """
    验证Token并从上下文中获取用户信息
    
    测试步骤：
    1. 通过Postman发送GET请求到 /api/v1/system/auth/test/token/verify
    2. 在请求头中添加 Authorization: Bearer <token>
    3. token 为 JWT Token
    
    返回：
    - 从上下文中获取的用户信息
    - 从AuthSchema中获取的用户信息
    """
    # 从上下文获取用户信息
    context_user_id = request.scope.get("user_id")
    context_username = request.scope.get("user_username")
    context_tenant_id = request.scope.get("tenant_id")
    context_dept_id = request.scope.get("dept_id")
    context_name = request.scope.get("name")  # 获取昵称
    
    # 从AuthSchema获取用户信息
    auth_user_id = auth.user.id if auth.user else None
    auth_username = auth.user.username if auth.user else None
    auth_name = auth.user.name if auth.user else None
    auth_nickname = auth.user.nick_name if auth.user else None  # 获取昵称
    auth_tenant_id = auth.user.tenant_id if auth.user else None
    auth_dept_id = auth.user.dept_id if auth.user else None
    
    return SuccessResponse(
        data={
            "message": "Token验证成功",
            "context_user_info": {
                "user_id": context_user_id,
                "username": context_username,
                "name": context_name,
                "tenant_id": context_tenant_id,
                "dept_id": context_dept_id
            },
            "auth_user_info": {
                "user_id": auth_user_id,
                "username": auth_username,
                "name": auth_name,
                "nickname": auth_nickname,  # 添加昵称
                "tenant_id": auth_tenant_id,
                "dept_id": auth_dept_id
            },
            "token_type": "JWT"
        },
        msg="Token验证成功"
    )


@AuthRouter.get(
    "/test/token/context",
    summary="从上下文中获取用户信息",
    description="仅从上下文中获取用户信息（需要先通过认证中间件）",
    dependencies=[Depends(get_current_user)],
)
async def get_context_info(request: Request):
    """
    仅从上下文中获取用户信息（需要先通过认证中间件）
    
    测试步骤：
    1. 通过Postman发送GET请求到 /api/v1/system/auth/test/token/context
    2. 在请求头中添加 Authorization: Bearer <token>
    3. token 为 JWT Token
    
    返回：
    - 从上下文中获取的用户信息
    """
    # 从上下文获取用户信息
    user_id = request.scope.get("user_id")
    username = request.scope.get("user_username")
    tenant_id = request.scope.get("tenant_id")
    dept_id = request.scope.get("dept_id")
    
    if not user_id or not username:
        return ErrorResponse(msg="未认证或上下文信息缺失")
    
    return SuccessResponse(
        data={
            "message": "从上下文获取用户信息成功",
            "user_info": {
                "user_id": user_id,
                "username": username,
                "tenant_id": tenant_id,
                "dept_id": dept_id
            },
            "status": "success"
        },
        msg="获取用户信息成功"
    )
