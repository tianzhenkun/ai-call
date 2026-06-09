from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query
from fastapi.responses import JSONResponse

from app.api.v1.system.auth.schema import AuthSchema
from app.common.response import ResponseSchema, SuccessResponse, TableResponse
from app.core.dependencies import get_current_user
from app.core.logger import log
from app.core.router_class import OperationLogRoute

from .crud import UserCRUD
from .schema import UserOutSchema
from .service import UserService

UserRouter = APIRouter(route_class=OperationLogRoute, prefix="/user", tags=["用户管理"])


@UserRouter.get(
    "/current/info",
    summary="查询当前用户信息",
    description="查询当前用户信息",
    response_model=ResponseSchema[UserOutSchema],
)
async def get_current_user_info_controller(
    auth: Annotated[AuthSchema, Depends(get_current_user)],
) -> JSONResponse:
    """
    查询当前用户信息

    参数:
    - auth (AuthSchema): 认证信息模型

    返回:
    - JSONResponse: 当前用户信息JSON响应
    """
    result_dict = await UserService.get_current_user_info_service(auth=auth)
    log.info("获取当前用户信息成功")
    return SuccessResponse(data=result_dict, msg="获取当前用户信息成功")


@UserRouter.get(
    "/list",
    summary="查询用户列表",
    description="查询用户列表",
)
async def get_obj_list_controller(
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    status: Annotated[str | None, Query(description="状态过滤")] = None,
    dept_id: Annotated[int | None, Query(description="部门ID过滤")] = None,
    keyword: Annotated[str | None, Query(description="关键词搜索")] = None,
    page_num: Annotated[int, Query(description="页码", ge=1)] = 1,
    page_size: Annotated[int, Query(description="每页数量", ge=1, le=100)] = 10,
) -> JSONResponse:
    """
    查询用户列表

    参数:
    - auth (AuthSchema): 认证信息模型
    - status (str | None): 状态过滤
    - dept_id (int | None): 部门ID过滤
    - keyword (str | None): 关键词搜索
    - page_num (int): 页码
    - page_size (int): 每页数量

    返回:
    - JSONResponse: 分页查询结果JSON响应
    """
    result = await UserCRUD(auth).get_page_crud(
        page_num=page_num,
        page_size=page_size,
        status=status,
        dept_id=dept_id,
        keyword=keyword,
    )
    
    rows = [
        UserOutSchema.model_validate(user).model_dump(by_alias=True)
        for user in result.get("rows", [])
    ]
    
    log.info("查询用户成功")
    return TableResponse(rows=rows, total=result.get("total", 0), msg="查询成功")


@UserRouter.get(
    "/detail/{id}",
    summary="查询用户详情",
    description="查询用户详情",
    response_model=ResponseSchema[UserOutSchema],
)
async def get_obj_detail_controller(
    id: Annotated[int, Path(description="用户ID")],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
) -> JSONResponse:
    """
    查询用户详情

    参数:
    - id (int): 用户ID
    - auth (AuthSchema): 认证信息模型

    返回:
    - JSONResponse: 用户详情JSON响应
    """
    result_dict = await UserService.get_detail_by_id_service(user_id=id, auth=auth)
    log.info(f"获取用户详情成功 {id}")
    return SuccessResponse(data=result_dict, msg="获取用户详情成功")
