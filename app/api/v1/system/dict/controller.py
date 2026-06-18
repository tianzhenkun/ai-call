from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query
from fastapi.responses import JSONResponse
from redis.asyncio.client import Redis

from app.api.v1.system.auth.schema import AuthSchema
from app.common.response import ResponseSchema, SuccessResponse, TableResponse
from app.core.base_params import PaginationQueryParam
from app.core.dependencies import get_current_user, redis_getter
from app.core.logger import log
from app.core.router_class import OperationLogRoute

from .schema import (
    DictDataOutSchema,
    DictDataQueryParam,
)
from .service import DictDataService

DictRouter = APIRouter(route_class=OperationLogRoute, prefix="/dict", tags=["字典管理"])


@DictRouter.get(
    "/data/list",
    summary="查询字典数据",
    description="查询字典数据",
)
async def get_data_list_controller(
    page: Annotated[PaginationQueryParam, Depends()],
    search: Annotated[DictDataQueryParam, Depends()],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
) -> JSONResponse:
    """
    查询字典数据

    参数:
    - page (PaginationQueryParam): 分页查询参数模型
    - search (DictDataQueryParam): 查询参数模型
    - auth (AuthSchema): 认证信息模型

    返回:
    - JSONResponse: 包含字典数据列表的响应模型
    """
    order_by = [{"dict_sort": "asc"}]
    if page.order_by:
        order_by = page.order_by
    result_dict_list = await DictDataService.get_obj_list_service(
        auth=auth, search=search, order_by=order_by
    )
    total = len(result_dict_list)
    start = (page.page_num - 1) * page.page_size
    end = min(start + page.page_size, total)
    rows = result_dict_list[start:end]
    log.info("查询字典数据列表成功")
    return TableResponse(rows=rows, total=total, msg="查询成功")


@DictRouter.get(
    "/data/type/{dict_type}",
    summary="根据字典类型获取数据列表",
    description="根据字典类型获取数据列表",
    response_model=ResponseSchema[list[DictDataOutSchema]],
)
async def get_data_by_type_controller(
    dict_type: Annotated[str, Path(description="字典类型")],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
) -> JSONResponse:
    """
    根据字典类型获取数据列表

    参数:
    - dict_type (str): 字典类型
    - auth (AuthSchema): 认证信息模型

    返回:
    - JSONResponse: 包含字典数据列表的响应模型
    """
    result_list = await DictDataService.get_obj_list_by_dict_type_service(
        auth=auth, dict_type=dict_type
    )
    log.info(f"根据字典类型获取数据成功: {dict_type}")
    return SuccessResponse(data=result_list, msg="获取字典数据成功")


@DictRouter.get(
    "/data/info/{dict_type}",
    summary="根据字典类型获取数据（从缓存）",
    description="根据字典类型获取数据（从缓存）",
    response_model=ResponseSchema[list[DictDataOutSchema]],
)
async def get_init_dict_data_controller(
    dict_type: str, redis: Annotated[Redis, Depends(redis_getter)]
) -> JSONResponse:
    """
    根据字典类型获取数据（从缓存）

    参数:
    - dict_type (str): 字典类型
    - redis (Redis): Redis数据库连接

    返回:
    - JSONResponse: 包含字典数据列表的响应模型
    """
    dict_data_query_result = await DictDataService.get_init_dict_service(
        redis=redis, dict_type=dict_type
    )
    log.info(f"获取字典数据成功：{dict_type}")
    return SuccessResponse(data=dict_data_query_result, msg="获取字典数据成功")


@DictRouter.get(
    "/data/label",
    summary="根据字典值获取标签",
    description="根据字典类型和字典值获取字典标签",
    response_model=ResponseSchema[str | None],
)
async def get_label_by_value_controller(
    dict_type: Annotated[str, Query(description="字典类型")],
    dict_value: Annotated[str, Query(description="字典值")],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
) -> JSONResponse:
    """
    根据字典类型和字典值获取字典标签

    参数:
    - dict_type (str): 字典类型
    - dict_value (str): 字典值
    - auth (AuthSchema): 认证信息模型

    返回:
    - JSONResponse: 包含字典标签的响应模型
    """
    label = await DictDataService.get_dict_label_by_value(
        auth=auth, dict_type=dict_type, dict_value=dict_value
    )
    return SuccessResponse(data=label, msg="获取字典标签成功")


@DictRouter.get(
    "/data/value",
    summary="根据标签获取字典值",
    description="根据字典类型和字典标签获取字典值",
    response_model=ResponseSchema[str | None],
)
async def get_value_by_label_controller(
    dict_type: Annotated[str, Query(description="字典类型")],
    dict_label: Annotated[str, Query(description="字典标签")],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
) -> JSONResponse:
    """
    根据字典类型和字典标签获取字典值

    参数:
    - dict_type (str): 字典类型
    - dict_label (str): 字典标签
    - auth (AuthSchema): 认证信息模型

    返回:
    - JSONResponse: 包含字典值的响应模型
    """
    value = await DictDataService.get_dict_value_by_label(
        auth=auth, dict_type=dict_type, dict_label=dict_label
    )
    return SuccessResponse(data=value, msg="获取字典值成功")


@DictRouter.post(
    "/data/labels/batch",
    summary="批量根据字典值获取标签",
    description="批量根据字典值获取字典标签",
    response_model=ResponseSchema[dict[str, str]],
)
async def batch_get_labels_by_values_controller(
    dict_type: Annotated[str, Query(description="字典类型")],
    dict_values: Annotated[list[str], Query(description="字典值列表")],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
) -> JSONResponse:
    """
    批量根据字典值获取字典标签

    参数:
    - dict_type (str): 字典类型
    - dict_values (list[str]): 字典值列表
    - auth (AuthSchema): 认证信息模型

    返回:
    - JSONResponse: 包含 {dict_value: dict_label} 映射的响应模型
    """
    result = await DictDataService.batch_get_dict_labels_by_values(
        auth=auth, dict_type=dict_type, dict_values=dict_values
    )
    return SuccessResponse(data=result, msg="批量获取字典标签成功")


@DictRouter.post(
    "/data/values/batch",
    summary="批量根据标签获取字典值",
    description="批量根据字典标签获取字典值",
    response_model=ResponseSchema[dict[str, str]],
)
async def batch_get_values_by_labels_controller(
    dict_type: Annotated[str, Query(description="字典类型")],
    dict_labels: Annotated[list[str], Query(description="字典标签列表")],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
) -> JSONResponse:
    """
    批量根据字典标签获取字典值

    参数:
    - dict_type (str): 字典类型
    - dict_labels (list[str]): 字典标签列表
    - auth (AuthSchema): 认证信息模型

    返回:
    - JSONResponse: 包含 {dict_label: dict_value} 映射的响应模型
    """
    result = await DictDataService.batch_get_dict_values_by_labels(
        auth=auth, dict_type=dict_type, dict_labels=dict_labels
    )
    return SuccessResponse(data=result, msg="批量获取字典值成功")
