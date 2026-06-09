from typing import Annotated

from fastapi import APIRouter, Depends, File, Path, Query, UploadFile
from fastapi.responses import JSONResponse

from app.api.v1.system.auth.schema import AuthSchema
from app.common.response import ResponseSchema, SuccessResponse
from app.core.dependencies import get_current_user
from app.core.logger import log

from .schema import OssOutSchema, OssUrlOutSchema
from .service import OssService

OssRouter = APIRouter(prefix="/oss", tags=["对象存储管理"])


@OssRouter.get(
    "/{oss_id}",
    summary="根据oss_id获取OSS对象",
    description="根据oss_id获取OSS对象完整信息",
    response_model=ResponseSchema[OssOutSchema],
)
async def get_by_oss_id_controller(
    oss_id: Annotated[int, Path(description="对象存储主键", gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
) -> JSONResponse:
    """
    根据oss_id获取OSS对象完整信息

    参数:
    - oss_id (int): 对象存储主键
    - auth (AuthSchema): 认证信息模型

    返回:
    - JSONResponse: 包含OSS对象完整信息的响应模型
    """
    result = await OssService.get_by_oss_id_service(auth=auth, oss_id=oss_id)
    log.info(f"根据oss_id获取OSS对象成功: {oss_id}")
    return SuccessResponse(data=result, msg="获取OSS对象成功")


@OssRouter.get(
    "/url/{oss_id}",
    summary="根据oss_id获取URL（常用）",
    description="根据oss_id获取URL信息，常用于获取文件访问地址",
    response_model=ResponseSchema[OssUrlOutSchema],
)
async def get_url_by_oss_id_controller(
    oss_id: Annotated[int, Path(description="对象存储主键", gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
) -> JSONResponse:
    """
    根据oss_id获取URL信息，常用于获取文件访问地址

    参数:
    - oss_id (int): 对象存储主键
    - auth (AuthSchema): 认证信息模型

    返回:
    - JSONResponse: 包含url、originalName、fileSuffix的响应模型
    """
    result = await OssService.get_url_by_oss_id_service(auth=auth, oss_id=oss_id)
    log.info(f"根据oss_id获取URL成功: {oss_id}")
    return SuccessResponse(data=result, msg="获取URL成功")


@OssRouter.post(
    "/list",
    summary="批量根据oss_id列表获取OSS对象",
    description="批量根据oss_id列表获取OSS对象完整信息",
    response_model=ResponseSchema[list[OssOutSchema]],
)
async def get_list_by_oss_ids_controller(
    oss_ids: Annotated[list[int], Query(description="对象存储主键列表")],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
) -> JSONResponse:
    """
    批量根据oss_id列表获取OSS对象完整信息

    参数:
    - oss_ids (list[int]): 对象存储主键列表
    - auth (AuthSchema): 认证信息模型

    返回:
    - JSONResponse: 包含OSS对象完整信息列表的响应模型
    """
    result = await OssService.get_list_by_oss_ids_service(auth=auth, oss_ids=oss_ids)
    log.info(f"批量获取OSS对象成功: {len(result)} 条")
    return SuccessResponse(data=result, msg="批量获取OSS对象成功")


@OssRouter.post(
    "/url/list",
    summary="批量根据oss_id列表获取URL（常用）",
    description="批量根据oss_id列表获取URL信息，常用于获取多个文件访问地址",
    response_model=ResponseSchema[list[OssUrlOutSchema]],
)
async def get_url_list_by_oss_ids_controller(
    oss_ids: Annotated[list[int], Query(description="对象存储主键列表")],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
) -> JSONResponse:
    """
    批量根据oss_id列表获取URL信息，常用于获取多个文件访问地址

    参数:
    - oss_ids (list[int]): 对象存储主键列表
    - auth (AuthSchema): 认证信息模型

    返回:
    - JSONResponse: 包含url、originalName、fileSuffix信息列表的响应模型
    """
    result = await OssService.get_url_list_by_oss_ids_service(auth=auth, oss_ids=oss_ids)
    log.info(f"批量获取URL成功: {len(result)} 条")
    return SuccessResponse(data=result, msg="批量获取URL成功")


@OssRouter.post(
    "/upload",
    summary="上传文件到OSS",
    description="上传文件到 MinIO，写入 sys_oss 记录，返回 oss_id",
)
async def upload_file_controller(
    file: Annotated[UploadFile, File(description="上传的文件")],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
) -> JSONResponse:
    data = await file.read()
    content_type = file.content_type or "application/octet-stream"
    oss_id = await OssService.upload_service(
        auth=auth,
        data=data,
        original_filename=file.filename or "upload",
        content_type=content_type,
    )
    log.info(f"文件上传成功: {file.filename}, oss_id: {oss_id}")
    return SuccessResponse(data={"ossId": oss_id}, msg="上传成功")
