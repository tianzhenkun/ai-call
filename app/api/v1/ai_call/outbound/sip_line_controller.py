from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query
from fastapi.responses import JSONResponse

from app.api.v1.system.auth.schema import AuthSchema
from app.common.response import ResponseSchema, SuccessResponse, TableResponse
from app.core.dependencies import get_current_user

from .controller import _identity
from .sip_line_schema import SipLineHealthOut, SipLineIn, SipLineOut
from .sip_line_service import SipLineService

OutboundSipLineRouter = APIRouter(tags=["通用外呼线路"])
_default_service = SipLineService()


def get_sip_line_service() -> SipLineService:
    return _default_service


@OutboundSipLineRouter.get(
    "/outbound-lines",
    summary="分页查询 SIP 外呼线路",
)
async def list_sip_lines_controller(
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[SipLineService, Depends(get_sip_line_service)],
    page_num: Annotated[int, Query(alias="pageNum", ge=1)] = 1,
    page_size: Annotated[int, Query(alias="pageSize", ge=1, le=200)] = 20,
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    rows, total = await service.list_lines(
        auth.db,
        tenant_id,
        page_num=page_num,
        page_size=page_size,
    )
    return TableResponse(rows=rows, total=total, msg="查询成功")


@OutboundSipLineRouter.get(
    "/outbound-lines/{line_id}",
    summary="查询 SIP 外呼线路详情",
    response_model=ResponseSchema[SipLineOut],
)
async def get_sip_line_controller(
    line_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[SipLineService, Depends(get_sip_line_service)],
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    line = await service.get_line(auth.db, tenant_id, line_id)
    return SuccessResponse(data=service.line_out(line), msg="查询成功")


@OutboundSipLineRouter.post(
    "/outbound-lines",
    summary="创建 SIP 外呼线路",
    response_model=ResponseSchema[SipLineOut],
)
async def create_sip_line_controller(
    request: SipLineIn,
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[SipLineService, Depends(get_sip_line_service)],
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    line = await service.create_line(auth.db, tenant_id, user_id, request)
    await auth.db.commit()
    return SuccessResponse(data=service.line_out(line), msg="创建成功")


@OutboundSipLineRouter.put(
    "/outbound-lines/{line_id}",
    summary="修改 SIP 外呼线路",
    response_model=ResponseSchema[SipLineOut],
)
async def update_sip_line_controller(
    line_id: Annotated[int, Path(gt=0)],
    request: SipLineIn,
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[SipLineService, Depends(get_sip_line_service)],
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    line = await service.update_line(
        auth.db,
        tenant_id,
        user_id,
        line_id,
        request,
    )
    await auth.db.commit()
    return SuccessResponse(data=service.line_out(line), msg="修改成功")


@OutboundSipLineRouter.post(
    "/outbound-lines/{line_id}/set-default",
    summary="设为默认 SIP 外呼线路",
    response_model=ResponseSchema[SipLineOut],
)
async def set_default_sip_line_controller(
    line_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[SipLineService, Depends(get_sip_line_service)],
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    line = await service.set_default(auth.db, tenant_id, user_id, line_id)
    await auth.db.commit()
    return SuccessResponse(data=service.line_out(line), msg="已设为默认线路")


@OutboundSipLineRouter.post(
    "/outbound-lines/{line_id}/enable",
    summary="启用 SIP 外呼线路",
    response_model=ResponseSchema[SipLineOut],
)
async def enable_sip_line_controller(
    line_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[SipLineService, Depends(get_sip_line_service)],
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    line = await service.enable(auth.db, tenant_id, user_id, line_id)
    await auth.db.commit()
    return SuccessResponse(data=service.line_out(line), msg="启用成功")


@OutboundSipLineRouter.post(
    "/outbound-lines/{line_id}/disable",
    summary="停用 SIP 外呼线路",
    response_model=ResponseSchema[SipLineOut],
)
async def disable_sip_line_controller(
    line_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[SipLineService, Depends(get_sip_line_service)],
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    line = await service.disable(auth.db, tenant_id, user_id, line_id)
    await auth.db.commit()
    return SuccessResponse(data=service.line_out(line), msg="停用成功")


@OutboundSipLineRouter.post(
    "/outbound-lines/{line_id}/preflight",
    summary="执行 SIP 外呼线路非拨号预检",
    response_model=ResponseSchema[SipLineHealthOut],
)
async def preflight_sip_line_controller(
    line_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[SipLineService, Depends(get_sip_line_service)],
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    result = await service.preflight(auth.db, tenant_id, user_id, line_id)
    await auth.db.commit()
    return SuccessResponse(data=result, msg="线路预检完成")


@OutboundSipLineRouter.delete(
    "/outbound-lines/{line_id}",
    summary="软删除 SIP 外呼线路",
)
async def delete_sip_line_controller(
    line_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[SipLineService, Depends(get_sip_line_service)],
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    await service.delete(auth.db, tenant_id, user_id, line_id)
    await auth.db.commit()
    return SuccessResponse(msg="删除成功")
