from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Header, Path
from fastapi.responses import JSONResponse

from app.api.v1.system.auth.schema import AuthSchema
from app.common.response import ResponseSchema, SuccessResponse
from app.core.database import async_db_session
from app.core.dependencies import get_current_user

from .controller import _identity
from .linphone_test_schema import (
    LinphoneTestAcceptedOut,
    LinphoneTestCapabilityOut,
    LinphoneTestRunIn,
    LinphoneTestStatusOut,
)
from .linphone_test_service import LinphoneTestService
from .rule_task_schema import AcceptedCommandOut

LinphoneTestRouter = APIRouter(
    prefix="/lab/outbound-task-tests",
    tags=["通话测试台 Linphone 适配验证"],
)
_default_linphone_test_service: LinphoneTestService | None = None


def get_linphone_test_service() -> LinphoneTestService:
    global _default_linphone_test_service
    if _default_linphone_test_service is None:
        _default_linphone_test_service = LinphoneTestService(async_db_session)
    return _default_linphone_test_service


@LinphoneTestRouter.get(
    "/{task_id}/capability",
    summary="查询本机 Linphone 测试资格",
    response_model=ResponseSchema[LinphoneTestCapabilityOut],
)
async def get_test_capability_controller(
    task_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[
        LinphoneTestService,
        Depends(get_linphone_test_service),
    ],
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    result = await service.get_capability(auth.db, tenant_id, task_id)
    return SuccessResponse(data=result, msg="查询成功")


@LinphoneTestRouter.post(
    "/{task_id}/runs",
    summary="启动本机 Linphone 测试",
    response_model=ResponseSchema[LinphoneTestAcceptedOut],
)
async def run_linphone_test_controller(
    task_id: Annotated[int, Path(gt=0)],
    request: LinphoneTestRunIn,
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    service: Annotated[
        LinphoneTestService,
        Depends(get_linphone_test_service),
    ],
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    result = await service.start_test(
        tenant_id=tenant_id,
        task_id=task_id,
        idempotency_key=idempotency_key,
        scenario=request.scenario.value,
    )
    return SuccessResponse(data=result, msg="测试拨打已受理")


@LinphoneTestRouter.get(
    "/{task_id}/status",
    summary="查询本机 Linphone 测试状态",
    response_model=ResponseSchema[LinphoneTestStatusOut],
)
async def get_test_status_controller(
    task_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[
        LinphoneTestService,
        Depends(get_linphone_test_service),
    ],
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    result = await service.get_status(auth.db, tenant_id, task_id)
    return SuccessResponse(data=result, msg="查询成功")


@LinphoneTestRouter.post(
    "/{task_id}/active-call/end",
    summary="结束本机 Linphone 测试当前通话",
    response_model=ResponseSchema[AcceptedCommandOut],
)
async def end_active_test_call_controller(
    task_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    service: Annotated[
        LinphoneTestService,
        Depends(get_linphone_test_service),
    ],
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    result = await service.end_active_call(
        tenant_id=tenant_id,
        task_id=task_id,
        idempotency_key=idempotency_key,
    )
    return SuccessResponse(data=result, msg="结束通话命令已受理")
