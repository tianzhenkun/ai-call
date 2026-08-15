from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, Path, Query, Request, UploadFile, status
from fastapi.responses import FileResponse, JSONResponse
from pydantic import ValidationError
from starlette.background import BackgroundTask

from app.api.v1.system.auth.schema import AuthSchema
from app.common.response import ResponseSchema, SuccessResponse, TableResponse
from app.core.database import async_db_session
from app.core.dependencies import get_current_user
from app.core.exceptions import CustomException

from .schema import BatchValidationRequest, ValidationResultOut
from .service import OutboundValidationService

OutboundValidationRouter = APIRouter(tags=["通用外呼名单校验"])
_default_service = OutboundValidationService(async_db_session)


def get_outbound_validation_service() -> OutboundValidationService:
    return _default_service


def _identity(auth: AuthSchema) -> tuple[str, int]:
    if auth.user is None:
        raise CustomException(
            msg="认证已失效",
            code=10401,
            status_code=status.HTTP_401_UNAUTHORIZED,
        )
    tenant_id = str(getattr(auth.user, "tenant_id", None) or "").strip()
    user_id = getattr(auth.user, "user_id", None)
    if not tenant_id or user_id is None:
        raise CustomException(
            msg="租户上下文缺失，请重新登录",
            code=10401,
            status_code=status.HTTP_401_UNAUTHORIZED,
        )
    return tenant_id, int(user_id)


def _parse_request(value: str) -> BatchValidationRequest:
    try:
        return BatchValidationRequest.model_validate_json(value)
    except ValidationError as exc:
        first_error = exc.errors()[0].get("msg", "request JSON 不合法")
        raise CustomException(
            msg=f"任务配置不合法：{first_error}",
            status_code=status.HTTP_400_BAD_REQUEST,
        ) from exc


@OutboundValidationRouter.post(
    "/outbound-validations/batch",
    summary="上传并异步校验批量外呼名单",
    response_model=ResponseSchema[ValidationResultOut],
)
async def create_batch_validation_controller(
    http_request: Request,
    file: Annotated[UploadFile, File(description="单个 .xlsx 名单文件")],
    request_json: Annotated[str, Form(alias="request", description="任务配置 JSON")],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[
        OutboundValidationService,
        Depends(get_outbound_validation_service),
    ],
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    form = await http_request.form()
    if len(form.getlist("file")) != 1:
        raise CustomException(
            msg="每次只能上传单个 .xlsx 名单文件",
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    request = _parse_request(request_json)
    validation = await service.accept_batch(
        db=auth.db,
        tenant_id=tenant_id,
        user_id=user_id,
        file=file,
        request=request,
    )
    try:
        await auth.db.commit()
    except Exception:
        service._delete_temp_file(validation.temp_file_path)
        raise
    service.schedule_validation(tenant_id, validation.id)
    return SuccessResponse(
        data=service.result_out(validation),
        msg="名单校验已受理",
    )


@OutboundValidationRouter.get(
    "/outbound-validations/{validation_id}",
    summary="查询批量名单校验状态",
    response_model=ResponseSchema[ValidationResultOut],
)
async def get_validation_controller(
    validation_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[
        OutboundValidationService,
        Depends(get_outbound_validation_service),
    ],
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    result = await service.get_result(auth.db, tenant_id, validation_id)
    return SuccessResponse(data=result, msg="查询成功")


@OutboundValidationRouter.get(
    "/outbound-validations/{validation_id}/issues",
    summary="分页查询名单问题行",
)
async def list_validation_issues_controller(
    validation_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[
        OutboundValidationService,
        Depends(get_outbound_validation_service),
    ],
    page_num: Annotated[int, Query(alias="pageNum", ge=1)] = 1,
    page_size: Annotated[int, Query(alias="pageSize", ge=1, le=200)] = 20,
    phone_number: Annotated[str | None, Query(alias="phoneNumber")] = None,
    reason: Annotated[str | None, Query()] = None,
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    rows, total = await service.list_issues(
        auth.db,
        tenant_id,
        validation_id,
        page_num=page_num,
        page_size=page_size,
        phone_number=phone_number,
        reason=reason,
    )
    return TableResponse(rows=rows, total=total, msg="查询成功")


@OutboundValidationRouter.post(
    "/outbound-validations/{validation_id}/issues/export",
    summary="导出名单问题明细",
)
async def export_validation_issues_controller(
    validation_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[
        OutboundValidationService,
        Depends(get_outbound_validation_service),
    ],
    phone_number: Annotated[str | None, Query(alias="phoneNumber")] = None,
    reason: Annotated[str | None, Query()] = None,
) -> FileResponse:
    tenant_id, _ = _identity(auth)
    export_path = await service.build_issue_export(
        auth.db,
        tenant_id,
        validation_id,
        phone_number=phone_number,
        reason=reason,
    )
    return FileResponse(
        export_path,
        filename="外呼名单问题明细.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        background=BackgroundTask(service._delete_temp_file, export_path),
    )


@OutboundValidationRouter.post(
    "/outbound-validations/{validation_id}/retry",
    summary="重试解析完成后的系统校验",
    response_model=ResponseSchema[ValidationResultOut],
)
async def retry_validation_controller(
    validation_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[
        OutboundValidationService,
        Depends(get_outbound_validation_service),
    ],
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    validation = await service.prepare_retry(auth.db, tenant_id, validation_id)
    await auth.db.commit()
    service.schedule_validation(tenant_id, validation.id)
    return SuccessResponse(data=service.result_out(validation), msg="重新校验已受理")


@OutboundValidationRouter.post(
    "/outbound-targets/import-template",
    summary="下载通用外呼名单模板",
)
async def download_outbound_template_controller(
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[
        OutboundValidationService,
        Depends(get_outbound_validation_service),
    ],
    prompt_profile_id: Annotated[int | None, Form(alias="promptProfileId", gt=0)] = None,
) -> FileResponse:
    tenant_id, _ = _identity(auth)
    template_path = await service.create_template(
        auth.db,
        tenant_id,
        prompt_profile_id,
    )
    return FileResponse(
        template_path,
        filename="外呼名单导入模板.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        background=BackgroundTask(service._delete_temp_file, template_path),
    )
