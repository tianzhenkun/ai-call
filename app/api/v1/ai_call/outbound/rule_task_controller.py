from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Header, Path, Query
from fastapi.responses import JSONResponse

from app.api.v1.system.auth.schema import AuthSchema
from app.common.response import ResponseSchema, SuccessResponse, TableResponse
from app.core.database import async_db_session
from app.core.dependencies import get_current_user

from .controller import _identity
from .rule_task_schema import (
    AcceptedCommandOut,
    CallRuleIn,
    CallRuleMetadataOut,
    CallRuleOut,
    CreateTaskRequest,
    CreateTaskResultOut,
    OutboundTaskOut,
    SingleValidationRequest,
    UpdateTaskScheduleRequest,
)
from .rule_task_service import OutboundRuleTaskService
from .schema import ValidationResultOut
from .service import OutboundValidationService

OutboundRuleTaskRouter = APIRouter(tags=["通用外呼规则与任务"])
_default_service = OutboundRuleTaskService(async_db_session)


def get_outbound_rule_task_service() -> OutboundRuleTaskService:
    return _default_service


@OutboundRuleTaskRouter.get(
    "/outbound-rules/meta",
    summary="查询呼叫规则元数据",
    response_model=ResponseSchema[CallRuleMetadataOut],
)
async def get_rule_metadata_controller(
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[
        OutboundRuleTaskService,
        Depends(get_outbound_rule_task_service),
    ],
) -> JSONResponse:
    _identity(auth)
    return SuccessResponse(data=service.metadata_out(), msg="查询成功")


@OutboundRuleTaskRouter.get(
    "/outbound-rules",
    summary="分页查询呼叫规则",
)
async def list_rules_controller(
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[
        OutboundRuleTaskService,
        Depends(get_outbound_rule_task_service),
    ],
    page_num: Annotated[int, Query(alias="pageNum", ge=1)] = 1,
    page_size: Annotated[int, Query(alias="pageSize", ge=1, le=200)] = 20,
    rule_name: Annotated[str | None, Query(alias="ruleName")] = None,
    enabled: Annotated[bool | None, Query()] = None,
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    rows, total = await service.list_rules(
        auth.db,
        tenant_id,
        user_id=user_id,
        page_num=page_num,
        page_size=page_size,
        rule_name=rule_name,
        enabled=enabled,
    )
    await auth.db.commit()
    return TableResponse(rows=rows, total=total, msg="查询成功")


@OutboundRuleTaskRouter.post(
    "/outbound-rules",
    summary="创建呼叫规则",
    response_model=ResponseSchema[CallRuleOut],
)
async def create_rule_controller(
    request: CallRuleIn,
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[
        OutboundRuleTaskService,
        Depends(get_outbound_rule_task_service),
    ],
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    rule = await service.create_rule(auth.db, tenant_id, user_id, request)
    await auth.db.commit()
    return SuccessResponse(data=service.rule_out(rule), msg="创建成功")


@OutboundRuleTaskRouter.put(
    "/outbound-rules/{rule_id}",
    summary="修改呼叫规则",
    response_model=ResponseSchema[CallRuleOut],
)
async def update_rule_controller(
    rule_id: Annotated[int, Path(gt=0)],
    request: CallRuleIn,
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[
        OutboundRuleTaskService,
        Depends(get_outbound_rule_task_service),
    ],
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    rule = await service.update_rule(auth.db, tenant_id, user_id, rule_id, request)
    await auth.db.commit()
    return SuccessResponse(data=service.rule_out(rule), msg="修改成功")


@OutboundRuleTaskRouter.delete(
    "/outbound-rules/{rule_id}",
    summary="软删除呼叫规则",
)
async def delete_rule_controller(
    rule_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[
        OutboundRuleTaskService,
        Depends(get_outbound_rule_task_service),
    ],
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    await service.delete_rule(auth.db, tenant_id, user_id, rule_id)
    await auth.db.commit()
    return SuccessResponse(msg="删除成功")


@OutboundRuleTaskRouter.post(
    "/outbound-validations/single",
    summary="校验单号码外呼配置",
    response_model=ResponseSchema[ValidationResultOut],
)
async def create_single_validation_controller(
    request: SingleValidationRequest,
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[
        OutboundRuleTaskService,
        Depends(get_outbound_rule_task_service),
    ],
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    validation = await service.validate_single(auth.db, tenant_id, user_id, request)
    await auth.db.commit()
    return SuccessResponse(
        data=OutboundValidationService.result_out(validation),
        msg="校验通过",
    )


@OutboundRuleTaskRouter.get(
    "/outbound-tasks",
    summary="分页查询正式外呼任务",
)
async def list_tasks_controller(
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[
        OutboundRuleTaskService,
        Depends(get_outbound_rule_task_service),
    ],
    page_num: Annotated[int, Query(alias="pageNum", ge=1)] = 1,
    page_size: Annotated[int, Query(alias="pageSize", ge=1, le=200)] = 20,
    task_name: Annotated[str | None, Query(alias="taskName")] = None,
    task_status: Annotated[str | None, Query(alias="status")] = None,
    begin_time: Annotated[str | None, Query(alias="beginTime")] = None,
    end_time: Annotated[str | None, Query(alias="endTime")] = None,
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    rows, total = await service.list_tasks(
        auth.db,
        tenant_id,
        page_num=page_num,
        page_size=page_size,
        task_name=task_name,
        task_status=task_status,
        begin_time=begin_time,
        end_time=end_time,
    )
    return TableResponse(rows=rows, total=total, msg="查询成功")


@OutboundRuleTaskRouter.post(
    "/outbound-tasks",
    summary="由校验结果创建正式外呼任务",
    response_model=ResponseSchema[CreateTaskResultOut],
)
async def create_task_controller(
    request: CreateTaskRequest,
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    service: Annotated[
        OutboundRuleTaskService,
        Depends(get_outbound_rule_task_service),
    ],
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    user_name = auth.user.nick_name if auth.user else None
    task, _ = await service.create_task(
        auth.db,
        tenant_id,
        user_id,
        user_name,
        idempotency_key,
        request,
    )
    await auth.db.commit()
    return SuccessResponse(
        data=CreateTaskResultOut(task_id=str(task.id)),
        msg="任务已创建",
    )


@OutboundRuleTaskRouter.get(
    "/outbound-tasks/{task_id}",
    summary="查询正式外呼任务详情",
    response_model=ResponseSchema[OutboundTaskOut],
)
async def get_task_controller(
    task_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[
        OutboundRuleTaskService,
        Depends(get_outbound_rule_task_service),
    ],
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    return SuccessResponse(
        data=await service.get_task(auth.db, tenant_id, task_id),
        msg="查询成功",
    )


@OutboundRuleTaskRouter.put(
    "/outbound-tasks/{task_id}/schedule",
    summary="修改待执行任务名称和计划时间",
    response_model=ResponseSchema[AcceptedCommandOut],
)
async def update_task_schedule_controller(
    task_id: Annotated[int, Path(gt=0)],
    request: UpdateTaskScheduleRequest,
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    service: Annotated[
        OutboundRuleTaskService,
        Depends(get_outbound_rule_task_service),
    ],
) -> JSONResponse:
    _ = idempotency_key
    tenant_id, _ = _identity(auth)
    result = await service.update_schedule(auth.db, tenant_id, task_id, request)
    await auth.db.commit()
    return SuccessResponse(data=result, msg="修改成功")


async def _run_task_action(
    task_id: Annotated[int, Path(gt=0)],
    action: str,
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    idempotency_key: str,
    service: OutboundRuleTaskService,
) -> JSONResponse:
    _ = idempotency_key
    tenant_id, _ = _identity(auth)
    result = await service.run_action(auth.db, tenant_id, task_id, action)
    await auth.db.commit()
    return SuccessResponse(data=result, msg="操作成功")


@OutboundRuleTaskRouter.post(
    "/outbound-tasks/{task_id}/pause",
    summary="暂停运行中的外呼任务",
    response_model=ResponseSchema[AcceptedCommandOut],
)
async def pause_task_controller(
    task_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    service: Annotated[
        OutboundRuleTaskService,
        Depends(get_outbound_rule_task_service),
    ],
) -> JSONResponse:
    return await _run_task_action(task_id, "pause", auth, idempotency_key, service)


@OutboundRuleTaskRouter.post(
    "/outbound-tasks/{task_id}/resume",
    summary="恢复暂停的外呼任务",
    response_model=ResponseSchema[AcceptedCommandOut],
)
async def resume_task_controller(
    task_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    service: Annotated[
        OutboundRuleTaskService,
        Depends(get_outbound_rule_task_service),
    ],
) -> JSONResponse:
    return await _run_task_action(task_id, "resume", auth, idempotency_key, service)


@OutboundRuleTaskRouter.post(
    "/outbound-tasks/{task_id}/stop",
    summary="停止运行中或暂停的外呼任务",
    response_model=ResponseSchema[AcceptedCommandOut],
)
async def stop_task_controller(
    task_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    service: Annotated[
        OutboundRuleTaskService,
        Depends(get_outbound_rule_task_service),
    ],
) -> JSONResponse:
    return await _run_task_action(task_id, "stop", auth, idempotency_key, service)


@OutboundRuleTaskRouter.post(
    "/outbound-tasks/{task_id}/cancel",
    summary="取消待执行的外呼任务",
    response_model=ResponseSchema[AcceptedCommandOut],
)
async def cancel_task_controller(
    task_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    service: Annotated[
        OutboundRuleTaskService,
        Depends(get_outbound_rule_task_service),
    ],
) -> JSONResponse:
    return await _run_task_action(task_id, "cancel", auth, idempotency_key, service)


@OutboundRuleTaskRouter.get(
    "/outbound-tasks/{task_id}/targets",
    summary="分页查询正式外呼任务对象",
)
async def list_task_targets_controller(
    task_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[
        OutboundRuleTaskService,
        Depends(get_outbound_rule_task_service),
    ],
    page_num: Annotated[int, Query(alias="pageNum", ge=1)] = 1,
    page_size: Annotated[int, Query(alias="pageSize", ge=1, le=200)] = 20,
    phone_number: Annotated[str | None, Query(alias="phoneNumber")] = None,
    customer_name: Annotated[str | None, Query(alias="customerName")] = None,
    target_status: Annotated[str | None, Query(alias="status")] = None,
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    rows, total = await service.list_targets(
        auth.db,
        tenant_id,
        task_id,
        page_num=page_num,
        page_size=page_size,
        phone_number=phone_number,
        customer_name=customer_name,
        target_status=target_status,
    )
    return TableResponse(rows=rows, total=total, msg="查询成功")
