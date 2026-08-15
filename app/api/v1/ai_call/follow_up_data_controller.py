from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Query
from fastapi.responses import JSONResponse

from app.api.v1.ai_call.follow_up_data_schema import (
    FollowUpClassification,
    FollowUpDataClassificationIn,
    FollowUpDataScheduleIn,
)
from app.api.v1.ai_call.outbound.controller import _identity
from app.api.v1.system.auth.schema import AuthSchema
from app.common.response import SuccessResponse, TableResponse
from app.core.dependencies import get_ai_call_manager
from app.services.ai_call.agent_console_reconciler import publish_agent_console_event
from app.services.ai_call.follow_up_data_service import AiCallFollowUpDataService

FollowUpDataRouter = APIRouter(tags=["跟进数据"])


@FollowUpDataRouter.get("/follow-up-data", summary="分页查询跟进数据")
async def list_follow_up_data_controller(
    auth: Annotated[AuthSchema, Depends(get_ai_call_manager)],
    classification: Annotated[FollowUpClassification | None, Query()] = None,
    keyword: Annotated[str | None, Query()] = None,
    task_id: Annotated[int | None, Query(alias="taskId")] = None,
    last_contact_at_begin: Annotated[
        datetime | None,
        Query(alias="lastContactAtBegin"),
    ] = None,
    last_contact_at_end: Annotated[
        datetime | None,
        Query(alias="lastContactAtEnd"),
    ] = None,
    next_follow_up_at_begin: Annotated[
        datetime | None,
        Query(alias="nextFollowUpAtBegin"),
    ] = None,
    next_follow_up_at_end: Annotated[
        datetime | None,
        Query(alias="nextFollowUpAtEnd"),
    ] = None,
    suggest_review: Annotated[bool | None, Query(alias="suggestReview")] = None,
    page_num: Annotated[int, Query(alias="pageNum", ge=1)] = 1,
    page_size: Annotated[int, Query(alias="pageSize", ge=1, le=200)] = 20,
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    rows, total = await AiCallFollowUpDataService.from_session(auth.db).list_page(
        tenant_id=tenant_id,
        classification=classification,
        keyword=keyword,
        task_id=task_id,
        last_contact_at_begin=last_contact_at_begin,
        last_contact_at_end=last_contact_at_end,
        next_follow_up_at_begin=next_follow_up_at_begin,
        next_follow_up_at_end=next_follow_up_at_end,
        suggest_review=suggest_review,
        page_num=page_num,
        page_size=page_size,
    )
    return TableResponse(rows=rows, total=total, msg="查询成功")


@FollowUpDataRouter.get(
    "/follow-up-data/{follow_up_data_id}",
    summary="查询跟进数据详情",
)
async def get_follow_up_data_controller(
    follow_up_data_id: int,
    auth: Annotated[AuthSchema, Depends(get_ai_call_manager)],
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    result = await AiCallFollowUpDataService.from_session(auth.db).get_detail(
        tenant_id=tenant_id,
        follow_up_data_id=follow_up_data_id,
    )
    return SuccessResponse(data=result, msg="查询成功")


@FollowUpDataRouter.put(
    "/follow-up-data/{follow_up_data_id}/classification",
    summary="人工调整跟进分类",
)
async def adjust_follow_up_data_classification_controller(
    follow_up_data_id: int,
    request: FollowUpDataClassificationIn,
    idempotency_key: Annotated[
        str,
        Header(alias="Idempotency-Key", min_length=1, max_length=128),
    ],
    auth: Annotated[AuthSchema, Depends(get_ai_call_manager)],
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    user = auth.user
    result = await AiCallFollowUpDataService.from_session(auth.db).adjust_classification(
        tenant_id=tenant_id,
        follow_up_data_id=follow_up_data_id,
        payload=request,
        idempotency_key=idempotency_key,
        changed_by=str(user_id),
        changed_by_name=(getattr(user, "nick_name", None) or getattr(user, "user_name", None)),
    )
    await auth.db.commit()
    return SuccessResponse(data=result, msg="分类已更新")


@FollowUpDataRouter.post(
    "/follow-up-data/{follow_up_data_id}/schedule",
    summary="安排回访",
)
async def schedule_follow_up_data_controller(
    follow_up_data_id: int,
    request: FollowUpDataScheduleIn,
    idempotency_key: Annotated[
        str,
        Header(alias="Idempotency-Key", min_length=1, max_length=128),
    ],
    auth: Annotated[AuthSchema, Depends(get_ai_call_manager)],
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    user = auth.user
    result = await AiCallFollowUpDataService.from_session(auth.db).schedule_follow_up(
        tenant_id=tenant_id,
        follow_up_data_id=follow_up_data_id,
        payload=request,
        idempotency_key=idempotency_key,
        changed_by=str(user_id),
        changed_by_name=(getattr(user, "nick_name", None) or getattr(user, "user_name", None)),
    )
    await auth.db.commit()
    await publish_agent_console_event(
        tenant_id,
        "follow_up.changed",
        {
            "follow_up_data_id": result["follow_up_data_id"],
            "follow_up_id": result["follow_up_id"],
        },
    )
    return SuccessResponse(data=result, msg="回访已安排")
