from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Path, Query
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.common.response import ResponseSchema, SuccessResponse, TableResponse

from .schema import (
    BrowserEventReportRequest,
    CreateSessionOut,
    CreateWebSessionRequest,
    EndSessionOut,
    EventListOut,
    EventOut,
    RecordDetailOut,
    RecordEventListOut,
    SessionStatusOut,
    TokenOut,
)
from .service import AiCallService, get_default_ai_call_service

AiCallRouter = APIRouter(prefix="/ai-call", tags=["智能外呼"])


async def ai_call_db_getter() -> AsyncGenerator[AsyncSession, None]:
    from app.core.dependencies import db_getter

    async for db in db_getter():
        yield db


def get_ai_call_service(
    db: Annotated[AsyncSession, Depends(ai_call_db_getter)],
) -> AiCallService:
    return get_default_ai_call_service(db)


@AiCallRouter.get("/health", summary="智能外呼模块健康检查")
async def ai_call_health() -> dict[str, str]:
    return {"status": "ok"}


@AiCallRouter.post(
    "/sessions",
    summary="创建 Web 通话会话",
    response_model=ResponseSchema[CreateSessionOut],
)
async def create_session_controller(
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
    request: Annotated[CreateWebSessionRequest | None, Body()] = None,
) -> JSONResponse:
    result = await service.create_web_session(
        voice=request.voice if request else None,
        prompt=request.prompt if request else None,
        business_type=request.business_type if request else None,
        business_id=request.business_id if request else None,
    )
    return SuccessResponse(
        data=CreateSessionOut.model_validate(result),
        msg="创建成功",
    )


@AiCallRouter.get(
    "/records",
    summary="查询通话记录列表",
)
async def list_records_controller(
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
    call_id: Annotated[str | None, Query(alias="callId")] = None,
    business_type: Annotated[str | None, Query(alias="businessType")] = None,
    business_id: Annotated[str | None, Query(alias="businessId")] = None,
    status: str | None = None,
    entry_type: Annotated[str | None, Query(alias="entryType")] = None,
    started_at_begin: Annotated[datetime | None, Query(alias="startedAtBegin")] = None,
    started_at_end: Annotated[datetime | None, Query(alias="startedAtEnd")] = None,
    page_num: Annotated[int, Query(alias="pageNum", ge=1)] = 1,
    page_size: Annotated[int, Query(alias="pageSize", ge=1, le=1000)] = 10,
) -> JSONResponse:
    result = await service.list_records(
        call_id=call_id,
        business_type=business_type,
        business_id=business_id,
        status=status,
        entry_type=entry_type,
        started_at_begin=started_at_begin,
        started_at_end=started_at_end,
        page_num=page_num,
        page_size=page_size,
    )
    return TableResponse(rows=result["rows"], total=result["total"], msg="查询成功")


@AiCallRouter.get(
    "/records/{callId}",
    summary="查询通话记录详情",
    response_model=ResponseSchema[RecordDetailOut],
)
async def get_record_detail_controller(
    call_id: Annotated[str, Path(alias="callId")],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.get_record_detail(call_id)
    return SuccessResponse(data=RecordDetailOut.model_validate(result), msg="查询成功")


@AiCallRouter.get(
    "/records/{callId}/events",
    summary="查询通话记录事件",
    response_model=ResponseSchema[RecordEventListOut],
)
async def list_record_events_controller(
    call_id: Annotated[str, Path(alias="callId")],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
    limit: Annotated[int, Query(ge=1, le=1000)] = 200,
    after_event_id: Annotated[str | None, Query(alias="afterEventId")] = None,
    event_type: Annotated[str | None, Query(alias="eventType")] = None,
    source: str | None = None,
) -> JSONResponse:
    result = await service.list_record_events(
        call_id=call_id,
        limit=limit,
        after_event_id=after_event_id,
        event_type=event_type,
        source=source,
    )
    return SuccessResponse(data=RecordEventListOut.model_validate(result), msg="查询成功")


@AiCallRouter.post(
    "/sessions/{callId}/token",
    summary="重新签发浏览器 Room Token",
    response_model=ResponseSchema[TokenOut],
)
async def reissue_token_controller(
    call_id: Annotated[str, Path(alias="callId")],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.reissue_browser_token(call_id)
    return SuccessResponse(data=TokenOut.model_validate(result), msg="签发成功")


@AiCallRouter.get(
    "/sessions/{callId}",
    summary="查询会话状态",
    response_model=ResponseSchema[SessionStatusOut],
)
async def get_session_controller(
    call_id: Annotated[str, Path(alias="callId")],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.get_session(call_id)
    return SuccessResponse(data=SessionStatusOut.model_validate(result), msg="查询成功")


@AiCallRouter.get(
    "/sessions/{callId}/events",
    summary="查询会话事件",
    response_model=ResponseSchema[EventListOut],
)
async def list_events_controller(
    call_id: Annotated[str, Path(alias="callId")],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
    limit: Annotated[int, Query(ge=1, le=1000)] = 200,
    after_event_id: Annotated[str | None, Query(alias="afterEventId")] = None,
) -> JSONResponse:
    result = await service.list_events(
        call_id=call_id,
        limit=limit,
        after_event_id=after_event_id,
    )
    return SuccessResponse(data=EventListOut.model_validate(result), msg="查询成功")


@AiCallRouter.post(
    "/sessions/{callId}/browser-events",
    summary="上报浏览器侧通话事件",
    response_model=ResponseSchema[EventOut],
)
async def report_browser_event_controller(
    call_id: Annotated[str, Path(alias="callId")],
    request: BrowserEventReportRequest,
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.report_browser_event(
        call_id=call_id,
        event_type=request.type,
        timestamp=request.timestamp,
    )
    return SuccessResponse(data=EventOut.model_validate(result), msg="上报成功")


@AiCallRouter.post(
    "/sessions/{callId}/end",
    summary="结束会话",
    response_model=ResponseSchema[EndSessionOut],
)
async def end_session_controller(
    call_id: Annotated[str, Path(alias="callId")],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.end_session(call_id)
    return SuccessResponse(data=EndSessionOut.model_validate(result), msg="结束成功")
