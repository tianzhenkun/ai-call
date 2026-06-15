from typing import Annotated

from fastapi import APIRouter, Body, Depends, Path, Query
from fastapi.responses import JSONResponse

from app.common.response import ResponseSchema, SuccessResponse

from .schema import (
    BrowserEventReportRequest,
    CreateSessionOut,
    CreateWebSessionRequest,
    EndSessionOut,
    EventListOut,
    EventOut,
    SessionStatusOut,
    TokenOut,
)
from .service import AiCallService, get_default_ai_call_service

AiCallRouter = APIRouter(prefix="/ai-call", tags=["智能外呼"])


def get_ai_call_service() -> AiCallService:
    return get_default_ai_call_service()


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
    )
    return SuccessResponse(
        data=CreateSessionOut.model_validate(result),
        msg="创建成功",
    )


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
        payload=request.payload,
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
