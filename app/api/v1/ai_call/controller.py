from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Path, Query
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.common.response import ResponseSchema, SuccessResponse, TableResponse

from .schema import (
    AcceptHandoffOut,
    AcceptHandoffRequest,
    BrowserEventReportRequest,
    CreateHandoffRequest,
    CreateSessionOut,
    CreateWebSessionRequest,
    DialogueSegmentListOut,
    EndSessionOut,
    EventListOut,
    EventOut,
    FailHandoffRequest,
    FinishHandoffRequest,
    HandoffListOut,
    HandoffOut,
    PromptComponentOut,
    PromptProfileCreateRequest,
    PromptProfileOut,
    PromptProfilePreviewOut,
    PromptProfilePreviewRequest,
    PromptProfileUpdateRequest,
    RecordDetailOut,
    RecordEventListOut,
    RecordingOut,
    SessionStatusOut,
    TokenOut,
    VoiceProfileCreateRequest,
    VoiceProfileOut,
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
    request: Annotated[CreateWebSessionRequest, Body()],
) -> JSONResponse:
    result = await service.create_web_session(
        voice=request.voice,
        prompt=None,
        business_id=request.business_id,
        scene_code=request.scene_code,
        business_params=request.business_params,
    )
    return SuccessResponse(
        data=CreateSessionOut.model_validate(result),
        msg="创建成功",
    )


@AiCallRouter.get(
    "/voice-profiles",
    summary="查询端到端音色配置列表",
)
async def list_voice_profiles_controller(
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
    voice_type: Annotated[str | None, Query(alias="voiceType")] = None,
    gender: Annotated[str | None, Query()] = None,
    target_model: Annotated[str | None, Query(alias="targetModel")] = None,
    page_num: Annotated[int, Query(alias="pageNum", ge=1)] = 1,
    page_size: Annotated[int, Query(alias="pageSize", ge=1, le=1000)] = 200,
) -> JSONResponse:
    result = await service.list_voice_profiles(
        voice_type=voice_type,
        gender=gender,
        target_model=target_model,
        page_num=page_num,
        page_size=page_size,
    )
    rows = [VoiceProfileOut.model_validate(row) for row in result["rows"]]
    return TableResponse(rows=rows, total=result["total"], msg="查询成功")


@AiCallRouter.post(
    "/voice-profiles",
    summary="登记自定义复刻音色",
    response_model=ResponseSchema[VoiceProfileOut],
)
async def create_voice_profile_controller(
    request: VoiceProfileCreateRequest,
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.create_voice_profile(request.model_dump())
    return SuccessResponse(data=VoiceProfileOut.model_validate(result), msg="创建成功")


@AiCallRouter.get(
    "/prompt-profiles",
    summary="查询业务提示词配置列表",
)
async def list_prompt_profiles_controller(
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
    scene_code: Annotated[str | None, Query(alias="sceneCode")] = None,
    page_num: Annotated[int, Query(alias="pageNum", ge=1)] = 1,
    page_size: Annotated[int, Query(alias="pageSize", ge=1, le=1000)] = 20,
) -> JSONResponse:
    result = await service.list_prompt_profiles(
        scene_code=scene_code,
        page_num=page_num,
        page_size=page_size,
    )
    return TableResponse(rows=result["rows"], total=result["total"], msg="查询成功")


@AiCallRouter.get(
    "/prompt-profiles/{profileId}",
    summary="查询业务提示词配置详情",
    response_model=ResponseSchema[PromptProfileOut],
)
async def get_prompt_profile_controller(
    profile_id: Annotated[int, Path(alias="profileId")],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.get_prompt_profile(profile_id)
    return SuccessResponse(data=PromptProfileOut.model_validate(result), msg="查询成功")


@AiCallRouter.post(
    "/prompt-profiles",
    summary="新增业务提示词配置",
    response_model=ResponseSchema[PromptProfileOut],
)
async def create_prompt_profile_controller(
    request: PromptProfileCreateRequest,
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.create_prompt_profile(request.model_dump())
    return SuccessResponse(data=PromptProfileOut.model_validate(result), msg="创建成功")


@AiCallRouter.put(
    "/prompt-profiles/{profileId}",
    summary="修改业务提示词配置",
    response_model=ResponseSchema[PromptProfileOut],
)
async def update_prompt_profile_controller(
    profile_id: Annotated[int, Path(alias="profileId")],
    request: PromptProfileUpdateRequest,
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.update_prompt_profile(profile_id, request.model_dump())
    return SuccessResponse(data=PromptProfileOut.model_validate(result), msg="保存成功")


@AiCallRouter.get(
    "/prompt-components",
    summary="查询平台公共提示词组件",
)
async def list_prompt_components_controller(
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.list_prompt_components()
    rows = [PromptComponentOut.model_validate(row) for row in result["rows"]]
    return TableResponse(rows=rows, total=result["total"], msg="查询成功")


@AiCallRouter.post(
    "/prompt-profiles/preview",
    summary="预览最终提示词",
    response_model=ResponseSchema[PromptProfilePreviewOut],
)
async def preview_prompt_profile_controller(
    request: PromptProfilePreviewRequest,
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.preview_prompt_profile(
        business_id=request.business_id,
        scene_code=request.scene_code,
        business_params=request.business_params,
        prompt=None,
    )
    return SuccessResponse(data=PromptProfilePreviewOut.model_validate(result), msg="预览成功")


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


@AiCallRouter.get(
    "/records/{callId}/recording",
    summary="查询通话录音",
    response_model=ResponseSchema[RecordingOut | None],
)
async def get_recording_controller(
    call_id: Annotated[str, Path(alias="callId")],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.get_recording(call_id)
    return SuccessResponse(
        data=RecordingOut.model_validate(result) if result else None,
        msg="查询成功",
    )


@AiCallRouter.get(
    "/records/{callId}/dialogue-segments",
    summary="查询通话对话文本段",
    response_model=ResponseSchema[DialogueSegmentListOut],
)
async def list_record_dialogue_segments_controller(
    call_id: Annotated[str, Path(alias="callId")],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
    speaker_type: Annotated[str | None, Query(alias="speakerType")] = None,
    limit: Annotated[int, Query(ge=1, le=5000)] = 1000,
) -> JSONResponse:
    result = await service.list_record_dialogue_segments(
        call_id=call_id,
        speaker_type=speaker_type,
        limit=limit,
    )
    return SuccessResponse(data=DialogueSegmentListOut.model_validate(result), msg="查询成功")


@AiCallRouter.get(
    "/records/{callId}/handoffs",
    summary="查询通话转人工记录",
    response_model=ResponseSchema[HandoffListOut],
)
async def list_record_handoffs_controller(
    call_id: Annotated[str, Path(alias="callId")],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.list_handoffs(call_id)
    return SuccessResponse(data=HandoffListOut.model_validate(result), msg="查询成功")


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


@AiCallRouter.get(
    "/sessions/{callId}/dialogue-preview",
    summary="查询运行态对话文本预览",
    response_model=ResponseSchema[DialogueSegmentListOut],
)
async def list_dialogue_preview_controller(
    call_id: Annotated[str, Path(alias="callId")],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.list_dialogue_preview(call_id)
    return SuccessResponse(data=DialogueSegmentListOut.model_validate(result), msg="查询成功")


@AiCallRouter.post(
    "/sessions/{callId}/handoffs",
    summary="创建转人工请求",
    response_model=ResponseSchema[HandoffOut],
)
async def create_handoff_controller(
    call_id: Annotated[str, Path(alias="callId")],
    request: CreateHandoffRequest,
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.create_handoff(
        call_id=call_id,
        source=request.source,
        reason=request.reason,
        request_message=request.request_message,
    )
    return SuccessResponse(data=HandoffOut.model_validate(result), msg="创建成功")


@AiCallRouter.get(
    "/sessions/{callId}/handoff",
    summary="查询当前转人工请求",
    response_model=ResponseSchema[HandoffOut | None],
)
async def get_current_handoff_controller(
    call_id: Annotated[str, Path(alias="callId")],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.get_current_handoff(call_id)
    return SuccessResponse(
        data=HandoffOut.model_validate(result) if result else None,
        msg="查询成功",
    )


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


@AiCallRouter.get(
    "/handoffs/joinable",
    summary="查询可接入转人工请求",
    response_model=ResponseSchema[HandoffListOut],
)
async def list_joinable_handoffs_controller(
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> JSONResponse:
    result = await service.list_joinable_handoffs(limit=limit)
    return SuccessResponse(data=HandoffListOut.model_validate(result), msg="查询成功")


@AiCallRouter.post(
    "/handoffs/{handoffId}/accept",
    summary="坐席接管转人工请求",
    response_model=ResponseSchema[AcceptHandoffOut],
)
async def accept_handoff_controller(
    handoff_id: Annotated[str, Path(alias="handoffId")],
    request: AcceptHandoffRequest,
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.accept_handoff(
        handoff_id=handoff_id,
        human_agent_identity=request.human_agent_identity,
    )
    return SuccessResponse(data=AcceptHandoffOut.model_validate(result), msg="接管成功")


@AiCallRouter.post(
    "/handoffs/{handoffId}/connected",
    summary="标记坐席已连接",
    response_model=ResponseSchema[HandoffOut],
)
async def connect_handoff_controller(
    handoff_id: Annotated[str, Path(alias="handoffId")],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.mark_handoff_connected(handoff_id)
    return SuccessResponse(data=HandoffOut.model_validate(result), msg="连接成功")


@AiCallRouter.post(
    "/handoffs/{handoffId}/complete",
    summary="完成转人工",
    response_model=ResponseSchema[HandoffOut],
)
async def complete_handoff_controller(
    handoff_id: Annotated[str, Path(alias="handoffId")],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
    request: Annotated[FinishHandoffRequest | None, Body()] = None,
) -> JSONResponse:
    result = await service.complete_handoff(
        handoff_id=handoff_id,
        reason=request.reason if request else None,
    )
    return SuccessResponse(data=HandoffOut.model_validate(result), msg="完成成功")


@AiCallRouter.post(
    "/handoffs/{handoffId}/cancel",
    summary="取消转人工",
    response_model=ResponseSchema[HandoffOut],
)
async def cancel_handoff_controller(
    handoff_id: Annotated[str, Path(alias="handoffId")],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
    request: Annotated[FinishHandoffRequest | None, Body()] = None,
) -> JSONResponse:
    result = await service.cancel_handoff(
        handoff_id=handoff_id,
        reason=request.reason if request else None,
    )
    return SuccessResponse(data=HandoffOut.model_validate(result), msg="取消成功")


@AiCallRouter.post(
    "/handoffs/{handoffId}/fail",
    summary="标记转人工失败",
    response_model=ResponseSchema[HandoffOut],
)
async def fail_handoff_controller(
    handoff_id: Annotated[str, Path(alias="handoffId")],
    request: FailHandoffRequest,
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.fail_handoff(
        handoff_id=handoff_id,
        failure_stage=request.failure_stage,
        failure_message=request.failure_message,
    )
    return SuccessResponse(data=HandoffOut.model_validate(result), msg="标记成功")
