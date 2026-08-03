from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Annotated, Literal

from fastapi import (
    APIRouter,
    Body,
    Depends,
    Header,
    HTTPException,
    Path,
    Query,
    Request,
    status,
)
from fastapi.responses import JSONResponse
from google.protobuf.json_format import MessageToDict
from livekit import api
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import AiCallHandoffModel, AiCallRecordModel
from app.api.v1.system.auth.schema import AuthSchema
from app.common.response import ResponseSchema, SuccessResponse, TableResponse
from app.config.setting import settings
from app.core.dependencies import get_current_user
from app.core.exceptions import CustomException
from app.core.security import OAuth2Schema
from app.services.ai_call.runtime_control.command_repository import (
    IdempotencyConflictError,
    RuntimeCommandRepository,
)
from app.services.ai_call.runtime_control.direct_sip_phone import (
    DirectSipPhoneError,
    prepare_direct_sip_phone,
)
from app.services.ai_call.runtime_control.entry_start_service import (
    RuntimeEntryStartError,
    RuntimeEntryStartService,
    StartEntryRequest,
)
from app.services.ai_call.runtime_control.handoff_repository import (
    HandoffCancelIntent,
    HandoffClaimConflictError,
    HandoffIdempotencyConflictError,
    HandoffNotFoundError,
    HandoffRuntimeModeError,
    HandoffTerminalBarrierError,
    RuntimeHandoffRepository,
)
from app.services.ai_call.runtime_control.health import (
    RuntimeTaskState,
    RuntimeWorkerHealth,
    default_runtime_worker_health,
)
from app.services.ai_call.runtime_control.roles import runtime_control_mode_for_entry
from app.services.ai_call.runtime_control.webhook_service import (
    RuntimeWebhookIngressService,
)

from .outbound.controller import _identity
from .schema import (
    AcceptHandoffOut,
    AcceptHandoffRequest,
    BrowserEventReportRequest,
    CreateHandoffRequest,
    CreateSessionOut,
    CreateSipSessionOut,
    CreateSipSessionRequest,
    CreateWebSessionRequest,
    DialogueSegmentListOut,
    EndSessionOut,
    EventListOut,
    EventOut,
    FailHandoffRequest,
    FinishHandoffRequest,
    HandoffAgentOut,
    HandoffAgentStatusRequest,
    HandoffListOut,
    HandoffOut,
    InterruptSummaryOut,
    PromptComponentOut,
    PromptProfileCreateRequest,
    PromptProfileOut,
    PromptProfilePreviewOut,
    PromptProfilePreviewRequest,
    PromptProfileUpdateRequest,
    RecordDetailOut,
    RecordEventListOut,
    RecordingOut,
    RuntimeDirectSipStartCallOut,
    RuntimeHandoffCommandOut,
    RuntimeStartCallOut,
    SemanticAnalysisOut,
    SessionStatusOut,
    TokenOut,
)
from .service import (
    AiCallService,
    get_default_ai_call_service,
    schedule_livekit_webhook_event,
)

AiCallRouter = APIRouter(prefix="/ai-call", tags=["智能外呼"])


async def ai_call_db_getter() -> AsyncGenerator[AsyncSession, None]:
    from app.core.dependencies import db_getter

    async for db in db_getter():
        yield db


def get_ai_call_service(
    db: Annotated[AsyncSession, Depends(ai_call_db_getter)],
) -> AiCallService:
    return get_default_ai_call_service(db)


def get_runtime_webhook_ingress_service(
    db: Annotated[AsyncSession, Depends(ai_call_db_getter)],
) -> RuntimeWebhookIngressService:
    return RuntimeWebhookIngressService(db, settings)


def get_runtime_worker_health() -> RuntimeWorkerHealth:
    return default_runtime_worker_health


@AiCallRouter.get(
    "/health",
    summary="智能外呼模块健康检查",
    response_model=None,
)
async def ai_call_health(
    runtime_health: Annotated[
        RuntimeWorkerHealth,
        Depends(get_runtime_worker_health),
    ],
) -> dict[str, str] | JSONResponse:
    snapshot = runtime_health.snapshot()
    if snapshot.state == RuntimeTaskState.FAILED:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "error",
                "runtime": "failed",
                "errorCode": snapshot.error_code or "runtime_task_failed",
            },
        )
    return {"status": "ok"}


@AiCallRouter.post("/livekit-webhook", summary="接收 LiveKit 房间事件")
async def livekit_webhook_controller(
    request: Request,
    ingress_service: Annotated[
        RuntimeWebhookIngressService,
        Depends(get_runtime_webhook_ingress_service),
    ],
    db: Annotated[AsyncSession, Depends(ai_call_db_getter)],
    authorization: Annotated[str | None, Header()] = None,
) -> JSONResponse:
    body = (await request.body()).decode("utf-8")
    auth_token = _livekit_webhook_token(authorization)
    webhook_event = _receive_livekit_webhook(body, auth_token)
    payload = MessageToDict(webhook_event, preserving_proto_field_name=False)
    track = payload.get("track")
    if webhook_event.event == "track_published" and isinstance(track, dict):
        track["type"] = api.TrackType.Name(webhook_event.track.type)
    decision = await ingress_service.receive_livekit(
        event_type=webhook_event.event,
        room_name=webhook_event.room.name or None,
        participant_identity=webhook_event.participant.identity or None,
        payload=payload,
    )
    await db.commit()
    if decision.disposition == "LEGACY":
        result = schedule_livekit_webhook_event(
            event_type=webhook_event.event,
            room_name=webhook_event.room.name or None,
            participant_identity=webhook_event.participant.identity or None,
            payload=payload,
        )
    else:
        result = {
            "persisted": decision.disposition in {"INBOX", "QUARANTINE"},
            "disposition": decision.disposition,
            "rowId": str(decision.row_id) if decision.row_id is not None else None,
            "status": decision.status,
        }
    return SuccessResponse(data=result, msg="接收成功")


def _livekit_webhook_token(authorization: str | None) -> str:
    if not authorization:
        raise HTTPException(status_code=401, detail="LiveKit webhook authorization missing")
    value = authorization.strip()
    prefix = "Bearer "
    if value.lower().startswith(prefix.lower()):
        value = value[len(prefix) :].strip()
    if not value:
        raise HTTPException(status_code=401, detail="LiveKit webhook authorization missing")
    return value


def _receive_livekit_webhook(body: str, auth_token: str):
    try:
        receiver = api.WebhookReceiver(
            api.TokenVerifier(settings.LIVEKIT_API_KEY, settings.LIVEKIT_API_SECRET)
        )
        return receiver.receive(body, auth_token)
    except Exception as exc:
        raise HTTPException(status_code=401, detail="LiveKit webhook authorization invalid") from exc


@AiCallRouter.post(
    "/sessions",
    summary="创建 Web 通话会话",
    response_model=ResponseSchema[CreateSessionOut | RuntimeStartCallOut],
)
async def create_session_controller(
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
    request: Annotated[CreateWebSessionRequest, Body()],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    idempotency_key: Annotated[
        str | None,
        Header(alias="Idempotency-Key"),
    ] = None,
) -> JSONResponse:
    tenant_id = str(
        getattr(getattr(auth, "user", None), "tenant_id", "") or ""
    ).strip()
    if not tenant_id:
        raise HTTPException(status_code=401, detail="租户上下文缺失")
    if runtime_control_mode_for_entry(settings, "web") == "owner_command_v1":
        normalized_idempotency_key = str(idempotency_key or "").strip()
        if not normalized_idempotency_key:
            raise CustomException(
                msg="Web START_CALL 必须提供 Idempotency-Key",
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                data={"errorCode": "IDEMPOTENCY_KEY_REQUIRED"},
            )
        try:
            snapshot = await RuntimeEntryStartService(
                settings=settings,
                repository=RuntimeCommandRepository(auth.db),
            ).submit(
                StartEntryRequest(
                    tenant_id=tenant_id,
                    entry_type="web",
                    idempotency_key=normalized_idempotency_key,
                    payload={
                        "voice": request.voice,
                        "business_id": request.business_id,
                        "scene_code": request.scene_code,
                        "business_params": request.business_params,
                    },
                    business_id=request.business_id,
                    scene_code=request.scene_code,
                    allocation_timeout_seconds=(
                        settings.AI_CALL_WEB_ALLOCATION_TIMEOUT_SECONDS
                    ),
                )
            )
        except IdempotencyConflictError as exc:
            raise CustomException(
                msg="幂等键已用于不同的请求",
                status_code=status.HTTP_409_CONFLICT,
                data={"errorCode": "IDEMPOTENCY_CONFLICT"},
            ) from exc
        if snapshot is None:
            raise CustomException(
                msg="web 入口仍由 legacy_local 承载",
                status_code=status.HTTP_409_CONFLICT,
            )
        return SuccessResponse(
            data=RuntimeStartCallOut(
                command_id=str(snapshot.command_id),
                call_id=snapshot.call_id,
                command_seq=str(snapshot.command_seq),
                command_type=snapshot.command_type,
                status=snapshot.status,
            ),
            msg="START_CALL 已受理",
            status_code=status.HTTP_202_ACCEPTED,
        )

    result = await service.create_web_session(
        voice=request.voice,
        prompt=None,
        business_id=request.business_id,
        scene_code=request.scene_code,
        business_params=request.business_params,
        tenant_id=tenant_id,
    )
    return SuccessResponse(
        data=CreateSessionOut.model_validate(result),
        msg="创建成功",
    )


async def _get_sip_session_auth(
    request: Request,
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
    token: Annotated[str | None, Depends(OAuth2Schema)],
) -> AuthSchema | None:
    if runtime_control_mode_for_entry(settings, "direct_sip") != "owner_command_v1":
        return None
    record_service = getattr(service, "record_service", None)
    repository = getattr(record_service, "repository", None)
    db = getattr(repository, "db", None)
    if db is None:
        raise CustomException(
            msg="Direct SIP 认证数据库会话缺失",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    return await get_current_user(request=request, db=db, redis=None, token=token)


@AiCallRouter.post(
    "/sip-sessions",
    summary="创建 SIP 外呼会话",
    response_model=ResponseSchema[
        CreateSipSessionOut | RuntimeDirectSipStartCallOut
    ],
)
async def create_sip_session_controller(
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
    request: Annotated[CreateSipSessionRequest, Body()],
    auth: Annotated[AuthSchema | None, Depends(_get_sip_session_auth)] = None,
    idempotency_key: Annotated[
        str | None,
        Header(alias="Idempotency-Key"),
    ] = None,
) -> JSONResponse:
    if runtime_control_mode_for_entry(settings, "direct_sip") == "owner_command_v1":
        tenant_id = str(
            getattr(getattr(auth, "user", None), "tenant_id", "") or ""
        ).strip()
        if not tenant_id:
            raise HTTPException(status_code=401, detail="租户上下文缺失")
        normalized_idempotency_key = str(idempotency_key or "").strip()
        if not normalized_idempotency_key:
            raise CustomException(
                msg="Direct SIP START_CALL 必须提供 Idempotency-Key",
                status_code=status.HTTP_400_BAD_REQUEST,
                data={"errorCode": "IDEMPOTENCY_KEY_REQUIRED"},
            )
        try:
            phone = prepare_direct_sip_phone(request.callee_phone_number)
            snapshot = await RuntimeEntryStartService(
                settings=settings,
                repository=RuntimeCommandRepository(auth.db),
            ).submit(
                StartEntryRequest(
                    tenant_id=tenant_id,
                    entry_type="direct_sip",
                    idempotency_key=normalized_idempotency_key,
                    payload={
                        "voice": request.voice,
                        "business_id": request.business_id,
                        "scene_code": request.scene_code,
                        "business_params": request.business_params,
                        "ringing_timeout_seconds": request.ringing_timeout_seconds,
                    },
                    business_id=request.business_id,
                    scene_code=request.scene_code,
                    allocation_timeout_seconds=(
                        settings.AI_CALL_DIRECT_SIP_ALLOCATION_TIMEOUT_SECONDS
                    ),
                    callee_phone_number=phone.plaintext,
                )
            )
        except IdempotencyConflictError as exc:
            raise CustomException(
                msg="幂等键已用于不同的请求",
                status_code=status.HTTP_409_CONFLICT,
                data={"errorCode": "IDEMPOTENCY_CONFLICT"},
            ) from exc
        except (DirectSipPhoneError, RuntimeEntryStartError) as exc:
            raise CustomException(
                msg=str(exc),
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                data={"errorCode": "INVALID_START_REQUEST"},
            ) from exc
        if snapshot is None:
            raise CustomException(
                msg="Direct SIP 入口未启用 Owner Command 模式",
                status_code=status.HTTP_409_CONFLICT,
                data={"errorCode": "LEGACY_ENTRY_ACTIVE"},
            )
        return SuccessResponse(
            data=RuntimeDirectSipStartCallOut(
                command_id=str(snapshot.command_id),
                call_id=snapshot.call_id,
                command_seq=str(snapshot.command_seq),
                command_type=snapshot.command_type,
                status=snapshot.status,
                callee_phone_number_masked=phone.masked,
            ),
            msg="START_CALL 已受理",
            status_code=status.HTTP_202_ACCEPTED,
        )

    try:
        result = await service.create_sip_session(
            callee_phone_number=request.callee_phone_number,
            voice=request.voice,
            business_id=request.business_id,
            scene_code=request.scene_code,
            business_params=request.business_params,
            ringing_timeout_seconds=request.ringing_timeout_seconds,
        )
    except CustomException:
        await _commit_ai_call_record_audit(service)
        raise
    return SuccessResponse(
        data=CreateSipSessionOut.model_validate(result),
        msg="创建成功",
    )


async def _commit_ai_call_record_audit(service: AiCallService) -> None:
    record_service = getattr(service, "record_service", None)
    repository = getattr(record_service, "repository", None)
    db = getattr(repository, "db", None)
    if db is not None:
        await db.commit()


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
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
    call_id: Annotated[str | None, Query(alias="callId")] = None,
    task_id: Annotated[int | None, Query(alias="taskId", gt=0)] = None,
    target_id: Annotated[int | None, Query(alias="targetId", gt=0)] = None,
    phone_number: Annotated[str | None, Query(alias="phoneNumber")] = None,
    customer_name: Annotated[str | None, Query(alias="customerName")] = None,
    call_result: Annotated[str | None, Query(alias="callResult")] = None,
    customer_intent: Annotated[
        Literal["positive", "neutral", "negative", "pending", "failed"] | None,
        Query(alias="customerIntent"),
    ] = None,
    follow_up_status: Annotated[
        Literal[
            "suggested",
            "pending",
            "processing",
            "completed",
            "closed",
            "none",
        ]
        | None,
        Query(alias="followUpStatus"),
    ] = None,
    business_type: Annotated[str | None, Query(alias="businessType")] = None,
    business_id: Annotated[str | None, Query(alias="businessId")] = None,
    status: str | None = None,
    entry_type: Annotated[str | None, Query(alias="entryType")] = None,
    formal_outbound_only: Annotated[
        bool,
        Query(alias="formalOutboundOnly"),
    ] = False,
    started_at_begin: Annotated[datetime | None, Query(alias="startedAtBegin")] = None,
    started_at_end: Annotated[datetime | None, Query(alias="startedAtEnd")] = None,
    page_num: Annotated[int, Query(alias="pageNum", ge=1)] = 1,
    page_size: Annotated[int, Query(alias="pageSize", ge=1, le=1000)] = 10,
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    result = await service.list_records(
        tenant_id=tenant_id,
        call_id=call_id,
        task_id=task_id,
        target_id=target_id,
        phone_number=phone_number,
        customer_name=customer_name,
        call_result=call_result,
        customer_intent=customer_intent,
        follow_up_status=follow_up_status,
        business_type=business_type,
        business_id=business_id,
        status=status,
        entry_type=entry_type,
        formal_outbound_only=formal_outbound_only,
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
    "/records/{callId}/interrupt-summary",
    summary="查询通话打断摘要",
    response_model=ResponseSchema[InterruptSummaryOut],
)
async def get_record_interrupt_summary_controller(
    call_id: Annotated[str, Path(alias="callId")],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.get_record_interrupt_summary(call_id)
    return SuccessResponse(data=InterruptSummaryOut.model_validate(result), msg="查询成功")


@AiCallRouter.get(
    "/records/{callId}/semantic-analysis",
    summary="查询通话语义分析",
    response_model=ResponseSchema[SemanticAnalysisOut | None],
)
async def get_record_semantic_analysis_controller(
    call_id: Annotated[str, Path(alias="callId")],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.get_record_semantic_analysis(call_id)
    return SuccessResponse(
        data=SemanticAnalysisOut.model_validate(result) if result else None,
        msg="查询成功",
    )


@AiCallRouter.post(
    "/records/{callId}/semantic-analysis/reanalyze",
    summary="重新生成通话语义分析",
    response_model=ResponseSchema[SemanticAnalysisOut],
)
async def reanalyze_record_semantic_analysis_controller(
    call_id: Annotated[str, Path(alias="callId")],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.reanalyze_record_semantic_analysis(call_id)
    return SuccessResponse(data=SemanticAnalysisOut.model_validate(result), msg="重分析完成")


@AiCallRouter.get(
    "/records/{callId}/recording",
    summary="查询通话录音",
    response_model=ResponseSchema[RecordingOut | None],
)
async def get_recording_controller(
    call_id: Annotated[str, Path(alias="callId")],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    result = await service.get_recording(tenant_id=tenant_id, call_id=call_id)
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
    auth: Annotated[AuthSchema, Depends(get_current_user)],
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    result = await service.report_browser_event(
        call_id=call_id,
        event_type=request.type,
        timestamp=request.timestamp,
        payload=request.model_dump(
            by_alias=True,
            exclude={"type", "timestamp"},
            exclude_none=True,
        ),
        tenant_id=tenant_id,
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


@AiCallRouter.get(
    "/handoff-agents/{agentIdentity}",
    summary="查询坐席状态",
    response_model=ResponseSchema[HandoffAgentOut],
)
async def get_handoff_agent_controller(
    agent_identity: Annotated[str, Path(alias="agentIdentity")],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.get_handoff_agent_status(agent_identity)
    return SuccessResponse(data=HandoffAgentOut.model_validate(result), msg="查询成功")


@AiCallRouter.post(
    "/handoff-agents/{agentIdentity}/status",
    summary="更新坐席状态",
    response_model=ResponseSchema[HandoffAgentOut],
)
async def update_handoff_agent_status_controller(
    agent_identity: Annotated[str, Path(alias="agentIdentity")],
    request: HandoffAgentStatusRequest,
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
) -> JSONResponse:
    result = await service.set_handoff_agent_status(
        human_agent_identity=agent_identity,
        status=request.status,
        skill_group=request.skill_group,
    )
    return SuccessResponse(data=HandoffAgentOut.model_validate(result), msg="保存成功")


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
    response_model=ResponseSchema[HandoffOut | RuntimeHandoffCommandOut],
)
async def cancel_handoff_controller(
    handoff_id: Annotated[str, Path(alias="handoffId")],
    service: Annotated[AiCallService, Depends(get_ai_call_service)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    request: Annotated[FinishHandoffRequest | None, Body()] = None,
    idempotency_key: Annotated[
        str | None,
        Header(alias="Idempotency-Key"),
    ] = None,
) -> JSONResponse:
    tenant_id = str(getattr(auth.user, "tenant_id", "") or "").strip()
    if not tenant_id:
        raise HTTPException(status_code=401, detail="租户上下文缺失")
    runtime_control_mode = await auth.db.scalar(
        select(AiCallRecordModel.runtime_control_mode)
        .select_from(AiCallHandoffModel)
        .join(
            AiCallRecordModel,
            (AiCallRecordModel.tenant_id == AiCallHandoffModel.tenant_id)
            & (AiCallRecordModel.call_id == AiCallHandoffModel.call_id),
        )
        .where(
            AiCallHandoffModel.tenant_id == tenant_id,
            AiCallHandoffModel.handoff_id == handoff_id,
        )
    )
    if runtime_control_mode is None:
        raise CustomException(msg="转人工任务不存在", status_code=404)
    if runtime_control_mode == "owner_command_v1":
        normalized_idempotency_key = str(idempotency_key or "").strip()
        if not normalized_idempotency_key:
            raise CustomException(
                msg="Owner 模式取消必须提供 Idempotency-Key",
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                data={"errorCode": "IDEMPOTENCY_KEY_REQUIRED"},
            )
        try:
            decision = await RuntimeHandoffRepository(auth.db).request_cancel(
                HandoffCancelIntent(
                    tenant_id=tenant_id,
                    handoff_id=handoff_id,
                    idempotency_key=normalized_idempotency_key,
                    reason=(request.reason if request and request.reason else "operator_cancelled"),
                )
            )
        except HandoffNotFoundError as exc:
            raise CustomException(msg="转人工任务不存在", status_code=404) from exc
        except HandoffIdempotencyConflictError as exc:
            raise CustomException(
                msg="幂等键已用于其他转人工操作",
                status_code=status.HTTP_409_CONFLICT,
                data={"errorCode": "IDEMPOTENCY_CONFLICT"},
            ) from exc
        except (
            HandoffClaimConflictError,
            HandoffRuntimeModeError,
            HandoffTerminalBarrierError,
        ) as exc:
            raise CustomException(
                msg="当前转人工状态不允许取消",
                status_code=status.HTTP_409_CONFLICT,
                data={"errorCode": "HANDOFF_CANCEL_CONFLICT"},
            ) from exc
        await auth.db.commit()
        return SuccessResponse(
            data=RuntimeHandoffCommandOut(
                handoff_id=decision.handoff_id,
                call_id=decision.call_id,
                handoff_status=decision.handoff_status,
                command_id=str(decision.command_id),
                command_seq=str(decision.command_seq),
                command_status=decision.command_status,
            ),
            msg="CANCEL_HANDOFF 已受理",
            status_code=status.HTTP_202_ACCEPTED,
        )
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
