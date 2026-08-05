from __future__ import annotations

import asyncio
import json
from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, Depends, Header, Query, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.system.auth.schema import AuthSchema
from app.common.response import SuccessResponse, TableResponse
from app.core.dependencies import db_getter, get_current_user
from app.core.exceptions import CustomException
from app.services.ai_call.agent_console_reconciler import (
    AiCallAgentConsoleReconciler,
    agent_console_event_broker,
    agent_console_stream_registry,
    publish_agent_console_event,
)
from app.services.ai_call.agent_console_service import AiCallAgentConsoleService
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.follow_up_service import AiCallFollowUpService
from app.services.ai_call.livekit_sip import HumanOnlySipSessionFactory

from .agent_console_schema import (
    AfterCallWorkIn,
    AgentAdminActionIn,
    AgentHandoffClaimIn,
    AgentMediaReadyIn,
    AgentPresenceOnlineIn,
    AgentPresenceSessionIn,
    AgentProfileCreateIn,
    AgentProfileUpdateIn,
    AgentSceneScopesIn,
    FollowUpAttemptIn,
    FollowUpCallIn,
    FollowUpCloseIn,
)
from .service import (
    end_agent_handoff_session_background,
    get_default_ai_call_service,
)

AgentConsoleRouter = APIRouter(prefix="/agent-console", tags=["AI Call 坐席工作台"])
AgentAdminRouter = APIRouter(prefix="/admin", tags=["AI Call 坐席管理"])

AuthenticatedUser = Annotated[AuthSchema, Depends(get_current_user)]


async def get_agent_console_service(
    db: Annotated[AsyncSession, Depends(db_getter)],
) -> AiCallAgentConsoleService:
    room_manager = get_default_ai_call_service(db).orchestrator.livekit_room_manager
    return AiCallAgentConsoleService(
        db,
        participant_verifier=room_manager.has_published_microphone,
    )


async def get_agent_console_complete_service(
    db: Annotated[AsyncSession, Depends(db_getter, scope="function")],
) -> AiCallAgentConsoleService:
    return AiCallAgentConsoleService(db)


async def get_follow_up_service(
    db: Annotated[AsyncSession, Depends(db_getter)],
) -> AiCallFollowUpService:
    ai_call_service = get_default_ai_call_service(db)
    callback_factory = None
    if ai_call_service.sip_client is not None:
        callback_factory = HumanOnlySipSessionFactory(
            room_manager=ai_call_service.orchestrator.livekit_room_manager,
            sip_client=ai_call_service.sip_client,
        )
    return AiCallFollowUpService(db, callback_factory=callback_factory)


async def get_agent_console_reconciler(
    db: Annotated[AsyncSession, Depends(db_getter)],
) -> AiCallAgentConsoleReconciler:
    room_manager = get_default_ai_call_service(db).orchestrator.livekit_room_manager
    return AiCallAgentConsoleReconciler(db, room_exists=room_manager.room_exists)


def _tenant_id(auth: AuthSchema) -> str:
    return str(getattr(auth.user, "tenant_id", None) or "000000")


def _sse_event(event) -> str:
    data = {
        "sequence": event.sequence,
        "type": event.event_type,
        "payload": event.payload,
        "occurred_at": event.occurred_at.isoformat(),
    }
    return (
        f"id: {event.sequence}\n"
        f"event: {event.event_type}\n"
        f"data: {json.dumps(data, ensure_ascii=False, separators=(',', ':'))}\n\n"
    )


async def _publish(auth: AuthSchema, event_type: str, payload: dict) -> None:
    await publish_agent_console_event(_tenant_id(auth), event_type, payload)


async def _agent_console_event_stream(
    request: Request,
    *,
    tenant_id: str,
    agent_identity: str,
    after_sequence: int,
    broker=agent_console_event_broker,
    registry=agent_console_stream_registry,
):
    lease = registry.replace(tenant_id, agent_identity)
    sequence = after_sequence
    try:
        while registry.is_current(lease):
            if await request.is_disconnected():
                return
            event_wait = asyncio.create_task(
                broker.wait_for_events(tenant_id, sequence)
            )
            replacement_wait = asyncio.create_task(lease.replaced.wait())
            try:
                completed, pending = await asyncio.wait(
                    {event_wait, replacement_wait},
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                for task in (event_wait, replacement_wait):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(
                    event_wait,
                    replacement_wait,
                    return_exceptions=True,
                )
            if replacement_wait in completed or not registry.is_current(lease):
                return
            events = event_wait.result()
            if await request.is_disconnected():
                return
            if not events:
                yield ": heartbeat\n\n"
                continue
            for event in events:
                sequence = event.sequence
                yield _sse_event(event)
    finally:
        registry.release(lease)


def _issue_handoff_token(auth: AuthSchema, handoff) -> dict:
    if not handoff.human_agent_identity:
        raise CustomException(msg="认领结果缺少坐席身份", status_code=500)
    ai_call_service = get_default_ai_call_service(auth.db)
    if ai_call_service.handoff_exception_manager is not None:
        ai_call_service.handoff_exception_manager.schedule_timeout(handoff)
    try:
        token = ai_call_service.orchestrator.issue_handoff_token(
            call_id=handoff.call_id,
            handoff_id=handoff.handoff_id,
            human_agent_identity=handoff.human_agent_identity,
        )
    except AiCallError as exc:
        if exc.error_id == "session_not_found":
            token = ai_call_service.orchestrator.issue_handoff_token_for_room(
                call_id=handoff.call_id,
                handoff_id=handoff.handoff_id,
                human_agent_identity=handoff.human_agent_identity,
                room_name=handoff.room_name,
            )
        else:
            error_code = (
                "CUSTOMER_NOT_CONNECTED"
                if exc.error_id == "invalid_session_state"
                else exc.error_id
            )
            raise CustomException(
                msg=exc.msg,
                status_code=exc.status_code,
                data={"errorCode": error_code},
            ) from exc
    return {
        "call_id": token.call_id,
        "handoff_id": token.handoff_id,
        "room_name": token.room_name,
        "livekit_url": token.livekit_url,
        "participant_token": token.participant_token,
        "participant_identity": token.participant_identity,
        "expires_in_seconds": token.expires_in_seconds,
    }


@AgentConsoleRouter.get("/bootstrap", summary="获取坐席工作台启动状态")
async def bootstrap_controller(
    auth: AuthenticatedUser,
    service: Annotated[AiCallAgentConsoleService, Depends(get_agent_console_service)],
):
    data = await service.bootstrap_payload(auth)
    data["event_sequence"] = agent_console_event_broker.latest_sequence(_tenant_id(auth))
    return SuccessResponse(data=data)


@AgentConsoleRouter.get("/events", summary="订阅坐席中心状态变化")
async def agent_console_events_controller(
    request: Request,
    auth: AuthenticatedUser,
    after_sequence: Annotated[int, Query(ge=0)] = 0,
):
    tenant_id = _tenant_id(auth)
    profile = await AiCallAgentConsoleService(auth.db).require_current_agent(auth)
    agent_identity = profile.agent_identity
    # SSE 不再读取数据库，提前释放认证查询占用的连接，避免长连接耗尽连接池。
    await auth.db.close()

    return StreamingResponse(
        _agent_console_event_stream(
            request,
            tenant_id=tenant_id,
            agent_identity=agent_identity,
            after_sequence=after_sequence,
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@AgentConsoleRouter.post("/presence/online", summary="坐席设备预检通过后上线")
async def online_controller(
    payload: AgentPresenceOnlineIn,
    auth: AuthenticatedUser,
    service: Annotated[AiCallAgentConsoleService, Depends(get_agent_console_service)],
):
    presence = await service.online(
        auth,
        console_session_id=str(payload.console_session_id),
        device_preflight_passed=payload.device_preflight_passed,
    )
    await _publish(
        auth,
        "presence.changed",
        {"agent_identity": presence.agent_identity, "status": presence.status},
    )
    return SuccessResponse(data=service.presence_payload(presence, presence.agent_identity))


@AgentConsoleRouter.post("/presence/heartbeat", summary="刷新坐席工作台心跳")
async def heartbeat_controller(
    payload: AgentPresenceSessionIn,
    auth: AuthenticatedUser,
    service: Annotated[AiCallAgentConsoleService, Depends(get_agent_console_service)],
):
    presence = await service.heartbeat(
        auth,
        console_session_id=str(payload.console_session_id),
    )
    return SuccessResponse(data=service.presence_payload(presence, presence.agent_identity))


@AgentConsoleRouter.post("/presence/pause", summary="暂停坐席接单")
async def pause_controller(
    payload: AgentPresenceSessionIn,
    auth: AuthenticatedUser,
    service: Annotated[AiCallAgentConsoleService, Depends(get_agent_console_service)],
):
    presence = await service.pause(auth, console_session_id=str(payload.console_session_id))
    await _publish(
        auth,
        "presence.changed",
        {"agent_identity": presence.agent_identity, "status": presence.status},
    )
    return SuccessResponse(data=service.presence_payload(presence, presence.agent_identity))


@AgentConsoleRouter.post("/presence/offline", summary="坐席下线")
async def offline_controller(
    payload: AgentPresenceSessionIn,
    auth: AuthenticatedUser,
    service: Annotated[AiCallAgentConsoleService, Depends(get_agent_console_service)],
):
    presence = await service.offline(auth, console_session_id=str(payload.console_session_id))
    await _publish(
        auth,
        "presence.changed",
        {"agent_identity": presence.agent_identity, "status": presence.status},
    )
    return SuccessResponse(data=service.presence_payload(presence, presence.agent_identity))


@AgentConsoleRouter.get("/handoffs/pending", summary="查询当前坐席公共待接池")
async def pending_handoffs_controller(
    auth: AuthenticatedUser,
    service: Annotated[AiCallAgentConsoleService, Depends(get_agent_console_service)],
    console_session_id: Annotated[UUID, Query()],
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
):
    rows = await service.list_pending_handoffs(
        auth,
        console_session_id=str(console_session_id),
        limit=limit,
    )
    return TableResponse(
        rows=await service.handoff_payloads(rows),
        total=len(rows),
    )


@AgentConsoleRouter.get(
    "/handoffs/{handoff_id}/context",
    summary="获取单条转人工完整上下文",
)
async def handoff_context_controller(
    handoff_id: str,
    auth: AuthenticatedUser,
    service: Annotated[AiCallAgentConsoleService, Depends(get_agent_console_service)],
    console_session_id: Annotated[UUID, Query()],
):
    return SuccessResponse(
        data=await service.handoff_context_payload(
            auth,
            handoff_id=handoff_id,
            console_session_id=str(console_session_id),
        )
    )


@AgentConsoleRouter.post("/handoffs/{handoff_id}/claim", summary="原子认领转人工任务")
async def claim_handoff_controller(
    handoff_id: str,
    payload: AgentHandoffClaimIn,
    auth: AuthenticatedUser,
    service: Annotated[AiCallAgentConsoleService, Depends(get_agent_console_service)],
    idempotency_key: Annotated[
        str | None,
        Header(alias="Idempotency-Key"),
    ] = None,
):
    claim_result = await service.claim_handoff_with_payload(
        auth,
        handoff_id=handoff_id,
        console_session_id=str(payload.console_session_id),
        idempotency_key=idempotency_key,
    )
    handoff = claim_result.handoff
    await _publish(
        auth,
        "handoff.changed",
        {"handoff_id": handoff.handoff_id, "call_id": handoff.call_id, "status": handoff.status},
    )
    if claim_result.command is None:
        return SuccessResponse(
            data={
                "handoff": claim_result.payload,
                "seat_token": _issue_handoff_token(auth, handoff),
            }
        )
    data = {
        "handoff": claim_result.payload,
        "acceptanceStatus": "ACCEPTED",
        "handoffId": claim_result.command.handoff_id,
        "commandId": str(claim_result.command.command_id),
        "commandSeq": str(claim_result.command.command_seq),
        "commandStatus": claim_result.command.command_status,
        "seat_token": _issue_handoff_token(auth, handoff),
    }
    return SuccessResponse(
        data=data,
        status_code=status.HTTP_202_ACCEPTED,
    )


@AgentConsoleRouter.post("/handoffs/{handoff_id}/media-ready", summary="确认坐席媒体已就绪")
async def media_ready_controller(
    handoff_id: str,
    payload: AgentMediaReadyIn,
    auth: AuthenticatedUser,
    service: Annotated[AiCallAgentConsoleService, Depends(get_agent_console_service)],
):
    handoff = await service.media_ready(
        auth,
        handoff_id=handoff_id,
        console_session_id=str(payload.console_session_id),
        participant_identity=payload.participant_identity,
    )
    ai_call_service = get_default_ai_call_service(auth.db)
    if ai_call_service.handoff_exception_manager is not None:
        ai_call_service.handoff_exception_manager.cancel_timeout(
            handoff.handoff_id,
            call_id=handoff.call_id,
            handoff_status=handoff.status,
            reason="media_ready",
        )
        ai_call_service.handoff_exception_manager.stop_waiting_tone(
            handoff.handoff_id,
            call_id=handoff.call_id,
            handoff_status=handoff.status,
            reason="media_ready",
        )
    if ai_call_service.recording_service is not None:
        await ai_call_service.recording_service.start_human_agent_recording(
            tenant_id=handoff.tenant_id,
            call_id=handoff.call_id,
            room_name=handoff.room_name,
            handoff_id=handoff.handoff_id,
            participant_identity=payload.participant_identity,
        )
    try:
        ai_call_service.orchestrator.record_handoff_event(
            call_id=handoff.call_id,
            event_type="handoff_connected",
            handoff_id=handoff.handoff_id,
            handoff_status=handoff.status,
            payload={"participantIdentity": payload.participant_identity},
        )
    except AiCallError:
        pass
    await _publish(
        auth,
        "handoff.changed",
        {"handoff_id": handoff.handoff_id, "call_id": handoff.call_id, "status": handoff.status},
    )
    return SuccessResponse(data=await service.handoff_payload(handoff))


@AgentConsoleRouter.post(
    "/handoffs/{handoff_id}/reconnect-token",
    summary="进入重连窗口并重新签发坐席 Token",
)
async def reconnect_token_controller(
    handoff_id: str,
    payload: AgentPresenceSessionIn,
    auth: AuthenticatedUser,
    service: Annotated[AiCallAgentConsoleService, Depends(get_agent_console_service)],
):
    handoff = await service.begin_reconnect(
        auth,
        handoff_id=handoff_id,
        console_session_id=str(payload.console_session_id),
    )
    await _publish(
        auth,
        "handoff.changed",
        {"handoff_id": handoff.handoff_id, "call_id": handoff.call_id, "status": handoff.status},
    )
    return SuccessResponse(
        data={
            "handoff": await service.handoff_payload(handoff),
            "seat_token": _issue_handoff_token(auth, handoff),
        }
    )


@AgentConsoleRouter.post("/handoffs/{handoff_id}/complete", summary="结束人工通话并进入快速话后确认")
async def complete_agent_handoff_controller(
    handoff_id: str,
    payload: AgentPresenceSessionIn,
    background_tasks: BackgroundTasks,
    auth: AuthenticatedUser,
    service: Annotated[
        AiCallAgentConsoleService,
        Depends(get_agent_console_complete_service),
    ],
):
    handoff = await service.complete_handoff(
        auth,
        handoff_id=handoff_id,
        console_session_id=str(payload.console_session_id),
    )
    background_tasks.add_task(
        end_agent_handoff_session_background,
        handoff.call_id,
        handoff.end_reason,
    )
    await _publish(
        auth,
        "handoff.changed",
        {"handoff_id": handoff.handoff_id, "call_id": handoff.call_id, "status": handoff.status},
    )
    return SuccessResponse(data=await service.handoff_payload(handoff))


@AgentConsoleRouter.put("/calls/{call_id}/after-call-work", summary="提交快速话后确认")
async def submit_after_call_work_controller(
    call_id: str,
    payload: AfterCallWorkIn,
    auth: AuthenticatedUser,
    service: Annotated[AiCallFollowUpService, Depends(get_follow_up_service)],
):
    work, follow_up = await service.submit_after_call_work(
        auth,
        call_id=call_id,
        payload=payload,
    )
    await _publish(
        auth,
        "follow_up.changed",
        {
            "call_id": call_id,
            "follow_up_id": str(follow_up.id) if follow_up else None,
            "status": follow_up.status if follow_up else None,
        },
    )
    return SuccessResponse(
        data={
            "after_call_work": service.after_call_work_payload(work),
            "follow_up": service.follow_up_payload(follow_up) if follow_up else None,
        }
    )


@AgentConsoleRouter.get("/follow-ups", summary="查询本人跟进和公共人工未接回访")
async def list_follow_ups_controller(
    auth: AuthenticatedUser,
    service: Annotated[AiCallFollowUpService, Depends(get_follow_up_service)],
):
    rows = await service.list_follow_ups(auth)
    return TableResponse(
        rows=[service.follow_up_payload(task) for task in rows],
        total=len(rows),
    )


@AgentConsoleRouter.get("/follow-ups/{follow_up_id}", summary="查询跟进任务详情")
async def get_follow_up_controller(
    follow_up_id: int,
    auth: AuthenticatedUser,
    service: Annotated[AiCallFollowUpService, Depends(get_follow_up_service)],
):
    task = await service.get_follow_up(auth, follow_up_id=follow_up_id)
    return SuccessResponse(data=service.follow_up_payload(task))


@AgentConsoleRouter.post("/follow-ups/{follow_up_id}/attempts", summary="追加联系尝试")
async def append_follow_up_attempt_controller(
    follow_up_id: int,
    payload: FollowUpAttemptIn,
    auth: AuthenticatedUser,
    service: Annotated[AiCallFollowUpService, Depends(get_follow_up_service)],
):
    attempt = await service.append_attempt(
        auth,
        follow_up_id=follow_up_id,
        payload=payload,
    )
    await _publish(
        auth,
        "follow_up.changed",
        {"follow_up_id": str(attempt.follow_up_id), "attempt_result": attempt.attempt_result},
    )
    return SuccessResponse(data=service.attempt_payload(attempt))


@AgentConsoleRouter.post("/follow-ups/{follow_up_id}/claim", summary="原子认领人工未接回访")
async def claim_follow_up_controller(
    follow_up_id: int,
    auth: AuthenticatedUser,
    service: Annotated[AiCallFollowUpService, Depends(get_follow_up_service)],
):
    task = await service.claim_follow_up(auth, follow_up_id=follow_up_id)
    await _publish(
        auth,
        "follow_up.changed",
        {"follow_up_id": str(task.id), "status": task.status},
    )
    return SuccessResponse(data=service.follow_up_payload(task))


@AgentConsoleRouter.post("/follow-ups/{follow_up_id}/call", summary="发起浏览器人工回拨")
async def start_follow_up_call_controller(
    follow_up_id: int,
    payload: FollowUpCallIn,
    auth: AuthenticatedUser,
    service: Annotated[AiCallFollowUpService, Depends(get_follow_up_service)],
):
    callback = await service.start_callback(
        auth,
        follow_up_id=follow_up_id,
        payload=payload,
    )
    await _publish(
        auth,
        "follow_up.callback_started",
        {"follow_up_id": str(follow_up_id), "call_id": callback.call_id},
    )
    return SuccessResponse(data=service.callback_payload(callback), msg="回拨任务已受理")


@AgentConsoleRouter.post(
    "/follow-ups/{follow_up_id}/call/{call_id}/end",
    summary="结束浏览器人工回拨",
)
async def end_follow_up_call_controller(
    follow_up_id: int,
    call_id: str,
    payload: AgentPresenceSessionIn,
    auth: AuthenticatedUser,
    service: Annotated[AiCallFollowUpService, Depends(get_follow_up_service)],
):
    record = await service.end_callback(
        auth,
        follow_up_id=follow_up_id,
        call_id=call_id,
        payload=payload,
    )
    await _publish(
        auth,
        "follow_up.callback_ended",
        {"follow_up_id": str(follow_up_id), "call_id": call_id},
    )
    return SuccessResponse(
        data={"call_id": call_id, "status": record.status, "end_reason": record.end_reason}
    )


@AgentConsoleRouter.post("/follow-ups/{follow_up_id}/complete", summary="完成跟进任务")
async def complete_follow_up_controller(
    follow_up_id: int,
    auth: AuthenticatedUser,
    service: Annotated[AiCallFollowUpService, Depends(get_follow_up_service)],
):
    task = await service.complete_follow_up(auth, follow_up_id=follow_up_id)
    await _publish(
        auth,
        "follow_up.changed",
        {"follow_up_id": str(task.id), "status": task.status},
    )
    return SuccessResponse(data=service.follow_up_payload(task))


@AgentConsoleRouter.post("/follow-ups/{follow_up_id}/close", summary="关闭跟进任务")
async def close_follow_up_controller(
    follow_up_id: int,
    payload: FollowUpCloseIn,
    auth: AuthenticatedUser,
    service: Annotated[AiCallFollowUpService, Depends(get_follow_up_service)],
):
    task = await service.close_follow_up(
        auth,
        follow_up_id=follow_up_id,
        payload=payload,
    )
    await _publish(
        auth,
        "follow_up.changed",
        {"follow_up_id": str(task.id), "status": task.status},
    )
    return SuccessResponse(data=service.follow_up_payload(task))


@AgentAdminRouter.get("/agents", summary="查询坐席档案")
async def list_agents_controller(
    auth: AuthenticatedUser,
    service: Annotated[
        AiCallAgentConsoleReconciler,
        Depends(get_agent_console_reconciler),
    ],
):
    return SuccessResponse(data=await service.list_agents(auth))


@AgentAdminRouter.post("/agents", summary="创建坐席档案")
async def create_agent_controller(
    payload: AgentProfileCreateIn,
    auth: AuthenticatedUser,
    service: Annotated[AiCallAgentConsoleService, Depends(get_agent_console_service)],
):
    profile = await service.create_profile(auth, payload)
    return SuccessResponse(data=await service.profile_payload(profile))


@AgentAdminRouter.put("/agents/{agent_id}", summary="更新坐席档案")
async def update_agent_controller(
    agent_id: int,
    payload: AgentProfileUpdateIn,
    auth: AuthenticatedUser,
    service: Annotated[AiCallAgentConsoleService, Depends(get_agent_console_service)],
):
    profile = await service.update_profile(auth, agent_id, enabled=payload.enabled)
    return SuccessResponse(data=await service.profile_payload(profile))


@AgentAdminRouter.put("/agents/{agent_id}/scene-scopes", summary="整组替换坐席场景授权")
async def replace_agent_scene_scopes_controller(
    agent_id: int,
    payload: AgentSceneScopesIn,
    auth: AuthenticatedUser,
    service: Annotated[AiCallAgentConsoleService, Depends(get_agent_console_service)],
):
    scene_codes = await service.replace_scene_scopes(auth, agent_id, payload)
    return SuccessResponse(data={"sceneCodes": scene_codes})


@AgentAdminRouter.get("/agents/{agent_id}/status", summary="查询坐席运行状态")
async def get_agent_status_controller(
    agent_id: int,
    auth: AuthenticatedUser,
    service: Annotated[
        AiCallAgentConsoleReconciler,
        Depends(get_agent_console_reconciler),
    ],
):
    return SuccessResponse(data=await service.get_agent_status(auth, agent_id))


@AgentAdminRouter.post("/agents/{agent_id}/release-stale", summary="释放坐席异常占用")
async def release_stale_agent_controller(
    agent_id: int,
    payload: AgentAdminActionIn,
    auth: AuthenticatedUser,
    service: Annotated[
        AiCallAgentConsoleReconciler,
        Depends(get_agent_console_reconciler),
    ],
):
    result = await service.release_stale_agent(
        auth,
        agent_id=agent_id,
        confirmed=payload.confirmed,
        reason=payload.reason,
    )
    await _publish(
        auth,
        "presence.changed",
        {"agent_identity": result["agent_identity"], "status": result["status"]},
    )
    return SuccessResponse(data=result)


@AgentAdminRouter.get("/handoffs", summary="查询转人工记录与指标")
async def list_admin_handoffs_controller(
    auth: AuthenticatedUser,
    service: Annotated[
        AiCallAgentConsoleReconciler,
        Depends(get_agent_console_reconciler),
    ],
):
    return SuccessResponse(data=await service.list_handoffs(auth))


@AgentAdminRouter.get("/handoffs/{handoff_id}", summary="查询转人工记录详情")
async def get_admin_handoff_controller(
    handoff_id: str,
    auth: AuthenticatedUser,
    service: Annotated[
        AiCallAgentConsoleReconciler,
        Depends(get_agent_console_reconciler),
    ],
):
    return SuccessResponse(data=await service.get_handoff_detail(auth, handoff_id))


@AgentAdminRouter.post("/handoffs/{handoff_id}/reconcile", summary="重新执行转人工状态补偿")
async def reconcile_admin_handoff_controller(
    handoff_id: str,
    payload: AgentAdminActionIn,
    auth: AuthenticatedUser,
    service: Annotated[
        AiCallAgentConsoleReconciler,
        Depends(get_agent_console_reconciler),
    ],
):
    result = await service.reconcile_handoff(
        auth,
        handoff_id=handoff_id,
        confirmed=payload.confirmed,
        reason=payload.reason,
    )
    return SuccessResponse(data=result)


@AgentAdminRouter.get("/follow-ups", summary="查询跟进任务与指标")
async def list_admin_follow_ups_controller(
    auth: AuthenticatedUser,
    service: Annotated[
        AiCallAgentConsoleReconciler,
        Depends(get_agent_console_reconciler),
    ],
    status: Annotated[str | None, Query()] = None,
    formal_outbound_only: Annotated[
        bool,
        Query(alias="formalOutboundOnly"),
    ] = False,
    source_started_at_begin: Annotated[
        datetime | None,
        Query(alias="sourceStartedAtBegin"),
    ] = None,
    source_started_at_end: Annotated[
        datetime | None,
        Query(alias="sourceStartedAtEnd"),
    ] = None,
    page_num: Annotated[int, Query(alias="pageNum", ge=1)] = 1,
    page_size: Annotated[int, Query(alias="pageSize", ge=1, le=100)] = 10,
):
    return SuccessResponse(
        data=await service.list_follow_ups(
            auth,
            status=status,
            formal_outbound_only=formal_outbound_only,
            source_started_at_begin=source_started_at_begin,
            source_started_at_end=source_started_at_end,
            page_num=page_num,
            page_size=page_size,
        )
    )


@AgentAdminRouter.get("/follow-ups/{follow_up_id}", summary="查询跟进任务详情")
async def get_admin_follow_up_controller(
    follow_up_id: int,
    auth: AuthenticatedUser,
    service: Annotated[
        AiCallAgentConsoleReconciler,
        Depends(get_agent_console_reconciler),
    ],
):
    return SuccessResponse(data=await service.get_follow_up_detail(auth, follow_up_id))
