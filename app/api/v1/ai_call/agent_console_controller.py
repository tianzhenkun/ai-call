from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.system.auth.schema import AuthSchema
from app.common.response import SuccessResponse, TableResponse
from app.core.dependencies import db_getter, get_current_user
from app.core.exceptions import CustomException
from app.services.ai_call.agent_console_service import AiCallAgentConsoleService
from app.services.ai_call.exceptions import AiCallError

from .agent_console_schema import (
    AgentHandoffClaimIn,
    AgentMediaReadyIn,
    AgentPresenceOnlineIn,
    AgentPresenceSessionIn,
    AgentProfileCreateIn,
    AgentProfileUpdateIn,
    AgentSceneScopesIn,
)
from .service import get_default_ai_call_service

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
        error_code = (
            "CUSTOMER_NOT_CONNECTED"
            if exc.error_id in {"session_not_found", "invalid_session_state"}
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
    return SuccessResponse(data=await service.bootstrap_payload(auth))


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
    return SuccessResponse(data=service.presence_payload(presence, presence.agent_identity))


@AgentConsoleRouter.post("/presence/offline", summary="坐席下线")
async def offline_controller(
    payload: AgentPresenceSessionIn,
    auth: AuthenticatedUser,
    service: Annotated[AiCallAgentConsoleService, Depends(get_agent_console_service)],
):
    presence = await service.offline(auth, console_session_id=str(payload.console_session_id))
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
        rows=[service.handoff_payload(handoff) for handoff in rows],
        total=len(rows),
    )


@AgentConsoleRouter.post("/handoffs/{handoff_id}/claim", summary="原子认领转人工任务")
async def claim_handoff_controller(
    handoff_id: str,
    payload: AgentHandoffClaimIn,
    auth: AuthenticatedUser,
    service: Annotated[AiCallAgentConsoleService, Depends(get_agent_console_service)],
):
    handoff = await service.claim_handoff(
        auth,
        handoff_id=handoff_id,
        console_session_id=str(payload.console_session_id),
    )
    return SuccessResponse(
        data={
            "handoff": service.handoff_payload(handoff),
            "seat_token": _issue_handoff_token(auth, handoff),
        }
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
    return SuccessResponse(data=service.handoff_payload(handoff))


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
    return SuccessResponse(
        data={
            "handoff": service.handoff_payload(handoff),
            "seat_token": _issue_handoff_token(auth, handoff),
        }
    )


@AgentAdminRouter.get("/agents", summary="查询坐席档案")
async def list_agents_controller(
    auth: AuthenticatedUser,
    service: Annotated[AiCallAgentConsoleService, Depends(get_agent_console_service)],
):
    profiles = await service.list_profiles(auth)
    rows = [await service.profile_payload(profile) for profile in profiles]
    return TableResponse(rows=rows, total=len(rows))


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
