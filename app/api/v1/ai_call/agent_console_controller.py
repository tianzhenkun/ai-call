from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.system.auth.schema import AuthSchema
from app.common.response import SuccessResponse, TableResponse
from app.core.dependencies import db_getter, get_current_user
from app.services.ai_call.agent_console_service import AiCallAgentConsoleService

from .agent_console_schema import AgentProfileCreateIn, AgentProfileUpdateIn, AgentSceneScopesIn

AgentConsoleRouter = APIRouter(prefix="/agent-console", tags=["AI Call 坐席工作台"])
AgentAdminRouter = APIRouter(prefix="/admin", tags=["AI Call 坐席管理"])

AuthenticatedUser = Annotated[AuthSchema, Depends(get_current_user)]


async def get_agent_console_service(
    db: Annotated[AsyncSession, Depends(db_getter)],
) -> AiCallAgentConsoleService:
    return AiCallAgentConsoleService(db)


@AgentConsoleRouter.get("/bootstrap", summary="获取坐席工作台启动状态")
async def bootstrap_controller(
    auth: AuthenticatedUser,
    service: Annotated[AiCallAgentConsoleService, Depends(get_agent_console_service)],
):
    profile = await service.require_current_agent(auth)
    return SuccessResponse(data={"profile": await service.profile_payload(profile)})


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
