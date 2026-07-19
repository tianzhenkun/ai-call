from __future__ import annotations

from datetime import datetime, timezone

from fastapi import status
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.agent_console_schema import AgentProfileCreateIn, AgentSceneScopesIn
from app.api.v1.ai_call.model import (
    AiCallAgentProfileModel,
    AiCallAgentSceneScopeModel,
    AiCallHandoffAgentModel,
)
from app.api.v1.system.auth.schema import AuthSchema
from app.common.constant import RET
from app.core.exceptions import CustomException
from app.utils.id_util import generate_snowflake_id


class AiCallAgentConsoleService:
    """坐席档案、登录身份映射和场景授权。"""

    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def require_current_agent(self, auth: AuthSchema) -> AiCallAgentProfileModel:
        user, tenant_id = self._identity(auth)
        result = await self.db.execute(
            select(AiCallAgentProfileModel).where(
                AiCallAgentProfileModel.tenant_id == tenant_id,
                AiCallAgentProfileModel.user_id == user.id,
            )
        )
        profile = result.scalar_one_or_none()
        if profile is None or not profile.enabled:
            raise CustomException(
                msg="当前账号未开通坐席功能",
                code=10403,
                status_code=status.HTTP_403_FORBIDDEN,
            )
        return profile

    async def require_scene_access(
        self, auth: AuthSchema, scene_code: str
    ) -> AiCallAgentProfileModel:
        profile = await self.require_current_agent(auth)
        result = await self.db.execute(
            select(AiCallAgentSceneScopeModel.id).where(
                AiCallAgentSceneScopeModel.tenant_id == profile.tenant_id,
                AiCallAgentSceneScopeModel.agent_identity == profile.agent_identity,
                AiCallAgentSceneScopeModel.scene_code == scene_code,
            )
        )
        if result.scalar_one_or_none() is None:
            raise CustomException(
                msg="当前坐席无权处理该业务场景",
                code=RET.ERROR.code,
                status_code=status.HTTP_403_FORBIDDEN,
                data={"errorCode": "AGENT_SCOPE_MISMATCH"},
            )
        return profile

    async def list_profiles(self, auth: AuthSchema) -> list[AiCallAgentProfileModel]:
        _, tenant_id = self._identity(auth)
        result = await self.db.execute(
            select(AiCallAgentProfileModel)
            .where(AiCallAgentProfileModel.tenant_id == tenant_id)
            .order_by(AiCallAgentProfileModel.id)
        )
        return list(result.scalars().all())

    async def create_profile(
        self, auth: AuthSchema, payload: AgentProfileCreateIn
    ) -> AiCallAgentProfileModel:
        user, tenant_id = self._identity(auth)
        if payload.enabled:
            raise CustomException(msg="启用坐席前必须先配置至少一个业务场景", status_code=409)
        now = datetime.now(timezone.utc)
        profile = AiCallAgentProfileModel(
            id=generate_snowflake_id(),
            tenant_id=tenant_id,
            agent_identity=payload.agent_identity,
            user_id=payload.user_id,
            enabled=False,
            created_by=user.id,
            created_at=now,
            updated_by=user.id,
            updated_at=now,
        )
        self.db.add(profile)
        await self.db.flush()
        return profile

    async def update_profile(
        self, auth: AuthSchema, profile_id: int, *, enabled: bool
    ) -> AiCallAgentProfileModel:
        user, tenant_id = self._identity(auth)
        profile = await self._get_profile(tenant_id, profile_id)
        if enabled and not await self._scene_codes(profile):
            raise CustomException(msg="启用坐席前必须先配置至少一个业务场景", status_code=409)
        profile.enabled = enabled
        profile.updated_by = user.id
        profile.updated_at = datetime.now(timezone.utc)
        if not enabled:
            result = await self.db.execute(
                select(AiCallHandoffAgentModel).where(
                    AiCallHandoffAgentModel.tenant_id == tenant_id,
                    AiCallHandoffAgentModel.agent_identity == profile.agent_identity,
                )
            )
            presence = result.scalar_one_or_none()
            if presence is not None and not presence.active_handoff_id:
                presence.status = "offline"
                presence.status_updated_at = profile.updated_at
        await self.db.flush()
        return profile

    async def replace_scene_scopes(
        self, auth: AuthSchema, profile_id: int, payload: AgentSceneScopesIn
    ) -> list[str]:
        user, tenant_id = self._identity(auth)
        profile = await self._get_profile(tenant_id, profile_id)
        if profile.enabled and not payload.scene_codes:
            raise CustomException(msg="启用坐席必须保留至少一个业务场景", status_code=409)
        await self.db.execute(
            delete(AiCallAgentSceneScopeModel).where(
                AiCallAgentSceneScopeModel.tenant_id == tenant_id,
                AiCallAgentSceneScopeModel.agent_identity == profile.agent_identity,
            )
        )
        now = datetime.now(timezone.utc)
        self.db.add_all(
            [
                AiCallAgentSceneScopeModel(
                    id=generate_snowflake_id(),
                    tenant_id=tenant_id,
                    agent_identity=profile.agent_identity,
                    scene_code=scene_code,
                    created_by=user.id,
                    created_at=now,
                )
                for scene_code in payload.scene_codes
            ]
        )
        await self.db.flush()
        return payload.scene_codes

    async def profile_payload(self, profile: AiCallAgentProfileModel) -> dict:
        return {
            "id": str(profile.id),
            "tenant_id": profile.tenant_id,
            "agent_identity": profile.agent_identity,
            "user_id": str(profile.user_id),
            "enabled": profile.enabled,
            "scene_codes": await self._scene_codes(profile),
        }

    async def _get_profile(self, tenant_id: str, profile_id: int) -> AiCallAgentProfileModel:
        result = await self.db.execute(
            select(AiCallAgentProfileModel).where(
                AiCallAgentProfileModel.tenant_id == tenant_id,
                AiCallAgentProfileModel.id == profile_id,
            )
        )
        profile = result.scalar_one_or_none()
        if profile is None:
            raise CustomException(msg="坐席档案不存在", status_code=404)
        return profile

    async def _scene_codes(self, profile: AiCallAgentProfileModel) -> list[str]:
        result = await self.db.execute(
            select(AiCallAgentSceneScopeModel.scene_code)
            .where(
                AiCallAgentSceneScopeModel.tenant_id == profile.tenant_id,
                AiCallAgentSceneScopeModel.agent_identity == profile.agent_identity,
            )
            .order_by(AiCallAgentSceneScopeModel.scene_code)
        )
        return list(result.scalars().all())

    @staticmethod
    def _identity(auth: AuthSchema):
        if auth.user is None or not str(auth.user.tenant_id).strip():
            raise CustomException(msg="认证已失效", code=10401, status_code=401)
        return auth.user, str(auth.user.tenant_id)
