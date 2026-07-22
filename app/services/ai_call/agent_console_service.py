from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

from fastapi import status
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import set_committed_value

from app.api.v1.ai_call.agent_console_schema import AgentProfileCreateIn, AgentSceneScopesIn
from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import (
    AiCallAgentProfileModel,
    AiCallAgentSceneScopeModel,
    AiCallHandoffAgentModel,
    AiCallHandoffModel,
)
from app.api.v1.system.auth.schema import AuthSchema
from app.common.constant import RET
from app.config.setting import settings
from app.core.exceptions import CustomException
from app.utils.id_util import generate_snowflake_id


class AiCallAgentConsoleService:
    """坐席身份、在线状态、场景路由和原子认领。"""

    def __init__(self, db: AsyncSession) -> None:
        self.db = db
        self.repository = AiCallRecordRepository(db)

    async def online(
        self,
        auth: AuthSchema,
        *,
        console_session_id: str,
        device_preflight_passed: bool,
    ) -> AiCallHandoffAgentModel:
        if not device_preflight_passed:
            self._raise_conflict("设备预检未通过，暂时不能上线", "MEDIA_NOT_READY")
        profile = await self.require_current_agent(auth)
        session_id = self._console_session_id(console_session_id)
        presence = await self._presence(profile)
        now = datetime.now(timezone.utc)
        if presence is None:
            presence = AiCallHandoffAgentModel(
                id=generate_snowflake_id(),
                tenant_id=profile.tenant_id,
                agent_identity=profile.agent_identity,
                skill_group="default",
                status="available",
                active_handoff_id=None,
                active_call_id=None,
                console_session_id=session_id,
                last_seen_at=now,
                status_updated_at=now,
            )
            self.db.add(presence)
        else:
            if presence.active_handoff_id:
                self._raise_conflict("坐席当前正在处理通话", "AGENT_ALREADY_IN_CALL")
            presence.skill_group = "default"
            presence.status = "available"
            presence.console_session_id = session_id
            presence.last_seen_at = now
            presence.status_updated_at = now
        await self.db.flush()
        return presence

    async def heartbeat(
        self,
        auth: AuthSchema,
        *,
        console_session_id: str,
    ) -> AiCallHandoffAgentModel:
        profile = await self.require_current_agent(auth)
        presence = await self._require_console_presence(profile, console_session_id)
        if presence.status == "offline":
            self._raise_conflict("坐席已离线，请重新上线", "AGENT_NOT_AVAILABLE")
        presence.last_seen_at = datetime.now(timezone.utc)
        await self.db.flush()
        return presence

    async def pause(
        self,
        auth: AuthSchema,
        *,
        console_session_id: str,
    ) -> AiCallHandoffAgentModel:
        return await self._set_idle_presence_status(
            auth,
            console_session_id=console_session_id,
            target_status="paused",
        )

    async def offline(
        self,
        auth: AuthSchema,
        *,
        console_session_id: str,
    ) -> AiCallHandoffAgentModel:
        return await self._set_idle_presence_status(
            auth,
            console_session_id=console_session_id,
            target_status="offline",
        )

    async def list_pending_handoffs(
        self,
        auth: AuthSchema,
        *,
        console_session_id: str,
        limit: int = 50,
    ) -> list[AiCallHandoffModel]:
        profile = await self.require_current_agent(auth)
        presence = await self._require_console_presence(profile, console_session_id)
        await self._ensure_available(presence)
        return await self.repository.list_console_pending_handoffs(
            tenant_id=profile.tenant_id,
            scene_codes=await self._scene_codes(profile),
            now=datetime.now(timezone.utc),
            limit=limit,
        )

    async def claim_handoff(
        self,
        auth: AuthSchema,
        *,
        handoff_id: str,
        console_session_id: str,
    ) -> AiCallHandoffModel:
        profile = await self.require_current_agent(auth)
        session_id = self._console_session_id(console_session_id)
        handoff = await self.repository.get_console_handoff_for_claim(
            tenant_id=profile.tenant_id,
            handoff_id=handoff_id,
        )
        if handoff is None:
            raise CustomException(msg="转人工任务不存在", status_code=404)
        if handoff.status == "accepted":
            if (
                handoff.human_agent_identity == profile.agent_identity
                and handoff.accepted_console_session_id == session_id
            ):
                return handoff
            error_code = (
                "CONSOLE_SESSION_CONFLICT"
                if handoff.human_agent_identity == profile.agent_identity
                else "HANDOFF_ALREADY_CLAIMED"
            )
            self._raise_conflict("转人工任务已被认领", error_code)
        if handoff.status != "requested":
            self._raise_conflict("转人工任务已不可认领", "HANDOFF_ALREADY_CLAIMED")

        now = datetime.now(timezone.utc)
        if handoff.expires_at is not None and self._ensure_utc(handoff.expires_at) <= now:
            self._raise_conflict("转人工任务已超时", "HANDOFF_EXPIRED")
        if handoff.scene_code not in await self._scene_codes(profile):
            raise CustomException(
                msg="当前坐席无权处理该业务场景",
                code=RET.ERROR.code,
                status_code=status.HTTP_403_FORBIDDEN,
                data={"errorCode": "AGENT_SCOPE_MISMATCH"},
            )

        presence = await self.repository.get_console_agent_for_claim(
            tenant_id=profile.tenant_id,
            agent_identity=profile.agent_identity,
        )
        if presence is None:
            self._raise_conflict("坐席未上线", "AGENT_NOT_AVAILABLE")
        self._ensure_console_owner(presence, session_id)
        await self._ensure_available(presence)

        claim_expires_at = now + timedelta(
            seconds=settings.AI_CALL_AGENT_CLAIM_CONNECT_TIMEOUT_SECONDS
        )
        if handoff.expires_at is not None:
            claim_expires_at = min(claim_expires_at, self._ensure_utc(handoff.expires_at))

        handoff_claimed = await self.repository.claim_console_handoff_if_requested(
            tenant_id=profile.tenant_id,
            handoff_id=handoff.handoff_id,
            agent_identity=profile.agent_identity,
            console_session_id=session_id,
            accepted_at=now,
            claim_expires_at=claim_expires_at,
        )
        if not handoff_claimed:
            await self.db.rollback()
            self._raise_conflict("转人工任务已被认领", "HANDOFF_ALREADY_CLAIMED")

        agent_claimed = await self.repository.claim_console_agent_if_available(
            tenant_id=profile.tenant_id,
            agent_identity=profile.agent_identity,
            console_session_id=session_id,
            handoff_id=handoff.handoff_id,
            call_id=handoff.call_id,
            now=now,
        )
        if not agent_claimed:
            await self.db.rollback()
            self._raise_conflict("坐席当前正在处理其他通话", "AGENT_ALREADY_IN_CALL")

        set_committed_value(handoff, "status", "accepted")
        set_committed_value(handoff, "human_agent_identity", profile.agent_identity)
        set_committed_value(handoff, "accepted_console_session_id", session_id)
        set_committed_value(handoff, "accepted_at", now)
        set_committed_value(handoff, "claim_expires_at", claim_expires_at)
        # 认领事务先落库，控制器随后才能签发 LiveKit Token。
        await self.db.commit()
        return handoff

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

    async def bootstrap_payload(self, auth: AuthSchema) -> dict:
        profile = await self.require_current_agent(auth)
        presence = await self._presence(profile)
        return {
            "profile": await self.profile_payload(profile),
            "presence": self.presence_payload(presence, profile.agent_identity),
        }

    @classmethod
    def presence_payload(
        cls,
        presence: AiCallHandoffAgentModel | None,
        agent_identity: str,
    ) -> dict:
        if presence is None:
            return {
                "id": None,
                "agent_identity": agent_identity,
                "skill_group": "default",
                "status": "offline",
                "active_handoff_id": None,
                "active_call_id": None,
                "console_session_id": None,
                "last_seen_at": None,
                "status_updated_at": None,
            }
        return {
            "id": str(presence.id),
            "agent_identity": presence.agent_identity,
            "skill_group": presence.skill_group,
            "status": presence.status,
            "active_handoff_id": presence.active_handoff_id,
            "active_call_id": presence.active_call_id,
            "console_session_id": presence.console_session_id,
            "last_seen_at": cls._api_datetime(presence.last_seen_at),
            "status_updated_at": cls._api_datetime(presence.status_updated_at),
        }

    @classmethod
    def handoff_payload(cls, handoff: AiCallHandoffModel) -> dict:
        return {
            "id": str(handoff.id),
            "handoff_id": handoff.handoff_id,
            "call_id": handoff.call_id,
            "room_name": handoff.room_name,
            "scene_code": handoff.scene_code,
            "status": handoff.status,
            "request_source": handoff.request_source,
            "request_reason": handoff.request_reason,
            "request_message": handoff.request_message,
            "human_agent_identity": handoff.human_agent_identity,
            "accepted_console_session_id": handoff.accepted_console_session_id,
            "requested_at": cls._api_datetime(handoff.requested_at),
            "accepted_at": cls._api_datetime(handoff.accepted_at),
            "expires_at": cls._api_datetime(handoff.expires_at),
            "claim_expires_at": cls._api_datetime(handoff.claim_expires_at),
        }

    async def _set_idle_presence_status(
        self,
        auth: AuthSchema,
        *,
        console_session_id: str,
        target_status: str,
    ) -> AiCallHandoffAgentModel:
        profile = await self.require_current_agent(auth)
        presence = await self._require_console_presence(profile, console_session_id)
        if presence.active_handoff_id:
            self._raise_conflict("坐席当前正在处理通话", "AGENT_ALREADY_IN_CALL")
        now = datetime.now(timezone.utc)
        presence.status = target_status
        presence.last_seen_at = now
        presence.status_updated_at = now
        await self.db.flush()
        return presence

    async def _presence(
        self,
        profile: AiCallAgentProfileModel,
    ) -> AiCallHandoffAgentModel | None:
        result = await self.db.execute(
            select(AiCallHandoffAgentModel).where(
                AiCallHandoffAgentModel.tenant_id == profile.tenant_id,
                AiCallHandoffAgentModel.agent_identity == profile.agent_identity,
            )
        )
        return result.scalar_one_or_none()

    async def _require_console_presence(
        self,
        profile: AiCallAgentProfileModel,
        console_session_id: str,
    ) -> AiCallHandoffAgentModel:
        session_id = self._console_session_id(console_session_id)
        presence = await self._presence(profile)
        if presence is None:
            self._raise_conflict("坐席未上线", "AGENT_NOT_AVAILABLE")
        self._ensure_console_owner(presence, session_id)
        return presence

    @classmethod
    def _ensure_console_owner(
        cls,
        presence: AiCallHandoffAgentModel,
        console_session_id: str,
    ) -> None:
        if presence.console_session_id != console_session_id:
            cls._raise_conflict("当前标签页不拥有坐席控制权", "CONSOLE_SESSION_CONFLICT")

    async def _ensure_available(self, presence: AiCallHandoffAgentModel) -> None:
        if presence.active_handoff_id or presence.status in {
            "claiming",
            "in_call",
            "reconnecting",
            "wrap_up_quick",
        }:
            self._raise_conflict("坐席当前正在处理其他通话", "AGENT_ALREADY_IN_CALL")
        if presence.status != "available":
            self._raise_conflict("坐席当前不可接单", "AGENT_NOT_AVAILABLE")
        if presence.last_seen_at is None or (
            datetime.now(timezone.utc) - self._ensure_utc(presence.last_seen_at)
        ) > timedelta(seconds=30):
            now = datetime.now(timezone.utc)
            presence.status = "offline"
            presence.status_updated_at = now
            await self.db.commit()
            self._raise_conflict("坐席心跳已过期，请重新上线", "AGENT_NOT_AVAILABLE")

    @staticmethod
    def _console_session_id(value: str) -> str:
        try:
            return str(UUID(str(value)))
        except (TypeError, ValueError, AttributeError) as exc:
            raise CustomException(msg="console_session_id 必须是有效 UUID", status_code=422) from exc

    @staticmethod
    def _ensure_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @classmethod
    def _api_datetime(cls, value: datetime | None) -> datetime | None:
        return cls._ensure_utc(value) if value is not None else None

    @staticmethod
    def _raise_conflict(message: str, error_code: str) -> None:
        raise CustomException(
            msg=message,
            code=RET.ERROR.code,
            status_code=status.HTTP_409_CONFLICT,
            data={"errorCode": error_code},
        )

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
