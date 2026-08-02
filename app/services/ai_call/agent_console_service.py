from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
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
    AiCallRecordModel,
)
from app.api.v1.system.auth.schema import AuthSchema
from app.common.constant import RET
from app.config.setting import settings
from app.core.exceptions import CustomException
from app.services.ai_call.runtime_control.handoff_repository import (
    HandoffAcceptIntent,
    HandoffClaimConflictError,
    HandoffCommandDecision,
    HandoffIdempotencyConflictError,
    HandoffNotFoundError,
    HandoffRuntimeModeError,
    HandoffTerminalBarrierError,
    RuntimeHandoffRepository,
)
from app.utils.id_util import generate_snowflake_id


@dataclass(frozen=True)
class HandoffClaimResult:
    handoff: AiCallHandoffModel
    payload: dict
    command: HandoffCommandDecision | None = None


class AiCallAgentConsoleService:
    """坐席身份、在线状态、场景路由和原子认领。"""

    def __init__(
        self,
        db: AsyncSession,
        *,
        participant_verifier: Callable[[str, str], Awaitable[bool]] | None = None,
    ) -> None:
        self.db = db
        self.repository = AiCallRecordRepository(db)
        self.participant_verifier = participant_verifier

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

    async def require_available_presence(
        self,
        auth: AuthSchema,
        *,
        console_session_id: str,
    ) -> tuple[AiCallAgentProfileModel, AiCallHandoffAgentModel]:
        profile = await self.require_current_agent(auth)
        presence = await self._require_console_presence(profile, console_session_id)
        await self._ensure_available(presence)
        return profile, presence

    async def claim_handoff(
        self,
        auth: AuthSchema,
        *,
        handoff_id: str,
        console_session_id: str,
        commit: bool = True,
        idempotency_key: str | None = None,
    ) -> AiCallHandoffModel:
        handoff, _command = await self._claim_handoff(
            auth,
            handoff_id=handoff_id,
            console_session_id=console_session_id,
            commit=commit,
            idempotency_key=idempotency_key,
        )
        return handoff

    async def _claim_handoff(
        self,
        auth: AuthSchema,
        *,
        handoff_id: str,
        console_session_id: str,
        commit: bool,
        idempotency_key: str | None,
    ) -> tuple[AiCallHandoffModel, HandoffCommandDecision | None]:
        profile = await self.require_current_agent(auth)
        session_id = self._console_session_id(console_session_id)
        handoff = await self.repository.get_console_handoff_for_claim(
            tenant_id=profile.tenant_id,
            handoff_id=handoff_id,
        )
        if handoff is None:
            raise CustomException(msg="转人工任务不存在", status_code=404)
        runtime_control_mode = await self.db.scalar(
            select(AiCallRecordModel.runtime_control_mode).where(
                AiCallRecordModel.tenant_id == profile.tenant_id,
                AiCallRecordModel.call_id == handoff.call_id,
            )
        )
        if runtime_control_mode == "owner_command_v1":
            if handoff.scene_code not in await self._scene_codes(profile):
                raise CustomException(
                    msg="当前坐席无权处理该业务场景",
                    code=RET.ERROR.code,
                    status_code=status.HTTP_403_FORBIDDEN,
                    data={"errorCode": "AGENT_SCOPE_MISMATCH"},
                )
            normalized_key = str(idempotency_key or "").strip()
            if not normalized_key:
                raise CustomException(
                    msg="Owner 模式认领必须提供 Idempotency-Key",
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    data={"errorCode": "IDEMPOTENCY_KEY_REQUIRED"},
                )
            try:
                command = await RuntimeHandoffRepository(self.db).accept(
                    HandoffAcceptIntent(
                        tenant_id=profile.tenant_id,
                        handoff_id=handoff.handoff_id,
                        agent_identity=profile.agent_identity,
                        console_session_id=session_id,
                        idempotency_key=normalized_key,
                    )
                )
            except HandoffNotFoundError as exc:
                raise CustomException(msg="转人工任务不存在", status_code=404) from exc
            except HandoffIdempotencyConflictError:
                self._raise_conflict("幂等键已用于其他认领请求", "IDEMPOTENCY_CONFLICT")
            except HandoffRuntimeModeError:
                self._raise_conflict("通话控制模式已经变化", "RUNTIME_MODE_CHANGED")
            except HandoffTerminalBarrierError:
                self._raise_conflict("客户通话已经结束", "CUSTOMER_NOT_CONNECTED")
            except HandoffClaimConflictError:
                self._raise_conflict("转人工任务已不可认领", "HANDOFF_ALREADY_CLAIMED")
            if commit:
                await self.db.commit()
            return handoff, command
        if handoff.status == "accepted":
            if (
                handoff.human_agent_identity == profile.agent_identity
                and handoff.accepted_console_session_id == session_id
            ):
                return handoff, None
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
        if commit:
            await self.db.commit()
        return handoff, None

    async def claim_handoff_with_payload(
        self,
        auth: AuthSchema,
        *,
        handoff_id: str,
        console_session_id: str,
        idempotency_key: str | None = None,
    ) -> HandoffClaimResult:
        handoff, command = await self._claim_handoff(
            auth,
            handoff_id=handoff_id,
            console_session_id=console_session_id,
            commit=False,
            idempotency_key=idempotency_key,
        )
        payload = await self.handoff_payload(handoff)
        await self.db.commit()
        return HandoffClaimResult(handoff=handoff, payload=payload, command=command)

    async def media_ready(
        self,
        auth: AuthSchema,
        *,
        handoff_id: str,
        console_session_id: str,
        participant_identity: str,
    ) -> AiCallHandoffModel:
        profile = await self.require_current_agent(auth)
        handoff, presence = await self._require_owned_handoff(
            profile,
            handoff_id=handoff_id,
            console_session_id=console_session_id,
            statuses={"accepted", "reconnecting"},
        )
        if participant_identity != f"human-agent-{handoff.handoff_id}":
            self._raise_conflict("坐席 Participant 身份不匹配", "CONSOLE_SESSION_CONFLICT")
        record = await self.repository.get_record(handoff.call_id)
        if record is not None and record.status in {"completed", "failed"}:
            self._raise_conflict("客户通话已经结束", "CUSTOMER_NOT_CONNECTED")
        if self.participant_verifier is None:
            raise CustomException(msg="LiveKit Participant 核验器未配置", status_code=503)
        if not await self.participant_verifier(handoff.room_name, participant_identity):
            self._raise_conflict("坐席麦克风尚未就绪", "MEDIA_NOT_READY")
        now = datetime.now(timezone.utc)
        handoff.status = "connected"
        handoff.connected_at = handoff.connected_at or now
        handoff.reconnect_expires_at = None
        presence.status = "in_call"
        presence.last_seen_at = now
        presence.status_updated_at = now
        await self.db.flush()
        return handoff

    async def begin_reconnect(
        self,
        auth: AuthSchema,
        *,
        handoff_id: str,
        console_session_id: str,
        now: datetime | None = None,
    ) -> AiCallHandoffModel:
        profile = await self.require_current_agent(auth)
        handoff, presence = await self._require_owned_handoff(
            profile,
            handoff_id=handoff_id,
            console_session_id=console_session_id,
            statuses={"connected", "reconnecting"},
        )
        current = now or datetime.now(timezone.utc)
        if handoff.status == "connected":
            handoff.status = "reconnecting"
            handoff.reconnect_expires_at = current + timedelta(
                seconds=settings.AI_CALL_AGENT_RECONNECT_GRACE_SECONDS
            )
        presence.status = "reconnecting"
        presence.last_seen_at = current
        presence.status_updated_at = current
        await self.db.commit()
        return handoff

    async def complete_handoff(
        self,
        auth: AuthSchema,
        *,
        handoff_id: str,
        console_session_id: str,
    ) -> AiCallHandoffModel:
        profile = await self.require_current_agent(auth)
        handoff, presence = await self._require_owned_handoff(
            profile,
            handoff_id=handoff_id,
            console_session_id=console_session_id,
            statuses={"connected", "reconnecting"},
        )
        now = datetime.now(timezone.utc)
        handoff.status = "completed"
        handoff.ended_at = now
        handoff.end_reason = "agent_completed"
        handoff.claim_expires_at = None
        handoff.reconnect_expires_at = None
        presence.status = "wrap_up_quick"
        presence.last_seen_at = now
        presence.status_updated_at = now
        await self.db.flush()
        return handoff

    async def reconcile_handoff_timeout(
        self,
        tenant_id: str,
        handoff_id: str,
        *,
        now: datetime | None = None,
    ) -> AiCallHandoffModel | None:
        current = now or datetime.now(timezone.utc)
        handoff = await self.repository.get_console_handoff_for_claim(
            tenant_id=tenant_id,
            handoff_id=handoff_id,
        )
        if handoff is None:
            return None
        if handoff.status in {"completed", "expired", "canceled", "failed"}:
            return handoff
        if (
            handoff.status == "reconnecting"
            and handoff.reconnect_expires_at is not None
            and self._ensure_utc(handoff.reconnect_expires_at) <= current
        ):
            handoff.status = "failed"
            handoff.ended_at = current
            handoff.end_reason = "reconnect_timeout"
            await self._set_claimed_presence(handoff, status_value="wrap_up_quick", release=False)
            await self.db.flush()
            await self._publish_handoff_state(handoff)
            return handoff
        if (
            handoff.status in {"requested", "accepted"}
            and handoff.expires_at is not None
            and self._ensure_utc(handoff.expires_at) <= current
        ):
            handoff.status = "expired"
            handoff.ended_at = current
            handoff.end_reason = "handoff_unanswered"
            await self._set_claimed_presence(handoff, status_value="available", release=True)
            record = await self.repository.get_record(handoff.call_id)
            await self.repository.create_unanswered_follow_up_if_missing(
                {
                    "id": generate_snowflake_id(),
                    "tenant_id": handoff.tenant_id,
                    "source_type": "handoff_unanswered",
                    "source_key": f"handoff:{handoff.handoff_id}",
                    "source_call_id": handoff.call_id,
                    "source_handoff_id": handoff.handoff_id,
                    "scene_code": handoff.scene_code,
                    "business_type": record.business_type if record is not None else None,
                    "business_id": record.business_id if record is not None else None,
                    "contact_ref": f"call:{handoff.call_id}",
                    "masked_contact": (
                        record.callee_phone_number_masked
                        if record is not None and record.callee_phone_number_masked
                        else "未提供"
                    ),
                    "owner_agent_identity": None,
                    "status": "pending",
                    "follow_up_reason": "首次人工接通等待超时",
                    "customer_callback_at": None,
                    "summary": None,
                    "closed_reason": None,
                    "closed_remark": None,
                    "completed_at": None,
                    "closed_at": None,
                    "created_at": current,
                    "updated_at": current,
                }
            )
            await self.db.flush()
            await self._publish_handoff_state(handoff)
            return handoff
        if (
            handoff.status == "accepted"
            and handoff.claim_expires_at is not None
            and self._ensure_utc(handoff.claim_expires_at) <= current
        ):
            record = await self.repository.get_record(handoff.call_id)
            await self._set_claimed_presence(handoff, status_value="available", release=True)
            if record is not None and record.status in {"completed", "failed"}:
                handoff.status = "canceled"
                handoff.ended_at = current
                handoff.end_reason = "customer_disconnected"
                await self.db.flush()
                await self._publish_handoff_state(handoff)
                return handoff
            handoff.status = "requested"
            handoff.human_agent_identity = None
            handoff.accepted_console_session_id = None
            handoff.accepted_at = None
            handoff.claim_expires_at = None
            await self.db.flush()
            await self._publish_handoff_state(handoff)
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
        current_handoff = None
        if presence is not None and presence.active_handoff_id:
            handoff = await self.repository.get_console_handoff_for_claim(
                tenant_id=profile.tenant_id,
                handoff_id=presence.active_handoff_id,
            )
            if (
                handoff is not None
                and handoff.human_agent_identity == profile.agent_identity
            ):
                current_handoff = await self.handoff_payload(handoff)
        return {
            "profile": await self.profile_payload(profile),
            "presence": self.presence_payload(presence, profile.agent_identity),
            "current_handoff": current_handoff,
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

    async def handoff_payload(self, handoff: AiCallHandoffModel) -> dict:
        payloads = await self.handoff_payloads([handoff])
        return payloads[0]

    async def handoff_context_payload(
        self,
        auth: AuthSchema,
        *,
        handoff_id: str,
        console_session_id: str,
    ) -> dict:
        profile = await self.require_current_agent(auth)
        session_id = self._console_session_id(console_session_id)
        await self._require_console_presence(profile, session_id)
        handoff = await self.repository.get_console_handoff_for_claim(
            tenant_id=profile.tenant_id,
            handoff_id=handoff_id,
        )
        if handoff is None:
            raise CustomException(msg="转人工任务不存在", status_code=404)
        if handoff.scene_code not in await self._scene_codes(profile):
            raise CustomException(
                msg="当前坐席无权处理该业务场景",
                code=RET.ERROR.code,
                status_code=status.HTTP_403_FORBIDDEN,
                data={"errorCode": "AGENT_SCOPE_MISMATCH"},
            )
        if handoff.status != "requested":
            if handoff.human_agent_identity != profile.agent_identity:
                self._raise_conflict("当前坐席不是任务负责人", "HANDOFF_ALREADY_CLAIMED")
            if handoff.accepted_console_session_id != session_id:
                self._raise_conflict(
                    "当前标签页不拥有媒体控制权",
                    "CONSOLE_SESSION_CONFLICT",
                )

        payload = await self.handoff_payload(handoff)
        payload.pop("pending_items", None)
        payload.pop("recent_dialogue", None)
        dialogue_segments = await self.repository.list_handoff_context_dialogue(
            handoff.call_id
        )
        payload["dialogue"] = [
            {
                "id": str(segment.id),
                "speaker_type": segment.speaker_type,
                "text": segment.segment_text.strip(),
                "occurred_at": self._api_datetime(
                    segment.started_at or segment.ended_at
                ),
            }
            for segment in dialogue_segments
        ]
        return payload

    async def handoff_payloads(
        self,
        handoffs: list[AiCallHandoffModel],
    ) -> list[dict]:
        if not handoffs:
            return []

        call_ids = list(dict.fromkeys(handoff.call_id for handoff in handoffs))
        records = await self.repository.list_records_by_call_ids(call_ids)
        dialogue_segments = await self.repository.list_dialogue_segments_by_call_ids(call_ids)
        records_by_call_id = {record.call_id: record for record in records}

        customer_names_by_call_id: dict[str, str | None] = {}
        for tenant_id in dict.fromkeys(handoff.tenant_id for handoff in handoffs):
            tenant_call_ids = [
                handoff.call_id
                for handoff in handoffs
                if handoff.tenant_id == tenant_id
            ]
            customer_names_by_call_id.update(
                await self.repository.outbound_customer_names_by_call_ids(
                    tenant_id=tenant_id,
                    call_ids=tenant_call_ids,
                )
            )

        dialogue_by_call_id: dict[str, list[dict]] = {}
        for segment in dialogue_segments:
            text = segment.segment_text.strip()
            if (
                segment.segment_status != "final"
                or segment.speaker_type not in {"customer", "ai", "human_agent"}
                or not text
            ):
                continue
            dialogue_by_call_id.setdefault(segment.call_id, []).append(
                {
                    "id": str(segment.id),
                    "speaker_type": segment.speaker_type,
                    "text": text,
                    "occurred_at": self._api_datetime(
                        segment.started_at or segment.ended_at
                    ),
                }
            )

        payloads: list[dict] = []
        for handoff in handoffs:
            recent_dialogue = dialogue_by_call_id.get(handoff.call_id, [])[-6:]
            request_text = self._handoff_request_text(handoff)
            latest_customer_text = next(
                (
                    turn["text"]
                    for turn in reversed(recent_dialogue)
                    if turn["speaker_type"] == "customer"
                ),
                None,
            )
            handoff_summary = request_text
            pending_items = [{"text": request_text, "evidence": "转人工请求"}]
            if latest_customer_text and latest_customer_text not in request_text:
                handoff_summary = (
                    f"{request_text}；客户最近表示：“{latest_customer_text}”"
                )
                pending_items.append(
                    {
                        "text": latest_customer_text,
                        "evidence": "客户最近表达",
                    }
                )

            record = records_by_call_id.get(handoff.call_id)
            payloads.append(
                {
                    **self._base_handoff_payload(handoff),
                    "masked_customer_name": customer_names_by_call_id.get(
                        handoff.call_id
                    ),
                    "masked_contact": (
                        record.callee_phone_number_masked if record else None
                    ),
                    "business_type": record.business_type if record else None,
                    "business_id": record.business_id if record else None,
                    "handoff_summary": handoff_summary,
                    "pending_items": pending_items,
                    "recent_dialogue": recent_dialogue,
                }
            )
        return payloads

    @classmethod
    def _base_handoff_payload(cls, handoff: AiCallHandoffModel) -> dict:
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
            "connected_at": cls._api_datetime(handoff.connected_at),
            "ended_at": cls._api_datetime(handoff.ended_at),
            "expires_at": cls._api_datetime(handoff.expires_at),
            "claim_expires_at": cls._api_datetime(handoff.claim_expires_at),
            "reconnect_expires_at": cls._api_datetime(handoff.reconnect_expires_at),
            "end_reason": handoff.end_reason,
            "failure_stage": handoff.failure_stage,
            "failure_message": handoff.failure_message,
        }

    @staticmethod
    def _handoff_request_text(handoff: AiCallHandoffModel) -> str:
        if handoff.request_message and handoff.request_message.strip():
            return handoff.request_message.strip()
        if handoff.request_reason == "customer_requested_human":
            return "客户请求转人工"
        if handoff.request_reason and handoff.request_reason.strip():
            return handoff.request_reason.strip()
        return "客户请求转人工"

    async def _require_owned_handoff(
        self,
        profile: AiCallAgentProfileModel,
        *,
        handoff_id: str,
        console_session_id: str,
        statuses: set[str],
    ) -> tuple[AiCallHandoffModel, AiCallHandoffAgentModel]:
        session_id = self._console_session_id(console_session_id)
        handoff = await self.repository.get_console_handoff_for_claim(
            tenant_id=profile.tenant_id,
            handoff_id=handoff_id,
        )
        if handoff is None:
            raise CustomException(msg="转人工任务不存在", status_code=404)
        if handoff.status not in statuses:
            self._raise_conflict("当前转人工状态不允许该操作", "HANDOFF_STATE_CONFLICT")
        if handoff.human_agent_identity != profile.agent_identity:
            self._raise_conflict("当前坐席不是任务负责人", "HANDOFF_ALREADY_CLAIMED")
        if handoff.accepted_console_session_id != session_id:
            self._raise_conflict("当前标签页不拥有媒体控制权", "CONSOLE_SESSION_CONFLICT")
        presence = await self.repository.get_console_agent_for_claim(
            tenant_id=profile.tenant_id,
            agent_identity=profile.agent_identity,
        )
        if presence is None or presence.active_handoff_id != handoff.handoff_id:
            self._raise_conflict("坐席活动通话状态不一致", "AGENT_ACTIVE_CALL_EXISTS")
        self._ensure_console_owner(presence, session_id)
        return handoff, presence

    async def _set_claimed_presence(
        self,
        handoff: AiCallHandoffModel,
        *,
        status_value: str,
        release: bool,
    ) -> None:
        if not handoff.human_agent_identity:
            return
        presence = await self.repository.get_console_agent_for_claim(
            tenant_id=handoff.tenant_id,
            agent_identity=handoff.human_agent_identity,
        )
        if presence is None or presence.active_handoff_id != handoff.handoff_id:
            return
        presence.status = status_value
        presence.status_updated_at = datetime.now(timezone.utc)
        if release:
            presence.active_handoff_id = None
            presence.active_call_id = None

    @staticmethod
    async def _publish_handoff_state(handoff: AiCallHandoffModel) -> None:
        from app.services.ai_call.agent_console_reconciler import (
            publish_agent_console_event,
        )

        await publish_agent_console_event(
            handoff.tenant_id,
            "handoff.changed",
            {
                "handoff_id": handoff.handoff_id,
                "call_id": handoff.call_id,
                "status": handoff.status,
            },
        )

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
