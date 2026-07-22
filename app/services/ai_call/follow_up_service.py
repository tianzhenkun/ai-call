from __future__ import annotations

from datetime import datetime, timezone

from fastapi import status
from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.agent_console_schema import (
    AfterCallWorkIn,
    FollowUpAttemptIn,
    FollowUpCloseIn,
)
from app.api.v1.ai_call.model import (
    AiCallAfterCallWorkModel,
    AiCallAgentSceneScopeModel,
    AiCallFollowUpAttemptModel,
    AiCallFollowUpTaskModel,
    AiCallHandoffAgentModel,
    AiCallHandoffModel,
    AiCallRecordModel,
)
from app.api.v1.system.auth.schema import AuthSchema
from app.common.constant import RET
from app.core.exceptions import CustomException
from app.services.ai_call.agent_console_service import AiCallAgentConsoleService
from app.utils.id_util import generate_snowflake_id


class AiCallFollowUpService:
    """快速话后确认和负责人固定的人工跟进。"""

    def __init__(self, db: AsyncSession) -> None:
        self.db = db
        self.agent_service = AiCallAgentConsoleService(db)

    async def submit_after_call_work(
        self,
        auth: AuthSchema,
        *,
        call_id: str,
        payload: AfterCallWorkIn,
    ) -> tuple[AiCallAfterCallWorkModel, AiCallFollowUpTaskModel | None]:
        profile = await self.agent_service.require_current_agent(auth)
        handoff = await self._owned_handoff_for_call(
            tenant_id=profile.tenant_id,
            call_id=call_id,
            agent_identity=profile.agent_identity,
        )
        if handoff.status not in {"completed", "failed"}:
            self._raise_conflict("人工通话尚未结束，不能提交话后结果", "HANDOFF_STATE_CONFLICT")

        existing = await self._after_call_work(profile.tenant_id, handoff.handoff_id)
        if existing is not None:
            follow_up = await self._follow_up_by_handoff(profile.tenant_id, handoff.handoff_id)
            return existing, follow_up

        now = datetime.now(timezone.utc)
        work = AiCallAfterCallWorkModel(
            id=generate_snowflake_id(),
            work_id=f"acw_{generate_snowflake_id()}",
            tenant_id=profile.tenant_id,
            call_id=handoff.call_id,
            handoff_id=handoff.handoff_id,
            agent_identity=profile.agent_identity,
            disposition_code=payload.disposition_code,
            summary=payload.summary,
            needs_follow_up=payload.needs_follow_up,
            submitted_at=now,
            created_at=now,
            updated_at=now,
        )
        self.db.add(work)

        follow_up = None
        if payload.needs_follow_up:
            record = await self._record(call_id)
            follow_up = AiCallFollowUpTaskModel(
                id=generate_snowflake_id(),
                tenant_id=profile.tenant_id,
                source_type="after_call_work",
                source_call_id=handoff.call_id,
                source_handoff_id=handoff.handoff_id,
                scene_code=handoff.scene_code,
                business_type=record.business_type if record is not None else None,
                business_id=record.business_id if record is not None else None,
                contact_ref=f"call:{handoff.call_id}",
                masked_contact=(
                    record.callee_phone_number_masked
                    if record is not None and record.callee_phone_number_masked
                    else "未提供"
                ),
                owner_agent_identity=profile.agent_identity,
                status="pending",
                follow_up_reason=handoff.request_message or "人工通话后续跟进",
                customer_callback_at=payload.customer_callback_at,
                summary=payload.summary,
                closed_reason=None,
                closed_remark=None,
                completed_at=None,
                closed_at=None,
                created_at=now,
                updated_at=now,
            )
            self.db.add(follow_up)

        await self._release_after_wrap_up(
            tenant_id=profile.tenant_id,
            agent_identity=profile.agent_identity,
            handoff_id=handoff.handoff_id,
            now=now,
        )
        await self.db.flush()
        return work, follow_up

    async def list_follow_ups(self, auth: AuthSchema) -> list[AiCallFollowUpTaskModel]:
        profile = await self.agent_service.require_current_agent(auth)
        scene_codes = await self._scene_codes(profile.tenant_id, profile.agent_identity)
        result = await self.db.execute(
            select(AiCallFollowUpTaskModel)
            .where(
                AiCallFollowUpTaskModel.tenant_id == profile.tenant_id,
                or_(
                    AiCallFollowUpTaskModel.owner_agent_identity == profile.agent_identity,
                    and_(
                        AiCallFollowUpTaskModel.owner_agent_identity.is_(None),
                        AiCallFollowUpTaskModel.source_type == "handoff_unanswered",
                        AiCallFollowUpTaskModel.status == "pending",
                        AiCallFollowUpTaskModel.scene_code.in_(scene_codes),
                    ),
                ),
            )
            .order_by(
                AiCallFollowUpTaskModel.customer_callback_at.is_(None),
                AiCallFollowUpTaskModel.customer_callback_at,
                AiCallFollowUpTaskModel.created_at,
            )
        )
        return list(result.scalars().all())

    async def get_follow_up(
        self,
        auth: AuthSchema,
        *,
        follow_up_id: int,
    ) -> AiCallFollowUpTaskModel:
        profile = await self.agent_service.require_current_agent(auth)
        task = await self._required_task(profile.tenant_id, follow_up_id)
        if task.owner_agent_identity == profile.agent_identity:
            return task
        if task.owner_agent_identity is None and task.source_type == "handoff_unanswered":
            await self.agent_service.require_scene_access(auth, task.scene_code)
            return task
        raise CustomException(msg="当前坐席无权查看该跟进任务", status_code=403)

    async def claim_follow_up(
        self,
        auth: AuthSchema,
        *,
        follow_up_id: int,
    ) -> AiCallFollowUpTaskModel:
        profile = await self.agent_service.require_current_agent(auth)
        task = await self._required_task(profile.tenant_id, follow_up_id)
        if task.owner_agent_identity is not None:
            self._raise_conflict("跟进任务已被认领", "FOLLOW_UP_ALREADY_CLAIMED")
        if task.source_type != "handoff_unanswered" or task.status != "pending":
            self._raise_conflict("当前跟进状态不允许认领", "FOLLOW_UP_STATE_CONFLICT")
        await self.agent_service.require_scene_access(auth, task.scene_code)
        now = datetime.now(timezone.utc)
        result = await self.db.execute(
            update(AiCallFollowUpTaskModel)
            .where(
                AiCallFollowUpTaskModel.id == follow_up_id,
                AiCallFollowUpTaskModel.tenant_id == profile.tenant_id,
                AiCallFollowUpTaskModel.status == "pending",
                AiCallFollowUpTaskModel.owner_agent_identity.is_(None),
            )
            .values(
                owner_agent_identity=profile.agent_identity,
                status="processing",
                updated_at=now,
            )
        )
        if result.rowcount != 1:
            await self.db.rollback()
            self._raise_conflict("跟进任务已被认领", "FOLLOW_UP_ALREADY_CLAIMED")
        await self.db.commit()
        return await self._required_task(profile.tenant_id, follow_up_id)

    async def append_attempt(
        self,
        auth: AuthSchema,
        *,
        follow_up_id: int,
        payload: FollowUpAttemptIn,
    ) -> AiCallFollowUpAttemptModel:
        profile, task = await self._required_owned_task(auth, follow_up_id)
        if task.status in {"completed", "closed"}:
            self._raise_conflict("终态跟进任务不能新增联系记录", "FOLLOW_UP_STATE_CONFLICT")
        now = datetime.now(timezone.utc)
        attempt = AiCallFollowUpAttemptModel(
            id=generate_snowflake_id(),
            tenant_id=profile.tenant_id,
            follow_up_id=task.id,
            agent_identity=profile.agent_identity,
            contact_channel=payload.contact_channel,
            attempt_result=payload.attempt_result,
            related_call_id=payload.related_call_id,
            ring_duration_seconds=payload.ring_duration_seconds,
            error_message=payload.error_message,
            remark=payload.remark,
            contacted_at=now,
            customer_callback_at=payload.customer_callback_at,
            created_at=now,
        )
        self.db.add(attempt)
        if payload.customer_callback_at is not None:
            task.customer_callback_at = payload.customer_callback_at
        if payload.attempt_result in {
            "no_answer",
            "busy",
            "rejected",
            "technical_failure",
        }:
            task.status = "pending"
        task.updated_at = now
        await self.db.flush()
        return attempt

    async def complete_follow_up(
        self,
        auth: AuthSchema,
        *,
        follow_up_id: int,
    ) -> AiCallFollowUpTaskModel:
        _, task = await self._required_owned_task(auth, follow_up_id)
        if task.status not in {"pending", "processing"}:
            self._raise_conflict("当前跟进状态不允许完成", "FOLLOW_UP_STATE_CONFLICT")
        now = datetime.now(timezone.utc)
        task.status = "completed"
        task.completed_at = now
        task.updated_at = now
        await self.db.flush()
        return task

    async def close_follow_up(
        self,
        auth: AuthSchema,
        *,
        follow_up_id: int,
        payload: FollowUpCloseIn,
    ) -> AiCallFollowUpTaskModel:
        _, task = await self._required_owned_task(auth, follow_up_id)
        if task.status not in {"pending", "processing"}:
            self._raise_conflict("当前跟进状态不允许关闭", "FOLLOW_UP_STATE_CONFLICT")
        now = datetime.now(timezone.utc)
        task.status = "closed"
        task.closed_reason = payload.closed_reason
        task.closed_remark = payload.closed_remark
        task.closed_at = now
        task.updated_at = now
        await self.db.flush()
        return task

    async def apply_ai_summary_draft(
        self,
        *,
        tenant_id: str,
        handoff_id: str,
        summary: str | None,
    ) -> None:
        normalized = summary.strip() if summary else ""
        if not normalized:
            return
        await self.db.execute(
            update(AiCallAfterCallWorkModel)
            .where(
                AiCallAfterCallWorkModel.tenant_id == tenant_id,
                AiCallAfterCallWorkModel.handoff_id == handoff_id,
                AiCallAfterCallWorkModel.summary.is_(None),
            )
            .values(summary=normalized, updated_at=datetime.now(timezone.utc))
        )
        await self.db.execute(
            update(AiCallFollowUpTaskModel)
            .where(
                AiCallFollowUpTaskModel.tenant_id == tenant_id,
                AiCallFollowUpTaskModel.source_handoff_id == handoff_id,
                AiCallFollowUpTaskModel.summary.is_(None),
            )
            .values(summary=normalized, updated_at=datetime.now(timezone.utc))
        )

    @classmethod
    def after_call_work_payload(cls, work: AiCallAfterCallWorkModel) -> dict:
        return {
            "id": str(work.id),
            "work_id": work.work_id,
            "call_id": work.call_id,
            "handoff_id": work.handoff_id,
            "agent_identity": work.agent_identity,
            "disposition_code": work.disposition_code,
            "summary": work.summary,
            "needs_follow_up": work.needs_follow_up,
            "submitted_at": cls._api_datetime(work.submitted_at),
        }

    @classmethod
    def follow_up_payload(cls, task: AiCallFollowUpTaskModel) -> dict:
        return {
            "id": str(task.id),
            "source_type": task.source_type,
            "source_call_id": task.source_call_id,
            "source_handoff_id": task.source_handoff_id,
            "scene_code": task.scene_code,
            "business_type": task.business_type,
            "business_id": task.business_id,
            "masked_contact": task.masked_contact,
            "owner_agent_identity": task.owner_agent_identity,
            "status": task.status,
            "follow_up_reason": task.follow_up_reason,
            "customer_callback_at": cls._api_datetime(task.customer_callback_at),
            "summary": task.summary,
            "closed_reason": task.closed_reason,
            "closed_remark": task.closed_remark,
            "completed_at": cls._api_datetime(task.completed_at),
            "closed_at": cls._api_datetime(task.closed_at),
            "created_at": cls._api_datetime(task.created_at),
            "updated_at": cls._api_datetime(task.updated_at),
        }

    @classmethod
    def attempt_payload(cls, attempt: AiCallFollowUpAttemptModel) -> dict:
        return {
            "id": str(attempt.id),
            "follow_up_id": str(attempt.follow_up_id),
            "agent_identity": attempt.agent_identity,
            "contact_channel": attempt.contact_channel,
            "attempt_result": attempt.attempt_result,
            "related_call_id": attempt.related_call_id,
            "ring_duration_seconds": attempt.ring_duration_seconds,
            "error_message": attempt.error_message,
            "remark": attempt.remark,
            "contacted_at": cls._api_datetime(attempt.contacted_at),
            "customer_callback_at": cls._api_datetime(attempt.customer_callback_at),
        }

    async def _owned_handoff_for_call(
        self,
        *,
        tenant_id: str,
        call_id: str,
        agent_identity: str,
    ) -> AiCallHandoffModel:
        result = await self.db.execute(
            select(AiCallHandoffModel)
            .where(
                AiCallHandoffModel.tenant_id == tenant_id,
                AiCallHandoffModel.call_id == call_id,
                AiCallHandoffModel.human_agent_identity == agent_identity,
            )
            .order_by(AiCallHandoffModel.requested_at.desc())
        )
        handoff = result.scalars().first()
        if handoff is None:
            raise CustomException(msg="当前坐席没有该通话的话后处理权限", status_code=403)
        return handoff

    async def _after_call_work(
        self,
        tenant_id: str,
        handoff_id: str,
    ) -> AiCallAfterCallWorkModel | None:
        result = await self.db.execute(
            select(AiCallAfterCallWorkModel).where(
                AiCallAfterCallWorkModel.tenant_id == tenant_id,
                AiCallAfterCallWorkModel.handoff_id == handoff_id,
            )
        )
        return result.scalar_one_or_none()

    async def _follow_up_by_handoff(
        self,
        tenant_id: str,
        handoff_id: str,
    ) -> AiCallFollowUpTaskModel | None:
        result = await self.db.execute(
            select(AiCallFollowUpTaskModel).where(
                AiCallFollowUpTaskModel.tenant_id == tenant_id,
                AiCallFollowUpTaskModel.source_handoff_id == handoff_id,
            )
        )
        return result.scalar_one_or_none()

    async def _record(self, call_id: str) -> AiCallRecordModel | None:
        result = await self.db.execute(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id == call_id)
        )
        return result.scalar_one_or_none()

    async def _required_task(
        self,
        tenant_id: str,
        follow_up_id: int,
    ) -> AiCallFollowUpTaskModel:
        result = await self.db.execute(
            select(AiCallFollowUpTaskModel).where(
                AiCallFollowUpTaskModel.tenant_id == tenant_id,
                AiCallFollowUpTaskModel.id == follow_up_id,
            )
        )
        task = result.scalar_one_or_none()
        if task is None:
            raise CustomException(msg="跟进任务不存在", status_code=404)
        return task

    async def _required_owned_task(self, auth: AuthSchema, follow_up_id: int):
        profile = await self.agent_service.require_current_agent(auth)
        task = await self._required_task(profile.tenant_id, follow_up_id)
        if task.owner_agent_identity != profile.agent_identity:
            raise CustomException(msg="当前坐席不是跟进任务负责人", status_code=403)
        return profile, task

    async def _scene_codes(self, tenant_id: str, agent_identity: str) -> list[str]:
        result = await self.db.execute(
            select(AiCallAgentSceneScopeModel.scene_code).where(
                AiCallAgentSceneScopeModel.tenant_id == tenant_id,
                AiCallAgentSceneScopeModel.agent_identity == agent_identity,
            )
        )
        return list(result.scalars().all())

    async def _release_after_wrap_up(
        self,
        *,
        tenant_id: str,
        agent_identity: str,
        handoff_id: str,
        now: datetime,
    ) -> None:
        result = await self.db.execute(
            select(AiCallHandoffAgentModel).where(
                AiCallHandoffAgentModel.tenant_id == tenant_id,
                AiCallHandoffAgentModel.agent_identity == agent_identity,
            )
        )
        presence = result.scalar_one_or_none()
        if presence is None or presence.active_handoff_id != handoff_id:
            return
        presence.status = "available"
        presence.active_handoff_id = None
        presence.active_call_id = None
        presence.status_updated_at = now

    @staticmethod
    def _api_datetime(value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _raise_conflict(message: str, error_code: str) -> None:
        raise CustomException(
            msg=message,
            code=RET.ERROR.code,
            status_code=status.HTTP_409_CONFLICT,
            data={"errorCode": error_code},
        )
