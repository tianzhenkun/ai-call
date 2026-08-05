from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi import status
from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import set_committed_value

from app.api.v1.ai_call.agent_console_schema import (
    AfterCallWorkIn,
    AgentPresenceSessionIn,
    FollowUpAttemptIn,
    FollowUpCallIn,
    FollowUpCloseIn,
)
from app.api.v1.ai_call.crud import AiCallRecordRepository
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
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.livekit_sip import HumanOnlySipSessionFactory
from app.utils.id_util import generate_snowflake_id

CLAIMABLE_UNASSIGNED_SOURCE_TYPES = {
    "handoff_unanswered",
    "ai_post_call",
}


@dataclass(frozen=True, slots=True)
class FollowUpCallbackAccepted:
    status: str
    call_id: str
    livekit_url: str
    participant_token: str
    participant_identity: str
    expires_in_seconds: int


class AiCallFollowUpService:
    """快速话后确认和负责人固定的人工跟进。"""

    def __init__(
        self,
        db: AsyncSession,
        *,
        callback_factory: HumanOnlySipSessionFactory | None = None,
    ) -> None:
        self.db = db
        self.agent_service = AiCallAgentConsoleService(db)
        self.record_repository = AiCallRecordRepository(db)
        self.callback_factory = callback_factory

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
        is_connected_terminal = (
            handoff.status in {"completed", "failed"}
            or (handoff.status == "canceled" and handoff.connected_at is not None)
        )
        if not is_connected_terminal:
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
                source_key=f"handoff:{handoff.handoff_id}",
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
                follow_up_reason="人工通话后续跟进",
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
                        AiCallFollowUpTaskModel.source_type.in_(
                            CLAIMABLE_UNASSIGNED_SOURCE_TYPES
                        ),
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
        tasks = list(result.scalars().all())
        await self._attach_latest_attempts(tasks)
        return tasks

    async def get_follow_up(
        self,
        auth: AuthSchema,
        *,
        follow_up_id: int,
    ) -> AiCallFollowUpTaskModel:
        profile = await self.agent_service.require_current_agent(auth)
        task = await self._required_task(profile.tenant_id, follow_up_id)
        allowed = task.owner_agent_identity == profile.agent_identity or (
            task.owner_agent_identity is None
            and task.source_type in CLAIMABLE_UNASSIGNED_SOURCE_TYPES
        )
        if not allowed:
            raise CustomException(msg="当前坐席无权查看该跟进任务", status_code=403)
        if task.owner_agent_identity is None:
            await self.agent_service.require_scene_access(auth, task.scene_code)
        await self._attach_follow_up_detail(task)
        return task

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
        if (
            task.source_type not in CLAIMABLE_UNASSIGNED_SOURCE_TYPES
            or task.status != "pending"
        ):
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
            self._raise_conflict("跟进任务已被认领", "FOLLOW_UP_ALREADY_CLAIMED")
        set_committed_value(task, "owner_agent_identity", profile.agent_identity)
        set_committed_value(task, "status", "processing")
        set_committed_value(task, "updated_at", now)
        return task

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

    async def start_callback(
        self,
        auth: AuthSchema,
        *,
        follow_up_id: int,
        payload: FollowUpCallIn,
    ) -> FollowUpCallbackAccepted:
        profile, task = await self._required_owned_task(auth, follow_up_id)
        if task.status not in {"pending", "processing"}:
            self._raise_conflict("当前跟进状态不允许回拨", "FOLLOW_UP_STATE_CONFLICT")
        await self.agent_service.require_scene_access(auth, task.scene_code)
        callee_phone_number = await self._callback_callee_phone_number(
            tenant_id=profile.tenant_id,
            source_call_id=task.source_call_id,
        )
        _, presence = await self.agent_service.require_available_presence(
            auth,
            console_session_id=str(payload.console_session_id),
        )
        if self.callback_factory is None:
            raise CustomException(msg="人工回拨服务未配置", status_code=503)

        call_id = f"call_{generate_snowflake_id()}"
        now = datetime.now(timezone.utc)
        agent_claimed = await self.db.execute(
            update(AiCallHandoffAgentModel)
            .where(
                AiCallHandoffAgentModel.tenant_id == profile.tenant_id,
                AiCallHandoffAgentModel.agent_identity == profile.agent_identity,
                AiCallHandoffAgentModel.status == "available",
                AiCallHandoffAgentModel.active_handoff_id.is_(None),
                AiCallHandoffAgentModel.active_call_id.is_(None),
                AiCallHandoffAgentModel.console_session_id
                == str(payload.console_session_id),
            )
            .values(
                status="claiming",
                active_call_id=call_id,
                last_seen_at=now,
                status_updated_at=now,
            )
            .execution_options(synchronize_session=False)
        )
        if agent_claimed.rowcount != 1:
            await self.db.rollback()
            self._raise_conflict("坐席当前正在处理其他通话", "AGENT_ALREADY_IN_CALL")
        set_committed_value(presence, "status", "claiming")
        set_committed_value(presence, "active_call_id", call_id)
        set_committed_value(presence, "last_seen_at", now)
        set_committed_value(presence, "status_updated_at", now)

        self.db.add(
            AiCallRecordModel(
                id=generate_snowflake_id(),
                tenant_id=profile.tenant_id,
                call_id=call_id,
                follow_up_id=task.id,
                business_type=task.business_type,
                business_id=task.business_id,
                scene_code=task.scene_code,
                entry_type="sip_callback",
                room_name=f"ai-call-{call_id}",
                participant_identity=f"sip-{call_id}",
                callee_phone_number_hash=self._phone_hash(callee_phone_number),
                callee_phone_number_masked=self._mask_phone(callee_phone_number),
                status="ringing",
                started_at=now,
            )
        )
        task.status = "processing"
        task.updated_at = now
        await self.db.flush()
        # 先提交可关联的回拨事实，避免 SIP webhook 先于本事务提交而丢失。
        await self.db.commit()

        try:
            session = await self.callback_factory.create(
                call_id=call_id,
                callee_phone_number=callee_phone_number,
            )
        except AiCallError as exc:
            await self.record_callback_outcome(
                call_id=call_id,
                attempt_result="technical_failure",
                error_message=exc.msg,
            )
            await self.db.commit()
            raise CustomException(
                msg=exc.msg,
                status_code=exc.status_code,
                data={"errorCode": exc.error_id},
            ) from exc
        except Exception as exc:
            error_message = "人工回拨服务调用失败"
            await self.record_callback_outcome(
                call_id=call_id,
                attempt_result="technical_failure",
                error_message=error_message,
            )
            await self.db.commit()
            raise CustomException(
                msg=error_message,
                status_code=502,
                data={"errorCode": "sip_callback_failed"},
            ) from exc

        return FollowUpCallbackAccepted(
            status="accepted",
            call_id=call_id,
            livekit_url=session.livekit_url,
            participant_token=session.participant_token,
            participant_identity=session.agent_participant_identity,
            expires_in_seconds=session.expires_in_seconds,
        )

    async def end_callback(
        self,
        auth: AuthSchema,
        *,
        follow_up_id: int,
        call_id: str,
        payload: AgentPresenceSessionIn,
    ) -> AiCallRecordModel:
        profile, task = await self._required_owned_task(auth, follow_up_id)
        if self.callback_factory is None:
            raise CustomException(msg="人工回拨服务未配置", status_code=503)
        record = await self._callback_record(
            tenant_id=profile.tenant_id,
            follow_up_id=task.id,
            call_id=call_id,
        )
        await self._callback_presence(
            tenant_id=profile.tenant_id,
            agent_identity=profile.agent_identity,
            console_session_id=str(payload.console_session_id),
            call_id=call_id,
        )
        if record.status not in {"ringing", "running"}:
            self._raise_conflict("当前回拨通话已经结束", "FOLLOW_UP_STATE_CONFLICT")

        await self.callback_factory.end(call_id=call_id)
        now = datetime.now(timezone.utc)
        record.status = "completed"
        record.ended_at = now
        record.end_reason = "callback_ended_by_agent"
        attempt = await self._attempt_by_call_id(call_id)
        task.status = "processing" if attempt and attempt.attempt_result == "connected" else "pending"
        task.updated_at = now
        await self._settle_callback_presence(
            tenant_id=profile.tenant_id,
            agent_identity=profile.agent_identity,
            call_id=call_id,
            connected=False,
            now=now,
        )
        await self.db.flush()
        return record

    async def record_callback_outcome(
        self,
        *,
        call_id: str,
        attempt_result: str,
        ring_duration_seconds: int | None = None,
        error_message: str | None = None,
    ) -> AiCallFollowUpAttemptModel:
        existing = await self._attempt_by_call_id(call_id)
        if existing is not None:
            return existing
        record = await self._record(call_id)
        if record is None or record.follow_up_id is None:
            raise CustomException(msg="人工回拨记录不存在", status_code=404)
        task = await self._required_task_by_id(record.follow_up_id)
        allowed_results = {
            "connected",
            "no_answer",
            "busy",
            "rejected",
            "invalid_contact",
            "technical_failure",
        }
        if attempt_result not in allowed_results:
            raise CustomException(msg="不支持的人工回拨结果", status_code=422)
        if attempt_result == "technical_failure" and not (error_message or "").strip():
            raise CustomException(msg="技术失败必须提供错误摘要", status_code=422)

        now = datetime.now(timezone.utc)
        attempt = AiCallFollowUpAttemptModel(
            id=generate_snowflake_id(),
            tenant_id=task.tenant_id,
            follow_up_id=task.id,
            agent_identity=task.owner_agent_identity or "system",
            contact_channel="system_callback",
            attempt_result=attempt_result,
            related_call_id=call_id,
            ring_duration_seconds=ring_duration_seconds,
            error_message=(error_message or "").strip() or None,
            remark=None,
            contacted_at=now,
            customer_callback_at=None,
            created_at=now,
        )
        self.db.add(attempt)
        if attempt_result == "connected":
            record.status = "running"
            record.answered_at = record.answered_at or now
            task.status = "processing"
        else:
            record.status = "failed" if attempt_result == "technical_failure" else "completed"
            record.ended_at = record.ended_at or now
            record.end_reason = f"callback_{attempt_result}"
            if attempt_result == "technical_failure":
                record.failure_stage = "sip_callback"
                record.failure_message = attempt.error_message
            task.status = "pending"
        task.updated_at = now
        await self._settle_callback_presence(
            tenant_id=task.tenant_id,
            agent_identity=task.owner_agent_identity,
            call_id=call_id,
            connected=attempt_result == "connected",
            now=now,
        )
        await self.db.flush()
        from app.services.ai_call.agent_console_reconciler import (
            publish_agent_console_event,
        )

        await publish_agent_console_event(
            task.tenant_id,
            "follow_up.callback_outcome",
            {
                "follow_up_id": str(task.id),
                "call_id": call_id,
                "attempt_result": attempt_result,
                "task_status": task.status,
            },
        )
        return attempt

    async def handle_livekit_webhook_event(
        self,
        *,
        event_type: str,
        room_name: str | None,
        participant_identity: str | None,
        payload: dict | None = None,
    ) -> dict:
        if not participant_identity or not participant_identity.startswith("sip-"):
            return {"handled": False, "reason": "non_sip_participant"}
        call_id = participant_identity.removeprefix("sip-")
        if not call_id or (room_name and room_name != f"ai-call-{call_id}"):
            return {"handled": False, "reason": "room_mismatch"}
        record = await self._record(call_id)
        if record is None or record.entry_type != "sip_callback":
            return {"handled": False, "reason": "not_human_callback"}

        if event_type == "participant_joined":
            attempt_result = "connected"
        elif event_type == "participant_left":
            existing_attempt = await self._attempt_by_call_id(call_id)
            if existing_attempt is not None and existing_attempt.attempt_result == "connected":
                task = await self._required_task_by_id(record.follow_up_id)
                now = datetime.now(timezone.utc)
                record.status = "completed"
                record.ended_at = record.ended_at or now
                record.end_reason = record.end_reason or "callback_completed"
                task.updated_at = now
                await self._settle_callback_presence(
                    tenant_id=task.tenant_id,
                    agent_identity=task.owner_agent_identity,
                    call_id=call_id,
                    connected=False,
                    now=now,
                )
                await self.db.flush()
                return {
                    "handled": True,
                    "action": "complete_connected_callback",
                    "callId": call_id,
                    "attemptResult": existing_attempt.attempt_result,
                }
            attempt_result = self._callback_attempt_result(payload or {})
            if attempt_result is None:
                return {"handled": False, "reason": "callback_status_unknown"}
        else:
            return {"handled": False, "reason": "unsupported_event"}

        error_message = (
            "SIP/LiveKit 人工回拨技术失败"
            if attempt_result == "technical_failure"
            else None
        )
        attempt = await self.record_callback_outcome(
            call_id=call_id,
            attempt_result=attempt_result,
            ring_duration_seconds=self._callback_ring_duration(payload or {}),
            error_message=error_message,
        )
        return {
            "handled": True,
            "action": "record_callback_outcome",
            "callId": call_id,
            "attemptResult": attempt.attempt_result,
        }

    async def complete_follow_up(
        self,
        auth: AuthSchema,
        *,
        follow_up_id: int,
    ) -> AiCallFollowUpTaskModel:
        _, task = await self._required_owned_task(auth, follow_up_id)
        if task.status not in {"pending", "processing"}:
            self._raise_conflict("当前跟进状态不允许完成", "FOLLOW_UP_STATE_CONFLICT")
        connected_attempt_id = await self.db.scalar(
            select(AiCallFollowUpAttemptModel.id)
            .where(
                AiCallFollowUpAttemptModel.tenant_id == task.tenant_id,
                AiCallFollowUpAttemptModel.follow_up_id == task.id,
                AiCallFollowUpAttemptModel.attempt_result == "connected",
            )
            .limit(1)
        )
        if connected_attempt_id is None:
            self._raise_conflict(
                "请先登记已联系结果，再完成任务",
                "FOLLOW_UP_STATE_CONFLICT",
            )
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
        latest_attempt = getattr(task, "_latest_attempt", None)
        payload = {
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
            "latest_attempt": (
                cls.attempt_payload(latest_attempt) if latest_attempt is not None else None
            ),
        }
        attempts = getattr(task, "_attempts", None)
        if attempts is not None:
            payload["attempts"] = [cls.attempt_payload(attempt) for attempt in attempts]
            payload["source_record"] = cls.follow_up_record_payload(
                getattr(task, "_source_record", None)
            )
            payload["callback_records"] = [
                cls.follow_up_record_payload(record)
                for record in getattr(task, "_callback_records", [])
            ]
        return payload

    @classmethod
    def follow_up_record_payload(cls, record: AiCallRecordModel | None) -> dict | None:
        if record is None:
            return None
        return {
            "id": str(record.id),
            "call_id": record.call_id,
            "entry_type": record.entry_type,
            "status": record.status,
            "end_reason": record.end_reason,
            "started_at": cls._api_datetime(record.started_at),
            "answered_at": cls._api_datetime(record.answered_at),
            "ended_at": cls._api_datetime(record.ended_at),
            "duration_ms": record.duration_ms,
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

    @staticmethod
    def callback_payload(callback: FollowUpCallbackAccepted) -> dict:
        return {
            "status": callback.status,
            "call_id": callback.call_id,
            "livekit_url": callback.livekit_url,
            "participant_token": callback.participant_token,
            "participant_identity": callback.participant_identity,
            "expires_in_seconds": callback.expires_in_seconds,
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

    async def _callback_callee_phone_number(
        self,
        *,
        tenant_id: str,
        source_call_id: str,
    ) -> str:
        phone_number = await self.db.scalar(
            select(AiCallRecordModel.callee_phone_number).where(
                AiCallRecordModel.tenant_id == tenant_id,
                AiCallRecordModel.call_id == source_call_id,
            )
        )
        if not phone_number:
            self._raise_conflict(
                "原通话未保存可回拨号码",
                "CALLBACK_NUMBER_UNAVAILABLE",
            )
        return phone_number

    async def _callback_record(
        self,
        *,
        tenant_id: str,
        follow_up_id: int,
        call_id: str,
    ) -> AiCallRecordModel:
        result = await self.db.execute(
            select(AiCallRecordModel).where(
                AiCallRecordModel.tenant_id == tenant_id,
                AiCallRecordModel.follow_up_id == follow_up_id,
                AiCallRecordModel.call_id == call_id,
                AiCallRecordModel.entry_type == "sip_callback",
            )
        )
        record = result.scalar_one_or_none()
        if record is None:
            raise CustomException(msg="人工回拨记录不存在", status_code=404)
        return record

    async def _callback_presence(
        self,
        *,
        tenant_id: str,
        agent_identity: str,
        console_session_id: str,
        call_id: str,
    ) -> AiCallHandoffAgentModel:
        result = await self.db.execute(
            select(AiCallHandoffAgentModel).where(
                AiCallHandoffAgentModel.tenant_id == tenant_id,
                AiCallHandoffAgentModel.agent_identity == agent_identity,
            )
        )
        presence = result.scalar_one_or_none()
        if presence is None:
            self._raise_conflict("坐席未上线", "AGENT_NOT_AVAILABLE")
        if presence.console_session_id != console_session_id:
            self._raise_conflict("当前标签页不拥有坐席控制权", "CONSOLE_SESSION_CONFLICT")
        if presence.active_call_id != call_id:
            self._raise_conflict("坐席活动通话状态不一致", "AGENT_ACTIVE_CALL_EXISTS")
        return presence

    async def _attempt_by_call_id(self, call_id: str) -> AiCallFollowUpAttemptModel | None:
        result = await self.db.execute(
            select(AiCallFollowUpAttemptModel).where(
                AiCallFollowUpAttemptModel.related_call_id == call_id
            )
        )
        return result.scalars().first()

    async def _attach_latest_attempts(
        self,
        tasks: list[AiCallFollowUpTaskModel],
    ) -> None:
        task_ids = [task.id for task in tasks]
        if not task_ids:
            return
        result = await self.db.execute(
            select(AiCallFollowUpAttemptModel)
            .where(AiCallFollowUpAttemptModel.follow_up_id.in_(task_ids))
            .order_by(
                AiCallFollowUpAttemptModel.follow_up_id,
                AiCallFollowUpAttemptModel.contacted_at.desc(),
                AiCallFollowUpAttemptModel.id.desc(),
            )
        )
        latest_by_task: dict[int, AiCallFollowUpAttemptModel] = {}
        for attempt in result.scalars():
            latest_by_task.setdefault(attempt.follow_up_id, attempt)
        for task in tasks:
            task._latest_attempt = latest_by_task.get(task.id)

    async def _attach_follow_up_detail(
        self,
        task: AiCallFollowUpTaskModel,
    ) -> None:
        attempts = list(
            (
                await self.db.execute(
                    select(AiCallFollowUpAttemptModel)
                    .where(
                        AiCallFollowUpAttemptModel.tenant_id == task.tenant_id,
                        AiCallFollowUpAttemptModel.follow_up_id == task.id,
                    )
                    .order_by(
                        AiCallFollowUpAttemptModel.contacted_at,
                        AiCallFollowUpAttemptModel.id,
                    )
                )
            )
            .scalars()
            .all()
        )
        _, source_record, callback_records = (
            await self.record_repository.get_follow_up_relation(
                tenant_id=task.tenant_id,
                call_id=task.source_call_id,
                follow_up_id=task.id,
            )
        )
        task._attempts = attempts
        task._latest_attempt = attempts[-1] if attempts else None
        task._source_record = source_record
        task._callback_records = callback_records

    async def _required_task_by_id(self, follow_up_id: int) -> AiCallFollowUpTaskModel:
        result = await self.db.execute(
            select(AiCallFollowUpTaskModel).where(AiCallFollowUpTaskModel.id == follow_up_id)
        )
        task = result.scalar_one_or_none()
        if task is None:
            raise CustomException(msg="跟进任务不存在", status_code=404)
        return task

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
        presence.last_seen_at = now
        presence.status_updated_at = now

    async def _settle_callback_presence(
        self,
        *,
        tenant_id: str,
        agent_identity: str | None,
        call_id: str,
        connected: bool,
        now: datetime,
    ) -> None:
        if not agent_identity:
            return
        result = await self.db.execute(
            select(AiCallHandoffAgentModel).where(
                AiCallHandoffAgentModel.tenant_id == tenant_id,
                AiCallHandoffAgentModel.agent_identity == agent_identity,
            )
        )
        presence = result.scalar_one_or_none()
        if presence is None or presence.active_call_id != call_id:
            return
        presence.status = "in_call" if connected else "available"
        if not connected:
            presence.active_call_id = None
        presence.status_updated_at = now

    @staticmethod
    def _mask_phone(phone_number: str) -> str:
        normalized = "".join(ch for ch in phone_number if ch.isdigit())
        if len(normalized) <= 7:
            return "***"
        return f"{normalized[:3]}****{normalized[-4:]}"

    @staticmethod
    def _phone_hash(phone_number: str) -> str:
        normalized = "".join(ch for ch in phone_number if ch.isdigit())
        digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
        return f"sha256:{digest}"

    @staticmethod
    def _callback_attempt_result(payload: dict) -> str | None:
        participant = payload.get("participant")
        participant = participant if isinstance(participant, dict) else {}
        attributes = participant.get("attributes") or payload.get("attributes") or {}
        attributes = attributes if isinstance(attributes, dict) else {}
        raw_status = (
            attributes.get("sip.callStatus")
            or payload.get("sipCallStatus")
            or payload.get("sip_call_status")
        )
        normalized = str(raw_status or "").strip().lower().replace("-", "_")
        mapping = {
            "no_answer": "no_answer",
            "timeout": "no_answer",
            "ringing_timeout": "no_answer",
            "busy": "busy",
            "rejected": "rejected",
            "declined": "rejected",
            "invalid_contact": "invalid_contact",
            "invalid_number": "invalid_contact",
            "failed": "technical_failure",
            "error": "technical_failure",
            "technical_failure": "technical_failure",
        }
        mapped = mapping.get(normalized)
        if mapped is not None:
            return mapped
        disconnect_reason = (
            payload.get("disconnectReason")
            or payload.get("disconnect_reason")
            or participant.get("disconnectReason")
            or participant.get("disconnect_reason")
        )
        reason_mapping = {
            "user_unavailable": "no_answer",
            "connection_timeout": "no_answer",
            "user_rejected": "rejected",
            "sip_trunk_failure": "technical_failure",
            "join_failure": "technical_failure",
            "media_failure": "technical_failure",
            "agent_error": "technical_failure",
        }
        return reason_mapping.get(str(disconnect_reason or "").strip().lower())

    @staticmethod
    def _callback_ring_duration(payload: dict) -> int | None:
        for key in ("ringDurationSeconds", "ring_duration_seconds"):
            value = payload.get(key)
            if isinstance(value, int) and value >= 0:
                return value
        return None

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
