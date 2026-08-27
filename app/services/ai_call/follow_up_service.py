from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, NoReturn
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import status
from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import set_committed_value

from app.api.v1.ai_call.agent_console_schema import (
    AfterCallWorkIn,
    AgentPresenceSessionIn,
    FollowUpAttemptIn,
    FollowUpCallIn,
    FollowUpCloseIn,
    FollowUpDataCallIn,
    FollowUpHandlingResultIn,
)
from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import (
    AiCallAfterCallWorkModel,
    AiCallAgentSceneScopeModel,
    AiCallFollowUpAttemptModel,
    AiCallFollowUpCallRequestModel,
    AiCallFollowUpClassificationHistoryModel,
    AiCallFollowUpDataModel,
    AiCallFollowUpHandlingResultModel,
    AiCallFollowUpTaskModel,
    AiCallHandoffAgentModel,
    AiCallHandoffModel,
    AiCallRecordModel,
)
from app.api.v1.ai_call.outbound.call_window import task_allows_call_at
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from app.api.v1.ai_call.outbound.sip_line_service import SipLineService
from app.api.v1.system.auth.schema import AuthSchema
from app.common.constant import RET
from app.config.setting import settings
from app.core.exceptions import CustomException
from app.services.ai_call.agent_console_service import AiCallAgentConsoleService
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.livekit_sip import HumanOnlySipSessionFactory
from app.utils.id_util import generate_snowflake_id

if TYPE_CHECKING:
    from app.services.ai_call.recording_service import AiCallRecordingService

CLAIMABLE_UNASSIGNED_SOURCE_TYPES = {
    "handoff_unanswered",
    "ai_post_call",
    "manual_schedule",
}
CONNECTED_SIP_STATUSES = {"active", "answered", "connected"}


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
        recording_service: AiCallRecordingService | None = None,
        sip_line_service: SipLineService | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.db = db
        self.agent_service = AiCallAgentConsoleService(db)
        self.record_repository = AiCallRecordRepository(db)
        self.callback_factory = callback_factory
        self.recording_service = recording_service
        self.sip_line_service = sip_line_service
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    @staticmethod
    def _callback_duration_ms(
        record: AiCallRecordModel, ended_at: datetime
    ) -> int | None:
        if record.duration_ms is not None or record.started_at is None:
            return record.duration_ms
        started_at = record.started_at
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=timezone.utc)
        if ended_at.tzinfo is None:
            ended_at = ended_at.replace(tzinfo=timezone.utc)
        return max(0, int((ended_at - started_at).total_seconds() * 1000))

    async def submit_after_call_work(
        self,
        auth: AuthSchema,
        *,
        call_id: str,
        payload: AfterCallWorkIn,
        idempotency_key: str | None = None,
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

        normalized_key = (idempotency_key or "").strip()
        fingerprint = None
        if payload.uses_classification_contract:
            if not normalized_key:
                raise CustomException(msg="缺少 Idempotency-Key", status_code=400)
            fingerprint = self._request_fingerprint(call_id, payload)
            existing = await self._after_call_work_by_key(
                profile.tenant_id, normalized_key
            )
            if existing is not None:
                return await self._replay_after_call_work(
                    existing,
                    handoff_id=handoff.handoff_id,
                    fingerprint=fingerprint,
                )

        existing = await self._after_call_work(profile.tenant_id, handoff.handoff_id)
        if existing is not None:
            if payload.uses_classification_contract:
                return await self._replay_after_call_work(
                    existing,
                    handoff_id=handoff.handoff_id,
                    fingerprint=fingerprint,
                )
            follow_up = await self._follow_up_by_handoff(profile.tenant_id, handoff.handoff_id)
            return existing, follow_up

        if payload.uses_classification_contract:
            assert fingerprint is not None
            return await self._submit_classification_after_call_work(
                profile=profile,
                handoff=handoff,
                payload=payload,
                idempotency_key=normalized_key,
                request_fingerprint=fingerprint,
                changed_by_name=auth.user.nick_name or auth.user.user_name,
            )

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

    async def _submit_classification_after_call_work(
        self,
        *,
        profile,
        handoff: AiCallHandoffModel,
        payload: AfterCallWorkIn,
        idempotency_key: str,
        request_fingerprint: str,
        changed_by_name: str | None,
    ) -> tuple[AiCallAfterCallWorkModel, AiCallFollowUpTaskModel | None]:
        now = datetime.now(timezone.utc)
        if (
            payload.uses_classification_contract
            and payload.next_follow_up_at is not None
            and payload.next_follow_up_at <= now
        ):
            raise CustomException(msg="计划跟进时间必须晚于当前时间", status_code=422)
        assert payload.classification is not None
        assert payload.conclusion is not None
        assert payload.expected_version is not None

        try:
            async with self.db.begin_nested():
                data, record = await self._apply_human_classification(
                    tenant_id=profile.tenant_id,
                    context_call_id=handoff.call_id,
                    history_call_id=handoff.call_id,
                    follow_up_task=None,
                    classification=payload.classification,
                    low_value_reason=payload.low_value_reason,
                    conclusion=payload.conclusion,
                    expected_version=payload.expected_version,
                    source="handoff_after_call",
                    changed_by=profile.agent_identity,
                    changed_by_name=changed_by_name,
                    idempotency_key=idempotency_key,
                    request_fingerprint=request_fingerprint,
                    now=now,
                )
                follow_up = await self._active_follow_up_for_data(
                    profile.tenant_id, data.id
                )
                if payload.classification == "low_value":
                    if follow_up is not None:
                        self._finish_task_for_classification(
                            follow_up,
                            classification="low_value",
                            low_value_reason=payload.low_value_reason,
                            conclusion=payload.conclusion,
                            now=now,
                        )
                elif payload.schedule_follow_up:
                    assert payload.next_follow_up_at is not None
                    if follow_up is None:
                        follow_up = AiCallFollowUpTaskModel(
                            id=generate_snowflake_id(),
                            tenant_id=profile.tenant_id,
                            follow_up_data_id=data.id,
                            source_type="after_call_work",
                            source_key=f"handoff:{handoff.handoff_id}",
                            source_call_id=handoff.call_id,
                            source_handoff_id=handoff.handoff_id,
                            scene_code=handoff.scene_code,
                            business_type=record.business_type,
                            business_id=record.business_id,
                            contact_ref=f"call:{handoff.call_id}",
                            masked_contact=record.callee_phone_number_masked or "未提供",
                            owner_agent_identity=profile.agent_identity,
                            status="pending",
                            follow_up_reason=payload.conclusion,
                            customer_callback_at=payload.next_follow_up_at,
                            summary=payload.conclusion,
                            closed_reason=None,
                            closed_remark=None,
                            completed_at=None,
                            closed_at=None,
                            created_at=now,
                            updated_at=now,
                        )
                        self.db.add(follow_up)
                    else:
                        follow_up.follow_up_reason = payload.conclusion
                        follow_up.customer_callback_at = payload.next_follow_up_at
                        follow_up.updated_at = now

                work = AiCallAfterCallWorkModel(
                    id=generate_snowflake_id(),
                    work_id=f"acw_{generate_snowflake_id()}",
                    tenant_id=profile.tenant_id,
                    call_id=handoff.call_id,
                    handoff_id=handoff.handoff_id,
                    agent_identity=profile.agent_identity,
                    follow_up_data_id=data.id,
                    idempotency_key=idempotency_key,
                    request_fingerprint=request_fingerprint,
                    disposition_code=None,
                    summary=payload.conclusion,
                    needs_follow_up=None,
                    classification=payload.classification,
                    low_value_reason=payload.low_value_reason,
                    next_follow_up_at=payload.next_follow_up_at,
                    result_version=data.version,
                    submitted_at=now,
                    created_at=now,
                    updated_at=now,
                )
                self.db.add(work)
                if data.blocking_human_call_id == handoff.call_id:
                    data.blocking_human_call_id = None
                await self._release_after_wrap_up(
                    tenant_id=profile.tenant_id,
                    agent_identity=profile.agent_identity,
                    handoff_id=handoff.handoff_id,
                    now=now,
                )
                await self.db.flush()
        except IntegrityError:
            existing = await self._after_call_work_by_key(
                profile.tenant_id, idempotency_key
            ) or await self._after_call_work(profile.tenant_id, handoff.handoff_id)
            if existing is not None:
                return await self._replay_after_call_work(
                    existing,
                    handoff_id=handoff.handoff_id,
                    fingerprint=request_fingerprint,
                )
            self._raise_conflict("话后结果提交冲突", "AFTER_CALL_WORK_CONFLICT")
        return work, follow_up

    async def list_follow_ups(
        self,
        auth: AuthSchema,
        *,
        status: list[str] | None = None,
        scene_code: str | None = None,
        source_type: str | None = None,
        customer_name: str | None = None,
        created_at_begin: datetime | None = None,
        created_at_end: datetime | None = None,
    ) -> list[AiCallFollowUpTaskModel]:
        conditions = await self._follow_up_list_conditions(
            auth,
            status=status,
            scene_code=scene_code,
            source_type=source_type,
            customer_name=customer_name,
            created_at_begin=created_at_begin,
            created_at_end=created_at_end,
        )
        result = await self.db.execute(
            select(AiCallFollowUpTaskModel)
            .where(*conditions)
            .order_by(
                AiCallFollowUpTaskModel.customer_callback_at.is_(None),
                AiCallFollowUpTaskModel.customer_callback_at,
                AiCallFollowUpTaskModel.created_at,
                AiCallFollowUpTaskModel.id,
            )
        )
        tasks = list(result.scalars().all())
        await self._attach_latest_attempts(tasks)
        await self._attach_pending_handling(tasks)
        await self._attach_follow_up_labels(tasks)
        return tasks

    async def list_follow_up_page(
        self,
        auth: AuthSchema,
        *,
        page_num: int,
        page_size: int,
        ownership: str | None = None,
        status: list[str] | None = None,
        scene_code: str | None = None,
        source_type: str | None = None,
        customer_name: str | None = None,
        created_at_begin: datetime | None = None,
        created_at_end: datetime | None = None,
    ) -> tuple[list[AiCallFollowUpTaskModel], int]:
        conditions = await self._follow_up_list_conditions(
            auth,
            ownership=ownership,
            status=status,
            scene_code=scene_code,
            source_type=source_type,
            customer_name=customer_name,
            created_at_begin=created_at_begin,
            created_at_end=created_at_end,
        )
        total = int(
            await self.db.scalar(
                select(func.count(AiCallFollowUpTaskModel.id)).where(*conditions)
            )
            or 0
        )
        result = await self.db.execute(
            select(AiCallFollowUpTaskModel)
            .where(*conditions)
            .order_by(
                AiCallFollowUpTaskModel.created_at.desc(),
                AiCallFollowUpTaskModel.id.desc(),
            )
            .offset((page_num - 1) * page_size)
            .limit(page_size)
        )
        tasks = list(result.scalars().all())
        await self._attach_latest_attempts(tasks)
        await self._attach_pending_handling(tasks)
        await self._attach_follow_up_labels(tasks)
        return tasks, total

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

    async def submit_handling_result(
        self,
        auth: AuthSchema,
        *,
        follow_up_id: int,
        payload: FollowUpHandlingResultIn,
        idempotency_key: str,
    ) -> tuple[AiCallFollowUpTaskModel, AiCallFollowUpHandlingResultModel]:
        profile = await self.agent_service.require_current_agent(auth)
        normalized_key = idempotency_key.strip()
        if not normalized_key:
            raise CustomException(msg="Idempotency-Key 不能为空", status_code=422)
        request_fingerprint = self._request_fingerprint(str(follow_up_id), payload)
        task = (
            await self.db.execute(
                select(AiCallFollowUpTaskModel)
                .where(
                    AiCallFollowUpTaskModel.tenant_id == profile.tenant_id,
                    AiCallFollowUpTaskModel.id == follow_up_id,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if task is None:
            raise CustomException(msg="跟进任务不存在", status_code=404)
        if task.owner_agent_identity != profile.agent_identity:
            raise CustomException(msg="当前坐席不是跟进任务负责人", status_code=403)
        existing = await self._handling_result_by_key(profile.tenant_id, normalized_key)
        if existing is not None:
            if (
                existing.follow_up_id != follow_up_id
                or (
                    existing.request_fingerprint is not None
                    and existing.request_fingerprint != request_fingerprint
                )
                or (
                    payload.uses_classification_contract
                    and existing.request_fingerprint is None
                )
            ):
                self._raise_conflict(
                    "幂等键已用于其他请求", "FOLLOW_UP_STATE_CONFLICT"
                )
            await self._attach_follow_up_detail(task)
            return task, existing
        if task.status not in {"pending", "processing"}:
            self._raise_conflict("终态跟进任务不能提交处理结果", "FOLLOW_UP_STATE_CONFLICT")

        attempt: AiCallFollowUpAttemptModel
        if payload.call_id:
            record = await self._callback_record(
                tenant_id=profile.tenant_id,
                follow_up_id=task.id,
                call_id=payload.call_id,
            )
            if record.status not in {"completed", "failed"}:
                self._raise_conflict("本次回拨尚未结束", "FOLLOW_UP_STATE_CONFLICT")
            attempt = (
                await self.db.execute(
                    select(AiCallFollowUpAttemptModel).where(
                        AiCallFollowUpAttemptModel.tenant_id == profile.tenant_id,
                        AiCallFollowUpAttemptModel.follow_up_id == task.id,
                        AiCallFollowUpAttemptModel.related_call_id == payload.call_id,
                    )
                )
            ).scalars().first()
            if attempt is None:
                self._raise_conflict("本次回拨结果尚未生成", "FOLLOW_UP_STATE_CONFLICT")
            if attempt.attempt_result != payload.contact_result:
                self._raise_conflict("联系结果与回拨事实不一致", "FOLLOW_UP_STATE_CONFLICT")
            if await self._handling_result_by_call(profile.tenant_id, payload.call_id):
                self._raise_conflict("本次回拨已提交处理结果", "FOLLOW_UP_STATE_CONFLICT")
            contact_channel = "manual_phone"
        else:
            contact_channel = payload.contact_channel
            if contact_channel is None:
                raise CustomException(msg="非电话回拨必须选择联系渠道", status_code=422)
            now = datetime.now(timezone.utc)
            attempt = AiCallFollowUpAttemptModel(
                id=generate_snowflake_id(),
                tenant_id=profile.tenant_id,
                follow_up_id=task.id,
                agent_identity=profile.agent_identity,
                contact_channel=contact_channel,
                attempt_result=payload.contact_result,
                related_call_id=None,
                ring_duration_seconds=None,
                error_message=(
                    payload.remark if payload.contact_result == "technical_failure" else None
                ),
                remark=payload.remark,
                contacted_at=now,
                customer_callback_at=None,
                created_at=now,
            )

        now = datetime.now(timezone.utc)
        if (
            payload.uses_classification_contract
            and payload.next_follow_up_at is not None
            and payload.next_follow_up_at <= now
        ):
            raise CustomException(msg="计划跟进时间必须晚于当前时间", status_code=422)
        savepoint = await self.db.begin_nested()
        try:
            follow_up_data = None
            result_remark = payload.remark
            next_action = payload.next_action
            closed_reason = payload.closed_reason
            if payload.uses_classification_contract:
                assert payload.expected_version is not None
                if payload.contact_result == "connected":
                    assert payload.classification is not None
                    assert payload.conclusion is not None
                    result_remark = payload.conclusion
                    follow_up_data, _ = await self._apply_human_classification(
                        tenant_id=profile.tenant_id,
                        context_call_id=payload.call_id or task.source_call_id,
                        history_call_id=payload.call_id,
                        follow_up_task=task,
                        classification=payload.classification,
                        low_value_reason=payload.low_value_reason,
                        conclusion=payload.conclusion,
                        expected_version=payload.expected_version,
                        source="manual_outbound",
                        changed_by=profile.agent_identity,
                        changed_by_name=auth.user.nick_name or auth.user.user_name,
                        idempotency_key=normalized_key,
                        request_fingerprint=request_fingerprint,
                        now=now,
                    )
                else:
                    assert payload.remark is not None
                    follow_up_data = await self._touch_follow_up_data(
                        tenant_id=profile.tenant_id,
                        context_call_id=payload.call_id or task.source_call_id,
                        follow_up_task=task,
                        conclusion=payload.remark,
                        expected_version=payload.expected_version,
                        changed_by=(
                            profile.agent_identity if payload.call_id else None
                        ),
                        now=now,
                    )
                next_action = (
                    "continue"
                    if payload.schedule_follow_up
                    else "close"
                    if payload.classification == "low_value"
                    else "complete"
                )
                closed_reason = (
                    {
                        "explicit_rejection": "customer_refused",
                        "invalid_contact": "invalid_contact",
                    }.get(payload.low_value_reason or "", "other")
                    if next_action == "close"
                    else None
                )
                if (
                    payload.call_id
                    and follow_up_data.blocking_human_call_id == payload.call_id
                ):
                    follow_up_data.blocking_human_call_id = None

            assert result_remark is not None
            assert next_action is not None
            handling_result = AiCallFollowUpHandlingResultModel(
                id=generate_snowflake_id(),
                tenant_id=profile.tenant_id,
                follow_up_id=task.id,
                follow_up_data_id=(
                    follow_up_data.id
                    if follow_up_data is not None
                    else task.follow_up_data_id
                ),
                idempotency_key=normalized_key,
                request_fingerprint=request_fingerprint,
                related_call_id=payload.call_id,
                contact_channel=contact_channel,
                contact_result=payload.contact_result,
                remark=result_remark,
                next_action=next_action,
                next_follow_up_at=payload.next_follow_up_at,
                closed_reason=closed_reason,
                classification=payload.classification,
                low_value_reason=payload.low_value_reason,
                result_version=(
                    follow_up_data.version if follow_up_data is not None else None
                ),
                agent_identity=profile.agent_identity,
                handled_at=now,
                created_at=now,
            )
            if payload.call_id is None:
                self.db.add(attempt)
            self.db.add(handling_result)
            attempt.remark = result_remark
            if next_action == "continue":
                task.status = "processing"
                task.customer_callback_at = payload.next_follow_up_at
            elif next_action == "complete":
                task.status = "completed"
                task.customer_callback_at = None
                task.completed_at = now
            elif payload.uses_classification_contract:
                self._finish_task_for_classification(
                    task,
                    classification="low_value",
                    low_value_reason=payload.low_value_reason,
                    conclusion=result_remark,
                    now=now,
                )
            else:
                task.status = "closed"
                task.customer_callback_at = None
                task.closed_reason = closed_reason
                task.closed_remark = result_remark
                task.closed_at = now
            task.updated_at = now
            await self.db.flush()
        except IntegrityError as exc:
            await savepoint.rollback()
            existing = await self._handling_result_by_key(
                profile.tenant_id, normalized_key
            )
            if (
                existing is not None
                and existing.follow_up_id == follow_up_id
                and (
                    existing.request_fingerprint is None
                    or existing.request_fingerprint == request_fingerprint
                )
            ):
                task = await self._required_task(profile.tenant_id, follow_up_id)
                await self._attach_follow_up_detail(task)
                return task, existing
            if existing is not None:
                self._raise_conflict(
                    "幂等键已用于其他跟进任务", "FOLLOW_UP_STATE_CONFLICT"
                )
            if payload.call_id and await self._handling_result_by_call(
                profile.tenant_id, payload.call_id
            ):
                self._raise_conflict("本次回拨已提交处理结果", "FOLLOW_UP_STATE_CONFLICT")
            raise CustomException(msg="处理结果提交冲突", status_code=409) from exc
        else:
            await savepoint.commit()
        await self._attach_follow_up_detail(task)
        return task, handling_result

    async def submit_follow_up_data_handling_result(
        self,
        auth: AuthSchema,
        *,
        follow_up_data_id: int,
        payload: FollowUpHandlingResultIn,
        idempotency_key: str,
    ) -> tuple[
        AiCallFollowUpDataModel,
        AiCallFollowUpTaskModel | None,
        AiCallFollowUpHandlingResultModel,
    ]:
        profile = await self.agent_service.require_current_agent(auth)
        normalized_key = idempotency_key.strip()
        if not normalized_key:
            raise CustomException(msg="Idempotency-Key 不能为空", status_code=422)
        if not payload.uses_classification_contract or payload.call_id is None:
            raise CustomException(msg="请使用统一话后结果契约", status_code=422)
        fingerprint = self._request_fingerprint(str(follow_up_data_id), payload)
        existing = await self._handling_result_by_key(profile.tenant_id, normalized_key)
        if existing is not None:
            if (
                existing.follow_up_data_id != follow_up_data_id
                or existing.request_fingerprint != fingerprint
            ):
                self._raise_conflict(
                    "幂等键已用于其他话后结果",
                    "FOLLOW_UP_STATE_CONFLICT",
                )
            data = await self._required_follow_up_data(
                profile.tenant_id, follow_up_data_id
            )
            task = await self._active_follow_up_for_data(
                profile.tenant_id, follow_up_data_id
            )
            return data, task, existing

        data = await self.db.scalar(
            select(AiCallFollowUpDataModel)
            .where(
                AiCallFollowUpDataModel.tenant_id == profile.tenant_id,
                AiCallFollowUpDataModel.id == follow_up_data_id,
            )
            .with_for_update()
        )
        if data is None:
            raise CustomException(msg="跟进数据不存在", status_code=404)
        record = await self.db.scalar(
            select(AiCallRecordModel)
            .where(
                AiCallRecordModel.tenant_id == profile.tenant_id,
                AiCallRecordModel.follow_up_data_id == follow_up_data_id,
                AiCallRecordModel.call_id == payload.call_id,
                AiCallRecordModel.follow_up_id.is_(None),
                AiCallRecordModel.operator_agent_identity == profile.agent_identity,
                AiCallRecordModel.entry_type == "sip_callback",
            )
            .with_for_update()
        )
        if record is None:
            raise CustomException(msg="跟进数据人工外呼记录不存在", status_code=404)
        if record.status not in {"completed", "failed"}:
            self._raise_conflict("本次人工外呼尚未结束", "FOLLOW_UP_STATE_CONFLICT")
        if data.blocking_human_call_id != record.call_id:
            self._raise_conflict(
                "本次人工外呼已处理或已被其他通话替代",
                "FOLLOW_UP_STATE_CONFLICT",
            )
        if await self._handling_result_by_call(profile.tenant_id, record.call_id):
            self._raise_conflict("本次人工外呼已提交话后结果", "FOLLOW_UP_STATE_CONFLICT")
        canonical_result = self._callback_result_for_record(record)
        if canonical_result != payload.contact_result:
            self._raise_conflict("联系结果与人工外呼事实不一致", "FOLLOW_UP_STATE_CONFLICT")
        now = datetime.now(timezone.utc)
        if payload.next_follow_up_at is not None and payload.next_follow_up_at <= now:
            raise CustomException(msg="计划回访时间必须晚于当前时间", status_code=422)

        try:
            async with self.db.begin_nested():
                assert payload.expected_version is not None
                if payload.contact_result == "connected":
                    assert payload.classification is not None
                    assert payload.conclusion is not None
                    data, _ = await self._apply_human_classification(
                        tenant_id=profile.tenant_id,
                        context_call_id=record.call_id,
                        history_call_id=record.call_id,
                        follow_up_task=None,
                        classification=payload.classification,
                        low_value_reason=payload.low_value_reason,
                        conclusion=payload.conclusion,
                        expected_version=payload.expected_version,
                        source="manual_outbound",
                        changed_by=profile.agent_identity,
                        changed_by_name=auth.user.nick_name or auth.user.user_name,
                        idempotency_key=normalized_key,
                        request_fingerprint=fingerprint,
                        now=now,
                    )
                    result_remark = payload.conclusion
                else:
                    assert payload.remark is not None
                    data = await self._touch_follow_up_data(
                        tenant_id=profile.tenant_id,
                        context_call_id=record.call_id,
                        follow_up_task=None,
                        conclusion=payload.remark,
                        expected_version=payload.expected_version,
                        changed_by=profile.agent_identity,
                        now=now,
                    )
                    result_remark = payload.remark

                task = await self._active_follow_up_for_data(
                    profile.tenant_id, follow_up_data_id
                )
                if payload.schedule_follow_up:
                    if data.classification not in {"interested", "nurturing"}:
                        self._raise_conflict(
                            "当前客户分类不能安排回访",
                            "FOLLOW_UP_CLASSIFICATION_CONFLICT",
                        )
                    assert payload.next_follow_up_at is not None
                    if task is None:
                        task = AiCallFollowUpTaskModel(
                            id=generate_snowflake_id(),
                            tenant_id=profile.tenant_id,
                            follow_up_data_id=data.id,
                            source_type="manual_schedule",
                            source_key=f"follow-up-data:{data.id}:call:{record.call_id}",
                            source_call_id=record.call_id,
                            source_handoff_id=None,
                            scene_code=record.scene_code or "",
                            business_type=record.business_type,
                            business_id=record.business_id,
                            contact_ref=f"call:{record.call_id}",
                            masked_contact=record.callee_phone_number_masked or "未提供",
                            owner_agent_identity=profile.agent_identity,
                            status="processing",
                            follow_up_reason=result_remark,
                            customer_callback_at=payload.next_follow_up_at,
                            summary=result_remark,
                            closed_reason=None,
                            closed_remark=None,
                            completed_at=None,
                            closed_at=None,
                            created_at=now,
                            updated_at=now,
                        )
                        self.db.add(task)
                    else:
                        if task.owner_agent_identity not in {
                            None,
                            profile.agent_identity,
                        }:
                            self._raise_conflict(
                                "当前回访任务已由其他坐席负责",
                                "FOLLOW_UP_OWNER_CONFLICT",
                            )
                        task.owner_agent_identity = profile.agent_identity
                        task.status = "processing"
                        task.follow_up_reason = result_remark
                        task.customer_callback_at = payload.next_follow_up_at
                        task.updated_at = now
                elif payload.classification in {"low_value", "converted"} and task:
                    self._finish_task_for_classification(
                        task,
                        classification=payload.classification,
                        low_value_reason=payload.low_value_reason,
                        conclusion=result_remark,
                        now=now,
                    )

                next_action = (
                    "continue"
                    if payload.schedule_follow_up
                    else "close"
                    if payload.classification == "low_value"
                    else "complete"
                )
                result = AiCallFollowUpHandlingResultModel(
                    id=generate_snowflake_id(),
                    tenant_id=profile.tenant_id,
                    follow_up_id=task.id if task is not None else None,
                    follow_up_data_id=data.id,
                    idempotency_key=normalized_key,
                    request_fingerprint=fingerprint,
                    related_call_id=record.call_id,
                    contact_channel="manual_phone",
                    contact_result=payload.contact_result,
                    remark=result_remark,
                    next_action=next_action,
                    next_follow_up_at=payload.next_follow_up_at,
                    closed_reason=(
                        {
                            "explicit_rejection": "customer_refused",
                            "invalid_contact": "invalid_contact",
                        }.get(payload.low_value_reason or "", "other")
                        if next_action == "close"
                        else None
                    ),
                    classification=payload.classification,
                    low_value_reason=payload.low_value_reason,
                    result_version=data.version,
                    agent_identity=profile.agent_identity,
                    handled_at=now,
                    created_at=now,
                )
                self.db.add(result)
                data.blocking_human_call_id = None
                await self.db.flush()
        except IntegrityError as exc:
            existing = await self._handling_result_by_key(
                profile.tenant_id, normalized_key
            )
            if (
                existing is not None
                and existing.follow_up_data_id == follow_up_data_id
                and existing.request_fingerprint == fingerprint
            ):
                data = await self._required_follow_up_data(
                    profile.tenant_id, follow_up_data_id
                )
                task = await self._active_follow_up_for_data(
                    profile.tenant_id, follow_up_data_id
                )
                return data, task, existing
            raise CustomException(msg="话后结果提交冲突", status_code=409) from exc
        return data, task, result

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
        data = None
        if task.follow_up_data_id is not None:
            data = await self.db.scalar(
                select(AiCallFollowUpDataModel)
                .where(
                    AiCallFollowUpDataModel.tenant_id == profile.tenant_id,
                    AiCallFollowUpDataModel.id == task.follow_up_data_id,
                )
                .with_for_update()
            )
            if data is None:
                self._raise_conflict(
                    "回访任务关联的跟进数据不存在",
                    "FOLLOW_UP_DATA_CONFLICT",
                )
            if data.blocking_human_call_id:
                self._raise_conflict(
                    "当前跟进数据存在进行中或待提交的人工通话",
                    "FOLLOW_UP_CALL_BLOCKED",
                )
        outbound_task = (
            await self._callback_outbound_task(
                tenant_id=profile.tenant_id,
                source_call_id=task.source_call_id,
                follow_up_data=data,
            )
            if self.sip_line_service is not None
            else None
        )
        callee_phone_number = await self._callback_callee_phone_number(
            tenant_id=profile.tenant_id,
            source_call_id=task.source_call_id,
        )
        _, presence = await self.agent_service.require_available_presence(
            auth,
            console_session_id=str(payload.console_session_id),
        )
        return await self._start_callback_session(
            profile=profile,
            presence=presence,
            console_session_id=str(payload.console_session_id),
            callee_phone_number=callee_phone_number,
            scene_code=task.scene_code,
            business_type=task.business_type,
            business_id=task.business_id,
            follow_up_task=task,
            follow_up_data=data,
            outbound_task=outbound_task,
        )

    async def start_follow_up_data_callback(
        self,
        auth: AuthSchema,
        *,
        follow_up_data_id: int,
        payload: FollowUpDataCallIn,
        idempotency_key: str,
    ) -> tuple[FollowUpCallbackAccepted, AiCallFollowUpTaskModel | None]:
        profile = await self.agent_service.require_current_agent(auth)
        normalized_key = idempotency_key.strip()
        if not normalized_key:
            raise CustomException(msg="Idempotency-Key 不能为空", status_code=422)
        fingerprint = self._request_fingerprint(str(follow_up_data_id), payload)
        existing_request = await self.db.scalar(
            select(AiCallFollowUpCallRequestModel).where(
                AiCallFollowUpCallRequestModel.tenant_id == profile.tenant_id,
                AiCallFollowUpCallRequestModel.idempotency_key == normalized_key,
            )
        )
        if existing_request is not None:
            return self._raise_replayed_call_request(existing_request, fingerprint)

        data = await self.db.scalar(
            select(AiCallFollowUpDataModel)
            .where(
                AiCallFollowUpDataModel.tenant_id == profile.tenant_id,
                AiCallFollowUpDataModel.id == follow_up_data_id,
            )
            .with_for_update()
        )
        if data is None:
            raise CustomException(msg="跟进数据不存在", status_code=404)
        existing_request = await self.db.scalar(
            select(AiCallFollowUpCallRequestModel).where(
                AiCallFollowUpCallRequestModel.tenant_id == profile.tenant_id,
                AiCallFollowUpCallRequestModel.idempotency_key == normalized_key,
            )
        )
        if existing_request is not None:
            return self._raise_replayed_call_request(existing_request, fingerprint)
        if data.blocking_human_call_id:
            self._raise_conflict(
                "当前跟进数据存在进行中或待提交的人工通话",
                "FOLLOW_UP_CALL_BLOCKED",
            )
        outbound_task = await self._callback_outbound_task(
            tenant_id=profile.tenant_id,
            source_call_id=data.source_call_id,
            follow_up_data=data,
        )

        task = await self._active_follow_up_for_data(
            profile.tenant_id, follow_up_data_id
        )
        previous_owner = task.owner_agent_identity if task is not None else None
        assignment_action = "direct"
        if task is not None:
            if previous_owner is None:
                assignment_action = "claim"
                task.owner_agent_identity = profile.agent_identity
                task.status = "processing"
            elif previous_owner == profile.agent_identity:
                assignment_action = "owned"
            else:
                if not payload.takeover:
                    raise CustomException(
                        msg="当前回访任务属于其他坐席，请确认接管后外呼",
                        status_code=409,
                        data={
                            "errorCode": "FOLLOW_UP_TAKEOVER_REQUIRED",
                            "ownerAgentIdentity": previous_owner,
                        },
                    )
                previous_presence = await self.db.scalar(
                    select(AiCallHandoffAgentModel).where(
                        AiCallHandoffAgentModel.tenant_id == profile.tenant_id,
                        AiCallHandoffAgentModel.agent_identity == previous_owner,
                    )
                )
                if previous_presence is not None and (
                    previous_presence.active_handoff_id
                    or previous_presence.active_call_id
                    or previous_presence.status
                    in {"claiming", "in_call", "reconnecting", "wrap_up_quick"}
                ):
                    self._raise_conflict(
                        "原负责坐席正在处理通话，暂不能接管",
                        "FOLLOW_UP_OWNER_BUSY",
                    )
                assignment_action = "takeover"
                task.owner_agent_identity = profile.agent_identity
                task.status = "processing"

        if task is not None:
            scene_code = task.scene_code
            source_call_id = task.source_call_id
            business_type = task.business_type
            business_id = task.business_id
        else:
            source_record = await self._record(data.source_call_id)
            scene_code = outbound_task.scene_code
            source_call_id = data.source_call_id
            business_type = (
                source_record.business_type if source_record is not None else "outbound_task"
            )
            business_id = (
                source_record.business_id
                if source_record is not None
                else str(data.task_id)
            )

        await self.agent_service.require_scene_access(auth, scene_code)
        callee_phone_number = await self._callback_callee_phone_number(
            tenant_id=profile.tenant_id,
            source_call_id=source_call_id,
        )
        _, presence = await self.agent_service.require_available_presence(
            auth,
            console_session_id=str(payload.console_session_id),
        )
        callback = await self._start_callback_session(
            profile=profile,
            presence=presence,
            console_session_id=str(payload.console_session_id),
            callee_phone_number=callee_phone_number,
            scene_code=scene_code,
            business_type=business_type,
            business_id=business_id,
            follow_up_task=task,
            follow_up_data=data,
            outbound_task=outbound_task,
            call_request={
                "idempotency_key": normalized_key,
                "request_fingerprint": fingerprint,
                "assignment_action": assignment_action,
                "previous_owner_agent_identity": previous_owner,
                "takeover_reason": payload.takeover_reason,
                "changed_by_name": auth.user.nick_name or auth.user.user_name,
            },
        )
        return callback, task

    async def _start_callback_session(
        self,
        *,
        profile,
        presence: AiCallHandoffAgentModel,
        console_session_id: str,
        callee_phone_number: str,
        scene_code: str,
        business_type: str | None,
        business_id: str | None,
        follow_up_task: AiCallFollowUpTaskModel | None,
        follow_up_data: AiCallFollowUpDataModel | None,
        outbound_task: AiCallOutboundTaskModel | None,
        call_request: dict | None = None,
    ) -> FollowUpCallbackAccepted:
        if self.callback_factory is None:
            raise CustomException(msg="人工回拨服务未配置", status_code=503)

        now = self.clock()
        sip_config = None
        if self.sip_line_service is not None:
            if outbound_task is None:
                self._raise_conflict(
                    "跟进数据缺少原外呼任务上下文",
                    "FOLLOW_UP_DATA_CONTEXT_MISSING",
                )
            self._require_callback_window(outbound_task, now)
            line = await self.sip_line_service.resolve_default(
                self.db,
                profile.tenant_id,
            )
            sip_config = self.sip_line_service.to_sip_config(line)

        call_id = f"call_{generate_snowflake_id()}"
        agent_claimed = await self.db.execute(
            update(AiCallHandoffAgentModel)
            .where(
                AiCallHandoffAgentModel.tenant_id == profile.tenant_id,
                AiCallHandoffAgentModel.agent_identity == profile.agent_identity,
                AiCallHandoffAgentModel.status == "available",
                AiCallHandoffAgentModel.active_handoff_id.is_(None),
                AiCallHandoffAgentModel.active_call_id.is_(None),
                AiCallHandoffAgentModel.console_session_id == console_session_id,
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
                follow_up_id=follow_up_task.id if follow_up_task else None,
                follow_up_data_id=follow_up_data.id if follow_up_data else None,
                operator_agent_identity=profile.agent_identity,
                business_type=business_type,
                business_id=business_id,
                scene_code=scene_code,
                entry_type="sip_callback",
                room_name=f"ai-call-{call_id}",
                participant_identity=f"sip-{call_id}",
                callee_phone_number_hash=self._phone_hash(callee_phone_number),
                callee_phone_number_masked=self._mask_phone(callee_phone_number),
                status="ringing",
                started_at=now,
            )
        )
        if follow_up_task is not None:
            follow_up_task.status = "processing"
            follow_up_task.updated_at = now
        if follow_up_data is not None:
            follow_up_data.blocking_human_call_id = call_id
            follow_up_data.updated_at = now
        if call_request is not None:
            self.db.add(
                AiCallFollowUpCallRequestModel(
                    id=generate_snowflake_id(),
                    tenant_id=profile.tenant_id,
                    follow_up_data_id=follow_up_data.id,
                    follow_up_id=follow_up_task.id if follow_up_task else None,
                    call_id=call_id,
                    new_owner_agent_identity=profile.agent_identity,
                    changed_by=profile.agent_identity,
                    created_at=now,
                    **call_request,
                )
            )
        await self.db.flush()
        # 先提交可关联的回拨事实，避免 SIP webhook 先于本事务提交而丢失。
        await self.db.commit()

        try:
            session = await self.callback_factory.create(
                call_id=call_id,
                callee_phone_number=callee_phone_number,
                config=sip_config,
            )
        except AiCallError as exc:
            await self._record_started_callback_failure(
                call_id=call_id, message=exc.msg, has_task=follow_up_task is not None
            )
            await self.db.commit()
            raise CustomException(
                msg=exc.msg,
                status_code=exc.status_code,
                data={"errorCode": exc.error_id},
            ) from exc
        except Exception as exc:
            error_message = "人工回拨服务调用失败"
            await self._record_started_callback_failure(
                call_id=call_id,
                message=error_message,
                has_task=follow_up_task is not None,
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

    @staticmethod
    def _require_callback_window(
        outbound_task: AiCallOutboundTaskModel,
        now: datetime,
    ) -> None:
        start_value = settings.AI_CALL_MANUAL_CALLBACK_WINDOW_START
        end_value = settings.AI_CALL_MANUAL_CALLBACK_WINDOW_END
        try:
            start = datetime.strptime(start_value, "%H:%M").time()
            end = datetime.strptime(end_value, "%H:%M").time()
            business_timezone = ZoneInfo(settings.AI_CALL_OUTBOUND_TIMEZONE)
            local_time = now.astimezone(business_timezone).time()
        except (ValueError, ZoneInfoNotFoundError) as exc:
            raise CustomException(
                msg="人工回拨时段配置错误",
                status_code=503,
                data={"errorCode": "CALLBACK_WINDOW_INVALID"},
            ) from exc
        if start >= end:
            raise CustomException(
                msg="人工回拨时段配置错误",
                status_code=503,
                data={"errorCode": "CALLBACK_WINDOW_INVALID"},
            )
        if not task_allows_call_at(outbound_task, now, business_timezone):
            AiCallFollowUpService._raise_conflict(
                f"当前时间不在原任务「{outbound_task.rule_name}」允许的外呼时段内",
                "CALLBACK_OUTSIDE_TASK_WINDOW",
            )
        if not start <= local_time < end:
            AiCallFollowUpService._raise_conflict(
                f"人工回拨仅允许在 {start_value}–{end_value} 发起",
                "CALLBACK_OUTSIDE_CALL_WINDOW",
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
        return await self._end_callback_record(
            profile=profile,
            record=record,
            console_session_id=str(payload.console_session_id),
            follow_up_task=task,
        )

    async def confirm_callback_connected(
        self,
        auth: AuthSchema,
        *,
        follow_up_id: int,
        call_id: str,
        payload: AgentPresenceSessionIn,
    ) -> AiCallFollowUpAttemptModel:
        profile, task = await self._required_owned_task(auth, follow_up_id)
        if self.callback_factory is None:
            raise CustomException(msg="人工回拨服务未配置", status_code=503)
        record = await self._callback_record(
            tenant_id=profile.tenant_id,
            follow_up_id=task.id,
            call_id=call_id,
        )
        await self._confirm_callback_connected_record(
            profile=profile,
            record=record,
            console_session_id=str(payload.console_session_id),
        )
        attempt = await self._attempt_by_call_id(call_id)
        assert attempt is not None
        return attempt

    async def confirm_follow_up_data_callback_connected(
        self,
        auth: AuthSchema,
        *,
        follow_up_data_id: int,
        call_id: str,
        payload: AgentPresenceSessionIn,
    ) -> AiCallRecordModel:
        profile = await self.agent_service.require_current_agent(auth)
        record = await self.db.scalar(
            select(AiCallRecordModel).where(
                AiCallRecordModel.tenant_id == profile.tenant_id,
                AiCallRecordModel.follow_up_data_id == follow_up_data_id,
                AiCallRecordModel.call_id == call_id,
                AiCallRecordModel.operator_agent_identity == profile.agent_identity,
                AiCallRecordModel.entry_type == "sip_callback",
            )
        )
        if record is None:
            raise CustomException(msg="人工外呼记录不存在", status_code=404)
        return await self._confirm_callback_connected_record(
            profile=profile,
            record=record,
            console_session_id=str(payload.console_session_id),
        )

    async def _confirm_callback_connected_record(
        self,
        *,
        profile,
        record: AiCallRecordModel,
        console_session_id: str,
    ) -> AiCallRecordModel:
        if self.callback_factory is None:
            raise CustomException(msg="人工回拨服务未配置", status_code=503)
        if record.answered_at is not None:
            return record
        if record.status in {"completed", "failed"}:
            self._raise_conflict("当前回拨通话已经结束", "FOLLOW_UP_STATE_CONFLICT")
        await self._callback_presence(
            tenant_id=profile.tenant_id,
            agent_identity=profile.agent_identity,
            console_session_id=console_session_id,
            call_id=record.call_id,
        )
        sip_status = await self.callback_factory.get_call_status(
            call_id=record.call_id
        )
        if str(sip_status or "").strip().lower() not in CONNECTED_SIP_STATUSES:
            self._raise_conflict("客户尚未接通", "CALLBACK_NOT_CONNECTED")
        if not await self._ensure_callback_connected(record):
            self._raise_conflict("当前回拨通话已经结束", "FOLLOW_UP_STATE_CONFLICT")
        return record

    async def _ensure_callback_connected(self, record: AiCallRecordModel) -> bool:
        if record.answered_at is not None:
            return True
        now = datetime.now(timezone.utc)
        claimed = await self.db.scalar(
            update(AiCallRecordModel)
            .where(
                AiCallRecordModel.id == record.id,
                AiCallRecordModel.answered_at.is_(None),
                AiCallRecordModel.status.not_in({"completed", "failed"}),
            )
            .values(status="running", answered_at=now)
            .returning(AiCallRecordModel.id)
        )
        if claimed is None:
            await self.db.refresh(record)
            return record.answered_at is not None
        record.status = "running"
        record.answered_at = now
        if record.follow_up_id is not None:
            await self.record_callback_outcome(
                call_id=record.call_id,
                attempt_result="connected",
            )
        else:
            await self._record_follow_up_data_callback_outcome(
                call_id=record.call_id,
                attempt_result="connected",
            )
        await self._start_callback_recording(record)
        return True

    async def end_follow_up_data_callback(
        self,
        auth: AuthSchema,
        *,
        follow_up_data_id: int,
        call_id: str,
        payload: AgentPresenceSessionIn,
    ) -> AiCallRecordModel:
        profile = await self.agent_service.require_current_agent(auth)
        record = await self.db.scalar(
            select(AiCallRecordModel).where(
                AiCallRecordModel.tenant_id == profile.tenant_id,
                AiCallRecordModel.follow_up_data_id == follow_up_data_id,
                AiCallRecordModel.call_id == call_id,
                AiCallRecordModel.operator_agent_identity == profile.agent_identity,
                AiCallRecordModel.entry_type == "sip_callback",
            )
        )
        if record is None:
            raise CustomException(msg="人工外呼记录不存在", status_code=404)
        task = (
            await self._required_task(profile.tenant_id, record.follow_up_id)
            if record.follow_up_id is not None
            else None
        )
        if task is not None and task.owner_agent_identity != profile.agent_identity:
            raise CustomException(msg="当前坐席不是回访任务负责人", status_code=403)
        return await self._end_callback_record(
            profile=profile,
            record=record,
            console_session_id=str(payload.console_session_id),
            follow_up_task=task,
        )

    async def _end_callback_record(
        self,
        *,
        profile,
        record: AiCallRecordModel,
        console_session_id: str,
        follow_up_task: AiCallFollowUpTaskModel | None,
    ) -> AiCallRecordModel:
        if self.callback_factory is None:
            raise CustomException(msg="人工回拨服务未配置", status_code=503)
        if record.status in {"completed", "failed"}:
            return record
        await self._callback_presence(
            tenant_id=profile.tenant_id,
            agent_identity=profile.agent_identity,
            console_session_id=console_session_id,
            call_id=record.call_id,
        )
        if record.status not in {"ringing", "running"}:
            self._raise_conflict("当前回拨通话已经结束", "FOLLOW_UP_STATE_CONFLICT")

        await self._stop_callback_recording(record)
        await self.callback_factory.end(call_id=record.call_id)
        now = datetime.now(timezone.utc)
        record.status = "completed"
        record.ended_at = now
        record.duration_ms = self._callback_duration_ms(record, now)
        record.end_reason = "callback_ended_by_agent"
        if follow_up_task is not None:
            follow_up_task.status = "processing"
            follow_up_task.updated_at = now
        await self._settle_callback_presence(
            tenant_id=profile.tenant_id,
            agent_identity=profile.agent_identity,
            call_id=record.call_id,
            connected=False,
            now=now,
        )
        await self.db.flush()
        return record

    async def _follow_up_list_conditions(
        self,
        auth: AuthSchema,
        *,
        ownership: str | None = None,
        status: list[str] | None = None,
        scene_code: str | None = None,
        source_type: str | None = None,
        customer_name: str | None = None,
        created_at_begin: datetime | None = None,
        created_at_end: datetime | None = None,
    ) -> list:
        profile = await self.agent_service.require_current_agent(auth)
        scene_codes = await self._scene_codes(profile.tenant_id, profile.agent_identity)
        conditions = [AiCallFollowUpTaskModel.tenant_id == profile.tenant_id]
        if ownership == "mine":
            conditions.append(
                AiCallFollowUpTaskModel.owner_agent_identity == profile.agent_identity
            )
        elif ownership == "unassigned":
            conditions.extend(
                [
                    AiCallFollowUpTaskModel.owner_agent_identity.is_(None),
                    AiCallFollowUpTaskModel.source_type.in_(
                        CLAIMABLE_UNASSIGNED_SOURCE_TYPES
                    ),
                    AiCallFollowUpTaskModel.status == "pending",
                    AiCallFollowUpTaskModel.scene_code.in_(scene_codes),
                ]
            )
        else:
            conditions.append(
                or_(
                    AiCallFollowUpTaskModel.owner_agent_identity
                    == profile.agent_identity,
                    and_(
                        AiCallFollowUpTaskModel.owner_agent_identity.is_(None),
                        AiCallFollowUpTaskModel.source_type.in_(
                            CLAIMABLE_UNASSIGNED_SOURCE_TYPES
                        ),
                        AiCallFollowUpTaskModel.status == "pending",
                        AiCallFollowUpTaskModel.scene_code.in_(scene_codes),
                    ),
                )
            )
        if status:
            conditions.append(AiCallFollowUpTaskModel.status.in_(status))
        if scene_code:
            conditions.append(AiCallFollowUpTaskModel.scene_code == scene_code)
        if source_type:
            conditions.append(AiCallFollowUpTaskModel.source_type == source_type)
        if normalized_customer_name := (customer_name or "").strip():
            conditions.append(
                select(AiCallOutboundAttemptModel.id)
                .join(
                    AiCallOutboundTargetModel,
                    and_(
                        AiCallOutboundTargetModel.tenant_id
                        == AiCallOutboundAttemptModel.tenant_id,
                        AiCallOutboundTargetModel.id
                        == AiCallOutboundAttemptModel.target_id,
                    ),
                )
                .where(
                    AiCallOutboundAttemptModel.tenant_id
                    == AiCallFollowUpTaskModel.tenant_id,
                    AiCallOutboundAttemptModel.call_id
                    == AiCallFollowUpTaskModel.source_call_id,
                    AiCallOutboundTargetModel.customer_name.contains(
                        normalized_customer_name
                    ),
                )
                .exists()
            )
        if created_at_begin:
            conditions.append(AiCallFollowUpTaskModel.created_at >= created_at_begin)
        if created_at_end:
            conditions.append(AiCallFollowUpTaskModel.created_at <= created_at_end)
        return conditions

    @staticmethod
    def _raise_replayed_call_request(
        request: AiCallFollowUpCallRequestModel,
        fingerprint: str,
    ) -> NoReturn:
        if request.request_fingerprint != fingerprint:
            AiCallFollowUpService._raise_conflict(
                "幂等键已用于其他人工外呼请求",
                "FOLLOW_UP_CALL_IDEMPOTENCY_CONFLICT",
            )
        raise CustomException(
            msg="本次人工外呼已受理，请刷新通话状态",
            status_code=409,
            data={
                "errorCode": "FOLLOW_UP_CALL_ALREADY_STARTED",
                "callId": request.call_id,
            },
        )

    async def _record_started_callback_failure(
        self,
        *,
        call_id: str,
        message: str,
        has_task: bool,
    ) -> None:
        if has_task:
            await self.record_callback_outcome(
                call_id=call_id,
                attempt_result="technical_failure",
                error_message=message,
            )
            return
        await self._record_follow_up_data_callback_outcome(
            call_id=call_id,
            attempt_result="technical_failure",
            error_message=message,
        )

    async def _record_follow_up_data_callback_outcome(
        self,
        *,
        call_id: str,
        attempt_result: str,
        error_message: str | None = None,
    ) -> AiCallRecordModel:
        allowed_results = {
            "connected",
            "no_answer",
            "busy",
            "rejected",
            "invalid_contact",
            "technical_failure",
        }
        if attempt_result not in allowed_results:
            raise CustomException(msg="不支持的人工外呼结果", status_code=422)
        if attempt_result == "technical_failure" and not (error_message or "").strip():
            raise CustomException(msg="技术失败必须提供错误摘要", status_code=422)
        record = await self._record(call_id)
        if (
            record is None
            or record.follow_up_data_id is None
            or record.follow_up_id is not None
        ):
            raise CustomException(msg="跟进数据人工外呼记录不存在", status_code=404)
        now = datetime.now(timezone.utc)
        if record.answered_at is None and attempt_result != "connected":
            await self.db.execute(
                update(AiCallFollowUpDataModel)
                .where(
                    AiCallFollowUpDataModel.id == record.follow_up_data_id,
                    AiCallFollowUpDataModel.tenant_id == record.tenant_id,
                    AiCallFollowUpDataModel.blocking_human_call_id == call_id,
                )
                .values(blocking_human_call_id=None, updated_at=now)
            )
        if record.status in {"completed", "failed"}:
            return record

        if attempt_result == "connected":
            record.status = "running"
            record.answered_at = record.answered_at or now
        else:
            record.status = (
                "failed" if attempt_result == "technical_failure" else "completed"
            )
            record.ended_at = record.ended_at or now
            record.duration_ms = self._callback_duration_ms(record, record.ended_at)
            record.end_reason = f"callback_{attempt_result}"
            if attempt_result == "technical_failure":
                record.failure_stage = "sip_callback"
                record.failure_message = (error_message or "").strip()
        await self._settle_callback_presence(
            tenant_id=record.tenant_id,
            agent_identity=record.operator_agent_identity,
            call_id=call_id,
            connected=attempt_result == "connected",
            now=now,
        )
        await self.db.flush()
        from app.services.ai_call.agent_console_reconciler import (
            publish_agent_console_event,
        )

        await publish_agent_console_event(
            record.tenant_id,
            "follow_up_data.callback_outcome",
            {
                "follow_up_data_id": str(record.follow_up_data_id),
                "call_id": call_id,
                "attempt_result": attempt_result,
            },
        )
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
            ended_at = record.ended_at or now
            record.ended_at = ended_at
            record.duration_ms = self._callback_duration_ms(record, ended_at)
            record.end_reason = f"callback_{attempt_result}"
            if attempt_result == "technical_failure":
                record.failure_stage = "sip_callback"
                record.failure_message = attempt.error_message
            task.status = "processing"
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
            if self._callback_sip_status(payload or {}) not in CONNECTED_SIP_STATUSES:
                return {"handled": False, "reason": "sip_not_connected"}
            attempt_result = "connected"
        elif event_type == "participant_left":
            existing_attempt = await self._attempt_by_call_id(call_id)
            connected = (
                existing_attempt is not None
                and existing_attempt.attempt_result == "connected"
            ) or record.answered_at is not None or (
                self._callback_sip_status(payload or {}) in CONNECTED_SIP_STATUSES
            )
            if connected:
                if record.answered_at is None:
                    if record.follow_up_id is not None:
                        await self.record_callback_outcome(
                            call_id=call_id,
                            attempt_result="connected",
                        )
                    else:
                        await self._record_follow_up_data_callback_outcome(
                            call_id=call_id,
                            attempt_result="connected",
                        )
                task = (
                    await self._required_task_by_id(record.follow_up_id)
                    if record.follow_up_id is not None
                    else None
                )
                await self._stop_callback_recording(record)
                now = datetime.now(timezone.utc)
                record.status = "completed"
                ended_at = record.ended_at or now
                record.ended_at = ended_at
                record.duration_ms = self._callback_duration_ms(record, ended_at)
                record.end_reason = record.end_reason or "callback_completed"
                if task is not None:
                    task.updated_at = now
                await self._settle_callback_presence(
                    tenant_id=record.tenant_id,
                    agent_identity=record.operator_agent_identity,
                    call_id=call_id,
                    connected=False,
                    now=now,
                )
                await self.db.flush()
                return {
                    "handled": True,
                    "action": "complete_connected_callback",
                    "callId": call_id,
                    "attemptResult": "connected",
                }
            attempt_result = self._callback_attempt_result(payload or {})
            if attempt_result is None:
                attempt_result = "technical_failure"
        else:
            return {"handled": False, "reason": "unsupported_event"}

        error_message = (
            "SIP/LiveKit 人工回拨技术失败"
            if attempt_result == "technical_failure"
            else None
        )
        if attempt_result == "connected":
            connected = await self._ensure_callback_connected(record)
            if connected:
                result = "connected"
            elif record.follow_up_id is not None:
                existing_attempt = await self._attempt_by_call_id(call_id)
                result = (
                    existing_attempt.attempt_result
                    if existing_attempt is not None
                    else self._callback_result_for_record(record)
                )
            else:
                result = self._callback_result_for_record(record)
        elif record.follow_up_id is not None:
            attempt = await self.record_callback_outcome(
                call_id=call_id,
                attempt_result=attempt_result,
                ring_duration_seconds=self._callback_ring_duration(payload or {}),
                error_message=error_message,
            )
            result = attempt.attempt_result
        else:
            await self._record_follow_up_data_callback_outcome(
                call_id=call_id,
                attempt_result=attempt_result,
                error_message=error_message,
            )
            result = attempt_result
        return {
            "handled": True,
            "action": "record_callback_outcome",
            "callId": call_id,
            "attemptResult": result,
        }

    async def _start_callback_recording(self, record: AiCallRecordModel) -> None:
        if self.recording_service is None:
            return
        await self.recording_service.start_for_session(
            tenant_id=record.tenant_id,
            call_id=record.call_id,
            room_name=record.room_name,
            customer_participant_identity=record.participant_identity,
            ai_participant_identity=None,
        )
        await self.recording_service.start_session_participant_recordings(
            tenant_id=record.tenant_id,
            call_id=record.call_id,
            room_name=record.room_name,
            customer_participant_identity=record.participant_identity,
            ai_participant_identity=None,
        )
        await self.recording_service.start_human_agent_recording(
            tenant_id=record.tenant_id,
            call_id=record.call_id,
            room_name=record.room_name,
            handoff_id=None,
            participant_identity=f"human-callback-{record.call_id}",
        )

    async def _stop_callback_recording(self, record: AiCallRecordModel) -> None:
        if self.recording_service is None:
            return
        await self.recording_service.stop_for_session(
            tenant_id=record.tenant_id,
            call_id=record.call_id,
        )

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
            "follow_up_data_id": (
                str(work.follow_up_data_id) if work.follow_up_data_id else None
            ),
            "disposition_code": work.disposition_code,
            "summary": work.summary,
            "needs_follow_up": work.needs_follow_up,
            "classification": work.classification,
            "low_value_reason": work.low_value_reason,
            "next_follow_up_at": cls._api_datetime(work.next_follow_up_at),
            "result_version": work.result_version,
            "submitted_at": cls._api_datetime(work.submitted_at),
        }

    @classmethod
    def follow_up_payload(cls, task: AiCallFollowUpTaskModel) -> dict:
        latest_attempt = getattr(task, "_latest_attempt", None)
        payload = {
            "id": str(task.id),
            "follow_up_data_id": (
                str(task.follow_up_data_id) if task.follow_up_data_id else None
            ),
            "classification": getattr(
                getattr(task, "_follow_up_data", None), "classification", None
            ),
            "low_value_reason": getattr(
                getattr(task, "_follow_up_data", None), "low_value_reason", None
            ),
            "follow_up_data_version": getattr(
                getattr(task, "_follow_up_data", None), "version", None
            ),
            "source_type": task.source_type,
            "source_call_id": task.source_call_id,
            "source_handoff_id": task.source_handoff_id,
            "scene_code": task.scene_code,
            "business_type": task.business_type,
            "business_id": task.business_id,
            "customer_name": getattr(task, "_customer_name", None),
            "task_name": getattr(task, "_task_name", None),
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
            "awaiting_handling_result": bool(
                getattr(task, "_pending_handling_call_id", None)
            ),
            "pending_handling_call_id": getattr(
                task, "_pending_handling_call_id", None
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
            payload["handling_results"] = [
                cls.handling_result_payload(result)
                for result in getattr(task, "_handling_results", [])
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

    @classmethod
    def handling_result_payload(
        cls, result: AiCallFollowUpHandlingResultModel
    ) -> dict:
        return {
            "id": str(result.id),
            "follow_up_id": (
                str(result.follow_up_id) if result.follow_up_id is not None else None
            ),
            "follow_up_data_id": (
                str(result.follow_up_data_id)
                if result.follow_up_data_id is not None
                else None
            ),
            "related_call_id": result.related_call_id,
            "contact_channel": result.contact_channel,
            "contact_result": result.contact_result,
            "remark": result.remark,
            "next_action": result.next_action,
            "next_follow_up_at": cls._api_datetime(result.next_follow_up_at),
            "closed_reason": result.closed_reason,
            "classification": result.classification,
            "low_value_reason": result.low_value_reason,
            "result_version": result.result_version,
            "agent_identity": result.agent_identity,
            "handled_at": cls._api_datetime(result.handled_at),
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

    async def _after_call_work_by_key(
        self,
        tenant_id: str,
        idempotency_key: str,
    ) -> AiCallAfterCallWorkModel | None:
        return await self.db.scalar(
            select(AiCallAfterCallWorkModel).where(
                AiCallAfterCallWorkModel.tenant_id == tenant_id,
                AiCallAfterCallWorkModel.idempotency_key == idempotency_key,
            )
        )

    async def _replay_after_call_work(
        self,
        work: AiCallAfterCallWorkModel,
        *,
        handoff_id: str,
        fingerprint: str | None,
    ) -> tuple[AiCallAfterCallWorkModel, AiCallFollowUpTaskModel | None]:
        if (
            work.handoff_id != handoff_id
            or work.request_fingerprint is None
            or work.request_fingerprint != fingerprint
        ):
            self._raise_conflict(
                "话后结果已提交或幂等键已用于其他请求",
                "AFTER_CALL_WORK_CONFLICT",
            )
        follow_up = await self._follow_up_by_handoff(work.tenant_id, handoff_id)
        if follow_up is None and work.follow_up_data_id is not None:
            follow_up = await self._latest_follow_up_for_data(
                work.tenant_id, work.follow_up_data_id
            )
        return work, follow_up

    async def _resolve_follow_up_data_context(
        self,
        *,
        tenant_id: str,
        context_call_id: str,
        follow_up_task: AiCallFollowUpTaskModel | None,
    ) -> tuple[
        AiCallFollowUpDataModel | None,
        AiCallRecordModel,
        AiCallOutboundAttemptModel | None,
    ]:
        record = await self.db.scalar(
            select(AiCallRecordModel)
            .where(AiCallRecordModel.call_id == context_call_id)
            .with_for_update()
        )
        if record is None:
            raise CustomException(msg="通话记录不存在", status_code=404)
        if record.tenant_id not in {None, tenant_id}:
            raise CustomException(msg="无权访问该通话记录", status_code=403)
        if record.tenant_id is None:
            record.tenant_id = tenant_id

        data_ids = {
            value
            for value in (
                record.follow_up_data_id,
                follow_up_task.follow_up_data_id if follow_up_task else None,
            )
            if value is not None
        }
        if len(data_ids) > 1:
            self._raise_conflict(
                "通话与回访任务关联了不同跟进数据",
                "FOLLOW_UP_DATA_CONFLICT",
            )
        if data_ids:
            data = await self.db.scalar(
                select(AiCallFollowUpDataModel)
                .where(
                    AiCallFollowUpDataModel.tenant_id == tenant_id,
                    AiCallFollowUpDataModel.id == data_ids.pop(),
                )
                .with_for_update()
            )
            if data is None:
                self._raise_conflict(
                    "关联的跟进数据不存在",
                    "FOLLOW_UP_DATA_CONFLICT",
                )
            return data, record, None

        source_call_id = (
            follow_up_task.source_call_id if follow_up_task else context_call_id
        )
        attempt = await self.db.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.tenant_id == tenant_id,
                AiCallOutboundAttemptModel.call_id == source_call_id,
            )
        )
        if attempt is None:
            self._raise_conflict(
                "通话缺少外呼任务与客户关联，无法生成跟进数据",
                "FOLLOW_UP_DATA_CONTEXT_MISSING",
            )
        data = await self.db.scalar(
            select(AiCallFollowUpDataModel)
            .where(
                AiCallFollowUpDataModel.tenant_id == tenant_id,
                AiCallFollowUpDataModel.task_id == attempt.task_id,
                AiCallFollowUpDataModel.target_id == attempt.target_id,
            )
            .with_for_update()
        )
        return data, record, attempt

    async def _apply_human_classification(
        self,
        *,
        tenant_id: str,
        context_call_id: str,
        history_call_id: str | None,
        follow_up_task: AiCallFollowUpTaskModel | None,
        classification: str,
        low_value_reason: str | None,
        conclusion: str,
        expected_version: int,
        source: str,
        changed_by: str,
        changed_by_name: str | None,
        idempotency_key: str,
        request_fingerprint: str,
        now: datetime,
    ) -> tuple[AiCallFollowUpDataModel, AiCallRecordModel]:
        data, record, attempt = await self._resolve_follow_up_data_context(
            tenant_id=tenant_id,
            context_call_id=context_call_id,
            follow_up_task=follow_up_task,
        )
        current_version = data.version if data is not None else 0
        if current_version != expected_version:
            raise CustomException(
                msg="跟进数据已更新，请刷新后重试",
                status_code=409,
                data={
                    "errorCode": "VERSION_CONFLICT",
                    "currentVersion": current_version,
                },
            )

        reason = self._classification_reason(classification, low_value_reason)
        from_classification = data.classification if data is not None else None
        if data is None:
            assert attempt is not None
            data = AiCallFollowUpDataModel(
                id=generate_snowflake_id(),
                tenant_id=tenant_id,
                task_id=attempt.task_id,
                target_id=attempt.target_id,
                source_call_id=attempt.call_id,
                classification=classification,
                classification_reason=reason,
                classification_source="human",
                classification_confidence=None,
                suggest_review=False,
                low_value_reason=low_value_reason,
                latest_conclusion=conclusion,
                last_contact_at=(
                    record.ended_at or record.started_at or now
                    if history_call_id is not None
                    else now
                ),
                blocking_human_call_id=None,
                version=1,
                classification_updated_at=now,
                classification_updated_by=changed_by,
                created_at=now,
                updated_at=now,
            )
            self.db.add(data)
        else:
            data.classification = classification
            data.classification_reason = reason
            data.classification_source = "human"
            data.classification_confidence = None
            data.suggest_review = False
            data.low_value_reason = low_value_reason
            data.latest_conclusion = conclusion
            data.last_contact_at = (
                record.ended_at or record.started_at or now
                if history_call_id is not None
                else now
            )
            data.classification_updated_at = now
            data.classification_updated_by = changed_by
            data.version += 1
            data.updated_at = now

        record.follow_up_data_id = data.id
        if history_call_id is not None:
            record.operator_agent_identity = changed_by
        if follow_up_task is not None:
            follow_up_task.follow_up_data_id = data.id
        self.db.add(
            AiCallFollowUpClassificationHistoryModel(
                id=generate_snowflake_id(),
                tenant_id=tenant_id,
                follow_up_data_id=data.id,
                from_classification=from_classification,
                to_classification=classification,
                change_reason=reason,
                source=source,
                call_id=history_call_id,
                semantic_analysis_id=None,
                semantic_analysis_version=None,
                ai_suggested_classification=None,
                ai_confidence=None,
                ai_reason=None,
                ai_evidence_json=None,
                ai_conflict=None,
                ai_adopted=None,
                idempotency_key=idempotency_key,
                request_fingerprint=request_fingerprint,
                result_version=data.version,
                changed_by=changed_by,
                changed_by_name=changed_by_name,
                created_at=now,
            )
        )
        return data, record

    async def _touch_follow_up_data(
        self,
        *,
        tenant_id: str,
        context_call_id: str,
        follow_up_task: AiCallFollowUpTaskModel | None,
        conclusion: str,
        expected_version: int,
        changed_by: str | None,
        now: datetime,
    ) -> AiCallFollowUpDataModel:
        data, record, _ = await self._resolve_follow_up_data_context(
            tenant_id=tenant_id,
            context_call_id=context_call_id,
            follow_up_task=follow_up_task,
        )
        if data is None:
            self._raise_conflict(
                "当前回访任务尚未关联跟进数据",
                "FOLLOW_UP_DATA_CONTEXT_MISSING",
            )
        if data.version != expected_version:
            raise CustomException(
                msg="跟进数据已更新，请刷新后重试",
                status_code=409,
                data={
                    "errorCode": "VERSION_CONFLICT",
                    "currentVersion": data.version,
                },
            )
        data.latest_conclusion = conclusion
        data.last_contact_at = (
            record.ended_at or record.started_at or now
            if changed_by is not None
            else now
        )
        data.version += 1
        data.updated_at = now
        record.follow_up_data_id = data.id
        if changed_by is not None:
            record.operator_agent_identity = changed_by
        if follow_up_task is not None:
            follow_up_task.follow_up_data_id = data.id
        return data

    async def _active_follow_up_for_data(
        self,
        tenant_id: str,
        follow_up_data_id: int,
    ) -> AiCallFollowUpTaskModel | None:
        return await self.db.scalar(
            select(AiCallFollowUpTaskModel)
            .where(
                AiCallFollowUpTaskModel.tenant_id == tenant_id,
                AiCallFollowUpTaskModel.follow_up_data_id == follow_up_data_id,
                AiCallFollowUpTaskModel.status.in_({"pending", "processing"}),
            )
            .with_for_update()
        )

    async def _latest_follow_up_for_data(
        self,
        tenant_id: str,
        follow_up_data_id: int,
    ) -> AiCallFollowUpTaskModel | None:
        return await self.db.scalar(
            select(AiCallFollowUpTaskModel)
            .where(
                AiCallFollowUpTaskModel.tenant_id == tenant_id,
                AiCallFollowUpTaskModel.follow_up_data_id == follow_up_data_id,
            )
            .order_by(AiCallFollowUpTaskModel.updated_at.desc())
        )

    @staticmethod
    def _finish_task_for_classification(
        task: AiCallFollowUpTaskModel,
        *,
        classification: str,
        low_value_reason: str | None,
        conclusion: str,
        now: datetime,
    ) -> None:
        task.customer_callback_at = None
        task.updated_at = now
        if classification == "converted":
            task.status = "completed"
            task.completed_at = now
            return
        task.status = "closed"
        task.closed_reason = {
            "explicit_rejection": "customer_refused",
            "invalid_contact": "invalid_contact",
        }.get(low_value_reason or "", "other")
        task.closed_remark = conclusion
        task.closed_at = now

    @staticmethod
    def _classification_reason(
        classification: str,
        low_value_reason: str | None,
    ) -> str:
        labels = {
            "interested": "有意向",
            "nurturing": "持续跟进",
            "low_value": "低价值",
            "converted": "已转化",
        }
        low_value_labels = {
            "explicit_rejection": "明确拒绝",
            "no_current_need": "当前无需求",
            "customer_mismatch": "需求不匹配",
            "non_target_customer": "非目标客户",
            "invalid_contact": "联系方式无效",
            "other": "其他",
        }
        suffix = (
            f"（{low_value_labels.get(low_value_reason, '其他')}）"
            if classification == "low_value"
            else ""
        )
        return f"坐席话后确认：{labels[classification]}{suffix}"

    @staticmethod
    def _request_fingerprint(scope_id: str, payload) -> str:
        body = json.dumps(
            {
                "scopeId": scope_id,
                "payload": payload.model_dump(mode="json"),
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(body.encode("utf-8")).hexdigest()

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

    async def _callback_outbound_task(
        self,
        *,
        tenant_id: str,
        source_call_id: str,
        follow_up_data: AiCallFollowUpDataModel | None,
    ) -> AiCallOutboundTaskModel:
        task_id = follow_up_data.task_id if follow_up_data is not None else None
        if task_id is None:
            task_id = await self.db.scalar(
                select(AiCallOutboundAttemptModel.task_id).where(
                    AiCallOutboundAttemptModel.tenant_id == tenant_id,
                    AiCallOutboundAttemptModel.call_id == source_call_id,
                )
            )
        outbound_task = (
            await self.db.scalar(
                select(AiCallOutboundTaskModel).where(
                    AiCallOutboundTaskModel.tenant_id == tenant_id,
                    AiCallOutboundTaskModel.id == task_id,
                )
            )
            if task_id is not None
            else None
        )
        if outbound_task is None:
            self._raise_conflict(
                "跟进数据缺少原外呼任务上下文",
                "FOLLOW_UP_DATA_CONTEXT_MISSING",
            )
        return outbound_task

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

    async def _handling_result_by_key(
        self, tenant_id: str, idempotency_key: str
    ) -> AiCallFollowUpHandlingResultModel | None:
        return (
            await self.db.execute(
                select(AiCallFollowUpHandlingResultModel).where(
                    AiCallFollowUpHandlingResultModel.tenant_id == tenant_id,
                    AiCallFollowUpHandlingResultModel.idempotency_key
                    == idempotency_key,
                )
            )
        ).scalar_one_or_none()

    async def _handling_result_by_call(
        self, tenant_id: str, call_id: str
    ) -> AiCallFollowUpHandlingResultModel | None:
        return (
            await self.db.execute(
                select(AiCallFollowUpHandlingResultModel).where(
                    AiCallFollowUpHandlingResultModel.tenant_id == tenant_id,
                    AiCallFollowUpHandlingResultModel.related_call_id == call_id,
                )
            )
        ).scalar_one_or_none()

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

    async def _attach_pending_handling(
        self, tasks: list[AiCallFollowUpTaskModel]
    ) -> None:
        task_ids = [task.id for task in tasks]
        if not task_ids:
            return
        terminal_call_ids = set(
            (
                await self.db.execute(
                    select(AiCallRecordModel.call_id).where(
                        AiCallRecordModel.tenant_id == tasks[0].tenant_id,
                        AiCallRecordModel.follow_up_id.in_(task_ids),
                        AiCallRecordModel.entry_type == "sip_callback",
                        AiCallRecordModel.status.in_({"completed", "failed"}),
                    )
                )
            ).scalars()
        )
        handled_call_ids = set(
            (
                await self.db.execute(
                    select(AiCallFollowUpHandlingResultModel.related_call_id).where(
                        AiCallFollowUpHandlingResultModel.tenant_id
                        == tasks[0].tenant_id,
                        AiCallFollowUpHandlingResultModel.follow_up_id.in_(task_ids),
                        AiCallFollowUpHandlingResultModel.related_call_id.is_not(None),
                    )
                )
            ).scalars()
        )
        callback_attempts = (
            (
                await self.db.execute(
                    select(AiCallFollowUpAttemptModel)
                    .where(
                        AiCallFollowUpAttemptModel.tenant_id == tasks[0].tenant_id,
                        AiCallFollowUpAttemptModel.follow_up_id.in_(task_ids),
                        AiCallFollowUpAttemptModel.related_call_id.is_not(None),
                        AiCallFollowUpAttemptModel.related_call_id.in_(terminal_call_ids),
                    )
                    .order_by(
                        AiCallFollowUpAttemptModel.contacted_at.desc(),
                        AiCallFollowUpAttemptModel.id.desc(),
                    )
                )
            )
            .scalars()
            .all()
        )
        pending_by_task: dict[int, str] = {}
        for attempt in callback_attempts:
            if (
                attempt.related_call_id not in handled_call_ids
                and attempt.follow_up_id not in pending_by_task
            ):
                pending_by_task[attempt.follow_up_id] = attempt.related_call_id
        for task in tasks:
            task._pending_handling_call_id = (
                None
                if task.status in {"completed", "closed"}
                else pending_by_task.get(task.id)
            )

    async def _attach_follow_up_labels(
        self, tasks: list[AiCallFollowUpTaskModel]
    ) -> None:
        call_ids = [task.source_call_id for task in tasks]
        if not call_ids:
            return
        rows = (
            await self.db.execute(
                select(
                    AiCallOutboundAttemptModel.call_id,
                    AiCallOutboundTargetModel.customer_name,
                    AiCallOutboundTaskModel.task_name,
                )
                .join(
                    AiCallOutboundTargetModel,
                    and_(
                        AiCallOutboundTargetModel.tenant_id
                        == AiCallOutboundAttemptModel.tenant_id,
                        AiCallOutboundTargetModel.id
                        == AiCallOutboundAttemptModel.target_id,
                    ),
                )
                .join(
                    AiCallOutboundTaskModel,
                    and_(
                        AiCallOutboundTaskModel.tenant_id
                        == AiCallOutboundAttemptModel.tenant_id,
                        AiCallOutboundTaskModel.id == AiCallOutboundAttemptModel.task_id,
                    ),
                )
                .where(
                    AiCallOutboundAttemptModel.tenant_id == tasks[0].tenant_id,
                    AiCallOutboundAttemptModel.call_id.in_(call_ids),
                )
            )
        ).all()
        labels = {
            row.call_id: (row.customer_name, row.task_name)
            for row in rows
        }
        for task in tasks:
            task._customer_name, task._task_name = labels.get(
                task.source_call_id, (None, None)
            )

    async def _attach_follow_up_detail(
        self,
        task: AiCallFollowUpTaskModel,
    ) -> None:
        task._follow_up_data = (
            await self.db.scalar(
                select(AiCallFollowUpDataModel).where(
                    AiCallFollowUpDataModel.tenant_id == task.tenant_id,
                    AiCallFollowUpDataModel.id == task.follow_up_data_id,
                )
            )
            if task.follow_up_data_id is not None
            else None
        )
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
        handling_results = list(
            (
                await self.db.execute(
                    select(AiCallFollowUpHandlingResultModel)
                    .where(
                        AiCallFollowUpHandlingResultModel.tenant_id == task.tenant_id,
                        AiCallFollowUpHandlingResultModel.follow_up_id == task.id,
                    )
                    .order_by(
                        AiCallFollowUpHandlingResultModel.handled_at,
                        AiCallFollowUpHandlingResultModel.id,
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
        task._handling_results = handling_results
        handled_call_ids = {
            result.related_call_id
            for result in handling_results
            if result.related_call_id
        }
        terminal_call_ids = {
            record.call_id
            for record in callback_records
            if record.status in {"completed", "failed"}
        }
        task._pending_handling_call_id = (
            None
            if task.status in {"completed", "closed"}
            else next(
                (
                    attempt.related_call_id
                    for attempt in reversed(attempts)
                    if attempt.related_call_id
                    and attempt.related_call_id in terminal_call_ids
                    and attempt.related_call_id not in handled_call_ids
                ),
                None,
            )
        )
        task._source_record = source_record
        task._callback_records = callback_records
        await self._attach_follow_up_labels([task])

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

    async def _required_follow_up_data(
        self,
        tenant_id: str,
        follow_up_data_id: int,
    ) -> AiCallFollowUpDataModel:
        data = await self.db.scalar(
            select(AiCallFollowUpDataModel).where(
                AiCallFollowUpDataModel.tenant_id == tenant_id,
                AiCallFollowUpDataModel.id == follow_up_data_id,
            )
        )
        if data is None:
            raise CustomException(msg="跟进数据不存在", status_code=404)
        return data

    @staticmethod
    def _callback_result_for_record(record: AiCallRecordModel) -> str:
        if record.answered_at is not None:
            return "connected"
        if record.end_reason and record.end_reason.startswith("callback_"):
            result = record.end_reason.removeprefix("callback_")
            if result in {
                "no_answer",
                "busy",
                "rejected",
                "invalid_contact",
                "technical_failure",
            }:
                return result
        return "technical_failure" if record.status == "failed" else "connected"

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
    def _callback_sip_status(payload: dict) -> str:
        participant = payload.get("participant")
        participant = participant if isinstance(participant, dict) else {}
        attributes = participant.get("attributes") or payload.get("attributes") or {}
        attributes = attributes if isinstance(attributes, dict) else {}
        raw_status = (
            attributes.get("sip.callStatus")
            or payload.get("sipCallStatus")
            or payload.get("sip_call_status")
        )
        return str(raw_status or "").strip().lower().replace("-", "_")

    @classmethod
    def _callback_attempt_result(cls, payload: dict) -> str | None:
        normalized = cls._callback_sip_status(payload)
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
        participant = payload.get("participant")
        participant = participant if isinstance(participant, dict) else {}
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
