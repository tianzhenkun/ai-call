from __future__ import annotations

import json
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

from sqlalchemy import and_, func, or_, select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.follow_up_data_schema import (
    FollowUpDataClassificationIn,
    FollowUpDataScheduleIn,
)
from app.api.v1.ai_call.model import (
    AiCallAfterCallWorkModel,
    AiCallFollowUpClassificationHistoryModel,
    AiCallFollowUpDataModel,
    AiCallFollowUpHandlingResultModel,
    AiCallFollowUpScheduleRequestModel,
    AiCallFollowUpTaskModel,
    AiCallHandoffModel,
    AiCallRecordModel,
    AiCallSemanticAnalysisModel,
)
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from app.core.exceptions import CustomException
from app.services.ai_call.classification_review import requires_classification_review
from app.utils.id_util import generate_snowflake_id

AI_CLASSIFICATIONS = {"interested", "nurturing", "low_value"}
PROTECTED_CLASSIFICATION_SOURCES = {"human", "system"}


class AiCallFollowUpDataService:
    """把正式外呼的 AI 分类写入独立跟进数据，不创建回访任务。"""

    def __init__(self, repository: AiCallRecordRepository) -> None:
        self.repository = repository
        self.db = repository.db

    @classmethod
    def from_session(cls, db: AsyncSession) -> AiCallFollowUpDataService:
        return cls(AiCallRecordRepository(db))

    async def list_page(
        self,
        *,
        tenant_id: str,
        classification: str | None,
        page_num: int,
        page_size: int,
        customer_name: str | None = None,
        task_id: int | None = None,
        last_contact_at_begin: datetime | None = None,
        last_contact_at_end: datetime | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        active_task = aliased(AiCallFollowUpTaskModel)
        source_record = aliased(AiCallRecordModel)
        joins = (
            (AiCallOutboundTargetModel, and_(
                AiCallOutboundTargetModel.tenant_id
                == AiCallFollowUpDataModel.tenant_id,
                AiCallOutboundTargetModel.task_id == AiCallFollowUpDataModel.task_id,
                AiCallOutboundTargetModel.id == AiCallFollowUpDataModel.target_id,
            )),
            (AiCallOutboundTaskModel, and_(
                AiCallOutboundTaskModel.tenant_id == AiCallFollowUpDataModel.tenant_id,
                AiCallOutboundTaskModel.id == AiCallFollowUpDataModel.task_id,
            )),
        )
        conditions = [AiCallFollowUpDataModel.tenant_id == tenant_id]
        if classification:
            conditions.append(AiCallFollowUpDataModel.classification == classification)
        else:
            conditions.append(AiCallFollowUpDataModel.classification.is_not(None))
        normalized_customer_name = (customer_name or "").strip()
        if normalized_customer_name:
            conditions.append(
                AiCallOutboundTargetModel.customer_name.contains(
                    normalized_customer_name
                )
            )
        if task_id is not None:
            conditions.append(AiCallFollowUpDataModel.task_id == task_id)
        if last_contact_at_begin is not None:
            conditions.append(
                AiCallFollowUpDataModel.last_contact_at >= last_contact_at_begin
            )
        if last_contact_at_end is not None:
            conditions.append(
                AiCallFollowUpDataModel.last_contact_at < last_contact_at_end
            )
        statement = select(
            AiCallFollowUpDataModel,
            AiCallOutboundTargetModel,
            AiCallOutboundTaskModel,
            active_task,
            source_record,
        )
        count_statement = select(func.count(AiCallFollowUpDataModel.id))
        for joined_model, on_clause in joins:
            statement = statement.join(joined_model, on_clause)
            count_statement = count_statement.join(joined_model, on_clause)
        active_on = and_(
            active_task.tenant_id == AiCallFollowUpDataModel.tenant_id,
            active_task.follow_up_data_id == AiCallFollowUpDataModel.id,
            active_task.status.in_({"pending", "processing"}),
        )
        record_on = and_(
            source_record.tenant_id == AiCallFollowUpDataModel.tenant_id,
            source_record.call_id == AiCallFollowUpDataModel.source_call_id,
        )
        statement = statement.outerjoin(active_task, active_on).outerjoin(
            source_record, record_on
        )
        count_statement = count_statement.outerjoin(active_task, active_on)
        total = int(
            await self.db.scalar(count_statement.where(*conditions)) or 0
        )
        rows = (
            await self.db.execute(
                statement.where(*conditions)
                .order_by(
                    AiCallFollowUpDataModel.last_contact_at.desc(),
                    AiCallFollowUpDataModel.id.desc(),
                )
                .offset((page_num - 1) * page_size)
                .limit(page_size)
            )
        ).all()
        submitted_ids = await self._submitted_data_ids(
            tenant_id=tenant_id,
            follow_up_data_ids=[row[0].id for row in rows],
        )
        return [
            self._row_payload(
                *row,
                has_submitted_result=row[0].id in submitted_ids,
            )
            for row in rows
        ], total

    async def get_detail(
        self,
        *,
        tenant_id: str,
        follow_up_data_id: int,
    ) -> dict[str, Any]:
        context = await self._load_context(
            tenant_id=tenant_id,
            follow_up_data_id=follow_up_data_id,
        )
        if context is None:
            raise CustomException(msg="跟进数据不存在", status_code=404)
        data, target, task, active_task, source_record = context
        records = list(
            (
                await self.db.scalars(
                    select(AiCallRecordModel)
                    .where(
                        AiCallRecordModel.tenant_id == tenant_id,
                        or_(
                            AiCallRecordModel.follow_up_data_id == data.id,
                            AiCallRecordModel.call_id == data.source_call_id,
                        ),
                    )
                    .order_by(AiCallRecordModel.started_at, AiCallRecordModel.id)
                )
            ).all()
        )
        call_ids = [record.call_id for record in records]
        semantic_by_call = {
            row.call_id: row
            for row in (
                (
                    await self.db.scalars(
                        select(AiCallSemanticAnalysisModel).where(
                            AiCallSemanticAnalysisModel.call_id.in_(call_ids)
                        )
                    )
                ).all()
                if call_ids
                else []
            )
        }
        acw_by_call = {
            row.call_id: row
            for row in (
                (
                    await self.db.scalars(
                        select(AiCallAfterCallWorkModel).where(
                            AiCallAfterCallWorkModel.tenant_id == tenant_id,
                            AiCallAfterCallWorkModel.call_id.in_(call_ids),
                        )
                    )
                ).all()
                if call_ids
                else []
            )
        }
        handling_by_call = {
            row.related_call_id: row
            for row in (
                (
                    await self.db.scalars(
                        select(AiCallFollowUpHandlingResultModel).where(
                            AiCallFollowUpHandlingResultModel.tenant_id == tenant_id,
                            AiCallFollowUpHandlingResultModel.related_call_id.in_(
                                call_ids
                            ),
                        )
                    )
                ).all()
                if call_ids
                else []
            )
        }
        history_without_call = list(
            (
                await self.db.scalars(
                    select(AiCallFollowUpClassificationHistoryModel)
                    .where(
                        AiCallFollowUpClassificationHistoryModel.tenant_id
                        == tenant_id,
                        AiCallFollowUpClassificationHistoryModel.follow_up_data_id
                        == data.id,
                        AiCallFollowUpClassificationHistoryModel.call_id.is_(None),
                    )
                    .order_by(AiCallFollowUpClassificationHistoryModel.created_at)
                )
            ).all()
        )
        timeline = [
            self._record_timeline_payload(
                record,
                semantic=semantic_by_call.get(record.call_id),
                after_call_work=acw_by_call.get(record.call_id),
                handling_result=handling_by_call.get(record.call_id),
            )
            for record in records
        ]
        timeline.extend(
            {
                "type": "classification_adjustment",
                "call_id": None,
                "occurred_at": history.created_at,
                "from_classification": history.from_classification,
                "to_classification": history.to_classification,
                "conclusion": history.change_reason,
                "operator": history.changed_by_name or history.changed_by,
            }
            for history in history_without_call
        )
        timeline.sort(
            key=lambda item: item["occurred_at"]
            or datetime.min.replace(tzinfo=timezone.utc)
        )
        payload = self._row_payload(
            data,
            target,
            task,
            active_task,
            source_record,
            has_submitted_result=any(
                item.get("after_call_result_status") == "submitted"
                for item in timeline
            ),
        )
        payload["timeline"] = timeline
        return payload

    async def adjust_classification(
        self,
        *,
        tenant_id: str,
        follow_up_data_id: int,
        payload: FollowUpDataClassificationIn,
        idempotency_key: str,
        changed_by: str,
        changed_by_name: str | None,
        source_call_id: str | None = None,
        ai_suggestion: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_key = idempotency_key.strip()
        if not normalized_key:
            raise CustomException(msg="缺少 Idempotency-Key", status_code=400)
        fingerprint = self._request_fingerprint(
            follow_up_data_id=follow_up_data_id,
            payload=payload.model_dump(mode="json", by_alias=True),
        )
        existing = await self._get_idempotent_history(tenant_id, normalized_key)
        if existing is not None:
            return self._replay_adjustment(existing, fingerprint)

        data = await self.db.scalar(
            select(AiCallFollowUpDataModel)
            .where(
                AiCallFollowUpDataModel.tenant_id == tenant_id,
                AiCallFollowUpDataModel.id == follow_up_data_id,
            )
            .with_for_update()
        )
        if data is None:
            raise CustomException(msg="跟进数据不存在", status_code=404)
        existing = await self._get_idempotent_history(tenant_id, normalized_key)
        if existing is not None:
            return self._replay_adjustment(existing, fingerprint)
        if data.version != payload.expected_version:
            raise CustomException(
                msg="跟进数据已更新，请刷新后重试",
                status_code=409,
                data={
                    "errorCode": "VERSION_CONFLICT",
                    "currentVersion": data.version,
                },
            )

        try:
            async with self.db.begin_nested():
                now = datetime.now(timezone.utc)
                from_classification = data.classification
                data.classification = payload.classification
                data.classification_reason = payload.reason
                if payload.conclusion is not None:
                    data.latest_conclusion = payload.conclusion
                data.classification_source = "human"
                data.classification_confidence = None
                data.suggest_review = False
                data.low_value_reason = payload.low_value_reason
                data.classification_updated_at = now
                data.classification_updated_by = changed_by
                data.version += 1
                data.updated_at = now
                await self._finish_active_task_for_classification(
                    data=data,
                    reason=payload.reason,
                    low_value_reason=payload.low_value_reason,
                    now=now,
                )
                history = AiCallFollowUpClassificationHistoryModel(
                    id=generate_snowflake_id(),
                    tenant_id=tenant_id,
                    follow_up_data_id=data.id,
                    from_classification=from_classification,
                    to_classification=payload.classification,
                    change_reason=payload.reason,
                    source="manual_adjustment",
                    call_id=source_call_id,
                    semantic_analysis_id=None,
                    semantic_analysis_version=None,
                    ai_suggested_classification=(ai_suggestion or {}).get(
                        "classification"
                    ),
                    ai_confidence=(ai_suggestion or {}).get("confidence"),
                    ai_reason=(ai_suggestion or {}).get("reason"),
                    ai_evidence_json=(
                        json.dumps(
                            (ai_suggestion or {}).get("evidence") or [],
                            ensure_ascii=False,
                        )
                        if ai_suggestion is not None
                        else None
                    ),
                    ai_conflict=(ai_suggestion or {}).get("evidence_conflict"),
                    ai_adopted=(
                        payload.classification
                        == (ai_suggestion or {}).get("classification")
                        if ai_suggestion is not None
                        else None
                    ),
                    idempotency_key=normalized_key,
                    request_fingerprint=fingerprint,
                    result_version=data.version,
                    changed_by=changed_by,
                    changed_by_name=changed_by_name,
                    created_at=now,
                )
                self.db.add(history)
                await self.db.flush()
        except IntegrityError:
            existing = await self._get_idempotent_history(tenant_id, normalized_key)
            if existing is None:
                raise
            return self._replay_adjustment(existing, fingerprint)
        return self._adjustment_result(history)

    async def review_classification(
        self,
        *,
        tenant_id: str,
        follow_up_data_id: int,
        analysis: AiCallSemanticAnalysisModel,
        payload: FollowUpDataClassificationIn,
        idempotency_key: str,
        changed_by: str,
        changed_by_name: str | None,
    ) -> dict[str, Any]:
        normalized_key = idempotency_key.strip()
        if not normalized_key:
            raise CustomException(msg="缺少 Idempotency-Key", status_code=400)
        fingerprint = self._request_fingerprint(
            follow_up_data_id=follow_up_data_id,
            payload=payload.model_dump(mode="json", by_alias=True),
        )
        existing = await self._get_idempotent_history(tenant_id, normalized_key)
        if existing is not None:
            if existing.call_id != analysis.call_id:
                raise CustomException(
                    msg="Idempotency-Key 已用于其他请求",
                    status_code=409,
                    data={"errorCode": "IDEMPOTENCY_CONFLICT"},
                )
            return self._replay_adjustment(existing, fingerprint)

        data = await self.db.scalar(
            select(AiCallFollowUpDataModel).where(
                AiCallFollowUpDataModel.tenant_id == tenant_id,
                AiCallFollowUpDataModel.id == follow_up_data_id,
            )
        )
        result = analysis.analysis_result_dict or {}
        if not requires_classification_review(
            analysis_status=analysis.analysis_status,
            analysis_result=result,
            review_status=analysis.follow_up_review_status,
            current_classification=data.classification if data else None,
        ):
            raise CustomException(msg="该通话无需复核或已经复核", status_code=409)

        latest_history = await self.db.scalar(
            select(AiCallFollowUpClassificationHistoryModel)
            .where(
                AiCallFollowUpClassificationHistoryModel.tenant_id == tenant_id,
                AiCallFollowUpClassificationHistoryModel.follow_up_data_id
                == follow_up_data_id,
            )
            .order_by(
                AiCallFollowUpClassificationHistoryModel.created_at.desc(),
                AiCallFollowUpClassificationHistoryModel.id.desc(),
            )
            .limit(1)
        )
        if (
            data is None
            or latest_history is None
            or latest_history.call_id != analysis.call_id
            or (
                latest_history.source not in {"ai_auto", "handoff_after_call"}
                and not (
                    latest_history.source == "transfer_failed"
                    and data.classification_source == "system"
                )
            )
        ):
            raise CustomException(
                msg="分类已被后续通话或人工更新，请刷新后查看",
                status_code=409,
                data={"errorCode": "CLASSIFICATION_REVIEW_STALE"},
            )

        adjustment = await self.adjust_classification(
            tenant_id=tenant_id,
            follow_up_data_id=follow_up_data_id,
            payload=payload,
            idempotency_key=normalized_key,
            changed_by=changed_by,
            changed_by_name=changed_by_name,
            source_call_id=analysis.call_id,
            ai_suggestion=result,
        )
        now = datetime.now(timezone.utc)
        analysis.follow_up_review_status = (
            "confirmed"
            if payload.classification == result.get("classification")
            else "adjusted"
        )
        analysis.follow_up_reviewed_by = changed_by
        analysis.follow_up_reviewed_by_name = changed_by_name
        analysis.follow_up_reviewed_at = now
        analysis.updated_at = now
        await self.db.flush()
        return adjustment

    async def schedule_follow_up(
        self,
        *,
        tenant_id: str,
        follow_up_data_id: int,
        payload: FollowUpDataScheduleIn,
        idempotency_key: str,
        changed_by: str,
        changed_by_name: str | None,
    ) -> dict[str, Any]:
        normalized_key = idempotency_key.strip()
        if not normalized_key:
            raise CustomException(msg="缺少 Idempotency-Key", status_code=400)
        fingerprint = self._request_fingerprint(
            follow_up_data_id=follow_up_data_id,
            payload=payload.model_dump(mode="json", by_alias=True),
        )
        existing = await self._get_schedule_request(tenant_id, normalized_key)
        if existing is not None:
            return self._replay_schedule(existing, fingerprint)

        data = await self.db.scalar(
            select(AiCallFollowUpDataModel)
            .where(
                AiCallFollowUpDataModel.tenant_id == tenant_id,
                AiCallFollowUpDataModel.id == follow_up_data_id,
            )
            .with_for_update()
        )
        if data is None:
            raise CustomException(msg="跟进数据不存在", status_code=404)
        existing = await self._get_schedule_request(tenant_id, normalized_key)
        if existing is not None:
            return self._replay_schedule(existing, fingerprint)
        if payload.next_follow_up_at <= datetime.now(timezone.utc):
            raise CustomException(
                msg="计划回访时间必须晚于当前时间",
                status_code=422,
            )
        if data.version != payload.expected_version:
            raise CustomException(
                msg="跟进数据已更新，请刷新后重试",
                status_code=409,
                data={
                    "errorCode": "VERSION_CONFLICT",
                    "currentVersion": data.version,
                },
            )
        latest_history = await self.db.scalar(
            select(AiCallFollowUpClassificationHistoryModel)
            .where(
                AiCallFollowUpClassificationHistoryModel.tenant_id == tenant_id,
                AiCallFollowUpClassificationHistoryModel.follow_up_data_id
                == follow_up_data_id,
            )
            .order_by(
                AiCallFollowUpClassificationHistoryModel.created_at.desc(),
                AiCallFollowUpClassificationHistoryModel.id.desc(),
            )
            .limit(1)
        )
        if (
            data.suggest_review
            and latest_history is not None
            and latest_history.ai_suggested_classification
            and latest_history.ai_suggested_classification != data.classification
        ):
            raise CustomException(
                msg="AI 建议分类与当前业务分类不一致，请先完成分类复核",
                status_code=409,
                data={"errorCode": "CLASSIFICATION_REVIEW_REQUIRED"},
            )
        if data.classification not in {"interested", "nurturing"}:
            raise CustomException(
                msg="当前分类不能安排回访，请先调整为有意向或持续跟进",
                status_code=409,
                data={"errorCode": "FOLLOW_UP_CLASSIFICATION_CONFLICT"},
            )

        context = await self._load_context(
            tenant_id=tenant_id,
            follow_up_data_id=follow_up_data_id,
        )
        if context is None:
            raise CustomException(msg="跟进数据上下文不完整", status_code=409)
        _, _, outbound_task, active_task, source_record = context
        try:
            async with self.db.begin_nested():
                now = datetime.now(timezone.utc)
                if active_task is None:
                    task_id = generate_snowflake_id()
                    active_task = AiCallFollowUpTaskModel(
                        id=task_id,
                        tenant_id=tenant_id,
                        follow_up_data_id=data.id,
                        source_type="manual_schedule",
                        source_key=f"follow-up-data:{data.id}:schedule:{task_id}",
                        source_call_id=data.source_call_id,
                        source_handoff_id=None,
                        scene_code=outbound_task.scene_code,
                        business_type="outbound_task",
                        business_id=str(data.task_id),
                        contact_ref=f"call:{data.source_call_id}",
                        masked_contact=(
                            source_record.callee_phone_number_masked
                            if source_record is not None
                            and source_record.callee_phone_number_masked
                            else "未提供"
                        ),
                        owner_agent_identity=None,
                        status="pending",
                        follow_up_reason=payload.follow_up_reason,
                        customer_callback_at=payload.next_follow_up_at,
                        summary=data.latest_conclusion,
                        closed_reason=None,
                        closed_remark=None,
                        completed_at=None,
                        closed_at=None,
                        created_at=now,
                        updated_at=now,
                    )
                    self.db.add(active_task)
                else:
                    active_task.follow_up_reason = payload.follow_up_reason
                    active_task.customer_callback_at = payload.next_follow_up_at
                    active_task.updated_at = now

                data.version += 1
                data.updated_at = now
                request = AiCallFollowUpScheduleRequestModel(
                    id=generate_snowflake_id(),
                    tenant_id=tenant_id,
                    follow_up_data_id=data.id,
                    follow_up_id=active_task.id,
                    idempotency_key=normalized_key,
                    request_fingerprint=fingerprint,
                    result_version=data.version,
                    changed_by=changed_by,
                    changed_by_name=changed_by_name,
                    created_at=now,
                )
                self.db.add(request)
                await self.db.flush()
        except IntegrityError:
            existing = await self._get_schedule_request(tenant_id, normalized_key)
            if existing is None:
                raise
            return self._replay_schedule(existing, fingerprint)
        return self._schedule_result(request)

    async def _load_context(
        self,
        *,
        tenant_id: str,
        follow_up_data_id: int,
    ):
        active_task = aliased(AiCallFollowUpTaskModel)
        source_record = aliased(AiCallRecordModel)
        return (
            await self.db.execute(
                select(
                    AiCallFollowUpDataModel,
                    AiCallOutboundTargetModel,
                    AiCallOutboundTaskModel,
                    active_task,
                    source_record,
                )
                .join(
                    AiCallOutboundTargetModel,
                    and_(
                        AiCallOutboundTargetModel.tenant_id
                        == AiCallFollowUpDataModel.tenant_id,
                        AiCallOutboundTargetModel.task_id
                        == AiCallFollowUpDataModel.task_id,
                        AiCallOutboundTargetModel.id
                        == AiCallFollowUpDataModel.target_id,
                    ),
                )
                .join(
                    AiCallOutboundTaskModel,
                    and_(
                        AiCallOutboundTaskModel.tenant_id
                        == AiCallFollowUpDataModel.tenant_id,
                        AiCallOutboundTaskModel.id == AiCallFollowUpDataModel.task_id,
                    ),
                )
                .outerjoin(
                    active_task,
                    and_(
                        active_task.tenant_id == AiCallFollowUpDataModel.tenant_id,
                        active_task.follow_up_data_id == AiCallFollowUpDataModel.id,
                        active_task.status.in_({"pending", "processing"}),
                    ),
                )
                .outerjoin(
                    source_record,
                    and_(
                        source_record.tenant_id == AiCallFollowUpDataModel.tenant_id,
                        source_record.call_id == AiCallFollowUpDataModel.source_call_id,
                    ),
                )
                .where(
                    AiCallFollowUpDataModel.tenant_id == tenant_id,
                    AiCallFollowUpDataModel.id == follow_up_data_id,
                )
            )
        ).one_or_none()

    @staticmethod
    def _row_payload(
        data: AiCallFollowUpDataModel,
        target: AiCallOutboundTargetModel,
        task: AiCallOutboundTaskModel,
        active_task: AiCallFollowUpTaskModel | None,
        source_record: AiCallRecordModel | None,
        *,
        has_submitted_result: bool = False,
    ) -> dict[str, Any]:
        return {
            "follow_up_data_id": str(data.id),
            "tenant_id": data.tenant_id,
            "task_id": str(data.task_id),
            "target_id": str(data.target_id),
            "source_call_id": data.source_call_id,
            "customer_name": target.customer_name,
            "masked_contact": (
                source_record.callee_phone_number_masked
                if source_record is not None
                else active_task.masked_contact if active_task is not None else None
            ),
            "task_name": task.task_name,
            "classification": data.classification,
            "classification_reason": data.classification_reason,
            "classification_source": data.classification_source,
            "classification_confidence": data.classification_confidence,
            "suggest_review": data.suggest_review,
            "low_value_reason": data.low_value_reason,
            "latest_conclusion": data.latest_conclusion,
            "last_contact_at": data.last_contact_at,
            "next_follow_up_at": (
                active_task.customer_callback_at if active_task is not None else None
            ),
            "active_follow_up_id": (
                str(active_task.id) if active_task is not None else None
            ),
            "follow_up_task_status": (
                active_task.status if active_task is not None else None
            ),
            "active_follow_up_owner_agent_identity": (
                active_task.owner_agent_identity if active_task is not None else None
            ),
            "active_follow_up_reason": (
                active_task.follow_up_reason if active_task is not None else None
            ),
            "classification_updated_at": data.classification_updated_at,
            "classification_updated_by": data.classification_updated_by,
            "after_call_result_status": (
                "pending"
                if data.blocking_human_call_id
                else "submitted" if has_submitted_result else "not_applicable"
            ),
            "blocking_human_call_id": data.blocking_human_call_id,
            "version": data.version,
        }

    async def _submitted_data_ids(
        self,
        *,
        tenant_id: str,
        follow_up_data_ids: list[int],
    ) -> set[int]:
        if not follow_up_data_ids:
            return set()
        statement = select(
            AiCallFollowUpHandlingResultModel.follow_up_data_id
        ).where(
            AiCallFollowUpHandlingResultModel.tenant_id == tenant_id,
            AiCallFollowUpHandlingResultModel.follow_up_data_id.in_(
                follow_up_data_ids
            ),
        ).union(
            select(AiCallRecordModel.follow_up_data_id)
            .join(
                AiCallAfterCallWorkModel,
                and_(
                    AiCallAfterCallWorkModel.tenant_id
                    == AiCallRecordModel.tenant_id,
                    AiCallAfterCallWorkModel.call_id == AiCallRecordModel.call_id,
                ),
            )
            .where(
                AiCallRecordModel.tenant_id == tenant_id,
                AiCallRecordModel.follow_up_data_id.in_(follow_up_data_ids),
            )
        )
        return {int(value) for value in await self.db.scalars(statement) if value}

    @staticmethod
    def _record_timeline_payload(
        record: AiCallRecordModel,
        *,
        semantic: AiCallSemanticAnalysisModel | None,
        after_call_work: AiCallAfterCallWorkModel | None,
        handling_result: AiCallFollowUpHandlingResultModel | None,
    ) -> dict[str, Any]:
        analysis_result = semantic.analysis_result_dict if semantic else None
        conclusion = (
            handling_result.remark
            if handling_result is not None
            else after_call_work.summary
            if after_call_work is not None
            else (analysis_result or {}).get("summary")
        )
        if handling_result is not None or after_call_work is not None:
            result_status = "submitted"
        elif (
            record.operator_agent_identity
            and record.answered_at is not None
            and record.status in {"completed", "failed"}
        ):
            result_status = "pending"
        else:
            result_status = "not_applicable"
        return {
            "type": "call",
            "call_id": record.call_id,
            "occurred_at": record.started_at,
            "entry_type": record.entry_type,
            "status": record.status,
            "end_reason": record.end_reason,
            "duration_ms": record.duration_ms,
            "operator_agent_identity": record.operator_agent_identity,
            "conclusion": conclusion,
            "after_call_result_status": result_status,
            "semantic_analysis": analysis_result,
            "next_follow_up_at": (
                handling_result.next_follow_up_at
                if handling_result is not None
                else None
            ),
        }

    async def _get_idempotent_history(
        self,
        tenant_id: str,
        idempotency_key: str,
    ) -> AiCallFollowUpClassificationHistoryModel | None:
        return await self.db.scalar(
            select(AiCallFollowUpClassificationHistoryModel).where(
                AiCallFollowUpClassificationHistoryModel.tenant_id == tenant_id,
                AiCallFollowUpClassificationHistoryModel.idempotency_key
                == idempotency_key,
            )
        )

    async def _get_schedule_request(
        self,
        tenant_id: str,
        idempotency_key: str,
    ) -> AiCallFollowUpScheduleRequestModel | None:
        return await self.db.scalar(
            select(AiCallFollowUpScheduleRequestModel).where(
                AiCallFollowUpScheduleRequestModel.tenant_id == tenant_id,
                AiCallFollowUpScheduleRequestModel.idempotency_key
                == idempotency_key,
            )
        )

    @staticmethod
    def _request_fingerprint(
        *,
        follow_up_data_id: int,
        payload: dict[str, Any],
    ) -> str:
        body = {
            "followUpDataId": str(follow_up_data_id),
            **payload,
        }
        return sha256(
            json.dumps(
                body,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()

    @classmethod
    def _replay_adjustment(
        cls,
        history: AiCallFollowUpClassificationHistoryModel,
        fingerprint: str,
    ) -> dict[str, Any]:
        if history.request_fingerprint != fingerprint:
            raise CustomException(
                msg="Idempotency-Key 已用于其他请求",
                status_code=409,
                data={"errorCode": "IDEMPOTENCY_CONFLICT"},
            )
        return cls._adjustment_result(history)

    @staticmethod
    def _adjustment_result(
        history: AiCallFollowUpClassificationHistoryModel,
    ) -> dict[str, Any]:
        return {
            "follow_up_data_id": str(history.follow_up_data_id),
            "classification": history.to_classification,
            "reason": history.change_reason,
            "version": history.result_version,
        }

    @classmethod
    def _replay_schedule(
        cls,
        request: AiCallFollowUpScheduleRequestModel,
        fingerprint: str,
    ) -> dict[str, Any]:
        if request.request_fingerprint != fingerprint:
            raise CustomException(
                msg="Idempotency-Key 已用于其他请求",
                status_code=409,
                data={"errorCode": "IDEMPOTENCY_CONFLICT"},
            )
        return cls._schedule_result(request)

    @staticmethod
    def _schedule_result(
        request: AiCallFollowUpScheduleRequestModel,
    ) -> dict[str, Any]:
        return {
            "follow_up_data_id": str(request.follow_up_data_id),
            "follow_up_id": str(request.follow_up_id),
            "version": request.result_version,
        }

    async def _finish_active_task_for_classification(
        self,
        *,
        data: AiCallFollowUpDataModel,
        reason: str,
        low_value_reason: str | None,
        now: datetime,
    ) -> None:
        if data.classification not in {"low_value", "converted"}:
            return
        task = await self.db.scalar(
            select(AiCallFollowUpTaskModel)
            .where(
                AiCallFollowUpTaskModel.tenant_id == data.tenant_id,
                AiCallFollowUpTaskModel.follow_up_data_id == data.id,
                AiCallFollowUpTaskModel.status.in_({"pending", "processing"}),
            )
            .with_for_update()
        )
        if task is None:
            return
        task.customer_callback_at = None
        task.updated_at = now
        if data.classification == "converted":
            task.status = "completed"
            task.completed_at = now
            return
        task.status = "closed"
        task.closed_reason = {
            "explicit_rejection": "customer_refused",
            "invalid_contact": "invalid_contact",
        }.get(low_value_reason or "", "other")
        task.closed_remark = reason
        task.closed_at = now

    async def apply_ai_analysis(
        self,
        analysis: AiCallSemanticAnalysisModel,
    ) -> AiCallFollowUpDataModel | None:
        if analysis.analysis_status != "2" or analysis.analysis_version < 1:
            return None
        context = await self._formal_call_context(analysis.call_id)
        if context is None:
            return None
        record, attempt = context
        data = await self._get_for_update(
            tenant_id=attempt.tenant_id,
            task_id=attempt.task_id,
            target_id=attempt.target_id,
        )
        result = analysis.analysis_result_dict or {}
        classification = result.get("classification")
        valid_dialogue = (
            result.get("valid_dialogue") is True
            and classification in AI_CLASSIFICATIONS
            and bool(str(result.get("reason") or "").strip())
        )
        if not valid_dialogue:
            return await self._link_invalid_dialogue(record, data)

        if await self._get_analysis_history(attempt.tenant_id, analysis) is not None:
            if data is not None and record.follow_up_data_id != data.id:
                record.follow_up_data_id = data.id
                await self.db.flush()
            return data

        now = datetime.now(timezone.utc)
        created_here = False
        if data is None:
            candidate_id = generate_snowflake_id()
            await self._insert_data_if_missing({
                "id": candidate_id,
                "tenant_id": attempt.tenant_id,
                "task_id": attempt.task_id,
                "target_id": attempt.target_id,
                "source_call_id": analysis.call_id,
                "classification": classification,
                "classification_reason": result["reason"],
                "classification_source": "ai",
                "classification_confidence": result.get("confidence") or "low",
                "suggest_review": self._suggest_review(result),
                "low_value_reason": result.get("low_value_reason"),
                "latest_conclusion": result.get("summary") or None,
                "last_contact_at": record.ended_at or record.started_at,
                "blocking_human_call_id": None,
                "version": 1,
                "classification_updated_at": now,
                "classification_updated_by": "ai",
                "created_at": now,
                "updated_at": now,
            })
            data = await self._get_for_update(
                tenant_id=attempt.tenant_id,
                task_id=attempt.task_id,
                target_id=attempt.target_id,
            )
            if data is None:
                return None
            created_here = data.id == candidate_id

        history = await self._get_analysis_history(attempt.tenant_id, analysis)
        if history is not None:
            record.follow_up_data_id = data.id
            await self.db.flush()
            return data

        from_classification = None if created_here else data.classification
        classification_conflict = bool(
            not created_here
            and data.classification
            and data.classification != classification
        )
        protected = (
            data.classification_source in PROTECTED_CLASSIFICATION_SOURCES
            or classification_conflict
        )
        if not protected:
            data.classification = classification
            data.classification_reason = result["reason"]
            data.classification_source = "ai"
            data.classification_confidence = result.get("confidence") or "low"
            data.low_value_reason = result.get("low_value_reason")
            data.classification_updated_at = now
            data.classification_updated_by = "ai"
        data.suggest_review = self._suggest_review(result) or classification_conflict
        data.latest_conclusion = result.get("summary") or None
        data.last_contact_at = record.ended_at or record.started_at
        data.updated_at = now
        if not created_here:
            data.version += 1
        record.follow_up_data_id = data.id

        await self._insert_history_if_missing({
            "id": generate_snowflake_id(),
            "tenant_id": attempt.tenant_id,
            "follow_up_data_id": data.id,
            "from_classification": from_classification,
            "to_classification": data.classification,
            "change_reason": result["reason"],
            "source": "ai_auto",
            "call_id": analysis.call_id,
            "semantic_analysis_id": analysis.id,
            "semantic_analysis_version": analysis.analysis_version,
            "ai_suggested_classification": classification,
            "ai_confidence": result.get("confidence") or "low",
            "ai_reason": result["reason"],
            "ai_evidence_json": json.dumps(
                result.get("evidence") or [], ensure_ascii=False
            ),
            "ai_conflict": result.get("evidence_conflict") is True,
            "ai_adopted": not protected,
            "idempotency_key": None,
            "request_fingerprint": None,
            "result_version": data.version,
            "changed_by": "ai",
            "changed_by_name": "AI",
            "created_at": now,
        })
        await self.db.flush()
        return data

    async def apply_transfer_failure(
        self,
        handoff: AiCallHandoffModel,
        *,
        reason: str,
    ) -> AiCallFollowUpDataModel | None:
        """转人工未接通只更新客户分类，不创建回访任务。"""
        record = await self.repository.get_record(handoff.call_id)
        attempt = await self.repository.get_outbound_attempt_by_call_id(
            handoff.call_id
        )
        if (
            record is None
            or attempt is None
            or attempt.tenant_id != handoff.tenant_id
            or attempt.dialer_type not in {"sip", "owner_runtime"}
        ):
            return None

        idempotency_key = f"transfer-failed:{handoff.handoff_id}"
        existing = await self._get_idempotent_history(
            handoff.tenant_id, idempotency_key
        )
        if existing is not None:
            return await self.db.get(
                AiCallFollowUpDataModel, existing.follow_up_data_id
            )

        data = await self._get_for_update(
            tenant_id=attempt.tenant_id,
            task_id=attempt.task_id,
            target_id=attempt.target_id,
        )
        now = datetime.now(timezone.utc)
        created_here = False
        if data is None:
            candidate_id = generate_snowflake_id()
            await self._insert_data_if_missing({
                "id": candidate_id,
                "tenant_id": attempt.tenant_id,
                "task_id": attempt.task_id,
                "target_id": attempt.target_id,
                "source_call_id": handoff.call_id,
                "classification": "nurturing",
                "classification_reason": reason,
                "classification_source": "system",
                "classification_confidence": None,
                "suggest_review": False,
                "low_value_reason": None,
                "latest_conclusion": reason,
                "last_contact_at": record.ended_at or record.started_at,
                "blocking_human_call_id": None,
                "version": 1,
                "classification_updated_at": now,
                "classification_updated_by": "system",
                "created_at": now,
                "updated_at": now,
            })
            data = await self._get_for_update(
                tenant_id=attempt.tenant_id,
                task_id=attempt.task_id,
                target_id=attempt.target_id,
            )
            if data is None:
                return None
            created_here = data.id == candidate_id

        existing = await self._get_idempotent_history(
            handoff.tenant_id, idempotency_key
        )
        if existing is not None:
            record.follow_up_data_id = data.id
            await self.db.flush()
            return data

        from_classification = None if created_here else data.classification
        if data.classification_source not in PROTECTED_CLASSIFICATION_SOURCES:
            data.classification = "nurturing"
            data.classification_reason = reason
            data.classification_source = "system"
            data.classification_confidence = None
            data.suggest_review = False
            data.low_value_reason = None
            data.classification_updated_at = now
            data.classification_updated_by = "system"
        data.latest_conclusion = reason
        data.last_contact_at = record.ended_at or record.started_at
        data.updated_at = now
        if not created_here:
            data.version += 1
        record.follow_up_data_id = data.id

        self.db.add(
            AiCallFollowUpClassificationHistoryModel(
                id=generate_snowflake_id(),
                tenant_id=handoff.tenant_id,
                follow_up_data_id=data.id,
                from_classification=from_classification,
                to_classification=data.classification,
                change_reason=reason,
                source="transfer_failed",
                call_id=handoff.call_id,
                semantic_analysis_id=None,
                semantic_analysis_version=None,
                ai_suggested_classification=None,
                ai_confidence=None,
                ai_reason=None,
                ai_evidence_json=None,
                ai_conflict=None,
                ai_adopted=None,
                idempotency_key=idempotency_key,
                request_fingerprint=sha256(
                    f"{handoff.call_id}:{handoff.handoff_id}:{reason}".encode()
                ).hexdigest(),
                result_version=data.version,
                changed_by="system",
                changed_by_name="系统",
                created_at=now,
            )
        )
        await self.db.flush()
        return data

    async def _formal_call_context(
        self,
        call_id: str,
    ) -> tuple[AiCallRecordModel, AiCallOutboundAttemptModel] | None:
        record = await self.repository.get_record(call_id)
        if record is None or record.entry_type not in {
            "sip_outbound",
            "outbound",
            "web",
        }:
            return None
        attempt = await self.db.scalar(
            select(AiCallOutboundAttemptModel)
            .join(
                AiCallOutboundTargetModel,
                and_(
                    AiCallOutboundTargetModel.tenant_id
                    == AiCallOutboundAttemptModel.tenant_id,
                    AiCallOutboundTargetModel.task_id
                    == AiCallOutboundAttemptModel.task_id,
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
            .where(AiCallOutboundAttemptModel.call_id == call_id)
        )
        if attempt is None or attempt.dialer_type not in {"sip", "owner_runtime"}:
            return None
        if record.tenant_id and record.tenant_id != attempt.tenant_id:
            return None
        if await self.repository.list_handoffs(call_id):
            return None
        return record, attempt

    async def _get_for_update(
        self,
        *,
        tenant_id: str,
        task_id: int,
        target_id: int,
    ) -> AiCallFollowUpDataModel | None:
        return await self.db.scalar(
            select(AiCallFollowUpDataModel)
            .where(
                AiCallFollowUpDataModel.tenant_id == tenant_id,
                AiCallFollowUpDataModel.task_id == task_id,
                AiCallFollowUpDataModel.target_id == target_id,
            )
            .with_for_update()
        )

    async def _get_analysis_history(
        self,
        tenant_id: str,
        analysis: AiCallSemanticAnalysisModel,
    ) -> AiCallFollowUpClassificationHistoryModel | None:
        return await self.db.scalar(
            select(AiCallFollowUpClassificationHistoryModel).where(
                AiCallFollowUpClassificationHistoryModel.tenant_id == tenant_id,
                AiCallFollowUpClassificationHistoryModel.semantic_analysis_id
                == analysis.id,
                AiCallFollowUpClassificationHistoryModel.semantic_analysis_version
                == analysis.analysis_version,
            )
        )

    async def _link_invalid_dialogue(
        self,
        record: AiCallRecordModel,
        data: AiCallFollowUpDataModel | None,
    ) -> AiCallFollowUpDataModel | None:
        if data is None or record.follow_up_data_id == data.id:
            return data
        record.follow_up_data_id = data.id
        data.last_contact_at = record.ended_at or record.started_at
        data.updated_at = datetime.now(timezone.utc)
        data.version += 1
        await self.db.flush()
        return data

    async def _insert_data_if_missing(self, values: dict[str, Any]) -> None:
        table = AiCallFollowUpDataModel.__table__
        dialect = self._dialect_name()
        if dialect == "postgresql":
            statement = postgresql_insert(table).values(**values).on_conflict_do_nothing(
                index_elements=[table.c.tenant_id, table.c.task_id, table.c.target_id]
            )
        elif dialect == "sqlite":
            statement = sqlite_insert(table).values(**values).on_conflict_do_nothing(
                index_elements=[table.c.tenant_id, table.c.task_id, table.c.target_id]
            )
        else:
            self.db.add(AiCallFollowUpDataModel(**values))
            await self.db.flush()
            return
        await self.db.execute(statement)

    async def _insert_history_if_missing(self, values: dict[str, Any]) -> None:
        table = AiCallFollowUpClassificationHistoryModel.__table__
        dialect = self._dialect_name()
        conflict_columns = [
            table.c.tenant_id,
            table.c.semantic_analysis_id,
            table.c.semantic_analysis_version,
        ]
        if dialect == "postgresql":
            statement = postgresql_insert(table).values(**values).on_conflict_do_nothing(
                index_elements=conflict_columns
            )
        elif dialect == "sqlite":
            statement = sqlite_insert(table).values(**values).on_conflict_do_nothing(
                index_elements=conflict_columns
            )
        else:
            self.db.add(AiCallFollowUpClassificationHistoryModel(**values))
            await self.db.flush()
            return
        await self.db.execute(statement)

    def _dialect_name(self) -> str:
        bind = self.db.get_bind()
        return bind.dialect.name if bind is not None else ""

    @staticmethod
    def _suggest_review(result: dict[str, Any]) -> bool:
        return result.get("confidence") != "high" or result.get(
            "evidence_conflict"
        ) is True
