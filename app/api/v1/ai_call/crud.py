from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import Select, String, and_, asc, cast, desc, func, or_, select, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from app.api.v1.ai_call.model import (
    AiCallAfterCallWorkModel,
    AiCallAsrJobModel,
    AiCallDialogueSegmentModel,
    AiCallEventModel,
    AiCallFollowUpClassificationHistoryModel,
    AiCallFollowUpDataModel,
    AiCallFollowUpHandlingResultModel,
    AiCallFollowUpTaskModel,
    AiCallHandoffAgentModel,
    AiCallHandoffModel,
    AiCallPromptCommonConfigModel,
    AiCallPromptProfileModel,
    AiCallPromptProfileVersionApplicationModel,
    AiCallPromptProfileVersionModel,
    AiCallQualityReviewModel,
    AiCallQualityScoreModel,
    AiCallRecordingModel,
    AiCallRecordingTrackModel,
    AiCallRecordModel,
    AiCallSemanticAnalysisModel,
    AiCallVoiceProfileModel,
)
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundExceptionBatchModel,
    AiCallOutboundExceptionPolicyModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from app.services.ai_call.classification_review import requires_classification_review
from app.utils.id_util import generate_snowflake_id

DEFAULT_SEMANTIC_ANALYSIS_SCENE_CODE = "ai_call_semantic_analysis"
SEMANTIC_ANALYSIS_STATUS_PENDING = "0"
SEMANTIC_ANALYSIS_STATUS_RUNNING = "1"
SEMANTIC_ANALYSIS_STATUS_SUCCEEDED = "2"
SEMANTIC_ANALYSIS_STATUS_FAILED = "3"
SEMANTIC_ANALYSIS_STATUS_NO_USER_INPUT = "4"
DEFAULT_QUALITY_SCORE_MODEL_VERSION = "quality-v1"
QUALITY_SCORE_STATUS_PENDING = "pending"
QUALITY_SCORE_STATUS_RUNNING = "processing"
QUALITY_SCORE_STATUS_COMPLETED = "completed"
QUALITY_SCORE_STATUS_FAILED = "failed"
QUALITY_SCORE_MAX_RETRY_COUNT = 3
QUALITY_SCORE_RETRY_COOLDOWN_MINUTES = 10
QUALITY_REVIEW_RESULTS = {"excellent", "good", "pass", "fail"}
DEFAULT_TENANT_ID = "000000"


@dataclass(frozen=True, slots=True)
class RecordingVerificationClaim:
    recording_id: int
    tenant_id: str
    call_id: str
    object_name: str | None
    started_at: datetime
    ended_at: datetime | None
    duration_ms: int | None
    verify_attempts: int
    verify_deadline_at: datetime | None
    claim_token: datetime


@dataclass(frozen=True, slots=True)
class RecordingTrackVerificationClaim:
    track_id: int
    tenant_id: str
    call_id: str
    track_role: str
    participant_identity: str
    object_name: str | None
    started_at: datetime
    ended_at: datetime | None
    duration_ms: int | None
    verify_attempts: int
    verify_deadline_at: datetime | None
    claim_token: datetime


class AiCallRecordRepository:
    """AI Call B1 专用持久化仓储。"""

    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def create_record(
        self,
        *,
        tenant_id: str | None = None,
        call_id: str,
        business_type: str | None,
        business_id: str | None,
        scene_code: str | None = None,
        prompt_source_key: str | None = None,
        entry_type: str,
        room_name: str,
        participant_identity: str,
        status: str,
        started_at: datetime,
        callee_phone_number_hash: str | None = None,
        callee_phone_number_masked: str | None = None,
    ) -> AiCallRecordModel:
        record = AiCallRecordModel(
            id=generate_snowflake_id(),
            tenant_id=tenant_id,
            call_id=call_id,
            business_type=business_type,
            business_id=business_id,
            scene_code=scene_code,
            prompt_source_key=prompt_source_key,
            entry_type=entry_type,
            room_name=room_name,
            participant_identity=participant_identity,
            callee_phone_number_hash=callee_phone_number_hash,
            callee_phone_number_masked=callee_phone_number_masked,
            status=status,
            started_at=started_at,
        )
        self.db.add(record)
        await self.db.flush()
        await self.db.refresh(record)
        return record

    async def get_record(self, call_id: str) -> AiCallRecordModel | None:
        result = await self.db.execute(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id == call_id)
        )
        return result.scalar_one_or_none()

    async def get_record_for_tenant(
        self,
        *,
        tenant_id: str,
        call_id: str,
    ) -> AiCallRecordModel | None:
        result = await self.db.execute(
            select(AiCallRecordModel).where(
                AiCallRecordModel.tenant_id == tenant_id,
                AiCallRecordModel.call_id == call_id,
            )
        )
        return result.scalar_one_or_none()

    async def get_exception_handling(
        self,
        *,
        tenant_id: str,
        call_id: str,
    ) -> dict | None:
        row = (
            await self.db.execute(
                select(
                    AiCallOutboundTargetModel,
                    AiCallOutboundExceptionBatchModel,
                )
                .join(
                    AiCallOutboundAttemptModel,
                    (
                        AiCallOutboundAttemptModel.tenant_id
                        == AiCallOutboundTargetModel.tenant_id
                    )
                    & (
                        AiCallOutboundAttemptModel.target_id
                        == AiCallOutboundTargetModel.id
                    ),
                )
                .outerjoin(
                    AiCallOutboundExceptionBatchModel,
                    (
                        AiCallOutboundExceptionBatchModel.tenant_id
                        == AiCallOutboundTargetModel.tenant_id
                    )
                    & (
                        AiCallOutboundExceptionBatchModel.id
                        == AiCallOutboundTargetModel.exception_batch_id
                    ),
                )
                .where(
                    AiCallOutboundTargetModel.tenant_id == tenant_id,
                    AiCallOutboundAttemptModel.call_id == call_id,
                    AiCallOutboundTargetModel.exception_category.is_not(None),
                )
                .limit(1)
            )
        ).first()
        if row is None:
            return None
        target, batch = row
        original_count = target.exception_original_attempt_count or target.attempt_count
        retry_count = max(0, target.attempt_count - original_count)
        max_retry_count = batch.max_retry_count if batch is not None else 0
        if batch is None and target.exception_category != "invalid_number":
            max_retry_count = int(
                await self.db.scalar(
                    select(AiCallOutboundExceptionPolicyModel.max_retry_count).where(
                        AiCallOutboundExceptionPolicyModel.tenant_id == tenant_id,
                        AiCallOutboundExceptionPolicyModel.category
                        == target.exception_category,
                    )
                )
                or {
                    "no_answer": 3,
                    "rejected": 2,
                    "early_hangup": 2,
                }.get(target.exception_category, 0)
            )
        if target.exception_category == "invalid_number":
            display_status = "UNAVAILABLE"
        elif target.exception_batch_id is None:
            display_status = "PENDING"
        elif target.status in {"DIALING", "IN_CALL"}:
            display_status = "CALLING"
        elif target.status == "RETRY_WAIT":
            display_status = "WAITING"
        elif target.status == "CANCELLED":
            display_status = "STOPPED"
        elif target.latest_result == "connected":
            display_status = "CONNECTED"
        elif retry_count >= max_retry_count:
            display_status = "MAXED"
        else:
            display_status = "STOPPED"
        return {
            "category": target.exception_category,
            "status": display_status,
            "originalAttemptCount": original_count,
            "retryCount": retry_count,
            "maxRetryCount": max_retry_count,
            "lastResult": target.latest_result,
        }

    async def get_follow_up_relation(
        self,
        *,
        tenant_id: str,
        call_id: str,
        follow_up_id: int | None,
    ) -> tuple[
        AiCallFollowUpTaskModel | None,
        AiCallRecordModel | None,
        list[AiCallRecordModel],
    ]:
        task_stmt = select(AiCallFollowUpTaskModel).where(
            AiCallFollowUpTaskModel.tenant_id == tenant_id
        )
        if follow_up_id is not None:
            task_stmt = task_stmt.where(AiCallFollowUpTaskModel.id == follow_up_id)
        else:
            task_stmt = task_stmt.where(
                AiCallFollowUpTaskModel.source_call_id == call_id
            )
        tasks = list((await self.db.execute(task_stmt)).scalars().all())
        if not tasks:
            return None, None, []
        active = [task for task in tasks if task.status not in {"completed", "closed"}]
        task = max(
            active or tasks,
            key=lambda item: (item.updated_at or item.created_at, item.id),
        )
        records = list(
            (
                await self.db.execute(
                    select(AiCallRecordModel)
                    .where(
                        AiCallRecordModel.tenant_id == tenant_id,
                        or_(
                            AiCallRecordModel.call_id == task.source_call_id,
                            AiCallRecordModel.follow_up_id == task.id,
                        ),
                    )
                    .order_by(AiCallRecordModel.started_at, AiCallRecordModel.id)
                )
            )
            .scalars()
            .all()
        )
        source_record = next(
            (record for record in records if record.call_id == task.source_call_id),
            None,
        )
        callback_records = [
            record for record in records if record.follow_up_id == task.id
        ]
        return task, source_record, callback_records

    async def get_after_call_work(
        self,
        *,
        tenant_id: str,
        call_id: str,
    ) -> AiCallAfterCallWorkModel | None:
        result = await self.db.execute(
            select(AiCallAfterCallWorkModel).where(
                AiCallAfterCallWorkModel.tenant_id == tenant_id,
                AiCallAfterCallWorkModel.call_id == call_id,
            )
        )
        return result.scalar_one_or_none()

    async def get_follow_up_handling_result(
        self,
        *,
        tenant_id: str,
        call_id: str,
    ) -> AiCallFollowUpHandlingResultModel | None:
        return await self.db.scalar(
            select(AiCallFollowUpHandlingResultModel)
            .where(
                AiCallFollowUpHandlingResultModel.tenant_id == tenant_id,
                AiCallFollowUpHandlingResultModel.related_call_id == call_id,
            )
            .order_by(AiCallFollowUpHandlingResultModel.handled_at.desc())
        )

    async def get_follow_up_data(
        self,
        *,
        tenant_id: str,
        follow_up_data_id: int,
    ) -> AiCallFollowUpDataModel | None:
        return await self.db.scalar(
            select(AiCallFollowUpDataModel).where(
                AiCallFollowUpDataModel.tenant_id == tenant_id,
                AiCallFollowUpDataModel.id == follow_up_data_id,
            )
        )

    async def get_active_follow_up_for_data(
        self,
        *,
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
            .order_by(
                AiCallFollowUpTaskModel.updated_at.desc(),
                AiCallFollowUpTaskModel.id.desc(),
            )
            .limit(1)
        )

    async def list_records_by_call_ids(
        self,
        call_ids: list[str],
    ) -> list[AiCallRecordModel]:
        if not call_ids:
            return []
        result = await self.db.execute(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id.in_(call_ids))
        )
        return list(result.scalars().all())

    async def list_dialogue_segments_by_call_ids(
        self,
        call_ids: list[str],
    ) -> list[AiCallDialogueSegmentModel]:
        if not call_ids:
            return []
        ranked_segments = (
            select(
                AiCallDialogueSegmentModel.id.label("segment_id"),
                func.row_number()
                .over(
                    partition_by=AiCallDialogueSegmentModel.call_id,
                    order_by=AiCallDialogueSegmentModel.segment_no.desc(),
                )
                .label("row_number"),
            )
            .where(
                AiCallDialogueSegmentModel.call_id.in_(call_ids),
                AiCallDialogueSegmentModel.segment_status == "final",
                AiCallDialogueSegmentModel.speaker_type.in_(
                    {"customer", "ai", "human_agent"}
                ),
                func.length(func.trim(AiCallDialogueSegmentModel.segment_text)) > 0,
            )
            .subquery()
        )
        result = await self.db.execute(
            select(AiCallDialogueSegmentModel)
            .join(
                ranked_segments,
                ranked_segments.c.segment_id == AiCallDialogueSegmentModel.id,
            )
            .where(ranked_segments.c.row_number <= 6)
            .order_by(
                asc(AiCallDialogueSegmentModel.call_id),
                asc(AiCallDialogueSegmentModel.segment_no),
            )
        )
        return list(result.scalars().all())

    async def list_handoff_context_dialogue(
        self,
        call_id: str,
        *,
        before_at: datetime | None = None,
    ) -> list[AiCallDialogueSegmentModel]:
        conditions = [
            AiCallDialogueSegmentModel.call_id == call_id,
            AiCallDialogueSegmentModel.segment_status.in_({"final", "interrupted"}),
            AiCallDialogueSegmentModel.speaker_type.in_({"ai", "customer"}),
            func.length(func.trim(AiCallDialogueSegmentModel.segment_text)) > 0,
        ]
        if before_at is not None:
            conditions.append(
                func.coalesce(
                    AiCallDialogueSegmentModel.started_at,
                    AiCallDialogueSegmentModel.ended_at,
                )
                <= before_at
            )
        result = await self.db.execute(
            select(AiCallDialogueSegmentModel)
            .where(*conditions)
            .order_by(
                asc(AiCallDialogueSegmentModel.segment_no),
                asc(AiCallDialogueSegmentModel.id),
            )
        )
        return list(result.scalars().all())

    async def outbound_customer_names_by_call_ids(
        self,
        *,
        tenant_id: str,
        call_ids: list[str],
    ) -> dict[str, str | None]:
        if not call_ids:
            return {}
        result = await self.db.execute(
            select(
                AiCallOutboundAttemptModel.call_id,
                AiCallOutboundTargetModel.customer_name,
            )
            .join(
                AiCallOutboundTargetModel,
                and_(
                    AiCallOutboundTargetModel.id == AiCallOutboundAttemptModel.target_id,
                    AiCallOutboundTargetModel.tenant_id
                    == AiCallOutboundAttemptModel.tenant_id,
                ),
            )
            .where(
                AiCallOutboundAttemptModel.tenant_id == tenant_id,
                AiCallOutboundAttemptModel.call_id.in_(call_ids),
            )
        )
        return dict(result.all())

    async def get_outbound_task_config_snapshot(
        self,
        task_id: int,
        *,
        tenant_id: str | None = None,
    ) -> str | None:
        statement = select(AiCallOutboundTaskModel.config_snapshot_json).where(
            AiCallOutboundTaskModel.id == task_id
        )
        if tenant_id:
            statement = statement.where(AiCallOutboundTaskModel.tenant_id == tenant_id)
        return await self.db.scalar(statement)

    async def get_outbound_attempt_task_snapshot(
        self,
        attempt_id: int,
        *,
        tenant_id: str,
    ) -> tuple[int, str] | None:
        row = (
            await self.db.execute(
                select(
                    AiCallOutboundTaskModel.id,
                    AiCallOutboundTaskModel.config_snapshot_json,
                )
                .join(
                    AiCallOutboundAttemptModel,
                    AiCallOutboundAttemptModel.task_id == AiCallOutboundTaskModel.id,
                )
                .where(
                    AiCallOutboundAttemptModel.id == attempt_id,
                    AiCallOutboundAttemptModel.tenant_id == tenant_id,
                    AiCallOutboundTaskModel.tenant_id == tenant_id,
                )
            )
        ).one_or_none()
        return (int(row[0]), str(row[1])) if row is not None else None

    async def get_outbound_attempt_task_config_snapshot(
        self,
        attempt_id: int,
        *,
        tenant_id: str | None = None,
    ) -> str | None:
        statement = (
            select(AiCallOutboundTaskModel.config_snapshot_json)
            .join(
                AiCallOutboundAttemptModel,
                AiCallOutboundAttemptModel.task_id == AiCallOutboundTaskModel.id,
            )
            .where(AiCallOutboundAttemptModel.id == attempt_id)
        )
        if tenant_id:
            statement = statement.where(
                AiCallOutboundAttemptModel.tenant_id == tenant_id,
                AiCallOutboundTaskModel.tenant_id == tenant_id,
            )
        return await self.db.scalar(statement)

    async def get_outbound_attempt_by_call_id(
        self,
        call_id: str,
    ) -> AiCallOutboundAttemptModel | None:
        result = await self.db.execute(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.call_id == call_id
            )
        )
        return result.scalar_one_or_none()

    async def has_task_owned_outbound_attempt(
        self,
        *,
        tenant_id: str,
        call_id: str,
    ) -> bool:
        attempt_id = await self.db.scalar(
            select(AiCallOutboundAttemptModel.id)
            .join(
                AiCallOutboundTargetModel,
                and_(
                    AiCallOutboundTargetModel.id
                    == AiCallOutboundAttemptModel.target_id,
                    AiCallOutboundTargetModel.tenant_id
                    == AiCallOutboundAttemptModel.tenant_id,
                    AiCallOutboundTargetModel.task_id
                    == AiCallOutboundAttemptModel.task_id,
                ),
            )
            .join(
                AiCallOutboundTaskModel,
                and_(
                    AiCallOutboundTaskModel.id == AiCallOutboundAttemptModel.task_id,
                    AiCallOutboundTaskModel.tenant_id
                    == AiCallOutboundAttemptModel.tenant_id,
                ),
            )
            .where(
                AiCallOutboundAttemptModel.tenant_id == tenant_id,
                AiCallOutboundAttemptModel.call_id == call_id,
            )
            .limit(1)
        )
        return attempt_id is not None

    async def get_active_sip_record_by_callee_hash(
        self,
        *,
        callee_phone_number_hash: str,
        active_statuses: set[str],
    ) -> AiCallRecordModel | None:
        if not callee_phone_number_hash or not active_statuses:
            return None
        result = await self.db.execute(
            select(AiCallRecordModel)
            .where(
                AiCallRecordModel.entry_type == "sip_outbound",
                AiCallRecordModel.callee_phone_number_hash == callee_phone_number_hash,
                AiCallRecordModel.status.in_(active_statuses),
                AiCallRecordModel.ended_at.is_(None),
            )
            .order_by(desc(AiCallRecordModel.started_at), desc(AiCallRecordModel.id))
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def update_record(self, call_id: str, **values) -> AiCallRecordModel | None:
        record = await self.get_record(call_id)
        if record is None:
            return None
        for key, value in values.items():
            if hasattr(record, key):
                setattr(record, key, value)
        await self.db.flush()
        await self.db.refresh(record)
        return record

    async def list_records(
        self,
        *,
        tenant_id: str | None = None,
        call_id: str | None = None,
        task_id: int | None = None,
        target_id: int | None = None,
        phone_number: str | None = None,
        customer_name: str | None = None,
        call_result: str | None = None,
        customer_intent: str | None = None,
        classification_review_status: str | None = None,
        follow_up_status: str | None = None,
        after_call_result_status: str | None = None,
        operator_agent_identity: str | None = None,
        business_type: str | None = None,
        business_id: str | None = None,
        status: str | None = None,
        entry_type: str | None = None,
        formal_outbound_only: bool = False,
        started_at_begin: datetime | None = None,
        started_at_end: datetime | None = None,
        page_num: int = 1,
        page_size: int = 10,
    ) -> tuple[list[AiCallRecordModel], int]:
        stmt = self._record_filters(
            select(AiCallRecordModel),
            tenant_id=tenant_id,
            call_id=call_id,
            task_id=task_id,
            target_id=target_id,
            phone_number=phone_number,
            customer_name=customer_name,
            call_result=call_result,
            customer_intent=customer_intent,
            classification_review_status=classification_review_status,
            follow_up_status=follow_up_status,
            after_call_result_status=after_call_result_status,
            operator_agent_identity=operator_agent_identity,
            business_type=business_type,
            business_id=business_id,
            status=status,
            entry_type=entry_type,
            formal_outbound_only=formal_outbound_only,
            started_at_begin=started_at_begin,
            started_at_end=started_at_end,
        )
        count_stmt = self._record_filters(
            select(func.count()).select_from(AiCallRecordModel),
            tenant_id=tenant_id,
            call_id=call_id,
            task_id=task_id,
            target_id=target_id,
            phone_number=phone_number,
            customer_name=customer_name,
            call_result=call_result,
            customer_intent=customer_intent,
            classification_review_status=classification_review_status,
            follow_up_status=follow_up_status,
            after_call_result_status=after_call_result_status,
            operator_agent_identity=operator_agent_identity,
            business_type=business_type,
            business_id=business_id,
            status=status,
            entry_type=entry_type,
            formal_outbound_only=formal_outbound_only,
            started_at_begin=started_at_begin,
            started_at_end=started_at_end,
        )
        total = int((await self.db.execute(count_stmt)).scalar_one())
        safe_page_num = max(1, page_num)
        safe_page_size = max(1, min(page_size, 1000))
        stmt = (
            stmt
            .order_by(desc(AiCallRecordModel.started_at), desc(AiCallRecordModel.id))
            .offset((safe_page_num - 1) * safe_page_size)
            .limit(safe_page_size)
        )
        rows = (await self.db.execute(stmt)).scalars().all()
        await self.attach_outbound_context(rows, tenant_id=tenant_id)
        await self._attach_quality_context(rows, tenant_id=tenant_id)
        await self._attach_semantic_analysis(rows)
        await self._attach_follow_up_context(rows, tenant_id=tenant_id)
        await self.attach_after_call_result_context(rows, tenant_id=tenant_id)
        return list(rows), total

    async def attach_after_call_result_context(
        self,
        records: list[AiCallRecordModel],
        *,
        tenant_id: str | None,
    ) -> None:
        call_ids = [record.call_id for record in records]
        if not call_ids:
            return
        after_call_work = select(AiCallAfterCallWorkModel.call_id).where(
            AiCallAfterCallWorkModel.call_id.in_(call_ids)
        )
        handling_result = select(
            AiCallFollowUpHandlingResultModel.related_call_id
        ).where(
            AiCallFollowUpHandlingResultModel.related_call_id.in_(call_ids)
        )
        if tenant_id:
            after_call_work = after_call_work.where(
                AiCallAfterCallWorkModel.tenant_id == tenant_id
            )
            handling_result = handling_result.where(
                AiCallFollowUpHandlingResultModel.tenant_id == tenant_id
            )
        submitted_call_ids = {
            value
            for value in await self.db.scalars(
                after_call_work.union(handling_result)
            )
            if value
        }
        for record in records:
            record._after_call_result_status = (
                "submitted"
                if record.call_id in submitted_call_ids
                else "pending"
                if record.operator_agent_identity
                and record.status in {"completed", "failed"}
                else "not_applicable"
            )

    async def _attach_quality_context(
        self,
        records: list[AiCallRecordModel],
        *,
        tenant_id: str | None,
    ) -> None:
        call_ids = [record.call_id for record in records]
        if not call_ids or not tenant_id:
            for record in records:
                record._quality_context = {}
            return

        score_rows = (
            await self.db.execute(
                select(AiCallQualityScoreModel).where(
                    AiCallQualityScoreModel.tenant_id == tenant_id,
                    AiCallQualityScoreModel.call_id.in_(call_ids),
                    AiCallQualityScoreModel.model_version
                    == DEFAULT_QUALITY_SCORE_MODEL_VERSION,
                )
            )
        ).scalars().all()
        review_rows = (
            await self.db.execute(
                select(AiCallQualityReviewModel).where(
                    AiCallQualityReviewModel.tenant_id == tenant_id,
                    AiCallQualityReviewModel.call_id.in_(call_ids),
                )
            )
        ).scalars().all()
        scores = {row.call_id: row for row in score_rows}
        reviews = {row.call_id: row for row in review_rows}

        for record in records:
            score = scores.get(record.call_id)
            review = reviews.get(record.call_id)
            score_status = (
                score.status
                if score is not None
                else (
                    QUALITY_SCORE_STATUS_PENDING
                    if self._quality_score_applicable(record)
                    else "not_applicable"
                )
            )
            record._quality_context = {
                "qualityScoreStatus": score_status,
                "qualityScore": (
                    score.score
                    if score is not None
                    and score.status == QUALITY_SCORE_STATUS_COMPLETED
                    else None
                ),
                "qualityReviewResult": review.quality_result if review else None,
                "qualityReviewReason": review.quality_reason if review else None,
            }

    async def _attach_semantic_analysis(
        self,
        records: list[AiCallRecordModel],
    ) -> None:
        call_ids = [record.call_id for record in records]
        if not call_ids:
            return
        follow_up_data_ids = [
            record.follow_up_data_id
            for record in records
            if record.follow_up_data_id is not None
        ]
        classifications_by_data_id = {}
        if follow_up_data_ids:
            classification_rows = (
                await self.db.execute(
                    select(
                        AiCallFollowUpDataModel.id,
                        AiCallFollowUpDataModel.classification,
                    ).where(AiCallFollowUpDataModel.id.in_(follow_up_data_ids))
                )
            ).all()
            classifications_by_data_id = {
                row.id: row.classification for row in classification_rows
            }
        current_classification_by_call_id = {
            record.call_id: classifications_by_data_id.get(record.follow_up_data_id)
            for record in records
        }
        analysis_rows = (
            await self.db.execute(
                select(
                    AiCallSemanticAnalysisModel.call_id,
                    AiCallSemanticAnalysisModel.analysis_status,
                    AiCallSemanticAnalysisModel.analysis_result,
                    AiCallSemanticAnalysisModel.customer_intent,
                    AiCallSemanticAnalysisModel.follow_up_suggested,
                    AiCallSemanticAnalysisModel.follow_up_consent,
                    AiCallSemanticAnalysisModel.follow_up_confidence,
                    AiCallSemanticAnalysisModel.follow_up_review_status,
                ).where(
                    AiCallSemanticAnalysisModel.call_id.in_(call_ids),
                    AiCallSemanticAnalysisModel.analysis_scene_code
                    == DEFAULT_SEMANTIC_ANALYSIS_SCENE_CODE,
                )
            )
        ).all()
        analysis_by_call_id = {}
        for row in analysis_rows:
            try:
                analysis_result = (
                    row.analysis_result
                    if isinstance(row.analysis_result, dict)
                    else json.loads(row.analysis_result or "{}")
                )
            except (TypeError, ValueError):
                analysis_result = {}
            classification_requires_review = requires_classification_review(
                analysis_status=row.analysis_status,
                analysis_result=analysis_result,
                review_status=row.follow_up_review_status,
                current_classification=current_classification_by_call_id.get(
                    row.call_id
                ),
            )
            analysis_by_call_id[row.call_id] = {
                "analysisStatus": row.analysis_status,
                "analysisResult": row.analysis_result,
                "customerIntent": row.customer_intent,
                "followUpSuggested": bool(row.follow_up_suggested),
                "followUpRequiresReview": (
                    row.analysis_status == SEMANTIC_ANALYSIS_STATUS_SUCCEEDED
                    and row.follow_up_review_status is None
                    and row.follow_up_consent != "refused"
                    and not (
                        row.follow_up_suggested
                        and row.follow_up_consent == "explicit"
                        and row.follow_up_confidence == "high"
                    )
                ),
                "followUpReviewStatus": row.follow_up_review_status,
                "classificationRequiresReview": classification_requires_review,
                "classificationReviewStatus": (
                    "reviewed"
                    if row.follow_up_review_status in {"confirmed", "adjusted"}
                    else "suggested"
                    if classification_requires_review
                    else None
                ),
            }
        for record in records:
            context = analysis_by_call_id.get(record.call_id, {})
            record._semantic_analysis_context = context
            record._semantic_analysis_result = context.get("analysisResult")

    async def _attach_follow_up_context(
        self,
        records: list[AiCallRecordModel],
        *,
        tenant_id: str | None,
    ) -> None:
        call_ids = [record.call_id for record in records]
        follow_up_ids = [
            record.follow_up_id
            for record in records
            if record.follow_up_id is not None
        ]
        if not tenant_id or (not call_ids and not follow_up_ids):
            return
        result = await self.db.execute(
            select(AiCallFollowUpTaskModel).where(
                AiCallFollowUpTaskModel.tenant_id == tenant_id,
                or_(
                    AiCallFollowUpTaskModel.source_call_id.in_(call_ids),
                    AiCallFollowUpTaskModel.id.in_(follow_up_ids),
                ),
            )
        )
        tasks = list(result.scalars().all())
        tasks_by_id = {task.id: task for task in tasks}
        tasks_by_call_id: dict[str, list[AiCallFollowUpTaskModel]] = {}
        for task in tasks:
            tasks_by_call_id.setdefault(task.source_call_id, []).append(task)

        for record in records:
            selected = (
                tasks_by_id.get(record.follow_up_id)
                if record.follow_up_id is not None
                else None
            )
            if selected is None:
                candidates = tasks_by_call_id.get(record.call_id, [])
                active = [
                    task
                    for task in candidates
                    if task.status not in {"completed", "closed"}
                ]
                pool = active or candidates
                if pool:
                    selected = max(
                        pool,
                        key=lambda task: (task.updated_at, task.id),
                    )
            record._follow_up_context = (
                {
                    "followUpId": str(selected.id),
                    "followUpStatus": selected.status,
                }
                if selected is not None
                else {}
            )

    async def attach_outbound_context(
        self,
        records: list[AiCallRecordModel],
        *,
        tenant_id: str | None,
    ) -> None:
        call_ids = [record.call_id for record in records]
        attempt_ids = [
            int(record.business_id)
            for record in records
            if record.business_type == "outbound_attempt"
            and str(record.business_id or "").isdigit()
        ]
        if not call_ids or not tenant_id:
            return
        context_rows = (
            await self.db.execute(
                select(
                    AiCallOutboundAttemptModel.id,
                    AiCallOutboundAttemptModel.call_id,
                    AiCallOutboundAttemptModel.task_id,
                    AiCallOutboundAttemptModel.target_id,
                    AiCallOutboundAttemptModel.attempt_no,
                    AiCallOutboundAttemptModel.call_result,
                    AiCallOutboundTaskModel.task_name,
                    AiCallOutboundTargetModel.phone_number,
                    AiCallOutboundTargetModel.customer_name,
                )
                .join(
                    AiCallOutboundTargetModel,
                    and_(
                        AiCallOutboundTargetModel.tenant_id
                        == AiCallOutboundAttemptModel.tenant_id,
                        AiCallOutboundTargetModel.id
                        == AiCallOutboundAttemptModel.target_id,
                        AiCallOutboundTargetModel.task_id
                        == AiCallOutboundAttemptModel.task_id,
                    ),
                )
                .join(
                    AiCallOutboundTaskModel,
                    and_(
                        AiCallOutboundTaskModel.tenant_id
                        == AiCallOutboundAttemptModel.tenant_id,
                        AiCallOutboundTaskModel.id
                        == AiCallOutboundAttemptModel.task_id,
                    ),
                )
                .where(
                    AiCallOutboundAttemptModel.tenant_id == tenant_id,
                    or_(
                        AiCallOutboundAttemptModel.call_id.in_(call_ids),
                        AiCallOutboundAttemptModel.id.in_(attempt_ids),
                    ),
                )
            )
        ).all()
        contexts_by_call_id = {
            row.call_id: {
                "taskId": str(row.task_id),
                "targetId": str(row.target_id),
                "taskName": row.task_name,
                "phoneNumber": row.phone_number,
                "customerName": row.customer_name,
                "attemptNo": row.attempt_no,
                "callResult": row.call_result,
            }
            for row in context_rows
        }
        contexts_by_attempt_id = {
            row.id: contexts_by_call_id[row.call_id] for row in context_rows
        }
        for record in records:
            context = contexts_by_call_id.get(record.call_id)
            if (
                context is None
                and record.business_type == "outbound_attempt"
                and str(record.business_id or "").isdigit()
            ):
                context = contexts_by_attempt_id.get(int(record.business_id))
            record._outbound_context = context or {}

    async def append_event(
        self,
        *,
        event_id: str,
        call_id: str,
        event_type: str,
        source: str,
        event_time: datetime,
        payload_json: str | None,
    ) -> AiCallEventModel:
        values = {
            "id": generate_snowflake_id(),
            "event_id": event_id,
            "call_id": call_id,
            "event_type": event_type,
            "source": source,
            "event_time": event_time,
            "payload_json": payload_json,
        }
        if await self._insert_event_idempotently(values):
            event = await self.get_event_by_event_id(event_id)
            if event is not None:
                return event

        existing = await self.get_event_by_event_id(event_id)
        if existing is not None:
            return existing
        event = AiCallEventModel(**values)
        self.db.add(event)
        await self.db.flush()
        await self.db.refresh(event)
        return event

    async def get_event_by_event_id(self, event_id: str) -> AiCallEventModel | None:
        result = await self.db.execute(
            select(AiCallEventModel).where(AiCallEventModel.event_id == event_id)
        )
        return result.scalar_one_or_none()

    async def list_existing_event_ids(
        self,
        *,
        call_id: str,
        event_ids: list[str],
    ) -> set[str]:
        if not event_ids:
            return set()

        existing_ids: set[str] = set()
        for batch in self._chunks(event_ids, 500):
            result = await self.db.execute(
                select(AiCallEventModel.event_id).where(
                    AiCallEventModel.call_id == call_id,
                    AiCallEventModel.event_id.in_(batch),
                )
            )
            existing_ids.update(result.scalars().all())
        return existing_ids

    async def list_events(
        self,
        *,
        call_id: str,
        limit: int = 200,
        after_event_id: str | None = None,
        event_type: str | None = None,
        source: str | None = None,
    ) -> list[AiCallEventModel]:
        safe_limit = max(1, min(limit, 1000))
        stmt = select(AiCallEventModel).where(AiCallEventModel.call_id == call_id)
        if after_event_id:
            after_event = await self.get_event_by_event_id(after_event_id)
            if after_event is None or after_event.call_id != call_id:
                return []
            stmt = stmt.where(AiCallEventModel.id > after_event.id)
        if event_type:
            stmt = stmt.where(AiCallEventModel.event_type == event_type)
        if source:
            stmt = stmt.where(AiCallEventModel.source == source)
        result = await self.db.execute(stmt.order_by(asc(AiCallEventModel.id)).limit(safe_limit))
        return list(result.scalars().all())

    async def get_last_event(self, call_id: str) -> AiCallEventModel | None:
        result = await self.db.execute(
            select(AiCallEventModel)
            .where(AiCallEventModel.call_id == call_id)
            .order_by(desc(AiCallEventModel.id))
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def create_recording(
        self,
        *,
        tenant_id: str,
        call_id: str,
        room_name: str,
        status: str,
        started_at: datetime,
        object_name: str | None = None,
    ) -> AiCallRecordingModel:
        recording = AiCallRecordingModel(
            id=generate_snowflake_id(),
            tenant_id=tenant_id,
            call_id=call_id,
            room_name=room_name,
            status=status,
            object_name=object_name,
            started_at=started_at,
        )
        self.db.add(recording)
        await self.db.flush()
        await self.db.refresh(recording)
        return recording

    async def get_recording(
        self,
        *,
        tenant_id: str,
        call_id: str,
    ) -> AiCallRecordingModel | None:
        result = await self.db.execute(
            select(AiCallRecordingModel).where(
                AiCallRecordingModel.tenant_id == tenant_id,
                AiCallRecordingModel.call_id == call_id,
            )
        )
        return result.scalar_one_or_none()

    async def update_recording(
        self,
        *,
        tenant_id: str,
        call_id: str,
        **values,
    ) -> AiCallRecordingModel | None:
        recording = await self.get_recording(
            tenant_id=tenant_id,
            call_id=call_id,
        )
        if recording is None:
            return None
        for key, value in values.items():
            if hasattr(recording, key):
                setattr(recording, key, value)
        await self.db.flush()
        await self.db.refresh(recording)
        return recording

    async def create_recording_track(
        self,
        *,
        tenant_id: str,
        call_id: str,
        room_name: str,
        track_role: str,
        participant_identity: str,
        status: str,
        started_at: datetime,
        handoff_id: str | None = None,
        object_name: str | None = None,
    ) -> AiCallRecordingTrackModel:
        track = AiCallRecordingTrackModel(
            id=generate_snowflake_id(),
            tenant_id=tenant_id,
            call_id=call_id,
            room_name=room_name,
            track_role=track_role,
            participant_identity=participant_identity,
            handoff_id=handoff_id,
            status=status,
            object_name=object_name,
            started_at=started_at,
        )
        self.db.add(track)
        await self.db.flush()
        await self.db.refresh(track)
        return track

    async def get_recording_track(
        self,
        *,
        tenant_id: str,
        call_id: str,
        track_role: str,
        participant_identity: str,
    ) -> AiCallRecordingTrackModel | None:
        result = await self.db.execute(
            select(AiCallRecordingTrackModel).where(
                AiCallRecordingTrackModel.tenant_id == tenant_id,
                AiCallRecordingTrackModel.call_id == call_id,
                AiCallRecordingTrackModel.track_role == track_role,
                AiCallRecordingTrackModel.participant_identity == participant_identity,
            )
        )
        return result.scalar_one_or_none()

    async def list_recording_tracks(
        self,
        *,
        tenant_id: str,
        call_id: str,
    ) -> list[AiCallRecordingTrackModel]:
        result = await self.db.execute(
            select(AiCallRecordingTrackModel)
            .where(
                AiCallRecordingTrackModel.tenant_id == tenant_id,
                AiCallRecordingTrackModel.call_id == call_id,
            )
            .order_by(
                asc(AiCallRecordingTrackModel.started_at),
                asc(AiCallRecordingTrackModel.id),
            )
        )
        return list(result.scalars().all())

    async def claim_due_recording_verifications(
        self,
        *,
        now: datetime,
        limit: int,
        claim_ttl: timedelta,
    ) -> list[RecordingVerificationClaim]:
        claim_token = now + claim_ttl
        recordings = list(
            (
                await self.db.scalars(
                    select(AiCallRecordingModel)
                    .where(
                        AiCallRecordingModel.status == "verifying",
                        or_(
                            AiCallRecordingModel.next_verify_at.is_(None),
                            AiCallRecordingModel.next_verify_at <= now,
                        ),
                    )
                    .order_by(
                        asc(AiCallRecordingModel.next_verify_at),
                        asc(AiCallRecordingModel.id),
                    )
                    .limit(max(1, limit))
                    .with_for_update(skip_locked=True)
                )
            ).all()
        )
        for recording in recordings:
            recording.next_verify_at = claim_token
        await self.db.flush()
        return [
            RecordingVerificationClaim(
                recording_id=recording.id,
                tenant_id=recording.tenant_id,
                call_id=recording.call_id,
                object_name=recording.object_name,
                started_at=recording.started_at,
                ended_at=recording.ended_at,
                duration_ms=recording.duration_ms,
                verify_attempts=int(recording.verify_attempts or 0),
                verify_deadline_at=recording.verify_deadline_at,
                claim_token=claim_token,
            )
            for recording in recordings
        ]

    async def update_due_recording(
        self,
        *,
        tenant_id: str,
        recording_id: int,
        claim_token: datetime,
        **values,
    ) -> bool:
        safe_values = {
            key: value
            for key, value in values.items()
            if hasattr(AiCallRecordingModel, key)
        }
        if not safe_values:
            return False
        result = await self.db.execute(
            update(AiCallRecordingModel)
            .where(
                AiCallRecordingModel.id == recording_id,
                AiCallRecordingModel.tenant_id == tenant_id,
                AiCallRecordingModel.status == "verifying",
                AiCallRecordingModel.next_verify_at == claim_token,
            )
            .values(**safe_values)
        )
        await self.db.flush()
        return result.rowcount == 1

    async def lock_due_recording(
        self,
        *,
        tenant_id: str,
        recording_id: int,
        claim_token: datetime,
    ) -> AiCallRecordingModel | None:
        return await self.db.scalar(
            select(AiCallRecordingModel)
            .where(
                AiCallRecordingModel.id == recording_id,
                AiCallRecordingModel.tenant_id == tenant_id,
                AiCallRecordingModel.status == "verifying",
                AiCallRecordingModel.next_verify_at == claim_token,
            )
            .with_for_update()
        )

    async def claim_due_recording_track_verifications(
        self,
        *,
        now: datetime,
        limit: int,
        claim_ttl: timedelta,
        terminal_recovery_deadline: timedelta | None = None,
    ) -> list[RecordingTrackVerificationClaim]:
        claim_token = now + claim_ttl
        due = and_(
            AiCallRecordingTrackModel.status == "verifying",
            or_(
                AiCallRecordingTrackModel.next_verify_at.is_(None),
                AiCallRecordingTrackModel.next_verify_at <= now,
            ),
        )
        if terminal_recovery_deadline is not None:
            due = or_(
                due,
                and_(
                    AiCallRecordingTrackModel.status.in_(("recording", "stopping")),
                    select(AiCallRecordModel.id)
                    .where(
                        AiCallRecordModel.tenant_id
                        == AiCallRecordingTrackModel.tenant_id,
                        AiCallRecordModel.call_id == AiCallRecordingTrackModel.call_id,
                        AiCallRecordModel.status.in_(("completed", "failed")),
                    )
                    .exists(),
                ),
            )
        tracks = list(
            (
                await self.db.scalars(
                    select(AiCallRecordingTrackModel)
                    .where(due)
                    .order_by(
                        asc(AiCallRecordingTrackModel.next_verify_at),
                        asc(AiCallRecordingTrackModel.id),
                    )
                    .limit(max(1, limit))
                    .with_for_update(skip_locked=True)
                )
            ).all()
        )
        for track in tracks:
            if track.status != "verifying":
                track.status = "verifying"
                track.stop_requested_at = track.stop_requested_at or now
                track.verify_deadline_at = (
                    track.verify_deadline_at or now + terminal_recovery_deadline
                )
            track.next_verify_at = claim_token
        await self.db.flush()
        return [
            RecordingTrackVerificationClaim(
                track_id=track.id,
                tenant_id=track.tenant_id,
                call_id=track.call_id,
                track_role=track.track_role,
                participant_identity=track.participant_identity,
                object_name=track.object_name,
                started_at=track.started_at,
                ended_at=track.ended_at,
                duration_ms=track.duration_ms,
                verify_attempts=int(track.verify_attempts or 0),
                verify_deadline_at=track.verify_deadline_at,
                claim_token=claim_token,
            )
            for track in tracks
        ]

    async def update_recording_track(
        self,
        *,
        tenant_id: str,
        track_id: int,
        **values,
    ) -> AiCallRecordingTrackModel | None:
        result = await self.db.execute(
            select(AiCallRecordingTrackModel).where(
                AiCallRecordingTrackModel.tenant_id == tenant_id,
                AiCallRecordingTrackModel.id == track_id,
            )
        )
        track = result.scalar_one_or_none()
        if track is None:
            return None
        for key, value in values.items():
            if hasattr(track, key):
                setattr(track, key, value)
        await self.db.flush()
        await self.db.refresh(track)
        return track

    async def update_due_recording_track(
        self,
        *,
        tenant_id: str,
        track_id: int,
        claim_token: datetime,
        **values,
    ) -> bool:
        safe_values = {
            key: value
            for key, value in values.items()
            if hasattr(AiCallRecordingTrackModel, key)
        }
        if not safe_values:
            return False
        result = await self.db.execute(
            update(AiCallRecordingTrackModel)
            .where(
                AiCallRecordingTrackModel.tenant_id == tenant_id,
                AiCallRecordingTrackModel.id == track_id,
                AiCallRecordingTrackModel.status == "verifying",
                AiCallRecordingTrackModel.next_verify_at == claim_token,
            )
            .values(**safe_values)
        )
        await self.db.flush()
        return result.rowcount == 1

    async def lock_due_recording_track(
        self,
        *,
        tenant_id: str,
        track_id: int,
        claim_token: datetime,
    ) -> AiCallRecordingTrackModel | None:
        return await self.db.scalar(
            select(AiCallRecordingTrackModel)
            .where(
                AiCallRecordingTrackModel.tenant_id == tenant_id,
                AiCallRecordingTrackModel.id == track_id,
                AiCallRecordingTrackModel.status == "verifying",
                AiCallRecordingTrackModel.next_verify_at == claim_token,
            )
            .with_for_update()
        )

    async def get_asr_job(
        self,
        *,
        track_id: int,
        provider: str,
        model: str,
    ) -> AiCallAsrJobModel | None:
        result = await self.db.execute(
            select(AiCallAsrJobModel).where(
                AiCallAsrJobModel.track_id == track_id,
                AiCallAsrJobModel.provider == provider,
                AiCallAsrJobModel.model == model,
            )
        )
        return result.scalar_one_or_none()

    async def create_asr_job(
        self,
        *,
        call_id: str,
        track_id: int,
        track_role: str,
        participant_identity: str,
        provider: str,
        model: str,
        status: str,
        source_url: str | None = None,
        submitted_at: datetime | None = None,
    ) -> AiCallAsrJobModel:
        existing = await self.get_asr_job(
            track_id=track_id,
            provider=provider,
            model=model,
        )
        if existing is not None:
            return existing
        job = AiCallAsrJobModel(
            id=generate_snowflake_id(),
            call_id=call_id,
            track_id=track_id,
            track_role=track_role,
            participant_identity=participant_identity,
            provider=provider,
            model=model,
            status=status,
            source_url=source_url,
            submitted_at=submitted_at,
        )
        self.db.add(job)
        await self.db.flush()
        await self.db.refresh(job)
        return job

    async def update_asr_job(
        self,
        job_id: int,
        **values,
    ) -> AiCallAsrJobModel | None:
        result = await self.db.execute(
            select(AiCallAsrJobModel).where(AiCallAsrJobModel.id == job_id)
        )
        job = result.scalar_one_or_none()
        if job is None:
            return None
        for key, value in values.items():
            if hasattr(job, key):
                setattr(job, key, value)
        await self.db.flush()
        await self.db.refresh(job)
        return job

    async def list_asr_jobs(
        self,
        *,
        tenant_id: str,
        call_id: str,
    ) -> list[AiCallAsrJobModel]:
        result = await self.db.execute(
            select(AiCallAsrJobModel)
            .join(
                AiCallRecordingTrackModel,
                AiCallRecordingTrackModel.id == AiCallAsrJobModel.track_id,
            )
            .where(
                AiCallRecordingTrackModel.tenant_id == tenant_id,
                AiCallRecordingTrackModel.call_id == call_id,
                AiCallAsrJobModel.call_id == call_id,
            )
            .order_by(
                asc(AiCallAsrJobModel.submitted_at),
                asc(AiCallAsrJobModel.id),
            )
        )
        return list(result.scalars().all())

    async def list_post_call_recovery_candidates(self, *, limit: int) -> list[str]:
        pending_recording = (
            select(AiCallRecordingModel.id)
            .where(
                AiCallRecordingModel.tenant_id == AiCallRecordModel.tenant_id,
                AiCallRecordingModel.call_id == AiCallRecordModel.call_id,
                AiCallRecordingModel.status.in_(
                    ("starting", "recording", "stopping", "verifying")
                ),
            )
            .exists()
        )
        pending_track = (
            select(AiCallRecordingTrackModel.id)
            .where(
                AiCallRecordingTrackModel.tenant_id == AiCallRecordModel.tenant_id,
                AiCallRecordingTrackModel.call_id == AiCallRecordModel.call_id,
                AiCallRecordingTrackModel.status.in_(
                    ("starting", "recording", "stopping", "verifying")
                ),
            )
            .exists()
        )
        completed_track = (
            select(AiCallRecordingTrackModel.id)
            .where(
                AiCallRecordingTrackModel.tenant_id == AiCallRecordModel.tenant_id,
                AiCallRecordingTrackModel.call_id == AiCallRecordModel.call_id,
                AiCallRecordingTrackModel.status == "completed",
                AiCallRecordingTrackModel.oss_id.is_not(None),
            )
            .exists()
        )
        active_asr = (
            select(AiCallAsrJobModel.id)
            .join(
                AiCallRecordingTrackModel,
                AiCallRecordingTrackModel.id == AiCallAsrJobModel.track_id,
            )
            .where(
                AiCallRecordingTrackModel.tenant_id == AiCallRecordModel.tenant_id,
                AiCallRecordingTrackModel.call_id == AiCallRecordModel.call_id,
                AiCallAsrJobModel.status.in_(("pending", "running")),
            )
            .exists()
        )
        missing_analysis = ~(
            select(AiCallSemanticAnalysisModel.id)
            .where(AiCallSemanticAnalysisModel.call_id == AiCallRecordModel.call_id)
            .exists()
        )
        result = await self.db.execute(
            select(AiCallRecordModel.call_id)
            .where(
                AiCallRecordModel.status.in_(("completed", "failed")),
                ~pending_recording,
                ~pending_track,
                completed_track,
                missing_analysis,
                ~active_asr,
            )
            .order_by(asc(AiCallRecordModel.ended_at), asc(AiCallRecordModel.id))
            .limit(max(1, limit))
        )
        return list(result.scalars().all())

    async def next_dialogue_segment_no(self, call_id: str) -> int:
        result = await self.db.execute(
            select(func.max(AiCallDialogueSegmentModel.segment_no)).where(
                AiCallDialogueSegmentModel.call_id == call_id
            )
        )
        max_segment_no = result.scalar_one_or_none()
        return int(max_segment_no or 0) + 1

    async def upsert_dialogue_segment(
        self,
        *,
        call_id: str,
        segment_no: int,
        speaker_type: str,
        speaker_identity: str | None,
        source: str,
        source_segment_id: str,
        segment_text: str,
        segment_status: str,
        started_at: datetime | None,
        ended_at: datetime | None,
        duration_ms: int | None,
        audio_start_ms: int | None = None,
        audio_end_ms: int | None = None,
        failure_stage: str | None = None,
        failure_message: str | None = None,
    ) -> AiCallDialogueSegmentModel:
        existing = await self.get_dialogue_segment_by_source(
            call_id=call_id,
            speaker_type=speaker_type,
            source=source,
            source_segment_id=source_segment_id,
        )
        if existing is None:
            existing = await self.get_dialogue_segment(
                call_id=call_id,
                segment_no=segment_no,
            )
        values = {
            "speaker_type": speaker_type,
            "speaker_identity": speaker_identity,
            "source": source,
            "source_segment_id": source_segment_id,
            "segment_text": segment_text,
            "segment_status": segment_status,
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_ms": duration_ms,
            "audio_start_ms": audio_start_ms,
            "audio_end_ms": audio_end_ms,
            "failure_stage": failure_stage,
            "failure_message": failure_message,
        }
        if existing is not None:
            for key, value in values.items():
                setattr(existing, key, value)
            await self.db.flush()
            await self.db.refresh(existing)
            return existing

        segment = AiCallDialogueSegmentModel(
            id=generate_snowflake_id(),
            call_id=call_id,
            segment_no=segment_no,
            **values,
        )
        self.db.add(segment)
        await self.db.flush()
        await self.db.refresh(segment)
        return segment

    async def get_dialogue_segment(
        self,
        *,
        call_id: str,
        segment_no: int,
    ) -> AiCallDialogueSegmentModel | None:
        result = await self.db.execute(
            select(AiCallDialogueSegmentModel).where(
                AiCallDialogueSegmentModel.call_id == call_id,
                AiCallDialogueSegmentModel.segment_no == segment_no,
            )
        )
        return result.scalar_one_or_none()

    async def get_dialogue_segment_by_source(
        self,
        *,
        call_id: str,
        speaker_type: str,
        source: str,
        source_segment_id: str,
    ) -> AiCallDialogueSegmentModel | None:
        result = await self.db.execute(
            select(AiCallDialogueSegmentModel).where(
                AiCallDialogueSegmentModel.call_id == call_id,
                AiCallDialogueSegmentModel.speaker_type == speaker_type,
                AiCallDialogueSegmentModel.source == source,
                AiCallDialogueSegmentModel.source_segment_id == source_segment_id,
            )
        )
        return result.scalar_one_or_none()

    async def list_dialogue_segments(
        self,
        call_id: str,
        *,
        speaker_type: str | None = None,
        limit: int = 1000,
    ) -> list[AiCallDialogueSegmentModel]:
        safe_limit = max(1, min(limit, 5000))
        stmt = select(AiCallDialogueSegmentModel).where(
            AiCallDialogueSegmentModel.call_id == call_id
        )
        if speaker_type:
            stmt = stmt.where(AiCallDialogueSegmentModel.speaker_type == speaker_type)
        result = await self.db.execute(
            stmt.order_by(asc(AiCallDialogueSegmentModel.segment_no)).limit(safe_limit)
        )
        return list(result.scalars().all())

    async def ensure_semantic_analysis_record(
        self,
        *,
        call_id: str,
        scene_code: str | None,
        analysis_scene_code: str = DEFAULT_SEMANTIC_ANALYSIS_SCENE_CODE,
    ) -> AiCallSemanticAnalysisModel:
        existing = await self.get_semantic_analysis(
            call_id=call_id,
            analysis_scene_code=analysis_scene_code,
        )
        if existing is not None:
            if scene_code and existing.scene_code != scene_code:
                existing.scene_code = scene_code
                existing.updated_at = datetime.now(timezone.utc)
                await self.db.flush()
                await self.db.refresh(existing)
            return existing

        now = datetime.now(timezone.utc)
        analysis = AiCallSemanticAnalysisModel(
            id=generate_snowflake_id(),
            call_id=call_id,
            scene_code=scene_code,
            analysis_scene_code=analysis_scene_code,
            analysis_status=SEMANTIC_ANALYSIS_STATUS_PENDING,
            analysis_retry_count=0,
            created_at=now,
            updated_at=now,
        )
        self.db.add(analysis)
        await self.db.flush()
        await self.db.refresh(analysis)
        return analysis

    async def get_semantic_analysis(
        self,
        *,
        call_id: str,
        analysis_scene_code: str = DEFAULT_SEMANTIC_ANALYSIS_SCENE_CODE,
    ) -> AiCallSemanticAnalysisModel | None:
        result = await self.db.execute(
            select(AiCallSemanticAnalysisModel).where(
                AiCallSemanticAnalysisModel.call_id == call_id,
                AiCallSemanticAnalysisModel.analysis_scene_code == analysis_scene_code,
            )
        )
        return result.scalar_one_or_none()

    async def get_semantic_analysis_for_update(
        self,
        *,
        call_id: str,
        analysis_scene_code: str = DEFAULT_SEMANTIC_ANALYSIS_SCENE_CODE,
    ) -> AiCallSemanticAnalysisModel | None:
        result = await self.db.execute(
            select(AiCallSemanticAnalysisModel)
            .where(
                AiCallSemanticAnalysisModel.call_id == call_id,
                AiCallSemanticAnalysisModel.analysis_scene_code
                == analysis_scene_code,
            )
            .with_for_update()
        )
        return result.scalar_one_or_none()

    async def claim_semantic_analysis(
        self,
        *,
        call_id: str,
        analysis_scene_code: str = DEFAULT_SEMANTIC_ANALYSIS_SCENE_CODE,
        now: datetime | None = None,
        max_retry_count: int = 3,
        retry_cooldown_minutes: int = 10,
        force: bool = False,
    ) -> AiCallSemanticAnalysisModel | None:
        analysis = await self.get_semantic_analysis(
            call_id=call_id,
            analysis_scene_code=analysis_scene_code,
        )
        if analysis is None:
            return None
        now = now or datetime.now(timezone.utc)
        if not force and not self._semantic_analysis_claimable(
            analysis,
            now=now,
            max_retry_count=max_retry_count,
            retry_cooldown_minutes=retry_cooldown_minutes,
        ):
            return None

        analysis.analysis_status = SEMANTIC_ANALYSIS_STATUS_RUNNING
        analysis.analysis_started_at = now
        analysis.analysis_finished_at = None
        analysis.analysis_error = None
        self._clear_semantic_post_call_fields(analysis)
        analysis.updated_at = now
        await self.db.flush()
        await self.db.refresh(analysis)
        return analysis

    async def update_semantic_analysis_success(
        self,
        *,
        call_id: str,
        analysis_result: dict,
        transcript_snapshot_json: str,
        transcript_hash: str,
        customer_intent: str | None = None,
        follow_up_suggested: bool = False,
        follow_up_consent: str | None = None,
        follow_up_reason: str | None = None,
        follow_up_preferred_at: datetime | None = None,
        follow_up_confidence: str | None = None,
        analysis_scene_code: str = DEFAULT_SEMANTIC_ANALYSIS_SCENE_CODE,
        now: datetime | None = None,
    ) -> AiCallSemanticAnalysisModel | None:
        analysis = await self.get_semantic_analysis(
            call_id=call_id,
            analysis_scene_code=analysis_scene_code,
        )
        if analysis is None:
            return None
        now = now or datetime.now(timezone.utc)
        analysis.analysis_status = SEMANTIC_ANALYSIS_STATUS_SUCCEEDED
        analysis.analysis_version = int(analysis.analysis_version or 0) + 1
        analysis.analysis_result = json.dumps(analysis_result, ensure_ascii=False)
        analysis.customer_intent = customer_intent
        analysis.follow_up_suggested = follow_up_suggested
        analysis.follow_up_consent = follow_up_consent
        analysis.follow_up_reason = follow_up_reason
        analysis.follow_up_preferred_at = follow_up_preferred_at
        analysis.follow_up_confidence = follow_up_confidence
        analysis.analysis_error = None
        analysis.analysis_finished_at = now
        analysis.transcript_snapshot_json = transcript_snapshot_json
        analysis.transcript_hash = transcript_hash
        analysis.updated_at = now
        await self.db.flush()
        await self.db.refresh(analysis)
        return analysis

    async def update_semantic_analysis_failed(
        self,
        *,
        call_id: str,
        analysis_error: str,
        transcript_snapshot_json: str | None = None,
        transcript_hash: str | None = None,
        analysis_scene_code: str = DEFAULT_SEMANTIC_ANALYSIS_SCENE_CODE,
        now: datetime | None = None,
    ) -> AiCallSemanticAnalysisModel | None:
        analysis = await self.get_semantic_analysis(
            call_id=call_id,
            analysis_scene_code=analysis_scene_code,
        )
        if analysis is None:
            return None
        now = now or datetime.now(timezone.utc)
        analysis.analysis_status = SEMANTIC_ANALYSIS_STATUS_FAILED
        analysis.analysis_error = analysis_error
        self._clear_semantic_post_call_fields(analysis)
        analysis.analysis_retry_count = int(analysis.analysis_retry_count or 0) + 1
        analysis.analysis_finished_at = now
        if transcript_snapshot_json is not None:
            analysis.transcript_snapshot_json = transcript_snapshot_json
        if transcript_hash is not None:
            analysis.transcript_hash = transcript_hash
        analysis.updated_at = now
        await self.db.flush()
        await self.db.refresh(analysis)
        return analysis

    async def update_semantic_analysis_no_user_input(
        self,
        *,
        call_id: str,
        analysis_error: str,
        transcript_snapshot_json: str,
        transcript_hash: str,
        analysis_scene_code: str = DEFAULT_SEMANTIC_ANALYSIS_SCENE_CODE,
        now: datetime | None = None,
    ) -> AiCallSemanticAnalysisModel | None:
        analysis = await self.get_semantic_analysis(
            call_id=call_id,
            analysis_scene_code=analysis_scene_code,
        )
        if analysis is None:
            return None
        now = now or datetime.now(timezone.utc)
        analysis.analysis_status = SEMANTIC_ANALYSIS_STATUS_NO_USER_INPUT
        analysis.analysis_error = analysis_error
        self._clear_semantic_post_call_fields(analysis)
        analysis.analysis_finished_at = now
        analysis.transcript_snapshot_json = transcript_snapshot_json
        analysis.transcript_hash = transcript_hash
        analysis.updated_at = now
        await self.db.flush()
        await self.db.refresh(analysis)
        return analysis

    async def ensure_quality_score(
        self,
        *,
        tenant_id: str,
        call_id: str,
        model_version: str = DEFAULT_QUALITY_SCORE_MODEL_VERSION,
    ) -> AiCallQualityScoreModel:
        now = datetime.now(timezone.utc)
        values = {
            "id": generate_snowflake_id(),
            "tenant_id": tenant_id,
            "call_id": call_id,
            "status": QUALITY_SCORE_STATUS_PENDING,
            "model_version": model_version,
            "retry_count": 0,
            "created_at": now,
            "updated_at": now,
        }
        table = AiCallQualityScoreModel.__table__
        dialect_name = self._dialect_name()
        if dialect_name == "postgresql":
            stmt = postgresql_insert(table).values(**values).on_conflict_do_nothing(
                index_elements=[table.c.tenant_id, table.c.call_id, table.c.model_version]
            )
            await self.db.execute(stmt)
        elif dialect_name == "sqlite":
            stmt = sqlite_insert(table).values(**values).on_conflict_do_nothing(
                index_elements=[table.c.tenant_id, table.c.call_id, table.c.model_version]
            )
            await self.db.execute(stmt)
        elif dialect_name == "mysql":
            insert_stmt = mysql_insert(table).values(**values)
            await self.db.execute(
                insert_stmt.on_duplicate_key_update(call_id=insert_stmt.inserted.call_id)
            )
        else:
            existing = await self.get_quality_score(
                tenant_id=tenant_id,
                call_id=call_id,
                model_version=model_version,
            )
            if existing is not None:
                return existing
            self.db.add(AiCallQualityScoreModel(**values))
        await self.db.flush()
        score = await self.get_quality_score(
            tenant_id=tenant_id,
            call_id=call_id,
            model_version=model_version,
        )
        assert score is not None
        return score

    async def get_quality_score(
        self,
        *,
        tenant_id: str,
        call_id: str,
        model_version: str = DEFAULT_QUALITY_SCORE_MODEL_VERSION,
    ) -> AiCallQualityScoreModel | None:
        result = await self.db.execute(
            select(AiCallQualityScoreModel).where(
                AiCallQualityScoreModel.tenant_id == tenant_id,
                AiCallQualityScoreModel.call_id == call_id,
                AiCallQualityScoreModel.model_version == model_version,
            )
        )
        return result.scalar_one_or_none()

    async def claim_quality_score(
        self,
        *,
        tenant_id: str,
        call_id: str,
        model_version: str = DEFAULT_QUALITY_SCORE_MODEL_VERSION,
        now: datetime | None = None,
    ) -> AiCallQualityScoreModel | None:
        now = now or datetime.now(timezone.utc)
        retry_cutoff = now - timedelta(minutes=QUALITY_SCORE_RETRY_COOLDOWN_MINUTES)
        stale_processing_cutoff = now - timedelta(
            minutes=QUALITY_SCORE_RETRY_COOLDOWN_MINUTES
        )
        result = await self.db.execute(
            update(AiCallQualityScoreModel)
            .where(
                AiCallQualityScoreModel.tenant_id == tenant_id,
                AiCallQualityScoreModel.call_id == call_id,
                AiCallQualityScoreModel.model_version == model_version,
                or_(
                    AiCallQualityScoreModel.status == QUALITY_SCORE_STATUS_PENDING,
                    and_(
                        AiCallQualityScoreModel.status == QUALITY_SCORE_STATUS_FAILED,
                        AiCallQualityScoreModel.retry_count < QUALITY_SCORE_MAX_RETRY_COUNT,
                        AiCallQualityScoreModel.updated_at <= retry_cutoff,
                    ),
                    and_(
                        AiCallQualityScoreModel.status == QUALITY_SCORE_STATUS_RUNNING,
                        AiCallQualityScoreModel.started_at <= stale_processing_cutoff,
                    ),
                ),
            )
            .values(
                status=QUALITY_SCORE_STATUS_RUNNING,
                started_at=now,
                finished_at=None,
                error_message=None,
                updated_at=now,
            )
            .execution_options(synchronize_session=False)
        )
        if result.rowcount != 1:
            return None
        return await self.get_quality_score(
            tenant_id=tenant_id,
            call_id=call_id,
            model_version=model_version,
        )

    async def list_recoverable_quality_score_call_ids(
        self,
        *,
        limit: int,
        now: datetime | None = None,
    ) -> list[str]:
        now = now or datetime.now(timezone.utc)
        retry_cutoff = now - timedelta(minutes=QUALITY_SCORE_RETRY_COOLDOWN_MINUTES)
        stale_processing_cutoff = now - timedelta(
            minutes=QUALITY_SCORE_RETRY_COOLDOWN_MINUTES
        )
        result = await self.db.execute(
            select(AiCallQualityScoreModel.call_id)
            .where(
                or_(
                    AiCallQualityScoreModel.status == QUALITY_SCORE_STATUS_PENDING,
                    and_(
                        AiCallQualityScoreModel.status == QUALITY_SCORE_STATUS_FAILED,
                        AiCallQualityScoreModel.retry_count < QUALITY_SCORE_MAX_RETRY_COUNT,
                        AiCallQualityScoreModel.updated_at <= retry_cutoff,
                    ),
                    and_(
                        AiCallQualityScoreModel.status == QUALITY_SCORE_STATUS_RUNNING,
                        AiCallQualityScoreModel.started_at <= stale_processing_cutoff,
                    ),
                )
            )
            .order_by(AiCallQualityScoreModel.updated_at)
            .limit(max(1, limit))
        )
        return list(result.scalars().all())

    async def update_quality_score_success(
        self,
        *,
        tenant_id: str,
        call_id: str,
        score: int,
        reason: str,
        model_version: str = DEFAULT_QUALITY_SCORE_MODEL_VERSION,
        now: datetime | None = None,
    ) -> AiCallQualityScoreModel:
        row = await self.get_quality_score(
            tenant_id=tenant_id,
            call_id=call_id,
            model_version=model_version,
        )
        if row is None:
            row = await self.ensure_quality_score(
                tenant_id=tenant_id,
                call_id=call_id,
                model_version=model_version,
            )
        now = now or datetime.now(timezone.utc)
        row.status = QUALITY_SCORE_STATUS_COMPLETED
        row.score = max(0, min(100, int(score)))
        row.reason = reason
        row.error_message = None
        row.finished_at = now
        row.updated_at = now
        await self.db.flush()
        await self.db.refresh(row)
        return row

    async def update_quality_score_failed(
        self,
        *,
        tenant_id: str,
        call_id: str,
        error_message: str,
        model_version: str = DEFAULT_QUALITY_SCORE_MODEL_VERSION,
        now: datetime | None = None,
    ) -> AiCallQualityScoreModel:
        row = await self.get_quality_score(
            tenant_id=tenant_id,
            call_id=call_id,
            model_version=model_version,
        )
        if row is None:
            row = await self.ensure_quality_score(
                tenant_id=tenant_id,
                call_id=call_id,
                model_version=model_version,
            )
        now = now or datetime.now(timezone.utc)
        row.status = QUALITY_SCORE_STATUS_FAILED
        row.retry_count = int(row.retry_count or 0) + 1
        row.error_message = error_message[:500]
        row.finished_at = now
        row.updated_at = now
        await self.db.flush()
        await self.db.refresh(row)
        return row

    async def get_quality_review(
        self,
        *,
        tenant_id: str,
        call_id: str,
    ) -> AiCallQualityReviewModel | None:
        result = await self.db.execute(
            select(AiCallQualityReviewModel).where(
                AiCallQualityReviewModel.tenant_id == tenant_id,
                AiCallQualityReviewModel.call_id == call_id,
            )
        )
        return result.scalar_one_or_none()

    async def upsert_quality_review(
        self,
        *,
        tenant_id: str,
        call_id: str,
        quality_result: str,
        quality_reason: str | None,
        reviewed_by: str,
        reviewed_by_name: str | None,
    ) -> AiCallQualityReviewModel:
        if quality_result not in QUALITY_REVIEW_RESULTS:
            raise ValueError("质检结果无效")
        reason = (quality_reason or "").strip()
        if quality_result == "fail" and not reason:
            raise ValueError("不合格原因不能为空")
        now = datetime.now(timezone.utc)
        review = await self.get_quality_review(tenant_id=tenant_id, call_id=call_id)
        if review is None:
            review = AiCallQualityReviewModel(
                id=generate_snowflake_id(),
                tenant_id=tenant_id,
                call_id=call_id,
                quality_result=quality_result,
                quality_reason=reason or None,
                reviewed_by=reviewed_by,
                reviewed_by_name=reviewed_by_name,
                reviewed_at=now,
                created_at=now,
                updated_at=now,
            )
            self.db.add(review)
        else:
            review.quality_result = quality_result
            review.quality_reason = reason or None
            review.reviewed_by = reviewed_by
            review.reviewed_by_name = reviewed_by_name
            review.reviewed_at = now
            review.updated_at = now
        await self.db.flush()
        await self.db.refresh(review)
        return review

    @staticmethod
    def _clear_semantic_post_call_fields(
        analysis: AiCallSemanticAnalysisModel,
    ) -> None:
        analysis.customer_intent = None
        analysis.follow_up_suggested = False
        analysis.follow_up_consent = None
        analysis.follow_up_reason = None
        analysis.follow_up_preferred_at = None
        analysis.follow_up_confidence = None

    async def create_prompt_profile(self, **values) -> AiCallPromptProfileModel:
        now = datetime.now(timezone.utc)
        profile = AiCallPromptProfileModel(
            id=generate_snowflake_id(),
            created_at=now,
            updated_at=now,
            **values,
        )
        self.db.add(profile)
        await self.db.flush()
        await self.db.refresh(profile)
        return profile

    async def get_prompt_common_config(
        self,
        *,
        tenant_id: str,
    ) -> AiCallPromptCommonConfigModel | None:
        return await self.db.scalar(
            select(AiCallPromptCommonConfigModel).where(
                AiCallPromptCommonConfigModel.tenant_id == tenant_id
            )
        )

    async def save_prompt_common_config(
        self,
        *,
        tenant_id: str,
        content: str,
    ) -> AiCallPromptCommonConfigModel:
        config = await self.get_prompt_common_config(tenant_id=tenant_id)
        now = datetime.now(timezone.utc)
        if config is None:
            config = AiCallPromptCommonConfigModel(
                id=generate_snowflake_id(),
                tenant_id=tenant_id,
                content=content,
                updated_at=now,
            )
            self.db.add(config)
        else:
            config.content = content
            config.updated_at = now
        await self.db.flush()
        await self.db.refresh(config)
        return config

    async def get_prompt_profile(
        self,
        profile_id: int,
        *,
        tenant_id: str,
        for_update: bool = False,
    ) -> AiCallPromptProfileModel | None:
        stmt = select(AiCallPromptProfileModel).where(
            AiCallPromptProfileModel.id == profile_id,
            AiCallPromptProfileModel.tenant_id == tenant_id,
        )
        if for_update:
            stmt = stmt.with_for_update()
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()

    async def get_prompt_profile_by_scene(
        self,
        *,
        tenant_id: str,
        scene_code: str,
    ) -> AiCallPromptProfileModel | None:
        stmt = select(AiCallPromptProfileModel).where(
            AiCallPromptProfileModel.tenant_id == tenant_id,
            AiCallPromptProfileModel.scene_code == scene_code,
        )
        result = await self.db.execute(stmt.limit(1))
        return result.scalar_one_or_none()

    async def list_prompt_profiles(
        self,
        *,
        tenant_id: str,
        scene_code: str | None = None,
        page_num: int = 1,
        page_size: int = 20,
    ) -> tuple[list[AiCallPromptProfileModel], int]:
        stmt = self._prompt_profile_filters(
            select(AiCallPromptProfileModel),
            tenant_id=tenant_id,
            scene_code=scene_code,
        )
        count_stmt = self._prompt_profile_filters(
            select(func.count()).select_from(AiCallPromptProfileModel),
            tenant_id=tenant_id,
            scene_code=scene_code,
        )
        total = int((await self.db.execute(count_stmt)).scalar_one())
        safe_page_num = max(1, page_num)
        safe_page_size = max(1, min(page_size, 1000))
        result = await self.db.execute(
            stmt
            .order_by(desc(AiCallPromptProfileModel.updated_at), desc(AiCallPromptProfileModel.id))
            .offset((safe_page_num - 1) * safe_page_size)
            .limit(safe_page_size)
        )
        return list(result.scalars().all()), total

    async def update_prompt_profile(
        self,
        profile_id: int,
        *,
        tenant_id: str,
        **values,
    ) -> AiCallPromptProfileModel | None:
        profile = await self.get_prompt_profile(
            profile_id,
            tenant_id=tenant_id,
            for_update=True,
        )
        if profile is None:
            return None
        for key, value in values.items():
            if hasattr(profile, key):
                setattr(profile, key, value)
        profile.updated_at = datetime.now(timezone.utc)
        await self.db.flush()
        await self.db.refresh(profile)
        return profile

    async def create_prompt_profile_version(
        self,
        *,
        tenant_id: str,
        profile_id: int,
        version_name: str,
        snapshot_json: str,
        creation_method: str,
        created_by: int | None,
        created_by_name: str | None,
        restored_from_version_id: int | None = None,
    ) -> AiCallPromptProfileVersionModel:
        version_no = int(
            await self.db.scalar(
                select(func.max(AiCallPromptProfileVersionModel.version_no)).where(
                    AiCallPromptProfileVersionModel.tenant_id == tenant_id,
                    AiCallPromptProfileVersionModel.profile_id == profile_id,
                )
            )
            or 0
        ) + 1
        version = AiCallPromptProfileVersionModel(
            id=generate_snowflake_id(),
            tenant_id=tenant_id,
            profile_id=profile_id,
            version_no=version_no,
            version_name=version_name,
            snapshot_json=snapshot_json,
            creation_method=creation_method,
            restored_from_version_id=restored_from_version_id,
            created_by=created_by,
            created_by_name=created_by_name,
            created_at=datetime.now(timezone.utc),
            deleted_at=None,
        )
        self.db.add(version)
        await self.db.flush()
        await self.db.refresh(version)
        return version

    async def update_prompt_profile_version_name(
        self,
        *,
        tenant_id: str,
        profile_id: int,
        version_id: int,
        version_name: str,
    ) -> AiCallPromptProfileVersionModel | None:
        version = await self.get_prompt_profile_version(
            tenant_id=tenant_id,
            profile_id=profile_id,
            version_id=version_id,
        )
        if version is None:
            return None
        version.version_name = version_name
        await self.db.flush()
        await self.db.refresh(version)
        return version

    async def create_prompt_profile_version_application(
        self,
        *,
        tenant_id: str,
        profile_id: int,
        from_version_id: int | None,
        to_version_id: int,
        applied_by: int | None,
        applied_by_name: str | None,
    ) -> AiCallPromptProfileVersionApplicationModel:
        application = AiCallPromptProfileVersionApplicationModel(
            id=generate_snowflake_id(),
            tenant_id=tenant_id,
            profile_id=profile_id,
            from_version_id=from_version_id,
            to_version_id=to_version_id,
            applied_by=applied_by,
            applied_by_name=applied_by_name,
            applied_at=datetime.now(timezone.utc),
        )
        self.db.add(application)
        await self.db.flush()
        return application

    async def list_prompt_profile_version_applications(
        self,
        *,
        tenant_id: str,
        profile_id: int,
    ) -> list[tuple]:
        from_version = aliased(AiCallPromptProfileVersionModel)
        to_version = aliased(AiCallPromptProfileVersionModel)
        result = await self.db.execute(
            select(
                AiCallPromptProfileVersionApplicationModel,
                from_version.version_no,
                from_version.version_name,
                to_version.version_no,
                to_version.version_name,
            )
            .outerjoin(
                from_version,
                and_(
                    from_version.id
                    == AiCallPromptProfileVersionApplicationModel.from_version_id,
                    from_version.tenant_id
                    == AiCallPromptProfileVersionApplicationModel.tenant_id,
                    from_version.profile_id
                    == AiCallPromptProfileVersionApplicationModel.profile_id,
                ),
            )
            .join(
                to_version,
                and_(
                    to_version.id
                    == AiCallPromptProfileVersionApplicationModel.to_version_id,
                    to_version.tenant_id
                    == AiCallPromptProfileVersionApplicationModel.tenant_id,
                    to_version.profile_id
                    == AiCallPromptProfileVersionApplicationModel.profile_id,
                ),
            )
            .where(
                AiCallPromptProfileVersionApplicationModel.tenant_id == tenant_id,
                AiCallPromptProfileVersionApplicationModel.profile_id == profile_id,
            )
            .order_by(
                AiCallPromptProfileVersionApplicationModel.applied_at.desc(),
                AiCallPromptProfileVersionApplicationModel.id.desc(),
            )
        )
        return list(result.tuples().all())

    async def list_prompt_profile_versions(
        self,
        *,
        tenant_id: str,
        profile_id: int,
    ) -> list[AiCallPromptProfileVersionModel]:
        result = await self.db.execute(
            select(AiCallPromptProfileVersionModel)
            .where(
                AiCallPromptProfileVersionModel.tenant_id == tenant_id,
                AiCallPromptProfileVersionModel.profile_id == profile_id,
                AiCallPromptProfileVersionModel.deleted_at.is_(None),
            )
            .order_by(AiCallPromptProfileVersionModel.version_no.desc())
        )
        return list(result.scalars().all())

    async def get_prompt_profile_version_summaries(
        self,
        *,
        tenant_id: str,
        profile_ids: list[int],
    ) -> dict[int, tuple[int, int]]:
        if not profile_ids:
            return {}
        version = aliased(AiCallPromptProfileVersionModel)
        current_version = aliased(AiCallPromptProfileVersionModel)
        rows = (
            await self.db.execute(
                select(
                    AiCallPromptProfileModel.id,
                    func.coalesce(
                        current_version.version_no,
                        func.max(version.version_no),
                    ),
                    func.count(version.id),
                )
                .join(
                    version,
                    and_(
                        version.tenant_id == AiCallPromptProfileModel.tenant_id,
                        version.profile_id == AiCallPromptProfileModel.id,
                        version.deleted_at.is_(None),
                    ),
                )
                .outerjoin(
                    current_version,
                    and_(
                        current_version.id == AiCallPromptProfileModel.current_version_id,
                        current_version.tenant_id == AiCallPromptProfileModel.tenant_id,
                        current_version.profile_id == AiCallPromptProfileModel.id,
                        current_version.deleted_at.is_(None),
                    ),
                )
                .where(
                    AiCallPromptProfileModel.tenant_id == tenant_id,
                    AiCallPromptProfileModel.id.in_(profile_ids),
                )
                .group_by(AiCallPromptProfileModel.id, current_version.version_no)
            )
        ).all()
        return {
            int(profile_id): (int(version_no), int(version_count))
            for profile_id, version_no, version_count in rows
        }

    async def get_prompt_profile_version(
        self,
        *,
        tenant_id: str,
        profile_id: int,
        version_id: int,
        include_deleted: bool = False,
    ) -> AiCallPromptProfileVersionModel | None:
        stmt = select(AiCallPromptProfileVersionModel).where(
            AiCallPromptProfileVersionModel.id == version_id,
            AiCallPromptProfileVersionModel.tenant_id == tenant_id,
            AiCallPromptProfileVersionModel.profile_id == profile_id,
        )
        if not include_deleted:
            stmt = stmt.where(AiCallPromptProfileVersionModel.deleted_at.is_(None))
        return await self.db.scalar(stmt)

    async def get_latest_prompt_profile_version(
        self,
        *,
        tenant_id: str,
        profile_id: int,
    ) -> AiCallPromptProfileVersionModel | None:
        return await self.db.scalar(
            select(AiCallPromptProfileVersionModel)
            .where(
                AiCallPromptProfileVersionModel.tenant_id == tenant_id,
                AiCallPromptProfileVersionModel.profile_id == profile_id,
                AiCallPromptProfileVersionModel.deleted_at.is_(None),
            )
            .order_by(AiCallPromptProfileVersionModel.version_no.desc())
            .limit(1)
        )

    async def soft_delete_prompt_profile_version(
        self,
        *,
        tenant_id: str,
        profile_id: int,
        version_id: int,
    ) -> AiCallPromptProfileVersionModel | None:
        version = await self.get_prompt_profile_version(
            tenant_id=tenant_id,
            profile_id=profile_id,
            version_id=version_id,
        )
        if version is None:
            return None
        version.deleted_at = datetime.now(timezone.utc)
        await self.db.flush()
        await self.db.refresh(version)
        return version

    async def ensure_builtin_voice_profiles(
        self,
        *,
        target_model: str,
        profiles: list[dict[str, object]],
    ) -> None:
        existing_count = int(
            (
                await self.db.execute(
                    select(func.count())
                    .select_from(AiCallVoiceProfileModel)
                    .where(
                        AiCallVoiceProfileModel.target_model == target_model,
                        AiCallVoiceProfileModel.voice_type == "内置",
                    )
                )
            ).scalar_one()
        )
        if existing_count >= len(profiles):
            return

        now = datetime.now(timezone.utc)
        for profile in profiles:
            values = {
                "id": generate_snowflake_id(),
                "created_at": now,
                "updated_at": now,
                **profile,
            }
            if not await self._insert_voice_profile_idempotently(values):
                exists = await self.get_voice_profile_by_voice(
                    voice=str(profile["voice"]),
                    target_model=target_model,
                )
                if exists is None:
                    self.db.add(AiCallVoiceProfileModel(**values))
        await self.db.flush()

    async def create_voice_profile(self, **values) -> AiCallVoiceProfileModel:
        now = datetime.now(timezone.utc)
        profile = AiCallVoiceProfileModel(
            id=generate_snowflake_id(),
            created_at=now,
            updated_at=now,
            **values,
        )
        self.db.add(profile)
        await self.db.flush()
        await self.db.refresh(profile)
        return profile

    async def get_voice_profile_by_voice(
        self,
        *,
        voice: str,
        target_model: str,
    ) -> AiCallVoiceProfileModel | None:
        result = await self.db.execute(
            select(AiCallVoiceProfileModel).where(
                AiCallVoiceProfileModel.voice == voice,
                AiCallVoiceProfileModel.target_model == target_model,
            )
        )
        return result.scalar_one_or_none()

    async def list_voice_profiles(
        self,
        *,
        target_model: str,
        voice_type: str | None = None,
        gender: str | None = None,
        page_num: int = 1,
        page_size: int = 200,
    ) -> tuple[list[AiCallVoiceProfileModel], int]:
        stmt = self._voice_profile_filters(
            select(AiCallVoiceProfileModel),
            target_model=target_model,
            voice_type=voice_type,
            gender=gender,
        )
        count_stmt = self._voice_profile_filters(
            select(func.count()).select_from(AiCallVoiceProfileModel),
            target_model=target_model,
            voice_type=voice_type,
            gender=gender,
        )
        total = int((await self.db.execute(count_stmt)).scalar_one())
        safe_page_num = max(1, page_num)
        safe_page_size = max(1, min(page_size, 1000))
        result = await self.db.execute(
            stmt
            .order_by(
                asc(AiCallVoiceProfileModel.sort_order),
                asc(AiCallVoiceProfileModel.id),
            )
            .offset((safe_page_num - 1) * safe_page_size)
            .limit(safe_page_size)
        )
        return list(result.scalars().all()), total

    async def create_handoff(
        self,
        *,
        handoff_id: str,
        call_id: str,
        room_name: str,
        status: str,
        request_source: str,
        request_reason: str | None,
        request_message: str | None,
        requested_at: datetime,
        expires_at: datetime | None,
        scene_code: str = "default",
    ) -> AiCallHandoffModel:
        handoff = AiCallHandoffModel(
            id=generate_snowflake_id(),
            handoff_id=handoff_id,
            call_id=call_id,
            room_name=room_name,
            scene_code=scene_code,
            status=status,
            request_source=request_source,
            request_reason=request_reason,
            request_message=request_message,
            requested_at=requested_at,
            expires_at=expires_at,
        )
        self.db.add(handoff)
        await self.db.flush()
        await self.db.refresh(handoff)
        return handoff

    async def get_handoff_by_id(self, handoff_id: str) -> AiCallHandoffModel | None:
        result = await self.db.execute(
            select(AiCallHandoffModel).where(AiCallHandoffModel.handoff_id == handoff_id)
        )
        return result.scalar_one_or_none()

    async def get_active_handoff(
        self,
        call_id: str,
        *,
        terminal_statuses: set[str],
    ) -> AiCallHandoffModel | None:
        result = await self.db.execute(
            select(AiCallHandoffModel)
            .where(
                AiCallHandoffModel.call_id == call_id,
                AiCallHandoffModel.status.notin_(terminal_statuses),
            )
            .order_by(desc(AiCallHandoffModel.requested_at), desc(AiCallHandoffModel.id))
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def list_handoffs(self, call_id: str) -> list[AiCallHandoffModel]:
        result = await self.db.execute(
            select(AiCallHandoffModel)
            .where(AiCallHandoffModel.call_id == call_id)
            .order_by(desc(AiCallHandoffModel.requested_at), desc(AiCallHandoffModel.id))
        )
        return list(result.scalars().all())

    async def list_joinable_handoffs(
        self,
        *,
        statuses: set[str],
        now: datetime,
        limit: int,
    ) -> list[AiCallHandoffModel]:
        result = await self.db.execute(
            select(AiCallHandoffModel)
            .where(
                AiCallHandoffModel.status.in_(statuses),
                (AiCallHandoffModel.expires_at.is_(None) | (AiCallHandoffModel.expires_at > now)),
            )
            .order_by(asc(AiCallHandoffModel.requested_at), asc(AiCallHandoffModel.id))
            .limit(max(1, min(limit, 100)))
        )
        return list(result.scalars().all())

    async def list_console_pending_handoffs(
        self,
        *,
        tenant_id: str,
        scene_codes: list[str],
        now: datetime,
        limit: int,
    ) -> list[AiCallHandoffModel]:
        if not scene_codes:
            return []
        result = await self.db.execute(
            select(AiCallHandoffModel)
            .where(
                AiCallHandoffModel.tenant_id == tenant_id,
                AiCallHandoffModel.status == "requested",
                AiCallHandoffModel.scene_code.in_(scene_codes),
                or_(AiCallHandoffModel.expires_at.is_(None), AiCallHandoffModel.expires_at > now),
            )
            .order_by(asc(AiCallHandoffModel.requested_at), asc(AiCallHandoffModel.id))
            .limit(max(1, min(limit, 100)))
        )
        return list(result.scalars().all())

    async def get_console_handoff_for_claim(
        self,
        *,
        tenant_id: str,
        handoff_id: str,
    ) -> AiCallHandoffModel | None:
        stmt = select(AiCallHandoffModel).where(
            AiCallHandoffModel.tenant_id == tenant_id,
            AiCallHandoffModel.handoff_id == handoff_id,
        )
        if self._dialect_name() == "postgresql":
            stmt = stmt.with_for_update()
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()

    async def get_console_agent_for_claim(
        self,
        *,
        tenant_id: str,
        agent_identity: str,
    ) -> AiCallHandoffAgentModel | None:
        stmt = select(AiCallHandoffAgentModel).where(
            AiCallHandoffAgentModel.tenant_id == tenant_id,
            AiCallHandoffAgentModel.agent_identity == agent_identity,
        )
        if self._dialect_name() == "postgresql":
            stmt = stmt.with_for_update()
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()

    async def claim_console_handoff_if_requested(
        self,
        *,
        tenant_id: str,
        handoff_id: str,
        agent_identity: str,
        console_session_id: str,
        accepted_at: datetime,
        claim_expires_at: datetime,
    ) -> bool:
        result = await self.db.execute(
            update(AiCallHandoffModel)
            .where(
                AiCallHandoffModel.tenant_id == tenant_id,
                AiCallHandoffModel.handoff_id == handoff_id,
                AiCallHandoffModel.status == "requested",
                or_(
                    AiCallHandoffModel.expires_at.is_(None),
                    AiCallHandoffModel.expires_at > accepted_at,
                ),
            )
            .values(
                status="accepted",
                human_agent_identity=agent_identity,
                accepted_console_session_id=console_session_id,
                accepted_at=accepted_at,
                claim_expires_at=claim_expires_at,
            )
            .execution_options(synchronize_session=False)
        )
        return result.rowcount == 1

    async def claim_console_agent_if_available(
        self,
        *,
        tenant_id: str,
        agent_identity: str,
        console_session_id: str,
        handoff_id: str,
        call_id: str,
        now: datetime,
    ) -> bool:
        result = await self.db.execute(
            update(AiCallHandoffAgentModel)
            .where(
                AiCallHandoffAgentModel.tenant_id == tenant_id,
                AiCallHandoffAgentModel.agent_identity == agent_identity,
                AiCallHandoffAgentModel.status == "available",
                AiCallHandoffAgentModel.active_handoff_id.is_(None),
                AiCallHandoffAgentModel.console_session_id == console_session_id,
            )
            .values(
                status="claiming",
                active_handoff_id=handoff_id,
                active_call_id=call_id,
                last_seen_at=now,
                status_updated_at=now,
            )
            .execution_options(synchronize_session=False)
        )
        return result.rowcount == 1

    async def create_unanswered_follow_up_if_missing(self, values: dict) -> None:
        await self.create_follow_up_if_missing(values)

    async def create_follow_up_if_missing(
        self,
        values: dict,
    ) -> AiCallFollowUpTaskModel:
        table = AiCallFollowUpTaskModel.__table__
        dialect_name = self._dialect_name()
        if dialect_name == "postgresql":
            stmt = (
                postgresql_insert(table)
                .values(**values)
                .on_conflict_do_nothing(
                    index_elements=[
                        table.c.tenant_id,
                        table.c.source_type,
                        table.c.source_key,
                    ]
                )
            )
        elif dialect_name == "sqlite":
            stmt = (
                sqlite_insert(table)
                .values(**values)
                .on_conflict_do_nothing(
                    index_elements=[
                        table.c.tenant_id,
                        table.c.source_type,
                        table.c.source_key,
                    ]
                )
            )
        elif dialect_name == "mysql":
            insert_stmt = mysql_insert(table).values(**values)
            stmt = insert_stmt.on_duplicate_key_update(
                source_key=insert_stmt.inserted.source_key
            )
        else:
            existing = await self.db.execute(
                select(AiCallFollowUpTaskModel.id).where(
                    AiCallFollowUpTaskModel.tenant_id == values["tenant_id"],
                    AiCallFollowUpTaskModel.source_type == values["source_type"],
                    AiCallFollowUpTaskModel.source_key == values["source_key"],
                )
            )
            if existing.scalar_one_or_none() is None:
                self.db.add(AiCallFollowUpTaskModel(**values))
                await self.db.flush()
        if dialect_name in {"postgresql", "sqlite", "mysql"}:
            await self.db.execute(stmt)
            await self.db.flush()
        return await self.get_follow_up_by_source(
            tenant_id=values["tenant_id"],
            source_type=values["source_type"],
            source_key=values["source_key"],
        )

    async def get_follow_up_by_source(
        self,
        *,
        tenant_id: str,
        source_type: str,
        source_key: str,
    ) -> AiCallFollowUpTaskModel:
        result = await self.db.execute(
            select(AiCallFollowUpTaskModel).where(
                AiCallFollowUpTaskModel.tenant_id == tenant_id,
                AiCallFollowUpTaskModel.source_type == source_type,
                AiCallFollowUpTaskModel.source_key == source_key,
            )
        )
        task = result.scalar_one_or_none()
        if task is None:
            raise RuntimeError("跟进任务幂等写入后未找到记录")
        return task

    async def update_handoff(
        self,
        handoff_id: str,
        **values,
    ) -> AiCallHandoffModel | None:
        handoff = await self.get_handoff_by_id(handoff_id)
        if handoff is None:
            return None
        for key, value in values.items():
            if hasattr(handoff, key):
                setattr(handoff, key, value)
        await self.db.flush()
        await self.db.refresh(handoff)
        return handoff

    async def get_handoff_agent(
        self,
        agent_identity: str,
    ) -> AiCallHandoffAgentModel | None:
        result = await self.db.execute(
            select(AiCallHandoffAgentModel).where(
                AiCallHandoffAgentModel.agent_identity == agent_identity
            )
        )
        return result.scalar_one_or_none()

    async def upsert_handoff_agent(
        self,
        *,
        agent_identity: str,
        skill_group: str,
        status: str,
        active_handoff_id: str | None,
        last_seen_at: datetime | None,
        status_updated_at: datetime,
    ) -> AiCallHandoffAgentModel:
        agent = await self.get_handoff_agent(agent_identity)
        values = {
            "skill_group": skill_group,
            "status": status,
            "active_handoff_id": active_handoff_id,
            "last_seen_at": last_seen_at,
            "status_updated_at": status_updated_at,
        }
        if agent is not None:
            for key, value in values.items():
                setattr(agent, key, value)
            await self.db.flush()
            await self.db.refresh(agent)
            return agent

        agent = AiCallHandoffAgentModel(
            id=generate_snowflake_id(),
            agent_identity=agent_identity,
            **values,
        )
        self.db.add(agent)
        await self.db.flush()
        await self.db.refresh(agent)
        return agent

    async def list_active_handoffs_for_call(
        self,
        call_id: str,
        *,
        terminal_statuses: set[str],
    ) -> list[AiCallHandoffModel]:
        result = await self.db.execute(
            select(AiCallHandoffModel)
            .where(
                AiCallHandoffModel.call_id == call_id,
                AiCallHandoffModel.status.notin_(terminal_statuses),
            )
            .order_by(asc(AiCallHandoffModel.requested_at), asc(AiCallHandoffModel.id))
        )
        return list(result.scalars().all())

    @staticmethod
    def _record_filters(stmt: Select, **filters) -> Select:
        tenant_id = str(filters.get("tenant_id") or "").strip() or None
        phone_number = str(filters.get("phone_number") or "").strip() or None
        customer_name = str(filters.get("customer_name") or "").strip() or None
        call_result = str(filters.get("call_result") or "").strip() or None
        customer_intent = (
            str(filters.get("customer_intent") or "").strip() or None
        )
        classification_review_status = (
            str(filters.get("classification_review_status") or "").strip()
            or None
        )
        follow_up_status = (
            str(filters.get("follow_up_status") or "").strip() or None
        )
        after_call_result_status = (
            str(filters.get("after_call_result_status") or "").strip() or None
        )
        operator_agent_identity = (
            str(filters.get("operator_agent_identity") or "").strip() or None
        )
        formal_outbound_only = bool(filters.get("formal_outbound_only"))
        has_outbound_filters = any(
            (
                filters.get("task_id"),
                filters.get("target_id"),
                phone_number,
                customer_name,
                call_result,
                formal_outbound_only,
            )
        )
        if tenant_id or has_outbound_filters:
            stmt = (
                stmt.outerjoin(
                    AiCallOutboundAttemptModel,
                    or_(
                        AiCallOutboundAttemptModel.call_id == AiCallRecordModel.call_id,
                        and_(
                            AiCallRecordModel.business_type == "outbound_attempt",
                            AiCallRecordModel.business_id
                            == cast(AiCallOutboundAttemptModel.id, String),
                        ),
                    ),
                )
                .outerjoin(
                    AiCallOutboundTargetModel,
                    and_(
                        AiCallOutboundTargetModel.tenant_id
                        == AiCallOutboundAttemptModel.tenant_id,
                        AiCallOutboundTargetModel.id
                        == AiCallOutboundAttemptModel.target_id,
                        AiCallOutboundTargetModel.task_id
                        == AiCallOutboundAttemptModel.task_id,
                    ),
                )
                .outerjoin(
                    AiCallOutboundTaskModel,
                    and_(
                        AiCallOutboundTaskModel.tenant_id
                        == AiCallOutboundAttemptModel.tenant_id,
                        AiCallOutboundTaskModel.id == AiCallOutboundAttemptModel.task_id,
                    ),
                )
            )
        if tenant_id:
            tenant_conditions = [
                and_(
                    AiCallOutboundAttemptModel.tenant_id == tenant_id,
                    AiCallOutboundTargetModel.id.is_not(None),
                    AiCallOutboundTaskModel.id.is_not(None),
                )
            ]
            if tenant_id == DEFAULT_TENANT_ID and not formal_outbound_only:
                # 历史/Web 记录没有租户字段，只归属框架默认租户，禁止向其他租户开放。
                tenant_conditions.append(AiCallOutboundAttemptModel.id.is_(None))
            stmt = stmt.where(or_(*tenant_conditions))
        elif has_outbound_filters:
            stmt = stmt.where(
                and_(
                    AiCallOutboundAttemptModel.id.is_not(None),
                    AiCallOutboundTargetModel.id.is_not(None),
                    AiCallOutboundTaskModel.id.is_not(None),
                )
            )
        if formal_outbound_only:
            stmt = stmt.where(
                AiCallRecordModel.entry_type.in_(
                    ("outbound", "sip_outbound", "web")
                )
            )
        if filters.get("call_id"):
            stmt = stmt.where(AiCallRecordModel.call_id == filters["call_id"])
        if filters.get("task_id"):
            stmt = stmt.where(
                AiCallOutboundAttemptModel.task_id == filters["task_id"],
            )
        if filters.get("target_id"):
            stmt = stmt.where(
                AiCallOutboundAttemptModel.target_id == filters["target_id"],
            )
        if phone_number:
            stmt = stmt.where(
                AiCallOutboundTargetModel.phone_number.contains(phone_number)
            )
        if customer_name:
            stmt = stmt.where(
                AiCallOutboundTargetModel.customer_name.contains(customer_name)
            )
        if call_result:
            stmt = stmt.where(
                AiCallOutboundAttemptModel.call_result == call_result,
            )
        if filters.get("business_type"):
            stmt = stmt.where(AiCallRecordModel.business_type == filters["business_type"])
        if filters.get("business_id"):
            stmt = stmt.where(AiCallRecordModel.business_id == filters["business_id"])
        if filters.get("status"):
            stmt = stmt.where(AiCallRecordModel.status == filters["status"])
        if filters.get("entry_type"):
            stmt = stmt.where(AiCallRecordModel.entry_type == filters["entry_type"])
        if customer_intent in {"positive", "neutral", "negative"}:
            stmt = stmt.where(
                select(AiCallSemanticAnalysisModel.id)
                .where(
                    AiCallSemanticAnalysisModel.call_id
                    == AiCallRecordModel.call_id,
                    AiCallSemanticAnalysisModel.analysis_scene_code
                    == DEFAULT_SEMANTIC_ANALYSIS_SCENE_CODE,
                    AiCallSemanticAnalysisModel.customer_intent
                    == customer_intent,
                )
                .exists()
            )
        elif customer_intent == "pending":
            stmt = stmt.where(
                select(AiCallSemanticAnalysisModel.id)
                .where(
                    AiCallSemanticAnalysisModel.call_id
                    == AiCallRecordModel.call_id,
                    AiCallSemanticAnalysisModel.analysis_scene_code
                    == DEFAULT_SEMANTIC_ANALYSIS_SCENE_CODE,
                    AiCallSemanticAnalysisModel.analysis_status.in_(
                        {
                            SEMANTIC_ANALYSIS_STATUS_PENDING,
                            SEMANTIC_ANALYSIS_STATUS_RUNNING,
                        }
                    ),
                )
                .exists()
            )
        elif customer_intent == "failed":
            stmt = stmt.where(
                select(AiCallSemanticAnalysisModel.id)
                .where(
                    AiCallSemanticAnalysisModel.call_id
                    == AiCallRecordModel.call_id,
                    AiCallSemanticAnalysisModel.analysis_scene_code
                    == DEFAULT_SEMANTIC_ANALYSIS_SCENE_CODE,
                    AiCallSemanticAnalysisModel.analysis_status
                    == SEMANTIC_ANALYSIS_STATUS_FAILED,
                )
                .exists()
            )
        if classification_review_status == "suggested":
            latest_classification_call = (
                select(AiCallFollowUpClassificationHistoryModel.call_id)
                .where(
                    AiCallFollowUpClassificationHistoryModel.tenant_id
                    == tenant_id,
                    AiCallFollowUpClassificationHistoryModel.follow_up_data_id
                    == AiCallRecordModel.follow_up_data_id,
                )
                .order_by(
                    AiCallFollowUpClassificationHistoryModel.created_at.desc(),
                    AiCallFollowUpClassificationHistoryModel.id.desc(),
                )
                .correlate(AiCallRecordModel)
                .limit(1)
                .scalar_subquery()
            )
            stmt = stmt.where(
                select(AiCallFollowUpDataModel.id)
                .where(
                    AiCallFollowUpDataModel.tenant_id == tenant_id,
                    AiCallFollowUpDataModel.id
                    == AiCallRecordModel.follow_up_data_id,
                    AiCallFollowUpDataModel.suggest_review.is_(True),
                )
                .exists(),
                latest_classification_call == AiCallRecordModel.call_id,
                select(AiCallSemanticAnalysisModel.id)
                .where(
                    AiCallSemanticAnalysisModel.call_id
                    == AiCallRecordModel.call_id,
                    AiCallSemanticAnalysisModel.analysis_scene_code
                    == DEFAULT_SEMANTIC_ANALYSIS_SCENE_CODE,
                    AiCallSemanticAnalysisModel.analysis_status
                    == SEMANTIC_ANALYSIS_STATUS_SUCCEEDED,
                    AiCallSemanticAnalysisModel.follow_up_review_status.is_(None),
                )
                .exists(),
            )
        elif classification_review_status == "reviewed":
            stmt = stmt.where(
                select(AiCallSemanticAnalysisModel.id)
                .where(
                    AiCallSemanticAnalysisModel.call_id
                    == AiCallRecordModel.call_id,
                    AiCallSemanticAnalysisModel.analysis_scene_code
                    == DEFAULT_SEMANTIC_ANALYSIS_SCENE_CODE,
                    AiCallSemanticAnalysisModel.follow_up_review_status.in_(
                        {"confirmed", "adjusted"}
                    ),
                )
                .exists()
            )
        if follow_up_status:
            exact_task_status = (
                select(AiCallFollowUpTaskModel.status)
                .where(
                    AiCallFollowUpTaskModel.tenant_id == tenant_id,
                    AiCallFollowUpTaskModel.id == AiCallRecordModel.follow_up_id,
                )
                .correlate(AiCallRecordModel)
                .limit(1)
                .scalar_subquery()
            )
            active_task_status = (
                select(AiCallFollowUpTaskModel.status)
                .where(
                    AiCallFollowUpTaskModel.tenant_id == tenant_id,
                    AiCallFollowUpTaskModel.source_call_id
                    == AiCallRecordModel.call_id,
                    AiCallFollowUpTaskModel.status.not_in(
                        {"completed", "closed"}
                    ),
                )
                .order_by(
                    AiCallFollowUpTaskModel.updated_at.desc(),
                    AiCallFollowUpTaskModel.id.desc(),
                )
                .correlate(AiCallRecordModel)
                .limit(1)
                .scalar_subquery()
            )
            latest_task_status = (
                select(AiCallFollowUpTaskModel.status)
                .where(
                    AiCallFollowUpTaskModel.tenant_id == tenant_id,
                    AiCallFollowUpTaskModel.source_call_id
                    == AiCallRecordModel.call_id,
                )
                .order_by(
                    AiCallFollowUpTaskModel.updated_at.desc(),
                    AiCallFollowUpTaskModel.id.desc(),
                )
                .correlate(AiCallRecordModel)
                .limit(1)
                .scalar_subquery()
            )
            selected_task_status = func.coalesce(
                exact_task_status,
                active_task_status,
                latest_task_status,
            )
            if follow_up_status in {
                "pending",
                "processing",
                "completed",
                "closed",
            }:
                stmt = stmt.where(selected_task_status == follow_up_status)
            elif follow_up_status == "none":
                stmt = stmt.where(selected_task_status.is_(None))
        if operator_agent_identity:
            stmt = stmt.where(
                AiCallRecordModel.operator_agent_identity
                == operator_agent_identity
            )
        if after_call_result_status not in {None, "all"}:
            after_call_work_conditions = [
                AiCallAfterCallWorkModel.call_id == AiCallRecordModel.call_id
            ]
            handling_result_conditions = [
                AiCallFollowUpHandlingResultModel.related_call_id
                == AiCallRecordModel.call_id
            ]
            if tenant_id:
                after_call_work_conditions.append(
                    AiCallAfterCallWorkModel.tenant_id == tenant_id
                )
                handling_result_conditions.append(
                    AiCallFollowUpHandlingResultModel.tenant_id == tenant_id
                )
            submitted = or_(
                select(AiCallAfterCallWorkModel.id)
                .where(*after_call_work_conditions)
                .exists(),
                select(AiCallFollowUpHandlingResultModel.id)
                .where(*handling_result_conditions)
                .exists(),
            )
            if after_call_result_status == "submitted":
                stmt = stmt.where(submitted)
            elif after_call_result_status == "pending":
                stmt = stmt.where(
                    AiCallRecordModel.operator_agent_identity.is_not(None),
                    AiCallRecordModel.status.in_({"completed", "failed"}),
                    ~submitted,
                )
            elif after_call_result_status == "not_applicable":
                stmt = stmt.where(
                    ~submitted,
                    or_(
                        AiCallRecordModel.operator_agent_identity.is_(None),
                        AiCallRecordModel.status.not_in({"completed", "failed"}),
                    ),
                )
        if filters.get("started_at_begin"):
            stmt = stmt.where(AiCallRecordModel.started_at >= filters["started_at_begin"])
        if filters.get("started_at_end"):
            stmt = stmt.where(AiCallRecordModel.started_at < filters["started_at_end"])
        return stmt

    @staticmethod
    def _prompt_profile_filters(stmt: Select, **filters) -> Select:
        if filters.get("tenant_id"):
            stmt = stmt.where(
                AiCallPromptProfileModel.tenant_id == filters["tenant_id"]
            )
        if filters.get("scene_code"):
            stmt = stmt.where(AiCallPromptProfileModel.scene_code == filters["scene_code"])
        return stmt

    @staticmethod
    def _voice_profile_filters(stmt: Select, **filters) -> Select:
        if filters.get("target_model"):
            stmt = stmt.where(AiCallVoiceProfileModel.target_model == filters["target_model"])
        if filters.get("voice_type"):
            stmt = stmt.where(AiCallVoiceProfileModel.voice_type == filters["voice_type"])
        if filters.get("gender"):
            stmt = stmt.where(AiCallVoiceProfileModel.gender == filters["gender"])
        return stmt

    async def _insert_event_idempotently(self, values: dict) -> bool:
        table = AiCallEventModel.__table__
        dialect_name = self._dialect_name()
        if dialect_name == "postgresql":
            stmt = (
                postgresql_insert(table)
                .values(**values)
                .on_conflict_do_nothing(index_elements=[table.c.event_id])
            )
        elif dialect_name == "sqlite":
            stmt = (
                sqlite_insert(table)
                .values(**values)
                .on_conflict_do_nothing(index_elements=[table.c.event_id])
            )
        elif dialect_name == "mysql":
            insert_stmt = mysql_insert(table).values(**values)
            stmt = insert_stmt.on_duplicate_key_update(event_id=insert_stmt.inserted.event_id)
        else:
            return False
        await self.db.execute(stmt)
        return True

    async def _insert_voice_profile_idempotently(self, values: dict) -> bool:
        table = AiCallVoiceProfileModel.__table__
        dialect_name = self._dialect_name()
        if dialect_name == "postgresql":
            stmt = (
                postgresql_insert(table)
                .values(**values)
                .on_conflict_do_nothing(index_elements=[table.c.target_model, table.c.voice])
            )
        elif dialect_name == "sqlite":
            stmt = (
                sqlite_insert(table)
                .values(**values)
                .on_conflict_do_nothing(index_elements=[table.c.target_model, table.c.voice])
            )
        elif dialect_name == "mysql":
            insert_stmt = mysql_insert(table).values(**values)
            stmt = insert_stmt.on_duplicate_key_update(voice=insert_stmt.inserted.voice)
        else:
            return False
        await self.db.execute(stmt)
        return True

    def _dialect_name(self) -> str:
        bind = self.db.get_bind()
        return bind.dialect.name if bind is not None else ""

    @staticmethod
    def _chunks(values: list[str], size: int):
        for index in range(0, len(values), size):
            yield values[index : index + size]

    @staticmethod
    def _quality_score_applicable(record: AiCallRecordModel) -> bool:
        outbound_context = getattr(record, "_outbound_context", {})
        return (
            (
                record.entry_type in {"sip_outbound", "outbound"}
                or (
                    record.entry_type == "web"
                    and bool(outbound_context.get("taskId"))
                )
            )
            and record.status == "completed"
            and outbound_context.get("callResult") == "connected"
        )

    @staticmethod
    def _semantic_analysis_claimable(
        analysis: AiCallSemanticAnalysisModel,
        *,
        now: datetime,
        max_retry_count: int,
        retry_cooldown_minutes: int,
    ) -> bool:
        if analysis.analysis_status == SEMANTIC_ANALYSIS_STATUS_PENDING:
            return True
        if analysis.analysis_status != SEMANTIC_ANALYSIS_STATUS_FAILED:
            return False
        if int(analysis.analysis_retry_count or 0) >= max_retry_count:
            return False
        updated_at = analysis.updated_at
        if updated_at.tzinfo is None and now.tzinfo is not None:
            updated_at = updated_at.replace(tzinfo=now.tzinfo)
        return updated_at <= now - timedelta(minutes=retry_cooldown_minutes)
