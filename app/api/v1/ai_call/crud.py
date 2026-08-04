from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import Select, and_, asc, desc, func, or_, select, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import (
    AiCallAfterCallWorkModel,
    AiCallAsrJobModel,
    AiCallDialogueSegmentModel,
    AiCallEventModel,
    AiCallFollowUpTaskModel,
    AiCallHandoffAgentModel,
    AiCallHandoffModel,
    AiCallPromptProfileModel,
    AiCallRecordingModel,
    AiCallRecordingTrackModel,
    AiCallRecordModel,
    AiCallSemanticAnalysisModel,
    AiCallVoiceProfileModel,
)
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from app.utils.id_util import generate_snowflake_id

DEFAULT_SEMANTIC_ANALYSIS_SCENE_CODE = "ai_call_semantic_analysis"
SEMANTIC_ANALYSIS_STATUS_PENDING = "0"
SEMANTIC_ANALYSIS_STATUS_RUNNING = "1"
SEMANTIC_ANALYSIS_STATUS_SUCCEEDED = "2"
SEMANTIC_ANALYSIS_STATUS_FAILED = "3"
SEMANTIC_ANALYSIS_STATUS_NO_USER_INPUT = "4"
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
    ) -> list[AiCallDialogueSegmentModel]:
        result = await self.db.execute(
            select(AiCallDialogueSegmentModel)
            .where(
                AiCallDialogueSegmentModel.call_id == call_id,
                AiCallDialogueSegmentModel.segment_status.in_(
                    {"final", "interrupted"}
                ),
                AiCallDialogueSegmentModel.speaker_type.in_({"ai", "customer"}),
                func.length(func.trim(AiCallDialogueSegmentModel.segment_text)) > 0,
            )
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

    async def get_outbound_task_config_snapshot(self, task_id: int) -> str | None:
        return await self.db.scalar(
            select(AiCallOutboundTaskModel.config_snapshot_json).where(
                AiCallOutboundTaskModel.id == task_id
            )
        )

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
        follow_up_status: str | None = None,
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
            follow_up_status=follow_up_status,
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
            follow_up_status=follow_up_status,
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
        await self._attach_outbound_context(rows, tenant_id=tenant_id)
        await self._attach_semantic_analysis(rows)
        await self._attach_follow_up_context(rows, tenant_id=tenant_id)
        return list(rows), total

    async def _attach_semantic_analysis(
        self,
        records: list[AiCallRecordModel],
    ) -> None:
        call_ids = [record.call_id for record in records]
        if not call_ids:
            return
        analysis_rows = (
            await self.db.execute(
                select(
                    AiCallSemanticAnalysisModel.call_id,
                    AiCallSemanticAnalysisModel.analysis_status,
                    AiCallSemanticAnalysisModel.analysis_result,
                    AiCallSemanticAnalysisModel.customer_intent,
                    AiCallSemanticAnalysisModel.follow_up_suggested,
                ).where(
                    AiCallSemanticAnalysisModel.call_id.in_(call_ids),
                    AiCallSemanticAnalysisModel.analysis_scene_code
                    == DEFAULT_SEMANTIC_ANALYSIS_SCENE_CODE,
                )
            )
        ).all()
        analysis_by_call_id = {
            row.call_id: {
                "analysisStatus": row.analysis_status,
                "analysisResult": row.analysis_result,
                "customerIntent": row.customer_intent,
                "followUpSuggested": bool(row.follow_up_suggested),
            }
            for row in analysis_rows
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

    async def _attach_outbound_context(
        self,
        records: list[AiCallRecordModel],
        *,
        tenant_id: str | None,
    ) -> None:
        call_ids = [record.call_id for record in records]
        if not call_ids or not tenant_id:
            return
        context_rows = (
            await self.db.execute(
                select(
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
                    AiCallOutboundAttemptModel.call_id.in_(call_ids),
                )
            )
        ).all()
        contexts = {
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
        for record in records:
            record._outbound_context = contexts.get(record.call_id, {})

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

    async def get_prompt_profile(
        self,
        profile_id: int,
    ) -> AiCallPromptProfileModel | None:
        result = await self.db.execute(
            select(AiCallPromptProfileModel).where(AiCallPromptProfileModel.id == profile_id)
        )
        return result.scalar_one_or_none()

    async def get_prompt_profile_by_scene(
        self,
        *,
        scene_code: str,
    ) -> AiCallPromptProfileModel | None:
        stmt = select(AiCallPromptProfileModel).where(
            AiCallPromptProfileModel.scene_code == scene_code,
        )
        result = await self.db.execute(stmt.limit(1))
        return result.scalar_one_or_none()

    async def list_prompt_profiles(
        self,
        *,
        scene_code: str | None = None,
        page_num: int = 1,
        page_size: int = 20,
    ) -> tuple[list[AiCallPromptProfileModel], int]:
        stmt = self._prompt_profile_filters(
            select(AiCallPromptProfileModel),
            scene_code=scene_code,
        )
        count_stmt = self._prompt_profile_filters(
            select(func.count()).select_from(AiCallPromptProfileModel),
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
        **values,
    ) -> AiCallPromptProfileModel | None:
        profile = await self.get_prompt_profile(profile_id)
        if profile is None:
            return None
        for key, value in values.items():
            if hasattr(profile, key):
                setattr(profile, key, value)
        profile.updated_at = datetime.now(timezone.utc)
        await self.db.flush()
        await self.db.refresh(profile)
        return profile

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
        follow_up_status = (
            str(filters.get("follow_up_status") or "").strip() or None
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
                    AiCallOutboundAttemptModel.call_id == AiCallRecordModel.call_id,
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
            stmt = stmt.where(AiCallRecordModel.entry_type == "sip_outbound")
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
            elif follow_up_status in {"suggested", "none"}:
                suggested = follow_up_status == "suggested"
                semantic_conditions = [
                    AiCallSemanticAnalysisModel.call_id
                    == AiCallRecordModel.call_id,
                    AiCallSemanticAnalysisModel.analysis_scene_code
                    == DEFAULT_SEMANTIC_ANALYSIS_SCENE_CODE,
                    AiCallSemanticAnalysisModel.follow_up_suggested
                    == suggested,
                ]
                if follow_up_status == "none":
                    semantic_conditions.append(
                        AiCallSemanticAnalysisModel.analysis_status
                        == SEMANTIC_ANALYSIS_STATUS_SUCCEEDED
                    )
                stmt = stmt.where(
                    select(AiCallSemanticAnalysisModel.id)
                    .where(*semantic_conditions)
                    .exists(),
                    selected_task_status.is_(None),
                )
        if filters.get("started_at_begin"):
            stmt = stmt.where(AiCallRecordModel.started_at >= filters["started_at_begin"])
        if filters.get("started_at_end"):
            stmt = stmt.where(AiCallRecordModel.started_at < filters["started_at_end"])
        return stmt

    @staticmethod
    def _prompt_profile_filters(stmt: Select, **filters) -> Select:
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
