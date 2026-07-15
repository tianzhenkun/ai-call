from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import Select, asc, desc, func, or_, select
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import (
    AiCallAsrJobModel,
    AiCallDialogueSegmentModel,
    AiCallEventModel,
    AiCallHandoffAgentModel,
    AiCallHandoffModel,
    AiCallPromptProfileModel,
    AiCallRecordingModel,
    AiCallRecordingTrackModel,
    AiCallRecordModel,
    AiCallSemanticAnalysisModel,
    AiCallVoiceProfileModel,
)
from app.utils.id_util import generate_snowflake_id

DEFAULT_SEMANTIC_ANALYSIS_SCENE_CODE = "ai_call_semantic_analysis"
SEMANTIC_ANALYSIS_STATUS_PENDING = "0"
SEMANTIC_ANALYSIS_STATUS_RUNNING = "1"
SEMANTIC_ANALYSIS_STATUS_SUCCEEDED = "2"
SEMANTIC_ANALYSIS_STATUS_FAILED = "3"
SEMANTIC_ANALYSIS_STATUS_NO_USER_INPUT = "4"


class AiCallRecordRepository:
    """AI Call B1 专用持久化仓储。"""

    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def create_record(
        self,
        *,
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
        call_id: str | None = None,
        business_type: str | None = None,
        business_id: str | None = None,
        status: str | None = None,
        entry_type: str | None = None,
        started_at_begin: datetime | None = None,
        started_at_end: datetime | None = None,
        page_num: int = 1,
        page_size: int = 10,
    ) -> tuple[list[AiCallRecordModel], int]:
        stmt = self._record_filters(
            select(AiCallRecordModel),
            call_id=call_id,
            business_type=business_type,
            business_id=business_id,
            status=status,
            entry_type=entry_type,
            started_at_begin=started_at_begin,
            started_at_end=started_at_end,
        )
        count_stmt = self._record_filters(
            select(func.count()).select_from(AiCallRecordModel),
            call_id=call_id,
            business_type=business_type,
            business_id=business_id,
            status=status,
            entry_type=entry_type,
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
        return list(rows), total

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
        call_id: str,
        room_name: str,
        status: str,
        started_at: datetime,
        object_name: str | None = None,
    ) -> AiCallRecordingModel:
        recording = AiCallRecordingModel(
            id=generate_snowflake_id(),
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

    async def get_recording(self, call_id: str) -> AiCallRecordingModel | None:
        result = await self.db.execute(
            select(AiCallRecordingModel).where(AiCallRecordingModel.call_id == call_id)
        )
        return result.scalar_one_or_none()

    async def update_recording(
        self,
        call_id: str,
        **values,
    ) -> AiCallRecordingModel | None:
        recording = await self.get_recording(call_id)
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
        call_id: str,
        track_role: str,
        participant_identity: str,
    ) -> AiCallRecordingTrackModel | None:
        result = await self.db.execute(
            select(AiCallRecordingTrackModel).where(
                AiCallRecordingTrackModel.call_id == call_id,
                AiCallRecordingTrackModel.track_role == track_role,
                AiCallRecordingTrackModel.participant_identity == participant_identity,
            )
        )
        return result.scalar_one_or_none()

    async def list_recording_tracks(self, call_id: str) -> list[AiCallRecordingTrackModel]:
        result = await self.db.execute(
            select(AiCallRecordingTrackModel)
            .where(AiCallRecordingTrackModel.call_id == call_id)
            .order_by(
                asc(AiCallRecordingTrackModel.started_at),
                asc(AiCallRecordingTrackModel.id),
            )
        )
        return list(result.scalars().all())

    async def list_due_recording_verifications(
        self,
        *,
        now: datetime,
        limit: int,
    ) -> list[AiCallRecordingModel]:
        result = await self.db.execute(
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
        )
        return list(result.scalars().all())

    async def list_due_recording_track_verifications(
        self,
        *,
        now: datetime,
        limit: int,
    ) -> list[AiCallRecordingTrackModel]:
        result = await self.db.execute(
            select(AiCallRecordingTrackModel)
            .where(
                AiCallRecordingTrackModel.status == "verifying",
                or_(
                    AiCallRecordingTrackModel.next_verify_at.is_(None),
                    AiCallRecordingTrackModel.next_verify_at <= now,
                ),
            )
            .order_by(
                asc(AiCallRecordingTrackModel.next_verify_at),
                asc(AiCallRecordingTrackModel.id),
            )
            .limit(max(1, limit))
        )
        return list(result.scalars().all())

    async def update_recording_track(
        self,
        track_id: int,
        **values,
    ) -> AiCallRecordingTrackModel | None:
        result = await self.db.execute(
            select(AiCallRecordingTrackModel).where(AiCallRecordingTrackModel.id == track_id)
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

    async def list_asr_jobs(self, call_id: str) -> list[AiCallAsrJobModel]:
        result = await self.db.execute(
            select(AiCallAsrJobModel)
            .where(AiCallAsrJobModel.call_id == call_id)
            .order_by(
                asc(AiCallAsrJobModel.submitted_at),
                asc(AiCallAsrJobModel.id),
            )
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
        analysis.analysis_finished_at = now
        analysis.transcript_snapshot_json = transcript_snapshot_json
        analysis.transcript_hash = transcript_hash
        analysis.updated_at = now
        await self.db.flush()
        await self.db.refresh(analysis)
        return analysis

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
    ) -> AiCallHandoffModel:
        handoff = AiCallHandoffModel(
            id=generate_snowflake_id(),
            handoff_id=handoff_id,
            call_id=call_id,
            room_name=room_name,
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
        if filters.get("call_id"):
            stmt = stmt.where(AiCallRecordModel.call_id == filters["call_id"])
        if filters.get("business_type"):
            stmt = stmt.where(AiCallRecordModel.business_type == filters["business_type"])
        if filters.get("business_id"):
            stmt = stmt.where(AiCallRecordModel.business_id == filters["business_id"])
        if filters.get("status"):
            stmt = stmt.where(AiCallRecordModel.status == filters["status"])
        if filters.get("entry_type"):
            stmt = stmt.where(AiCallRecordModel.entry_type == filters["entry_type"])
        if filters.get("started_at_begin"):
            stmt = stmt.where(AiCallRecordModel.started_at >= filters["started_at_begin"])
        if filters.get("started_at_end"):
            stmt = stmt.where(AiCallRecordModel.started_at <= filters["started_at_end"])
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
