from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import (
    AiCallDialogueSegmentModel,
    AiCallRecordModel,
)
from app.services.ai_call.runtime_control.timing import read_database_time
from app.utils.id_util import generate_snowflake_id

_PERSISTED_STATUSES = frozenset({"final", "interrupted", "failed"})
_FINAL_STATUSES = frozenset({"complete", "uncertain"})


@dataclass(frozen=True, slots=True)
class OwnerDialogueFence:
    tenant_id: str
    call_id: str
    owner_id: str
    fencing_token: int


@dataclass(frozen=True, slots=True)
class OwnerDialogueSegment:
    segment_no: int
    speaker_type: str
    speaker_identity: str | None
    source: str
    source_segment_id: str
    text: str
    segment_status: str
    started_at: datetime | None
    ended_at: datetime | None
    duration_ms: int | None = None
    audio_start_ms: int | None = None
    audio_end_ms: int | None = None
    failure_stage: str | None = None
    failure_message: str | None = None


@dataclass(frozen=True, slots=True)
class OwnerDialogueBatchResult:
    accepted: bool
    persisted_count: int


class OwnerDialogueSequenceConflictError(RuntimeError):
    pass


class OwnerDialogueRepository:
    """Persist dialogue only while the supplied Owner/fencing is current."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        database_clock: Callable[[AsyncSession], Awaitable[datetime]] = (
            read_database_time
        ),
    ) -> None:
        self._session = session
        self._database_clock = database_clock

    async def next_segment_no(self, fence: OwnerDialogueFence) -> int | None:
        if await self._lock_current_record(fence) is None:
            return None
        current = await self._session.scalar(
            select(func.max(AiCallDialogueSegmentModel.segment_no)).where(
                AiCallDialogueSegmentModel.tenant_id == fence.tenant_id,
                AiCallDialogueSegmentModel.call_id == fence.call_id,
            )
        )
        return int(current or 0) + 1

    async def persist_batch(
        self,
        fence: OwnerDialogueFence,
        segments: Sequence[OwnerDialogueSegment],
    ) -> OwnerDialogueBatchResult:
        for segment in segments:
            self._validate_segment(segment)
        if await self._lock_current_record(fence) is None:
            return OwnerDialogueBatchResult(accepted=False, persisted_count=0)

        persisted_count = 0
        for segment in segments:
            source_segment_id = self._fenced_source_segment_id(
                fence.fencing_token,
                segment.source_segment_id,
            )
            existing = await self._session.scalar(
                select(AiCallDialogueSegmentModel).where(
                    AiCallDialogueSegmentModel.tenant_id == fence.tenant_id,
                    AiCallDialogueSegmentModel.call_id == fence.call_id,
                    AiCallDialogueSegmentModel.speaker_type == segment.speaker_type,
                    AiCallDialogueSegmentModel.source == segment.source,
                    AiCallDialogueSegmentModel.source_segment_id == source_segment_id,
                )
            )
            if existing is None:
                existing = await self._session.scalar(
                    select(AiCallDialogueSegmentModel).where(
                        AiCallDialogueSegmentModel.tenant_id == fence.tenant_id,
                        AiCallDialogueSegmentModel.call_id == fence.call_id,
                        AiCallDialogueSegmentModel.segment_no == segment.segment_no,
                    )
                )
                if existing is not None:
                    raise OwnerDialogueSequenceConflictError(
                        f"dialogue segment number {segment.segment_no} is already in use"
                    )
                existing = AiCallDialogueSegmentModel(
                    id=generate_snowflake_id(),
                    tenant_id=fence.tenant_id,
                    call_id=fence.call_id,
                    segment_no=segment.segment_no,
                    speaker_type=segment.speaker_type,
                    speaker_identity=segment.speaker_identity,
                    source=segment.source,
                    source_segment_id=source_segment_id,
                    segment_text=segment.text,
                    segment_status=segment.segment_status,
                    started_at=segment.started_at,
                    ended_at=segment.ended_at,
                    duration_ms=segment.duration_ms,
                    audio_start_ms=segment.audio_start_ms,
                    audio_end_ms=segment.audio_end_ms,
                    failure_stage=segment.failure_stage,
                    failure_message=segment.failure_message,
                )
                self._session.add(existing)
            else:
                existing.speaker_identity = segment.speaker_identity
                existing.segment_text = segment.text
                existing.segment_status = segment.segment_status
                existing.started_at = segment.started_at
                existing.ended_at = segment.ended_at
                existing.duration_ms = segment.duration_ms
                existing.audio_start_ms = segment.audio_start_ms
                existing.audio_end_ms = segment.audio_end_ms
                existing.failure_stage = segment.failure_stage
                existing.failure_message = segment.failure_message
            persisted_count += 1

        await self._session.flush()
        return OwnerDialogueBatchResult(
            accepted=True,
            persisted_count=persisted_count,
        )

    async def finalize(
        self,
        fence: OwnerDialogueFence,
        *,
        status: str,
        error: str | None = None,
    ) -> bool:
        if status not in _FINAL_STATUSES:
            raise ValueError("dialogue persistence final status is invalid")
        record = await self._lock_current_record(fence)
        if record is None or record.terminal_requested_at is None:
            return False
        now = await self._database_clock(self._session)
        record.dialogue_persistence_status = status
        record.dialogue_persistence_error = self._safe_error(error)
        record.dialogue_persistence_completed_at = now
        await self._session.flush()
        return True

    async def _lock_current_record(
        self,
        fence: OwnerDialogueFence,
    ) -> AiCallRecordModel | None:
        record = await self._session.scalar(
            select(AiCallRecordModel)
            .where(
                AiCallRecordModel.tenant_id == fence.tenant_id,
                AiCallRecordModel.call_id == fence.call_id,
            )
            .with_for_update()
        )
        if record is None:
            return None
        now = await self._database_clock(self._session)
        lease_expires_at = record.runtime_lease_expires_at
        if lease_expires_at is not None and lease_expires_at.tzinfo is None:
            lease_expires_at = lease_expires_at.replace(tzinfo=now.tzinfo)
        if (
            record.runtime_control_mode != "owner_command_v1"
            or record.runtime_owner_id != fence.owner_id
            or record.runtime_fencing_token != fence.fencing_token
            or lease_expires_at is None
            or lease_expires_at <= now
            or record.dialogue_persistence_status != "pending"
        ):
            return None
        return record

    @staticmethod
    def _validate_segment(segment: OwnerDialogueSegment) -> None:
        if segment.segment_no <= 0:
            raise ValueError("dialogue segment number must be positive")
        if not segment.source_segment_id.strip():
            raise ValueError("dialogue source segment id must not be empty")
        if segment.segment_status not in _PERSISTED_STATUSES:
            raise ValueError("dialogue segment status is not persistable")

    @staticmethod
    def _fenced_source_segment_id(fencing_token: int, source_segment_id: str) -> str:
        value = f"f{fencing_token}:{source_segment_id.strip()}"
        if len(value) <= 128:
            return value
        digest = hashlib.sha256(value.encode()).hexdigest()
        return f"f{fencing_token}:sha256:{digest}"

    @staticmethod
    def _safe_error(error: str | None) -> str | None:
        if error is None:
            return None
        value = " ".join(error.strip().split())
        return value[:500] or None
