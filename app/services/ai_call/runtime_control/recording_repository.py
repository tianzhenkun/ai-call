from __future__ import annotations

import re
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import (
    AiCallRecordingModel,
    AiCallRecordModel,
)
from app.services.ai_call.runtime_control.models import AiCallRuntimeEffectModel
from app.utils.id_util import generate_snowflake_id

if TYPE_CHECKING:
    from app.services.ai_call.runtime_control.effect_repository import (
        ProviderObservation,
    )

_TERMINAL_RECORDING_STATUSES = frozenset({"completed", "failed"})
_SAFE_FAILURE_CODE = re.compile(r"[A-Za-z0-9_.:-]{1,128}")


class OwnerRecordingRepository:
    """Project fenced Runtime Egress facts onto the tenant recording row."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        id_generator: Callable[[], int] = generate_snowflake_id,
        verify_deadline: timedelta = timedelta(minutes=15),
    ) -> None:
        self._session = session
        self._id_generator = id_generator
        self._verify_deadline = verify_deadline

    async def project(
        self,
        *,
        record: AiCallRecordModel,
        effect: AiCallRuntimeEffectModel,
        source_effect: AiCallRuntimeEffectModel | None,
        observation: ProviderObservation,
        now: datetime,
    ) -> AiCallRecordingModel | None:
        if effect.effect_type not in {"START_EGRESS", "STOP_EGRESS"}:
            return None
        recording = await self._session.scalar(
            select(AiCallRecordingModel)
            .where(
                AiCallRecordingModel.tenant_id == record.tenant_id,
                AiCallRecordingModel.call_id == record.call_id,
            )
            .with_for_update()
        )
        if recording is not None and recording.status in _TERMINAL_RECORDING_STATUSES:
            return recording
        if effect.effect_type == "START_EGRESS":
            recording = self._project_start(
                recording=recording,
                record=record,
                effect=effect,
                observation=observation,
                now=now,
            )
        else:
            if source_effect is None:
                raise RuntimeError("STOP_EGRESS recording projection requires source effect")
            if recording is None:
                return None
            self._project_stop(
                recording=recording,
                effect=effect,
                source_effect=source_effect,
                observation=observation,
                now=now,
            )
        return recording

    def _project_start(
        self,
        *,
        recording: AiCallRecordingModel | None,
        record: AiCallRecordModel,
        effect: AiCallRuntimeEffectModel,
        observation: ProviderObservation,
        now: datetime,
    ) -> AiCallRecordingModel:
        if recording is None:
            recording = AiCallRecordingModel(
                id=self._id_generator(),
                tenant_id=str(record.tenant_id),
                call_id=record.call_id,
                room_name=record.room_name,
                status="starting",
                egress_generation=effect.resource_generation,
                object_name=observation.object_name,
                started_at=observation.started_at or now,
            )
            self._session.add(recording)
        recording.egress_generation = effect.resource_generation
        if observation.object_name:
            recording.object_name = observation.object_name
        if observation.started_at:
            recording.started_at = observation.started_at

        kind = observation.kind.value
        if kind == "RESOURCE_PRESENT" and observation.provider_reference:
            if recording.status in {"starting", "recording"}:
                recording.status = "recording"
            recording.egress_id = observation.provider_reference
            recording.failure_stage = None
            recording.failure_message = None
        elif kind == "PERMANENT_NO_RESOURCE":
            recording.status = "failed"
            recording.ended_at = observation.ended_at or now
            recording.failure_stage = "egress_start"
            recording.failure_message = _safe_failure_summary(
                observation.failure_code,
                default="no_resource",
            )
        elif recording.status == "starting":
            recording.failure_stage = "egress_start_uncertain"
            recording.failure_message = _safe_failure_summary(
                observation.failure_code,
                default="egress_start_uncertain",
            )
        return recording

    def _project_stop(
        self,
        *,
        recording: AiCallRecordingModel,
        effect: AiCallRuntimeEffectModel,
        source_effect: AiCallRuntimeEffectModel,
        observation: ProviderObservation,
        now: datetime,
    ) -> None:
        recording.egress_generation = source_effect.resource_generation
        if observation.provider_reference:
            recording.egress_id = observation.provider_reference
        elif source_effect.provider_reference:
            recording.egress_id = source_effect.provider_reference
        if observation.object_name:
            recording.object_name = observation.object_name
        if observation.kind.value == "TERMINAL_CONFIRMED":
            recording.status = "verifying"
            recording.stop_requested_at = recording.stop_requested_at or now
            recording.ended_at = observation.ended_at or now
            recording.duration_ms = observation.duration_ms
            recording.verify_attempts = 0
            recording.next_verify_at = now
            recording.verify_deadline_at = (
                recording.verify_deadline_at or now + self._verify_deadline
            )
            recording.failure_stage = None
            recording.failure_message = None
        elif recording.status in {"starting", "recording", "stopping"}:
            recording.status = "stopping"
            recording.stop_requested_at = recording.stop_requested_at or now
            if observation.failure_code:
                recording.failure_stage = "egress_stop_uncertain"
                recording.failure_message = _safe_failure_summary(
                    observation.failure_code,
                    default="egress_stop_uncertain",
                )


def _safe_failure_summary(value: str | None, *, default: str) -> str:
    normalized = str(value or "").strip()
    if normalized:
        return normalized if _SAFE_FAILURE_CODE.fullmatch(normalized) else "provider_failure"
    return default if _SAFE_FAILURE_CODE.fullmatch(default) else "provider_failure"
