from __future__ import annotations

import re
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import (
    AiCallRecordingTrackModel,
    AiCallRecordModel,
)
from app.services.ai_call.runtime_control.customer_track import customer_track_keys
from app.services.ai_call.runtime_control.models import AiCallRuntimeEffectModel
from app.utils.id_util import generate_snowflake_id

if TYPE_CHECKING:
    from app.services.ai_call.runtime_control.effect_repository import (
        ProviderObservation,
    )

_TERMINAL_TRACK_STATUSES = frozenset({"completed", "failed"})
_SAFE_FAILURE_CODE = re.compile(r"[A-Za-z0-9_.:-]{1,128}")


class OwnerTrackRecordingRepository:
    """Project fenced customer Track Egress facts onto one tenant Track row."""

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
    ) -> AiCallRecordingTrackModel | None:
        if effect.effect_type not in {"START_TRACK_EGRESS", "STOP_TRACK_EGRESS"}:
            return None

        identity = str(record.participant_identity or "")
        start_effect = effect
        if effect.effect_type == "STOP_TRACK_EGRESS":
            if source_effect is None:
                raise RuntimeError("STOP_TRACK_EGRESS requires source create effect")
            if source_effect.effect_type != "START_TRACK_EGRESS":
                raise RuntimeError("STOP_TRACK_EGRESS source must be customer start")
            start_effect = source_effect

        self._validate_customer_effect(
            record=record,
            effect=effect,
            start_effect=start_effect,
            participant_identity=identity,
        )
        track = await self._session.scalar(
            select(AiCallRecordingTrackModel)
            .where(
                AiCallRecordingTrackModel.tenant_id == record.tenant_id,
                AiCallRecordingTrackModel.call_id == record.call_id,
                AiCallRecordingTrackModel.track_role == "customer",
                AiCallRecordingTrackModel.participant_identity == identity,
            )
            .with_for_update()
        )

        if track is not None and track.status in _TERMINAL_TRACK_STATUSES:
            return track
        if effect.effect_type == "START_TRACK_EGRESS":
            if track is None:
                track = AiCallRecordingTrackModel(
                    id=self._id_generator(),
                    tenant_id=str(record.tenant_id),
                    call_id=record.call_id,
                    room_name=record.room_name,
                    track_role="customer",
                    participant_identity=identity,
                    status="starting",
                    egress_generation=1,
                    object_name=observation.object_name,
                    started_at=observation.started_at or now,
                )
                self._session.add(track)
            self._project_start(track, effect, observation, now)
        else:
            if track is None:
                return None
            self._project_stop(
                track=track,
                source_effect=start_effect,
                effect=effect,
                observation=observation,
                now=now,
            )
        return track

    @staticmethod
    def _validate_customer_effect(
        *,
        record: AiCallRecordModel,
        effect: AiCallRuntimeEffectModel,
        start_effect: AiCallRuntimeEffectModel,
        participant_identity: str,
    ) -> None:
        _, provider_key, resource_key = customer_track_keys(
            record.call_id,
            participant_identity,
        )
        if (
            start_effect.tenant_id != record.tenant_id
            or start_effect.call_id != record.call_id
            or start_effect.provider_idempotency_key != provider_key
            or start_effect.resource_key != resource_key
            or start_effect.resource_generation != 1
            or effect.tenant_id != record.tenant_id
            or effect.call_id != record.call_id
            or effect.resource_key != resource_key
            or effect.resource_generation != 1
        ):
            raise RuntimeError("customer Track effect identity mismatch")

    def _project_start(
        self,
        track: AiCallRecordingTrackModel,
        effect: AiCallRuntimeEffectModel,
        observation: ProviderObservation,
        now: datetime,
    ) -> None:
        if observation.object_name:
            track.object_name = observation.object_name
        if observation.started_at:
            track.started_at = observation.started_at
        if (
            observation.kind.value == "RESOURCE_PRESENT"
            and observation.provider_reference
        ):
            if track.status in {"starting", "recording"}:
                track.status = "recording"
            track.egress_id = observation.provider_reference
            track.failure_stage = None
            track.failure_message = None
        elif observation.kind.value == "PERMANENT_NO_RESOURCE" or (
            observation.kind.value == "RESOURCE_ABSENT"
            and effect.status == "FAILED"
            and effect.error_message == "no_resource"
        ):
            track.status = "failed"
            track.ended_at = observation.ended_at or now
            track.failure_stage = "egress_start"
            track.failure_message = _safe_failure_summary(
                observation.failure_code,
                default=effect.error_message or "no_resource",
            )
        elif track.status == "starting":
            track.failure_stage = "egress_start_uncertain"
            track.failure_message = _safe_failure_summary(
                observation.failure_code,
                default="egress_start_uncertain",
            )

    def _project_stop(
        self,
        *,
        track: AiCallRecordingTrackModel,
        source_effect: AiCallRuntimeEffectModel,
        effect: AiCallRuntimeEffectModel,
        observation: ProviderObservation,
        now: datetime,
    ) -> None:
        track.egress_generation = source_effect.resource_generation
        if observation.provider_reference:
            track.egress_id = observation.provider_reference
        elif source_effect.provider_reference:
            track.egress_id = source_effect.provider_reference
        if observation.object_name:
            track.object_name = observation.object_name

        terminal = observation.kind.value == "TERMINAL_CONFIRMED" or (
            effect.status == "APPLIED" and effect.terminal_confirmed_at is not None
        )
        if terminal:
            track.status = "verifying"
            track.stop_requested_at = track.stop_requested_at or now
            track.ended_at = observation.ended_at or now
            track.duration_ms = observation.duration_ms
            track.verify_attempts = 0
            track.next_verify_at = now
            track.verify_deadline_at = (
                track.verify_deadline_at or now + self._verify_deadline
            )
            track.failure_stage = None
            track.failure_message = None
        elif track.status in {"starting", "recording", "stopping"}:
            track.status = "stopping"
            track.stop_requested_at = track.stop_requested_at or now
            if observation.failure_code:
                track.failure_stage = "egress_stop_uncertain"
                track.failure_message = _safe_failure_summary(
                    observation.failure_code,
                    default="egress_stop_uncertain",
                )


def _safe_failure_summary(value: str | None, *, default: str) -> str:
    normalized = str(value or "").strip()
    if normalized:
        return normalized if _SAFE_FAILURE_CODE.fullmatch(normalized) else "provider_failure"
    return default if _SAFE_FAILURE_CODE.fullmatch(default) else "provider_failure"
