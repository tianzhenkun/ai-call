from __future__ import annotations

import hashlib
import secrets
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from sqlalchemy import and_, exists, func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from app.api.v1.ai_call.model import AiCallRecordModel
from app.services.ai_call.runtime_control.command_repository import CommandClaim
from app.services.ai_call.runtime_control.customer_track import customer_track_keys
from app.services.ai_call.runtime_control.models import (
    AiCallRuntimeCommandModel,
    AiCallRuntimeEffectDependencyModel,
    AiCallRuntimeEffectModel,
    AiCallRuntimeWorkerModel,
    AiCallSipLineReservationModel,
)
from app.services.ai_call.runtime_control.owner_repository import OwnerLease
from app.services.ai_call.runtime_control.recording_repository import (
    OwnerRecordingRepository,
)
from app.services.ai_call.runtime_control.timing import read_database_time
from app.services.ai_call.runtime_control.types import CommandStatus, EffectStatus
from app.utils.id_util import generate_snowflake_id

CREATE_EFFECT_TYPES = frozenset(
    {
        "CREATE_ROOM",
        "CREATE_SIP_PARTICIPANT",
        "ATTACH_AGENT_PARTICIPANT",
        "START_EGRESS",
        "START_TRACK_EGRESS",
    }
)
AUXILIARY_START_EFFECT_TYPES = frozenset(
    {"START_EGRESS", "START_TRACK_EGRESS"}
)
DESTROY_EFFECT_TYPES = frozenset(
    {
        "HANGUP_SIP",
        "DISCONNECT_AGENT_PARTICIPANT",
        "STOP_EGRESS",
        "STOP_TRACK_EGRESS",
        "DELETE_ROOM",
    }
)
_DESTROY_FOR_CREATE = {
    "CREATE_ROOM": "DELETE_ROOM",
    "CREATE_SIP_PARTICIPANT": "HANGUP_SIP",
    "ATTACH_AGENT_PARTICIPANT": "DISCONNECT_AGENT_PARTICIPANT",
    "START_EGRESS": "STOP_EGRESS",
    "START_TRACK_EGRESS": "STOP_TRACK_EGRESS",
}


class ProviderObservationKind(StrEnum):
    RESOURCE_PRESENT = "RESOURCE_PRESENT"
    RESOURCE_ABSENT = "RESOURCE_ABSENT"
    TERMINAL_CONFIRMED = "TERMINAL_CONFIRMED"
    RETRYABLE_FAILURE = "RETRYABLE_FAILURE"
    PERMANENT_NO_RESOURCE = "PERMANENT_NO_RESOURCE"
    UNCERTAIN = "UNCERTAIN"
    ACCEPTED = "ACCEPTED"


@dataclass(frozen=True, slots=True)
class ProviderObservation:
    kind: ProviderObservationKind
    provider_reference: str | None = None
    provider_status: str | None = None
    object_name: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    duration_ms: int | None = None
    file_size: int | None = None
    failure_code: str | None = None
    error_message: str | None = None
    retry_after: timedelta = timedelta(0)


@dataclass(frozen=True, slots=True)
class EffectSpec:
    effect_type: str
    idempotency_key: str
    provider_namespace: str
    provider_idempotency_key: str
    resource_key: str
    resource_generation: int
    reconcile_deadline_at: datetime | None = None
    source_create_effect_id: int | None = None
    create_protection_deadline_at: datetime | None = None
    execution_phase: int = 0
    prerequisite_effect_ids: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class EffectSnapshot:
    effect_id: int
    tenant_id: str
    call_id: str
    command_id: int
    effect_type: str
    status: str
    resource_key: str
    resource_generation: int
    source_create_effect_id: int | None


@dataclass(frozen=True, slots=True)
class EffectClaim:
    effect_id: int
    tenant_id: str
    call_id: str
    effect_type: str
    processing_owner_id: str
    processing_fencing_token: int
    processing_token: str
    processing_expires_at: datetime
    source_create_effect_id: int | None
    create_protection_deadline_at: datetime | None
    attempt_count: int
    reconcile_only: bool
    provider_namespace: str
    resource_key: str
    reservation_token: str | None = None


class EffectRegistrationError(RuntimeError):
    pass


class RuntimeEffectRepository:
    def __init__(
        self,
        session: AsyncSession,
        *,
        id_generator: Callable[[], int] = generate_snowflake_id,
        processing_token_generator: Callable[[], str] = lambda: secrets.token_urlsafe(24),
        processing_lease_ttl: timedelta = timedelta(seconds=30),
        required_absence_observations: int = 2,
        database_clock: Callable[[AsyncSession], Awaitable[datetime]] = read_database_time,
        recording_repository: OwnerRecordingRepository | None = None,
    ) -> None:
        if required_absence_observations < 1:
            raise ValueError("required_absence_observations must be positive")
        self._session = session
        self._id_generator = id_generator
        self._processing_token_generator = processing_token_generator
        self._processing_lease_ttl = processing_lease_ttl
        self._required_absence_observations = required_absence_observations
        self._database_clock = database_clock
        self._recording_repository = recording_repository or OwnerRecordingRepository(
            session,
            id_generator=id_generator,
        )

    async def register(
        self,
        command_claim: CommandClaim,
        spec: EffectSpec,
    ) -> EffectSnapshot:
        self._validate_effect_type(spec)
        existing = await self._find_by_idempotency(
            command_claim.tenant_id,
            spec.idempotency_key,
        )
        if existing is not None:
            await self._validate_existing(existing, command_claim, spec)
            return _effect_snapshot(existing)

        record, command = await self._authorize_first_registration(command_claim)
        if record.terminal_requested_at is not None and spec.effect_type in CREATE_EFFECT_TYPES:
            raise EffectRegistrationError(
                "terminal barrier forbids registering a new create effect"
            )
        source_create = await self._validate_source_create(command_claim, spec)
        await self._validate_dependency_graph(command_claim, spec)

        now = await self._database_clock(self._session)
        if (
            record.runtime_lease_expires_at is None
            or record.runtime_lease_expires_at <= now
            or command.processing_expires_at is None
            or command.processing_expires_at <= now
        ):
            raise EffectRegistrationError("command claim is expired")

        existing = await self._find_by_idempotency(
            command_claim.tenant_id,
            spec.idempotency_key,
        )
        if existing is not None:
            await self._validate_existing(existing, command_claim, spec)
            return _effect_snapshot(existing)

        effect = AiCallRuntimeEffectModel(
            id=self._id_generator(),
            tenant_id=command_claim.tenant_id,
            call_id=command_claim.call_id,
            command_id=command.id,
            effect_type=spec.effect_type,
            idempotency_key=spec.idempotency_key,
            fencing_token=command_claim.processing_fencing_token,
            status=EffectStatus.PENDING,
            provider_namespace=spec.provider_namespace,
            provider_idempotency_key=spec.provider_idempotency_key,
            resource_key=spec.resource_key,
            resource_generation=spec.resource_generation,
            source_create_effect_id=(source_create.id if source_create else None),
            create_protection_deadline_at=spec.create_protection_deadline_at,
            reconcile_deadline_at=spec.reconcile_deadline_at,
            execution_phase=spec.execution_phase,
            created_at=now,
            updated_at=now,
        )
        try:
            async with self._session.begin_nested():
                self._session.add(effect)
                await self._session.flush()
        except IntegrityError:
            winner = await self._find_by_idempotency(
                command_claim.tenant_id,
                spec.idempotency_key,
            )
            if winner is None:
                raise
            await self._validate_existing(winner, command_claim, spec)
            return _effect_snapshot(winner)
        for prerequisite_id in sorted(set(spec.prerequisite_effect_ids)):
            self._session.add(
                AiCallRuntimeEffectDependencyModel(
                    id=self._id_generator(),
                    tenant_id=command_claim.tenant_id,
                    effect_id=effect.id,
                    prerequisite_effect_id=prerequisite_id,
                    required_status=EffectStatus.APPLIED,
                    created_at=now,
                )
            )
        await self._session.flush()
        return _effect_snapshot(effect)

    async def register_end_graph(
        self,
        command_claim: CommandClaim,
    ) -> list[EffectSnapshot]:
        if command_claim.command_type != "END_CALL":
            raise EffectRegistrationError("destroy graph requires an END_CALL claim")
        record, command = await self._authorize_first_registration(command_claim)
        now = await self._database_clock(self._session)
        if record.terminal_requested_at is None:
            raise EffectRegistrationError("destroy graph requires a terminal barrier")
        if (
            record.runtime_lease_expires_at is None
            or record.runtime_lease_expires_at <= now
            or command.processing_expires_at is None
            or command.processing_expires_at <= now
        ):
            raise EffectRegistrationError("command claim is expired")
        creates = list(
            (
                await self._session.scalars(
                    select(AiCallRuntimeEffectModel)
                    .where(
                        AiCallRuntimeEffectModel.tenant_id == command_claim.tenant_id,
                        AiCallRuntimeEffectModel.call_id == command_claim.call_id,
                        AiCallRuntimeEffectModel.effect_type.in_(CREATE_EFFECT_TYPES),
                    )
                    .order_by(AiCallRuntimeEffectModel.id)
                    .with_for_update()
                )
            ).all()
        )
        for create in creates:
            if (
                create.effect_type == "START_TRACK_EGRESS"
                and create.status == EffectStatus.PENDING
                and create.attempt_count == 0
                and create.processing_owner_id is None
                and create.processing_fencing_token is None
                and create.processing_token is None
                and create.processing_expires_at is None
            ):
                create.status = EffectStatus.FAILED
                create.error_message = "no_resource"
                create.reconcile_after = None
                create.updated_at = now
        non_room: list[EffectSnapshot] = []
        room_creates: list[AiCallRuntimeEffectModel] = []
        for create in creates:
            if create.effect_type == "CREATE_ROOM":
                room_creates.append(create)
                continue
            non_room.append(await self.register(command_claim, _destroy_spec(create)))

        snapshots = list(non_room)
        prerequisites = tuple(effect.effect_id for effect in non_room)
        for create in room_creates:
            snapshots.append(
                await self.register(
                    command_claim,
                    _destroy_spec(create, prerequisite_effect_ids=prerequisites),
                )
            )
        return snapshots

    async def claim_next(self, owner_lease: OwnerLease) -> EffectClaim | None:
        now = await self._database_clock(self._session)
        eligible_status = or_(
            and_(
                AiCallRuntimeEffectModel.status == EffectStatus.PENDING,
                or_(
                    AiCallRuntimeEffectModel.reconcile_after.is_(None),
                    AiCallRuntimeEffectModel.reconcile_after <= now,
                ),
            ),
            and_(
                AiCallRuntimeEffectModel.status == EffectStatus.RECONCILE_REQUIRED,
                or_(
                    AiCallRuntimeEffectModel.reconcile_after.is_(None),
                    AiCallRuntimeEffectModel.reconcile_after <= now,
                ),
            ),
            and_(
                AiCallRuntimeEffectModel.status == EffectStatus.APPLYING,
                AiCallRuntimeEffectModel.processing_expires_at.is_not(None),
                AiCallRuntimeEffectModel.processing_expires_at <= now,
            ),
        )
        candidates = (
            await self._session.execute(
                select(
                    AiCallRuntimeEffectModel.id,
                    AiCallRuntimeEffectModel.effect_type,
                    AiCallRuntimeEffectModel.status,
                    AiCallRuntimeEffectModel.resource_key,
                    AiCallRuntimeEffectModel.resource_generation,
                    AiCallRuntimeEffectModel.attempt_count,
                    AiCallRecordModel.participant_identity,
                    AiCallRecordModel.answered_at,
                    AiCallRecordModel.terminal_requested_at,
                )
                .join(
                    AiCallRecordModel,
                    and_(
                        AiCallRecordModel.tenant_id
                        == AiCallRuntimeEffectModel.tenant_id,
                        AiCallRecordModel.call_id == AiCallRuntimeEffectModel.call_id,
                    ),
                )
                .where(
                    AiCallRuntimeEffectModel.tenant_id == owner_lease.tenant_id,
                    AiCallRuntimeEffectModel.call_id == owner_lease.call_id,
                    eligible_status,
                )
                .order_by(
                    AiCallRuntimeEffectModel.execution_phase,
                    AiCallRuntimeEffectModel.id,
                )
                .limit(16)
            )
        ).all()
        prerequisite = aliased(AiCallRuntimeEffectModel)
        for candidate in candidates:
            claim_gate = []
            if candidate.effect_type == "START_TRACK_EGRESS":
                participant_identity = str(candidate.participant_identity or "")
                first_attempt = (
                    candidate.status == EffectStatus.PENDING
                    and candidate.attempt_count == 0
                )
                if candidate.answered_at is None or (
                    first_attempt and candidate.terminal_requested_at is not None
                ):
                    continue
                try:
                    expected_resource_key = customer_track_keys(
                        owner_lease.call_id,
                        participant_identity,
                    )[2]
                except ValueError:
                    continue
                if (
                    candidate.resource_key != expected_resource_key
                    or candidate.resource_generation != 1
                ):
                    continue
                record_gate = [
                    AiCallRecordModel.tenant_id == owner_lease.tenant_id,
                    AiCallRecordModel.call_id == owner_lease.call_id,
                    AiCallRecordModel.participant_identity == participant_identity,
                    AiCallRecordModel.answered_at.is_not(None),
                ]
                if first_attempt:
                    record_gate.append(
                        AiCallRecordModel.terminal_requested_at.is_(None)
                    )
                claim_gate.extend(
                    (
                        AiCallRuntimeEffectModel.resource_key
                        == expected_resource_key,
                        AiCallRuntimeEffectModel.resource_generation == 1,
                        exists().where(*record_gate),
                    )
                )
            token = self._processing_token_generator()
            owner_valid = exists().where(
                AiCallRecordModel.tenant_id == owner_lease.tenant_id,
                AiCallRecordModel.call_id == owner_lease.call_id,
                AiCallRecordModel.runtime_owner_id == owner_lease.owner_id,
                AiCallRecordModel.runtime_fencing_token == owner_lease.fencing_token,
                AiCallRecordModel.runtime_lease_expires_at.is_not(None),
                AiCallRecordModel.runtime_lease_expires_at > func.clock_timestamp(),
                or_(
                    AiCallRecordModel.terminal_requested_at.is_(None),
                    AiCallRuntimeEffectModel.effect_type.in_(DESTROY_EFFECT_TYPES),
                    and_(
                        AiCallRuntimeEffectModel.effect_type.in_(CREATE_EFFECT_TYPES),
                        AiCallRuntimeEffectModel.status != EffectStatus.PENDING,
                    ),
                    and_(
                        AiCallRuntimeEffectModel.effect_type
                        == "START_TRACK_EGRESS",
                        AiCallRuntimeEffectModel.status == EffectStatus.PENDING,
                        AiCallRuntimeEffectModel.attempt_count > 0,
                    ),
                ),
            )
            unmet_dependency = exists(
                select(AiCallRuntimeEffectDependencyModel.id)
                .outerjoin(
                    prerequisite,
                    and_(
                        prerequisite.tenant_id
                        == AiCallRuntimeEffectDependencyModel.tenant_id,
                        prerequisite.id
                        == AiCallRuntimeEffectDependencyModel.prerequisite_effect_id,
                    ),
                )
                .where(
                    AiCallRuntimeEffectDependencyModel.tenant_id
                    == owner_lease.tenant_id,
                    AiCallRuntimeEffectDependencyModel.effect_id == candidate.id,
                    or_(
                        prerequisite.id.is_(None),
                        prerequisite.status
                        != AiCallRuntimeEffectDependencyModel.required_status,
                    ),
                )
            )
            row = (
                await self._session.execute(
                    update(AiCallRuntimeEffectModel)
                    .where(
                        AiCallRuntimeEffectModel.id == candidate.id,
                        AiCallRuntimeEffectModel.tenant_id == owner_lease.tenant_id,
                        AiCallRuntimeEffectModel.call_id == owner_lease.call_id,
                        eligible_status,
                        owner_valid,
                        ~unmet_dependency,
                        *claim_gate,
                    )
                    .values(
                        status=EffectStatus.APPLYING,
                        processing_owner_id=owner_lease.owner_id,
                        processing_fencing_token=owner_lease.fencing_token,
                        processing_token=token,
                        processing_expires_at=func.clock_timestamp()
                        + self._processing_lease_ttl,
                        attempt_count=AiCallRuntimeEffectModel.attempt_count + 1,
                        updated_at=func.clock_timestamp(),
                    )
                    .returning(
                        AiCallRuntimeEffectModel.id,
                        AiCallRuntimeEffectModel.tenant_id,
                        AiCallRuntimeEffectModel.call_id,
                        AiCallRuntimeEffectModel.effect_type,
                        AiCallRuntimeEffectModel.processing_owner_id,
                        AiCallRuntimeEffectModel.processing_fencing_token,
                        AiCallRuntimeEffectModel.processing_token,
                        AiCallRuntimeEffectModel.processing_expires_at,
                        AiCallRuntimeEffectModel.source_create_effect_id,
                        AiCallRuntimeEffectModel.create_protection_deadline_at,
                        AiCallRuntimeEffectModel.attempt_count,
                        AiCallRuntimeEffectModel.provider_namespace,
                        AiCallRuntimeEffectModel.resource_key,
                    )
                )
            ).one_or_none()
            if row is not None:
                reservation_token = await self._session.scalar(
                    select(AiCallSipLineReservationModel.reservation_token).where(
                        AiCallSipLineReservationModel.tenant_id == owner_lease.tenant_id,
                        AiCallSipLineReservationModel.call_id == owner_lease.call_id,
                        AiCallSipLineReservationModel.status != "RELEASED",
                    )
                )
                return EffectClaim(
                    effect_id=row.id,
                    tenant_id=row.tenant_id,
                    call_id=row.call_id,
                    effect_type=row.effect_type,
                    processing_owner_id=row.processing_owner_id,
                    processing_fencing_token=row.processing_fencing_token,
                    processing_token=row.processing_token,
                    processing_expires_at=row.processing_expires_at,
                    source_create_effect_id=row.source_create_effect_id,
                    create_protection_deadline_at=row.create_protection_deadline_at,
                    attempt_count=row.attempt_count,
                    reconcile_only=(
                        candidate.effect_type in CREATE_EFFECT_TYPES
                        and (
                            candidate.status != EffectStatus.PENDING
                            or (
                                candidate.effect_type == "START_TRACK_EGRESS"
                                and candidate.attempt_count > 0
                            )
                        )
                    ),
                    provider_namespace=row.provider_namespace,
                    resource_key=row.resource_key,
                    reservation_token=reservation_token,
                )
        return None

    async def submit(
        self,
        claim: EffectClaim,
        observation: ProviderObservation,
    ) -> bool:
        record = await self._session.scalar(
            select(AiCallRecordModel)
            .where(
                AiCallRecordModel.tenant_id == claim.tenant_id,
                AiCallRecordModel.call_id == claim.call_id,
            )
            .with_for_update()
        )
        if (
            record is None
            or record.runtime_owner_id != claim.processing_owner_id
            or record.runtime_fencing_token != claim.processing_fencing_token
            or record.runtime_lease_expires_at is None
        ):
            return False

        effect_ids = sorted(
            {claim.effect_id}
            | (
                {claim.source_create_effect_id}
                if claim.source_create_effect_id is not None
                else set()
            )
        )
        effects = {
            effect.id: effect
            for effect in (
                await self._session.scalars(
                    select(AiCallRuntimeEffectModel)
                    .where(
                        AiCallRuntimeEffectModel.tenant_id == claim.tenant_id,
                        AiCallRuntimeEffectModel.id.in_(effect_ids),
                    )
                    .order_by(AiCallRuntimeEffectModel.id)
                    .with_for_update()
                )
            ).all()
        }
        effect = effects.get(claim.effect_id)
        if (
            effect is None
            or effect.status != EffectStatus.APPLYING
            or effect.processing_owner_id != claim.processing_owner_id
            or effect.processing_fencing_token != claim.processing_fencing_token
            or effect.processing_token != claim.processing_token
            or effect.processing_expires_at is None
        ):
            return False

        reservation = None
        if claim.reservation_token is not None and effect.effect_type in {
            "CREATE_SIP_PARTICIPANT",
            "HANGUP_SIP",
        }:
            reservation = await self._session.scalar(
                select(AiCallSipLineReservationModel)
                .where(
                    AiCallSipLineReservationModel.tenant_id == claim.tenant_id,
                    AiCallSipLineReservationModel.call_id == claim.call_id,
                    AiCallSipLineReservationModel.reservation_token
                    == claim.reservation_token,
                    AiCallSipLineReservationModel.fencing_token
                    == claim.processing_fencing_token,
                    AiCallSipLineReservationModel.status != "RELEASED",
                )
                .with_for_update()
            )
            if reservation is None:
                return False

        now = await self._database_clock(self._session)
        if record.runtime_lease_expires_at <= now or effect.processing_expires_at <= now:
            return False

        source = None
        if effect.effect_type in CREATE_EFFECT_TYPES:
            self._apply_create_observation(effect, observation, now)
        else:
            source = effects.get(effect.source_create_effect_id)
            if source is None:
                raise EffectRegistrationError("destroy effect has no source create effect")
            self._apply_destroy_observation(effect, source, observation, now)
        self._apply_reservation_observation(reservation, effect, observation, now)
        await self._recording_repository.project(
            record=record,
            effect=effect,
            source_effect=source,
            observation=observation,
            now=now,
        )
        await self._session.flush()
        return True

    @staticmethod
    def _apply_reservation_observation(
        reservation: AiCallSipLineReservationModel | None,
        effect: AiCallRuntimeEffectModel,
        observation: ProviderObservation,
        now: datetime,
    ) -> None:
        if reservation is None:
            return
        if effect.effect_type == "CREATE_SIP_PARTICIPANT":
            if (
                observation.kind == ProviderObservationKind.RESOURCE_PRESENT
                and effect.status == EffectStatus.APPLIED
                and effect.provider_reference
            ):
                if reservation.status in {"RESERVED", "RECONCILE_REQUIRED"}:
                    reservation.status = "ACTIVE"
                    reservation.reconcile_after = None
            elif observation.kind == ProviderObservationKind.PERMANENT_NO_RESOURCE:
                reservation.status = "RELEASED"
                reservation.released_at = now
                reservation.reconcile_after = None
            elif observation.kind in {
                ProviderObservationKind.ACCEPTED,
                ProviderObservationKind.RETRYABLE_FAILURE,
                ProviderObservationKind.UNCERTAIN,
                ProviderObservationKind.RESOURCE_PRESENT,
            }:
                reservation.status = "RECONCILE_REQUIRED"
                reservation.reconcile_after = now + observation.retry_after
        elif effect.effect_type == "HANGUP_SIP":
            if effect.status == EffectStatus.APPLIED:
                reservation.status = "RELEASED"
                reservation.released_at = now
                reservation.reconcile_after = None
            elif observation.kind == ProviderObservationKind.UNCERTAIN:
                reservation.status = "RECONCILE_REQUIRED"
                reservation.reconcile_after = now + observation.retry_after
        reservation.updated_at = now

    async def mark_cleanup_clean(self, owner_lease: OwnerLease) -> bool:
        record = await self._session.scalar(
            select(AiCallRecordModel)
            .where(
                AiCallRecordModel.tenant_id == owner_lease.tenant_id,
                AiCallRecordModel.call_id == owner_lease.call_id,
            )
            .with_for_update()
        )
        if (
            record is None
            or record.runtime_owner_id != owner_lease.owner_id
            or record.runtime_fencing_token != owner_lease.fencing_token
            or record.runtime_lease_expires_at is None
            or record.terminal_requested_at is None
        ):
            return False
        if (
            record.runtime_control_mode == "owner_command_v1"
            and record.dialogue_persistence_status == "pending"
        ):
            return False
        worker = await self._session.scalar(
            select(AiCallRuntimeWorkerModel)
            .where(AiCallRuntimeWorkerModel.worker_id == owner_lease.owner_id)
            .with_for_update()
        )
        if worker is None:
            return False
        effects = list(
            (
                await self._session.scalars(
                    select(AiCallRuntimeEffectModel)
                    .where(
                        AiCallRuntimeEffectModel.tenant_id == owner_lease.tenant_id,
                        AiCallRuntimeEffectModel.call_id == owner_lease.call_id,
                    )
                    .order_by(AiCallRuntimeEffectModel.id)
                    .with_for_update()
                )
            ).all()
        )
        creates = {effect.id: effect for effect in effects if _is_create(effect)}
        destroys = [effect for effect in effects if _is_destroy(effect)]
        now = await self._database_clock(self._session)
        for create in creates.values():
            if not _create_is_quiet(create, now):
                return False
            related = [
                effect for effect in destroys if effect.source_create_effect_id == create.id
            ]
            if create.status == EffectStatus.FAILED and create.error_message == "no_resource":
                if any(effect.status != EffectStatus.APPLIED for effect in related):
                    return False
            elif not related or any(
                effect.status != EffectStatus.APPLIED
                or effect.terminal_confirmed_at is None
                for effect in related
            ):
                return False
        unreleased_reservation = await self._session.scalar(
            select(
                exists().where(
                    AiCallSipLineReservationModel.tenant_id == owner_lease.tenant_id,
                    AiCallSipLineReservationModel.call_id == owner_lease.call_id,
                    AiCallSipLineReservationModel.status != "RELEASED",
                )
            )
        )
        if unreleased_reservation:
            return False

        now = await self._database_clock(self._session)
        if record.runtime_lease_expires_at <= now:
            return False

        if record.runtime_capacity_class == "active" and worker.active_call_count > 0:
            worker.active_call_count -= 1
        elif (
            record.runtime_capacity_class == "cleanup"
            and worker.active_cleanup_count > 0
        ):
            worker.active_cleanup_count -= 1
        worker.updated_at = now
        record.runtime_capacity_class = "none"
        record.runtime_owner_id = None
        record.runtime_lease_expires_at = None
        record.resource_cleanup_status = "clean"
        record.resource_cleanup_error = None
        record.resource_cleanup_next_retry_at = None
        record.resource_cleanup_completed_at = now
        await self._session.flush()
        return True

    async def _authorize_first_registration(
        self,
        claim: CommandClaim,
    ) -> tuple[AiCallRecordModel, AiCallRuntimeCommandModel]:
        record = await self._session.scalar(
            select(AiCallRecordModel)
            .where(
                AiCallRecordModel.tenant_id == claim.tenant_id,
                AiCallRecordModel.call_id == claim.call_id,
            )
            .with_for_update()
        )
        if (
            record is None
            or record.runtime_owner_id != claim.processing_owner_id
            or record.runtime_fencing_token != claim.processing_fencing_token
            or record.runtime_lease_expires_at is None
        ):
            raise EffectRegistrationError("command claim does not hold the Record owner")
        command = await self._session.scalar(
            select(AiCallRuntimeCommandModel)
            .where(
                AiCallRuntimeCommandModel.tenant_id == claim.tenant_id,
                AiCallRuntimeCommandModel.call_id == claim.call_id,
                AiCallRuntimeCommandModel.id == claim.command_id,
            )
            .with_for_update()
        )
        if (
            command is None
            or command.status != CommandStatus.PROCESSING
            or command.processing_owner_id != claim.processing_owner_id
            or command.processing_fencing_token != claim.processing_fencing_token
            or command.processing_token != claim.processing_token
            or command.processing_expires_at is None
        ):
            raise EffectRegistrationError("source Command processing token is not valid")
        return record, command

    async def _validate_source_create(
        self,
        claim: CommandClaim,
        spec: EffectSpec,
    ) -> AiCallRuntimeEffectModel | None:
        if spec.effect_type in CREATE_EFFECT_TYPES:
            if spec.source_create_effect_id is not None:
                raise EffectRegistrationError("create effect cannot reference a source create")
            return None
        if spec.source_create_effect_id is None:
            raise EffectRegistrationError("destroy effect must reference its source create")
        source = await self._session.scalar(
            select(AiCallRuntimeEffectModel)
            .where(
                AiCallRuntimeEffectModel.tenant_id == claim.tenant_id,
                AiCallRuntimeEffectModel.call_id == claim.call_id,
                AiCallRuntimeEffectModel.id == spec.source_create_effect_id,
            )
            .with_for_update()
        )
        if (
            source is None
            or source.effect_type not in CREATE_EFFECT_TYPES
            or source.provider_namespace != spec.provider_namespace
            or source.resource_generation != spec.resource_generation
            or source.resource_key != spec.resource_key
            or _DESTROY_FOR_CREATE[source.effect_type] != spec.effect_type
            or source.reconcile_deadline_at != spec.create_protection_deadline_at
        ):
            raise EffectRegistrationError("destroy effect does not match its source create")
        return source

    async def _validate_dependency_graph(
        self,
        claim: CommandClaim,
        spec: EffectSpec,
    ) -> None:
        expected_phase = (
            20
            if spec.effect_type == "DELETE_ROOM"
            else 10
            if spec.effect_type in DESTROY_EFFECT_TYPES
            else 0
        )
        if spec.execution_phase != expected_phase:
            raise EffectRegistrationError("effect execution phase is not canonical")

        prerequisite_ids = tuple(sorted(set(spec.prerequisite_effect_ids)))
        if len(prerequisite_ids) != len(spec.prerequisite_effect_ids):
            raise EffectRegistrationError("effect prerequisites must be unique")
        if spec.effect_type != "DELETE_ROOM":
            if prerequisite_ids:
                raise EffectRegistrationError(
                    "only DELETE_ROOM may declare effect prerequisites"
                )
            return

        prerequisites = list(
            (
                await self._session.scalars(
                    select(AiCallRuntimeEffectModel).where(
                        AiCallRuntimeEffectModel.tenant_id == claim.tenant_id,
                        AiCallRuntimeEffectModel.call_id == claim.call_id,
                        AiCallRuntimeEffectModel.id.in_(prerequisite_ids),
                    )
                )
            ).all()
        )
        if {effect.id for effect in prerequisites} != set(prerequisite_ids):
            raise EffectRegistrationError(
                "effect prerequisite is missing or belongs to another call"
            )
        if any(
            effect.effect_type not in DESTROY_EFFECT_TYPES - {"DELETE_ROOM"}
            or effect.source_create_effect_id is None
            for effect in prerequisites
        ):
            raise EffectRegistrationError(
                "DELETE_ROOM prerequisites must be non-room destroy effects"
            )

        expected_source_ids = set(
            await self._session.scalars(
                select(AiCallRuntimeEffectModel.id).where(
                    AiCallRuntimeEffectModel.tenant_id == claim.tenant_id,
                    AiCallRuntimeEffectModel.call_id == claim.call_id,
                    AiCallRuntimeEffectModel.effect_type.in_(
                        CREATE_EFFECT_TYPES - {"CREATE_ROOM"}
                    ),
                )
            )
        )
        prerequisite_source_ids = {
            effect.source_create_effect_id for effect in prerequisites
        }
        if prerequisite_source_ids != expected_source_ids:
            raise EffectRegistrationError(
                "DELETE_ROOM prerequisites do not cover the complete destroy graph"
            )

    async def _find_by_idempotency(
        self, tenant_id: str, idempotency_key: str
    ) -> AiCallRuntimeEffectModel | None:
        return await self._session.scalar(
            select(AiCallRuntimeEffectModel).where(
                AiCallRuntimeEffectModel.tenant_id == tenant_id,
                AiCallRuntimeEffectModel.idempotency_key == idempotency_key,
            )
        )

    @staticmethod
    def _validate_effect_type(spec: EffectSpec) -> None:
        if spec.effect_type not in CREATE_EFFECT_TYPES | DESTROY_EFFECT_TYPES:
            raise EffectRegistrationError(f"unsupported effect type: {spec.effect_type}")

    async def _validate_existing(
        self,
        effect: AiCallRuntimeEffectModel,
        claim: CommandClaim,
        spec: EffectSpec,
    ) -> None:
        await self._validate_dependency_graph(claim, spec)
        prerequisite_ids = tuple(
            sorted(
                await self._session.scalars(
                    select(
                        AiCallRuntimeEffectDependencyModel.prerequisite_effect_id
                    ).where(
                        AiCallRuntimeEffectDependencyModel.tenant_id
                        == claim.tenant_id,
                        AiCallRuntimeEffectDependencyModel.effect_id == effect.id,
                    )
                )
            )
        )
        expected = (
            claim.call_id,
            spec.effect_type,
            spec.provider_namespace,
            spec.provider_idempotency_key,
            spec.resource_key,
            spec.resource_generation,
            spec.source_create_effect_id,
            spec.create_protection_deadline_at,
            spec.reconcile_deadline_at,
            spec.execution_phase,
            tuple(sorted(spec.prerequisite_effect_ids)),
        )
        actual = (
            effect.call_id,
            effect.effect_type,
            effect.provider_namespace,
            effect.provider_idempotency_key,
            effect.resource_key,
            effect.resource_generation,
            effect.source_create_effect_id,
            effect.create_protection_deadline_at,
            effect.reconcile_deadline_at,
            effect.execution_phase,
            prerequisite_ids,
        )
        if actual != expected:
            raise EffectRegistrationError(
                "effect idempotency key already exists with a different specification"
            )

    def _apply_create_observation(
        self,
        effect: AiCallRuntimeEffectModel,
        observation: ProviderObservation,
        now: datetime,
    ) -> None:
        if (
            observation.kind == ProviderObservationKind.RESOURCE_PRESENT
            and observation.provider_reference
        ):
            effect.status = EffectStatus.APPLIED
            effect.provider_reference = observation.provider_reference
        elif observation.kind == ProviderObservationKind.RESOURCE_PRESENT:
            effect.status = EffectStatus.RECONCILE_REQUIRED
            effect.reconcile_after = now + observation.retry_after
            effect.error_message = "resource_reference_missing"
        elif observation.kind == ProviderObservationKind.PERMANENT_NO_RESOURCE:
            effect.status = EffectStatus.FAILED
            effect.error_message = "no_resource"
        elif observation.kind == ProviderObservationKind.RETRYABLE_FAILURE:
            effect.status = EffectStatus.PENDING
            effect.reconcile_after = now + observation.retry_after
            effect.error_message = observation.error_message
        else:
            effect.status = EffectStatus.RECONCILE_REQUIRED
            effect.reconcile_after = now + observation.retry_after
            effect.error_message = observation.error_message
        _clear_processing(effect)
        effect.updated_at = now

    def _apply_destroy_observation(
        self,
        effect: AiCallRuntimeEffectModel,
        source: AiCallRuntimeEffectModel,
        observation: ProviderObservation,
        now: datetime,
    ) -> None:
        quiet = _create_is_quiet(source, now) and (
            effect.create_protection_deadline_at is None
            or now >= effect.create_protection_deadline_at
            or source.status in {EffectStatus.APPLIED, EffectStatus.FAILED}
        )
        if observation.kind == ProviderObservationKind.RETRYABLE_FAILURE:
            effect.status = EffectStatus.PENDING
            effect.reconcile_after = now + observation.retry_after
            effect.error_message = observation.error_message
            effect.absence_observation_count = 0
        elif not quiet:
            effect.status = EffectStatus.RECONCILE_REQUIRED
            effect.reconcile_after = now + observation.retry_after
            effect.absence_observation_count = 0
        elif observation.kind == ProviderObservationKind.RESOURCE_ABSENT:
            effect.absence_observation_count += 1
            if effect.absence_observation_count >= self._required_absence_observations:
                effect.status = EffectStatus.APPLIED
                effect.absence_confirmed_at = now
                effect.terminal_confirmed_at = now
            else:
                effect.status = EffectStatus.RECONCILE_REQUIRED
                effect.reconcile_after = now + observation.retry_after
        elif observation.kind == ProviderObservationKind.TERMINAL_CONFIRMED:
            effect.status = EffectStatus.APPLIED
            effect.terminal_confirmed_at = now
        else:
            effect.status = EffectStatus.RECONCILE_REQUIRED
            effect.reconcile_after = now + observation.retry_after
            effect.error_message = observation.error_message
            effect.absence_observation_count = 0
        _clear_processing(effect)
        effect.updated_at = now


def _destroy_spec(
    create: AiCallRuntimeEffectModel,
    *,
    prerequisite_effect_ids: tuple[int, ...] = (),
) -> EffectSpec:
    destroy_type = _DESTROY_FOR_CREATE[create.effect_type]
    provider_key_material = (
        f"{create.provider_namespace}|{create.provider_idempotency_key}|{destroy_type}"
    )
    provider_key_hash = hashlib.sha256(provider_key_material.encode()).hexdigest()
    return EffectSpec(
        effect_type=destroy_type,
        idempotency_key=f"end:{create.call_id}:{destroy_type}:{create.id}",
        provider_namespace=create.provider_namespace,
        provider_idempotency_key=f"destroy:{provider_key_hash}",
        resource_key=create.resource_key,
        resource_generation=create.resource_generation,
        source_create_effect_id=create.id,
        create_protection_deadline_at=create.reconcile_deadline_at,
        execution_phase=20 if destroy_type == "DELETE_ROOM" else 10,
        prerequisite_effect_ids=prerequisite_effect_ids,
    )


def _create_is_quiet(effect: AiCallRuntimeEffectModel, now: datetime) -> bool:
    return (
        effect.status == EffectStatus.APPLIED
        or (
            effect.status == EffectStatus.FAILED
            and effect.error_message == "no_resource"
        )
        or (
            effect.reconcile_deadline_at is not None
            and now >= effect.reconcile_deadline_at
        )
    )


def _is_create(effect: AiCallRuntimeEffectModel) -> bool:
    return effect.effect_type in CREATE_EFFECT_TYPES


def _is_destroy(effect: AiCallRuntimeEffectModel) -> bool:
    return effect.effect_type in DESTROY_EFFECT_TYPES


def _clear_processing(effect: AiCallRuntimeEffectModel) -> None:
    effect.processing_owner_id = None
    effect.processing_fencing_token = None
    effect.processing_token = None
    effect.processing_expires_at = None


def _effect_snapshot(effect: AiCallRuntimeEffectModel) -> EffectSnapshot:
    return EffectSnapshot(
        effect_id=effect.id,
        tenant_id=effect.tenant_id,
        call_id=effect.call_id,
        command_id=effect.command_id,
        effect_type=effect.effect_type,
        status=effect.status,
        resource_key=effect.resource_key,
        resource_generation=effect.resource_generation,
        source_create_effect_id=effect.source_create_effect_id,
    )
