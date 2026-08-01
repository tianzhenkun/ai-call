from __future__ import annotations

import hashlib
import json
import secrets
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import and_, exists, func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import AiCallRecordModel
from app.services.ai_call.runtime_control.models import (
    AiCallEndEvidenceModel,
    AiCallRuntimeCommandModel,
    AiCallRuntimeEffectModel,
    AiCallSipLineReservationModel,
)
from app.services.ai_call.runtime_control.owner_repository import OwnerLease
from app.services.ai_call.runtime_control.timing import read_database_time
from app.services.ai_call.runtime_control.types import CommandStatus
from app.utils.id_util import generate_snowflake_id

START_CALL = "START_CALL"
END_CALL = "END_CALL"

_PREEMPTIBLE_COMMAND_STATUSES = frozenset(
    {
        CommandStatus.PENDING,
        CommandStatus.DISPATCHING,
        CommandStatus.PUBLISHED,
        CommandStatus.PROCESSING,
        CommandStatus.RETRY_WAIT,
    }
)
_RECORD_TERMINAL_STATUSES = frozenset({"completed", "failed"})


class RuntimeCommandRepositoryError(RuntimeError):
    pass


class IdempotencyConflictError(RuntimeCommandRepositoryError):
    pass


class RuntimeRecordNotFoundError(RuntimeCommandRepositoryError):
    pass


class TerminalBarrierError(RuntimeCommandRepositoryError):
    pass


class InvalidCommandIntentError(RuntimeCommandRepositoryError):
    pass


class InvalidCommandDecisionError(RuntimeCommandRepositoryError):
    pass


@dataclass(frozen=True, slots=True)
class StartCallIntent:
    tenant_id: str
    entry_type: str
    idempotency_key: str
    payload: Mapping[str, object]
    business_type: str | None = None
    business_id: str | None = None
    scene_code: str | None = None
    prompt_source_key: str | None = None
    allocation_deadline_at: datetime | None = None
    sensitive_payload_ciphertext: str | None = None
    payload_key_version: str | None = None


@dataclass(frozen=True, slots=True)
class CommandIntent:
    tenant_id: str
    call_id: str
    command_type: str
    idempotency_key: str
    payload: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class EndCallIntent:
    tenant_id: str
    call_id: str
    source: str
    end_reason: str
    dedupe_key: str
    provider: str | None = None
    provider_namespace: str | None = None
    provider_event_id: str | None = None
    event_at: datetime | None = None
    evidence: Mapping[str, object] | None = None


@dataclass(frozen=True, slots=True)
class CommandSnapshot:
    command_id: int
    tenant_id: str
    call_id: str
    command_seq: int
    command_type: str
    idempotency_key: str
    request_fingerprint: str
    status: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class EndCallDecision:
    command_id: int
    evidence_id: int
    call_id: str
    command_seq: int
    terminal_requested_at: datetime


@dataclass(frozen=True, slots=True)
class CommandClaim:
    command_id: int
    tenant_id: str
    call_id: str
    command_seq: int
    command_type: str
    processing_owner_id: str
    processing_fencing_token: int
    processing_token: str
    processing_expires_at: datetime
    payload_json: str | None
    attempt_count: int


@dataclass(frozen=True, slots=True)
class CommandDecision:
    status: CommandStatus
    result: Mapping[str, object] | None = None
    error_message: str | None = None
    retry_after: timedelta | None = None


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def canonical_request_fingerprint(parts: Mapping[str, object]) -> str:
    encoded = _canonical_json(parts)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def start_call_request_fingerprint(request: StartCallIntent) -> str:
    return canonical_request_fingerprint(
        {
            "tenant_id": request.tenant_id,
            "command_type": START_CALL,
            "entry_type": request.entry_type,
            "payload": request.payload,
        }
    )


def command_request_fingerprint(request: CommandIntent) -> str:
    return canonical_request_fingerprint(
        {
            "tenant_id": request.tenant_id,
            "call_id": request.call_id,
            "command_type": request.command_type,
            "payload": request.payload,
        }
    )


def end_call_request_fingerprint(request: EndCallIntent) -> str:
    return canonical_request_fingerprint(
        {
            "tenant_id": request.tenant_id,
            "call_id": request.call_id,
            "command_type": END_CALL,
        }
    )


class RuntimeCommandRepository:
    def __init__(
        self,
        session: AsyncSession,
        *,
        id_generator: Callable[[], int] = generate_snowflake_id,
        processing_token_generator: Callable[[], str] = lambda: secrets.token_urlsafe(24),
        processing_lease_ttl: timedelta = timedelta(seconds=30),
        database_clock: Callable[[AsyncSession], Awaitable[datetime]] = read_database_time,
    ) -> None:
        self._session = session
        self._id_generator = id_generator
        self._processing_token_generator = processing_token_generator
        self._processing_lease_ttl = processing_lease_ttl
        self._database_clock = database_clock

    async def create_start_call(self, request: StartCallIntent) -> CommandSnapshot:
        fingerprint = start_call_request_fingerprint(request)
        existing = await self._find_by_idempotency(
            tenant_id=request.tenant_id,
            idempotency_key=request.idempotency_key,
        )
        if existing is not None:
            return self._matching_snapshot(existing, fingerprint)

        now = await self._database_clock(self._session)
        call_id = f"call_{self._id_generator()}"
        record = AiCallRecordModel(
            id=self._id_generator(),
            tenant_id=request.tenant_id,
            call_id=call_id,
            business_type=request.business_type,
            business_id=request.business_id,
            scene_code=request.scene_code,
            prompt_source_key=request.prompt_source_key,
            entry_type=request.entry_type,
            room_name=f"ai-call-{call_id}",
            participant_identity=f"caller-{call_id}",
            status="preparing",
            started_at=now,
            runtime_control_mode="owner_command_v1",
            next_command_seq=2,
        )
        command = AiCallRuntimeCommandModel(
            id=self._id_generator(),
            tenant_id=request.tenant_id,
            call_id=call_id,
            command_seq=1,
            command_type=START_CALL,
            idempotency_key=request.idempotency_key,
            request_fingerprint=fingerprint,
            dispatch_priority=100,
            allocation_deadline_at=request.allocation_deadline_at,
            payload_json=_canonical_json(request.payload),
            sensitive_payload_ciphertext=request.sensitive_payload_ciphertext,
            payload_key_version=request.payload_key_version,
            status=CommandStatus.PENDING,
            created_at=now,
            updated_at=now,
        )

        try:
            async with self._session.begin_nested():
                self._session.add_all((record, command))
                await self._session.flush()
        except IntegrityError:
            winner = await self._find_by_idempotency(
                tenant_id=request.tenant_id,
                idempotency_key=request.idempotency_key,
            )
            if winner is None:
                raise
            return self._matching_snapshot(winner, fingerprint)

        return self._snapshot(command)

    async def append_command(self, request: CommandIntent) -> CommandSnapshot:
        if request.command_type in {START_CALL, END_CALL}:
            raise InvalidCommandIntentError(
                f"{request.command_type} must use its dedicated repository method"
            )

        fingerprint = command_request_fingerprint(request)
        existing = await self._find_by_idempotency(
            tenant_id=request.tenant_id,
            idempotency_key=request.idempotency_key,
        )
        if existing is not None:
            return self._matching_snapshot(existing, fingerprint)

        record = await self._lock_record(request.tenant_id, request.call_id)
        if record.terminal_requested_at is not None:
            raise TerminalBarrierError(
                f"call {request.call_id} already has a terminal barrier"
            )

        now = await self._database_clock(self._session)
        command = AiCallRuntimeCommandModel(
            id=self._id_generator(),
            tenant_id=request.tenant_id,
            call_id=request.call_id,
            command_seq=record.next_command_seq,
            command_type=request.command_type,
            idempotency_key=request.idempotency_key,
            request_fingerprint=fingerprint,
            dispatch_priority=100,
            payload_json=_canonical_json(request.payload),
            expected_fencing_token=record.runtime_fencing_token,
            target_owner_id=record.runtime_owner_id,
            status=CommandStatus.PENDING,
            created_at=now,
            updated_at=now,
        )

        try:
            async with self._session.begin_nested():
                record.next_command_seq += 1
                self._session.add(command)
                await self._session.flush()
        except IntegrityError:
            winner = await self._find_by_idempotency(
                tenant_id=request.tenant_id,
                idempotency_key=request.idempotency_key,
            )
            if winner is None:
                raise
            return self._matching_snapshot(winner, fingerprint)

        return self._snapshot(command)

    async def request_end(self, request: EndCallIntent) -> EndCallDecision:
        record = await self._lock_record(request.tenant_id, request.call_id)
        now = await self._database_clock(self._session)
        fingerprint = end_call_request_fingerprint(request)

        command = await self._lock_end_command(request.tenant_id, request.call_id)
        if command is None:
            command = AiCallRuntimeCommandModel(
                id=self._id_generator(),
                tenant_id=request.tenant_id,
                call_id=request.call_id,
                command_seq=record.next_command_seq,
                command_type=END_CALL,
                idempotency_key=f"end:{request.call_id}",
                request_fingerprint=fingerprint,
                dispatch_priority=0,
                payload_json=None,
                expected_fencing_token=record.runtime_fencing_token,
                target_owner_id=record.runtime_owner_id,
                status=CommandStatus.PENDING,
                created_at=now,
                updated_at=now,
            )
            record.next_command_seq += 1
            self._session.add(command)
            await self._session.flush()
        else:
            self._matching_snapshot(command, fingerprint)

        if record.terminal_requested_at is None:
            record.terminal_requested_at = now
        if record.status not in _RECORD_TERMINAL_STATUSES:
            record.status = "ending"
        if record.resource_cleanup_status == "not_started":
            record.resource_cleanup_status = "reconciling"
        if record.end_reason is None:
            record.end_reason = request.end_reason

        await self._preempt_ordinary_commands(
            tenant_id=request.tenant_id,
            call_id=request.call_id,
            end_command=command,
            now=now,
        )
        record.last_applied_command_seq = max(
            record.last_applied_command_seq,
            command.command_seq - 1,
        )

        evidence = await self._append_end_evidence(
            request=request,
            command_id=command.id,
            received_at=now,
        )
        await self._session.flush()

        terminal_requested_at = record.terminal_requested_at
        if terminal_requested_at is None:
            raise RuntimeError("terminal barrier was not established")
        return EndCallDecision(
            command_id=command.id,
            evidence_id=evidence.id,
            call_id=request.call_id,
            command_seq=command.command_seq,
            terminal_requested_at=terminal_requested_at,
        )

    async def claim_next_for_owner(self, lease: OwnerLease) -> CommandClaim | None:
        return await self._claim_for_owner(lease, end_only=False)

    async def claim_pending_end(self, lease: OwnerLease) -> CommandClaim | None:
        return await self._claim_for_owner(lease, end_only=True)

    async def complete(
        self,
        claim: CommandClaim,
        decision: CommandDecision,
    ) -> bool:
        status = CommandStatus(decision.status)
        allowed_statuses = {
            CommandStatus.SUCCEEDED,
            CommandStatus.RETRY_WAIT,
            CommandStatus.DEAD,
            CommandStatus.SUPERSEDED,
            CommandStatus.CANCELED,
        }
        if status not in allowed_statuses:
            raise InvalidCommandDecisionError(f"unsupported completion status: {status}")
        if claim.command_type == END_CALL and status == CommandStatus.DEAD:
            raise InvalidCommandDecisionError("END_CALL cannot transition to DEAD")

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
            or (
                claim.command_type != END_CALL
                and record.terminal_requested_at is not None
            )
        ):
            return False

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
            return False

        now = await self._database_clock(self._session)
        if (
            record.runtime_lease_expires_at <= now
            or command.processing_expires_at <= now
        ):
            return False

        if status == CommandStatus.DEAD and await self._command_has_effect(command):
            raise InvalidCommandDecisionError(
                "a command with registered effects cannot be completed as DEAD"
            )

        is_terminal = status != CommandStatus.RETRY_WAIT
        if is_terminal:
            expected_sequence = record.last_applied_command_seq + 1
            if command.command_seq != expected_sequence:
                return False
            record.last_applied_command_seq = command.command_seq

        command.status = status
        command.processing_owner_id = None
        command.processing_fencing_token = None
        command.processing_token = None
        command.processing_expires_at = None
        command.result_json = (
            _canonical_json(decision.result) if decision.result is not None else None
        )
        command.error_message = decision.error_message
        command.updated_at = now
        if status == CommandStatus.RETRY_WAIT:
            retry_after = decision.retry_after or timedelta(0)
            if retry_after.total_seconds() < 0:
                raise InvalidCommandDecisionError("retry_after must not be negative")
            command.next_retry_at = now + retry_after
            command.finished_at = None
        else:
            command.next_retry_at = None
            command.finished_at = now
        await self._session.flush()
        return True

    async def expire_unallocated_start(self, tenant_id: str, call_id: str) -> bool:
        """Fail a START_CALL whose persisted allocation deadline passed untouched."""
        record = await self._lock_record(tenant_id, call_id)
        if (
            record.runtime_control_mode != "owner_command_v1"
            or record.runtime_owner_id is not None
            or record.runtime_fencing_token != 0
            or record.runtime_lease_expires_at is not None
            or record.runtime_capacity_class != "none"
            or record.terminal_requested_at is not None
        ):
            return False

        command = await self._session.scalar(
            select(AiCallRuntimeCommandModel)
            .where(
                AiCallRuntimeCommandModel.tenant_id == tenant_id,
                AiCallRuntimeCommandModel.call_id == call_id,
                AiCallRuntimeCommandModel.command_type == START_CALL,
            )
            .with_for_update()
        )
        if (
            command is None
            or command.status not in {CommandStatus.PENDING, CommandStatus.RETRY_WAIT}
            or command.allocation_deadline_at is None
            or await self._call_has_effect(tenant_id, call_id)
            or await self._has_unreleased_reservation(tenant_id, call_id)
        ):
            return False

        now = await self._database_clock(self._session)
        if command.allocation_deadline_at > now:
            return False

        command.status = CommandStatus.DEAD
        command.target_owner_id = None
        command.expected_fencing_token = None
        command.dispatch_token = None
        command.dispatch_expires_at = None
        command.processing_owner_id = None
        command.processing_fencing_token = None
        command.processing_token = None
        command.processing_expires_at = None
        command.next_retry_at = None
        command.finished_at = now
        command.error_message = "ALLOCATION_TIMEOUT"
        command.result_json = _canonical_json({"error": "ALLOCATION_TIMEOUT"})
        command.updated_at = now
        record.last_applied_command_seq = max(
            record.last_applied_command_seq,
            command.command_seq,
        )
        record.status = "failed"
        record.failure_stage = "allocation"
        record.failure_message = "ALLOCATION_TIMEOUT"
        record.ended_at = now
        record.resource_cleanup_status = "clean"
        record.resource_cleanup_error = None
        record.resource_cleanup_next_retry_at = None
        record.resource_cleanup_completed_at = now
        await self._session.flush()
        return True

    async def _claim_for_owner(
        self,
        lease: OwnerLease,
        *,
        end_only: bool,
    ) -> CommandClaim | None:
        now = await self._database_clock(self._session)
        due_status = or_(
            AiCallRuntimeCommandModel.status == CommandStatus.PENDING,
            and_(
                AiCallRuntimeCommandModel.status == CommandStatus.RETRY_WAIT,
                AiCallRuntimeCommandModel.next_retry_at.is_not(None),
                AiCallRuntimeCommandModel.next_retry_at <= now,
            ),
        )
        command_type_condition = (
            AiCallRuntimeCommandModel.command_type == END_CALL
            if end_only
            else AiCallRuntimeCommandModel.command_type != END_CALL
        )
        candidates = (
            await self._session.execute(
                select(
                    AiCallRuntimeCommandModel.id,
                    AiCallRuntimeCommandModel.command_type,
                    AiCallRuntimeCommandModel.status,
                )
                .where(
                    AiCallRuntimeCommandModel.tenant_id == lease.tenant_id,
                    AiCallRuntimeCommandModel.call_id == lease.call_id,
                    AiCallRuntimeCommandModel.target_owner_id == lease.owner_id,
                    AiCallRuntimeCommandModel.expected_fencing_token
                    == lease.fencing_token,
                    command_type_condition,
                    due_status,
                )
                .order_by(
                    AiCallRuntimeCommandModel.dispatch_priority,
                    AiCallRuntimeCommandModel.command_seq,
                )
                .limit(16)
            )
        ).all()

        for candidate in candidates:
            token = self._processing_token_generator()
            owner_conditions = [
                AiCallRecordModel.tenant_id == lease.tenant_id,
                AiCallRecordModel.call_id == lease.call_id,
                AiCallRecordModel.runtime_control_mode == "owner_command_v1",
                AiCallRecordModel.runtime_owner_id == lease.owner_id,
                AiCallRecordModel.runtime_fencing_token == lease.fencing_token,
                AiCallRecordModel.runtime_lease_expires_at.is_not(None),
                AiCallRecordModel.runtime_lease_expires_at > func.clock_timestamp(),
            ]
            if end_only:
                owner_conditions.append(
                    AiCallRecordModel.terminal_requested_at.is_not(None)
                )
            else:
                owner_conditions.extend(
                    (
                        AiCallRecordModel.terminal_requested_at.is_(None),
                        AiCallRecordModel.last_applied_command_seq + 1
                        == AiCallRuntimeCommandModel.command_seq,
                    )
                )

            cas_conditions: list[Any] = [
                AiCallRuntimeCommandModel.id == candidate.id,
                AiCallRuntimeCommandModel.tenant_id == lease.tenant_id,
                AiCallRuntimeCommandModel.call_id == lease.call_id,
                AiCallRuntimeCommandModel.target_owner_id == lease.owner_id,
                AiCallRuntimeCommandModel.expected_fencing_token
                == lease.fencing_token,
                due_status,
                exists().where(*owner_conditions),
            ]
            if (
                candidate.command_type == START_CALL
                and candidate.status == CommandStatus.RETRY_WAIT
            ):
                cas_conditions.append(
                    ~exists().where(
                        AiCallRuntimeEffectModel.tenant_id == lease.tenant_id,
                        AiCallRuntimeEffectModel.call_id == lease.call_id,
                        AiCallRuntimeEffectModel.command_id == candidate.id,
                    )
                )

            row = (
                await self._session.execute(
                    update(AiCallRuntimeCommandModel)
                    .where(*cas_conditions)
                    .values(
                        status=CommandStatus.PROCESSING,
                        processing_owner_id=lease.owner_id,
                        processing_fencing_token=lease.fencing_token,
                        processing_token=token,
                        processing_expires_at=func.clock_timestamp()
                        + self._processing_lease_ttl,
                        claimed_at=func.clock_timestamp(),
                        attempt_count=AiCallRuntimeCommandModel.attempt_count + 1,
                        next_retry_at=None,
                        dispatch_token=None,
                        dispatch_expires_at=None,
                        updated_at=func.clock_timestamp(),
                    )
                    .returning(
                        AiCallRuntimeCommandModel.id,
                        AiCallRuntimeCommandModel.tenant_id,
                        AiCallRuntimeCommandModel.call_id,
                        AiCallRuntimeCommandModel.command_seq,
                        AiCallRuntimeCommandModel.command_type,
                        AiCallRuntimeCommandModel.processing_owner_id,
                        AiCallRuntimeCommandModel.processing_fencing_token,
                        AiCallRuntimeCommandModel.processing_token,
                        AiCallRuntimeCommandModel.processing_expires_at,
                        AiCallRuntimeCommandModel.payload_json,
                        AiCallRuntimeCommandModel.attempt_count,
                    )
                )
            ).one_or_none()
            if row is not None:
                return CommandClaim(
                    command_id=row.id,
                    tenant_id=row.tenant_id,
                    call_id=row.call_id,
                    command_seq=row.command_seq,
                    command_type=row.command_type,
                    processing_owner_id=row.processing_owner_id,
                    processing_fencing_token=row.processing_fencing_token,
                    processing_token=row.processing_token,
                    processing_expires_at=row.processing_expires_at,
                    payload_json=row.payload_json,
                    attempt_count=row.attempt_count,
                )
        return None

    async def _command_has_effect(self, command: AiCallRuntimeCommandModel) -> bool:
        return bool(
            await self._session.scalar(
                select(
                    exists().where(
                        AiCallRuntimeEffectModel.tenant_id == command.tenant_id,
                        AiCallRuntimeEffectModel.call_id == command.call_id,
                        AiCallRuntimeEffectModel.command_id == command.id,
                    )
                )
            )
        )

    async def _call_has_effect(self, tenant_id: str, call_id: str) -> bool:
        return bool(
            await self._session.scalar(
                select(
                    exists().where(
                        AiCallRuntimeEffectModel.tenant_id == tenant_id,
                        AiCallRuntimeEffectModel.call_id == call_id,
                    )
                )
            )
        )

    async def _has_unreleased_reservation(self, tenant_id: str, call_id: str) -> bool:
        return bool(
            await self._session.scalar(
                select(
                    exists().where(
                        AiCallSipLineReservationModel.tenant_id == tenant_id,
                        AiCallSipLineReservationModel.call_id == call_id,
                        AiCallSipLineReservationModel.status != "RELEASED",
                    )
                )
            )
        )

    async def _find_by_idempotency(
        self,
        *,
        tenant_id: str,
        idempotency_key: str,
    ) -> AiCallRuntimeCommandModel | None:
        return await self._session.scalar(
            select(AiCallRuntimeCommandModel).where(
                AiCallRuntimeCommandModel.tenant_id == tenant_id,
                AiCallRuntimeCommandModel.idempotency_key == idempotency_key,
            )
        )

    async def _lock_record(
        self,
        tenant_id: str,
        call_id: str,
    ) -> AiCallRecordModel:
        record = await self._session.scalar(
            select(AiCallRecordModel)
            .where(
                AiCallRecordModel.tenant_id == tenant_id,
                AiCallRecordModel.call_id == call_id,
            )
            .with_for_update()
        )
        if record is None:
            raise RuntimeRecordNotFoundError(
                f"runtime record not found: tenant={tenant_id}, call={call_id}"
            )
        return record

    async def _lock_end_command(
        self,
        tenant_id: str,
        call_id: str,
    ) -> AiCallRuntimeCommandModel | None:
        return await self._session.scalar(
            select(AiCallRuntimeCommandModel)
            .where(
                AiCallRuntimeCommandModel.tenant_id == tenant_id,
                AiCallRuntimeCommandModel.call_id == call_id,
                AiCallRuntimeCommandModel.command_type == END_CALL,
            )
            .with_for_update()
        )

    async def _preempt_ordinary_commands(
        self,
        *,
        tenant_id: str,
        call_id: str,
        end_command: AiCallRuntimeCommandModel,
        now: datetime,
    ) -> None:
        await self._session.execute(
            update(AiCallRuntimeCommandModel)
            .where(
                AiCallRuntimeCommandModel.tenant_id == tenant_id,
                AiCallRuntimeCommandModel.call_id == call_id,
                AiCallRuntimeCommandModel.command_type != END_CALL,
                AiCallRuntimeCommandModel.command_seq < end_command.command_seq,
                AiCallRuntimeCommandModel.status.in_(_PREEMPTIBLE_COMMAND_STATUSES),
            )
            .values(
                status=CommandStatus.SUPERSEDED,
                dispatch_token=None,
                dispatch_expires_at=None,
                processing_token=None,
                processing_expires_at=None,
                cancel_requested_at=now,
                preempted_by_command_id=end_command.id,
                finished_at=now,
                updated_at=now,
            )
        )

    async def _append_end_evidence(
        self,
        *,
        request: EndCallIntent,
        command_id: int,
        received_at: datetime,
    ) -> AiCallEndEvidenceModel:
        existing = await self._session.scalar(
            select(AiCallEndEvidenceModel)
            .where(
                AiCallEndEvidenceModel.tenant_id == request.tenant_id,
                AiCallEndEvidenceModel.dedupe_key == request.dedupe_key,
            )
            .with_for_update()
        )
        if existing is not None:
            self._validate_evidence_winner(existing, request)
            if existing.command_id is None:
                existing.command_id = command_id
            return existing

        evidence = AiCallEndEvidenceModel(
            id=self._id_generator(),
            tenant_id=request.tenant_id,
            call_id=request.call_id,
            command_id=command_id,
            source=request.source,
            end_reason=request.end_reason,
            provider=request.provider,
            provider_namespace=request.provider_namespace,
            provider_event_id=request.provider_event_id,
            event_at=request.event_at,
            received_at=received_at,
            dedupe_key=request.dedupe_key,
            evidence_json=(
                _canonical_json(request.evidence) if request.evidence is not None else None
            ),
        )
        try:
            async with self._session.begin_nested():
                self._session.add(evidence)
                await self._session.flush()
        except IntegrityError:
            winner = await self._session.scalar(
                select(AiCallEndEvidenceModel)
                .where(
                    AiCallEndEvidenceModel.tenant_id == request.tenant_id,
                    AiCallEndEvidenceModel.dedupe_key == request.dedupe_key,
                )
                .with_for_update()
            )
            if winner is None:
                raise
            self._validate_evidence_winner(winner, request)
            return winner
        return evidence

    @staticmethod
    def _validate_evidence_winner(
        evidence: AiCallEndEvidenceModel,
        request: EndCallIntent,
    ) -> None:
        if evidence.call_id != request.call_id:
            raise IdempotencyConflictError(
                "end evidence dedupe key already belongs to a different call"
            )

    @staticmethod
    def _matching_snapshot(
        command: AiCallRuntimeCommandModel,
        fingerprint: str,
    ) -> CommandSnapshot:
        if command.request_fingerprint != fingerprint:
            raise IdempotencyConflictError(
                "idempotency key already exists with a different request fingerprint"
            )
        return RuntimeCommandRepository._snapshot(command)

    @staticmethod
    def _snapshot(command: AiCallRuntimeCommandModel) -> CommandSnapshot:
        return CommandSnapshot(
            command_id=command.id,
            tenant_id=command.tenant_id,
            call_id=command.call_id,
            command_seq=command.command_seq,
            command_type=command.command_type,
            idempotency_key=command.idempotency_key,
            request_fingerprint=command.request_fingerprint,
            status=command.status,
            created_at=command.created_at,
        )
