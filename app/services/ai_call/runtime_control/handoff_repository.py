from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import (
    AiCallHandoffAgentModel,
    AiCallHandoffModel,
    AiCallRecordModel,
)
from app.services.ai_call.runtime_control.command_repository import (
    canonical_request_fingerprint,
)
from app.services.ai_call.runtime_control.models import AiCallRuntimeCommandModel
from app.services.ai_call.runtime_control.postgres_wakeup import publish_control_wakeup
from app.services.ai_call.runtime_control.timing import read_database_time
from app.services.ai_call.runtime_control.types import CommandStatus
from app.utils.id_util import generate_snowflake_id

HANDOFF_ACCEPTED = "HANDOFF_ACCEPTED"
CANCEL_HANDOFF = "CANCEL_HANDOFF"
_HANDOFF_TERMINAL_STATUSES = frozenset({"completed", "canceled", "failed", "expired"})


class RuntimeHandoffRepositoryError(RuntimeError):
    pass


class HandoffNotFoundError(RuntimeHandoffRepositoryError):
    pass


class HandoffClaimConflictError(RuntimeHandoffRepositoryError):
    pass


class HandoffIdempotencyConflictError(RuntimeHandoffRepositoryError):
    pass


class HandoffRuntimeModeError(RuntimeHandoffRepositoryError):
    pass


class HandoffTerminalBarrierError(RuntimeHandoffRepositoryError):
    pass


@dataclass(frozen=True, slots=True)
class HandoffAcceptIntent:
    tenant_id: str
    handoff_id: str
    agent_identity: str
    console_session_id: str
    idempotency_key: str


@dataclass(frozen=True, slots=True)
class HandoffCancelIntent:
    tenant_id: str
    handoff_id: str
    idempotency_key: str
    reason: str


@dataclass(frozen=True, slots=True)
class HandoffCommandDecision:
    handoff_id: str
    call_id: str
    handoff_status: str
    command_id: int
    command_seq: int
    command_status: str


def _canonical_json(value: Mapping[str, object]) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class RuntimeHandoffRepository:
    """Owner 模式 Handoff 预占与命令登记；不执行媒体副作用。"""

    def __init__(
        self,
        session: AsyncSession,
        *,
        id_generator: Callable[[], int] = generate_snowflake_id,
        database_clock: Callable[[AsyncSession], Awaitable[datetime]] = read_database_time,
        claim_ttl: timedelta = timedelta(seconds=15),
    ) -> None:
        self._session = session
        self._id_generator = id_generator
        self._database_clock = database_clock
        self._claim_ttl = claim_ttl

    async def accept(self, request: HandoffAcceptIntent) -> HandoffCommandDecision:
        call_id = await self._find_call_id(request.tenant_id, request.handoff_id)
        record = await self._lock_record(request.tenant_id, call_id)
        handoff = await self._lock_handoff(
            request.tenant_id,
            request.handoff_id,
            call_id,
        )
        presence = await self._lock_presence(request.tenant_id, request.agent_identity)
        command = await self._lock_command(request.tenant_id, request.idempotency_key)
        payload = {
            "agent_identity": request.agent_identity,
            "console_session_id": request.console_session_id,
            "handoff_id": request.handoff_id,
        }
        fingerprint = self._fingerprint(record, HANDOFF_ACCEPTED, payload)
        if command is not None:
            self._validate_existing_command(command, record, HANDOFF_ACCEPTED, fingerprint)
            self._validate_existing_claim(handoff, presence, request)
            return self._decision(handoff, command)

        self._validate_record(record)
        now = await self._database_clock(self._session)
        self._validate_new_claim(handoff, presence, request, now)
        claim_expires_at = now + self._claim_ttl
        if handoff.expires_at is not None:
            claim_expires_at = min(claim_expires_at, _ensure_utc(handoff.expires_at))
        command = self._new_command(
            record=record,
            command_type=HANDOFF_ACCEPTED,
            idempotency_key=request.idempotency_key,
            fingerprint=fingerprint,
            payload=payload,
            now=now,
        )
        handoff.status = "accepted"
        handoff.human_agent_identity = request.agent_identity
        handoff.accepted_console_session_id = request.console_session_id
        handoff.accepted_at = now
        handoff.claim_expires_at = claim_expires_at
        presence.status = "claiming"
        presence.active_handoff_id = handoff.handoff_id
        presence.active_call_id = handoff.call_id
        presence.last_seen_at = now
        presence.status_updated_at = now
        record.next_command_seq += 1
        self._session.add(command)
        await self._session.flush()
        await publish_control_wakeup(self._session)
        return self._decision(handoff, command)

    async def request_cancel(
        self,
        request: HandoffCancelIntent,
    ) -> HandoffCommandDecision:
        call_id = await self._find_call_id(request.tenant_id, request.handoff_id)
        record = await self._lock_record(request.tenant_id, call_id)
        handoff = await self._lock_handoff(
            request.tenant_id,
            request.handoff_id,
            call_id,
        )
        presence = None
        if handoff.human_agent_identity is not None:
            presence = await self._lock_presence(
                request.tenant_id,
                handoff.human_agent_identity,
            )
        command = await self._lock_command(request.tenant_id, request.idempotency_key)
        payload = {"handoff_id": request.handoff_id, "reason": request.reason}
        fingerprint = self._fingerprint(record, CANCEL_HANDOFF, payload)
        if command is not None:
            self._validate_existing_command(command, record, CANCEL_HANDOFF, fingerprint)
            return self._decision(handoff, command)

        self._validate_record(record)
        if handoff.status in _HANDOFF_TERMINAL_STATUSES:
            raise HandoffClaimConflictError(
                f"handoff {request.handoff_id} is already terminal"
            )
        if handoff.human_agent_identity is not None and presence is None:
            raise HandoffClaimConflictError("claimed handoff presence is missing")
        now = await self._database_clock(self._session)
        command = self._new_command(
            record=record,
            command_type=CANCEL_HANDOFF,
            idempotency_key=request.idempotency_key,
            fingerprint=fingerprint,
            payload=payload,
            now=now,
        )
        record.next_command_seq += 1
        self._session.add(command)
        await self._session.flush()
        await publish_control_wakeup(self._session)
        return self._decision(handoff, command)

    async def _find_call_id(self, tenant_id: str, handoff_id: str) -> str:
        call_id = await self._session.scalar(
            select(AiCallHandoffModel.call_id).where(
                AiCallHandoffModel.tenant_id == tenant_id,
                AiCallHandoffModel.handoff_id == handoff_id,
            )
        )
        if call_id is None:
            raise HandoffNotFoundError(f"handoff {handoff_id} was not found")
        return call_id

    async def _lock_record(self, tenant_id: str, call_id: str) -> AiCallRecordModel:
        record = await self._session.scalar(
            select(AiCallRecordModel)
            .where(
                AiCallRecordModel.tenant_id == tenant_id,
                AiCallRecordModel.call_id == call_id,
            )
            .with_for_update()
        )
        if record is None:
            raise HandoffNotFoundError(f"call {call_id} was not found")
        return record

    async def _lock_handoff(
        self,
        tenant_id: str,
        handoff_id: str,
        call_id: str,
    ) -> AiCallHandoffModel:
        handoff = await self._session.scalar(
            select(AiCallHandoffModel)
            .where(
                AiCallHandoffModel.tenant_id == tenant_id,
                AiCallHandoffModel.handoff_id == handoff_id,
                AiCallHandoffModel.call_id == call_id,
            )
            .with_for_update()
        )
        if handoff is None:
            raise HandoffNotFoundError(f"handoff {handoff_id} was not found")
        return handoff

    async def _lock_presence(
        self,
        tenant_id: str,
        agent_identity: str,
    ) -> AiCallHandoffAgentModel | None:
        return await self._session.scalar(
            select(AiCallHandoffAgentModel)
            .where(
                AiCallHandoffAgentModel.tenant_id == tenant_id,
                AiCallHandoffAgentModel.agent_identity == agent_identity,
            )
            .with_for_update()
        )

    async def _lock_command(
        self,
        tenant_id: str,
        idempotency_key: str,
    ) -> AiCallRuntimeCommandModel | None:
        return await self._session.scalar(
            select(AiCallRuntimeCommandModel)
            .where(
                AiCallRuntimeCommandModel.tenant_id == tenant_id,
                AiCallRuntimeCommandModel.idempotency_key == idempotency_key,
            )
            .with_for_update()
        )

    @staticmethod
    def _validate_record(record: AiCallRecordModel) -> None:
        if record.runtime_control_mode != "owner_command_v1":
            raise HandoffRuntimeModeError(
                f"call {record.call_id} is not owned by runtime control"
            )
        if record.terminal_requested_at is not None:
            raise HandoffTerminalBarrierError(
                f"call {record.call_id} already has a terminal barrier"
            )

    @staticmethod
    def _validate_new_claim(
        handoff: AiCallHandoffModel,
        presence: AiCallHandoffAgentModel | None,
        request: HandoffAcceptIntent,
        now: datetime,
    ) -> None:
        if handoff.status != "requested":
            raise HandoffClaimConflictError(
                f"handoff {handoff.handoff_id} cannot be claimed from {handoff.status}"
            )
        if handoff.expires_at is not None and _ensure_utc(handoff.expires_at) <= now:
            raise HandoffClaimConflictError(f"handoff {handoff.handoff_id} is expired")
        if presence is None:
            raise HandoffClaimConflictError("agent presence is missing")
        if presence.console_session_id != request.console_session_id:
            raise HandoffClaimConflictError("agent console session does not own presence")
        if presence.status != "available" or presence.active_handoff_id is not None:
            raise HandoffClaimConflictError("agent is not available")

    @staticmethod
    def _validate_existing_claim(
        handoff: AiCallHandoffModel,
        presence: AiCallHandoffAgentModel | None,
        request: HandoffAcceptIntent,
    ) -> None:
        if (
            handoff.status != "accepted"
            or handoff.human_agent_identity != request.agent_identity
            or handoff.accepted_console_session_id != request.console_session_id
            or presence is None
            or presence.status != "claiming"
            or presence.active_handoff_id != handoff.handoff_id
            or presence.active_call_id != handoff.call_id
        ):
            raise HandoffClaimConflictError(
                "idempotent command does not match the persisted claim"
            )

    @staticmethod
    def _fingerprint(
        record: AiCallRecordModel,
        command_type: str,
        payload: Mapping[str, object],
    ) -> str:
        return canonical_request_fingerprint(
            {
                "tenant_id": record.tenant_id,
                "call_id": record.call_id,
                "command_type": command_type,
                "payload": payload,
            }
        )

    @staticmethod
    def _validate_existing_command(
        command: AiCallRuntimeCommandModel,
        record: AiCallRecordModel,
        command_type: str,
        fingerprint: str,
    ) -> None:
        if (
            command.call_id != record.call_id
            or command.command_type != command_type
            or command.request_fingerprint != fingerprint
        ):
            raise HandoffIdempotencyConflictError(
                "idempotency key already belongs to a different handoff action"
            )

    def _new_command(
        self,
        *,
        record: AiCallRecordModel,
        command_type: str,
        idempotency_key: str,
        fingerprint: str,
        payload: Mapping[str, object],
        now: datetime,
    ) -> AiCallRuntimeCommandModel:
        return AiCallRuntimeCommandModel(
            id=self._id_generator(),
            tenant_id=record.tenant_id,
            call_id=record.call_id,
            command_seq=record.next_command_seq,
            command_type=command_type,
            idempotency_key=idempotency_key,
            request_fingerprint=fingerprint,
            dispatch_priority=100,
            payload_json=_canonical_json(payload),
            expected_fencing_token=record.runtime_fencing_token,
            target_owner_id=record.runtime_owner_id,
            status=CommandStatus.PENDING,
            created_at=now,
            updated_at=now,
        )

    @staticmethod
    def _decision(
        handoff: AiCallHandoffModel,
        command: AiCallRuntimeCommandModel,
    ) -> HandoffCommandDecision:
        return HandoffCommandDecision(
            handoff_id=handoff.handoff_id,
            call_id=handoff.call_id,
            handoff_status=handoff.status,
            command_id=command.id,
            command_seq=command.command_seq,
            command_status=str(command.status),
        )
