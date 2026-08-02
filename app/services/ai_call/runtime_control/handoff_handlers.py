from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.model import (
    AiCallHandoffAgentModel,
    AiCallHandoffModel,
    AiCallRecordModel,
)
from app.services.ai_call.runtime_control.command_repository import (
    CommandClaim,
    EndCallIntent,
    RuntimeCommandRepository,
)
from app.services.ai_call.runtime_control.models import AiCallRuntimeCommandModel
from app.services.ai_call.runtime_control.owner_repository import OwnerLease
from app.services.ai_call.runtime_control.timing import read_database_time
from app.services.ai_call.runtime_control.types import CommandStatus
from app.utils.id_util import generate_snowflake_id


@dataclass(frozen=True, slots=True)
class AgentMediaObservation:
    ready: bool
    participant_identity: str | None = None
    participant_sid: str | None = None
    track_sid: str | None = None


@dataclass(frozen=True, slots=True)
class HandoffHandlerResult:
    command_completed: bool
    state_changed: bool


@dataclass(frozen=True, slots=True)
class _MediaTarget:
    handoff_id: str
    room_name: str
    participant_identity: str
    media_state_version: int


class AgentMediaProvider(Protocol):
    async def query_agent_media(
        self,
        room_name: str,
        participant_identity: str,
    ) -> AgentMediaObservation: ...


DatabaseClock = Callable[[AsyncSession], Awaitable[datetime]]


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _decode_payload(claim: CommandClaim) -> dict[str, object]:
    if not claim.payload_json:
        raise ValueError(f"{claim.command_type} command payload is missing")
    try:
        payload = json.loads(claim.payload_json)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{claim.command_type} command payload is invalid") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{claim.command_type} command payload must be an object")
    return payload


def _payload_handoff_id(claim: CommandClaim) -> str:
    value = _decode_payload(claim).get("handoff_id")
    if not isinstance(value, str) or not value:
        raise ValueError(f"{claim.command_type} handoff_id is missing")
    return value


def _payload_media_version(claim: CommandClaim) -> int:
    value = _decode_payload(claim).get("media_state_version")
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{claim.command_type} media_state_version is invalid")
    return value


class _HandoffStateRepository:
    def __init__(
        self,
        session: AsyncSession,
        *,
        database_clock: DatabaseClock,
    ) -> None:
        self._session = session
        self._database_clock = database_clock

    async def inspect_media_target(
        self,
        claim: CommandClaim,
        lease: OwnerLease,
        *,
        allowed_handoff_statuses: frozenset[str],
    ) -> _MediaTarget | None:
        handoff_id = _payload_handoff_id(claim)
        expected_version = _payload_media_version(claim)
        record = await self._session.scalar(
            select(AiCallRecordModel).where(
                AiCallRecordModel.tenant_id == claim.tenant_id,
                AiCallRecordModel.call_id == claim.call_id,
            )
        )
        handoff = await self._session.scalar(
            select(AiCallHandoffModel).where(
                AiCallHandoffModel.tenant_id == claim.tenant_id,
                AiCallHandoffModel.call_id == claim.call_id,
                AiCallHandoffModel.handoff_id == handoff_id,
            )
        )
        command = await self._session.get(AiCallRuntimeCommandModel, claim.command_id)
        if record is None or handoff is None or command is None:
            return None
        now = await self._database_clock(self._session)
        if not self._runtime_claim_matches(record, command, claim, lease, now):
            return None
        if (
            record.terminal_requested_at is not None
            or handoff.status not in allowed_handoff_statuses
            or handoff.media_state_version != expected_version
            or not handoff.participant_identity
            or not handoff.human_agent_identity
            or not handoff.accepted_console_session_id
        ):
            return None
        presence = await self._session.scalar(
            select(AiCallHandoffAgentModel).where(
                AiCallHandoffAgentModel.tenant_id == claim.tenant_id,
                AiCallHandoffAgentModel.agent_identity
                == handoff.human_agent_identity,
            )
        )
        if not self._presence_matches(presence, handoff):
            return None
        return _MediaTarget(
            handoff_id=handoff.handoff_id,
            room_name=handoff.room_name,
            participant_identity=handoff.participant_identity,
            media_state_version=expected_version,
        )

    async def submit_media_ready(
        self,
        claim: CommandClaim,
        lease: OwnerLease,
        target: _MediaTarget,
        observation: AgentMediaObservation,
    ) -> HandoffHandlerResult:
        rows = await self._lock_runtime_rows(claim, target.handoff_id)
        if rows is None:
            return HandoffHandlerResult(False, False)
        record, handoff, presence, command = rows
        now = await self._database_clock(self._session)
        if not self._runtime_claim_matches(record, command, claim, lease, now):
            return HandoffHandlerResult(False, False)
        if record.terminal_requested_at is not None:
            return HandoffHandlerResult(False, False)
        if not self._presence_matches(presence, handoff):
            return HandoffHandlerResult(False, False)
        if handoff.media_state_version != target.media_state_version:
            completed = self._complete_command(
                record,
                command,
                CommandStatus.RETRY_WAIT,
                now,
                result={"reason": "media_state_version_changed"},
                retry_after=timedelta(milliseconds=100),
            )
            await self._session.flush()
            return HandoffHandlerResult(completed, False)
        if handoff.status not in {"accepted", "reconnecting"}:
            completed = self._complete_command(
                record,
                command,
                CommandStatus.SUPERSEDED,
                now,
                result={"reason": f"handoff_status:{handoff.status}"},
            )
            await self._session.flush()
            return HandoffHandlerResult(completed, False)
        if not self._observation_matches(target, observation):
            completed = self._complete_command(
                record,
                command,
                CommandStatus.RETRY_WAIT,
                now,
                result={"reason": "agent_media_not_ready"},
                retry_after=timedelta(milliseconds=250),
            )
            await self._session.flush()
            return HandoffHandlerResult(completed, False)

        handoff.status = "connected"
        if handoff.connected_at is None:
            handoff.connected_at = now
        handoff.participant_identity = observation.participant_identity
        handoff.participant_sid = observation.participant_sid
        handoff.track_sid = observation.track_sid
        handoff.verified_at = now
        handoff.evidence_source = "provider_query"
        handoff.media_state_version += 1
        handoff.media_invalidated_at = None
        handoff.reconnect_expires_at = None
        presence.status = "in_call"
        presence.status_updated_at = now
        completed = self._complete_command(
            record,
            command,
            CommandStatus.SUCCEEDED,
            now,
            result={
                "handoff_id": handoff.handoff_id,
                "media_state_version": handoff.media_state_version,
            },
        )
        if not completed:
            return HandoffHandlerResult(False, False)
        await self._session.flush()
        return HandoffHandlerResult(True, True)

    async def submit_media_invalidated(
        self,
        claim: CommandClaim,
        lease: OwnerLease,
        target: _MediaTarget,
        observation: AgentMediaObservation,
    ) -> HandoffHandlerResult:
        rows = await self._lock_runtime_rows(claim, target.handoff_id)
        if rows is None:
            return HandoffHandlerResult(False, False)
        record, handoff, presence, command = rows
        now = await self._database_clock(self._session)
        if not self._runtime_claim_matches(record, command, claim, lease, now):
            return HandoffHandlerResult(False, False)
        if record.terminal_requested_at is not None:
            return HandoffHandlerResult(False, False)
        if not self._presence_matches(presence, handoff):
            return HandoffHandlerResult(False, False)
        if handoff.media_state_version != target.media_state_version:
            completed = self._complete_command(
                record,
                command,
                CommandStatus.SUCCEEDED,
                now,
                result={"reason": "newer_media_state_observed"},
            )
            await self._session.flush()
            return HandoffHandlerResult(completed, False)
        if observation.ready:
            completed = self._complete_command(
                record,
                command,
                CommandStatus.SUCCEEDED,
                now,
                result={"reason": "media_already_ready"},
            )
            await self._session.flush()
            return HandoffHandlerResult(completed, False)

        changed = handoff.status == "connected"
        if changed:
            handoff.status = "reconnecting"
            handoff.reconnect_expires_at = now + timedelta(seconds=15)
            presence.status = "claiming"
            presence.status_updated_at = now
        completed = self._complete_command(
            record,
            command,
            CommandStatus.SUCCEEDED,
            now,
            result={"handoff_id": handoff.handoff_id, "reconnecting": changed},
        )
        if not completed:
            return HandoffHandlerResult(False, False)
        await self._session.flush()
        return HandoffHandlerResult(True, changed)

    async def complete_handoff_accepted(
        self,
        claim: CommandClaim,
        lease: OwnerLease,
    ) -> HandoffHandlerResult:
        handoff_id = _payload_handoff_id(claim)
        rows = await self._lock_runtime_rows(claim, handoff_id)
        if rows is None:
            return HandoffHandlerResult(False, False)
        record, handoff, presence, command = rows
        now = await self._database_clock(self._session)
        valid = (
            self._runtime_claim_matches(record, command, claim, lease, now)
            and record.terminal_requested_at is None
            and handoff.status == "accepted"
            and self._presence_matches(presence, handoff)
            and presence.status == "claiming"
        )
        if not valid:
            return HandoffHandlerResult(False, False)
        completed = self._complete_command(
            record,
            command,
            CommandStatus.SUCCEEDED,
            now,
            result={"handoff_id": handoff.handoff_id},
        )
        await self._session.flush()
        return HandoffHandlerResult(completed, False)

    async def cancel_handoff(
        self,
        claim: CommandClaim,
        lease: OwnerLease,
        *,
        id_generator: Callable[[], int],
    ) -> HandoffHandlerResult:
        payload = _decode_payload(claim)
        handoff_id = _payload_handoff_id(claim)
        reason = str(payload.get("reason") or "canceled")[:64]
        rows = await self._lock_runtime_rows(claim, handoff_id, require_presence=False)
        if rows is None:
            return HandoffHandlerResult(False, False)
        record, handoff, presence, command = rows
        now = await self._database_clock(self._session)
        if not self._runtime_claim_matches(record, command, claim, lease, now):
            return HandoffHandlerResult(False, False)
        if record.terminal_requested_at is not None:
            return HandoffHandlerResult(False, False)
        if handoff.status in {"connected", "reconnecting"} or (
            handoff.status == "accepted" and handoff.connected_at is not None
        ):
            await RuntimeCommandRepository(
                self._session,
                id_generator=id_generator,
                database_clock=self._database_clock,
            ).request_end(
                EndCallIntent(
                    tenant_id=claim.tenant_id,
                    call_id=claim.call_id,
                    source="runtime_handler",
                    end_reason="handoff_cancel_after_connected",
                    dedupe_key=f"runtime:handoff-cancel:{handoff.handoff_id}",
                    evidence={"handoff_id": handoff.handoff_id, "reason": reason},
                )
            )
            return HandoffHandlerResult(True, False)
        if handoff.status not in {"requested", "accepted"}:
            completed = self._complete_command(
                record,
                command,
                CommandStatus.SUPERSEDED,
                now,
                result={"reason": f"handoff_status:{handoff.status}"},
            )
            await self._session.flush()
            return HandoffHandlerResult(completed, False)

        handoff.status = "canceled"
        handoff.end_reason = reason
        handoff.ended_at = now
        if (
            presence is not None
            and presence.status == "claiming"
            and self._presence_matches(presence, handoff)
        ):
            presence.status = "available"
            presence.active_handoff_id = None
            presence.active_call_id = None
            presence.status_updated_at = now
        completed = self._complete_command(
            record,
            command,
            CommandStatus.SUCCEEDED,
            now,
            result={"handoff_id": handoff.handoff_id, "canceled": True},
        )
        if not completed:
            return HandoffHandlerResult(False, False)
        await self._session.flush()
        return HandoffHandlerResult(True, True)

    async def _lock_runtime_rows(
        self,
        claim: CommandClaim,
        handoff_id: str,
        *,
        require_presence: bool = True,
    ) -> tuple[
        AiCallRecordModel,
        AiCallHandoffModel,
        AiCallHandoffAgentModel | None,
        AiCallRuntimeCommandModel,
    ] | None:
        record = await self._session.scalar(
            select(AiCallRecordModel)
            .where(
                AiCallRecordModel.tenant_id == claim.tenant_id,
                AiCallRecordModel.call_id == claim.call_id,
            )
            .with_for_update()
        )
        handoff = await self._session.scalar(
            select(AiCallHandoffModel)
            .where(
                AiCallHandoffModel.tenant_id == claim.tenant_id,
                AiCallHandoffModel.call_id == claim.call_id,
                AiCallHandoffModel.handoff_id == handoff_id,
            )
            .with_for_update()
        )
        if record is None or handoff is None:
            return None
        presence = None
        if handoff.human_agent_identity:
            presence = await self._session.scalar(
                select(AiCallHandoffAgentModel)
                .where(
                    AiCallHandoffAgentModel.tenant_id == claim.tenant_id,
                    AiCallHandoffAgentModel.agent_identity
                    == handoff.human_agent_identity,
                )
                .with_for_update()
            )
        if require_presence and presence is None:
            return None
        command = await self._session.scalar(
            select(AiCallRuntimeCommandModel)
            .where(
                AiCallRuntimeCommandModel.tenant_id == claim.tenant_id,
                AiCallRuntimeCommandModel.call_id == claim.call_id,
                AiCallRuntimeCommandModel.id == claim.command_id,
            )
            .with_for_update()
        )
        if command is None:
            return None
        return record, handoff, presence, command

    @staticmethod
    def _runtime_claim_matches(
        record: AiCallRecordModel,
        command: AiCallRuntimeCommandModel,
        claim: CommandClaim,
        lease: OwnerLease,
        now: datetime,
    ) -> bool:
        return bool(
            record.runtime_control_mode == "owner_command_v1"
            and record.runtime_owner_id == lease.owner_id
            and record.runtime_owner_id == claim.processing_owner_id
            and record.runtime_fencing_token == lease.fencing_token
            and record.runtime_fencing_token == claim.processing_fencing_token
            and record.runtime_lease_expires_at is not None
            and _ensure_utc(record.runtime_lease_expires_at) > _ensure_utc(now)
            and command.status == CommandStatus.PROCESSING
            and command.processing_owner_id == claim.processing_owner_id
            and command.processing_fencing_token == claim.processing_fencing_token
            and command.processing_token == claim.processing_token
            and command.processing_expires_at is not None
            and _ensure_utc(command.processing_expires_at) > _ensure_utc(now)
            and command.command_type == claim.command_type
            and command.command_seq == claim.command_seq
            and command.command_seq == record.last_applied_command_seq + 1
            and command.expected_fencing_token == lease.fencing_token
            and command.target_owner_id == lease.owner_id
            and lease.tenant_id == claim.tenant_id
            and lease.call_id == claim.call_id
        )

    @staticmethod
    def _presence_matches(
        presence: AiCallHandoffAgentModel | None,
        handoff: AiCallHandoffModel,
    ) -> bool:
        return bool(
            presence is not None
            and presence.active_handoff_id == handoff.handoff_id
            and presence.active_call_id == handoff.call_id
            and presence.console_session_id == handoff.accepted_console_session_id
        )

    @staticmethod
    def _observation_matches(
        target: _MediaTarget,
        observation: AgentMediaObservation,
    ) -> bool:
        return bool(
            observation.ready
            and observation.participant_identity == target.participant_identity
            and observation.participant_sid
            and observation.track_sid
        )

    @staticmethod
    def _complete_command(
        record: AiCallRecordModel,
        command: AiCallRuntimeCommandModel,
        status: CommandStatus,
        now: datetime,
        *,
        result: dict[str, object],
        retry_after: timedelta | None = None,
    ) -> bool:
        if status != CommandStatus.RETRY_WAIT:
            if command.command_seq != record.last_applied_command_seq + 1:
                return False
            record.last_applied_command_seq = command.command_seq
        command.status = status
        command.processing_owner_id = None
        command.processing_fencing_token = None
        command.processing_token = None
        command.processing_expires_at = None
        command.result_json = json.dumps(
            result,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        command.error_message = None
        command.updated_at = now
        if status == CommandStatus.RETRY_WAIT:
            command.next_retry_at = now + (retry_after or timedelta(0))
            command.finished_at = None
        else:
            command.next_retry_at = None
            command.finished_at = now
        return True


class AgentMediaReadyHandler:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        provider: AgentMediaProvider,
        *,
        database_clock: DatabaseClock = read_database_time,
    ) -> None:
        self._session_factory = session_factory
        self._provider = provider
        self._database_clock = database_clock

    async def handle(
        self,
        claim: CommandClaim,
        lease: OwnerLease,
    ) -> HandoffHandlerResult:
        async with self._session_factory() as session:
            target = await _HandoffStateRepository(
                session,
                database_clock=self._database_clock,
            ).inspect_media_target(
                claim,
                lease,
                allowed_handoff_statuses=frozenset({"accepted", "reconnecting"}),
            )
        if target is None:
            return HandoffHandlerResult(False, False)
        observation = await self._provider.query_agent_media(
            target.room_name,
            target.participant_identity,
        )
        async with self._session_factory.begin() as session:
            return await _HandoffStateRepository(
                session,
                database_clock=self._database_clock,
            ).submit_media_ready(claim, lease, target, observation)


class AgentMediaInvalidatedHandler:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        provider: AgentMediaProvider,
        *,
        database_clock: DatabaseClock = read_database_time,
    ) -> None:
        self._session_factory = session_factory
        self._provider = provider
        self._database_clock = database_clock

    async def handle(
        self,
        claim: CommandClaim,
        lease: OwnerLease,
    ) -> HandoffHandlerResult:
        async with self._session_factory() as session:
            target = await _HandoffStateRepository(
                session,
                database_clock=self._database_clock,
            ).inspect_media_target(
                claim,
                lease,
                allowed_handoff_statuses=frozenset(
                    {"accepted", "connected", "reconnecting"}
                ),
            )
        if target is None:
            return HandoffHandlerResult(False, False)
        observation = await self._provider.query_agent_media(
            target.room_name,
            target.participant_identity,
        )
        async with self._session_factory.begin() as session:
            return await _HandoffStateRepository(
                session,
                database_clock=self._database_clock,
            ).submit_media_invalidated(claim, lease, target, observation)


class HandoffAcceptedHandler:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        database_clock: DatabaseClock = read_database_time,
    ) -> None:
        self._session_factory = session_factory
        self._database_clock = database_clock

    async def handle(
        self,
        claim: CommandClaim,
        lease: OwnerLease,
    ) -> HandoffHandlerResult:
        async with self._session_factory.begin() as session:
            return await _HandoffStateRepository(
                session,
                database_clock=self._database_clock,
            ).complete_handoff_accepted(claim, lease)


class CancelHandoffHandler:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        database_clock: DatabaseClock = read_database_time,
        id_generator: Callable[[], int] = generate_snowflake_id,
    ) -> None:
        self._session_factory = session_factory
        self._database_clock = database_clock
        self._id_generator = id_generator

    async def handle(
        self,
        claim: CommandClaim,
        lease: OwnerLease,
    ) -> HandoffHandlerResult:
        async with self._session_factory.begin() as session:
            return await _HandoffStateRepository(
                session,
                database_clock=self._database_clock,
            ).cancel_handoff(
                claim,
                lease,
                id_generator=self._id_generator,
            )
