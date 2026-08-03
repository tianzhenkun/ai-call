from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

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
from app.services.ai_call.runtime_control.models import (
    AiCallEndEvidenceModel,
    AiCallRuntimeCommandModel,
)
from app.services.ai_call.runtime_control.owner_repository import OwnerLease
from app.services.ai_call.runtime_control.types import CommandStatus

NOW = datetime(2026, 8, 2, 5, tzinfo=timezone.utc)


@pytest.fixture
async def handoff_handler_session_factory(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'handoff-handler.db'}")
    async with engine.begin() as connection:
        for table in (
            AiCallRecordModel.__table__,
            AiCallHandoffModel.__table__,
            AiCallHandoffAgentModel.__table__,
            AiCallRuntimeCommandModel.__table__,
            AiCallEndEvidenceModel.__table__,
        ):
            await connection.run_sync(table.create)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


class MediaProviderStub:
    def __init__(
        self,
        observation,
        *,
        after_query: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.observation = observation
        self.after_query = after_query
        self.queries: list[tuple[str, str]] = []

    async def query_agent_media(self, room_name: str, participant_identity: str):
        self.queries.append((room_name, participant_identity))
        if self.after_query is not None:
            await self.after_query()
        return self.observation


async def _seed_command(
    session_factory,
    *,
    command_type: str,
    command_id: int = 301,
    command_seq: int = 2,
    handoff_status: str = "accepted",
    presence_status: str = "claiming",
    media_state_version: int = 1,
) -> CommandClaim:
    payload = {
        "evidence_id": "401",
        "handoff_id": "handoff-1",
        "media_state_version": media_state_version,
    }
    processing_expires_at = NOW + timedelta(minutes=1)
    async with session_factory() as session, session.begin():
        session.add_all(
            (
                AiCallRecordModel(
                    id=101,
                    tenant_id="tenant-a",
                    call_id="call-1",
                    entry_type="web",
                    room_name="ai-call-call-1",
                    participant_identity="caller-call-1",
                    status="ready",
                    started_at=NOW,
                    runtime_control_mode="owner_command_v1",
                    runtime_owner_id="runtime-1",
                    runtime_fencing_token=7,
                    runtime_lease_expires_at=processing_expires_at,
                    runtime_capacity_class="active",
                    next_command_seq=command_seq + 1,
                    last_applied_command_seq=command_seq - 1,
                ),
                AiCallHandoffModel(
                    id=201,
                    tenant_id="tenant-a",
                    handoff_id="handoff-1",
                    call_id="call-1",
                    room_name="ai-call-call-1",
                    scene_code="default",
                    status=handoff_status,
                    request_source="customer",
                    requested_at=NOW,
                    human_agent_identity="agent-1",
                    accepted_console_session_id="11111111-1111-1111-1111-111111111111",
                    accepted_at=NOW,
                    claim_expires_at=processing_expires_at,
                    participant_identity="human-agent-handoff-1",
                    participant_sid="PA_EVENT",
                    track_sid="TR_EVENT",
                    media_state_version=media_state_version,
                    media_invalidated_at=(
                        NOW if command_type == "AGENT_MEDIA_INVALIDATED" else None
                    ),
                    last_media_event_key="EV_1",
                ),
                AiCallHandoffAgentModel(
                    id=202,
                    tenant_id="tenant-a",
                    agent_identity="agent-1",
                    skill_group="default",
                    status=presence_status,
                    active_handoff_id="handoff-1",
                    active_call_id="call-1",
                    console_session_id="11111111-1111-1111-1111-111111111111",
                    last_seen_at=NOW,
                    status_updated_at=NOW,
                ),
                AiCallRuntimeCommandModel(
                    id=command_id,
                    tenant_id="tenant-a",
                    call_id="call-1",
                    command_seq=command_seq,
                    command_type=command_type,
                    idempotency_key=f"test:{command_type}:{command_id}",
                    request_fingerprint="fingerprint",
                    dispatch_priority=100,
                    payload_json=json.dumps(payload),
                    expected_fencing_token=7,
                    target_owner_id="runtime-1",
                    status=CommandStatus.PROCESSING,
                    processing_owner_id="runtime-1",
                    processing_fencing_token=7,
                    processing_token=f"token-{command_id}",
                    processing_expires_at=processing_expires_at,
                    claimed_at=NOW,
                    attempt_count=1,
                    created_at=NOW,
                    updated_at=NOW,
                ),
            )
        )
    return CommandClaim(
        command_id=command_id,
        tenant_id="tenant-a",
        call_id="call-1",
        command_seq=command_seq,
        command_type=command_type,
        processing_owner_id="runtime-1",
        processing_fencing_token=7,
        processing_token=f"token-{command_id}",
        processing_expires_at=processing_expires_at,
        payload_json=json.dumps(payload),
        attempt_count=1,
    )


def _lease() -> OwnerLease:
    return OwnerLease(
        tenant_id="tenant-a",
        call_id="call-1",
        owner_id="runtime-1",
        fencing_token=7,
        lease_expires_at=NOW + timedelta(minutes=1),
        capacity_class="active",
    )


def _ready_observation():
    from app.services.ai_call.runtime_control.handoff_handlers import (
        AgentMediaObservation,
    )

    return AgentMediaObservation(
        ready=True,
        participant_identity="human-agent-handoff-1",
        participant_sid="PA_QUERY",
        track_sid="TR_QUERY",
    )


async def _constant_time(_session) -> datetime:
    return NOW + timedelta(seconds=1)


@pytest.mark.anyio
async def test_media_ready_requires_current_owner_command_and_provider_query(
    handoff_handler_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.handoff_handlers import (
        AgentMediaReadyHandler,
    )

    claim = await _seed_command(
        handoff_handler_session_factory,
        command_type="AGENT_MEDIA_READY",
    )
    provider = MediaProviderStub(_ready_observation())
    result = await AgentMediaReadyHandler(
        handoff_handler_session_factory,
        provider,
        database_clock=_constant_time,
    ).handle(claim, _lease())

    async with handoff_handler_session_factory() as session:
        handoff = await session.scalar(select(AiCallHandoffModel))
        presence = await session.scalar(select(AiCallHandoffAgentModel))
        command = await session.get(AiCallRuntimeCommandModel, claim.command_id)
    assert result.state_changed is True
    assert provider.queries == [("ai-call-call-1", "human-agent-handoff-1")]
    assert handoff.status == "connected"
    assert handoff.media_state_version == 2
    assert handoff.participant_sid == "PA_QUERY"
    assert handoff.track_sid == "TR_QUERY"
    assert presence.status == "in_call"
    assert command.status == "SUCCEEDED"


@pytest.mark.anyio
async def test_media_ready_version_race_changes_zero_rows(
    handoff_handler_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.handoff_handlers import (
        AgentMediaReadyHandler,
    )

    claim = await _seed_command(
        handoff_handler_session_factory,
        command_type="AGENT_MEDIA_READY",
    )

    async def advance_version() -> None:
        async with handoff_handler_session_factory() as session, session.begin():
            handoff = await session.scalar(select(AiCallHandoffModel).with_for_update())
            handoff.media_state_version += 1
            handoff.media_invalidated_at = NOW + timedelta(milliseconds=500)

    result = await AgentMediaReadyHandler(
        handoff_handler_session_factory,
        MediaProviderStub(_ready_observation(), after_query=advance_version),
        database_clock=_constant_time,
    ).handle(claim, _lease())

    async with handoff_handler_session_factory() as session:
        handoff = await session.scalar(select(AiCallHandoffModel))
        presence = await session.scalar(select(AiCallHandoffAgentModel))
        command = await session.get(AiCallRuntimeCommandModel, claim.command_id)
    assert result.state_changed is False
    assert handoff.status == "accepted"
    assert handoff.media_state_version == 2
    assert presence.status == "claiming"
    assert command.status == "RETRY_WAIT"


@pytest.mark.anyio
async def test_media_invalidated_moves_connected_to_reconnecting_once(
    handoff_handler_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.handoff_handlers import (
        AgentMediaInvalidatedHandler,
        AgentMediaObservation,
    )

    claim = await _seed_command(
        handoff_handler_session_factory,
        command_type="AGENT_MEDIA_INVALIDATED",
        handoff_status="connected",
        presence_status="in_call",
    )
    handler = AgentMediaInvalidatedHandler(
        handoff_handler_session_factory,
        MediaProviderStub(AgentMediaObservation(ready=False)),
        database_clock=_constant_time,
    )
    result = await handler.handle(claim, _lease())
    repeated = await handler.handle(claim, _lease())

    async with handoff_handler_session_factory() as session:
        handoff = await session.scalar(select(AiCallHandoffModel))
        presence = await session.scalar(select(AiCallHandoffAgentModel))
        command = await session.get(AiCallRuntimeCommandModel, claim.command_id)
    assert result.state_changed is True
    assert repeated.state_changed is False
    assert handoff.status == "reconnecting"
    assert presence.status == "claiming"
    assert command.status == "SUCCEEDED"


@pytest.mark.anyio
async def test_rejoin_with_higher_version_returns_to_connected(
    handoff_handler_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.handoff_handlers import (
        AgentMediaReadyHandler,
    )

    claim = await _seed_command(
        handoff_handler_session_factory,
        command_type="AGENT_MEDIA_READY",
        handoff_status="reconnecting",
        presence_status="claiming",
        media_state_version=5,
    )
    result = await AgentMediaReadyHandler(
        handoff_handler_session_factory,
        MediaProviderStub(_ready_observation()),
        database_clock=_constant_time,
    ).handle(claim, _lease())

    async with handoff_handler_session_factory() as session:
        handoff = await session.scalar(select(AiCallHandoffModel))
        presence = await session.scalar(select(AiCallHandoffAgentModel))
    assert result.state_changed is True
    assert handoff.status == "connected"
    assert handoff.media_state_version == 6
    assert presence.status == "in_call"


@pytest.mark.anyio
async def test_owner_loss_during_query_cannot_submit_connected(
    handoff_handler_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.handoff_handlers import (
        AgentMediaReadyHandler,
    )

    claim = await _seed_command(
        handoff_handler_session_factory,
        command_type="AGENT_MEDIA_READY",
    )

    async def replace_owner() -> None:
        async with handoff_handler_session_factory() as session, session.begin():
            record = await session.scalar(select(AiCallRecordModel).with_for_update())
            record.runtime_owner_id = "runtime-2"
            record.runtime_fencing_token = 8

    result = await AgentMediaReadyHandler(
        handoff_handler_session_factory,
        MediaProviderStub(_ready_observation(), after_query=replace_owner),
        database_clock=_constant_time,
    ).handle(claim, _lease())

    async with handoff_handler_session_factory() as session:
        handoff = await session.scalar(select(AiCallHandoffModel))
        presence = await session.scalar(select(AiCallHandoffAgentModel))
    assert result.state_changed is False
    assert handoff.status == "accepted"
    assert presence.status == "claiming"


@pytest.mark.anyio
async def test_end_preempts_handoff_and_preserves_terminal_barrier(
    handoff_handler_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.handoff_handlers import (
        AgentMediaReadyHandler,
    )

    claim = await _seed_command(
        handoff_handler_session_factory,
        command_type="AGENT_MEDIA_READY",
    )

    async def request_end() -> None:
        ids = iter((501, 502))
        async with handoff_handler_session_factory() as session, session.begin():
            await RuntimeCommandRepository(
                session,
                id_generator=ids.__next__,
                database_clock=_constant_time,
            ).request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id="call-1",
                    source="customer",
                    end_reason="customer_hangup",
                    dedupe_key="customer:end:call-1",
                )
            )

    result = await AgentMediaReadyHandler(
        handoff_handler_session_factory,
        MediaProviderStub(_ready_observation(), after_query=request_end),
        database_clock=_constant_time,
    ).handle(claim, _lease())

    async with handoff_handler_session_factory() as session:
        record = await session.scalar(select(AiCallRecordModel))
        handoff = await session.scalar(select(AiCallHandoffModel))
        commands = list(
            (
                await session.scalars(
                    select(AiCallRuntimeCommandModel).order_by(
                        AiCallRuntimeCommandModel.command_seq
                    )
                )
            ).all()
        )
        end_count = await session.scalar(
            select(func.count())
            .select_from(AiCallRuntimeCommandModel)
            .where(AiCallRuntimeCommandModel.command_type == "END_CALL")
        )
    assert result.state_changed is False
    assert record.terminal_requested_at is not None
    assert record.status == "ending"
    assert handoff.status == "accepted"
    assert commands[0].status == "SUPERSEDED"
    assert end_count == 1


@pytest.mark.anyio
async def test_handoff_accepted_handler_only_completes_valid_claim(
    handoff_handler_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.handoff_handlers import (
        HandoffAcceptedHandler,
    )

    claim = await _seed_command(
        handoff_handler_session_factory,
        command_type="HANDOFF_ACCEPTED",
    )
    result = await HandoffAcceptedHandler(
        handoff_handler_session_factory,
        database_clock=_constant_time,
    ).handle(claim, _lease())

    async with handoff_handler_session_factory() as session:
        handoff = await session.scalar(select(AiCallHandoffModel))
        presence = await session.scalar(select(AiCallHandoffAgentModel))
        command = await session.get(AiCallRuntimeCommandModel, claim.command_id)
    assert result.command_completed is True
    assert result.state_changed is False
    assert handoff.status == "accepted"
    assert presence.status == "claiming"
    assert command.status == "SUCCEEDED"


@pytest.mark.anyio
async def test_handoff_accepted_handler_completes_after_media_ready_race(
    handoff_handler_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.handoff_handlers import (
        HandoffAcceptedHandler,
    )

    claim = await _seed_command(
        handoff_handler_session_factory,
        command_type="HANDOFF_ACCEPTED",
        handoff_status="connected",
        presence_status="in_call",
    )
    result = await HandoffAcceptedHandler(
        handoff_handler_session_factory,
        database_clock=_constant_time,
    ).handle(claim, _lease())

    async with handoff_handler_session_factory() as session:
        command = await session.get(AiCallRuntimeCommandModel, claim.command_id)
    assert result.command_completed is True
    assert result.state_changed is False
    assert command.status == "SUCCEEDED"


@pytest.mark.anyio
async def test_cancel_before_connected_releases_claiming_presence(
    handoff_handler_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.handoff_handlers import (
        CancelHandoffHandler,
    )

    claim = await _seed_command(
        handoff_handler_session_factory,
        command_type="CANCEL_HANDOFF",
    )
    result = await CancelHandoffHandler(
        handoff_handler_session_factory,
        database_clock=_constant_time,
    ).handle(claim, _lease())

    async with handoff_handler_session_factory() as session:
        handoff = await session.scalar(select(AiCallHandoffModel))
        presence = await session.scalar(select(AiCallHandoffAgentModel))
        command = await session.get(AiCallRuntimeCommandModel, claim.command_id)
    assert result.state_changed is True
    assert handoff.status == "canceled"
    assert presence.status == "available"
    assert presence.active_handoff_id is None
    assert presence.active_call_id is None
    assert command.status == "SUCCEEDED"


@pytest.mark.anyio
async def test_cancel_after_connected_creates_one_end_call(
    handoff_handler_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.handoff_handlers import (
        CancelHandoffHandler,
    )

    claim = await _seed_command(
        handoff_handler_session_factory,
        command_type="CANCEL_HANDOFF",
        handoff_status="connected",
        presence_status="in_call",
    )
    result = await CancelHandoffHandler(
        handoff_handler_session_factory,
        database_clock=_constant_time,
        id_generator=iter((601, 602)).__next__,
    ).handle(claim, _lease())

    async with handoff_handler_session_factory() as session:
        record = await session.scalar(select(AiCallRecordModel))
        handoff = await session.scalar(select(AiCallHandoffModel))
        commands = list(
            (
                await session.scalars(
                    select(AiCallRuntimeCommandModel).order_by(
                        AiCallRuntimeCommandModel.command_seq
                    )
                )
            ).all()
        )
    assert result.command_completed is True
    assert record.status == "ending"
    assert record.terminal_requested_at is not None
    assert handoff.status == "connected"
    assert [command.command_type for command in commands] == [
        "CANCEL_HANDOFF",
        "END_CALL",
    ]
    assert commands[0].status == "SUPERSEDED"
