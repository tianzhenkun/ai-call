from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.model import (
    AiCallHandoffAgentModel,
    AiCallHandoffModel,
    AiCallRecordModel,
)
from app.services.ai_call.runtime_control.models import AiCallRuntimeCommandModel


@pytest.fixture
async def handoff_session_factory(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'runtime-handoff.db'}")
    async with engine.begin() as connection:
        for table in (
            AiCallRecordModel.__table__,
            AiCallHandoffModel.__table__,
            AiCallHandoffAgentModel.__table__,
            AiCallRuntimeCommandModel.__table__,
        ):
            await connection.run_sync(table.create)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


async def _seed_owner_handoff(
    session_factory,
    *,
    handoff_id: str = "handoff-1",
    call_id: str = "call-1",
    tenant_id: str = "tenant-a",
    agent_identity: str = "agent-1",
    console_session_id: str = "11111111-1111-1111-1111-111111111111",
) -> None:
    now = datetime(2026, 8, 2, 2, tzinfo=timezone.utc)
    suffix = int(handoff_id.rsplit("-", 1)[-1])
    async with session_factory() as session, session.begin():
        session.add(
            AiCallRecordModel(
                id=100 + suffix,
                tenant_id=tenant_id,
                call_id=call_id,
                entry_type="web",
                room_name=f"room-{call_id}",
                participant_identity=f"caller-{call_id}",
                status="ready",
                started_at=now,
                runtime_control_mode="owner_command_v1",
                runtime_owner_id="runtime-1",
                runtime_fencing_token=7,
                runtime_lease_expires_at=now + timedelta(minutes=1),
                runtime_capacity_class="active",
                next_command_seq=2,
                last_applied_command_seq=1,
            )
        )
        session.add(
            AiCallHandoffModel(
                id=200 + suffix,
                tenant_id=tenant_id,
                handoff_id=handoff_id,
                call_id=call_id,
                room_name=f"room-{call_id}",
                scene_code="default",
                status="requested",
                request_source="customer",
                requested_at=now,
                expires_at=now + timedelta(minutes=1),
            )
        )
        if await session.scalar(
            select(func.count())
            .select_from(AiCallHandoffAgentModel)
            .where(
                AiCallHandoffAgentModel.tenant_id == tenant_id,
                AiCallHandoffAgentModel.agent_identity == agent_identity,
            )
        ) == 0:
            session.add(
                AiCallHandoffAgentModel(
                    id=300 + suffix,
                    tenant_id=tenant_id,
                    agent_identity=agent_identity,
                    skill_group="default",
                    status="available",
                    console_session_id=console_session_id,
                    status_updated_at=now,
                )
            )


def _accept_intent(**overrides):
    from app.services.ai_call.runtime_control.handoff_repository import (
        HandoffAcceptIntent,
    )

    values = {
        "tenant_id": "tenant-a",
        "handoff_id": "handoff-1",
        "agent_identity": "agent-1",
        "console_session_id": "11111111-1111-1111-1111-111111111111",
        "idempotency_key": "handoff:handoff-1:accept:agent-1",
    }
    values.update(overrides)
    return HandoffAcceptIntent(**values)


@pytest.mark.anyio
async def test_accept_locks_record_handoff_presence_then_appends_command(
    handoff_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.handoff_repository import (
        RuntimeHandoffRepository,
    )

    await _seed_owner_handoff(handoff_session_factory)
    now = datetime(2026, 8, 2, 2, 0, 5, tzinfo=timezone.utc)
    async with handoff_session_factory() as session, session.begin():
        decision = await RuntimeHandoffRepository(
            session,
            id_generator=lambda: 9001,
            database_clock=lambda _session: _constant_time(now),
        ).accept(_accept_intent())

    async with handoff_session_factory() as session:
        handoff = await session.scalar(select(AiCallHandoffModel))
        presence = await session.scalar(select(AiCallHandoffAgentModel))
        command = await session.scalar(select(AiCallRuntimeCommandModel))

    assert decision.command_id == 9001
    assert decision.command_status == "PENDING"
    assert handoff.status == "accepted"
    assert handoff.human_agent_identity == "agent-1"
    assert handoff.accepted_console_session_id == _accept_intent().console_session_id
    assert presence.status == "claiming"
    assert presence.active_handoff_id == "handoff-1"
    assert presence.active_call_id == "call-1"
    assert command.command_type == "HANDOFF_ACCEPTED"
    assert command.command_seq == 2
    assert command.target_owner_id == "runtime-1"
    assert command.expected_fencing_token == 7
    assert json.loads(command.payload_json) == {
        "agent_identity": "agent-1",
        "console_session_id": _accept_intent().console_session_id,
        "handoff_id": "handoff-1",
    }


@pytest.mark.anyio
async def test_accept_retry_is_idempotent(handoff_session_factory) -> None:
    from app.services.ai_call.runtime_control.handoff_repository import (
        RuntimeHandoffRepository,
    )

    await _seed_owner_handoff(handoff_session_factory)
    now = datetime(2026, 8, 2, 2, 0, 5, tzinfo=timezone.utc)
    async with handoff_session_factory() as session, session.begin():
        repository = RuntimeHandoffRepository(
            session,
            id_generator=lambda: 9001,
            database_clock=lambda _session: _constant_time(now),
        )
        first = await repository.accept(_accept_intent())
        second = await repository.accept(_accept_intent())

    async with handoff_session_factory() as session:
        command_count = await session.scalar(
            select(func.count()).select_from(AiCallRuntimeCommandModel)
        )
        record = await session.scalar(select(AiCallRecordModel))
    assert second == first
    assert command_count == 1
    assert record.next_command_seq == 3


@pytest.mark.anyio
async def test_one_agent_cannot_claim_two_handoffs(handoff_session_factory) -> None:
    from app.services.ai_call.runtime_control.handoff_repository import (
        HandoffClaimConflictError,
        RuntimeHandoffRepository,
    )

    await _seed_owner_handoff(handoff_session_factory)
    await _seed_owner_handoff(
        handoff_session_factory,
        handoff_id="handoff-2",
        call_id="call-2",
    )
    now = datetime(2026, 8, 2, 2, 0, 5, tzinfo=timezone.utc)
    async with handoff_session_factory() as session, session.begin():
        repository = RuntimeHandoffRepository(
            session,
            id_generator=lambda: 9001,
            database_clock=lambda _session: _constant_time(now),
        )
        await repository.accept(_accept_intent())
        with pytest.raises(HandoffClaimConflictError):
            await repository.accept(
                _accept_intent(
                    handoff_id="handoff-2",
                    idempotency_key="handoff:handoff-2:accept:agent-1",
                )
            )


@pytest.mark.anyio
async def test_cancel_request_does_not_release_claim_before_runtime_executes(
    handoff_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.handoff_repository import (
        HandoffCancelIntent,
        RuntimeHandoffRepository,
    )

    await _seed_owner_handoff(handoff_session_factory)
    now = datetime(2026, 8, 2, 2, 0, 5, tzinfo=timezone.utc)
    async with handoff_session_factory() as session, session.begin():
        repository = RuntimeHandoffRepository(
            session,
            id_generator=iter((9001, 9002)).__next__,
            database_clock=lambda _session: _constant_time(now),
        )
        await repository.accept(_accept_intent())
        decision = await repository.request_cancel(
            HandoffCancelIntent(
                tenant_id="tenant-a",
                handoff_id="handoff-1",
                idempotency_key="handoff:handoff-1:cancel:customer",
                reason="customer_cancelled",
            )
        )

    async with handoff_session_factory() as session:
        handoff = await session.scalar(select(AiCallHandoffModel))
        presence = await session.scalar(select(AiCallHandoffAgentModel))
        cancel_command = await session.scalar(
            select(AiCallRuntimeCommandModel).where(
                AiCallRuntimeCommandModel.id == decision.command_id
            )
        )
    assert handoff.status == "accepted"
    assert presence.status == "claiming"
    assert cancel_command.command_type == "CANCEL_HANDOFF"
    assert json.loads(cancel_command.payload_json) == {
        "handoff_id": "handoff-1",
        "reason": "customer_cancelled",
    }


@pytest.mark.anyio
async def test_cross_tenant_handoff_action_changes_zero_rows(
    handoff_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.handoff_repository import (
        HandoffNotFoundError,
        RuntimeHandoffRepository,
    )

    await _seed_owner_handoff(handoff_session_factory)
    async with handoff_session_factory() as session, session.begin():
        repository = RuntimeHandoffRepository(session)
        with pytest.raises(HandoffNotFoundError):
            await repository.accept(_accept_intent(tenant_id="tenant-b"))

    async with handoff_session_factory() as session:
        handoff = await session.scalar(select(AiCallHandoffModel))
        presence = await session.scalar(select(AiCallHandoffAgentModel))
        command_count = await session.scalar(
            select(func.count()).select_from(AiCallRuntimeCommandModel)
        )
    assert handoff.status == "requested"
    assert presence.status == "available"
    assert command_count == 0


async def _constant_time(value: datetime) -> datetime:
    return value
