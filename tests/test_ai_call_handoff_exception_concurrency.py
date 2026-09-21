from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema
from test_ai_call_phase_b1_records import (
    BlockingSystemPromptPlayer,
    FakeLiveKitRoomManager,
    FakeSystemPromptPlayer,
    build_b1_orchestrator,
    wait_until,
)

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import AiCallHandoffModel, AiCallRecordModel
from app.core.base_model import MappedBase
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.handoff_exception_manager import AiCallHandoffExceptionManager
from app.services.ai_call.handoff_service import AiCallHandoffService
from app.services.ai_call.runtime_control.livekit_provider import (
    OwnerRuntimeAgentManager,
    RuntimeProviderResource,
)
from app.services.ai_call.runtime_control.runtime_service import RuntimeRegistry
from app.services.ai_call.session_registry import CallSessionStatus


class SlowSystemPromptPlayer(FakeSystemPromptPlayer):
    async def play(self, **kwargs) -> None:
        await super().play(**kwargs)
        await asyncio.sleep(0.02)


@pytest.fixture
async def closure_context(tmp_path):
    postgres_dsn = os.getenv("AI_CALL_TEST_POSTGRES_DSN", "").strip()
    schema = f"handoff_close_{uuid4().hex}" if postgres_dsn else None
    engine = create_async_engine(
        postgres_dsn or f"sqlite+aiosqlite:///{tmp_path / 'handoff-close.db'}",
        execution_options={"schema_translate_map": {None: schema}},
    )
    async with engine.begin() as connection:
        if schema:
            await connection.execute(CreateSchema(schema))
        await connection.run_sync(MappedBase.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(timezone.utc)
    handoff = AiCallHandoffModel(
        id=1,
        tenant_id="000000",
        handoff_id="handoff-close-once",
        call_id="call-close-once",
        room_name="room-close-once",
        status="expired",
        request_source="customer",
        requested_at=now,
        expires_at=now,
        ended_at=now,
        end_reason="handoff_unanswered",
    )
    async with factory.begin() as db:
        db.add_all([
            AiCallRecordModel(
                id=1,
                tenant_id="000000",
                call_id=handoff.call_id,
                room_name=handoff.room_name,
                participant_identity="customer-test",
                entry_type="web",
                status="connected",
                started_at=now,
            ),
            handoff,
        ])
    room_manager = FakeLiveKitRoomManager()
    prompt_player = SlowSystemPromptPlayer()
    managers = [
        AiCallHandoffExceptionManager(
            orchestrator=build_b1_orchestrator(livekit_room_manager=room_manager),
            session_factory=factory,
            system_prompt_player=prompt_player,
            unavailable_prompt_audio_path=tmp_path / "handoff-unavailable.wav",
        )
        for _ in range(2)
    ]
    try:
        yield factory, handoff, room_manager, prompt_player, managers
    finally:
        await asyncio.gather(*(manager.shutdown() for manager in managers))
        if schema:
            async with engine.begin() as connection:
                await connection.execute(DropSchema(schema, cascade=True))
        await engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize("replay_after_completion", [False, True])
async def test_concurrent_managers_close_expired_handoff_once(
    closure_context, replay_after_completion,
) -> None:
    factory, handoff, room_manager, prompt_player, managers = closure_context
    for manager in managers:
        manager.trigger_exception_close(handoff, call_end_reason="handoff_timeout")
        if replay_after_completion:
            await asyncio.gather(*manager._closure_tasks.values())
    await asyncio.gather(*(task for manager in managers for task in manager._closure_tasks.values()))

    assert len(prompt_player.played) == 1
    assert room_manager.deleted_rooms == [handoff.room_name]
    event_types = [
        event.type
        for manager in managers
        for event in manager.orchestrator.event_store.list_all(handoff.call_id)
    ]
    assert event_types.count("handoff_auto_end_scheduled") == 1
    assert event_types.count("handoff_expired") == 1
    assert event_types.count("handoff_auto_ended") == 1
    assert event_types.count("session_completed") == 1
    async with factory() as db:
        record = await AiCallRecordRepository(db).get_record(handoff.call_id)
        assert record.status == "completed"


@pytest.mark.anyio
async def test_failed_close_retries_without_replaying_completed_prompt(
    closure_context, monkeypatch,
) -> None:
    factory, handoff, room_manager, prompt_player, managers = closure_context
    delete_room = room_manager.delete_room

    async def fail_delete_room(_room_name):
        raise AiCallError(error_id="room_delete_failed", msg="模拟关房失败")

    monkeypatch.setattr(room_manager, "delete_room", fail_delete_room)
    managers[0].trigger_exception_close(handoff, call_end_reason="handoff_timeout")
    outcomes = await asyncio.gather(*managers[0]._closure_tasks.values(), return_exceptions=True)
    assert any(isinstance(outcome, Exception) for outcome in outcomes)
    async with factory() as db:
        saved = await AiCallRecordRepository(db).get_handoff_by_id(handoff.handoff_id)
        assert saved.exception_close_token is None
        assert saved.exception_prompt_completed_at is not None
        assert (await AiCallRecordRepository(db).get_record(handoff.call_id)).status == "connected"

    monkeypatch.setattr(room_manager, "delete_room", delete_room)
    managers[1].trigger_exception_close(handoff, call_end_reason="handoff_timeout")
    await asyncio.gather(*managers[1]._closure_tasks.values())
    assert len(prompt_player.played) == 1
    assert room_manager.deleted_rooms == [handoff.room_name]


@pytest.mark.anyio
@pytest.mark.parametrize("runtime_mode", ["legacy", "legacy_agent_stop_failure", "owner"])
async def test_runtime_room_delete_failure_recovers_without_replaying_prompt(
    closure_context, monkeypatch, runtime_mode,
) -> None:
    factory, handoff, room_manager, prompt_player, managers = closure_context
    manager = managers[0]
    orchestrator = manager.orchestrator
    if runtime_mode == "owner":
        owner_agents = OwnerRuntimeAgentManager(
            orchestrator=orchestrator,
            runtime_registry=RuntimeRegistry(),
        )
        await owner_agents.start(RuntimeProviderResource(
            call_id=handoff.call_id,
            room_name=handoff.room_name,
            customer_participant_identity="customer-test",
            agent_participant_identity="agent-test",
        ))
    else:
        session = await orchestrator.create_sip_session(
            call_id=handoff.call_id, voice=None, prompt=None,
        )
        handoff.room_name = session.room_name
        async with factory.begin() as db:
            repository = AiCallRecordRepository(db)
            (await repository.get_handoff_by_id(handoff.handoff_id)).room_name = session.room_name
            (await repository.get_record(handoff.call_id)).room_name = session.room_name

    if runtime_mode == "legacy_agent_stop_failure":
        async def fail_agent_stop(_call_id):
            raise RuntimeError("模拟 Agent 停止失败")

        monkeypatch.setattr(orchestrator.agent_runner, "stop", fail_agent_stop)

    delete_room = room_manager.delete_room
    delete_attempts = []

    async def fail_first_delete(room_name):
        delete_attempts.append(room_name)
        if len(delete_attempts) == 1:
            raise RuntimeError("模拟 LiveKit 删房失败")
        await delete_room(room_name)

    monkeypatch.setattr(room_manager, "delete_room", fail_first_delete)
    manager.trigger_exception_close(handoff, call_end_reason="handoff_timeout")
    outcomes = await asyncio.gather(*manager._closure_tasks.values(), return_exceptions=True)

    assert any(isinstance(outcome, Exception) for outcome in outcomes)
    assert orchestrator.registry.get(handoff.call_id).status == CallSessionStatus.ENDING
    assert delete_attempts == [handoff.room_name]
    assert room_manager.deleted_rooms == []
    assert len(prompt_player.played) == 1
    event_types = [event.type for event in orchestrator.event_store.list_all(handoff.call_id)]
    assert "handoff_auto_end_runtime_failed" in event_types
    if runtime_mode == "legacy_agent_stop_failure":
        assert any(
            event.type == "session_cleanup_failed" and event.payload["step"] == "agent_stop"
            for event in orchestrator.event_store.list_all(handoff.call_id)
        )
    assert "handoff_auto_ended" not in event_types
    assert "session_completed" not in event_types
    async with factory() as db:
        repository = AiCallRecordRepository(db)
        saved = await repository.get_handoff_by_id(handoff.handoff_id)
        record = await repository.get_record(handoff.call_id)
        assert saved.exception_close_token is None
        assert saved.exception_prompt_completed_at is not None
        assert record.status == "connected"
        assert record.ended_at is None

    await manager.start()
    await wait_until(lambda: any(
        event.type == "handoff_auto_ended"
        for event in orchestrator.event_store.list_all(handoff.call_id)
    ))
    assert orchestrator.registry.get(handoff.call_id).status == CallSessionStatus.COMPLETED
    assert delete_attempts == [handoff.room_name, handoff.room_name]
    assert room_manager.deleted_rooms == [handoff.room_name]
    assert len(prompt_player.played) == 1
    event_types = [event.type for event in orchestrator.event_store.list_all(handoff.call_id)]
    assert event_types.count("handoff_auto_ended") == 1
    assert event_types.count("session_completed") == 1
    async with factory() as db:
        record = await AiCallRecordRepository(db).get_record(handoff.call_id)
        assert record.status == "completed"
        assert record.ended_at is not None


@pytest.mark.anyio
@pytest.mark.parametrize("ensure_room_deleted", [False, True])
async def test_abort_completed_session_only_retries_room_cleanup_when_required(
    monkeypatch, ensure_room_deleted,
) -> None:
    room_manager = FakeLiveKitRoomManager()
    orchestrator = build_b1_orchestrator(livekit_room_manager=room_manager)
    session = await orchestrator.create_sip_session(voice=None, prompt=None)
    delete_room = room_manager.delete_room

    async def fail_delete_room(_room_name):
        raise RuntimeError("模拟 LiveKit 删房失败")

    monkeypatch.setattr(room_manager, "delete_room", fail_delete_room)
    await orchestrator.end_session(session.call_id)
    assert orchestrator.registry.get(session.call_id).status == CallSessionStatus.COMPLETED
    assert room_manager.deleted_rooms == []

    monkeypatch.setattr(room_manager, "delete_room", delete_room)
    await orchestrator.abort_session(
        session.call_id,
        strict_agent_stop=False,
        ensure_room_deleted=ensure_room_deleted,
    )
    assert room_manager.deleted_rooms == ([session.room_name] if ensure_room_deleted else [])
    assert [
        event.type for event in orchestrator.event_store.list_all(session.call_id)
    ].count("session_completed") == 1


@pytest.mark.anyio
async def test_mismatched_runtime_room_keeps_handoff_close_retryable(closure_context) -> None:
    factory, handoff, room_manager, _prompt_player, managers = closure_context
    manager = managers[0]
    session = await manager.orchestrator.create_sip_session(
        call_id=handoff.call_id, voice=None, prompt=None,
    )
    assert session.room_name != handoff.room_name

    manager.trigger_exception_close(handoff, call_end_reason="handoff_timeout")
    outcomes = await asyncio.gather(*manager._closure_tasks.values(), return_exceptions=True)
    assert any(isinstance(outcome, Exception) for outcome in outcomes)
    assert room_manager.deleted_rooms == []
    failures = [
        event for event in manager.orchestrator.event_store.list_all(handoff.call_id)
        if event.type == "handoff_auto_end_runtime_failed"
    ]
    assert len(failures) == 1
    assert failures[0].payload["errorId"] == "handoff_room_mismatch"
    async with factory() as db:
        repository = AiCallRecordRepository(db)
        assert (await repository.get_handoff_by_id(handoff.handoff_id)).exception_close_token is None
        assert (await repository.get_record(handoff.call_id)).status == "connected"


@pytest.mark.anyio
@pytest.mark.parametrize("expired", [False, True])
async def test_close_claim_only_recovers_after_lease_expires(closure_context, expired) -> None:
    factory, handoff, room_manager, prompt_player, managers = closure_context
    now = datetime.now(timezone.utc)
    async with factory.begin() as db:
        saved = await AiCallRecordRepository(db).get_handoff_by_id(handoff.handoff_id)
        saved.exception_close_token = "previous-process"
        saved.exception_close_expires_at = now + timedelta(seconds=-1 if expired else 60)
        saved.exception_prompt_completed_at = now
    managers[0].trigger_exception_close(handoff, call_end_reason="handoff_timeout")
    await asyncio.gather(*managers[0]._closure_tasks.values())
    assert prompt_player.played == []
    assert room_manager.deleted_rooms == ([handoff.room_name] if expired else [])
    async with factory() as db:
        saved = await AiCallRecordRepository(db).get_handoff_by_id(handoff.handoff_id)
        assert saved.exception_close_token == (None if expired else "previous-process")


@pytest.mark.anyio
async def test_lost_lease_stops_executor_without_releasing_successor_claim(
    closure_context, monkeypatch,
) -> None:
    factory, handoff, room_manager, _prompt_player, managers = closure_context
    prompt_player = BlockingSystemPromptPlayer()
    managers[0].system_prompt_player = prompt_player
    monkeypatch.setattr(
        "app.services.ai_call.handoff_exception_manager.EXCEPTION_CLOSE_RENEW_SECONDS", 0.01,
    )
    managers[0].trigger_exception_close(handoff, call_end_reason="handoff_timeout")
    pending = list(managers[0]._closure_tasks.values())
    await asyncio.wait_for(prompt_player.started.wait(), timeout=1)
    async with factory.begin() as db:
        saved = await AiCallRecordRepository(db).get_handoff_by_id(handoff.handoff_id)
        saved.exception_close_token = "successor-process"
        saved.exception_close_expires_at = datetime.now(timezone.utc) + timedelta(seconds=60)
    outcomes = await asyncio.gather(*pending, return_exceptions=True)
    assert any(isinstance(outcome, Exception) for outcome in outcomes)
    assert prompt_player.cancelled.is_set()
    assert room_manager.deleted_rooms == []
    async with factory() as db:
        saved = await AiCallRecordRepository(db).get_handoff_by_id(handoff.handoff_id)
        assert saved.exception_close_token == "successor-process"
        assert saved.exception_prompt_completed_at is None
        assert (await AiCallRecordRepository(db).get_record(handoff.call_id)).status == "connected"


@pytest.mark.anyio
async def test_parallel_timeout_workers_emit_one_expiry_and_close(closure_context) -> None:
    factory, handoff, room_manager, prompt_player, managers = closure_context
    async with factory.begin() as db:
        saved = await AiCallRecordRepository(db).get_handoff_by_id(handoff.handoff_id)
        saved.status = "requested"
        saved.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        saved.ended_at = None
    for manager in managers:
        manager.schedule_timeout(saved)
    await asyncio.gather(*(task for manager in managers for task in manager._timeout_tasks.values()))
    await asyncio.gather(*(task for manager in managers for task in manager._closure_tasks.values()))
    event_types = [
        event.type
        for manager in managers
        for event in manager.orchestrator.event_store.list_all(handoff.call_id)
    ]
    assert event_types.count("handoff_expired") == 1
    assert event_types.count("handoff_auto_ended") == 1
    assert len(prompt_player.played) == 1
    assert room_manager.deleted_rooms == [handoff.room_name]


@pytest.mark.anyio
@pytest.mark.parametrize("entrypoint", ["lazy_read", "scheduled_timeout", "recovery_scan"])
async def test_queue_expiry_does_not_close_a_claim_with_time_remaining(
    closure_context, entrypoint,
) -> None:
    factory, handoff, room_manager, prompt_player, managers = closure_context
    now = datetime.now(timezone.utc)
    async with factory.begin() as db:
        saved = await db.get(AiCallHandoffModel, handoff.id)
        saved.status = "accepted"
        saved.expires_at = now - timedelta(seconds=1)
        saved.claim_expires_at = now + timedelta(seconds=10)
        saved.ended_at = None

    if entrypoint == "lazy_read":
        async with factory.begin() as db:
            current = await AiCallHandoffService(AiCallRecordRepository(db)).get_current(handoff.call_id)
            assert current is not None
            assert current.status == "accepted"
    elif entrypoint == "scheduled_timeout":
        managers[0].schedule_timeout(saved)
        scheduled = [
            event for event in managers[0].orchestrator.event_store.list_all(handoff.call_id)
            if event.type == "handoff_timeout_task_started"
        ]
        assert scheduled[-1].payload["deadlineAt"] == saved.claim_expires_at.isoformat()
    else:
        await managers[0].reconcile_pending_closures()
        await asyncio.gather(*managers[0]._closure_tasks.values())
        assert managers[0]._timeout_tasks == {}

    async with factory() as db:
        assert (await db.get(AiCallHandoffModel, handoff.id)).status == "accepted"
    assert room_manager.deleted_rooms == []
    assert prompt_player.played == []


@pytest.mark.anyio
async def test_stale_lazy_expiry_does_not_overwrite_connected_handoff(closure_context) -> None:
    factory, handoff, _room_manager, _prompt_player, _managers = closure_context
    async with factory.begin() as db:
        saved = await AiCallRecordRepository(db).get_handoff_by_id(handoff.handoff_id)
        saved.status = "requested"
        saved.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    async with factory.begin() as observer:
        repository = AiCallRecordRepository(observer)
        stale_handoff = await repository.get_handoff_by_id(handoff.handoff_id)
        async with factory.begin() as other_db:
            connected = await AiCallRecordRepository(other_db).get_handoff_by_id(handoff.handoff_id)
            connected.status = "connected"
        assert stale_handoff.status == "requested"
        service = AiCallHandoffService(repository)
        assert await service.expire_request(handoff.handoff_id) is None
        assert service.consume_expired_handoffs() == []
    async with factory() as db:
        assert (await AiCallRecordRepository(db).get_handoff_by_id(handoff.handoff_id)).status == "connected"


@pytest.mark.anyio
@pytest.mark.parametrize("status", ["expired", "failed", "canceled", "requested"])
async def test_startup_recovery_closes_orphaned_handoff_without_new_request(
    closure_context, status,
) -> None:
    factory, handoff, room_manager, prompt_player, managers = closure_context
    async with factory.begin() as db:
        saved = await AiCallRecordRepository(db).get_handoff_by_id(handoff.handoff_id)
        saved.status = status
        saved.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        saved.exception_close_token = "crashed-process"
        saved.exception_close_expires_at = saved.expires_at
    await managers[0].start()
    await wait_until(lambda: bool(room_manager.deleted_rooms))
    await wait_until(lambda: any(
        event.type == "handoff_auto_ended"
        for event in managers[0].orchestrator.event_store.list_all(handoff.call_id)
    ))
    await managers[0].shutdown()
    assert len(prompt_player.played) == 1
    assert room_manager.deleted_rooms == [handoff.room_name]


@pytest.mark.anyio
async def test_slow_prompt_renews_lease_and_shutdown_releases_claim(
    closure_context, monkeypatch,
) -> None:
    factory, handoff, room_manager, _prompt_player, managers = closure_context
    prompt_player = BlockingSystemPromptPlayer()
    managers[0].system_prompt_player = prompt_player
    monkeypatch.setattr(
        "app.services.ai_call.handoff_exception_manager.EXCEPTION_CLOSE_RENEW_SECONDS", 0.01,
    )
    managers[0].trigger_exception_close(handoff, call_end_reason="handoff_timeout")
    await asyncio.wait_for(prompt_player.started.wait(), timeout=1)
    deadline = datetime.now(timezone.utc) + timedelta(seconds=2)
    async with factory.begin() as db:
        saved = await AiCallRecordRepository(db).get_handoff_by_id(handoff.handoff_id)
        saved.exception_close_expires_at = deadline
    renewed = False
    for _ in range(50):
        async with factory() as db:
            saved = await AiCallRecordRepository(db).get_handoff_by_id(handoff.handoff_id)
            if saved.exception_close_expires_at.replace(tzinfo=timezone.utc) > deadline + timedelta(seconds=30):
                renewed = True
                break
        await asyncio.sleep(0.01)
    assert renewed
    await managers[0].shutdown()
    assert prompt_player.cancelled.is_set()
    assert room_manager.deleted_rooms == []
    async with factory() as db:
        saved = await AiCallRecordRepository(db).get_handoff_by_id(handoff.handoff_id)
        assert saved.exception_close_token is None
        assert saved.exception_prompt_completed_at is None
