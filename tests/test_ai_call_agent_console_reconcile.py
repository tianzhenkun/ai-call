from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.model import (
    AiCallAgentProfileModel,
    AiCallAgentSceneScopeModel,
    AiCallEventModel,
    AiCallFollowUpAttemptModel,
    AiCallFollowUpTaskModel,
    AiCallHandoffAgentModel,
    AiCallHandoffModel,
    AiCallRecordModel,
)
from app.api.v1.system.auth.schema import AuthSchema
from app.api.v1.system.user.model import UserModel
from app.core.base_model import MappedBase
from app.core.exceptions import CustomException
from app.services.ai_call.agent_console_reconciler import (
    AgentConsoleEventBroker,
    AgentConsoleStreamRegistry,
    AiCallAgentConsoleReconciler,
    publish_agent_console_event,
)
from app.services.ai_call.exceptions import AiCallError


def _auth(db, *, user_id: int = 1, tenant_id: str = "tenant-a") -> AuthSchema:
    return AuthSchema(
        db=db,
        check_data_scope=False,
        user=UserModel(
            user_id=user_id,
            tenant_id=tenant_id,
            user_name=f"admin-{user_id}",
            nick_name=f"管理员{user_id}",
            user_type="sys_user",
        ),
    )


@pytest.fixture
async def session_factory(tmp_path):
    database_path = tmp_path / "agent-console-reconcile.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{database_path}")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


async def _seed_management_rows(session_factory) -> None:
    now = datetime.now(timezone.utc)
    async with session_factory() as db, db.begin():
        db.add_all([
            AiCallAgentProfileModel(
                id=20,
                tenant_id="tenant-a",
                agent_identity="agent-20",
                user_id=20,
                enabled=True,
                created_by=1,
                created_at=now,
                updated_by=1,
                updated_at=now,
            ),
            AiCallAgentProfileModel(
                id=21,
                tenant_id="tenant-a",
                agent_identity="agent-21",
                user_id=21,
                enabled=False,
                created_by=1,
                created_at=now,
                updated_by=1,
                updated_at=now,
            ),
            AiCallAgentSceneScopeModel(
                id=200,
                tenant_id="tenant-a",
                agent_identity="agent-20",
                scene_code="intro_contract",
                created_by=1,
                created_at=now,
            ),
            AiCallHandoffAgentModel(
                id=20,
                tenant_id="tenant-a",
                agent_identity="agent-20",
                skill_group="default",
                status="in_call",
                active_handoff_id="handoff-1",
                active_call_id="call-1",
                console_session_id="session-1",
                last_seen_at=now,
                status_updated_at=now,
            ),
            AiCallRecordModel(
                id=100,
                call_id="call-1",
                business_type="lead",
                business_id="lead-1",
                scene_code="intro_contract",
                prompt_source_key="intro_contract",
                entry_type="sip_outbound",
                room_name="room-1",
                participant_identity="sip-call-1",
                callee_phone_number_hash="hash-1",
                callee_phone_number_masked="138****0000",
                status="running",
                started_at=now - timedelta(seconds=30),
            ),
            AiCallHandoffModel(
                id=200,
                tenant_id="tenant-a",
                handoff_id="handoff-1",
                call_id="call-1",
                room_name="room-1",
                scene_code="intro_contract",
                status="connected",
                request_source="customer",
                request_reason="customer_requested_human",
                request_message="请转人工",
                human_agent_identity="agent-20",
                requested_at=now - timedelta(seconds=20),
                accepted_at=now - timedelta(seconds=15),
                connected_at=now - timedelta(seconds=10),
                expires_at=now + timedelta(seconds=40),
            ),
            AiCallFollowUpTaskModel(
                id=300,
                tenant_id="tenant-a",
                source_type="after_call_work",
                source_key="handoff:handoff-1",
                source_call_id="call-1",
                source_handoff_id="handoff-1",
                scene_code="intro_contract",
                business_type="lead",
                business_id="lead-1",
                contact_ref="call:call-1",
                masked_contact="138****0000",
                owner_agent_identity="agent-20",
                status="pending",
                follow_up_reason="继续跟进",
                customer_callback_at=now - timedelta(minutes=5),
                created_at=now - timedelta(hours=1),
                updated_at=now,
            ),
            AiCallFollowUpAttemptModel(
                id=301,
                tenant_id="tenant-a",
                follow_up_id=300,
                agent_identity="agent-20",
                contact_channel="manual_phone",
                attempt_result="no_answer",
                related_call_id=None,
                contacted_at=now - timedelta(minutes=10),
                created_at=now - timedelta(minutes=10),
            ),
        ])


@pytest.mark.anyio
async def test_admin_queries_return_metrics_details_and_string_ids(session_factory) -> None:
    await _seed_management_rows(session_factory)
    async with session_factory() as db:
        service = AiCallAgentConsoleReconciler(db, room_exists=lambda _room: False)
        auth = _auth(db)

        agents = await service.list_agents(auth)
        assert agents["metrics"] == {
            "enabled": 1,
            "online": 1,
            "available": 0,
            "in_call": 1,
            "stale_occupied": 0,
        }
        assert agents["rows"][0]["id"] == "20"
        assert agents["rows"][0]["user_id"] == "20"

        handoffs = await service.list_handoffs(auth)
        assert handoffs["metrics"]["request_count"] == 1
        assert handoffs["metrics"]["connected_rate_within_60_seconds"] == 1.0
        assert handoffs["rows"][0]["id"] == "200"
        detail = await service.get_handoff_detail(auth, "handoff-1")
        assert detail["handoff"]["call_id"] == "call-1"
        assert detail["record"]["prompt_source_key"] == "intro_contract"
        assert detail["follow_up"]["id"] == "300"

        follow_ups = await service.list_follow_ups(auth)
        assert follow_ups["metrics"]["pending"] == 1
        assert follow_ups["metrics"]["overdue"] == 1
        assert follow_ups["rows"][0]["latest_attempt"]["id"] == "301"
        follow_up_detail = await service.get_follow_up_detail(auth, 300)
        assert follow_up_detail["task"]["id"] == "300"
        assert follow_up_detail["attempts"][0]["attempt_result"] == "no_answer"


@pytest.mark.anyio
async def test_release_stale_refuses_active_room_then_audits_safe_release(
    session_factory,
) -> None:
    await _seed_management_rows(session_factory)
    room_active = True

    async def room_exists(_room_name: str) -> bool:
        return room_active

    async with session_factory() as db:
        service = AiCallAgentConsoleReconciler(db, room_exists=room_exists)
        auth = _auth(db)
        with pytest.raises(CustomException) as blocked:
            await service.release_stale_agent(
                auth,
                agent_id=20,
                confirmed=True,
                reason="排查异常占用",
            )
        assert blocked.value.data == {"errorCode": "STALE_RELEASE_NOT_ALLOWED"}

        room_active = False
        released = await service.release_stale_agent(
            auth,
            agent_id=20,
            confirmed=True,
            reason="确认 Room 已不存在",
        )
        await db.commit()
        assert released["status"] == "offline"
        assert released["active_call_id"] is None
        events = list(
            (
                await db.execute(
                    select(AiCallEventModel).where(
                        AiCallEventModel.event_type == "agent_stale_released"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(events) == 1
        assert events[0].payload["operator_user_id"] == "1"
        assert events[0].payload["reason"] == "确认 Room 已不存在"


@pytest.mark.anyio
async def test_release_orphan_in_call_presence_without_active_reference(session_factory) -> None:
    await _seed_management_rows(session_factory)
    async with session_factory() as db:
        presence = await db.get(AiCallHandoffAgentModel, 20)
        presence.active_handoff_id = None
        presence.active_call_id = None
        await db.commit()

        service = AiCallAgentConsoleReconciler(db, room_exists=lambda _room: True)
        released = await service.release_stale_agent(
            _auth(db),
            agent_id=20,
            confirmed=True,
            reason="修复无活动引用的 in_call",
        )

        assert released["status"] == "offline"
        event = (
            await db.execute(
                select(AiCallEventModel).where(
                    AiCallEventModel.event_type == "agent_stale_released"
                )
            )
        ).scalar_one()
        assert event.call_id == "admin-agent-20"


@pytest.mark.anyio
async def test_release_stale_preserves_state_when_room_lookup_fails(session_factory) -> None:
    await _seed_management_rows(session_factory)

    async def room_lookup_failed(_room_name: str) -> bool:
        raise AiCallError(
            error_id="room_lookup_failed",
            msg="LiveKit Room 核验失败",
            status_code=502,
        )

    async with session_factory() as db:
        service = AiCallAgentConsoleReconciler(db, room_exists=room_lookup_failed)
        with pytest.raises(CustomException) as failed:
            await service.release_stale_agent(
                _auth(db),
                agent_id=20,
                confirmed=True,
                reason="尝试释放",
            )
        assert failed.value.status_code == 502
        assert failed.value.data == {"errorCode": "room_lookup_failed"}
        presence = await db.get(AiCallHandoffAgentModel, 20)
        assert presence.status == "in_call"
        assert presence.active_call_id == "call-1"


@pytest.mark.anyio
async def test_reconcile_handoff_is_idempotent_and_audited(session_factory) -> None:
    now = datetime.now(timezone.utc)
    async with session_factory() as db, db.begin():
        db.add(
            AiCallRecordModel(
                id=100,
                call_id="call-expired",
                scene_code="intro_contract",
                entry_type="sip_outbound",
                room_name="room-expired",
                participant_identity="sip-call-expired",
                callee_phone_number_hash="hash-expired",
                callee_phone_number_masked="139****0000",
                status="running",
                started_at=now - timedelta(minutes=2),
            )
        )
        db.add(
            AiCallHandoffModel(
                id=200,
                tenant_id="tenant-a",
                handoff_id="handoff-expired",
                call_id="call-expired",
                room_name="room-expired",
                scene_code="intro_contract",
                status="requested",
                request_source="customer",
                request_reason="customer_requested_human",
                request_message="请转人工",
                requested_at=now - timedelta(seconds=70),
                expires_at=now - timedelta(seconds=10),
            )
        )

    async with session_factory() as db:
        service = AiCallAgentConsoleReconciler(db, room_exists=lambda _room: False)
        auth = _auth(db)
        first = await service.reconcile_handoff(
            auth,
            handoff_id="handoff-expired",
            confirmed=True,
            reason="重新执行超时补偿",
            now=now,
        )
        second = await service.reconcile_handoff(
            auth,
            handoff_id="handoff-expired",
            confirmed=True,
            reason="重新执行超时补偿",
            now=now,
        )
        await db.commit()
        assert first["status"] == "expired"
        assert second["status"] == "expired"
        follow_up_count = await db.scalar(
            select(func.count(AiCallFollowUpTaskModel.id)).where(
                AiCallFollowUpTaskModel.source_handoff_id == "handoff-expired"
            )
        )
        audit_count = await db.scalar(
            select(func.count(AiCallEventModel.id)).where(
                AiCallEventModel.event_type == "handoff_reconciled"
            )
        )
        assert follow_up_count == 1
        assert audit_count == 1


@pytest.mark.anyio
async def test_reconcile_connected_handoff_closes_missing_room_without_requeue(
    session_factory,
) -> None:
    await _seed_management_rows(session_factory)
    async with session_factory() as db:
        service = AiCallAgentConsoleReconciler(db, room_exists=lambda _room: False)
        result = await service.reconcile_handoff(
            _auth(db),
            handoff_id="handoff-1",
            confirmed=True,
            reason="Room 已不存在",
        )

        assert result["status"] == "failed"
        assert result["end_reason"] == "room_missing"
        presence = await db.get(AiCallHandoffAgentModel, 20)
        assert presence.status == "wrap_up_quick"
        assert presence.active_handoff_id == "handoff-1"
        record = (
            await db.execute(select(AiCallRecordModel).where(AiCallRecordModel.call_id == "call-1"))
        ).scalar_one()
        assert record.status == "completed"
        assert record.end_reason == "room_missing"


@pytest.mark.anyio
async def test_event_broker_orders_events_and_supports_sequence_resume() -> None:
    broker = AgentConsoleEventBroker(history_size=10)
    first = await broker.publish("tenant-a", "handoff.requested", {"handoff_id": "h1"})
    second = await broker.publish("tenant-a", "presence.changed", {"status": "available"})

    assert first.sequence < second.sequence
    assert broker.latest_sequence("tenant-a") == second.sequence
    resumed = broker.events_after("tenant-a", first.sequence)
    assert [event.event_type for event in resumed] == ["presence.changed"]
    assert resumed[0].payload == {"status": "available"}


@pytest.mark.anyio
async def test_event_stream_registry_replaces_the_previous_agent_connection() -> None:
    registry = AgentConsoleStreamRegistry()
    first = registry.replace("tenant-a", "agent-20")
    second = registry.replace("tenant-a", "agent-20")

    assert first.replaced.is_set()
    assert not second.replaced.is_set()
    assert not registry.is_current(first)
    assert registry.is_current(second)

    registry.release(first)
    assert registry.is_current(second)
    registry.release(second)
    assert not registry.is_current(second)


@pytest.mark.anyio
async def test_push_failure_is_best_effort_and_does_not_raise() -> None:
    class FailingBroker:
        async def publish(self, *_args, **_kwargs):
            raise RuntimeError("push unavailable")

    result = await publish_agent_console_event(
        "tenant-a",
        "handoff.changed",
        {"handoff_id": "handoff-1"},
        broker=FailingBroker(),
    )

    assert result is None
