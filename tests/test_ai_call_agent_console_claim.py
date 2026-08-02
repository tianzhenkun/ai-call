from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import jwt
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call import AiCallRouter
from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import (
    AiCallAgentProfileModel,
    AiCallAgentSceneScopeModel,
    AiCallDialogueSegmentModel,
    AiCallFollowUpTaskModel,
    AiCallHandoffAgentModel,
    AiCallHandoffModel,
    AiCallRecordModel,
)
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
)
from app.api.v1.system.auth.schema import AuthSchema
from app.api.v1.system.user.model import UserModel
from app.core.base_model import MappedBase
from app.core.exceptions import CustomException
from app.services.ai_call.agent_console_service import AiCallAgentConsoleService
from app.services.ai_call.handoff_service import AiCallHandoffService
from app.services.ai_call.livekit_room import LiveKitRoomManager


def _auth(db, *, user_id: int, tenant_id: str = "tenant-a") -> AuthSchema:
    return AuthSchema(
        db=db,
        check_data_scope=False,
        user=UserModel(
            user_id=user_id,
            tenant_id=tenant_id,
            user_name=f"agent-{user_id}",
            nick_name=f"坐席{user_id}",
            user_type="sys_user",
        ),
    )


@pytest.fixture
async def session_factory(tmp_path):
    database_path = tmp_path / "agent-console-claim.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{database_path}",
        connect_args={"timeout": 5},
    )
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


async def _seed_agent(
    session_factory,
    *,
    user_id: int,
    agent_identity: str,
    console_session_id: str,
    scene_codes: tuple[str, ...] = ("intro_contract",),
    status: str = "available",
) -> None:
    now = datetime.now(timezone.utc)
    async with session_factory() as db, db.begin():
        db.add(
            AiCallAgentProfileModel(
                id=user_id,
                tenant_id="tenant-a",
                agent_identity=agent_identity,
                user_id=user_id,
                enabled=True,
                created_by=1,
                created_at=now,
                updated_by=1,
                updated_at=now,
            )
        )
        db.add_all(
            [
                AiCallAgentSceneScopeModel(
                    id=user_id * 100 + index,
                    tenant_id="tenant-a",
                    agent_identity=agent_identity,
                    scene_code=scene_code,
                    created_by=1,
                    created_at=now,
                )
                for index, scene_code in enumerate(scene_codes, start=1)
            ]
        )
        db.add(
            AiCallHandoffAgentModel(
                id=user_id,
                tenant_id="tenant-a",
                agent_identity=agent_identity,
                skill_group="default",
                status=status,
                active_handoff_id=None,
                active_call_id=None,
                console_session_id=console_session_id,
                last_seen_at=now,
                status_updated_at=now,
            )
        )


async def _seed_handoff(
    session_factory,
    *,
    row_id: int,
    handoff_id: str,
    scene_code: str = "intro_contract",
    tenant_id: str = "tenant-a",
    status: str = "requested",
    expires_delta: timedelta = timedelta(seconds=60),
) -> None:
    now = datetime.now(timezone.utc)
    async with session_factory() as db, db.begin():
        db.add(
            AiCallHandoffModel(
                id=row_id,
                tenant_id=tenant_id,
                handoff_id=handoff_id,
                call_id=f"call-{handoff_id}",
                room_name=f"room-{handoff_id}",
                scene_code=scene_code,
                status=status,
                request_source="customer",
                request_reason="customer_requested_human",
                request_message="请转人工",
                requested_at=now,
                expires_at=now + expires_delta,
            )
        )


def _error_code(exc: CustomException) -> str | None:
    return exc.data.get("errorCode") if isinstance(exc.data, dict) else None


def test_agent_console_task3_routes_are_registered() -> None:
    paths = {route.path for route in AiCallRouter.routes}
    assert {
        "/ai-call/agent-console/bootstrap",
        "/ai-call/agent-console/presence/online",
        "/ai-call/agent-console/presence/pause",
        "/ai-call/agent-console/presence/offline",
        "/ai-call/agent-console/presence/heartbeat",
        "/ai-call/agent-console/handoffs/pending",
        "/ai-call/agent-console/handoffs/{handoff_id}/claim",
        "/ai-call/agent-console/handoffs/{handoff_id}/media-ready",
        "/ai-call/agent-console/handoffs/{handoff_id}/reconnect-token",
    } <= paths


@pytest.mark.anyio
async def test_livekit_media_ready_requires_unmuted_microphone_track() -> None:
    manager = LiveKitRoomManager("ws://livekit.test", "key", "secret", 60)

    async def microphone_participant(**_kwargs):
        return {"tracks": [{"type": 0, "source": 2, "muted": False}]}

    manager._post_room_service = microphone_participant
    assert await manager.has_published_microphone("room-1", "human-agent-1") is True

    async def muted_participant(**_kwargs):
        return {"tracks": [{"type": 0, "source": 2, "muted": True}]}

    manager._post_room_service = muted_participant
    assert await manager.has_published_microphone("room-1", "human-agent-1") is False


def test_livekit_participant_lookup_admin_token_is_scoped_to_room() -> None:
    manager = LiveKitRoomManager("ws://livekit.test", "key", "secret", 60)

    token = manager._issue_room_admin_token(room_name="room-1")
    payload = jwt.decode(token, "secret", algorithms=["HS256"])

    assert payload["video"]["roomAdmin"] is True
    assert payload["video"]["room"] == "room-1"


@pytest.mark.anyio
async def test_livekit_room_lookup_uses_exact_room_name() -> None:
    manager = LiveKitRoomManager("ws://livekit.test", "key", "secret", 60)

    async def list_rooms(**_kwargs):
        return {"rooms": [{"name": "room-1"}, {"name": "room-2"}]}

    manager._post_room_service = list_rooms
    assert await manager.room_exists("room-1") is True
    assert await manager.room_exists("room-missing") is False


@pytest.mark.anyio
async def test_handoff_creation_freezes_source_call_scene_code(session_factory) -> None:
    now = datetime.now(timezone.utc)
    async with session_factory() as db:
        db.add(
            AiCallRecordModel(
                id=1,
                call_id="call-scene",
                business_type="lead",
                business_id="lead-1",
                scene_code="intro_contract",
                entry_type="web",
                room_name="room-scene",
                participant_identity="customer-scene",
                status="running",
                started_at=now,
            )
        )
        await db.commit()
        handoff, created = await AiCallHandoffService(
            AiCallRecordRepository(db),
            request_timeout_seconds=60,
        ).create_request(
            call_id="call-scene",
            room_name="room-scene",
            source="customer",
            reason="customer_requested_human",
            request_message="请转人工",
        )
        await db.commit()

    assert created is True
    assert handoff.scene_code == "intro_contract"


@pytest.mark.anyio
async def test_presence_requires_preflight_and_enforces_console_session(session_factory) -> None:
    console_session_id = str(uuid4())
    await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        console_session_id=console_session_id,
        status="offline",
    )

    async with session_factory() as db:
        service = AiCallAgentConsoleService(db)
        auth = _auth(db, user_id=20)

        with pytest.raises(CustomException) as preflight:
            await service.online(
                auth,
                console_session_id=console_session_id,
                device_preflight_passed=False,
            )
        assert _error_code(preflight.value) == "MEDIA_NOT_READY"

        presence = await service.online(
            auth,
            console_session_id=console_session_id,
            device_preflight_passed=True,
        )
        assert presence.status == "available"
        assert presence.console_session_id == console_session_id
        assert presence.skill_group == "default"

        with pytest.raises(CustomException) as conflict:
            await service.heartbeat(auth, console_session_id=str(uuid4()))
        assert _error_code(conflict.value) == "CONSOLE_SESSION_CONFLICT"

        paused = await service.pause(auth, console_session_id=console_session_id)
        assert paused.status == "paused"
        offline = await service.offline(auth, console_session_id=console_session_id)
        assert offline.status == "offline"


@pytest.mark.anyio
async def test_bootstrap_restores_completed_handoff_during_quick_wrap_up(
    session_factory,
) -> None:
    console_session_id = str(uuid4())
    await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        console_session_id=console_session_id,
        status="wrap_up_quick",
    )
    await _seed_handoff(
        session_factory,
        row_id=1,
        handoff_id="wrap-up",
        status="completed",
    )
    async with session_factory() as db, db.begin():
        presence_row = (
            await db.execute(
                select(AiCallHandoffAgentModel).where(
                    AiCallHandoffAgentModel.agent_identity == "agent-20"
                )
            )
        ).scalar_one()
        presence_row.active_handoff_id = "wrap-up"
        presence_row.active_call_id = "call-wrap-up"
        handoff_row = (
            await db.execute(
                select(AiCallHandoffModel).where(
                    AiCallHandoffModel.handoff_id == "wrap-up"
                )
            )
        ).scalar_one()
        handoff_row.human_agent_identity = "agent-20"
        handoff_row.accepted_console_session_id = console_session_id

    async with session_factory() as db:
        payload = await AiCallAgentConsoleService(db).bootstrap_payload(
            _auth(db, user_id=20)
        )

    assert payload["presence"]["status"] == "wrap_up_quick"
    assert payload["current_handoff"]["handoff_id"] == "wrap-up"
    assert payload["current_handoff"]["call_id"] == "call-wrap-up"
    assert payload["current_handoff"]["status"] == "completed"


@pytest.mark.anyio
async def test_stale_available_presence_is_persisted_as_offline(session_factory) -> None:
    console_session_id = str(uuid4())
    await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        console_session_id=console_session_id,
    )
    async with session_factory() as db, db.begin():
        presence = (
            await db.execute(
                select(AiCallHandoffAgentModel).where(
                    AiCallHandoffAgentModel.agent_identity == "agent-20"
                )
            )
        ).scalar_one()
        presence.last_seen_at = datetime.now(timezone.utc) - timedelta(seconds=31)

    async with session_factory() as db:
        with pytest.raises(CustomException) as stale:
            await AiCallAgentConsoleService(db).list_pending_handoffs(
                _auth(db, user_id=20),
                console_session_id=console_session_id,
            )
    assert _error_code(stale.value) == "AGENT_NOT_AVAILABLE"

    async with session_factory() as db:
        status_value = (
            await db.execute(
                select(AiCallHandoffAgentModel.status).where(
                    AiCallHandoffAgentModel.agent_identity == "agent-20"
                )
            )
        ).scalar_one()
    assert status_value == "offline"


@pytest.mark.anyio
async def test_pending_pool_filters_tenant_status_expiry_and_scene_scope(session_factory) -> None:
    console_session_id = str(uuid4())
    await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        console_session_id=console_session_id,
    )
    await _seed_handoff(session_factory, row_id=1, handoff_id="visible")
    await _seed_handoff(
        session_factory,
        row_id=2,
        handoff_id="wrong-scene",
        scene_code="intro_overseas",
    )
    await _seed_handoff(
        session_factory,
        row_id=3,
        handoff_id="expired",
        expires_delta=timedelta(seconds=-1),
    )
    await _seed_handoff(
        session_factory,
        row_id=4,
        handoff_id="already-accepted",
        status="accepted",
    )
    await _seed_handoff(
        session_factory,
        row_id=5,
        handoff_id="other-tenant",
        tenant_id="tenant-b",
    )

    async with session_factory() as db:
        service = AiCallAgentConsoleService(db)
        rows = await service.list_pending_handoffs(
            _auth(db, user_id=20),
            console_session_id=console_session_id,
        )

    assert [row.handoff_id for row in rows] == ["visible"]


@pytest.mark.anyio
async def test_handoff_payloads_include_batched_business_context_and_recent_dialogue(
    session_factory,
) -> None:
    now = datetime.now(timezone.utc)
    call_id = "call-visible"
    dialogue = [
        ("ai", "较早的开场内容"),
        ("customer", "我想确认合同到期时间"),
        ("ai", "合同将在本月底到期"),
        ("customer", "续约价格有没有变化"),
        ("ai", "具体价格需要人工进一步确认"),
        ("human_agent", "我来协助您确认"),
        ("customer", "请帮我确认下周是否可以续约"),
    ]
    await _seed_handoff(session_factory, row_id=1, handoff_id="visible")
    async with session_factory() as db, db.begin():
        db.add_all(
            [
                AiCallRecordModel(
                    id=100,
                    call_id=call_id,
                    business_type="lead",
                    business_id="lead-100",
                    scene_code="intro_contract",
                    entry_type="sip_outbound",
                    room_name="room-visible",
                    participant_identity="sip-visible",
                    callee_phone_number_hash="hash-visible",
                    callee_phone_number_masked="138****0000",
                    status="running",
                    started_at=now,
                ),
                AiCallOutboundTargetModel(
                    id=200,
                    tenant_id="tenant-a",
                    task_id=300,
                    validation_id=400,
                    source_validation_row_id=500,
                    source_row_number=1,
                    phone_number="encrypted-phone",
                    customer_name="张先生",
                    status="calling",
                    attempt_count=1,
                    created_at=now,
                    updated_at=now,
                ),
                AiCallOutboundAttemptModel(
                    id=201,
                    tenant_id="tenant-a",
                    task_id=300,
                    target_id=200,
                    attempt_no=1,
                    call_id=call_id,
                    dialer_type="sip_outbound",
                    status="connected",
                    started_at=now,
                    created_at=now,
                    updated_at=now,
                ),
                *[
                    AiCallDialogueSegmentModel(
                        id=600 + segment_no,
                        call_id=call_id,
                        segment_no=segment_no,
                        speaker_type=speaker_type,
                        source="test",
                        source_segment_id=f"segment-{segment_no}",
                        segment_text=segment_text,
                        segment_status="final",
                        started_at=now + timedelta(seconds=segment_no),
                    )
                    for segment_no, (speaker_type, segment_text) in enumerate(
                        dialogue,
                        start=1,
                    )
                ],
            ]
        )

    async with session_factory() as db:
        handoffs = await AiCallRecordRepository(db).list_console_pending_handoffs(
            tenant_id="tenant-a",
            scene_codes=["intro_contract"],
            now=now,
            limit=50,
        )
        payloads = await AiCallAgentConsoleService(db).handoff_payloads(handoffs)

    assert len(payloads) == 1
    payload = payloads[0]
    assert payload["masked_customer_name"] == "张先生"
    assert payload["masked_contact"] == "138****0000"
    assert payload["business_type"] == "lead"
    assert payload["business_id"] == "lead-100"
    assert [turn["speaker_type"] for turn in payload["recent_dialogue"]] == [
        "customer",
        "ai",
        "customer",
        "ai",
        "human_agent",
        "customer",
    ]
    assert payload["handoff_summary"] == (
        "请转人工；客户最近表示：“请帮我确认下周是否可以续约”"
    )
    assert payload["pending_items"] == [
        {"text": "请转人工", "evidence": "转人工请求"},
        {
            "text": "请帮我确认下周是否可以续约",
            "evidence": "客户最近表达",
        },
    ]


@pytest.mark.anyio
async def test_handoff_context_returns_all_final_ai_customer_dialogue_in_order(
    session_factory,
) -> None:
    console_session_id = str(uuid4())
    await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        console_session_id=console_session_id,
    )
    await _seed_handoff(session_factory, row_id=1, handoff_id="handoff-1")
    now = datetime.now(timezone.utc)
    valid_dialogue = [
        ("ai", "AI 第 1 条"),
        ("customer", "客户第 2 条"),
        ("ai", "AI 第 3 条"),
        ("customer", "客户第 4 条"),
        ("ai", "AI 第 5 条"),
        ("customer", "客户第 6 条"),
        ("ai", "AI 第 7 条"),
        ("customer", "客户第 8 条"),
    ]
    async with session_factory() as db, db.begin():
        db.add_all(
            [
                AiCallDialogueSegmentModel(
                    id=700 + segment_no,
                    call_id="call-handoff-1",
                    segment_no=segment_no,
                    speaker_type=speaker_type,
                    source="test",
                    source_segment_id=f"valid-{segment_no}",
                    segment_text=text,
                    segment_status="final",
                    started_at=now + timedelta(seconds=segment_no),
                )
                for segment_no, (speaker_type, text) in enumerate(
                    valid_dialogue,
                    start=1,
                )
            ]
            + [
                AiCallDialogueSegmentModel(
                    id=709,
                    call_id="call-handoff-1",
                    segment_no=9,
                    speaker_type="human_agent",
                    source="test",
                    source_segment_id="human-9",
                    segment_text="人工坐席内容不属于转接前上下文",
                    segment_status="final",
                    started_at=now + timedelta(seconds=9),
                ),
                AiCallDialogueSegmentModel(
                    id=710,
                    call_id="call-handoff-1",
                    segment_no=10,
                    speaker_type="ai",
                    source="test",
                    source_segment_id="draft-10",
                    segment_text="未完成内容",
                    segment_status="draft",
                    started_at=now + timedelta(seconds=10),
                ),
            ]
        )

    async with session_factory() as db:
        payload = await AiCallAgentConsoleService(db).handoff_context_payload(
            _auth(db, user_id=20),
            handoff_id="handoff-1",
            console_session_id=console_session_id,
        )

    assert [turn["text"] for turn in payload["dialogue"]] == [
        text for _speaker, text in valid_dialogue
    ]
    assert [turn["speaker_type"] for turn in payload["dialogue"]] == [
        speaker for speaker, _text in valid_dialogue
    ]
    assert "pending_items" not in payload
    assert "recent_dialogue" not in payload


@pytest.mark.anyio
async def test_handoff_context_requires_current_console_session(
    session_factory,
) -> None:
    console_session_id = str(uuid4())
    await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        console_session_id=console_session_id,
    )
    await _seed_handoff(session_factory, row_id=1, handoff_id="handoff-1")

    async with session_factory() as db:
        with pytest.raises(CustomException) as conflict:
            await AiCallAgentConsoleService(db).handoff_context_payload(
                _auth(db, user_id=20),
                handoff_id="handoff-1",
                console_session_id=str(uuid4()),
            )

    assert _error_code(conflict.value) == "CONSOLE_SESSION_CONFLICT"


async def _claim(
    session_factory,
    *,
    user_id: int,
    handoff_id: str,
    console_session_id: str,
):
    async with session_factory() as db:
        async with db.begin():
            service = AiCallAgentConsoleService(db)
            try:
                return await service.claim_handoff(
                    _auth(db, user_id=user_id),
                    handoff_id=handoff_id,
                    console_session_id=console_session_id,
                )
            except CustomException as exc:
                return exc


@pytest.mark.anyio
async def test_claim_can_build_response_payload_before_leaving_request_transaction(
    session_factory,
) -> None:
    console_session_id = str(uuid4())
    await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        console_session_id=console_session_id,
    )
    await _seed_handoff(session_factory, row_id=1, handoff_id="handoff-1")

    async with session_factory() as db, db.begin():
        service = AiCallAgentConsoleService(db)
        claim_result = await service.claim_handoff_with_payload(
            _auth(db, user_id=20),
            handoff_id="handoff-1",
            console_session_id=console_session_id,
        )

    assert claim_result.payload["handoff_id"] == "handoff-1"
    assert claim_result.payload["status"] == "accepted"
    assert claim_result.handoff.status == "accepted"


@pytest.mark.anyio
async def test_owner_handoff_claim_uses_authenticated_agent_and_appends_command(
    session_factory,
    monkeypatch,
) -> None:
    from app.services.ai_call import agent_console_service
    from app.services.ai_call.runtime_control.handoff_repository import (
        RuntimeHandoffRepository,
    )
    from app.services.ai_call.runtime_control.models import AiCallRuntimeCommandModel

    console_session_id = str(uuid4())
    await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        console_session_id=console_session_id,
    )
    await _seed_handoff(session_factory, row_id=1, handoff_id="handoff-1")
    now = datetime.now(timezone.utc)

    async def database_clock(_session):
        return now

    monkeypatch.setattr(
        agent_console_service,
        "RuntimeHandoffRepository",
        lambda session: RuntimeHandoffRepository(
            session,
            database_clock=database_clock,
        ),
    )
    async with session_factory() as db, db.begin():
        db.add(
            AiCallRecordModel(
                id=900,
                tenant_id="tenant-a",
                call_id="call-handoff-1",
                entry_type="web",
                room_name="room-call-handoff-1",
                participant_identity="caller-call-handoff-1",
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

    async with session_factory() as db, db.begin():
        claim_result = await AiCallAgentConsoleService(db).claim_handoff_with_payload(
            _auth(db, user_id=20),
            handoff_id="handoff-1",
            console_session_id=console_session_id,
            idempotency_key="handoff:handoff-1:claim:browser-1",
        )

    async with session_factory() as db:
        command = await db.scalar(select(AiCallRuntimeCommandModel))
        presence = await db.scalar(
            select(AiCallHandoffAgentModel).where(
                AiCallHandoffAgentModel.agent_identity == "agent-20"
            )
        )
    assert claim_result.command.command_id == command.id
    assert claim_result.command.command_status == "PENDING"
    assert json.loads(command.payload_json)["agent_identity"] == "agent-20"
    assert presence.status == "claiming"


@pytest.mark.anyio
async def test_two_agents_claiming_same_handoff_only_one_succeeds(session_factory) -> None:
    first_session = str(uuid4())
    second_session = str(uuid4())
    await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        console_session_id=first_session,
    )
    await _seed_agent(
        session_factory,
        user_id=21,
        agent_identity="agent-21",
        console_session_id=second_session,
    )
    await _seed_handoff(session_factory, row_id=1, handoff_id="handoff-1")

    results = await asyncio.gather(
        _claim(
            session_factory,
            user_id=20,
            handoff_id="handoff-1",
            console_session_id=first_session,
        ),
        _claim(
            session_factory,
            user_id=21,
            handoff_id="handoff-1",
            console_session_id=second_session,
        ),
    )

    successes = [result for result in results if isinstance(result, AiCallHandoffModel)]
    conflicts = [result for result in results if isinstance(result, CustomException)]
    assert len(successes) == 1
    assert len(conflicts) == 1
    assert _error_code(conflicts[0]) == "HANDOFF_ALREADY_CLAIMED"

    async with session_factory() as db:
        handoff = (
            await db.execute(
                select(AiCallHandoffModel).where(AiCallHandoffModel.handoff_id == "handoff-1")
            )
        ).scalar_one()
        agents = list(
            (
                await db.execute(
                    select(AiCallHandoffAgentModel).order_by(
                        AiCallHandoffAgentModel.agent_identity
                    )
                )
            )
            .scalars()
            .all()
        )
    assert handoff.status == "accepted"
    assert handoff.accepted_console_session_id in {first_session, second_session}
    assert handoff.claim_expires_at is not None
    assert (
        handoff.claim_expires_at.replace(tzinfo=timezone.utc)
        - handoff.accepted_at.replace(tzinfo=timezone.utc)
    ) == timedelta(seconds=15)
    assert sum(agent.status == "claiming" for agent in agents) == 1
    assert sum(agent.status == "available" for agent in agents) == 1


@pytest.mark.anyio
async def test_same_agent_claiming_two_handoffs_only_one_succeeds(session_factory) -> None:
    console_session_id = str(uuid4())
    await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        console_session_id=console_session_id,
    )
    await _seed_handoff(session_factory, row_id=1, handoff_id="handoff-1")
    await _seed_handoff(session_factory, row_id=2, handoff_id="handoff-2")

    results = await asyncio.gather(
        _claim(
            session_factory,
            user_id=20,
            handoff_id="handoff-1",
            console_session_id=console_session_id,
        ),
        _claim(
            session_factory,
            user_id=20,
            handoff_id="handoff-2",
            console_session_id=console_session_id,
        ),
    )

    successes = [result for result in results if isinstance(result, AiCallHandoffModel)]
    conflicts = [result for result in results if isinstance(result, CustomException)]
    assert len(successes) == 1
    assert len(conflicts) == 1
    assert _error_code(conflicts[0]) == "AGENT_ALREADY_IN_CALL"

    async with session_factory() as db:
        statuses = list(
            (
                await db.execute(
                    select(AiCallHandoffModel.status).order_by(AiCallHandoffModel.handoff_id)
                )
            )
            .scalars()
            .all()
        )
    assert statuses == ["accepted", "requested"]


@pytest.mark.anyio
async def test_same_agent_session_claim_retry_returns_same_result(session_factory) -> None:
    console_session_id = str(uuid4())
    await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        console_session_id=console_session_id,
    )
    await _seed_handoff(session_factory, row_id=1, handoff_id="handoff-1")

    first = await _claim(
        session_factory,
        user_id=20,
        handoff_id="handoff-1",
        console_session_id=console_session_id,
    )
    second = await _claim(
        session_factory,
        user_id=20,
        handoff_id="handoff-1",
        console_session_id=console_session_id,
    )

    assert isinstance(first, AiCallHandoffModel)
    assert isinstance(second, AiCallHandoffModel)
    assert second.id == first.id
    assert second.accepted_at.replace(tzinfo=timezone.utc) == first.accepted_at
    assert second.claim_expires_at.replace(tzinfo=timezone.utc) == first.claim_expires_at


@pytest.mark.anyio
async def test_media_ready_requires_livekit_microphone_before_connected(session_factory) -> None:
    console_session_id = str(uuid4())
    await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        console_session_id=console_session_id,
    )
    await _seed_handoff(session_factory, row_id=1, handoff_id="handoff-1")
    claimed = await _claim(
        session_factory,
        user_id=20,
        handoff_id="handoff-1",
        console_session_id=console_session_id,
    )
    assert isinstance(claimed, AiCallHandoffModel)
    assert claimed.status == "accepted"

    async def microphone_missing(_room_name: str, _participant_identity: str) -> bool:
        return False

    async with session_factory() as db:
        service = AiCallAgentConsoleService(db, participant_verifier=microphone_missing)
        with pytest.raises(CustomException) as not_ready:
            await service.media_ready(
                _auth(db, user_id=20),
                handoff_id="handoff-1",
                console_session_id=console_session_id,
                participant_identity="human-agent-handoff-1",
            )
    assert _error_code(not_ready.value) == "MEDIA_NOT_READY"

    async def microphone_ready(_room_name: str, _participant_identity: str) -> bool:
        return True

    async with session_factory() as db, db.begin():
        connected = await AiCallAgentConsoleService(
            db,
            participant_verifier=microphone_ready,
        ).media_ready(
            _auth(db, user_id=20),
            handoff_id="handoff-1",
            console_session_id=console_session_id,
            participant_identity="human-agent-handoff-1",
        )
    assert connected.status == "connected"


@pytest.mark.anyio
async def test_claim_timeout_requeues_before_total_wait_deadline(session_factory) -> None:
    console_session_id = str(uuid4())
    await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        console_session_id=console_session_id,
    )
    await _seed_handoff(session_factory, row_id=1, handoff_id="handoff-1")
    await _claim(
        session_factory,
        user_id=20,
        handoff_id="handoff-1",
        console_session_id=console_session_id,
    )
    now = datetime.now(timezone.utc)
    async with session_factory() as db, db.begin():
        handoff = (
            await db.execute(select(AiCallHandoffModel).where(AiCallHandoffModel.id == 1))
        ).scalar_one()
        handoff.claim_expires_at = now - timedelta(seconds=1)
        handoff.expires_at = now + timedelta(seconds=30)

    async with session_factory() as db, db.begin():
        reconciled = await AiCallAgentConsoleService(db).reconcile_handoff_timeout(
            "tenant-a",
            "handoff-1",
            now=now,
        )
    assert reconciled.status == "requested"
    assert reconciled.human_agent_identity is None

    async with session_factory() as db:
        presence = (
            await db.execute(select(AiCallHandoffAgentModel).where(AiCallHandoffAgentModel.id == 20))
        ).scalar_one()
    assert presence.status == "available"
    assert presence.active_handoff_id is None


@pytest.mark.anyio
async def test_total_wait_timeout_creates_one_unanswered_follow_up(session_factory) -> None:
    await _seed_handoff(
        session_factory,
        row_id=1,
        handoff_id="handoff-1",
        expires_delta=timedelta(seconds=-1),
    )
    now = datetime.now(timezone.utc)
    async with session_factory() as db:
        service = AiCallAgentConsoleService(db)
        await service.reconcile_handoff_timeout("tenant-a", "handoff-1", now=now)
        await service.reconcile_handoff_timeout("tenant-a", "handoff-1", now=now)
        await db.commit()

    async with session_factory() as db:
        handoff = (
            await db.execute(select(AiCallHandoffModel).where(AiCallHandoffModel.id == 1))
        ).scalar_one()
        follow_ups = list((await db.execute(select(AiCallFollowUpTaskModel))).scalars().all())
    assert handoff.status == "expired"
    assert len(follow_ups) == 1
    assert follow_ups[0].source_type == "handoff_unanswered"
    assert follow_ups[0].status == "pending"
    assert follow_ups[0].customer_callback_at is None


@pytest.mark.anyio
async def test_reconnect_timeout_fails_without_returning_to_public_pool(session_factory) -> None:
    console_session_id = str(uuid4())
    await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        console_session_id=console_session_id,
    )
    await _seed_handoff(session_factory, row_id=1, handoff_id="handoff-1")
    await _claim(
        session_factory,
        user_id=20,
        handoff_id="handoff-1",
        console_session_id=console_session_id,
    )
    async with session_factory() as db, db.begin():
        handoff = (
            await db.execute(select(AiCallHandoffModel).where(AiCallHandoffModel.id == 1))
        ).scalar_one()
        handoff.status = "connected"
        presence = (
            await db.execute(select(AiCallHandoffAgentModel).where(AiCallHandoffAgentModel.id == 20))
        ).scalar_one()
        presence.status = "in_call"

    now = datetime.now(timezone.utc)
    async with session_factory() as db, db.begin():
        reconnecting = await AiCallAgentConsoleService(db).begin_reconnect(
            _auth(db, user_id=20),
            handoff_id="handoff-1",
            console_session_id=console_session_id,
            now=now,
        )
    async with session_factory() as db, db.begin():
        reconnecting = (
            await db.execute(select(AiCallHandoffModel).where(AiCallHandoffModel.id == 1))
        ).scalar_one()
        reconnecting.reconnect_expires_at = now - timedelta(seconds=1)

    async with session_factory() as db, db.begin():
        failed = await AiCallAgentConsoleService(db).reconcile_handoff_timeout(
            "tenant-a",
            "handoff-1",
            now=now,
        )
    assert failed.status == "failed"
    assert failed.end_reason == "reconnect_timeout"

    async with session_factory() as db:
        presence = (
            await db.execute(select(AiCallHandoffAgentModel).where(AiCallHandoffAgentModel.id == 20))
        ).scalar_one()
    assert presence.status == "wrap_up_quick"
    assert presence.active_handoff_id == "handoff-1"
