from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call import AiCallRouter
from app.api.v1.ai_call.agent_console_schema import (
    AfterCallWorkIn,
    AgentPresenceSessionIn,
    FollowUpAttemptIn,
    FollowUpCallIn,
    FollowUpCloseIn,
)
from app.api.v1.ai_call.model import (
    AiCallAfterCallWorkModel,
    AiCallAgentProfileModel,
    AiCallAgentSceneScopeModel,
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
from app.services.ai_call.agent_console_service import AiCallAgentConsoleService
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.follow_up_service import AiCallFollowUpService
from app.services.ai_call.livekit_sip import (
    HumanCallbackSessionResult,
    HumanOnlySipSessionFactory,
)


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
    database_path = tmp_path / "agent-console-follow-up.db"
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
    scene_code: str = "intro_contract",
    status: str = "available",
    active_handoff_id: str | None = None,
    active_call_id: str | None = None,
) -> str:
    now = datetime.now(timezone.utc)
    console_session_id = str(uuid4())
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
        db.add(
            AiCallAgentSceneScopeModel(
                id=user_id * 100,
                tenant_id="tenant-a",
                agent_identity=agent_identity,
                scene_code=scene_code,
                created_by=1,
                created_at=now,
            )
        )
        db.add(
            AiCallHandoffAgentModel(
                id=user_id,
                tenant_id="tenant-a",
                agent_identity=agent_identity,
                skill_group="default",
                status=status,
                active_handoff_id=active_handoff_id,
                active_call_id=active_call_id,
                console_session_id=console_session_id,
                last_seen_at=now,
                status_updated_at=now,
            )
        )
    return console_session_id


async def _seed_completed_handoff(session_factory) -> str:
    now = datetime.now(timezone.utc)
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        status="wrap_up_quick",
        active_handoff_id="handoff-1",
        active_call_id="call-1",
    )
    async with session_factory() as db, db.begin():
        db.add(
            AiCallRecordModel(
                id=1,
                call_id="call-1",
                business_type="lead",
                business_id="lead-1",
                scene_code="intro_contract",
                entry_type="sip",
                room_name="room-1",
                participant_identity="customer-1",
                callee_phone_number_hash="hash-1",
                callee_phone_number_masked="138****0000",
                status="completed",
                started_at=now,
                ended_at=now,
            )
        )
        db.add(
            AiCallHandoffModel(
                id=1,
                tenant_id="tenant-a",
                handoff_id="handoff-1",
                call_id="call-1",
                room_name="room-1",
                scene_code="intro_contract",
                status="completed",
                request_source="customer",
                request_reason="customer_requested_human",
                request_message="请转人工",
                human_agent_identity="agent-20",
                requested_at=now,
                accepted_at=now,
                connected_at=now,
                ended_at=now,
            )
        )
    return console_session_id


async def _seed_unanswered_follow_up(session_factory, *, task_id: int = 100) -> None:
    now = datetime.now(timezone.utc)
    async with session_factory() as db, db.begin():
        db.add(
            AiCallRecordModel(
                id=task_id + 10_000,
                tenant_id="tenant-a",
                call_id=f"call-unanswered-{task_id}",
                business_type="lead",
                business_id="lead-2",
                scene_code="intro_contract",
                entry_type="sip",
                room_name=f"room-unanswered-{task_id}",
                participant_identity=f"customer-unanswered-{task_id}",
                callee_phone_number="13800000000",
                callee_phone_number_hash="source-phone-hash",
                callee_phone_number_masked="138****0000",
                status="completed",
                started_at=now,
                ended_at=now,
            )
        )
        db.add(
            AiCallFollowUpTaskModel(
                id=task_id,
                tenant_id="tenant-a",
                source_type="handoff_unanswered",
                source_key=f"handoff:handoff-unanswered-{task_id}",
                source_call_id=f"call-unanswered-{task_id}",
                source_handoff_id=f"handoff-unanswered-{task_id}",
                scene_code="intro_contract",
                business_type="lead",
                business_id="lead-2",
                contact_ref=f"call:call-unanswered-{task_id}",
                masked_contact="139****0000",
                owner_agent_identity=None,
                status="pending",
                follow_up_reason="首次人工接通等待超时",
                customer_callback_at=None,
                summary=None,
                closed_reason=None,
                closed_remark=None,
                completed_at=None,
                closed_at=None,
                created_at=now,
                updated_at=now,
            )
        )


async def _seed_ai_post_call_follow_up(session_factory, *, task_id: int = 102) -> None:
    now = datetime.now(timezone.utc)
    async with session_factory() as db, db.begin():
        db.add(
            AiCallFollowUpTaskModel(
                id=task_id,
                tenant_id="tenant-a",
                source_type="ai_post_call",
                source_key=f"call:call-ai-post-{task_id}",
                source_call_id=f"call-ai-post-{task_id}",
                source_handoff_id=None,
                scene_code="intro_contract",
                business_type="lead",
                business_id="lead-ai",
                contact_ref=f"call:call-ai-post-{task_id}",
                masked_contact="137****0000",
                owner_agent_identity=None,
                status="pending",
                follow_up_reason="客户明确要求顾问回访",
                customer_callback_at=None,
                summary="客户希望顾问后续联系。",
                closed_reason=None,
                closed_remark=None,
                completed_at=None,
                closed_at=None,
                created_at=now,
                updated_at=now,
            )
        )


def _error_code(exc: CustomException) -> str | None:
    return exc.data.get("errorCode") if isinstance(exc.data, dict) else None


def test_task5_routes_are_registered() -> None:
    paths = {route.path for route in AiCallRouter.routes}
    assert {
        "/ai-call/agent-console/handoffs/{handoff_id}/complete",
        "/ai-call/agent-console/calls/{call_id}/after-call-work",
        "/ai-call/agent-console/follow-ups",
        "/ai-call/agent-console/follow-ups/{follow_up_id}",
        "/ai-call/agent-console/follow-ups/{follow_up_id}/attempts",
        "/ai-call/agent-console/follow-ups/{follow_up_id}/claim",
        "/ai-call/agent-console/follow-ups/{follow_up_id}/call",
        "/ai-call/agent-console/follow-ups/{follow_up_id}/call/{call_id}/end",
        "/ai-call/agent-console/follow-ups/{follow_up_id}/complete",
        "/ai-call/agent-console/follow-ups/{follow_up_id}/close",
    } <= paths


def test_quick_wrap_up_requires_only_disposition_and_follow_up_flag() -> None:
    payload = AfterCallWorkIn(disposition_code="resolved", needs_follow_up=False)
    assert payload.summary is None

    with pytest.raises(ValidationError):
        AfterCallWorkIn(needs_follow_up=False)
    with pytest.raises(ValidationError):
        AfterCallWorkIn(disposition_code="resolved")


@pytest.mark.anyio
async def test_owned_handoff_completion_enters_quick_wrap_up(session_factory) -> None:
    now = datetime.now(timezone.utc)
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        status="in_call",
        active_handoff_id="handoff-connected",
        active_call_id="call-connected",
    )
    async with session_factory() as db, db.begin():
        db.add(
            AiCallHandoffModel(
                id=2,
                tenant_id="tenant-a",
                handoff_id="handoff-connected",
                call_id="call-connected",
                room_name="room-connected",
                scene_code="intro_contract",
                status="connected",
                request_source="customer",
                human_agent_identity="agent-20",
                accepted_console_session_id=console_session_id,
                requested_at=now,
                accepted_at=now,
                connected_at=now,
            )
        )

    async with session_factory() as db:
        service = AiCallAgentConsoleService(db)
        handoff = await service.complete_handoff(
            _auth(db, user_id=20),
            handoff_id="handoff-connected",
            console_session_id=console_session_id,
        )
        assert handoff.status == "completed"
        assert handoff.end_reason == "agent_completed"
        presence = (
            await db.execute(
                select(AiCallHandoffAgentModel).where(
                    AiCallHandoffAgentModel.agent_identity == "agent-20"
                )
            )
        ).scalar_one()
        assert presence.status == "wrap_up_quick"
        assert presence.active_handoff_id == "handoff-connected"


@pytest.mark.anyio
async def test_repeated_owned_handoff_completion_is_idempotent(session_factory) -> None:
    now = datetime.now(timezone.utc)
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        status="in_call",
        active_handoff_id="handoff-connected",
        active_call_id="call-connected",
    )
    async with session_factory() as db, db.begin():
        db.add(
            AiCallHandoffModel(
                id=21,
                tenant_id="tenant-a",
                handoff_id="handoff-connected",
                call_id="call-connected",
                room_name="room-connected",
                scene_code="intro_contract",
                status="connected",
                request_source="customer",
                human_agent_identity="agent-20",
                accepted_console_session_id=console_session_id,
                requested_at=now,
                accepted_at=now,
                connected_at=now,
            )
        )

    async with session_factory() as db, db.begin():
        service = AiCallAgentConsoleService(db)
        auth = _auth(db, user_id=20)
        first = await service.complete_handoff(
            auth,
            handoff_id="handoff-connected",
            console_session_id=console_session_id,
        )
        second = await service.complete_handoff(
            auth,
            handoff_id="handoff-connected",
            console_session_id=console_session_id,
        )

        assert first.status == "completed"
        assert second.status == "completed"
        presence = (
            await db.execute(
                select(AiCallHandoffAgentModel).where(
                    AiCallHandoffAgentModel.agent_identity == "agent-20"
                )
            )
        ).scalar_one()
        assert presence.status == "wrap_up_quick"
        assert presence.active_handoff_id == "handoff-connected"
        assert presence.active_call_id == "call-connected"


@pytest.mark.anyio
async def test_connected_terminal_handoff_completion_recovers_wrap_up_state(
    session_factory,
) -> None:
    now = datetime.now(timezone.utc)
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        status="online",
        active_handoff_id=None,
        active_call_id="call-ended",
    )
    async with session_factory() as db, db.begin():
        db.add(
            AiCallHandoffModel(
                id=22,
                tenant_id="tenant-a",
                handoff_id="handoff-ended",
                call_id="call-ended",
                room_name="room-ended",
                scene_code="intro_contract",
                status="canceled",
                request_source="customer",
                human_agent_identity="agent-20",
                accepted_console_session_id=console_session_id,
                requested_at=now,
                accepted_at=now,
                connected_at=now,
                ended_at=now,
                end_reason="remote_hangup",
            )
        )

    async with session_factory() as db, db.begin():
        service = AiCallAgentConsoleService(db)
        handoff = await service.complete_handoff(
            _auth(db, user_id=20),
            handoff_id="handoff-ended",
            console_session_id=console_session_id,
        )

        assert handoff.status == "canceled"
        presence = (
            await db.execute(
                select(AiCallHandoffAgentModel).where(
                    AiCallHandoffAgentModel.agent_identity == "agent-20"
                )
            )
        ).scalar_one()
        assert presence.status == "wrap_up_quick"
        assert presence.active_handoff_id == "handoff-ended"
        assert presence.active_call_id == "call-ended"


@pytest.mark.anyio
async def test_submit_acw_creates_one_owned_follow_up_and_releases_agent(session_factory) -> None:
    console_session_id = await _seed_completed_handoff(session_factory)
    async with session_factory() as db, db.begin():
        presence = (
            await db.execute(
                select(AiCallHandoffAgentModel).where(
                    AiCallHandoffAgentModel.agent_identity == "agent-20"
                )
            )
        ).scalar_one()
        presence.last_seen_at = datetime.now(timezone.utc) - timedelta(minutes=2)

    async with session_factory() as db:
        service = AiCallFollowUpService(db)
        auth = _auth(db, user_id=20)
        acw, follow_up = await service.submit_after_call_work(
            auth,
            call_id="call-1",
            payload=AfterCallWorkIn(
                disposition_code="follow_up_required",
                needs_follow_up=True,
            ),
        )
        await db.commit()

        assert acw.summary is None
        assert follow_up is not None
        assert follow_up.owner_agent_identity == "agent-20"
        assert follow_up.follow_up_reason == "人工通话后续跟进"
        assert follow_up.customer_callback_at is None

        presence = (
            await db.execute(
                select(AiCallHandoffAgentModel).where(
                    AiCallHandoffAgentModel.agent_identity == "agent-20"
                )
            )
        ).scalar_one()
        assert presence.status == "available"
        assert presence.active_handoff_id is None
        assert presence.active_call_id is None
        assert presence.last_seen_at == presence.status_updated_at

        _, available_presence = await service.agent_service.require_available_presence(
            auth,
            console_session_id=console_session_id,
        )
        assert available_presence.status == "available"


@pytest.mark.anyio
async def test_submit_acw_accepts_connected_handoff_ended_by_remote_hangup(
    session_factory,
) -> None:
    await _seed_completed_handoff(session_factory)
    async with session_factory() as db, db.begin():
        handoff = (
            await db.execute(
                select(AiCallHandoffModel).where(
                    AiCallHandoffModel.handoff_id == "handoff-1"
                )
            )
        ).scalar_one()
        handoff.status = "canceled"
        handoff.end_reason = "remote_hangup"

    async with session_factory() as db:
        acw, follow_up = await AiCallFollowUpService(db).submit_after_call_work(
            _auth(db, user_id=20),
            call_id="call-1",
            payload=AfterCallWorkIn(
                disposition_code="resolved",
                needs_follow_up=False,
            ),
        )
        await db.commit()

        assert acw.handoff_id == "handoff-1"
        assert follow_up is None
        presence = (
            await db.execute(
                select(AiCallHandoffAgentModel).where(
                    AiCallHandoffAgentModel.agent_identity == "agent-20"
                )
            )
        ).scalar_one()
        assert presence.status == "available"
        assert presence.active_handoff_id is None
        assert presence.active_call_id is None


@pytest.mark.anyio
async def test_submit_acw_rejects_canceled_handoff_that_never_connected(
    session_factory,
) -> None:
    await _seed_completed_handoff(session_factory)
    async with session_factory() as db, db.begin():
        handoff = (
            await db.execute(
                select(AiCallHandoffModel).where(
                    AiCallHandoffModel.handoff_id == "handoff-1"
                )
            )
        ).scalar_one()
        handoff.status = "canceled"
        handoff.connected_at = None
        handoff.end_reason = "customer_canceled"

    async with session_factory() as db:
        with pytest.raises(CustomException) as conflict:
            await AiCallFollowUpService(db).submit_after_call_work(
                _auth(db, user_id=20),
                call_id="call-1",
                payload=AfterCallWorkIn(
                    disposition_code="other",
                    needs_follow_up=False,
                ),
            )

    assert _error_code(conflict.value) == "HANDOFF_STATE_CONFLICT"


@pytest.mark.anyio
async def test_repeated_acw_submission_does_not_duplicate_work_or_follow_up(session_factory) -> None:
    await _seed_completed_handoff(session_factory)

    async with session_factory() as db:
        service = AiCallFollowUpService(db)
        auth = _auth(db, user_id=20)
        payload = AfterCallWorkIn(
            disposition_code="follow_up_required",
            needs_follow_up=True,
        )
        first = await service.submit_after_call_work(auth, call_id="call-1", payload=payload)
        second = await service.submit_after_call_work(auth, call_id="call-1", payload=payload)
        await db.commit()

        work_count = await db.scalar(select(func.count()).select_from(AiCallAfterCallWorkModel))
        task_count = await db.scalar(select(func.count()).select_from(AiCallFollowUpTaskModel))
        assert work_count == 1
        assert task_count == 1
        assert first[0].id == second[0].id
        assert first[1].id == second[1].id


@pytest.mark.anyio
async def test_unanswered_follow_up_claim_is_atomic_and_owner_is_fixed(session_factory) -> None:
    await _seed_agent(session_factory, user_id=20, agent_identity="agent-20")
    await _seed_agent(session_factory, user_id=21, agent_identity="agent-21")
    await _seed_unanswered_follow_up(session_factory)

    ready = asyncio.Event()
    started = 0
    lock = asyncio.Lock()

    async def claim(user_id: int):
        nonlocal started
        async with session_factory() as db:
            async with lock:
                started += 1
                if started == 2:
                    ready.set()
            await ready.wait()
            try:
                task = await AiCallFollowUpService(db).claim_follow_up(
                    _auth(db, user_id=user_id),
                    follow_up_id=100,
                )
                await db.commit()
                return task.owner_agent_identity
            except CustomException as exc:
                return _error_code(exc)

    results = await asyncio.gather(claim(20), claim(21))
    assert sorted(results) in [
        ["FOLLOW_UP_ALREADY_CLAIMED", "agent-20"],
        ["FOLLOW_UP_ALREADY_CLAIMED", "agent-21"],
    ]

    winner = next(value for value in results if value.startswith("agent-"))
    loser_id = 21 if winner == "agent-20" else 20
    async with session_factory() as db:
        with pytest.raises(CustomException) as conflict:
            await AiCallFollowUpService(db).claim_follow_up(
                _auth(db, user_id=loser_id),
                follow_up_id=100,
            )
        assert _error_code(conflict.value) == "FOLLOW_UP_ALREADY_CLAIMED"


@pytest.mark.anyio
async def test_claim_keeps_request_owned_transaction_valid(session_factory) -> None:
    await _seed_agent(session_factory, user_id=20, agent_identity="agent-20")
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        async with db.begin():
            task = await AiCallFollowUpService(db).claim_follow_up(
                _auth(db, user_id=20),
                follow_up_id=100,
            )
            assert task.owner_agent_identity == "agent-20"

    async with session_factory() as db:
        persisted = await db.get(AiCallFollowUpTaskModel, 100)
        assert persisted is not None
        assert persisted.owner_agent_identity == "agent-20"
        assert persisted.status == "processing"


@pytest.mark.anyio
async def test_ai_post_call_follow_up_can_be_claimed_by_scoped_agent(
    session_factory,
) -> None:
    await _seed_agent(session_factory, user_id=20, agent_identity="agent-20")
    await _seed_ai_post_call_follow_up(session_factory)

    async with session_factory() as db:
        service = AiCallFollowUpService(db)
        auth = _auth(db, user_id=20)

        rows = await service.list_follow_ups(auth)
        task = await service.claim_follow_up(auth, follow_up_id=102)

        assert [row.id for row in rows] == [102]
        assert task.owner_agent_identity == "agent-20"
        assert task.status == "processing"


@pytest.mark.anyio
async def test_owner_can_append_attempt_complete_or_close_with_rules(session_factory) -> None:
    await _seed_agent(session_factory, user_id=20, agent_identity="agent-20")
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        service = AiCallFollowUpService(db)
        auth = _auth(db, user_id=20)
        task = await service.claim_follow_up(auth, follow_up_id=100)
        assert task.owner_agent_identity == "agent-20"

        attempt = await service.append_attempt(
            auth,
            follow_up_id=100,
            payload=FollowUpAttemptIn(
                contact_channel="manual_phone",
                attempt_result="no_answer",
            ),
        )
        assert attempt.attempt_result == "no_answer"
        assert attempt.customer_callback_at is None
        assert task.status == "pending"

        with pytest.raises(CustomException, match="请先登记已联系结果"):
            await service.complete_follow_up(auth, follow_up_id=100)

        connected_attempt = await service.append_attempt(
            auth,
            follow_up_id=100,
            payload=FollowUpAttemptIn(
                contact_channel="wechat",
                attempt_result="connected",
                remark="客户已确认问题解决",
            ),
        )
        assert connected_attempt.attempt_result == "connected"

        completed = await service.complete_follow_up(auth, follow_up_id=100)
        assert completed.status == "completed"
        assert completed.owner_agent_identity == "agent-20"

        attempts = list(
            (
                await db.execute(
                    select(AiCallFollowUpAttemptModel).where(
                        AiCallFollowUpAttemptModel.follow_up_id == 100
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(attempts) == 2

    with pytest.raises(ValidationError):
        FollowUpCloseIn(closed_reason="other")
    assert FollowUpCloseIn(closed_reason="customer_refused").closed_remark is None
    with pytest.raises(ValidationError):
        FollowUpCloseIn(closed_reason="created_by_error")
    assert (
        FollowUpCloseIn(
            closed_reason="created_by_error",
            closed_remark="本地验收夹具重复创建",
        ).closed_reason
        == "created_by_error"
    )


@pytest.mark.anyio
async def test_agent_follow_up_payload_exposes_latest_contact_attempt(
    session_factory,
) -> None:
    await _seed_agent(session_factory, user_id=20, agent_identity="agent-20")
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        service = AiCallFollowUpService(db)
        auth = _auth(db, user_id=20)
        await service.claim_follow_up(auth, follow_up_id=100)
        await service.append_attempt(
            auth,
            follow_up_id=100,
            payload=FollowUpAttemptIn(
                contact_channel="manual_phone",
                attempt_result="no_answer",
            ),
        )
        await service.append_attempt(
            auth,
            follow_up_id=100,
            payload=FollowUpAttemptIn(
                contact_channel="wechat",
                attempt_result="connected",
            ),
        )

        rows = await service.list_follow_ups(auth)
        payload = service.follow_up_payload(rows[0])

        assert payload["latest_attempt"]["contact_channel"] == "wechat"
        assert payload["latest_attempt"]["attempt_result"] == "connected"


@pytest.mark.anyio
async def test_follow_up_list_respects_owner_and_scene_and_close_is_terminal(session_factory) -> None:
    await _seed_agent(session_factory, user_id=20, agent_identity="agent-20")
    await _seed_unanswered_follow_up(session_factory, task_id=100)
    await _seed_unanswered_follow_up(session_factory, task_id=101)

    async with session_factory() as db:
        service = AiCallFollowUpService(db)
        auth = _auth(db, user_id=20)
        rows = await service.list_follow_ups(auth)
        assert {task.id for task in rows} == {100, 101}

        await service.claim_follow_up(auth, follow_up_id=101)
        closed = await service.close_follow_up(
            auth,
            follow_up_id=101,
            payload=FollowUpCloseIn(closed_reason="customer_refused"),
        )
        assert closed.status == "closed"
        assert closed.closed_reason == "customer_refused"
        assert closed.closed_at is not None


@pytest.mark.anyio
async def test_ai_summary_draft_never_overwrites_human_summary(session_factory) -> None:
    await _seed_completed_handoff(session_factory)

    async with session_factory() as db:
        service = AiCallFollowUpService(db)
        auth = _auth(db, user_id=20)
        acw, follow_up = await service.submit_after_call_work(
            auth,
            call_id="call-1",
            payload=AfterCallWorkIn(
                disposition_code="follow_up_required",
                needs_follow_up=True,
                summary="人工确认摘要",
            ),
        )
        await service.apply_ai_summary_draft(
            tenant_id="tenant-a",
            handoff_id="handoff-1",
            summary="后到的 AI 摘要",
        )
        await db.commit()

        await db.refresh(acw)
        await db.refresh(follow_up)
        assert acw.summary == "人工确认摘要"
        assert follow_up.summary == "人工确认摘要"


class _FakeRoomManager:
    def __init__(self) -> None:
        self.created: list[str] = []
        self.deleted: list[str] = []

    async def create_room(self, room_name: str) -> None:
        self.created.append(room_name)

    async def delete_room(self, room_name: str) -> None:
        self.deleted.append(room_name)

    def issue_browser_token(self, room_name: str, participant_identity: str):
        return type(
            "Token",
            (),
            {
                "livekit_url": "wss://livekit.example.com",
                "participant_token": "agent-token",
                "participant_identity": participant_identity,
                "expires_in_seconds": 60,
            },
        )()


class _FakeSipClient:
    def __init__(self) -> None:
        self.created: list[dict] = []

    async def create_participant(self, **kwargs):
        self.created.append(kwargs)
        return type(
            "SipResult",
            (),
            {
                "participant_identity": kwargs["participant_identity"],
                "sip_call_id": "sip-call-1",
                "sip_call_status": "dialing",
            },
        )()


class _FakeCallbackFactory:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.ended_calls: list[str] = []

    async def create(self, **kwargs) -> HumanCallbackSessionResult:
        self.calls.append(kwargs)
        call_id = kwargs["call_id"]
        return HumanCallbackSessionResult(
            call_id=call_id,
            room_name=f"ai-call-{call_id}",
            customer_participant_identity=f"sip-{call_id}",
            agent_participant_identity=f"human-callback-{call_id}",
            livekit_url="wss://livekit.example.com",
            participant_token="agent-token",
            expires_in_seconds=60,
        )

    async def end(self, *, call_id: str) -> None:
        self.ended_calls.append(call_id)


class _FailingCallbackFactory:
    async def create(self, **_kwargs):
        raise AiCallError(
            error_id="sip_create_participant_failed",
            msg="SIP Participant 创建失败",
            status_code=502,
        )


@pytest.mark.parametrize(
    ("disconnect_reason", "expected"),
    [
        ("USER_UNAVAILABLE", "no_answer"),
        ("CONNECTION_TIMEOUT", "no_answer"),
        ("USER_REJECTED", "rejected"),
        ("SIP_TRUNK_FAILURE", "technical_failure"),
        ("MEDIA_FAILURE", "technical_failure"),
    ],
)
def test_callback_result_maps_livekit_disconnect_reason(
    disconnect_reason: str,
    expected: str,
) -> None:
    assert (
        AiCallFollowUpService._callback_attempt_result(
            {"disconnectReason": disconnect_reason}
        )
        == expected
    )


@pytest.mark.anyio
async def test_human_only_factory_creates_room_and_sip_without_agent_runner() -> None:
    room_manager = _FakeRoomManager()
    sip_client = _FakeSipClient()
    factory = HumanOnlySipSessionFactory(
        room_manager=room_manager,
        sip_client=sip_client,
    )

    result = await factory.create(
        call_id="call-callback",
        callee_phone_number="13800000000",
    )

    assert room_manager.created == ["ai-call-call-callback"]
    assert sip_client.created == [
        {
            "room_name": "ai-call-call-callback",
            "participant_identity": "sip-call-callback",
            "callee_phone_number": "13800000000",
            "wait_until_answered": False,
        }
    ]
    assert result.agent_participant_identity == "human-callback-call-callback"
    assert result.participant_token == "agent-token"


@pytest.mark.anyio
async def test_callback_requires_owner_and_available_presence_without_persisting_phone(
    session_factory,
) -> None:
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
    )
    await _seed_agent(session_factory, user_id=21, agent_identity="agent-21")
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        owner_service = AiCallFollowUpService(db, callback_factory=_FakeCallbackFactory())
        owner_auth = _auth(db, user_id=20)
        await owner_service.claim_follow_up(owner_auth, follow_up_id=100)

        with pytest.raises(CustomException):
            await owner_service.start_callback(
                _auth(db, user_id=21),
                follow_up_id=100,
                payload=FollowUpCallIn(
                    console_session_id=console_session_id,
                ),
            )

        result = await owner_service.start_callback(
            owner_auth,
            follow_up_id=100,
            payload=FollowUpCallIn(
                console_session_id=console_session_id,
            ),
        )
        assert result.status == "accepted"
        assert result.call_id.startswith("call_")
        assert "13800000000" not in repr(result)

        record = (
            await db.execute(
                select(AiCallRecordModel).where(AiCallRecordModel.call_id == result.call_id)
            )
        ).scalar_one()
        assert record.follow_up_id == 100
        assert record.callee_phone_number_hash != "13800000000"
        assert record.callee_phone_number_masked == "138****0000"
        assert "13800000000" not in repr(record.__dict__)
        assert owner_service.callback_factory.calls == [
            {"call_id": result.call_id, "callee_phone_number": "13800000000"}
        ]

        presence = (
            await db.execute(
                select(AiCallHandoffAgentModel).where(
                    AiCallHandoffAgentModel.agent_identity == "agent-20"
                )
            )
        ).scalar_one()
        assert presence.status == "claiming"
        assert presence.active_call_id == result.call_id

        with pytest.raises(CustomException) as busy:
            await owner_service.start_callback(
                owner_auth,
                follow_up_id=100,
                payload=FollowUpCallIn(
                    console_session_id=console_session_id,
                ),
            )
        assert _error_code(busy.value) == "AGENT_ALREADY_IN_CALL"


@pytest.mark.anyio
async def test_owned_follow_up_detail_lists_source_and_callback_records(
    session_factory,
) -> None:
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
    )
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        service = AiCallFollowUpService(db, callback_factory=_FakeCallbackFactory())
        auth = _auth(db, user_id=20)
        await service.claim_follow_up(auth, follow_up_id=100)
        callback = await service.start_callback(
            auth,
            follow_up_id=100,
            payload=FollowUpCallIn(console_session_id=console_session_id),
        )

        task = await service.get_follow_up(auth, follow_up_id=100)
        detail = service.follow_up_payload(task)

        assert detail["source_record"]["call_id"] == "call-unanswered-100"
        assert [record["call_id"] for record in detail["callback_records"]] == [
            callback.call_id
        ]
        assert detail["attempts"] == []


@pytest.mark.anyio
async def test_callback_rejects_follow_up_without_a_saved_source_phone(session_factory) -> None:
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
    )
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        source = (
            await db.execute(
                select(AiCallRecordModel).where(
                    AiCallRecordModel.call_id == "call-unanswered-100"
                )
            )
        ).scalar_one()
        source.callee_phone_number = None
        await db.commit()

        service = AiCallFollowUpService(db, callback_factory=_FakeCallbackFactory())
        auth = _auth(db, user_id=20)
        await service.claim_follow_up(auth, follow_up_id=100)
        with pytest.raises(CustomException) as failed:
            await service.start_callback(
                auth,
                follow_up_id=100,
                payload=FollowUpCallIn(console_session_id=console_session_id),
            )

        assert _error_code(failed.value) == "CALLBACK_NUMBER_UNAVAILABLE"
        assert service.callback_factory.calls == []


@pytest.mark.anyio
async def test_no_answer_callback_appends_once_returns_pending_and_releases_agent(
    session_factory,
) -> None:
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
    )
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        service = AiCallFollowUpService(db, callback_factory=_FakeCallbackFactory())
        auth = _auth(db, user_id=20)
        await service.claim_follow_up(auth, follow_up_id=100)
        callback = await service.start_callback(
            auth,
            follow_up_id=100,
            payload=FollowUpCallIn(
                console_session_id=console_session_id,
            ),
        )

        first = await service.record_callback_outcome(
            call_id=callback.call_id,
            attempt_result="no_answer",
            ring_duration_seconds=12,
        )
        second = await service.record_callback_outcome(
            call_id=callback.call_id,
            attempt_result="no_answer",
            ring_duration_seconds=12,
        )
        assert first.id == second.id
        assert first.related_call_id == callback.call_id

        task = (
            await db.execute(
                select(AiCallFollowUpTaskModel).where(AiCallFollowUpTaskModel.id == 100)
            )
        ).scalar_one()
        assert task.status == "pending"
        attempts = list((await db.execute(select(AiCallFollowUpAttemptModel))).scalars().all())
        assert len(attempts) == 1
        assert attempts[0].attempt_result == "no_answer"
        assert attempts[0].ring_duration_seconds == 12

        presence = (
            await db.execute(
                select(AiCallHandoffAgentModel).where(
                    AiCallHandoffAgentModel.agent_identity == "agent-20"
                )
            )
        ).scalar_one()
        assert presence.status == "available"
        assert presence.active_call_id is None


@pytest.mark.anyio
async def test_callback_livekit_webhook_maps_no_answer_to_follow_up_outcome(
    session_factory,
) -> None:
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
    )
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        service = AiCallFollowUpService(db, callback_factory=_FakeCallbackFactory())
        auth = _auth(db, user_id=20)
        await service.claim_follow_up(auth, follow_up_id=100)
        callback = await service.start_callback(
            auth,
            follow_up_id=100,
            payload=FollowUpCallIn(
                console_session_id=console_session_id,
            ),
        )

        result = await service.handle_livekit_webhook_event(
            event_type="participant_left",
            room_name=f"ai-call-{callback.call_id}",
            participant_identity=f"sip-{callback.call_id}",
            payload={
                "participant": {
                    "attributes": {"sip.callStatus": "no_answer"},
                }
            },
        )

        assert result == {
            "handled": True,
            "action": "record_callback_outcome",
            "callId": callback.call_id,
            "attemptResult": "no_answer",
        }
        attempt = (
            await db.execute(
                select(AiCallFollowUpAttemptModel).where(
                    AiCallFollowUpAttemptModel.related_call_id == callback.call_id
                )
            )
        ).scalar_one()
        assert attempt.attempt_result == "no_answer"


@pytest.mark.anyio
async def test_immediate_sip_failure_records_technical_failure_and_releases_agent(
    session_factory,
) -> None:
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
    )
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        service = AiCallFollowUpService(db, callback_factory=_FailingCallbackFactory())
        auth = _auth(db, user_id=20)
        await service.claim_follow_up(auth, follow_up_id=100)

        with pytest.raises(CustomException) as failed:
            await service.start_callback(
                auth,
                follow_up_id=100,
                payload=FollowUpCallIn(
                    console_session_id=console_session_id,
                ),
            )
        assert failed.value.data == {"errorCode": "sip_create_participant_failed"}

        record = (
            await db.execute(
                select(AiCallRecordModel).where(AiCallRecordModel.follow_up_id == 100)
            )
        ).scalar_one()
        assert record.status == "failed"
        assert record.failure_stage == "sip_callback"
        attempt = (
            await db.execute(
                select(AiCallFollowUpAttemptModel).where(
                    AiCallFollowUpAttemptModel.related_call_id == record.call_id
                )
            )
        ).scalar_one()
        assert attempt.attempt_result == "technical_failure"
        task = await db.get(AiCallFollowUpTaskModel, 100)
        assert task.status == "pending"
        presence = await db.get(AiCallHandoffAgentModel, 20)
        assert presence.status == "available"
        assert presence.active_call_id is None


@pytest.mark.anyio
async def test_concurrent_callbacks_allow_only_one_active_call(session_factory) -> None:
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
    )
    await _seed_unanswered_follow_up(session_factory)
    async with session_factory() as db:
        service = AiCallFollowUpService(db)
        await service.claim_follow_up(_auth(db, user_id=20), follow_up_id=100)
        await db.commit()

    callback_factory = _FakeCallbackFactory()

    async def start_callback():
        async with session_factory() as db:
            service = AiCallFollowUpService(db, callback_factory=callback_factory)
            try:
                return await service.start_callback(
                    _auth(db, user_id=20),
                    follow_up_id=100,
                    payload=FollowUpCallIn(
                        console_session_id=console_session_id,
                    ),
                )
            except CustomException as exc:
                return exc

    results = await asyncio.gather(start_callback(), start_callback())

    accepted = [item for item in results if not isinstance(item, CustomException)]
    conflicts = [item for item in results if isinstance(item, CustomException)]
    assert len(accepted) == 1
    assert len(conflicts) == 1
    assert _error_code(conflicts[0]) == "AGENT_ALREADY_IN_CALL"
    assert len(callback_factory.calls) == 1
    async with session_factory() as db:
        record_count = await db.scalar(
            select(func.count(AiCallRecordModel.id)).where(
                AiCallRecordModel.follow_up_id == 100
            )
        )
        assert record_count == 1


@pytest.mark.anyio
async def test_connected_callback_hangup_finishes_call_and_releases_agent(
    session_factory,
) -> None:
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
    )
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        service = AiCallFollowUpService(db, callback_factory=_FakeCallbackFactory())
        auth = _auth(db, user_id=20)
        await service.claim_follow_up(auth, follow_up_id=100)
        callback = await service.start_callback(
            auth,
            follow_up_id=100,
            payload=FollowUpCallIn(
                console_session_id=console_session_id,
            ),
        )

        await service.handle_livekit_webhook_event(
            event_type="participant_joined",
            room_name=f"ai-call-{callback.call_id}",
            participant_identity=f"sip-{callback.call_id}",
        )
        await db.commit()
        result = await service.handle_livekit_webhook_event(
            event_type="participant_left",
            room_name=f"ai-call-{callback.call_id}",
            participant_identity=f"sip-{callback.call_id}",
        )

        assert result == {
            "handled": True,
            "action": "complete_connected_callback",
            "callId": callback.call_id,
            "attemptResult": "connected",
        }
        record = (
            await db.execute(
                select(AiCallRecordModel).where(
                    AiCallRecordModel.call_id == callback.call_id
                )
            )
        ).scalar_one()
        assert record.status == "completed"
        assert record.ended_at is not None
        assert record.end_reason == "callback_completed"
        task = await db.get(AiCallFollowUpTaskModel, 100)
        assert task.status == "processing"
        presence = await db.get(AiCallHandoffAgentModel, 20)
        assert presence.status == "available"
        assert presence.active_call_id is None


@pytest.mark.anyio
async def test_agent_ends_connected_callback_and_releases_presence(session_factory) -> None:
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
    )
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        factory = _FakeCallbackFactory()
        service = AiCallFollowUpService(db, callback_factory=factory)
        auth = _auth(db, user_id=20)
        await service.claim_follow_up(auth, follow_up_id=100)
        callback = await service.start_callback(
            auth,
            follow_up_id=100,
            payload=FollowUpCallIn(console_session_id=console_session_id),
        )
        await service.record_callback_outcome(
            call_id=callback.call_id,
            attempt_result="connected",
        )

        record = await service.end_callback(
            auth,
            follow_up_id=100,
            call_id=callback.call_id,
            payload=AgentPresenceSessionIn(console_session_id=console_session_id),
        )

        assert factory.ended_calls == [callback.call_id]
        assert record.status == "completed"
        assert record.end_reason == "callback_ended_by_agent"
        task = await db.get(AiCallFollowUpTaskModel, 100)
        assert task.status == "processing"
        presence = await db.get(AiCallHandoffAgentModel, 20)
        assert presence.status == "available"
        assert presence.active_call_id is None
