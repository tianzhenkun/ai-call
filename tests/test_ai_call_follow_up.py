from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call import AiCallRouter
from app.api.v1.ai_call.agent_console_schema import (
    AfterCallWorkIn,
    FollowUpAttemptIn,
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
from app.services.ai_call.follow_up_service import AiCallFollowUpService


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


async def _seed_completed_handoff(session_factory) -> None:
    now = datetime.now(timezone.utc)
    await _seed_agent(
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


async def _seed_unanswered_follow_up(session_factory, *, task_id: int = 100) -> None:
    now = datetime.now(timezone.utc)
    async with session_factory() as db, db.begin():
        db.add(
            AiCallFollowUpTaskModel(
                id=task_id,
                tenant_id="tenant-a",
                source_type="handoff_unanswered",
                source_call_id="call-unanswered",
                source_handoff_id=f"handoff-unanswered-{task_id}",
                scene_code="intro_contract",
                business_type="lead",
                business_id="lead-2",
                contact_ref="call:call-unanswered",
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
async def test_submit_acw_creates_one_owned_follow_up_and_releases_agent(session_factory) -> None:
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
            ),
        )
        await db.commit()

        assert acw.summary is None
        assert follow_up is not None
        assert follow_up.owner_agent_identity == "agent-20"
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
        assert len(attempts) == 1

    with pytest.raises(ValidationError):
        FollowUpCloseIn(closed_reason="other")
    assert FollowUpCloseIn(closed_reason="customer_refused").closed_remark is None


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
