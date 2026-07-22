from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call import AiCallRouter
from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import (
    AiCallAgentProfileModel,
    AiCallAgentSceneScopeModel,
    AiCallHandoffAgentModel,
    AiCallHandoffModel,
    AiCallRecordModel,
)
from app.api.v1.system.auth.schema import AuthSchema
from app.api.v1.system.user.model import UserModel
from app.core.base_model import MappedBase
from app.core.exceptions import CustomException
from app.services.ai_call.agent_console_service import AiCallAgentConsoleService
from app.services.ai_call.handoff_service import AiCallHandoffService


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
    } <= paths


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
