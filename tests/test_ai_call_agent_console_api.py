from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call import AiCallRouter, agent_console_controller
from app.api.v1.ai_call.agent_console_controller import (
    get_agent_console_reconciler,
    get_agent_console_service,
)
from app.api.v1.ai_call.agent_console_schema import (
    AgentHandoffClaimIn,
    AgentMediaReadyIn,
    AgentPresenceSessionIn,
    AgentProfileCreateIn,
    AgentSceneScopesIn,
)
from app.api.v1.ai_call.model import AiCallAgentProfileModel, AiCallAgentSceneScopeModel
from app.api.v1.system.auth.schema import AuthSchema
from app.api.v1.system.user.model import UserModel
from app.core.base_model import MappedBase
from app.core.dependencies import get_current_user
from app.core.exceptions import CustomException, handle_exception
from app.services.ai_call.agent_console_reconciler import AiCallAgentConsoleReconciler
from app.services.ai_call.agent_console_service import AiCallAgentConsoleService


def _auth(db, *, user_id: int = 20, tenant_id: str = "tenant-a"):
    user = UserModel(
        user_id=user_id,
        tenant_id=tenant_id,
        user_name=f"user-{user_id}",
        nick_name=f"坐席{user_id}",
        user_type="sys_user",
    )
    return AuthSchema(db=db, user=user, check_data_scope=False)


@pytest.fixture
async def db_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session
    await engine.dispose()


def _client(auth_factory, service: AiCallAgentConsoleService) -> TestClient:
    app = FastAPI()
    handle_exception(app)
    app.include_router(AiCallRouter)
    app.dependency_overrides[get_current_user] = auth_factory
    app.dependency_overrides[get_agent_console_service] = lambda: service
    app.dependency_overrides[get_agent_console_reconciler] = lambda: (
        AiCallAgentConsoleReconciler(service.db, room_exists=lambda _room: False)
    )
    return TestClient(app)


def test_task7_management_and_sse_routes_are_registered() -> None:
    paths = {route.path for route in AiCallRouter.routes}
    assert {
        "/ai-call/agent-console/events",
        "/ai-call/agent-console/handoffs/{handoff_id}/context",
        "/ai-call/admin/agents/{agent_id}/status",
        "/ai-call/admin/agents/{agent_id}/release-stale",
        "/ai-call/admin/handoffs",
        "/ai-call/admin/handoffs/{handoff_id}",
        "/ai-call/admin/handoffs/{handoff_id}/reconcile",
        "/ai-call/admin/follow-ups",
        "/ai-call/admin/follow-ups/{follow_up_id}",
    } <= paths


@pytest.mark.anyio
async def test_handoff_context_controller_passes_console_session_to_service(
    db_session,
) -> None:
    console_session_id = uuid4()
    auth = _auth(db_session)
    console_service = SimpleNamespace(
        handoff_context_payload=AsyncMock(
            return_value={
                "handoff_id": "handoff-1",
                "dialogue": [{"speaker_type": "ai", "text": "您好"}],
            }
        )
    )

    response = await agent_console_controller.handoff_context_controller(
        "handoff-1",
        auth,
        console_service,
        console_session_id,
    )

    console_service.handoff_context_payload.assert_awaited_once_with(
        auth,
        handoff_id="handoff-1",
        console_session_id=str(console_session_id),
    )
    assert json.loads(response.body)["data"]["dialogue"][0]["text"] == "您好"


@pytest.mark.anyio
async def test_pending_handoffs_awaits_batched_rich_payload(db_session) -> None:
    handoff = SimpleNamespace(handoff_id="handoff-1")
    console_service = SimpleNamespace(
        list_pending_handoffs=AsyncMock(return_value=[handoff]),
        handoff_payloads=AsyncMock(
            return_value=[{"handoff_id": "handoff-1", "handoff_summary": "请转人工"}]
        ),
    )
    console_session_id = uuid4()

    await agent_console_controller.pending_handoffs_controller(
        _auth(db_session),
        console_service,
        console_session_id,
        50,
    )

    console_service.handoff_payloads.assert_awaited_once_with([handoff])


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("controller_name", "service_method", "payload_type"),
    [
        ("reconnect_token_controller", "begin_reconnect", AgentPresenceSessionIn),
    ],
)
async def test_token_handoff_endpoints_await_rich_payload(
    db_session,
    monkeypatch,
    controller_name,
    service_method,
    payload_type,
) -> None:
    handoff = SimpleNamespace(
        handoff_id="handoff-1",
        call_id="call-1",
        status="accepted",
    )
    rich_payload = {"handoff_id": "handoff-1", "handoff_summary": "请转人工"}
    console_service = SimpleNamespace(
        **{
            service_method: AsyncMock(return_value=handoff),
            "handoff_payload": AsyncMock(return_value=rich_payload),
        }
    )
    monkeypatch.setattr(
        agent_console_controller,
        "_issue_handoff_token",
        lambda _auth, _handoff: {"participant_token": "test-token"},
    )
    monkeypatch.setattr(agent_console_controller, "_publish", AsyncMock())

    await getattr(agent_console_controller, controller_name)(
        "handoff-1",
        payload_type(console_session_id=uuid4()),
        _auth(db_session),
        console_service,
    )

    console_service.handoff_payload.assert_awaited_once_with(handoff)


@pytest.mark.anyio
async def test_claim_handoff_uses_payload_built_before_transaction_commit(
    db_session,
    monkeypatch,
) -> None:
    handoff = SimpleNamespace(
        handoff_id="handoff-1",
        call_id="call-1",
        status="accepted",
    )
    rich_payload = {"handoff_id": "handoff-1", "handoff_summary": "请转人工"}
    console_service = SimpleNamespace(
        claim_handoff_with_payload=AsyncMock(
            return_value=SimpleNamespace(
                handoff=handoff,
                payload=rich_payload,
                command=SimpleNamespace(
                    handoff_id="handoff-1",
                    command_id=202,
                    command_seq=2,
                    command_status="PENDING",
                ),
            )
        ),
    )
    monkeypatch.setattr(
        agent_console_controller,
        "_issue_handoff_token",
        lambda _auth, _handoff: {"participant_token": "test-token"},
    )
    monkeypatch.setattr(agent_console_controller, "_publish", AsyncMock())
    auth = _auth(db_session)

    response = await agent_console_controller.claim_handoff_controller(
        "handoff-1",
        AgentHandoffClaimIn(console_session_id=uuid4()),
        auth,
        console_service,
        idempotency_key="handoff:handoff-1:claim:test",
    )

    console_service.claim_handoff_with_payload.assert_awaited_once_with(
        auth,
        handoff_id="handoff-1",
        console_session_id=console_service.claim_handoff_with_payload.call_args.kwargs[
            "console_session_id"
        ],
        idempotency_key="handoff:handoff-1:claim:test",
    )
    body = json.loads(response.body)
    assert response.status_code == 202
    assert body["data"]["handoff"] == rich_payload
    assert body["data"]["commandId"] == "202"
    assert body["data"]["commandSeq"] == "2"
    assert body["data"]["commandStatus"] == "PENDING"
    assert "seat_token" not in body["data"]


@pytest.mark.anyio
async def test_media_ready_stops_waiting_tone_and_starts_human_recording(
    db_session,
    monkeypatch,
) -> None:
    handoff = SimpleNamespace(
        handoff_id="handoff-1",
        call_id="call-1",
        room_name="room-1",
        status="connected",
    )
    console_service = SimpleNamespace(
        media_ready=AsyncMock(return_value=handoff),
        handoff_payload=AsyncMock(return_value={"handoff_id": "handoff-1"}),
    )
    manager = SimpleNamespace(
        cancel_timeout=Mock(),
        stop_waiting_tone=Mock(),
    )
    recording_service = SimpleNamespace(
        start_human_agent_recording=AsyncMock(),
    )
    orchestrator = SimpleNamespace(record_handoff_event=Mock())
    ai_call_service = SimpleNamespace(
        handoff_exception_manager=manager,
        recording_service=recording_service,
        orchestrator=orchestrator,
    )
    publish = AsyncMock()
    monkeypatch.setattr(
        agent_console_controller,
        "get_default_ai_call_service",
        lambda _db: ai_call_service,
    )
    monkeypatch.setattr(agent_console_controller, "_publish", publish)

    await agent_console_controller.media_ready_controller(
        "handoff-1",
        AgentMediaReadyIn(
            console_session_id=uuid4(),
            participant_identity="human-agent-handoff-1",
        ),
        _auth(db_session),
        console_service,
    )

    manager.cancel_timeout.assert_called_once_with(
        "handoff-1",
        call_id="call-1",
        handoff_status="connected",
        reason="media_ready",
    )
    manager.stop_waiting_tone.assert_called_once_with(
        "handoff-1",
        call_id="call-1",
        handoff_status="connected",
        reason="media_ready",
    )
    recording_service.start_human_agent_recording.assert_awaited_once_with(
        call_id="call-1",
        room_name="room-1",
        handoff_id="handoff-1",
        participant_identity="human-agent-handoff-1",
    )
    orchestrator.record_handoff_event.assert_called_once_with(
        call_id="call-1",
        event_type="handoff_connected",
        handoff_id="handoff-1",
        handoff_status="connected",
        payload={"participantIdentity": "human-agent-handoff-1"},
    )
    console_service.handoff_payload.assert_awaited_once_with(handoff)


@pytest.mark.anyio
async def test_complete_handoff_ends_running_customer_session(
    db_session,
    monkeypatch,
) -> None:
    handoff = SimpleNamespace(
        handoff_id="handoff-1",
        call_id="call-1",
        status="completed",
        end_reason="agent_completed",
    )
    console_service = SimpleNamespace(
        complete_handoff=AsyncMock(return_value=handoff),
        handoff_payload=AsyncMock(return_value={"handoff_id": "handoff-1"}),
    )
    ai_call_service = SimpleNamespace(
        end_running_session_after_handoff=AsyncMock(),
    )
    publish = AsyncMock()
    monkeypatch.setattr(
        agent_console_controller,
        "get_default_ai_call_service",
        lambda _db: ai_call_service,
    )
    monkeypatch.setattr(agent_console_controller, "_publish", publish)

    await agent_console_controller.complete_agent_handoff_controller(
        "handoff-1",
        AgentPresenceSessionIn(console_session_id=uuid4()),
        _auth(db_session),
        console_service,
    )

    ai_call_service.end_running_session_after_handoff.assert_awaited_once_with(
        "call-1",
        "agent_completed",
    )
    console_service.handoff_payload.assert_awaited_once_with(handoff)


@pytest.mark.anyio
async def test_agent_console_requires_login_and_enabled_profile(db_session) -> None:
    service = AiCallAgentConsoleService(db_session)

    async def unauthenticated():
        raise CustomException(msg="认证已失效", code=10401, status_code=401)

    with _client(unauthenticated, service) as client:
        assert client.get("/ai-call/agent-console/bootstrap").status_code == 401

    now = datetime.now(timezone.utc)
    db_session.add(
        AiCallAgentProfileModel(
            id=1,
            tenant_id="tenant-a",
            agent_identity="agent-20",
            user_id=20,
            enabled=True,
            created_by=1,
            created_at=now,
            updated_by=1,
            updated_at=now,
        )
    )
    await db_session.commit()

    async def authenticated():
        return _auth(db_session)

    with _client(authenticated, service) as client:
        response = client.get("/ai-call/agent-console/bootstrap")
        assert response.status_code == 200


@pytest.mark.anyio
async def test_event_stream_stops_before_waiting_after_browser_disconnect() -> None:
    request = SimpleNamespace(is_disconnected=AsyncMock(return_value=True))
    broker = SimpleNamespace(wait_for_events=AsyncMock())
    registry = SimpleNamespace(
        replace=Mock(return_value=object()),
        is_current=Mock(return_value=True),
        release=Mock(),
    )

    stream = agent_console_controller._agent_console_event_stream(
        request,
        tenant_id="tenant-a",
        agent_identity="agent-20",
        after_sequence=0,
        broker=broker,
        registry=registry,
    )
    with pytest.raises(StopAsyncIteration):
        await anext(stream)

    broker.wait_for_events.assert_not_awaited()
    registry.release.assert_called_once()


@pytest.mark.anyio
async def test_admin_endpoints_only_require_login(db_session) -> None:
    service = AiCallAgentConsoleService(db_session)

    async def unauthenticated():
        raise CustomException(msg="认证已失效", code=10401, status_code=401)

    with _client(unauthenticated, service) as client:
        assert client.get("/ai-call/admin/agents").status_code == 401

    async def authenticated():
        return _auth(db_session)

    with _client(authenticated, service) as client:
        assert client.get("/ai-call/admin/agents").status_code == 200
        assert client.get("/ai-call/agent-console/events").status_code == 403


@pytest.mark.anyio
async def test_disabled_or_scope_mismatched_agent_is_rejected(db_session) -> None:
    service = AiCallAgentConsoleService(db_session)
    profile = AiCallAgentProfileModel(
        id=1,
        tenant_id="tenant-a",
        agent_identity="agent-20",
        user_id=20,
        enabled=False,
        created_by=1,
        created_at=datetime.now(timezone.utc),
        updated_by=1,
        updated_at=datetime.now(timezone.utc),
    )
    db_session.add(profile)
    db_session.add(
        AiCallAgentSceneScopeModel(
            id=2,
            tenant_id="tenant-a",
            agent_identity="agent-20",
            scene_code="intro_contract",
            created_by=1,
            created_at=datetime.now(timezone.utc),
        )
    )
    await db_session.commit()

    auth = _auth(db_session)
    with pytest.raises(CustomException) as disabled:
        await service.require_current_agent(auth)
    assert disabled.value.status_code == 403

    profile.enabled = True
    await db_session.commit()
    with pytest.raises(CustomException) as mismatch:
        await service.require_scene_access(auth, "intro_overseas")
    assert mismatch.value.status_code == 403
    assert mismatch.value.data == {"errorCode": "AGENT_SCOPE_MISMATCH"}


@pytest.mark.anyio
async def test_admin_crud_replaces_scopes_and_requires_scope_before_enable(db_session) -> None:
    service = AiCallAgentConsoleService(db_session)
    auth = _auth(db_session, user_id=1)

    profile = await service.create_profile(
        auth,
        AgentProfileCreateIn(user_id=20, agent_identity="agent-20", enabled=False),
    )
    assert profile.user_id == 20

    with pytest.raises(CustomException):
        await service.update_profile(auth, profile.id, enabled=True)

    scopes = await service.replace_scene_scopes(
        auth,
        profile.id,
        AgentSceneScopesIn(scene_codes=["intro_contract", "intro_overseas"]),
    )
    assert scopes == ["intro_contract", "intro_overseas"]

    enabled = await service.update_profile(auth, profile.id, enabled=True)
    assert enabled.enabled is True

    replaced = await service.replace_scene_scopes(
        auth,
        profile.id,
        AgentSceneScopesIn(scene_codes=["intro_document"]),
    )
    assert replaced == ["intro_document"]
