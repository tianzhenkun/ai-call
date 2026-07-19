from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call import AiCallRouter
from app.api.v1.ai_call.agent_console_controller import get_agent_console_service
from app.api.v1.ai_call.agent_console_schema import AgentProfileCreateIn, AgentSceneScopesIn
from app.api.v1.ai_call.model import AiCallAgentProfileModel, AiCallAgentSceneScopeModel
from app.api.v1.system.auth.schema import AuthSchema
from app.api.v1.system.user.model import UserModel
from app.core.base_model import MappedBase
from app.core.dependencies import get_current_user
from app.core.exceptions import CustomException, handle_exception
from app.services.ai_call.agent_console_service import AiCallAgentConsoleService


def _auth(db, *, permissions: set[str], user_id: int = 20, tenant_id: str = "tenant-a"):
    user = UserModel(
        user_id=user_id,
        tenant_id=tenant_id,
        user_name=f"user-{user_id}",
        nick_name=f"坐席{user_id}",
        user_type="sys_user",
    )
    return AuthSchema(db=db, user=user, permissions=permissions, check_data_scope=False)


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
    return TestClient(app)


@pytest.mark.anyio
async def test_agent_console_requires_login_and_permission(db_session) -> None:
    service = AiCallAgentConsoleService(db_session)

    async def unauthenticated():
        raise CustomException(msg="认证已失效", code=10401, status_code=401)

    with _client(unauthenticated, service) as client:
        assert client.get("/ai-call/agent-console/bootstrap").status_code == 401

    async def without_permission():
        return _auth(db_session, permissions=set())

    with _client(without_permission, service) as client:
        response = client.get("/ai-call/agent-console/bootstrap")
        assert response.status_code == 403


@pytest.mark.anyio
async def test_admin_endpoints_require_manage_permission(db_session) -> None:
    service = AiCallAgentConsoleService(db_session)

    async def console_only():
        return _auth(db_session, permissions={"ai_call:agent:console"})

    with _client(console_only, service) as client:
        assert client.get("/ai-call/admin/agents").status_code == 403


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

    auth = _auth(db_session, permissions={"ai_call:agent:console"})
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
    auth = _auth(db_session, permissions={"ai_call:agent:manage"}, user_id=1)

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
