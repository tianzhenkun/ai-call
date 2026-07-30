from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.outbound.rule_task_controller import create_task_controller
from app.api.v1.ai_call.outbound.rule_task_schema import CreateTaskRequest
from app.api.v1.system.auth.schema import AuthSchema
from app.api.v1.system.user.model import UserModel
from app.core import dependencies
from app.services.ai_call.sqlite_serialization import begin_sqlite_immediate_write


@pytest.fixture
async def sqlite_sessions():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    statements: list[str] = []

    @event.listens_for(engine.sync_engine, "before_cursor_execute")
    def capture_statement(_conn, _cursor, statement, _parameters, _context, _executemany):
        statements.append(statement)

    try:
        yield async_sessionmaker(engine, expire_on_commit=False), statements
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_same_immediate_transaction_can_reenter(sqlite_sessions) -> None:
    factory, statements = sqlite_sessions
    async with factory() as session:
        assert await begin_sqlite_immediate_write(session) is True
        transaction = session.sync_session.get_transaction()

        assert await begin_sqlite_immediate_write(session) is True

        assert session.sync_session.get_transaction() is transaction
        assert [sql for sql in statements if sql == "BEGIN IMMEDIATE"] == ["BEGIN IMMEDIATE"]


@pytest.mark.anyio
@pytest.mark.parametrize("finish_method", ["commit", "rollback"])
async def test_finished_immediate_transaction_starts_a_new_one(
    sqlite_sessions,
    finish_method: str,
) -> None:
    factory, statements = sqlite_sessions
    async with factory() as session:
        assert await begin_sqlite_immediate_write(session) is True
        first_transaction = session.sync_session.get_transaction()
        await getattr(session, finish_method)()

        assert await begin_sqlite_immediate_write(session) is True

        assert session.sync_session.get_transaction() is not first_transaction
        assert [sql for sql in statements if sql == "BEGIN IMMEDIATE"] == [
            "BEGIN IMMEDIATE",
            "BEGIN IMMEDIATE",
        ]


@pytest.mark.anyio
async def test_external_transaction_is_not_treated_as_helper_owned(
    sqlite_sessions,
) -> None:
    factory, statements = sqlite_sessions
    async with factory() as session:
        await session.execute(text("SELECT 1"))

        with pytest.raises(RuntimeError, match="首条 SQL 前"):
            await begin_sqlite_immediate_write(session)

        assert "BEGIN IMMEDIATE" not in statements


@pytest.mark.anyio
async def test_create_task_controller_claims_db_getter_logical_transaction(
    sqlite_sessions,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory, statements = sqlite_sessions
    monkeypatch.setattr(dependencies, "async_db_session", factory)
    getter = dependencies.db_getter()
    session = await anext(getter)
    user = UserModel()
    user.user_id = 7
    user.tenant_id = "tenant-a"
    user.nick_name = "管理员"
    auth = AuthSchema(db=session, user=user)
    request = CreateTaskRequest(
        task_name="控制器事务测试",
        task_mode="single",
        prompt_profile_id="1",
        scene_code="intro_contract",
        voice="Tina",
        rule_id="2",
        execution_mode="immediate",
        phone_number="19900001120",
        validation_id="3",
    )

    class StubTaskService:
        async def create_task(self, db, *_args):
            assert db is session
            assert await begin_sqlite_immediate_write(db) is True
            return SimpleNamespace(id=123), True

    try:
        response = await create_task_controller(
            request,
            auth,
            "controller-idempotency-key",
            StubTaskService(),
        )
    finally:
        await getter.aclose()

    assert response.status_code == 200
    assert [sql for sql in statements if sql == "BEGIN IMMEDIATE"] == ["BEGIN IMMEDIATE"]
