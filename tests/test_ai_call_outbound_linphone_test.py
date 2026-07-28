from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import String, UniqueConstraint
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.outbound.rule_task_model import AiCallOutboundAttemptModel
from app.core.base_model import MappedBase


@pytest.fixture
async def database(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'linphone.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


def _attempt(
    attempt_id: int,
    *,
    tenant_id: str = "tenant-a",
    command_idempotency_key: str | None = None,
    active_slot: str | None = None,
) -> AiCallOutboundAttemptModel:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    return AiCallOutboundAttemptModel(
        id=attempt_id,
        tenant_id=tenant_id,
        task_id=attempt_id + 1000,
        target_id=attempt_id + 2000,
        attempt_no=1,
        call_id=f"linphone-test-{attempt_id}",
        dialer_type="linphone_local",
        test_scenario="local_outbound",
        command_idempotency_key=command_idempotency_key,
        active_slot=active_slot,
        status="PENDING",
        started_at=now,
        created_at=now,
        updated_at=now,
    )


def test_outbound_attempt_has_nullable_linphone_test_metadata() -> None:
    columns = AiCallOutboundAttemptModel.__table__.columns
    expected_lengths = {
        "dialer_type": 32,
        "test_scenario": 32,
        "command_idempotency_key": 128,
        "active_slot": 32,
    }

    for column_name, expected_length in expected_lengths.items():
        column = columns[column_name]
        assert column.nullable
        assert isinstance(column.type, String)
        assert column.type.length == expected_length


def test_outbound_attempt_has_linphone_test_unique_guards() -> None:
    unique_constraints = {
        constraint.name: tuple(column.name for column in constraint.columns)
        for constraint in AiCallOutboundAttemptModel.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    }

    assert unique_constraints["uk_outbound_attempt_tenant_command"] == (
        "tenant_id",
        "command_idempotency_key",
    )
    assert unique_constraints["uk_outbound_attempt_tenant_active_slot"] == (
        "tenant_id",
        "active_slot",
    )


@pytest.mark.anyio
async def test_command_idempotency_key_uniqueness_is_tenant_scoped(database) -> None:
    async with database() as session:
        session.add(_attempt(1, command_idempotency_key="command-1"))
        await session.commit()

        session.add(_attempt(2, command_idempotency_key="command-1"))
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()

        session.add(
            _attempt(
                3,
                tenant_id="tenant-b",
                command_idempotency_key="command-1",
            )
        )
        await session.commit()

        session.add_all([_attempt(4), _attempt(5)])
        await session.commit()


@pytest.mark.anyio
async def test_active_slot_uniqueness_is_tenant_scoped(database) -> None:
    async with database() as session:
        session.add(_attempt(11, active_slot="linphone-local-active"))
        await session.commit()

        session.add(_attempt(12, active_slot="linphone-local-active"))
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()

        session.add(
            _attempt(
                13,
                tenant_id="tenant-b",
                active_slot="linphone-local-active",
            )
        )
        await session.commit()

        session.add_all([_attempt(14), _attempt(15)])
        await session.commit()


def test_linphone_test_postgres_migration_adds_columns_and_unique_indexes() -> None:
    migration_path = (
        Path(__file__).parents[1]
        / "docs"
        / "livekit-ai-outbound"
        / "sql"
        / "phase-h4-outbound-linphone-test-postgres.sql"
    )
    migration = " ".join(
        migration_path.read_text(encoding="utf-8").lower().split()
    )

    assert "add column if not exists dialer_type varchar(32)" in migration
    assert "add column if not exists test_scenario varchar(32)" in migration
    assert "add column if not exists command_idempotency_key varchar(128)" in migration
    assert "add column if not exists active_slot varchar(32)" in migration
    assert (
        "create unique index concurrently if not exists "
        "uk_outbound_attempt_tenant_command "
        "on ai_call_outbound_attempt (tenant_id, command_idempotency_key)"
        in migration
    )
    assert (
        "create unique index concurrently if not exists "
        "uk_outbound_attempt_tenant_active_slot "
        "on ai_call_outbound_attempt (tenant_id, active_slot)"
        in migration
    )
    assert "must run with autocommit enabled and outside any transaction block" in migration
    assert (
        "postgresql does not allow create index concurrently inside a transaction block"
        in migration
    )
    assert "foreign key" not in migration
    assert "jsonb" not in migration
