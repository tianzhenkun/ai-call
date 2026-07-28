from pathlib import Path

from sqlalchemy import String, UniqueConstraint

from app.api.v1.ai_call.outbound.rule_task_model import AiCallOutboundAttemptModel


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
        "create unique index if not exists uk_outbound_attempt_tenant_command "
        "on ai_call_outbound_attempt (tenant_id, command_idempotency_key)"
        in migration
    )
    assert (
        "create unique index if not exists uk_outbound_attempt_tenant_active_slot "
        "on ai_call_outbound_attempt (tenant_id, active_slot)"
        in migration
    )
    assert "foreign key" not in migration
    assert "jsonb" not in migration
