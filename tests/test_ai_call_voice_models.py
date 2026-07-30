from __future__ import annotations

import re
from pathlib import Path

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy import (
    inspect as sa_inspect,
)

from app.api.v1.ai_call.voice.model import (
    AiCallTenantVoiceProfileModel,
    AiCallVoiceDeletionModel,
    AiCallVoiceEnrollmentModel,
)

SQL_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs/livekit-ai-outbound/sql/phase-h6-voice-enrollment-postgres.sql"
)

MODEL_COLUMNS: dict[str, dict[str, tuple[type[object], bool, int | None]]] = {
    "ai_call_tenant_voice_profile": {
        "id": (BigInteger, False, None),
        "tenant_id": (String, False, 64),
        "display_name": (String, False, 100),
        "voice": (String, True, 128),
        "voice_type": (String, False, 32),
        "gender": (String, False, 16),
        "language": (String, False, 32),
        "target_model": (String, False, 64),
        "provider": (String, False, 32),
        "status": (String, False, 32),
        "latest_enrollment_id": (BigInteger, True, None),
        "provider_created_at": (DateTime, True, None),
        "error_message": (String, True, 1000),
        "created_by": (BigInteger, False, None),
        "deleted_by": (BigInteger, True, None),
        "deleted_at": (DateTime, True, None),
        "created_at": (DateTime, False, None),
        "updated_at": (DateTime, False, None),
    },
    "ai_call_voice_enrollment": {
        "id": (BigInteger, False, None),
        "tenant_id": (String, False, 64),
        "voice_profile_id": (BigInteger, False, None),
        "idempotency_key": (String, False, 128),
        "request_hash": (String, False, 64),
        "preferred_name": (String, False, 16),
        "language": (String, False, 32),
        "transcript": (String, True, 2000),
        "sample_object_key": (String, True, 500),
        "sample_sha256": (String, False, 64),
        "status": (String, False, 32),
        "provider_voice": (String, True, 128),
        "provider_request_id": (String, True, 128),
        "attempt_count": (Integer, False, None),
        "next_retry_at": (DateTime, True, None),
        "lease_owner": (String, True, 128),
        "lease_expires_at": (DateTime, True, None),
        "error_message": (String, True, 1000),
        "cleanup_error_message": (String, True, 1000),
        "consent_user_id": (BigInteger, False, None),
        "consent_at": (DateTime, False, None),
        "started_at": (DateTime, True, None),
        "finished_at": (DateTime, True, None),
        "created_at": (DateTime, False, None),
        "updated_at": (DateTime, False, None),
    },
    "ai_call_voice_deletion": {
        "id": (BigInteger, False, None),
        "tenant_id": (String, False, 64),
        "voice_profile_id": (BigInteger, False, None),
        "idempotency_key": (String, False, 128),
        "status": (String, False, 32),
        "provider_request_id": (String, True, 128),
        "attempt_count": (Integer, False, None),
        "next_retry_at": (DateTime, True, None),
        "lease_owner": (String, True, 128),
        "lease_expires_at": (DateTime, True, None),
        "historical_task_count": (Integer, False, None),
        "error_message": (String, True, 1000),
        "requested_by": (BigInteger, False, None),
        "started_at": (DateTime, True, None),
        "finished_at": (DateTime, True, None),
        "created_at": (DateTime, False, None),
        "updated_at": (DateTime, False, None),
    },
}

SQL_TABLE_CONTRACTS: dict[str, dict[str, tuple[str, bool, int | None]]] = {
    "ai_call_tenant_voice_profile": {
        "id": ("bigint", False, None),
        "tenant_id": ("varchar(64)", False, None),
        "display_name": ("varchar(100)", False, None),
        "voice": ("varchar(128)", True, None),
        "voice_type": ("varchar(32)", False, None),
        "gender": ("varchar(16)", False, None),
        "language": ("varchar(32)", False, None),
        "target_model": ("varchar(64)", False, None),
        "provider": ("varchar(32)", False, None),
        "status": ("varchar(32)", False, None),
        "latest_enrollment_id": ("bigint", True, None),
        "provider_created_at": ("timestamptz", True, None),
        "error_message": ("varchar(1000)", True, None),
        "created_by": ("bigint", False, None),
        "deleted_by": ("bigint", True, None),
        "deleted_at": ("timestamptz", True, None),
        "created_at": ("timestamptz", False, None),
        "updated_at": ("timestamptz", False, None),
    },
    "ai_call_voice_enrollment": {
        "id": ("bigint", False, None),
        "tenant_id": ("varchar(64)", False, None),
        "voice_profile_id": ("bigint", False, None),
        "idempotency_key": ("varchar(128)", False, None),
        "request_hash": ("varchar(64)", False, None),
        "preferred_name": ("varchar(16)", False, None),
        "language": ("varchar(32)", False, None),
        "transcript": ("varchar(2000)", True, None),
        "sample_object_key": ("varchar(500)", True, None),
        "sample_sha256": ("varchar(64)", False, None),
        "status": ("varchar(32)", False, None),
        "provider_voice": ("varchar(128)", True, None),
        "provider_request_id": ("varchar(128)", True, None),
        "attempt_count": ("integer", False, 0),
        "next_retry_at": ("timestamptz", True, None),
        "lease_owner": ("varchar(128)", True, None),
        "lease_expires_at": ("timestamptz", True, None),
        "error_message": ("varchar(1000)", True, None),
        "cleanup_error_message": ("varchar(1000)", True, None),
        "consent_user_id": ("bigint", False, None),
        "consent_at": ("timestamptz", False, None),
        "started_at": ("timestamptz", True, None),
        "finished_at": ("timestamptz", True, None),
        "created_at": ("timestamptz", False, None),
        "updated_at": ("timestamptz", False, None),
    },
    "ai_call_voice_deletion": {
        "id": ("bigint", False, None),
        "tenant_id": ("varchar(64)", False, None),
        "voice_profile_id": ("bigint", False, None),
        "idempotency_key": ("varchar(128)", False, None),
        "status": ("varchar(32)", False, None),
        "provider_request_id": ("varchar(128)", True, None),
        "attempt_count": ("integer", False, 0),
        "next_retry_at": ("timestamptz", True, None),
        "lease_owner": ("varchar(128)", True, None),
        "lease_expires_at": ("timestamptz", True, None),
        "historical_task_count": ("integer", False, 0),
        "error_message": ("varchar(1000)", True, None),
        "requested_by": ("bigint", False, None),
        "started_at": ("timestamptz", True, None),
        "finished_at": ("timestamptz", True, None),
        "created_at": ("timestamptz", False, None),
        "updated_at": ("timestamptz", False, None),
    },
}

UNIQUE_CONTRACTS = {
    "uk_tenant_voice_model_voice": (
        "ai_call_tenant_voice_profile",
        ("tenant_id", "target_model", "voice"),
    ),
    "uk_voice_enrollment_tenant_key": (
        "ai_call_voice_enrollment",
        ("tenant_id", "idempotency_key"),
    ),
    "uk_voice_deletion_tenant_key": (
        "ai_call_voice_deletion",
        ("tenant_id", "idempotency_key"),
    ),
}

INDEX_CONTRACTS = {
    "idx_tenant_voice_status_updated": (
        "ai_call_tenant_voice_profile",
        ("tenant_id", "status", "updated_at"),
    ),
    "idx_tenant_voice_tenant_id": (
        "ai_call_tenant_voice_profile",
        ("tenant_id", "id"),
    ),
    "idx_voice_enrollment_claim": (
        "ai_call_voice_enrollment",
        ("status", "next_retry_at", "id"),
    ),
    "idx_voice_enrollment_profile": (
        "ai_call_voice_enrollment",
        ("tenant_id", "voice_profile_id", "created_at"),
    ),
    "idx_voice_deletion_claim": (
        "ai_call_voice_deletion",
        ("status", "next_retry_at", "id"),
    ),
    "idx_voice_deletion_profile": (
        "ai_call_voice_deletion",
        ("tenant_id", "voice_profile_id", "created_at"),
    ),
}


def _unique_constraints(model: type) -> dict[str, tuple[str, ...]]:
    return {
        constraint.name: tuple(column.name for column in constraint.columns)
        for constraint in model.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    }


def _indexes(model: type) -> dict[str, tuple[str, ...]]:
    return {
        index.name: tuple(column.name for column in index.columns)
        for index in model.__table__.indexes
    }


def _assert_model_columns(model: type) -> None:
    expected = MODEL_COLUMNS[model.__table__.name]

    assert model.__permission_strategy__ is None
    assert set(model.__table__.columns.keys()) == set(expected)
    assert not model.__table__.foreign_keys
    assert not sa_inspect(model).relationships

    for name, (column_type, nullable, length) in expected.items():
        column = model.__table__.columns[name]
        assert isinstance(column.type, column_type)
        assert column.nullable is nullable
        assert not isinstance(column.type, JSON)
        if length is not None:
            assert column.type.length == length
        if isinstance(column.type, DateTime):
            assert column.type.timezone is True


def _assert_counter_defaults(model: type, *column_names: str) -> None:
    for column_name in column_names:
        column = model.__table__.columns[column_name]
        assert column.default is not None
        assert column.default.arg == 0
        assert column.server_default is not None
        assert str(column.server_default.arg) == "0"


def _parse_sql_tables(sql: str) -> dict[str, dict[str, object]]:
    tables: dict[str, dict[str, object]] = {}
    table_pattern = re.compile(
        r"create\s+table\s+if\s+not\s+exists\s+(?P<name>\w+)\s*"
        r"\((?P<body>.*?)\);",
        re.IGNORECASE | re.DOTALL,
    )
    column_pattern = re.compile(
        r"^(?P<name>\w+)\s+(?P<type>varchar\(\d+\)|bigint|integer|timestamptz)"
        r"(?P<rest>.*)$",
        re.IGNORECASE,
    )
    unique_pattern = re.compile(
        r"^constraint\s+(?P<name>\w+)\s+unique\s+\((?P<columns>[^)]+)\)$",
        re.IGNORECASE,
    )

    for match in table_pattern.finditer(sql):
        columns: dict[str, tuple[str, bool, int | None]] = {}
        primary_key: tuple[str, ...] = ()
        unique_constraints: dict[str, tuple[str, ...]] = {}
        for raw_definition in match.group("body").splitlines():
            definition = raw_definition.strip().rstrip(",")
            if not definition:
                continue
            unique_match = unique_pattern.match(definition)
            if unique_match:
                unique_constraints[unique_match.group("name").lower()] = tuple(
                    column.strip().lower()
                    for column in unique_match.group("columns").split(",")
                )
                continue
            column_match = column_pattern.match(definition)
            assert column_match, f"无法解析列定义：{definition}"
            name = column_match.group("name").lower()
            rest = column_match.group("rest").lower()
            is_primary_key = bool(re.search(r"\bprimary\s+key\b", rest))
            if is_primary_key:
                primary_key += (name,)
            nullable = not (is_primary_key or re.search(r"\bnot\s+null\b", rest))
            default_match = re.search(r"\bdefault\s+(-?\d+)\b", rest)
            default = int(default_match.group(1)) if default_match else None
            columns[name] = (column_match.group("type").lower(), nullable, default)
        tables[match.group("name").lower()] = {
            "columns": columns,
            "primary_key": primary_key,
            "unique_constraints": unique_constraints,
        }
    return tables


def _parse_sql_indexes(sql: str) -> dict[str, tuple[str, tuple[str, ...]]]:
    pattern = re.compile(
        r"create\s+index\s+if\s+not\s+exists\s+(?P<name>\w+)\s+"
        r"on\s+(?P<table>\w+)\s+\((?P<columns>[^)]+)\);",
        re.IGNORECASE | re.DOTALL,
    )
    return {
        match.group("name").lower(): (
            match.group("table").lower(),
            tuple(column.strip().lower() for column in match.group("columns").split(",")),
        )
        for match in pattern.finditer(sql)
    }


def test_tenant_voice_profile_model_matches_contract() -> None:
    _assert_model_columns(AiCallTenantVoiceProfileModel)
    assert AiCallTenantVoiceProfileModel.__table__.columns.id.autoincrement is False
    assert _unique_constraints(AiCallTenantVoiceProfileModel) == {
        "uk_tenant_voice_model_voice": ("tenant_id", "target_model", "voice"),
    }
    assert _indexes(AiCallTenantVoiceProfileModel) == {
        "idx_tenant_voice_status_updated": ("tenant_id", "status", "updated_at"),
        "idx_tenant_voice_tenant_id": ("tenant_id", "id"),
    }


def test_voice_enrollment_model_matches_contract() -> None:
    _assert_model_columns(AiCallVoiceEnrollmentModel)
    assert AiCallVoiceEnrollmentModel.__table__.columns.id.autoincrement is False
    _assert_counter_defaults(AiCallVoiceEnrollmentModel, "attempt_count")
    assert _unique_constraints(AiCallVoiceEnrollmentModel) == {
        "uk_voice_enrollment_tenant_key": ("tenant_id", "idempotency_key"),
    }
    assert _indexes(AiCallVoiceEnrollmentModel) == {
        "idx_voice_enrollment_claim": ("status", "next_retry_at", "id"),
        "idx_voice_enrollment_profile": ("tenant_id", "voice_profile_id", "created_at"),
    }


def test_voice_deletion_model_matches_contract() -> None:
    _assert_model_columns(AiCallVoiceDeletionModel)
    assert AiCallVoiceDeletionModel.__table__.columns.id.autoincrement is False
    _assert_counter_defaults(
        AiCallVoiceDeletionModel,
        "attempt_count",
        "historical_task_count",
    )
    assert _unique_constraints(AiCallVoiceDeletionModel) == {
        "uk_voice_deletion_tenant_key": ("tenant_id", "idempotency_key"),
    }
    assert _indexes(AiCallVoiceDeletionModel) == {
        "idx_voice_deletion_claim": ("status", "next_retry_at", "id"),
        "idx_voice_deletion_profile": ("tenant_id", "voice_profile_id", "created_at"),
    }


def test_counter_server_defaults_are_reflected_by_sqlite() -> None:
    engine = create_engine("sqlite://")
    try:
        for model, column_names in (
            (AiCallVoiceEnrollmentModel, ("attempt_count",)),
            (AiCallVoiceDeletionModel, ("attempt_count", "historical_task_count")),
        ):
            model.__table__.create(engine)
            reflected_columns = {
                column["name"]: column for column in sa_inspect(engine).get_columns(model.__tablename__)
            }
            for column_name in column_names:
                assert reflected_columns[column_name]["default"] == "0"
    finally:
        engine.dispose()


def test_phase_h6_sql_matches_explicit_contract() -> None:
    sql = SQL_PATH.read_text(encoding="utf-8")
    tables = _parse_sql_tables(sql)

    assert set(tables) == set(SQL_TABLE_CONTRACTS)
    for table_name, expected_columns in SQL_TABLE_CONTRACTS.items():
        table = tables[table_name]
        assert table["columns"] == expected_columns
        assert table["primary_key"] == ("id",)
        assert f"comment on table {table_name}" in sql.lower()

    expected_unique_constraints = {
        table_name: {
            name: columns
            for name, (constraint_table_name, columns) in UNIQUE_CONTRACTS.items()
            if constraint_table_name == table_name
        }
        for table_name in SQL_TABLE_CONTRACTS
    }
    parsed_unique_constraints = {
        table_name: table["unique_constraints"] for table_name, table in tables.items()
    }
    assert parsed_unique_constraints == expected_unique_constraints
    assert _parse_sql_indexes(sql) == INDEX_CONTRACTS

    assert not re.search(r"\bjsonb?\b", sql, re.IGNORECASE)
    assert not re.search(r"\breferences\b", sql, re.IGNORECASE)
    assert not re.search(r"\bforeign\s+key\b", sql, re.IGNORECASE)
