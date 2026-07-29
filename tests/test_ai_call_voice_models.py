from __future__ import annotations

from pathlib import Path

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy import (
    inspect as sa_inspect,
)

from app.api.v1.ai_call.voice.model import (
    AiCallTenantVoiceProfileModel,
    AiCallVoiceDeletionModel,
    AiCallVoiceEnrollmentModel,
)

SQL_PATH = Path(
    "docs/livekit-ai-outbound/sql/phase-h6-voice-enrollment-postgres.sql"
)


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


def _assert_columns(
    model: type,
    expected: dict[str, tuple[type[object], bool, int | None]],
) -> None:
    assert model.__permission_strategy__ is None
    assert model.__table__.name in {
        "ai_call_tenant_voice_profile",
        "ai_call_voice_enrollment",
        "ai_call_voice_deletion",
    }
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


def test_tenant_voice_profile_model_matches_contract() -> None:
    _assert_columns(
        AiCallTenantVoiceProfileModel,
        {
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
    )
    assert _unique_constraints(AiCallTenantVoiceProfileModel) == {
        "uk_tenant_voice_model_voice": ("tenant_id", "target_model", "voice"),
    }
    assert _indexes(AiCallTenantVoiceProfileModel) == {
        "idx_tenant_voice_status_updated": ("tenant_id", "status", "updated_at"),
        "idx_tenant_voice_tenant_id": ("tenant_id", "id"),
    }
    assert AiCallTenantVoiceProfileModel.__table__.columns.id.autoincrement is False


def test_voice_enrollment_model_matches_contract() -> None:
    _assert_columns(
        AiCallVoiceEnrollmentModel,
        {
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
    )
    assert _unique_constraints(AiCallVoiceEnrollmentModel) == {
        "uk_voice_enrollment_tenant_key": ("tenant_id", "idempotency_key"),
    }
    assert _indexes(AiCallVoiceEnrollmentModel) == {
        "idx_voice_enrollment_claim": ("status", "next_retry_at", "id"),
        "idx_voice_enrollment_profile": ("tenant_id", "voice_profile_id", "created_at"),
    }
    assert AiCallVoiceEnrollmentModel.__table__.columns.attempt_count.default.arg == 0


def test_voice_deletion_model_matches_contract() -> None:
    _assert_columns(
        AiCallVoiceDeletionModel,
        {
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
    )
    assert _unique_constraints(AiCallVoiceDeletionModel) == {
        "uk_voice_deletion_tenant_key": ("tenant_id", "idempotency_key"),
    }
    assert _indexes(AiCallVoiceDeletionModel) == {
        "idx_voice_deletion_claim": ("status", "next_retry_at", "id"),
        "idx_voice_deletion_profile": ("tenant_id", "voice_profile_id", "created_at"),
    }
    assert AiCallVoiceDeletionModel.__table__.columns.attempt_count.default.arg == 0
    assert AiCallVoiceDeletionModel.__table__.columns.historical_task_count.default.arg == 0


def test_phase_h6_sql_matches_model_contract() -> None:
    sql = SQL_PATH.read_text(encoding="utf-8").lower()

    for table in (
        "ai_call_tenant_voice_profile",
        "ai_call_voice_enrollment",
        "ai_call_voice_deletion",
    ):
        assert f"create table if not exists {table}" in sql
        assert f"comment on table {table}" in sql

    for constraint in (
        "uk_tenant_voice_model_voice",
        "uk_voice_enrollment_tenant_key",
        "uk_voice_deletion_tenant_key",
    ):
        assert constraint in sql

    for index in (
        "idx_tenant_voice_status_updated",
        "idx_tenant_voice_tenant_id",
        "idx_voice_enrollment_claim",
        "idx_voice_enrollment_profile",
        "idx_voice_deletion_claim",
        "idx_voice_deletion_profile",
    ):
        assert f"create index if not exists {index}" in sql

    assert "jsonb" not in sql
    assert " references " not in sql
    assert "foreign key" not in sql
