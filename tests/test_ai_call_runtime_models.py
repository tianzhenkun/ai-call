from __future__ import annotations

import importlib
from pathlib import Path
from types import ModuleType

import pytest
from sqlalchemy import Text, UniqueConstraint, inspect

from app.api.v1.ai_call.model import AiCallRecordModel
from app.api.v1.ai_call.outbound.rule_task_model import AiCallOutboundAttemptModel


def _load_runtime_models() -> ModuleType:
    try:
        return importlib.import_module("app.services.ai_call.runtime_control.models")
    except ModuleNotFoundError:
        pytest.fail("runtime_control.models 尚未实现")


def _column_names(model: type) -> set[str]:
    return {column.key for column in inspect(model).columns}


def _unique_column_sets(model: type) -> set[tuple[str, ...]]:
    return {
        tuple(constraint.columns.keys())
        for constraint in model.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    }


def test_record_has_owner_terminal_and_cleanup_contract_fields() -> None:
    required = {
        "tenant_id",
        "runtime_control_mode",
        "runtime_owner_id",
        "runtime_fencing_token",
        "runtime_lease_expires_at",
        "runtime_heartbeat_at",
        "runtime_capacity_class",
        "startup_reconcile_deadline_at",
        "startup_reconcile_policy_version",
        "startup_reconcile_budget_json",
        "agent_participant_identity",
        "agent_participant_sid",
        "agent_audio_track_sid",
        "agent_resource_generation",
        "agent_media_ready_at",
        "next_command_seq",
        "last_applied_command_seq",
        "terminal_requested_at",
        "resource_cleanup_status",
        "resource_cleanup_error",
        "resource_cleanup_next_retry_at",
        "resource_cleanup_completed_at",
    }
    assert required <= _column_names(AiCallRecordModel)

    columns = inspect(AiCallRecordModel).columns
    assert columns.tenant_id.nullable is True
    assert columns.runtime_control_mode.nullable is False
    assert "legacy_local" in str(columns.runtime_control_mode.server_default.arg)
    assert isinstance(columns.startup_reconcile_budget_json.type, Text)
    assert (
        "runtime_owner_id",
        "runtime_lease_expires_at",
    ) in {tuple(index.columns.keys()) for index in AiCallRecordModel.__table__.indexes}


def test_record_has_direct_sip_plaintext_column_without_plaintext_index() -> None:
    columns = inspect(AiCallRecordModel).columns

    assert columns.callee_phone_number.nullable is True
    assert columns.callee_phone_number.type.length == 32
    assert all(
        "callee_phone_number" not in tuple(index.columns.keys())
        for index in AiCallRecordModel.__table__.indexes
    )


def test_outbound_attempt_has_independent_projection_lease_fields() -> None:
    assert {
        "reconcile_owner_id",
        "reconcile_token",
        "reconcile_expires_at",
        "reconcile_after",
        "reconcile_attempt_count",
    } <= _column_names(AiCallOutboundAttemptModel)
    index_columns = {
        tuple(index.columns.keys()) for index in AiCallOutboundAttemptModel.__table__.indexes
    }
    assert ("status", "reconcile_after") in index_columns
    assert ("reconcile_expires_at",) in index_columns

    migration = (
        Path(__file__).parents[1]
        / "docs"
        / "livekit-ai-outbound"
        / "sql"
        / "phase-i1-owner-command-db-control-plane.sql"
    ).read_text(encoding="utf-8").lower()
    for fragment in (
        "reconcile_owner_id varchar(128)",
        "reconcile_token varchar(128)",
        "reconcile_expires_at timestamptz",
        "reconcile_after timestamptz",
        "reconcile_attempt_count integer not null default 0",
        "idx_outbound_attempt_reconcile",
        "idx_outbound_attempt_reconcile_lease",
    ):
        assert fragment in migration


def test_runtime_control_tables_and_required_columns_are_frozen() -> None:
    models = _load_runtime_models()
    expected = {
        models.AiCallRuntimeWorkerModel: {
            "worker_id",
            "status",
            "capacity",
            "active_call_count",
            "cleanup_capacity",
            "active_cleanup_count",
            "heartbeat_at",
            "lease_expires_at",
            "stream_cleanup_owner_id",
            "stream_cleanup_token",
            "stream_cleanup_expires_at",
            "stream_cleanup_after",
            "created_at",
            "updated_at",
        },
        models.AiCallRuntimeCommandModel: {
            "id",
            "tenant_id",
            "call_id",
            "command_seq",
            "command_type",
            "idempotency_key",
            "request_fingerprint",
            "dispatch_priority",
            "allocation_deadline_at",
            "payload_json",
            "sensitive_payload_ciphertext",
            "payload_key_version",
            "expected_fencing_token",
            "target_owner_id",
            "status",
            "dispatch_token",
            "dispatch_expires_at",
            "attempt_count",
            "next_retry_at",
            "published_at",
            "stream_message_id",
            "processing_owner_id",
            "processing_fencing_token",
            "processing_token",
            "processing_expires_at",
            "claimed_at",
            "cancel_requested_at",
            "preempted_by_command_id",
            "finished_at",
            "result_json",
            "error_message",
            "created_at",
            "updated_at",
        },
        models.AiCallEndEvidenceModel: {
            "id",
            "tenant_id",
            "call_id",
            "command_id",
            "source",
            "end_reason",
            "provider",
            "provider_namespace",
            "provider_event_id",
            "event_at",
            "received_at",
            "dedupe_key",
            "evidence_json",
        },
        models.AiCallRuntimeEffectModel: {
            "id",
            "tenant_id",
            "call_id",
            "command_id",
            "effect_type",
            "idempotency_key",
            "fencing_token",
            "status",
            "processing_token",
            "processing_expires_at",
            "provider_namespace",
            "provider_idempotency_key",
            "resource_key",
            "resource_generation",
            "source_create_effect_id",
            "create_protection_deadline_at",
            "absence_observation_count",
            "absence_confirmed_at",
            "terminal_confirmed_at",
            "provider_reference",
            "execution_phase",
            "processing_owner_id",
            "processing_fencing_token",
            "reconcile_after",
            "reconcile_deadline_at",
            "attempt_count",
            "error_message",
            "created_at",
            "updated_at",
        },
        models.AiCallRuntimeEffectDependencyModel: {
            "id",
            "tenant_id",
            "effect_id",
            "prerequisite_effect_id",
            "required_status",
            "created_at",
        },
        models.AiCallSipLineReservationModel: {
            "id",
            "tenant_id",
            "line_id",
            "call_id",
            "attempt_id",
            "status",
            "reservation_token",
            "fencing_token",
            "acquired_at",
            "reconcile_after",
            "released_at",
            "error_message",
            "created_at",
            "updated_at",
        },
    }

    for model, required_columns in expected.items():
        assert required_columns == _column_names(model)


def test_runtime_control_unique_constraints_match_the_contract() -> None:
    models = _load_runtime_models()

    assert {
        ("tenant_id", "idempotency_key"),
        ("tenant_id", "call_id", "command_seq"),
    } <= _unique_column_sets(models.AiCallRuntimeCommandModel)
    assert {
        ("tenant_id", "idempotency_key"),
        (
            "tenant_id",
            "provider_namespace",
            "effect_type",
            "resource_key",
        ),
        ("tenant_id", "provider_namespace", "provider_idempotency_key"),
    } <= _unique_column_sets(models.AiCallRuntimeEffectModel)
    assert ("tenant_id", "dedupe_key") in _unique_column_sets(
        models.AiCallEndEvidenceModel
    )
    assert (
        "tenant_id",
        "effect_id",
        "prerequisite_effect_id",
    ) in _unique_column_sets(models.AiCallRuntimeEffectDependencyModel)
    assert ("call_id",) in _unique_column_sets(
        models.AiCallSipLineReservationModel
    )


def test_runtime_control_json_snapshots_are_text_and_no_table_has_foreign_keys() -> None:
    models = _load_runtime_models()
    text_columns = (
        AiCallRecordModel.__table__.c.startup_reconcile_budget_json,
        models.AiCallRuntimeCommandModel.__table__.c.payload_json,
        models.AiCallRuntimeCommandModel.__table__.c.sensitive_payload_ciphertext,
        models.AiCallRuntimeCommandModel.__table__.c.result_json,
        models.AiCallEndEvidenceModel.__table__.c.evidence_json,
    )
    assert all(isinstance(column.type, Text) for column in text_columns)

    all_models = (
        AiCallRecordModel,
        AiCallOutboundAttemptModel,
        models.AiCallRuntimeWorkerModel,
        models.AiCallRuntimeCommandModel,
        models.AiCallEndEvidenceModel,
        models.AiCallRuntimeEffectModel,
        models.AiCallRuntimeEffectDependencyModel,
        models.AiCallSipLineReservationModel,
    )
    assert all(not model.__table__.foreign_keys for model in all_models)


def test_command_and_effect_status_enums_are_closed() -> None:
    runtime_types = importlib.import_module(
        "app.services.ai_call.runtime_control.types"
    )

    assert {status.value for status in runtime_types.CommandStatus} == {
        "PENDING",
        "DISPATCHING",
        "PUBLISHED",
        "PROCESSING",
        "RETRY_WAIT",
        "SUCCEEDED",
        "DEAD",
        "SUPERSEDED",
        "CANCELED",
    }
    assert {status.value for status in runtime_types.EffectStatus} == {
        "PENDING",
        "APPLYING",
        "APPLIED",
        "RECONCILE_REQUIRED",
        "FAILED",
    }
