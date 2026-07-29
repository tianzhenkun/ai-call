from pathlib import Path

from app.api.v1.ai_call.outbound.model import AiCallOutboundValidationModel
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTaskModel,
)
from app.api.v1.ai_call.outbound.sip_line_model import AiCallSipLineModel


def test_sip_line_model_is_tenant_scoped_without_secrets_or_foreign_keys() -> None:
    columns = set(AiCallSipLineModel.__table__.columns.keys())
    assert {
        "tenant_id",
        "line_code",
        "line_name",
        "enabled",
        "default_marker",
        "adapter_type",
        "route_mode",
        "trunk_id",
        "proxy_host",
        "proxy_port",
        "auth_mode",
        "caller_number",
        "destination_country",
        "max_concurrency",
        "originate_timeout_seconds",
        "health_status",
        "health_message",
        "last_checked_at",
        "deleted",
        "deleted_at",
        "created_by",
        "updated_by",
        "created_at",
        "updated_at",
    } <= columns
    assert "password" not in columns
    assert "secret" not in columns
    assert not AiCallSipLineModel.__table__.foreign_keys

    unique_names = {
        constraint.name
        for constraint in AiCallSipLineModel.__table__.constraints
        if constraint.name
    }
    assert "uk_ai_call_sip_line_tenant_code" in unique_names
    assert "uk_ai_call_sip_line_tenant_default" in unique_names


def test_outbound_models_store_line_and_provider_diagnostics() -> None:
    assert {"line_id", "line_snapshot_json"} <= set(
        AiCallOutboundValidationModel.__table__.columns.keys()
    )
    assert {"line_id", "line_name"} <= set(
        AiCallOutboundTaskModel.__table__.columns.keys()
    )
    assert {
        "line_id",
        "line_code",
        "provider_status_code",
        "provider_reason",
        "hangup_cause",
    } <= set(AiCallOutboundAttemptModel.__table__.columns.keys())

    assert not AiCallOutboundValidationModel.__table__.foreign_keys
    assert not AiCallOutboundTaskModel.__table__.foreign_keys
    assert not AiCallOutboundAttemptModel.__table__.foreign_keys


def test_sip_line_migration_adds_line_and_attempt_diagnostics() -> None:
    migration_path = (
        Path(__file__).parents[1]
        / "docs"
        / "livekit-ai-outbound"
        / "sql"
        / "phase-h5-outbound-sip-line-postgres.sql"
    )
    migration = migration_path.read_text(encoding="utf-8").lower()
    assert "create table if not exists ai_call_sip_line" in migration
    assert "add column if not exists line_id" in migration
    assert "add column if not exists provider_status_code" in migration
    assert "uk_ai_call_sip_line_tenant_default" in migration
    assert "jsonb" not in migration
