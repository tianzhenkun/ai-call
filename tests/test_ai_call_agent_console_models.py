from __future__ import annotations

from sqlalchemy import inspect as sa_inspect

from app.api.v1.ai_call.model import (
    AiCallAfterCallWorkModel,
    AiCallAgentProfileModel,
    AiCallAgentSceneScopeModel,
    AiCallFollowUpAttemptModel,
    AiCallFollowUpTaskModel,
    AiCallHandoffAgentModel,
    AiCallHandoffModel,
)
from app.config.setting import Settings


def _unique_columns(model: type) -> set[tuple[str, ...]]:
    return {
        tuple(column.name for column in constraint.columns)
        for constraint in model.__table__.constraints
        if constraint.__class__.__name__ == "UniqueConstraint"
    }


def _index_columns(model: type) -> set[tuple[str, ...]]:
    return {
        tuple(column.name for column in index.columns)
        for index in model.__table__.indexes
    }


def test_agent_console_timeout_defaults_are_frozen() -> None:
    settings = Settings(_env_file=None)

    assert settings.AI_CALL_AGENT_CLAIM_CONNECT_TIMEOUT_SECONDS == 15
    assert settings.AI_CALL_AGENT_RECONNECT_GRACE_SECONDS == 15
    assert settings.AI_CALL_HANDOFF_TOTAL_WAIT_SECONDS == 60


def test_agent_console_models_match_tenant_scoped_contract() -> None:
    expected_columns = {
        AiCallAgentProfileModel: {
            "tenant_id",
            "agent_identity",
            "user_id",
            "enabled",
            "created_by",
            "created_at",
            "updated_by",
            "updated_at",
        },
        AiCallAgentSceneScopeModel: {
            "tenant_id",
            "agent_identity",
            "scene_code",
            "created_by",
            "created_at",
        },
        AiCallAfterCallWorkModel: {
            "work_id",
            "tenant_id",
            "call_id",
            "handoff_id",
            "agent_identity",
            "disposition_code",
            "summary",
            "needs_follow_up",
            "submitted_at",
            "created_at",
            "updated_at",
        },
        AiCallFollowUpTaskModel: {
            "tenant_id",
            "source_type",
            "source_key",
            "source_call_id",
            "source_handoff_id",
            "scene_code",
            "business_type",
            "business_id",
            "contact_ref",
            "masked_contact",
            "owner_agent_identity",
            "status",
            "follow_up_reason",
            "customer_callback_at",
            "summary",
            "closed_reason",
            "closed_remark",
            "completed_at",
            "closed_at",
            "created_at",
            "updated_at",
        },
        AiCallFollowUpAttemptModel: {
            "tenant_id",
            "follow_up_id",
            "agent_identity",
            "contact_channel",
            "attempt_result",
            "related_call_id",
            "ring_duration_seconds",
            "error_message",
            "remark",
            "contacted_at",
            "customer_callback_at",
            "created_at",
        },
    }

    for model, columns in expected_columns.items():
        assert columns <= set(model.__table__.columns.keys())
        assert not model.__table__.foreign_keys
        assert not sa_inspect(model).relationships


def test_handoff_models_include_v1_tenant_and_media_fields() -> None:
    assert {
        "tenant_id",
        "scene_code",
        "accepted_console_session_id",
        "claim_expires_at",
        "reconnect_expires_at",
    } <= set(AiCallHandoffModel.__table__.columns.keys())
    assert {"tenant_id", "active_call_id", "console_session_id"} <= set(
        AiCallHandoffAgentModel.__table__.columns.keys()
    )

    assert ("tenant_id", "handoff_id") in _unique_columns(AiCallHandoffModel)
    assert ("tenant_id", "agent_identity") in _unique_columns(AiCallHandoffAgentModel)
    assert ("tenant_id", "status", "requested_at") in _index_columns(AiCallHandoffModel)
    assert ("tenant_id", "status") in _index_columns(AiCallHandoffAgentModel)


def test_new_models_define_required_unique_constraints_and_indexes() -> None:
    assert {
        ("tenant_id", "agent_identity"),
        ("tenant_id", "user_id"),
    } <= _unique_columns(AiCallAgentProfileModel)
    assert ("tenant_id", "agent_identity", "scene_code") in _unique_columns(
        AiCallAgentSceneScopeModel
    )
    assert {
        ("tenant_id", "work_id"),
        ("tenant_id", "handoff_id"),
    } <= _unique_columns(AiCallAfterCallWorkModel)
    assert ("tenant_id", "source_handoff_id") in _unique_columns(AiCallFollowUpTaskModel)
    assert ("tenant_id", "source_type", "source_key") in _unique_columns(
        AiCallFollowUpTaskModel
    )
    assert {
        ("tenant_id", "follow_up_id", "contacted_at"),
        ("tenant_id", "related_call_id"),
    } <= _index_columns(AiCallFollowUpAttemptModel)
