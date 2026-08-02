from __future__ import annotations

from pathlib import Path

from sqlalchemy import CheckConstraint, UniqueConstraint

from app.api.v1.ai_call.model import (
    AiCallDialogueSegmentModel,
    AiCallRecordModel,
)


def test_owner_dialogue_model_exposes_integrity_contract() -> None:
    record_table = AiCallRecordModel.__table__
    assert record_table.c.dialogue_persistence_status.nullable is False
    assert record_table.c.dialogue_persistence_status.server_default.arg.text == (
        "'not_started'"
    )
    assert record_table.c.dialogue_persistence_error.type.length == 500
    assert record_table.c.dialogue_persistence_completed_at.type.timezone is True
    assert {
        constraint.name
        for constraint in record_table.constraints
        if isinstance(constraint, CheckConstraint)
    } >= {
        "ck_ai_call_record_dialogue_status",
        "ck_ai_call_record_owner_dialogue_started",
    }

    dialogue_table = AiCallDialogueSegmentModel.__table__
    assert dialogue_table.c.tenant_id.nullable is False
    unique_columns = {
        tuple(column.name for column in constraint.columns)
        for constraint in dialogue_table.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert ("tenant_id", "call_id", "segment_no") in unique_columns
    assert (
        "tenant_id",
        "call_id",
        "speaker_type",
        "source",
        "source_segment_id",
    ) in unique_columns


def test_owner_dialogue_postgres_migration_fails_closed_before_tenant_backfill() -> None:
    migration = (
        Path(__file__).parents[1]
        / "docs/livekit-ai-outbound/sql/phase-i4-owner-runtime-dialogue-persistence.sql"
    ).read_text(encoding="utf-8")

    assert "ai_call_dialogue_segment_tenant_backfill_failed" in migration
    assert "left join ai_call_record" in migration
    assert "record.tenant_id is null" in migration
    assert "alter column tenant_id set not null" in migration
    assert "unique (tenant_id, call_id, segment_no)" in migration
    assert (
        "unique (tenant_id, call_id, speaker_type, source, source_segment_id)"
        in migration
    )
    assert "runtime_control_mode = 'owner_command_v1'" in migration
    assert "dialogue_persistence_status = 'uncertain'" in migration
