from __future__ import annotations

import os
from pathlib import Path

import psycopg
import pytest
from psycopg import ClientCursor

pytestmark = pytest.mark.anyio

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATH = (
    PROJECT_ROOT
    / "docs/livekit-ai-outbound/sql/phase-i4-owner-runtime-dialogue-persistence.sql"
)


def _dsn() -> str:
    value = os.getenv("AI_CALL_TEST_POSTGRES_DSN", "").strip()
    if not value:
        pytest.fail("AI_CALL_TEST_POSTGRES_DSN 未配置，必须通过隔离 PostgreSQL 脚本运行")
    return value.replace("postgresql+asyncpg://", "postgresql://", 1)


def _execute(sql: str) -> None:
    with psycopg.connect(
        _dsn(),
        autocommit=True,
        cursor_factory=ClientCursor,
    ) as connection:
        connection.execute(sql)


def _reset_legacy_schema() -> None:
    _execute(
        """
        drop table if exists ai_call_dialogue_segment cascade;
        drop table if exists ai_call_record cascade;

        create table ai_call_record (
            id bigint primary key,
            tenant_id varchar(20),
            call_id varchar(64) not null unique,
            runtime_control_mode varchar(32) not null default 'legacy_local',
            ended_at timestamptz
        );

        create table ai_call_dialogue_segment (
            id bigint primary key,
            call_id varchar(64) not null,
            segment_no integer not null,
            speaker_type varchar(32) not null,
            source varchar(32) not null,
            source_segment_id varchar(128) not null,
            constraint uk_ai_call_dialogue_call_no
                unique (call_id, segment_no),
            constraint uk_ai_call_dialogue_source_segment
                unique (call_id, speaker_type, source, source_segment_id)
        );

        create index idx_ai_call_dialogue_speaker
            on ai_call_dialogue_segment (call_id, speaker_type, segment_no);
        """
    )


async def test_owner_dialogue_migration_backfills_and_is_idempotent() -> None:
    _reset_legacy_schema()
    _execute(
        """
        insert into ai_call_record (
            id, tenant_id, call_id, runtime_control_mode, ended_at
        ) values
            (1, 'tenant-a', 'legacy-call', 'legacy_local', null),
            (2, 'tenant-a', 'active-owner-call', 'owner_command_v1', null),
            (3, 'tenant-b', 'ended-owner-call', 'owner_command_v1', now());

        insert into ai_call_dialogue_segment (
            id, call_id, segment_no, speaker_type, source, source_segment_id
        ) values
            (11, 'legacy-call', 1, 'customer', 'qwen_realtime', 'legacy-1'),
            (12, 'ended-owner-call', 1, 'ai', 'qwen_realtime', 'owner-1');
        """
    )

    migration = MIGRATION_PATH.read_text(encoding="utf-8")
    _execute(migration)
    _execute(migration)

    with psycopg.connect(_dsn(), cursor_factory=ClientCursor) as connection:
        statuses = dict(
            connection.execute(
                """
                select call_id, dialogue_persistence_status
                from ai_call_record
                order by call_id
                """
            ).fetchall()
        )
        tenants = dict(
            connection.execute(
                """
                select call_id, tenant_id
                from ai_call_dialogue_segment
                order by call_id
                """
            ).fetchall()
        )
        constraints = {
            row[0]: row[1]
            for row in connection.execute(
                """
                select conname, pg_get_constraintdef(oid)
                from pg_constraint
                where conrelid = 'ai_call_dialogue_segment'::regclass
                  and contype = 'u'
                """
            ).fetchall()
        }

    assert statuses == {
        "active-owner-call": "pending",
        "ended-owner-call": "uncertain",
        "legacy-call": "not_started",
    }
    assert tenants == {
        "ended-owner-call": "tenant-b",
        "legacy-call": "tenant-a",
    }
    assert constraints["uk_ai_call_dialogue_call_no"] == (
        "UNIQUE (tenant_id, call_id, segment_no)"
    )
    assert constraints["uk_ai_call_dialogue_source_segment"] == (
        "UNIQUE (tenant_id, call_id, speaker_type, source, source_segment_id)"
    )


async def test_owner_dialogue_migration_rejects_unowned_tenant_backfill() -> None:
    _reset_legacy_schema()
    _execute(
        """
        insert into ai_call_dialogue_segment (
            id, call_id, segment_no, speaker_type, source, source_segment_id
        ) values (11, 'orphan-call', 1, 'customer', 'qwen_realtime', 'orphan-1');
        """
    )

    with pytest.raises(
        psycopg.Error,
        match="ai_call_dialogue_segment_tenant_backfill_failed",
    ):
        _execute(MIGRATION_PATH.read_text(encoding="utf-8"))
