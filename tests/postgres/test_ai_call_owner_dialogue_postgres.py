from __future__ import annotations

import os
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
import pytest
from psycopg import ClientCursor
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.model import (
    AiCallDialogueSegmentModel,
    AiCallRecordModel,
)
from app.core.base_model import MappedBase
from app.services.ai_call.runtime_control.dialogue_repository import (
    OwnerDialogueFence,
    OwnerDialogueRepository,
    OwnerDialogueSegment,
)

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


async def test_owner_dialogue_postgres_fences_takeover_and_replay() -> None:
    engine = create_async_engine(
        os.environ["AI_CALL_TEST_POSTGRES_DSN"],
        isolation_level="READ COMMITTED",
    )
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: MappedBase.metadata.drop_all(
                sync_connection,
                tables=[
                    AiCallDialogueSegmentModel.__table__,
                    AiCallRecordModel.__table__,
                ],
            )
        )
        await connection.run_sync(
            lambda sync_connection: MappedBase.metadata.create_all(
                sync_connection,
                tables=[
                    AiCallRecordModel.__table__,
                    AiCallDialogueSegmentModel.__table__,
                ],
            )
        )

    now = datetime.now(timezone.utc)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    fence_a = OwnerDialogueFence("tenant-a", "call-a", "runtime-a", 7)
    fence_b = OwnerDialogueFence("tenant-a", "call-a", "runtime-b", 8)
    segment = OwnerDialogueSegment(
        segment_no=1,
        speaker_type="customer",
        speaker_identity="customer-a",
        source="qwen_realtime",
        source_segment_id="item-a",
        text="你好",
        segment_status="final",
        started_at=now - timedelta(seconds=1),
        ended_at=now,
        duration_ms=1_000,
    )

    async with session_factory.begin() as session:
        session.add(
            AiCallRecordModel(
                id=1,
                tenant_id="tenant-a",
                call_id="call-a",
                entry_type="outbound",
                room_name="room-a",
                participant_identity="caller-a",
                status="active",
                started_at=now,
                runtime_control_mode="owner_command_v1",
                runtime_owner_id="runtime-a",
                runtime_fencing_token=7,
                runtime_lease_expires_at=now + timedelta(minutes=1),
                runtime_capacity_class="active",
                dialogue_persistence_status="pending",
            )
        )

    async with session_factory.begin() as session:
        repository = OwnerDialogueRepository(session)
        assert (await repository.persist_batch(fence_a, [segment])).accepted is True

    async with session_factory.begin() as session:
        record = await session.scalar(select(AiCallRecordModel).with_for_update())
        assert record is not None
        record.runtime_owner_id = "runtime-b"
        record.runtime_fencing_token = 8
        record.runtime_lease_expires_at = now + timedelta(minutes=1)

    async with session_factory.begin() as session:
        stale = await OwnerDialogueRepository(session).persist_batch(
            fence_a,
            [replace(segment, segment_no=2, source_segment_id="item-b")],
        )
        assert stale.accepted is False

    async with session_factory.begin() as session:
        repository = OwnerDialogueRepository(session)
        assert await repository.next_segment_no(fence_b) == 2
        takeover = await repository.persist_batch(
            fence_b,
            [replace(segment, segment_no=2)],
        )
        assert takeover.accepted is True
        record = await session.scalar(select(AiCallRecordModel).with_for_update())
        assert record is not None
        record.terminal_requested_at = now
        assert await repository.finalize(fence_b, status="complete") is True

    async with session_factory() as session:
        rows = list(
            (
                await session.scalars(
                    select(AiCallDialogueSegmentModel).order_by(
                        AiCallDialogueSegmentModel.segment_no
                    )
                )
            ).all()
        )
    assert [row.source_segment_id for row in rows] == ["f7:item-a", "f8:item-a"]
    await engine.dispose()
