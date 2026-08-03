from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import UniqueConstraint
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import AiCallRecordingModel
from app.core.base_model import MappedBase

MIGRATION_PATH = Path(
    "docs/livekit-ai-outbound/sql/phase-i5-owner-runtime-main-recording.sql"
)


def _unique_column_sets() -> set[tuple[str, ...]]:
    return {
        tuple(column.name for column in constraint.columns)
        for constraint in AiCallRecordingModel.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    }


def test_main_recording_contract_is_tenant_scoped() -> None:
    table = AiCallRecordingModel.__table__

    assert table.c.tenant_id.nullable is False
    assert table.c.egress_generation.nullable is True
    assert ("tenant_id", "call_id") in _unique_column_sets()
    assert ("call_id",) not in _unique_column_sets()


def test_main_recording_migration_fails_closed_before_tenant_backfill() -> None:
    migration = MIGRATION_PATH.read_text()

    assert "ai_call_recording_tenant_backfill_failed" in migration
    assert "alter column tenant_id set not null" in migration
    assert "unique (tenant_id, call_id)" in migration
    assert "idx_ai_call_recording_tenant_verify_due" in migration


@pytest.mark.anyio
async def test_main_recording_repository_never_crosses_tenants() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        started_at = datetime(2026, 8, 3, tzinfo=timezone.utc)
        db.add_all(
            [
                AiCallRecordingModel(
                    id=1,
                    tenant_id="tenant-a",
                    call_id="call-shared",
                    room_name="room-a",
                    status="recording",
                    object_name="a.mp3",
                    started_at=started_at,
                ),
                AiCallRecordingModel(
                    id=2,
                    tenant_id="tenant-b",
                    call_id="call-shared",
                    room_name="room-b",
                    status="recording",
                    object_name="b.mp3",
                    started_at=started_at,
                ),
            ]
        )
        await db.flush()

        tenant_a = await repository.get_recording(
            tenant_id="tenant-a",
            call_id="call-shared",
        )
        tenant_b = await repository.get_recording(
            tenant_id="tenant-b",
            call_id="call-shared",
        )
        missing = await repository.get_recording(
            tenant_id="tenant-c",
            call_id="call-shared",
        )
        await repository.update_recording(
            tenant_id="tenant-a",
            call_id="call-shared",
            status="completed",
        )

        assert tenant_a is not None and tenant_a.room_name == "room-a"
        assert tenant_a.status == "completed"
        assert tenant_b is not None and tenant_b.room_name == "room-b"
        assert tenant_b.status == "recording"
        assert missing is None

    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()
