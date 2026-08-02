from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
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

NOW = datetime(2026, 8, 3, 2, 0, tzinfo=timezone.utc)


async def _database_clock(_session) -> datetime:
    return NOW


def _record(*, owner_id: str = "runtime-a", fencing_token: int = 7):
    return AiCallRecordModel(
        id=1,
        tenant_id="tenant-a",
        call_id="call-a",
        entry_type="outbound",
        room_name="room-a",
        participant_identity="caller-a",
        status="active",
        started_at=NOW - timedelta(minutes=1),
        runtime_control_mode="owner_command_v1",
        runtime_owner_id=owner_id,
        runtime_fencing_token=fencing_token,
        runtime_lease_expires_at=NOW + timedelta(seconds=15),
        runtime_capacity_class="active",
        dialogue_persistence_status="pending",
    )


def _fence(*, owner_id: str = "runtime-a", fencing_token: int = 7):
    return OwnerDialogueFence(
        tenant_id="tenant-a",
        call_id="call-a",
        owner_id=owner_id,
        fencing_token=fencing_token,
    )


def _segment(*, segment_no: int = 1, source_segment_id: str = "item-a"):
    return OwnerDialogueSegment(
        segment_no=segment_no,
        speaker_type="customer",
        speaker_identity="customer-a",
        source="qwen_realtime",
        source_segment_id=source_segment_id,
        text="你好",
        segment_status="final",
        started_at=NOW - timedelta(seconds=1),
        ended_at=NOW,
        duration_ms=1_000,
    )


@pytest.mark.anyio
async def test_owner_dialogue_repository_is_fenced_and_replay_idempotent() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: MappedBase.metadata.create_all(
                sync_connection,
                tables=[
                    AiCallRecordModel.__table__,
                    AiCallDialogueSegmentModel.__table__,
                ],
            )
        )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory.begin() as session:
        session.add(_record())

    async with session_factory.begin() as session:
        repository = OwnerDialogueRepository(session, database_clock=_database_clock)
        assert await repository.next_segment_no(_fence()) == 1
        first = await repository.persist_batch(_fence(), [_segment()])
        replay = await repository.persist_batch(_fence(), [_segment(segment_no=99)])

    assert first.accepted is True
    assert first.persisted_count == 1
    assert replay.accepted is True
    assert replay.persisted_count == 1

    async with session_factory.begin() as session:
        record = await session.scalar(select(AiCallRecordModel).with_for_update())
        assert record is not None
        record.runtime_owner_id = "runtime-b"
        record.runtime_fencing_token = 8
        stale = await OwnerDialogueRepository(
            session,
            database_clock=_database_clock,
        ).persist_batch(_fence(), [_segment(segment_no=2, source_segment_id="item-b")])

    assert stale.accepted is False
    assert stale.persisted_count == 0
    async with session_factory() as session:
        rows = list((await session.scalars(select(AiCallDialogueSegmentModel))).all())
    assert len(rows) == 1
    assert rows[0].tenant_id == "tenant-a"
    assert rows[0].segment_no == 1
    assert rows[0].source_segment_id == "f7:item-a"
    await engine.dispose()


@pytest.mark.anyio
async def test_owner_dialogue_complete_requires_terminal_barrier() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: MappedBase.metadata.create_all(
                sync_connection,
                tables=[AiCallRecordModel.__table__],
            )
        )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory.begin() as session:
        session.add(_record())

    async with session_factory.begin() as session:
        repository = OwnerDialogueRepository(session, database_clock=_database_clock)
        assert await repository.finalize(_fence(), status="complete") is False
        record = await session.scalar(select(AiCallRecordModel).with_for_update())
        assert record is not None
        record.terminal_requested_at = NOW
        assert await repository.finalize(_fence(), status="complete") is True

    async with session_factory() as session:
        record = await session.scalar(select(AiCallRecordModel))
    assert record is not None
    assert record.dialogue_persistence_status == "complete"
    assert record.dialogue_persistence_completed_at is not None
    assert record.dialogue_persistence_completed_at.replace(tzinfo=timezone.utc) == NOW
    await engine.dispose()
