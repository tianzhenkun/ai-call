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
from app.services.ai_call.event_store import InMemoryEventStore
from app.services.ai_call.runtime_control.dialogue_bridge import (
    OwnerRuntimeDialogueBridge,
)
from app.services.ai_call.runtime_control.dialogue_repository import (
    OwnerDialogueFence,
    OwnerDialogueRepository,
)

NOW = datetime(2026, 8, 3, 3, 0, tzinfo=timezone.utc)


async def _database_clock(_session) -> datetime:
    return NOW


def _repository(session) -> OwnerDialogueRepository:
    return OwnerDialogueRepository(session, database_clock=_database_clock)


def _record(*, call_id: str = "call-a") -> AiCallRecordModel:
    return AiCallRecordModel(
        id=1,
        tenant_id="tenant-a",
        call_id=call_id,
        entry_type="outbound",
        room_name=f"room-{call_id}",
        participant_identity=f"caller-{call_id}",
        status="ending",
        started_at=NOW - timedelta(minutes=1),
        runtime_control_mode="owner_command_v1",
        runtime_owner_id="runtime-a",
        runtime_fencing_token=7,
        runtime_lease_expires_at=NOW + timedelta(days=1),
        runtime_capacity_class="active",
        terminal_requested_at=NOW,
        dialogue_persistence_status="pending",
    )


def _fence(call_id: str = "call-a") -> OwnerDialogueFence:
    return OwnerDialogueFence("tenant-a", call_id, "runtime-a", 7)


async def _database() -> tuple[object, async_sessionmaker]:
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
    return engine, async_sessionmaker(engine, expire_on_commit=False)


@pytest.mark.anyio
async def test_owner_dialogue_bridge_persists_completed_event_and_drains() -> None:
    engine, session_factory = await _database()
    async with session_factory.begin() as session:
        session.add(_record())

    bridge = OwnerRuntimeDialogueBridge(
        session_factory,
        flush_interval_seconds=0.01,
        repository_factory=_repository,
    )
    event_store = InMemoryEventStore()
    bridge.attach_event_store(event_store)
    await bridge.start()
    assert await bridge.bind_call(_fence()) is True

    event_store.append(
        call_id="call-a",
        type="user_transcript_done",
        source="provider",
        payload={"item_id": "item-a", "transcript": "你好"},
        timestamp=NOW,
    )
    result = await bridge.finalize_call(
        _fence(),
        ended_at=NOW + timedelta(seconds=1),
        timeout_seconds=1,
    )

    assert result.status == "complete"
    assert result.persisted_count == 1
    async with session_factory() as session:
        record = await session.scalar(select(AiCallRecordModel))
        rows = list((await session.scalars(select(AiCallDialogueSegmentModel))).all())
    assert record is not None
    assert record.dialogue_persistence_status == "complete"
    assert [(row.source_segment_id, row.segment_text) for row in rows] == [
        ("f7:item-a", "你好")
    ]
    await bridge.stop()
    await engine.dispose()


@pytest.mark.anyio
async def test_owner_dialogue_bridge_queue_full_finishes_uncertain_without_blocking() -> None:
    engine, session_factory = await _database()
    async with session_factory.begin() as session:
        session.add(_record())

    bridge = OwnerRuntimeDialogueBridge(
        session_factory,
        queue_max_size=1,
        flush_interval_seconds=0.01,
        repository_factory=_repository,
    )
    event_store = InMemoryEventStore()
    bridge.attach_event_store(event_store)
    assert await bridge.bind_call(_fence()) is True

    event_store.append(
        "call-a",
        "user_transcript_done",
        "provider",
        {"item_id": "item-a", "transcript": "第一句"},
        NOW,
    )
    event_store.append(
        "call-a",
        "user_transcript_done",
        "provider",
        {"item_id": "item-b", "transcript": "第二句"},
        NOW + timedelta(seconds=2),
    )

    await bridge.start()
    result = await bridge.finalize_call(
        _fence(),
        ended_at=NOW + timedelta(seconds=3),
        timeout_seconds=1,
    )

    assert result.status == "uncertain"
    assert result.dropped_count == 1
    async with session_factory() as session:
        record = await session.scalar(select(AiCallRecordModel))
    assert record is not None
    assert record.dialogue_persistence_status == "uncertain"
    assert record.dialogue_persistence_error == "queue_full"
    await bridge.stop()
    await engine.dispose()
