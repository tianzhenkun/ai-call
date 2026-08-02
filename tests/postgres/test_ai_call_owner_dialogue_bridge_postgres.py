from __future__ import annotations

import asyncio
import math
import os
import time
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select
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
)

pytestmark = pytest.mark.anyio


async def test_owner_dialogue_completed_event_p95_is_below_one_second() -> None:
    dsn = os.getenv("AI_CALL_TEST_POSTGRES_DSN", "").strip()
    if not dsn:
        pytest.fail("AI_CALL_TEST_POSTGRES_DSN 未配置，必须通过隔离 PostgreSQL 脚本运行")
    engine = create_async_engine(dsn, isolation_level="READ COMMITTED")
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
    fence = OwnerDialogueFence("tenant-a", "call-latency", "runtime-a", 7)
    async with session_factory.begin() as session:
        session.add(
            AiCallRecordModel(
                id=1,
                tenant_id=fence.tenant_id,
                call_id=fence.call_id,
                entry_type="outbound",
                room_name="room-latency",
                participant_identity="caller-latency",
                status="active",
                started_at=now,
                runtime_control_mode="owner_command_v1",
                runtime_owner_id=fence.owner_id,
                runtime_fencing_token=fence.fencing_token,
                runtime_lease_expires_at=now + timedelta(minutes=5),
                runtime_capacity_class="active",
                dialogue_persistence_status="pending",
            )
        )

    bridge = OwnerRuntimeDialogueBridge(
        session_factory,
        flush_interval_seconds=0.01,
    )
    event_store = InMemoryEventStore()
    bridge.attach_event_store(event_store)
    await bridge.start()
    assert await bridge.bind_call(fence) is True

    latencies: list[float] = []
    for index in range(20):
        started = time.perf_counter()
        event_store.append(
            call_id=fence.call_id,
            type="user_transcript_done",
            source="provider",
            payload={
                "item_id": f"item-{index}",
                "transcript": f"第 {index + 1} 句",
            },
            timestamp=now + timedelta(seconds=index),
        )
        deadline = started + 1.0
        while True:
            async with session_factory() as session:
                count = await session.scalar(
                    select(func.count(AiCallDialogueSegmentModel.id)).where(
                        AiCallDialogueSegmentModel.tenant_id == fence.tenant_id,
                        AiCallDialogueSegmentModel.call_id == fence.call_id,
                    )
                )
            if count == index + 1:
                break
            if time.perf_counter() >= deadline:
                pytest.fail(f"第 {index + 1} 条完成句在 1 秒内未落库")
            await asyncio.sleep(0.01)
        latencies.append(time.perf_counter() - started)

    async with session_factory.begin() as session:
        record = await session.scalar(
            select(AiCallRecordModel).where(
                AiCallRecordModel.tenant_id == fence.tenant_id,
                AiCallRecordModel.call_id == fence.call_id,
            )
        )
        assert record is not None
        record.terminal_requested_at = datetime.now(timezone.utc)

    finalized = await bridge.finalize_call(
        fence,
        ended_at=datetime.now(timezone.utc),
        timeout_seconds=1,
    )
    p95 = sorted(latencies)[math.ceil(len(latencies) * 0.95) - 1]
    print(f"OWNER_DIALOGUE_P95_MS {p95 * 1_000:.3f}")
    assert finalized.status == "complete"
    assert finalized.persisted_count == 20
    assert p95 < 1.0

    await bridge.stop()
    await engine.dispose()
