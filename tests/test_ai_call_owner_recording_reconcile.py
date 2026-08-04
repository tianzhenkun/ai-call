from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import (
    AiCallRecordingModel,
    AiCallRecordingTrackModel,
    AiCallRecordModel,
)
from app.api.v1.system.oss.model import OssModel
from app.api.v1.system.oss.service import OssService
from app.core.base_model import MappedBase
from app.services.ai_call.recording_service import (
    AiCallRecordingReconcileWorker,
    AiCallRecordingService,
)

NOW = datetime.now(timezone.utc) - timedelta(seconds=1)
OSS_CONFIG = {
    "bucket_name": "recordings",
    "endpoint": "minio.test:9000",
    "domain": "https://files.test",
    "is_https": "N",
    "access_key": "test-access",
    "secret_key": "test-secret",
    "region": "",
}


async def _database(*, deadline_at: datetime):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker.begin() as db:
        db.add(
            AiCallRecordModel(
                id=1,
                tenant_id="tenant-a",
                call_id="call-a",
                entry_type="web",
                room_name="room-a",
                participant_identity="browser-a",
                status="completed",
                started_at=NOW - timedelta(minutes=1),
                ended_at=NOW,
                terminal_requested_at=NOW,
                runtime_control_mode="owner_command_v1",
                runtime_owner_id=None,
                runtime_fencing_token=9,
                runtime_capacity_class="none",
                resource_cleanup_status="clean",
                resource_cleanup_completed_at=NOW,
                dialogue_persistence_status="complete",
                dialogue_persistence_completed_at=NOW,
            )
        )
        db.add(
            AiCallRecordingModel(
                id=2,
                tenant_id="tenant-a",
                call_id="call-a",
                room_name="room-a",
                status="verifying",
                egress_id="EG_main",
                egress_generation=1,
                object_name="ai-call/recordings/call-a.ogg",
                started_at=NOW - timedelta(minutes=1),
                ended_at=NOW,
                duration_ms=55_000,
                stop_requested_at=NOW,
                verify_attempts=0,
                next_verify_at=NOW,
                verify_deadline_at=deadline_at,
            )
        )
    return engine, session_maker


def _worker(session_maker) -> AiCallRecordingReconcileWorker:
    return AiCallRecordingReconcileWorker(
        session_maker,
        lambda repository: AiCallRecordingService(repository, enabled=True),
    )


@pytest.mark.anyio
async def test_owner_recording_object_late_retries_then_completes_once(
    monkeypatch,
) -> None:
    engine, session_maker = await _database(deadline_at=NOW + timedelta(minutes=15))
    monkeypatch.setattr(OssService, "_active_config", OSS_CONFIG)
    observations = iter((None, 4096))

    async def resolve_size(_config, _object_name):
        return next(observations)

    monkeypatch.setattr(
        OssService,
        "resolve_existing_object_size",
        staticmethod(resolve_size),
    )
    worker = _worker(session_maker)

    assert await worker.flush_once() == set()
    async with session_maker.begin() as db:
        recording = await db.get(AiCallRecordingModel, 2)
        assert recording is not None
        assert recording.status == "verifying"
        assert recording.verify_attempts == 1
        recording.next_verify_at = NOW

    assert await worker.flush_once() == {"call-a"}
    assert await worker.flush_once() == set()

    async with session_maker() as db:
        recording = await db.get(AiCallRecordingModel, 2)
        record = await db.get(AiCallRecordModel, 1)
        oss_count = await db.scalar(select(func.count()).select_from(OssModel))
        assert recording is not None and recording.status == "completed"
        assert recording.oss_id is not None
        assert recording.duration_ms == 55_000
        assert oss_count == 1
        assert record is not None
        assert record.runtime_owner_id is None
        assert record.runtime_capacity_class == "none"
        assert record.resource_cleanup_status == "clean"

    await engine.dispose()


@pytest.mark.anyio
async def test_completed_owner_recordings_recover_missed_offline_asr_enqueue() -> None:
    engine, session_maker = await _database(deadline_at=NOW + timedelta(minutes=15))
    async with session_maker.begin() as db:
        recording = await db.get(AiCallRecordingModel, 2)
        assert recording is not None
        recording.status = "completed"
        recording.oss_id = 10
        recording.next_verify_at = None
        db.add(
            AiCallRecordingTrackModel(
                id=3,
                tenant_id="tenant-a",
                call_id="call-a",
                room_name="room-a",
                track_role="customer",
                participant_identity="customer-a",
                status="completed",
                oss_id=11,
                object_name="ai-call/recordings/tracks/call-a/customer.ogg",
                started_at=NOW - timedelta(minutes=1),
                ended_at=NOW,
                duration_ms=55_000,
            )
        )

    ready_for_asr: list[str] = []
    worker = AiCallRecordingReconcileWorker(
        session_maker,
        lambda repository: AiCallRecordingService(repository, enabled=True),
        on_call_ready_for_asr=ready_for_asr.append,
    )

    assert await worker.flush_once() == {"call-a"}
    assert ready_for_asr == ["call-a"]

    await engine.dispose()


@pytest.mark.anyio
async def test_stale_recording_verification_claim_cannot_register_oss(
    monkeypatch,
) -> None:
    engine, session_maker = await _database(deadline_at=NOW + timedelta(minutes=15))
    monkeypatch.setattr(OssService, "_active_config", OSS_CONFIG)

    async def resolve_size(_config, _object_name):
        return 2048

    monkeypatch.setattr(
        OssService,
        "resolve_existing_object_size",
        staticmethod(resolve_size),
    )
    async with session_maker.begin() as first_db:
        first_claim = (
            await AiCallRecordRepository(first_db).claim_due_recording_verifications(
                now=NOW,
                limit=1,
                claim_ttl=timedelta(seconds=1),
            )
        )[0]
    async with session_maker.begin() as second_db:
        second_claim = (
            await AiCallRecordRepository(second_db).claim_due_recording_verifications(
                now=NOW + timedelta(seconds=2),
                limit=1,
                claim_ttl=timedelta(seconds=30),
            )
        )[0]

    async with session_maker.begin() as stale_db:
        stale_service = AiCallRecordingService(
            AiCallRecordRepository(stale_db),
            enabled=True,
        )
        assert not await stale_service._verify_main_recording(first_claim, now=NOW)

    async with session_maker() as db:
        assert await db.scalar(select(func.count()).select_from(OssModel)) == 0

    async with session_maker.begin() as current_db:
        current_service = AiCallRecordingService(
            AiCallRecordRepository(current_db),
            enabled=True,
        )
        assert await current_service._verify_main_recording(second_claim, now=NOW)

    async with session_maker() as db:
        recording = await db.get(AiCallRecordingModel, 2)
        assert recording is not None and recording.status == "completed"
        assert await db.scalar(select(func.count()).select_from(OssModel)) == 1

    await engine.dispose()


@pytest.mark.anyio
async def test_owner_recording_deadline_fails_without_reacquiring_runtime_owner(
    monkeypatch,
) -> None:
    engine, session_maker = await _database(deadline_at=NOW - timedelta(seconds=1))
    monkeypatch.setattr(OssService, "_active_config", OSS_CONFIG)

    async def missing_object(_config, _object_name):
        return None

    monkeypatch.setattr(
        OssService,
        "resolve_existing_object_size",
        staticmethod(missing_object),
    )

    assert await _worker(session_maker).flush_once() == {"call-a"}

    async with session_maker() as db:
        recording = await db.get(AiCallRecordingModel, 2)
        record = await db.get(AiCallRecordModel, 1)
        assert recording is not None and recording.status == "failed"
        assert recording.failure_stage == "oss_missing"
        assert record is not None and record.runtime_owner_id is None
        assert record.runtime_fencing_token == 9
        assert record.resource_cleanup_status == "clean"

    await engine.dispose()
