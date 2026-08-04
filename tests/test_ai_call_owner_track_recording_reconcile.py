from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import AiCallRecordingTrackModel, AiCallRecordModel
from app.api.v1.system.oss.service import OssService
from app.core.base_model import MappedBase
from app.services.ai_call import recording_service as recording_service_module
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


async def _database(
    *,
    deadline_at: datetime,
    track_status: str = "verifying",
    track_ended_at: datetime | None = NOW,
):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker.begin() as db:
        db.add(
            AiCallRecordModel(
                id=2,
                tenant_id="tenant-a",
                call_id="call-a",
                entry_type="web",
                room_name="room-a",
                participant_identity="customer-a",
                status="completed",
                started_at=NOW - timedelta(minutes=1),
                ended_at=NOW,
                runtime_control_mode="owner_command_v1",
            )
        )
        db.add(
            AiCallRecordingTrackModel(
                id=1,
                tenant_id="tenant-a",
                call_id="call-a",
                room_name="room-a",
                track_role="customer",
                participant_identity="customer-a",
                status=track_status,
                object_name="ai-call/recordings/tracks/call-a/customer-a.ogg",
                started_at=NOW - timedelta(minutes=1),
                ended_at=track_ended_at,
                stop_requested_at=NOW,
                verify_attempts=0,
                next_verify_at=NOW,
                verify_deadline_at=deadline_at,
            )
        )
    return engine, session_maker


def _worker(session_maker, *, service_factory=None):
    return AiCallRecordingReconcileWorker(
        session_maker,
        service_factory
        or (lambda repository: AiCallRecordingService(repository, enabled=True)),
    )


@pytest.mark.anyio
async def test_track_reconcile_uses_claim_cas_for_completion(monkeypatch) -> None:
    info_messages: list[str] = []
    monkeypatch.setattr(
        recording_service_module.log,
        "info",
        lambda message, *args: info_messages.append(message.format(*args)),
    )
    engine, session_maker = await _database(deadline_at=NOW + timedelta(minutes=15))
    monkeypatch.setattr(OssService, "_active_config", OSS_CONFIG)
    register = AsyncMock(return_value=101)

    async def resolve_size(_config, _object_name):
        return 2048

    monkeypatch.setattr(
        OssService,
        "resolve_existing_object_size",
        staticmethod(resolve_size),
    )

    async with session_maker.begin() as db:
        repository = AiCallRecordRepository(db)
        egress = type(
            "EgressSpy",
            (),
            {"start_room_audio_recording": AsyncMock(), "stop_egress": AsyncMock()},
        )()
        service = AiCallRecordingService(repository, enabled=True, egress_manager=egress)
        service._register_recording_object = register
        repository.lock_due_recording = AsyncMock(
            side_effect=AssertionError("track verification must not lock main recording")
        )
        unscoped_update = AsyncMock(
            side_effect=AssertionError("track verification must use claim CAS")
        )
        repository.update_recording_track = unscoped_update
        assert await service.reconcile_due_recordings() == {"call-a"}
        assert unscoped_update.await_count == 0
        assert register.await_count == 1

    async with session_maker() as db:
        track = await db.get(AiCallRecordingTrackModel, 1)
        assert track is not None
        assert track.status == "completed"
        assert track.oss_id == 101
    egress.start_room_audio_recording.assert_not_awaited()
    egress.stop_egress.assert_not_awaited()
    assert any("已通过OSS回查确认完成状态" in message for message in info_messages)
    assert all("停止结果不确定" not in message for message in info_messages)

    await engine.dispose()


@pytest.mark.anyio
async def test_terminal_call_recovers_track_left_recording(monkeypatch) -> None:
    engine, session_maker = await _database(
        deadline_at=NOW + timedelta(minutes=15),
        track_status="recording",
        track_ended_at=None,
    )
    monkeypatch.setattr(OssService, "_active_config", OSS_CONFIG)
    monkeypatch.setattr(
        OssService,
        "resolve_existing_object_size",
        staticmethod(AsyncMock(return_value=935_281)),
    )

    async with session_maker.begin() as db:
        service = AiCallRecordingService(
            AiCallRecordRepository(db),
            enabled=True,
        )
        service._register_recording_object = AsyncMock(return_value=103)
        assert await service.reconcile_due_recordings() == {"call-a"}

    async with session_maker() as db:
        track = await db.get(AiCallRecordingTrackModel, 1)
        assert track is not None
        assert track.status == "completed"
        assert track.oss_id == 103
        assert track.ended_at == NOW.replace(tzinfo=None)
        assert track.duration_ms == 60_000

    await engine.dispose()


@pytest.mark.anyio
async def test_track_reconcile_first_missing_then_visible_registers_once(monkeypatch) -> None:
    engine, session_maker = await _database(deadline_at=NOW + timedelta(minutes=15))
    monkeypatch.setattr(OssService, "_active_config", OSS_CONFIG)
    observations = iter((None, 4096, 4096))

    async def resolve_size(_config, _object_name):
        return next(observations)

    monkeypatch.setattr(
        OssService,
        "resolve_existing_object_size",
        staticmethod(resolve_size),
    )
    register = AsyncMock(return_value=102)

    async with session_maker.begin() as db:
        repository = AiCallRecordRepository(db)
        service = AiCallRecordingService(repository, enabled=True)
        service._register_recording_object = register
        assert await service.reconcile_due_recordings() == set()

    async with session_maker.begin() as db:
        track = await db.get(AiCallRecordingTrackModel, 1)
        assert track is not None
        track.next_verify_at = NOW

    async with session_maker.begin() as db:
        repository = AiCallRecordRepository(db)
        service = AiCallRecordingService(repository, enabled=True)
        service._register_recording_object = register
        assert await service.reconcile_due_recordings() == {"call-a"}

    async with session_maker.begin() as db:
        repository = AiCallRecordRepository(db)
        service = AiCallRecordingService(repository, enabled=True)
        service._register_recording_object = register
        assert await service.reconcile_due_recordings() == set()

    assert register.await_count == 1
    await engine.dispose()


@pytest.mark.anyio
async def test_stale_track_claim_cannot_update_or_register(monkeypatch) -> None:
    engine, session_maker = await _database(deadline_at=NOW + timedelta(minutes=15))
    monkeypatch.setattr(OssService, "_active_config", OSS_CONFIG)

    async def resolve_size(_config, _object_name):
        return 2048

    monkeypatch.setattr(
        OssService,
        "resolve_existing_object_size",
        staticmethod(resolve_size),
    )
    async with session_maker.begin() as db:
        repository = AiCallRecordRepository(db)
        claim = (
            await repository.claim_due_recording_track_verifications(
                now=NOW,
                limit=1,
                claim_ttl=timedelta(seconds=30),
            )
        )[0]

    async with session_maker.begin() as db:
        repository = AiCallRecordRepository(db)
        assert not await repository.update_due_recording_track(
            tenant_id=claim.tenant_id,
            track_id=claim.track_id,
            claim_token=claim.claim_token + timedelta(seconds=1),
            status="failed",
        )
        track = await repository.lock_due_recording_track(
            tenant_id=claim.tenant_id,
            track_id=claim.track_id,
            claim_token=claim.claim_token,
        )
        assert track is not None

    await engine.dispose()


@pytest.mark.anyio
async def test_track_deadline_failure_allows_offline_asr_and_keeps_error_visible(
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
    worker = _worker(session_maker)
    assert await worker.flush_once() == {"call-a"}

    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        service = AiCallRecordingService(repository, enabled=True)
        track = await db.get(AiCallRecordingTrackModel, 1)
        assert track is not None
        assert track.status == "failed"
        assert track.failure_stage == "oss_missing"
        assert track.failure_message is not None
        assert await service.is_ready_for_offline_asr(
            tenant_id="tenant-a",
            call_id="call-a",
        )

    await engine.dispose()
