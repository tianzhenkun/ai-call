from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import UniqueConstraint, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call import crud as ai_call_crud
from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import AiCallRecordingTrackModel
from app.api.v1.ai_call.service import AiCallService
from app.core.base_model import MappedBase
from app.core.exceptions import CustomException
from app.services.ai_call.offline_asr_service import AiCallOfflineAsrService
from app.services.ai_call.recording_service import AiCallRecordingService
from app.services.ai_call.session_registry import CallSessionStatus

NOW = datetime.now(timezone.utc)
MIGRATION_PATH = (
    Path(__file__).parents[1]
    / "docs/livekit-ai-outbound/sql/phase-i6-owner-runtime-customer-track-recording.sql"
)


def test_recording_track_tenant_model_and_migration_contract() -> None:
    table = AiCallRecordingTrackModel.__table__

    assert table.c.tenant_id.nullable is False
    assert table.c.egress_generation.nullable is True

    unique_columns = {
        tuple(column.name for column in constraint.columns)
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert (
        "tenant_id",
        "call_id",
        "track_role",
        "participant_identity",
    ) in unique_columns
    assert ("call_id", "track_role", "participant_identity") not in unique_columns

    migration = MIGRATION_PATH.read_text(encoding="utf-8").lower()
    assert "ai_call_recording_track_tenant_backfill_failed" in migration
    assert "alter column tenant_id set not null" in migration
    assert "unique (tenant_id, call_id, track_role, participant_identity)" in migration
    assert "having count(record.id) filter" in migration
    assert ") <> 1" in migration
    assert "nullif(btrim(track.tenant_id), '') is null" in migration
    assert "track.tenant_id is distinct from record.tenant_id" in migration


def test_recording_track_tenant_participant_start_requires_explicit_context() -> None:
    for method in (
        AiCallRecordingService.start_session_participant_recordings,
        AiCallRecordingService.start_human_agent_recording,
    ):
        parameters = inspect.signature(method).parameters
        assert "tenant_id" in parameters
        assert parameters["tenant_id"].kind is inspect.Parameter.KEYWORD_ONLY


@pytest.mark.anyio
async def test_recording_track_tenant_browser_start_fails_closed_without_record_service() -> None:
    service = object.__new__(AiCallService)
    service.recording_service = AsyncMock()
    service.record_service = None
    service.orchestrator = SimpleNamespace(
        get_session=AsyncMock(
            return_value=SimpleNamespace(
                status=CallSessionStatus.CONNECTED,
                room_name="room-a",
            )
        )
    )

    with pytest.raises(CustomException, match="通话录音缺少租户上下文"):
        await service._start_browser_ready_recording_tracks("call-a")

    service.recording_service.start_session_participant_recordings.assert_not_awaited()


@pytest.mark.anyio
async def test_recording_track_tenant_repository_isolates_equal_participants() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker.begin() as db:
        repository = AiCallRecordRepository(db)
        tenant_a_track = await repository.create_recording_track(
            tenant_id="tenant-a",
            call_id="shared-call",
            room_name="room-a",
            track_role="customer",
            participant_identity="shared-participant",
            status="recording",
            started_at=NOW,
        )
        tenant_b_track = await repository.create_recording_track(
            tenant_id="tenant-b",
            call_id="shared-call",
            room_name="room-b",
            track_role="customer",
            participant_identity="shared-participant",
            status="recording",
            started_at=NOW,
        )

        assert (
            await repository.get_recording_track(
                tenant_id="tenant-a",
                call_id="shared-call",
                track_role="customer",
                participant_identity="shared-participant",
            )
        ).id == tenant_a_track.id
        assert [
            track.id
            for track in await repository.list_recording_tracks(
                tenant_id="tenant-a",
                call_id="shared-call",
            )
        ] == [tenant_a_track.id]

        assert (
            await repository.update_recording_track(
                tenant_id="tenant-a",
                track_id=tenant_b_track.id,
                status="failed",
            )
            is None
        )
        tenant_b_status = await db.scalar(
            select(AiCallRecordingTrackModel.status).where(
                AiCallRecordingTrackModel.id == tenant_b_track.id
            )
        )
        assert tenant_b_status == "recording"

    await engine.dispose()


@pytest.mark.anyio
async def test_recording_track_tenant_wrong_claim_token_cannot_update() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker.begin() as db:
        repository = AiCallRecordRepository(db)
        track = await repository.create_recording_track(
            tenant_id="tenant-a",
            call_id="call-a",
            room_name="room-a",
            track_role="customer",
            participant_identity="customer-a",
            status="verifying",
            object_name="ai-call/recordings/tracks/call-a/customer.mp3",
            started_at=NOW - timedelta(minutes=1),
        )
        await repository.update_recording_track(
            tenant_id="tenant-a",
            track_id=track.id,
            next_verify_at=NOW,
            verify_deadline_at=NOW + timedelta(minutes=15),
        )

        claims = await repository.claim_due_recording_track_verifications(
            now=NOW,
            limit=1,
            claim_ttl=timedelta(minutes=2),
        )

        assert len(claims) == 1
        assert isinstance(claims[0], ai_call_crud.RecordingTrackVerificationClaim)
        assert claims[0].tenant_id == "tenant-a"
        assert claims[0].track_id == track.id
        assert not await repository.update_due_recording_track(
            tenant_id="tenant-a",
            track_id=track.id,
            claim_token=claims[0].claim_token + timedelta(seconds=1),
            status="completed",
        )
        assert (
            await db.scalar(
                select(AiCallRecordingTrackModel.status).where(
                    AiCallRecordingTrackModel.id == track.id
                )
            )
            == "verifying"
        )

    await engine.dispose()


@pytest.mark.anyio
async def test_recording_track_tenant_offline_asr_fails_closed_without_record() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker.begin() as db:
        service = AiCallOfflineAsrService(
            AiCallRecordRepository(db),
            provider=object(),
        )

        with pytest.raises(RuntimeError, match="offline ASR missing tenant context"):
            await service.process_call("missing-record")

    await engine.dispose()
