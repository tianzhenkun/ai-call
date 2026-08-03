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
from app.services.ai_call.event_store import InMemoryEventStore
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

    indexes = {
        index.name: tuple(column.name for column in index.columns)
        for index in table.indexes
    }
    assert indexes["idx_ai_call_recording_track_verify_due"] == (
        "status",
        "next_verify_at",
        "id",
    )

    migration = MIGRATION_PATH.read_text(encoding="utf-8").lower()
    assert "ai_call_recording_track_tenant_backfill_failed" in migration
    assert "alter column tenant_id set not null" in migration
    assert "unique (tenant_id, call_id, track_role, participant_identity)" in migration
    assert "having count(record.id) filter" in migration
    assert ") <> 1" in migration
    assert "nullif(btrim(track.tenant_id), '') is null" in migration
    assert "track.tenant_id is distinct from record.tenant_id" in migration
    assert "idx_ai_call_recording_track_verify_due" in migration
    assert "on ai_call_recording_track (status, next_verify_at, id)" in migration


def test_recording_track_tenant_participant_start_requires_explicit_context() -> None:
    for method in (
        AiCallRecordingService.start_session_participant_recordings,
        AiCallRecordingService.start_human_agent_recording,
    ):
        parameters = inspect.signature(method).parameters
        assert "tenant_id" in parameters
        assert parameters["tenant_id"].kind is inspect.Parameter.KEYWORD_ONLY

    list_asr_parameters = inspect.signature(
        AiCallRecordRepository.list_asr_jobs
    ).parameters
    assert list_asr_parameters["tenant_id"].kind is inspect.Parameter.KEYWORD_ONLY
    assert list_asr_parameters["call_id"].kind is inspect.Parameter.KEYWORD_ONLY


@pytest.mark.anyio
@pytest.mark.parametrize("event_type", ["browser_disconnect", "browser_first_audio"])
async def test_browser_event_rejects_cross_tenant_before_side_effects(
    event_type: str,
) -> None:
    orchestrator = SimpleNamespace(
        get_session=AsyncMock(),
        report_browser_event=AsyncMock(),
    )
    record_service = SimpleNamespace(
        repository=None,
        get_record_for_tenant=AsyncMock(return_value=None),
        get_record=AsyncMock(
            return_value=SimpleNamespace(tenant_id="tenant-a", call_id="shared-call")
        ),
        mark_answered=AsyncMock(),
        complete_session=AsyncMock(),
    )
    recording_service = SimpleNamespace(
        stop_for_session=AsyncMock(),
        start_session_participant_recordings=AsyncMock(),
    )
    service = AiCallService(
        orchestrator,
        record_service=record_service,
        recording_service=recording_service,
    )

    with pytest.raises(CustomException, match="通话事件租户上下文不匹配"):
        await service.report_browser_event(
            call_id="shared-call",
            event_type=event_type,
            timestamp=None,
            tenant_id="tenant-b",
        )

    orchestrator.get_session.assert_not_awaited()
    orchestrator.report_browser_event.assert_not_awaited()
    recording_service.stop_for_session.assert_not_awaited()
    recording_service.start_session_participant_recordings.assert_not_awaited()
    record_service.mark_answered.assert_not_awaited()
    record_service.complete_session.assert_not_awaited()


@pytest.mark.anyio
async def test_browser_event_rejects_missing_tenant_before_side_effects() -> None:
    orchestrator = SimpleNamespace(report_browser_event=AsyncMock())
    record_service = SimpleNamespace(
        repository=None,
        get_record_for_tenant=AsyncMock(),
    )
    service = AiCallService(orchestrator, record_service=record_service)

    with pytest.raises(CustomException, match="通话事件缺少租户上下文"):
        await service.report_browser_event(
            call_id="call-a",
            event_type="browser_first_audio",
            timestamp=None,
        )

    record_service.get_record_for_tenant.assert_not_awaited()
    orchestrator.report_browser_event.assert_not_awaited()


@pytest.mark.anyio
async def test_owner_browser_ready_does_not_route_through_legacy_orchestrator() -> None:
    orchestrator = SimpleNamespace(
        event_store=InMemoryEventStore(),
        report_browser_event=AsyncMock(),
    )
    record_service = SimpleNamespace(
        repository=None,
        get_record_for_tenant=AsyncMock(
            return_value=SimpleNamespace(
                tenant_id="tenant-a",
                call_id="owner-call",
                runtime_control_mode="owner_command_v1",
            )
        ),
        mark_owner_customer_ready=AsyncMock(return_value=True),
    )
    service = AiCallService(orchestrator, record_service=record_service)

    result = await service.report_browser_event(
        call_id="owner-call",
        event_type="browser_ready",
        timestamp=NOW,
        payload={"source": "browser"},
        tenant_id="tenant-a",
    )

    assert result.call_id == "owner-call"
    assert result.type == "browser_ready"
    assert result.source == "browser"
    assert result.timestamp == NOW
    assert result.payload["source"] == "browser"
    assert result.payload["reportedAt"] == NOW.isoformat()
    orchestrator.report_browser_event.assert_not_awaited()
    record_service.mark_owner_customer_ready.assert_awaited_once_with(
        tenant_id="tenant-a",
        call_id="owner-call",
    )


@pytest.mark.anyio
async def test_owner_browser_disconnect_requests_end_without_legacy_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.api.v1.ai_call import service as ai_call_service_module

    end_requests = []

    class FakeRuntimeCommandRepository:
        def __init__(self, db) -> None:
            self.db = db

        async def request_end(self, request) -> None:
            end_requests.append(request)

    monkeypatch.setattr(
        ai_call_service_module,
        "RuntimeCommandRepository",
        FakeRuntimeCommandRepository,
    )

    orchestrator = SimpleNamespace(
        event_store=InMemoryEventStore(),
        report_browser_event=AsyncMock(),
    )
    record_service = SimpleNamespace(
        repository=SimpleNamespace(db=object()),
        get_record_for_tenant=AsyncMock(
            return_value=SimpleNamespace(
                tenant_id="tenant-a",
                call_id="owner-call",
                runtime_control_mode="owner_command_v1",
            )
        ),
        complete_session=AsyncMock(),
    )
    service = AiCallService(orchestrator, record_service=record_service)

    result = await service.report_browser_event(
        call_id="owner-call",
        event_type="browser_disconnect",
        timestamp=NOW,
        tenant_id="tenant-a",
    )

    assert result.type == "browser_disconnect"
    assert result.source == "browser"
    assert result.timestamp == NOW
    orchestrator.report_browser_event.assert_not_awaited()
    record_service.complete_session.assert_not_awaited()
    assert len(end_requests) == 1
    assert end_requests[0].tenant_id == "tenant-a"
    assert end_requests[0].call_id == "owner-call"
    assert end_requests[0].source == "browser_client"
    assert end_requests[0].end_reason == "browser_disconnect"
    assert end_requests[0].dedupe_key == "browser_disconnect:owner-call"


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

    with pytest.raises(CustomException, match="通话录音对应的通话记录不存在"):
        await service._start_browser_ready_recording_tracks(
            tenant_id="tenant-a",
            call_id="call-a",
        )

    service.recording_service.start_session_participant_recordings.assert_not_awaited()


@pytest.mark.anyio
async def test_recording_track_tenant_browser_start_rejects_record_mismatch() -> None:
    service = object.__new__(AiCallService)
    service.recording_service = AsyncMock()
    service.record_service = SimpleNamespace(
        get_record_for_tenant=AsyncMock(
            return_value=SimpleNamespace(
                tenant_id="tenant-b",
                room_name="room-b",
                participant_identity="browser-b",
            )
        )
    )
    service.orchestrator = SimpleNamespace(
        get_session=AsyncMock(
            return_value=SimpleNamespace(
                status=CallSessionStatus.CONNECTED,
                room_name="room-a",
            )
        )
    )

    with pytest.raises(CustomException, match="通话录音租户上下文不匹配"):
        await service._start_browser_ready_recording_tracks(
            tenant_id="tenant-a",
            call_id="shared-call",
        )

    service.recording_service.start_session_participant_recordings.assert_not_awaited()


@pytest.mark.anyio
async def test_recording_track_tenant_reconcile_checks_equal_call_ids_independently() -> None:
    claims = [
        ai_call_crud.RecordingTrackVerificationClaim(
            track_id=index,
            tenant_id=tenant_id,
            call_id="shared-call",
            track_role="customer",
            participant_identity=f"customer-{tenant_id}",
            object_name=f"tracks/{tenant_id}.mp3",
            started_at=NOW - timedelta(minutes=1),
            ended_at=None,
            duration_ms=None,
            verify_attempts=0,
            verify_deadline_at=NOW + timedelta(minutes=15),
            claim_token=NOW + timedelta(minutes=2),
        )
        for index, tenant_id in enumerate(("tenant-a", "tenant-b"), start=1)
    ]
    repository = SimpleNamespace(
        claim_due_recording_verifications=AsyncMock(return_value=[]),
        claim_due_recording_track_verifications=AsyncMock(return_value=claims),
    )
    service = AiCallRecordingService(
        repository,
        enabled=True,
        transaction_checkpoint=AsyncMock(),
    )
    service._verify_participant_recording = AsyncMock(return_value=True)
    service.is_ready_for_offline_asr = AsyncMock(return_value=True)

    assert await service.reconcile_due_recordings() == {"shared-call"}
    checked_tenants = {
        item.kwargs["tenant_id"]
        for item in service.is_ready_for_offline_asr.await_args_list
    }
    assert checked_tenants == {"tenant-a", "tenant-b"}


@pytest.mark.anyio
async def test_recording_detail_asr_jobs_are_scoped_through_tenant_tracks() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker.begin() as db:
        repository = AiCallRecordRepository(db)
        recordings = {}
        for tenant_id in ("tenant-a", "tenant-b"):
            recordings[tenant_id] = await repository.create_recording(
                tenant_id=tenant_id,
                call_id="shared-call",
                room_name=f"room-{tenant_id}",
                status="completed",
                started_at=NOW - timedelta(minutes=1),
            )
            track = await repository.create_recording_track(
                tenant_id=tenant_id,
                call_id="shared-call",
                room_name=f"room-{tenant_id}",
                track_role="customer",
                participant_identity=f"customer-{tenant_id}",
                status="completed",
                started_at=NOW - timedelta(minutes=1),
            )
            job = await repository.create_asr_job(
                call_id="shared-call",
                track_id=track.id,
                track_role="customer",
                participant_identity=track.participant_identity,
                provider="test",
                model="test-model",
                status="completed",
                source_url=f"https://source.test/{tenant_id}.ogg",
                submitted_at=NOW,
            )
            await repository.update_asr_job(
                job.id,
                transcription_url=f"https://asr.test/{tenant_id}.json",
            )

        detail = await AiCallRecordingService(
            repository,
            enabled=True,
        ).recording_to_dict(recordings["tenant-a"])

        assert [job["sourceUrl"] for job in detail["asrJobs"]] == [
            "https://source.test/tenant-a.ogg"
        ]
        assert [job["transcriptionUrl"] for job in detail["asrJobs"]] == [
            "https://asr.test/tenant-a.json"
        ]

    await engine.dispose()


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
            await repository.lock_due_recording_track(
                tenant_id="tenant-a",
                track_id=track.id,
                claim_token=claims[0].claim_token + timedelta(seconds=1),
            )
            is None
        )
        assert (
            await repository.lock_due_recording_track(
                tenant_id="tenant-b",
                track_id=track.id,
                claim_token=claims[0].claim_token,
            )
            is None
        )
        assert not await repository.update_due_recording_track(
            tenant_id="tenant-b",
            track_id=track.id,
            claim_token=claims[0].claim_token,
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
