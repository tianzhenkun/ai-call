from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.model import AiCallRecordingModel, AiCallRecordModel
from app.core.base_model import MappedBase
from app.services.ai_call.runtime_control.effect_repository import (
    ProviderObservation,
    ProviderObservationKind,
)
from app.services.ai_call.runtime_control.models import AiCallRuntimeEffectModel
from app.services.ai_call.runtime_control.recording_repository import (
    OwnerRecordingRepository,
)

NOW = datetime(2026, 8, 3, 8, 0, tzinfo=timezone.utc)


def _record() -> AiCallRecordModel:
    return AiCallRecordModel(
        id=1,
        tenant_id="tenant-a",
        call_id="call-a",
        entry_type="web",
        room_name="room-a",
        participant_identity="browser-a",
        status="connected",
        started_at=NOW,
        runtime_control_mode="owner_command_v1",
    )


def _effect(effect_type: str) -> AiCallRuntimeEffectModel:
    return AiCallRuntimeEffectModel(
        id=10 if effect_type == "START_EGRESS" else 11,
        tenant_id="tenant-a",
        call_id="call-a",
        command_id=2,
        effect_type=effect_type,
        idempotency_key=f"effect:{effect_type}",
        fencing_token=7,
        status="APPLIED",
        provider_namespace="livekit:test",
        provider_idempotency_key=f"provider:{effect_type}",
        resource_key="egress:main:call-a",
        resource_generation=1,
        source_create_effect_id=10 if effect_type == "STOP_EGRESS" else None,
        created_at=NOW,
        updated_at=NOW,
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("effect_type", "observation", "expected_status"),
    [
        (
            "START_EGRESS",
            ProviderObservation(
                kind=ProviderObservationKind.RESOURCE_PRESENT,
                provider_reference="EG_main",
                object_name="ai-call/main/call-a.ogg",
            ),
            "recording",
        ),
        (
            "START_EGRESS",
            ProviderObservation(kind=ProviderObservationKind.UNCERTAIN),
            "starting",
        ),
        (
            "START_EGRESS",
            ProviderObservation(
                kind=ProviderObservationKind.PERMANENT_NO_RESOURCE,
                failure_code="oss_config_missing",
            ),
            "failed",
        ),
        (
            "STOP_EGRESS",
            ProviderObservation(kind=ProviderObservationKind.ACCEPTED),
            "stopping",
        ),
        (
            "STOP_EGRESS",
            ProviderObservation(
                kind=ProviderObservationKind.TERMINAL_CONFIRMED,
                ended_at=NOW,
                duration_ms=1200,
            ),
            "verifying",
        ),
    ],
)
async def test_owner_recording_projection_maps_provider_observation(
    effect_type: str,
    observation: ProviderObservation,
    expected_status: str,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)

    async with session_maker() as db:
        record = _record()
        db.add(record)
        if effect_type == "STOP_EGRESS":
            db.add(
                AiCallRecordingModel(
                    id=20,
                    tenant_id="tenant-a",
                    call_id="call-a",
                    room_name="room-a",
                    status="recording",
                    egress_id="EG_main",
                    egress_generation=1,
                    object_name="ai-call/main/call-a.ogg",
                    started_at=NOW,
                )
            )
        await db.flush()

        repository = OwnerRecordingRepository(db, id_generator=lambda: 20)
        projected = await repository.project(
            record=record,
            effect=_effect(effect_type),
            source_effect=_effect("START_EGRESS") if effect_type == "STOP_EGRESS" else None,
            observation=observation,
            now=NOW,
        )

        assert projected is not None
        assert projected.status == expected_status
        assert projected.egress_generation == 1

    await engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize("terminal_status", ["completed", "failed"])
async def test_owner_recording_projection_never_regresses_terminal_status(
    terminal_status: str,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)

    async with session_maker() as db:
        record = _record()
        recording = AiCallRecordingModel(
            id=20,
            tenant_id="tenant-a",
            call_id="call-a",
            room_name="room-a",
            status=terminal_status,
            egress_id="EG_main",
            egress_generation=1,
            object_name="ai-call/main/call-a.ogg",
            started_at=NOW,
        )
        db.add_all([record, recording])
        await db.flush()

        projected = await OwnerRecordingRepository(db).project(
            record=record,
            effect=_effect("STOP_EGRESS"),
            source_effect=_effect("START_EGRESS"),
            observation=ProviderObservation(
                kind=ProviderObservationKind.TERMINAL_CONFIRMED,
            ),
            now=NOW,
        )

        assert projected is recording
        assert projected.status == terminal_status

    await engine.dispose()


@pytest.mark.anyio
async def test_owner_recording_projection_stores_only_safe_failure_code() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)

    async with session_maker() as db:
        record = _record()
        db.add(record)
        await db.flush()
        projected = await OwnerRecordingRepository(db, id_generator=lambda: 20).project(
            record=record,
            effect=_effect("START_EGRESS"),
            source_effect=None,
            observation=ProviderObservation(
                kind=ProviderObservationKind.PERMANENT_NO_RESOURCE,
                failure_code="secret=abc\nTraceback: leaked",
                error_message="access_key=leaked",
            ),
            now=NOW,
        )

        assert projected is not None
        assert projected.failure_message == "provider_failure"
        assert "secret" not in projected.failure_message
        assert "Traceback" not in projected.failure_message
        assert "access_key" not in projected.failure_message

    await engine.dispose()
