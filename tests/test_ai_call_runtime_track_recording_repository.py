from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.model import (
    AiCallRecordingModel,
    AiCallRecordingTrackModel,
    AiCallRecordModel,
)
from app.core.base_model import MappedBase
from app.services.ai_call.runtime_control.customer_track import customer_track_keys
from app.services.ai_call.runtime_control.effect_repository import (
    ProviderObservation,
    ProviderObservationKind,
)
from app.services.ai_call.runtime_control.models import AiCallRuntimeEffectModel
from app.services.ai_call.runtime_control.track_recording_repository import (
    OwnerTrackRecordingRepository,
)

NOW = datetime(2026, 8, 3, 8, 0, tzinfo=timezone.utc)


def _record(*, tenant_id: str = "tenant-a") -> AiCallRecordModel:
    return AiCallRecordModel(
        id=1,
        tenant_id=tenant_id,
        call_id="call-a",
        entry_type="web",
        room_name="room-a",
        participant_identity="customer-a",
        status="connected",
        started_at=NOW,
        runtime_control_mode="owner_command_v1",
    )


def _start_effect(
    *,
    tenant_id: str = "tenant-a",
    participant_identity: str = "customer-a",
) -> AiCallRuntimeEffectModel:
    _, provider_key, resource_key = customer_track_keys(
        "call-a", participant_identity
    )
    return AiCallRuntimeEffectModel(
        id=10,
        tenant_id=tenant_id,
        call_id="call-a",
        command_id=2,
        effect_type="START_TRACK_EGRESS",
        idempotency_key="start:call-a:customer-track",
        fencing_token=7,
        status="APPLIED",
        provider_reference="EG_customer",
        provider_namespace="livekit:test",
        provider_idempotency_key=provider_key,
        resource_key=resource_key,
        resource_generation=1,
        created_at=NOW,
        updated_at=NOW,
    )


def _stop_effect(*, source_effect_id: int = 10) -> AiCallRuntimeEffectModel:
    _, _, resource_key = customer_track_keys("call-a", "customer-a")
    return AiCallRuntimeEffectModel(
        id=11,
        tenant_id="tenant-a",
        call_id="call-a",
        command_id=3,
        effect_type="STOP_TRACK_EGRESS",
        idempotency_key="end:call-a:STOP_TRACK_EGRESS:10",
        fencing_token=7,
        status="APPLIED",
        provider_namespace="livekit:test",
        provider_idempotency_key="destroy:customer",
        resource_key=resource_key,
        resource_generation=1,
        source_create_effect_id=source_effect_id,
        terminal_confirmed_at=NOW,
        created_at=NOW,
        updated_at=NOW,
    )


async def _session_maker() -> tuple[async_sessionmaker, object]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    return async_sessionmaker(engine, expire_on_commit=False), engine


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("observation", "expected_status"),
    [
        (
            ProviderObservation(
                kind=ProviderObservationKind.RESOURCE_PRESENT,
                provider_reference="EG_customer",
                object_name="tracks/call-a/customer-customer-a.ogg",
                started_at=NOW,
            ),
            "recording",
        ),
        (
            ProviderObservation(
                kind=ProviderObservationKind.UNCERTAIN,
                failure_code="provider_timeout",
            ),
            "starting",
        ),
        (
            ProviderObservation(
                kind=ProviderObservationKind.PERMANENT_NO_RESOURCE,
                failure_code="secret=should-not-persist",
                error_message="access_key=should-not-persist",
            ),
            "failed",
        ),
    ],
)
async def test_customer_track_projection_maps_start_observation(
    observation: ProviderObservation,
    expected_status: str,
) -> None:
    session_maker, engine = await _session_maker()
    try:
        async with session_maker() as session:
            record = _record()
            effect = _start_effect()
            session.add_all([record, effect])
            await session.flush()

            projected = await OwnerTrackRecordingRepository(
                session, id_generator=lambda: 20
            ).project(
                record=record,
                effect=effect,
                source_effect=None,
                observation=observation,
                now=NOW,
            )

            assert projected is not None
            assert projected.tenant_id == "tenant-a"
            assert projected.call_id == "call-a"
            assert projected.track_role == "customer"
            assert projected.participant_identity == "customer-a"
            assert projected.egress_generation == 1
            assert projected.status == expected_status
            if expected_status == "recording":
                assert projected.egress_id == "EG_customer"
                assert projected.object_name == "tracks/call-a/customer-customer-a.ogg"
            if expected_status == "failed":
                assert projected.failure_message == "provider_failure"
                assert "secret" not in projected.failure_message
                assert "access_key" not in projected.failure_message
    finally:
        await engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize("status", ["completed", "failed"])
async def test_customer_track_projection_never_regresses_terminal_status(
    status: str,
) -> None:
    session_maker, engine = await _session_maker()
    try:
        async with session_maker() as session:
            record = _record()
            effect = _start_effect()
            track = AiCallRecordingTrackModel(
                id=20,
                tenant_id="tenant-a",
                call_id="call-a",
                room_name="room-a",
                track_role="customer",
                participant_identity="customer-a",
                status=status,
                egress_id="EG_customer",
                egress_generation=1,
                object_name="tracks/call-a/customer-customer-a.ogg",
                started_at=NOW,
            )
            session.add_all([record, effect, track])
            await session.flush()

            projected = await OwnerTrackRecordingRepository(session).project(
                record=record,
                effect=effect,
                source_effect=None,
                observation=ProviderObservation(
                    kind=ProviderObservationKind.RESOURCE_PRESENT,
                    provider_reference="EG_new",
                ),
                now=NOW + timedelta(minutes=1),
            )

            assert projected is track
            assert projected.status == status
            assert projected.egress_id == "EG_customer"
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_customer_track_projection_maps_stop_and_does_not_touch_main_recording() -> None:
    session_maker, engine = await _session_maker()
    try:
        async with session_maker() as session:
            record = _record()
            main = AiCallRecordingModel(
                id=30,
                tenant_id="tenant-a",
                call_id="call-a",
                room_name="room-a",
                status="recording",
                egress_id="EG_main",
                egress_generation=1,
                object_name="main/call-a.ogg",
                started_at=NOW,
            )
            track = AiCallRecordingTrackModel(
                id=20,
                tenant_id="tenant-a",
                call_id="call-a",
                room_name="room-a",
                track_role="customer",
                participant_identity="customer-a",
                status="recording",
                egress_id="EG_customer",
                egress_generation=1,
                object_name="tracks/call-a/customer-customer-a.ogg",
                started_at=NOW,
            )
            start = _start_effect()
            stop = _stop_effect()
            session.add_all([record, main, track, start, stop])
            await session.flush()

            projected = await OwnerTrackRecordingRepository(session).project(
                record=record,
                effect=stop,
                source_effect=start,
                observation=ProviderObservation(
                    kind=ProviderObservationKind.TERMINAL_CONFIRMED,
                    provider_reference="EG_customer",
                    ended_at=NOW + timedelta(seconds=20),
                    duration_ms=20_000,
                ),
                now=NOW + timedelta(seconds=21),
            )

            assert projected is track
            assert projected.status == "verifying"
            assert projected.duration_ms == 20_000
            assert projected.next_verify_at == NOW + timedelta(seconds=21)
            assert main.status == "recording"
            assert main.egress_id == "EG_main"
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_customer_track_stop_requires_source_and_never_fabricates_missing_track() -> None:
    session_maker, engine = await _session_maker()
    try:
        async with session_maker() as session:
            record = _record()
            stop = _stop_effect()
            session.add_all([record, stop])
            await session.flush()

            with pytest.raises(RuntimeError, match="source create effect"):
                await OwnerTrackRecordingRepository(session).project(
                    record=record,
                    effect=stop,
                    source_effect=None,
                    observation=ProviderObservation(
                        kind=ProviderObservationKind.TERMINAL_CONFIRMED,
                    ),
                    now=NOW,
                )

            start = _start_effect()
            session.add(start)
            await session.flush()
            missing = await OwnerTrackRecordingRepository(session).project(
                record=record,
                effect=stop,
                source_effect=start,
                observation=ProviderObservation(
                    kind=ProviderObservationKind.TERMINAL_CONFIRMED,
                ),
                now=NOW,
            )
            assert missing is None
            assert (
                await session.scalar(select(AiCallRecordingTrackModel).where(
                    AiCallRecordingTrackModel.tenant_id == "tenant-a",
                    AiCallRecordingTrackModel.call_id == "call-a",
                ))
                is None
            )
    finally:
        await engine.dispose()
