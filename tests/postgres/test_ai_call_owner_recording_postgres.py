from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from app.api.v1.ai_call.model import (
    AiCallRecordingModel,
    AiCallRecordModel,
)
from app.core.base_model import MappedBase
from app.services.ai_call.runtime_control.effect_repository import (
    EffectClaim,
    ProviderObservation,
    ProviderObservationKind,
    RuntimeEffectRepository,
)
from app.services.ai_call.runtime_control.models import (
    AiCallRuntimeEffectDependencyModel,
    AiCallRuntimeEffectModel,
    AiCallRuntimeWorkerModel,
)
from app.services.ai_call.runtime_control.owner_repository import OwnerLease

pytestmark = pytest.mark.anyio


def _dsn() -> str:
    value = os.getenv("AI_CALL_TEST_POSTGRES_DSN", "").strip()
    if not value:
        pytest.fail("AI_CALL_TEST_POSTGRES_DSN 未配置，必须通过隔离 PostgreSQL 脚本运行")
    return value


async def _reset_database() -> tuple[AsyncEngine, async_sessionmaker]:
    engine = create_async_engine(_dsn(), isolation_level="READ COMMITTED")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.drop_all)
        await connection.run_sync(MappedBase.metadata.create_all)
    return engine, async_sessionmaker(engine, expire_on_commit=False)


async def _seed_claim(
    session_maker: async_sessionmaker,
    *,
    tenant_id: str = "tenant-a",
    call_id: str = "call-a",
    record_owner_id: str = "runtime-a",
    record_fencing_token: int = 7,
    effect_owner_id: str = "runtime-a",
    effect_fencing_token: int = 7,
    processing_token: str = "token-a",
    effect_id: int = 10,
    record_id: int = 1,
) -> EffectClaim:
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(minutes=5)
    async with session_maker.begin() as session:
        session.add(
            AiCallRecordModel(
                id=record_id,
                tenant_id=tenant_id,
                call_id=call_id,
                entry_type="web",
                room_name=f"room-{tenant_id}-{call_id}",
                participant_identity=f"browser-{tenant_id}-{call_id}",
                status="connected",
                started_at=now,
                runtime_control_mode="owner_command_v1",
                runtime_owner_id=record_owner_id,
                runtime_fencing_token=record_fencing_token,
                runtime_lease_expires_at=expires_at,
            )
        )
        session.add(
            AiCallRuntimeEffectModel(
                id=effect_id,
                tenant_id=tenant_id,
                call_id=call_id,
                command_id=2,
                effect_type="START_EGRESS",
                idempotency_key=f"start:{call_id}:start-main-egress",
                fencing_token=effect_fencing_token,
                status="APPLYING",
                processing_token=processing_token,
                processing_expires_at=expires_at,
                provider_namespace="livekit:postgres-test",
                provider_idempotency_key=f"egress:main:{call_id}",
                resource_key=f"egress:main:{call_id}",
                resource_generation=1,
                processing_owner_id=effect_owner_id,
                processing_fencing_token=effect_fencing_token,
                created_at=now,
                updated_at=now,
            )
        )
    return EffectClaim(
        effect_id=effect_id,
        tenant_id=tenant_id,
        call_id=call_id,
        effect_type="START_EGRESS",
        processing_owner_id=effect_owner_id,
        processing_fencing_token=effect_fencing_token,
        processing_token=processing_token,
        processing_expires_at=expires_at,
        source_create_effect_id=None,
        create_protection_deadline_at=None,
        attempt_count=1,
        reconcile_only=False,
        provider_namespace="livekit:postgres-test",
        resource_key=f"egress:main:{call_id}",
    )


def _present() -> ProviderObservation:
    return ProviderObservation(
        kind=ProviderObservationKind.RESOURCE_PRESENT,
        provider_reference="EG_main",
        provider_status="EGRESS_ACTIVE",
        object_name="ai-call/main/call-a.ogg",
    )


async def test_effect_and_recording_projection_commit_atomically() -> None:
    engine, session_maker = await _reset_database()
    try:
        claim = await _seed_claim(session_maker)
        async with session_maker.begin() as session:
            assert await RuntimeEffectRepository(session).submit(claim, _present())

        async with session_maker() as session:
            effect_status = await session.scalar(
                select(AiCallRuntimeEffectModel.status).where(
                    AiCallRuntimeEffectModel.id == claim.effect_id
                )
            )
            recording = await session.scalar(select(AiCallRecordingModel))
            assert effect_status == "APPLIED"
            assert recording is not None
            assert recording.status == "recording"
            assert recording.egress_id == "EG_main"
            assert recording.egress_generation == 1
    finally:
        await engine.dispose()


async def test_projection_exception_rolls_back_effect_submission() -> None:
    class RaisingProjection:
        async def project(self, **_kwargs):
            raise RuntimeError("projection failed")

    engine, session_maker = await _reset_database()
    try:
        claim = await _seed_claim(session_maker)
        with pytest.raises(RuntimeError, match="projection failed"):
            async with session_maker.begin() as session:
                await RuntimeEffectRepository(
                    session,
                    recording_repository=RaisingProjection(),
                ).submit(claim, _present())

        async with session_maker() as session:
            effect = await session.get(AiCallRuntimeEffectModel, claim.effect_id)
            recording_count = await session.scalar(
                select(func.count()).select_from(AiCallRecordingModel)
            )
            assert effect is not None and effect.status == "APPLYING"
            assert effect.processing_token == claim.processing_token
            assert recording_count == 0
    finally:
        await engine.dispose()


async def test_stale_owner_late_submission_updates_no_recording() -> None:
    engine, session_maker = await _reset_database()
    try:
        claim = await _seed_claim(
            session_maker,
            record_owner_id="runtime-b",
            record_fencing_token=8,
        )
        async with session_maker.begin() as session:
            assert not await RuntimeEffectRepository(session).submit(claim, _present())

        async with session_maker() as session:
            recording_count = await session.scalar(
                select(func.count()).select_from(AiCallRecordingModel)
            )
            effect_status = await session.scalar(
                select(AiCallRuntimeEffectModel.status).where(
                    AiCallRuntimeEffectModel.id == claim.effect_id
                )
            )
            assert recording_count == 0
            assert effect_status == "APPLYING"
    finally:
        await engine.dispose()


async def test_wrong_processing_token_updates_no_recording() -> None:
    engine, session_maker = await _reset_database()
    try:
        claim = await _seed_claim(session_maker)
        wrong_claim = EffectClaim(
            effect_id=claim.effect_id,
            tenant_id=claim.tenant_id,
            call_id=claim.call_id,
            effect_type=claim.effect_type,
            processing_owner_id=claim.processing_owner_id,
            processing_fencing_token=claim.processing_fencing_token,
            processing_token="wrong-token",
            processing_expires_at=claim.processing_expires_at,
            source_create_effect_id=None,
            create_protection_deadline_at=None,
            attempt_count=claim.attempt_count,
            reconcile_only=claim.reconcile_only,
            provider_namespace=claim.provider_namespace,
            resource_key=claim.resource_key,
        )
        async with session_maker.begin() as session:
            assert not await RuntimeEffectRepository(session).submit(
                wrong_claim,
                _present(),
            )
        async with session_maker() as session:
            recording_count = await session.scalar(
                select(func.count()).select_from(AiCallRecordingModel)
            )
            effect_status = await session.scalar(
                select(AiCallRuntimeEffectModel.status).where(
                    AiCallRuntimeEffectModel.id == claim.effect_id
                )
            )
            assert recording_count == 0
            assert effect_status == "APPLYING"
    finally:
        await engine.dispose()


async def test_two_runtime_submissions_keep_one_recording_row() -> None:
    engine, session_maker = await _reset_database()
    try:
        claim = await _seed_claim(session_maker)

        async def submit_once() -> bool:
            async with session_maker.begin() as session:
                return await RuntimeEffectRepository(session).submit(claim, _present())

        results = await asyncio.gather(submit_once(), submit_once())
        async with session_maker() as session:
            recording_count = await session.scalar(
                select(func.count()).select_from(AiCallRecordingModel)
            )
            effect_status = await session.scalar(
                select(AiCallRuntimeEffectModel.status).where(
                    AiCallRuntimeEffectModel.id == claim.effect_id
                )
            )
            assert sorted(results) == [False, True]
            assert recording_count == 1
            assert effect_status == "APPLIED"
    finally:
        await engine.dispose()


async def test_recording_projection_is_invisible_to_other_tenant() -> None:
    engine, session_maker = await _reset_database()
    try:
        claim = await _seed_claim(session_maker)
        async with session_maker.begin() as session:
            assert await RuntimeEffectRepository(session).submit(claim, _present())

        async with session_maker() as session:
            effect_status = await session.scalar(
                select(AiCallRuntimeEffectModel.status).where(
                    AiCallRuntimeEffectModel.id == claim.effect_id
                )
            )
            tenant_a_count = await session.scalar(
                select(func.count())
                .select_from(AiCallRecordingModel)
                .where(
                    AiCallRecordingModel.tenant_id == "tenant-a",
                    AiCallRecordingModel.call_id == "call-a",
                )
            )
            tenant_b_count = await session.scalar(
                select(func.count())
                .select_from(AiCallRecordingModel)
                .where(
                    AiCallRecordingModel.tenant_id == "tenant-b",
                    AiCallRecordingModel.call_id == "call-a",
                )
            )
            assert effect_status == "APPLIED"
            assert tenant_a_count == 1
            assert tenant_b_count == 0
    finally:
        await engine.dispose()


async def test_stop_egress_gates_delete_room_but_not_long_oss_verification() -> None:
    engine, session_maker = await _reset_database()
    try:
        start_claim = await _seed_claim(session_maker)
        async with session_maker.begin() as session:
            assert await RuntimeEffectRepository(session).submit(start_claim, _present())

        now = datetime.now(timezone.utc)
        lease = OwnerLease(
            tenant_id="tenant-a",
            call_id="call-a",
            owner_id="runtime-a",
            fencing_token=7,
            lease_expires_at=now + timedelta(minutes=5),
            capacity_class="active",
        )
        async with session_maker.begin() as session:
            record = await session.scalar(select(AiCallRecordModel))
            assert record is not None
            record.status = "completed"
            record.terminal_requested_at = now
            record.runtime_capacity_class = "active"
            record.dialogue_persistence_status = "complete"
            record.dialogue_persistence_completed_at = now
            session.add(
                AiCallRuntimeWorkerModel(
                    worker_id="runtime-a",
                    status="READY",
                    capacity=1,
                    cleanup_capacity=1,
                    active_call_count=1,
                    active_cleanup_count=0,
                    heartbeat_at=now,
                    lease_expires_at=now + timedelta(minutes=5),
                    created_at=now,
                    updated_at=now,
                )
            )
            session.add_all(
                [
                    AiCallRuntimeEffectModel(
                        id=11,
                        tenant_id="tenant-a",
                        call_id="call-a",
                        command_id=3,
                        effect_type="STOP_EGRESS",
                        idempotency_key="end:call-a:STOP_EGRESS:10",
                        fencing_token=7,
                        status="PENDING",
                        provider_namespace="livekit:postgres-test",
                        provider_idempotency_key="destroy:egress-main-call-a",
                        resource_key="egress:main:call-a",
                        resource_generation=1,
                        source_create_effect_id=10,
                        execution_phase=10,
                        created_at=now,
                        updated_at=now,
                    ),
                    AiCallRuntimeEffectModel(
                        id=12,
                        tenant_id="tenant-a",
                        call_id="call-a",
                        command_id=2,
                        effect_type="CREATE_ROOM",
                        idempotency_key="start:call-a:create-room",
                        fencing_token=7,
                        status="APPLIED",
                        provider_namespace="livekit:postgres-test",
                        provider_idempotency_key="room:call-a",
                        resource_key="room:call-a:g1",
                        resource_generation=7,
                        provider_reference="room-a",
                        created_at=now,
                        updated_at=now,
                    ),
                    AiCallRuntimeEffectModel(
                        id=13,
                        tenant_id="tenant-a",
                        call_id="call-a",
                        command_id=3,
                        effect_type="DELETE_ROOM",
                        idempotency_key="end:call-a:DELETE_ROOM:12",
                        fencing_token=7,
                        status="PENDING",
                        provider_namespace="livekit:postgres-test",
                        provider_idempotency_key="destroy:room-call-a",
                        resource_key="room:call-a:g1",
                        resource_generation=7,
                        source_create_effect_id=12,
                        execution_phase=20,
                        created_at=now,
                        updated_at=now,
                    ),
                    AiCallRuntimeEffectDependencyModel(
                        id=14,
                        tenant_id="tenant-a",
                        effect_id=13,
                        prerequisite_effect_id=11,
                        required_status="APPLIED",
                        created_at=now,
                    ),
                ]
            )

        async with session_maker.begin() as session:
            effects = RuntimeEffectRepository(session)
            stop = await effects.claim_next(lease)
            assert stop is not None and stop.effect_type == "STOP_EGRESS"
            assert await effects.claim_next(lease) is None
            assert await effects.submit(
                stop,
                ProviderObservation(
                    kind=ProviderObservationKind.ACCEPTED,
                    provider_reference="EG_main",
                    provider_status="EGRESS_ACTIVE",
                    object_name="ai-call/main/call-a.ogg",
                ),
            )

            stop_reconcile = await effects.claim_next(lease)
            assert stop_reconcile is not None
            assert stop_reconcile.effect_type == "STOP_EGRESS"
            assert stop_reconcile.reconcile_only is True
            assert await effects.submit(
                stop_reconcile,
                ProviderObservation(
                    kind=ProviderObservationKind.TERMINAL_CONFIRMED,
                    provider_reference="EG_main",
                    provider_status="EGRESS_COMPLETE",
                    object_name="ai-call/main/call-a.ogg",
                    ended_at=now,
                    duration_ms=60_000,
                ),
            )
            recording = await session.scalar(select(AiCallRecordingModel))
            assert recording is not None
            assert recording.status == "verifying"
            assert recording.next_verify_at is not None
            assert recording.verify_deadline_at is not None

            delete_room = await effects.claim_next(lease)
            assert delete_room is not None and delete_room.effect_type == "DELETE_ROOM"
            assert await effects.submit(
                delete_room,
                ProviderObservation(kind=ProviderObservationKind.TERMINAL_CONFIRMED),
            )
            assert await effects.mark_cleanup_clean(lease)

        async with session_maker() as session:
            record = await session.scalar(select(AiCallRecordModel))
            recording = await session.scalar(select(AiCallRecordingModel))
            assert record is not None
            assert record.runtime_owner_id is None
            assert record.runtime_capacity_class == "none"
            assert record.resource_cleanup_status == "clean"
            assert recording is not None and recording.status == "verifying"
    finally:
        await engine.dispose()
