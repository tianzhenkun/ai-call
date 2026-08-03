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
from app.services.ai_call.runtime_control.models import AiCallRuntimeEffectModel

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
