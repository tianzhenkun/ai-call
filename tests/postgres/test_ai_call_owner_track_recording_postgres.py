from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
import pytest
from psycopg import ClientCursor
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import AiCallRecordingTrackModel, AiCallRecordModel
from app.core.base_model import MappedBase
from app.services.ai_call.runtime_control.command_repository import CommandClaim
from app.services.ai_call.runtime_control.customer_track import customer_track_keys
from app.services.ai_call.runtime_control.effect_repository import (
    EffectRegistrationError,
    RuntimeEffectRepository,
)
from app.services.ai_call.runtime_control.models import (
    AiCallRuntimeCommandModel,
    AiCallRuntimeEffectModel,
)
from app.services.ai_call.runtime_control.owner_repository import OwnerLease

pytestmark = pytest.mark.anyio

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATH = (
    PROJECT_ROOT
    / "docs/livekit-ai-outbound/sql/phase-i6-owner-runtime-customer-track-recording.sql"
)


def _dsn() -> str:
    value = os.getenv("AI_CALL_TEST_POSTGRES_DSN", "").strip()
    if not value:
        pytest.fail("AI_CALL_TEST_POSTGRES_DSN 未配置，必须通过隔离 PostgreSQL 脚本运行")
    return value


def _psycopg_dsn() -> str:
    return _dsn().replace("postgresql+asyncpg://", "postgresql://", 1)


def _execute(sql: str) -> None:
    with psycopg.connect(
        _psycopg_dsn(),
        autocommit=True,
        cursor_factory=ClientCursor,
    ) as connection:
        connection.execute(sql)


def _reset_legacy_track_schema(*, with_tenant_column: bool = False) -> None:
    tenant_column = "tenant_id varchar(20)," if with_tenant_column else ""
    _execute(
        f"""
        drop table if exists ai_call_recording_track cascade;
        drop table if exists ai_call_record cascade;

        create table ai_call_record (
            id bigint primary key,
            tenant_id varchar(20),
            call_id varchar(64) not null
        );

        create table ai_call_recording_track (
            id bigint primary key,
            {tenant_column}
            call_id varchar(64) not null,
            track_role varchar(32) not null,
            participant_identity varchar(128) not null,
            egress_id varchar(128),
            oss_id bigint,
            status varchar(32) not null,
            next_verify_at timestamptz
        );
        """
    )


async def test_track_migration_backfills_tenant_and_is_idempotent() -> None:
    _reset_legacy_track_schema()
    _execute(
        """
        insert into ai_call_record (id, tenant_id, call_id)
        values (1, 'tenant-a', 'call-a');

        insert into ai_call_recording_track (
            id, call_id, track_role, participant_identity, status
        ) values (11, 'call-a', 'customer', 'customer-a', 'recording');
        """
    )

    migration = MIGRATION_PATH.read_text(encoding="utf-8")
    _execute(migration)
    _execute(migration)

    with psycopg.connect(_psycopg_dsn(), cursor_factory=ClientCursor) as connection:
        tenant_id, generation = connection.execute(
            """
            select tenant_id, egress_generation
            from ai_call_recording_track
            where id = 11
            """
        ).fetchone()
        constraint_definition = connection.execute(
            """
            select pg_get_constraintdef(oid)
            from pg_constraint
            where conrelid = 'ai_call_recording_track'::regclass
              and conname = 'uk_ai_call_recording_track_tenant_participant'
            """
        ).fetchone()[0]
        indexes = {
            row[0]
            for row in connection.execute(
                """
                select indexname
                from pg_indexes
                where tablename = 'ai_call_recording_track'
                """
            ).fetchall()
        }

    assert tenant_id == "tenant-a"
    assert generation is None
    assert constraint_definition == (
        "UNIQUE (tenant_id, call_id, track_role, participant_identity)"
    )
    assert "idx_ai_call_recording_track_verify_due" in indexes


@pytest.mark.parametrize("failure_case", ["missing", "ambiguous", "wrong_tenant"])
async def test_track_migration_fails_closed_for_invalid_tenant_ownership(
    failure_case: str,
) -> None:
    _reset_legacy_track_schema(with_tenant_column=failure_case == "wrong_tenant")
    if failure_case == "missing":
        _execute(
            """
            insert into ai_call_recording_track (
                id, call_id, track_role, participant_identity, status
            ) values (11, 'call-a', 'customer', 'customer-a', 'recording');
            """
        )
    elif failure_case == "ambiguous":
        _execute(
            """
            insert into ai_call_record (id, tenant_id, call_id)
            values
                (1, 'tenant-a', 'call-a'),
                (2, 'tenant-b', 'call-a');
            insert into ai_call_recording_track (
                id, call_id, track_role, participant_identity, status
            ) values (11, 'call-a', 'customer', 'customer-a', 'recording');
            """
        )
    else:
        _execute(
            """
            insert into ai_call_record (id, tenant_id, call_id)
            values (1, 'tenant-a', 'call-a');
            insert into ai_call_recording_track (
                id, tenant_id, call_id, track_role, participant_identity, status
            ) values (
                11, 'tenant-b', 'call-a', 'customer', 'customer-a', 'recording'
            );
            """
        )

    with pytest.raises(
        psycopg.Error,
        match="ai_call_recording_track_tenant_backfill_failed",
    ):
        _execute(MIGRATION_PATH.read_text(encoding="utf-8"))


async def _reset_database() -> tuple[AsyncEngine, async_sessionmaker]:
    engine = create_async_engine(_dsn(), isolation_level="READ COMMITTED")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.drop_all)
        await connection.run_sync(MappedBase.metadata.create_all)
    return engine, async_sessionmaker(engine, expire_on_commit=False)


def _lease(now: datetime) -> OwnerLease:
    return OwnerLease(
        tenant_id="tenant-a",
        call_id="call-a",
        owner_id="runtime-a",
        fencing_token=7,
        lease_expires_at=now + timedelta(minutes=5),
        capacity_class="active",
    )


async def _seed_track_start(
    session_maker: async_sessionmaker,
    *,
    answered: bool,
    identity: str = "customer-a",
    resource_identity: str | None = None,
    terminal: bool = False,
    effect_status: str = "PENDING",
) -> tuple[OwnerLease, datetime]:
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(minutes=5)
    _, provider_key, resource_key = customer_track_keys(
        "call-a",
        resource_identity or identity,
    )
    async with session_maker.begin() as session:
        session.add(
            AiCallRecordModel(
                id=1,
                tenant_id="tenant-a",
                call_id="call-a",
                entry_type="web",
                room_name="room-a",
                participant_identity=identity,
                status="connected",
                started_at=now,
                answered_at=now if answered else None,
                terminal_requested_at=now if terminal else None,
                dialogue_persistence_status="pending",
                runtime_control_mode="owner_command_v1",
                runtime_owner_id="runtime-a",
                runtime_fencing_token=7,
                runtime_lease_expires_at=expires_at,
                runtime_capacity_class="active",
            )
        )
        session.add(
            AiCallRuntimeEffectModel(
                id=10,
                tenant_id="tenant-a",
                call_id="call-a",
                command_id=2,
                effect_type="START_TRACK_EGRESS",
                idempotency_key="start:call-a:track",
                fencing_token=7,
                status=effect_status,
                processing_owner_id="runtime-a" if effect_status == "APPLYING" else None,
                processing_fencing_token=7 if effect_status == "APPLYING" else None,
                processing_token="existing-token" if effect_status == "APPLYING" else None,
                processing_expires_at=expires_at if effect_status == "APPLYING" else None,
                provider_namespace="livekit:postgres-test",
                provider_idempotency_key=provider_key,
                resource_key=resource_key,
                resource_generation=1,
                attempt_count=1 if effect_status == "APPLYING" else 0,
                created_at=now,
                updated_at=now,
            )
        )
    return _lease(now), now


async def test_track_claim_requires_answered_and_matching_identity() -> None:
    engine, session_maker = await _reset_database()
    try:
        lease, _ = await _seed_track_start(session_maker, answered=False)
        async with session_maker.begin() as session:
            assert await RuntimeEffectRepository(session).claim_next(lease) is None

        async with session_maker.begin() as session:
            record = await session.scalar(select(AiCallRecordModel).with_for_update())
            assert record is not None
            record.answered_at = datetime.now(timezone.utc)

        async with session_maker.begin() as session:
            claim = await RuntimeEffectRepository(session).claim_next(lease)
            assert claim is not None
            assert claim.effect_type == "START_TRACK_EGRESS"
    finally:
        await engine.dispose()


async def test_track_claim_rejects_resource_digest_mismatch() -> None:
    engine, session_maker = await _reset_database()
    try:
        lease, _ = await _seed_track_start(
            session_maker,
            answered=True,
            identity="customer-a",
            resource_identity="customer-b",
        )
        async with session_maker.begin() as session:
            assert await RuntimeEffectRepository(session).claim_next(lease) is None
    finally:
        await engine.dispose()


async def _end_claim(now: datetime) -> CommandClaim:
    return CommandClaim(
        command_id=20,
        tenant_id="tenant-a",
        call_id="call-a",
        command_seq=2,
        command_type="END_CALL",
        processing_owner_id="runtime-a",
        processing_fencing_token=7,
        processing_token="end-token",
        processing_expires_at=now + timedelta(minutes=5),
        payload_json=None,
        attempt_count=1,
    )


async def _seed_end_command(
    session_maker: async_sessionmaker,
    *,
    now: datetime,
) -> None:
    async with session_maker.begin() as session:
        session.add(
            AiCallRuntimeCommandModel(
                id=20,
                tenant_id="tenant-a",
                call_id="call-a",
                command_seq=2,
                command_type="END_CALL",
                idempotency_key="end:call-a",
                request_fingerprint="fingerprint",
                status="PROCESSING",
                attempt_count=1,
                processing_owner_id="runtime-a",
                processing_fencing_token=7,
                processing_token="end-token",
                processing_expires_at=now + timedelta(minutes=5),
                created_at=now,
                updated_at=now,
            )
        )


@pytest.mark.parametrize(
    ("start_status", "expected_status"),
    [("PENDING", "FAILED"), ("APPLYING", "APPLYING")],
)
async def test_terminal_graph_handles_track_start_by_claim_state(
    start_status: str,
    expected_status: str,
) -> None:
    engine, session_maker = await _reset_database()
    try:
        _lease_value, now = await _seed_track_start(
            session_maker,
            answered=True,
            terminal=True,
            effect_status=start_status,
        )
        await _seed_end_command(session_maker, now=now)
        async with session_maker.begin() as session:
            snapshots = await RuntimeEffectRepository(session).register_end_graph(
                await _end_claim(now)
            )
            assert any(item.effect_type == "STOP_TRACK_EGRESS" for item in snapshots)

        async with session_maker() as session:
            start = await session.get(AiCallRuntimeEffectModel, 10)
            assert start is not None
            assert start.status == expected_status
            if expected_status == "FAILED":
                assert start.error_message == "no_resource"
                assert start.reconcile_after is None
            stop_count = await session.scalar(
                select(func.count())
                .select_from(AiCallRuntimeEffectModel)
                .where(AiCallRuntimeEffectModel.effect_type == "STOP_TRACK_EGRESS")
            )
            assert stop_count == 1
    finally:
        await engine.dispose()


async def test_terminal_graph_rolls_back_track_shortcut_when_stop_registration_fails(
) -> None:
    engine, session_maker = await _reset_database()
    try:
        _lease_value, now = await _seed_track_start(
            session_maker,
            answered=True,
            terminal=True,
        )
        await _seed_end_command(session_maker, now=now)
        async with session_maker.begin() as session:
            session.add(
                AiCallRuntimeEffectModel(
                    id=30,
                    tenant_id="tenant-a",
                    call_id="call-a",
                    command_id=20,
                    effect_type="STOP_TRACK_EGRESS",
                    idempotency_key="end:call-a:STOP_TRACK_EGRESS:10",
                    fencing_token=7,
                    status="PENDING",
                    provider_namespace="livekit:postgres-test",
                    provider_idempotency_key="conflicting-stop",
                    resource_key="wrong-resource",
                    resource_generation=1,
                    source_create_effect_id=10,
                    execution_phase=10,
                    created_at=now,
                    updated_at=now,
                )
            )

        with pytest.raises(EffectRegistrationError):
            async with session_maker.begin() as session:
                await RuntimeEffectRepository(session).register_end_graph(
                    await _end_claim(now)
                )

        async with session_maker() as session:
            start = await session.get(AiCallRuntimeEffectModel, 10)
            assert start is not None
            assert start.status == "PENDING"
            assert start.error_message is None
    finally:
        await engine.dispose()


async def _seed_verification_tracks(
    session_maker: async_sessionmaker,
    *,
    count: int,
) -> datetime:
    now = datetime.now(timezone.utc)
    async with session_maker.begin() as session:
        for index in range(1, count + 1):
            session.add(
                AiCallRecordingTrackModel(
                    id=100 + index,
                    tenant_id="tenant-a",
                    call_id=f"verify-call-{index}",
                    room_name=f"verify-room-{index}",
                    track_role="customer",
                    participant_identity=f"verify-customer-{index}",
                    status="verifying",
                    started_at=now - timedelta(minutes=1),
                    next_verify_at=now - timedelta(seconds=1),
                )
            )
    return now


async def test_track_verification_claim_skips_rows_locked_by_another_worker() -> None:
    engine, session_maker = await _reset_database()
    try:
        now = await _seed_verification_tracks(session_maker, count=2)
        async with session_maker.begin() as first_session:
            first_claims = await AiCallRecordRepository(
                first_session
            ).claim_due_recording_track_verifications(
                now=now,
                limit=1,
                claim_ttl=timedelta(minutes=2),
            )
            async with session_maker.begin() as second_session:
                second_claims = await AiCallRecordRepository(
                    second_session
                ).claim_due_recording_track_verifications(
                    now=now,
                    limit=1,
                    claim_ttl=timedelta(minutes=2),
                )

        assert len(first_claims) == 1
        assert len(second_claims) == 1
        assert first_claims[0].track_id != second_claims[0].track_id
    finally:
        await engine.dispose()


async def test_track_verification_rejects_wrong_token_and_cross_tenant_writes() -> None:
    engine, session_maker = await _reset_database()
    try:
        now = await _seed_verification_tracks(session_maker, count=1)
        async with session_maker.begin() as session:
            repository = AiCallRecordRepository(session)
            claims = await repository.claim_due_recording_track_verifications(
                now=now,
                limit=1,
                claim_ttl=timedelta(minutes=2),
            )
        assert len(claims) == 1
        claim = claims[0]

        async with session_maker.begin() as session:
            repository = AiCallRecordRepository(session)
            assert (
                await repository.lock_due_recording_track(
                    tenant_id="tenant-b",
                    track_id=claim.track_id,
                    claim_token=claim.claim_token,
                )
                is None
            )
            assert (
                await repository.lock_due_recording_track(
                    tenant_id="tenant-a",
                    track_id=claim.track_id,
                    claim_token=claim.claim_token + timedelta(seconds=1),
                )
                is None
            )
            assert not await repository.update_due_recording_track(
                tenant_id="tenant-b",
                track_id=claim.track_id,
                claim_token=claim.claim_token,
                status="completed",
            )
            assert not await repository.update_due_recording_track(
                tenant_id="tenant-a",
                track_id=claim.track_id,
                claim_token=claim.claim_token + timedelta(seconds=1),
                status="completed",
            )

        async with session_maker() as session:
            status = await session.scalar(
                select(AiCallRecordingTrackModel.status).where(
                    AiCallRecordingTrackModel.id == claim.track_id
                )
            )
        assert status == "verifying"
    finally:
        await engine.dispose()
