from __future__ import annotations

import asyncio
import os
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from uuid import UUID

import psycopg
import pytest
from psycopg import ClientCursor
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.api.v1.ai_call.model import AiCallRecordModel
from app.api.v1.ai_call.outbound.sip_line_model import AiCallSipLineModel
from app.services.ai_call.runtime_control.command_repository import (
    CommandDecision,
    CommandIntent,
    EndCallIntent,
    IdempotencyConflictError,
    RuntimeCommandRepository,
    StartCallIntent,
    TerminalBarrierError,
)
from app.services.ai_call.runtime_control.dispatcher_service import (
    DispatcherControlService,
)
from app.services.ai_call.runtime_control.effect_repository import (
    EffectRegistrationError,
    EffectSpec,
    ProviderObservation,
    ProviderObservationKind,
    RuntimeEffectRepository,
)
from app.services.ai_call.runtime_control.handlers import EndCallHandler, StartCallHandler
from app.services.ai_call.runtime_control.models import (
    AiCallEndEvidenceModel,
    AiCallRuntimeCommandModel,
    AiCallRuntimeEffectDependencyModel,
    AiCallRuntimeEffectModel,
    AiCallRuntimeWorkerModel,
    AiCallSipLineReservationModel,
)
from app.services.ai_call.runtime_control.owner_repository import (
    DispatcherOwnerRepository,
    OwnerLease,
    RecoveryOwnerRepository,
    RuntimeOwnerRepository,
    WorkerRegistration,
    WorkerRegistryRepository,
)
from app.services.ai_call.runtime_control.provider_stub import (
    ScriptedProviderStub,
    StubObservationKind,
)
from app.services.ai_call.runtime_control.recovery_service import (
    RecoveryControlService,
)
from app.services.ai_call.runtime_control.runtime_service import (
    RuntimeControlService,
    RuntimeRegistry,
)
from app.services.ai_call.runtime_control.startup_recovery import (
    StartupReconcileService,
)
from app.services.ai_call.runtime_control.timing import read_database_time
from app.services.ai_call.runtime_control.types import CommandStatus

pytestmark = pytest.mark.anyio

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATH = (
    PROJECT_ROOT
    / "docs/livekit-ai-outbound/sql/phase-i1-owner-command-db-control-plane.sql"
)


def _async_dsn() -> str:
    dsn = os.getenv("AI_CALL_TEST_POSTGRES_DSN", "").strip()
    if not dsn:
        pytest.fail("AI_CALL_TEST_POSTGRES_DSN 未配置，必须通过隔离 PostgreSQL 脚本运行")
    return dsn


def _psycopg_dsn() -> str:
    return _async_dsn().replace("postgresql+asyncpg://", "postgresql://", 1)


def _execute_script(sql: str) -> None:
    with psycopg.connect(
        _psycopg_dsn(),
        autocommit=True,
        cursor_factory=ClientCursor,
    ) as connection:
        connection.execute(sql)


def _reset_legacy_schema() -> None:
    statements = (
        "drop table if exists ai_call_runtime_effect_dependency cascade",
        "drop table if exists ai_call_runtime_effect cascade",
        "drop table if exists ai_call_end_evidence cascade",
        "drop table if exists ai_call_runtime_command cascade",
        "drop table if exists ai_call_sip_line_reservation cascade",
        "drop table if exists ai_call_runtime_worker cascade",
        "drop table if exists ai_call_outbound_attempt cascade",
        "drop table if exists ai_call_record cascade",
        """
        create table ai_call_record (
            id bigint primary key,
            call_id varchar(64) not null unique,
            room_name varchar(128) not null
        )
        """,
        """
        create table ai_call_outbound_attempt (
            id bigint primary key,
            tenant_id varchar(64) not null,
            command_idempotency_key varchar(128),
            status varchar(32) not null
        )
        """,
    )
    with psycopg.connect(_psycopg_dsn(), autocommit=True) as connection:
        for statement in statements:
            connection.execute(statement)


async def test_postgres_major_and_isolation_are_frozen() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    try:
        async with engine.connect() as connection:
            isolation = await connection.scalar(text("show transaction_isolation"))
            version_num = await connection.scalar(
                text("select current_setting('server_version_num')::int")
            )
        assert isolation == "read committed"
        assert version_num is not None and version_num >= 160000
    finally:
        await engine.dispose()


async def test_migration_rejects_duplicate_room_names() -> None:
    _reset_legacy_schema()
    with psycopg.connect(_psycopg_dsn(), autocommit=True) as connection:
        connection.execute(
            """
            insert into ai_call_record (id, call_id, room_name)
            values (1, 'call-a', 'duplicate-room'), (2, 'call-b', 'duplicate-room')
            """
        )

    with pytest.raises(psycopg.Error, match="room_name"):
        _execute_script(MIGRATION_PATH.read_text(encoding="utf-8"))


async def test_migration_is_idempotent_and_uses_portable_contract_types() -> None:
    _reset_legacy_schema()
    migration_sql = MIGRATION_PATH.read_text(encoding="utf-8")
    _execute_script(migration_sql)
    _execute_script(migration_sql)

    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    try:
        async with engine.connect() as connection:
            table_count = await connection.scalar(
                text(
                    """
                    select count(*)
                    from information_schema.tables
                    where table_schema = current_schema()
                      and table_name in (
                        'ai_call_runtime_worker',
                        'ai_call_runtime_command',
                        'ai_call_end_evidence',
                        'ai_call_runtime_effect',
                        'ai_call_runtime_effect_dependency',
                        'ai_call_sip_line_reservation'
                      )
                    """
                )
            )
            room_unique = await connection.scalar(
                text(
                    """
                    select count(*)
                    from pg_constraint
                    where conname = 'uk_ai_call_record_room_name'
                      and conrelid = 'ai_call_record'::regclass
                    """
                )
            )
            timezone_column_count = await connection.scalar(
                text(
                    """
                    select count(*)
                    from information_schema.columns
                    where table_schema = current_schema()
                      and table_name = 'ai_call_runtime_command'
                      and column_name in ('created_at', 'processing_expires_at')
                      and data_type = 'timestamp with time zone'
                    """
                )
            )
            jsonb_column_count = await connection.scalar(
                text(
                    """
                    select count(*)
                    from information_schema.columns
                    where table_schema = current_schema()
                      and table_name like 'ai_call_runtime_%'
                      and data_type in ('json', 'jsonb')
                    """
                )
            )
            foreign_key_count = await connection.scalar(
                text(
                    """
                    select count(*)
                    from information_schema.table_constraints
                    where constraint_schema = current_schema()
                      and table_name in (
                        'ai_call_runtime_worker',
                        'ai_call_runtime_command',
                        'ai_call_end_evidence',
                        'ai_call_runtime_effect',
                        'ai_call_runtime_effect_dependency',
                        'ai_call_sip_line_reservation'
                      )
                      and constraint_type = 'FOREIGN KEY'
                    """
                )
            )
        assert table_count == 6
        assert room_unique == 1
        assert timezone_column_count == 2
        assert jsonb_column_count == 0
        assert foreign_key_count == 0
    finally:
        await engine.dispose()


async def _reset_repository_schema(engine) -> None:
    tables = (
        AiCallSipLineModel.__table__,
        AiCallRuntimeEffectDependencyModel.__table__,
        AiCallRuntimeEffectModel.__table__,
        AiCallEndEvidenceModel.__table__,
        AiCallRuntimeCommandModel.__table__,
        AiCallSipLineReservationModel.__table__,
        AiCallRuntimeWorkerModel.__table__,
        AiCallRecordModel.__table__,
    )
    async with engine.begin() as connection:
        for table in tables:
            await connection.run_sync(table.drop, checkfirst=True)
        for table in reversed(tables):
            await connection.run_sync(table.create)


async def _insert_sip_line(session, *, line_id: int, max_concurrency: int) -> None:
    now = await session.scalar(text("select clock_timestamp()"))
    await session.execute(
        text(
            """
            insert into ai_call_sip_line (
                id, tenant_id, line_code, line_name, enabled, adapter_type,
                route_mode, auth_mode, caller_number, destination_country,
                max_concurrency, originate_timeout_seconds, health_status,
                deleted, created_by, updated_by, created_at, updated_at
            ) values (
                :line_id, 'tenant-a', :line_code, :line_name, true, 'stub',
                'managed_trunk_id', 'managed_trunk', 'masked', 'CN',
                :max_concurrency, 45, 'READY', false, 1, 1, :now, :now
            )
            """
        ).bindparams(
            line_id=line_id,
            line_code=f"line-{line_id}",
            line_name=f"Line {line_id}",
            max_concurrency=max_concurrency,
            now=now,
        )
    )


async def test_command_idempotency_is_tenant_scoped_and_conflicts_are_atomic() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            repository = RuntimeCommandRepository(session)
            request = StartCallIntent(
                tenant_id="tenant-a",
                entry_type="web",
                idempotency_key="start:shared-key",
                payload={"business_id": "business-1"},
            )
            first = await repository.create_start_call(request)
            repeated = await repository.create_start_call(request)

            assert repeated == first

            with pytest.raises(IdempotencyConflictError):
                await repository.create_start_call(
                    StartCallIntent(
                        tenant_id="tenant-a",
                        entry_type="web",
                        idempotency_key="start:shared-key",
                        payload={"business_id": "business-2"},
                    )
                )

            other_tenant = await repository.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-b",
                    entry_type="web",
                    idempotency_key="start:shared-key",
                    payload={"business_id": "business-1"},
                )
            )
            assert other_tenant.call_id != first.call_id

        async with factory() as session:
            assert (
                await session.scalar(
                    text("select count(*) from ai_call_runtime_command")
                )
                == 2
            )
            assert await session.scalar(text("select count(*) from ai_call_record")) == 2
    finally:
        await engine.dispose()


async def test_committed_start_response_loss_retries_to_original_command() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    armed = False

    def fail_after_commit(_session) -> None:
        if armed:
            raise ConnectionError("injected committed response loss")

    event.listen(AsyncSession.sync_session_class, "after_commit", fail_after_commit)
    try:
        async with factory() as session:
            first = await RuntimeCommandRepository(session).create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:response-loss",
                    payload={"business_id": "response-loss"},
                )
            )
            armed = True
            with pytest.raises(ConnectionError, match="committed response loss"):
                await session.commit()
    finally:
        event.remove(AsyncSession.sync_session_class, "after_commit", fail_after_commit)

    async with factory.begin() as session:
        repeated = await RuntimeCommandRepository(session).create_start_call(
            StartCallIntent(
                tenant_id="tenant-a",
                entry_type="web",
                idempotency_key="start:response-loss",
                payload={"business_id": "response-loss"},
            )
        )
        assert repeated == first
        assert (
            await session.scalar(
                text(
                    "select count(*) from ai_call_record "
                    "where tenant_id='tenant-a'"
                )
            )
            == 1
        )
        assert (
            await session.scalar(
                text(
                    "select count(*) from ai_call_runtime_command "
                    "where tenant_id='tenant-a'"
                )
            )
            == 1
        )
    await engine.dispose()


async def test_concurrent_idempotency_race_keeps_only_the_committed_winner() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    both_ready = asyncio.Event()
    ready_count = 0
    ready_lock = asyncio.Lock()

    async def synchronized_clock(session):
        nonlocal ready_count
        now = await read_database_time(session)
        async with ready_lock:
            ready_count += 1
            if ready_count == 2:
                both_ready.set()
        await asyncio.wait_for(both_ready.wait(), timeout=3)
        return now

    async def create_once():
        async with factory.begin() as session:
            return await RuntimeCommandRepository(
                session,
                database_clock=synchronized_clock,
            ).create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:concurrent-winner",
                    payload={"business_id": "business-race"},
                )
            )

    try:
        first, second = await asyncio.gather(create_once(), create_once())
        assert first == second

        async with factory() as session:
            assert (
                await session.scalar(
                    text("select count(*) from ai_call_runtime_command")
                )
                == 1
            )
            assert await session.scalar(text("select count(*) from ai_call_record")) == 1
    finally:
        await engine.dispose()


async def test_command_sequence_and_terminal_barrier_are_monotonic() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            repository = RuntimeCommandRepository(session)
            start = await repository.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:sequence",
                    payload={"business_id": "business-sequence"},
                )
            )
            second = await repository.append_command(
                CommandIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    command_type="HANDOFF_ACCEPTED",
                    idempotency_key="handoff:sequence",
                    payload={"handoff_id": "handoff-1"},
                )
            )
            third = await repository.append_command(
                CommandIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    command_type="AGENT_MEDIA_READY",
                    idempotency_key="media:sequence",
                    payload={"track_sid": "track-1"},
                )
            )

            assert (start.command_seq, second.command_seq, third.command_seq) == (1, 2, 3)

            end = await repository.request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    source="agent",
                    end_reason="agent_hangup",
                    dedupe_key="agent:sequence:end",
                )
            )
            barrier = end.terminal_requested_at

            with pytest.raises(TerminalBarrierError):
                await repository.append_command(
                    CommandIntent(
                        tenant_id="tenant-a",
                        call_id=start.call_id,
                        command_type="CANCEL_HANDOFF",
                        idempotency_key="cancel:too-late",
                        payload={},
                    )
                )

            repeated_end = await repository.request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    source="timeout",
                    end_reason="timeout",
                    dedupe_key="timeout:sequence:end",
                )
            )
            assert repeated_end.command_id == end.command_id
            assert repeated_end.terminal_requested_at == barrier

        async with factory() as session:
            record = (
                await session.execute(
                    text(
                        "select status, terminal_requested_at, end_reason, "
                        "resource_cleanup_status from ai_call_record "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            assert tuple(record) == ("ending", barrier, "agent_hangup", "reconciling")
    finally:
        await engine.dispose()


async def test_end_evidence_is_multi_source_but_end_call_is_unique_and_preempts() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            repository = RuntimeCommandRepository(session)
            start = await repository.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="direct_sip",
                    idempotency_key="start:end-evidence",
                    payload={"phone_hash": "hash-1"},
                )
            )
            processing = await repository.append_command(
                CommandIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    command_type="HANDOFF_ACCEPTED",
                    idempotency_key="handoff:end-evidence",
                    payload={"handoff_id": "handoff-1"},
                )
            )
            await session.execute(
                text(
                    "update ai_call_runtime_command "
                    "set status='PROCESSING', processing_token='old-token', "
                    "processing_owner_id='runtime-1' where id=:command_id"
                ).bindparams(command_id=processing.command_id)
            )

            first = await repository.request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    source="customer_sip",
                    end_reason="customer_hangup",
                    dedupe_key="livekit:cluster-a:event-1",
                    provider="livekit",
                    provider_namespace="cluster-a",
                    provider_event_id="event-1",
                )
            )
            second = await repository.request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    source="agent",
                    end_reason="agent_hangup",
                    dedupe_key="agent:end-evidence:end",
                )
            )

            assert first.command_id == second.command_id

        async with factory() as session:
            end_commands = await session.scalar(
                text(
                    "select count(*) from ai_call_runtime_command "
                    "where command_type='END_CALL'"
                )
            )
            evidence_count = await session.scalar(
                text("select count(*) from ai_call_end_evidence")
            )
            old_command = (
                await session.execute(
                    text(
                        "select status, processing_token, cancel_requested_at, "
                        "preempted_by_command_id from ai_call_runtime_command "
                        "where id=:command_id"
                    ).bindparams(command_id=processing.command_id)
                )
            ).one()

            assert end_commands == 1
            assert evidence_count == 2
            assert old_command.status == CommandStatus.SUPERSEDED
            assert old_command.processing_token is None
            assert old_command.cancel_requested_at is not None
            assert old_command.preempted_by_command_id == first.command_id
    finally:
        await engine.dispose()


async def test_worker_registration_uses_database_time_and_stable_instance_identity() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            before = await session.scalar(text("select clock_timestamp()"))
            registry = WorkerRegistryRepository(
                session,
                lease_ttl=timedelta(seconds=30),
            )
            lease = await registry.register(
                WorkerRegistration(
                    deployment_instance_id="runtime-a",
                    startup_id=UUID("12345678-1234-5678-1234-567812345678"),
                    capacity=2,
                    cleanup_capacity=1,
                )
            )
            after = await session.scalar(text("select clock_timestamp()"))
            worker = (
                await session.execute(
                    text(
                        "select status, heartbeat_at, lease_expires_at "
                        "from ai_call_runtime_worker where worker_id=:worker_id"
                    ).bindparams(worker_id=lease.worker_id)
                )
            ).one()

            assert lease.worker_id == (
                "runtime-a:12345678-1234-5678-1234-567812345678"
            )
            assert worker.status == "READY"
            assert before <= worker.heartbeat_at <= after
            assert worker.lease_expires_at - worker.heartbeat_at == timedelta(seconds=30)
            assert await registry.heartbeat(lease) is True
    finally:
        await engine.dispose()


async def test_owner_dispatcher_assigns_once_and_runtime_only_renews_exact_lease() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            commands = RuntimeCommandRepository(session)
            first_call = await commands.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:owner-first",
                    payload={"business_id": "owner-first"},
                )
            )
            second_call = await commands.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:owner-second",
                    payload={"business_id": "owner-second"},
                )
            )
            worker = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-owner",
                    startup_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            dispatcher = DispatcherOwnerRepository(session)
            lease = await dispatcher.assign_initial_owner(
                "tenant-a", first_call.call_id
            )

            assert lease is not None
            assert lease.owner_id == worker.worker_id
            assert lease.fencing_token == 1
            assert lease.capacity_class == "active"
            assert (
                await dispatcher.assign_initial_owner("tenant-a", first_call.call_id)
                is None
            )
            assert (
                await dispatcher.assign_initial_owner("tenant-a", second_call.call_id)
                is None
            )

            runtime = RuntimeOwnerRepository(session)
            assert await runtime.validate(lease) is True
            wrong_fencing = OwnerLease(
                tenant_id=lease.tenant_id,
                call_id=lease.call_id,
                owner_id=lease.owner_id,
                fencing_token=lease.fencing_token + 1,
                lease_expires_at=lease.lease_expires_at,
                capacity_class=lease.capacity_class,
            )
            assert await runtime.renew(wrong_fencing) is None
            unowned = OwnerLease(
                tenant_id="tenant-a",
                call_id=second_call.call_id,
                owner_id=lease.owner_id,
                fencing_token=0,
                lease_expires_at=lease.lease_expires_at,
                capacity_class="none",
            )
            assert await runtime.renew(unowned) is None

            renewed = await runtime.renew(lease)
            assert renewed is not None
            assert renewed.lease_expires_at > lease.lease_expires_at
            assert await runtime.renew(lease) is None
            await session.execute(
                text(
                    "update ai_call_record set runtime_lease_expires_at="
                    "clock_timestamp() - interval '1 second' where call_id=:call_id"
                ).bindparams(call_id=first_call.call_id)
            )
            assert await runtime.renew(renewed) is None

            counts = (
                await session.execute(
                    text(
                        "select active_call_count, active_cleanup_count "
                        "from ai_call_runtime_worker where worker_id=:worker_id"
                    ).bindparams(worker_id=worker.worker_id)
                )
            ).one()
            assert tuple(counts) == (1, 0)

            recovery = RecoveryOwnerRepository(session)
            assert (
                await recovery.assign_cleanup_owner("tenant-a", second_call.call_id)
                is None
            )
    finally:
        await engine.dispose()


async def test_dispatcher_atomically_reserves_runtime_and_sip_line_capacity() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            await _insert_sip_line(session, line_id=701, max_concurrency=1)
            start = await RuntimeCommandRepository(session).create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="direct_sip",
                    idempotency_key="start:dual-resource",
                    payload={"line_id": 701, "phone_hash": "hash-dual-resource"},
                )
            )
            worker = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-dual-resource",
                    startup_id=UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )

            lease = await DispatcherOwnerRepository(session).assign_initial_owner(
                "tenant-a", start.call_id
            )

            assert lease is not None
            assert lease.owner_id == worker.worker_id
            reservation = (
                await session.execute(
                    text(
                        "select status, line_id, fencing_token, reservation_token "
                        "from ai_call_sip_line_reservation where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            assert reservation.status == "RESERVED"
            assert reservation.line_id == 701
            assert reservation.fencing_token == lease.fencing_token
            assert reservation.reservation_token

            counts = (
                await session.execute(
                    text(
                        "select active_call_count from ai_call_runtime_worker "
                        "where worker_id=:worker_id"
                    ).bindparams(worker_id=worker.worker_id)
                )
            ).one()
            assert counts.active_call_count == 1
    finally:
        await engine.dispose()


async def test_concurrent_dispatchers_cannot_split_the_last_sip_line_slot() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            await _insert_sip_line(session, line_id=702, max_concurrency=1)
            commands = RuntimeCommandRepository(session)
            first = await commands.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="direct_sip",
                    idempotency_key="start:dual-race-1",
                    payload={"line_id": 702, "phone_hash": "hash-dual-race-1"},
                )
            )
            second = await commands.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="direct_sip",
                    idempotency_key="start:dual-race-2",
                    payload={"line_id": 702, "phone_hash": "hash-dual-race-2"},
                )
            )
            first_worker = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-dual-race-a",
                    startup_id=UUID("f1111111-1111-4111-8111-111111111111"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            second_worker = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-dual-race-b",
                    startup_id=UUID("f2222222-2222-4222-8222-222222222222"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )

        async def assign_once(call_id: str):
            async with factory.begin() as session:
                return await DispatcherOwnerRepository(session).assign_initial_owner(
                    "tenant-a", call_id
                )

        leases = await asyncio.gather(
            assign_once(first.call_id),
            assign_once(second.call_id),
        )
        assert sum(lease is not None for lease in leases) == 1

        async with factory() as session:
            assert (
                await session.scalar(
                    text(
                        "select count(*) from ai_call_sip_line_reservation "
                        "where line_id=702 and status <> 'RELEASED'"
                    )
                )
                == 1
            )
            active_counts = {
                row.worker_id: row.active_call_count
                for row in (
                    await session.execute(
                        text(
                            "select worker_id, active_call_count "
                            "from ai_call_runtime_worker where worker_id in "
                            "(:first_worker, :second_worker)"
                        ).bindparams(
                            first_worker=first_worker.worker_id,
                            second_worker=second_worker.worker_id,
                        )
                    )
                ).all()
            }
            assert sum(active_counts.values()) == 1
    finally:
        await engine.dispose()


async def test_sip_reservation_follows_effect_lifecycle_and_rejects_stale_token() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            await _insert_sip_line(session, line_id=703, max_concurrency=1)
            commands = RuntimeCommandRepository(session)
            effects = RuntimeEffectRepository(session)
            start = await commands.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="direct_sip",
                    idempotency_key="start:reservation-lifecycle",
                    payload={"line_id": 703, "phone_hash": "hash-reservation-lifecycle"},
                )
            )
            await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-reservation-lifecycle",
                    startup_id=UUID("a3333333-3333-4333-8333-333333333333"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            lease = await DispatcherOwnerRepository(session).assign_initial_owner(
                "tenant-a", start.call_id
            )
            assert lease is not None
            start_claim = await commands.claim_next_for_owner(lease)
            assert start_claim is not None
            sip_effect = await effects.register(
                start_claim,
                EffectSpec(
                    effect_type="CREATE_SIP_PARTICIPANT",
                    idempotency_key="effect:create-sip:reservation-lifecycle",
                    provider_namespace="stub:reservation-lifecycle",
                    provider_idempotency_key="provider:create-sip:reservation-lifecycle",
                    resource_key="sip:reservation-lifecycle:g1",
                    resource_generation=lease.fencing_token,
                ),
            )
            assert await commands.complete(
                start_claim,
                CommandDecision(status=CommandStatus.SUCCEEDED),
            )

            create_claim = await effects.claim_next(lease)
            assert create_claim is not None
            assert create_claim.reservation_token
            reservation_token = create_claim.reservation_token
            assert await effects.submit(
                create_claim,
                ProviderObservation(
                    kind=ProviderObservationKind.RESOURCE_PRESENT,
                    provider_reference="sip-ref",
                ),
            )
            assert (
                await session.scalar(
                    text(
                        "select status from ai_call_sip_line_reservation "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
                == "ACTIVE"
            )

            end = await commands.request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    source="customer_sip",
                    end_reason="customer_hangup",
                    dedupe_key="customer:reservation-lifecycle:end",
                )
            )
            end_claim = await commands.claim_pending_end(lease)
            assert end_claim is not None and end_claim.command_id == end.command_id
            graph = await effects.register_end_graph(end_claim)
            assert len(graph) == 1 and graph[0].effect_type == "HANGUP_SIP"
            assert await commands.complete(
                end_claim,
                CommandDecision(status=CommandStatus.SUCCEEDED),
            )

            hangup_claim = await effects.claim_next(lease)
            assert hangup_claim is not None
            assert hangup_claim.reservation_token == reservation_token
            await session.execute(
                text(
                    "update ai_call_sip_line_reservation set "
                    "reservation_token='stale-reservation-token' where call_id=:call_id"
                ).bindparams(call_id=start.call_id)
            )
            assert await effects.submit(
                hangup_claim,
                ProviderObservation(kind=ProviderObservationKind.TERMINAL_CONFIRMED),
            ) is False
            await session.execute(
                text(
                    "update ai_call_sip_line_reservation set "
                    "reservation_token=:reservation_token where call_id=:call_id"
                ).bindparams(
                    reservation_token=reservation_token,
                    call_id=start.call_id,
                )
            )
            assert await effects.submit(
                hangup_claim,
                ProviderObservation(kind=ProviderObservationKind.TERMINAL_CONFIRMED),
            )
            assert (
                await session.scalar(
                    text(
                        "select status from ai_call_sip_line_reservation "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
                == "RELEASED"
            )
            assert await effects.mark_cleanup_clean(lease)
            assert (
                await session.scalar(
                    text(
                        "select resource_cleanup_status from ai_call_record "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
                == "clean"
            )
            assert sip_effect.effect_id > 0
    finally:
        await engine.dispose()


async def test_reservation_insert_failure_rolls_back_owner_and_worker_capacity() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            await _insert_sip_line(session, line_id=704, max_concurrency=1)
            start = await RuntimeCommandRepository(session).create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="direct_sip",
                    idempotency_key="start:reservation-fault",
                    payload={"line_id": 704, "phone_hash": "hash-reservation-fault"},
                )
            )
            worker = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-reservation-fault",
                    startup_id=UUID("a4444444-4444-4444-8444-444444444444"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            await session.execute(
                text(
                    """
                    create or replace function ai_call_test_fail_reservation_insert()
                    returns trigger language plpgsql as $$
                    begin
                        raise exception 'injected reservation failure';
                    end;
                    $$
                    """
                )
            )
            await session.execute(
                text(
                    """
                    create trigger ai_call_test_fail_reservation_insert_trigger
                    before insert on ai_call_sip_line_reservation
                    for each row execute function ai_call_test_fail_reservation_insert()
                    """
                )
            )

        with pytest.raises(Exception, match="injected reservation failure"):
            async with factory.begin() as session:
                await DispatcherOwnerRepository(session).assign_initial_owner(
                    "tenant-a", start.call_id
                )

        async with factory() as session:
            record = (
                await session.execute(
                    text(
                        "select runtime_owner_id, runtime_fencing_token, "
                        "runtime_capacity_class from ai_call_record "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            counts = (
                await session.execute(
                    text(
                        "select active_call_count from ai_call_runtime_worker "
                        "where worker_id=:worker_id"
                    ).bindparams(worker_id=worker.worker_id)
                )
            ).one()
            assert tuple(record) == (None, 0, "none")
            assert counts.active_call_count == 0
            assert (
                await session.scalar(
                    text(
                        "select count(*) from ai_call_sip_line_reservation "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
                == 0
            )
    finally:
        await engine.dispose()


async def test_owner_concurrent_dispatchers_only_consume_one_capacity_slot() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            start = await RuntimeCommandRepository(session).create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:owner-race",
                    payload={"business_id": "owner-race"},
                )
            )
            worker = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-race",
                    startup_id=UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
                    capacity=2,
                    cleanup_capacity=1,
                )
            )

        async def assign_once():
            async with factory.begin() as session:
                return await DispatcherOwnerRepository(
                    session
                ).assign_initial_owner("tenant-a", start.call_id)

        first, second = await asyncio.gather(assign_once(), assign_once())
        assert sum(lease is not None for lease in (first, second)) == 1

        async with factory() as session:
            record = (
                await session.execute(
                    text(
                        "select runtime_owner_id, runtime_fencing_token, "
                        "runtime_capacity_class from ai_call_record where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            active_count = await session.scalar(
                text(
                    "select active_call_count from ai_call_runtime_worker "
                    "where worker_id=:worker_id"
                ).bindparams(worker_id=worker.worker_id)
            )
            assert tuple(record) == (worker.worker_id, 1, "active")
            assert active_count == 1
    finally:
        await engine.dispose()


async def test_concurrent_recovery_scans_assign_expired_cleanup_owner_once() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            commands = RuntimeCommandRepository(session)
            start = await commands.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="direct_sip",
                    idempotency_key="start:recovery-race",
                    payload={"phone_hash": "hash-recovery-race"},
                )
            )
            old_worker = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-recovery-old",
                    startup_id=UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            old_lease = await DispatcherOwnerRepository(session).assign_initial_owner(
                "tenant-a", start.call_id
            )
            assert old_lease is not None
            await commands.request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    source="recovery_test",
                    end_reason="owner_lost",
                    dedupe_key="recovery-test:owner-lost",
                )
            )
            await session.execute(
                text(
                    "update ai_call_record set runtime_lease_expires_at="
                    "clock_timestamp() - interval '1 second' where call_id=:call_id"
                ).bindparams(call_id=start.call_id)
            )
            await session.execute(
                text(
                    "update ai_call_runtime_worker set lease_expires_at="
                    "clock_timestamp() - interval '1 second' where worker_id=:worker_id"
                ).bindparams(worker_id=old_worker.worker_id)
            )
            new_worker = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-recovery-new",
                    startup_id=UUID("ffffffff-ffff-4fff-8fff-ffffffffffff"),
                    capacity=0,
                    cleanup_capacity=1,
                )
            )

        first, second = await asyncio.gather(
            RecoveryControlService(factory).run_once(),
            RecoveryControlService(factory).run_once(),
        )
        assert first + second == 1

        async with factory() as session:
            record = (
                await session.execute(
                    text(
                        "select runtime_owner_id, runtime_fencing_token, "
                        "runtime_capacity_class from ai_call_record "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            assert tuple(record) == (new_worker.worker_id, 2, "cleanup")
            counts = (
                await session.execute(
                    text(
                        "select worker_id, active_call_count, active_cleanup_count "
                        "from ai_call_runtime_worker order by worker_id"
                    )
                )
            ).all()
            by_worker = {row.worker_id: row for row in counts}
            assert by_worker[old_worker.worker_id].active_call_count == 0
            assert by_worker[new_worker.worker_id].active_cleanup_count == 1
    finally:
        await engine.dispose()


async def test_startup_uncertain_deadline_marks_no_resource_failed_and_releases_owner() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            commands = RuntimeCommandRepository(session)
            start = await commands.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:startup-uncertain-no-resource",
                    payload={"business_id": "startup-uncertain-no-resource"},
                )
            )
            worker = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-startup-uncertain",
                    startup_id=UUID("11111111-1111-4111-8111-111111111111"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            lease = await DispatcherOwnerRepository(session).assign_initial_owner(
                "tenant-a", start.call_id
            )
            assert lease is not None
            now = await read_database_time(session)
            session.add(
                AiCallRuntimeEffectModel(
                    id=93001,
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    command_id=start.command_id,
                    effect_type="CREATE_ROOM",
                    idempotency_key="effect:startup-uncertain-no-resource",
                    fencing_token=lease.fencing_token,
                    status="FAILED",
                    provider_namespace="stub:startup-uncertain",
                    provider_idempotency_key="room:startup-uncertain",
                    resource_key="room:startup-uncertain",
                    resource_generation=lease.fencing_token,
                    error_message="no_resource",
                    created_at=now,
                    updated_at=now,
                )
            )
            await session.flush()
            await session.execute(
                text(
                    "update ai_call_record set "
                    "startup_reconcile_deadline_at=clock_timestamp() - interval '1 second' "
                    "where call_id=:call_id"
                ).bindparams(call_id=start.call_id)
            )

        assert await StartupReconcileService(factory).run_once() == 1

        async with factory() as session:
            command = (
                await session.execute(
                    text(
                        "select status, error_message from ai_call_runtime_command "
                        "where id=:command_id"
                    ).bindparams(command_id=start.command_id)
                )
            ).one()
            assert tuple(command) == ("DEAD", "START_NOT_CREATED")
            record = (
                await session.execute(
                    text(
                        "select status, failure_stage, failure_message, "
                        "runtime_owner_id, runtime_capacity_class, "
                        "resource_cleanup_status, resource_cleanup_completed_at "
                        "from ai_call_record where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            assert tuple(record) == (
                "failed",
                "startup_reconcile",
                "START_NOT_CREATED",
                None,
                "none",
                "clean",
                record.resource_cleanup_completed_at,
            )
            assert record.resource_cleanup_completed_at is not None
            assert (
                await session.scalar(
                    text(
                        "select active_call_count from ai_call_runtime_worker "
                        "where worker_id=:worker_id"
                    ).bindparams(worker_id=worker.worker_id)
                )
                == 0
            )
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    ("effect_status", "effect_error", "decision"),
    [
        ("APPLIED", None, "resource_present"),
        ("RECONCILE_REQUIRED", "provider timeout", "unknown"),
    ],
)
async def test_startup_uncertain_deadline_establishes_end_for_present_or_unknown(
    effect_status: str,
    effect_error: str | None,
    decision: str,
) -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            commands = RuntimeCommandRepository(session)
            start = await commands.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key=f"start:startup-uncertain-{decision}",
                    payload={"business_id": f"startup-uncertain-{decision}"},
                )
            )
            worker = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-startup-end",
                    startup_id=UUID("22222222-2222-4222-8222-222222222222"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            lease = await DispatcherOwnerRepository(session).assign_initial_owner(
                "tenant-a", start.call_id
            )
            assert lease is not None
            now = await read_database_time(session)
            session.add(
                AiCallRuntimeEffectModel(
                    id=93002,
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    command_id=start.command_id,
                    effect_type="CREATE_ROOM",
                    idempotency_key=f"effect:startup-uncertain-{decision}",
                    fencing_token=lease.fencing_token,
                    status=effect_status,
                    provider_namespace="stub:startup-uncertain",
                    provider_idempotency_key=f"room:startup-uncertain-{decision}",
                    resource_key=f"room:startup-uncertain-{decision}",
                    resource_generation=lease.fencing_token,
                    error_message=effect_error,
                    created_at=now,
                    updated_at=now,
                )
            )
            await session.flush()
            await session.execute(
                text(
                    "update ai_call_record set "
                    "startup_reconcile_deadline_at=clock_timestamp() - interval '1 second' "
                    "where call_id=:call_id"
                ).bindparams(call_id=start.call_id)
            )

        assert await StartupReconcileService(factory).run_once() == 1

        async with factory() as session:
            commands = (
                await session.execute(
                    text(
                        "select command_type, status from ai_call_runtime_command "
                        "where call_id=:call_id order by command_seq"
                    ).bindparams(call_id=start.call_id)
                )
            ).all()
            assert commands == [("START_CALL", "SUPERSEDED"), ("END_CALL", "PENDING")]
            record = (
                await session.execute(
                    text(
                        "select status, terminal_requested_at, runtime_owner_id, "
                        "failure_stage, failure_message from ai_call_record "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            assert record.status == "ending"
            assert record.terminal_requested_at is not None
            assert record.runtime_owner_id == worker.worker_id
            assert record.failure_stage == "startup_reconcile"
            assert record.failure_message == f"START_UNCERTAIN:{decision}"
    finally:
        await engine.dispose()


async def test_startup_uncertain_reservation_blocks_no_resource_failure() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            commands = RuntimeCommandRepository(session)
            start = await commands.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="direct_sip",
                    idempotency_key="start:startup-uncertain-reservation",
                    payload={"phone_hash": "hash-startup-uncertain-reservation"},
                )
            )
            worker = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-startup-reservation",
                    startup_id=UUID("33333333-3333-4333-8333-333333333333"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            lease = await DispatcherOwnerRepository(session).assign_initial_owner(
                "tenant-a", start.call_id
            )
            assert lease is not None
            now = await read_database_time(session)
            session.add(
                AiCallRuntimeEffectModel(
                    id=93003,
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    command_id=start.command_id,
                    effect_type="CREATE_ROOM",
                    idempotency_key="effect:startup-uncertain-reservation",
                    fencing_token=lease.fencing_token,
                    status="FAILED",
                    provider_namespace="stub:startup-uncertain",
                    provider_idempotency_key="room:startup-uncertain-reservation",
                    resource_key="room:startup-uncertain-reservation",
                    resource_generation=lease.fencing_token,
                    error_message="no_resource",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.add(
                AiCallSipLineReservationModel(
                    id=93004,
                    tenant_id="tenant-a",
                    line_id=301,
                    call_id=start.call_id,
                    status="RECONCILE_REQUIRED",
                    reservation_token="reservation:startup-uncertain",
                    fencing_token=lease.fencing_token,
                    acquired_at=now,
                    created_at=now,
                    updated_at=now,
                )
            )
            await session.flush()
            await session.execute(
                text(
                    "update ai_call_record set "
                    "startup_reconcile_deadline_at=clock_timestamp() - interval '1 second' "
                    "where call_id=:call_id"
                ).bindparams(call_id=start.call_id)
            )

        assert await StartupReconcileService(factory).run_once() == 1

        async with factory() as session:
            commands = (
                await session.execute(
                    text(
                        "select command_type, status from ai_call_runtime_command "
                        "where call_id=:call_id order by command_seq"
                    ).bindparams(call_id=start.call_id)
                )
            ).all()
            assert commands == [("START_CALL", "SUPERSEDED"), ("END_CALL", "PENDING")]
            record = (
                await session.execute(
                    text(
                        "select status, resource_cleanup_status, runtime_owner_id "
                        "from ai_call_record where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            assert tuple(record) == ("ending", "reconciling", worker.worker_id)
            assert (
                await session.scalar(
                    text(
                        "select status from ai_call_sip_line_reservation "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
                == "RECONCILE_REQUIRED"
            )
    finally:
        await engine.dispose()


async def test_owner_recovery_parks_attention_without_releasing_resources() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            commands = RuntimeCommandRepository(session)
            start = await commands.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="direct_sip",
                    idempotency_key="start:owner-recovery",
                    payload={"phone_hash": "hash-owner-recovery"},
                )
            )
            old_worker = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-old",
                    startup_id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            active_lease = await DispatcherOwnerRepository(
                session
            ).assign_initial_owner("tenant-a", start.call_id)
            assert active_lease is not None
            end = await commands.request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    source="runtime_recovery",
                    end_reason="owner_lost",
                    dedupe_key="runtime-recovery:owner-lost",
                )
            )
            now = await read_database_time(session)
            session.add(
                AiCallRuntimeEffectModel(
                    id=91001,
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    command_id=end.command_id,
                    effect_type="DELETE_SIP_PARTICIPANT",
                    idempotency_key="effect:owner-recovery",
                    fencing_token=active_lease.fencing_token,
                    status="RECONCILE_REQUIRED",
                    provider_namespace="stub:owner-recovery",
                    provider_idempotency_key="provider:owner-recovery",
                    resource_key="sip:owner-recovery",
                    resource_generation=active_lease.fencing_token,
                    created_at=now,
                    updated_at=now,
                )
            )
            session.add(
                AiCallSipLineReservationModel(
                    id=92001,
                    tenant_id="tenant-a",
                    line_id=101,
                    call_id=start.call_id,
                    status="RECONCILE_REQUIRED",
                    reservation_token="reservation-owner-recovery",
                    fencing_token=active_lease.fencing_token,
                    acquired_at=now,
                    created_at=now,
                    updated_at=now,
                )
            )
            await session.flush()
            await session.execute(
                text(
                    "update ai_call_record set runtime_lease_expires_at="
                    "clock_timestamp() - interval '1 second' where call_id=:call_id"
                ).bindparams(call_id=start.call_id)
            )
            await session.execute(
                text(
                    "update ai_call_runtime_worker set lease_expires_at="
                    "clock_timestamp() - interval '1 second' where worker_id=:worker_id"
                ).bindparams(worker_id=old_worker.worker_id)
            )
            new_worker = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-new",
                    startup_id=UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
                    capacity=0,
                    cleanup_capacity=1,
                )
            )

            recovery = RecoveryOwnerRepository(session)
            cleanup_lease = await recovery.assign_cleanup_owner(
                "tenant-a", start.call_id
            )
            assert cleanup_lease is not None
            assert cleanup_lease.owner_id == new_worker.worker_id
            assert cleanup_lease.fencing_token == 2
            assert cleanup_lease.capacity_class == "cleanup"
            assert (
                await session.scalar(
                    text(
                        "select fencing_token from ai_call_sip_line_reservation "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
                == 2
            )
            await session.execute(
                text(
                    "update ai_call_runtime_effect set status='APPLYING', "
                    "processing_owner_id=:owner_id, processing_token='in-flight', "
                    "processing_expires_at=clock_timestamp() + interval '10 seconds' "
                    "where id=91001"
                ).bindparams(owner_id=new_worker.worker_id)
            )
            assert await recovery.park_attention(
                cleanup_lease, timedelta(seconds=30)
            ) is False
            await session.execute(
                text(
                    "update ai_call_runtime_effect set status='RECONCILE_REQUIRED', "
                    "processing_owner_id=null, processing_token=null, "
                    "processing_expires_at=null where id=91001"
                )
            )
            assert await recovery.park_attention(
                cleanup_lease, timedelta(seconds=30)
            )

            parked = (
                await session.execute(
                    text(
                        "select runtime_owner_id, runtime_capacity_class, "
                        "resource_cleanup_status, resource_cleanup_next_retry_at "
                        "from ai_call_record where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            assert parked.runtime_owner_id is None
            assert parked.runtime_capacity_class == "attention"
            assert parked.resource_cleanup_status == "attention_required"
            assert parked.resource_cleanup_next_retry_at is not None
            assert (
                await session.scalar(
                    text(
                        "select status from ai_call_sip_line_reservation "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
                == "RECONCILE_REQUIRED"
            )
            assert (
                await session.scalar(
                    text(
                        "select status from ai_call_runtime_effect where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
                == "RECONCILE_REQUIRED"
            )
            assert (
                await recovery.assign_cleanup_owner("tenant-a", start.call_id)
                is None
            )

            await session.execute(
                text(
                    "update ai_call_record set resource_cleanup_next_retry_at="
                    "clock_timestamp() - interval '1 second' where call_id=:call_id"
                ).bindparams(call_id=start.call_id)
            )
            reassigned = await recovery.assign_cleanup_owner(
                "tenant-a", start.call_id
            )
            assert reassigned is not None
            assert reassigned.fencing_token == 3
            assert (
                await session.scalar(
                    text(
                        "select fencing_token from ai_call_sip_line_reservation "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
                == 3
            )

            counts = (
                await session.execute(
                    text(
                        "select worker_id, active_call_count, active_cleanup_count "
                        "from ai_call_runtime_worker order by worker_id"
                    )
                )
            ).all()
            by_worker = {row.worker_id: row for row in counts}
            assert by_worker[old_worker.worker_id].active_call_count == 0
            assert by_worker[new_worker.worker_id].active_call_count == 0
            assert by_worker[new_worker.worker_id].active_cleanup_count == 1
    finally:
        await engine.dispose()


async def test_command_order_and_command_claim_completion_are_fenced() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            repository = RuntimeCommandRepository(session)
            start = await repository.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:command-order",
                    payload={"business_id": "command-order"},
                )
            )
            await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-command-order",
                    startup_id=UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            lease = await DispatcherOwnerRepository(
                session
            ).assign_initial_owner("tenant-a", start.call_id)
            assert lease is not None

            start_claim = await repository.claim_next_for_owner(lease)
            assert start_claim is not None
            assert start_claim.command_seq == 1
            assert start_claim.attempt_count == 1
            assert await repository.complete(
                start_claim,
                CommandDecision(
                    status=CommandStatus.SUCCEEDED,
                    result={"started": True},
                ),
            )

            second = await repository.append_command(
                CommandIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    command_type="HANDOFF_ACCEPTED",
                    idempotency_key="handoff:command-order",
                    payload={"handoff_id": "handoff-order"},
                )
            )
            third = await repository.append_command(
                CommandIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    command_type="AGENT_MEDIA_READY",
                    idempotency_key="media:command-order",
                    payload={"track_sid": "track-order"},
                )
            )
            await session.execute(
                text(
                    "update ai_call_runtime_command set status='RETRY_WAIT', "
                    "next_retry_at=clock_timestamp() + interval '30 seconds' "
                    "where id=:command_id"
                ).bindparams(command_id=second.command_id)
            )
            assert await repository.claim_next_for_owner(lease) is None
            await session.execute(
                text(
                    "update ai_call_runtime_command set next_retry_at="
                    "clock_timestamp() - interval '1 second' where id=:command_id"
                ).bindparams(command_id=second.command_id)
            )

        async def claim_once():
            async with factory.begin() as session:
                return await RuntimeCommandRepository(session).claim_next_for_owner(lease)

        first_claim, second_claim = await asyncio.gather(claim_once(), claim_once())
        claims = [claim for claim in (first_claim, second_claim) if claim is not None]
        assert len(claims) == 1
        winner = claims[0]
        assert winner.command_id == second.command_id
        assert winner.command_seq == 2

        async with factory.begin() as session:
            repository = RuntimeCommandRepository(session)
            assert await repository.complete(
                replace(winner, processing_token="stale-token"),
                CommandDecision(status=CommandStatus.SUCCEEDED),
            ) is False
            assert await repository.complete(
                replace(
                    winner,
                    processing_fencing_token=winner.processing_fencing_token + 1,
                ),
                CommandDecision(status=CommandStatus.SUCCEEDED),
            ) is False
            assert await repository.complete(
                replace(winner, processing_owner_id="runtime-stale-owner"),
                CommandDecision(status=CommandStatus.SUCCEEDED),
            ) is False
            assert await repository.complete(
                winner,
                CommandDecision(status=CommandStatus.SUCCEEDED),
            )

            third_claim = await repository.claim_next_for_owner(lease)
            assert third_claim is not None
            assert third_claim.command_id == third.command_id
            await session.execute(
                text(
                    "update ai_call_runtime_command set processing_expires_at="
                    "clock_timestamp() - interval '1 second' where id=:command_id"
                ).bindparams(command_id=third.command_id)
            )
            assert await repository.complete(
                third_claim,
                CommandDecision(status=CommandStatus.SUCCEEDED),
            ) is False
            await session.execute(
                text(
                    "update ai_call_runtime_command set processing_expires_at="
                    "clock_timestamp() + interval '30 seconds' where id=:command_id"
                ).bindparams(command_id=third.command_id)
            )
            await session.execute(
                text(
                    "update ai_call_record set runtime_lease_expires_at="
                    "clock_timestamp() - interval '1 second' where call_id=:call_id"
                ).bindparams(call_id=start.call_id)
            )
            assert await repository.complete(
                third_claim,
                CommandDecision(status=CommandStatus.SUCCEEDED),
            ) is False
    finally:
        await engine.dispose()


async def test_end_preempt_revokes_old_claim_and_end_claim_wins_once() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            repository = RuntimeCommandRepository(session)
            start = await repository.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:end-preempt-claim",
                    payload={"business_id": "end-preempt-claim"},
                )
            )
            await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-end-preempt",
                    startup_id=UUID("ffffffff-ffff-4fff-8fff-ffffffffffff"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            lease = await DispatcherOwnerRepository(
                session
            ).assign_initial_owner("tenant-a", start.call_id)
            assert lease is not None
            old_claim = await repository.claim_next_for_owner(lease)
            assert old_claim is not None
            ordinary = await repository.append_command(
                CommandIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    command_type="HANDOFF_ACCEPTED",
                    idempotency_key="handoff:end-preempt-claim",
                    payload={"handoff_id": "handoff-preempt"},
                )
            )
            end = await repository.request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    source="customer_sip",
                    end_reason="customer_hangup",
                    dedupe_key="customer:end-preempt-claim",
                )
            )
            assert await repository.complete(
                old_claim,
                CommandDecision(status=CommandStatus.SUCCEEDED),
            ) is False

        async def claim_end_once():
            async with factory.begin() as session:
                return await RuntimeCommandRepository(session).claim_pending_end(lease)

        first_claim, second_claim = await asyncio.gather(
            claim_end_once(), claim_end_once()
        )
        claims = [claim for claim in (first_claim, second_claim) if claim is not None]
        assert len(claims) == 1
        end_claim = claims[0]
        assert end_claim.command_id == end.command_id

        async with factory.begin() as session:
            assert await RuntimeCommandRepository(session).complete(
                end_claim,
                CommandDecision(status=CommandStatus.SUCCEEDED),
            )

        async with factory() as session:
            rows = (
                await session.execute(
                    text(
                        "select id, status, processing_token from ai_call_runtime_command "
                        "where call_id=:call_id order by command_seq"
                    ).bindparams(call_id=start.call_id)
                )
            ).all()
            by_id = {row.id: row for row in rows}
            assert by_id[start.command_id].status == "SUPERSEDED"
            assert by_id[start.command_id].processing_token is None
            assert by_id[ordinary.command_id].status == "SUPERSEDED"
            assert by_id[end.command_id].status == "SUCCEEDED"
            assert await session.scalar(
                text(
                    "select last_applied_command_seq from ai_call_record "
                    "where call_id=:call_id"
                ).bindparams(call_id=start.call_id)
            ) == end.command_seq
    finally:
        await engine.dispose()


async def test_command_claim_start_retry_requires_no_effect_and_no_barrier() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            repository = RuntimeCommandRepository(session)
            start = await repository.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:claim-retry",
                    payload={"business_id": "claim-retry"},
                )
            )
            await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-claim-retry",
                    startup_id=UUID("11111111-1111-4111-8111-111111111111"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            lease = await DispatcherOwnerRepository(
                session
            ).assign_initial_owner("tenant-a", start.call_id)
            assert lease is not None

            first_claim = await repository.claim_next_for_owner(lease)
            assert first_claim is not None
            assert await repository.complete(
                first_claim,
                CommandDecision(
                    status=CommandStatus.RETRY_WAIT,
                    retry_after=timedelta(0),
                ),
            )
            retry_claim = await repository.claim_next_for_owner(lease)
            assert retry_claim is not None
            assert retry_claim.attempt_count == 2
            assert await repository.complete(
                retry_claim,
                CommandDecision(
                    status=CommandStatus.RETRY_WAIT,
                    retry_after=timedelta(0),
                ),
            )

            now = await read_database_time(session)
            session.add(
                AiCallRuntimeEffectModel(
                    id=93001,
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    command_id=start.command_id,
                    effect_type="CREATE_ROOM",
                    idempotency_key="effect:claim-retry",
                    fencing_token=lease.fencing_token,
                    status="RECONCILE_REQUIRED",
                    provider_namespace="stub:claim-retry",
                    provider_idempotency_key="provider:claim-retry",
                    resource_key="room:claim-retry",
                    resource_generation=lease.fencing_token,
                    created_at=now,
                    updated_at=now,
                )
            )
            await session.flush()
            assert await repository.claim_next_for_owner(lease) is None

            await repository.request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    source="timeout",
                    end_reason="startup_uncertain",
                    dedupe_key="timeout:claim-retry",
                )
            )
            assert await repository.claim_next_for_owner(lease) is None
    finally:
        await engine.dispose()


async def test_effect_registration_is_authorized_but_later_claim_is_command_independent() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            commands = RuntimeCommandRepository(session)
            effects = RuntimeEffectRepository(session)
            start = await commands.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="direct_sip",
                    idempotency_key="start:effect-independent",
                    payload={"phone_hash": "effect-independent"},
                )
            )
            await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-effect-independent",
                    startup_id=UUID("22222222-2222-4222-8222-222222222222"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            lease = await DispatcherOwnerRepository(
                session
            ).assign_initial_owner("tenant-a", start.call_id)
            assert lease is not None
            start_claim = await commands.claim_next_for_owner(lease)
            assert start_claim is not None

            room_spec = EffectSpec(
                effect_type="CREATE_ROOM",
                idempotency_key="effect:create-room:independent",
                provider_namespace="stub:independent",
                provider_idempotency_key="provider:create-room:independent",
                resource_key="room:effect-independent:g1",
                resource_generation=lease.fencing_token,
            )
            room = await effects.register(start_claim, room_spec)
            assert await effects.register(start_claim, room_spec) == room
            sip = await effects.register(
                start_claim,
                EffectSpec(
                    effect_type="CREATE_SIP_PARTICIPANT",
                    idempotency_key="effect:create-sip:independent",
                    provider_namespace="stub:independent",
                    provider_idempotency_key="provider:create-sip:independent",
                    resource_key="sip:effect-independent:g1",
                    resource_generation=lease.fencing_token,
                ),
            )
            with pytest.raises(EffectRegistrationError):
                await effects.register(
                    replace(start_claim, processing_token="stale-command-token"),
                    EffectSpec(
                        effect_type="START_EGRESS",
                        idempotency_key="effect:stale-command",
                        provider_namespace="stub:independent",
                        provider_idempotency_key="provider:stale-command",
                        resource_key="egress:effect-independent:g1",
                        resource_generation=lease.fencing_token,
                    ),
                )
            assert await commands.complete(
                start_claim,
                CommandDecision(status=CommandStatus.SUCCEEDED),
            )

            first_create_claim = await effects.claim_next(lease)
            assert first_create_claim is not None
            assert await effects.submit(
                first_create_claim,
                ProviderObservation(
                    kind=ProviderObservationKind.RESOURCE_PRESENT,
                    provider_reference="room-ref",
                ),
            )
            second_create_claim = await effects.claim_next(lease)
            assert second_create_claim is not None
            assert {first_create_claim.effect_id, second_create_claim.effect_id} == {
                room.effect_id,
                sip.effect_id,
            }
            assert await effects.submit(
                second_create_claim,
                ProviderObservation(
                    kind=ProviderObservationKind.RESOURCE_PRESENT,
                    provider_reference="sip-ref",
                ),
            )

            await commands.request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    source="agent",
                    end_reason="agent_hangup",
                    dedupe_key="agent:effect-independent:end",
                )
            )
            end_claim = await commands.claim_pending_end(lease)
            assert end_claim is not None
            with pytest.raises(EffectRegistrationError):
                await effects.register(
                    end_claim,
                    EffectSpec(
                        effect_type="START_EGRESS",
                        idempotency_key="effect:create-after-end",
                        provider_namespace="stub:independent",
                        provider_idempotency_key="provider:create-after-end",
                        resource_key="egress:create-after-end:g1",
                        resource_generation=lease.fencing_token,
                    ),
                )
            graph = await effects.register_end_graph(end_claim)
            assert {effect.effect_type for effect in graph} == {
                "HANGUP_SIP",
                "DELETE_ROOM",
            }
            assert await commands.complete(
                end_claim,
                CommandDecision(status=CommandStatus.SUCCEEDED),
            )

            hangup = await effects.claim_next(lease)
            assert hangup is not None and hangup.effect_type == "HANGUP_SIP"
            assert await effects.claim_next(lease) is None
            assert await effects.submit(
                hangup,
                ProviderObservation(kind=ProviderObservationKind.TERMINAL_CONFIRMED),
            )
            delete_room = await effects.claim_next(lease)
            assert delete_room is not None and delete_room.effect_type == "DELETE_ROOM"
            assert await effects.submit(
                delete_room,
                ProviderObservation(kind=ProviderObservationKind.TERMINAL_CONFIRMED),
            )
            assert await effects.mark_cleanup_clean(lease)

        async with factory() as session:
            record = (
                await session.execute(
                    text(
                        "select runtime_owner_id, runtime_capacity_class, "
                        "resource_cleanup_status from ai_call_record where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            assert tuple(record) == (None, "none", "clean")
    finally:
        await engine.dispose()


async def test_effect_quiet_gate_and_stale_token_prevent_early_cleanup() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            commands = RuntimeCommandRepository(session)
            effects = RuntimeEffectRepository(session)
            start = await commands.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:quiet-gate",
                    payload={"business_id": "quiet-gate"},
                )
            )
            await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-quiet-gate",
                    startup_id=UUID("33333333-3333-4333-8333-333333333333"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            lease = await DispatcherOwnerRepository(
                session
            ).assign_initial_owner("tenant-a", start.call_id)
            assert lease is not None
            start_claim = await commands.claim_next_for_owner(lease)
            assert start_claim is not None
            now = await read_database_time(session)
            create = await effects.register(
                start_claim,
                EffectSpec(
                    effect_type="CREATE_ROOM",
                    idempotency_key="effect:create-room:quiet-gate",
                    provider_namespace="stub:quiet-gate",
                    provider_idempotency_key="provider:create-room:quiet-gate",
                    resource_key="room:quiet-gate:g1",
                    resource_generation=lease.fencing_token,
                    reconcile_deadline_at=now + timedelta(seconds=60),
                ),
            )
            create_claim = await effects.claim_next(lease)
            assert create_claim is not None
            assert await effects.submit(
                create_claim,
                ProviderObservation(
                    kind=ProviderObservationKind.UNCERTAIN,
                    retry_after=timedelta(seconds=60),
                ),
            )

            await commands.request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    source="timeout",
                    end_reason="startup_uncertain",
                    dedupe_key="timeout:quiet-gate:end",
                )
            )
            end_claim = await commands.claim_pending_end(lease)
            assert end_claim is not None
            graph = await effects.register_end_graph(end_claim)
            destroy = graph[0]
            assert await commands.complete(
                end_claim,
                CommandDecision(status=CommandStatus.SUCCEEDED),
            )

            destroy_claim = await effects.claim_next(lease)
            assert destroy_claim is not None
            assert destroy_claim.effect_id == destroy.effect_id
            await session.execute(
                text(
                    "update ai_call_runtime_effect set processing_expires_at="
                    "clock_timestamp() - interval '1 second' where id=:effect_id"
                ).bindparams(effect_id=destroy.effect_id)
            )
            takeover = await effects.claim_next(lease)
            assert takeover is not None
            assert takeover.processing_token != destroy_claim.processing_token
            assert await effects.submit(
                destroy_claim,
                ProviderObservation(kind=ProviderObservationKind.RESOURCE_ABSENT),
            ) is False
            assert await effects.submit(
                replace(
                    takeover,
                    processing_fencing_token=takeover.processing_fencing_token + 1,
                ),
                ProviderObservation(kind=ProviderObservationKind.RESOURCE_ABSENT),
            ) is False
            assert await effects.submit(
                takeover,
                ProviderObservation(kind=ProviderObservationKind.RESOURCE_ABSENT),
            )
            assert await effects.mark_cleanup_clean(lease) is False

            await session.execute(
                text(
                    "update ai_call_runtime_effect set "
                    "reconcile_deadline_at=clock_timestamp() - interval '1 second' "
                    "where id=:create_id"
                ).bindparams(create_id=create.effect_id)
            )
            await session.execute(
                text(
                    "update ai_call_runtime_effect set "
                    "create_protection_deadline_at=clock_timestamp() - interval '1 second', "
                    "reconcile_after=clock_timestamp() - interval '1 second' "
                    "where id=:destroy_id"
                ).bindparams(destroy_id=destroy.effect_id)
            )
            first_absence = await effects.claim_next(lease)
            assert first_absence is not None
            assert await effects.submit(
                first_absence,
                ProviderObservation(kind=ProviderObservationKind.RESOURCE_ABSENT),
            )
            assert await effects.mark_cleanup_clean(lease) is False
            interrupted = await effects.claim_next(lease)
            assert interrupted is not None
            assert await effects.submit(
                interrupted,
                ProviderObservation(kind=ProviderObservationKind.UNCERTAIN),
            )
            after_interruption = await effects.claim_next(lease)
            assert after_interruption is not None
            assert await effects.submit(
                after_interruption,
                ProviderObservation(kind=ProviderObservationKind.RESOURCE_ABSENT),
            )
            assert await effects.mark_cleanup_clean(lease) is False
            second_absence = await effects.claim_next(lease)
            assert second_absence is not None
            assert await effects.submit(
                second_absence,
                ProviderObservation(kind=ProviderObservationKind.RESOURCE_ABSENT),
            )
            assert await effects.mark_cleanup_clean(lease)

        async with factory() as session:
            destroy_row = (
                await session.execute(
                    text(
                        "select status, absence_observation_count, absence_confirmed_at, "
                        "terminal_confirmed_at from ai_call_runtime_effect where id=:effect_id"
                    ).bindparams(effect_id=destroy.effect_id)
                )
            ).one()
            assert destroy_row.status == "APPLIED"
            assert destroy_row.absence_observation_count == 2
            assert destroy_row.absence_confirmed_at is not None
            assert destroy_row.terminal_confirmed_at is not None
    finally:
        await engine.dispose()


async def test_stub_handlers_close_start_and_end_without_real_provider() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    room_key = "room:stub-handler:g1"
    agent_key = "agent:stub-handler:g1"
    provider = ScriptedProviderStub(
        {
            room_key: [
                StubObservationKind.RESOURCE_PRESENT,
                StubObservationKind.DESTROY_CONFIRMED,
            ],
            agent_key: [
                StubObservationKind.RESOURCE_PRESENT,
                StubObservationKind.DESTROY_CONFIRMED,
            ],
        }
    )
    try:
        async with factory.begin() as session:
            commands = RuntimeCommandRepository(session)
            start = await commands.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:stub-handler",
                    payload={"business_id": "stub-handler"},
                )
            )
            await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-stub-handler",
                    startup_id=UUID("44444444-4444-4444-8444-444444444444"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            lease = await DispatcherOwnerRepository(
                session
            ).assign_initial_owner("tenant-a", start.call_id)
            assert lease is not None
            start_claim = await commands.claim_next_for_owner(lease)
            assert start_claim is not None

        start_result = await StartCallHandler(factory, provider).handle(
            start_claim,
            lease,
            [
                EffectSpec(
                    effect_type="CREATE_ROOM",
                    idempotency_key="effect:create-room:stub-handler",
                    provider_namespace="stub:handler",
                    provider_idempotency_key="provider:create-room:stub-handler",
                    resource_key=room_key,
                    resource_generation=lease.fencing_token,
                ),
                EffectSpec(
                    effect_type="ATTACH_AGENT_PARTICIPANT",
                    idempotency_key="effect:attach-agent:stub-handler",
                    provider_namespace="stub:handler",
                    provider_idempotency_key="provider:attach-agent:stub-handler",
                    resource_key=agent_key,
                    resource_generation=lease.fencing_token,
                ),
            ],
        )
        assert start_result.command_completed is True

        async with factory.begin() as session:
            commands = RuntimeCommandRepository(session)
            await commands.request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    source="web_client",
                    end_reason="user_requested",
                    dedupe_key="web:stub-handler:end",
                )
            )
            end_claim = await commands.claim_pending_end(lease)
            assert end_claim is not None

        end_result = await EndCallHandler(factory, provider).handle(end_claim, lease)
        assert end_result.logical_end_completed is True
        assert end_result.resource_cleanup_status == "clean"

        async with factory() as session:
            record = (
                await session.execute(
                    text(
                        "select status, resource_cleanup_status, runtime_owner_id "
                        "from ai_call_record where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            assert tuple(record) == ("completed", "clean", None)
            assert await session.scalar(
                text(
                    "select count(*) from ai_call_runtime_command "
                    "where call_id=:call_id and command_type='END_CALL'"
                ).bindparams(call_id=start.call_id)
            ) == 1
            assert await session.scalar(
                text(
                    "select count(*) from ai_call_runtime_effect where call_id=:call_id"
                ).bindparams(call_id=start.call_id)
            ) == 4
        assert all(set(call) == {"provider_namespace", "effect_type", "resource_key"} for call in provider.calls)
    finally:
        await engine.dispose()


async def test_two_runtime_services_use_independent_registries_in_db_only_loop() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            start = await RuntimeCommandRepository(session).create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:two-runtime-services",
                    payload={"business_id": "two-runtime-services"},
                )
            )
            worker_a = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="pod-a",
                    startup_id=UUID("aaaaaaaa-1111-4111-8111-111111111111"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            worker_b = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="pod-b",
                    startup_id=UUID("bbbbbbbb-2222-4222-8222-222222222222"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )

        assert await DispatcherControlService(factory).run_once() == 1
        room_key = f"room:{start.call_id}:g1"
        agent_key = f"agent:{start.call_id}:g1"
        provider_a = ScriptedProviderStub(
            {
                room_key: [
                    StubObservationKind.RESOURCE_PRESENT,
                    StubObservationKind.DESTROY_CONFIRMED,
                ],
                agent_key: [
                    StubObservationKind.RESOURCE_PRESENT,
                    StubObservationKind.DESTROY_CONFIRMED,
                ],
            }
        )
        provider_b = ScriptedProviderStub({})
        runtime_a = RuntimeControlService(
            worker_id=worker_a.worker_id,
            registry=RuntimeRegistry(),
            session_factory=factory,
            provider=provider_a,
        )
        runtime_b = RuntimeControlService(
            worker_id=worker_b.worker_id,
            registry=RuntimeRegistry(),
            session_factory=factory,
            provider=provider_b,
        )
        assert runtime_a.registry is not runtime_b.registry
        assert provider_a is not provider_b

        assert await runtime_b.run_once() == 0
        assert await runtime_a.run_once() == 1

        async with factory.begin() as session:
            await RuntimeCommandRepository(session).request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    source="web_client",
                    end_reason="user_requested",
                    dedupe_key="web:two-runtime-services:end",
                )
            )
        assert await runtime_b.run_once() == 0
        assert await runtime_a.run_once() == 1

        async with factory() as session:
            record = (
                await session.execute(
                    text(
                        "select status, resource_cleanup_status, runtime_owner_id "
                        "from ai_call_record where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            assert tuple(record) == ("completed", "clean", None)
    finally:
        await engine.dispose()


async def test_runtime_effect_recovery_releases_owner_after_cleanup_converges() -> None:
    class LocalHandle:
        def __init__(self) -> None:
            self.stopped = asyncio.Event()

        async def fail_closed(self) -> None:
            self.stopped.set()

    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            start = await RuntimeCommandRepository(session).create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:delayed-cleanup",
                    payload={"business_id": "delayed-cleanup"},
                )
            )
            worker = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-delayed-cleanup",
                    startup_id=UUID("cccccccc-3333-4333-8333-333333333333"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )

        assert await DispatcherControlService(factory).run_once() == 1
        room_key = f"room:{start.call_id}:g1"
        agent_key = f"agent:{start.call_id}:g1"
        provider = ScriptedProviderStub(
            {
                room_key: [
                    StubObservationKind.RESOURCE_PRESENT,
                    ProviderObservation(
                        kind=ProviderObservationKind.ACCEPTED,
                        retry_after=timedelta(seconds=60),
                    ),
                    StubObservationKind.DESTROY_CONFIRMED,
                ],
                agent_key: [
                    StubObservationKind.RESOURCE_PRESENT,
                    StubObservationKind.DESTROY_CONFIRMED,
                ],
            }
        )
        handle = LocalHandle()
        runtime = RuntimeControlService(
            worker_id=worker.worker_id,
            registry=RuntimeRegistry(local_handles={start.call_id: handle}),
            session_factory=factory,
            provider=provider,
        )

        assert await runtime.run_once() == 1
        async with factory.begin() as session:
            await RuntimeCommandRepository(session).request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    source="web_client",
                    end_reason="user_requested",
                    dedupe_key="web:delayed-cleanup:end",
                )
            )
        assert await runtime.run_once() == 1

        async with factory.begin() as session:
            before = (
                await session.execute(
                    text(
                        "select status, resource_cleanup_status, runtime_owner_id "
                        "from ai_call_record where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            assert tuple(before) == ("completed", "attention_required", None)
            await session.execute(
                text(
                    "update ai_call_runtime_effect set "
                    "reconcile_after=clock_timestamp() - interval '1 second' "
                    "where call_id=:call_id and status='RECONCILE_REQUIRED'"
                ).bindparams(call_id=start.call_id)
            )
            await session.execute(
                text(
                    "update ai_call_record set "
                    "resource_cleanup_next_retry_at=clock_timestamp() - interval '1 second' "
                    "where call_id=:call_id"
                ).bindparams(call_id=start.call_id)
            )

        assert await RecoveryControlService(factory).run_once() == 1
        assert await runtime.run_once() == 1

        async with factory() as session:
            after = (
                await session.execute(
                    text(
                        "select status, resource_cleanup_status, runtime_owner_id, "
                        "runtime_capacity_class from ai_call_record where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            assert tuple(after) == ("completed", "clean", None, "none")
            assert handle.stopped.is_set()
    finally:
        await engine.dispose()


async def test_runtime_watchdog_stops_local_handle_and_cancels_slow_provider() -> None:
    class SlowProvider:
        def __init__(self) -> None:
            self.started = False
            self.cancelled = False

        async def apply(self, effect):
            self.started = True
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                self.cancelled = True
                raise
            return ProviderObservation(kind=ProviderObservationKind.RESOURCE_PRESENT)

    class LocalHandle:
        def __init__(self) -> None:
            self.fail_closed_called = asyncio.Event()

        async def fail_closed(self) -> None:
            self.fail_closed_called.set()

    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            start = await RuntimeCommandRepository(session).create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:watchdog-gate",
                    payload={"business_id": "watchdog-gate"},
                )
            )
            worker = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-watchdog-gate",
                    startup_id=UUID("dddddddd-4444-4444-8444-444444444444"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
        assert await DispatcherControlService(factory).run_once() == 1

        provider = SlowProvider()
        handle = LocalHandle()
        registry = RuntimeRegistry(local_handles={start.call_id: handle})
        runtime = RuntimeControlService(
            worker_id=worker.worker_id,
            registry=registry,
            session_factory=factory,
            provider=provider,
            owner_lease_ttl=timedelta(seconds=0.5),
            fail_closed_margin_seconds=0.1,
        )

        assert await runtime.run_once() == 0
        assert handle.fail_closed_called.is_set()
        assert provider.started is True
        assert provider.cancelled is True

        async with factory() as session:
            applied = await session.scalar(
                text(
                    "select count(*) from ai_call_runtime_effect "
                    "where call_id=:call_id and status='APPLIED'"
                ).bindparams(call_id=start.call_id)
            )
            assert applied == 0
    finally:
        await engine.dispose()


async def test_effect_repository_rejects_incomplete_or_dangling_destroy_graph() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            commands = RuntimeCommandRepository(session)
            effects = RuntimeEffectRepository(session)
            start = await commands.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:destroy-graph-validation",
                    payload={"business_id": "destroy-graph-validation"},
                )
            )
            await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-destroy-graph",
                    startup_id=UUID("eeeeeeee-5555-4555-8555-555555555555"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            lease = await DispatcherOwnerRepository(session).assign_initial_owner(
                "tenant-a", start.call_id
            )
            assert lease is not None
            start_claim = await commands.claim_next_for_owner(lease)
            assert start_claim is not None
            room = await effects.register(
                start_claim,
                EffectSpec(
                    effect_type="CREATE_ROOM",
                    idempotency_key="effect:create-room:destroy-graph",
                    provider_namespace="stub:destroy-graph",
                    provider_idempotency_key="provider:create-room:destroy-graph",
                    resource_key="room:destroy-graph:g1",
                    resource_generation=lease.fencing_token,
                ),
            )
            agent = await effects.register(
                start_claim,
                EffectSpec(
                    effect_type="ATTACH_AGENT_PARTICIPANT",
                    idempotency_key="effect:attach-agent:destroy-graph",
                    provider_namespace="stub:destroy-graph",
                    provider_idempotency_key="provider:attach-agent:destroy-graph",
                    resource_key="agent:destroy-graph:g1",
                    resource_generation=lease.fencing_token,
                ),
            )
            await commands.request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    source="timeout",
                    end_reason="test",
                    dedupe_key="timeout:destroy-graph:end",
                )
            )
            end_claim = await commands.claim_pending_end(lease)
            assert end_claim is not None

            with pytest.raises(EffectRegistrationError):
                await effects.register(
                    end_claim,
                    EffectSpec(
                        effect_type="DELETE_ROOM",
                        idempotency_key=f"end:{start.call_id}:DELETE_ROOM:{room.effect_id}",
                        provider_namespace="stub:destroy-graph",
                        provider_idempotency_key="provider:delete-room:destroy-graph",
                        resource_key="room:destroy-graph:g1",
                        resource_generation=lease.fencing_token,
                        source_create_effect_id=room.effect_id,
                        execution_phase=20,
                    ),
                )

            with pytest.raises(EffectRegistrationError):
                await effects.register(
                    end_claim,
                    EffectSpec(
                        effect_type="DISCONNECT_AGENT_PARTICIPANT",
                        idempotency_key=(
                            f"end:{start.call_id}:DISCONNECT_AGENT_PARTICIPANT:"
                            f"{agent.effect_id}"
                        ),
                        provider_namespace="stub:destroy-graph",
                        provider_idempotency_key="provider:disconnect-agent:destroy-graph",
                        resource_key="agent:destroy-graph:g1",
                        resource_generation=lease.fencing_token,
                        source_create_effect_id=agent.effect_id,
                        execution_phase=10,
                        prerequisite_effect_ids=(999999999,),
                    ),
                )

            graph = await effects.register_end_graph(end_claim)
            delete_room = next(
                effect for effect in graph if effect.effect_type == "DELETE_ROOM"
            )
            delete_row = await session.get(
                AiCallRuntimeEffectModel, delete_room.effect_id
            )
            assert delete_row is not None
            with pytest.raises(EffectRegistrationError):
                await effects.register(
                    end_claim,
                    EffectSpec(
                        effect_type=delete_row.effect_type,
                        idempotency_key=delete_row.idempotency_key,
                        provider_namespace=delete_row.provider_namespace,
                        provider_idempotency_key=delete_row.provider_idempotency_key,
                        resource_key=delete_row.resource_key,
                        resource_generation=delete_row.resource_generation,
                        source_create_effect_id=delete_row.source_create_effect_id,
                        create_protection_deadline_at=(
                            delete_row.create_protection_deadline_at
                        ),
                        reconcile_deadline_at=delete_row.reconcile_deadline_at,
                        execution_phase=delete_row.execution_phase,
                        prerequisite_effect_ids=(),
                    ),
                )
    finally:
        await engine.dispose()


async def test_missing_dependency_row_is_fail_closed_during_effect_claim() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            commands = RuntimeCommandRepository(session)
            effects = RuntimeEffectRepository(session)
            start = await commands.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="direct_sip",
                    idempotency_key="start:dangling-dependency",
                    payload={"phone_hash": "dangling-dependency"},
                )
            )
            await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-dangling-dependency",
                    startup_id=UUID("ffffffff-6666-4666-8666-666666666666"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            lease = await DispatcherOwnerRepository(session).assign_initial_owner(
                "tenant-a", start.call_id
            )
            assert lease is not None
            start_claim = await commands.claim_next_for_owner(lease)
            assert start_claim is not None
            await effects.register(
                start_claim,
                EffectSpec(
                    effect_type="CREATE_SIP_PARTICIPANT",
                    idempotency_key="effect:create-sip:dangling-dependency",
                    provider_namespace="stub:dangling-dependency",
                    provider_idempotency_key="provider:create-sip:dangling-dependency",
                    resource_key="sip:dangling-dependency:g1",
                    resource_generation=lease.fencing_token,
                ),
            )
            await commands.request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    source="timeout",
                    end_reason="test",
                    dedupe_key="timeout:dangling-dependency:end",
                )
            )
            end_claim = await commands.claim_pending_end(lease)
            assert end_claim is not None
            graph = await effects.register_end_graph(end_claim)
            assert len(graph) == 1
            session.add(
                AiCallRuntimeEffectDependencyModel(
                    id=99990001,
                    tenant_id="tenant-a",
                    effect_id=graph[0].effect_id,
                    prerequisite_effect_id=99990002,
                    required_status="APPLIED",
                    created_at=await read_database_time(session),
                )
            )
            assert await commands.complete(
                end_claim,
                CommandDecision(status=CommandStatus.SUCCEEDED),
            )

        async with factory.begin() as session:
            assert await RuntimeEffectRepository(session).claim_next(lease) is None
    finally:
        await engine.dispose()
