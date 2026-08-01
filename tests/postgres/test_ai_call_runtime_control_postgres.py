from __future__ import annotations

import asyncio
import json
import os
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import psycopg
import pytest
from psycopg import ClientCursor
from sqlalchemy import event, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.api.v1.ai_call.model import AiCallRecordModel
from app.api.v1.ai_call.outbound.attempt_reconciler import (
    OutboundAttemptReconcileWorker,
)
from app.api.v1.ai_call.outbound.owner_runtime_start import (
    OwnerRuntimeOutboundStart,
)
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from app.api.v1.ai_call.outbound.sip_line_model import AiCallSipLineModel
from app.api.v1.ai_call.outbound.task_executor import OutboundTaskExecutor
from app.services.ai_call.runtime_control.bootstrap_service import (
    RuntimeBootstrapNotFoundError,
    RuntimeBootstrapService,
)
from app.services.ai_call.runtime_control.command_repository import (
    CommandDecision,
    CommandIntent,
    EndCallIntent,
    IdempotencyConflictError,
    RuntimeCommandRepository,
    RuntimeCommandResultError,
    RuntimeControlModeError,
    StartCallIntent,
    TerminalBarrierError,
)
from app.services.ai_call.runtime_control.direct_sip_phone import (
    prepare_direct_sip_phone,
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
from app.services.ai_call.runtime_control.entry_start_service import (
    RuntimeEntryStartService,
    StartEntryRequest,
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
    build_worker_id,
)
from app.services.ai_call.runtime_control.postgres_wakeup import (
    CONTROL_WAKEUP_CHANNEL,
    PostgresWakeupListener,
    publish_control_wakeup,
)
from app.services.ai_call.runtime_control.provider_stub import (
    DeterministicDbOnlyProviderStub,
    DeterministicWebProviderStub,
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
from app.services.ai_call.runtime_control.runtime_token_service import (
    RuntimeTokenGateError,
    RuntimeTokenGateRepository,
    RuntimeTokenNotFoundError,
)
from app.services.ai_call.runtime_control.start_readiness_repository import (
    StartReadinessRejected,
)
from app.services.ai_call.runtime_control.startup_recovery import (
    StartupReconcileService,
)
from app.services.ai_call.runtime_control.timing import read_database_time
from app.services.ai_call.runtime_control.types import CommandStatus
from app.utils.id_util import generate_snowflake_id

pytestmark = pytest.mark.anyio

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATH = (
    PROJECT_ROOT
    / "docs/livekit-ai-outbound/sql/phase-i1-owner-command-db-control-plane.sql"
)
DIRECT_SIP_MIGRATION_PATH = (
    PROJECT_ROOT
    / "docs/livekit-ai-outbound/sql/phase-i2-direct-sip-db-only-plaintext.sql"
)


def _direct_sip_intent(
    *,
    idempotency_key: str,
    payload: dict[str, object],
) -> StartCallIntent:
    phone = prepare_direct_sip_phone("13812345678")
    return StartCallIntent(
        tenant_id="tenant-a",
        entry_type="direct_sip",
        idempotency_key=idempotency_key,
        payload=payload,
        callee_phone_number=phone.plaintext,
        callee_phone_number_masked=phone.masked,
        callee_phone_number_hash=phone.fingerprint,
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


async def test_direct_sip_plaintext_migration_is_idempotent() -> None:
    _reset_legacy_schema()
    _execute_script(MIGRATION_PATH.read_text(encoding="utf-8"))
    migration_sql = DIRECT_SIP_MIGRATION_PATH.read_text(encoding="utf-8")
    _execute_script(migration_sql)
    _execute_script(migration_sql)

    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    try:
        async with engine.connect() as connection:
            column = (
                await connection.execute(
                    text(
                        "select data_type, character_maximum_length, is_nullable "
                        "from information_schema.columns "
                        "where table_schema=current_schema() "
                        "and table_name='ai_call_record' "
                        "and column_name='callee_phone_number'"
                    )
                )
            ).one()
        assert tuple(column) == ("character varying", 32, "YES")
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
        AiCallOutboundAttemptModel.__table__,
        AiCallOutboundTargetModel.__table__,
        AiCallOutboundTaskModel.__table__,
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


async def test_command_query_is_tenant_scoped_and_returns_only_parsed_persistent_state() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            tenant_a = await RuntimeCommandRepository(session).create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:command-query:a",
                    payload={"voice": "voice-a"},
                    sensitive_payload_ciphertext="ciphertext-must-not-leak",
                )
            )
            tenant_b = await RuntimeCommandRepository(session).create_start_call(
                StartCallIntent(
                    tenant_id="tenant-b",
                    entry_type="web",
                    idempotency_key="start:command-query:b",
                    payload={"voice": "voice-b"},
                )
            )

        async with factory.begin() as session:
            await session.execute(
                text(
                    "update ai_call_runtime_command set "
                    "status='SUCCEEDED', result_json=:result_json, "
                    "dispatch_token='dispatch-secret', "
                    "processing_token='processing-secret', "
                    "claimed_at=clock_timestamp(), finished_at=clock_timestamp() "
                    "where id=:command_id"
                ).bindparams(
                    command_id=tenant_a.command_id,
                    result_json='{"nested":{"ok":true}}',
                )
            )

        async with factory() as session:
            repository = RuntimeCommandRepository(session)
            snapshot = await repository.get_command(
                tenant_id="tenant-a",
                command_id=tenant_a.command_id,
            )
            assert snapshot is not None
            assert snapshot.command_id == tenant_a.command_id
            assert snapshot.call_id == tenant_a.call_id
            assert snapshot.status == "SUCCEEDED"
            assert snapshot.result == {"nested": {"ok": True}}
            assert snapshot.error_message is None
            assert snapshot.claimed_at is not None
            assert snapshot.finished_at is not None
            assert not hasattr(snapshot, "sensitive_payload_ciphertext")
            assert not hasattr(snapshot, "processing_token")
            assert not hasattr(snapshot, "dispatch_token")

            assert (
                await repository.get_command(
                    tenant_id="tenant-b",
                    command_id=tenant_a.command_id,
                )
                is None
            )
            assert (
                await repository.get_command(
                    tenant_id="tenant-a",
                    command_id=tenant_b.command_id,
                )
                is None
            )
    finally:
        await engine.dispose()


async def test_command_query_fails_closed_for_non_object_result_json() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            start = await RuntimeCommandRepository(session).create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:command-query:corrupt",
                    payload={},
                )
            )
            await session.execute(
                text(
                    "update ai_call_runtime_command set result_json='[]' "
                    "where id=:command_id"
                ).bindparams(command_id=start.command_id)
            )

        async with factory() as session:
            with pytest.raises(RuntimeCommandResultError):
                await RuntimeCommandRepository(session).get_command(
                    tenant_id="tenant-a",
                    command_id=start.command_id,
                )
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


async def test_runtime_bootstrap_reads_owner_snapshot_with_tenant_boundary() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            start = await RuntimeCommandRepository(session).create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:bootstrap-read",
                    payload={},
                )
            )
            await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-bootstrap-read",
                    startup_id=UUID("17171717-1717-4171-8171-171717171717"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            lease = await DispatcherOwnerRepository(session).assign_initial_owner(
                "tenant-a", start.call_id
            )
            assert lease is not None
            record = await session.scalar(
                select(AiCallRecordModel).where(
                    AiCallRecordModel.tenant_id == "tenant-a",
                    AiCallRecordModel.call_id == start.call_id,
                )
            )
            assert record is not None
            record.status = "ready"
            record.agent_participant_identity = "agent-bootstrap"
            record.agent_resource_generation = lease.fencing_token
            record.agent_media_ready_at = await read_database_time(session)
            now = await read_database_time(session)
            for effect_id, effect_type, resource_key in (
                (9201, "CREATE_ROOM", "room:bootstrap"),
                (9202, "ATTACH_AGENT_PARTICIPANT", "agent:bootstrap"),
            ):
                session.add(
                    AiCallRuntimeEffectModel(
                        id=effect_id,
                        tenant_id="tenant-a",
                        call_id=start.call_id,
                        command_id=start.command_id,
                        effect_type=effect_type,
                        idempotency_key=f"bootstrap:{effect_type}",
                        fencing_token=lease.fencing_token,
                        status="APPLIED",
                        provider_namespace="stub:bootstrap",
                        provider_idempotency_key=f"provider:{effect_type}",
                        resource_key=resource_key,
                        resource_generation=lease.fencing_token,
                        created_at=now,
                        updated_at=now,
                    )
                )

        async with factory() as session:
            snapshot = await RuntimeBootstrapService(session).get(
                tenant_id="tenant-a",
                call_id=start.call_id,
            )
            assert snapshot.phase == "ready"
            assert snapshot.token_available is False
            token_gate = await RuntimeTokenGateRepository(session).authorize(
                tenant_id="tenant-a",
                call_id=start.call_id,
            )
            assert token_gate.participant_identity == f"caller-{start.call_id}"
            assert token_gate.runtime_fencing_token == lease.fencing_token
            with pytest.raises(RuntimeBootstrapNotFoundError):
                await RuntimeBootstrapService(session).get(
                    tenant_id="tenant-b",
                    call_id=start.call_id,
                )
            with pytest.raises(RuntimeTokenNotFoundError):
                await RuntimeTokenGateRepository(session).authorize(
                    tenant_id="tenant-b",
                    call_id=start.call_id,
                )

        async with factory.begin() as session:
            worker = await session.get(AiCallRuntimeWorkerModel, lease.owner_id)
            assert worker is not None
            worker.status = "DRAINING"

        async with factory() as session:
            draining_gate = await RuntimeTokenGateRepository(session).authorize(
                tenant_id="tenant-a",
                call_id=start.call_id,
            )
            assert draining_gate.runtime_fencing_token == lease.fencing_token

        async with factory.begin() as session:
            worker = await session.get(AiCallRuntimeWorkerModel, lease.owner_id)
            assert worker is not None
            worker.lease_expires_at = await read_database_time(session) - timedelta(
                seconds=1
            )

        async with factory() as session:
            with pytest.raises(RuntimeTokenGateError) as exc_info:
                await RuntimeTokenGateRepository(session).authorize(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                )
            assert exc_info.value.error_code == "OWNER_UNAVAILABLE"
    finally:
        await engine.dispose()


async def test_committed_initial_owner_response_loss_does_not_duplicate_capacity() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            start = await RuntimeCommandRepository(session).create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:owner-response-loss",
                    payload={"business_id": "owner-response-loss"},
                )
            )
            worker = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-owner-response-loss",
                    startup_id=UUID("15151515-1515-4151-8151-151515151515"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )

        armed = False

        def fail_after_commit(_session) -> None:
            if armed:
                raise ConnectionError("injected committed response loss")

        event.listen(AsyncSession.sync_session_class, "after_commit", fail_after_commit)
        try:
            async with factory() as session:
                lease = await DispatcherOwnerRepository(session).assign_initial_owner(
                    "tenant-a", start.call_id
                )
                assert lease is not None
                armed = True
                with pytest.raises(ConnectionError, match="committed response loss"):
                    await session.commit()
        finally:
            event.remove(AsyncSession.sync_session_class, "after_commit", fail_after_commit)

        async with factory.begin() as session:
            assert (
                await DispatcherOwnerRepository(session).assign_initial_owner(
                    "tenant-a", start.call_id
                )
                is None
            )
            record = (
                await session.execute(
                    text(
                        "select runtime_owner_id, runtime_fencing_token, "
                        "runtime_capacity_class from ai_call_record "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            assert tuple(record) == (worker.worker_id, 1, "active")
            assert (
                await session.scalar(
                    text(
                        "select active_call_count from ai_call_runtime_worker "
                        "where worker_id=:worker_id"
                    ).bindparams(worker_id=worker.worker_id)
                )
                == 1
            )
    finally:
        await engine.dispose()


async def test_committed_effect_registration_and_submit_response_loss_are_replay_safe() -> None:
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
                    idempotency_key="start:effect-response-loss",
                    payload={"business_id": "effect-response-loss"},
                )
            )
            await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-effect-response-loss",
                    startup_id=UUID("16161616-1616-4161-8161-161616161616"),
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

        spec = EffectSpec(
            effect_type="CREATE_ROOM",
            idempotency_key="effect:create-room:response-loss",
            provider_namespace="stub:response-loss",
            provider_idempotency_key="provider:create-room:response-loss",
            resource_key="room:response-loss",
            resource_generation=lease.fencing_token,
        )

        armed = False

        def fail_after_commit(_session) -> None:
            if armed:
                raise ConnectionError("injected committed response loss")

        event.listen(AsyncSession.sync_session_class, "after_commit", fail_after_commit)
        try:
            async with factory() as session:
                snapshot = await RuntimeEffectRepository(session).register(
                    start_claim, spec
                )
                armed = True
                with pytest.raises(ConnectionError, match="committed response loss"):
                    await session.commit()
        finally:
            event.remove(AsyncSession.sync_session_class, "after_commit", fail_after_commit)

        async with factory.begin() as session:
            repeated = await RuntimeEffectRepository(session).register(start_claim, spec)
            assert repeated == snapshot
            assert (
                await session.scalar(
                    text(
                        "select count(*) from ai_call_runtime_effect "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
                == 1
            )
            assert await RuntimeCommandRepository(session).complete(
                start_claim,
                CommandDecision(status=CommandStatus.SUCCEEDED),
            )
            effect_claim = await RuntimeEffectRepository(session).claim_next(lease)
            assert effect_claim is not None

        event.listen(AsyncSession.sync_session_class, "after_commit", fail_after_commit)
        try:
            async with factory() as session:
                assert await RuntimeEffectRepository(session).submit(
                    effect_claim,
                    ProviderObservation(
                        kind=ProviderObservationKind.RESOURCE_PRESENT,
                        provider_reference="room-response-loss",
                    ),
                )
                with pytest.raises(ConnectionError, match="committed response loss"):
                    await session.commit()
        finally:
            event.remove(AsyncSession.sync_session_class, "after_commit", fail_after_commit)

        async with factory.begin() as session:
            assert await RuntimeEffectRepository(session).claim_next(lease) is None
            state = (
                await session.execute(
                    text(
                        "select status, provider_reference, processing_token "
                        "from ai_call_runtime_effect where id=:effect_id"
                    ).bindparams(effect_id=effect_claim.effect_id)
                )
            ).one()
            assert tuple(state) == ("APPLIED", "room-response-loss", None)
    finally:
        await engine.dispose()


async def test_dispatcher_expires_unallocated_start_at_persisted_deadline() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            start = await RuntimeCommandRepository(session).create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:allocation-deadline",
                    payload={"business_id": "allocation-deadline"},
                    allocation_deadline_at=await read_database_time(session),
                )
            )
            await session.execute(
                text(
                    "update ai_call_runtime_command set "
                    "allocation_deadline_at=clock_timestamp() - interval '1 second' "
                    "where id=:command_id"
                ).bindparams(command_id=start.command_id)
            )

        assert await DispatcherControlService(factory).run_once() == 1

        async with factory() as session:
            command = (
                await session.execute(
                    text(
                        "select status, error_message, target_owner_id, "
                        "processing_token from ai_call_runtime_command "
                        "where id=:command_id"
                    ).bindparams(command_id=start.command_id)
                )
            ).one()
            record = (
                await session.execute(
                    text(
                        "select status, failure_stage, failure_message, "
                        "runtime_owner_id, runtime_capacity_class, "
                        "resource_cleanup_status from ai_call_record "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            assert tuple(command) == ("DEAD", "ALLOCATION_TIMEOUT", None, None)
            assert tuple(record) == (
                "failed",
                "allocation",
                "ALLOCATION_TIMEOUT",
                None,
                "none",
                "clean",
            )
            assert (
                await session.scalar(
                    text(
                        "select count(*) from ai_call_runtime_command "
                        "where call_id=:call_id and command_type='END_CALL'"
                    ).bindparams(call_id=start.call_id)
                )
                == 0
            )
    finally:
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
            assert end.command_status == "PENDING"
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


async def test_runtime_end_rejects_legacy_record_without_terminal_mutation() -> None:
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
                    idempotency_key="start:legacy-end-rejected",
                    payload={},
                )
            )
            record = await session.scalar(
                select(AiCallRecordModel).where(
                    AiCallRecordModel.tenant_id == "tenant-a",
                    AiCallRecordModel.call_id == start.call_id,
                )
            )
            assert record is not None
            record.runtime_control_mode = "legacy_local"
            await session.flush()

            with pytest.raises(RuntimeControlModeError):
                await repository.request_end(
                    EndCallIntent(
                        tenant_id="tenant-a",
                        call_id=start.call_id,
                        source="web_client",
                        end_reason="user_requested",
                        dedupe_key="web:legacy-end-rejected",
                    )
                )

            assert record.terminal_requested_at is None
            assert record.status == "preparing"
            assert await session.scalar(
                select(AiCallRuntimeCommandModel.id).where(
                    AiCallRuntimeCommandModel.tenant_id == "tenant-a",
                    AiCallRuntimeCommandModel.call_id == start.call_id,
                    AiCallRuntimeCommandModel.command_type == "END_CALL",
                )
            ) is None
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
                _direct_sip_intent(
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


async def test_committed_recovery_takeover_response_loss_does_not_increment_fencing_twice() -> None:
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
                    idempotency_key="start:recovery-response-loss",
                    payload={"business_id": "recovery-response-loss"},
                )
            )
            old_worker = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-recovery-response-old",
                    startup_id=UUID("17171717-1717-4171-8171-171717171717"),
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
                    dedupe_key="recovery-response-loss:owner-lost",
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
                    deployment_instance_id="runtime-recovery-response-new",
                    startup_id=UUID("18181818-1818-4181-8181-181818181818"),
                    capacity=0,
                    cleanup_capacity=1,
                )
            )

        def fail_after_commit(_session) -> None:
            raise ConnectionError("injected committed response loss")

        event.listen(AsyncSession.sync_session_class, "after_commit", fail_after_commit)
        try:
            async with factory() as session:
                takeover = await RecoveryOwnerRepository(session).assign_cleanup_owner(
                    "tenant-a", start.call_id
                )
                assert takeover is not None
                with pytest.raises(ConnectionError, match="committed response loss"):
                    await session.commit()
        finally:
            event.remove(AsyncSession.sync_session_class, "after_commit", fail_after_commit)

        async with factory.begin() as session:
            assert (
                await RecoveryOwnerRepository(session).assign_cleanup_owner(
                    "tenant-a", start.call_id
                )
                is None
            )
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
                        "select active_call_count, active_cleanup_count "
                        "from ai_call_runtime_worker where worker_id=:worker_id"
                    ).bindparams(worker_id=new_worker.worker_id)
                )
            ).one()
            assert tuple(counts) == (0, 1)
    finally:
        await engine.dispose()


async def test_committed_allocation_timeout_response_loss_replays_terminal_fact() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            start = await RuntimeCommandRepository(session).create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:allocation-response-loss",
                    payload={"business_id": "allocation-response-loss"},
                    allocation_deadline_at=await read_database_time(session),
                )
            )

        def fail_after_commit(_session) -> None:
            raise ConnectionError("injected committed response loss")

        event.listen(AsyncSession.sync_session_class, "after_commit", fail_after_commit)
        try:
            async with factory() as session:
                assert await RuntimeCommandRepository(session).expire_unallocated_start(
                    "tenant-a", start.call_id
                )
                with pytest.raises(ConnectionError, match="committed response loss"):
                    await session.commit()
        finally:
            event.remove(AsyncSession.sync_session_class, "after_commit", fail_after_commit)

        async with factory.begin() as session:
            assert (
                await RuntimeCommandRepository(session).expire_unallocated_start(
                    "tenant-a", start.call_id
                )
                is False
            )
            command = (
                await session.execute(
                    text(
                        "select status, error_message from ai_call_runtime_command "
                        "where id=:command_id"
                    ).bindparams(command_id=start.command_id)
                )
            ).one()
            assert tuple(command) == ("DEAD", "ALLOCATION_TIMEOUT")
            assert (
                await session.scalar(
                    text(
                        "select count(*) from ai_call_runtime_command "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
                == 1
            )
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
                _direct_sip_intent(
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
                _direct_sip_intent(
                    idempotency_key="start:dual-race-1",
                    payload={"line_id": 702, "phone_hash": "hash-dual-race-1"},
                )
            )
            second = await commands.create_start_call(
                _direct_sip_intent(
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
                _direct_sip_intent(
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
                    kind=ProviderObservationKind.ACCEPTED,
                    provider_reference=None,
                ),
            )
            assert (
                await session.scalar(
                    text(
                        "select status from ai_call_sip_line_reservation "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
                == "RECONCILE_REQUIRED"
            )

            create_claim = await effects.claim_next(lease)
            assert create_claim is not None
            assert create_claim.effect_id == sip_effect.effect_id
            assert create_claim.reservation_token == reservation_token
            assert await effects.submit(
                create_claim,
                ProviderObservation(
                    kind=ProviderObservationKind.RESOURCE_PRESENT,
                    provider_reference=None,
                ),
            )
            assert (
                await session.scalar(
                    text(
                        "select status from ai_call_sip_line_reservation "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
                == "RECONCILE_REQUIRED"
            )

            create_claim = await effects.claim_next(lease)
            assert create_claim is not None
            assert create_claim.effect_id == sip_effect.effect_id
            assert create_claim.reservation_token == reservation_token
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
                _direct_sip_intent(
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
                _direct_sip_intent(
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


async def test_cleanup_assignment_rechecks_worker_lease_after_lock_wait() -> None:
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
                    idempotency_key="start:cleanup-lock-wait",
                    payload={"business_id": "cleanup-lock-wait"},
                )
            )
            old_worker = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-cleanup-old",
                    startup_id=UUID("12121212-1212-4121-8121-121212121212"),
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
                    dedupe_key="recovery-test:cleanup-lock-wait",
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
            new_worker = await WorkerRegistryRepository(
                session,
                lease_ttl=timedelta(seconds=1),
            ).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-cleanup-new",
                    startup_id=UUID("13131313-1313-4131-8131-131313131313"),
                    capacity=0,
                    cleanup_capacity=1,
                )
            )

        locker = factory()
        await locker.begin()
        try:
            await locker.execute(
                select(AiCallRuntimeWorkerModel)
                .where(AiCallRuntimeWorkerModel.worker_id == new_worker.worker_id)
                .with_for_update()
            )

            async def assign_after_worker_lock() -> OwnerLease | None:
                async with factory.begin() as session:
                    return await RecoveryOwnerRepository(session).assign_cleanup_owner(
                        "tenant-a", start.call_id
                    )

            assign_task = asyncio.create_task(assign_after_worker_lock())
            await asyncio.sleep(1.25)
            await locker.commit()
            lease = await assign_task
        finally:
            await locker.close()

        assert lease is None
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
            assert tuple(record) == (old_worker.worker_id, 1, "active")
            assert (
                await session.scalar(
                    text(
                        "select active_cleanup_count from ai_call_runtime_worker "
                        "where worker_id=:worker_id"
                    ).bindparams(worker_id=new_worker.worker_id)
                )
                == 0
            )
    finally:
        await engine.dispose()


async def test_effect_submit_rechecks_owner_lease_after_record_lock_wait() -> None:
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
                    idempotency_key="start:effect-lock-wait",
                    payload={"business_id": "effect-lock-wait"},
                )
            )
            await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-effect-lock-wait",
                    startup_id=UUID("14141414-1414-4141-8141-141414141414"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            lease = await DispatcherOwnerRepository(
                session,
                lease_ttl=timedelta(seconds=1),
            ).assign_initial_owner("tenant-a", start.call_id)
            assert lease is not None
            start_claim = await commands.claim_next_for_owner(lease)
            assert start_claim is not None
            await effects.register(
                start_claim,
                EffectSpec(
                    effect_type="CREATE_ROOM",
                    idempotency_key="effect:create-room:lock-wait",
                    provider_namespace="stub:lock-wait",
                    provider_idempotency_key="provider:create-room:lock-wait",
                    resource_key="room:lock-wait",
                    resource_generation=lease.fencing_token,
                ),
            )
            assert await commands.complete(
                start_claim,
                CommandDecision(status=CommandStatus.SUCCEEDED),
            )
            effect_claim = await effects.claim_next(lease)
            assert effect_claim is not None

        locker = factory()
        await locker.begin()
        try:
            await locker.execute(
                select(AiCallRecordModel)
                .where(
                    AiCallRecordModel.tenant_id == "tenant-a",
                    AiCallRecordModel.call_id == start.call_id,
                )
                .with_for_update()
            )

            async def submit_after_record_lock() -> bool:
                async with factory.begin() as session:
                    return await RuntimeEffectRepository(session).submit(
                        effect_claim,
                        ProviderObservation(
                            kind=ProviderObservationKind.RESOURCE_PRESENT,
                            provider_reference="room-ref",
                        ),
                    )

            submit_task = asyncio.create_task(submit_after_record_lock())
            await asyncio.sleep(1.25)
            await locker.commit()
            submitted = await submit_task
        finally:
            await locker.close()

        assert submitted is False
        async with factory() as session:
            state = (
                await session.execute(
                    text(
                        "select status, processing_token from ai_call_runtime_effect "
                        "where id=:effect_id"
                    ).bindparams(effect_id=effect_claim.effect_id)
                )
            ).one()
            assert tuple(state) == ("APPLYING", effect_claim.processing_token)
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
                _direct_sip_intent(
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
                _direct_sip_intent(
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


async def test_attention_parking_rechecks_database_time_after_record_lock_wait() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    locker = factory()
    parking = factory()
    try:
        async with factory.begin() as session:
            commands = RuntimeCommandRepository(session)
            start = await commands.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:lock-wait-deadline",
                    payload={"business_id": "lock-wait-deadline"},
                )
            )
            await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-lock-wait",
                    startup_id=UUID("77777777-7777-4777-8777-777777777777"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            active_lease = await DispatcherOwnerRepository(
                session,
                lease_ttl=timedelta(seconds=1),
            ).assign_initial_owner("tenant-a", start.call_id)
            assert active_lease is not None
            end = await commands.request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    source="runtime_recovery",
                    end_reason="lock_wait_deadline",
                    dedupe_key="runtime-recovery:lock-wait-deadline",
                )
            )
            now = await read_database_time(session)
            session.add(
                AiCallRuntimeEffectModel(
                    id=93011,
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    command_id=end.command_id,
                    effect_type="CREATE_ROOM",
                    idempotency_key="effect:lock-wait-deadline",
                    fencing_token=active_lease.fencing_token,
                    status="RECONCILE_REQUIRED",
                    provider_namespace="stub:lock-wait-deadline",
                    provider_idempotency_key="provider:lock-wait-deadline",
                    resource_key="room:lock-wait-deadline",
                    resource_generation=active_lease.fencing_token,
                    created_at=now,
                    updated_at=now,
                )
            )
            await session.flush()

        await locker.begin()
        await locker.scalar(
            select(AiCallRecordModel)
            .where(AiCallRecordModel.call_id == start.call_id)
            .with_for_update()
        )

        async def park_after_lock_wait() -> bool:
            async with parking.begin():
                return await RecoveryOwnerRepository(parking).park_attention(
                    active_lease,
                    timedelta(seconds=30),
                )

        park_task = asyncio.create_task(park_after_lock_wait())
        await asyncio.sleep(1.25)
        await locker.commit()
        assert await park_task is False

        async with factory() as session:
            record = (
                await session.execute(
                    text(
                        "select runtime_owner_id, runtime_capacity_class "
                        "from ai_call_record where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            assert tuple(record) == (active_lease.owner_id, "active")
    finally:
        if locker.in_transaction():
            await locker.rollback()
        await locker.close()
        await parking.close()
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
                _direct_sip_intent(
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

        async with factory() as session:
            ready_record = await session.scalar(
                select(AiCallRecordModel).where(
                    AiCallRecordModel.tenant_id == "tenant-a",
                    AiCallRecordModel.call_id == start.call_id,
                )
            )
            assert ready_record is not None
            assert ready_record.status == "ready"
            assert (
                ready_record.agent_participant_identity
                == f"agent-{start.call_id}-g{lease.fencing_token}"
            )
            assert ready_record.agent_participant_sid == f"stub:{agent_key}"
            assert (
                ready_record.agent_audio_track_sid
                == f"stub-track-{start.call_id}-g{lease.fencing_token}"
            )
            assert ready_record.agent_resource_generation == lease.fencing_token
            assert ready_record.agent_media_ready_at is not None
            token_gate = await RuntimeTokenGateRepository(session).authorize(
                tenant_id="tenant-a",
                call_id=start.call_id,
            )
            assert token_gate.participant_identity == f"caller-{start.call_id}"

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


async def test_start_handler_recovers_ready_after_effect_commit_before_command_commit() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    provider = ScriptedProviderStub({})
    try:
        async with factory.begin() as session:
            commands = RuntimeCommandRepository(session)
            start = await commands.create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="start:ready-after-effect-commit",
                    payload={"business_id": "ready-after-effect-commit"},
                )
            )
            await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="runtime-ready-recovery",
                    startup_id=UUID("45454545-4545-4545-8545-454545454545"),
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

        specs = [
            EffectSpec(
                effect_type="CREATE_ROOM",
                idempotency_key="effect:create-room:ready-recovery",
                provider_namespace="stub:ready-recovery",
                provider_idempotency_key="provider:create-room:ready-recovery",
                resource_key="room:ready-recovery:g1",
                resource_generation=lease.fencing_token,
            ),
            EffectSpec(
                effect_type="ATTACH_AGENT_PARTICIPANT",
                idempotency_key="effect:attach-agent:ready-recovery",
                provider_namespace="stub:ready-recovery",
                provider_idempotency_key="provider:attach-agent:ready-recovery",
                resource_key="agent:ready-recovery:g1",
                resource_generation=lease.fencing_token,
            ),
        ]
        async with factory.begin() as session:
            effects = RuntimeEffectRepository(session)
            for spec in specs:
                await effects.register(start_claim, spec)

        for spec in specs:
            async with factory.begin() as session:
                effect_claim = await RuntimeEffectRepository(session).claim_next(lease)
                assert effect_claim is not None
                assert effect_claim.effect_type == spec.effect_type
            async with factory.begin() as session:
                assert await RuntimeEffectRepository(session).submit(
                    effect_claim,
                    ProviderObservation(
                        kind=ProviderObservationKind.RESOURCE_PRESENT,
                        provider_reference=f"persisted:{effect_claim.resource_key}",
                    ),
                )

        stale_lease = replace(lease, owner_id="runtime-stale")
        with pytest.raises(StartReadinessRejected):
            await StartCallHandler(factory, provider).handle(
                start_claim,
                stale_lease,
                specs,
            )

        stale_claim = replace(
            start_claim,
            processing_owner_id="runtime-stale",
        )
        stale_result = await StartCallHandler(factory, provider).handle(
            stale_claim,
            stale_lease,
            specs,
        )
        assert stale_result.command_completed is False

        async with factory() as session:
            unchanged = (
                await session.execute(
                    text(
                        "select r.status, c.status, r.last_applied_command_seq "
                        "from ai_call_record r "
                        "join ai_call_runtime_command c on c.call_id=r.call_id "
                        "where r.call_id=:call_id and c.id=:command_id"
                    ).bindparams(
                        call_id=start.call_id,
                        command_id=start_claim.command_id,
                    )
                )
            ).one()
            assert tuple(unchanged) == ("preparing", "PROCESSING", 0)

        result = await StartCallHandler(factory, provider).handle(
            start_claim,
            lease,
            specs,
        )

        assert result.command_completed is True
        assert result.applied_effect_count == 2
        assert provider.calls == []
        async with factory() as session:
            record = await session.scalar(
                select(AiCallRecordModel).where(
                    AiCallRecordModel.tenant_id == "tenant-a",
                    AiCallRecordModel.call_id == start.call_id,
                )
            )
            assert record is not None
            assert record.status == "ready"
            assert (
                record.agent_participant_sid
                == "persisted:agent:ready-recovery:g1"
            )
            assert record.agent_resource_generation == lease.fencing_token
            assert record.agent_media_ready_at is not None
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


async def test_postgres_wakeup_is_delivered_on_commit_but_not_rollback() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    factory = async_sessionmaker(engine, expire_on_commit=False)
    listener = PostgresWakeupListener(engine)
    stop_event = asyncio.Event()
    try:
        assert await listener.start() is True
        async with factory.begin() as session:
            await publish_control_wakeup(session)
            assert (
                await listener.wait(timeout_seconds=0.05, stop_event=stop_event)
                is False
            )

        assert (
            await listener.wait(timeout_seconds=0.5, stop_event=stop_event) is True
        )

        async with factory() as session:
            transaction = await session.begin()
            await publish_control_wakeup(session)
            await transaction.rollback()

        assert (
            await listener.wait(timeout_seconds=0.05, stop_event=stop_event)
            is False
        )
        assert listener.notification_count == 1
    finally:
        await listener.stop()
        await engine.dispose()


async def test_postgres_wakeup_forged_payload_creates_no_business_fact() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    listener = PostgresWakeupListener(engine)
    dispatcher = DispatcherControlService(
        factory,
        scan_interval_seconds=30,
        wakeup_listener=listener,
    )
    try:
        await dispatcher.start()
        async with factory.begin() as session:
            await session.execute(
                text("select pg_notify(:channel, :payload)"),
                {
                    "channel": CONTROL_WAKEUP_CHANNEL,
                    "payload": "tenant-a:forged-call-id",
                },
            )

        for _ in range(50):
            if listener.notification_count == 1:
                break
            await asyncio.sleep(0.01)
        assert listener.notification_count == 1

        async with factory() as session:
            assert await session.scalar(text("select count(*) from ai_call_record")) == 0
            assert (
                await session.scalar(
                    text("select count(*) from ai_call_runtime_command")
                )
                == 0
            )
    finally:
        await dispatcher.stop()
        await engine.dispose()


async def test_postgres_wakeup_periodic_scan_recovers_without_listener() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    dispatcher = DispatcherControlService(
        factory,
        scan_interval_seconds=0.01,
    )
    try:
        async with factory.begin() as session:
            await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="wakeup-fallback",
                    startup_id=UUID("50505050-5050-4050-8050-505050505050"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
        await dispatcher.start()
        async with factory.begin() as session:
            start = await RuntimeCommandRepository(session).create_start_call(
                StartCallIntent(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="postgres-wakeup:fallback",
                    payload={"business_id": "fallback"},
                )
            )

        owner_id = None
        for _ in range(100):
            async with factory() as session:
                owner_id = await session.scalar(
                    text(
                        "select runtime_owner_id from ai_call_record "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            if owner_id is not None:
                break
            await asyncio.sleep(0.01)

        assert owner_id is not None
    finally:
        await dispatcher.stop()
        await engine.dispose()


async def test_postgres_wakeup_two_dispatchers_and_runtimes_keep_single_winner() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    settings = SimpleNamespace(AI_CALL_OWNER_COMMAND_V1_ENTRIES="web")
    provider_a = DeterministicWebProviderStub()
    provider_b = DeterministicWebProviderStub()
    runtimes = (
        RuntimeControlService(
            worker_id=build_worker_id(
                "wakeup-runtime-a",
                UUID("60606060-6060-4060-8060-606060606060"),
            ),
            registry=RuntimeRegistry(),
            session_factory=factory,
            provider=provider_a,
            scan_interval_seconds=30,
            wakeup_listener=PostgresWakeupListener(engine),
        ),
        RuntimeControlService(
            worker_id=build_worker_id(
                "wakeup-runtime-b",
                UUID("70707070-7070-4070-8070-707070707070"),
            ),
            registry=RuntimeRegistry(),
            session_factory=factory,
            provider=provider_b,
            scan_interval_seconds=30,
            wakeup_listener=PostgresWakeupListener(engine),
        ),
    )
    dispatchers = (
        DispatcherControlService(
            factory,
            scan_interval_seconds=30,
            wakeup_listener=PostgresWakeupListener(engine),
        ),
        DispatcherControlService(
            factory,
            scan_interval_seconds=30,
            wakeup_listener=PostgresWakeupListener(engine),
        ),
    )
    started_services: list[object] = []
    try:
        for service in (*runtimes, *dispatchers):
            await service.start()
            started_services.append(service)

        async with factory.begin() as session:
            start = await RuntimeEntryStartService(
                settings=settings,
                repository=RuntimeCommandRepository(session),
            ).submit(
                StartEntryRequest(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="postgres-wakeup:single-winner",
                    payload={"voice": "voice-a", "business_id": "single-winner"},
                    business_id="single-winner",
                    allocation_timeout_seconds=30,
                )
            )
            assert start is not None

        start_status = None
        for _ in range(200):
            async with factory() as session:
                start_status = await session.scalar(
                    text(
                        "select status from ai_call_runtime_command "
                        "where id=:command_id"
                    ).bindparams(command_id=start.command_id)
                )
            if start_status == "SUCCEEDED":
                break
            await asyncio.sleep(0.01)
        assert start_status == "SUCCEEDED"

        async with factory.begin() as session:
            end = await RuntimeCommandRepository(session).request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    source="web_client",
                    end_reason="user_requested",
                    dedupe_key="postgres-wakeup:single-winner:end",
                )
            )

        end_status = None
        for _ in range(200):
            async with factory() as session:
                end_status = await session.scalar(
                    text(
                        "select status from ai_call_runtime_command "
                        "where id=:command_id"
                    ).bindparams(command_id=end.command_id)
                )
            if end_status == "SUCCEEDED":
                break
            await asyncio.sleep(0.01)
        assert end_status == "SUCCEEDED"

        async with factory() as session:
            record = (
                await session.execute(
                    text(
                        "select status, resource_cleanup_status, runtime_owner_id, "
                        "runtime_capacity_class from ai_call_record "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            assert tuple(record) == ("completed", "clean", None, "none")
            worker_counts = (
                await session.execute(
                    text(
                        "select active_call_count, active_cleanup_count "
                        "from ai_call_runtime_worker order by worker_id"
                    )
                )
            ).all()
            assert worker_counts == [(0, 0), (0, 0)]
            dispatch_fields_written = await session.scalar(
                text(
                    "select count(*) from ai_call_runtime_command where "
                    "dispatch_token is not null or dispatch_expires_at is not null or "
                    "published_at is not null or stream_message_id is not null"
                )
            )
            assert dispatch_fields_written == 0

        provider_calls = provider_a.calls + provider_b.calls
        assert len(provider_calls) == 4
        assert len(
            {(call["effect_type"], call["resource_key"]) for call in provider_calls}
        ) == 4
    finally:
        for service in reversed(started_services):
            await service.stop()
        await engine.dispose()


async def test_web_db_only_two_dispatchers_and_runtimes_complete_start_end_loop() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    settings = SimpleNamespace(AI_CALL_OWNER_COMMAND_V1_ENTRIES="web")
    try:
        async with factory.begin() as session:
            start = await RuntimeEntryStartService(
                settings=settings,
                repository=RuntimeCommandRepository(session),
            ).submit(
                StartEntryRequest(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="web:db-only:full-loop",
                    payload={
                        "voice": "voice-a",
                        "business_id": "business-full-loop",
                        "scene_code": "collection",
                        "business_params": {"case": "full-loop"},
                    },
                    business_id="business-full-loop",
                    scene_code="collection",
                    allocation_timeout_seconds=30,
                )
            )
            assert start is not None
            worker_a = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="web-runtime-a",
                    startup_id=UUID("10101010-1010-4010-8010-101010101010"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            worker_b = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="web-runtime-b",
                    startup_id=UUID("20202020-2020-4020-8020-202020202020"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )

        dispatcher_counts = await asyncio.gather(
            DispatcherControlService(factory).run_once(),
            DispatcherControlService(factory).run_once(),
        )
        assert sum(dispatcher_counts) == 1

        provider_a = DeterministicWebProviderStub()
        provider_b = DeterministicWebProviderStub()
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

        start_counts = await asyncio.gather(
            runtime_a.run_once(),
            runtime_b.run_once(),
        )
        assert sorted(start_counts) == [0, 1]

        async with factory() as session:
            start_query = await RuntimeCommandRepository(session).get_command(
                tenant_id="tenant-a",
                command_id=start.command_id,
            )
            assert start_query is not None
            assert start_query.status == "SUCCEEDED"
            bootstrap = await RuntimeBootstrapService(session).get(
                tenant_id="tenant-a",
                call_id=start.call_id,
            )
            assert bootstrap.phase == "ready"
            assert bootstrap.token_available is False

        async with factory.begin() as session:
            end = await RuntimeCommandRepository(session).request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    source="web_client",
                    end_reason="user_requested",
                    dedupe_key="web:db-only:full-loop:end",
                )
            )

        end_counts = await asyncio.gather(
            runtime_a.run_once(),
            runtime_b.run_once(),
        )
        assert sorted(end_counts) == [0, 1]

        async with factory() as session:
            end_query = await RuntimeCommandRepository(session).get_command(
                tenant_id="tenant-a",
                command_id=end.command_id,
            )
            assert end_query is not None
            assert end_query.status == "SUCCEEDED"
            bootstrap = await RuntimeBootstrapService(session).get(
                tenant_id="tenant-a",
                call_id=start.call_id,
            )
            assert bootstrap.phase == "terminal"
            assert bootstrap.token_available is False

            record = (
                await session.execute(
                    text(
                        "select status, resource_cleanup_status, runtime_owner_id, "
                        "runtime_capacity_class, entry_type from ai_call_record "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            assert tuple(record) == ("completed", "clean", None, "none", "web")

            worker_counts = (
                await session.execute(
                    text(
                        "select active_call_count, active_cleanup_count "
                        "from ai_call_runtime_worker order by worker_id"
                    )
                )
            ).all()
            assert worker_counts == [(0, 0), (0, 0)]
            assert (
                await session.scalar(
                    text("select count(*) from ai_call_sip_line_reservation")
                )
                == 0
            )
            assert await session.scalar(text("select count(*) from ai_call_sip_line")) == 0
            assert (
                await session.scalar(
                    text("select count(*) from ai_call_record where entry_type <> 'web'")
                )
                == 0
            )

            commands = (
                await session.execute(
                    text(
                        "select command_type, status, dispatch_token, "
                        "dispatch_expires_at, published_at, stream_message_id "
                        "from ai_call_runtime_command where call_id=:call_id "
                        "order by command_seq"
                    ).bindparams(call_id=start.call_id)
                )
            ).all()
            assert commands == [
                ("START_CALL", "SUCCEEDED", None, None, None, None),
                ("END_CALL", "SUCCEEDED", None, None, None, None),
            ]

        provider_calls = provider_a.calls + provider_b.calls
        expected_effect_calls = {
            ("CREATE_ROOM", f"room:{start.call_id}:g1"),
            ("ATTACH_AGENT_PARTICIPANT", f"agent:{start.call_id}:g1"),
            ("DISCONNECT_AGENT_PARTICIPANT", f"agent:{start.call_id}:g1"),
            ("DELETE_ROOM", f"room:{start.call_id}:g1"),
        }
        assert {
            (call["effect_type"], call["resource_key"]) for call in provider_calls
        } == expected_effect_calls
        assert len(provider_calls) == len(expected_effect_calls)
    finally:
        await engine.dispose()


async def test_direct_sip_db_only_two_dispatchers_and_runtimes_complete_start_end_loop(
) -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    settings = SimpleNamespace(AI_CALL_OWNER_COMMAND_V1_ENTRIES="direct_sip")
    try:
        async with factory.begin() as session:
            start = await RuntimeEntryStartService(
                settings=settings,
                repository=RuntimeCommandRepository(session),
            ).submit(
                StartEntryRequest(
                    tenant_id="tenant-a",
                    entry_type="direct_sip",
                    idempotency_key="direct-sip:db-only:full-loop",
                    payload={
                        "voice": "voice-a",
                        "business_id": "business-direct-sip",
                        "scene_code": "collection",
                        "business_params": {"case": "direct-sip-full-loop"},
                        "ringing_timeout_seconds": 30,
                    },
                    business_id="business-direct-sip",
                    scene_code="collection",
                    allocation_timeout_seconds=30,
                    callee_phone_number="13812345678",
                )
            )
            assert start is not None
            worker_a = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="direct-sip-runtime-a",
                    startup_id=UUID("30303030-3030-4030-8030-303030303030"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            worker_b = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="direct-sip-runtime-b",
                    startup_id=UUID("40404040-4040-4040-8040-404040404040"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )

        dispatcher_counts = await asyncio.gather(
            DispatcherControlService(factory).run_once(),
            DispatcherControlService(factory).run_once(),
        )
        assert sum(dispatcher_counts) == 1

        provider_a = DeterministicDbOnlyProviderStub()
        provider_b = DeterministicDbOnlyProviderStub()
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

        start_counts = await asyncio.gather(
            runtime_a.run_once(),
            runtime_b.run_once(),
        )
        assert sorted(start_counts) == [0, 1]

        async with factory() as session:
            start_query = await RuntimeCommandRepository(session).get_command(
                tenant_id="tenant-a",
                command_id=start.command_id,
            )
            assert start_query is not None
            assert start_query.status == "SUCCEEDED"
            record_and_command = (
                await session.execute(
                    text(
                        "select r.callee_phone_number, "
                        "r.callee_phone_number_masked, c.payload_json, "
                        "c.sensitive_payload_ciphertext, c.payload_key_version "
                        "from ai_call_record r join ai_call_runtime_command c "
                        "on c.tenant_id=r.tenant_id and c.call_id=r.call_id "
                        "where r.call_id=:call_id and c.command_type='START_CALL'"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            assert record_and_command.callee_phone_number == "13812345678"
            assert record_and_command.callee_phone_number_masked == "138****5678"
            assert "13812345678" not in (record_and_command.payload_json or "")
            assert record_and_command.sensitive_payload_ciphertext is None
            assert record_and_command.payload_key_version is None

            create_effects = (
                await session.execute(
                    text(
                        "select effect_type, status from ai_call_runtime_effect "
                        "where call_id=:call_id and effect_type in "
                        "('CREATE_ROOM', 'ATTACH_AGENT_PARTICIPANT', "
                        "'CREATE_SIP_PARTICIPANT') order by effect_type"
                    ).bindparams(call_id=start.call_id)
                )
            ).all()
            assert {effect.effect_type for effect in create_effects} == {
                "CREATE_ROOM",
                "ATTACH_AGENT_PARTICIPANT",
                "CREATE_SIP_PARTICIPANT",
            }
            assert {effect.status for effect in create_effects} == {"APPLIED"}

        async with factory.begin() as session:
            end = await RuntimeCommandRepository(session).request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id=start.call_id,
                    source="direct_sip_client",
                    end_reason="user_requested",
                    dedupe_key="direct-sip:db-only:full-loop:end",
                )
            )

        end_counts = await asyncio.gather(
            runtime_a.run_once(),
            runtime_b.run_once(),
        )
        assert sorted(end_counts) == [0, 1]

        async with factory() as session:
            end_query = await RuntimeCommandRepository(session).get_command(
                tenant_id="tenant-a",
                command_id=end.command_id,
            )
            assert end_query is not None
            assert end_query.status == "SUCCEEDED"
            record = (
                await session.execute(
                    text(
                        "select status, resource_cleanup_status, runtime_owner_id, "
                        "runtime_capacity_class, entry_type, callee_phone_number "
                        "from ai_call_record where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            assert tuple(record) == (
                "completed",
                "clean",
                None,
                "none",
                "direct_sip",
                "13812345678",
            )

            destroy_effects = (
                await session.execute(
                    text(
                        "select effect_type, status from ai_call_runtime_effect "
                        "where call_id=:call_id and effect_type in "
                        "('HANGUP_SIP', 'DISCONNECT_AGENT_PARTICIPANT', "
                        "'DELETE_ROOM') order by effect_type"
                    ).bindparams(call_id=start.call_id)
                )
            ).all()
            assert {effect.effect_type for effect in destroy_effects} == {
                "HANGUP_SIP",
                "DISCONNECT_AGENT_PARTICIPANT",
                "DELETE_ROOM",
            }
            assert {effect.status for effect in destroy_effects} == {"APPLIED"}
            assert (
                await session.scalar(
                    text(
                        "select count(*) from ai_call_sip_line_reservation "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
                == 0
            )
            worker_counts = (
                await session.execute(
                    text(
                        "select active_call_count, active_cleanup_count "
                        "from ai_call_runtime_worker order by worker_id"
                    )
                )
            ).all()
            assert worker_counts == [(0, 0), (0, 0)]

        provider_calls = provider_a.calls + provider_b.calls
        assert len(provider_calls) == 6
        assert all(
            set(call) == {"provider_namespace", "effect_type", "resource_key"}
            for call in provider_calls
        )
        assert len(
            {(call["effect_type"], call["resource_key"]) for call in provider_calls}
        ) == 6
        assert "13812345678" not in json.dumps(provider_calls, ensure_ascii=False)
    finally:
        await engine.dispose()


async def test_web_db_only_waits_without_worker_then_expires_from_database_deadline() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    settings = SimpleNamespace(AI_CALL_OWNER_COMMAND_V1_ENTRIES="web")
    try:
        async with factory.begin() as session:
            start = await RuntimeEntryStartService(
                settings=settings,
                repository=RuntimeCommandRepository(session),
            ).submit(
                StartEntryRequest(
                    tenant_id="tenant-a",
                    entry_type="web",
                    idempotency_key="web:db-only:allocation-timeout",
                    payload={"voice": "voice-a"},
                    allocation_timeout_seconds=30,
                )
            )
            assert start is not None

        async with factory() as session:
            pending = await RuntimeCommandRepository(session).get_command(
                tenant_id="tenant-a",
                command_id=start.command_id,
            )
            assert pending is not None
            assert pending.status == "PENDING"
            bootstrap = await RuntimeBootstrapService(session).get(
                tenant_id="tenant-a",
                call_id=start.call_id,
            )
            assert bootstrap.phase == "starting"
            assert bootstrap.token_available is False

        async with factory.begin() as session:
            await session.execute(
                text(
                    "update ai_call_runtime_command set "
                    "allocation_deadline_at=clock_timestamp() - interval '1 second' "
                    "where id=:command_id"
                ).bindparams(command_id=start.command_id)
            )

        assert await DispatcherControlService(factory).run_once() == 1

        async with factory() as session:
            expired = await RuntimeCommandRepository(session).get_command(
                tenant_id="tenant-a",
                command_id=start.command_id,
            )
            assert expired is not None
            assert expired.status == "DEAD"
            assert expired.result == {"error": "ALLOCATION_TIMEOUT"}
            assert expired.error_message == "ALLOCATION_TIMEOUT"
            record = (
                await session.execute(
                    text(
                        "select status, failure_stage, failure_message, "
                        "resource_cleanup_status, runtime_owner_id "
                        "from ai_call_record where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
            ).one()
            assert tuple(record) == (
                "failed",
                "allocation",
                "ALLOCATION_TIMEOUT",
                "clean",
                None,
            )
            assert (
                await session.scalar(
                    text(
                        "select count(*) from ai_call_runtime_effect "
                        "where call_id=:call_id"
                    ).bindparams(call_id=start.call_id)
                )
                == 0
            )
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


async def test_web_db_only_idempotency_and_terminal_barrier_leave_no_extra_rows() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    settings = SimpleNamespace(AI_CALL_OWNER_COMMAND_V1_ENTRIES="web")
    request = StartEntryRequest(
        tenant_id="tenant-a",
        entry_type="web",
        idempotency_key="web:db-only:idempotency",
        payload={"voice": "voice-a", "business_id": "business-a"},
        business_id="business-a",
        allocation_timeout_seconds=30,
    )
    try:
        async with factory.begin() as session:
            service = RuntimeEntryStartService(
                settings=settings,
                repository=RuntimeCommandRepository(session),
            )
            first = await service.submit(request)
            repeated = await service.submit(request)
            assert first is not None
            assert repeated == first
            with pytest.raises(IdempotencyConflictError):
                await service.submit(
                    replace(
                        request,
                        payload={"voice": "voice-b", "business_id": "business-a"},
                    )
                )

            worker = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="web-runtime-barrier",
                    startup_id=UUID("30303030-3030-4030-8030-303030303030"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            lease = await DispatcherOwnerRepository(session).assign_initial_owner(
                "tenant-a",
                first.call_id,
            )
            assert lease is not None
            claim = await RuntimeCommandRepository(session).claim_next_for_owner(lease)
            assert claim is not None
            await RuntimeCommandRepository(session).request_end(
                EndCallIntent(
                    tenant_id="tenant-a",
                    call_id=first.call_id,
                    source="web_client",
                    end_reason="user_requested",
                    dedupe_key="web:db-only:idempotency:end",
                )
            )

            with pytest.raises(TerminalBarrierError):
                await RuntimeCommandRepository(session).append_command(
                    CommandIntent(
                        tenant_id="tenant-a",
                        call_id=first.call_id,
                        command_type="UPDATE_PROMPT",
                        idempotency_key="web:db-only:late-command",
                        payload={},
                    )
                )
            with pytest.raises(EffectRegistrationError):
                await RuntimeEffectRepository(session).register(
                    claim,
                    EffectSpec(
                        effect_type="CREATE_ROOM",
                        idempotency_key="web:db-only:late-effect",
                        provider_namespace=f"stub:{worker.worker_id}",
                        provider_idempotency_key="web:db-only:late-effect",
                        resource_key=f"room:{first.call_id}:g1",
                        resource_generation=lease.fencing_token,
                    ),
                )

        async with factory() as session:
            assert await session.scalar(text("select count(*) from ai_call_record")) == 1
            assert (
                await session.scalar(
                    text(
                        "select count(*) from ai_call_runtime_command "
                        "where command_type='START_CALL'"
                    )
                )
                == 1
            )
            assert (
                await session.scalar(text("select count(*) from ai_call_runtime_effect"))
                == 0
            )
    finally:
        await engine.dispose()


async def test_web_db_only_latency_measurement() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    settings = SimpleNamespace(AI_CALL_OWNER_COMMAND_V1_ENTRIES="web")
    runtime: RuntimeControlService | None = None
    try:
        async with factory.begin() as session:
            worker = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="web-runtime-latency",
                    startup_id=UUID("40404040-4040-4040-8040-404040404040"),
                    capacity=64,
                    cleanup_capacity=64,
                )
            )
            service = RuntimeEntryStartService(
                settings=settings,
                repository=RuntimeCommandRepository(session),
            )
            starts = []
            for index in range(20):
                start = await service.submit(
                    StartEntryRequest(
                        tenant_id="tenant-a",
                        entry_type="web",
                        idempotency_key=f"web:db-only:latency:{index}",
                        payload={
                            "voice": "voice-a",
                            "business_id": f"latency-{index}",
                            "scene_code": "collection",
                            "business_params": {"sample": index},
                        },
                        business_id=f"latency-{index}",
                        scene_code="collection",
                        allocation_timeout_seconds=30,
                    )
                )
                assert start is not None
                starts.append(start)

        assigned = await DispatcherControlService(factory).run_once()
        assert assigned == len(starts)

        provider = DeterministicWebProviderStub()
        runtime = RuntimeControlService(
            worker_id=worker.worker_id,
            registry=RuntimeRegistry(),
            session_factory=factory,
            provider=provider,
            batch_size=64,
        )
        assert await runtime.run_once() == len(starts)
        assert await runtime.run_once() == 0

        async with factory() as session:
            latency_rows = (
                await session.execute(
                    text(
                        "select extract(epoch from (claimed_at - created_at)) * 1000 "
                        "from ai_call_runtime_command "
                        "where command_type='START_CALL' order by created_at, id"
                    )
                )
            ).all()
            latencies_ms = sorted(float(row[0]) for row in latency_rows)
            backlog = await session.scalar(
                text(
                    "select count(*) from ai_call_runtime_command "
                    "where status in ('PENDING', 'RETRY_WAIT', 'PROCESSING')"
                )
            )
            worker_state = (
                await session.execute(
                    text(
                        "select active_call_count, capacity "
                        "from ai_call_runtime_worker where worker_id=:worker_id"
                    ).bindparams(worker_id=worker.worker_id)
                )
            ).one()
            dispatch_fields_written = await session.scalar(
                text(
                    "select count(*) from ai_call_runtime_command where "
                    "dispatch_token is not null or dispatch_expires_at is not null or "
                    "published_at is not null or stream_message_id is not null"
                )
            )
            server_version_num = await session.scalar(
                text("select current_setting('server_version_num')::int")
            )
            isolation_level = await session.scalar(
                text("show transaction_isolation")
            )

        assert len(latencies_ms) == len(starts)
        assert backlog == 0
        assert worker_state.active_call_count == len(starts)
        assert worker_state.active_call_count < worker_state.capacity
        assert dispatch_fields_written == 0
        assert len(provider.calls) == len(starts) * 2

        p50_index = (50 * len(latencies_ms) + 99) // 100 - 1
        p95_index = (95 * len(latencies_ms) + 99) // 100 - 1
        metrics = {
            "sample_count": len(latencies_ms),
            "p50_ms": round(latencies_ms[p50_index], 3),
            "p95_ms": round(latencies_ms[p95_index], 3),
            "max_ms": round(latencies_ms[-1], 3),
            "scan_backlog_remaining": int(backlog),
            "worker_active_count": int(worker_state.active_call_count),
            "worker_capacity": int(worker_state.capacity),
            "dispatch_or_stream_fields_written": int(dispatch_fields_written),
            "postgres_server_version_num": int(server_version_num),
            "isolation_level": str(isolation_level),
        }
        print("WEB_DB_ONLY_LATENCY " + json.dumps(metrics, sort_keys=True))
    finally:
        if runtime is not None:
            await runtime.stop()
        await engine.dispose()


async def test_postgres_wakeup_latency_measurement() -> None:
    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    settings = SimpleNamespace(AI_CALL_OWNER_COMMAND_V1_ENTRIES="web")
    listener_runtime = PostgresWakeupListener(engine)
    listener_dispatcher = PostgresWakeupListener(engine)
    worker_id = build_worker_id(
        "postgres-wakeup-latency",
        UUID("80808080-8080-4080-8080-808080808080"),
    )
    provider = DeterministicWebProviderStub()
    runtime = RuntimeControlService(
        worker_id=worker_id,
        registry=RuntimeRegistry(),
        session_factory=factory,
        provider=provider,
        capacity=64,
        cleanup_capacity=64,
        batch_size=64,
        scan_interval_seconds=30,
        wakeup_listener=listener_runtime,
    )
    dispatcher = DispatcherControlService(
        factory,
        batch_size=64,
        scan_interval_seconds=30,
        wakeup_listener=listener_dispatcher,
    )
    runtime_started = False
    dispatcher_started = False
    try:
        await runtime.start()
        runtime_started = True
        await dispatcher.start()
        dispatcher_started = True
        starts = []
        for index in range(20):
            async with factory.begin() as session:
                start = await RuntimeEntryStartService(
                    settings=settings,
                    repository=RuntimeCommandRepository(session),
                ).submit(
                    StartEntryRequest(
                        tenant_id="tenant-a",
                        entry_type="web",
                        idempotency_key=f"postgres-wakeup:latency:{index}",
                        payload={
                            "voice": "voice-a",
                            "business_id": f"latency-{index}",
                            "scene_code": "collection",
                            "business_params": {"sample": index},
                        },
                        business_id=f"latency-{index}",
                        scene_code="collection",
                        allocation_timeout_seconds=30,
                    )
                )
                assert start is not None
                starts.append(start)

            claimed_at = None
            for _ in range(200):
                async with factory() as session:
                    claimed_at = await session.scalar(
                        text(
                            "select claimed_at from ai_call_runtime_command "
                            "where id=:command_id"
                        ).bindparams(command_id=start.command_id)
                    )
                if claimed_at is not None:
                    break
                await asyncio.sleep(0.005)
            assert claimed_at is not None

        backlog = None
        for _ in range(400):
            async with factory() as session:
                backlog = await session.scalar(
                    text(
                        "select count(*) from ai_call_runtime_command "
                        "where status in ('PENDING', 'RETRY_WAIT', 'PROCESSING')"
                    )
                )
            if backlog == 0:
                break
            await asyncio.sleep(0.005)
        assert backlog == 0

        async with factory() as session:
            latency_rows = (
                await session.execute(
                    text(
                        "select extract(epoch from (claimed_at - created_at)) * 1000 "
                        "from ai_call_runtime_command "
                        "where command_type='START_CALL' order by created_at, id"
                    )
                )
            ).all()
            latencies_ms = sorted(float(row[0]) for row in latency_rows)
            worker_state = (
                await session.execute(
                    text(
                        "select active_call_count, capacity "
                        "from ai_call_runtime_worker where worker_id=:worker_id"
                    ).bindparams(worker_id=worker_id)
                )
            ).one()
            dispatch_fields_written = await session.scalar(
                text(
                    "select count(*) from ai_call_runtime_command where "
                    "dispatch_token is not null or dispatch_expires_at is not null or "
                    "published_at is not null or stream_message_id is not null"
                )
            )
            server_version_num = await session.scalar(
                text("select current_setting('server_version_num')::int")
            )
            isolation_level = await session.scalar(text("show transaction_isolation"))

        assert len(latencies_ms) == len(starts) == 20
        assert worker_state.active_call_count == len(starts)
        assert worker_state.active_call_count < worker_state.capacity
        assert dispatch_fields_written == 0
        assert len(provider.calls) == len(starts) * 2

        p50_index = (50 * len(latencies_ms) + 99) // 100 - 1
        p95_index = (95 * len(latencies_ms) + 99) // 100 - 1
        metrics = {
            "sample_count": len(latencies_ms),
            "p50_ms": round(latencies_ms[p50_index], 3),
            "p95_ms": round(latencies_ms[p95_index], 3),
            "max_ms": round(latencies_ms[-1], 3),
            "runtime_notification_count": listener_runtime.notification_count,
            "runtime_timeout_count": listener_runtime.timeout_count,
            "dispatcher_notification_count": listener_dispatcher.notification_count,
            "dispatcher_timeout_count": listener_dispatcher.timeout_count,
            "scan_backlog_remaining": int(backlog),
            "worker_active_count": int(worker_state.active_call_count),
            "worker_capacity": int(worker_state.capacity),
            "dispatch_or_stream_fields_written": int(dispatch_fields_written),
            "postgres_server_version_num": int(server_version_num),
            "isolation_level": str(isolation_level),
        }
        print("POSTGRES_WAKEUP_LATENCY " + json.dumps(metrics, sort_keys=True))
        assert metrics["p95_ms"] < 1000
    finally:
        if dispatcher_started:
            await dispatcher.stop()
        if runtime_started:
            await runtime.stop()
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
                _direct_sip_intent(
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


async def test_outbound_db_only_two_instances_create_one_dialing_chain() -> None:
    class RejectingLegacyDialer:
        dialer_type = "sip"
        manages_call_record = True

        def __init__(self) -> None:
            self.called = False

        async def dial(self, *args, **kwargs):
            del args, kwargs
            self.called = True
            raise AssertionError("owner runtime outbound must not call legacy dial()")

    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    await _reset_repository_schema(engine)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    runtime_a = None
    runtime_b = None
    try:
        task_id = generate_snowflake_id()
        target_id = generate_snowflake_id()
        line_id = generate_snowflake_id()
        phone_number = "13800138001"
        async with factory.begin() as session:
            now = await read_database_time(session)
            await _insert_sip_line(session, line_id=line_id, max_concurrency=1)
            line_snapshot = {
                "lineId": str(line_id),
                "lineCode": f"line-{line_id}",
                "lineName": f"Line {line_id}",
                "adapterType": "livekit_sip",
                "routeMode": "managed_trunk_id",
                "trunkId": "stub-trunk",
                "proxyHost": None,
                "proxyPort": None,
                "authMode": "managed_trunk",
                "callerNumber": "masked",
                "destinationCountry": "CN",
                "maxConcurrency": 1,
                "originateTimeoutSeconds": 45,
            }
            session.add(
                AiCallOutboundTaskModel(
                    id=task_id,
                    tenant_id="tenant-a",
                    validation_id=generate_snowflake_id(),
                    idempotency_key=f"outbound-owner:{task_id}",
                    request_fingerprint="a" * 64,
                    task_name="Outbound DB-only 双实例",
                    task_mode="batch",
                    status="SCHEDULED",
                    total_targets=1,
                    completed_targets=0,
                    connected_targets=0,
                    failed_targets=0,
                    execution_mode="immediate",
                    scheduled_at=None,
                    started_at=None,
                    ended_at=None,
                    prompt_profile_id="prompt-1",
                    prompt_name="合同介绍",
                    scene_code="intro_contract",
                    voice="Tina",
                    voice_name="Tina",
                    rule_id=generate_snowflake_id(),
                    rule_name="测试规则",
                    rule_summary="测试规则摘要",
                    line_id=line_id,
                    line_name=f"Line {line_id}",
                    config_snapshot_json=json.dumps(
                        {
                            "request": {"taskName": "Outbound DB-only 双实例"},
                            "prompt": {
                                "id": "prompt-1",
                                "sceneCode": "intro_contract",
                            },
                            "voice": {"voice": "Tina"},
                            "rule": {
                                "retryCount": 0,
                                "retryIntervalsMinutes": [],
                                "retryableResults": [],
                            },
                            "sipLine": line_snapshot,
                        }
                    ),
                    error_message=None,
                    created_by=1,
                    created_by_name="测试用户",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.add(
                AiCallOutboundTargetModel(
                    id=target_id,
                    tenant_id="tenant-a",
                    task_id=task_id,
                    validation_id=generate_snowflake_id(),
                    source_validation_row_id=generate_snowflake_id(),
                    source_row_number=2,
                    phone_number=phone_number,
                    customer_name="客户一",
                    status="PENDING",
                    attempt_count=0,
                    latest_result=None,
                    next_attempt_at=None,
                    created_at=now,
                    updated_at=now,
                )
            )
            worker_a = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="outbound-runtime-a",
                    startup_id=UUID("11111111-1111-4111-8111-111111111111"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )
            worker_b = await WorkerRegistryRepository(session).register(
                WorkerRegistration(
                    deployment_instance_id="outbound-runtime-b",
                    startup_id=UUID("22222222-2222-4222-8222-222222222222"),
                    capacity=1,
                    cleanup_capacity=1,
                )
            )

        first_dialer = RejectingLegacyDialer()
        second_dialer = RejectingLegacyDialer()

        def outbound_executor(dialer) -> OutboundTaskExecutor:
            return OutboundTaskExecutor(
                factory,
                dialer,
                now_provider=lambda: now,
                business_timezone="UTC",
                owner_runtime_start=OwnerRuntimeOutboundStart(),
            )

        executor_counts = await asyncio.gather(
            outbound_executor(first_dialer).run_once(),
            outbound_executor(second_dialer).run_once(),
        )
        assert sum(executor_counts) == 1
        assert first_dialer.called is False
        assert second_dialer.called is False

        dispatcher_counts = await asyncio.gather(
            DispatcherControlService(factory).run_once(),
            DispatcherControlService(factory).run_once(),
        )
        assert sum(dispatcher_counts) == 1

        provider_a = DeterministicDbOnlyProviderStub()
        provider_b = DeterministicDbOnlyProviderStub()
        runtime_a = RuntimeControlService(
            worker_id=worker_a.worker_id,
            registry=RuntimeRegistry(),
            session_factory=factory,
            provider=provider_a,
            capacity=1,
            cleanup_capacity=1,
        )
        runtime_b = RuntimeControlService(
            worker_id=worker_b.worker_id,
            registry=RuntimeRegistry(),
            session_factory=factory,
            provider=provider_b,
            capacity=1,
            cleanup_capacity=1,
        )
        runtime_counts = await asyncio.gather(
            runtime_a.run_once(),
            runtime_b.run_once(),
        )
        assert sum(runtime_counts) == 1

        reconciler_counts = await asyncio.gather(
            OutboundAttemptReconcileWorker(
                factory,
                worker_id="outbound-reconciler-a",
            ).run_once(),
            OutboundAttemptReconcileWorker(
                factory,
                worker_id="outbound-reconciler-b",
            ).run_once(),
        )
        assert sum(reconciler_counts) == 1

        async with factory() as session:
            attempt = await session.scalar(
                select(AiCallOutboundAttemptModel).where(
                    AiCallOutboundAttemptModel.task_id == task_id
                )
            )
            target = await session.get(AiCallOutboundTargetModel, target_id)
            record = await session.scalar(select(AiCallRecordModel))
            command = await session.scalar(
                select(AiCallRuntimeCommandModel).where(
                    AiCallRuntimeCommandModel.command_type == "START_CALL"
                )
            )
            reservation = await session.scalar(select(AiCallSipLineReservationModel))
            effects = list(
                (
                    await session.scalars(
                        select(AiCallRuntimeEffectModel).order_by(
                            AiCallRuntimeEffectModel.id
                        )
                    )
                ).all()
            )
            counts = {
                "attempt": int(
                    await session.scalar(
                        select(text("count(*)")).select_from(
                            AiCallOutboundAttemptModel
                        )
                    )
                    or 0
                ),
                "record": int(
                    await session.scalar(
                        select(text("count(*)")).select_from(AiCallRecordModel)
                    )
                    or 0
                ),
                "start_command": int(
                    await session.scalar(
                        select(text("count(*)"))
                        .select_from(AiCallRuntimeCommandModel)
                        .where(
                            AiCallRuntimeCommandModel.command_type == "START_CALL"
                        )
                    )
                    or 0
                ),
                "reservation": int(
                    await session.scalar(
                        select(text("count(*)")).select_from(
                            AiCallSipLineReservationModel
                        )
                    )
                    or 0
                ),
            }

        assert counts == {
            "attempt": 1,
            "record": 1,
            "start_command": 1,
            "reservation": 1,
        }
        assert attempt is not None and attempt.status == "DIALING"
        assert target is not None and target.status == "DIALING"
        assert reservation is not None
        assert reservation.attempt_id == attempt.id
        assert reservation.status == "ACTIVE"
        assert record is not None and record.status == "ready"
        assert record.callee_phone_number is None
        assert command is not None and command.status == "SUCCEEDED"
        assert len(effects) == 3
        provider_calls = provider_a.calls + provider_b.calls
        assert len(provider_calls) == 3
        assert phone_number not in (command.payload_json or "")
        assert phone_number not in repr(provider_calls)
        assert phone_number not in repr(
            [
                (
                    effect.provider_namespace,
                    effect.provider_idempotency_key,
                    effect.resource_key,
                    effect.error_message,
                )
                for effect in effects
            ]
        )
        assert phone_number not in repr(
            [
                attempt.error_message,
                record.failure_message,
                command.error_message,
                reservation.error_message,
            ]
        )
    finally:
        if runtime_a is not None:
            await runtime_a.stop()
        if runtime_b is not None:
            await runtime_b.stop()
        await engine.dispose()
