from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.model import AiCallEventModel, AiCallRecordModel
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from app.api.v1.ai_call.outbound.sip_line_model import AiCallSipLineModel
from app.api.v1.ai_call.outbound.task_executor import OutboundTaskExecutor
from app.core.base_model import MappedBase
from app.services.ai_call.runtime_control.command_repository import (
    RuntimeCommandRepository,
)
from app.services.ai_call.runtime_control.models import (
    AiCallRuntimeCommandModel,
    AiCallRuntimeEffectModel,
    AiCallRuntimeWorkerModel,
    AiCallSipLineReservationModel,
)
from app.services.ai_call.runtime_control.owner_repository import (
    DispatcherOwnerRepository,
)
from app.utils.id_util import generate_snowflake_id


async def _constant_time(value: datetime) -> datetime:
    return value


@pytest.fixture
async def database(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'owner-runtime.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


class RejectingDialer:
    dialer_type = "sip"
    manages_call_record = True

    def __init__(self) -> None:
        self.called = False

    async def dial(self, *args, **kwargs):
        del args, kwargs
        self.called = True
        raise AssertionError("owner runtime outbound must not call legacy dial()")


def _line_snapshot() -> dict[str, object]:
    return {
        "lineId": "340700000000000001",
        "lineCode": "provider-a",
        "lineName": "供应商 A",
        "adapterType": "livekit_sip",
        "routeMode": "inline_hostname",
        "trunkId": None,
        "proxyHost": "127.0.0.1",
        "proxyPort": 5089,
        "authMode": "ip_allowlist",
        "callerNumber": "1000",
        "destinationCountry": "CN",
        "maxConcurrency": 1,
        "originateTimeoutSeconds": 45,
    }


async def _seed_due_task(
    database,
    now: datetime,
    *,
    target_count: int = 1,
    tenant_id: str = "tenant-a",
    line_id: int = 340700000000000001,
    retry_count: int = 0,
    retry_intervals_minutes: list[int] | None = None,
    retryable_results: list[str] | None = None,
) -> tuple[int, int, str]:
    task_id = generate_snowflake_id()
    target_id = generate_snowflake_id()
    phone_number = "13800138001"
    line = _line_snapshot()
    line["lineId"] = str(line_id)
    if tenant_id != "tenant-a" or line_id != 340700000000000001:
        line["lineCode"] = f"provider-{tenant_id}-{line_id}"
    snapshot = json.dumps({
        "request": {"taskName": "Owner Runtime 外呼"},
        "prompt": {"id": "prompt-1", "sceneCode": "intro_contract"},
        "voice": {"voice": "Tina"},
        "rule": {
            "retryCount": retry_count,
            "retryIntervalsMinutes": retry_intervals_minutes or [],
            "retryableResults": retryable_results or [],
        },
        "sipLine": line,
    })
    async with database.begin() as session:
        session.add(
            AiCallSipLineModel(
                id=int(line["lineId"]),
                tenant_id=tenant_id,
                line_code=str(line["lineCode"]),
                line_name=str(line["lineName"]),
                enabled=True,
                default_marker="OUTBOUND",
                adapter_type=str(line["adapterType"]),
                route_mode=str(line["routeMode"]),
                trunk_id=None,
                proxy_host=str(line["proxyHost"]),
                proxy_port=int(line["proxyPort"]),
                auth_mode=str(line["authMode"]),
                caller_number=str(line["callerNumber"]),
                destination_country=str(line["destinationCountry"]),
                max_concurrency=int(line["maxConcurrency"]),
                originate_timeout_seconds=int(line["originateTimeoutSeconds"]),
                health_status="AVAILABLE",
                health_message=None,
                last_checked_at=now,
                deleted=False,
                deleted_at=None,
                created_by=1,
                updated_by=1,
                created_at=now,
                updated_at=now,
            )
        )
        session.add(
            AiCallOutboundTaskModel(
                id=task_id,
                tenant_id=tenant_id,
                validation_id=generate_snowflake_id(),
                idempotency_key=f"owner-runtime-{task_id}",
                request_fingerprint=f"{task_id:064d}"[-64:],
                task_name="Owner Runtime 外呼",
                task_mode="batch",
                status="SCHEDULED",
                total_targets=target_count,
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
                line_id=int(line["lineId"]),
                line_name=str(line["lineName"]),
                config_snapshot_json=snapshot,
                error_message=None,
                created_by=1,
                created_by_name="测试用户",
                created_at=now,
                updated_at=now,
            )
        )
        for index in range(target_count):
            session.add(
                AiCallOutboundTargetModel(
                    id=target_id if index == 0 else generate_snowflake_id(),
                    tenant_id=tenant_id,
                    task_id=task_id,
                    validation_id=generate_snowflake_id(),
                    source_validation_row_id=generate_snowflake_id(),
                    source_row_number=index + 2,
                    phone_number=(
                        phone_number if index == 0 else f"13800138{index + 1:03d}"
                    ),
                    customer_name=f"客户{index + 1}",
                    status="PENDING",
                    attempt_count=0,
                    latest_result=None,
                    next_attempt_at=None,
                    created_at=now,
                    updated_at=now,
                )
            )
    return task_id, target_id, phone_number


async def _seed_ready_worker(database, now: datetime) -> str:
    worker_id = "runtime-a:00000000-0000-0000-0000-000000000001"
    async with database.begin() as session:
        session.add(
            AiCallRuntimeWorkerModel(
                worker_id=worker_id,
                status="READY",
                capacity=1,
                cleanup_capacity=1,
                active_call_count=0,
                active_cleanup_count=0,
                heartbeat_at=now,
                lease_expires_at=now.replace(hour=2),
                created_at=now,
                updated_at=now,
            )
        )
    return worker_id


async def _seed_owner_assigned_chain(
    database,
    now: datetime,
) -> tuple[int, int, str, str]:
    from app.api.v1.ai_call.outbound.owner_runtime_start import (
        OwnerRuntimeOutboundStart,
    )

    task_id, target_id, _phone_number = await _seed_due_task(database, now)
    worker_id = await _seed_ready_worker(database, now)
    executor = OutboundTaskExecutor(
        database,
        RejectingDialer(),
        now_provider=lambda: now,
        business_timezone="UTC",
        owner_runtime_start=OwnerRuntimeOutboundStart(
            database_clock=lambda _session: _constant_time(now)
        ),
    )
    assert await executor.run_once() == 1
    async with database() as session:
        attempt = await session.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.task_id == task_id
            )
        )
        assert attempt is not None
        lease = await DispatcherOwnerRepository(
            session,
            database_clock=lambda _session: _constant_time(
                now.replace(tzinfo=None)
            ),
        ).assign_initial_owner("tenant-a", attempt.call_id)
        await session.commit()
        call_id = attempt.call_id
    assert lease is not None
    return task_id, target_id, call_id, worker_id


async def _seed_start_completion_facts(
    database,
    *,
    call_id: str,
    now: datetime,
    missing: str | None = None,
) -> None:
    async with database.begin() as session:
        attempt = await session.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.call_id == call_id
            )
        )
        record = await session.scalar(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id == call_id)
        )
        command = await session.scalar(
            select(AiCallRuntimeCommandModel).where(
                AiCallRuntimeCommandModel.call_id == call_id,
                AiCallRuntimeCommandModel.command_type == "START_CALL",
            )
        )
        reservation = await session.scalar(
            select(AiCallSipLineReservationModel).where(
                AiCallSipLineReservationModel.call_id == call_id
            )
        )
        assert attempt is not None
        assert record is not None
        assert command is not None
        assert reservation is not None
        record.status = "preparing" if missing == "record" else "ready"
        command.status = "PENDING" if missing == "command" else "SUCCEEDED"
        reservation.status = "RESERVED" if missing == "reservation" else "ACTIVE"
        if missing != "effect":
            session.add(
                AiCallRuntimeEffectModel(
                    id=generate_snowflake_id(),
                    tenant_id="tenant-a",
                    call_id=call_id,
                    command_id=command.id,
                    effect_type="CREATE_SIP_PARTICIPANT",
                    idempotency_key=f"test:sip:{call_id}",
                    fencing_token=reservation.fencing_token,
                    status="APPLIED",
                    provider_namespace="stub:runtime-a",
                    provider_idempotency_key=f"provider:sip:{call_id}",
                    resource_key=f"sip:{call_id}:g{reservation.fencing_token}",
                    resource_generation=reservation.fencing_token,
                    source_create_effect_id=None,
                    create_protection_deadline_at=None,
                    reconcile_after=None,
                    reconcile_deadline_at=None,
                    error_message=None,
                    provider_reference=f"stub:sip:{call_id}",
                    created_at=now,
                    updated_at=now,
                )
            )


@pytest.mark.anyio
async def test_owner_runtime_executor_atomically_queues_start_without_dialing(
    database,
) -> None:
    from app.api.v1.ai_call.outbound.owner_runtime_start import (
        OwnerRuntimeOutboundStart,
    )

    now = datetime(2026, 8, 2, 1, 0, tzinfo=timezone.utc)
    task_id, target_id, phone_number = await _seed_due_task(database, now)
    dialer = RejectingDialer()
    executor = OutboundTaskExecutor(
        database,
        dialer,
        now_provider=lambda: now,
        business_timezone="UTC",
        owner_runtime_start=OwnerRuntimeOutboundStart(
            database_clock=lambda _session: _constant_time(now)
        ),
    )

    assert await executor.run_once() == 1
    assert await executor.run_once() == 0
    assert dialer.called is False

    async with database() as session:
        attempt = await session.scalar(
            select(AiCallOutboundAttemptModel).where(AiCallOutboundAttemptModel.task_id == task_id)
        )
        assert attempt is not None
        record = await session.scalar(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id == attempt.call_id)
        )
        command = await session.scalar(
            select(AiCallRuntimeCommandModel).where(
                AiCallRuntimeCommandModel.call_id == attempt.call_id,
                AiCallRuntimeCommandModel.command_type == "START_CALL",
            )
        )
        target = await session.get(AiCallOutboundTargetModel, target_id)
        attempt_count = int(
            await session.scalar(select(func.count(AiCallOutboundAttemptModel.id))) or 0
        )

    assert attempt_count == 1
    assert attempt.status == "QUEUED"
    assert attempt.dialer_type == "owner_runtime"
    assert record is not None
    assert record.tenant_id == "tenant-a"
    assert record.entry_type == "outbound"
    assert record.runtime_control_mode == "owner_command_v1"
    assert command is not None
    assert command.status == "PENDING"
    assert command.call_id == record.call_id == attempt.call_id
    assert target is not None and target.status == "DIALING"
    payload = json.loads(command.payload_json or "{}")
    assert payload == {
        "attempt_id": str(attempt.id),
        "attempt_no": 1,
        "line_code": "provider-a",
        "line_id": "340700000000000001",
        "prompt_profile_id": "prompt-1",
        "scene_code": "intro_contract",
        "target_id": str(target_id),
        "task_id": str(task_id),
        "voice": "Tina",
    }
    assert phone_number not in (command.payload_json or "")


@pytest.mark.anyio
async def test_owner_runtime_start_failure_rolls_back_target_and_all_start_facts(
    database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.api.v1.ai_call.outbound.owner_runtime_start import (
        OwnerRuntimeOutboundStart,
    )

    now = datetime(2026, 8, 2, 1, 0, tzinfo=timezone.utc)
    task_id, target_id, _phone_number = await _seed_due_task(database, now)
    original_create = RuntimeCommandRepository.create_start_call

    async def create_then_fail(self, request):
        await original_create(self, request)
        raise RuntimeError("forced failure after start facts flush")

    monkeypatch.setattr(
        RuntimeCommandRepository,
        "create_start_call",
        create_then_fail,
    )
    executor = OutboundTaskExecutor(
        database,
        RejectingDialer(),
        now_provider=lambda: now,
        business_timezone="UTC",
        owner_runtime_start=OwnerRuntimeOutboundStart(
            database_clock=lambda _session: _constant_time(now)
        ),
    )

    with pytest.raises(RuntimeError, match="forced failure"):
        await executor.run_once()

    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        target = await session.get(AiCallOutboundTargetModel, target_id)
        attempt_count = int(
            await session.scalar(select(func.count(AiCallOutboundAttemptModel.id))) or 0
        )
        record_count = int(await session.scalar(select(func.count(AiCallRecordModel.id))) or 0)
        command_count = int(
            await session.scalar(select(func.count(AiCallRuntimeCommandModel.id))) or 0
        )

    assert task is not None and task.status == "RUNNING"
    assert target is not None and target.status == "PENDING"
    assert target.attempt_count == 0
    assert attempt_count == record_count == command_count == 0


@pytest.mark.anyio
async def test_parallel_owner_runtime_executors_create_one_start_chain(database) -> None:
    from app.api.v1.ai_call.outbound.owner_runtime_start import (
        OwnerRuntimeOutboundStart,
    )

    now = datetime(2026, 8, 2, 1, 0, tzinfo=timezone.utc)
    task_id, _target_id, _phone_number = await _seed_due_task(database, now)

    def executor() -> OutboundTaskExecutor:
        return OutboundTaskExecutor(
            database,
            RejectingDialer(),
            now_provider=lambda: now,
            business_timezone="UTC",
            owner_runtime_start=OwnerRuntimeOutboundStart(
                database_clock=lambda _session: _constant_time(now)
            ),
        )

    processed = await asyncio.gather(executor().run_once(), executor().run_once())

    async with database() as session:
        attempt_count = int(
            await session.scalar(select(func.count(AiCallOutboundAttemptModel.id))) or 0
        )
        record_count = int(await session.scalar(select(func.count(AiCallRecordModel.id))) or 0)
        command_count = int(
            await session.scalar(select(func.count(AiCallRuntimeCommandModel.id))) or 0
        )
        attempt = await session.scalar(
            select(AiCallOutboundAttemptModel).where(AiCallOutboundAttemptModel.task_id == task_id)
        )

    assert sum(processed) == 1
    assert attempt_count == record_count == command_count == 1
    assert attempt is not None and attempt.status == "QUEUED"


@pytest.mark.anyio
async def test_dispatcher_atomically_assigns_outbound_owner_line_and_attempt(
    database,
) -> None:
    from app.api.v1.ai_call.outbound.owner_runtime_start import (
        OwnerRuntimeOutboundStart,
    )

    now = datetime(2026, 8, 2, 1, 0, tzinfo=timezone.utc)
    task_id, _target_id, _phone_number = await _seed_due_task(database, now)
    worker_id = await _seed_ready_worker(database, now)
    executor = OutboundTaskExecutor(
        database,
        RejectingDialer(),
        now_provider=lambda: now,
        business_timezone="UTC",
        owner_runtime_start=OwnerRuntimeOutboundStart(
            database_clock=lambda _session: _constant_time(now)
        ),
    )
    assert await executor.run_once() == 1
    database_now = now.replace(tzinfo=None)

    async with database() as session:
        attempt = await session.scalar(
            select(AiCallOutboundAttemptModel).where(AiCallOutboundAttemptModel.task_id == task_id)
        )
        assert attempt is not None
        lease = await DispatcherOwnerRepository(
            session,
            database_clock=lambda _session: _constant_time(database_now),
        ).assign_initial_owner("tenant-a", attempt.call_id)
        await session.commit()
        call_id = attempt.call_id
        attempt_id = attempt.id

    assert lease is not None and lease.owner_id == worker_id
    async with database() as session:
        attempt = await session.get(AiCallOutboundAttemptModel, attempt_id)
        record = await session.scalar(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id == call_id)
        )
        worker = await session.get(AiCallRuntimeWorkerModel, worker_id)
        reservation = await session.scalar(
            select(AiCallSipLineReservationModel).where(
                AiCallSipLineReservationModel.call_id == call_id
            )
        )

    assert attempt is not None and attempt.status == "STARTING"
    assert reservation is not None
    assert reservation.attempt_id == attempt.id
    assert reservation.status == "RESERVED"
    assert record is not None and record.runtime_owner_id == worker_id
    assert worker is not None and worker.active_call_count == 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    "invalid_attempt",
    ["missing", "tenant", "call_id", "status"],
)
async def test_dispatcher_fails_closed_for_invalid_outbound_attempt_graph(
    database,
    invalid_attempt: str,
) -> None:
    from app.api.v1.ai_call.outbound.owner_runtime_start import (
        OwnerRuntimeOutboundStart,
    )

    now = datetime(2026, 8, 2, 1, 0, tzinfo=timezone.utc)
    task_id, _target_id, _phone_number = await _seed_due_task(database, now)
    worker_id = await _seed_ready_worker(database, now)
    executor = OutboundTaskExecutor(
        database,
        RejectingDialer(),
        now_provider=lambda: now,
        business_timezone="UTC",
        owner_runtime_start=OwnerRuntimeOutboundStart(
            database_clock=lambda _session: _constant_time(now)
        ),
    )
    assert await executor.run_once() == 1

    async with database.begin() as session:
        attempt = await session.scalar(
            select(AiCallOutboundAttemptModel).where(AiCallOutboundAttemptModel.task_id == task_id)
        )
        assert attempt is not None
        call_id = attempt.call_id
        if invalid_attempt == "missing":
            await session.delete(attempt)
        elif invalid_attempt == "tenant":
            attempt.tenant_id = "tenant-b"
        elif invalid_attempt == "call_id":
            attempt.call_id = "call-other"
        else:
            attempt.status = "DIALING"

    async with database() as session:
        lease = await DispatcherOwnerRepository(
            session,
            database_clock=lambda _session: _constant_time(now.replace(tzinfo=None)),
        ).assign_initial_owner("tenant-a", call_id)
        await session.commit()

    assert lease is None
    async with database() as session:
        record = await session.scalar(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id == call_id)
        )
        worker = await session.get(AiCallRuntimeWorkerModel, worker_id)
        reservation_count = int(
            await session.scalar(
                select(func.count(AiCallSipLineReservationModel.id)).where(
                    AiCallSipLineReservationModel.call_id == call_id
                )
            )
            or 0
        )

    assert record is not None and record.runtime_owner_id is None
    assert worker is not None and worker.active_call_count == 0
    assert reservation_count == 0


@pytest.mark.anyio
async def test_dispatcher_rejects_outbound_refs_changed_after_candidate_read(
    database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.api.v1.ai_call.outbound.owner_runtime_start import (
        OwnerRuntimeOutboundStart,
    )

    now = datetime(2026, 8, 2, 1, 0, tzinfo=timezone.utc)
    task_id, _target_id, _phone_number = await _seed_due_task(database, now)
    worker_id = await _seed_ready_worker(database, now)
    executor = OutboundTaskExecutor(
        database,
        RejectingDialer(),
        now_provider=lambda: now,
        business_timezone="UTC",
        owner_runtime_start=OwnerRuntimeOutboundStart(
            database_clock=lambda _session: _constant_time(now)
        ),
    )
    assert await executor.run_once() == 1

    async with database() as session:
        attempt = await session.scalar(
            select(AiCallOutboundAttemptModel).where(AiCallOutboundAttemptModel.task_id == task_id)
        )
        assert attempt is not None
        repository = DispatcherOwnerRepository(
            session,
            database_clock=lambda _session: _constant_time(now.replace(tzinfo=None)),
        )
        original_lock_command = repository._lock_command

        async def lock_changed_command(
            tenant_id: str,
            call_id: str,
            command_type: str,
        ):
            command = await original_lock_command(tenant_id, call_id, command_type)
            assert command is not None
            changed_payload = json.loads(command.payload_json or "{}")
            changed_payload["attempt_id"] = str(attempt.id + 1)
            return type(
                "ChangedCommand",
                (),
                {
                    "status": command.status,
                    "payload_json": json.dumps(changed_payload),
                },
            )()

        monkeypatch.setattr(repository, "_lock_command", lock_changed_command)
        lease = await repository.assign_initial_owner("tenant-a", attempt.call_id)
        call_id = attempt.call_id
        await session.rollback()

    assert lease is None
    async with database() as session:
        record = await session.scalar(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id == call_id)
        )
        worker = await session.get(AiCallRuntimeWorkerModel, worker_id)
        reservation_count = int(
            await session.scalar(
                select(func.count(AiCallSipLineReservationModel.id)).where(
                    AiCallSipLineReservationModel.call_id == call_id
                )
            )
            or 0
        )

    assert record is not None and record.runtime_owner_id is None
    assert worker is not None and worker.active_call_count == 0
    assert reservation_count == 0


@pytest.mark.anyio
async def test_attempt_reconciler_keeps_queued_without_owner_or_reservation(
    database,
) -> None:
    from app.api.v1.ai_call.outbound.attempt_reconciler import (
        OutboundAttemptReconciler,
    )
    from app.api.v1.ai_call.outbound.owner_runtime_start import (
        OwnerRuntimeOutboundStart,
    )

    now = datetime(2026, 8, 2, 1, 0, tzinfo=timezone.utc)
    task_id, _target_id, _phone_number = await _seed_due_task(database, now)
    executor = OutboundTaskExecutor(
        database,
        RejectingDialer(),
        now_provider=lambda: now,
        business_timezone="UTC",
        owner_runtime_start=OwnerRuntimeOutboundStart(
            database_clock=lambda _session: _constant_time(now)
        ),
    )
    assert await executor.run_once() == 1
    database_now = now.replace(tzinfo=None)

    async with database.begin() as session:
        claim = await OutboundAttemptReconciler(
            session,
            worker_id="reconciler-a",
            database_clock=lambda _session: _constant_time(database_now),
        ).claim_next()
    assert claim is not None

    async with database.begin() as session:
        result = await OutboundAttemptReconciler(
            session,
            worker_id="reconciler-a",
            database_clock=lambda _session: _constant_time(database_now),
        ).submit(claim)

    assert result is not None and result.status == "QUEUED"
    async with database() as session:
        attempt = await session.get(AiCallOutboundAttemptModel, claim.attempt_id)
    assert attempt is not None and attempt.status == "QUEUED"
    assert attempt.reconcile_owner_id is None
    assert attempt.reconcile_token is None
    assert attempt.reconcile_expires_at is None
    assert attempt.reconcile_after is not None


@pytest.mark.anyio
async def test_attempt_reconciler_projects_reserved_owner_to_starting(database) -> None:
    from app.api.v1.ai_call.outbound.attempt_reconciler import (
        OutboundAttemptReconciler,
    )

    now = datetime(2026, 8, 2, 1, 0, tzinfo=timezone.utc)
    task_id, _target_id, call_id, _worker_id = await _seed_owner_assigned_chain(
        database,
        now,
    )
    async with database.begin() as session:
        attempt = await session.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.task_id == task_id
            )
        )
        assert attempt is not None
        attempt.status = "QUEUED"
    database_now = now.replace(tzinfo=None)

    async with database.begin() as session:
        reconciler = OutboundAttemptReconciler(
            session,
            worker_id="reconciler-a",
            database_clock=lambda _session: _constant_time(database_now),
        )
        claim = await reconciler.claim_next()
    assert claim is not None
    async with database.begin() as session:
        result = await OutboundAttemptReconciler(
            session,
            worker_id="reconciler-a",
            database_clock=lambda _session: _constant_time(database_now),
        ).submit(claim)

    assert result is not None
    assert result.previous_status == "QUEUED"
    assert result.status == "STARTING"
    async with database() as session:
        attempt = await session.get(AiCallOutboundAttemptModel, claim.attempt_id)
    assert attempt is not None and attempt.status == "STARTING"


@pytest.mark.anyio
async def test_attempt_reconciler_projects_complete_start_facts_to_dialing(
    database,
) -> None:
    from app.api.v1.ai_call.outbound.attempt_reconciler import (
        OutboundAttemptReconciler,
    )

    now = datetime(2026, 8, 2, 1, 0, tzinfo=timezone.utc)
    _task_id, target_id, call_id, _worker_id = await _seed_owner_assigned_chain(
        database,
        now,
    )
    await _seed_start_completion_facts(database, call_id=call_id, now=now)
    database_now = now.replace(tzinfo=None)

    async with database.begin() as session:
        claim = await OutboundAttemptReconciler(
            session,
            worker_id="reconciler-a",
            database_clock=lambda _session: _constant_time(database_now),
        ).claim_next()
    assert claim is not None
    async with database.begin() as session:
        result = await OutboundAttemptReconciler(
            session,
            worker_id="reconciler-a",
            database_clock=lambda _session: _constant_time(database_now),
        ).submit(claim)

    assert result is not None and result.status == "DIALING"
    async with database() as session:
        attempt = await session.get(AiCallOutboundAttemptModel, claim.attempt_id)
        target = await session.get(AiCallOutboundTargetModel, target_id)
    assert attempt is not None and attempt.status == "DIALING"
    assert target is not None and target.status == "DIALING"
    assert attempt.reconcile_after == database_now + timedelta(seconds=1)


@pytest.mark.anyio
@pytest.mark.parametrize("missing", ["record", "command", "reservation", "effect"])
async def test_attempt_reconciler_does_not_project_dialing_with_missing_fact(
    database,
    missing: str,
) -> None:
    from app.api.v1.ai_call.outbound.attempt_reconciler import (
        OutboundAttemptReconciler,
    )

    now = datetime(2026, 8, 2, 1, 0, tzinfo=timezone.utc)
    _task_id, _target_id, call_id, _worker_id = await _seed_owner_assigned_chain(
        database,
        now,
    )
    await _seed_start_completion_facts(
        database,
        call_id=call_id,
        now=now,
        missing=missing,
    )
    database_now = now.replace(tzinfo=None)

    async with database.begin() as session:
        claim = await OutboundAttemptReconciler(
            session,
            worker_id="reconciler-a",
            database_clock=lambda _session: _constant_time(database_now),
        ).claim_next()
    assert claim is not None
    async with database.begin() as session:
        result = await OutboundAttemptReconciler(
            session,
            worker_id="reconciler-a",
            database_clock=lambda _session: _constant_time(database_now),
        ).submit(claim)

    assert result is not None and result.status == "STARTING"


@pytest.mark.anyio
async def test_attempt_reconciler_rejects_old_token_after_lease_takeover(
    database,
) -> None:
    from app.api.v1.ai_call.outbound.attempt_reconciler import (
        OutboundAttemptReconciler,
    )
    from app.api.v1.ai_call.outbound.owner_runtime_start import (
        OwnerRuntimeOutboundStart,
    )

    now = datetime(2026, 8, 2, 1, 0, tzinfo=timezone.utc)
    await _seed_due_task(database, now)
    executor = OutboundTaskExecutor(
        database,
        RejectingDialer(),
        now_provider=lambda: now,
        business_timezone="UTC",
        owner_runtime_start=OwnerRuntimeOutboundStart(
            database_clock=lambda _session: _constant_time(now)
        ),
    )
    assert await executor.run_once() == 1
    database_now = now.replace(tzinfo=None)
    late = database_now + timedelta(seconds=31)

    async with database.begin() as session:
        first = await OutboundAttemptReconciler(
            session,
            worker_id="reconciler-a",
            token_generator=lambda: "token-a",
            database_clock=lambda _session: _constant_time(database_now),
        ).claim_next()
    assert first is not None
    async with database.begin() as session:
        blocked = await OutboundAttemptReconciler(
            session,
            worker_id="reconciler-b",
            token_generator=lambda: "token-b",
            database_clock=lambda _session: _constant_time(database_now),
        ).claim_next()
    assert blocked is None
    async with database.begin() as session:
        expired_result = await OutboundAttemptReconciler(
            session,
            worker_id="reconciler-a",
            database_clock=lambda _session: _constant_time(late),
        ).submit(first)
    assert expired_result is None
    async with database.begin() as session:
        second = await OutboundAttemptReconciler(
            session,
            worker_id="reconciler-b",
            token_generator=lambda: "token-b",
            database_clock=lambda _session: _constant_time(late),
        ).claim_next()
    assert second is not None

    async with database.begin() as session:
        stale_result = await OutboundAttemptReconciler(
            session,
            worker_id="reconciler-a",
            database_clock=lambda _session: _constant_time(late),
        ).submit(first)
    assert stale_result is None
    async with database() as session:
        attempt = await session.get(AiCallOutboundAttemptModel, first.attempt_id)
    assert attempt is not None
    assert attempt.reconcile_owner_id == "reconciler-b"
    assert attempt.reconcile_token == "token-b"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "limits",
    [
        {"per_tenant": 1, "per_task": 10, "per_line": 10},
        {"per_tenant": 10, "per_task": 1, "per_line": 10},
        {"per_tenant": 10, "per_task": 10, "per_line": 1},
    ],
)
async def test_owner_runtime_queue_limit_stops_before_second_target(
    database,
    limits: dict[str, int],
) -> None:
    from app.api.v1.ai_call.outbound.owner_runtime_start import (
        OwnerRuntimeOutboundStart,
    )
    from app.api.v1.ai_call.outbound.queue_control import OutboundQueueLimits

    now = datetime(2026, 8, 2, 1, 0, tzinfo=timezone.utc)
    task_id, _target_id, _phone_number = await _seed_due_task(
        database,
        now,
        target_count=2,
    )
    executor = OutboundTaskExecutor(
        database,
        RejectingDialer(),
        target_batch_size=2,
        now_provider=lambda: now,
        business_timezone="UTC",
        owner_runtime_start=OwnerRuntimeOutboundStart(
            database_clock=lambda _session: _constant_time(now)
        ),
        owner_queue_limits=OutboundQueueLimits(**limits),
    )

    assert await executor.run_once() == 1

    async with database() as session:
        attempt_count = int(
            await session.scalar(select(func.count(AiCallOutboundAttemptModel.id))) or 0
        )
        pending_count = int(
            await session.scalar(
                select(func.count(AiCallOutboundTargetModel.id)).where(
                    AiCallOutboundTargetModel.task_id == task_id,
                    AiCallOutboundTargetModel.status == "PENDING",
                )
            )
            or 0
        )
        command_count = int(
            await session.scalar(select(func.count(AiCallRuntimeCommandModel.id))) or 0
        )

    assert attempt_count == command_count == 1
    assert pending_count == 1


@pytest.mark.anyio
async def test_owner_runtime_queue_snapshot_reports_wait_and_timeout_metrics(
    database,
) -> None:
    from app.api.v1.ai_call.outbound.owner_runtime_start import (
        OwnerRuntimeOutboundStart,
    )
    from app.api.v1.ai_call.outbound.queue_control import (
        OutboundQueueLimits,
        OutboundQueueRepository,
    )

    created_at = datetime(2026, 8, 2, 1, 0, tzinfo=timezone.utc)
    observed_at = created_at + timedelta(seconds=12)
    task_id, _target_id, _phone_number = await _seed_due_task(database, created_at)
    limits = OutboundQueueLimits(per_tenant=100, per_task=20, per_line=50)
    executor = OutboundTaskExecutor(
        database,
        RejectingDialer(),
        now_provider=lambda: created_at,
        business_timezone="UTC",
        owner_runtime_start=OwnerRuntimeOutboundStart(
            database_clock=lambda _session: _constant_time(created_at)
        ),
        owner_queue_limits=limits,
    )
    assert await executor.run_once() == 1

    async with database.begin() as session:
        record = await session.scalar(select(AiCallRecordModel))
        assert record is not None
        record.failure_stage = "allocation"
        record.failure_message = "ALLOCATION_TIMEOUT"

    async with database.begin() as session:
        snapshot = await OutboundQueueRepository(
            session,
            limits=limits,
            database_clock=lambda _session: _constant_time(observed_at),
        ).lock_and_snapshot(
            tenant_id="tenant-a",
            task_id=task_id,
            line_id=int(_line_snapshot()["lineId"]),
        )

    assert snapshot.tenant_queued == 1
    assert snapshot.task_queued == 1
    assert snapshot.line_queued == 1
    assert snapshot.oldest_wait_seconds == 12.0
    assert snapshot.allocation_timeout_count == 1
    assert snapshot.limits == limits
    assert snapshot.has_capacity is True


@pytest.mark.anyio
async def test_dispatcher_orders_first_candidate_from_each_tenant_line_lane_first(
    database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.api.v1.ai_call.outbound.owner_runtime_start import (
        OwnerRuntimeOutboundStart,
    )
    from app.services.ai_call.runtime_control.dispatcher_service import (
        DispatcherControlService,
    )

    first_time = datetime(2026, 8, 2, 1, 0, tzinfo=timezone.utc)
    second_time = first_time + timedelta(seconds=1)
    await _seed_due_task(database, first_time, target_count=3)

    def executor(now: datetime) -> OutboundTaskExecutor:
        return OutboundTaskExecutor(
            database,
            RejectingDialer(),
            target_batch_size=3,
            now_provider=lambda: now,
            business_timezone="UTC",
            owner_runtime_start=OwnerRuntimeOutboundStart(
                database_clock=lambda _session: _constant_time(now)
            ),
        )

    assert await executor(first_time).run_once() == 3
    await _seed_due_task(
        database,
        second_time,
        tenant_id="tenant-b",
        line_id=340700000000000002,
    )
    assert await executor(second_time).run_once() == 1

    considered: list[tuple[str, str]] = []

    async def never_expire(self, tenant_id: str, call_id: str) -> bool:
        del self, tenant_id, call_id
        return False

    async def capture_candidate(self, tenant_id: str, call_id: str):
        del self
        considered.append((tenant_id, call_id))
        return None

    monkeypatch.setattr(
        RuntimeCommandRepository,
        "expire_unallocated_start",
        never_expire,
    )
    monkeypatch.setattr(
        DispatcherOwnerRepository,
        "assign_initial_owner",
        capture_candidate,
    )

    assert await DispatcherControlService(database, batch_size=2).run_once() == 0
    assert [tenant_id for tenant_id, _call_id in considered] == [
        "tenant-a",
        "tenant-b",
    ]


@pytest.mark.anyio
async def test_attempt_terminal_projection_retries_allocation_timeout(
    database,
) -> None:
    from app.api.v1.ai_call.outbound.attempt_reconciler import (
        OutboundAttemptReconciler,
    )
    from app.api.v1.ai_call.outbound.owner_runtime_start import (
        OwnerRuntimeOutboundStart,
    )

    now = datetime(2026, 8, 2, 1, 0, tzinfo=timezone.utc)
    task_id, target_id, _phone_number = await _seed_due_task(
        database,
        now,
        retry_count=1,
        retry_intervals_minutes=[1],
        retryable_results=["call_failed"],
    )
    executor = OutboundTaskExecutor(
        database,
        RejectingDialer(),
        now_provider=lambda: now,
        business_timezone="UTC",
        owner_runtime_start=OwnerRuntimeOutboundStart(
            database_clock=lambda _session: _constant_time(now)
        ),
    )
    assert await executor.run_once() == 1
    database_now = now.replace(tzinfo=None)
    expired_at = database_now + timedelta(seconds=31)

    async with database.begin() as session:
        attempt = await session.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.task_id == task_id
            )
        )
        assert attempt is not None
        assert await RuntimeCommandRepository(
            session,
            database_clock=lambda _session: _constant_time(expired_at),
        ).expire_unallocated_start("tenant-a", attempt.call_id)

    async with database.begin() as session:
        claim = await OutboundAttemptReconciler(
            session,
            worker_id="reconciler-timeout",
            database_clock=lambda _session: _constant_time(expired_at),
        ).claim_next()
    assert claim is not None
    async with database.begin() as session:
        result = await OutboundAttemptReconciler(
            session,
            worker_id="reconciler-timeout",
            database_clock=lambda _session: _constant_time(expired_at),
        ).submit(claim)

    assert result is not None and result.status == "FAILED"
    async with database() as session:
        attempt = await session.get(AiCallOutboundAttemptModel, claim.attempt_id)
        target = await session.get(AiCallOutboundTargetModel, target_id)
        task = await session.get(AiCallOutboundTaskModel, task_id)
    assert attempt is not None
    assert attempt.status == "FAILED"
    assert attempt.call_result == "call_failed"
    assert attempt.error_message == "ALLOCATION_TIMEOUT"
    assert target is not None and target.status == "RETRY_WAIT"
    assert target.latest_result == "call_failed"
    assert target.next_attempt_at == expired_at + timedelta(minutes=1)
    assert task is not None and task.status == "RUNNING"


@pytest.mark.anyio
async def test_attempt_terminal_projection_requires_media_evidence_for_connected(
    database,
) -> None:
    from app.api.v1.ai_call.outbound.attempt_reconciler import (
        OutboundAttemptReconciler,
    )

    now = datetime(2026, 8, 2, 1, 0, tzinfo=timezone.utc)
    task_id, target_id, call_id, _worker_id = await _seed_owner_assigned_chain(
        database,
        now,
    )
    await _seed_start_completion_facts(database, call_id=call_id, now=now)
    database_now = now.replace(tzinfo=None)

    async with database.begin() as session:
        claim = await OutboundAttemptReconciler(
            session,
            worker_id="reconciler-terminal",
            database_clock=lambda _session: _constant_time(database_now),
        ).claim_next()
    assert claim is not None
    async with database.begin() as session:
        projected = await OutboundAttemptReconciler(
            session,
            worker_id="reconciler-terminal",
            database_clock=lambda _session: _constant_time(database_now),
        ).submit(claim)
    assert projected is not None and projected.status == "DIALING"

    terminal_at = database_now + timedelta(seconds=2)
    async with database.begin() as session:
        record = await session.scalar(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id == call_id)
        )
        assert record is not None
        record.status = "completed"
        record.answered_at = terminal_at
        record.ended_at = terminal_at
        session.add(
            AiCallEventModel(
                id=generate_snowflake_id(),
                call_id=call_id,
                event_id=f"media-connected-{call_id}",
                event_type="media_connected",
                source="provider_stub",
                event_time=terminal_at,
                payload_json="{}",
            )
        )

    async with database.begin() as session:
        terminal_claim = await OutboundAttemptReconciler(
            session,
            worker_id="reconciler-terminal-restarted",
            database_clock=lambda _session: _constant_time(terminal_at),
        ).claim_next()
    assert terminal_claim is not None
    async with database.begin() as session:
        terminal_result = await OutboundAttemptReconciler(
            session,
            worker_id="reconciler-terminal-restarted",
            database_clock=lambda _session: _constant_time(terminal_at),
        ).submit(terminal_claim)

    assert terminal_result is not None and terminal_result.status == "COMPLETED"
    async with database() as session:
        attempt = await session.get(AiCallOutboundAttemptModel, terminal_claim.attempt_id)
        target = await session.get(AiCallOutboundTargetModel, target_id)
        task = await session.get(AiCallOutboundTaskModel, task_id)
    assert attempt is not None and attempt.call_result == "connected"
    assert target is not None and target.status == "COMPLETED"
    assert target.latest_result == "connected"
    assert task is not None and task.status == "COMPLETED"
    assert task.completed_targets == 1
    assert task.connected_targets == 1
    assert task.failed_targets == 0
