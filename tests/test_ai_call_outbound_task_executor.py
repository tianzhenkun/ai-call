from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.model import AiCallRecordModel
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from app.api.v1.ai_call.outbound.rule_task_service import OutboundRuleTaskService
from app.api.v1.ai_call.outbound.task_executor import (
    DialResult,
    OutboundDialRequest,
    OutboundTaskExecutor,
    OutboundTaskWorker,
)
from app.config.setting import Settings
from app.core.base_model import MappedBase
from app.utils.id_util import generate_snowflake_id


def test_outbound_target_persists_next_attempt_at_and_migration() -> None:
    assert "next_attempt_at" in AiCallOutboundTargetModel.__table__.columns

    migration_path = (
        Path(__file__).parents[1]
        / "docs"
        / "livekit-ai-outbound"
        / "sql"
        / "phase-h3-outbound-task-executor-postgres.sql"
    )
    migration = migration_path.read_text(encoding="utf-8").lower()
    assert "add column if not exists next_attempt_at" in migration


@pytest.fixture
async def database(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'executor.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


class SequenceDialer:
    def __init__(self, results: list[DialResult]) -> None:
        self.results = list(results)
        self.requests: list[OutboundDialRequest] = []

    async def dial(self, request: OutboundDialRequest) -> DialResult:
        self.requests.append(request)
        return self.results.pop(0)


class FailingDialer:
    async def dial(self, request: OutboundDialRequest) -> DialResult:
        del request
        raise RuntimeError("mock gateway unavailable")


class BlockingDialer:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def dial(self, request: OutboundDialRequest) -> DialResult:
        del request
        self.started.set()
        await self.release.wait()
        return DialResult(call_result="connected")


def _snapshot(
    *,
    retry_count: int = 0,
    retry_intervals_minutes: list[int] | None = None,
    retryable_results: list[str] | None = None,
    call_windows: list[dict[str, str]] | None = None,
) -> str:
    return json.dumps(
        {
            "request": {"taskName": "执行器测试任务"},
            "prompt": {"id": "prompt-1", "sceneCode": "intro_contract"},
            "voice": {"voice": "Tina"},
            "rule": {
                "retryCount": retry_count,
                "retryIntervalsMinutes": retry_intervals_minutes or [],
                "retryableResults": retryable_results or [],
                **({"callWindows": call_windows} if call_windows is not None else {}),
            },
        }
    )


def _sqlite_time(value: datetime) -> datetime:
    return value.replace(tzinfo=None)


async def _seed_task(
    database,
    *,
    now: datetime,
    target_count: int = 1,
    execution_mode: str = "immediate",
    scheduled_at: datetime | None = None,
    snapshot: str | None = None,
) -> tuple[int, list[int]]:
    task_id = generate_snowflake_id()
    target_ids = [generate_snowflake_id() for _ in range(target_count)]
    async with database() as session:
        session.add(
            AiCallOutboundTaskModel(
                id=task_id,
                tenant_id="tenant-a",
                validation_id=generate_snowflake_id(),
                idempotency_key=f"executor-{task_id}",
                request_fingerprint=f"{task_id:064d}"[-64:],
                task_name="执行器测试任务",
                task_mode="batch",
                status="SCHEDULED",
                total_targets=target_count,
                completed_targets=0,
                connected_targets=0,
                failed_targets=0,
                execution_mode=execution_mode,
                scheduled_at=scheduled_at,
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
                config_snapshot_json=snapshot or _snapshot(),
                error_message=None,
                created_by=1,
                created_by_name="测试用户",
                created_at=now,
                updated_at=now,
            )
        )
        for index, target_id in enumerate(target_ids, start=1):
            session.add(
                AiCallOutboundTargetModel(
                    id=target_id,
                    tenant_id="tenant-a",
                    task_id=task_id,
                    validation_id=generate_snowflake_id(),
                    source_validation_row_id=generate_snowflake_id(),
                    source_row_number=index + 1,
                    phone_number=f"13800138{index:03d}",
                    customer_name=f"客户{index}",
                    status="PENDING",
                    attempt_count=0,
                    latest_result=None,
                    next_attempt_at=None,
                    created_at=now,
                    updated_at=now,
                )
            )
        await session.commit()
    return task_id, target_ids


@pytest.mark.anyio
async def test_executor_skips_future_task_and_completes_due_task_once(database) -> None:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    future_task_id, _ = await _seed_task(
        database,
        now=now,
        execution_mode="scheduled",
        scheduled_at=now + timedelta(hours=1),
    )
    task_id, target_ids = await _seed_task(database, now=now)
    dialer = SequenceDialer([DialResult(call_result="connected", duration_ms=1200)])
    executor = OutboundTaskExecutor(database, dialer, now_provider=lambda: now)

    assert await executor.run_once() == 1
    assert await executor.run_once() == 0

    async with database() as session:
        future_task = await session.get(AiCallOutboundTaskModel, future_task_id)
        task = await session.get(AiCallOutboundTaskModel, task_id)
        target = await session.get(AiCallOutboundTargetModel, target_ids[0])
        attempts = (
            await session.scalars(
                select(AiCallOutboundAttemptModel).where(
                    AiCallOutboundAttemptModel.task_id == task_id
                )
            )
        ).all()
        record = await session.scalar(
            select(AiCallRecordModel).where(
                AiCallRecordModel.call_id == attempts[0].call_id
            )
        )

    assert future_task is not None and future_task.status == "SCHEDULED"
    assert task is not None
    assert task.status == "COMPLETED"
    assert task.completed_targets == 1
    assert task.connected_targets == 1
    assert task.failed_targets == 0
    assert task.started_at == _sqlite_time(now)
    assert task.ended_at == _sqlite_time(now)
    assert target is not None
    assert target.status == "COMPLETED"
    assert target.attempt_count == 1
    assert target.latest_result == "connected"
    assert target.next_attempt_at is None
    assert len(attempts) == 1
    assert attempts[0].attempt_no == 1
    assert attempts[0].status == "COMPLETED"
    assert attempts[0].call_result == "connected"
    assert record is not None
    assert record.entry_type == "outbound_mock"
    assert record.business_type == "outbound_task"
    assert record.business_id == str(task_id)
    assert record.status == "completed"
    assert record.duration_ms == 1200
    assert len(dialer.requests) == 1
    assert dialer.requests[0].target_id == target_ids[0]


@pytest.mark.anyio
async def test_parallel_executors_do_not_duplicate_attempt(database) -> None:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    task_id, _ = await _seed_task(database, now=now)
    first_dialer = SequenceDialer([DialResult(call_result="connected")])
    second_dialer = SequenceDialer([DialResult(call_result="connected")])
    processed = await asyncio.gather(
        OutboundTaskExecutor(
            database,
            first_dialer,
            now_provider=lambda: now,
        ).run_once(),
        OutboundTaskExecutor(
            database,
            second_dialer,
            now_provider=lambda: now,
        ).run_once(),
    )

    async with database() as session:
        attempt_count = int(
            await session.scalar(
                select(func.count(AiCallOutboundAttemptModel.id)).where(
                    AiCallOutboundAttemptModel.task_id == task_id
                )
            )
            or 0
        )
    assert sum(processed) == 1
    assert attempt_count == 1
    assert len(first_dialer.requests) + len(second_dialer.requests) == 1


@pytest.mark.anyio
async def test_executor_waits_for_retry_time_then_uses_next_attempt(database) -> None:
    clock = [datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)]
    task_id, target_ids = await _seed_task(
        database,
        now=clock[0],
        snapshot=_snapshot(
            retry_count=1,
            retry_intervals_minutes=[30],
            retryable_results=["no_answer"],
        ),
    )
    dialer = SequenceDialer([
        DialResult(call_result="no_answer", error_message="无人接听"),
        DialResult(call_result="connected", duration_ms=800),
    ])
    executor = OutboundTaskExecutor(database, dialer, now_provider=lambda: clock[0])

    assert await executor.run_once() == 1
    assert await executor.run_once() == 0

    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        target = await session.get(AiCallOutboundTargetModel, target_ids[0])
    assert task is not None and task.status == "RUNNING"
    assert target is not None
    assert target.status == "RETRY_WAIT"
    assert target.attempt_count == 1
    assert target.latest_result == "no_answer"
    assert target.next_attempt_at == _sqlite_time(clock[0] + timedelta(minutes=30))

    clock[0] += timedelta(minutes=30)
    assert await executor.run_once() == 1

    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        target = await session.get(AiCallOutboundTargetModel, target_ids[0])
        attempts = (
            await session.scalars(
                select(AiCallOutboundAttemptModel)
                .where(AiCallOutboundAttemptModel.target_id == target_ids[0])
                .order_by(AiCallOutboundAttemptModel.attempt_no)
            )
        ).all()
        record_count = int(
            await session.scalar(
                select(func.count(AiCallRecordModel.id)).where(
                    AiCallRecordModel.call_id.in_([item.call_id for item in attempts])
                )
            )
            or 0
        )

    assert task is not None and task.status == "COMPLETED"
    assert task.completed_targets == 1
    assert task.connected_targets == 1
    assert target is not None
    assert target.status == "COMPLETED"
    assert target.attempt_count == 2
    assert target.next_attempt_at is None
    assert [item.attempt_no for item in attempts] == [1, 2]
    assert [item.call_result for item in attempts] == ["no_answer", "connected"]
    assert record_count == 2


@pytest.mark.anyio
async def test_dialer_exception_becomes_object_failure_not_task_system_failure(database) -> None:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    task_id, target_ids = await _seed_task(database, now=now)
    executor = OutboundTaskExecutor(database, FailingDialer(), now_provider=lambda: now)

    assert await executor.run_once() == 1

    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        target = await session.get(AiCallOutboundTargetModel, target_ids[0])
        attempt = await session.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.task_id == task_id
            )
        )
        record = await session.scalar(
            select(AiCallRecordModel).where(
                AiCallRecordModel.call_id == attempt.call_id
            )
        )
    assert task is not None
    assert task.status == "COMPLETED"
    assert task.failed_targets == 1
    assert task.error_message is None
    assert target is not None
    assert target.status == "COMPLETED"
    assert target.latest_result == "call_failed"
    assert attempt is not None
    assert attempt.status == "FAILED"
    assert attempt.error_message == "mock gateway unavailable"
    assert record is not None
    assert record.status == "failed"
    assert record.failure_stage == "outbound_mock"


@pytest.mark.anyio
async def test_executor_waits_until_configured_call_window(database) -> None:
    clock = [datetime(2026, 7, 28, 8, 30, tzinfo=timezone.utc)]
    task_id, _ = await _seed_task(
        database,
        now=clock[0],
        snapshot=_snapshot(
            call_windows=[{"startTime": "09:00", "endTime": "10:00"}],
        ),
    )
    dialer = SequenceDialer([DialResult(call_result="connected")])
    executor = OutboundTaskExecutor(
        database,
        dialer,
        now_provider=lambda: clock[0],
        business_timezone="UTC",
    )

    assert await executor.run_once() == 0
    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
    assert task is not None and task.status == "SCHEDULED"

    clock[0] = datetime(2026, 7, 28, 9, 0, tzinfo=timezone.utc)
    assert await executor.run_once() == 1


@pytest.mark.anyio
async def test_closed_window_task_does_not_starve_later_due_task(database) -> None:
    now = datetime(2026, 7, 28, 9, 0, tzinfo=timezone.utc)
    closed_task_id, _ = await _seed_task(
        database,
        now=now,
        snapshot=_snapshot(
            call_windows=[{"startTime": "10:00", "endTime": "11:00"}],
        ),
    )
    due_task_id, _ = await _seed_task(
        database,
        now=now,
        snapshot=_snapshot(
            call_windows=[{"startTime": "09:00", "endTime": "10:00"}],
        ),
    )
    executor = OutboundTaskExecutor(
        database,
        SequenceDialer([DialResult(call_result="connected")]),
        task_batch_size=1,
        now_provider=lambda: now,
        business_timezone="UTC",
    )

    assert await executor.run_once() == 1
    async with database() as session:
        closed_task = await session.get(AiCallOutboundTaskModel, closed_task_id)
        due_task = await session.get(AiCallOutboundTaskModel, due_task_id)
    assert closed_task is not None and closed_task.status == "SCHEDULED"
    assert due_task is not None and due_task.status == "COMPLETED"


@pytest.mark.anyio
async def test_task_actions_pause_resume_and_stop_without_new_attempt(database) -> None:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    task_id, target_ids = await _seed_task(database, now=now, target_count=2)
    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        assert task is not None
        task.status = "RUNNING"
        task.started_at = now
        await session.commit()

    service = OutboundRuleTaskService(database)
    dialer = SequenceDialer([DialResult(call_result="connected")])
    executor = OutboundTaskExecutor(database, dialer, now_provider=lambda: now)

    async with database() as session:
        await service.run_action(session, "tenant-a", task_id, "pause")
        await session.commit()
    assert await executor.run_once() == 0

    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        assert task is not None and task.status == "PAUSED"
        await service.run_action(session, "tenant-a", task_id, "resume")
        await session.commit()
        assert task.status == "RUNNING"

    async with database() as session:
        await service.run_action(session, "tenant-a", task_id, "stop")
        await service.run_action(session, "tenant-a", task_id, "stop")
        await session.commit()

    assert await executor.run_once() == 0
    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        targets = (
            await session.scalars(
                select(AiCallOutboundTargetModel).where(
                    AiCallOutboundTargetModel.id.in_(target_ids)
                )
            )
        ).all()
        attempt_count = int(
            await session.scalar(
                select(func.count(AiCallOutboundAttemptModel.id)).where(
                    AiCallOutboundAttemptModel.task_id == task_id
                )
            )
            or 0
        )
    assert task is not None
    assert task.status == "STOPPED"
    assert task.completed_targets == 2
    assert task.ended_at is not None
    assert {target.status for target in targets} == {"CANCELLED"}
    assert attempt_count == 0


@pytest.mark.anyio
async def test_stop_allows_current_attempt_to_finish_and_cancels_remaining(database) -> None:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    task_id, target_ids = await _seed_task(database, now=now, target_count=2)
    dialer = BlockingDialer()
    executor = OutboundTaskExecutor(database, dialer, now_provider=lambda: now)
    run_task = asyncio.create_task(executor.run_once())
    await asyncio.wait_for(dialer.started.wait(), timeout=0.5)

    service = OutboundRuleTaskService(database)
    async with database() as session:
        await service.run_action(session, "tenant-a", task_id, "stop")
        await session.commit()
        task = await session.get(AiCallOutboundTaskModel, task_id)
        assert task is not None and task.status == "STOPPING"
    dialer.release.set()

    assert await run_task == 1
    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        targets = (
            await session.scalars(
                select(AiCallOutboundTargetModel)
                .where(AiCallOutboundTargetModel.id.in_(target_ids))
                .order_by(AiCallOutboundTargetModel.source_row_number)
            )
        ).all()
    assert task is not None
    assert task.status == "STOPPED"
    assert task.completed_targets == 2
    assert task.connected_targets == 1
    assert [target.status for target in targets] == ["COMPLETED", "CANCELLED"]


@pytest.mark.anyio
async def test_pause_waits_for_current_attempt_then_keeps_remaining_pending(database) -> None:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    task_id, target_ids = await _seed_task(database, now=now, target_count=2)
    dialer = BlockingDialer()
    executor = OutboundTaskExecutor(database, dialer, now_provider=lambda: now)
    run_task = asyncio.create_task(executor.run_once())
    await asyncio.wait_for(dialer.started.wait(), timeout=0.5)

    service = OutboundRuleTaskService(database)
    async with database() as session:
        await service.run_action(session, "tenant-a", task_id, "pause")
        await session.commit()
        task = await session.get(AiCallOutboundTaskModel, task_id)
        assert task is not None and task.status == "PAUSING"
    dialer.release.set()

    assert await run_task == 1
    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        targets = (
            await session.scalars(
                select(AiCallOutboundTargetModel)
                .where(AiCallOutboundTargetModel.id.in_(target_ids))
                .order_by(AiCallOutboundTargetModel.source_row_number)
            )
        ).all()
    assert task is not None
    assert task.status == "PAUSED"
    assert task.completed_targets == 1
    assert [target.status for target in targets] == ["COMPLETED", "PENDING"]


@pytest.mark.anyio
async def test_next_poll_recovers_attempt_left_dialing_by_worker_interruption(database) -> None:
    clock = [datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)]
    task_id, target_ids = await _seed_task(database, now=clock[0])
    blocking_dialer = BlockingDialer()
    interrupted_executor = OutboundTaskExecutor(
        database,
        blocking_dialer,
        now_provider=lambda: clock[0],
        dialing_timeout_seconds=300,
    )
    interrupted_run = asyncio.create_task(interrupted_executor.run_once())
    await asyncio.wait_for(blocking_dialer.started.wait(), timeout=0.5)
    interrupted_run.cancel()
    await asyncio.gather(interrupted_run, return_exceptions=True)

    clock[0] += timedelta(minutes=5)
    recovery_dialer = SequenceDialer([])
    recovery_executor = OutboundTaskExecutor(
        database,
        recovery_dialer,
        now_provider=lambda: clock[0],
        dialing_timeout_seconds=300,
    )
    assert await recovery_executor.run_once() == 0

    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        target = await session.get(AiCallOutboundTargetModel, target_ids[0])
        attempt = await session.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.task_id == task_id
            )
        )
        record = await session.scalar(
            select(AiCallRecordModel).where(
                AiCallRecordModel.call_id == attempt.call_id
            )
        )
    assert task is not None and task.status == "COMPLETED"
    assert target is not None and target.status == "COMPLETED"
    assert attempt is not None
    assert attempt.status == "FAILED"
    assert attempt.call_result == "call_failed"
    assert attempt.error_message == "执行器中断，拨打结果未知"
    assert record is not None and record.status == "failed"
    assert recovery_dialer.requests == []


def test_outbound_executor_is_disabled_by_default() -> None:
    assert Settings.model_fields["AI_CALL_OUTBOUND_EXECUTOR_ENABLED"].default is False


@pytest.mark.anyio
async def test_worker_survives_one_poll_error_and_stops_cleanly() -> None:
    class FlakyExecutor:
        def __init__(self) -> None:
            self.calls = 0
            self.succeeded = asyncio.Event()

        async def run_once(self) -> int:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("temporary failure")
            self.succeeded.set()
            return 0

    executor = FlakyExecutor()
    worker = OutboundTaskWorker(executor, poll_interval_seconds=0.01)  # type: ignore[arg-type]
    await worker.start()
    try:
        await asyncio.wait_for(executor.succeeded.wait(), timeout=0.5)
    finally:
        await worker.stop()

    calls_after_stop = executor.calls
    await asyncio.sleep(0.03)
    assert executor.calls == calls_after_stop
