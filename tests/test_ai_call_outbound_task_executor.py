from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import (
    AiCallEventModel,
    AiCallRecordingTrackModel,
    AiCallRecordModel,
)
from app.api.v1.ai_call.outbound.attempt_projection import (
    enroll_terminal_exception,
    exception_category_for,
    outbound_retry_interval,
)
from app.api.v1.ai_call.outbound.exception_service import OutboundExceptionService
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundExceptionBatchModel,
    AiCallOutboundExceptionPolicyModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from app.api.v1.ai_call.outbound.rule_task_service import OutboundRuleTaskService
from app.api.v1.ai_call.outbound.sip_line_model import AiCallSipLineModel
from app.api.v1.ai_call.outbound.task_executor import (
    DialResult,
    MockOutboundDialer,
    OutboundDialRequest,
    OutboundTaskExecutor,
    OutboundTaskWorker,
    TaskKey,
)
from app.config.setting import Settings
from app.core.base_model import MappedBase
from app.services.ai_call.runtime_control.models import AiCallEndEvidenceModel
from app.utils.id_util import generate_snowflake_id


def test_outbound_target_persists_next_attempt_at_and_migration() -> None:
    assert "next_attempt_at" in AiCallOutboundTargetModel.__table__.columns
    assert "next_dispatch_at" in AiCallOutboundTaskModel.__table__.columns
    assert "last_dispatched_at" in AiCallOutboundTaskModel.__table__.columns

    migration_path = (
        Path(__file__).parents[1]
        / "docs"
        / "livekit-ai-outbound"
        / "sql"
        / "phase-h3-outbound-task-executor-postgres.sql"
    )
    migration = migration_path.read_text(encoding="utf-8").lower()
    assert "add column if not exists next_attempt_at" in migration
    assert "add column if not exists next_dispatch_at" in migration
    assert "add column if not exists last_dispatched_at" in migration
    assert "idx_outbound_task_dispatch" in migration
    assert "idx_outbound_task_scheduled_dispatch" in migration
    assert "idx_outbound_task_running_dispatch" in migration
    assert "idx_outbound_attempt_stale" in migration


def test_outbound_exception_models_and_migration() -> None:
    assert {"tenant_id", "category", "interval_days", "max_retry_count"} <= {
        column.name for column in AiCallOutboundExceptionPolicyModel.__table__.columns
    }
    assert {
        "tenant_id",
        "category",
        "status",
        "interval_days",
        "max_retry_count",
        "cutoff_at",
        "active_slot",
    } <= {column.name for column in AiCallOutboundExceptionBatchModel.__table__.columns}
    assert {
        "exception_category",
        "exception_source_result",
        "exception_original_attempt_count",
        "exception_batch_id",
        "exception_entered_at",
    } <= {column.name for column in AiCallOutboundTargetModel.__table__.columns}

    migration = (
        Path(__file__).parents[1]
        / "docs"
        / "livekit-ai-outbound"
        / "sql"
        / "phase-h11-outbound-exception-retry-postgres.sql"
    ).read_text(encoding="utf-8").lower()
    assert "create table if not exists ai_call_outbound_exception_policy" in migration
    assert "create table if not exists ai_call_outbound_exception_batch" in migration
    assert "add column if not exists exception_category" in migration
    assert "uk_outbound_exception_batch_tenant_active" in migration


@pytest.fixture
async def database(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'executor.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


class SequenceDialer:
    dialer_type = "mock"
    manages_call_record = False

    def __init__(self, results: list[DialResult]) -> None:
        self.results = list(results)
        self.requests: list[OutboundDialRequest] = []
        self.call_ids: list[str] = []

    async def dial(
        self,
        request: OutboundDialRequest,
        *,
        call_id: str,
        on_connected,
    ) -> DialResult:
        del on_connected
        self.requests.append(request)
        self.call_ids.append(call_id)
        return self.results.pop(0)


class SipSequenceDialer(SequenceDialer):
    dialer_type = "sip"
    manages_call_record = True

    def __init__(
        self,
        results: list[DialResult],
        *,
        terminate_result: bool = True,
    ) -> None:
        super().__init__(results)
        self.terminate_result = terminate_result
        self.terminated: list[tuple[OutboundDialRequest, str]] = []

    async def terminate(
        self,
        request: OutboundDialRequest,
        *,
        call_id: str,
        end_reason: str,
    ) -> bool:
        assert end_reason == "outbound_stale_recovery"
        self.terminated.append((request, call_id))
        return self.terminate_result


class FailingDialer:
    dialer_type = "mock"
    manages_call_record = False

    async def dial(
        self,
        request: OutboundDialRequest,
        *,
        call_id: str,
        on_connected,
    ) -> DialResult:
        del request, call_id, on_connected
        raise RuntimeError("mock gateway unavailable")


class UnexpectedFailingSipDialer:
    dialer_type = "sip"
    manages_call_record = True

    def __init__(self, *, terminate_result: bool) -> None:
        self.terminate_result = terminate_result
        self.terminated: list[tuple[OutboundDialRequest, str]] = []

    async def dial(
        self,
        request: OutboundDialRequest,
        *,
        call_id: str,
        on_connected,
    ) -> DialResult:
        del request, call_id, on_connected
        raise RuntimeError("unexpected SIP adapter error")

    async def terminate(
        self,
        request: OutboundDialRequest,
        *,
        call_id: str,
        end_reason: str,
    ) -> bool:
        assert end_reason == "outbound_dialer_error"
        self.terminated.append((request, call_id))
        return self.terminate_result


class BlockingDialer:
    dialer_type = "mock"
    manages_call_record = False

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def dial(
        self,
        request: OutboundDialRequest,
        *,
        call_id: str,
        on_connected,
    ) -> DialResult:
        del request, call_id, on_connected
        self.started.set()
        await self.release.wait()
        return DialResult(call_result="connected")


class BlockingSipDialer(BlockingDialer):
    dialer_type = "sip"
    manages_call_record = True


class LifecycleDialer:
    dialer_type = "linphone_test"
    manages_call_record = True

    def __init__(self) -> None:
        self.call_ids: list[str] = []
        self.connected = asyncio.Event()
        self.release = asyncio.Event()

    async def dial(
        self,
        request: OutboundDialRequest,
        *,
        call_id: str,
        on_connected,
    ) -> DialResult:
        del request
        self.call_ids.append(call_id)
        await on_connected()
        await on_connected()
        self.connected.set()
        await self.release.wait()
        return DialResult(call_result="connected", duration_ms=3000)


class RealStartupFailDialer:
    dialer_type = "linphone_test"
    manages_call_record = True

    async def dial(
        self,
        request: OutboundDialRequest,
        *,
        call_id: str,
        on_connected,
    ) -> DialResult:
        del request, call_id, on_connected
        raise RuntimeError("SIP startup failed before record")


class RealResultDialer:
    dialer_type = "linphone_test"
    manages_call_record = True

    def __init__(self, result: DialResult) -> None:
        self.result = result

    async def dial(
        self,
        request: OutboundDialRequest,
        *,
        call_id: str,
        on_connected,
    ) -> DialResult:
        del request, call_id, on_connected
        return self.result


def _snapshot(
    *,
    retry_count: int = 0,
    retry_intervals_minutes: list[int] | None = None,
    retryable_results: list[str] | None = None,
    call_windows: list[dict[str, str]] | None = None,
    line_snapshot: dict | None = None,
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
            **({"sipLine": line_snapshot} if line_snapshot is not None else {}),
        }
    )


def test_retry_policy_uses_explicit_results_and_never_retries_generic_failure() -> None:
    task = AiCallOutboundTaskModel(
        config_snapshot_json=_snapshot(
            retry_count=1,
            retry_intervals_minutes=[30],
            retryable_results=["rejected", "call_failed"],
        )
    )

    assert outbound_retry_interval(task, 1, "rejected") == 30
    assert outbound_retry_interval(task, 1, "call_failed") is None


def test_exception_category_groups_busy_with_no_answer() -> None:
    assert exception_category_for("no_answer") == "no_answer"
    assert exception_category_for("busy") == "no_answer"
    assert exception_category_for("rejected") == "rejected"
    assert exception_category_for("invalid_number") == "invalid_number"
    assert exception_category_for("call_failed") is None


def available_line_snapshot() -> dict:
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
    line_snapshot: dict | None = None,
) -> tuple[int, list[int]]:
    task_id = generate_snowflake_id()
    target_ids = [generate_snowflake_id() for _ in range(target_count)]
    async with database() as session:
        if line_snapshot is not None:
            existing_line = await session.get(
                AiCallSipLineModel,
                int(line_snapshot["lineId"]),
            )
            if existing_line is None:
                session.add(
                    AiCallSipLineModel(
                        id=int(line_snapshot["lineId"]),
                        tenant_id="tenant-a",
                        line_code=str(line_snapshot["lineCode"]),
                        line_name=str(line_snapshot["lineName"]),
                        enabled=True,
                        default_marker="OUTBOUND",
                        adapter_type=str(line_snapshot["adapterType"]),
                        route_mode=str(line_snapshot["routeMode"]),
                        trunk_id=line_snapshot["trunkId"],
                        proxy_host=line_snapshot["proxyHost"],
                        proxy_port=line_snapshot["proxyPort"],
                        auth_mode=str(line_snapshot["authMode"]),
                        caller_number=str(line_snapshot["callerNumber"]),
                        destination_country=str(
                            line_snapshot["destinationCountry"]
                        ),
                        max_concurrency=int(line_snapshot["maxConcurrency"]),
                        originate_timeout_seconds=int(
                            line_snapshot["originateTimeoutSeconds"]
                        ),
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
                line_id=(
                    int(line_snapshot["lineId"])
                    if line_snapshot is not None
                    else None
                ),
                line_name=(
                    str(line_snapshot["lineName"])
                    if line_snapshot is not None
                    else None
                ),
                config_snapshot_json=(
                    snapshot or _snapshot(line_snapshot=line_snapshot)
                ),
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
async def test_exception_batch_claims_only_current_pending_targets(database) -> None:
    now = datetime(2026, 8, 13, 1, 0, tzinfo=timezone.utc)
    task_id, target_ids = await _seed_task(database, now=now, target_count=2)
    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        first = await session.get(AiCallOutboundTargetModel, target_ids[0])
        second = await session.get(AiCallOutboundTargetModel, target_ids[1])
        assert task is not None and first is not None and second is not None
        task.status = "COMPLETED"
        task.completed_targets = 2
        task.ended_at = now
        for target in (first, second):
            target.status = "COMPLETED"
            target.attempt_count = 2
            target.latest_result = "no_answer"
            target.exception_category = "no_answer"
            target.exception_source_result = "no_answer"
            target.exception_original_attempt_count = 2
            target.exception_entered_at = now
        second.exception_batch_id = generate_snowflake_id()
        await session.commit()

    service = OutboundExceptionService()
    async with database() as session:
        batch = await service.start_batch(
            session,
            "tenant-a",
            1,
            "no_answer",
            "exception-batch-1",
        )
        await session.commit()
        first = await session.get(AiCallOutboundTargetModel, target_ids[0])
        second = await session.get(AiCallOutboundTargetModel, target_ids[1])
        assert first is not None and second is not None
        assert batch.accepted is True
        assert batch.target_count == 1
        assert first.exception_batch_id == int(batch.batch_id)
        assert first.status == "RETRY_WAIT"
        assert first.next_attempt_at is not None
        assert second.exception_batch_id != int(batch.batch_id)

    async with database() as session:
        replay = await service.start_batch(
            session,
            "tenant-a",
            1,
            "no_answer",
            "exception-batch-1",
        )
        assert replay.batch_id == batch.batch_id


@pytest.mark.anyio
async def test_terminal_original_failure_enters_exception_pool(database) -> None:
    now = datetime(2026, 8, 13, 2, 0, tzinfo=timezone.utc)
    _, target_ids = await _seed_task(database, now=now)
    executor = OutboundTaskExecutor(
        database,
        SequenceDialer([DialResult(call_result="busy")]),
        now_provider=lambda: now,
    )

    assert await executor.run_once() == 1

    async with database() as session:
        target = await session.get(AiCallOutboundTargetModel, target_ids[0])
        assert target is not None
        assert target.status == "COMPLETED"
        assert target.latest_result == "busy"
        assert target.exception_category == "no_answer"
        assert target.exception_source_result == "busy"
        assert target.exception_original_attempt_count == 1
        assert target.exception_batch_id is None


@pytest.mark.anyio
async def test_early_hangup_requires_customer_disconnect_evidence(database) -> None:
    now = datetime(2026, 8, 13, 2, 30, tzinfo=timezone.utc)
    _, target_ids = await _seed_task(database, now=now)
    executor = OutboundTaskExecutor(
        database,
        SequenceDialer([DialResult(call_result="connected", duration_ms=3_000)]),
        now_provider=lambda: now,
    )
    assert await executor.run_once() == 1

    async with database() as session:
        target = await session.get(AiCallOutboundTargetModel, target_ids[0])
        attempt = await session.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.target_id == target_ids[0]
            )
        )
        assert target is not None and attempt is not None
        record = await session.scalar(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id == attempt.call_id)
        )
        assert record is not None
        assert target.exception_category is None
        record.entry_type = "direct_sip"
        session.add(
            AiCallEndEvidenceModel(
                id=generate_snowflake_id(),
                tenant_id="tenant-a",
                call_id=attempt.call_id,
                command_id=None,
                source="livekit_webhook",
                end_reason="sip_participant_left",
                provider="livekit",
                provider_namespace="test",
                provider_event_id="event-1",
                event_at=now,
                received_at=now,
                dedupe_key="early-hangup-event-1",
                evidence_json=json.dumps(
                    {
                        "event": "participant_left",
                        "disconnectReason": "CLIENT_INITIATED",
                    }
                ),
            )
        )
        await session.flush()
        assert (
            await enroll_terminal_exception(
                session,
                target=target,
                attempt=attempt,
                record=record,
                now=now,
            )
            == "early_hangup"
        )
        assert target.latest_result == "early_hangup"


@pytest.mark.anyio
async def test_exception_batch_executes_without_reopening_source_task(database) -> None:
    now = datetime(2026, 8, 13, 3, 0, tzinfo=timezone.utc)
    task_id, target_ids = await _seed_task(database, now=now)
    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        target = await session.get(AiCallOutboundTargetModel, target_ids[0])
        assert task is not None and target is not None
        task.status = "COMPLETED"
        task.completed_targets = 1
        task.failed_targets = 1
        task.ended_at = now
        target.status = "COMPLETED"
        target.attempt_count = 1
        target.latest_result = "no_answer"
        target.exception_category = "no_answer"
        target.exception_source_result = "no_answer"
        target.exception_original_attempt_count = 1
        target.exception_entered_at = now
        await session.commit()

    service = OutboundExceptionService()
    async with database() as session:
        batch_out = await service.start_batch(
            session,
            "tenant-a",
            1,
            "no_answer",
            "completed-task-exception",
        )
        target = await session.get(AiCallOutboundTargetModel, target_ids[0])
        assert target is not None
        target.next_attempt_at = now
        await session.commit()

    executor = OutboundTaskExecutor(
        database,
        SequenceDialer([DialResult(call_result="connected", duration_ms=8_000)]),
        now_provider=lambda: now,
    )
    assert await executor.run_once() == 1

    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        target = await session.get(AiCallOutboundTargetModel, target_ids[0])
        batch = await session.get(
            AiCallOutboundExceptionBatchModel,
            int(batch_out.batch_id),
        )
        assert task is not None and target is not None and batch is not None
        assert task.status == "COMPLETED"
        assert target.status == "COMPLETED"
        assert target.attempt_count == 2
        assert target.latest_result == "connected"
        assert batch.status == "COMPLETED"
        assert batch.active_slot is None
        latest_attempt = await session.scalar(
            select(AiCallOutboundAttemptModel)
            .where(AiCallOutboundAttemptModel.target_id == target.id)
            .order_by(AiCallOutboundAttemptModel.attempt_no.desc())
            .limit(1)
        )
        assert latest_attempt is not None
        exception_handling = await AiCallRecordRepository(
            session
        ).get_exception_handling(
            tenant_id="tenant-a",
            call_id=latest_attempt.call_id,
        )
        assert exception_handling == {
            "category": "no_answer",
            "status": "CONNECTED",
            "originalAttemptCount": 1,
            "retryCount": 1,
            "maxRetryCount": 3,
            "lastResult": "connected",
        }


@pytest.mark.anyio
async def test_executor_passes_snapshotted_line_to_sip_dialer(database) -> None:
    now = datetime(2026, 7, 29, 8, 0, tzinfo=timezone.utc)
    dialer = SipSequenceDialer([DialResult(call_result="connected")])
    task_id, _ = await _seed_task(
        database,
        now=now,
        line_snapshot=available_line_snapshot(),
    )

    executor = OutboundTaskExecutor(database, dialer, now_provider=lambda: now)

    assert await executor.run_once() == 1
    assert dialer.requests[0].line is not None
    assert dialer.requests[0].line.line_code == "provider-a"

    async with database() as db:
        attempt = await db.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.task_id == task_id
            )
        )
    assert attempt is not None
    assert attempt.line_id == 340700000000000001
    assert attempt.line_code == "provider-a"


@pytest.mark.anyio
async def test_executor_uses_task_voice_snapshot_without_current_voice_asset(
    database,
) -> None:
    now = datetime(2026, 7, 29, 8, 0, tzinfo=timezone.utc)
    task_id, _ = await _seed_task(database, now=now)
    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        assert task is not None
        task.voice = "qwen-omni-vc-deleted"
        task.voice_name = "已删除的租户音色"
        task.voice_type = "自定义复刻"
        task.voice_target_model = "qwen3.5-omni-plus-realtime"
        await session.commit()

    dialer = SequenceDialer([DialResult(call_result="connected")])
    executor = OutboundTaskExecutor(database, dialer, now_provider=lambda: now)

    assert await executor.run_once() == 1
    assert len(dialer.requests) == 1
    assert dialer.requests[0].voice == "qwen-omni-vc-deleted"


@pytest.mark.anyio
async def test_executor_persists_provider_diagnostics(database) -> None:
    now = datetime(2026, 7, 29, 8, 0, tzinfo=timezone.utc)
    result = DialResult(
        call_result="call_failed",
        error_message="上游线路错误",
        provider_status_code="508",
        provider_reason="Q.850 cause=31",
        hangup_cause="NORMAL_UNSPECIFIED",
    )
    dialer = SipSequenceDialer([result])
    task_id, _ = await _seed_task(
        database,
        now=now,
        line_snapshot=available_line_snapshot(),
    )

    executor = OutboundTaskExecutor(database, dialer, now_provider=lambda: now)

    assert await executor.run_once() == 1
    async with database() as db:
        attempt = await db.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.task_id == task_id
            )
        )
    assert attempt is not None
    assert attempt.provider_status_code == "508"
    assert attempt.provider_reason == "Q.850 cause=31"
    assert attempt.hangup_cause == "NORMAL_UNSPECIFIED"


@pytest.mark.anyio
async def test_executor_keeps_attempt_pending_when_dialer_cannot_cleanup(database) -> None:
    now = datetime(2026, 7, 29, 8, 0, tzinfo=timezone.utc)
    dialer = SipSequenceDialer([
        DialResult(
            call_result="call_failed",
            error_message="SIP 资源清理失败，保持待对账",
            retry_allowed=False,
            settle_attempt=False,
        )
    ])
    task_id, target_ids = await _seed_task(
        database,
        now=now,
        line_snapshot=available_line_snapshot(),
    )
    executor = OutboundTaskExecutor(database, dialer, now_provider=lambda: now)

    assert await executor.run_once() == 1
    async with database() as db:
        task = await db.get(AiCallOutboundTaskModel, task_id)
        target = await db.get(AiCallOutboundTargetModel, target_ids[0])
        attempt = await db.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.task_id == task_id
            )
        )
    assert task is not None and task.status == "RUNNING"
    assert target is not None and target.status == "DIALING"
    assert attempt is not None and attempt.status == "DIALING"


@pytest.mark.anyio
async def test_executor_keeps_managed_exception_pending_when_cleanup_fails(
    database,
) -> None:
    now = datetime(2026, 7, 29, 8, 0, tzinfo=timezone.utc)
    dialer = UnexpectedFailingSipDialer(terminate_result=False)
    task_id, target_ids = await _seed_task(
        database,
        now=now,
        line_snapshot=available_line_snapshot(),
    )
    executor = OutboundTaskExecutor(database, dialer, now_provider=lambda: now)

    assert await executor.run_once() == 1
    assert len(dialer.terminated) == 1
    async with database() as db:
        task = await db.get(AiCallOutboundTaskModel, task_id)
        target = await db.get(AiCallOutboundTargetModel, target_ids[0])
        attempt = await db.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.task_id == task_id
            )
        )
    assert task is not None and task.status == "RUNNING"
    assert target is not None and target.status == "DIALING"
    assert attempt is not None and attempt.status == "DIALING"


@pytest.mark.anyio
async def test_sip_executor_fails_task_without_line_snapshot(database) -> None:
    now = datetime(2026, 7, 29, 8, 0, tzinfo=timezone.utc)
    dialer = SipSequenceDialer([DialResult(call_result="connected")])
    task_id, target_ids = await _seed_task(database, now=now)

    executor = OutboundTaskExecutor(database, dialer, now_provider=lambda: now)

    assert await executor.run_once() == 0
    assert dialer.requests == []
    async with database() as db:
        task = await db.get(AiCallOutboundTaskModel, task_id)
        target = await db.get(AiCallOutboundTargetModel, target_ids[0])
        attempts = (
            await db.scalars(
                select(AiCallOutboundAttemptModel).where(
                    AiCallOutboundAttemptModel.task_id == task_id
                )
            )
        ).all()
    assert task is not None
    assert task.status == "FAILED"
    assert task.error_message == "任务缺少有效的 SIP 线路快照"
    assert target is not None and target.status == "PENDING"
    assert attempts == []


@pytest.mark.anyio
async def test_sip_executor_does_not_dial_disabled_snapshotted_line(database) -> None:
    now = datetime(2026, 7, 29, 8, 0, tzinfo=timezone.utc)
    dialer = SipSequenceDialer([DialResult(call_result="connected")])
    task_id, target_ids = await _seed_task(
        database,
        now=now,
        line_snapshot=available_line_snapshot(),
    )
    async with database() as db:
        line = await db.get(AiCallSipLineModel, 340700000000000001)
        assert line is not None
        line.enabled = False
        await db.commit()

    executor = OutboundTaskExecutor(database, dialer, now_provider=lambda: now)

    assert await executor.run_once() == 0
    assert dialer.requests == []
    async with database() as db:
        task = await db.get(AiCallOutboundTaskModel, task_id)
        target = await db.get(AiCallOutboundTargetModel, target_ids[0])
    assert task is not None and task.status == "FAILED"
    assert task.error_message == "任务绑定的 SIP 线路已停用或删除"
    assert target is not None and target.attempt_count == 0


@pytest.mark.anyio
async def test_sip_executor_respects_persisted_line_concurrency(database) -> None:
    now = datetime(2026, 7, 29, 8, 0, tzinfo=timezone.utc)
    line = available_line_snapshot()
    task_id, _ = await _seed_task(
        database,
        now=now,
        line_snapshot=line,
    )
    async with database() as db:
        db.add(
            AiCallOutboundAttemptModel(
                id=generate_snowflake_id(),
                tenant_id="tenant-a",
                task_id=generate_snowflake_id(),
                target_id=generate_snowflake_id(),
                attempt_no=1,
                call_id="existing-live-call",
                dialer_type="sip",
                status="IN_CALL",
                call_result=None,
                error_message=None,
                line_id=int(line["lineId"]),
                line_code=str(line["lineCode"]),
                provider_status_code=None,
                provider_reason=None,
                hangup_cause=None,
                started_at=now,
                ended_at=None,
                created_at=now,
                updated_at=now,
            )
        )
        await db.commit()
    dialer = SipSequenceDialer([DialResult(call_result="connected")])

    executor = OutboundTaskExecutor(database, dialer, now_provider=lambda: now)

    assert await executor.run_once() == 0
    assert dialer.requests == []
    async with database() as db:
        task = await db.get(AiCallOutboundTaskModel, task_id)
    assert task is not None and task.status == "RUNNING"


def test_mock_dialer_declares_non_record_managed_protocol() -> None:
    dialer = MockOutboundDialer()

    assert dialer.dialer_type == "mock"
    assert dialer.manages_call_record is False


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("configured_result", "expected_result"),
    [
        ("connected", DialResult(call_result="connected")),
        (
            "busy",
            DialResult(
                call_result="busy",
                error_message="模拟拨打结果：busy",
            ),
        ),
    ],
)
async def test_mock_dialer_preserves_result_without_connected_callback(
    configured_result,
    expected_result,
) -> None:
    callback_count = 0

    async def on_connected() -> None:
        nonlocal callback_count
        callback_count += 1

    request = OutboundDialRequest(
        tenant_id="tenant-a",
        task_id=1,
        target_id=2,
        attempt_no=1,
        phone_number="13800138000",
        customer_name="测试客户",
        scene_code="intro_contract",
        voice="Tina",
        prompt_profile_id="prompt-1",
    )

    result = await MockOutboundDialer(configured_result).dial(
        request,
        call_id="stable-call-id",
        on_connected=on_connected,
    )

    assert result == expected_result
    assert callback_count == 0


@pytest.mark.anyio
async def test_manual_claim_bypasses_schedule_and_window_and_writes_metadata(
    database,
) -> None:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    task_id, target_ids = await _seed_task(
        database,
        now=now,
        execution_mode="scheduled",
        scheduled_at=now + timedelta(days=1),
        snapshot=_snapshot(
            call_windows=[{"startTime": "09:00", "endTime": "10:00"}],
        ),
    )
    executor = OutboundTaskExecutor(
        database,
        LifecycleDialer(),
        now_provider=lambda: now,
        business_timezone="UTC",
    )

    claimed = await executor.claim_manual_test(
        TaskKey("tenant-a", task_id),
        command_idempotency_key="command-1",
        test_scenario="ai_only",
        active_slot="linphone_test",
    )

    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        target = await session.get(AiCallOutboundTargetModel, target_ids[0])
        attempt = await session.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.call_id == claimed.call_id
            )
        )
        record_count = int(
            await session.scalar(select(func.count(AiCallRecordModel.id))) or 0
        )

    assert task is not None and task.status == "RUNNING"
    assert task.started_at == _sqlite_time(now)
    assert target is not None and target.status == "DIALING"
    assert target.attempt_count == 1
    assert attempt is not None
    assert attempt.status == "DIALING"
    assert attempt.dialer_type == "linphone_test"
    assert attempt.test_scenario == "ai_only"
    assert attempt.command_idempotency_key == "command-1"
    assert attempt.active_slot == "linphone_test"
    assert record_count == 0


@pytest.mark.anyio
async def test_manual_claim_rejects_non_scheduled_task_without_changes(database) -> None:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    task_id, target_ids = await _seed_task(database, now=now)
    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        assert task is not None
        task.status = "RUNNING"
        await session.commit()

    executor = OutboundTaskExecutor(
        database,
        LifecycleDialer(),
        now_provider=lambda: now,
    )
    with pytest.raises(ValueError, match="SCHEDULED"):
        await executor.claim_manual_test(
            TaskKey("tenant-a", task_id),
            command_idempotency_key="command-non-scheduled",
            test_scenario="ai_only",
            active_slot="linphone_test",
        )

    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        target = await session.get(AiCallOutboundTargetModel, target_ids[0])
        attempt_count = int(
            await session.scalar(
                select(func.count(AiCallOutboundAttemptModel.id)).where(
                    AiCallOutboundAttemptModel.task_id == task_id
                )
            )
            or 0
        )
    assert task is not None
    assert task.status == "RUNNING"
    assert task.started_at is None
    assert target is not None
    assert target.status == "PENDING"
    assert target.attempt_count == 0
    assert attempt_count == 0


@pytest.mark.anyio
async def test_manual_claim_rejects_multi_target_task_without_attempt(database) -> None:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    task_id, target_ids = await _seed_task(database, now=now, target_count=2)
    executor = OutboundTaskExecutor(
        database,
        LifecycleDialer(),
        now_provider=lambda: now,
    )

    with pytest.raises(ValueError, match="单个 PENDING 对象"):
        await executor.claim_manual_test(
            TaskKey("tenant-a", task_id),
            command_idempotency_key="command-multi-target",
            test_scenario="ai_only",
            active_slot="linphone_test",
        )

    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        targets = (
            await session.scalars(
                select(AiCallOutboundTargetModel)
                .where(AiCallOutboundTargetModel.id.in_(target_ids))
                .order_by(AiCallOutboundTargetModel.id)
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
    assert task.status == "SCHEDULED"
    assert task.started_at is None
    assert [target.status for target in targets] == ["PENDING", "PENDING"]
    assert [target.attempt_count for target in targets] == [0, 0]
    assert attempt_count == 0


@pytest.mark.anyio
async def test_manual_claim_rejects_non_pending_target_without_attempt(database) -> None:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    task_id, target_ids = await _seed_task(database, now=now)
    async with database() as session:
        target = await session.get(AiCallOutboundTargetModel, target_ids[0])
        assert target is not None
        target.status = "RETRY_WAIT"
        target.next_attempt_at = now + timedelta(minutes=5)
        await session.commit()

    executor = OutboundTaskExecutor(
        database,
        LifecycleDialer(),
        now_provider=lambda: now,
    )
    with pytest.raises(ValueError, match="单个 PENDING 对象"):
        await executor.claim_manual_test(
            TaskKey("tenant-a", task_id),
            command_idempotency_key="command-non-pending",
            test_scenario="ai_only",
            active_slot="linphone_test",
        )

    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        target = await session.get(AiCallOutboundTargetModel, target_ids[0])
        attempt_count = int(
            await session.scalar(
                select(func.count(AiCallOutboundAttemptModel.id)).where(
                    AiCallOutboundAttemptModel.task_id == task_id
                )
            )
            or 0
        )
    assert task is not None
    assert task.status == "SCHEDULED"
    assert task.started_at is None
    assert target is not None
    assert target.status == "RETRY_WAIT"
    assert target.attempt_count == 0
    assert target.next_attempt_at == _sqlite_time(now + timedelta(minutes=5))
    assert attempt_count == 0


@pytest.mark.anyio
async def test_execute_claimed_reuses_call_id_and_preserves_in_call_intermediate_state(
    database,
) -> None:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    task_id, target_ids = await _seed_task(database, now=now)
    dialer = LifecycleDialer()
    executor = OutboundTaskExecutor(database, dialer, now_provider=lambda: now)
    claimed = await executor.claim_manual_test(
        TaskKey("tenant-a", task_id),
        command_idempotency_key="command-lifecycle",
        test_scenario="ai_only",
        active_slot="linphone_test",
    )

    execution = asyncio.create_task(executor.execute_claimed(claimed))
    await asyncio.wait_for(dialer.connected.wait(), timeout=0.5)

    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        target = await session.get(AiCallOutboundTargetModel, target_ids[0])
        attempt = await session.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.call_id == claimed.call_id
            )
        )
    assert dialer.call_ids == [claimed.call_id]
    assert task is not None and task.status == "RUNNING"
    assert target is not None and target.status == "IN_CALL"
    assert attempt is not None and attempt.status == "IN_CALL"
    assert attempt.active_slot == "linphone_test"

    dialer.release.set()
    await execution

    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        target = await session.get(AiCallOutboundTargetModel, target_ids[0])
        attempt = await session.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.call_id == claimed.call_id
            )
        )
    assert task is not None and task.status == "COMPLETED"
    assert target is not None and target.status == "COMPLETED"
    assert target.latest_result == "connected"
    assert attempt is not None and attempt.status == "COMPLETED"
    assert attempt.call_result == "connected"
    assert attempt.active_slot is None


@pytest.mark.anyio
async def test_execute_claimed_retries_transient_sqlite_lock_when_settling(
    tmp_path,
) -> None:
    now = datetime(2026, 7, 29, 8, 0, tzinfo=timezone.utc)
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'locked-executor.db'}",
        connect_args={"timeout": 0.05},
    )
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    database = async_sessionmaker(engine, expire_on_commit=False)
    task_id, target_ids = await _seed_task(database, now=now)
    retry_delays: list[float] = []

    try:
        async with database() as lock_session:

            async def release_sqlite_lock(delay: float) -> None:
                retry_delays.append(delay)
                await lock_session.rollback()

            executor = OutboundTaskExecutor(
                database,
                RealResultDialer(DialResult(call_result="connected")),
                now_provider=lambda: now,
                settle_retry_delays_seconds=(0.25,),
                sleep=release_sqlite_lock,
            )
            claimed = await executor.claim_manual_test(
                TaskKey("tenant-a", task_id),
                command_idempotency_key="command-sqlite-lock-retry",
                test_scenario="ai_only",
                active_slot="linphone_test",
            )
            await lock_session.execute(
                update(AiCallOutboundTaskModel)
                .where(AiCallOutboundTaskModel.id == task_id)
                .values(updated_at=now + timedelta(seconds=1))
            )

            await executor.execute_claimed(claimed)

        async with database() as session:
            task = await session.get(AiCallOutboundTaskModel, task_id)
            target = await session.get(AiCallOutboundTargetModel, target_ids[0])
            attempt = await session.scalar(
                select(AiCallOutboundAttemptModel).where(
                    AiCallOutboundAttemptModel.call_id == claimed.call_id
                )
            )
    finally:
        await engine.dispose()

    assert retry_delays == [0.25]
    assert task is not None and task.status == "COMPLETED"
    assert target is not None and target.status == "COMPLETED"
    assert attempt is not None and attempt.status == "COMPLETED"


@pytest.mark.anyio
async def test_real_startup_failure_without_record_still_finishes_attempt(database) -> None:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    task_id, target_ids = await _seed_task(database, now=now)
    executor = OutboundTaskExecutor(
        database,
        RealStartupFailDialer(),
        now_provider=lambda: now,
    )
    claimed = await executor.claim_manual_test(
        TaskKey("tenant-a", task_id),
        command_idempotency_key="command-startup-fail",
        test_scenario="ai_only",
        active_slot="linphone_test",
    )

    await executor.execute_claimed(claimed)

    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        target = await session.get(AiCallOutboundTargetModel, target_ids[0])
        attempt = await session.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.call_id == claimed.call_id
            )
        )
        record = await session.scalar(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id == claimed.call_id)
        )
    assert task is not None and task.status == "COMPLETED"
    assert task.failed_targets == 1
    assert target is not None and target.status == "COMPLETED"
    assert target.latest_result == "call_failed"
    assert attempt is not None and attempt.status == "FAILED"
    assert attempt.error_message == "SIP startup failed before record"
    assert attempt.active_slot is None
    assert record is None


@pytest.mark.anyio
async def test_real_managed_record_fields_are_not_overwritten(database) -> None:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    task_id, _ = await _seed_task(database, now=now)
    executor = OutboundTaskExecutor(
        database,
        RealResultDialer(
            DialResult(call_result="call_failed", error_message="executor result")
        ),
        now_provider=lambda: now,
    )
    claimed = await executor.claim_manual_test(
        TaskKey("tenant-a", task_id),
        command_idempotency_key="command-real-record",
        test_scenario="ai_only",
        active_slot="linphone_test",
    )
    async with database() as session:
        session.add(
            AiCallRecordModel(
                id=generate_snowflake_id(),
                call_id=claimed.call_id,
                follow_up_id=None,
                business_type="outbound_task",
                business_id=str(task_id),
                scene_code="intro_contract",
                prompt_source_key="prompt-1",
                entry_type="sip",
                room_name=f"ai-call-{claimed.call_id}",
                participant_identity=f"sip-{claimed.call_id}",
                callee_phone_number_hash=None,
                callee_phone_number_masked=None,
                status="failed",
                end_reason="provider_hangup",
                failure_stage="sip_runtime",
                failure_message="provider detail",
                started_at=now,
                answered_at=now,
                ended_at=now,
                duration_ms=1234,
            )
        )
        await session.commit()

    await executor.execute_claimed(claimed)

    async with database() as session:
        record = await session.scalar(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id == claimed.call_id)
        )
    assert record is not None
    assert record.status == "failed"
    assert record.end_reason == "provider_hangup"
    assert record.failure_stage == "sip_runtime"
    assert record.failure_message == "provider detail"
    assert record.duration_ms == 1234


@pytest.mark.anyio
async def test_manual_claim_unique_guards_roll_back_task_transition(database) -> None:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    first_task_id, _ = await _seed_task(database, now=now)
    command_task_id, _ = await _seed_task(database, now=now)
    slot_task_id, _ = await _seed_task(database, now=now)
    executor = OutboundTaskExecutor(
        database,
        LifecycleDialer(),
        now_provider=lambda: now,
    )
    await executor.claim_manual_test(
        TaskKey("tenant-a", first_task_id),
        command_idempotency_key="same-command",
        test_scenario="ai_only",
        active_slot="same-slot",
    )

    with pytest.raises(IntegrityError):
        await executor.claim_manual_test(
            TaskKey("tenant-a", command_task_id),
            command_idempotency_key="same-command",
            test_scenario="ai_only",
            active_slot="different-slot",
        )
    with pytest.raises(IntegrityError):
        await executor.claim_manual_test(
            TaskKey("tenant-a", slot_task_id),
            command_idempotency_key="different-command",
            test_scenario="ai_only",
            active_slot="same-slot",
        )

    async with database() as session:
        command_task = await session.get(AiCallOutboundTaskModel, command_task_id)
        slot_task = await session.get(AiCallOutboundTaskModel, slot_task_id)
        conflicting_attempts = int(
            await session.scalar(
                select(func.count(AiCallOutboundAttemptModel.id)).where(
                    AiCallOutboundAttemptModel.task_id.in_(
                        [command_task_id, slot_task_id]
                    )
                )
            )
            or 0
        )
    assert command_task is not None and command_task.status == "SCHEDULED"
    assert slot_task is not None and slot_task.status == "SCHEDULED"
    assert conflicting_attempts == 0


@pytest.mark.anyio
async def test_stale_recovery_ignores_real_managed_attempt(database) -> None:
    clock = [datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)]
    task_id, target_ids = await _seed_task(database, now=clock[0])
    claiming_executor = OutboundTaskExecutor(
        database,
        LifecycleDialer(),
        now_provider=lambda: clock[0],
        dialing_timeout_seconds=300,
    )
    claimed = await claiming_executor.claim_manual_test(
        TaskKey("tenant-a", task_id),
        command_idempotency_key="command-stale-real",
        test_scenario="ai_only",
        active_slot="linphone_test",
    )

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
                AiCallOutboundAttemptModel.call_id == claimed.call_id
            )
        )
    assert task is not None and task.status == "RUNNING"
    assert target is not None and target.status == "DIALING"
    assert attempt is not None and attempt.status == "DIALING"
    assert recovery_dialer.requests == []


@pytest.mark.anyio
@pytest.mark.parametrize("media_evidence", ["event", "completed_tracks"])
async def test_stale_recovery_reconciles_terminal_sip_attempt_without_redial(
    database,
    media_evidence,
) -> None:
    clock = [datetime(2026, 7, 29, 8, 0, tzinfo=timezone.utc)]
    task_id, target_ids = await _seed_task(
        database,
        now=clock[0],
        line_snapshot=available_line_snapshot(),
    )
    blocking_dialer = BlockingSipDialer()
    executor = OutboundTaskExecutor(
        database,
        blocking_dialer,
        now_provider=lambda: clock[0],
        dialing_timeout_seconds=300,
    )
    interrupted_run = asyncio.create_task(executor.run_once())
    await asyncio.wait_for(blocking_dialer.started.wait(), timeout=0.5)
    interrupted_run.cancel()
    await asyncio.gather(interrupted_run, return_exceptions=True)

    async with database() as db:
        attempt = await db.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.task_id == task_id
            )
        )
        assert attempt is not None
        answered_at = clock[0] + timedelta(seconds=2)
        db.add(
            AiCallRecordModel(
                id=generate_snowflake_id(),
                tenant_id="tenant-a",
                call_id=attempt.call_id,
                business_type="outbound_task",
                business_id=str(task_id),
                scene_code="intro_contract",
                prompt_source_key="prompt-1",
                entry_type="sip_outbound",
                room_name=f"ai-call-{attempt.call_id}",
                participant_identity=f"sip-{attempt.call_id}",
                callee_phone_number_hash=None,
                callee_phone_number_masked=None,
                status="completed",
                end_reason="remote_hangup",
                failure_stage=None,
                failure_message=None,
                started_at=clock[0],
                answered_at=answered_at,
                ended_at=clock[0] + timedelta(seconds=20),
                duration_ms=18000,
            )
        )
        if media_evidence == "event":
            db.add(
                AiCallEventModel(
                    id=generate_snowflake_id(),
                    call_id=attempt.call_id,
                    event_id=f"event-{generate_snowflake_id()}",
                    event_type="media_connected",
                    source="livekit",
                    event_time=answered_at,
                    payload_json=None,
                )
            )
        else:
            for role in ("ai", "customer"):
                db.add(
                    AiCallRecordingTrackModel(
                        id=generate_snowflake_id(),
                        tenant_id="tenant-a",
                        call_id=attempt.call_id,
                        room_name=f"ai-call-{attempt.call_id}",
                        track_role=role,
                        participant_identity=f"{role}-{attempt.call_id}",
                        handoff_id=None,
                        status="completed",
                        egress_id=f"egress-{role}-{attempt.call_id}",
                        oss_id=generate_snowflake_id(),
                        object_name=(
                            f"ai-call/recordings/{attempt.call_id}-{role}.ogg"
                        ),
                        started_at=answered_at,
                        ended_at=clock[0] + timedelta(seconds=20),
                        duration_ms=18000,
                    )
                )
        await db.commit()

    clock[0] += timedelta(minutes=5)
    recovery_dialer = SipSequenceDialer([])
    recovery_executor = OutboundTaskExecutor(
        database,
        recovery_dialer,
        now_provider=lambda: clock[0],
        dialing_timeout_seconds=300,
    )

    assert await recovery_executor.run_once() == 0
    assert recovery_dialer.requests == []
    async with database() as db:
        task = await db.get(AiCallOutboundTaskModel, task_id)
        target = await db.get(AiCallOutboundTargetModel, target_ids[0])
        attempt = await db.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.task_id == task_id
            )
        )
    assert task is not None and task.status == "COMPLETED"
    assert target is not None and target.latest_result == "connected"
    assert attempt is not None and attempt.status == "COMPLETED"


@pytest.mark.anyio
async def test_stale_sip_attempt_without_record_fails_without_retry(database) -> None:
    clock = [datetime(2026, 7, 29, 8, 0, tzinfo=timezone.utc)]
    line = available_line_snapshot()
    task_id, target_ids = await _seed_task(
        database,
        now=clock[0],
        line_snapshot=line,
        snapshot=_snapshot(
            retry_count=1,
            retry_intervals_minutes=[1],
            retryable_results=["call_failed"],
            line_snapshot=line,
        ),
    )
    blocking_dialer = BlockingSipDialer()
    executor = OutboundTaskExecutor(
        database,
        blocking_dialer,
        now_provider=lambda: clock[0],
        dialing_timeout_seconds=300,
    )
    interrupted_run = asyncio.create_task(executor.run_once())
    await asyncio.wait_for(blocking_dialer.started.wait(), timeout=0.5)
    interrupted_run.cancel()
    await asyncio.gather(interrupted_run, return_exceptions=True)

    clock[0] += timedelta(minutes=5)
    recovery_dialer = SipSequenceDialer([])
    recovery_executor = OutboundTaskExecutor(
        database,
        recovery_dialer,
        now_provider=lambda: clock[0],
        dialing_timeout_seconds=300,
    )

    assert await recovery_executor.run_once() == 0
    assert len(recovery_dialer.terminated) == 1
    async with database() as db:
        task = await db.get(AiCallOutboundTaskModel, task_id)
        target = await db.get(AiCallOutboundTargetModel, target_ids[0])
        attempt = await db.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.task_id == task_id
            )
        )
    assert task is not None and task.status == "COMPLETED"
    assert target is not None and target.status == "COMPLETED"
    assert target.next_attempt_at is None
    assert attempt is not None and attempt.status == "FAILED"
    assert attempt.error_message == (
        "正式 SIP 执行器中断且未找到通话记录，禁止自动重拨"
    )


@pytest.mark.anyio
async def test_stale_sip_attempt_stays_pending_when_cleanup_fails(database) -> None:
    clock = [datetime(2026, 7, 29, 8, 0, tzinfo=timezone.utc)]
    line = available_line_snapshot()
    task_id, target_ids = await _seed_task(
        database,
        now=clock[0],
        line_snapshot=line,
    )
    blocking_dialer = BlockingSipDialer()
    executor = OutboundTaskExecutor(
        database,
        blocking_dialer,
        now_provider=lambda: clock[0],
        dialing_timeout_seconds=300,
    )
    interrupted_run = asyncio.create_task(executor.run_once())
    await asyncio.wait_for(blocking_dialer.started.wait(), timeout=0.5)
    interrupted_run.cancel()
    await asyncio.gather(interrupted_run, return_exceptions=True)

    clock[0] += timedelta(minutes=5)
    recovery_dialer = SipSequenceDialer([], terminate_result=False)
    recovery_executor = OutboundTaskExecutor(
        database,
        recovery_dialer,
        now_provider=lambda: clock[0],
        dialing_timeout_seconds=300,
    )

    assert await recovery_executor.run_once() == 0
    assert len(recovery_dialer.terminated) == 1
    async with database() as db:
        task = await db.get(AiCallOutboundTaskModel, task_id)
        target = await db.get(AiCallOutboundTargetModel, target_ids[0])
        attempt = await db.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.task_id == task_id
            )
        )
    assert task is not None and task.status == "RUNNING"
    assert target is not None and target.status == "DIALING"
    assert attempt is not None and attempt.status == "DIALING"


@pytest.mark.anyio
async def test_stale_recovery_finishes_legacy_null_dialer_attempt(database) -> None:
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

    async with database() as session:
        attempt = await session.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.task_id == task_id
            )
        )
        assert attempt is not None
        attempt.dialer_type = None
        await session.commit()

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
    assert attempt.dialer_type is None
    assert attempt.status == "FAILED"
    assert attempt.call_result == "call_failed"
    assert attempt.error_message == "执行器中断，拨打结果未知"
    assert record is not None and record.status == "failed"
    assert recovery_dialer.requests == []


@pytest.mark.anyio
async def test_stale_mock_attempt_updates_record_with_real_managed_executor(
    database,
) -> None:
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
    recovery_executor = OutboundTaskExecutor(
        database,
        LifecycleDialer(),
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
    assert attempt.dialer_type == "mock"
    assert attempt.status == "FAILED"
    assert record is not None
    assert record.status == "failed"
    assert record.end_reason == "call_failed"
    assert record.failure_stage == "outbound_mock"


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
    executor = OutboundTaskExecutor(
        database,
        dialer,
        now_provider=lambda: now,
        business_timezone="UTC",
    )

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
    assert dialer.call_ids == [attempts[0].call_id]


@pytest.mark.anyio
async def test_mock_executor_counts_partial_success_and_failure(database) -> None:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    task_id, target_ids = await _seed_task(database, now=now, target_count=2)
    dialer = SequenceDialer([
        DialResult(call_result="connected"),
        DialResult(call_result="call_failed", error_message="模拟呼叫失败"),
    ])
    executor = OutboundTaskExecutor(database, dialer, now_provider=lambda: now)

    assert await executor.run_once() == 2

    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        targets = (
            await session.scalars(
                select(AiCallOutboundTargetModel)
                .where(AiCallOutboundTargetModel.id.in_(target_ids))
                .order_by(AiCallOutboundTargetModel.source_row_number)
            )
        ).all()
        attempts = (
            await session.scalars(
                select(AiCallOutboundAttemptModel)
                .where(AiCallOutboundAttemptModel.task_id == task_id)
                .order_by(AiCallOutboundAttemptModel.created_at)
            )
        ).all()

    assert task is not None
    assert task.status == "COMPLETED"
    assert task.completed_targets == 2
    assert task.connected_targets == 1
    assert task.failed_targets == 1
    assert [target.status for target in targets] == ["COMPLETED", "COMPLETED"]
    assert [target.latest_result for target in targets] == [
        "connected",
        "call_failed",
    ]
    assert [attempt.status for attempt in attempts] == ["COMPLETED", "FAILED"]
    assert {attempt.dialer_type for attempt in attempts} == {"mock"}


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
    assert closed_task.next_dispatch_at == datetime(2026, 7, 28, 10, 0)
    assert due_task is not None and due_task.status == "COMPLETED"


@pytest.mark.anyio
async def test_running_tasks_rotate_fairly_across_polls(database) -> None:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    first_task_id, _ = await _seed_task(database, now=now, target_count=2)
    second_task_id, _ = await _seed_task(database, now=now, target_count=2)
    async with database() as session:
        await session.execute(
            AiCallOutboundTaskModel.__table__.update()
            .where(AiCallOutboundTaskModel.id.in_([first_task_id, second_task_id]))
            .values(status="RUNNING", started_at=now)
        )
        await session.commit()

    dialer = SequenceDialer([
        DialResult(call_result="connected"),
        DialResult(call_result="connected"),
    ])
    executor = OutboundTaskExecutor(
        database,
        dialer,
        task_batch_size=1,
        target_batch_size=1,
        now_provider=lambda: now,
        business_timezone="UTC",
    )

    assert await executor.run_once() == 1
    assert await executor.run_once() == 1
    assert {request.task_id for request in dialer.requests} == {
        first_task_id,
        second_task_id,
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("transitional_status", "expected_status", "expected_target_status"),
    [
        ("PAUSING", "PAUSED", "PENDING"),
        ("STOPPING", "STOPPED", "CANCELLED"),
    ],
)
async def test_poll_reconciles_transitional_task_without_active_attempt(
    database,
    transitional_status,
    expected_status,
    expected_target_status,
) -> None:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    task_id, target_ids = await _seed_task(database, now=now)
    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        assert task is not None
        task.status = transitional_status
        task.started_at = now
        await session.commit()

    executor = OutboundTaskExecutor(
        database,
        SequenceDialer([]),
        now_provider=lambda: now,
    )
    assert await executor.run_once() == 0

    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
        target = await session.get(AiCallOutboundTargetModel, target_ids[0])
    assert task is not None and task.status == expected_status
    assert target is not None and target.status == expected_target_status


@pytest.mark.anyio
async def test_executor_does_not_claim_cross_tenant_target_with_corrupt_link(
    database,
) -> None:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    task_id, target_ids = await _seed_task(database, now=now)
    async with database() as session:
        target = await session.get(AiCallOutboundTargetModel, target_ids[0])
        assert target is not None
        target.tenant_id = "tenant-b"
        await session.commit()

    dialer = SequenceDialer([])
    executor = OutboundTaskExecutor(database, dialer, now_provider=lambda: now)
    assert await executor.run_once() == 0

    async with database() as session:
        target = await session.get(AiCallOutboundTargetModel, target_ids[0])
        attempt_count = int(
            await session.scalar(select(func.count(AiCallOutboundAttemptModel.id)))
            or 0
        )
    assert target is not None and target.status == "PENDING"
    assert attempt_count == 0
    assert dialer.requests == []


@pytest.mark.anyio
async def test_scheduled_time_uses_configured_business_timezone(database) -> None:
    now = datetime(2026, 7, 28, 2, 0, tzinfo=timezone.utc)
    task_id, _ = await _seed_task(
        database,
        now=now,
        execution_mode="scheduled",
        scheduled_at=datetime(2026, 7, 28, 10, 0, tzinfo=timezone.utc),
        snapshot=_snapshot(
            call_windows=[{"startTime": "09:00", "endTime": "11:00"}],
        ),
    )
    executor = OutboundTaskExecutor(
        database,
        SequenceDialer([DialResult(call_result="connected")]),
        now_provider=lambda: now,
        business_timezone="Asia/Shanghai",
    )

    assert await executor.run_once() == 1
    async with database() as session:
        task = await session.get(AiCallOutboundTaskModel, task_id)
    assert task is not None and task.status == "COMPLETED"


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
async def test_stop_waits_for_in_call_attempt_before_stopping(database) -> None:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    task_id, target_ids = await _seed_task(database, now=now, target_count=2)
    dialer = LifecycleDialer()
    executor = OutboundTaskExecutor(database, dialer, now_provider=lambda: now)
    run_task = asyncio.create_task(executor.run_once())
    await asyncio.wait_for(dialer.connected.wait(), timeout=0.5)

    service = OutboundRuleTaskService(database)
    async with database() as session:
        await service.run_action(session, "tenant-a", task_id, "stop")
        await session.commit()
        task = await session.get(AiCallOutboundTaskModel, task_id)
        targets = (
            await session.scalars(
                select(AiCallOutboundTargetModel)
                .where(AiCallOutboundTargetModel.id.in_(target_ids))
                .order_by(AiCallOutboundTargetModel.source_row_number)
            )
        ).all()
        attempt = await session.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.task_id == task_id
            )
        )
    assert task is not None and task.status == "STOPPING"
    assert [target.status for target in targets] == ["IN_CALL", "CANCELLED"]
    assert attempt is not None and attempt.status == "IN_CALL"
    assert not run_task.done()

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
    assert task is not None and task.status == "STOPPED"
    assert [target.status for target in targets] == ["COMPLETED", "CANCELLED"]


@pytest.mark.anyio
async def test_pause_waits_for_in_call_attempt_before_pausing(database) -> None:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    task_id, target_ids = await _seed_task(database, now=now, target_count=2)
    dialer = LifecycleDialer()
    executor = OutboundTaskExecutor(database, dialer, now_provider=lambda: now)
    run_task = asyncio.create_task(executor.run_once())
    await asyncio.wait_for(dialer.connected.wait(), timeout=0.5)

    service = OutboundRuleTaskService(database)
    async with database() as session:
        await service.run_action(session, "tenant-a", task_id, "pause")
        await session.commit()
        task = await session.get(AiCallOutboundTaskModel, task_id)
        targets = (
            await session.scalars(
                select(AiCallOutboundTargetModel)
                .where(AiCallOutboundTargetModel.id.in_(target_ids))
                .order_by(AiCallOutboundTargetModel.source_row_number)
            )
        ).all()
        attempt = await session.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.task_id == task_id
            )
        )
    assert task is not None and task.status == "PAUSING"
    assert [target.status for target in targets] == ["IN_CALL", "PENDING"]
    assert attempt is not None and attempt.status == "IN_CALL"
    assert not run_task.done()

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
    assert task is not None and task.status == "PAUSED"
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


def test_outbound_dialer_mode_defaults_to_mock() -> None:
    assert Settings.model_fields["AI_CALL_OUTBOUND_DIALER_MODE"].default == "mock"


@pytest.mark.anyio
async def test_worker_uses_sip_only_when_explicitly_selected(monkeypatch) -> None:
    from app.plugin import init_app

    async def do_not_start(self) -> None:
        del self

    monkeypatch.setattr(OutboundTaskWorker, "start", do_not_start)
    monkeypatch.setattr(init_app.settings, "SQL_DB_ENABLE", True)
    monkeypatch.setattr(init_app.settings, "AI_CALL_OUTBOUND_EXECUTOR_ENABLED", True)
    monkeypatch.setattr(init_app.settings, "AI_CALL_OUTBOUND_DIALER_MODE", "sip")
    monkeypatch.setattr(init_app.settings, "AI_CALL_SIP_OUTBOUND_ENABLED", True)

    worker = await init_app._start_ai_call_outbound_task_worker()

    assert worker is not None
    assert worker.executor.dialer.dialer_type == "sip"
    await worker.stop()


@pytest.mark.anyio
async def test_worker_rejects_sip_mode_when_sip_is_disabled(monkeypatch) -> None:
    from app.plugin import init_app

    monkeypatch.setattr(init_app.settings, "SQL_DB_ENABLE", True)
    monkeypatch.setattr(init_app.settings, "AI_CALL_OUTBOUND_EXECUTOR_ENABLED", True)
    monkeypatch.setattr(init_app.settings, "AI_CALL_OUTBOUND_DIALER_MODE", "sip")
    monkeypatch.setattr(init_app.settings, "AI_CALL_SIP_OUTBOUND_ENABLED", False)

    with pytest.raises(RuntimeError, match="SIP 外呼总开关"):
        await init_app._start_ai_call_outbound_task_worker()


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
