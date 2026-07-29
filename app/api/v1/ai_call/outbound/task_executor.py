from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import exists, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.model import AiCallEventModel, AiCallRecordModel
from app.core.logger import log
from app.utils.id_util import generate_snowflake_id

from .rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from .sip_line_model import AiCallSipLineModel
from .sip_line_schema import SipLineSnapshot

TERMINAL_TARGET_STATUSES = {"COMPLETED", "CANCELLED"}
BUSY_END_REASONS = {
    "busy",
    "busy_here",
    "callee_busy",
    "sip_busy",
    "user_busy",
    "sip_486",
}
NO_ANSWER_END_REASONS = {
    "connect_timeout",
    "no_answer",
    "ringing_timeout",
    "sip_connect_timeout",
    "user_unavailable",
    "sip_408",
    "sip_480",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class OutboundDialRequest:
    tenant_id: str
    task_id: int
    target_id: int
    attempt_no: int
    phone_number: str
    customer_name: str | None
    scene_code: str
    voice: str
    prompt_profile_id: str | None
    line: SipLineSnapshot | None = None


@dataclass(frozen=True, slots=True)
class DialResult:
    call_result: str
    error_message: str | None = None
    duration_ms: int = 0
    provider_status_code: str | None = None
    provider_reason: str | None = None
    hangup_cause: str | None = None
    retry_allowed: bool = True


@dataclass(frozen=True, slots=True)
class ClaimedAttempt:
    request: OutboundDialRequest
    call_id: str


@dataclass(frozen=True, slots=True)
class TaskKey:
    tenant_id: str
    task_id: int


ConnectedCallback = Callable[[], Awaitable[None]]


class OutboundDialer(Protocol):
    dialer_type: str
    manages_call_record: bool

    async def dial(
        self,
        request: OutboundDialRequest,
        *,
        call_id: str,
        on_connected: ConnectedCallback,
    ) -> DialResult: ...


class MockOutboundDialer:
    """只产生数据库演练结果，不发起网络或 SIP 请求。"""

    dialer_type = "mock"
    manages_call_record = False

    def __init__(self, call_result: str = "connected") -> None:
        self.call_result = call_result

    async def dial(
        self,
        request: OutboundDialRequest,
        *,
        call_id: str,
        on_connected: ConnectedCallback,
    ) -> DialResult:
        del request, call_id, on_connected
        if self.call_result == "connected":
            return DialResult(call_result="connected")
        return DialResult(
            call_result=self.call_result,
            error_message=f"模拟拨打结果：{self.call_result}",
        )


class OutboundTaskExecutor:
    """按小批次执行已到期通用外呼任务。"""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        dialer: OutboundDialer,
        *,
        task_batch_size: int = 10,
        target_batch_size: int = 20,
        now_provider: Callable[[], datetime] = _utc_now,
        business_timezone: str = "Asia/Shanghai",
        dialing_timeout_seconds: int = 300,
        managed_attempt_timeout_seconds: int = 900,
    ) -> None:
        self.session_factory = session_factory
        self.dialer = dialer
        self.task_batch_size = max(1, task_batch_size)
        self.target_batch_size = max(1, target_batch_size)
        self.now_provider = now_provider
        self.business_timezone = ZoneInfo(business_timezone)
        self.dialing_timeout_seconds = max(1, dialing_timeout_seconds)
        self.managed_attempt_timeout_seconds = max(
            self.dialing_timeout_seconds,
            managed_attempt_timeout_seconds,
        )

    async def run_once(self) -> int:
        now = self.now_provider()
        await self._recover_stale_attempts(now)
        await self._reconcile_transitional_tasks(now)
        await self._claim_due_tasks(now)
        task_keys = await self._list_running_task_keys(now)
        processed = 0
        for task_key in task_keys:
            for _ in range(self.target_batch_size):
                claimed = await self._claim_target(task_key, self.now_provider())
                if claimed is None:
                    await self._refresh_task_counters(task_key, self.now_provider())
                    break
                await self.execute_claimed(claimed)
                processed += 1
        return processed

    async def claim_manual_test(
        self,
        task_key: TaskKey,
        command_idempotency_key: str,
        test_scenario: str,
        active_slot: str,
    ) -> ClaimedAttempt:
        now = self.now_provider()
        async with self.session_factory() as db:
            task = await db.scalar(
                select(AiCallOutboundTaskModel)
                .where(
                    AiCallOutboundTaskModel.tenant_id == task_key.tenant_id,
                    AiCallOutboundTaskModel.id == task_key.task_id,
                )
                .with_for_update()
            )
            if task is None or task.status != "SCHEDULED":
                raise ValueError("人工测试只接受 SCHEDULED 任务")

            targets = (
                await db.scalars(
                    select(AiCallOutboundTargetModel)
                    .where(
                        AiCallOutboundTargetModel.tenant_id == task_key.tenant_id,
                        AiCallOutboundTargetModel.task_id == task_key.task_id,
                    )
                    .order_by(AiCallOutboundTargetModel.id)
                    .limit(2)
                    .with_for_update()
                )
            ).all()
            if (
                task.total_targets != 1
                or len(targets) != 1
                or targets[0].status != "PENDING"
            ):
                raise ValueError("人工测试只接受单个 PENDING 对象的任务")
            target = targets[0]
            attempt_no = target.attempt_count + 1

            task_result = await db.execute(
                update(AiCallOutboundTaskModel)
                .where(
                    AiCallOutboundTaskModel.tenant_id == task_key.tenant_id,
                    AiCallOutboundTaskModel.id == task_key.task_id,
                    AiCallOutboundTaskModel.status == "SCHEDULED",
                )
                .values(
                    status="RUNNING",
                    started_at=now,
                    next_dispatch_at=None,
                    updated_at=now,
                )
            )
            target_result = await db.execute(
                update(AiCallOutboundTargetModel)
                .where(
                    AiCallOutboundTargetModel.tenant_id == task_key.tenant_id,
                    AiCallOutboundTargetModel.task_id == task_key.task_id,
                    AiCallOutboundTargetModel.id == target.id,
                    AiCallOutboundTargetModel.status == "PENDING",
                )
                .values(
                    status="DIALING",
                    attempt_count=AiCallOutboundTargetModel.attempt_count + 1,
                    next_attempt_at=None,
                    updated_at=now,
                )
            )
            if task_result.rowcount != 1 or target_result.rowcount != 1:
                await db.rollback()
                raise ValueError("任务或对象状态已变化，无法认领人工测试")

            request = OutboundDialRequest(
                tenant_id=task.tenant_id,
                task_id=task.id,
                target_id=target.id,
                attempt_no=attempt_no,
                phone_number=target.phone_number,
                customer_name=target.customer_name,
                scene_code=task.scene_code,
                voice=task.voice,
                prompt_profile_id=task.prompt_profile_id,
                line=self._task_line_snapshot(task),
            )
            call_id = uuid4().hex
            db.add(
                AiCallOutboundAttemptModel(
                    id=generate_snowflake_id(),
                    tenant_id=request.tenant_id,
                    task_id=request.task_id,
                    target_id=request.target_id,
                    attempt_no=request.attempt_no,
                    call_id=call_id,
                    dialer_type=self.dialer.dialer_type,
                    test_scenario=test_scenario,
                    command_idempotency_key=command_idempotency_key,
                    active_slot=active_slot,
                    status="DIALING",
                    call_result=None,
                    error_message=None,
                    line_id=(
                        int(request.line.line_id)
                        if request.line is not None
                        else None
                    ),
                    line_code=(
                        request.line.line_code
                        if request.line is not None
                        else None
                    ),
                    provider_status_code=None,
                    provider_reason=None,
                    hangup_cause=None,
                    started_at=now,
                    ended_at=None,
                    created_at=now,
                    updated_at=now,
                )
            )
            if not self.dialer.manages_call_record:
                db.add(self._mock_record(request, call_id, now))
            try:
                await db.commit()
            except Exception:
                await db.rollback()
                raise
            return ClaimedAttempt(request=request, call_id=call_id)

    async def execute_claimed(self, claimed: ClaimedAttempt) -> None:
        try:
            result = await self.dialer.dial(
                claimed.request,
                call_id=claimed.call_id,
                on_connected=lambda: self._mark_in_call(claimed),
            )
        except Exception as exc:
            result = DialResult(
                call_result="call_failed",
                error_message=str(exc) or exc.__class__.__name__,
            )
        await self._finish_attempt(
            claimed.request,
            claimed.call_id,
            result,
            self.now_provider(),
        )

    async def _mark_in_call(self, claimed: ClaimedAttempt) -> None:
        async with self.session_factory() as db:
            attempt = await db.scalar(
                select(AiCallOutboundAttemptModel)
                .where(
                    AiCallOutboundAttemptModel.tenant_id
                    == claimed.request.tenant_id,
                    AiCallOutboundAttemptModel.task_id == claimed.request.task_id,
                    AiCallOutboundAttemptModel.target_id == claimed.request.target_id,
                    AiCallOutboundAttemptModel.call_id == claimed.call_id,
                )
                .with_for_update()
            )
            target = await db.scalar(
                select(AiCallOutboundTargetModel)
                .where(
                    AiCallOutboundTargetModel.tenant_id
                    == claimed.request.tenant_id,
                    AiCallOutboundTargetModel.task_id == claimed.request.task_id,
                    AiCallOutboundTargetModel.id == claimed.request.target_id,
                )
                .with_for_update()
            )
            if attempt is None or target is None:
                return
            if attempt.status == "IN_CALL" and target.status == "IN_CALL":
                return
            if attempt.status != "DIALING" or target.status != "DIALING":
                return
            now = self.now_provider()
            attempt.status = "IN_CALL"
            attempt.updated_at = now
            target.status = "IN_CALL"
            target.updated_at = now
            await db.commit()

    async def _recover_stale_attempts(self, now: datetime) -> None:
        cutoff = now - timedelta(seconds=self.dialing_timeout_seconds)
        managed_cutoff = now - timedelta(
            seconds=self.managed_attempt_timeout_seconds
        )
        async with self.session_factory() as db:
            attempts = (
                await db.scalars(
                    select(AiCallOutboundAttemptModel)
                    .where(
                        AiCallOutboundAttemptModel.started_at <= cutoff,
                        or_(
                            (
                                (AiCallOutboundAttemptModel.status == "DIALING")
                                & (
                                    AiCallOutboundAttemptModel.dialer_type.is_(
                                        None
                                    )
                                    | (
                                        AiCallOutboundAttemptModel.dialer_type
                                        == "mock"
                                    )
                                )
                            ),
                            (
                                AiCallOutboundAttemptModel.status.in_(
                                    {"DIALING", "IN_CALL"}
                                )
                                & (
                                    AiCallOutboundAttemptModel.dialer_type
                                    == "sip"
                                )
                            ),
                        ),
                    )
                    .order_by(AiCallOutboundAttemptModel.started_at)
                    .limit(self.target_batch_size)
                )
            ).all()
            recovery_items: list[
                tuple[OutboundDialRequest, str, DialResult]
            ] = []
            for attempt in attempts:
                task = await db.scalar(
                    select(AiCallOutboundTaskModel).where(
                        AiCallOutboundTaskModel.tenant_id == attempt.tenant_id,
                        AiCallOutboundTaskModel.id == attempt.task_id,
                    )
                )
                target = await db.scalar(
                    select(AiCallOutboundTargetModel).where(
                        AiCallOutboundTargetModel.tenant_id == attempt.tenant_id,
                        AiCallOutboundTargetModel.task_id == attempt.task_id,
                        AiCallOutboundTargetModel.id == attempt.target_id,
                    )
                )
                if (
                    task is None
                    or target is None
                    or target.status != attempt.status
                ):
                    continue
                request = OutboundDialRequest(
                    tenant_id=attempt.tenant_id,
                    task_id=task.id,
                    target_id=target.id,
                    attempt_no=attempt.attempt_no,
                    phone_number=target.phone_number,
                    customer_name=target.customer_name,
                    scene_code=task.scene_code,
                    voice=task.voice,
                    prompt_profile_id=task.prompt_profile_id,
                    line=self._task_line_snapshot(task),
                )
                if attempt.dialer_type in {None, "mock"}:
                    result = DialResult(
                        call_result="call_failed",
                        error_message="执行器中断，拨打结果未知",
                    )
                else:
                    record = await db.scalar(
                        select(AiCallRecordModel).where(
                            AiCallRecordModel.call_id == attempt.call_id
                        )
                    )
                    media_connected = bool(
                        await db.scalar(
                            select(
                                exists().where(
                                    AiCallEventModel.call_id == attempt.call_id,
                                    AiCallEventModel.event_type
                                    == "media_connected",
                                )
                            )
                        )
                    )
                    if (
                        record is not None
                        and str(record.status or "").lower()
                        in {"completed", "failed"}
                    ):
                        result = self._terminal_sip_record_result(
                            record,
                            media_connected=media_connected,
                        )
                    elif record is None:
                        result = DialResult(
                            call_result="call_failed",
                            error_message=(
                                "正式 SIP 执行器中断且未找到通话记录，禁止自动重拨"
                            ),
                            retry_allowed=False,
                        )
                    elif (
                        attempt.status == "DIALING"
                        and record.answered_at is not None
                        and media_connected
                    ):
                        attempt.status = "IN_CALL"
                        attempt.updated_at = now
                        target.status = "IN_CALL"
                        target.updated_at = now
                        continue
                    elif attempt.started_at <= managed_cutoff:
                        result = DialResult(
                            call_result="call_failed",
                            error_message="正式 SIP 通话状态对账超时，禁止自动重拨",
                            retry_allowed=False,
                        )
                    else:
                        continue
                recovery_items.append((request, attempt.call_id, result))
            await db.commit()
        for request, call_id, result in recovery_items:
            await self._finish_attempt(
                request,
                call_id,
                result,
                now,
            )

    async def _reconcile_transitional_tasks(self, now: datetime) -> None:
        async with self.session_factory() as db:
            tasks = (
                await db.scalars(
                    select(AiCallOutboundTaskModel)
                    .where(
                        AiCallOutboundTaskModel.status.in_(["PAUSING", "STOPPING"])
                    )
                    .order_by(
                        AiCallOutboundTaskModel.updated_at,
                        AiCallOutboundTaskModel.id,
                    )
                    .limit(self.task_batch_size * 4)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            for task in tasks:
                if task.status == "STOPPING":
                    await db.execute(
                        update(AiCallOutboundTargetModel)
                        .where(
                            AiCallOutboundTargetModel.tenant_id == task.tenant_id,
                            AiCallOutboundTargetModel.task_id == task.id,
                            AiCallOutboundTargetModel.status.in_(
                                ["PENDING", "RETRY_WAIT"]
                            ),
                        )
                        .values(
                            status="CANCELLED",
                            next_attempt_at=None,
                            updated_at=now,
                        )
                    )
                    await db.flush()
                await self._refresh_task_counters_in_session(db, task, now)
            await db.commit()

    async def _claim_due_tasks(self, now: datetime) -> None:
        schedule_now = self._schedule_comparison_now(now)
        async with self.session_factory() as db:
            candidates = (
                await db.scalars(
                    select(AiCallOutboundTaskModel)
                    .where(
                        AiCallOutboundTaskModel.status == "SCHEDULED",
                        or_(
                            AiCallOutboundTaskModel.next_dispatch_at.is_(None),
                            AiCallOutboundTaskModel.next_dispatch_at <= now,
                        ),
                        or_(
                            AiCallOutboundTaskModel.execution_mode == "immediate",
                            AiCallOutboundTaskModel.scheduled_at <= schedule_now,
                        ),
                    )
                    .order_by(
                        AiCallOutboundTaskModel.next_dispatch_at,
                        AiCallOutboundTaskModel.id,
                    )
                    .limit(self.task_batch_size * 4)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            claimed_count = 0
            for task in candidates:
                if not self._within_call_window(task, now):
                    task.next_dispatch_at = self._next_call_window_at(task, now)
                    task.updated_at = now
                    continue
                task.status = "RUNNING"
                task.started_at = task.started_at or now
                task.next_dispatch_at = None
                task.updated_at = now
                claimed_count += 1
                if claimed_count >= self.task_batch_size:
                    break
            await db.commit()

    def _schedule_comparison_now(self, now: datetime) -> datetime:
        """兼容现有 API 将业务本地时间字符串按 UTC 标签持久化的契约。"""
        aware_now = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
        business_now = aware_now.astimezone(self.business_timezone)
        return business_now.replace(tzinfo=timezone.utc)

    async def _list_running_task_keys(self, now: datetime) -> list[TaskKey]:
        async with self.session_factory() as db:
            tasks = (
                await db.scalars(
                    select(AiCallOutboundTaskModel)
                    .where(
                        AiCallOutboundTaskModel.status == "RUNNING",
                        or_(
                            AiCallOutboundTaskModel.next_dispatch_at.is_(None),
                            AiCallOutboundTaskModel.next_dispatch_at <= now,
                        ),
                    )
                    .order_by(
                        AiCallOutboundTaskModel.last_dispatched_at.asc().nulls_first(),
                        AiCallOutboundTaskModel.id,
                    )
                    .limit(self.task_batch_size * 4)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            task_keys: list[TaskKey] = []
            for task in tasks:
                active_count, claimable_count = await self._target_work_counts(
                    db,
                    task.tenant_id,
                    task.id,
                    now,
                )
                if active_count == 0:
                    task_keys.append(TaskKey(task.tenant_id, task.id))
                elif claimable_count > 0 and self._within_call_window(task, now):
                    task_keys.append(TaskKey(task.tenant_id, task.id))
                elif not self._within_call_window(task, now):
                    task.next_dispatch_at = self._next_call_window_at(task, now)
                else:
                    task.next_dispatch_at = await self._next_target_attempt_at(
                        db,
                        task.tenant_id,
                        task.id,
                    )
                task.last_dispatched_at = now
                task.updated_at = now
                if len(task_keys) >= self.task_batch_size:
                    break
            await db.commit()
            return task_keys

    @staticmethod
    async def _target_work_counts(
        db: AsyncSession,
        tenant_id: str,
        task_id: int,
        now: datetime,
    ) -> tuple[int, int]:
        active_count = int(
            await db.scalar(
                select(func.count(AiCallOutboundTargetModel.id)).where(
                    AiCallOutboundTargetModel.tenant_id == tenant_id,
                    AiCallOutboundTargetModel.task_id == task_id,
                    AiCallOutboundTargetModel.status.in_(
                        ["PENDING", "DIALING", "IN_CALL", "RETRY_WAIT"]
                    ),
                )
            )
            or 0
        )
        claimable_count = int(
            await db.scalar(
                select(func.count(AiCallOutboundTargetModel.id)).where(
                    AiCallOutboundTargetModel.tenant_id == tenant_id,
                    AiCallOutboundTargetModel.task_id == task_id,
                    or_(
                        AiCallOutboundTargetModel.status == "PENDING",
                        (
                            (AiCallOutboundTargetModel.status == "RETRY_WAIT")
                            & (AiCallOutboundTargetModel.next_attempt_at <= now)
                        ),
                    ),
                )
            )
            or 0
        )
        return active_count, claimable_count

    @staticmethod
    async def _next_target_attempt_at(
        db: AsyncSession,
        tenant_id: str,
        task_id: int,
    ) -> datetime | None:
        return await db.scalar(
            select(func.min(AiCallOutboundTargetModel.next_attempt_at)).where(
                AiCallOutboundTargetModel.tenant_id == tenant_id,
                AiCallOutboundTargetModel.task_id == task_id,
                AiCallOutboundTargetModel.status == "RETRY_WAIT",
            )
        )

    async def _claim_target(
        self,
        task_key: TaskKey,
        now: datetime,
    ) -> ClaimedAttempt | None:
        async with self.session_factory() as db:
            task = await db.scalar(
                select(AiCallOutboundTaskModel)
                .where(
                    AiCallOutboundTaskModel.tenant_id == task_key.tenant_id,
                    AiCallOutboundTaskModel.id == task_key.task_id,
                )
                .with_for_update()
            )
            if (
                task is None
                or task.status != "RUNNING"
                or not self._within_call_window(task, now)
            ):
                return None
            line = self._task_line_snapshot(task)
            if self.dialer.dialer_type == "sip" and line is None:
                task.status = "FAILED"
                task.error_message = "任务缺少有效的 SIP 线路快照"
                task.ended_at = now
                task.updated_at = now
                await db.commit()
                return None
            if self.dialer.dialer_type == "sip" and line is not None:
                current_line = await db.scalar(
                    select(AiCallSipLineModel)
                    .where(
                        AiCallSipLineModel.tenant_id == task.tenant_id,
                        AiCallSipLineModel.id == int(line.line_id),
                        AiCallSipLineModel.enabled.is_(True),
                        AiCallSipLineModel.deleted.is_(False),
                    )
                    .with_for_update()
                )
                if current_line is None:
                    task.status = "FAILED"
                    task.error_message = "任务绑定的 SIP 线路已停用或删除"
                    task.ended_at = now
                    task.updated_at = now
                    await db.commit()
                    return None
                active_line_attempts = int(
                    await db.scalar(
                        select(func.count(AiCallOutboundAttemptModel.id)).where(
                            AiCallOutboundAttemptModel.tenant_id == task.tenant_id,
                            AiCallOutboundAttemptModel.line_id == current_line.id,
                            AiCallOutboundAttemptModel.status.in_(
                                {"DIALING", "IN_CALL"}
                            ),
                        )
                    )
                    or 0
                )
                if active_line_attempts >= current_line.max_concurrency:
                    return None
            candidates = (
                await db.scalars(
                    select(AiCallOutboundTargetModel.id)
                    .where(
                        AiCallOutboundTargetModel.tenant_id == task_key.tenant_id,
                        AiCallOutboundTargetModel.task_id == task_key.task_id,
                        or_(
                            AiCallOutboundTargetModel.status == "PENDING",
                            (
                                (AiCallOutboundTargetModel.status == "RETRY_WAIT")
                                & (AiCallOutboundTargetModel.next_attempt_at <= now)
                            ),
                        ),
                    )
                    .order_by(
                        AiCallOutboundTargetModel.source_row_number,
                        AiCallOutboundTargetModel.id,
                    )
                    .limit(self.target_batch_size)
                )
            ).all()
            for target_id in candidates:
                result = await db.execute(
                    update(AiCallOutboundTargetModel)
                    .where(
                        AiCallOutboundTargetModel.id == target_id,
                        AiCallOutboundTargetModel.tenant_id == task_key.tenant_id,
                        AiCallOutboundTargetModel.task_id == task_key.task_id,
                        exists(
                            select(AiCallOutboundTaskModel.id).where(
                                AiCallOutboundTaskModel.tenant_id
                                == task_key.tenant_id,
                                AiCallOutboundTaskModel.id == task_key.task_id,
                                AiCallOutboundTaskModel.status == "RUNNING",
                            )
                        ),
                        or_(
                            AiCallOutboundTargetModel.status == "PENDING",
                            (
                                (AiCallOutboundTargetModel.status == "RETRY_WAIT")
                                & (AiCallOutboundTargetModel.next_attempt_at <= now)
                            ),
                        ),
                    )
                    .values(
                        status="DIALING",
                        attempt_count=AiCallOutboundTargetModel.attempt_count + 1,
                        next_attempt_at=None,
                        updated_at=now,
                    )
                )
                if result.rowcount != 1:
                    continue
                target = await db.scalar(
                    select(AiCallOutboundTargetModel).where(
                        AiCallOutboundTargetModel.tenant_id == task_key.tenant_id,
                        AiCallOutboundTargetModel.task_id == task_key.task_id,
                        AiCallOutboundTargetModel.id == target_id,
                    )
                )
                if target is None:
                    return None
                request = OutboundDialRequest(
                    tenant_id=task.tenant_id,
                    task_id=task.id,
                    target_id=target.id,
                    attempt_no=target.attempt_count,
                    phone_number=target.phone_number,
                    customer_name=target.customer_name,
                    scene_code=task.scene_code,
                    voice=task.voice,
                    prompt_profile_id=task.prompt_profile_id,
                    line=line,
                )
                call_id = uuid4().hex
                db.add(
                    AiCallOutboundAttemptModel(
                        id=generate_snowflake_id(),
                        tenant_id=request.tenant_id,
                        task_id=request.task_id,
                        target_id=request.target_id,
                        attempt_no=request.attempt_no,
                        call_id=call_id,
                        dialer_type=self.dialer.dialer_type,
                        status="DIALING",
                        call_result=None,
                        error_message=None,
                        line_id=(
                            int(request.line.line_id)
                            if request.line is not None
                            else None
                        ),
                        line_code=(
                            request.line.line_code
                            if request.line is not None
                            else None
                        ),
                        provider_status_code=None,
                        provider_reason=None,
                        hangup_cause=None,
                        started_at=now,
                        ended_at=None,
                        created_at=now,
                        updated_at=now,
                    )
                )
                if not self.dialer.manages_call_record:
                    db.add(self._mock_record(request, call_id, now))
                await db.commit()
                return ClaimedAttempt(request=request, call_id=call_id)
            await db.rollback()
            return None

    @staticmethod
    def _mock_record(
        request: OutboundDialRequest,
        call_id: str,
        now: datetime,
    ) -> AiCallRecordModel:
        return AiCallRecordModel(
            id=generate_snowflake_id(),
            call_id=call_id,
            follow_up_id=None,
            business_type="outbound_task",
            business_id=str(request.task_id),
            scene_code=request.scene_code,
            prompt_source_key=request.prompt_profile_id,
            entry_type="outbound_mock",
            room_name=f"mock-outbound-{call_id}",
            participant_identity=f"outbound-target-{request.target_id}",
            callee_phone_number_hash=None,
            callee_phone_number_masked=None,
            status="created",
            end_reason=None,
            failure_stage=None,
            failure_message=None,
            started_at=now,
            answered_at=None,
            ended_at=None,
            duration_ms=None,
        )

    @staticmethod
    def _terminal_sip_record_result(
        record: AiCallRecordModel,
        *,
        media_connected: bool,
    ) -> DialResult:
        duration_ms = max(0, int(record.duration_ms or 0))
        if record.answered_at is not None and media_connected:
            return DialResult(
                call_result="connected",
                duration_ms=duration_ms,
            )
        if record.answered_at is not None:
            return DialResult(
                call_result="call_failed",
                error_message="未检测到媒体接通证据",
                duration_ms=duration_ms,
                retry_allowed=False,
            )
        reason = str(record.end_reason or "").strip().lower()
        error_message = record.failure_message or record.end_reason
        if reason in BUSY_END_REASONS:
            return DialResult(
                call_result="busy",
                error_message=error_message,
                duration_ms=duration_ms,
            )
        if reason in NO_ANSWER_END_REASONS:
            return DialResult(
                call_result="no_answer",
                error_message=error_message,
                duration_ms=duration_ms,
            )
        return DialResult(
            call_result="call_failed",
            error_message=error_message or "SIP 外呼失败",
            duration_ms=duration_ms,
        )

    async def _finish_attempt(
        self,
        request: OutboundDialRequest,
        call_id: str,
        result: DialResult,
        now: datetime,
    ) -> None:
        async with self.session_factory() as db:
            task = await db.scalar(
                select(AiCallOutboundTaskModel)
                .where(
                    AiCallOutboundTaskModel.tenant_id == request.tenant_id,
                    AiCallOutboundTaskModel.id == request.task_id,
                )
                .with_for_update()
            )
            target = await db.scalar(
                select(AiCallOutboundTargetModel)
                .where(
                    AiCallOutboundTargetModel.tenant_id == request.tenant_id,
                    AiCallOutboundTargetModel.task_id == request.task_id,
                    AiCallOutboundTargetModel.id == request.target_id,
                )
                .with_for_update()
            )
            attempt = await db.scalar(
                select(AiCallOutboundAttemptModel)
                .where(
                    AiCallOutboundAttemptModel.tenant_id == request.tenant_id,
                    AiCallOutboundAttemptModel.task_id == request.task_id,
                    AiCallOutboundAttemptModel.target_id == request.target_id,
                    AiCallOutboundAttemptModel.call_id == call_id,
                )
                .with_for_update()
            )
            if task is None or target is None or attempt is None:
                await db.rollback()
                return
            if (
                attempt.status not in {"DIALING", "IN_CALL"}
                or target.status not in {"DIALING", "IN_CALL"}
            ):
                await db.rollback()
                return

            connected = result.call_result == "connected"
            attempt.status = "COMPLETED" if connected else "FAILED"
            attempt.call_result = result.call_result
            attempt.error_message = result.error_message
            attempt.provider_status_code = result.provider_status_code
            attempt.provider_reason = result.provider_reason
            attempt.hangup_cause = result.hangup_cause
            attempt.active_slot = None
            attempt.ended_at = now
            attempt.updated_at = now

            if attempt.dialer_type in {None, "mock"}:
                record = await db.scalar(
                    select(AiCallRecordModel).where(
                        AiCallRecordModel.call_id == call_id
                    )
                )
                if record is not None:
                    record.status = "completed" if connected else "failed"
                    record.end_reason = result.call_result
                    record.failure_stage = None if connected else "outbound_mock"
                    record.failure_message = (
                        None if connected else result.error_message
                    )
                    record.answered_at = now if connected else None
                    record.ended_at = now
                    record.duration_ms = max(0, result.duration_ms)

            target.latest_result = result.call_result
            target.updated_at = now
            task.next_dispatch_at = None
            if connected:
                target.status = "COMPLETED"
                target.next_attempt_at = None
            else:
                retry_interval = (
                    None
                    if (
                        not result.retry_allowed
                        or task.status in {"STOPPING", "STOPPED", "CANCELLED"}
                    )
                    else self._retry_interval(task, request.attempt_no, result.call_result)
                )
                if retry_interval is None:
                    target.status = "COMPLETED"
                    target.next_attempt_at = None
                else:
                    target.status = "RETRY_WAIT"
                    target.next_attempt_at = now + timedelta(minutes=retry_interval)
            await db.flush()
            await self._refresh_task_counters_in_session(db, task, now)
            await db.commit()

    async def _refresh_task_counters(
        self,
        task_key: TaskKey,
        now: datetime,
    ) -> None:
        async with self.session_factory() as db:
            task = await db.scalar(
                select(AiCallOutboundTaskModel)
                .where(
                    AiCallOutboundTaskModel.tenant_id == task_key.tenant_id,
                    AiCallOutboundTaskModel.id == task_key.task_id,
                )
                .with_for_update()
            )
            if task is None:
                return
            await self._refresh_task_counters_in_session(db, task, now)
            await db.commit()

    @staticmethod
    async def _refresh_task_counters_in_session(
        db: AsyncSession,
        task: AiCallOutboundTaskModel,
        now: datetime,
    ) -> None:
        status_rows = (
            await db.execute(
                select(
                    AiCallOutboundTargetModel.status,
                    AiCallOutboundTargetModel.latest_result,
                    func.count(AiCallOutboundTargetModel.id),
                )
                .where(
                    AiCallOutboundTargetModel.tenant_id == task.tenant_id,
                    AiCallOutboundTargetModel.task_id == task.id,
                )
                .group_by(
                    AiCallOutboundTargetModel.status,
                    AiCallOutboundTargetModel.latest_result,
                )
            )
        ).all()
        task.completed_targets = sum(
            int(count)
            for target_status, _, count in status_rows
            if target_status in TERMINAL_TARGET_STATUSES
        )
        task.connected_targets = sum(
            int(count)
            for target_status, latest_result, count in status_rows
            if target_status == "COMPLETED" and latest_result == "connected"
        )
        task.failed_targets = sum(
            int(count)
            for target_status, latest_result, count in status_rows
            if target_status == "COMPLETED" and latest_result != "connected"
        )
        active_count = sum(
            int(count)
            for target_status, _, count in status_rows
            if target_status not in TERMINAL_TARGET_STATUSES
        )
        dialing_count = sum(
            int(count)
            for target_status, _, count in status_rows
            if target_status in {"DIALING", "IN_CALL"}
        )
        if task.status == "PAUSING" and dialing_count == 0:
            if active_count == 0:
                task.status = "COMPLETED"
                task.ended_at = now
            else:
                task.status = "PAUSED"
        elif task.status == "STOPPING" and dialing_count == 0:
            task.status = "STOPPED"
            task.ended_at = now
        elif active_count == 0 and task.status not in {
            "PAUSED",
            "STOPPED",
            "CANCELLED",
            "FAILED",
        }:
            task.status = "COMPLETED"
            task.ended_at = now
        task.updated_at = now

    @staticmethod
    def _retry_interval(
        task: AiCallOutboundTaskModel,
        attempt_no: int,
        call_result: str,
    ) -> int | None:
        try:
            snapshot = json.loads(task.config_snapshot_json)
            rule = snapshot["rule"]
            retry_count = int(rule.get("retryCount", 0))
            retryable_results = set(rule.get("retryableResults", []))
            intervals = list(rule.get("retryIntervalsMinutes", []))
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return None
        if (
            call_result not in retryable_results
            or attempt_no > retry_count
            or attempt_no > len(intervals)
        ):
            return None
        interval = intervals[attempt_no - 1]
        return interval if isinstance(interval, int) and interval > 0 else None

    def _within_call_window(
        self,
        task: AiCallOutboundTaskModel,
        now: datetime,
    ) -> bool:
        try:
            snapshot = json.loads(task.config_snapshot_json)
            windows = snapshot["rule"].get("callWindows")
        except (KeyError, TypeError, json.JSONDecodeError):
            return False
        if windows is None:
            return True
        if not isinstance(windows, list) or not windows:
            return False
        aware_now = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
        current_time = aware_now.astimezone(self.business_timezone).strftime("%H:%M")
        return any(
            isinstance(window, dict)
            and isinstance(window.get("startTime"), str)
            and isinstance(window.get("endTime"), str)
            and window["startTime"] <= current_time < window["endTime"]
            for window in windows
        )

    @staticmethod
    def _task_line_snapshot(
        task: AiCallOutboundTaskModel,
    ) -> SipLineSnapshot | None:
        try:
            snapshot = json.loads(task.config_snapshot_json)
            raw_line = snapshot.get("sipLine")
            if not isinstance(raw_line, dict):
                return None
            line = SipLineSnapshot.model_validate(raw_line)
            if task.line_id is not None and int(line.line_id) != task.line_id:
                return None
            return line
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

    def _next_call_window_at(
        self,
        task: AiCallOutboundTaskModel,
        now: datetime,
    ) -> datetime:
        try:
            snapshot = json.loads(task.config_snapshot_json)
            windows = snapshot["rule"].get("callWindows")
        except (KeyError, TypeError, json.JSONDecodeError):
            return now + timedelta(minutes=5)
        if not isinstance(windows, list) or not windows:
            return now + timedelta(minutes=5)
        aware_now = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
        business_now = aware_now.astimezone(self.business_timezone)
        candidates: list[datetime] = []
        for window in windows:
            if not isinstance(window, dict):
                continue
            start_time = window.get("startTime")
            if not isinstance(start_time, str):
                continue
            try:
                hour, minute = (int(part) for part in start_time.split(":", 1))
                candidate = business_now.replace(
                    hour=hour,
                    minute=minute,
                    second=0,
                    microsecond=0,
                )
            except (TypeError, ValueError):
                continue
            if candidate <= business_now:
                candidate += timedelta(days=1)
            candidates.append(candidate.astimezone(timezone.utc))
        return min(candidates) if candidates else now + timedelta(minutes=5)


class OutboundTaskWorker:
    def __init__(
        self,
        executor: OutboundTaskExecutor,
        *,
        poll_interval_seconds: float = 2.0,
    ) -> None:
        self.executor = executor
        self.poll_interval_seconds = max(0.05, poll_interval_seconds)
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stopping.set()
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def _run(self) -> None:
        while not self._stopping.is_set():
            try:
                await self.executor.run_once()
            except Exception:
                log.exception("AI Call 通用外呼执行器轮询失败")
            try:
                await asyncio.wait_for(
                    self._stopping.wait(),
                    timeout=self.poll_interval_seconds,
                )
            except TimeoutError:
                continue
