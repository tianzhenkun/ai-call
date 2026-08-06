from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import AiCallRecordModel

from .rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)

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
    "browser_disconnect",
    "connect_timeout",
    "no_answer",
    "ringing_timeout",
    "sip_connect_timeout",
    "user_unavailable",
    "sip_408",
    "sip_480",
}


@dataclass(frozen=True, slots=True)
class AttemptTerminalDecision:
    attempt_status: str
    call_result: str
    error_message: str | None


def terminal_attempt_decision(
    record: AiCallRecordModel,
    *,
    media_connected: bool,
) -> AttemptTerminalDecision | None:
    if record.status not in {"completed", "failed"} or record.ended_at is None:
        return None
    if record.status == "completed" and record.answered_at is not None and media_connected:
        return AttemptTerminalDecision(
            attempt_status="COMPLETED",
            call_result="connected",
            error_message=None,
        )
    reason = str(record.end_reason or "").strip().lower()
    error_message = record.failure_message or record.end_reason
    if reason in BUSY_END_REASONS:
        call_result = "busy"
    elif reason in NO_ANSWER_END_REASONS:
        call_result = "no_answer"
    else:
        call_result = "call_failed"
    return AttemptTerminalDecision(
        attempt_status="FAILED",
        call_result=call_result,
        error_message=error_message or "外呼未成功接通",
    )


def apply_terminal_projection(
    *,
    task: AiCallOutboundTaskModel,
    target: AiCallOutboundTargetModel,
    attempt: AiCallOutboundAttemptModel,
    record: AiCallRecordModel,
    decision: AttemptTerminalDecision,
    now: datetime,
) -> None:
    attempt.status = decision.attempt_status
    attempt.call_result = decision.call_result
    attempt.error_message = decision.error_message
    attempt.active_slot = None
    attempt.ended_at = record.ended_at or now
    attempt.updated_at = now

    target.latest_result = decision.call_result
    target.updated_at = now
    task.next_dispatch_at = None
    if decision.call_result == "connected":
        target.status = "COMPLETED"
        target.next_attempt_at = None
        return

    retry_interval = (
        None
        if task.status in {"STOPPING", "STOPPED", "CANCELLED"}
        else outbound_retry_interval(task, attempt.attempt_no, decision.call_result)
    )
    if retry_interval is None:
        target.status = "COMPLETED"
        target.next_attempt_at = None
    else:
        target.status = "RETRY_WAIT"
        target.next_attempt_at = now + timedelta(minutes=retry_interval)


def outbound_retry_interval(
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


async def refresh_task_counters(
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
