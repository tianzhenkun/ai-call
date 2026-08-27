from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import AiCallRecordModel
from app.services.ai_call.runtime_control.models import AiCallEndEvidenceModel

from .rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundExceptionBatchModel,
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
    "sip_600",
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
REJECTED_END_REASONS = {
    "call_rejected",
    "decline",
    "rejected",
    "sip_603",
}
INVALID_NUMBER_END_REASONS = {
    "address_incomplete",
    "invalid_number",
    "subscriber_absent",
    "unallocated_number",
    "user_not_registered",
    "sip_404",
    "sip_410",
    "sip_484",
    "sip_604",
}


def exception_category_for(call_result: str | None) -> str | None:
    if call_result in {"no_answer", "busy"}:
        return "no_answer"
    if call_result == "rejected":
        return "rejected"
    if call_result == "invalid_number":
        return "invalid_number"
    return None


@dataclass(frozen=True, slots=True)
class AttemptTerminalDecision:
    attempt_status: str
    call_result: str
    error_message: str | None
    provider_status_code: str | None = None
    provider_reason: str | None = None
    hangup_cause: str | None = None


def extract_provider_status_code(*values: str | None) -> str | None:
    for value in values:
        match = re.search(r"(?i)\bsip[_\s:-]?([1-6]\d{2})\b", value or "")
        if match:
            return match.group(1)
    return None


def extract_hangup_cause(value: str | None) -> str | None:
    match = re.search(
        r"(?i)\bhangup[_\s-]?cause\s*[:=]\s*([A-Z0-9_-]+)",
        value or "",
    )
    return match.group(1) if match else None


def terminal_attempt_decision(
    record: AiCallRecordModel | None,
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
    provider_status_code = extract_provider_status_code(
        record.end_reason,
        record.failure_message,
    )
    hangup_cause = extract_hangup_cause(record.failure_message)
    if reason in BUSY_END_REASONS:
        call_result = "busy"
    elif reason in NO_ANSWER_END_REASONS:
        call_result = "no_answer"
    elif reason in REJECTED_END_REASONS:
        call_result = "rejected"
    elif reason in INVALID_NUMBER_END_REASONS:
        call_result = "invalid_number"
    else:
        call_result = "call_failed"
    return AttemptTerminalDecision(
        attempt_status="FAILED",
        call_result=call_result,
        error_message=error_message or "外呼未成功接通",
        provider_status_code=provider_status_code,
        provider_reason=error_message,
        hangup_cause=hangup_cause,
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
    attempt.provider_status_code = decision.provider_status_code
    attempt.provider_reason = decision.provider_reason
    attempt.hangup_cause = decision.hangup_cause
    attempt.active_slot = None
    attempt.ended_at = (record.ended_at if record is not None else None) or now
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


async def enroll_terminal_exception(
    db: AsyncSession,
    *,
    target: AiCallOutboundTargetModel,
    attempt: AiCallOutboundAttemptModel,
    record: AiCallRecordModel | None,
    now: datetime,
) -> str | None:
    if (
        target.status != "COMPLETED"
        or target.next_attempt_at is not None
        or target.exception_category is not None
    ):
        return None
    active_attempt_count = int(
        await db.scalar(
            select(func.count(AiCallOutboundAttemptModel.id)).where(
                AiCallOutboundAttemptModel.tenant_id == target.tenant_id,
                AiCallOutboundAttemptModel.target_id == target.id,
                AiCallOutboundAttemptModel.status.in_({"DIALING", "IN_CALL"}),
            )
        )
        or 0
    )
    if active_attempt_count:
        return None
    category = exception_category_for(attempt.call_result)
    if category is None and await _is_customer_early_hangup(
        db,
        tenant_id=target.tenant_id,
        call_id=attempt.call_id,
        record=record,
    ):
        category = "early_hangup"
    if category is None:
        return None
    target.exception_category = category
    target.exception_source_result = (
        "early_hangup" if category == "early_hangup" else attempt.call_result
    )
    target.exception_original_attempt_count = target.attempt_count
    target.exception_batch_id = None
    target.exception_entered_at = now
    if category == "early_hangup":
        target.latest_result = "early_hangup"
    target.updated_at = now
    return category


async def apply_exception_terminal_projection(
    db: AsyncSession,
    *,
    task: AiCallOutboundTaskModel,
    target: AiCallOutboundTargetModel,
    attempt: AiCallOutboundAttemptModel,
    record: AiCallRecordModel | None,
    decision: AttemptTerminalDecision,
    now: datetime,
    retry_allowed: bool = True,
) -> None:
    attempt.status = decision.attempt_status
    attempt.call_result = decision.call_result
    attempt.error_message = decision.error_message
    attempt.provider_status_code = decision.provider_status_code
    attempt.provider_reason = decision.provider_reason
    attempt.hangup_cause = decision.hangup_cause
    attempt.active_slot = None
    attempt.ended_at = (record.ended_at if record is not None else None) or now
    attempt.updated_at = now

    category = exception_category_for(decision.call_result)
    if category is None and await _is_customer_early_hangup(
        db,
        tenant_id=target.tenant_id,
        call_id=attempt.call_id,
        record=record,
    ):
        category = "early_hangup"
    target.latest_result = (
        "early_hangup" if category == "early_hangup" else decision.call_result
    )
    target.updated_at = now
    target.next_attempt_at = None

    batch = await db.scalar(
        select(AiCallOutboundExceptionBatchModel)
        .where(
            AiCallOutboundExceptionBatchModel.tenant_id == target.tenant_id,
            AiCallOutboundExceptionBatchModel.id == target.exception_batch_id,
        )
        .with_for_update()
    )
    original_count = target.exception_original_attempt_count or target.attempt_count
    retry_count = max(0, target.attempt_count - original_count)
    if task.status in {"STOPPING", "STOPPED", "CANCELLED"}:
        target.status = "CANCELLED"
    elif (
        not retry_allowed
        or category is None
        or category == "invalid_number"
        or batch is None
    ):
        target.status = "COMPLETED"
    elif retry_count >= batch.max_retry_count:
        target.status = "COMPLETED"
    else:
        target.status = "RETRY_WAIT"
        target.next_attempt_at = now + timedelta(days=batch.interval_days)
    await db.flush()
    if batch is not None:
        await complete_exception_batch_if_done(db, batch, now)


async def complete_exception_batch_if_done(
    db: AsyncSession,
    batch: AiCallOutboundExceptionBatchModel,
    now: datetime,
) -> bool:
    active_count = int(
        await db.scalar(
            select(func.count(AiCallOutboundTargetModel.id)).where(
                AiCallOutboundTargetModel.tenant_id == batch.tenant_id,
                AiCallOutboundTargetModel.exception_batch_id == batch.id,
                AiCallOutboundTargetModel.status.not_in(TERMINAL_TARGET_STATUSES),
            )
        )
        or 0
    )
    if active_count:
        return False
    batch.status = "COMPLETED"
    batch.active_slot = None
    batch.ended_at = now
    batch.updated_at = now
    return True


async def _is_customer_early_hangup(
    db: AsyncSession,
    *,
    tenant_id: str,
    call_id: str,
    record: AiCallRecordModel | None,
) -> bool:
    if (
        record is None
        or record.entry_type != "direct_sip"
        or record.answered_at is None
        or record.duration_ms is None
        or record.duration_ms > 5_000
    ):
        return False
    evidence = await db.scalar(
        select(AiCallEndEvidenceModel)
        .where(
            AiCallEndEvidenceModel.tenant_id == tenant_id,
            AiCallEndEvidenceModel.call_id == call_id,
        )
        .order_by(
            func.coalesce(
                AiCallEndEvidenceModel.event_at,
                AiCallEndEvidenceModel.received_at,
            ),
            AiCallEndEvidenceModel.id,
        )
        .limit(1)
    )
    if (
        evidence is None
        or evidence.source != "livekit_webhook"
        or evidence.end_reason != "sip_participant_left"
        or not evidence.evidence_json
    ):
        return False
    try:
        payload = json.loads(evidence.evidence_json)
    except (TypeError, json.JSONDecodeError):
        return False
    participant = payload.get("participant")
    participant_data = participant if isinstance(participant, dict) else {}
    disconnect_reason = payload.get("disconnectReason") or participant_data.get(
        "disconnectReason"
    )
    return str(disconnect_reason or "").upper() == "CLIENT_INITIATED"


def outbound_retry_interval(
    task: AiCallOutboundTaskModel,
    attempt_no: int,
    call_result: str,
) -> int | None:
    if call_result == "call_failed":
        return None
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
                AiCallOutboundTargetModel.exception_category,
                AiCallOutboundTargetModel.exception_source_result,
                func.count(AiCallOutboundTargetModel.id),
            )
            .where(
                AiCallOutboundTargetModel.tenant_id == task.tenant_id,
                AiCallOutboundTargetModel.task_id == task.id,
            )
            .group_by(
                AiCallOutboundTargetModel.status,
                AiCallOutboundTargetModel.latest_result,
                AiCallOutboundTargetModel.exception_category,
                AiCallOutboundTargetModel.exception_source_result,
            )
        )
    ).all()
    task.completed_targets = sum(
        int(count)
        for target_status, _, exception_category, _, count in status_rows
        if exception_category is not None or target_status in TERMINAL_TARGET_STATUSES
    )
    task.connected_targets = sum(
        int(count)
        for target_status, latest_result, exception_category, source_result, count in status_rows
        if (exception_category is not None or target_status == "COMPLETED")
        and (source_result if exception_category else latest_result)
        in {"connected", "early_hangup"}
    )
    task.failed_targets = sum(
        int(count)
        for target_status, latest_result, exception_category, source_result, count in status_rows
        if (exception_category is not None or target_status == "COMPLETED")
        and (source_result if exception_category else latest_result)
        not in {"connected", "early_hangup"}
    )
    active_count = sum(
        int(count)
        for target_status, _, exception_category, _, count in status_rows
        if exception_category is None and target_status not in TERMINAL_TARGET_STATUSES
    )
    dialing_count = sum(
        int(count)
        for target_status, _, exception_category, _, count in status_rows
        if exception_category is None and target_status in {"DIALING", "IN_CALL"}
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
