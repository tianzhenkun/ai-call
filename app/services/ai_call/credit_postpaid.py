"""正式电话的 POSTPAID 准入与完成，复用信用点投递箱保存原请求和回执。"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

from sqlalchemy import select

from app.services.ai_call.credit_metering import (
    CREDIT_METER_ITEM_CODE,
    CREDIT_OWNER_TYPE,
    CREDIT_PRODUCT_CODE,
    CREDIT_SCENARIO_CODE,
    CreditAdmissionDenied,
    CreditMeteringClient,
    CreditMeteringRejected,
    CreditMeteringUnavailable,
    ReachCreditUsageOutboxModel,
    _call_billing_state,
    _decimal_string,
    _json_body,
)
from app.services.ai_call.livekit_sip import MAX_TOTAL_CALL_DURATION_SECONDS

ADMISSION_PATH = "/system/credit/external/v1/usage-admissions"
COMPLETE_PATH = f"{ADMISSION_PATH}/complete"
QUERY_PATH = f"{ADMISSION_PATH}/query"
_SCOPE_KEYS = ("tenantId", "ownerType", "ownerId", "productCode", "scenarioCode", "meterItemCode")


def prepare_call_admission(session, *, tenant_id, owner_id, attempt_id, call_id, now):
    session.add(ReachCreditUsageOutboxModel(
        tenant_id=tenant_id, owner_id=owner_id, product_code=CREDIT_PRODUCT_CODE,
        scenario_code=CREDIT_SCENARIO_CODE, meter_item_code=CREDIT_METER_ITEM_CODE,
        source_id=call_id, idempotency_key=f"reach:call:{attempt_id}", quantity=Decimal(0),
        occurred_at=now, payload_json=_json_body({"attemptId": str(attempt_id), "callId": call_id}),
        admission_json=_json_body({"admissionKey": f"reach:admission:{attempt_id}"}),
        status="admission", next_attempt_at=now + timedelta(seconds=30),
        created_at=now, updated_at=now,
    ))


def _scope(event):
    return {
        "tenantId": event.tenant_id, "ownerType": CREDIT_OWNER_TYPE, "ownerId": event.owner_id,
        "productCode": event.product_code, "scenarioCode": event.scenario_code,
        "meterItemCode": event.meter_item_code,
    }


def _validate_receipt(receipt, request, admission):
    if not isinstance(receipt, dict):
        raise CreditMeteringUnavailable("平台准入回执结构无效")
    expected = {key: request[key] for key in ("productCode", "scenarioCode", "meterItemCode", "sourceId")}
    expected["admissionKey"] = admission["admissionKey"]
    if any(receipt.get(key) != value for key, value in expected.items()):
        raise CreditMeteringUnavailable("平台准入回执与原通话不一致")
    original = admission.get("receipt", {})
    if original and any(receipt.get(key) != original.get(key) for key in (
        "meteringEnabled", "chargingMode", "policyVersion", "pricingVersion", "pricingSnapshot",
    )):
        raise CreditMeteringUnavailable("平台原准入计价快照发生变化")
    admission_id = receipt.get("admissionId") or receipt.get("id")
    original_id = admission.get("receipt", {}).get("admissionId") or admission.get("receipt", {}).get("id")
    if (
        not isinstance(admission_id, str) or not admission_id
        or (original_id and original_id != admission_id)
        or (receipt.get("id") is not None and receipt["id"] != admission_id)
        or receipt.get("chargingMode") != "POSTPAID"
        or not isinstance(receipt.get("meteringEnabled"), bool)
        or receipt.get("status") not in {"OPEN", "COMPLETED", "NO_CHARGE"}
    ):
        raise CreditMeteringUnavailable("平台准入回执身份或状态无效")
    if receipt["meteringEnabled"]:
        try:
            snapshot = json.loads(receipt["pricingSnapshot"])
            if not isinstance(snapshot, dict) or not snapshot or not receipt.get("policyVersion") or not receipt.get("pricingVersion"):
                raise ValueError
        except (KeyError, TypeError, ValueError) as exc:
            raise CreditMeteringUnavailable("平台准入缺少有效计价快照或版本") from exc
    if original.get("status") in {"COMPLETED", "NO_CHARGE"}:
        if receipt["status"] != original["status"] or receipt.get("chargeOrderId") != original.get("chargeOrderId"):
            raise CreditMeteringUnavailable("平台原完成状态或消费单发生变化")
        try:
            if Decimal(str(receipt["billedPoints"])) != Decimal(str(original["billedPoints"])):
                raise ValueError
        except (KeyError, InvalidOperation, ValueError) as exc:
            raise CreditMeteringUnavailable("平台原消费单应收金额发生变化") from exc
    return receipt


async def _query(client, request, admission):
    rows = await client.post(QUERY_PATH, {
        **{key: request[key] for key in _SCOPE_KEYS},
        "admissionKeys": [admission["admissionKey"]],
    })
    if not isinstance(rows, list) or len(rows) > 1:
        raise CreditMeteringUnavailable("平台准入查询未返回有效列表")
    return _validate_receipt(rows[0], request, admission) if rows else None


async def _pause_call_task(session, call_id, reason):
    from app.api.v1.ai_call.outbound.rule_task_model import (
        AiCallOutboundAttemptModel,
        AiCallOutboundExceptionBatchModel,
        AiCallOutboundTargetModel,
        AiCallOutboundTaskModel,
    )

    reason = reason[:1000]
    attempt = await session.scalar(select(AiCallOutboundAttemptModel).where(
        AiCallOutboundAttemptModel.call_id == call_id,
    ))
    if attempt is None:
        return
    task = await session.scalar(select(AiCallOutboundTaskModel).where(
        AiCallOutboundTaskModel.tenant_id == attempt.tenant_id,
        AiCallOutboundTaskModel.id == attempt.task_id,
    ).with_for_update())
    target = await session.scalar(select(AiCallOutboundTargetModel).where(
        AiCallOutboundTargetModel.tenant_id == attempt.tenant_id,
        AiCallOutboundTargetModel.id == attempt.target_id,
    ))
    now = datetime.now(timezone.utc)
    if target is not None and target.exception_batch_id is not None:
        batch = await session.get(AiCallOutboundExceptionBatchModel, target.exception_batch_id, with_for_update=True)
        if batch is not None and batch.status == "RUNNING":
            batch.status, batch.error_message, batch.updated_at = "PAUSED", reason, now
    elif task is not None and task.status == "RUNNING":
        task.status, task.error_message, task.updated_at = "PAUSING", reason, now


async def restore_credit_blocked_target(session, *, task, target, attempt, now):
    """准入失败尚未拨号的号码保留在原队列，用户继续后再创建新的尝试。"""
    if attempt.dialer_type not in {"sip", "owner_runtime"} or task.status in {"STOPPING", "STOPPED", "CANCELLED", "FAILED"}:
        return False
    event = await session.scalar(select(ReachCreditUsageOutboxModel).where(
        ReachCreditUsageOutboxModel.tenant_id == attempt.tenant_id,
        ReachCreditUsageOutboxModel.source_id == attempt.call_id,
        ReachCreditUsageOutboxModel.admission_json.is_not(None),
        ReachCreditUsageOutboxModel.last_error.is_not(None),
    ))
    if event is None or json.loads(event.admission_json).get("dialClaimed"):
        return False
    target.status, target.next_attempt_at, target.updated_at = "RETRY_WAIT", now, now
    return True


async def require_call_admission(session_factory, *, call_id: str, client: CreditMeteringClient):
    """调用方已有持久化 attempt；失败时本次不拨号，并暂停对应任务或补呼批次。"""
    from app.api.v1.ai_call.model import AiCallRecordModel
    from app.api.v1.ai_call.outbound.rule_task_model import AiCallOutboundAttemptModel, AiCallOutboundTaskModel

    previously_requested = False
    try:
        async with session_factory.begin() as session:
            attempt = await session.scalar(select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.call_id == call_id,
            ))
            if attempt is not None and attempt.test_scenario is not None:
                return
            if attempt is None or attempt.ended_at is not None:
                raise CreditAdmissionDenied("ATTEMPT_NOT_ACTIVE", "通话尝试不存在或已经结束")
            event = await session.scalar(select(ReachCreditUsageOutboxModel).where(
                ReachCreditUsageOutboxModel.source_id == call_id,
            ).with_for_update())
            if event is None or event.admission_json is None:
                raise CreditAdmissionDenied("ADMISSION_MISSING", "通话缺少持久化计费准入记录")
            admission = json.loads(event.admission_json)
            if event.status != "admission" or admission.get("dialClaimed") or admission.get("refused"):
                raise CreditAdmissionDenied("ADMISSION_NOT_OPEN", "本次通话准入已使用或结束，禁止重复拨号")
            request = {**_scope(event), "sourceId": call_id, "admissionKey": admission["admissionKey"]}
            previously_requested = admission.get("requested", False)
            admission["requested"] = True
            event.admission_json = _json_body(admission)

        # requested 先提交；崩溃后以原键查询，不能把重复 admit 的 OPEN 当成重新拨号依据。
        receipt = await _query(client, request, admission) if previously_requested else None
        if receipt is None:
            receipt = _validate_receipt(await client.post(ADMISSION_PATH, request), request, admission)
        if receipt["status"] != "OPEN":
            raise CreditAdmissionDenied("ADMISSION_ALREADY_COMPLETED", "平台准入已结束，禁止重复拨号")

        async with session_factory.begin() as session:
            attempt = await session.scalar(select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.call_id == call_id,
            ))
            if attempt is None or attempt.ended_at is not None:
                raise CreditAdmissionDenied("ATTEMPT_NOT_ACTIVE", "准入期间通话已结束")
            task = await session.get(AiCallOutboundTaskModel, attempt.task_id)
            record = await session.scalar(select(AiCallRecordModel).where(AiCallRecordModel.call_id == call_id))
            if (task is not None and task.status in {"STOPPING", "STOPPED", "CANCELLED", "FAILED"}) or (
                record is not None and (record.ended_at is not None or record.terminal_requested_at is not None)
            ):
                raise CreditAdmissionDenied("ATTEMPT_STOPPED", "准入期间通话已请求结束")
            event = await session.scalar(select(ReachCreditUsageOutboxModel).where(
                ReachCreditUsageOutboxModel.source_id == call_id,
            ).with_for_update())
            admission = json.loads(event.admission_json)
            if event.status != "admission" or admission.get("dialClaimed"):
                raise CreditAdmissionDenied("ADMISSION_NOT_OPEN", "本次通话准入已使用或结束")
            admission.update(receipt=receipt, dialClaimed=True)
            event.admission_json = _json_body(admission)
            event.last_error = None
    except (CreditAdmissionDenied, CreditMeteringRejected, CreditMeteringUnavailable) as exc:
        async with session_factory.begin() as session:
            # 与任务认领保持 task -> outbox 锁顺序。
            await _pause_call_task(session, call_id, str(exc))
            event = await session.scalar(select(ReachCreditUsageOutboxModel).where(
                ReachCreditUsageOutboxModel.source_id == call_id,
            ).with_for_update())
            if event is not None:
                event.last_error = str(exc)
                if isinstance(exc, CreditMeteringRejected) and not previously_requested and exc.status_code != 409:
                    admission = json.loads(event.admission_json)
                    admission["refused"] = {"code": exc.status_code, "message": str(exc), "details": exc.details}
                    event.admission_json = _json_body(admission)
        raise


def update_postpaid_billing(event, billing_state, record, now):
    admission = json.loads(event.admission_json)
    admission["billingState"] = billing_state
    if event.status == "waived":
        return
    if event.status == "sent":
        if billing_state.startswith("waived"):
            event.status, event.last_error = "adjustment", "接听依据已变更，原消费待核对调整"
    elif event.status not in {"sending", "adjustment"}:
        event.status = "blocked" if billing_state == "pending_review" and not admission.get("completion") else "pending"
        event.next_attempt_at = None if event.status == "blocked" else now
        event.last_error = "通话收费依据待核实" if event.status == "blocked" else None
    if not admission.get("completion"):
        event.quantity = Decimal(min(max(0, int(record.duration_ms or 0)), MAX_TOTAL_CALL_DURATION_SECONDS * 1000)) / 1000
        event.occurred_at = record.ended_at
    event.admission_json = _json_body(admission)
    event.updated_at = now


async def prepare_postpaid_completion(session, event, now):
    from app.api.v1.ai_call.model import AiCallRecordModel
    from app.api.v1.ai_call.outbound.rule_task_model import AiCallOutboundAttemptModel

    admission = json.loads(event.admission_json)
    attempt = await session.scalar(select(AiCallOutboundAttemptModel).where(
        AiCallOutboundAttemptModel.tenant_id == event.tenant_id,
        AiCallOutboundAttemptModel.call_id == event.source_id,
    ))
    record = await session.scalar(select(AiCallRecordModel).where(
        AiCallRecordModel.tenant_id == event.tenant_id,
        AiCallRecordModel.call_id == event.source_id,
    ))
    if attempt is None or attempt.ended_at is None or (record is not None and record.ended_at is None):
        event.status, event.next_attempt_at = "admission", now + timedelta(seconds=30)
        return False
    if not admission.get("requested") or admission.get("refused"):
        event.status, event.next_attempt_at = "waived", None
        return False
    if record is None:
        # Legacy 在准入失败时尚未创建通话；已准入却丢失通话记录不能推断免收。
        billing_state = "pending_review" if admission.get("dialClaimed") else "waived_not_started"
    elif record.answered_at is None:
        billing_state = "waived_not_connected"
    else:
        billing_state = await _call_billing_state(session, event.source_id, record=record)
        if billing_state == "chargeable" and attempt.call_result != "connected":
            billing_state = "pending_review"
    if record is not None:
        update_postpaid_billing(event, billing_state, record, now)
        admission = json.loads(event.admission_json)
    admission["billingState"] = billing_state
    if billing_state == "pending_review" and not admission.get("completion"):
        event.status, event.next_attempt_at, event.last_error = "blocked", None, "通话收费依据待核实"
        event.admission_json = _json_body(admission)
        return False
    event.admission_json = _json_body(admission)
    return True


def _terminal_status(receipt, admission):
    if receipt["status"] not in {"COMPLETED", "NO_CHARGE"}:
        return None
    try:
        points = {name: Decimal(str(receipt[name])) for name in (
            "billedPoints", "settledPoints", "outstandingPoints", "refundedPoints",
        )}
        if any(not value.is_finite() or value < 0 for value in points.values()):
            raise ValueError
    except (KeyError, InvalidOperation, ValueError) as exc:
        raise CreditMeteringUnavailable("平台完成回执金额无效") from exc
    if receipt["status"] == "NO_CHARGE":
        if any(points.values()) or receipt.get("chargeOrderId"):
            raise CreditMeteringUnavailable("平台免收回执包含消费金额或消费单")
        if admission.get("completion", {}).get("outcome") == "CHARGE" or admission.get("billingState") == "chargeable":
            return "adjustment", "平台已免收，与本地收费事实不一致，请核实原准入"
        return "waived", None
    if points["billedPoints"] > 0 and (
        not isinstance(receipt.get("chargeOrderId"), str) or not receipt["chargeOrderId"]
        or receipt.get("chargeStatus") not in {"SETTLED", "PARTIAL_OUTSTANDING", "OUTSTANDING", "REFUNDED", "PARTIAL_REFUNDED"}
    ):
        raise CreditMeteringUnavailable("平台完成回执缺少有效消费单")
    if points["billedPoints"] != points["settledPoints"] + points["outstandingPoints"] + points["refundedPoints"]:
        raise CreditMeteringUnavailable("平台消费单应收、已结算、待缴与调整金额不一致")
    if admission.get("billingState") != "chargeable":
        if str(admission.get("billingState", "")).startswith("waived") and (
            points["billedPoints"] == 0
            or (receipt.get("chargeStatus") == "REFUNDED" and points["refundedPoints"] == points["billedPoints"])
        ):
            return "waived", None
        return "adjustment", "平台原消费已入账，接听依据变更后需按原单核对调整"
    return "sent", None


async def dispatch_postpaid_completion(client, payload, admission_json):
    admission = json.loads(admission_json)
    query_only = admission.get("adjustmentQueryOnly", False)
    request = {key: payload[key] for key in (*_SCOPE_KEYS, "sourceId")}
    request["admissionKey"] = admission["admissionKey"]
    try:
        receipt = await _query(client, request, admission)
        if receipt is None:
            if query_only or admission.get("receipt"):
                raise CreditMeteringUnavailable("原已确认准入暂未查到，保留原凭证继续核对")
            receipt = _validate_receipt(await client.post(ADMISSION_PATH, request), request, admission)
            # admit 重放可能是旧快照；完成/恢复以 query 为准。
            receipt = await _query(client, request, admission)
            if receipt is None:
                raise CreditMeteringUnavailable("原准入尚未查到，继续核对原请求")
        terminal = _terminal_status(receipt, admission)
        if terminal is not None:
            admission["receipt"] = receipt
            return *terminal, terminal[0] == "adjustment", _json_body(admission)
        if query_only:
            raise CreditMeteringUnavailable("原单调整尚未查到有效完成回执")
        admission["receipt"] = receipt
        completion = admission.get("completion")
        outcome = "CHARGE" if admission["billingState"] == "chargeable" else "NO_CHARGE"
        if completion is not None and (
            completion["outcome"] != outcome or admission["billingState"] == "pending_review"
        ):
            return "pending", "完成请求结果未明且收费依据已变更，持续查询原准入", True, _json_body(admission)
        if completion is None:
            # 首次请求已在持有投递租约的事务内冻结，必须先落库再调用平台。
            return "pending", "平台准入已恢复，等待保存完成请求", True, _json_body(admission)
        receipt = _validate_receipt(await client.post(COMPLETE_PATH, completion), request, admission)
        terminal = _terminal_status(receipt, admission)
        if terminal is None:
            raise CreditMeteringUnavailable("平台完成结果待核实")
        admission["receipt"] = receipt
        return *terminal, terminal[0] == "adjustment", _json_body(admission)
    except CreditMeteringRejected as exc:
        if query_only:
            return "adjustment", str(exc), True, _json_body(admission)
        # 409 不依赖平台中文文案；下一次查询原键，不能生成新消费。
        return "pending" if exc.status_code == 409 else "failed", str(exc), exc.status_code == 409, _json_body(admission)
    except CreditMeteringUnavailable as exc:
        return "adjustment" if query_only else "pending", str(exc), True, _json_body(admission)


async def reconcile_postpaid_adjustments(session, *, client, tenant_id, owner_id):
    """继续任务前核对原单；这里只查询，调整仍由平台有权限的操作人执行。"""
    events = await session.stream_scalars(select(ReachCreditUsageOutboxModel).where(
        ReachCreditUsageOutboxModel.tenant_id == tenant_id,
        ReachCreditUsageOutboxModel.owner_id == owner_id,
        ReachCreditUsageOutboxModel.scenario_code == CREDIT_SCENARIO_CODE,
        ReachCreditUsageOutboxModel.status == "adjustment",
        ReachCreditUsageOutboxModel.admission_json.is_not(None),
    ).order_by(ReachCreditUsageOutboxModel.id).with_for_update().execution_options(yield_per=100))
    async for event in events:
        admission = json.loads(event.admission_json)
        admission["adjustmentQueryOnly"] = True
        status, error, _, admission_json = await dispatch_postpaid_completion(
            client, {**_scope(event), "sourceId": event.source_id}, _json_body(admission),
        )
        event.status, event.admission_json, event.last_error = status, admission_json, error
        event.updated_at = datetime.now(timezone.utc)
        event.next_attempt_at = event.updated_at + timedelta(seconds=30) if status == "adjustment" else None
    await session.flush()


def freeze_postpaid_completion(event):
    admission = json.loads(event.admission_json)
    receipt = admission.get("receipt")
    if receipt and not admission.get("completion"):
        charge = admission["billingState"] == "chargeable"
        admission["completion"] = {
            **_scope(event), "admissionId": receipt.get("admissionId") or receipt["id"],
            "idempotencyKey": event.idempotency_key, "outcome": "CHARGE" if charge else "NO_CHARGE",
            **({"quantity": _decimal_string(event.quantity), "occurredAt": round(
                event.occurred_at.replace(tzinfo=timezone.utc).timestamp() * 1000
                if event.occurred_at.tzinfo is None else event.occurred_at.timestamp() * 1000
            )} if charge else {"reason": admission["billingState"]}),
        }
        event.admission_json = _json_body(admission)
