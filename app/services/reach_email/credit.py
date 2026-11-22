"""邮件后付费：同一邮件只使用一个准入、完成和退信调整键。"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

from sqlalchemy import select

from app.services.ai_call.credit_metering import (
    CREDIT_ELIGIBILITY_PATH,
    CREDIT_PRODUCT_CODE,
    CreditAdmissionDenied,
    CreditMeteringUnavailable,
    ReachCreditUsageOutboxModel,
    _json_body,
)
from app.services.ai_call.credit_postpaid import (
    ADMISSION_PATH,
    _query,
    _scope,
    _validate_receipt,
)
from app.services.reach_email.models import Message

EMAIL_SCENARIO_CODE = "reach_email_send"
EMAIL_METER_ITEM_CODE = "email_sent"
# 平台需提供按原消费单校验归属的签名入口；启用邮件计费前必须完成联调。
EMAIL_ADJUSTMENT_PATH = "/system/credit/external/v1/charge-orders/adjustments"


def _event_query(message):
    return select(ReachCreditUsageOutboxModel).where(
        ReachCreditUsageOutboxModel.tenant_id == message.tenant_id,
        ReachCreditUsageOutboxModel.owner_id == message.owner_id,
        ReachCreditUsageOutboxModel.scenario_code == EMAIL_SCENARIO_CODE,
        ReachCreditUsageOutboxModel.source_id == message.id,
    )


async def prepare_email_admission(session, message):
    event = await session.scalar(_event_query(message).with_for_update())
    if event is not None:
        if event.status != "admission":
            raise CreditAdmissionDenied("EMAIL_ADMISSION_USED", "该邮件的计费准入已经结束")
        return
    stamp = datetime.now(timezone.utc)
    session.add(ReachCreditUsageOutboxModel(
        tenant_id=message.tenant_id, owner_id=message.owner_id,
        product_code=CREDIT_PRODUCT_CODE, scenario_code=EMAIL_SCENARIO_CODE,
        meter_item_code=EMAIL_METER_ITEM_CODE, quantity=Decimal(0), source_id=message.id,
        idempotency_key=f"reach:email:{message.id}", occurred_at=stamp,
        payload_json=_json_body({"messageId": message.id, "taskId": message.task_id}),
        admission_json=_json_body({"admissionKey": f"reach:email:admission:{message.id}"}),
        status="admission", next_attempt_at=stamp + timedelta(seconds=30),
        created_at=stamp, updated_at=stamp,
    ))


async def require_email_admission(sessions, *, message_id, tenant_id, owner_id, client):
    eligibility = await client.post(CREDIT_ELIGIBILITY_PATH, {
        "tenantId": tenant_id, "ownerType": "USER", "ownerId": owner_id,
        "productCode": CREDIT_PRODUCT_CODE, "scenarioCode": EMAIL_SCENARIO_CODE,
        "meterItemCode": EMAIL_METER_ITEM_CODE, "estimatedPoints": "1",
    })
    if not isinstance(eligibility, dict) or not isinstance(eligibility.get("eligible"), bool):
        raise CreditMeteringUnavailable("邮件计费资格回执无效")
    try:
        minimum = Decimal(str(eligibility["minimumStartPoints"]))
    except (KeyError, InvalidOperation, TypeError) as exc:
        raise CreditMeteringUnavailable("邮件计费最低启动点数无效") from exc
    if eligibility.get("meteringEnabled") is not True or not minimum.is_finite() or minimum < 1:
        raise CreditAdmissionDenied("EMAIL_CREDIT_NOT_ACTIVE", "邮件计费目录或最低启动点数尚未配置为 1 点")
    if not eligibility["eligible"]:
        raise CreditAdmissionDenied(
            str(eligibility.get("reasonCode") or "EMAIL_CREDIT_NOT_ELIGIBLE"),
            str(eligibility.get("reasonMessage") or "当前信用点账户不能发送邮件"),
            eligibility,
        )
    async with sessions.begin() as session:
        message = await session.scalar(select(Message).where(
            Message.id == message_id, Message.tenant_id == tenant_id,
            Message.owner_id == owner_id,
        ))
        if message is None or message.status != "sending":
            raise CreditAdmissionDenied("EMAIL_NOT_SENDING", "邮件不再处于发送状态")
        event = await session.scalar(_event_query(message).with_for_update())
        if event is None or event.status != "admission":
            raise CreditAdmissionDenied("EMAIL_ADMISSION_MISSING", "邮件缺少有效计费准入")
        admission = json.loads(event.admission_json)
        previous = bool(admission.get("requested"))
        admission["requested"] = True
        event.admission_json = _json_body(admission)
        request = {**_scope(event), "sourceId": message.id,
                   "admissionKey": admission["admissionKey"]}

    receipt = admission.get("receipt")
    if receipt is None:
        receipt = await _query(client, request, admission) if previous else None
        if receipt is None:
            receipt = _validate_receipt(await client.post(ADMISSION_PATH, request), request, admission)
    if receipt["status"] != "OPEN" or receipt["meteringEnabled"] is not True:
        raise CreditAdmissionDenied("EMAIL_CREDIT_NOT_ACTIVE", "邮件计费未启用或准入已经结束")
    try:
        pricing = json.loads(receipt["pricingSnapshot"])
        exact_price = (pricing.get("pricingMode") == "FIXED"
            and Decimal(str(pricing["unitQuantity"])) == 1
            and Decimal(str(pricing["unitPoints"])) == 1)
    except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
        raise CreditMeteringUnavailable("邮件计价快照无效") from exc
    if not exact_price:
        raise CreditAdmissionDenied("EMAIL_PRICE_MISMATCH", "邮件固定价格不是每封 1 点")

    async with sessions.begin() as session:
        message = await session.scalar(select(Message).where(
            Message.id == message_id, Message.tenant_id == tenant_id,
            Message.owner_id == owner_id,
        ))
        if message is None or message.status != "sending":
            raise CreditAdmissionDenied("EMAIL_NOT_SENDING", "准入期间邮件发送状态已变化")
        event = await session.scalar(_event_query(message).with_for_update())
        if event is None or event.status != "admission":
            raise CreditAdmissionDenied("EMAIL_ADMISSION_USED", "该邮件的计费准入已经结束")
        admission = json.loads(event.admission_json)
        admission["receipt"] = _validate_receipt(receipt, request, admission)
        event.admission_json = _json_body(admission)
        event.last_error = None


async def record_email_outcome(session, message):
    event = await session.scalar(_event_query(message).with_for_update())
    if event is None:
        return
    _apply_email_outcome(event, message, datetime.now(timezone.utc))


def _apply_email_outcome(event, message, stamp):
    admission = json.loads(event.admission_json)
    if message.status in {"accepted", "failed", "cancelled"} and not admission.get("requested"):
        # 关闭新准入后，未请求过准入的邮件不能在收尾时补建收费单。
        admission["billingState"] = "waived_not_admitted"
        event.status, event.next_attempt_at = "waived", None
    elif message.status == "accepted":
        admission["billingState"] = "chargeable"
        event.quantity = Decimal(1)
        event.occurred_at = (message.sent_at or stamp).replace(tzinfo=timezone.utc)
        event.status, event.next_attempt_at = "pending", stamp
    elif message.status in {"failed", "cancelled"}:
        admission["billingState"] = "waived_not_accepted"
        event.status = "pending" if admission.get("requested") else "waived"
        event.next_attempt_at = stamp if event.status == "pending" else None
    elif message.status == "unknown":
        event.status, event.next_attempt_at = "blocked", None
        event.last_error = "邮件提交结果待核实"
    event.admission_json = _json_body(admission)
    event.updated_at = stamp


async def prepare_email_completion(session, event, stamp):
    admission = json.loads(event.admission_json)
    if admission.get("billingState") in {"chargeable", "waived_not_accepted"}:
        if admission["billingState"] == "waived_not_accepted" and not admission.get("requested"):
            event.status, event.next_attempt_at = "waived", None
            return False
        return True
    message = await session.scalar(select(Message).where(
        Message.id == event.source_id, Message.tenant_id == event.tenant_id,
        Message.owner_id == event.owner_id,
    ))
    if message is not None and message.status in {"accepted", "failed", "cancelled", "unknown"}:
        # 批量停止和重启恢复也按持久化结果收尾，不依赖本轮发送开关。
        _apply_email_outcome(event, message, stamp)
        return event.status == "pending"
    event.status, event.next_attempt_at = "admission", stamp + timedelta(seconds=30)
    return False


async def request_email_refund(session, message):
    event = await session.scalar(_event_query(message).with_for_update())
    if event is None:
        return
    admission = json.loads(event.admission_json)
    if admission.get("refundRequested"):
        return
    admission["refundRequested"] = True
    event.admission_json = _json_body(admission)
    if event.status == "sent":
        event.status = "adjustment"
        event.next_attempt_at = datetime.now(timezone.utc)


def freeze_email_refund(event):
    admission = json.loads(event.admission_json)
    receipt = admission.get("receipt") or {}
    if not admission.get("refundRequested") or not receipt.get("chargeOrderId"):
        raise CreditMeteringUnavailable("退信缺少原消费单，暂不能调整")
    if "refundRequest" not in admission:
        admission["refundRequest"] = {
            **_scope(event), "chargeOrderId": receipt["chargeOrderId"],
            "sourceId": event.source_id, "admissionKey": admission["admissionKey"],
            "targetRefundedPoints": str(receipt["billedPoints"]),
            "idempotencyKey": f"reach:email:refund:{event.source_id}",
            "reason": "邮件最终 5.x 退信", "adjustmentType": "ADJUSTMENT",
        }
        event.admission_json = _json_body(admission)


def _fully_refunded(receipt, charge_order_id):
    try:
        billed = Decimal(str(receipt["billedPoints"]))
        settled = Decimal(str(receipt["settledPoints"]))
        outstanding = Decimal(str(receipt["outstandingPoints"]))
        refunded = Decimal(str(receipt["refundedPoints"]))
    except (KeyError, TypeError, InvalidOperation) as exc:
        raise CreditMeteringUnavailable("退还回执金额无效") from exc
    if receipt.get("chargeOrderId", receipt.get("id")) != charge_order_id or billed <= 0:
        raise CreditMeteringUnavailable("退还回执与原消费单不一致")
    if (any(not value.is_finite() or value < 0 for value in (billed, settled, outstanding, refunded))
            or billed != settled + outstanding + refunded):
        raise CreditMeteringUnavailable("退还回执金额不守恒")
    return refunded == billed and settled == outstanding == 0 and receipt.get("chargeStatus", receipt.get("status")) == "REFUNDED"


async def dispatch_email_refund(client, payload, admission_json):
    admission = json.loads(admission_json)
    request = admission["refundRequest"]
    query = {**{key: payload[key] for key in (
        "tenantId", "ownerType", "ownerId", "productCode", "scenarioCode", "meterItemCode")},
        "sourceId": payload["sourceId"]}
    receipt = await _query(client, query, admission)
    if receipt is None or receipt["status"] != "COMPLETED":
        raise CreditMeteringUnavailable("退信原消费单尚未确认")
    if _fully_refunded(receipt, request["chargeOrderId"]):
        admission["receipt"] = receipt
        return "waived", _json_body(admission)
    result = await client.post(EMAIL_ADJUSTMENT_PATH, request)
    if not isinstance(result, dict) or not _fully_refunded(result, request["chargeOrderId"]):
        raise CreditMeteringUnavailable("平台尚未确认全额退还原消费单")
    admission["receipt"] = receipt
    return "waived", _json_body(admission)
