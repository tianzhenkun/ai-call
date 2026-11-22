"""按已授权的业务对象读取平台准入回执；不创建或改变账务。"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import Annotated

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_serializer,
)
from pydantic.alias_generators import to_camel
from sqlalchemy import String, and_, cast, func, literal, or_, select, tuple_

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import AiCallRecordModel
from app.api.v1.ai_call.outbound.rule_task_model import AiCallOutboundAttemptModel
from app.core.exceptions import CustomException
from app.services.ai_call.credit_metering import (
    CREDIT_METER_ITEM_CODE,
    CREDIT_PRODUCT_CODE,
    CREDIT_SCENARIO_CODE,
    CreditMeteringClient,
    CreditMeteringUnavailable,
    ReachCreditUsageOutboxModel,
)
from app.services.ai_call.credit_postpaid import (
    QUERY_PATH,
    _scope,
    _terminal_status,
    _validate_receipt,
)
from app.services.reach_email.credit import EMAIL_METER_ITEM_CODE, EMAIL_SCENARIO_CODE
from app.services.reach_email.models import Message, Task

MONEY_FIELDS = ("billed_points", "settled_points", "outstanding_points", "refunded_points")


def _unique_ids(values):
    if len(set(values)) != len(values):
        raise ValueError("查询对象不能重复")
    return values


BillingId = Annotated[str, StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=128)]
BillingIds = Annotated[list[BillingId], Field(min_length=1, max_length=100), AfterValidator(_unique_ids)]


class BillingSchema(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True, extra="forbid")


class CallBillingQuery(BillingSchema):
    call_ids: BillingIds


class EmailTaskBillingQuery(BillingSchema):
    task_ids: BillingIds


class EmailMessageBillingQuery(BillingSchema):
    message_ids: BillingIds


class EmailBillingRecipient(BillingSchema):
    sender_email: str = Field(max_length=254, strict=True)
    recipient_email: str = Field(min_length=1, max_length=254, strict=True)


def _unique_recipients(values):
    _unique_ids([(value.sender_email, value.recipient_email) for value in values])
    return values


class EmailRecipientBillingQuery(BillingSchema):
    task_id: BillingId
    recipients: Annotated[
        list[EmailBillingRecipient], Field(min_length=1, max_length=100), AfterValidator(_unique_recipients),
    ]


class BillingSummary(BillingSchema):
    item_count: int = 0
    confirmed_count: int = 0
    pending_count: int = 0
    unconfirmed_count: int = 0
    waived_count: int = 0
    adjustment_count: int = 0
    billed_points: Decimal = Decimal(0)
    settled_points: Decimal = Decimal(0)
    outstanding_points: Decimal = Decimal(0)
    refunded_points: Decimal = Decimal(0)
    queried_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @field_serializer(*MONEY_FIELDS)
    def serialize_money(self, value: Decimal) -> str:
        return format(value, "f")


def get_billing_client():
    return CreditMeteringClient.from_settings()


def call_event_join(attempt):
    event = ReachCreditUsageOutboxModel
    return and_(
        event.tenant_id == attempt.tenant_id,
        event.source_id == attempt.call_id,
        event.idempotency_key == literal("reach:call:") + cast(attempt.id, String),
        event.product_code == CREDIT_PRODUCT_CODE,
        event.scenario_code == CREDIT_SCENARIO_CODE,
        event.meter_item_code == CREDIT_METER_ITEM_CODE,
    )


async def add_billing_entries(entries, *, client):
    """每条原业务记录只累计一次，按原账户和计量范围查询实时金额。"""
    groups = defaultdict(list)
    for result, event in entries:
        result.item_count += 1
        if event is None or event.admission_json is None:
            result.unconfirmed_count += 1
            continue
        try:
            admission = json.loads(event.admission_json)
        except (ValueError, TypeError) as exc:
            raise CreditMeteringUnavailable("原业务计费凭证无效") from exc
        if (
            not isinstance(admission, dict)
            or not isinstance(admission.get("admissionKey"), str)
            or not admission["admissionKey"].strip()
            or not isinstance(admission.get("receipt", {}), dict)
            or not isinstance(admission.get("completion", {}), dict)
        ):
            raise CreditMeteringUnavailable("原业务计费凭证结构或准入键无效")
        if not admission.get("receipt") and (not admission.get("requested") or admission.get("refused")):
            result.adjustment_count += bool(admission.get("refundRequested"))
            if event.status == "waived":
                result.confirmed_count += 1
                result.waived_count += 1
            else:
                result.pending_count += 1
            continue
        groups[tuple(_scope(event).items())].append((result, event, admission))

    for scope_items, group in groups.items():
        if client is None:
            raise CreditMeteringUnavailable("Reach计费客户端尚未配置")
        keys = [admission["admissionKey"] for _, _, admission in group]
        if len(set(keys)) != len(keys):
            raise CreditMeteringUnavailable("原业务记录的准入键重复")
        for offset in range(0, len(group), 100):
            expected = {entry[2]["admissionKey"]: entry for entry in group[offset:offset + 100]}
            receipts = await client.post(QUERY_PATH, {**dict(scope_items), "admissionKeys": list(expected)})
            if not isinstance(receipts, list):
                raise CreditMeteringUnavailable("平台准入查询未返回有效列表")
            seen = set()
            for receipt in receipts:
                key = receipt.get("admissionKey") if isinstance(receipt, dict) else None
                if not isinstance(key, str) or key not in expected or key in seen:
                    raise CreditMeteringUnavailable("平台返回了重复或不属于本次查询的准入记录")
                seen.add(key)
                result, event, admission = expected[key]
                receipt = _validate_receipt(receipt, {**_scope(event), "sourceId": event.source_id}, admission)
                terminal = _terminal_status(receipt, admission)
                if terminal is None:
                    result.pending_count += 1
                    result.adjustment_count += bool(admission.get("refundRequested"))
                    continue
                result.confirmed_count += 1
                result.waived_count += terminal[0] == "waived"
                refund_pending = admission.get("refundRequested") and (
                    Decimal(str(receipt["refundedPoints"])) < Decimal(str(receipt["billedPoints"]))
                )
                result.adjustment_count += terminal[0] == "adjustment" or bool(refund_pending)
                for field in MONEY_FIELDS:
                    setattr(result, field, getattr(result, field) + Decimal(str(receipt[to_camel(field)])))
            for key in expected.keys() - seen:
                result, _, admission = expected[key]
                result.unconfirmed_count += 1
                result.adjustment_count += bool(admission.get("refundRequested"))


async def query_call_billing(db, *, tenant_id, user_id, call_ids, client):
    record, attempt, event = AiCallRecordModel, AiCallOutboundAttemptModel, ReachCreditUsageOutboxModel
    owned_ids = AiCallRecordRepository._record_filters(
        select(record.id), tenant_id=tenant_id, user_id=user_id,
    ).where(record.call_id.in_(call_ids)).correlate(None)
    assigned_ids = AiCallRecordRepository._assigned_follow_up_record_ids(
        tenant_id=tenant_id, user_id=user_id,
    ).where(record.call_id.in_(call_ids)).correlate(None)
    # 费用查询沿用通话详情的本人或已获派坐席读取权限，两次读取均重新应用该条件。
    readable = record.id.in_(owned_ids.union(assigned_ids))
    owned = set(await db.scalars(select(record.call_id).where(readable, record.call_id.in_(call_ids))))
    if owned != set(call_ids):
        raise CustomException(msg="通话记录不存在或无权访问", status_code=404)
    results = {call_id: BillingSummary() for call_id in call_ids}
    query = AiCallRecordRepository._join_record_outbound_context(
        select(record.call_id, record.entry_type, attempt, event).select_from(record),
    ).outerjoin(event, and_(call_event_join(attempt), event.source_id == record.call_id))
    rows = (await db.execute(query.where(
        readable, record.call_id.in_(call_ids),
        or_(attempt.id.is_(None), attempt.tenant_id == tenant_id),
    ))).all()
    if {row[0] for row in rows} != set(call_ids):
        raise CustomException(msg="通话记录不存在或无权访问", status_code=404)
    # 回拨可能按原外呼任务授权，但不能借用原通话的费用；历史 SIP 缺凭证须待核实。
    entries = [(results[call_id], event_row) for call_id, entry_type, attempt_row, event_row in rows
               if event_row is not None or (attempt_row is None and entry_type == "sip_outbound") or (
                   attempt_row is not None and attempt_row.call_id == call_id
                   and attempt_row.test_scenario is None
                   and (attempt_row.dialer_type in {"sip", "owner_runtime"}
                        or (attempt_row.dialer_type is None and entry_type == "sip_outbound"))
               )]
    await add_billing_entries(entries, client=client)
    return results


def _email_query(service):
    event = ReachCreditUsageOutboxModel
    return service.query(Message).add_columns(event).outerjoin(event, and_(
        event.tenant_id == Message.tenant_id, event.owner_id == Message.owner_id,
        event.product_code == CREDIT_PRODUCT_CODE, event.scenario_code == EMAIL_SCENARIO_CODE,
        event.meter_item_code == EMAIL_METER_ITEM_CODE, event.source_id == Message.id,
        event.idempotency_key == literal("reach:email:") + Message.id,
    ))


def email_outbound_activity_query(service):
    """业务行与费用共用枚举范围；有准入不等于已经尝试发送。"""
    return _email_query(service).where(
        Message.direction == "outbound",
        or_(Message.attempt_count > 0, ReachCreditUsageOutboxModel.id.is_not(None),
            Message.sent_at.is_not(None), Message.status.in_(("accepted", "unknown"))),
    )


async def _add_email_pages(db, query, result_for, *, client):
    ids = query.with_only_columns(Message.id).subquery()
    upper_id = await db.scalar(select(func.max(ids.c.id)))
    cursor = ""
    while upper_id and cursor < upper_id:
        rows = (await db.execute(query.where(Message.id > cursor, Message.id <= upper_id)
                                 .order_by(Message.id).limit(100))).all()
        if not rows:
            break
        cursor = rows[-1][0].id
        await add_billing_entries([(result_for(message), event) for message, event in rows], client=client)


async def query_email_task_billing(service, *, task_ids, client):
    owned = set(await service.db.scalars(service.query(Task).where(Task.id.in_(task_ids)).with_only_columns(Task.id)))
    if owned != set(task_ids):
        raise CustomException(msg="邮件任务不存在或无权访问", status_code=404)
    results = {task_id: BillingSummary() for task_id in task_ids}
    await _add_email_pages(
        service.db, email_outbound_activity_query(service).where(Message.task_id.in_(task_ids)),
        lambda message: results[message.task_id], client=client,
    )
    return results


async def query_email_message_billing(service, *, message_ids, client):
    owned = set(await service.db.scalars(service.query(Message).where(Message.id.in_(message_ids))
                                        .with_only_columns(Message.id)))
    if owned != set(message_ids):
        raise CustomException(msg="邮件不存在或无权访问", status_code=404)
    results = {message_id: BillingSummary() for message_id in message_ids}
    rows = (await service.db.execute(email_outbound_activity_query(service).where(Message.id.in_(message_ids)))).all()
    await add_billing_entries([(results[message.id], event) for message, event in rows], client=client)
    return results


async def query_email_recipient_billing(service, *, task_id, recipients, client):
    if await service.db.scalar(service.query(Task).where(Task.id == task_id).with_only_columns(Task.id)) is None:
        raise CustomException(msg="邮件任务不存在或无权访问", status_code=404)
    pairs = [(recipient.sender_email, recipient.recipient_email) for recipient in recipients]
    query = email_outbound_activity_query(service).where(
        Message.task_id == task_id, tuple_(Message.from_email, Message.to_email).in_(pairs),
    )
    owned = set((await service.db.execute(query.with_only_columns(Message.from_email, Message.to_email).distinct())).all())
    if owned != set(pairs):
        raise CustomException(msg="收发邮箱记录不存在或无权访问", status_code=404)
    results = {pair: BillingSummary() for pair in pairs}
    await _add_email_pages(
        service.db, query, lambda message: results[(message.from_email, message.to_email)], client=client,
    )
    return [{"senderEmail": sender, "recipientEmail": recipient, "billing": results[(sender, recipient)]}
            for sender, recipient in pairs]
