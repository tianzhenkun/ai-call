"""按原任务关联查询平台账务；不创建准入，不改写投递或消费记录。"""

from __future__ import annotations

from sqlalchemy import func, select

from app.api.v1.ai_call.outbound.rule_task_model import AiCallOutboundAttemptModel
from app.api.v1.ai_call.outbound.rule_task_schema import TaskBillingSummaryOut
from app.services.ai_call.billing_read import (
    MONEY_FIELDS,
    BillingSummary,
    add_billing_entries,
    call_event_join,
)
from app.services.ai_call.credit_metering import ReachCreditUsageOutboxModel


async def query_task_billing(db, *, task, client):
    attempt = AiCallOutboundAttemptModel
    event_model = ReachCreditUsageOutboxModel
    conditions = (
        attempt.tenant_id == task.tenant_id,
        attempt.task_id == task.id,
        attempt.dialer_type.in_(("sip", "owner_runtime")),
        attempt.test_scenario.is_(None),
    )
    # 固定本次查询的通话范围，持续执行中的任务不会使翻页无法结束。
    upper_id = await db.scalar(select(func.max(attempt.id)).where(*conditions))
    result = BillingSummary()
    cursor = 0
    while upper_id and cursor < upper_id:
        rows = (await db.execute(
            select(attempt.id, event_model).outerjoin(event_model, call_event_join(attempt))
            .where(*conditions, attempt.id > cursor, attempt.id <= upper_id)
            .order_by(attempt.id).limit(100)
        )).all()
        if not rows:
            break
        cursor = rows[-1][0]
        await add_billing_entries([(result, event) for _, event in rows], client=client)
    return TaskBillingSummaryOut(
        task_id=str(task.id), call_count=result.item_count,
        confirmed_call_count=result.confirmed_count, pending_call_count=result.pending_count,
        unconfirmed_call_count=result.unconfirmed_count, waived_call_count=result.waived_count,
        adjustment_call_count=result.adjustment_count, queried_at=result.queried_at,
        **{field: getattr(result, field) for field in MONEY_FIELDS},
    )
