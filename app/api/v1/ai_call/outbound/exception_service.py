from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile

from fastapi import status
from openpyxl import Workbook
from sqlalchemy import case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import CustomException
from app.services.ai_call.sqlite_serialization import begin_sqlite_immediate_write
from app.utils.id_util import generate_snowflake_id

from .exception_schema import (
    ExceptionActiveBatchOut,
    ExceptionBatchOut,
    ExceptionPolicyIn,
    ExceptionPolicyOut,
    ExceptionSummaryCardOut,
    ExceptionSummaryOut,
    ExceptionTargetOut,
)
from .rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundExceptionBatchModel,
    AiCallOutboundExceptionPolicyModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)

EXCEPTION_DEFAULTS = {
    "no_answer": (30, 3),
    "rejected": (120, 2),
    "early_hangup": (15, 2),
}
EXCEPTION_CATEGORIES = (*EXCEPTION_DEFAULTS, "invalid_number")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _format_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _mask_phone_number(phone_number: str | None) -> str | None:
    if phone_number is None:
        return None
    digits = "".join(character for character in phone_number if character.isdigit())
    return "***" if len(digits) <= 7 else f"{digits[:3]}****{digits[-4:]}"


class OutboundExceptionService:
    async def get_summary(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: int,
    ) -> ExceptionSummaryOut:
        policies = await self._ensure_policies(db, tenant_id, user_id)
        display_status = self._display_status_expression()
        rows = (
            await db.execute(
                select(
                    AiCallOutboundTargetModel.exception_category,
                    display_status,
                    func.count(AiCallOutboundTargetModel.id),
                )
                .outerjoin(
                    AiCallOutboundExceptionBatchModel,
                    (AiCallOutboundExceptionBatchModel.tenant_id == tenant_id)
                    & (
                        AiCallOutboundExceptionBatchModel.id
                        == AiCallOutboundTargetModel.exception_batch_id
                    ),
                )
                .where(
                    AiCallOutboundTargetModel.tenant_id == tenant_id,
                    AiCallOutboundTargetModel.exception_category.is_not(None),
                )
                .group_by(AiCallOutboundTargetModel.exception_category, display_status)
            )
        ).all()
        counts: dict[str, dict[str, int]] = {}
        for category, target_status, count in rows:
            counts.setdefault(str(category), {})[str(target_status)] = int(count)
        active_batch_rows = (
            await db.scalars(
                select(AiCallOutboundExceptionBatchModel).where(
                    AiCallOutboundExceptionBatchModel.tenant_id == tenant_id,
                    AiCallOutboundExceptionBatchModel.status == "RUNNING",
                )
            )
        ).all()
        completed_rows = (
            await db.execute(
                select(
                    AiCallOutboundTargetModel.exception_batch_id,
                    func.count(AiCallOutboundTargetModel.id),
                )
                .where(
                    AiCallOutboundTargetModel.tenant_id == tenant_id,
                    AiCallOutboundTargetModel.exception_batch_id.in_(
                        [batch.id for batch in active_batch_rows]
                    ),
                    AiCallOutboundTargetModel.status.in_({"COMPLETED", "CANCELLED"}),
                )
                .group_by(AiCallOutboundTargetModel.exception_batch_id)
            )
        ).all() if active_batch_rows else []
        completed_by_batch = {
            int(batch_id): int(count) for batch_id, count in completed_rows
        }
        active_batches = {
            batch.category: ExceptionActiveBatchOut(
                batch_id=str(batch.id),
                target_count=batch.target_count,
                completed_count=completed_by_batch.get(batch.id, 0),
                created_by=str(batch.created_by),
                created_by_name=batch.created_by_name,
                started_at=_format_datetime(batch.started_at) or "",
            )
            for batch in active_batch_rows
        }
        cards = []
        for category in EXCEPTION_CATEGORIES:
            category_counts = counts.get(category, {})
            policy = policies.get(category)
            active_batch = active_batches.get(category)
            retryable = category != "invalid_number"
            pending_count = category_counts.get("PENDING", 0)
            cards.append(
                ExceptionSummaryCardOut(
                    category=category,
                    total_count=sum(category_counts.values()),
                    pending_count=pending_count,
                    maxed_out_count=category_counts.get("MAXED", 0),
                    policy=(
                        self._policy_out(policy)
                        if policy is not None and retryable
                        else None
                    ),
                    active_batch=active_batch,
                    can_start=retryable and active_batch is None and pending_count > 0,
                    disabled_reason=(
                        "空号停机不可重新外呼"
                        if not retryable
                        else "本批重新外呼尚未完成"
                        if active_batch is not None
                        else "当前没有待重呼号码"
                        if pending_count == 0
                        else None
                    ),
                )
            )
        return ExceptionSummaryOut(cards=cards)

    async def update_policy(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: int,
        category: str,
        request: ExceptionPolicyIn,
    ) -> ExceptionPolicyOut:
        self._require_retryable_category(category)
        await begin_sqlite_immediate_write(db)
        policies = await self._ensure_policies(db, tenant_id, user_id)
        policy = policies[category]
        policy.interval_days = request.interval_days
        policy.max_retry_count = request.max_retry_count
        policy.updated_by = user_id
        policy.updated_at = _now()
        await db.flush()
        return self._policy_out(policy)

    async def start_batch(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: int,
        user_name: str | None,
        category: str,
        idempotency_key: str,
    ) -> ExceptionBatchOut:
        self._require_retryable_category(category)
        key = idempotency_key.strip()
        if not key or len(key) > 128:
            raise CustomException(msg="Idempotency-Key 不合法", status_code=400)
        await begin_sqlite_immediate_write(db)
        fingerprint = hashlib.sha256(category.encode()).hexdigest()
        existing = await db.scalar(
            select(AiCallOutboundExceptionBatchModel).where(
                AiCallOutboundExceptionBatchModel.tenant_id == tenant_id,
                AiCallOutboundExceptionBatchModel.idempotency_key == key,
            )
        )
        if existing is not None:
            if existing.request_fingerprint != fingerprint:
                raise CustomException(
                    msg="Idempotency-Key 已用于其他请求",
                    status_code=status.HTTP_409_CONFLICT,
                )
            return self._batch_out(existing)

        await self._ensure_policies(db, tenant_id, user_id)
        policy = await db.scalar(
            select(AiCallOutboundExceptionPolicyModel)
            .where(
                AiCallOutboundExceptionPolicyModel.tenant_id == tenant_id,
                AiCallOutboundExceptionPolicyModel.category == category,
            )
            .with_for_update()
        )
        if policy is None:
            raise CustomException(msg="异常外呼规则不存在", status_code=404)

        existing = await db.scalar(
            select(AiCallOutboundExceptionBatchModel).where(
                AiCallOutboundExceptionBatchModel.tenant_id == tenant_id,
                AiCallOutboundExceptionBatchModel.idempotency_key == key,
            )
        )
        if existing is not None:
            if existing.request_fingerprint != fingerprint:
                raise CustomException(
                    msg="Idempotency-Key 已用于其他请求",
                    status_code=status.HTTP_409_CONFLICT,
                )
            return self._batch_out(existing)

        active = await db.scalar(
            select(AiCallOutboundExceptionBatchModel).where(
                AiCallOutboundExceptionBatchModel.tenant_id == tenant_id,
                AiCallOutboundExceptionBatchModel.active_slot == category,
            )
        )
        if active is not None:
            raise CustomException(
                msg="该异常类别已有未完成的重新外呼批次",
                status_code=status.HTTP_409_CONFLICT,
                data={"activeBatchId": str(active.id)},
            )

        cutoff_at = _now()
        targets = (
            await db.scalars(
                select(AiCallOutboundTargetModel)
                .where(
                    AiCallOutboundTargetModel.tenant_id == tenant_id,
                    AiCallOutboundTargetModel.exception_category == category,
                    AiCallOutboundTargetModel.exception_batch_id.is_(None),
                    AiCallOutboundTargetModel.status == "COMPLETED",
                    AiCallOutboundTargetModel.exception_entered_at <= cutoff_at,
                )
                .order_by(
                    AiCallOutboundTargetModel.exception_entered_at,
                    AiCallOutboundTargetModel.id,
                )
                .with_for_update(skip_locked=True)
            )
        ).all()
        if not targets:
            raise CustomException(
                msg="当前没有待重新外呼的号码",
                status_code=status.HTTP_409_CONFLICT,
            )

        batch = AiCallOutboundExceptionBatchModel(
            id=generate_snowflake_id(),
            tenant_id=tenant_id,
            category=category,
            status="RUNNING",
            interval_days=policy.interval_days,
            max_retry_count=policy.max_retry_count,
            cutoff_at=cutoff_at,
            target_count=len(targets),
            idempotency_key=key,
            request_fingerprint=fingerprint,
            active_slot=category,
            created_by=user_id,
            created_by_name=user_name,
            started_at=cutoff_at,
            ended_at=None,
            created_at=cutoff_at,
            updated_at=cutoff_at,
        )
        db.add(batch)
        await db.flush()
        last_attempt_rows = (
            await db.execute(
                select(
                    AiCallOutboundAttemptModel.target_id,
                    func.max(AiCallOutboundAttemptModel.ended_at),
                )
                .where(
                    AiCallOutboundAttemptModel.tenant_id == tenant_id,
                    AiCallOutboundAttemptModel.target_id.in_([item.id for item in targets]),
                )
                .group_by(AiCallOutboundAttemptModel.target_id)
            )
        ).all()
        last_attempt_at = dict(last_attempt_rows)
        for target in targets:
            target.exception_batch_id = batch.id
            target.status = "RETRY_WAIT"
            target.next_attempt_at = (
                last_attempt_at.get(target.id) or cutoff_at
            ) + timedelta(days=batch.interval_days)
            target.updated_at = cutoff_at
        await db.flush()
        return self._batch_out(batch)

    async def list_targets(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: int,
        *,
        category: str,
        target_status: str | None,
        keyword: str | None,
        page_num: int,
        page_size: int,
    ) -> tuple[list[ExceptionTargetOut], int]:
        self._require_category(category)
        policies = await self._ensure_policies(db, tenant_id, user_id)
        display_status = self._display_status_expression()
        conditions = [
            AiCallOutboundTargetModel.tenant_id == tenant_id,
            AiCallOutboundTargetModel.exception_category == category,
        ]
        if target_status:
            conditions.append(display_status == target_status)
        if keyword and keyword.strip():
            value = keyword.strip()
            conditions.append(
                or_(
                    AiCallOutboundTargetModel.customer_name.contains(value),
                    AiCallOutboundTargetModel.phone_number.contains(value),
                    AiCallOutboundTaskModel.task_name.contains(value),
                )
            )
        join_condition = (
            (AiCallOutboundExceptionBatchModel.tenant_id == tenant_id)
            & (
                AiCallOutboundExceptionBatchModel.id
                == AiCallOutboundTargetModel.exception_batch_id
            )
        )
        total = int(
            await db.scalar(
                select(func.count(AiCallOutboundTargetModel.id))
                .join(
                    AiCallOutboundTaskModel,
                    (AiCallOutboundTaskModel.tenant_id == tenant_id)
                    & (AiCallOutboundTaskModel.id == AiCallOutboundTargetModel.task_id),
                )
                .outerjoin(AiCallOutboundExceptionBatchModel, join_condition)
                .where(*conditions)
            )
            or 0
        )
        rows = (
            await db.execute(
                select(
                    AiCallOutboundTargetModel,
                    AiCallOutboundTaskModel,
                    AiCallOutboundExceptionBatchModel,
                    display_status,
                )
                .join(
                    AiCallOutboundTaskModel,
                    (AiCallOutboundTaskModel.tenant_id == tenant_id)
                    & (AiCallOutboundTaskModel.id == AiCallOutboundTargetModel.task_id),
                )
                .outerjoin(AiCallOutboundExceptionBatchModel, join_condition)
                .where(*conditions)
                .order_by(
                    AiCallOutboundTargetModel.exception_entered_at.desc(),
                    AiCallOutboundTargetModel.id.desc(),
                )
                .offset((page_num - 1) * page_size)
                .limit(page_size)
            )
        ).all()
        target_ids = [target.id for target, _, _, _ in rows]
        attempts = (
            await db.scalars(
                select(AiCallOutboundAttemptModel)
                .where(
                    AiCallOutboundAttemptModel.tenant_id == tenant_id,
                    AiCallOutboundAttemptModel.target_id.in_(target_ids),
                )
                .order_by(
                    AiCallOutboundAttemptModel.target_id,
                    AiCallOutboundAttemptModel.attempt_no.desc(),
                )
            )
        ).all() if target_ids else []
        latest_attempts: dict[int, AiCallOutboundAttemptModel] = {}
        for attempt in attempts:
            latest_attempts.setdefault(attempt.target_id, attempt)

        result = []
        for target, task, batch, row_status in rows:
            original_count = target.exception_original_attempt_count or target.attempt_count
            retry_count = max(0, target.attempt_count - original_count)
            max_retry_count = (
                batch.max_retry_count
                if batch is not None
                else (policies[category].max_retry_count if category in policies else 0)
            )
            attempt = latest_attempts.get(target.id)
            result.append(
                ExceptionTargetOut(
                    target_id=str(target.id),
                    customer_name=target.customer_name,
                    phone_number=_mask_phone_number(target.phone_number),
                    task_id=str(task.id),
                    task_name=task.task_name,
                    category=category,
                    source_result=target.exception_source_result or target.latest_result or category,
                    original_attempt_count=original_count,
                    retry_count=retry_count,
                    max_retry_count=max_retry_count,
                    status=row_status,
                    next_attempt_at=_format_datetime(target.next_attempt_at),
                    last_attempt_at=_format_datetime(attempt.ended_at if attempt else None),
                    last_result=target.latest_result,
                    call_id=attempt.call_id if attempt else None,
                )
            )
        return result, total

    async def export_targets(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: int,
        category: str,
    ) -> Path:
        rows, _ = await self.list_targets(
            db,
            tenant_id,
            user_id,
            category=category,
            target_status=None,
            keyword=None,
            page_num=1,
            page_size=1_000_000,
        )
        workbook = Workbook(write_only=True)
        sheet = workbook.create_sheet("异常号码")
        sheet.append([
            "客户名称",
            "号码",
            "所属任务",
            "最终异常结果",
            "原任务外呼次数",
            "异常重呼进度",
            "处理状态",
            "下次执行时间",
            "最后外呼时间",
            "最后结果",
        ])
        for row in rows:
            sheet.append([
                row.customer_name,
                row.phone_number,
                row.task_name,
                row.source_result,
                row.original_attempt_count,
                f"{row.retry_count}/{row.max_retry_count}",
                row.status,
                row.next_attempt_at,
                row.last_attempt_at,
                row.last_result,
            ])
        temp = NamedTemporaryFile(prefix=f"exception-{category}-", suffix=".xlsx", delete=False)
        temp.close()
        workbook.save(temp.name)
        return Path(temp.name)

    async def _ensure_policies(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: int,
    ) -> dict[str, AiCallOutboundExceptionPolicyModel]:
        policies = {
            item.category: item
            for item in (
                await db.scalars(
                    select(AiCallOutboundExceptionPolicyModel).where(
                        AiCallOutboundExceptionPolicyModel.tenant_id == tenant_id
                    )
                )
            ).all()
        }
        now = _now()
        for category, (interval_days, max_retry_count) in EXCEPTION_DEFAULTS.items():
            if category not in policies:
                policy = AiCallOutboundExceptionPolicyModel(
                    id=generate_snowflake_id(),
                    tenant_id=tenant_id,
                    category=category,
                    interval_days=interval_days,
                    max_retry_count=max_retry_count,
                    created_by=user_id,
                    updated_by=user_id,
                    created_at=now,
                    updated_at=now,
                )
                db.add(policy)
                policies[category] = policy
        await db.flush()
        return policies

    @staticmethod
    def _display_status_expression():
        retry_count = (
            AiCallOutboundTargetModel.attempt_count
            - AiCallOutboundTargetModel.exception_original_attempt_count
        )
        return case(
            (
                AiCallOutboundTargetModel.exception_category == "invalid_number",
                "UNAVAILABLE",
            ),
            (AiCallOutboundTargetModel.exception_batch_id.is_(None), "PENDING"),
            (AiCallOutboundTargetModel.status.in_({"DIALING", "IN_CALL"}), "CALLING"),
            (AiCallOutboundTargetModel.status == "RETRY_WAIT", "WAITING"),
            (AiCallOutboundTargetModel.status == "CANCELLED", "STOPPED"),
            (
                (AiCallOutboundTargetModel.status == "COMPLETED")
                & (AiCallOutboundTargetModel.latest_result == "invalid_number"),
                "UNAVAILABLE",
            ),
            (
                (AiCallOutboundTargetModel.status == "COMPLETED")
                & (AiCallOutboundTargetModel.latest_result == "connected"),
                "CONNECTED",
            ),
            (
                (AiCallOutboundTargetModel.status == "COMPLETED")
                & (retry_count >= AiCallOutboundExceptionBatchModel.max_retry_count),
                "MAXED",
            ),
            else_="STOPPED",
        )

    @staticmethod
    def _policy_out(policy: AiCallOutboundExceptionPolicyModel) -> ExceptionPolicyOut:
        return ExceptionPolicyOut(
            category=policy.category,
            interval_days=policy.interval_days,
            max_retry_count=policy.max_retry_count,
            retryable=True,
        )

    @staticmethod
    def _batch_out(batch: AiCallOutboundExceptionBatchModel) -> ExceptionBatchOut:
        return ExceptionBatchOut(
            accepted=True,
            batch_id=str(batch.id),
            category=batch.category,
            status=batch.status,
            target_count=batch.target_count,
            interval_days=batch.interval_days,
            max_retry_count=batch.max_retry_count,
            created_by=str(batch.created_by),
            created_by_name=batch.created_by_name,
            started_at=_format_datetime(batch.started_at) or "",
        )

    @staticmethod
    def _require_category(category: str) -> None:
        if category not in EXCEPTION_CATEGORIES:
            raise CustomException(msg="不支持的异常类别", status_code=400)

    @classmethod
    def _require_retryable_category(cls, category: str) -> None:
        cls._require_category(category)
        if category == "invalid_number":
            raise CustomException(msg="空号停机不可重新外呼", status_code=409)
