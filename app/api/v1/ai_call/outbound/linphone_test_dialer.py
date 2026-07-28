from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.model import AiCallRecordModel
from app.api.v1.ai_call.service import AiCallService, get_default_ai_call_service

from .task_executor import ConnectedCallback, DialResult, OutboundDialRequest

TERMINAL_STATUSES = {"completed", "failed"}
BUSY_END_REASONS = {
    "busy",
    "busy_here",
    "callee_busy",
    "sip_busy",
    "user_busy",
}
NO_ANSWER_END_REASONS = {
    "connect_timeout",
    "no_answer",
    "ringing_timeout",
    "sip_connect_timeout",
    "user_unavailable",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class LinphoneTestDialer:
    """通过现有 SIP 会话服务执行单路 Linphone 人工测试。"""

    dialer_type = "linphone_test"
    manages_call_record = True

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        ai_call_service_factory: Callable[[AsyncSession], AiCallService] = (
            get_default_ai_call_service
        ),
        poll_seconds: float = 0.5,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        now: Callable[[], datetime] = _utc_now,
    ) -> None:
        self.session_factory = session_factory
        self.ai_call_service_factory = ai_call_service_factory
        self.poll_seconds = max(0.0, poll_seconds)
        self.sleep = sleep
        self.now = now

    async def dial(
        self,
        request: OutboundDialRequest,
        *,
        call_id: str,
        on_connected: ConnectedCallback,
    ) -> DialResult:
        create_error: Exception | None = None
        async with self.session_factory() as db:
            service = self.ai_call_service_factory(db)
            try:
                await service.create_sip_session(
                    callee_phone_number=request.phone_number,
                    voice=request.voice,
                    call_id=call_id,
                    business_type="outbound_task",
                    business_id=str(request.task_id),
                    scene_code=request.scene_code,
                    business_params={
                        "customer_name": request.customer_name or "",
                        "target_id": str(request.target_id),
                    },
                )
            except Exception as exc:
                create_error = exc
                try:
                    await db.commit()
                except Exception:
                    await db.rollback()
            else:
                try:
                    await db.commit()
                except Exception as exc:
                    await db.rollback()
                    create_error = exc

        if create_error is not None:
            record = await self._read_record(call_id)
            if self._is_terminal(record):
                return self._map_record(record)
            message = str(create_error).strip() or create_error.__class__.__name__
            return DialResult(
                call_result="call_failed",
                error_message=f"Linphone 测试拨号失败：{message}",
            )

        await on_connected()
        while True:
            record = await self._read_record(call_id)
            if self._is_terminal(record):
                return self._map_record(record)
            await self.sleep(self.poll_seconds)

    async def _read_record(self, call_id: str) -> AiCallRecordModel | None:
        async with self.session_factory() as db:
            return await db.scalar(
                select(AiCallRecordModel).where(AiCallRecordModel.call_id == call_id)
            )

    @staticmethod
    def _is_terminal(record: AiCallRecordModel | None) -> bool:
        return (
            record is not None
            and str(record.status or "").lower() in TERMINAL_STATUSES
        )

    def _map_record(self, record: AiCallRecordModel) -> DialResult:
        if record.answered_at is not None:
            ended_at = record.ended_at or self.now()
            answered_at = self._with_utc_if_naive(record.answered_at)
            ended_at = self._with_utc_if_naive(ended_at)
            duration_ms = max(
                0,
                int((ended_at - answered_at).total_seconds() * 1000),
            )
            return DialResult(call_result="connected", duration_ms=duration_ms)

        end_reason = str(record.end_reason or "").strip()
        normalized_reason = end_reason.lower()
        if normalized_reason in BUSY_END_REASONS:
            call_result = "busy"
            default_message = "被叫正忙"
        elif normalized_reason in NO_ANSWER_END_REASONS:
            call_result = "no_answer"
            default_message = "无人接听"
        else:
            call_result = "call_failed"
            default_message = "通话失败"
        return DialResult(
            call_result=call_result,
            error_message=record.failure_message or end_reason or default_message,
        )

    @staticmethod
    def _with_utc_if_naive(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
