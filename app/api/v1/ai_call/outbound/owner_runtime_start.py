from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.ai_call.runtime_control.command_repository import (
    RuntimeCommandRepository,
    StartCallIntent,
)
from app.services.ai_call.runtime_control.direct_sip_phone import (
    prepare_direct_sip_phone,
)
from app.services.ai_call.runtime_control.timing import read_database_time
from app.utils.id_util import generate_snowflake_id

from .rule_task_model import AiCallOutboundAttemptModel
from .task_executor import OutboundDialRequest


class OwnerRuntimeOutboundStart:
    """在现有外呼认领事务中创建 DB-only Runtime 启动事实。"""

    def __init__(
        self,
        *,
        id_generator: Callable[[], int] = generate_snowflake_id,
        allocation_timeout_seconds: float = 30.0,
        database_clock: Callable[
            [AsyncSession], Awaitable[datetime]
        ] = read_database_time,
    ) -> None:
        self._id_generator = id_generator
        self._allocation_timeout_seconds = allocation_timeout_seconds
        self._database_clock = database_clock

    async def create(
        self,
        session: AsyncSession,
        request: OutboundDialRequest,
        *,
        now: datetime,
    ) -> str:
        if request.answer_mode == "linphone" and request.line is None:
            raise ValueError("Owner Runtime 外呼缺少 SIP 线路快照")
        if request.answer_mode == "linphone" and not request.phone_number:
            raise ValueError("Owner Runtime 外呼缺少手机号")

        phone = (
            prepare_direct_sip_phone(request.phone_number)
            if request.answer_mode == "linphone"
            else None
        )
        attempt_id = self._id_generator()
        idempotency_key = (
            f"outbound:{request.tenant_id}:{request.task_id}:"
            f"{request.target_id}:{request.attempt_no}"
        )
        payload = {
            "attempt_id": str(attempt_id),
            "attempt_no": request.attempt_no,
            "business_params": {"customerName": request.customer_name or ""},
            "line_code": request.line.line_code if request.line is not None else None,
            "line_id": str(request.line.line_id) if request.line is not None else None,
            "prompt_profile_id": request.prompt_profile_id,
            "scene_code": request.scene_code,
            "target_id": str(request.target_id),
            "task_id": str(request.task_id),
            "voice": request.voice,
        }
        command = await RuntimeCommandRepository(
            session,
            database_clock=self._database_clock,
        ).create_start_call(
            StartCallIntent(
                tenant_id=request.tenant_id,
                entry_type="outbound" if request.answer_mode == "linphone" else "web",
                idempotency_key=idempotency_key,
                payload=payload,
                business_type="outbound_attempt",
                business_id=str(attempt_id),
                scene_code=request.scene_code,
                prompt_source_key=request.prompt_profile_id,
                allocation_timeout_seconds=self._allocation_timeout_seconds,
                callee_phone_number=phone.plaintext if phone is not None else None,
                callee_phone_number_masked=phone.masked if phone is not None else None,
                callee_phone_number_hash=phone.fingerprint if phone is not None else None,
            )
        )
        session.add(
            AiCallOutboundAttemptModel(
                id=attempt_id,
                tenant_id=request.tenant_id,
                task_id=request.task_id,
                target_id=request.target_id,
                attempt_no=request.attempt_no,
                call_id=command.call_id,
                dialer_type="owner_runtime",
                test_scenario=None,
                command_idempotency_key=idempotency_key,
                active_slot=None,
                status="QUEUED",
                call_result=None,
                error_message=None,
                line_id=int(request.line.line_id) if request.line is not None else None,
                line_code=request.line.line_code if request.line is not None else None,
                provider_status_code=None,
                provider_reason=None,
                hangup_cause=None,
                started_at=now,
                ended_at=None,
                created_at=now,
                updated_at=now,
            )
        )
        await session.flush()
        return command.call_id
