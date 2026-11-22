from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.model import AiCallRecordModel
from app.api.v1.ai_call.service import AiCallService, get_default_ai_call_service
from app.core.logger import log

from .task_executor import ConnectedCallback, DialResult, OutboundDialRequest, OutboundTaskExecutor


class WebOutboundDialer:
    """本地 Web 任务复用 Legacy 会话，接听事实来自 browser_ready。"""

    dialer_type = "web"
    manages_call_record = True

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        ai_call_service_factory: Callable[[AsyncSession], AiCallService] = get_default_ai_call_service,
        poll_seconds: float = 0.5,
        timeout_seconds: float = 900,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.session_factory = session_factory
        self.ai_call_service_factory = ai_call_service_factory
        self.poll_seconds = poll_seconds
        self.timeout_seconds = timeout_seconds
        self.sleep = sleep

    async def dial(
        self, request: OutboundDialRequest, *, call_id: str, on_connected: ConnectedCallback,
    ) -> DialResult:
        if request.answer_mode != "web" or request.phone_number is not None:
            raise ValueError("Web 执行器只接受无手机号的浏览器任务")
        try:
            async with asyncio.timeout(self.timeout_seconds):
                async with self.session_factory() as db:
                    service = self.ai_call_service_factory(db)
                    try:
                        await service.create_web_session(
                            call_id=call_id, tenant_id=request.tenant_id,
                            business_type="outbound_task", business_id=str(request.task_id),
                            voice=request.voice, model=request.model, prompt=None,
                            scene_code=request.scene_code, prompt_snapshot=request.prompt_snapshot,
                            business_params={
                                **request.business_params, "targetId": str(request.target_id),
                                "attemptNo": request.attempt_no,
                            },
                        )
                    except (Exception, asyncio.CancelledError):
                        try:
                            await db.commit()
                        except Exception:
                            await db.rollback()
                        raise
                    await db.commit()
                connected = False
                while True:
                    record = await self._read_record(call_id)
                    if record is not None:
                        if record.answered_at is not None and not connected:
                            await on_connected()
                            connected = True
                        if record.status in {"completed", "failed"}:
                            if not await self.terminate(request, call_id=call_id, end_reason="web_terminal_cleanup"):
                                return DialResult(call_result="call_failed", retry_allowed=False,
                                                  settle_attempt=False, error_message="Web 通话已结束，资源清理失败，等待对账")
                            return OutboundTaskExecutor._terminal_sip_record_result(
                                record, media_connected=True,
                            )
                    await self.sleep(self.poll_seconds)
        except asyncio.CancelledError:
            await self.terminate(request, call_id=call_id, end_reason="web_executor_stopped")
            raise
        except Exception as exc:
            cleaned = await self.terminate(request, call_id=call_id, end_reason="web_task_failed")
            message = getattr(exc, "msg", None) or str(exc) or type(exc).__name__
            return DialResult(
                call_result="call_failed",
                error_message=f"Web 通话失败：{message}" + ("" if cleaned else "；资源清理失败，等待对账"),
                retry_allowed=False,
                settle_attempt=cleaned,
            )

    async def _read_record(self, call_id: str) -> AiCallRecordModel | None:
        async with self.session_factory() as db:
            return await db.scalar(select(AiCallRecordModel).where(AiCallRecordModel.call_id == call_id))

    async def terminate(self, request: OutboundDialRequest, *, call_id: str, end_reason: str) -> bool:
        if request.answer_mode != "web":
            return False
        async with self.session_factory() as db:
            service = self.ai_call_service_factory(db)
            try:
                await service.terminate_web_session(call_id, tenant_id=request.tenant_id, end_reason=end_reason)
                await db.commit()
            except Exception as exc:
                await db.rollback()
                log.warning("Web 任务清理失败：callId={}, error={}", call_id, type(exc).__name__)
                return False
        return True
