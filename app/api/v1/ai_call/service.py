from __future__ import annotations

from datetime import datetime

from fastapi import status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.common.constant import RET
from app.config.setting import settings
from app.core.exceptions import CustomException
from app.services.ai_call.event_store import AiCallEvent
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.orchestrator import (
    AiCallOrchestrator,
    BrowserEventReportResult,
    CreateSessionResult,
    EndSessionResult,
    EventListResult,
    ReissueTokenResult,
    SessionStatusResult,
)
from app.services.ai_call.record_service import AiCallRecordService
from app.services.ai_call.session_registry import CallSessionStatus
from app.utils.id_util import generate_snowflake_id


class AiCallService:
    def __init__(
        self,
        orchestrator: AiCallOrchestrator,
        record_service: AiCallRecordService | None = None,
    ) -> None:
        self.orchestrator = orchestrator
        self.record_service = record_service

    async def create_web_session(
        self,
        voice: str | None,
        prompt: str | None,
        business_type: str | None = None,
        business_id: str | None = None,
    ) -> CreateSessionResult:
        if self.record_service is None:
            try:
                return await self.orchestrator.create_web_session(voice=voice, prompt=prompt)
            except AiCallError as exc:
                raise self._to_custom_exception(exc) from exc

        call_id = f"call_{generate_snowflake_id()}"
        room_name = f"ai-call-{call_id}"
        participant_identity = f"browser-{call_id}"
        await self.record_service.create_web_record(
            call_id=call_id,
            business_type=business_type,
            business_id=business_id,
            room_name=room_name,
            participant_identity=participant_identity,
        )
        try:
            result = await self.orchestrator.create_web_session(
                voice=voice,
                prompt=prompt,
                call_id=call_id,
            )
        except AiCallError as exc:
            await self._mirror_runtime_events(call_id)
            await self.record_service.fail_session(
                call_id,
                end_reason=exc.error_id,
                failure_stage=exc.error_id,
                failure_message=exc.msg,
            )
            raise self._to_custom_exception(exc) from exc
        await self._mirror_runtime_events(call_id)
        await self.record_service.mark_status(call_id, result.status)
        return result

    async def reissue_browser_token(self, call_id: str) -> ReissueTokenResult:
        try:
            result = await self.orchestrator.reissue_browser_token(call_id)
        except AiCallError as exc:
            raise self._to_custom_exception(exc) from exc
        await self._mirror_runtime_events(call_id)
        return result

    async def get_session(self, call_id: str) -> SessionStatusResult:
        try:
            return await self.orchestrator.get_session(call_id)
        except AiCallError as exc:
            raise self._to_custom_exception(exc) from exc

    async def list_events(
        self,
        call_id: str,
        limit: int,
        after_event_id: str | None,
    ) -> EventListResult:
        try:
            if self.record_service is not None:
                rows = await self.record_service.list_events(
                    call_id,
                    limit=limit,
                    after_event_id=after_event_id,
                )
                if rows:
                    runtime_rows = [
                        AiCallEvent(
                            event_id=row.event_id,
                            call_id=row.call_id,
                            type=row.event_type,
                            timestamp=row.event_time,
                            source=row.source,
                            payload=row.payload,
                        )
                        for row in rows
                    ]
                    return EventListResult(rows=runtime_rows, total=len(runtime_rows))
            return await self.orchestrator.list_events(
                call_id=call_id,
                limit=limit,
                after_event_id=after_event_id,
            )
        except AiCallError as exc:
            raise self._to_custom_exception(exc) from exc

    async def report_browser_event(
        self,
        call_id: str,
        event_type: str,
        timestamp: datetime | None,
    ) -> BrowserEventReportResult:
        try:
            result = await self.orchestrator.report_browser_event(
                call_id=call_id,
                event_type=event_type,
                timestamp=timestamp,
            )
        except AiCallError as exc:
            raise self._to_custom_exception(exc) from exc
        await self._mirror_runtime_events(call_id)
        if self.record_service is not None and event_type == "browser_ready":
            await self.record_service.mark_answered(call_id, result.timestamp)
        return result

    async def end_session(self, call_id: str) -> EndSessionResult:
        try:
            result = await self.orchestrator.end_session(call_id)
        except AiCallError as exc:
            raise self._to_custom_exception(exc) from exc
        await self._mirror_runtime_events(call_id)
        if self.record_service is not None:
            if result.status == CallSessionStatus.COMPLETED:
                await self.record_service.complete_session(
                    call_id,
                    end_reason="web_user_end",
                )
                await self.record_service.append_terminal_event(
                    call_id=call_id,
                    event_type="session_completed",
                    source="orchestrator",
                    payload={"endReason": "web_user_end"},
                )
            elif result.status == CallSessionStatus.FAILED:
                await self.record_service.fail_session(
                    call_id,
                    end_reason="unknown",
                    failure_stage="runtime",
                    failure_message="会话已失败",
                )
        return result

    async def list_records(
        self,
        *,
        call_id: str | None = None,
        business_type: str | None = None,
        business_id: str | None = None,
        status: str | None = None,
        entry_type: str | None = None,
        started_at_begin: datetime | None = None,
        started_at_end: datetime | None = None,
        page_num: int = 1,
        page_size: int = 10,
    ) -> dict:
        self._ensure_record_service()
        rows, total = await self.record_service.list_records(
            call_id=call_id,
            business_type=business_type,
            business_id=business_id,
            status=status,
            entry_type=entry_type,
            started_at_begin=started_at_begin,
            started_at_end=started_at_end,
            page_num=page_num,
            page_size=page_size,
        )
        return {
            "rows": [self.record_service.record_to_dict(row) for row in rows],
            "total": total,
        }

    async def get_record_detail(self, call_id: str) -> dict:
        self._ensure_record_service()
        record = await self.record_service.get_record(call_id)
        if record is None:
            raise CustomException(msg="通话记录不存在", code=RET.ERROR.code, status_code=404)
        last_event = await self.record_service.get_last_event(call_id)
        return {
            "record": self.record_service.record_to_dict(record),
            "lastEvent": self.record_service.event_to_dict(last_event) if last_event else None,
        }

    async def list_record_events(
        self,
        call_id: str,
        *,
        limit: int = 200,
        after_event_id: str | None = None,
        event_type: str | None = None,
        source: str | None = None,
    ) -> dict:
        self._ensure_record_service()
        rows = await self.record_service.list_events(
            call_id,
            limit=limit,
            after_event_id=after_event_id,
            event_type=event_type,
            source=source,
        )
        return {
            "rows": [self.record_service.event_to_dict(row) for row in rows],
            "total": len(rows),
        }

    async def _mirror_runtime_events(self, call_id: str) -> None:
        if self.record_service is None:
            return
        with_runtime_rows = self.orchestrator.event_store.list(call_id=call_id, limit=1000)
        await self.record_service.mirror_runtime_events(with_runtime_rows)

    def _ensure_record_service(self) -> None:
        if self.record_service is None:
            raise CustomException(msg="通话记录服务未启用", code=RET.ERROR.code, status_code=500)

    @staticmethod
    def _to_custom_exception(exc: AiCallError) -> CustomException:
        return CustomException(
            msg=exc.msg,
            code=RET.ERROR.code,
            status_code=exc.status_code or status.HTTP_500_INTERNAL_SERVER_ERROR,
            data=None,
        )


_default_orchestrator: AiCallOrchestrator | None = None


def get_default_ai_call_service(db: AsyncSession | None = None) -> AiCallService:
    global _default_orchestrator
    if _default_orchestrator is None:
        _default_orchestrator = AiCallOrchestrator.from_settings(settings)
    record_service = AiCallRecordService(AiCallRecordRepository(db)) if db is not None else None
    return AiCallService(_default_orchestrator, record_service)
