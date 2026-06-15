from __future__ import annotations

from datetime import datetime

from fastapi import status

from app.common.constant import RET
from app.config.setting import settings
from app.core.exceptions import CustomException
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


class AiCallService:
    def __init__(self, orchestrator: AiCallOrchestrator) -> None:
        self.orchestrator = orchestrator

    async def create_web_session(
        self,
        voice: str | None,
        prompt: str | None,
    ) -> CreateSessionResult:
        try:
            return await self.orchestrator.create_web_session(voice=voice, prompt=prompt)
        except AiCallError as exc:
            raise self._to_custom_exception(exc) from exc

    async def reissue_browser_token(self, call_id: str) -> ReissueTokenResult:
        try:
            return await self.orchestrator.reissue_browser_token(call_id)
        except AiCallError as exc:
            raise self._to_custom_exception(exc) from exc

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
            return await self.orchestrator.report_browser_event(
                call_id=call_id,
                event_type=event_type,
                timestamp=timestamp,
            )
        except AiCallError as exc:
            raise self._to_custom_exception(exc) from exc

    async def end_session(self, call_id: str) -> EndSessionResult:
        try:
            return await self.orchestrator.end_session(call_id)
        except AiCallError as exc:
            raise self._to_custom_exception(exc) from exc

    @staticmethod
    def _to_custom_exception(exc: AiCallError) -> CustomException:
        return CustomException(
            msg=exc.msg,
            code=RET.ERROR.code,
            status_code=exc.status_code or status.HTTP_500_INTERNAL_SERVER_ERROR,
            data=None,
        )


_default_service: AiCallService | None = None


def get_default_ai_call_service() -> AiCallService:
    global _default_service
    if _default_service is None:
        _default_service = AiCallService(AiCallOrchestrator.from_settings(settings))
    return _default_service
