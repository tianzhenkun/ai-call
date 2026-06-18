from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import status

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import AiCallHandoffModel
from app.common.constant import RET
from app.core.exceptions import CustomException
from app.services.ai_call.session_registry import CallSessionStatus, utc_now
from app.utils.id_util import generate_snowflake_id

HANDOFF_STATUS_REQUESTED = "requested"
HANDOFF_STATUS_ACCEPTED = "accepted"
HANDOFF_STATUS_CONNECTED = "connected"
HANDOFF_STATUS_COMPLETED = "completed"
HANDOFF_STATUS_CANCELED = "canceled"
HANDOFF_STATUS_FAILED = "failed"
HANDOFF_STATUS_EXPIRED = "expired"

HANDOFF_TERMINAL_STATUSES = {
    HANDOFF_STATUS_COMPLETED,
    HANDOFF_STATUS_CANCELED,
    HANDOFF_STATUS_FAILED,
    HANDOFF_STATUS_EXPIRED,
}
HANDOFF_ACTIVE_STATUSES = {
    HANDOFF_STATUS_REQUESTED,
    HANDOFF_STATUS_ACCEPTED,
    HANDOFF_STATUS_CONNECTED,
}
HANDOFF_EXPIRABLE_STATUSES = {
    HANDOFF_STATUS_REQUESTED,
    HANDOFF_STATUS_ACCEPTED,
}
HANDOFF_JOINABLE_STATUSES = {
    HANDOFF_STATUS_REQUESTED,
}
VALID_HANDOFF_SOURCES = {"operator", "system", "customer"}
CALL_TERMINAL_STATUSES = {
    CallSessionStatus.COMPLETED.value,
    CallSessionStatus.FAILED.value,
}


class AiCallHandoffService:
    """B3 转人工状态服务。"""

    def __init__(
        self,
        repository: AiCallRecordRepository,
        *,
        request_timeout_seconds: int = 120,
    ) -> None:
        self.repository = repository
        self.request_timeout_seconds = max(1, request_timeout_seconds)
        self._expired_handoffs: list[AiCallHandoffModel] = []

    async def create_request(
        self,
        *,
        call_id: str,
        room_name: str,
        source: str,
        reason: str | None,
        request_message: str | None,
    ) -> tuple[AiCallHandoffModel, bool]:
        await self._ensure_call_can_handoff(call_id)
        request_source = self._validate_source(source)
        active = await self.get_current(call_id)
        if active is not None:
            return active, False

        requested_at = utc_now()
        expires_at = requested_at + timedelta(seconds=self.request_timeout_seconds)
        handoff = await self.repository.create_handoff(
            handoff_id=f"handoff_{generate_snowflake_id()}",
            call_id=call_id,
            room_name=room_name,
            status=HANDOFF_STATUS_REQUESTED,
            request_source=request_source,
            request_reason=reason,
            request_message=request_message,
            requested_at=requested_at,
            expires_at=expires_at,
        )
        return handoff, True

    async def get_current(self, call_id: str) -> AiCallHandoffModel | None:
        handoff = await self.repository.get_active_handoff(
            call_id,
            terminal_statuses=HANDOFF_TERMINAL_STATUSES,
        )
        if handoff is None:
            return None
        handoff = await self._expire_if_needed(handoff)
        if handoff is None or handoff.status in HANDOFF_TERMINAL_STATUSES:
            return None
        return handoff

    async def list_handoffs(self, call_id: str) -> list[AiCallHandoffModel]:
        await self._ensure_call_exists(call_id)
        return await self.repository.list_handoffs(call_id)

    async def list_joinable_handoffs(self, *, limit: int = 50) -> list[AiCallHandoffModel]:
        return await self.repository.list_joinable_handoffs(
            statuses=HANDOFF_JOINABLE_STATUSES,
            now=utc_now(),
            limit=limit,
        )

    async def accept(
        self,
        *,
        handoff_id: str,
        human_agent_identity: str,
    ) -> AiCallHandoffModel:
        handoff = await self._get_required(handoff_id)
        handoff = await self._expire_if_needed(handoff)
        if handoff is None:
            self._raise_invalid_status("当前转人工状态不允许接管")
        if handoff.status != HANDOFF_STATUS_REQUESTED:
            self._raise_invalid_status("当前转人工状态不允许接管")
        return await self._update_required(
            handoff.handoff_id,
            status=HANDOFF_STATUS_ACCEPTED,
            human_agent_identity=human_agent_identity,
            accepted_at=utc_now(),
        )

    async def mark_connected(self, handoff_id: str) -> AiCallHandoffModel:
        handoff = await self._get_required(handoff_id)
        handoff = await self._expire_if_needed(handoff)
        if handoff is None:
            self._raise_invalid_status("当前转人工状态不允许标记已连接")
        if handoff.status != HANDOFF_STATUS_ACCEPTED:
            self._raise_invalid_status("当前转人工状态不允许标记已连接")
        return await self._update_required(
            handoff.handoff_id,
            status=HANDOFF_STATUS_CONNECTED,
            connected_at=utc_now(),
        )

    async def complete(
        self,
        *,
        handoff_id: str,
        reason: str | None,
    ) -> AiCallHandoffModel:
        handoff = await self._get_required(handoff_id)
        if handoff.status != HANDOFF_STATUS_CONNECTED:
            self._raise_invalid_status("当前转人工状态不允许完成")
        return await self._finish(
            handoff,
            status=HANDOFF_STATUS_COMPLETED,
            end_reason=reason or "agent_completed",
        )

    async def cancel(
        self,
        *,
        handoff_id: str,
        reason: str | None,
    ) -> AiCallHandoffModel:
        handoff = await self._get_required(handoff_id)
        handoff = await self._expire_if_needed(handoff)
        if handoff is None:
            self._raise_invalid_status("当前转人工状态不允许取消")
        if handoff.status not in HANDOFF_ACTIVE_STATUSES:
            self._raise_invalid_status("当前转人工状态不允许取消")
        return await self._finish(
            handoff,
            status=HANDOFF_STATUS_CANCELED,
            end_reason=reason or "operator_cancelled",
        )

    async def fail(
        self,
        *,
        handoff_id: str,
        failure_stage: str,
        failure_message: str | None,
    ) -> AiCallHandoffModel:
        handoff = await self._get_required(handoff_id)
        if handoff.status not in HANDOFF_ACTIVE_STATUSES:
            self._raise_invalid_status("当前转人工状态不允许失败标记")
        return await self._finish(
            handoff,
            status=HANDOFF_STATUS_FAILED,
            end_reason=failure_stage,
            failure_stage=failure_stage,
            failure_message=failure_message,
        )

    async def fail_request(
        self,
        *,
        handoff_id: str,
        failure_stage: str,
        failure_message: str,
    ) -> AiCallHandoffModel | None:
        handoff = await self.repository.get_handoff_by_id(handoff_id)
        if handoff is None or handoff.status in HANDOFF_TERMINAL_STATUSES:
            return handoff
        return await self._finish(
            handoff,
            status=HANDOFF_STATUS_FAILED,
            end_reason=failure_stage,
            failure_stage=failure_stage,
            failure_message=failure_message,
        )

    async def expire_request(self, handoff_id: str) -> AiCallHandoffModel | None:
        handoff = await self.repository.get_handoff_by_id(handoff_id)
        if handoff is None:
            return None
        expired = await self._expire_if_needed(handoff)
        if expired is None or expired.status != HANDOFF_STATUS_EXPIRED:
            return None
        return expired

    async def finalize_active_for_call(
        self,
        call_id: str,
        *,
        end_reason: str,
    ) -> list[AiCallHandoffModel]:
        active_rows = await self.repository.list_active_handoffs_for_call(
            call_id,
            terminal_statuses=HANDOFF_TERMINAL_STATUSES,
        )
        finalized: list[AiCallHandoffModel] = []
        for handoff in active_rows:
            target_status = (
                HANDOFF_STATUS_COMPLETED
                if handoff.status == HANDOFF_STATUS_CONNECTED
                else HANDOFF_STATUS_CANCELED
            )
            finalized.append(
                await self._finish(
                    handoff,
                    status=target_status,
                    end_reason=end_reason,
                )
            )
        return finalized

    def handoff_to_dict(self, handoff: AiCallHandoffModel) -> dict[str, Any]:
        return {
            "id": str(handoff.id),
            "handoffId": handoff.handoff_id,
            "callId": handoff.call_id,
            "roomName": handoff.room_name,
            "status": handoff.status,
            "requestSource": handoff.request_source,
            "requestReason": handoff.request_reason,
            "requestMessage": handoff.request_message,
            "humanAgentIdentity": handoff.human_agent_identity,
            "requestedAt": handoff.requested_at,
            "acceptedAt": handoff.accepted_at,
            "connectedAt": handoff.connected_at,
            "endedAt": handoff.ended_at,
            "expiresAt": handoff.expires_at,
            "endReason": handoff.end_reason,
            "failureStage": handoff.failure_stage,
            "failureMessage": handoff.failure_message,
        }

    def consume_expired_handoffs(self) -> list[AiCallHandoffModel]:
        rows = list(self._expired_handoffs)
        self._expired_handoffs.clear()
        return rows

    async def _ensure_call_exists(self, call_id: str) -> None:
        record = await self.repository.get_record(call_id)
        if record is None:
            raise CustomException(
                msg="通话记录不存在",
                code=RET.ERROR.code,
                status_code=status.HTTP_404_NOT_FOUND,
            )

    async def _ensure_call_can_handoff(self, call_id: str) -> None:
        record = await self.repository.get_record(call_id)
        if record is None:
            raise CustomException(
                msg="通话记录不存在",
                code=RET.ERROR.code,
                status_code=status.HTTP_404_NOT_FOUND,
            )
        if record.status in CALL_TERMINAL_STATUSES:
            raise CustomException(
                msg="会话已结束，不允许转人工",
                code=RET.ERROR.code,
                status_code=status.HTTP_409_CONFLICT,
            )

    async def _get_required(self, handoff_id: str) -> AiCallHandoffModel:
        handoff = await self.repository.get_handoff_by_id(handoff_id)
        if handoff is None:
            raise CustomException(
                msg="转人工记录不存在",
                code=RET.ERROR.code,
                status_code=status.HTTP_404_NOT_FOUND,
            )
        return handoff

    async def _expire_if_needed(
        self,
        handoff: AiCallHandoffModel,
    ) -> AiCallHandoffModel | None:
        if handoff.status in HANDOFF_TERMINAL_STATUSES:
            return None
        if handoff.status not in HANDOFF_EXPIRABLE_STATUSES or handoff.expires_at is None:
            return handoff
        if self._ensure_utc(handoff.expires_at) > utc_now():
            return handoff
        expired = await self._finish(
            handoff,
            status=HANDOFF_STATUS_EXPIRED,
            end_reason="timeout",
        )
        self._expired_handoffs.append(expired)
        return expired

    async def _finish(
        self,
        handoff: AiCallHandoffModel,
        *,
        status: str,
        end_reason: str,
        failure_stage: str | None = None,
        failure_message: str | None = None,
    ) -> AiCallHandoffModel:
        return await self._update_required(
            handoff.handoff_id,
            status=status,
            ended_at=handoff.ended_at or utc_now(),
            end_reason=end_reason,
            failure_stage=failure_stage,
            failure_message=failure_message,
        )

    async def _update_required(self, handoff_id: str, **values) -> AiCallHandoffModel:
        handoff = await self.repository.update_handoff(handoff_id, **values)
        if handoff is None:
            raise CustomException(
                msg="转人工记录不存在",
                code=RET.ERROR.code,
                status_code=status.HTTP_404_NOT_FOUND,
            )
        return handoff

    @staticmethod
    def _validate_source(source: str) -> str:
        value = (source or "operator").strip().lower()
        if value not in VALID_HANDOFF_SOURCES:
            raise CustomException(
                msg="不支持的转人工请求来源",
                code=RET.ERROR.code,
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        return value

    @staticmethod
    def _raise_invalid_status(msg: str) -> None:
        raise CustomException(
            msg=msg,
            code=RET.ERROR.code,
            status_code=status.HTTP_409_CONFLICT,
        )

    @staticmethod
    def _ensure_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
