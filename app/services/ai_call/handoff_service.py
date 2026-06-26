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
HANDOFF_ACCEPT_MIN_REMAINING_SECONDS = 3
VALID_HANDOFF_SOURCES = {"operator", "system", "customer"}
HANDOFF_AGENT_STATUS_ONLINE = "online"
HANDOFF_AGENT_STATUS_BUSY = "busy"
HANDOFF_AGENT_STATUS_OFFLINE = "offline"
HANDOFF_AGENT_ONLINE_STALE_SECONDS = 15
VALID_MANUAL_HANDOFF_AGENT_STATUSES = {
    HANDOFF_AGENT_STATUS_ONLINE,
    HANDOFF_AGENT_STATUS_OFFLINE,
}
DEFAULT_HANDOFF_SKILL_GROUP = "default"
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
            now=utc_now() + timedelta(seconds=HANDOFF_ACCEPT_MIN_REMAINING_SECONDS),
            limit=limit,
        )

    async def set_agent_status(
        self,
        *,
        human_agent_identity: str,
        agent_status: str,
        skill_group: str | None = None,
    ):
        identity = self._normalize_agent_identity(human_agent_identity)
        target_status = self._validate_manual_agent_status(agent_status)
        existing = await self.repository.get_handoff_agent(identity)
        if (
            existing is not None
            and existing.status == HANDOFF_AGENT_STATUS_BUSY
            and existing.active_handoff_id
        ):
            self._raise_invalid_status("坐席正在通话中，不能手动切换状态")
        now = utc_now()
        return await self.repository.upsert_handoff_agent(
            agent_identity=identity,
            skill_group=self._normalize_skill_group(skill_group),
            status=target_status,
            active_handoff_id=None,
            last_seen_at=now,
            status_updated_at=now,
        )

    async def get_agent_status(self, human_agent_identity: str):
        identity = self._normalize_agent_identity(human_agent_identity)
        agent = await self.repository.get_handoff_agent(identity)
        if agent is None:
            return None
        return await self._refresh_agent_presence(agent)

    async def accept(
        self,
        *,
        handoff_id: str,
        human_agent_identity: str,
    ) -> AiCallHandoffModel:
        agent_identity = self._normalize_agent_identity(human_agent_identity)
        handoff = await self._get_required(handoff_id)
        handoff = await self._expire_if_needed(handoff)
        if handoff is None:
            self._raise_invalid_status("当前转人工状态不允许接管")
        if handoff.status != HANDOFF_STATUS_REQUESTED:
            self._raise_invalid_status("当前转人工状态不允许接管")
        self._ensure_accept_window(handoff)
        await self._ensure_agent_can_accept(agent_identity)
        accepted = await self._update_required(
            handoff.handoff_id,
            status=HANDOFF_STATUS_ACCEPTED,
            human_agent_identity=agent_identity,
            accepted_at=utc_now(),
        )
        await self._mark_agent_busy(agent_identity, accepted.handoff_id)
        return accepted

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

    async def confirm_accepted(
        self,
        *,
        handoff_id: str,
        human_agent_identity: str,
    ) -> AiCallHandoffModel:
        agent_identity = self._normalize_agent_identity(human_agent_identity)
        handoff = await self._get_required(handoff_id)
        handoff = await self._expire_if_needed(handoff)
        if handoff is None:
            self._raise_invalid_status("当前转人工状态不允许确认接管")
        if handoff.status not in {HANDOFF_STATUS_REQUESTED, HANDOFF_STATUS_ACCEPTED}:
            self._raise_invalid_status("当前转人工状态不允许确认接管")
        accepted = await self._update_required(
            handoff.handoff_id,
            status=HANDOFF_STATUS_ACCEPTED,
            human_agent_identity=agent_identity,
            accepted_at=handoff.accepted_at or utc_now(),
        )
        await self._mark_agent_busy(agent_identity, accepted.handoff_id)
        return accepted

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
            "requestedAt": self._api_datetime(handoff.requested_at),
            "acceptedAt": self._api_datetime(handoff.accepted_at),
            "connectedAt": self._api_datetime(handoff.connected_at),
            "endedAt": self._api_datetime(handoff.ended_at),
            "expiresAt": self._api_datetime(handoff.expires_at),
            "endReason": handoff.end_reason,
            "failureStage": handoff.failure_stage,
            "failureMessage": handoff.failure_message,
        }

    def handoff_agent_to_dict(self, agent) -> dict[str, Any]:
        return {
            "id": str(agent.id),
            "humanAgentIdentity": agent.agent_identity,
            "skillGroup": agent.skill_group,
            "status": agent.status,
            "activeHandoffId": agent.active_handoff_id,
            "lastSeenAt": agent.last_seen_at,
            "statusUpdatedAt": agent.status_updated_at,
        }

    def default_handoff_agent_to_dict(self, human_agent_identity: str) -> dict[str, Any]:
        identity = self._normalize_agent_identity(human_agent_identity)
        return {
            "id": None,
            "humanAgentIdentity": identity,
            "skillGroup": DEFAULT_HANDOFF_SKILL_GROUP,
            "status": HANDOFF_AGENT_STATUS_OFFLINE,
            "activeHandoffId": None,
            "lastSeenAt": None,
            "statusUpdatedAt": None,
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
        finished = await self._update_required(
            handoff.handoff_id,
            status=status,
            ended_at=handoff.ended_at or utc_now(),
            end_reason=end_reason,
            failure_stage=failure_stage,
            failure_message=failure_message,
        )
        await self._release_agent_if_current(finished)
        return finished

    async def _update_required(self, handoff_id: str, **values) -> AiCallHandoffModel:
        handoff = await self.repository.update_handoff(handoff_id, **values)
        if handoff is None:
            raise CustomException(
                msg="转人工记录不存在",
                code=RET.ERROR.code,
                status_code=status.HTTP_404_NOT_FOUND,
            )
        return handoff

    async def _ensure_agent_can_accept(self, human_agent_identity: str) -> None:
        agent = await self.repository.get_handoff_agent(human_agent_identity)
        if agent is None:
            self._raise_invalid_status("坐席未上线，不能接管转人工")
        agent = await self._expire_stale_agent_if_needed(agent)
        if agent.status != HANDOFF_AGENT_STATUS_ONLINE or agent.active_handoff_id:
            self._raise_invalid_status("坐席当前不可接管转人工")

    async def _refresh_agent_presence(self, agent):
        agent = await self._expire_stale_agent_if_needed(agent)
        if (
            agent.status != HANDOFF_AGENT_STATUS_ONLINE
            or agent.active_handoff_id
        ):
            return agent
        now = utc_now()
        return await self.repository.upsert_handoff_agent(
            agent_identity=agent.agent_identity,
            skill_group=agent.skill_group,
            status=agent.status,
            active_handoff_id=agent.active_handoff_id,
            last_seen_at=now,
            status_updated_at=agent.status_updated_at,
        )

    async def _expire_stale_agent_if_needed(self, agent):
        if (
            agent.status != HANDOFF_AGENT_STATUS_ONLINE
            or agent.active_handoff_id
            or not self._is_agent_presence_stale(agent)
        ):
            return agent
        now = utc_now()
        return await self.repository.upsert_handoff_agent(
            agent_identity=agent.agent_identity,
            skill_group=agent.skill_group,
            status=HANDOFF_AGENT_STATUS_OFFLINE,
            active_handoff_id=None,
            last_seen_at=agent.last_seen_at,
            status_updated_at=now,
        )

    def _is_agent_presence_stale(self, agent) -> bool:
        if agent.last_seen_at is None:
            return True
        elapsed = utc_now() - self._ensure_utc(agent.last_seen_at)
        return elapsed > timedelta(seconds=HANDOFF_AGENT_ONLINE_STALE_SECONDS)

    def _ensure_accept_window(self, handoff: AiCallHandoffModel) -> None:
        if handoff.expires_at is None:
            return
        remaining = self._ensure_utc(handoff.expires_at) - utc_now()
        if remaining <= timedelta(seconds=HANDOFF_ACCEPT_MIN_REMAINING_SECONDS):
            self._raise_invalid_status("转人工请求即将超时，请重新发起")

    async def _mark_agent_busy(self, human_agent_identity: str, handoff_id: str) -> None:
        existing = await self.repository.get_handoff_agent(human_agent_identity)
        now = utc_now()
        await self.repository.upsert_handoff_agent(
            agent_identity=human_agent_identity,
            skill_group=existing.skill_group if existing is not None else DEFAULT_HANDOFF_SKILL_GROUP,
            status=HANDOFF_AGENT_STATUS_BUSY,
            active_handoff_id=handoff_id,
            last_seen_at=now,
            status_updated_at=now,
        )

    async def _release_agent_if_current(self, handoff: AiCallHandoffModel) -> None:
        if not handoff.human_agent_identity:
            return
        agent = await self.repository.get_handoff_agent(handoff.human_agent_identity)
        if agent is None or agent.active_handoff_id != handoff.handoff_id:
            return
        now = utc_now()
        await self.repository.upsert_handoff_agent(
            agent_identity=agent.agent_identity,
            skill_group=agent.skill_group,
            status=HANDOFF_AGENT_STATUS_ONLINE,
            active_handoff_id=None,
            last_seen_at=now,
            status_updated_at=now,
        )

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

    @classmethod
    def _validate_manual_agent_status(cls, agent_status: str) -> str:
        value = (agent_status or "").strip().lower()
        if value not in VALID_MANUAL_HANDOFF_AGENT_STATUSES:
            raise CustomException(
                msg="不支持的坐席状态",
                code=RET.ERROR.code,
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        return value

    @staticmethod
    def _normalize_agent_identity(human_agent_identity: str) -> str:
        value = (human_agent_identity or "").strip()
        if not value:
            raise CustomException(
                msg="坐席标识不能为空",
                code=RET.ERROR.code,
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        return value

    @staticmethod
    def _normalize_skill_group(skill_group: str | None) -> str:
        return (skill_group or DEFAULT_HANDOFF_SKILL_GROUP).strip() or DEFAULT_HANDOFF_SKILL_GROUP

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

    @classmethod
    def _api_datetime(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return cls._ensure_utc(value)
