from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import AiCallEventModel, AiCallRecordModel
from app.services.ai_call.event_store import AiCallEvent
from app.services.ai_call.session_registry import CallSessionStatus, utc_now

SENSITIVE_PAYLOAD_KEYS = {
    "api_key",
    "apikey",
    "authorization",
    "audio_delta",
    "audiodata",
    "bearer",
    "dashscope_api_key",
    "livekit_api_secret",
    "opening_message",
    "participant_token",
    "prompt",
    "token",
}


class AiCallRecordService:
    """B1 通话记录和关键事件持久化服务。"""

    def __init__(self, repository: AiCallRecordRepository) -> None:
        self.repository = repository

    async def create_web_record(
        self,
        *,
        call_id: str,
        business_type: str | None,
        business_id: str | None,
        room_name: str,
        participant_identity: str,
        started_at: datetime | None = None,
    ) -> AiCallRecordModel:
        return await self.repository.create_record(
            call_id=call_id,
            business_type=business_type,
            business_id=business_id,
            entry_type="web",
            room_name=room_name,
            participant_identity=participant_identity,
            status=CallSessionStatus.CREATED.value,
            started_at=started_at or utc_now(),
        )

    async def mark_status(
        self,
        call_id: str,
        status: str | CallSessionStatus,
    ) -> AiCallRecordModel | None:
        return await self.repository.update_record(
            call_id,
            status=self._status_value(status),
        )

    async def mark_answered(
        self,
        call_id: str,
        answered_at: datetime | None = None,
    ) -> AiCallRecordModel | None:
        record = await self.repository.get_record(call_id)
        if record is None:
            return None
        return await self.repository.update_record(
            call_id,
            status=CallSessionStatus.CONNECTED.value,
            answered_at=record.answered_at or answered_at or utc_now(),
        )

    async def complete_session(
        self,
        call_id: str,
        *,
        end_reason: str,
        ended_at: datetime | None = None,
    ) -> AiCallRecordModel | None:
        return await self._finish_session(
            call_id,
            status=CallSessionStatus.COMPLETED.value,
            end_reason=end_reason,
            failure_stage=None,
            failure_message=None,
            ended_at=ended_at,
        )

    async def fail_session(
        self,
        call_id: str,
        *,
        end_reason: str,
        failure_stage: str | None,
        failure_message: str | None,
        ended_at: datetime | None = None,
    ) -> AiCallRecordModel | None:
        return await self._finish_session(
            call_id,
            status=CallSessionStatus.FAILED.value,
            end_reason=end_reason,
            failure_stage=failure_stage,
            failure_message=failure_message,
            ended_at=ended_at,
        )

    async def mirror_runtime_events(
        self,
        events: list[AiCallEvent],
        skip_event_types: set[str] | None = None,
    ) -> list[AiCallEventModel]:
        persisted: list[AiCallEventModel] = []
        skipped = skip_event_types or set()
        for event in events:
            if event.type in skipped:
                continue
            persisted.append(
                await self.repository.append_event(
                    event_id=event.event_id,
                    call_id=event.call_id,
                    event_type=event.type,
                    source=event.source,
                    event_time=event.timestamp,
                    payload_json=self._payload_to_json(event.payload),
                )
            )
        return persisted

    async def append_terminal_event(
        self,
        *,
        call_id: str,
        event_type: str,
        source: str,
        payload: dict[str, Any],
        event_time: datetime | None = None,
    ) -> AiCallEventModel:
        from app.utils.id_util import generate_snowflake_id

        return await self.repository.append_event(
            event_id=f"evt_{generate_snowflake_id()}",
            call_id=call_id,
            event_type=event_type,
            source=source,
            event_time=event_time or utc_now(),
            payload_json=self._payload_to_json(payload),
        )

    async def get_record(self, call_id: str) -> AiCallRecordModel | None:
        return await self.repository.get_record(call_id)

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
    ) -> tuple[list[AiCallRecordModel], int]:
        return await self.repository.list_records(
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

    async def list_events(
        self,
        call_id: str,
        *,
        limit: int = 200,
        after_event_id: str | None = None,
        event_type: str | None = None,
        source: str | None = None,
    ) -> list[AiCallEventModel]:
        return await self.repository.list_events(
            call_id=call_id,
            limit=limit,
            after_event_id=after_event_id,
            event_type=event_type,
            source=source,
        )

    async def get_last_event(self, call_id: str) -> AiCallEventModel | None:
        return await self.repository.get_last_event(call_id)

    def record_to_dict(self, record: AiCallRecordModel) -> dict[str, Any]:
        return {
            "id": str(record.id),
            "callId": record.call_id,
            "businessType": record.business_type,
            "businessId": record.business_id,
            "entryType": record.entry_type,
            "roomName": record.room_name,
            "participantIdentity": record.participant_identity,
            "status": record.status,
            "endReason": record.end_reason,
            "failureStage": record.failure_stage,
            "failureMessage": record.failure_message,
            "startedAt": record.started_at,
            "answeredAt": record.answered_at,
            "endedAt": record.ended_at,
            "durationMs": record.duration_ms,
        }

    def event_to_dict(self, event: AiCallEventModel) -> dict[str, Any]:
        return {
            "id": str(event.id),
            "eventId": event.event_id,
            "callId": event.call_id,
            "eventType": event.event_type,
            "source": event.source,
            "eventTime": event.event_time,
            "payload": event.payload,
        }

    async def _finish_session(
        self,
        call_id: str,
        *,
        status: str,
        end_reason: str,
        failure_stage: str | None,
        failure_message: str | None,
        ended_at: datetime | None,
    ) -> AiCallRecordModel | None:
        record = await self.repository.get_record(call_id)
        if record is None:
            return None
        final_ended_at = ended_at or utc_now()
        duration_start = record.answered_at or record.started_at
        final_ended_at = self._ensure_utc(final_ended_at)
        duration_start = self._ensure_utc(duration_start)
        duration_ms = max(0, int((final_ended_at - duration_start).total_seconds() * 1000))
        return await self.repository.update_record(
            call_id,
            status=status,
            end_reason=end_reason,
            failure_stage=failure_stage,
            failure_message=failure_message,
            ended_at=record.ended_at or final_ended_at,
            duration_ms=record.duration_ms if record.duration_ms is not None else duration_ms,
        )

    @classmethod
    def _payload_to_json(cls, payload: dict[str, Any] | None) -> str | None:
        sanitized = cls._sanitize_payload(payload or {})
        if not sanitized:
            return None
        return json.dumps(sanitized, ensure_ascii=False, sort_keys=True)

    @classmethod
    def _sanitize_payload(cls, value: Any) -> Any:
        if isinstance(value, dict):
            safe: dict[str, Any] = {}
            for key, item in value.items():
                normalized = key.replace("-", "_").lower()
                if normalized in SENSITIVE_PAYLOAD_KEYS:
                    safe[key] = "<redacted>"
                else:
                    safe[key] = cls._sanitize_payload(item)
            return safe
        if isinstance(value, list):
            return [cls._sanitize_payload(item) for item in value]
        return value

    @staticmethod
    def _status_value(status: str | CallSessionStatus) -> str:
        return status.value if isinstance(status, CallSessionStatus) else status

    @staticmethod
    def _ensure_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
