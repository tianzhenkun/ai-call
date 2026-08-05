from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import AiCallEventModel, AiCallRecordModel
from app.services.ai_call.event_store import AiCallEvent
from app.services.ai_call.runtime_control.customer_media_repository import (
    OwnerCustomerMediaRepository,
)
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

# B1 只持久化可复盘的低频关键事件，高频音频帧事件保留在运行态。
PERSISTED_EVENT_TYPES = frozenset({
    "agent_error",
    "agent_start_failed",
    "agent_started",
    "audio_playout_queue_full",
    "audio_playout_queue_watermark",
    "audio_playout_response_failed",
    "browser_disconnect",
    "browser_first_audio",
    "browser_audio_hold_completed",
    "browser_audio_hold_confirmed",
    "browser_audio_hold_expired",
    "browser_audio_hold_rejected_echo",
    "browser_audio_hold_requested",
    "browser_audio_input_diagnostics",
    "browser_interrupt_candidate_deferred",
    "browser_interrupt_candidate_promoted",
    "browser_pre_stop_completed",
    "browser_pre_stop_confirmed",
    "browser_pre_stop_expired",
    "browser_pre_stop_rejected_echo",
    "browser_pre_stop_requested",
    "browser_pre_stop_skipped",
    "browser_ready",
    "browser_token_issued",
    "browser_user_speech_segment",
    "browser_user_speech_started",
    "call_end_auto_failed",
    "call_end_intent_detected",
    "call_end_interrupted",
    "call_end_scheduled",
    "call_end_tool_missing",
    "call_end_tool_ignored",
    "call_end_tool_requested",
    "agent_suspended_for_handoff",
    "handoff_accepted",
    "handoff_canceled",
    "handoff_completed",
    "handoff_connected",
    "handoff_expired",
    "handoff_failed",
    "handoff_auto_end_scheduled",
    "handoff_auto_ended",
    "handoff_auto_trigger_failed",
    "handoff_auto_triggered",
    "handoff_confirmation_confirmed",
    "handoff_confirmation_declined",
    "handoff_confirmation_requested",
    "handoff_intent_detected",
    "handoff_intent_ignored",
    "handoff_tool_ignored",
    "handoff_tool_requested",
    "handoff_prompt_cleanup_failed",
    "handoff_prompt_done",
    "handoff_prompt_failed",
    "handoff_prompt_started",
    "handoff_requested",
    "handoff_timeout_task_canceled",
    "handoff_timeout_task_started",
    "handoff_unavailable_prompt_done",
    "handoff_unavailable_prompt_failed",
    "handoff_unavailable_prompt_started",
    "handoff_waiting_tone_failed",
    "handoff_waiting_tone_started",
    "handoff_waiting_tone_stopped",
    "input_audio_cleared",
    "input_audio_committed",
    "interrupt_audio_stop_completed",
    "interrupt_audio_stop_requested",
    "interrupt_candidate",
    "interrupt_cleanup_failed",
    "interrupt_confirmed",
    "interrupt_ignored",
    "interrupt_pending",
    "media_connected",
    "model_audio_done",
    "model_error",
    "model_response_done",
    "model_response_started",
    "model_session_started",
    "model_session_updated",
    "no_barge_unstarted_response_deferred",
    "opening_started",
    "playout_queue_flushed",
    "provider_event_unmapped",
    "response_generation_invalidated",
    "room_created",
    "session_completed",
    "session_created",
    "session_failed",
    "session_preparing",
    "session_ready",
    "session_ending",
    "sip_answered",
    "sip_ai_playback_echo_deferred",
    "sip_failed",
    "sip_hangup",
    "sip_invite_sent",
    "sip_impulse_noise_ignored",
    "sip_interrupt_candidate",
    "sip_interrupt_candidate_confirmed",
    "sip_interrupt_candidate_expired",
    "sip_interrupt_confirmed",
    "sip_interrupt_rejected",
    "sip_preflight_failed",
    "sip_preflight_passed",
    "sip_pre_stop_deferred",
    "sip_pre_stop",
    "sip_provider_speech_started_deferred",
    "sip_recovery_started",
    "sip_ringing",
    "sip_vad_shadow_ended",
    "sip_vad_shadow_error",
    "sip_vad_shadow_started",
    "user_speech_started",
    "user_speech_stopped",
    "user_transcript_failed",
    "user_transcript_semantic_rejected",
    "stale_audio_dropped",
})


def _optional_text(value: Any) -> str | None:
    normalized = str(value).strip() if value is not None else ""
    return normalized or None


class AiCallRecordService:
    """B1 通话记录和关键事件持久化服务。"""

    def __init__(self, repository: AiCallRecordRepository) -> None:
        self.repository = repository

    async def create_web_record(
        self,
        *,
        tenant_id: str | None = None,
        call_id: str,
        business_id: str | None,
        room_name: str,
        participant_identity: str,
        started_at: datetime | None = None,
        business_type: str | None = None,
    ) -> AiCallRecordModel:
        return await self.repository.create_record(
            tenant_id=tenant_id,
            call_id=call_id,
            business_type=business_type,
            business_id=business_id,
            entry_type="web",
            room_name=room_name,
            participant_identity=participant_identity,
            status=CallSessionStatus.CREATED.value,
            started_at=started_at or utc_now(),
        )

    async def create_sip_record(
        self,
        *,
        tenant_id: str | None = None,
        call_id: str,
        business_id: str | None,
        room_name: str,
        participant_identity: str,
        started_at: datetime | None = None,
        business_type: str | None = None,
        callee_phone_number_hash: str | None = None,
        callee_phone_number_masked: str | None = None,
    ) -> AiCallRecordModel:
        return await self.repository.create_record(
            tenant_id=tenant_id,
            call_id=call_id,
            business_type=business_type,
            business_id=business_id,
            entry_type="sip_outbound",
            room_name=room_name,
            participant_identity=participant_identity,
            callee_phone_number_hash=callee_phone_number_hash,
            callee_phone_number_masked=callee_phone_number_masked,
            status=CallSessionStatus.CREATED.value,
            started_at=started_at or utc_now(),
        )

    async def get_active_sip_record_by_callee_hash(
        self,
        callee_phone_number_hash: str,
    ) -> AiCallRecordModel | None:
        active_statuses = {
            CallSessionStatus.CREATED.value,
            CallSessionStatus.PREPARING.value,
            CallSessionStatus.READY.value,
            CallSessionStatus.CONNECTED.value,
            CallSessionStatus.USER_SPEAKING.value,
            CallSessionStatus.AI_THINKING.value,
            CallSessionStatus.AI_SPEAKING.value,
            CallSessionStatus.INTERRUPTED.value,
            CallSessionStatus.WAITING.value,
            CallSessionStatus.ENDING.value,
        }
        return await self.repository.get_active_sip_record_by_callee_hash(
            callee_phone_number_hash=callee_phone_number_hash,
            active_statuses=active_statuses,
        )

    async def get_execution_config(
        self,
        record: AiCallRecordModel,
    ) -> dict[str, str | None] | None:
        if not record.business_id:
            return None
        try:
            business_id = int(record.business_id)
        except (TypeError, ValueError):
            return None
        if record.business_type == "outbound_task":
            snapshot_json = await self.repository.get_outbound_task_config_snapshot(
                business_id
            )
        elif record.business_type == "outbound_attempt":
            snapshot_json = await self.repository.get_outbound_attempt_task_config_snapshot(
                business_id,
                tenant_id=record.tenant_id,
            )
        else:
            return None
        try:
            snapshot = json.loads(snapshot_json or "")
            prompt = snapshot["prompt"]
            voice = snapshot["voice"]
            rule = snapshot["rule"]
            if not all(isinstance(item, dict) for item in (prompt, voice, rule)):
                return None
        except (KeyError, TypeError, ValueError):
            return None

        result = {
            "promptProfileId": _optional_text(prompt.get("id")),
            "promptName": _optional_text(prompt.get("name")),
            "sceneCode": _optional_text(prompt.get("sceneCode")),
            "voice": _optional_text(voice.get("voice")),
            "voiceName": _optional_text(
                voice.get("voiceName") or voice.get("displayName")
            ),
            "ruleName": _optional_text(rule.get("ruleName")),
        }
        return result if any(result.values()) else None

    async def mark_status(
        self,
        call_id: str,
        status: str | CallSessionStatus,
    ) -> AiCallRecordModel | None:
        return await self.repository.update_record(
            call_id,
            status=self._status_value(status),
        )

    async def update_prompt_context(
        self,
        call_id: str,
        *,
        scene_code: str | None,
        prompt_source_key: str | None,
    ) -> AiCallRecordModel | None:
        return await self.repository.update_record(
            call_id,
            scene_code=scene_code,
            prompt_source_key=prompt_source_key,
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

    async def mirror_runtime_events(self, events: list[AiCallEvent]) -> list[AiCallEventModel]:
        persisted: list[AiCallEventModel] = []
        persistable_events = [event for event in events if self.should_persist_event(event)]
        for call_id, call_events in self._group_events_by_call(persistable_events).items():
            existing_event_ids = await self.repository.list_existing_event_ids(
                call_id=call_id,
                event_ids=[event.event_id for event in call_events],
            )
            for event in call_events:
                if event.event_id in existing_event_ids:
                    continue
                persisted_event = await self.repository.append_event(
                    event_id=event.event_id,
                    call_id=event.call_id,
                    event_type=event.type,
                    source=event.source,
                    event_time=event.timestamp,
                    payload_json=self._payload_to_json(event.payload),
                )
                persisted.append(persisted_event)
                existing_event_ids.add(event.event_id)
        return persisted

    @staticmethod
    def should_persist_event(event: AiCallEvent) -> bool:
        return event.type in PERSISTED_EVENT_TYPES

    async def get_record(self, call_id: str) -> AiCallRecordModel | None:
        return await self.repository.get_record(call_id)

    async def get_record_for_tenant(
        self,
        *,
        tenant_id: str,
        call_id: str,
    ) -> AiCallRecordModel | None:
        return await self.repository.get_record_for_tenant(
            tenant_id=tenant_id,
            call_id=call_id,
        )

    async def mark_owner_customer_ready(self, *, tenant_id: str, call_id: str) -> bool:
        return await OwnerCustomerMediaRepository(self.repository.db).mark_browser_ready(
            tenant_id=tenant_id,
            call_id=call_id,
        )

    async def list_records(
        self,
        *,
        tenant_id: str | None = None,
        call_id: str | None = None,
        task_id: int | None = None,
        target_id: int | None = None,
        phone_number: str | None = None,
        customer_name: str | None = None,
        call_result: str | None = None,
        customer_intent: str | None = None,
        follow_up_status: str | None = None,
        business_type: str | None = None,
        business_id: str | None = None,
        status: str | None = None,
        entry_type: str | None = None,
        formal_outbound_only: bool = False,
        started_at_begin: datetime | None = None,
        started_at_end: datetime | None = None,
        page_num: int = 1,
        page_size: int = 10,
    ) -> tuple[list[AiCallRecordModel], int]:
        return await self.repository.list_records(
            tenant_id=tenant_id,
            call_id=call_id,
            task_id=task_id,
            target_id=target_id,
            phone_number=phone_number,
            customer_name=customer_name,
            call_result=call_result,
            customer_intent=customer_intent,
            follow_up_status=follow_up_status,
            business_type=business_type,
            business_id=business_id,
            status=status,
            entry_type=entry_type,
            formal_outbound_only=formal_outbound_only,
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
        outbound_context = getattr(record, "_outbound_context", {})
        semantic_context = getattr(record, "_semantic_analysis_context", {})
        follow_up_context = getattr(record, "_follow_up_context", {})
        quality_context = getattr(record, "_quality_context", {})
        semantic_summary = None
        semantic_analysis_result = getattr(record, "_semantic_analysis_result", None)
        if semantic_analysis_result:
            try:
                parsed_analysis = json.loads(semantic_analysis_result)
            except (TypeError, ValueError):
                parsed_analysis = None
            if isinstance(parsed_analysis, dict):
                raw_summary = parsed_analysis.get("summary")
                if isinstance(raw_summary, str) and raw_summary.strip():
                    semantic_summary = raw_summary.strip()
        return {
            "id": str(record.id),
            "callId": record.call_id,
            "taskId": outbound_context.get("taskId"),
            "targetId": outbound_context.get("targetId"),
            "taskName": outbound_context.get("taskName"),
            "customerName": outbound_context.get("customerName"),
            "phoneNumber": outbound_context.get("phoneNumber"),
            "attemptNo": outbound_context.get("attemptNo"),
            "callResult": outbound_context.get("callResult"),
            "summary": semantic_summary,
            "analysisStatus": semantic_context.get("analysisStatus"),
            "customerIntent": semantic_context.get("customerIntent"),
            "followUpSuggested": bool(
                semantic_context.get("followUpSuggested", False)
            ),
            "followUpId": follow_up_context.get("followUpId"),
            "followUpStatus": follow_up_context.get("followUpStatus"),
            "qualityScoreStatus": quality_context.get("qualityScoreStatus"),
            "qualityScore": quality_context.get("qualityScore"),
            "qualityReviewResult": quality_context.get("qualityReviewResult"),
            "businessType": record.business_type,
            "businessId": record.business_id,
            "sceneCode": record.scene_code,
            "promptSourceKey": record.prompt_source_key,
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
        if record.ended_at is not None:
            return record
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

    @staticmethod
    def _group_events_by_call(events: list[AiCallEvent]) -> dict[str, list[AiCallEvent]]:
        grouped: dict[str, list[AiCallEvent]] = {}
        for event in events:
            grouped.setdefault(event.call_id, []).append(event)
        return grouped

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
