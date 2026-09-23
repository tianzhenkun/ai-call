from __future__ import annotations

import json
from typing import Literal

from app.api.v1.ai_call.model import AiCallRecordModel
from app.services.ai_call.runtime_control.models import AiCallEndEvidenceModel

EndCategory = Literal["agent", "customer", "system_normal", "system_error", "unknown"]

_REASON_CATEGORIES: dict[str, EndCategory] = {
    **dict.fromkeys(("agent_completed", "callback_ended_by_agent"), "agent"),
    **dict.fromkeys(("sip_client_initiated", "web_user_end"), "customer"),
    **dict.fromkeys(
        (
            "customer_end",
            "ai_completed",
            "normal_completed",
            "policy_limit",
            "policy_turn_limit",
            "policy_duration_limit",
            "policy_no_response",
            "handoff_timeout",
            "no_online_agent",
            "handoff_cancel_after_connected",
        ),
        "system_normal",
    ),
    **dict.fromkeys(
        (
            "agent_error",
            "agent_start_failed",
            "model_error",
            "audio_transport_error",
            "provider_transport_error",
            "runtime_failed",
            "runtime_shutdown",
            "orchestrator_shutdown",
            "owner_lost",
            "runtime_task_failed",
            "runtime_task_cancelled",
            "runtime_task_exited",
            "reconnect_timeout",
            "browser_connection_failed",
            "browser_ready_timeout",
            "handoff_failed",
            "handoff_service_unavailable",
            "callback_technical_failure",
            "sip_transport_error",
        ),
        "system_error",
    ),
}


def sip_disconnect_end_reason(payload: dict | None) -> str:
    """供已验证客户 SIP 身份的 Webhook 入口记录具体原因。"""
    payload = payload or {}
    participant = payload.get("participant")
    participant = participant if isinstance(participant, dict) else {}
    disconnect_reason = (
        participant.get("disconnectReason")
        or participant.get("disconnect_reason")
        or payload.get("disconnectReason")
        or payload.get("disconnect_reason")
    )
    if not isinstance(disconnect_reason, str):
        return "sip_participant_left"
    if disconnect_reason == "CLIENT_INITIATED":
        return "sip_client_initiated"
    if disconnect_reason in {
        "MEDIA_FAILURE",
        "CONNECTION_TIMEOUT",
        "SERVER_SHUTDOWN",
        "SIP_TRUNK_FAILURE",
    }:
        return "sip_transport_error"
    return "sip_participant_left"


def call_end_category(
    record: AiCallRecordModel,
    first_evidence: AiCallEndEvidenceModel | None = None,
) -> EndCategory | None:
    """保留首次终止原因，只用同一客户参与者的断开证据细化笼统原因。"""
    reason = (record.end_reason or "").strip()
    category = _REASON_CATEGORIES.get(reason)
    if category is not None:
        return category
    if not reason and record.status not in {"completed", "failed", "ending"}:
        return None
    if (
        reason not in {"sip_participant_left", "browser_disconnect"}
        or first_evidence is None
        or first_evidence.source != "livekit_webhook"
        or first_evidence.end_reason != reason
        or first_evidence.tenant_id != record.tenant_id
        or first_evidence.call_id != record.call_id
    ):
        return "unknown"
    try:
        payload = json.loads(first_evidence.evidence_json or "null")
    except (TypeError, ValueError):
        return "unknown"
    if not isinstance(payload, dict):
        return "unknown"
    participant = payload.get("participant")
    room = payload.get("room")
    if (
        payload.get("event") != "participant_left"
        or not isinstance(participant, dict)
        or not isinstance(room, dict)
        or not record.participant_identity
        or not record.room_name
        or participant.get("identity") != record.participant_identity
        or room.get("name") != record.room_name
    ):
        return "unknown"
    specific_reason = sip_disconnect_end_reason(payload)
    if specific_reason == "sip_transport_error":
        return "system_error"
    if specific_reason == "sip_client_initiated" and (
        record.entry_type == "web"
        or (participant.get("kind") == "SIP" and record.answered_at is not None)
    ):
        # 只能确认客户侧线路主动断开，不能据此区分手机按键与网关结束。
        return "customer"
    return "unknown"
