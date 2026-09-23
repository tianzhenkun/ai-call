import json
from datetime import datetime, timezone

import pytest

from app.api.v1.ai_call.model import AiCallRecordModel
from app.services.ai_call.call_termination import call_end_category
from app.services.ai_call.runtime_control.models import AiCallEndEvidenceModel


@pytest.mark.parametrize(
    "invalid_part",
    ["room", "participant", "kind", "tenant", "call", "source", "json", "disconnect"],
)
def test_customer_attribution_requires_matching_valid_evidence(invalid_part):
    record = AiCallRecordModel(
        tenant_id="tenant-a",
        call_id="call-1",
        room_name="room-1",
        participant_identity="customer-1",
        status="completed",
        entry_type="direct_sip",
        answered_at=datetime.now(timezone.utc),
        end_reason="sip_participant_left",
    )
    payload = {
        "event": "participant_left",
        "room": {"name": "room-1"},
        "participant": {
            "identity": "customer-1",
            "kind": "SIP",
            "disconnectReason": "CLIENT_INITIATED",
        },
    }
    if invalid_part == "room":
        payload["room"]["name"] = "other-room"
    if invalid_part == "participant":
        payload["participant"]["identity"] = "human-agent-1"
    if invalid_part == "kind":
        payload["participant"]["kind"] = "STANDARD"
    if invalid_part == "disconnect":
        payload["participant"]["disconnectReason"] = []
    evidence = AiCallEndEvidenceModel(
        tenant_id="other" if invalid_part == "tenant" else "tenant-a",
        call_id="other" if invalid_part == "call" else "call-1",
        source="other" if invalid_part == "source" else "livekit_webhook",
        end_reason="sip_participant_left",
        evidence_json="invalid-json" if invalid_part == "json" else json.dumps(payload),
    )
    assert call_end_category(record, evidence) == "unknown"
