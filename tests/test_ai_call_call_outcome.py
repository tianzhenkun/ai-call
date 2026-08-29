import json
from datetime import datetime, timezone

from app.api.v1.ai_call.model import AiCallRecordModel
from app.api.v1.ai_call.schema import RecordOut
from app.services.ai_call.call_outcome import detect_answer_type
from app.services.ai_call.record_service import AiCallRecordService


def test_detect_answer_type_requires_real_dialogue_and_separates_voicemail() -> None:
    assert detect_answer_type(
        call_result="connected",
        analysis_status="2",
        analysis_result={"valid_dialogue": True},
    ) == "human"
    assert detect_answer_type(
        call_result="connected",
        analysis_status="2",
        analysis_result={"valid_dialogue": False, "tags": ["语音留言"]},
    ) == "voicemail"
    assert detect_answer_type(
        call_result="connected",
        analysis_status="2",
        analysis_result={"valid_dialogue": False},
    ) == "transport"
    assert detect_answer_type(
        call_result="connected",
        analysis_status="0",
        analysis_result=None,
    ) == "transport"
    assert detect_answer_type(
        call_result="no_answer",
        analysis_status=None,
        analysis_result=None,
    ) is None


def test_record_response_keeps_answer_type() -> None:
    response = RecordOut.model_validate(
        {
            "id": "record-1",
            "callId": "call-1",
            "entryType": "sip_outbound",
            "status": "completed",
            "startedAt": "2026-08-29T09:08:17+08:00",
            "callResult": "connected",
            "answerType": "voicemail",
        }
    ).model_dump(by_alias=True)

    assert response["answerType"] == "voicemail"


def test_record_list_defaults_connected_without_analysis_to_transport() -> None:
    record = AiCallRecordModel(
        id=1,
        tenant_id="000000",
        call_id="call-no-analysis",
        entry_type="sip_outbound",
        status="completed",
        room_name="room-no-analysis",
        participant_identity="sip-no-analysis",
        started_at=datetime.now(timezone.utc),
    )
    record._outbound_context = {"callResult": "connected"}

    response = AiCallRecordService(None).record_to_dict(record)  # type: ignore[arg-type]

    assert response["answerType"] == "transport"


def test_record_list_removes_internal_evidence_from_summary() -> None:
    record = AiCallRecordModel(
        id=2,
        tenant_id="000000",
        call_id="call-internal-summary",
        entry_type="sip_outbound",
        status="completed",
        room_name="room-internal-summary",
        participant_identity="sip-internal-summary",
        started_at=datetime.now(timezone.utc),
    )
    record._outbound_context = {"callResult": "connected"}
    record._semantic_analysis_result = json.dumps(
        {"summary": "客户希望了解服务。（semantic_evidence.analysis_usage=record_only）"}
    )

    response = AiCallRecordService(None).record_to_dict(record)  # type: ignore[arg-type]

    assert response["summary"] == "客户希望了解服务。"
