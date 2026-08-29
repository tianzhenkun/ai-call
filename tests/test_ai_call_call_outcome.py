from app.api.v1.ai_call.schema import RecordOut
from app.services.ai_call.call_outcome import detect_answer_type


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
