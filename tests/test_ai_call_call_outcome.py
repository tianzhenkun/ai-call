import json
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, literal, select
from sqlalchemy.orm import Session

from app.api.v1.ai_call.call_outcome_query import voicemail_expression
from app.api.v1.ai_call.model import AiCallRecordModel, AiCallSemanticAnalysisModel
from app.api.v1.ai_call.schema import RecordOut
from app.services.ai_call.call_outcome import detect_answer_type, is_voicemail_analysis
from app.services.ai_call.record_service import AiCallRecordService


@pytest.mark.parametrize("analysis_result", [
    {"valid_dialogue": False, "summary": "进入语音信箱"},
    {"valid_dialogue": False, "reason": "提示音后录制留言"},
    {"valid_dialogue": False, "tags": ["语音留言"]},
    {"valid_dialogue": False, "key_points": ["录音完成后挂断"]},
    {"valid_dialogue": True, "tags": ["语音留言"]},
    {"valid_dialogue": False, "evidence": ["客户说：上次打到了我的语音信箱"]},
    {"valid_dialogue": False, "summary": "未形成有效业务对话", "nested": {"summary": "语音信箱"}},
])
def test_sql_and_record_voicemail_detection_use_same_fields(analysis_result):
    engine = create_engine("sqlite://")
    AiCallSemanticAnalysisModel.__table__.create(engine)
    try:
        with Session(engine) as db:
            db.add(AiCallSemanticAnalysisModel(
                id=1, call_id="call-voicemail-parity",
                analysis_scene_code="ai_call_semantic_analysis", analysis_status="2",
                analysis_result=json.dumps(analysis_result, ensure_ascii=False, indent=2),
                created_at=datetime.now(timezone.utc), updated_at=datetime.now(timezone.utc),
            ))
            db.flush()
            assert db.scalar(select(voicemail_expression(literal("call-voicemail-parity")))) == is_voicemail_analysis(analysis_result)
    finally:
        engine.dispose()


@pytest.mark.parametrize("analysis_result,expected", [
    ({"valid_dialogue": True}, "connected"),
    ({"valid_dialogue": False, "classification": "low_value"}, "connected"),
    ({"valid_dialogue": False, "tags": ["语音留言"]}, "no_answer"),
])
def test_record_business_result_separates_voicemail_from_connected(analysis_result, expected) -> None:
    record = AiCallRecordModel(
        id=1, tenant_id="000000", call_id="call-outcome", entry_type="sip_outbound",
        status="completed", started_at=datetime.now(timezone.utc),
    )
    record._outbound_context = {"callResult": "connected"}
    record._semantic_analysis_context = {"analysisStatus": "2"}
    record._semantic_analysis_result = json.dumps(analysis_result, ensure_ascii=False)
    response = AiCallRecordService(None).record_to_dict(record)
    assert response["callResult"] == expected
    assert record._outbound_context["callResult"] == "connected"


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
