from __future__ import annotations

import importlib
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.controller import AiCallRouter, get_ai_call_service
from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import AiCallDialogueSegmentModel, AiCallHandoffModel
from app.core.base_model import MappedBase
from app.core.dependencies import get_current_user
from app.services.ai_call.record_service import AiCallRecordService


def _semantic_module():
    try:
        return importlib.import_module("app.services.ai_call.semantic_analysis")
    except ModuleNotFoundError as exc:
        pytest.fail(f"semantic analysis module missing: {exc}")


def _segment(
    *,
    segment_no: int,
    speaker_type: str,
    text: str,
    source: str = "qwen_realtime",
    started_offset_seconds: float = 0,
    ended_offset_seconds: float | None = None,
) -> AiCallDialogueSegmentModel:
    started_at = datetime(2026, 7, 8, 10, 0, tzinfo=timezone.utc) + timedelta(
        seconds=started_offset_seconds,
    )
    ended_at = started_at + timedelta(seconds=1 if ended_offset_seconds is None else 0)
    if ended_offset_seconds is not None:
        ended_at = datetime(2026, 7, 8, 10, 0, tzinfo=timezone.utc) + timedelta(
            seconds=ended_offset_seconds,
        )
    return AiCallDialogueSegmentModel(
        id=segment_no,
        call_id="call_semantic_evidence",
        segment_no=segment_no,
        speaker_type=speaker_type,
        speaker_identity="browser-call" if speaker_type == "customer" else "agent-call",
        source=source,
        source_segment_id=f"{source}_{segment_no}",
        segment_text=text,
        segment_status="final",
        started_at=started_at,
        ended_at=ended_at,
        duration_ms=int((ended_at - started_at).total_seconds() * 1000),
    )


def _build_snapshot(rows: list[AiCallDialogueSegmentModel]) -> dict:
    module = _semantic_module()
    return module.SemanticTranscriptBuilder().build(
        call_id="call_semantic_evidence",
        scene_code="intro_geo",
        rows=rows,
    )


def _handoff(
    *,
    status: str = "requested",
    handoff_id: str = "handoff_semantic_1",
    requested_offset_seconds: float = 10,
    accepted_offset_seconds: float | None = None,
    connected_offset_seconds: float | None = None,
    ended_offset_seconds: float | None = None,
    human_agent_identity: str | None = None,
) -> AiCallHandoffModel:
    base = datetime(2026, 7, 8, 10, 0, tzinfo=timezone.utc)

    def at(offset: float | None) -> datetime | None:
        if offset is None:
            return None
        return base + timedelta(seconds=offset)

    return AiCallHandoffModel(
        id=9001,
        handoff_id=handoff_id,
        call_id="call_semantic_evidence",
        room_name="room_semantic_evidence",
        status=status,
        request_source="customer",
        request_reason="customer_request",
        request_message="用户明确要求转人工",
        human_agent_identity=human_agent_identity,
        requested_at=at(requested_offset_seconds),
        accepted_at=at(accepted_offset_seconds),
        connected_at=at(connected_offset_seconds),
        ended_at=at(ended_offset_seconds),
        expires_at=at(requested_offset_seconds + 120),
    )


def _user_turns(snapshot: dict) -> list[dict]:
    return [turn for turn in snapshot["turns"] if turn["role"] == "user"]


def test_semantic_analysis_model_is_independent_table_without_relationships() -> None:
    assert "ai_call_semantic_analysis" in MappedBase.metadata.tables

    from app.api.v1.ai_call.model import AiCallSemanticAnalysisModel

    assert not AiCallSemanticAnalysisModel.__table__.foreign_keys
    assert not sa_inspect(AiCallSemanticAnalysisModel).relationships


def test_semantic_analysis_prompt_ignores_assistant_customer_style_text() -> None:
    module = _semantic_module()

    assert "只把 role=user" in module.SEMANTIC_ANALYSIS_SYSTEM_PROMPT
    assert "assistant" in module.SEMANTIC_ANALYSIS_SYSTEM_PROMPT
    assert "即使 assistant 轮次使用客户口吻" in module.SEMANTIC_ANALYSIS_SYSTEM_PROMPT


def test_semantic_analysis_prompt_handles_uncertain_transcripts_conservatively() -> None:
    module = _semantic_module()

    assert "transcript_quality" in module.SEMANTIC_ANALYSIS_SYSTEM_PROMPT
    assert "低置信" in module.SEMANTIC_ANALYSIS_SYSTEM_PROMPT
    assert "不得基于孤立、冲突或单路 ASR 片段输出强业务结论" in (
        module.SEMANTIC_ANALYSIS_SYSTEM_PROMPT
    )


def test_semantic_analysis_prompt_requires_turn_evidence_for_strong_facts() -> None:
    module = _semantic_module()

    assert "semantic_evidence" in module.SEMANTIC_ANALYSIS_SYSTEM_PROMPT
    assert "supports_strong_fact" in module.SEMANTIC_ANALYSIS_SYSTEM_PROMPT
    assert "unsupported_strong_fact" in module.SEMANTIC_ANALYSIS_SYSTEM_PROMPT
    assert "analysis_usage=record_only" in module.SEMANTIC_ANALYSIS_SYSTEM_PROMPT


def test_semantic_snapshot_includes_handoff_metadata_without_human_turns() -> None:
    module = _semantic_module()
    snapshot = module.SemanticTranscriptBuilder().build(
        call_id="call_semantic_evidence",
        scene_code="intro_geo",
        rows=[
            _segment(
                segment_no=1,
                speaker_type="ai",
                text="您想了解哪个产品方向？",
                started_offset_seconds=0,
                ended_offset_seconds=3,
            ),
            _segment(
                segment_no=2,
                speaker_type="customer",
                text="帮我转人工。",
                started_offset_seconds=8,
                ended_offset_seconds=9,
            ),
        ],
        handoffs=[_handoff(status="requested")],
    )

    assert snapshot["handoffs"] == [
        {
            "handoff_id": "handoff_semantic_1",
            "status": "requested",
            "request_source": "customer",
            "request_reason": "customer_request",
            "request_message": "用户明确要求转人工",
            "human_agent_identity": None,
            "requested_at": "2026-07-08T10:00:10+00:00",
            "accepted_at": None,
            "connected_at": None,
            "ended_at": None,
            "expires_at": "2026-07-08T10:02:10+00:00",
            "end_reason": None,
            "failure_stage": None,
            "failure_message": None,
        }
    ]
    assert all(turn["speaker_type"] != "human_agent" for turn in snapshot["turns"])
    assert snapshot["metadata"]["handoff_summary"] == {
        "has_handoff": True,
        "has_connected_handoff": False,
        "human_turn_count": 0,
        "human_agent_turn_count": 0,
        "human_transcript_status": "not_applicable",
    }


def test_semantic_snapshot_keeps_human_agent_turn_as_assistant_context() -> None:
    module = _semantic_module()
    snapshot = module.SemanticTranscriptBuilder().build(
        call_id="call_semantic_evidence",
        scene_code="intro_geo",
        rows=[
            _segment(
                segment_no=1,
                speaker_type="customer",
                text="我想找人工顾问。",
                started_offset_seconds=8,
                ended_offset_seconds=9,
            ),
            _segment(
                segment_no=2,
                speaker_type="human_agent",
                text="您好，我是人工顾问，我来继续沟通。",
                source="offline_asr",
                started_offset_seconds=35,
                ended_offset_seconds=38,
            ),
            _segment(
                segment_no=3,
                speaker_type="customer",
                text="我想确认你们能不能做合同审查。",
                source="offline_asr",
                started_offset_seconds=40,
                ended_offset_seconds=43,
            ),
        ],
        handoffs=[
            _handoff(
                status="connected",
                accepted_offset_seconds=20,
                connected_offset_seconds=30,
                human_agent_identity="agent-debug-001",
            )
        ],
    )

    human_turns = [
        turn for turn in snapshot["turns"] if turn["speaker_type"] == "human_agent"
    ]
    assert human_turns == [
        {
            "seq": 2,
            "role": "assistant",
            "speaker_type": "human_agent",
            "speaker_identity": "agent-call",
            "handoff_id": "handoff_semantic_1",
            "text": "您好，我是人工顾问，我来继续沟通。",
            "source": "offline_asr",
            "segment_status": "final",
            "started_at": "2026-07-08T10:00:35+00:00",
            "ended_at": "2026-07-08T10:00:38+00:00",
        }
    ]
    assert "semantic_evidence" not in human_turns[0]
    assert _user_turns(snapshot)[-1]["handoff_id"] == "handoff_semantic_1"
    assert snapshot["metadata"]["handoff_summary"] == {
        "has_handoff": True,
        "has_connected_handoff": True,
        "human_turn_count": 2,
        "human_agent_turn_count": 1,
        "human_transcript_status": "available",
    }


def test_semantic_snapshot_marks_human_agent_track_crosstalk_as_low_confidence() -> None:
    module = _semantic_module()
    now = datetime(2026, 7, 8, 10, 0, tzinfo=timezone.utc)
    snapshot = module.SemanticTranscriptBuilder().build(
        call_id="call_handoff_crosstalk",
        scene_code="intro_geo",
        rows=[
            AiCallDialogueSegmentModel(
                id=1,
                call_id="call_handoff_crosstalk",
                segment_no=1,
                speaker_type="customer",
                speaker_identity="browser-call",
                source="offline_asr",
                source_segment_id="offline_customer_room_audio",
                segment_text=(
                    "你把门关上，我不知道你听的话是我电脑里边的说的话，"
                    "还是我客厅说的话呀。"
                ),
                segment_status="final",
                started_at=now + timedelta(seconds=54),
                ended_at=now + timedelta(seconds=64),
                duration_ms=10000,
            ),
            AiCallDialogueSegmentModel(
                id=2,
                call_id="call_handoff_crosstalk",
                segment_no=2,
                speaker_type="human_agent",
                speaker_identity="human-agent-handoff-1",
                source="offline_asr",
                source_segment_id="offline_human_agent_why",
                segment_text="I not why?",
                segment_status="final",
                started_at=now + timedelta(seconds=55),
                ended_at=now + timedelta(seconds=56),
                duration_ms=1000,
            ),
        ],
        handoffs=[
            _handoff(
                handoff_id="handoff_crosstalk_1",
                status="connected",
                requested_offset_seconds=10,
                accepted_offset_seconds=20,
                connected_offset_seconds=30,
                ended_offset_seconds=80,
                human_agent_identity="agent-debug-001",
            )
        ],
    )

    human_turn = next(
        turn for turn in snapshot["turns"] if turn["speaker_type"] == "human_agent"
    )
    assert "semantic_evidence" not in human_turn
    assert human_turn["transcript_quality"] == {
        "low_confidence_source": True,
        "reason_codes": ["human_agent_track_customer_overlap"],
        "overlap_speaker_type": "customer",
        "overlap_source": "offline_asr",
        "overlap_text": (
            "你把门关上，我不知道你听的话是我电脑里边的说的话，"
            "还是我客厅说的话呀。"
        ),
    }

    quality = snapshot["metadata"]["transcript_quality"]
    assert quality["has_uncertain_transcript"] is True
    assert "human_agent_track_crosstalk" in quality["signals"]
    assert "human_agent_track_customer_overlap" in quality["reasons"]
    assert snapshot["metadata"]["human_agent_track_crosstalk_count"] == 1


def test_semantic_snapshot_keeps_normal_human_agent_overlap_trusted() -> None:
    module = _semantic_module()
    now = datetime(2026, 7, 8, 10, 0, tzinfo=timezone.utc)
    snapshot = module.SemanticTranscriptBuilder().build(
        call_id="call_handoff_normal_overlap",
        scene_code="intro_geo",
        rows=[
            AiCallDialogueSegmentModel(
                id=1,
                call_id="call_handoff_normal_overlap",
                segment_no=1,
                speaker_type="customer",
                speaker_identity="browser-call",
                source="offline_asr",
                source_segment_id="offline_customer_overlap",
                segment_text="好的，我继续说一下需求。",
                segment_status="final",
                started_at=now + timedelta(seconds=54),
                ended_at=now + timedelta(seconds=57),
                duration_ms=3000,
            ),
            AiCallDialogueSegmentModel(
                id=2,
                call_id="call_handoff_normal_overlap",
                segment_no=2,
                speaker_type="human_agent",
                speaker_identity="human-agent-handoff-1",
                source="offline_asr",
                source_segment_id="offline_human_agent_normal",
                segment_text="我这边能听到，稍等我帮您看一下。",
                segment_status="final",
                started_at=now + timedelta(seconds=54),
                ended_at=now + timedelta(seconds=57),
                duration_ms=3000,
            ),
        ],
        handoffs=[
            _handoff(
                handoff_id="handoff_normal_overlap_1",
                status="connected",
                requested_offset_seconds=10,
                accepted_offset_seconds=20,
                connected_offset_seconds=30,
                ended_offset_seconds=80,
                human_agent_identity="agent-debug-001",
            )
        ],
    )

    human_turn = next(
        turn for turn in snapshot["turns"] if turn["speaker_type"] == "human_agent"
    )
    assert "transcript_quality" not in human_turn
    quality = snapshot["metadata"]["transcript_quality"]
    assert "human_agent_track_crosstalk" not in quality["signals"]
    assert snapshot["metadata"]["human_agent_track_crosstalk_count"] == 0


def test_semantic_analysis_result_tags_human_agent_crosstalk_risk() -> None:
    module = _semantic_module()
    result = module.enforce_semantic_evidence_on_result(
        module.normalize_analysis_result({
            "summary": "客户要求转人工，转人工后对收音来源表示困惑。",
            "feedback_type": "负向",
            "key_points": ["客户要求转人工"],
            "time_hint": {},
            "tags": [],
        }),
        {
            "turns": [],
            "metadata": {
                "transcript_quality": {
                    "has_uncertain_transcript": True,
                    "signals": ["human_agent_track_crosstalk"],
                    "reasons": ["human_agent_track_customer_overlap"],
                }
            },
        },
    )

    assert "转写噪声风险" in result["tags"]


def test_semantic_analysis_result_removes_internal_evidence_annotations() -> None:
    module = _semantic_module()

    result = module.enforce_semantic_evidence_on_result(
        module.normalize_analysis_result({
            "summary": (
                "客户确认当前方便沟通"
                "（role=user, seq=3, supports_strong_fact=true, "
                "supported_strong_fact_types=['commitment']）。"
                "后续片段semantic_evidence.analysis_usage=record_only且无supports_strong_fact，置信度低；"
                "客户结束通话。"
            ),
            "feedback_type": "正向",
            "key_points": [
                (
                    "客户表示对AI搜索中的品牌呈现有关注"
                    "（role=user, seq=5, supports_strong_fact=true, "
                    "supported_strong_fact_types=['requirement_conclusion']）"
                ),
                "客户主动询问‘你们有试用吗？’（new_question_or_intent=true）",
                "客户明确要求转人工（handoff request）",
                (
                    "人工接入后存在转写噪声"
                    "（transcript_quality.has_uncertain_transcript=true, "
                    "human_agent_track_crosstalk_count=2）"
                ),
                "后续重复‘三个吧’但semantic_evidence.analysis_usage=record_only且无supports_strong_fact",
                "客户承诺由秘书提供资料（多轮出现'supports_commitment_fact'，但未明确资料内容）",
            ],
            "time_hint": {},
            "tags": [],
        }),
        {"turns": []},
    )

    rendered = json.dumps(result, ensure_ascii=False)
    assert "role=user" not in rendered
    assert "supports_strong_fact" not in rendered
    assert "commitment" not in rendered
    assert "requirement_conclusion" not in rendered
    assert "new_question_or_intent" not in rendered
    assert "supports_commitment_fact" not in rendered
    assert "handoff request" not in rendered
    assert "transcript_quality" not in rendered
    assert "human_agent_track_crosstalk_count" not in rendered
    assert result["key_points"] == [
        "客户表示对AI搜索中的品牌呈现有关注",
        "客户主动询问‘你们有试用吗？’",
        "客户明确要求转人工",
        "人工接入后存在转写噪声",
        "客户承诺由秘书提供资料",
    ]
    assert result["summary"] == "客户确认当前方便沟通。客户结束通话。"


def test_semantic_analysis_result_removes_record_only_time_hint() -> None:
    module = _semantic_module()

    result = module.enforce_semantic_evidence_on_result(
        module.normalize_analysis_result({
            "summary": (
                "客户希望继续了解GEO产品。"
                "客户说‘等我下午叫你去’，可下午继续沟通。"
            ),
            "feedback_type": "中性",
            "key_points": [
                "客户希望继续了解GEO产品",
                "客户下午可继续沟通",
            ],
            "time_hint": {
                "time_text": "下午",
                "time_value": "2026-07-10T14:00:00",
                "original_texts": ["等我下午叫你去"],
            },
            "tags": [],
        }),
        {
            "turns": [
                {
                    "role": "user",
                    "text": "啊，你等会儿再去买菜吧，等我下午叫你去，你再去旅馆。",
                    "semantic_evidence": {
                        "analysis_usage": "record_only",
                        "key_point_candidate": False,
                        "supports_strong_fact": True,
                        "supported_strong_fact_types": ["time", "commitment"],
                    },
                }
            ]
        },
    )

    rendered = json.dumps(result, ensure_ascii=False)
    assert "下午叫你去" not in rendered
    assert result["summary"] == "客户希望继续了解GEO产品。"
    assert result["key_points"] == ["客户希望继续了解GEO产品"]
    assert result["time_hint"] == {
        "time_text": "",
        "time_value": "",
        "original_texts": [],
    }
    assert "转写噪声风险" in result["tags"]


def test_semantic_analysis_result_removes_metadata_timestamp_time_hint() -> None:
    module = _semantic_module()

    result = module.enforce_semantic_evidence_on_result(
        module.normalize_analysis_result({
            "summary": "客户询问产品可审核的合同类型。",
            "feedback_type": "中性",
            "key_points": ["客户询问产品可审核的合同类型"],
            "time_hint": {
                "time_text": "2026-07-08",
                "time_value": "2026-07-08T00:51:07.813907Z",
                "original_texts": [
                    "2026-07-08T00:51:07.813907",
                    "2026-07-08T00:52:55.092192",
                ],
            },
            "tags": [],
        }),
        {
            "turns": [
                {
                    "role": "user",
                    "text": "你们可以审核什么样的合同？",
                    "started_at": "2026-07-08T00:51:07.813907",
                    "ended_at": "2026-07-08T00:51:17.180307",
                    "semantic_evidence": {
                        "analysis_usage": "use_as_customer_signal",
                        "key_point_candidate": True,
                    },
                }
            ]
        },
    )

    assert result["time_hint"] == {
        "time_text": "",
        "time_value": "",
        "original_texts": [],
    }
    assert "转写噪声风险" in result["tags"]


def test_semantic_analysis_result_removes_assistant_only_claims() -> None:
    module = _semantic_module()

    result = module.enforce_semantic_evidence_on_result(
        module.normalize_analysis_result({
            "summary": (
                "客户关注准确率。"
                "客户合同类型为合作协议，且多为短期合作。"
                "客户同意后续安排演示。"
            ),
            "feedback_type": "正向",
            "key_points": [
                "合同类型主要是短期合作协议",
                "关注合同智能审查产品的准确率",
                "同意安排小范围演示或样本测试",
            ],
            "time_hint": {},
            "tags": ["短期合作协议", "关注准确率"],
        }),
        {
            "turns": [
                {
                    "seq": 1,
                    "role": "user",
                    "text": "嗯，你们的准确率怎么样啊，我合作协议吧。",
                    "semantic_evidence": {
                        "analysis_usage": "use_as_customer_signal",
                        "key_point_candidate": True,
                    },
                },
                {
                    "seq": 2,
                    "role": "assistant",
                    "text": (
                        "了解，主要是短期合作协议。"
                        "您平时审一份这样的合同大概需要多久呢？"
                    ),
                },
                {
                    "seq": 3,
                    "role": "user",
                    "text": "可以安排演示。",
                    "semantic_evidence": {
                        "analysis_usage": "use_as_customer_signal",
                        "key_point_candidate": True,
                    },
                },
            ]
        },
    )

    rendered = json.dumps(result, ensure_ascii=False)
    assert "短期合作协议" not in rendered
    assert "短期合作" not in rendered
    assert "客户关注准确率" in result["summary"]
    assert "客户同意后续安排演示" in result["summary"]
    assert result["key_points"] == [
        "关注合同智能审查产品的准确率",
        "同意安排小范围演示或样本测试",
    ]
    assert result["tags"] == ["关注准确率", "转写噪声风险"]


def test_semantic_analysis_result_removes_assistant_only_time_hint() -> None:
    module = _semantic_module()

    result = module.enforce_semantic_evidence_on_result(
        module.normalize_analysis_result({
            "summary": "客户询问试用时间。",
            "feedback_type": "中性",
            "key_points": ["客户询问试用时间"],
            "time_hint": {
                "time_text": "星期10",
                "time_value": "",
                "original_texts": ["星期10"],
            },
            "tags": ["低置信时间表述"],
        }),
        {
            "turns": [
                {
                    "seq": 1,
                    "role": "user",
                    "text": "什么时候呢？",
                    "semantic_evidence": {
                        "analysis_usage": "use_as_customer_signal",
                        "key_point_candidate": True,
                    },
                },
                {
                    "seq": 2,
                    "role": "assistant",
                    "text": "星期10。",
                },
            ]
        },
    )

    assert result["time_hint"] == {
        "time_text": "",
        "time_value": "",
        "original_texts": [],
    }
    assert "转写噪声风险" in result["tags"]


def test_semantic_analysis_result_removes_record_only_text_from_summary_and_key_points() -> None:
    module = _semantic_module()
    snapshot = _build_snapshot([
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="您是想先看一个通用场景的演示，还是有特定的合同类型想重点了解？",
            started_offset_seconds=0,
            ended_offset_seconds=3,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            text="我强把我的核桃上了，上去看一下效果。",
            source="offline_asr",
            started_offset_seconds=4,
            ended_offset_seconds=6,
        ),
        _segment(
            segment_no=3,
            speaker_type="customer",
            text="你们有测试平台吗？",
            source="qwen_realtime",
            started_offset_seconds=7,
            ended_offset_seconds=8,
        ),
    ])

    result = module.enforce_semantic_evidence_on_result(
        module.normalize_analysis_result({
            "summary": (
                "客户询问是否有测试平台。"
                "客户提出‘把我的核桃上了，上去看一下效果’，"
                "疑似意图上传自有合同试用。"
            ),
            "feedback_type": "中性",
            "key_points": [
                "客户询问是否有测试平台",
                "客户提出‘我强把我的核桃上了，上去看一下效果’，疑似意图上传自有合同试用",
            ],
            "time_hint": {},
            "tags": [],
        }),
        snapshot,
    )

    rendered = json.dumps(result, ensure_ascii=False)
    assert "核桃" not in rendered
    assert "上传自有合同试用" not in rendered
    assert result["summary"] == "客户询问是否有测试平台。"
    assert result["key_points"] == ["客户询问是否有测试平台"]
    assert "转写噪声风险" in result["tags"]


def test_semantic_analysis_result_removes_transcript_listing_sentence_from_summary() -> None:
    module = _semantic_module()

    result = module.enforce_semantic_evidence_on_result(
        module.normalize_analysis_result({
            "summary": (
                "’‘你没有demo吗？’‘可以试用吗？’‘有试用版吗？’等表达参与对话。"
                "客户明确指出当前合同审核痛点是‘都是风险’，并关注试用安排。"
            ),
            "feedback_type": "正向",
            "key_points": ["客户关注试用安排"],
            "time_hint": {},
            "tags": [],
        }),
        {"turns": []},
    )

    assert result["summary"] == "客户明确指出当前合同审核痛点是‘都是风险’，并关注试用安排。"
    assert result["key_points"] == ["客户关注试用安排"]


def test_semantic_analysis_result_strips_leading_dangling_summary_punctuation() -> None:
    module = _semantic_module()

    result = module.enforce_semantic_evidence_on_result(
        module.normalize_analysis_result({
            "summary": "’，表现出对AI话术的误解与抵触；随后关注识别准确率。",
            "feedback_type": "负向",
            "key_points": ["客户关注识别准确率"],
            "time_hint": {},
            "tags": [],
        }),
        {"turns": []},
    )

    assert result["summary"] == "客户表现出对AI话术的误解与抵触；随后关注识别准确率。"


def test_semantic_analysis_result_strips_unmatched_closing_parenthesis_prefix() -> None:
    module = _semantic_module()

    result = module.enforce_semantic_evidence_on_result(
        module.normalize_analysis_result({
            "summary": "）。客户主动提出转人工，随后对话偏离业务主题。",
            "feedback_type": "中性",
            "key_points": [],
            "time_hint": {},
            "tags": [],
        }),
        {"turns": []},
    )

    assert result["summary"] == "客户主动提出转人工，随后对话偏离业务主题。"


def test_semantic_analysis_result_adds_customer_subject_after_prefix_cleanup() -> None:
    module = _semantic_module()

    result = module.enforce_semantic_evidence_on_result(
        module.normalize_analysis_result({
            "summary": "'，表现出对AI预设话术的不信任。",
            "feedback_type": "中性",
            "key_points": [],
            "time_hint": {},
            "tags": [],
        }),
        {"turns": []},
    )

    assert result["summary"] == "客户表现出对AI预设话术的不信任。"


def test_semantic_analysis_result_strips_dangling_punctuation_after_sentence_boundary() -> None:
    module = _semantic_module()

    result = module.enforce_semantic_evidence_on_result(
        module.normalize_analysis_result({
            "summary": "客户关注识别准确率。'，反映对AI话术预设前提的抵触。",
            "feedback_type": "中性",
            "key_points": [],
            "time_hint": {},
            "tags": [],
        }),
        {"turns": []},
    )

    assert result["summary"] == "客户关注识别准确率。反映对AI话术预设前提的抵触。"


def test_semantic_analysis_result_removes_speculative_asr_correction_parenthetical() -> None:
    module = _semantic_module()

    result = module.enforce_semantic_evidence_on_result(
        module.normalize_analysis_result({
            "summary": "客户询问'单证黑员识别，你识别的准确率怎么样？'（应为'单证关键字段识别'的ASR错误）。",
            "feedback_type": "中性",
            "key_points": [],
            "time_hint": {},
            "tags": [],
        }),
        {"turns": []},
    )

    assert result["summary"] == "客户询问'单证黑员识别，你识别的准确率怎么样？'。"


def test_semantic_analysis_result_removes_short_record_only_text_when_not_background() -> None:
    module = _semantic_module()
    snapshot = _build_snapshot([
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="我先介绍一下产品。",
            started_offset_seconds=0,
            ended_offset_seconds=2,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            text="不帮我了。",
            source="offline_asr",
            started_offset_seconds=3,
            ended_offset_seconds=4,
        ),
    ])

    result = module.enforce_semantic_evidence_on_result(
        module.normalize_analysis_result({
            "summary": "客户说‘不帮我了’，表现出拒绝倾向。",
            "feedback_type": "负向",
            "key_points": ["客户表达‘不帮我了’"],
            "time_hint": {},
            "tags": [],
        }),
        snapshot,
    )

    rendered = json.dumps(result, ensure_ascii=False)
    assert "不帮我了" not in rendered
    assert result["key_points"] == []


def test_semantic_analysis_result_cleans_prefix_after_record_only_sentence_removal() -> None:
    module = _semantic_module()
    snapshot = _build_snapshot([
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="请问您这边是否也在处理单证？",
            started_offset_seconds=0,
            ended_offset_seconds=2,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            text="不帮我了。",
            source="offline_asr",
            started_offset_seconds=3,
            ended_offset_seconds=4,
        ),
    ])

    result = module.enforce_semantic_evidence_on_result(
        module.normalize_analysis_result({
            "summary": "客户说‘不帮我了’，表现出抵触；后续关注识别准确率。",
            "feedback_type": "负向",
            "key_points": [],
            "time_hint": {},
            "tags": [],
        }),
        snapshot,
    )

    assert result["summary"] == "后续关注识别准确率。"


def test_semantic_evidence_blocks_isolated_conflicting_name_from_identity_fact() -> None:
    snapshot = _build_snapshot([
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="您好，我简单介绍一下 GEO 服务，请问现在方便吗？",
            started_offset_seconds=0,
            ended_offset_seconds=3,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            text="Amen.",
            source="offline_asr",
            started_offset_seconds=4,
            ended_offset_seconds=5,
        ),
        _segment(
            segment_no=3,
            speaker_type="customer",
            text="王冕。",
            source="qwen_realtime",
            started_offset_seconds=4,
            ended_offset_seconds=5,
        ),
    ])

    turn = _user_turns(snapshot)[0]
    evidence = turn["semantic_evidence"]

    assert turn["text"] == "王冕。"
    assert evidence["responds_to_previous_ai"] is False
    assert evidence["source_conflict"] is True
    assert evidence["low_confidence_source"] is True
    assert evidence["supports_strong_fact"] is False
    assert "identity" in evidence["unsupported_strong_fact_types"]


def test_semantic_evidence_allows_real_identity_when_context_supports_it() -> None:
    asked_name_snapshot = _build_snapshot([
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="方便的话，请问怎么称呼您？",
            started_offset_seconds=0,
            ended_offset_seconds=2,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            text="王冕。",
            source="offline_asr",
            started_offset_seconds=3,
            ended_offset_seconds=4,
        ),
    ])
    self_intro_snapshot = _build_snapshot([
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="您好，我是灵宸智能助手。",
            started_offset_seconds=0,
            ended_offset_seconds=2,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            text="我叫王冕。",
            source="qwen_realtime",
            started_offset_seconds=3,
            ended_offset_seconds=4,
        ),
    ])

    asked_evidence = _user_turns(asked_name_snapshot)[0]["semantic_evidence"]
    self_intro_evidence = _user_turns(self_intro_snapshot)[0]["semantic_evidence"]

    assert asked_evidence["responds_to_previous_ai"] is True
    assert asked_evidence["supports_strong_fact"] is True
    assert "identity" in asked_evidence["supported_strong_fact_types"]
    assert self_intro_evidence["supports_strong_fact"] is True
    assert "identity" in self_intro_evidence["supported_strong_fact_types"]


def test_semantic_evidence_keeps_real_topic_jump_as_new_question() -> None:
    snapshot = _build_snapshot([
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="您主要关注品牌曝光还是线索增长？",
            started_offset_seconds=0,
            ended_offset_seconds=2,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            text="DeepSeek 也能做吗？",
            source="qwen_realtime",
            started_offset_seconds=3,
            ended_offset_seconds=4,
        ),
    ])

    evidence = _user_turns(snapshot)[0]["semantic_evidence"]

    assert evidence["new_question_or_intent"] is True
    assert evidence["key_point_candidate"] is True
    assert evidence["analysis_usage"] == "use_as_customer_signal"


def test_semantic_evidence_keeps_business_question_without_strong_conclusion() -> None:
    snapshot = _build_snapshot([
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="您目前团队在合同审查环节主要遇到的痛点是什么呢？",
            started_offset_seconds=0,
            ended_offset_seconds=2,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            text="你们可以审核什么样的合同？",
            source="qwen_realtime",
            started_offset_seconds=3,
            ended_offset_seconds=5,
        ),
    ])

    evidence = _user_turns(snapshot)[0]["semantic_evidence"]

    assert evidence["new_question_or_intent"] is True
    assert evidence["business_detail"] is True
    assert evidence["key_point_candidate"] is True
    assert evidence["supports_strong_fact"] is False
    assert evidence["supported_strong_fact_types"] == []
    assert evidence["analysis_usage"] == "use_as_customer_signal"


def test_semantic_evidence_keeps_short_answer_to_previous_question() -> None:
    snapshot = _build_snapshot([
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="请问现在方便吗？",
            started_offset_seconds=0,
            ended_offset_seconds=2,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            text="方便。",
            source="qwen_realtime",
            started_offset_seconds=3,
            ended_offset_seconds=4,
        ),
    ])

    turn = _user_turns(snapshot)[0]
    evidence = turn["semantic_evidence"]

    assert evidence["responds_to_previous_ai"] is True
    assert evidence["weak_feedback"] is False
    assert "identity" not in evidence["unsupported_strong_fact_types"]
    assert evidence["analysis_usage"] == "use_as_customer_signal"
    assert _semantic_module().SemanticTranscriptBuilder().has_effective_user_input(snapshot)


def test_semantic_evidence_marks_direct_future_call_instruction_as_consent() -> None:
    snapshot = _build_snapshot([
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="方便留一个邮箱或微信号吗？",
            started_offset_seconds=0,
            ended_offset_seconds=2,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            text="你到时候打一个电话就行。",
            source="qwen_realtime",
            started_offset_seconds=3,
            ended_offset_seconds=5,
        ),
    ])

    evidence = _user_turns(snapshot)[0]["semantic_evidence"]

    assert evidence["analysis_usage"] == "use_as_customer_signal"
    assert evidence["supports_strong_fact"] is True
    assert "follow_up_consent" in evidence["supported_strong_fact_types"]


def test_semantic_evidence_marks_trial_link_acceptance_as_follow_up_consent() -> None:
    snapshot = _build_snapshot([
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="我稍后可以把试用申请链接发给您，您看方便吗？",
            started_offset_seconds=0,
            ended_offset_seconds=2,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            text="方便。",
            source="qwen_realtime",
            started_offset_seconds=3,
            ended_offset_seconds=4,
        ),
    ])

    evidence = _user_turns(snapshot)[0]["semantic_evidence"]

    assert evidence["supports_strong_fact"] is True
    assert "follow_up_consent" in evidence["supported_strong_fact_types"]


def test_semantic_analysis_does_not_treat_answer_before_demo_offer_as_acceptance() -> None:
    module = _semantic_module()
    rows = [
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="我们先看整体品牌表现，再深入具体平台。您觉得这样安排可以吗？",
            started_offset_seconds=0,
            ended_offset_seconds=3,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            text="行。",
            source="qwen_realtime",
            started_offset_seconds=4,
            ended_offset_seconds=5,
        ),
        _segment(
            segment_no=3,
            speaker_type="ai",
            text="我可以帮您安排一次产品演示。您看是这周还是下周方便呢？",
            started_offset_seconds=6,
            ended_offset_seconds=9,
        ),
    ]
    snapshot = _build_snapshot(rows)

    raw_result = {
        "summary": (
            "客户持续关注上传、试用和效果指标；"
            "当AI提出演示邀约时，客户明确应答‘行’，表示接受后续安排。"
        ),
        "feedback_type": "中性",
        "key_points": [
            "客户关注上传、试用和效果指标",
            "客户应答AI提出的演示邀约‘行’",
        ],
        "time_hint": {},
        "tags": ["指标关注", "演示接受"],
        "follow_up": {
            "required": True,
            "consent": "explicit",
            "reason": "客户同意产品演示",
            "confidence": "high",
        },
        "classification": "nurturing",
        "confidence": "medium",
        "valid_dialogue": True,
        "reason": (
            "客户持续追问产品落地问题，表现出实质性兴趣；"
            "对指标和演示表现出开放态度，仍需进一步培育。"
        ),
        "evidence": ["有没有具体指标？", "都感兴趣。", "行。"],
        "evidence_conflict": False,
        "low_value_reason": None,
    }
    result = module.enforce_semantic_evidence_on_result(raw_result, snapshot)

    rendered = json.dumps(result, ensure_ascii=False)
    assert result["summary"] == "客户持续关注上传、试用和效果指标。"
    assert result["key_points"] == ["客户关注上传、试用和效果指标"]
    assert result["tags"] == ["指标关注"]
    assert result["reason"] == "客户持续追问产品落地问题，表现出实质性兴趣。"
    assert result["follow_up"]["consent"] == "missing"
    assert result["follow_up"]["confidence"] == "low"
    assert result["classification"] == "nurturing"
    assert "演示接受" not in rendered
    assert "同意产品演示" not in rendered

    confirmed = module.enforce_semantic_evidence_on_result(
        raw_result,
        _build_snapshot([
            *rows,
            _segment(
                segment_no=4,
                speaker_type="customer",
                text="行。",
                source="qwen_realtime",
                started_offset_seconds=10,
                ended_offset_seconds=11,
            ),
        ]),
    )
    assert confirmed["follow_up"]["consent"] == "explicit"
    assert "演示接受" in confirmed["tags"]


def test_semantic_evidence_marks_weak_feedback_without_business_fact() -> None:
    snapshot = _build_snapshot([
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="我先把产品背景说明一下。",
            started_offset_seconds=0,
            ended_offset_seconds=2,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            text="嗯。",
            source="qwen_realtime",
            started_offset_seconds=3,
            ended_offset_seconds=4,
        ),
    ])

    evidence = _user_turns(snapshot)[0]["semantic_evidence"]

    assert evidence["weak_feedback"] is True
    assert evidence["key_point_candidate"] is False
    assert evidence["supports_strong_fact"] is False
    assert evidence["analysis_usage"] == "record_only"


def test_semantic_evidence_marks_business_detail_as_key_point_candidate() -> None:
    snapshot = _build_snapshot([
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="您这边主要想解决哪类业务问题？",
            started_offset_seconds=0,
            ended_offset_seconds=2,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            text="我们主要是品牌曝光，业务合同比较多。",
            source="qwen_realtime",
            started_offset_seconds=3,
            ended_offset_seconds=5,
        ),
    ])

    evidence = _user_turns(snapshot)[0]["semantic_evidence"]

    assert evidence["business_detail"] is True
    assert evidence["key_point_candidate"] is True
    assert evidence["supports_strong_fact"] is True
    assert "requirement_conclusion" in evidence["supported_strong_fact_types"]


def test_semantic_evidence_marks_lawyer_review_objection_as_business_detail() -> None:
    snapshot = _build_snapshot([
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="您觉得这些方面里，哪一块对您团队来说目前最头疼？",
            started_offset_seconds=0,
            ended_offset_seconds=2,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            text="修改建议吧，修改建议需要找律师，专业的律师来做。",
            source="qwen_realtime",
            started_offset_seconds=3,
            ended_offset_seconds=6,
        ),
    ])

    evidence = _user_turns(snapshot)[0]["semantic_evidence"]

    assert evidence["business_detail"] is True
    assert evidence["key_point_candidate"] is True
    assert evidence["analysis_usage"] == "use_as_customer_signal"


def test_semantic_evidence_marks_document_trade_scope_as_business_detail() -> None:
    snapshot = _build_snapshot([
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="您这边主要处理哪类单证比较多呢？",
            started_offset_seconds=0,
            ended_offset_seconds=2,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            text="境外贸易吧。",
            source="offline_asr",
            started_offset_seconds=3,
            ended_offset_seconds=5,
        ),
    ])

    evidence = _user_turns(snapshot)[0]["semantic_evidence"]

    assert evidence["business_detail"] is True
    assert evidence["key_point_candidate"] is True
    assert evidence["analysis_usage"] == "use_as_customer_signal"


def test_semantic_evidence_marks_conversation_control_as_key_point_candidate() -> None:
    snapshot = _build_snapshot([
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="您看我是稍后再联系您，还是今天就先到这里？",
            started_offset_seconds=0,
            ended_offset_seconds=2,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            text="先到这儿吧。",
            source="qwen_realtime",
            started_offset_seconds=3,
            ended_offset_seconds=5,
        ),
    ])

    evidence = _user_turns(snapshot)[0]["semantic_evidence"]

    assert evidence["conversation_control_intent"] is True
    assert evidence["key_point_candidate"] is True
    assert evidence["analysis_usage"] == "use_as_customer_signal"


def test_semantic_evidence_treats_preference_answer_as_requirement_not_time() -> None:
    snapshot = _build_snapshot([
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="您这边是更关注提升审查效率，还是希望把风险识别做得更全面一些呢？",
            started_offset_seconds=0,
            ended_offset_seconds=2,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            text="强大一点吧。",
            source="offline_asr",
            started_offset_seconds=3,
            ended_offset_seconds=5,
        ),
    ])

    evidence = _user_turns(snapshot)[0]["semantic_evidence"]

    assert evidence["business_detail"] is True
    assert evidence["key_point_candidate"] is True
    assert "requirement_conclusion" in evidence["supported_strong_fact_types"]
    assert "time" not in evidence["supported_strong_fact_types"]


def test_semantic_evidence_keeps_geo_recommendation_probability_answer() -> None:
    snapshot = _build_snapshot([
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="您这边主要关注品牌在AI里的呈现效果，还是希望提升被推荐的概率呢？",
            started_offset_seconds=0,
            ended_offset_seconds=2,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            text="推荐概率吧。",
            source="qwen_realtime",
            started_offset_seconds=3,
            ended_offset_seconds=4,
        ),
    ])

    evidence = _user_turns(snapshot)[0]["semantic_evidence"]

    assert evidence["responds_to_previous_ai"] is True
    assert evidence["business_detail"] is True
    assert evidence["key_point_candidate"] is True
    assert evidence["analysis_usage"] == "use_as_customer_signal"
    assert "requirement_conclusion" in evidence["supported_strong_fact_types"]


def test_semantic_analysis_result_removes_identity_claim_without_supporting_evidence() -> None:
    module = _semantic_module()
    snapshot = _build_snapshot([
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="您好，我简单介绍一下 GEO 服务。",
            started_offset_seconds=0,
            ended_offset_seconds=3,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            text="Amen.",
            source="offline_asr",
            started_offset_seconds=4,
            ended_offset_seconds=5,
        ),
        _segment(
            segment_no=3,
            speaker_type="customer",
            text="王冕。",
            source="qwen_realtime",
            started_offset_seconds=4,
            ended_offset_seconds=5,
        ),
    ])
    result = module.enforce_semantic_evidence_on_result(
        module.normalize_analysis_result({
            "summary": "客户王冕在通话中表现出基础配合意愿。",
            "feedback_type": "中性",
            "key_points": ["客户自报姓名为王冕"],
            "time_hint": {},
            "tags": [],
        }),
        snapshot,
    )

    rendered = json.dumps(result, ensure_ascii=False)
    assert "王冕" not in rendered
    assert result["summary"] == "客户身份信息未被可靠确认，存在转写噪声风险。"
    assert result["key_points"] == []
    assert "转写噪声风险" in result["tags"]


def test_semantic_analysis_result_keeps_quoted_noise_that_is_not_identity_claim() -> None:
    module = _semantic_module()
    snapshot = _build_snapshot([
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="您好，我简单介绍一下合同智能审查产品，请问现在方便吗？",
            started_offset_seconds=0,
            ended_offset_seconds=3,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            text="Amen.",
            source="offline_asr",
            started_offset_seconds=4,
            ended_offset_seconds=5,
        ),
        _segment(
            segment_no=3,
            speaker_type="customer",
            text="方面。",
            source="qwen_realtime",
            started_offset_seconds=4,
            ended_offset_seconds=5,
        ),
    ])

    result = module.enforce_semantic_evidence_on_result(
        module.normalize_analysis_result({
            "summary": "客户先回应'方面。'，随后询问合同审核范围。",
            "feedback_type": "中性",
            "key_points": ["客户询问合同审核范围"],
            "time_hint": {},
            "tags": [],
        }),
        snapshot,
    )

    assert result["summary"] == "客户先回应'方面。'，随后询问合同审核范围。"
    assert result["key_points"] == ["客户询问合同审核范围"]
    assert result["tags"] == []


def test_semantic_analysis_result_removes_rejected_candidate_source_text() -> None:
    module = _semantic_module()
    now = datetime(2026, 7, 8, 10, 0, tzinfo=timezone.utc)
    snapshot = _build_snapshot([
        _segment(
            segment_no=1,
            speaker_type="ai",
            text="您看是希望先安排一次线上沟通，还是等您这边有明确需求再联系？",
            started_offset_seconds=0,
            ended_offset_seconds=3,
        ),
        AiCallDialogueSegmentModel(
            id=2,
            call_id="call_semantic_evidence",
            segment_no=2,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="qwen_realtime",
            source_segment_id="rt_customer_short",
            segment_text="两人是吧。",
            segment_status="final",
            started_at=now + timedelta(seconds=4),
            ended_at=now + timedelta(seconds=5),
            duration_ms=1000,
        ),
        AiCallDialogueSegmentModel(
            id=3,
            call_id="call_semantic_evidence",
            segment_no=3,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="offline_asr",
            source_segment_id="offline_customer_short",
            segment_text="聊点啥？",
            segment_status="final",
            started_at=now + timedelta(seconds=4),
            ended_at=now + timedelta(seconds=5),
            duration_ms=1000,
        ),
    ])

    result = module.enforce_semantic_evidence_on_result(
        module.normalize_analysis_result({
            "summary": "客户回应‘两人是吧’，对应离线 ASR 为‘聊点啥？’，存在转写噪声。",
            "feedback_type": "中性",
            "key_points": ["离线 ASR 识别为‘聊点啥？’"],
            "time_hint": {},
            "tags": [],
        }),
        snapshot,
    )

    rendered = json.dumps(result, ensure_ascii=False)
    assert "聊点啥" not in rendered
    assert result["key_points"] == []
    assert "转写噪声风险" in result["tags"]


@pytest.mark.anyio
async def test_semantic_analysis_record_status_lifecycle() -> None:
    module = _semantic_module()
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)

        created = await repository.ensure_semantic_analysis_record(
            call_id="call_semantic_lifecycle",
            scene_code="intro_geo",
        )
        duplicate = await repository.ensure_semantic_analysis_record(
            call_id="call_semantic_lifecycle",
            scene_code="intro_geo",
        )

        assert created.id == duplicate.id
        assert created.analysis_status == module.ANALYSIS_STATUS_PENDING

        claimed = await repository.claim_semantic_analysis(
            call_id="call_semantic_lifecycle",
            now=datetime(2026, 7, 2, 10, 0, tzinfo=timezone.utc),
        )
        assert claimed is not None
        assert claimed.analysis_status == module.ANALYSIS_STATUS_RUNNING
        assert claimed.analysis_started_at is not None

        result = module.normalize_analysis_result({
            "summary": "客户下午方便继续沟通。",
            "feedback_type": "正向",
            "key_points": ["客户愿意继续沟通"],
            "time_hint": {
                "time_text": "下午",
                "time_value": "",
                "original_texts": ["下午再说"],
            },
            "tags": ["可跟进"],
            "extra": "ignored",
        })
        finished = await repository.update_semantic_analysis_success(
            call_id="call_semantic_lifecycle",
            analysis_result=result,
            transcript_snapshot_json='{"turns":[]}',
            transcript_hash="hash_1",
            now=datetime(2026, 7, 2, 10, 1, tzinfo=timezone.utc),
        )
        assert finished is not None
        assert finished.analysis_status == module.ANALYSIS_STATUS_SUCCEEDED
        assert finished.analysis_version == 1
        assert finished.analysis_result_dict == result
        assert finished.analysis_finished_at is not None

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_record_service_persists_prompt_context_for_semantic_analysis() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        record = await record_service.create_web_record(
            call_id="call_semantic_context",
            business_id="biz_1",
            room_name="ai-call-call_semantic_context",
            participant_identity="browser-call_semantic_context",
        )
        assert record.scene_code is None
        assert record.prompt_source_key is None

        updated = await record_service.update_prompt_context(
            "call_semantic_context",
            scene_code="intro_geo",
            prompt_source_key="static_profile",
        )

        assert updated is not None
        assert updated.scene_code == "intro_geo"
        assert updated.prompt_source_key == "static_profile"

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_failed_semantic_analysis_can_be_reclaimed_after_cooldown() -> None:
    module = _semantic_module()
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        await repository.ensure_semantic_analysis_record(
            call_id="call_semantic_retry",
            scene_code="intro_geo",
        )
        await repository.claim_semantic_analysis(
            call_id="call_semantic_retry",
            now=datetime(2026, 7, 2, 10, 0, tzinfo=timezone.utc),
        )
        failed = await repository.update_semantic_analysis_failed(
            call_id="call_semantic_retry",
            analysis_error="llm timeout",
            transcript_snapshot_json='{"turns":[]}',
            transcript_hash="hash_retry",
            now=datetime(2026, 7, 2, 10, 1, tzinfo=timezone.utc),
        )

        assert failed is not None
        assert failed.analysis_status == module.ANALYSIS_STATUS_FAILED
        assert failed.analysis_retry_count == 1
        assert (
            await repository.claim_semantic_analysis(
                call_id="call_semantic_retry",
                now=datetime(2026, 7, 2, 10, 5, tzinfo=timezone.utc),
                retry_cooldown_minutes=10,
            )
            is None
        )

        reclaimed = await repository.claim_semantic_analysis(
            call_id="call_semantic_retry",
            now=datetime(2026, 7, 2, 10, 12, tzinfo=timezone.utc),
            retry_cooldown_minutes=10,
        )
        assert reclaimed is not None
        assert reclaimed.analysis_status == module.ANALYSIS_STATUS_RUNNING

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_semantic_transcript_prefers_offline_asr_and_keeps_interrupted_ai() -> None:
    module = _semantic_module()
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        started_at = datetime(2026, 7, 2, 10, 0, tzinfo=timezone.utc)
        await repository.upsert_dialogue_segment(
            call_id="call_semantic_snapshot",
            segment_no=2,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="qwen_realtime",
            source_segment_id="rt_customer_1",
            segment_text="实时文本不要优先用",
            segment_status="final",
            started_at=started_at + timedelta(seconds=2),
            ended_at=started_at + timedelta(seconds=3),
            duration_ms=1000,
        )
        await repository.upsert_dialogue_segment(
            call_id="call_semantic_snapshot",
            segment_no=3,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="offline_asr",
            source_segment_id="offline_customer_1",
            segment_text="下午两点再联系我。",
            segment_status="final",
            started_at=started_at + timedelta(seconds=2),
            ended_at=started_at + timedelta(seconds=3),
            duration_ms=1000,
        )
        await repository.upsert_dialogue_segment(
            call_id="call_semantic_snapshot",
            segment_no=1,
            speaker_type="ai",
            speaker_identity="agent-call",
            source="qwen_realtime",
            source_segment_id="ai_1",
            segment_text="您好，我是灵宸智能助手。",
            segment_status="interrupted",
            started_at=started_at,
            ended_at=started_at + timedelta(seconds=1),
            duration_ms=1000,
        )
        await repository.upsert_dialogue_segment(
            call_id="call_semantic_snapshot",
            segment_no=4,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="qwen_realtime",
            source_segment_id="rt_customer_partial",
            segment_text="草稿",
            segment_status="partial",
            started_at=started_at + timedelta(seconds=4),
            ended_at=started_at + timedelta(seconds=5),
            duration_ms=1000,
        )

        rows = await repository.list_dialogue_segments("call_semantic_snapshot")
        snapshot = module.SemanticTranscriptBuilder().build(
            call_id="call_semantic_snapshot",
            scene_code="intro_geo",
            rows=rows,
        )

        assert [turn["role"] for turn in snapshot["turns"]] == ["assistant", "user"]
        assert snapshot["turns"][0]["segment_status"] == "interrupted"
        assert snapshot["turns"][0]["note"] == "AI 话术被用户打断，不代表用户完整听到"
        assert snapshot["turns"][1]["text"] == "下午两点再联系我。"
        assert snapshot["turns"][1]["source"] == "offline_asr"
        assert snapshot["metadata"]["fallback_to_realtime"] is False
        assert snapshot["metadata"]["has_interrupted_ai"] is True

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


def test_semantic_transcript_supplements_realtime_customer_gaps_when_offline_asr_exists() -> None:
    module = _semantic_module()
    now = datetime(2026, 7, 2, 10, 0, tzinfo=timezone.utc)
    rows = [
        AiCallDialogueSegmentModel(
            id=1,
            call_id="call_semantic_realtime_gap",
            segment_no=1,
            speaker_type="ai",
            speaker_identity="agent-call",
            source="qwen_realtime",
            source_segment_id="ai_1",
            segment_text="您这边主要是市场、品牌还是增长团队在关注这类问题呢？",
            segment_status="final",
            started_at=now,
            ended_at=now + timedelta(seconds=5),
            duration_ms=5000,
        ),
        AiCallDialogueSegmentModel(
            id=2,
            call_id="call_semantic_realtime_gap",
            segment_no=2,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="qwen_realtime",
            source_segment_id="rt_customer_gap",
            segment_text="主要是品牌吧。",
            segment_status="final",
            started_at=now + timedelta(seconds=6),
            ended_at=now + timedelta(seconds=8),
            duration_ms=2000,
        ),
        AiCallDialogueSegmentModel(
            id=3,
            call_id="call_semantic_realtime_gap",
            segment_no=3,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="offline_asr",
            source_segment_id="offline_customer_1",
            segment_text="嗯，可以，你们有 demo 吗？",
            segment_status="final",
            started_at=now + timedelta(seconds=20),
            ended_at=now + timedelta(seconds=24),
            duration_ms=4000,
        ),
        AiCallDialogueSegmentModel(
            id=4,
            call_id="call_semantic_realtime_gap",
            segment_no=4,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="qwen_realtime",
            source_segment_id="rt_customer_duplicate",
            segment_text="你们有 demo 吗？",
            segment_status="final",
            started_at=now + timedelta(seconds=25),
            ended_at=now + timedelta(seconds=26),
            duration_ms=1000,
        ),
    ]

    snapshot = module.SemanticTranscriptBuilder().build(
        call_id="call_semantic_realtime_gap",
        scene_code="intro_geo",
        rows=rows,
    )

    user_turns = [turn for turn in snapshot["turns"] if turn["role"] == "user"]
    assert [turn["text"] for turn in user_turns] == [
        "主要是品牌吧。",
        "嗯，可以，你们有 demo 吗？",
    ]
    assert [turn["source"] for turn in user_turns] == [
        "qwen_realtime",
        "offline_asr",
    ]
    assert snapshot["metadata"]["fallback_to_realtime"] is False
    assert snapshot["metadata"]["realtime_supplemented_count"] == 1


def test_semantic_transcript_falls_back_to_realtime_and_records_reason() -> None:
    module = _semantic_module()
    now = datetime(2026, 7, 2, 10, 0, tzinfo=timezone.utc)
    rows = [
        AiCallDialogueSegmentModel(
            id=1,
            call_id="call_semantic_fallback",
            segment_no=1,
            speaker_type="customer",
            speaker_identity=None,
            source="offline_asr",
            source_segment_id="offline_failed",
            segment_text="",
            segment_status="timeout",
            started_at=now,
            ended_at=now,
            duration_ms=0,
            failure_stage="timeout",
            failure_message="provider timeout",
        ),
        AiCallDialogueSegmentModel(
            id=2,
            call_id="call_semantic_fallback",
            segment_no=2,
            speaker_type="customer",
            speaker_identity=None,
            source="qwen_realtime",
            source_segment_id="rt_final",
            segment_text="我现在不方便。",
            segment_status="final",
            started_at=now + timedelta(seconds=1),
            ended_at=now + timedelta(seconds=2),
            duration_ms=1000,
        ),
    ]

    snapshot = module.SemanticTranscriptBuilder().build(
        call_id="call_semantic_fallback",
        scene_code=None,
        rows=rows,
    )

    assert snapshot["turns"][0]["text"] == "我现在不方便。"
    assert snapshot["turns"][0]["source"] == "qwen_realtime"
    assert snapshot["metadata"]["fallback_to_realtime"] is True
    assert snapshot["metadata"]["fallback_reason"] == "offline_asr_timeout"


def test_semantic_transcript_uses_realtime_when_overlapping_offline_asr_is_low_quality() -> None:
    module = _semantic_module()
    now = datetime(2026, 7, 2, 10, 0, tzinfo=timezone.utc)
    rows = [
        AiCallDialogueSegmentModel(
            id=1,
            call_id="call_semantic_quality_fallback",
            segment_no=1,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="offline_asr",
            source_segment_id="offline_customer_bad",
            segment_text="No, tell down.",
            segment_status="final",
            started_at=now + timedelta(seconds=1),
            ended_at=now + timedelta(seconds=2),
            duration_ms=1000,
        ),
        AiCallDialogueSegmentModel(
            id=2,
            call_id="call_semantic_quality_fallback",
            segment_no=2,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="qwen_realtime",
            source_segment_id="rt_customer_good",
            segment_text="行。",
            segment_status="final",
            started_at=now + timedelta(seconds=1),
            ended_at=now + timedelta(seconds=2),
            duration_ms=1000,
        ),
    ]

    snapshot = module.SemanticTranscriptBuilder().build(
        call_id="call_semantic_quality_fallback",
        scene_code="intro_geo",
        rows=rows,
    )

    assert [turn["text"] for turn in snapshot["turns"]] == ["行。"]
    assert snapshot["turns"][0]["source"] == "qwen_realtime"
    assert snapshot["turns"][0]["source_decision"]["selected_source"] == "qwen_realtime"
    assert snapshot["turns"][0]["source_decision"]["fallback_reason"] == (
        "offline_asr_low_quality_language_mismatch"
    )
    assert snapshot["metadata"]["fallback_to_realtime"] is True
    assert snapshot["metadata"]["fallback_reason"] == (
        "offline_asr_low_quality_language_mismatch"
    )


def test_semantic_transcript_uses_realtime_when_mixed_language_offline_hides_business_detail() -> None:
    module = _semantic_module()
    now = datetime(2026, 7, 8, 1, 20, tzinfo=timezone.utc)
    rows = [
        AiCallDialogueSegmentModel(
            id=1,
            call_id="call_semantic_mixed_language_business",
            segment_no=1,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="offline_asr",
            source_segment_id="offline_customer_mixed",
            segment_text="Senfri斯菲。",
            segment_status="final",
            started_at=now + timedelta(seconds=54),
            ended_at=now + timedelta(seconds=57),
            duration_ms=3000,
        ),
        AiCallDialogueSegmentModel(
            id=2,
            call_id="call_semantic_mixed_language_business",
            segment_no=2,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="qwen_realtime",
            source_segment_id="rt_customer_pain",
            segment_text="审查费时费力。",
            segment_status="final",
            started_at=now + timedelta(seconds=56),
            ended_at=now + timedelta(seconds=63),
            duration_ms=7000,
        ),
    ]

    snapshot = module.SemanticTranscriptBuilder().build(
        call_id="call_semantic_mixed_language_business",
        scene_code="intro_contract",
        rows=rows,
    )

    assert [turn["text"] for turn in snapshot["turns"]] == ["审查费时费力。"]
    assert snapshot["turns"][0]["source"] == "qwen_realtime"
    assert snapshot["turns"][0]["source_decision"]["fallback_reason"] == (
        "offline_asr_low_quality_language_mismatch"
    )
    assert snapshot["metadata"]["offline_asr_quality_rejected_count"] == 1


def test_semantic_transcript_marks_uncertainty_without_dropping_clean_turns() -> None:
    module = _semantic_module()
    now = datetime(2026, 7, 2, 10, 0, tzinfo=timezone.utc)
    rows = [
        AiCallDialogueSegmentModel(
            id=1,
            call_id="call_semantic_uncertain_snapshot",
            segment_no=1,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="offline_asr",
            source_segment_id="offline_customer_bad",
            segment_text="No, tell down.",
            segment_status="final",
            started_at=now + timedelta(seconds=1),
            ended_at=now + timedelta(seconds=2),
            duration_ms=1000,
        ),
        AiCallDialogueSegmentModel(
            id=2,
            call_id="call_semantic_uncertain_snapshot",
            segment_no=2,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="qwen_realtime",
            source_segment_id="rt_customer_ack",
            segment_text="行。",
            segment_status="final",
            started_at=now + timedelta(seconds=1),
            ended_at=now + timedelta(seconds=2),
            duration_ms=1000,
        ),
        AiCallDialogueSegmentModel(
            id=3,
            call_id="call_semantic_uncertain_snapshot",
            segment_no=3,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="offline_asr",
            source_segment_id="offline_customer_time",
            segment_text="明天下午。",
            segment_status="final",
            started_at=now + timedelta(seconds=10),
            ended_at=now + timedelta(seconds=12),
            duration_ms=2000,
        ),
    ]

    snapshot = module.SemanticTranscriptBuilder().build(
        call_id="call_semantic_uncertain_snapshot",
        scene_code="intro_geo",
        rows=rows,
    )

    assert [turn["text"] for turn in snapshot["turns"]] == ["行。", "明天下午。"]
    quality = snapshot["metadata"]["transcript_quality"]
    assert quality["has_uncertain_transcript"] is True
    assert "source_conflict" in quality["signals"]
    assert "offline_asr_low_quality_language_mismatch" in quality["reasons"]
    assert "不得把低置信片段当作强业务结论" in quality["analysis_guidance"]


def test_semantic_transcript_replaces_polluted_offline_spans_with_realtime_evidence() -> None:
    module = _semantic_module()
    now = datetime(2026, 7, 2, 10, 0, tzinfo=timezone.utc)
    rows = [
        AiCallDialogueSegmentModel(
            id=1,
            call_id="call_semantic_polluted_offline",
            segment_no=1,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="offline_asr",
            source_segment_id="offline_customer_mixed_noise",
            segment_text="你好，你有孩子了，Amen.",
            segment_status="final",
            started_at=now + timedelta(seconds=1),
            ended_at=now + timedelta(seconds=9),
            duration_ms=8000,
        ),
        AiCallDialogueSegmentModel(
            id=2,
            call_id="call_semantic_polluted_offline",
            segment_no=2,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="qwen_realtime",
            source_segment_id="rt_customer_hello",
            segment_text="你好。",
            segment_status="final",
            started_at=now + timedelta(seconds=7),
            ended_at=now + timedelta(seconds=7, milliseconds=100),
            duration_ms=100,
        ),
        AiCallDialogueSegmentModel(
            id=3,
            call_id="call_semantic_polluted_offline",
            segment_no=3,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="qwen_realtime",
            source_segment_id="rt_customer_restart",
            segment_text="你又开始了。",
            segment_status="final",
            started_at=now + timedelta(seconds=7, milliseconds=200),
            ended_at=now + timedelta(seconds=7, milliseconds=900),
            duration_ms=700,
        ),
        AiCallDialogueSegmentModel(
            id=4,
            call_id="call_semantic_polluted_offline",
            segment_no=4,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="qwen_realtime",
            source_segment_id="rt_customer_convenient",
            segment_text="方便。",
            segment_status="final",
            started_at=now + timedelta(seconds=8),
            ended_at=now + timedelta(seconds=9),
            duration_ms=1000,
        ),
        AiCallDialogueSegmentModel(
            id=5,
            call_id="call_semantic_polluted_offline",
            segment_no=5,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="offline_asr",
            source_segment_id="offline_customer_short_fragment",
            segment_text="赶。",
            segment_status="final",
            started_at=now + timedelta(seconds=24),
            ended_at=now + timedelta(seconds=26),
            duration_ms=2000,
        ),
        AiCallDialogueSegmentModel(
            id=6,
            call_id="call_semantic_polluted_offline",
            segment_no=6,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="qwen_realtime",
            source_segment_id="rt_customer_interest",
            segment_text="感兴趣。",
            segment_status="final",
            started_at=now + timedelta(seconds=25),
            ended_at=now + timedelta(seconds=26),
            duration_ms=1000,
        ),
    ]

    snapshot = module.SemanticTranscriptBuilder().build(
        call_id="call_semantic_polluted_offline",
        scene_code="intro_geo",
        rows=rows,
    )

    user_turns = [turn for turn in snapshot["turns"] if turn["role"] == "user"]
    assert [turn["text"] for turn in user_turns] == [
        "你好。",
        "你又开始了。",
        "方便。",
        "感兴趣。",
    ]
    assert all(turn["source"] == "qwen_realtime" for turn in user_turns)
    assert snapshot["metadata"]["fallback_to_realtime"] is True
    assert snapshot["metadata"]["offline_asr_quality_rejected_count"] == 2
    assert {
        decision["fallback_reason"]
        for decision in snapshot["metadata"]["customer_source_decisions"]
    } == {
        "offline_asr_low_quality_language_mismatch",
        "offline_asr_shadowed_by_richer_realtime",
    }


def test_semantic_transcript_rejects_long_offline_span_diverging_from_realtime_turns() -> None:
    module = _semantic_module()
    now = datetime(2026, 7, 6, 9, 53, tzinfo=timezone.utc)
    rows = [
        AiCallDialogueSegmentModel(
            id=1,
            call_id="call_semantic_cjk_polluted_offline",
            segment_no=1,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="offline_asr",
            source_segment_id="offline_customer_polluted_span",
            segment_text="方便我知你唔要买好了，行。",
            segment_status="final",
            started_at=now + timedelta(seconds=46),
            ended_at=now + timedelta(seconds=60),
            duration_ms=14000,
        ),
        AiCallDialogueSegmentModel(
            id=2,
            call_id="call_semantic_cjk_polluted_offline",
            segment_no=2,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="qwen_realtime",
            source_segment_id="rt_customer_hello",
            segment_text="你好。",
            segment_status="final",
            started_at=now + timedelta(seconds=47),
            ended_at=now + timedelta(seconds=47, milliseconds=100),
            duration_ms=100,
        ),
        AiCallDialogueSegmentModel(
            id=3,
            call_id="call_semantic_cjk_polluted_offline",
            segment_no=3,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="qwen_realtime",
            source_segment_id="rt_customer_available",
            segment_text="方便。",
            segment_status="final",
            started_at=now + timedelta(seconds=48),
            ended_at=now + timedelta(seconds=49),
            duration_ms=1000,
        ),
        AiCallDialogueSegmentModel(
            id=4,
            call_id="call_semantic_cjk_polluted_offline",
            segment_no=4,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="qwen_realtime",
            source_segment_id="rt_customer_ack",
            segment_text="好的。",
            segment_status="final",
            started_at=now + timedelta(seconds=55),
            ended_at=now + timedelta(seconds=56),
            duration_ms=1000,
        ),
    ]

    snapshot = module.SemanticTranscriptBuilder().build(
        call_id="call_semantic_cjk_polluted_offline",
        scene_code="intro_contract",
        rows=rows,
    )

    user_turns = [turn for turn in snapshot["turns"] if turn["role"] == "user"]
    assert [turn["text"] for turn in user_turns] == ["你好。", "方便。", "好的。"]
    assert all(turn["source"] == "qwen_realtime" for turn in user_turns)
    assert snapshot["metadata"]["fallback_to_realtime"] is True
    assert snapshot["metadata"]["offline_asr_quality_rejected_count"] == 1
    assert snapshot["metadata"]["fallback_reason"] == "offline_asr_span_realtime_divergence"


def test_semantic_transcript_rejects_short_offline_question_diverging_from_realtime_turns() -> None:
    module = _semantic_module()
    rows = [
        _segment(
            segment_no=1,
            speaker_type="customer",
            source="offline_asr",
            text="可以知道吗？",
            started_offset_seconds=10,
            ended_offset_seconds=12,
        ),
        _segment(
            segment_no=2,
            speaker_type="customer",
            source="qwen_realtime",
            text="可以知道了。",
            started_offset_seconds=10.5,
            ended_offset_seconds=11.3,
        ),
        _segment(
            segment_no=3,
            speaker_type="customer",
            source="offline_asr",
            text="聊点啥？",
            started_offset_seconds=20,
            ended_offset_seconds=22,
        ),
        _segment(
            segment_no=4,
            speaker_type="customer",
            source="qwen_realtime",
            text="行。",
            started_offset_seconds=20.3,
            ended_offset_seconds=20.8,
        ),
        _segment(
            segment_no=5,
            speaker_type="customer",
            source="qwen_realtime",
            text="两人是吧。",
            started_offset_seconds=20.9,
            ended_offset_seconds=21.8,
        ),
    ]

    snapshot = module.SemanticTranscriptBuilder().build(
        call_id="call_semantic_short_offline_question_divergence",
        scene_code="intro_document",
        rows=rows,
    )

    user_turns = [turn for turn in snapshot["turns"] if turn["role"] == "user"]
    assert [turn["text"] for turn in user_turns] == [
        "可以知道了。",
        "行。",
        "两人是吧。",
    ]
    assert all(turn["source"] == "qwen_realtime" for turn in user_turns)
    assert snapshot["metadata"]["fallback_to_realtime"] is True
    assert snapshot["metadata"]["offline_asr_quality_rejected_count"] == 2
    assert {
        decision["fallback_reason"]
        for decision in snapshot["metadata"]["customer_source_decisions"]
    } == {"offline_asr_short_question_realtime_divergence"}


def test_semantic_analysis_result_is_normalized_to_fixed_contract_fields() -> None:
    module = _semantic_module()

    result = module.normalize_analysis_result({
        "summary": 123,
        "feedback_type": "其他",
        "key_points": "客户说下午联系",
        "time_hint": {"time_text": "下午", "original_texts": "下午再联系"},
        "tags": ["跟进", 7, ""],
        "unexpected": "ignored",
    })

    assert list(result.keys()) == [
        "summary",
        "feedback_type",
        "key_points",
        "time_hint",
        "tags",
        "follow_up",
        "classification",
        "confidence",
        "valid_dialogue",
        "reason",
        "evidence",
        "evidence_conflict",
        "low_value_reason",
    ]
    assert result["summary"] == "123"
    assert result["feedback_type"] == "中性"
    assert result["key_points"] == ["客户说下午联系"]
    assert result["time_hint"] == {
        "time_text": "下午",
        "time_value": "",
        "original_texts": ["下午再联系"],
    }
    assert result["tags"] == ["跟进", "7"]
    assert result["follow_up"] == {
        "required": False,
        "consent": "missing",
        "reason": "",
        "preferred_time": None,
        "confidence": "low",
    }
    assert result["classification"] is None
    assert result["valid_dialogue"] is False


@pytest.mark.anyio
async def test_semantic_analysis_marks_no_effective_user_input_without_llm_call() -> None:
    module = _semantic_module()
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    class FailingAnalyzer:
        async def analyze(self, *, transcript_snapshot: dict, reference_date: str | None = None):
            _ = transcript_snapshot, reference_date
            raise AssertionError("LLM should not be called without effective user text")

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        service = module.AiCallSemanticAnalysisService(
            repository,
            analyzer=FailingAnalyzer(),
        )

        analysis = await service.analyze_call_once(
            call_id="call_semantic_no_user",
            scene_code="intro_geo",
            now=datetime(2026, 7, 2, 10, 0, tzinfo=timezone.utc),
        )

        assert analysis.analysis_status == module.ANALYSIS_STATUS_NO_USER_INPUT
        assert analysis.analysis_error == "未获取到用户有效话术，无需进行语义分析"
        assert analysis.transcript_snapshot_dict["metadata"]["fallback_to_realtime"] is False

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_semantic_analysis_snapshot_records_asr_timeout_fallback_reason() -> None:
    module = _semantic_module()
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    class FakeAnalyzer:
        async def analyze(self, *, transcript_snapshot: dict, reference_date: str | None = None):
            _ = reference_date
            assert transcript_snapshot["metadata"]["fallback_to_realtime"] is True
            assert transcript_snapshot["metadata"]["fallback_reason"] == "offline_asr_timeout"
            return {
                "summary": "客户下午方便继续沟通。",
                "feedback_type": "正向",
                "key_points": ["客户下午方便沟通"],
                "time_hint": {
                    "time_text": "下午",
                    "time_value": "",
                    "original_texts": ["下午再联系"],
                },
                "tags": ["可跟进"],
            }

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        started_at = datetime(2026, 7, 2, 10, 0, tzinfo=timezone.utc)
        await repository.create_record(
            tenant_id="000000",
            call_id="call_semantic_asr_timeout",
            business_type=None,
            business_id=None,
            scene_code="intro_geo",
            entry_type="web",
            room_name="room-semantic-asr-timeout",
            participant_identity="browser-call",
            status="completed",
            started_at=started_at,
        )
        track = await repository.create_recording_track(
            tenant_id="000000",
            call_id="call_semantic_asr_timeout",
            room_name="room-semantic-asr-timeout",
            track_role="customer",
            participant_identity="browser-call",
            status="completed",
            started_at=started_at,
        )
        job = await repository.create_asr_job(
            call_id="call_semantic_asr_timeout",
            track_id=track.id,
            track_role="customer",
            participant_identity="browser-call",
            provider="dashscope_paraformer",
            model="paraformer-v2",
            status="pending",
            source_url="https://files.test/customer.ogg",
        )
        await repository.update_asr_job(
            job.id,
            status="failed",
            failure_stage="asr_transcribe",
            failure_message="ASR 任务轮询超时",
            completed_at=datetime(2026, 7, 2, 10, 2, tzinfo=timezone.utc),
        )
        await repository.upsert_dialogue_segment(
            call_id="call_semantic_asr_timeout",
            segment_no=1,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="qwen_realtime",
            source_segment_id="rt_customer_1",
            segment_text="下午再联系我。",
            segment_status="final",
            started_at=datetime(2026, 7, 2, 10, 0, tzinfo=timezone.utc),
            ended_at=datetime(2026, 7, 2, 10, 1, tzinfo=timezone.utc),
            duration_ms=1000,
        )
        service = module.AiCallSemanticAnalysisService(
            repository,
            analyzer=FakeAnalyzer(),
        )

        analysis = await service.analyze_call_once(
            call_id="call_semantic_asr_timeout",
            scene_code="intro_geo",
            now=datetime(2026, 7, 2, 10, 3, tzinfo=timezone.utc),
        )

        assert analysis.analysis_status == module.ANALYSIS_STATUS_SUCCEEDED
        assert analysis.transcript_snapshot_dict["metadata"]["fallback_reason"] == (
            "offline_asr_timeout"
        )
        assert analysis.transcript_snapshot_dict["metadata"]["offline_asr_jobs"][0]["status"] == (
            "failed"
        )

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_semantic_analysis_service_passes_handoffs_into_snapshot() -> None:
    module = _semantic_module()
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    class FakeAnalyzer:
        def __init__(self) -> None:
            self.snapshot: dict | None = None

        async def analyze(self, *, transcript_snapshot: dict, reference_date: str | None = None):
            _ = reference_date
            self.snapshot = transcript_snapshot
            assert transcript_snapshot["handoffs"][0]["handoff_id"] == "handoff_service_1"
            human_turns = [
                turn
                for turn in transcript_snapshot["turns"]
                if turn["speaker_type"] == "human_agent"
            ]
            assert human_turns[0]["role"] == "assistant"
            assert human_turns[0]["handoff_id"] == "handoff_service_1"
            return {
                "summary": "客户要求转人工，并在人工阶段确认合同审查能力。",
                "feedback_type": "正向",
                "key_points": ["客户关注合同审查能力"],
                "time_hint": {},
                "tags": ["转人工已接通"],
            }

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        started_at = datetime(2026, 7, 9, 10, 0, tzinfo=timezone.utc)
        await repository.upsert_dialogue_segment(
            call_id="call_semantic_handoff_service",
            segment_no=1,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="qwen_realtime",
            source_segment_id="rt_customer_handoff",
            segment_text="帮我转人工。",
            segment_status="final",
            started_at=started_at + timedelta(seconds=8),
            ended_at=started_at + timedelta(seconds=9),
            duration_ms=1000,
        )
        await repository.upsert_dialogue_segment(
            call_id="call_semantic_handoff_service",
            segment_no=2,
            speaker_type="human_agent",
            speaker_identity="agent-debug-001",
            source="offline_asr",
            source_segment_id="human_agent_1",
            segment_text="您好，我是人工顾问，我来继续沟通。",
            segment_status="final",
            started_at=started_at + timedelta(seconds=35),
            ended_at=started_at + timedelta(seconds=38),
            duration_ms=3000,
        )
        await repository.upsert_dialogue_segment(
            call_id="call_semantic_handoff_service",
            segment_no=3,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="offline_asr",
            source_segment_id="offline_customer_handoff",
            segment_text="我想确认你们能不能做合同审查。",
            segment_status="final",
            started_at=started_at + timedelta(seconds=40),
            ended_at=started_at + timedelta(seconds=43),
            duration_ms=3000,
        )
        await repository.create_handoff(
            handoff_id="handoff_service_1",
            call_id="call_semantic_handoff_service",
            room_name="room_semantic_handoff_service",
            status="connected",
            request_source="customer",
            request_reason="customer_request",
            request_message="用户明确要求转人工",
            requested_at=started_at + timedelta(seconds=10),
            expires_at=started_at + timedelta(seconds=130),
        )
        await repository.update_handoff(
            "handoff_service_1",
            human_agent_identity="agent-debug-001",
            accepted_at=started_at + timedelta(seconds=20),
            connected_at=started_at + timedelta(seconds=30),
        )

        analyzer = FakeAnalyzer()
        service = module.AiCallSemanticAnalysisService(repository, analyzer=analyzer)
        analysis = await service.analyze_call_once(
            call_id="call_semantic_handoff_service",
            scene_code="intro_contract",
            now=started_at + timedelta(seconds=50),
        )

        assert analysis.analysis_status == module.ANALYSIS_STATUS_SUCCEEDED
        assert analyzer.snapshot is not None
        assert analysis.transcript_snapshot_dict["handoffs"][0]["handoff_id"] == (
            "handoff_service_1"
        )

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


def test_semantic_analysis_record_query_endpoint_returns_existing_analysis() -> None:
    class FakeAiCallService:
        async def require_record_for_tenant(self, **query) -> None:
            assert query == {
                "tenant_id": "tenant-a",
                "call_id": "call_semantic_api",
            }

        async def get_record_semantic_analysis(self, call_id: str):
            assert call_id == "call_semantic_api"
            return {
                "callId": call_id,
                "sceneCode": "intro_geo",
                "analysisSceneCode": "ai_call_semantic_analysis",
                "analysisStatus": "2",
                "analysisResult": {
                    "summary": "客户表示下午方便沟通。",
                    "feedback_type": "正向",
                    "key_points": ["客户下午方便沟通"],
                    "time_hint": {
                        "time_text": "下午",
                        "time_value": "",
                        "original_texts": ["下午再联系"],
                    },
                    "tags": ["可跟进"],
                },
                "analysisError": None,
                "analysisRetryCount": 0,
                "analysisStartedAt": None,
                "analysisFinishedAt": None,
                "transcriptHash": "hash_api",
                "transcriptSnapshot": {"turns": []},
                "createdAt": None,
                "updatedAt": None,
            }

    app = FastAPI()
    app.dependency_overrides[get_ai_call_service] = lambda: FakeAiCallService()
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(
        user=SimpleNamespace(tenant_id="tenant-a", user_id=1),
    )
    app.include_router(AiCallRouter)
    client = TestClient(app)

    response = client.get("/ai-call/records/call_semantic_api/semantic-analysis")

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["analysisStatus"] == "2"
    assert payload["data"]["analysisResult"]["feedback_type"] == "正向"


@pytest.mark.anyio
async def test_semantic_analysis_service_reanalyzes_succeeded_record() -> None:
    module = _semantic_module()
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    class FakeAnalyzer:
        def __init__(self) -> None:
            self.snapshots: list[dict] = []

        async def analyze(self, *, transcript_snapshot: dict, reference_date: str | None = None):
            self.snapshots.append(transcript_snapshot)
            assert reference_date == "2026-07-08"
            user_texts = [
                turn["text"]
                for turn in transcript_snapshot["turns"]
                if turn["role"] == "user"
            ]
            assert user_texts == ["可以知道了。"]
            return {
                "summary": "客户表示可以知道了。",
                "feedback_type": "中性",
                "key_points": [],
                "time_hint": {},
                "tags": ["重分析"],
            }

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        await repository.ensure_semantic_analysis_record(
            call_id="call_semantic_reanalysis",
            scene_code="intro_document",
        )
        await repository.claim_semantic_analysis(
            call_id="call_semantic_reanalysis",
            now=datetime(2026, 7, 8, 10, 0, tzinfo=timezone.utc),
        )
        old = await repository.update_semantic_analysis_success(
            call_id="call_semantic_reanalysis",
            analysis_result={
                "summary": "客户问可以知道吗。",
                "feedback_type": "中性",
                "key_points": ["客户问可以知道吗"],
                "time_hint": {},
                "tags": [],
            },
            transcript_snapshot_json='{"turns":[{"role":"user","text":"可以知道吗？"}]}',
            transcript_hash="old_hash",
            now=datetime(2026, 7, 8, 10, 1, tzinfo=timezone.utc),
        )
        assert old is not None
        await repository.upsert_dialogue_segment(
            call_id="call_semantic_reanalysis",
            segment_no=1,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="qwen_realtime",
            source_segment_id="rt_customer_1",
            segment_text="可以知道了。",
            segment_status="final",
            started_at=datetime(2026, 7, 8, 10, 2, tzinfo=timezone.utc),
            ended_at=datetime(2026, 7, 8, 10, 3, tzinfo=timezone.utc),
            duration_ms=1000,
        )

        analyzer = FakeAnalyzer()
        service = module.AiCallSemanticAnalysisService(repository, analyzer=analyzer)
        refreshed = await service.reanalyze_call_once(
            call_id="call_semantic_reanalysis",
            scene_code="intro_document",
            reference_date="2026-07-08",
            now=datetime(2026, 7, 8, 10, 4, tzinfo=timezone.utc),
        )

        assert refreshed.id == old.id
        assert refreshed.analysis_status == module.ANALYSIS_STATUS_SUCCEEDED
        assert refreshed.analysis_version == 2
        assert refreshed.analysis_result_dict["summary"] == "客户表示可以知道了。"
        rendered = json.dumps(refreshed.analysis_result_dict, ensure_ascii=False)
        assert "可以知道吗" not in rendered
        assert refreshed.transcript_hash != "old_hash"
        assert refreshed.transcript_snapshot_dict["turns"][0]["text"] == "可以知道了。"
        assert len(analyzer.snapshots) == 1

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


def test_semantic_analysis_reanalyze_endpoint_returns_refreshed_analysis() -> None:
    class FakeAiCallService:
        async def require_record_for_tenant(self, **query) -> None:
            assert query == {
                "tenant_id": "tenant-a",
                "call_id": "call_semantic_api",
            }

        async def reanalyze_record_semantic_analysis(self, call_id: str):
            assert call_id == "call_semantic_api"
            return {
                "callId": call_id,
                "sceneCode": "intro_document",
                "analysisSceneCode": "ai_call_semantic_analysis",
                "analysisStatus": "2",
                "analysisResult": {
                    "summary": "客户表示可以知道了。",
                    "feedback_type": "中性",
                    "key_points": [],
                    "time_hint": {},
                    "tags": ["重分析"],
                },
                "analysisError": None,
                "analysisRetryCount": 0,
                "analysisStartedAt": None,
                "analysisFinishedAt": None,
                "transcriptHash": "new_hash",
                "transcriptSnapshot": {"turns": [{"role": "user", "text": "可以知道了。"}]},
                "createdAt": None,
                "updatedAt": None,
            }

    app = FastAPI()
    app.dependency_overrides[get_ai_call_service] = lambda: FakeAiCallService()
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(
        user=SimpleNamespace(tenant_id="tenant-a", user_id=1),
    )
    app.include_router(AiCallRouter)
    client = TestClient(app)

    response = client.post("/ai-call/records/call_semantic_api/semantic-analysis/reanalyze")

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["analysisStatus"] == "2"
    assert payload["data"]["analysisResult"]["summary"] == "客户表示可以知道了。"


@pytest.mark.anyio
async def test_openai_compatible_semantic_analyzer_posts_json_chat_completion() -> None:
    module = _semantic_module()
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        payload = json.loads(request.content)
        user_payload = json.loads(payload["messages"][1]["content"])

        assert str(request.url) == "https://dashscope.test/compatible-mode/v1/chat/completions"
        assert request.headers["authorization"] == "Bearer test-key"
        assert payload["model"] == "qwen-plus"
        assert payload["response_format"] == {"type": "json_object"}
        assert "客户价值分类字段" in payload["messages"][0]["content"]
        assert "不得输出 converted" in payload["messages"][0]["content"]
        assert user_payload["reference_date"] == "2026-07-02"
        assert user_payload["transcript_json"]["call_id"] == "call_semantic_llm"

        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "summary": "客户下午方便继续沟通。",
                                    "feedback_type": "正向",
                                    "key_points": ["客户愿意继续沟通"],
                                    "time_hint": {
                                        "time_text": "下午",
                                        "time_value": "",
                                        "original_texts": ["下午再联系"],
                                    },
                                    "tags": ["可跟进"],
                                },
                                ensure_ascii=False,
                            )
                        }
                    }
                ]
            },
        )

    analyzer = module.OpenAICompatibleSemanticAnalyzer(
        base_url="https://dashscope.test/compatible-mode/v1/",
        api_key="test-key",
        model="qwen-plus",
        timeout_seconds=1.0,
        transport=httpx.MockTransport(handler),
    )

    result = await analyzer.analyze(
        transcript_snapshot={
            "call_id": "call_semantic_llm",
            "scene_code": "intro_geo",
            "turns": [{"role": "user", "text": "下午再联系我"}],
            "metadata": {"fallback_to_realtime": False},
        },
        reference_date="2026-07-02",
    )

    assert len(requests) == 1
    assert result["feedback_type"] == "正向"
    assert result["time_hint"]["original_texts"] == ["下午再联系"]


@pytest.mark.anyio
async def test_semantic_analysis_worker_consumes_queue_and_marks_success(tmp_path) -> None:
    module = _semantic_module()
    db_path = tmp_path / "semantic_worker.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    class FakeAnalyzer:
        def __init__(self) -> None:
            self.snapshots: list[dict] = []

        async def analyze(self, *, transcript_snapshot: dict, reference_date: str | None = None):
            self.snapshots.append(transcript_snapshot)
            assert reference_date == "2026-07-02"
            return {
                "summary": "客户下午方便继续沟通。",
                "feedback_type": "正向",
                "key_points": ["客户下午方便沟通"],
                "time_hint": {
                    "time_text": "下午",
                    "time_value": "",
                    "original_texts": ["下午再联系"],
                },
                "tags": ["可跟进"],
            }

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        await repository.upsert_dialogue_segment(
            call_id="call_semantic_worker",
            segment_no=1,
            speaker_type="customer",
            speaker_identity="browser-call",
            source="qwen_realtime",
            source_segment_id="rt_customer_1",
            segment_text="下午再联系我。",
            segment_status="final",
            started_at=datetime(2026, 7, 2, 10, 0, tzinfo=timezone.utc),
            ended_at=datetime(2026, 7, 2, 10, 1, tzinfo=timezone.utc),
            duration_ms=1000,
        )
        await db.commit()

    analyzer = FakeAnalyzer()
    worker = module.AiCallSemanticAnalysisWorker(
        session_maker,
        analyzer=analyzer,
        enabled=True,
        queue_max_size=10,
        reference_date_factory=lambda: "2026-07-02",
    )
    worker.enqueue("call_semantic_worker", scene_code="intro_geo")

    assert await worker.process_one() is True

    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        analysis = await repository.get_semantic_analysis(call_id="call_semantic_worker")

        assert analysis is not None
        assert analysis.analysis_status == module.ANALYSIS_STATUS_SUCCEEDED
        assert analysis.analysis_result_dict["summary"] == "客户下午方便继续沟通。"
        assert analysis.transcript_snapshot_dict["metadata"]["fallback_to_realtime"] is True
        assert analysis.created_at <= analysis.analysis_started_at
        assert analysis.analysis_started_at < analysis.analysis_finished_at
    assert analyzer.snapshots[0]["scene_code"] == "intro_geo"

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_offline_asr_worker_enqueues_semantic_after_handoff_asr_snapshot_ready(
    monkeypatch,
    tmp_path,
) -> None:
    module = _semantic_module()
    offline_module = importlib.import_module("app.services.ai_call.offline_asr_service")
    db_path = tmp_path / "handoff_asr_to_semantic.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    async def fake_play_url(self, oss_id: int) -> str | None:
        _ = self
        return {
            101: "https://files.test/customer-track.ogg",
            102: "https://files.test/human-agent-track.ogg",
        }.get(oss_id)

    class FakeProvider:
        provider_name = "fake_asr"
        model_name = "fake-model"

        async def transcribe(self, *, audio_url: str):
            if "customer-track" in audio_url:
                return offline_module.OfflineAsrResult(
                    task_id="task-customer",
                    transcription_url="https://asr.test/customer.json",
                    segments=[
                        offline_module.OfflineAsrSegment(
                            text="我想确认你们能不能做合同审查。",
                            begin_time_ms=1000,
                            end_time_ms=3000,
                        )
                    ],
                )
            if "human-agent-track" in audio_url:
                return offline_module.OfflineAsrResult(
                    task_id="task-human-agent",
                    transcription_url="https://asr.test/human-agent.json",
                    segments=[
                        offline_module.OfflineAsrSegment(
                            text="您好，我是人工顾问，我来继续沟通。",
                            begin_time_ms=500,
                            end_time_ms=1800,
                        )
                    ],
                )
            raise AssertionError(f"unexpected ASR call: {audio_url}")

    class FakeAnalyzer:
        def __init__(self) -> None:
            self.snapshots: list[dict] = []

        async def analyze(self, *, transcript_snapshot: dict, reference_date: str | None = None):
            _ = reference_date
            self.snapshots.append(transcript_snapshot)
            human_turns = [
                turn
                for turn in transcript_snapshot["turns"]
                if turn["speaker_type"] == "human_agent"
            ]
            assert transcript_snapshot["handoffs"][0]["handoff_id"] == "handoff_worker_1"
            assert human_turns[0]["role"] == "assistant"
            assert human_turns[0]["handoff_id"] == "handoff_worker_1"
            assert "semantic_evidence" not in human_turns[0]
            assert transcript_snapshot["metadata"]["handoff_summary"] == {
                "has_handoff": True,
                "has_connected_handoff": True,
                "human_turn_count": 2,
                "human_agent_turn_count": 1,
                "human_transcript_status": "available",
            }
            return {
                "summary": "客户转人工后确认合同审查能力。",
                "feedback_type": "正向",
                "key_points": ["客户关注合同审查能力"],
                "time_hint": {},
                "tags": ["转人工已接通"],
            }

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr(offline_module.AiCallOfflineAsrService, "_play_url", fake_play_url)
    analyzer = FakeAnalyzer()
    semantic_worker = module.AiCallSemanticAnalysisWorker(
        session_maker,
        analyzer=analyzer,
        enabled=True,
        queue_max_size=10,
        reference_date_factory=lambda: "2026-07-09",
    )
    offline_worker = offline_module.AiCallOfflineAsrWorker(
        session_maker,
        provider=FakeProvider(),
        enabled=True,
        on_call_ready_for_semantic_analysis=semantic_worker.enqueue,
    )

    started_at = datetime(2026, 7, 9, 10, 0, tzinfo=timezone.utc)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        await repository.create_record(
            tenant_id="000000",
            call_id="call_after_handoff_asr",
            business_type=None,
            business_id=None,
            scene_code="intro_contract",
            entry_type="web",
            room_name="room_after_handoff_asr",
            participant_identity="browser-call-after-handoff-asr",
            status="completed",
            started_at=started_at,
        )
        await repository.create_handoff(
            handoff_id="handoff_worker_1",
            call_id="call_after_handoff_asr",
            room_name="room_after_handoff_asr",
            status="connected",
            request_source="customer",
            request_reason="customer_request",
            request_message="用户明确要求转人工",
            requested_at=started_at + timedelta(seconds=10),
            expires_at=started_at + timedelta(seconds=130),
        )
        await repository.update_handoff(
            "handoff_worker_1",
            human_agent_identity="agent-debug-001",
            accepted_at=started_at + timedelta(seconds=20),
            connected_at=started_at + timedelta(seconds=30),
            ended_at=started_at + timedelta(seconds=70),
        )
        customer_track = await repository.create_recording_track(
            tenant_id="000000",
            call_id="call_after_handoff_asr",
            room_name="room_after_handoff_asr",
            track_role="customer",
            participant_identity="browser-call-after-handoff-asr",
            status="completed",
            started_at=started_at + timedelta(seconds=30),
        )
        human_track = await repository.create_recording_track(
            tenant_id="000000",
            call_id="call_after_handoff_asr",
            room_name="room_after_handoff_asr",
            track_role="human_agent",
            participant_identity="human-agent-handoff-worker-1",
            handoff_id="handoff_worker_1",
            status="completed",
            started_at=started_at + timedelta(seconds=30),
        )
        await repository.update_recording_track(
            tenant_id="000000",
            track_id=customer_track.id,
            oss_id=101,
        )
        await repository.update_recording_track(
            tenant_id="000000",
            track_id=human_track.id,
            oss_id=102,
        )
        await db.commit()

    assert analyzer.snapshots == []

    await offline_worker._process_call("call_after_handoff_asr")

    assert analyzer.snapshots == []
    assert await semantic_worker.process_one() is True

    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        rows = await repository.list_dialogue_segments("call_after_handoff_asr")
        analysis = await repository.get_semantic_analysis(call_id="call_after_handoff_asr")

        assert [(row.source, row.speaker_type, row.segment_text) for row in rows] == [
            ("offline_asr", "human_agent", "您好，我是人工顾问，我来继续沟通。"),
            ("offline_asr", "customer", "我想确认你们能不能做合同审查。"),
        ]
        assert analysis is not None
        assert analysis.analysis_status == module.ANALYSIS_STATUS_SUCCEEDED
        assert analysis.scene_code == "intro_contract"
        assert analysis.transcript_snapshot_dict["handoffs"][0]["handoff_id"] == (
            "handoff_worker_1"
        )
        assert any(
            turn["speaker_type"] == "human_agent"
            and turn["handoff_id"] == "handoff_worker_1"
            for turn in analysis.transcript_snapshot_dict["turns"]
        )
    assert len(analyzer.snapshots) == 1

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()
