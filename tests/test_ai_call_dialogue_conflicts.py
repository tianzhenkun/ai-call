from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.api.v1.ai_call.model import AiCallDialogueSegmentModel
from app.services.ai_call.dialogue_merge import (
    is_cross_source_customer_transcript_conflict,
)
from app.services.ai_call.dialogue_service import AiCallDialogueService
from app.services.ai_call.semantic_analysis import SemanticTranscriptBuilder

BASE_TIME = datetime(2026, 7, 30, 7, 23, tzinfo=timezone.utc)


def _segment(
    *,
    segment_no: int,
    source: str,
    text: str,
    started_ms: int,
    ended_ms: int,
) -> AiCallDialogueSegmentModel:
    return AiCallDialogueSegmentModel(
        id=segment_no,
        call_id="call_transcript_conflict",
        segment_no=segment_no,
        speaker_type="customer",
        speaker_identity="sip-call_transcript_conflict",
        source=source,
        source_segment_id=f"{source}_{segment_no}",
        segment_text=text,
        segment_status="final",
        started_at=BASE_TIME + timedelta(milliseconds=started_ms),
        ended_at=BASE_TIME + timedelta(milliseconds=ended_ms),
        duration_ms=ended_ms - started_ms,
    )


@pytest.mark.parametrize(
    (
        "offline_text",
        "realtime_text",
        "offline_ended_ms",
        "realtime_started_ms",
    ),
    [
        ("品牌推荐吗？", "怎么推荐吗？", 560, 879),
        ("也是吧。", "演示吧。", 320, 643),
        ("这大强手机号。", "就单签手机号。", 640, 1007),
    ],
)
def test_detects_reported_nearby_cross_source_customer_conflicts(
    offline_text: str,
    realtime_text: str,
    offline_ended_ms: int,
    realtime_started_ms: int,
) -> None:
    assert is_cross_source_customer_transcript_conflict(
        source="offline_asr",
        speaker_type="customer",
        text=offline_text,
        started_at=BASE_TIME,
        ended_at=BASE_TIME + timedelta(milliseconds=offline_ended_ms),
        candidate_source="qwen_realtime",
        candidate_speaker_type="customer",
        candidate_text=realtime_text,
        candidate_started_at=BASE_TIME + timedelta(milliseconds=realtime_started_ms),
        candidate_ended_at=BASE_TIME
        + timedelta(milliseconds=realtime_started_ms + 1000),
    )


def test_does_not_merge_distinct_nearby_customer_utterances() -> None:
    assert not is_cross_source_customer_transcript_conflict(
        source="offline_asr",
        speaker_type="customer",
        text="稍后再联系吧。",
        started_at=BASE_TIME,
        ended_at=BASE_TIME + timedelta(milliseconds=1000),
        candidate_source="qwen_realtime",
        candidate_speaker_type="customer",
        candidate_text="没有其他问题了。",
        candidate_started_at=BASE_TIME + timedelta(milliseconds=1300),
        candidate_ended_at=BASE_TIME + timedelta(milliseconds=2400),
    )


def test_does_not_mark_combined_offline_segment_as_conflict() -> None:
    assert not is_cross_source_customer_transcript_conflict(
        source="offline_asr",
        speaker_type="customer",
        text="好的，可以，没有了，挂了吧。",
        started_at=BASE_TIME,
        ended_at=BASE_TIME + timedelta(milliseconds=2400),
        candidate_source="qwen_realtime",
        candidate_speaker_type="customer",
        candidate_text="没有了，挂了吧。",
        candidate_started_at=BASE_TIME + timedelta(milliseconds=2673),
        candidate_ended_at=BASE_TIME + timedelta(milliseconds=4115),
    )


def test_dialogue_query_keeps_realtime_and_hides_nearby_offline_conflict() -> None:
    offline = _segment(
        segment_no=1,
        source="offline_asr",
        text="这大强手机号。",
        started_ms=0,
        ended_ms=640,
    )
    realtime = _segment(
        segment_no=2,
        source="qwen_realtime",
        text="就单签手机号。",
        started_ms=1007,
        ended_ms=2439,
    )

    rows = AiCallDialogueService._canonical_segments([offline, realtime])

    assert [(row.source, row.segment_text) for row in rows] == [
        ("qwen_realtime", "就单签手机号。"),
    ]


def test_semantic_snapshot_marks_nearby_cross_source_conflict() -> None:
    offline = _segment(
        segment_no=1,
        source="offline_asr",
        text="品牌推荐吗？",
        started_ms=0,
        ended_ms=560,
    )
    realtime = _segment(
        segment_no=2,
        source="qwen_realtime",
        text="怎么推荐吗？",
        started_ms=879,
        ended_ms=2151,
    )

    snapshot = SemanticTranscriptBuilder().build(
        call_id="call_transcript_conflict",
        scene_code="intro_geo",
        rows=[offline, realtime],
    )

    customer_turns = [
        turn for turn in snapshot["turns"] if turn["speaker_type"] == "customer"
    ]
    assert [turn["text"] for turn in customer_turns] == ["怎么推荐吗？"]
    assert customer_turns[0]["source_decision"]["fallback_reason"] == (
        "nearby_transcript_conflict"
    )
    assert customer_turns[0]["semantic_evidence"]["source_conflict"] is True
    assert snapshot["metadata"]["offline_asr_quality_rejected_count"] == 1
