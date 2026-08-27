from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.api.v1.ai_call.model import AiCallDialogueSegmentModel
from app.api.v1.ai_call.service import AiCallService
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
    speaker_type: str = "customer",
) -> AiCallDialogueSegmentModel:
    return AiCallDialogueSegmentModel(
        id=segment_no,
        call_id="call_transcript_conflict",
        segment_no=segment_no,
        speaker_type=speaker_type,
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


def test_dialogue_query_keeps_later_offline_customer_after_unbounded_realtime_segment() -> None:
    stale_realtime = _segment(
        segment_no=1,
        source="qwen_realtime",
        text="转人工。",
        started_ms=1000,
        ended_ms=180000,
    )
    later_offline = _segment(
        segment_no=2,
        source="offline_asr",
        text="好的，可以。",
        started_ms=60000,
        ended_ms=61000,
    )

    rows = AiCallDialogueService._canonical_segments(
        [stale_realtime, later_offline]
    )

    assert [(row.source, row.segment_text) for row in rows] == [
        ("qwen_realtime", "转人工。"),
        ("offline_asr", "好的，可以。"),
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


def test_availability_short_answer_prefers_offline_asr_over_garbled_realtime() -> None:
    offline = _segment(
        segment_no=1,
        source="offline_asr",
        text="方便。",
        started_ms=0,
        ended_ms=240,
    )
    realtime = _segment(
        segment_no=2,
        source="qwen_realtime",
        text="大面。",
        started_ms=674,
        ended_ms=1383,
    )

    selected = SemanticTranscriptBuilder().select_customer_rows([offline, realtime])
    displayed = AiCallDialogueService._canonical_segments([offline, realtime])

    assert [(row.source, row.segment_text) for row in selected] == [
        ("offline_asr", "方便。"),
    ]
    assert [(row.source, row.segment_text) for row in displayed] == [
        ("offline_asr", "方便。"),
    ]


@pytest.mark.anyio
async def test_record_dialogue_uses_semantic_customer_selection_and_time_order() -> None:
    opening = _segment(
        segment_no=1,
        source="qwen_realtime",
        speaker_type="ai",
        text="您好，请问现在方便沟通吗？",
        started_ms=0,
        ended_ms=1000,
    )
    realtime = _segment(
        segment_no=2,
        source="qwen_realtime",
        text="啥呀。",
        started_ms=6569,
        ended_ms=7349,
    )
    reply = _segment(
        segment_no=3,
        source="qwen_realtime",
        speaker_type="ai",
        text="没关系，我简单介绍一下。",
        started_ms=7800,
        ended_ms=9000,
    )
    offline = _segment(
        segment_no=10,
        source="offline_asr",
        text="啥呀。",
        started_ms=5100,
        ended_ms=5340,
    )

    class Repository:
        async def list_dialogue_segments(self, *_args, **_kwargs):
            return [opening, realtime, reply, offline]

    service = AiCallService(
        object(),
        dialogue_service=AiCallDialogueService(Repository()),
    )

    result = await service.list_record_dialogue_segments("call_transcript_conflict")

    assert [(row["source"], row["text"]) for row in result["rows"]] == [
        ("qwen_realtime", "您好，请问现在方便沟通吗？"),
        ("offline_asr", "啥呀。"),
        ("qwen_realtime", "没关系，我简单介绍一下。"),
    ]


def test_semantic_customer_selection_keeps_distinct_repeated_text() -> None:
    offline = _segment(
        segment_no=1,
        source="offline_asr",
        text="行。",
        started_ms=0,
        ended_ms=400,
    )
    later_realtime = _segment(
        segment_no=2,
        source="qwen_realtime",
        text="行。",
        started_ms=5000,
        ended_ms=5400,
    )

    rows = SemanticTranscriptBuilder().select_customer_rows(
        [offline, later_realtime]
    )

    assert [(row.source, row.segment_text) for row in rows] == [
        ("offline_asr", "行。"),
        ("qwen_realtime", "行。"),
    ]
