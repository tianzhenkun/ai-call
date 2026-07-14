from __future__ import annotations

import io
import json
from typing import Any

from tools.ai_call_semantic_acceptance import (
    _absent_user_quote_issues,
    _assistant_customer_voice_issues,
    _human_agent_crosstalk_contract_issues,
    _record_only_leak_issues,
    _segment_row,
    _semantic_result_issues,
    run,
)


def test_semantic_acceptance_cli_flags_assistant_customer_voice_and_writes_report(tmp_path) -> None:
    requested_urls: list[str] = []
    output_path = tmp_path / "semantic-acceptance.md"
    json_output_path = tmp_path / "semantic-acceptance.json"

    def fake_get_json(url: str, timeout_seconds: float) -> dict[str, Any]:
        requested_urls.append(url)
        assert timeout_seconds == 7.0
        if url.endswith("/ai-call/records?entryType=web&pageSize=2"):
            return {
                "code": 200,
                "data": {
                    "rows": [
                        {"callId": "call_clean"},
                        {"callId": "call_voice"},
                    ],
                    "total": 2,
                },
            }
        if url.endswith("/ai-call/records/call_clean"):
            return {
                "code": 200,
                "data": {
                    "record": {
                        "callId": "call_clean",
                        "entryType": "web",
                        "sceneCode": "intro_document",
                        "status": "completed",
                    }
                },
            }
        if url.endswith("/ai-call/records/call_clean/events?limit=1000"):
            return {
                "code": 200,
                "data": {
                    "rows": [
                        _event("user_speech_started", "2026-07-08T08:00:00.000Z"),
                        _event("user_speech_stopped", "2026-07-08T08:00:00.600Z"),
                        _event("browser_first_audio", "2026-07-08T08:00:01.000Z"),
                    ]
                },
            }
        if url.endswith("/ai-call/records/call_clean/dialogue-segments?limit=1000"):
            return {
                "code": 200,
                "data": {
                    "rows": [
                        _segment("call_clean", 1, "ai", "您好，请问现在方便吗？", 0, 1),
                        _segment("call_clean", 2, "customer", "方便。", 2, 3),
                    ],
                    "total": 2,
                },
            }
        if url.endswith("/ai-call/records/call_clean/semantic-analysis"):
            return {
                "code": 200,
                "data": {
                    "callId": "call_clean",
                    "analysisStatus": "2",
                    "analysisResult": {
                        "summary": "客户表示现在方便沟通。",
                        "feedback_type": "正向",
                        "key_points": ["客户表示方便沟通"],
                        "time_hint": {},
                        "tags": [],
                    },
                    "transcriptSnapshot": {},
                },
            }
        if url.endswith("/ai-call/records/call_voice"):
            return {
                "code": 200,
                "data": {
                    "record": {
                        "callId": "call_voice",
                        "entryType": "web",
                        "sceneCode": "intro_contract",
                        "status": "completed",
                    }
                },
            }
        if url.endswith("/ai-call/records/call_voice/events?limit=1000"):
            return {
                "code": 200,
                "data": {
                    "rows": [
                        _event("user_speech_started", "2026-07-08T08:01:00.000Z"),
                        _event("user_speech_stopped", "2026-07-08T08:01:00.400Z"),
                        _event("browser_first_audio", "2026-07-08T08:01:01.000Z"),
                    ]
                },
            }
        if url.endswith("/ai-call/records/call_voice/dialogue-segments?limit=1000"):
            return {
                "code": 200,
                "data": {
                    "rows": [
                        _segment(
                            "call_voice",
                            1,
                            "ai",
                            "我们这边合同量比较大，法务人手紧张，经常担心漏审关键条款。",
                            0,
                            4,
                        ),
                        _segment("call_voice", 2, "customer", "你好。", 5, 6),
                    ],
                    "total": 2,
                },
            }
        if url.endswith("/ai-call/records/call_voice/semantic-analysis"):
            return {
                "code": 200,
                "data": {
                    "callId": "call_voice",
                    "analysisStatus": "2",
                    "analysisResult": {
                        "summary": "客户表示我们这边合同量比较大，法务人手紧张。",
                        "feedback_type": "中性",
                        "key_points": ["客户合同量比较大"],
                        "time_hint": {},
                        "tags": [],
                    },
                    "transcriptSnapshot": {
                        "turns": [
                            {
                                "role": "user",
                                "text": "旧的客户文本。",
                                "source": "offline_asr",
                                "segment_status": "final",
                            }
                        ]
                    },
                },
            }
        raise AssertionError(f"unexpected url: {url}")

    stdout = io.StringIO()
    exit_code = run(
        [
            "--base-url",
            "http://127.0.0.1:19012",
            "--recent",
            "2",
            "--entry-type",
            "web",
            "--timeout-seconds",
            "7",
            "--output",
            str(output_path),
            "--json-output",
            str(json_output_path),
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    assert exit_code == 1
    output = stdout.getvalue()
    assert "semantic_acceptance calls=2 verdict=FAIL high=2 review=0 fetchFailed=0" in output
    markdown = output_path.read_text(encoding="utf-8")
    assert "# AI Call 语义分析 P1 验收报告" in markdown
    assert "assistant_customer_voice_risk" in markdown
    assert "assistant_text_leaked_into_semantic_result" in markdown
    assert "stale_stored_transcript_snapshot" not in markdown
    report = json.loads(json_output_path.read_text(encoding="utf-8"))
    voice_call = next(call for call in report["calls"] if call["callId"] == "call_voice")
    assert voice_call["semanticSnapshot"]["source"] == "stored_semantic_analysis"
    assert voice_call["snapshotComparison"] == {
        "storedPresent": True,
        "storedDiffersFromRebuilt": True,
    }
    assert requested_urls == [
        "http://127.0.0.1:19012/ai-call/records?entryType=web&pageSize=2",
        "http://127.0.0.1:19012/ai-call/records/call_clean",
        "http://127.0.0.1:19012/ai-call/records/call_clean/events?limit=1000",
        "http://127.0.0.1:19012/ai-call/records/call_clean/dialogue-segments?limit=1000",
        "http://127.0.0.1:19012/ai-call/records/call_clean/semantic-analysis",
        "http://127.0.0.1:19012/ai-call/records/call_voice",
        "http://127.0.0.1:19012/ai-call/records/call_voice/events?limit=1000",
        "http://127.0.0.1:19012/ai-call/records/call_voice/dialogue-segments?limit=1000",
        "http://127.0.0.1:19012/ai-call/records/call_voice/semantic-analysis",
    ]


def test_assistant_customer_voice_ignores_provider_question_about_customer_pain() -> None:
    snapshot = {
        "turns": [
            {
                "seq": 1,
                "role": "assistant",
                "source": "qwen_realtime",
                "text": (
                    "好的，那我用一句话说明：我们的合同智能审查能在签约前帮您快速识别"
                    "合同里的关键风险。您目前团队在合同审核上主要痛点是流转慢还是担心漏审风险？"
                ),
            }
        ]
    }

    assert _assistant_customer_voice_issues(snapshot) == []


def test_absent_user_quote_accepts_quote_contained_in_combined_user_turn() -> None:
    result = {
        "summary": "",
        "key_points": [
            "客户连续询问试用/演示相关问题（‘我说可以试用吗？’‘有试用版吗？’）。",
        ],
    }
    snapshot = {
        "turns": [
            {
                "role": "user",
                "text": "我说可以试用吗？有试用版吗？",
            }
        ]
    }

    assert _absent_user_quote_issues(result, snapshot) == []


def test_record_only_leak_flags_short_non_background_text() -> None:
    result = {
        "summary": "客户说不帮我了。",
        "key_points": ["客户说不帮我了"],
    }
    snapshot = {
        "turns": [
            {
                "seq": 1,
                "role": "user",
                "text": "不帮我了。",
                "semantic_evidence": {
                    "analysis_usage": "record_only",
                },
            }
        ]
    }

    issues = _record_only_leak_issues(result, snapshot)

    assert issues[0]["type"] == "record_only_user_text_leaked_into_semantic_result"


def test_human_agent_crosstalk_contract_flags_unmarked_english_overlap() -> None:
    rows = [
        _segment_row(
            {
                **_segment("call_crosstalk", 1, "customer", "喂，郭说话听见了不？", 47, 53),
                "source": "offline_asr",
            },
            fallback_call_id="call_crosstalk",
            index=0,
        ),
        _segment_row(
            {
                **_segment("call_crosstalk", 2, "human_agent", "Why?", 48, 49),
                "source": "offline_asr",
            },
            fallback_call_id="call_crosstalk",
            index=1,
        ),
    ]
    snapshot = {
        "turns": [
            {
                "seq": 2,
                "role": "assistant",
                "speaker_type": "human_agent",
                "text": "Why?",
                "source": "offline_asr",
                "segment_status": "final",
            }
        ],
        "metadata": {"transcript_quality": {"signals": [], "reasons": []}},
    }

    issues = _human_agent_crosstalk_contract_issues(rows, snapshot)

    assert issues == [
        {
            "type": "human_agent_crosstalk_not_marked_low_confidence",
            "severity": "high",
            "turnSeq": 2,
            "text": "Why?",
            "reason": (
                "human_agent offline ASR has English-like overlap with customer audio "
                "but snapshot lacks low-confidence crosstalk marker"
            ),
        }
    ]


def test_human_agent_crosstalk_contract_keeps_normal_chinese_overlap_clean() -> None:
    rows = [
        _segment_row(
            {
                **_segment("call_normal_overlap", 1, "customer", "好的，我继续说一下需求。", 47, 50),
                "source": "offline_asr",
            },
            fallback_call_id="call_normal_overlap",
            index=0,
        ),
        _segment_row(
            {
                **_segment(
                    "call_normal_overlap",
                    2,
                    "human_agent",
                    "我这边能听到，稍等我帮您看一下。",
                    47,
                    50,
                ),
                "source": "offline_asr",
            },
            fallback_call_id="call_normal_overlap",
            index=1,
        ),
    ]

    assert _human_agent_crosstalk_contract_issues(rows, {"turns": []}) == []


def test_semantic_result_quote_accepts_text_present_in_stored_snapshot() -> None:
    analysis = {
        "analysisStatus": "2",
        "analysisResult": {
            "summary": "",
            "feedback_type": "正向",
            "key_points": ["客户询问‘有试用版吗？’。"],
            "time_hint": {},
            "tags": [],
        },
        "transcriptSnapshot": {
            "turns": [
                {
                    "role": "user",
                    "text": "有试用版吗？",
                    "source": "offline_asr",
                    "segment_status": "final",
                }
            ]
        },
    }
    rebuilt_snapshot = {
        "turns": [
            {
                "role": "user",
                "text": "可以试用吗？",
                "source": "offline_asr",
                "segment_status": "final",
                "semantic_evidence": {
                    "analysis_usage": "use_as_customer_signal",
                },
            }
        ]
    }

    issue_types = {
        issue["type"]
        for issue in _semantic_result_issues(analysis, rebuilt_snapshot)
    }

    assert "semantic_result_quote_absent_from_rebuilt_user_turns" not in issue_types


def _event(event_type: str, event_time: str) -> dict[str, Any]:
    return {
        "eventType": event_type,
        "source": "provider",
        "eventTime": event_time,
        "payload": {},
    }


def _segment(
    call_id: str,
    segment_no: int,
    speaker_type: str,
    text: str,
    started_offset_seconds: int,
    ended_offset_seconds: int,
) -> dict[str, Any]:
    return {
        "callId": call_id,
        "segmentNo": segment_no,
        "speakerType": speaker_type,
        "speakerIdentity": "agent-call" if speaker_type == "ai" else "browser-call",
        "source": "qwen_realtime",
        "sourceSegmentId": f"{speaker_type}_{segment_no}",
        "segmentText": text,
        "segmentStatus": "final",
        "startedAt": f"2026-07-08T08:00:{started_offset_seconds:02d}+00:00",
        "endedAt": f"2026-07-08T08:00:{ended_offset_seconds:02d}+00:00",
        "durationMs": (ended_offset_seconds - started_offset_seconds) * 1000,
    }
