from __future__ import annotations

import io
from typing import Any

from app.services.ai_call.record_service import PERSISTED_EVENT_TYPES
from app.services.ai_call.timeline_audit import build_timeline_audit
from tools.ai_call_timeline_audit import run


def test_timeline_audit_defer_evidence_event_is_persistable() -> None:
    assert "no_barge_unstarted_response_deferred" in PERSISTED_EVENT_TYPES


def test_timeline_audit_flags_ai_reply_started_during_customer_speech() -> None:
    report = build_timeline_audit(
        call_id="call_overlap",
        record={"callId": "call_overlap", "entryType": "web", "startedAt": "2026-07-08T06:39:00Z"},
        events=[
            _event("user_speech_started", "2026-07-08T06:39:12.083Z"),
            _event(
                "model_response_started",
                "2026-07-08T06:39:12.148Z",
                response_id="resp_overlap",
            ),
            _event("user_speech_stopped", "2026-07-08T06:39:14.338Z"),
        ],
        dialogue_segments=[],
    )

    assert report["passed"] is False
    assert report["summary"]["highSeverityCount"] == 1
    issue = report["issues"][0]
    assert issue["type"] == "ai_started_during_customer_speech"
    assert issue["severity"] == "high"
    assert issue["responseId"] == "resp_overlap"
    assert issue["speechStartTime"] == "2026-07-08T06:39:12.083Z"
    assert issue["speechStopTime"] == "2026-07-08T06:39:14.338Z"
    assert issue["deltaMsFromSpeechStart"] == 65


def test_timeline_audit_treats_deferred_unstarted_response_as_covered_overlap() -> None:
    report = build_timeline_audit(
        call_id="call_covered",
        record={"callId": "call_covered", "entryType": "web"},
        events=[
            _event("user_speech_started", "2026-07-08T06:39:12.083Z"),
            _event(
                "model_response_started",
                "2026-07-08T06:39:12.148Z",
                response_id="resp_old",
            ),
            _event(
                "no_barge_unstarted_response_deferred",
                "2026-07-08T06:39:12.149Z",
                response_id="resp_old",
            ),
            _event(
                "stale_audio_dropped",
                "2026-07-08T06:39:12.220Z",
                response_id="resp_old",
                reason="cancelled_response",
            ),
            _event("user_speech_stopped", "2026-07-08T06:39:14.338Z"),
        ],
        dialogue_segments=[],
    )

    assert report["passed"] is True
    assert report["summary"]["coveredOverlapCount"] == 1
    assert report["summary"]["highSeverityCount"] == 0
    assert report["issues"] == []


def test_timeline_audit_flags_unexpected_stale_audio_drop() -> None:
    report = build_timeline_audit(
        call_id="call_stale",
        record={"callId": "call_stale", "entryType": "web"},
        events=[
            _event(
                "stale_audio_dropped",
                "2026-07-08T06:40:01.000Z",
                response_id="resp_late",
                reason="session_not_ai_speaking",
                delta_bytes=512,
            ),
        ],
        dialogue_segments=[],
    )

    assert report["passed"] is False
    assert report["summary"]["staleAudioDropCount"] == 1
    issue = report["issues"][0]
    assert issue["type"] == "unexpected_stale_audio_drop"
    assert issue["reason"] == "session_not_ai_speaking"
    assert issue["severity"] == "high"
    assert issue["deltaBytes"] == 512


def test_timeline_audit_groups_repeated_stale_audio_drops() -> None:
    report = build_timeline_audit(
        call_id="call_repeated_stale",
        record={"callId": "call_repeated_stale", "entryType": "web"},
        events=[
            _event(
                "stale_audio_dropped",
                "2026-07-08T06:40:01.000Z",
                response_id="resp_late",
                reason="session_not_ai_speaking",
                delta_bytes=512,
            ),
            _event(
                "stale_audio_dropped",
                "2026-07-08T06:40:01.050Z",
                response_id="resp_late",
                reason="session_not_ai_speaking",
                delta_bytes=256,
            ),
        ],
        dialogue_segments=[],
    )

    assert report["summary"]["issueCount"] == 1
    issue = report["issues"][0]
    assert issue["dropCount"] == 2
    assert issue["deltaBytes"] == 768


def test_timeline_audit_handles_db_style_events_with_missing_time() -> None:
    report = build_timeline_audit(
        call_id="call_db_shape",
        record={"callId": "call_db_shape", "entryType": "web"},
        events=[
            {"event_type": "model_session_started", "payload_json": "{}"},
            {
                "event_type": "user_speech_started",
                "event_time": "2026-07-08T06:39:12.083Z",
                "payload_json": "{}",
            },
            {
                "event_type": "model_response_started",
                "event_time": "2026-07-08T06:39:12.148Z",
                "payload_json": '{"response": {"id": "resp_db"}}',
            },
            {
                "event_type": "user_speech_stopped",
                "event_time": "2026-07-08T06:39:14.338Z",
                "payload_json": "{}",
            },
        ],
        dialogue_segments=[],
    )

    assert report["passed"] is False
    assert report["issues"][0]["responseId"] == "resp_db"


def test_timeline_audit_flags_slow_first_audio_after_customer_turn() -> None:
    report = build_timeline_audit(
        call_id="call_slow_audio",
        record={"callId": "call_slow_audio", "entryType": "web"},
        events=[
            _event("user_speech_started", "2026-07-08T07:20:32.011Z"),
            _event("user_speech_stopped", "2026-07-08T07:20:33.285Z"),
            _event(
                "call_end_tool_ignored",
                "2026-07-08T07:20:35.192Z",
                reason="customer_end_without_terminal_user_signal",
            ),
            _event(
                "model_response_started",
                "2026-07-08T07:20:46.009Z",
                response_id="resp_recovery",
            ),
            _event("browser_first_audio", "2026-07-08T07:20:46.970Z"),
        ],
        dialogue_segments=[],
    )

    assert report["passed"] is False
    issue = report["issues"][0]
    assert issue["type"] == "slow_ai_first_audio_after_customer_turn"
    assert issue["severity"] == "high"
    assert issue["customerStopTime"] == "2026-07-08T07:20:33.285Z"
    assert issue["firstAudioTime"] == "2026-07-08T07:20:46.970Z"
    assert issue["customerStopToFirstAudioMs"] == 13685
    assert issue["reason"] == "call_end_tool_ignored_before_next_audio"


def test_timeline_audit_does_not_flag_when_customer_quickly_continues_turn() -> None:
    report = build_timeline_audit(
        call_id="call_customer_continues",
        record={"callId": "call_customer_continues", "entryType": "web"},
        events=[
            _event("user_speech_started", "2026-07-08T07:20:10.508Z"),
            _event("user_speech_stopped", "2026-07-08T07:20:10.600Z"),
            _event("user_speech_started", "2026-07-08T07:20:11.000Z"),
            _event("user_speech_stopped", "2026-07-08T07:20:12.000Z"),
            _event("browser_first_audio", "2026-07-08T07:20:16.100Z"),
        ],
        dialogue_segments=[],
    )

    assert report["passed"] is True
    assert report["issues"] == []


def test_timeline_audit_cli_summarizes_single_call() -> None:
    requested_urls: list[str] = []

    def fake_get_json(url: str, timeout_seconds: float) -> dict[str, Any]:
        requested_urls.append(url)
        assert timeout_seconds == 7.0
        if url.endswith("/ai-call/records/call_cli"):
            return {
                "code": 200,
                "data": {
                    "record": {
                        "callId": "call_cli",
                        "entryType": "web",
                        "status": "completed",
                        "startedAt": "2026-07-08T06:39:00Z",
                    }
                },
            }
        if url.endswith("/ai-call/records/call_cli/events?limit=1000"):
            return {
                "code": 200,
                "data": {
                    "rows": [
                        _event("user_speech_started", "2026-07-08T06:39:12.083Z"),
                        _event(
                            "model_response_started",
                            "2026-07-08T06:39:12.148Z",
                            response_id="resp_overlap",
                        ),
                        _event("user_speech_stopped", "2026-07-08T06:39:14.338Z"),
                    ]
                },
            }
        if url.endswith("/ai-call/records/call_cli/dialogue-segments?limit=1000"):
            return {"code": 200, "data": {"rows": [], "total": 0}}
        raise AssertionError(f"unexpected url: {url}")

    stdout = io.StringIO()
    exit_code = run(
        [
            "--base-url",
            "http://127.0.0.1:19012",
            "--call-id",
            "call_cli",
            "--timeout-seconds",
            "7",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    assert exit_code == 0
    output = stdout.getvalue()
    assert "timeline_audit callId=call_cli passed=false issues=1 high=1 covered=0" in output
    assert "issue type=ai_started_during_customer_speech severity=high responseId=resp_overlap" in output
    assert requested_urls == [
        "http://127.0.0.1:19012/ai-call/records/call_cli",
        "http://127.0.0.1:19012/ai-call/records/call_cli/events?limit=1000",
        "http://127.0.0.1:19012/ai-call/records/call_cli/dialogue-segments?limit=1000",
    ]


def test_timeline_audit_cli_summarizes_recent_web_calls() -> None:
    requested_urls: list[str] = []

    def fake_get_json(url: str, timeout_seconds: float) -> dict[str, Any]:
        requested_urls.append(url)
        assert timeout_seconds == 7.0
        if url.endswith("/ai-call/records?entryType=web&pageSize=2"):
            return {
                "code": 200,
                "data": {
                    "rows": [
                        {"callId": "call_clean"},
                        {"callId": "call_bad"},
                    ],
                    "total": 2,
                },
            }
        if url.endswith("/ai-call/records/call_clean"):
            return {"code": 200, "data": {"record": {"callId": "call_clean", "entryType": "web"}}}
        if url.endswith("/ai-call/records/call_clean/events?limit=1000"):
            return {
                "code": 200,
                "data": {
                    "rows": [
                        _event("user_speech_started", "2026-07-08T06:39:12.083Z"),
                        _event("user_speech_stopped", "2026-07-08T06:39:12.500Z"),
                    ]
                },
            }
        if url.endswith("/ai-call/records/call_clean/dialogue-segments?limit=1000"):
            return {"code": 200, "data": {"rows": [], "total": 0}}
        if url.endswith("/ai-call/records/call_bad"):
            return {"code": 200, "data": {"record": {"callId": "call_bad", "entryType": "web"}}}
        if url.endswith("/ai-call/records/call_bad/events?limit=1000"):
            return {
                "code": 200,
                "data": {
                    "rows": [
                        _event("user_speech_started", "2026-07-08T06:40:01.000Z"),
                        _event(
                            "model_response_started",
                            "2026-07-08T06:40:01.020Z",
                            response_id="resp_bad",
                        ),
                        _event("user_speech_stopped", "2026-07-08T06:40:03.000Z"),
                    ]
                },
            }
        if url.endswith("/ai-call/records/call_bad/dialogue-segments?limit=1000"):
            return {"code": 200, "data": {"rows": [], "total": 0}}
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
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    assert exit_code == 0
    output = stdout.getvalue()
    assert "timeline_audit calls=2 passed=1 failed=1 issues=1 high=1 covered=0 fetchFailed=0" in output
    assert "call callId=call_bad passed=false issues=1 high=1 covered=0" in output
    assert requested_urls == [
        "http://127.0.0.1:19012/ai-call/records?entryType=web&pageSize=2",
        "http://127.0.0.1:19012/ai-call/records/call_clean",
        "http://127.0.0.1:19012/ai-call/records/call_clean/events?limit=1000",
        "http://127.0.0.1:19012/ai-call/records/call_clean/dialogue-segments?limit=1000",
        "http://127.0.0.1:19012/ai-call/records/call_bad",
        "http://127.0.0.1:19012/ai-call/records/call_bad/events?limit=1000",
        "http://127.0.0.1:19012/ai-call/records/call_bad/dialogue-segments?limit=1000",
    ]


def _event(
    event_type: str,
    event_time: str,
    *,
    response_id: str | None = None,
    reason: str | None = None,
    delta_bytes: int | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if response_id is not None:
        payload["responseId"] = response_id
        payload["response"] = {"id": response_id}
    if reason is not None:
        payload["reason"] = reason
    if delta_bytes is not None:
        payload["deltaBytes"] = delta_bytes
    return {
        "eventType": event_type,
        "source": "provider",
        "eventTime": event_time,
        "payload": payload,
    }
