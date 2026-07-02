from __future__ import annotations

import io
import json
import math
import subprocess
import sys
from typing import Any

from app.services.ai_call.interrupt_offline_analysis import build_offline_interrupt_report
from app.services.ai_call.interrupt_p1_evaluation import build_p1_evaluation
from tools.ai_call_interrupt_replay import run
from tools.ai_call_p1_eval import run as run_p1_eval


def test_offline_interrupt_report_finds_customer_audio_over_ai_audio() -> None:
    sample_rate = 16_000
    customer_pcm = _pcm_with_tone(
        sample_rate=sample_rate,
        duration_ms=1_000,
        hot_ranges_ms=[(300, 600)],
    )
    ai_pcm = _pcm_with_tone(
        sample_rate=sample_rate,
        duration_ms=1_000,
        hot_ranges_ms=[(200, 800)],
    )

    report = build_offline_interrupt_report(
        call_id="call_analysis",
        record={
            "callId": "call_analysis",
            "status": "completed",
            "endReason": "remote_hangup",
            "startedAt": "2026-06-26T03:00:00Z",
        },
        events=[
            {
                "eventType": "interrupt_candidate",
                "source": "sip",
                "eventTime": "2026-06-26T03:00:00.350Z",
                "payload": {"source": "sip"},
            },
            {
                "eventType": "user_speech_started",
                "source": "provider",
                "eventTime": "2026-06-26T03:00:00.420Z",
                "payload": {},
            },
            {
                "eventType": "sip_interrupt_candidate_confirmed",
                "source": "sip",
                "eventTime": "2026-06-26T03:00:00.500Z",
                "payload": {"confirmedBy": "provider_speech_started"},
            },
        ],
        recording={
            "callId": "call_analysis",
            "tracks": [
                {
                    "trackRole": "customer",
                    "startedAt": "2026-06-26T03:00:00Z",
                    "status": "completed",
                },
                {
                    "trackRole": "ai",
                    "startedAt": "2026-06-26T03:00:00Z",
                    "status": "completed",
                },
            ],
        },
        pcm_by_role={"customer": customer_pcm, "ai": ai_pcm},
        sample_rate=sample_rate,
        window_ms=100,
        min_rms_dbfs=-45.0,
        min_snr_db=6.0,
        min_segment_ms=100,
    )

    assert report["callId"] == "call_analysis"
    assert report["record"]["endReason"] == "remote_hangup"
    assert report["eventSummary"]["interruptCandidateCount"] == 1
    assert report["eventSummary"]["interruptConfirmedCount"] == 1
    assert report["eventSummary"]["providerSpeechStartedCount"] == 1
    assert report["audioAnalysis"]["customerSegments"] == [
        {
            "startMs": 300,
            "endMs": 600,
            "durationMs": 300,
            "peakRmsDbfs": report["audioAnalysis"]["customerSegments"][0]["peakRmsDbfs"],
        }
    ]
    assert report["audioAnalysis"]["aiActiveSegments"] == [
        {
            "startMs": 200,
            "endMs": 800,
            "durationMs": 600,
            "peakRmsDbfs": report["audioAnalysis"]["aiActiveSegments"][0]["peakRmsDbfs"],
        }
    ]
    window = report["possibleInterruptWindows"][0]
    assert {key: window[key] for key in ("startMs", "endMs", "durationMs", "reason")} == {
        "startMs": 300,
        "endMs": 600,
        "durationMs": 300,
        "reason": "customer_audio_over_ai_audio",
    }
    assert window["eventAlignment"]["verdict"] == "confirmed"
    assert [
        event["eventType"] for event in window["eventAlignment"]["candidateEvents"]
    ] == ["interrupt_candidate"]
    assert [
        event["eventType"] for event in window["eventAlignment"]["providerSpeechStartedEvents"]
    ] == ["user_speech_started"]
    assert [
        event["eventType"] for event in window["eventAlignment"]["confirmedEvents"]
    ] == ["sip_interrupt_candidate_confirmed"]
    assert window["eventAlignment"]["candidateEvents"][0]["offsetMs"] == 350
    assert window["eventAlignment"]["providerSpeechStartedEvents"][0]["offsetMs"] == 420
    assert window["eventAlignment"]["confirmedEvents"][0]["offsetMs"] == 500


def test_offline_interrupt_report_counts_sip_p1_confirmation() -> None:
    sample_rate = 16_000

    report = build_offline_interrupt_report(
        call_id="call_analysis_p1",
        record={
            "callId": "call_analysis_p1",
            "status": "completed",
            "startedAt": "2026-06-30T03:00:00Z",
        },
        events=[
            {
                "eventType": "sip_interrupt_candidate",
                "source": "sip",
                "eventTime": "2026-06-30T03:00:00.350Z",
                "payload": {"reason": "sip_uplink_speech_during_ai_audio"},
            },
            {
                "eventType": "sip_interrupt_confirmed",
                "source": "sip",
                "eventTime": "2026-06-30T03:00:00.500Z",
                "payload": {"decision": "confirmed"},
            },
        ],
        recording={
            "callId": "call_analysis_p1",
            "tracks": [
                {"trackRole": "customer", "startedAt": "2026-06-30T03:00:00Z"},
                {"trackRole": "ai", "startedAt": "2026-06-30T03:00:00Z"},
            ],
        },
        pcm_by_role={
            "customer": _pcm_with_tone(
                sample_rate=sample_rate,
                duration_ms=1_000,
                hot_ranges_ms=[(300, 600)],
            ),
            "ai": _pcm_with_tone(
                sample_rate=sample_rate,
                duration_ms=1_000,
                hot_ranges_ms=[(200, 800)],
            ),
        },
        sample_rate=sample_rate,
        window_ms=100,
        min_rms_dbfs=-45.0,
        min_snr_db=6.0,
        min_segment_ms=100,
    )

    assert report["eventSummary"]["interruptConfirmedCount"] == 1
    window = report["possibleInterruptWindows"][0]
    assert window["eventAlignment"]["verdict"] == "confirmed"
    assert [
        event["eventType"] for event in window["eventAlignment"]["confirmedEvents"]
    ] == ["sip_interrupt_confirmed"]


def test_offline_interrupt_report_marks_candidate_without_confirm() -> None:
    sample_rate = 16_000

    report = build_offline_interrupt_report(
        call_id="call_unconfirmed",
        record={
            "callId": "call_unconfirmed",
            "status": "completed",
            "startedAt": "2026-06-26T03:00:00Z",
        },
        events=[
            {
                "eventType": "sip_interrupt_candidate",
                "source": "sip",
                "eventTime": "2026-06-26T03:00:00.350Z",
                "payload": {"reason": "local_energy_candidate"},
            },
        ],
        recording={
            "callId": "call_unconfirmed",
            "tracks": [
                {"trackRole": "customer", "startedAt": "2026-06-26T03:00:00Z"},
                {"trackRole": "ai", "startedAt": "2026-06-26T03:00:00Z"},
            ],
        },
        pcm_by_role={
            "customer": _pcm_with_tone(
                sample_rate=sample_rate,
                duration_ms=1_000,
                hot_ranges_ms=[(300, 600)],
            ),
            "ai": _pcm_with_tone(
                sample_rate=sample_rate,
                duration_ms=1_000,
                hot_ranges_ms=[(200, 800)],
            ),
        },
        sample_rate=sample_rate,
        window_ms=100,
        min_rms_dbfs=-45.0,
        min_snr_db=6.0,
        min_segment_ms=100,
    )

    assert report["possibleInterruptWindows"][0]["eventAlignment"]["verdict"] == (
        "candidate_not_confirmed"
    )
    assert [
        event["eventType"]
        for event in report["possibleInterruptWindows"][0]["eventAlignment"]["candidateEvents"]
    ] == ["sip_interrupt_candidate"]
    assert report["possibleInterruptWindows"][0]["eventAlignment"]["confirmedEvents"] == []


def test_interrupt_replay_cli_fetches_call_and_outputs_json_report() -> None:
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
                        "status": "completed",
                        "endReason": "remote_hangup",
                        "startedAt": "2026-06-26T03:00:00Z",
                    }
                },
            }
        if url.endswith("/ai-call/records/call_cli/events?limit=1000"):
            return {
                "code": 200,
                "data": {
                    "rows": [
                        {
                            "eventType": "sip_hangup",
                            "source": "livekit",
                            "eventTime": "2026-06-26T03:00:00.500Z",
                            "payload": {},
                        }
                    ]
                },
            }
        if url.endswith("/ai-call/records/call_cli/recording"):
            return {
                "code": 200,
                "data": {
                    "callId": "call_cli",
                    "tracks": [
                        {
                            "trackRole": "customer",
                            "status": "completed",
                            "startedAt": "2026-06-26T03:00:00Z",
                            "playUrl": "https://files.test/customer.ogg",
                        },
                        {
                            "trackRole": "ai",
                            "status": "completed",
                            "startedAt": "2026-06-26T03:00:00Z",
                            "playUrl": "https://files.test/ai.ogg",
                        },
                    ],
                },
            }
        raise AssertionError(f"unexpected url: {url}")

    def fake_decode_audio(url: str, sample_rate: int, timeout_seconds: float) -> bytes:
        assert sample_rate == 16_000
        assert timeout_seconds == 7.0
        if url.endswith("/customer.ogg"):
            return _pcm_with_tone(
                sample_rate=sample_rate,
                duration_ms=1_000,
                hot_ranges_ms=[(300, 600)],
            )
        if url.endswith("/ai.ogg"):
            return _pcm_with_tone(
                sample_rate=sample_rate,
                duration_ms=1_000,
                hot_ranges_ms=[(200, 800)],
            )
        raise AssertionError(f"unexpected audio url: {url}")

    stdout = io.StringIO()
    exit_code = run(
        [
            "--base-url",
            "http://127.0.0.1:19012",
            "--call-id",
            "call_cli",
            "--timeout-seconds",
            "7",
            "--json",
        ],
        get_json=fake_get_json,
        decode_audio=fake_decode_audio,
        stdout=stdout,
    )

    assert exit_code == 0
    payload = json.loads(stdout.getvalue())
    assert payload["callId"] == "call_cli"
    assert payload["record"]["endReason"] == "remote_hangup"
    assert payload["eventSummary"]["sipHangupCount"] == 1
    assert payload["possibleInterruptWindows"][0]["reason"] == "customer_audio_over_ai_audio"
    assert requested_urls == [
        "http://127.0.0.1:19012/ai-call/records/call_cli",
        "http://127.0.0.1:19012/ai-call/records/call_cli/events?limit=1000",
        "http://127.0.0.1:19012/ai-call/records/call_cli/recording",
    ]

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
        decode_audio=fake_decode_audio,
        stdout=stdout,
    )

    assert exit_code == 0
    assert "verdict=likely_noise" in stdout.getvalue()


def test_interrupt_replay_cli_summarizes_recent_sip_calls() -> None:
    requested_urls: list[str] = []

    def fake_get_json(url: str, timeout_seconds: float) -> dict[str, Any]:
        requested_urls.append(url)
        assert timeout_seconds == 7.0
        if url.endswith("/ai-call/records?entryType=sip_outbound&pageSize=2"):
            return {
                "code": 200,
                "rows": [
                    {"callId": "call_confirmed"},
                    {"callId": "call_noise"},
                ],
                "total": 2,
            }
        if url.endswith("/ai-call/records/call_confirmed"):
            return {
                "code": 200,
                "data": {
                    "record": {
                        "callId": "call_confirmed",
                        "status": "completed",
                        "endReason": "remote_hangup",
                        "startedAt": "2026-06-26T03:00:00Z",
                    }
                },
            }
        if url.endswith("/ai-call/records/call_confirmed/events?limit=1000"):
            return {
                "code": 200,
                "data": {
                    "rows": [
                        {
                            "eventType": "interrupt_candidate",
                            "source": "sip",
                            "eventTime": "2026-06-26T03:00:00.350Z",
                            "payload": {},
                        },
                        {
                            "eventType": "sip_interrupt_candidate_confirmed",
                            "source": "sip",
                            "eventTime": "2026-06-26T03:00:00.500Z",
                            "payload": {},
                        },
                    ]
                },
            }
        if url.endswith("/ai-call/records/call_noise"):
            return {
                "code": 200,
                "data": {
                    "record": {
                        "callId": "call_noise",
                        "status": "completed",
                        "endReason": "agent_completed",
                        "startedAt": "2026-06-26T03:01:00Z",
                    }
                },
            }
        if url.endswith("/ai-call/records/call_noise/events?limit=1000"):
            return {"code": 200, "data": {"rows": []}}
        if url.endswith("/ai-call/records/call_confirmed/recording") or url.endswith(
            "/ai-call/records/call_noise/recording"
        ):
            call_id = "call_confirmed" if "call_confirmed" in url else "call_noise"
            return {
                "code": 200,
                "data": {
                    "callId": call_id,
                    "tracks": [
                        {
                            "trackRole": "customer",
                            "status": "completed",
                            "startedAt": "2026-06-26T03:00:00Z",
                            "playUrl": f"https://files.test/{call_id}/customer.ogg",
                        },
                        {
                            "trackRole": "ai",
                            "status": "completed",
                            "startedAt": "2026-06-26T03:00:00Z",
                            "playUrl": f"https://files.test/{call_id}/ai.ogg",
                        },
                    ],
                },
            }
        raise AssertionError(f"unexpected url: {url}")

    def fake_decode_audio(url: str, sample_rate: int, timeout_seconds: float) -> bytes:
        assert sample_rate == 16_000
        assert timeout_seconds == 7.0
        if url.endswith("/customer.ogg"):
            return _pcm_with_tone(
                sample_rate=sample_rate,
                duration_ms=1_000,
                hot_ranges_ms=[(300, 600)],
            )
        if url.endswith("/ai.ogg"):
            return _pcm_with_tone(
                sample_rate=sample_rate,
                duration_ms=1_000,
                hot_ranges_ms=[(200, 800)],
            )
        raise AssertionError(f"unexpected audio url: {url}")

    stdout = io.StringIO()
    exit_code = run(
        [
            "--base-url",
            "http://127.0.0.1:19012",
            "--recent",
            "2",
            "--timeout-seconds",
            "7",
            "--json",
        ],
        get_json=fake_get_json,
        decode_audio=fake_decode_audio,
        stdout=stdout,
    )

    assert exit_code == 0
    payload = json.loads(stdout.getvalue())
    assert payload["mode"] == "recent"
    assert payload["totalCalls"] == 2
    assert payload["windowCount"] == 2
    assert payload["verdictCounts"] == {
        "confirmed": 1,
        "candidate_not_confirmed": 0,
        "missed_candidate": 0,
        "likely_noise": 1,
    }
    assert [item["callId"] for item in payload["calls"]] == [
        "call_confirmed",
        "call_noise",
    ]
    assert payload["failedCalls"] == []
    assert requested_urls[0] == (
        "http://127.0.0.1:19012/ai-call/records?entryType=sip_outbound&pageSize=2"
    )

    stdout = io.StringIO()
    exit_code = run(
        [
            "--base-url",
            "http://127.0.0.1:19012",
            "--recent",
            "2",
            "--timeout-seconds",
            "7",
        ],
        get_json=fake_get_json,
        decode_audio=fake_decode_audio,
        stdout=stdout,
    )

    assert exit_code == 0
    assert "recent calls=2 windows=2 confirmed=1 likely_noise=1" in stdout.getvalue()


def test_interrupt_replay_script_can_run_by_file_path() -> None:
    result = subprocess.run(
        [sys.executable, "tools/ai_call_interrupt_replay.py", "--help"],
        text=True,
        capture_output=True,
        timeout=10,
    )

    assert result.returncode == 0
    assert "Offline replay report" in result.stdout


def test_p1_evaluation_marks_rejected_pre_stop_as_false_pre_stop() -> None:
    report = build_p1_evaluation(
        call_id="call_false_pre_stop",
        record={"callId": "call_false_pre_stop", "startedAt": "2026-07-01T08:00:00Z"},
        events=[
            _event(
                "sip_interrupt_candidate",
                "2026-07-01T08:00:01.000Z",
                response_id="resp_noise",
                generation=0,
                candidate_duration_ms=180,
                rms_dbfs=-13.49,
                snr_db=36.51,
                speech_quality_rejection="short_hot_onset_drop",
            ),
            _event(
                "sip_pre_stop",
                "2026-07-01T08:00:01.260Z",
                response_id="resp_noise",
                generation=1,
                candidate_duration_ms=240,
                candidate_to_stop_ms=2,
                rms_dbfs=-28.42,
                snr_db=21.58,
            ),
            _event(
                "sip_interrupt_rejected",
                "2026-07-01T08:00:01.760Z",
                response_id="resp_noise",
                generation=1,
                reason="rejected_noise",
                pre_stop_to_decision_ms=500,
            ),
        ],
    )

    assert report["summary"] == {
        "confirmedPreStops": 0,
        "falsePreStops": 1,
        "candidateOnly": 0,
        "preStopPending": 0,
        "providerSpeechStarted": 0,
    }
    window = report["windows"][0]
    assert window["outcome"] == "false_pre_stop_rejected"
    assert window["severity"] == "fail"
    assert window["candidateToPreStopMs"] == 260
    assert window["preStopToDecisionMs"] == 500
    assert window["speechQualityRejection"] == "short_hot_onset_drop"


def test_p1_evaluation_marks_confirmed_pre_stop_and_candidate_only() -> None:
    report = build_p1_evaluation(
        call_id="call_mixed",
        record={"callId": "call_mixed", "startedAt": "2026-07-01T08:00:00Z"},
        events=[
            _event(
                "sip_interrupt_candidate",
                "2026-07-01T08:00:01.000Z",
                response_id="resp_confirmed",
                generation=0,
                candidate_duration_ms=180,
            ),
            _event(
                "sip_pre_stop",
                "2026-07-01T08:00:01.220Z",
                response_id="resp_confirmed",
                generation=1,
                candidate_duration_ms=240,
            ),
            _event(
                "sip_interrupt_confirmed",
                "2026-07-01T08:00:01.520Z",
                response_id="resp_confirmed",
                generation=1,
                reason="clean_window_confirmed",
            ),
            _event(
                "sip_interrupt_candidate",
                "2026-07-01T08:00:03.000Z",
                response_id="resp_candidate_only",
                generation=1,
                candidate_duration_ms=180,
                rms_dbfs=-31.0,
                snr_db=18.0,
            ),
        ],
    )

    assert report["summary"] == {
        "confirmedPreStops": 1,
        "falsePreStops": 0,
        "candidateOnly": 1,
        "preStopPending": 0,
        "providerSpeechStarted": 0,
    }
    assert [window["outcome"] for window in report["windows"]] == [
        "confirmed_pre_stop",
        "candidate_without_pre_stop",
    ]
    assert report["windows"][0]["severity"] == "pass"
    assert report["windows"][1]["severity"] == "info"


def test_p1_evaluation_flags_offline_customer_speech_without_fast_pre_stop() -> None:
    report = build_p1_evaluation(
        call_id="call_missed_short_speech",
        record={"callId": "call_missed_short_speech", "startedAt": "2026-07-01T08:00:00Z"},
        events=[
            {
                "eventType": "model_response_started",
                "eventTime": "2026-07-01T08:00:00.000Z",
                "payload": {"response": {"id": "resp_opening"}},
            },
            _event(
                "sip_interrupt_candidate",
                "2026-07-01T08:00:01.120Z",
                response_id="resp_opening",
                generation=0,
                candidate_duration_ms=180,
                rms_dbfs=-15.5,
                snr_db=34.0,
            ),
            {
                "eventType": "model_response_done",
                "eventTime": "2026-07-01T08:00:05.000Z",
                "payload": {"response": {"id": "resp_opening"}},
            },
        ],
        dialogue_segments=[
            {
                "speakerType": "ai",
                "source": "qwen_realtime",
                "segmentStatus": "interrupted",
                "startedAt": "2026-07-01T08:00:00.000Z",
                "endedAt": "2026-07-01T08:00:05.000Z",
                "segmentText": "您好张总，我是灵宸智能助手。",
            },
            {
                "speakerType": "customer",
                "source": "offline_asr",
                "segmentStatus": "final",
                "startedAt": "2026-07-01T08:00:01.000Z",
                "endedAt": "2026-07-01T08:00:01.900Z",
                "segmentText": "好的。",
            },
        ],
        max_pre_stop_latency_ms=500,
    )

    assert report["quality"]["passed"] is False
    assert report["quality"]["missedCustomerSpeech"] == 1
    missed = report["quality"]["missedCustomerSpeechWindows"][0]
    assert missed["severity"] == "fail"
    assert missed["text"] == "好的。"
    assert missed["nearestCandidateToSpeechStartMs"] == 120
    assert missed["speechStartToPreStopMs"] is None
    assert missed["reason"] == "offline_customer_speech_without_fast_pre_stop"


def test_p1_evaluation_flags_slow_manual_session_end() -> None:
    report = build_p1_evaluation(
        call_id="call_slow_end",
        record={"callId": "call_slow_end", "startedAt": "2026-07-01T08:00:00Z"},
        events=[
            {
                "eventType": "session_ending",
                "eventTime": "2026-07-01T08:00:10.000Z",
                "payload": {},
            },
            {
                "eventType": "session_completed",
                "eventTime": "2026-07-01T08:00:20.200Z",
                "payload": {"endReason": "web_user_end"},
            },
        ],
        max_end_completion_ms=1_000,
    )

    assert report["quality"]["passed"] is False
    assert report["quality"]["slowSessionEnds"] == 1
    slow_end = report["quality"]["slowSessionEndWindows"][0]
    assert slow_end["severity"] == "fail"
    assert slow_end["endReason"] == "web_user_end"
    assert slow_end["endingToCompletedMs"] == 10_200
    assert slow_end["reason"] == "session_end_completion_too_slow"


def test_p1_eval_cli_summarizes_recent_calls_without_fetching_recordings() -> None:
    requested_urls: list[str] = []

    def fake_get_json(url: str, timeout_seconds: float) -> dict[str, Any]:
        requested_urls.append(url)
        assert timeout_seconds == 7.0
        if url.endswith("/ai-call/records?entryType=sip_outbound&pageSize=2"):
            return {
                "code": 200,
                "rows": [
                    {"callId": "call_false"},
                    {"callId": "call_confirmed"},
                ],
                "total": 2,
            }
        if url.endswith("/ai-call/records/call_false"):
            return {
                "code": 200,
                "data": {
                    "record": {
                        "callId": "call_false",
                        "status": "completed",
                        "startedAt": "2026-07-01T08:00:00Z",
                    }
                },
            }
        if url.endswith("/ai-call/records/call_false/events?limit=1000"):
            return {
                "code": 200,
                "data": {
                    "rows": [
                        _event(
                            "sip_interrupt_candidate",
                            "2026-07-01T08:00:01.000Z",
                            response_id="resp_false",
                            generation=0,
                        ),
                        _event(
                            "sip_pre_stop",
                            "2026-07-01T08:00:01.240Z",
                            response_id="resp_false",
                            generation=1,
                        ),
                        _event(
                            "sip_interrupt_rejected",
                            "2026-07-01T08:00:01.740Z",
                            response_id="resp_false",
                            generation=1,
                            reason="rejected_noise",
                        ),
                    ]
                },
            }
        if url.endswith("/ai-call/records/call_false/dialogue-segments?limit=1000"):
            return {"code": 200, "data": {"rows": [], "total": 0}}
        if url.endswith("/ai-call/records/call_confirmed"):
            return {
                "code": 200,
                "data": {
                    "record": {
                        "callId": "call_confirmed",
                        "status": "completed",
                        "startedAt": "2026-07-01T08:01:00Z",
                    }
                },
            }
        if url.endswith("/ai-call/records/call_confirmed/events?limit=1000"):
            return {
                "code": 200,
                "data": {
                    "rows": [
                        _event(
                            "sip_interrupt_candidate",
                            "2026-07-01T08:01:01.000Z",
                            response_id="resp_ok",
                            generation=0,
                        ),
                        _event(
                            "sip_pre_stop",
                            "2026-07-01T08:01:01.200Z",
                            response_id="resp_ok",
                            generation=1,
                        ),
                        _event(
                            "sip_interrupt_confirmed",
                            "2026-07-01T08:01:01.500Z",
                            response_id="resp_ok",
                            generation=1,
                        ),
                    ]
                },
            }
        if url.endswith("/ai-call/records/call_confirmed/dialogue-segments?limit=1000"):
            return {"code": 200, "data": {"rows": [], "total": 0}}
        raise AssertionError(f"unexpected url: {url}")

    stdout = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--base-url",
            "http://127.0.0.1:19011",
            "--recent",
            "2",
            "--timeout-seconds",
            "7",
            "--json",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    assert exit_code == 0
    payload = json.loads(stdout.getvalue())
    assert payload["summary"] == {
        "calls": 2,
        "windows": 2,
        "confirmedPreStops": 1,
        "falsePreStops": 1,
        "candidateOnly": 0,
        "preStopPending": 0,
        "providerSpeechStarted": 0,
        "failedCalls": 0,
    }
    assert requested_urls == [
        "http://127.0.0.1:19011/ai-call/records?entryType=sip_outbound&pageSize=2",
        "http://127.0.0.1:19011/ai-call/records/call_false",
        "http://127.0.0.1:19011/ai-call/records/call_false/events?limit=1000",
        "http://127.0.0.1:19011/ai-call/records/call_false/dialogue-segments?limit=1000",
        "http://127.0.0.1:19011/ai-call/records/call_confirmed",
        "http://127.0.0.1:19011/ai-call/records/call_confirmed/events?limit=1000",
        "http://127.0.0.1:19011/ai-call/records/call_confirmed/dialogue-segments?limit=1000",
    ]

    stdout = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--base-url",
            "http://127.0.0.1:19011",
            "--recent",
            "2",
            "--timeout-seconds",
            "7",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    assert exit_code == 0
    output = stdout.getvalue()
    assert "p1_eval calls=2 windows=2 confirmedPreStops=1 falsePreStops=1" in output
    assert "call callId=call_false windows=1 confirmedPreStops=0 falsePreStops=1" in output


def test_p1_eval_cli_fetches_dialogue_segments_for_quality_gates() -> None:
    requested_urls: list[str] = []

    def fake_get_json(url: str, timeout_seconds: float) -> dict[str, Any]:
        requested_urls.append(url)
        assert timeout_seconds == 7.0
        if url.endswith("/ai-call/records/call_quality"):
            return {
                "code": 200,
                "data": {
                    "record": {
                        "callId": "call_quality",
                        "status": "completed",
                        "startedAt": "2026-07-01T08:00:00Z",
                    }
                },
            }
        if url.endswith("/ai-call/records/call_quality/events?limit=1000"):
            return {
                "code": 200,
                "data": {
                    "rows": [
                        _event(
                            "sip_interrupt_candidate",
                            "2026-07-01T08:00:01.120Z",
                            response_id="resp_opening",
                            generation=0,
                            candidate_duration_ms=180,
                        ),
                    ]
                },
            }
        if url.endswith("/ai-call/records/call_quality/dialogue-segments?limit=1000"):
            return {
                "code": 200,
                "data": {
                    "rows": [
                        {
                            "speakerType": "ai",
                            "source": "qwen_realtime",
                            "segmentStatus": "interrupted",
                            "startedAt": "2026-07-01T08:00:00.000Z",
                            "endedAt": "2026-07-01T08:00:05.000Z",
                            "segmentText": "您好张总。",
                        },
                        {
                            "speakerType": "customer",
                            "source": "offline_asr",
                            "segmentStatus": "final",
                            "startedAt": "2026-07-01T08:00:01.000Z",
                            "endedAt": "2026-07-01T08:00:01.900Z",
                            "segmentText": "好的。",
                        },
                    ]
                },
            }
        raise AssertionError(f"unexpected url: {url}")

    stdout = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--base-url",
            "http://127.0.0.1:19011",
            "--call-id",
            "call_quality",
            "--timeout-seconds",
            "7",
            "--json",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    assert exit_code == 0
    payload = json.loads(stdout.getvalue())
    assert payload["quality"]["passed"] is False
    assert payload["quality"]["missedCustomerSpeech"] == 1
    assert requested_urls == [
        "http://127.0.0.1:19011/ai-call/records/call_quality",
        "http://127.0.0.1:19011/ai-call/records/call_quality/events?limit=1000",
        "http://127.0.0.1:19011/ai-call/records/call_quality/dialogue-segments?limit=1000",
    ]


def test_p1_eval_script_can_run_by_file_path() -> None:
    result = subprocess.run(
        [sys.executable, "tools/ai_call_p1_eval.py", "--help"],
        text=True,
        capture_output=True,
        timeout=10,
    )

    assert result.returncode == 0
    assert "P1 SIP barge-in evaluation" in result.stdout


def _event(
    event_type: str,
    event_time: str,
    *,
    response_id: str | None = None,
    generation: int | None = None,
    candidate_duration_ms: int | None = None,
    candidate_to_stop_ms: int | None = None,
    pre_stop_to_decision_ms: int | None = None,
    rms_dbfs: float | None = None,
    snr_db: float | None = None,
    speech_quality_rejection: str | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if response_id is not None:
        payload["responseId"] = response_id
    if generation is not None:
        payload["generation"] = generation
    if candidate_duration_ms is not None:
        payload["candidateDurationMs"] = candidate_duration_ms
    if candidate_to_stop_ms is not None:
        payload["candidateToStopMs"] = candidate_to_stop_ms
    if pre_stop_to_decision_ms is not None:
        payload["preStopToDecisionMs"] = pre_stop_to_decision_ms
    if rms_dbfs is not None:
        payload["rmsDbfs"] = rms_dbfs
    if snr_db is not None:
        payload["snrDb"] = snr_db
    if speech_quality_rejection is not None:
        payload["speechQualityRejection"] = speech_quality_rejection
    if reason is not None:
        payload["reason"] = reason
    return {
        "eventType": event_type,
        "source": "agent" if event_type.startswith("sip_") else "provider",
        "eventTime": event_time,
        "payload": payload,
    }


def _pcm_with_tone(
    *,
    sample_rate: int,
    duration_ms: int,
    hot_ranges_ms: list[tuple[int, int]],
) -> bytes:
    total_samples = sample_rate * duration_ms // 1000
    frames = bytearray()
    for sample_index in range(total_samples):
        t_ms = sample_index * 1000 // sample_rate
        hot = any(start_ms <= t_ms < end_ms for start_ms, end_ms in hot_ranges_ms)
        amplitude = 8_000 if hot else 0
        value = round(amplitude * math.sin(2 * math.pi * 440 * sample_index / sample_rate))
        frames.extend(int(value).to_bytes(2, byteorder="little", signed=True))
    return bytes(frames)
