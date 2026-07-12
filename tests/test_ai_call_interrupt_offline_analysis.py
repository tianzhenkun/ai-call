from __future__ import annotations

import io
import json
import math
import struct
import subprocess
import sys
import wave
from pathlib import Path
from typing import Any

from app.services.ai_call.interrupt_offline_analysis import build_offline_interrupt_report
from app.services.ai_call.interrupt_p1_evaluation import build_p1_evaluation
from tools.ai_call_export_p1_audio_fixture import run as run_export_p1_audio_fixture
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
        "confirmedWithoutPreStop": 0,
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
        "confirmedWithoutPreStop": 0,
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


def test_p1_evaluation_marks_provider_confirmed_candidate_without_pre_stop() -> None:
    report = build_p1_evaluation(
        call_id="call_provider_confirmed_candidate",
        record={
            "callId": "call_provider_confirmed_candidate",
            "startedAt": "2026-07-10T07:33:19Z",
        },
        events=[
            _event(
                "sip_interrupt_candidate",
                "2026-07-10T07:33:20.673Z",
                response_id="resp_echo_guarded",
                generation=3,
                candidate_duration_ms=180,
                reason="sip_uplink_speech_during_ai_audio",
            ),
            _event(
                "sip_ai_playback_echo_deferred",
                "2026-07-10T07:33:20.673Z",
                response_id="resp_echo_guarded",
                generation=3,
                candidate_duration_ms=180,
                reason="awaiting_ai_playback_echo_guard",
            ),
            _event("user_speech_started", "2026-07-10T07:33:20.755Z"),
            _event(
                "sip_interrupt_candidate_confirmed",
                "2026-07-10T07:33:20.877Z",
                response_id="resp_echo_guarded",
                generation=3,
                reason="user_speech_started_during_ai_audio",
            ),
            _event(
                "interrupt_audio_stop_completed",
                "2026-07-10T07:33:20.879Z",
                response_id="resp_echo_guarded",
                generation=3,
                reason="user_speech_started_during_ai_audio",
            ),
        ],
    )

    assert report["summary"]["candidateOnly"] == 0
    assert report["summary"]["confirmedWithoutPreStop"] == 1
    window = report["windows"][0]
    assert window["outcome"] == "confirmed_without_pre_stop"
    assert window["severity"] == "warn"
    assert window["candidateTime"] == "2026-07-10T07:33:20.673Z"
    assert window["preStopTime"] is None
    assert window["decisionTime"] == "2026-07-10T07:33:20.877Z"
    assert window["decisionEventType"] == "sip_interrupt_candidate_confirmed"
    assert window["providerSpeechStarted"] is True
    assert window["candidateToDecisionMs"] == 204


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


def test_p1_evaluation_does_not_flag_fast_candidate_resolution_as_missed_speech() -> None:
    report = build_p1_evaluation(
        call_id="call_fast_candidate_resolution",
        record={
            "callId": "call_fast_candidate_resolution",
            "startedAt": "2026-07-10T07:33:19Z",
        },
        events=[
            _event(
                "sip_interrupt_candidate",
                "2026-07-10T07:33:20.673Z",
                response_id="resp_echo_guarded",
                generation=3,
                candidate_duration_ms=180,
            ),
            _event("user_speech_started", "2026-07-10T07:33:20.755Z"),
            _event(
                "sip_interrupt_candidate_confirmed",
                "2026-07-10T07:33:20.877Z",
                response_id="resp_echo_guarded",
                generation=3,
                reason="user_speech_started_during_ai_audio",
            ),
        ],
        dialogue_segments=[
            {
                "speakerType": "ai",
                "source": "qwen_realtime",
                "segmentStatus": "interrupted",
                "startedAt": "2026-07-10T07:33:19.500Z",
                "endedAt": "2026-07-10T07:33:21.000Z",
                "segmentText": "好的张总，那就不打扰您了。",
            },
            {
                "speakerType": "customer",
                "source": "offline_asr",
                "segmentStatus": "final",
                "startedAt": "2026-07-10T07:33:19.714Z",
                "endedAt": "2026-07-10T07:33:21.200Z",
                "segmentText": "哎，怎么不打扰我？",
            },
        ],
        max_pre_stop_latency_ms=500,
    )

    assert report["summary"]["confirmedWithoutPreStop"] == 1
    assert report["quality"]["missedCustomerSpeech"] == 0
    assert report["quality"]["passed"] is True


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


def test_p1_evaluation_flags_slow_call_end_intent_schedule() -> None:
    report = build_p1_evaluation(
        call_id="call_slow_call_end_intent",
        record={"callId": "call_slow_call_end_intent"},
        events=[
            {
                "eventType": "call_end_intent_detected",
                "eventTime": "2026-07-03T09:00:01.000Z",
                "payload": {"transcriptPreview": "好，挂了吧。"},
            },
            _event("model_response_started", "2026-07-03T09:00:02.000Z"),
            _event("model_audio_delta", "2026-07-03T09:00:02.120Z"),
            {
                "eventType": "call_end_scheduled",
                "eventTime": "2026-07-03T09:00:05.200Z",
                "payload": {"endReason": "customer_end"},
            },
        ],
        max_call_end_intent_schedule_ms=1_000,
    )

    assert report["quality"]["passed"] is False
    assert report["quality"]["slowCallEndIntents"] == 1
    issue = report["quality"]["slowCallEndIntentWindows"][0]
    assert issue["reason"] == "call_end_schedule_too_slow"
    assert issue["intentToScheduleMs"] == 4200
    assert issue["maxCallEndIntentScheduleMs"] == 1000
    assert issue["transcriptPreview"] == "好，挂了吧。"


def test_p1_evaluation_records_fast_call_end_intent_schedule() -> None:
    report = build_p1_evaluation(
        call_id="call_fast_call_end_intent",
        record={"callId": "call_fast_call_end_intent"},
        events=[
            {
                "eventType": "call_end_intent_detected",
                "eventTime": "2026-07-03T09:00:01.000Z",
                "payload": {"transcriptPreview": "挂了吧。"},
            },
            {
                "eventType": "call_end_scheduled",
                "eventTime": "2026-07-03T09:00:01.220Z",
                "payload": {"endReason": "customer_end"},
            },
        ],
        max_call_end_intent_schedule_ms=1_000,
    )

    assert report["quality"]["passed"] is True
    assert report["quality"]["callEndIntents"] == 1
    assert report["quality"]["slowCallEndIntents"] == 0
    window = report["quality"]["callEndIntentWindows"][0]
    assert window["passed"] is True
    assert window["reason"] == "passed"
    assert window["intentToScheduleMs"] == 220
    assert window["transcriptPreview"] == "挂了吧。"


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
        "confirmedWithoutPreStop": 0,
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


def test_p1_sample_matrix_flags_slow_near_end_speech() -> None:
    from app.services.ai_call.interrupt_p1_sample_matrix import (
        build_p1_sample_matrix_evaluation,
    )

    report = build_p1_evaluation(
        call_id="call_slow_short_speech",
        record={"callId": "call_slow_short_speech", "startedAt": "2026-07-03T09:00:00Z"},
        events=[
            _event(
                "sip_interrupt_candidate",
                "2026-07-03T09:00:01.430Z",
                response_id="resp_opening",
                generation=0,
                candidate_duration_ms=180,
            ),
            _event(
                "sip_pre_stop",
                "2026-07-03T09:00:01.620Z",
                response_id="resp_opening",
                generation=1,
                candidate_duration_ms=260,
            ),
            _event(
                "sip_interrupt_confirmed",
                "2026-07-03T09:00:01.900Z",
                response_id="resp_opening",
                generation=1,
            ),
        ],
    )

    matrix = build_p1_sample_matrix_evaluation(
        reports_by_call_id={"call_slow_short_speech": report},
        samples=[
            {
                "id": "short_hmm",
                "callId": "call_slow_short_speech",
                "category": "near_end_speech",
                "expectation": "must_interrupt",
                "speechStartTime": "2026-07-03T09:00:01.000Z",
                "maxPreStopLatencyMs": 500,
            }
        ],
    )

    assert matrix["summary"] == {
        "samples": 1,
        "passed": 0,
        "failed": 1,
        "missingReports": 0,
        "categories": {"near_end_speech": {"samples": 1, "passed": 0, "failed": 1}},
    }
    sample = matrix["samples"][0]
    assert sample["passed"] is False
    assert sample["reason"] == "pre_stop_too_slow"
    assert sample["evidence"]["speechStartToPreStopMs"] == 620
    assert sample["evidence"]["maxPreStopLatencyMs"] == 500


def test_p1_sample_matrix_flags_confirmed_echo_like_window() -> None:
    from app.services.ai_call.interrupt_p1_sample_matrix import (
        build_p1_sample_matrix_evaluation,
    )

    report = build_p1_evaluation(
        call_id="call_echo",
        record={"callId": "call_echo", "startedAt": "2026-07-03T09:00:00Z"},
        events=[
            _event(
                "sip_interrupt_candidate",
                "2026-07-03T09:00:12.000Z",
                response_id="resp_final",
                generation=3,
                candidate_duration_ms=180,
            ),
            _event(
                "sip_pre_stop",
                "2026-07-03T09:00:12.260Z",
                response_id="resp_final",
                generation=4,
                candidate_duration_ms=260,
            ),
            _event(
                "sip_interrupt_confirmed",
                "2026-07-03T09:00:12.560Z",
                response_id="resp_final",
                generation=4,
                reason="sip_clean_window",
            ),
        ],
    )

    matrix = build_p1_sample_matrix_evaluation(
        reports_by_call_id={"call_echo": report},
        samples=[
            {
                "id": "final_echo",
                "callId": "call_echo",
                "category": "ai_echo_like",
                "expectation": "must_not_interrupt",
                "windowStartTime": "2026-07-03T09:00:11.500Z",
                "windowEndTime": "2026-07-03T09:00:13.000Z",
            }
        ],
    )

    sample = matrix["samples"][0]
    assert sample["passed"] is False
    assert sample["reason"] == "unexpected_pre_stop"
    assert sample["evidence"]["preStopTime"] == "2026-07-03T09:00:12.260Z"
    assert sample["evidence"]["outcome"] == "confirmed_pre_stop"


def test_p1_sample_matrix_passes_deferred_candidate_window() -> None:
    from app.services.ai_call.interrupt_p1_sample_matrix import (
        build_p1_sample_matrix_evaluation,
    )

    report = build_p1_evaluation(
        call_id="call_deferred_noise",
        record={"callId": "call_deferred_noise", "startedAt": "2026-07-08T09:00:00Z"},
        events=[
            _event(
                "sip_interrupt_candidate",
                "2026-07-08T09:00:01.180Z",
                response_id="resp_opening",
                generation=0,
                candidate_duration_ms=180,
                reason="sip_uplink_speech_during_ai_audio",
            ),
            _event(
                "sip_pre_stop_deferred",
                "2026-07-08T09:00:01.180Z",
                response_id="resp_opening",
                generation=0,
                candidate_duration_ms=180,
                reason="awaiting_pre_stop_authority",
            ),
        ],
    )

    matrix = build_p1_sample_matrix_evaluation(
        reports_by_call_id={"call_deferred_noise": report},
        samples=[
            {
                "id": "opening_short_noise_deferred",
                "callId": "call_deferred_noise",
                "category": "continuous_non_speech_impact",
                "expectation": "must_defer",
                "windowStartTime": "2026-07-08T09:00:01.000Z",
                "windowEndTime": "2026-07-08T09:00:01.500Z",
            }
        ],
    )

    sample = matrix["samples"][0]
    assert sample["passed"] is True
    assert sample["reason"] == "passed"
    assert sample["evidence"]["candidateTime"] == "2026-07-08T09:00:01.180Z"
    assert sample["evidence"]["preStopTime"] is None


def test_p1_sample_matrix_fails_deferred_window_with_pre_stop() -> None:
    from app.services.ai_call.interrupt_p1_sample_matrix import (
        build_p1_sample_matrix_evaluation,
    )

    report = build_p1_evaluation(
        call_id="call_bad_deferred",
        record={"callId": "call_bad_deferred", "startedAt": "2026-07-08T09:00:00Z"},
        events=[
            _event(
                "sip_interrupt_candidate",
                "2026-07-08T09:00:01.180Z",
                response_id="resp_opening",
                generation=0,
                candidate_duration_ms=180,
            ),
            _event(
                "sip_pre_stop",
                "2026-07-08T09:00:01.220Z",
                response_id="resp_opening",
                generation=1,
                candidate_duration_ms=180,
            ),
            _event(
                "sip_interrupt_rejected",
                "2026-07-08T09:00:01.520Z",
                response_id="resp_opening",
                generation=1,
                reason="rejected_noise",
            ),
        ],
    )

    matrix = build_p1_sample_matrix_evaluation(
        reports_by_call_id={"call_bad_deferred": report},
        samples=[
            {
                "id": "opening_noise_pre_stopped",
                "callId": "call_bad_deferred",
                "category": "opening_noise",
                "expectation": "must_defer",
                "windowStartTime": "2026-07-08T09:00:01.000Z",
                "windowEndTime": "2026-07-08T09:00:01.500Z",
            }
        ],
    )

    sample = matrix["samples"][0]
    assert sample["passed"] is False
    assert sample["reason"] == "unexpected_pre_stop"
    assert sample["evidence"]["outcome"] == "false_pre_stop_rejected"


def test_p1_sample_matrix_flags_candidate_without_fast_pre_stop() -> None:
    from app.services.ai_call.interrupt_p1_sample_matrix import (
        build_p1_sample_matrix_evaluation,
    )

    report = build_p1_evaluation(
        call_id="call_short_command_candidate",
        record={"callId": "call_short_command_candidate", "startedAt": "2026-07-10T02:03:30Z"},
        events=[
            _event(
                "sip_interrupt_candidate",
                "2026-07-10T02:03:34.773Z",
                response_id="resp_turn",
                generation=3,
                candidate_duration_ms=180,
                reason="sip_uplink_speech_during_ai_audio",
            ),
            _event(
                "sip_pre_stop_deferred",
                "2026-07-10T02:03:34.773Z",
                response_id="resp_turn",
                generation=3,
                candidate_duration_ms=180,
                reason="awaiting_pre_stop_authority",
            ),
            _event(
                "sip_interrupt_candidate_expired",
                "2026-07-10T02:03:39.773Z",
                reason="sip_barge_in_expired",
            ),
        ],
    )

    matrix = build_p1_sample_matrix_evaluation(
        reports_by_call_id={"call_short_command_candidate": report},
        samples=[
            {
                "id": "short_command_candidate",
                "callId": "call_short_command_candidate",
                "category": "short_command_candidate",
                "expectation": "must_pre_stop_after_candidate",
                "candidateTime": "2026-07-10T02:03:34.773Z",
                "maxCandidateToPreStopMs": 500,
            }
        ],
    )

    sample = matrix["samples"][0]
    assert sample["passed"] is False
    assert sample["reason"] == "missing_pre_stop"
    assert sample["evidence"]["candidateTime"] == "2026-07-10T02:03:34.773Z"
    assert sample["evidence"]["candidateToPreStopMs"] is None
    assert sample["evidence"]["maxCandidateToPreStopMs"] == 500


def test_p1_sample_matrix_preserves_structured_live_sample_annotations() -> None:
    from app.services.ai_call.interrupt_p1_sample_matrix import (
        build_p1_sample_matrix_evaluation,
    )

    report = build_p1_evaluation(
        call_id="call_multi_segment_speech",
        record={
            "callId": "call_multi_segment_speech",
            "startedAt": "2026-07-10T10:00:00Z",
        },
        events=[
            _event(
                "sip_interrupt_candidate",
                "2026-07-10T10:00:07.820Z",
                response_id="resp_mid_turn",
                generation=4,
                candidate_duration_ms=180,
            ),
            _event(
                "sip_interrupt_candidate_expired",
                "2026-07-10T10:00:10.078Z",
                response_id="resp_mid_turn",
                generation=4,
                reason="stale_deferred_pre_stop_candidate",
            ),
        ],
    )

    matrix = build_p1_sample_matrix_evaluation(
        reports_by_call_id={"call_multi_segment_speech": report},
        samples=[
            {
                "id": "live_multi_segment_speech",
                "callId": "call_multi_segment_speech",
                "sourceType": "live_call",
                "category": "midcall_near_end_speech",
                "expectation": "must_pre_stop_after_candidate",
                "candidateTime": "2026-07-10T10:00:07.820Z",
                "maxCandidateToPreStopMs": 500,
                "utterance": {
                    "text": "行行行。",
                    "source": "offline_asr",
                    "startedAt": "2026-07-10T10:00:06.790Z",
                    "endedAt": "2026-07-10T10:00:13.970Z",
                    "durationMs": 7180,
                    "audioStartMs": 74900,
                    "audioEndMs": 82080,
                },
                "turnEvidence": {
                    "candidateTime": "2026-07-10T10:00:07.820Z",
                    "candidateToSpeechStartMs": 1030,
                    "preStopTime": None,
                    "speechStartToPreStopMs": None,
                    "expiredReason": "stale_deferred_pre_stop_candidate",
                    "candidateDurationMs": 180,
                    "snrDb": 16.42,
                },
                "acousticContext": {
                    "speechPattern": "multi_segment_near_end",
                    "aiOverlap": True,
                    "nearAiTail": False,
                    "sourceDevice": "linphone",
                },
            }
        ],
    )

    sample = matrix["samples"][0]
    assert sample["utterance"]["text"] == "行行行。"
    assert sample["turnEvidence"]["candidateToSpeechStartMs"] == 1030
    assert sample["acousticContext"]["speechPattern"] == "multi_segment_near_end"
    assert matrix["summary"]["annotations"] == {
        "utteranceSources": {"offline_asr": 1},
        "speechPatterns": {"multi_segment_near_end": 1},
        "turnEvidenceExpiredReasons": {"stale_deferred_pre_stop_candidate": 1},
    }


def test_p1_sample_matrix_passes_fast_provider_resolved_candidate() -> None:
    from app.services.ai_call.interrupt_p1_sample_matrix import (
        build_p1_sample_matrix_evaluation,
    )

    report = build_p1_evaluation(
        call_id="call_echo_guard_provider_resolved",
        record={
            "callId": "call_echo_guard_provider_resolved",
            "startedAt": "2026-07-10T07:33:19Z",
        },
        events=[
            _event(
                "sip_interrupt_candidate",
                "2026-07-10T07:33:20.673Z",
                response_id="resp_echo_guarded",
                generation=3,
                candidate_duration_ms=180,
            ),
            _event("user_speech_started", "2026-07-10T07:33:20.755Z"),
            _event(
                "sip_interrupt_candidate_confirmed",
                "2026-07-10T07:33:20.877Z",
                response_id="resp_echo_guarded",
                generation=3,
                reason="user_speech_started_during_ai_audio",
            ),
        ],
    )

    matrix = build_p1_sample_matrix_evaluation(
        reports_by_call_id={"call_echo_guard_provider_resolved": report},
        samples=[
            {
                "id": "echo_guard_provider_resolved",
                "callId": "call_echo_guard_provider_resolved",
                "category": "echo_guarded_near_end_speech",
                "expectation": "must_resolve_after_candidate",
                "candidateTime": "2026-07-10T07:33:20.673Z",
                "maxCandidateToResolutionMs": 500,
            }
        ],
    )

    sample = matrix["samples"][0]
    assert sample["passed"] is True
    assert sample["reason"] == "passed"
    assert sample["evidence"]["candidateToResolutionMs"] == 204
    assert sample["evidence"]["outcome"] == "confirmed_without_pre_stop"


def test_p1_sample_matrix_fails_pending_pre_stop_clean_window() -> None:
    from app.services.ai_call.interrupt_p1_sample_matrix import (
        build_p1_sample_matrix_evaluation,
    )

    report = build_p1_evaluation(
        call_id="call_pending_pre_stop",
        record={"callId": "call_pending_pre_stop", "startedAt": "2026-07-08T09:00:00Z"},
        events=[
            _event(
                "sip_interrupt_candidate",
                "2026-07-08T09:00:02.000Z",
                response_id="resp_turn",
                generation=0,
                candidate_duration_ms=240,
            ),
            _event(
                "sip_pre_stop",
                "2026-07-08T09:00:02.240Z",
                response_id="resp_turn",
                generation=1,
                candidate_duration_ms=240,
            ),
        ],
    )

    matrix = build_p1_sample_matrix_evaluation(
        reports_by_call_id={"call_pending_pre_stop": report},
        samples=[
            {
                "id": "pre_stop_must_resolve",
                "callId": "call_pending_pre_stop",
                "category": "clean_window",
                "expectation": "must_confirm_or_reject",
                "windowStartTime": "2026-07-08T09:00:02.000Z",
                "windowEndTime": "2026-07-08T09:00:02.800Z",
            }
        ],
    )

    sample = matrix["samples"][0]
    assert sample["passed"] is False
    assert sample["reason"] == "pre_stop_pending"
    assert sample["evidence"]["preStopTime"] == "2026-07-08T09:00:02.240Z"


def test_p1_sample_matrix_flags_slow_call_end_intent_schedule() -> None:
    from app.services.ai_call.interrupt_p1_sample_matrix import (
        build_p1_sample_matrix_evaluation,
    )

    report = build_p1_evaluation(
        call_id="call_slow_call_end",
        record={"callId": "call_slow_call_end", "startedAt": "2026-07-03T09:00:00Z"},
        events=[
            {
                "eventType": "call_end_intent_detected",
                "eventTime": "2026-07-03T09:00:10.000Z",
                "payload": {"transcriptPreview": "好，挂了吧。"},
            },
            {
                "eventType": "call_end_scheduled",
                "eventTime": "2026-07-03T09:00:14.500Z",
                "payload": {"endReason": "customer_end"},
            },
        ],
        max_call_end_intent_schedule_ms=1_000,
    )

    matrix = build_p1_sample_matrix_evaluation(
        reports_by_call_id={"call_slow_call_end": report},
        samples=[
            {
                "id": "customer_end_should_schedule_fast",
                "callId": "call_slow_call_end",
                "category": "call_end",
                "expectation": "must_schedule_call_end",
                "intentTime": "2026-07-03T09:00:10.000Z",
                "maxScheduleLatencyMs": 1000,
            }
        ],
    )

    sample = matrix["samples"][0]
    assert sample["passed"] is False
    assert sample["reason"] == "call_end_schedule_too_slow"
    assert sample["evidence"]["intentToScheduleMs"] == 4500
    assert sample["evidence"]["maxScheduleLatencyMs"] == 1000


def test_p1_sample_matrix_fails_missing_call_end_intent() -> None:
    from app.services.ai_call.interrupt_p1_sample_matrix import (
        build_p1_sample_matrix_evaluation,
    )

    report = build_p1_evaluation(
        call_id="call_missing_call_end",
        record={"callId": "call_missing_call_end", "startedAt": "2026-07-03T09:00:00Z"},
        events=[
            {
                "eventType": "call_end_scheduled",
                "eventTime": "2026-07-03T09:00:14.500Z",
                "payload": {"endReason": "customer_end"},
            },
        ],
    )

    matrix = build_p1_sample_matrix_evaluation(
        reports_by_call_id={"call_missing_call_end": report},
        samples=[
            {
                "id": "customer_end_intent_missing",
                "callId": "call_missing_call_end",
                "category": "call_end",
                "expectation": "must_schedule_call_end",
                "intentTime": "2026-07-03T09:00:10.000Z",
                "maxScheduleLatencyMs": 1000,
            }
        ],
    )

    sample = matrix["samples"][0]
    assert sample["passed"] is False
    assert sample["reason"] == "missing_call_end_intent"
    assert sample["evidence"]["intentTime"] == "2026-07-03T09:00:10.000Z"


def test_p1_sample_matrix_flags_coverage_gate_failures() -> None:
    from app.services.ai_call.interrupt_p1_sample_matrix import (
        build_p1_sample_matrix_evaluation,
    )

    report = build_p1_evaluation(
        call_id="call_single_seed",
        record={"callId": "call_single_seed", "startedAt": "2026-07-03T09:00:00Z"},
        events=[
            _event(
                "sip_interrupt_candidate",
                "2026-07-03T09:00:01.120Z",
                response_id="resp_opening",
                generation=0,
            ),
            _event(
                "sip_pre_stop",
                "2026-07-03T09:00:01.260Z",
                response_id="resp_opening",
                generation=1,
            ),
            _event(
                "sip_interrupt_confirmed",
                "2026-07-03T09:00:01.560Z",
                response_id="resp_opening",
                generation=1,
            ),
        ],
    )

    matrix = build_p1_sample_matrix_evaluation(
        reports_by_call_id={"call_single_seed": report},
        samples=[
            {
                "id": "single_live_near_end",
                "callId": "call_single_seed",
                "category": "near_end_speech",
                "sourceType": "live_call",
                "expectation": "must_interrupt",
                "speechStartTime": "2026-07-03T09:00:01.000Z",
                "maxPreStopLatencyMs": 500,
            }
        ],
        coverage_gates={
            "minSamples": 4,
            "requiredCategories": {
                "near_end_speech": 1,
                "opening_noise": 1,
                "continuous_noise": 1,
            },
            "requiredSourceTypes": {
                "live_call": 1,
                "synthetic": 1,
            },
            "requiredExpectations": {
                "must_interrupt": 1,
                "must_defer": 1,
            },
        },
    )

    assert matrix["summary"]["failed"] == 0
    coverage = matrix["summary"]["coverage"]
    assert coverage["passed"] is False
    assert coverage["failureCount"] == 5
    assert coverage["actual"]["samples"] == 1
    assert coverage["actual"]["categories"] == {"near_end_speech": 1}
    assert coverage["actual"]["sourceTypes"] == {"live_call": 1}
    assert {
        (failure["gate"], failure.get("key"), failure["required"], failure["actual"])
        for failure in coverage["failures"]
    } == {
        ("min_samples", None, 4, 1),
        ("category", "opening_noise", 1, 0),
        ("category", "continuous_noise", 1, 0),
        ("source_type", "synthetic", 1, 0),
        ("expectation", "must_defer", 1, 0),
    }


def test_p1_eval_cli_runs_sample_matrix_file(tmp_path) -> None:
    matrix_path = tmp_path / "p1_matrix.json"
    matrix_path.write_text(
        json.dumps(
            {
                "samples": [
                    {
                        "id": "short_ok",
                        "callId": "call_matrix",
                        "category": "near_end_speech",
                        "expectation": "must_interrupt",
                        "speechStartTime": "2026-07-03T09:00:01.000Z",
                        "maxPreStopLatencyMs": 500,
                    },
                    {
                        "id": "echo_fail",
                        "callId": "call_matrix",
                        "category": "ai_echo_like",
                        "expectation": "must_not_interrupt",
                        "windowStartTime": "2026-07-03T09:00:03.000Z",
                        "windowEndTime": "2026-07-03T09:00:04.000Z",
                    },
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def fake_get_json(url: str, timeout_seconds: float) -> dict[str, Any]:
        assert timeout_seconds == 7.0
        if url.endswith("/ai-call/records/call_matrix"):
            return {
                "code": 200,
                "data": {
                    "record": {
                        "callId": "call_matrix",
                        "status": "completed",
                        "startedAt": "2026-07-03T09:00:00Z",
                    }
                },
            }
        if url.endswith("/ai-call/records/call_matrix/events?limit=1000"):
            return {
                "code": 200,
                "data": {
                    "rows": [
                        _event(
                            "sip_interrupt_candidate",
                            "2026-07-03T09:00:01.120Z",
                            response_id="resp_opening",
                            generation=0,
                        ),
                        _event(
                            "sip_pre_stop",
                            "2026-07-03T09:00:01.260Z",
                            response_id="resp_opening",
                            generation=1,
                        ),
                        _event(
                            "sip_interrupt_confirmed",
                            "2026-07-03T09:00:01.560Z",
                            response_id="resp_opening",
                            generation=1,
                        ),
                        _event(
                            "sip_interrupt_candidate",
                            "2026-07-03T09:00:03.200Z",
                            response_id="resp_final",
                            generation=2,
                        ),
                        _event(
                            "sip_pre_stop",
                            "2026-07-03T09:00:03.420Z",
                            response_id="resp_final",
                            generation=3,
                        ),
                    ]
                },
            }
        if url.endswith("/ai-call/records/call_matrix/dialogue-segments?limit=1000"):
            return {"code": 200, "data": {"rows": [], "total": 0}}
        raise AssertionError(f"unexpected url: {url}")

    stdout = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--base-url",
            "http://127.0.0.1:19011",
            "--sample-matrix",
            str(matrix_path),
            "--timeout-seconds",
            "7",
            "--json",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    assert exit_code == 2
    payload = json.loads(stdout.getvalue())
    assert payload["mode"] == "sample_matrix"
    assert payload["summary"]["samples"] == 2
    assert payload["summary"]["passed"] == 1
    assert payload["summary"]["failed"] == 1
    assert [sample["id"] for sample in payload["samples"]] == ["short_ok", "echo_fail"]


def test_p1_eval_cli_fails_sample_matrix_when_coverage_gate_fails(tmp_path) -> None:
    matrix_path = tmp_path / "p1_matrix_coverage.json"
    matrix_path.write_text(
        json.dumps(
            {
                "coverageGates": {
                    "minSamples": 2,
                    "requiredCategories": {"near_end_speech": 1, "opening_noise": 1},
                    "requiredSourceTypes": {"live_call": 1, "synthetic": 1},
                },
                "samples": [
                    {
                        "id": "short_ok",
                        "callId": "call_matrix_coverage",
                        "category": "near_end_speech",
                        "sourceType": "live_call",
                        "expectation": "must_interrupt",
                        "speechStartTime": "2026-07-03T09:00:01.000Z",
                        "maxPreStopLatencyMs": 500,
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def fake_get_json(url: str, timeout_seconds: float) -> dict[str, Any]:
        assert timeout_seconds == 7.0
        if url.endswith("/ai-call/records/call_matrix_coverage"):
            return {
                "code": 200,
                "data": {
                    "record": {
                        "callId": "call_matrix_coverage",
                        "status": "completed",
                        "startedAt": "2026-07-03T09:00:00Z",
                    }
                },
            }
        if url.endswith("/ai-call/records/call_matrix_coverage/events?limit=1000"):
            return {
                "code": 200,
                "data": {
                    "rows": [
                        _event(
                            "sip_interrupt_candidate",
                            "2026-07-03T09:00:01.120Z",
                            response_id="resp_opening",
                            generation=0,
                        ),
                        _event(
                            "sip_pre_stop",
                            "2026-07-03T09:00:01.260Z",
                            response_id="resp_opening",
                            generation=1,
                        ),
                        _event(
                            "sip_interrupt_confirmed",
                            "2026-07-03T09:00:01.560Z",
                            response_id="resp_opening",
                            generation=1,
                        ),
                    ]
                },
            }
        if url.endswith("/ai-call/records/call_matrix_coverage/dialogue-segments?limit=1000"):
            return {"code": 200, "data": {"rows": [], "total": 0}}
        raise AssertionError(f"unexpected url: {url}")

    stdout = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--base-url",
            "http://127.0.0.1:19011",
            "--sample-matrix",
            str(matrix_path),
            "--timeout-seconds",
            "7",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    assert exit_code == 2
    output = stdout.getvalue()
    assert "p1_sample_matrix samples=1 passed=1 failed=0" in output
    assert "sourceType=live_call evaluationSource=api_history" in output
    assert "coverage status=fail failures=3" in output
    assert "coverage_failure gate=min_samples key=None required=2 actual=1" in output
    assert "coverage_failure gate=category key=opening_noise required=1 actual=0" in output
    assert "coverage_failure gate=source_type key=synthetic required=1 actual=0" in output


def test_p1_eval_cli_uses_sample_matrix_fixture_reports_without_fetching_api(tmp_path) -> None:
    matrix_path = tmp_path / "p1_matrix_fixtures.json"
    matrix_path.write_text(
        json.dumps(
            {
                "coverageGates": {
                    "minSamples": 2,
                    "requiredCategories": {
                        "short_command_candidate": 1,
                        "continuous_noise": 1,
                    },
                    "requiredSourceTypes": {
                        "synthetic": 1,
                        "corpus_noise_mix": 1,
                    },
                    "requiredExpectations": {
                        "must_interrupt": 1,
                        "must_defer": 1,
                    },
                },
                "fixtureReports": {
                    "fixture_synthetic_short_stop": {
                        "record": {
                            "callId": "fixture_synthetic_short_stop",
                            "startedAt": "2026-07-10T09:00:00Z",
                        },
                        "events": [
                            _event(
                                "sip_interrupt_candidate",
                                "2026-07-10T09:00:01.120Z",
                                response_id="resp_fixture_short",
                                generation=0,
                            ),
                            _event(
                                "sip_pre_stop",
                                "2026-07-10T09:00:01.260Z",
                                response_id="resp_fixture_short",
                                generation=1,
                            ),
                            _event(
                                "sip_interrupt_confirmed",
                                "2026-07-10T09:00:01.560Z",
                                response_id="resp_fixture_short",
                                generation=1,
                            ),
                        ],
                    },
                    "fixture_corpus_continuous_noise": {
                        "record": {
                            "callId": "fixture_corpus_continuous_noise",
                            "startedAt": "2026-07-10T09:01:00Z",
                        },
                        "events": [
                            _event(
                                "sip_interrupt_candidate",
                                "2026-07-10T09:01:01.200Z",
                                response_id="resp_fixture_noise",
                                generation=0,
                            )
                        ],
                    },
                },
                "samples": [
                    {
                        "id": "synthetic_short_stop",
                        "callId": "fixture_synthetic_short_stop",
                        "sourceType": "synthetic",
                        "category": "short_command_candidate",
                        "expectation": "must_interrupt",
                        "speechStartTime": "2026-07-10T09:00:01.000Z",
                        "maxPreStopLatencyMs": 500,
                    },
                    {
                        "id": "corpus_continuous_noise_deferred",
                        "callId": "fixture_corpus_continuous_noise",
                        "sourceType": "corpus_noise_mix",
                        "category": "continuous_noise",
                        "expectation": "must_defer",
                        "windowStartTime": "2026-07-10T09:01:01.000Z",
                        "windowEndTime": "2026-07-10T09:01:01.800Z",
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def fake_get_json(url: str, _timeout_seconds: float) -> dict[str, Any]:
        raise AssertionError(f"unexpected API fetch for fixture matrix: {url}")

    stdout = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--base-url",
            "http://127.0.0.1:19011",
            "--sample-matrix",
            str(matrix_path),
            "--json",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    payload = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert payload["summary"]["failed"] == 0
    assert payload["summary"]["coverage"]["passed"] is True
    assert payload["summary"]["coverage"]["actual"]["sourceTypes"] == {
        "synthetic": 1,
        "corpus_noise_mix": 1,
    }
    assert payload["summary"]["evaluationSources"] == {"fixture_report": 2}
    assert [sample["id"] for sample in payload["samples"]] == [
        "synthetic_short_stop",
        "corpus_continuous_noise_deferred",
    ]
    assert [sample["evaluationSource"] for sample in payload["samples"]] == [
        "fixture_report",
        "fixture_report",
    ]


def test_p1_eval_cli_builds_sample_matrix_from_audio_fixtures_without_fetching_api(tmp_path) -> None:
    matrix_path = tmp_path / "p1_matrix_audio_fixtures.json"
    matrix_path.write_text(
        json.dumps(
            {
                "coverageGates": {
                    "minSamples": 2,
                    "requiredCategories": {
                        "short_command_candidate": 1,
                        "continuous_noise": 1,
                    },
                    "requiredSourceTypes": {
                        "synthetic": 1,
                        "corpus_noise_mix": 1,
                    },
                    "requiredExpectations": {
                        "must_interrupt": 1,
                        "must_defer": 1,
                    },
                },
                "audioFixtures": {
                    "fixture_audio_short_stop": {
                        "startedAt": "2026-07-10T09:10:00Z",
                        "sampleRateHz": 8000,
                        "frameMs": 20,
                        "pcmSegments": [
                            {"durationMs": 400, "amplitude": 3000}
                        ],
                        "vadWindows": [{"startMs": 0, "endMs": 400}],
                    },
                    "fixture_audio_continuous_noise": {
                        "startedAt": "2026-07-10T09:11:00Z",
                        "sampleRateHz": 8000,
                        "frameMs": 20,
                        "pcmSegments": [
                            {"durationMs": 400, "amplitude": 1000}
                        ],
                        "vadWindows": [{"startMs": 0, "endMs": 400}],
                    },
                },
                "samples": [
                    {
                        "id": "audio_short_stop",
                        "callId": "fixture_audio_short_stop",
                        "sourceType": "synthetic",
                        "category": "short_command_candidate",
                        "expectation": "must_interrupt",
                        "speechStartTime": "2026-07-10T09:10:00.000Z",
                        "maxPreStopLatencyMs": 500,
                    },
                    {
                        "id": "audio_continuous_noise_deferred",
                        "callId": "fixture_audio_continuous_noise",
                        "sourceType": "corpus_noise_mix",
                        "category": "continuous_noise",
                        "expectation": "must_defer",
                        "windowStartTime": "2026-07-10T09:11:00.000Z",
                        "windowEndTime": "2026-07-10T09:11:00.500Z",
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def fake_get_json(url: str, _timeout_seconds: float) -> dict[str, Any]:
        raise AssertionError(f"unexpected API fetch for audio fixture matrix: {url}")

    stdout = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--base-url",
            "http://127.0.0.1:19011",
            "--sample-matrix",
            str(matrix_path),
            "--json",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    payload = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert payload["summary"]["failed"] == 0
    assert payload["summary"]["coverage"]["passed"] is True
    assert payload["samples"][0]["evidence"]["speechStartToPreStopMs"] == 340
    assert payload["samples"][1]["evidence"]["outcome"] == "candidate_without_pre_stop"


def test_p1_eval_cli_audio_fixture_frame_amplitudes_drive_replay(tmp_path) -> None:
    matrix_path = tmp_path / "p1_matrix_audio_frame_amplitudes.json"
    matrix_path.write_text(
        json.dumps(
            {
                "coverageGates": {
                    "minSamples": 1,
                    "requiredCategories": {"short_command_candidate": 1},
                    "requiredSourceTypes": {"synthetic": 1},
                    "requiredExpectations": {"must_interrupt": 1},
                },
                "audioFixtures": {
                    "fixture_audio_modulated_short_stop": {
                        "startedAt": "2026-07-10T09:12:00Z",
                        "sampleRateHz": 8000,
                        "frameMs": 20,
                        "frameAmplitudes": [
                            3000,
                            3600,
                            2800,
                            4200,
                            3100,
                            3900,
                            3300,
                            4500,
                            3600,
                            4200,
                            3900,
                            3500,
                        ],
                        "vadWindows": [{"startMs": 0, "endMs": 240}],
                    }
                },
                "samples": [
                    {
                        "id": "audio_modulated_short_stop",
                        "callId": "fixture_audio_modulated_short_stop",
                        "sourceType": "synthetic",
                        "category": "short_command_candidate",
                        "expectation": "must_interrupt",
                        "speechStartTime": "2026-07-10T09:12:00.000Z",
                        "maxPreStopLatencyMs": 300,
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def fake_get_json(url: str, _timeout_seconds: float) -> dict[str, Any]:
        raise AssertionError(f"unexpected API fetch for audio fixture matrix: {url}")

    stdout = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--base-url",
            "http://127.0.0.1:19011",
            "--sample-matrix",
            str(matrix_path),
            "--json",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    payload = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert payload["summary"]["coverage"]["passed"] is True
    assert payload["samples"][0]["evidence"]["speechStartToPreStopMs"] == 220


def test_p1_eval_cli_audio_fixture_can_replay_runner_authority(tmp_path) -> None:
    matrix_path = tmp_path / "p1_matrix_audio_authority_fixture.json"
    matrix_path.write_text(
        json.dumps(
            {
                "fixtureCoverageGates": {
                    "minSamples": 1,
                    "requiredCategories": {"short_command_candidate": 1},
                    "requiredSourceTypes": {"audio_authority_fixture": 1},
                    "requiredExpectations": {"must_pre_stop_after_candidate": 1},
                },
                "audioFixtures": {
                    "fixture_audio_authority_modulated_short_stop": {
                        "authorityReplay": True,
                        "startedAt": "2026-07-12T12:00:00Z",
                        "sampleRateHz": 8000,
                        "frameMs": 20,
                        "playbackTarget": True,
                        "responseId": "resp_audio_authority",
                        "generation": 2,
                        "frameAmplitudes": [
                            3000,
                            3600,
                            2800,
                            4200,
                            3100,
                            3900,
                            3300,
                            4500,
                            3600,
                            4200,
                            3900,
                            3500,
                        ],
                        "vadWindows": [{"startMs": 0, "endMs": 240}],
                    }
                },
                "samples": [
                    {
                        "id": "audio_authority_modulated_short_stop",
                        "callId": "fixture_audio_authority_modulated_short_stop",
                        "category": "short_command_candidate",
                        "expectation": "must_pre_stop_after_candidate",
                        "candidateTime": "2026-07-12T12:00:00.160Z",
                        "maxCandidateToPreStopMs": 80,
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def fake_get_json(url: str, _timeout_seconds: float) -> dict[str, Any]:
        raise AssertionError(f"unexpected API fetch for audio authority fixture: {url}")

    stdout = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--sample-matrix",
            str(matrix_path),
            "--fixture-only",
            "--json",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    payload = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert payload["summary"]["failed"] == 0
    assert payload["summary"]["coverage"]["passed"] is True
    assert payload["summary"]["evaluationSources"] == {"audio_authority_fixture": 1}
    assert payload["samples"][0]["evaluationSource"] == "audio_authority_fixture"
    assert payload["samples"][0]["sourceType"] == "audio_authority_fixture"
    assert payload["samples"][0]["evidence"]["candidateToPreStopMs"] == 60
    assert payload["samples"][0]["evidence"]["outcome"] == "pre_stop_pending"


def test_p1_eval_cli_audio_authority_replay_uses_ai_track_echo_guard(
    tmp_path,
) -> None:
    sample_rate_hz = 8_000
    frame_ms = 20
    frame_samples = sample_rate_hz * frame_ms // 1000
    customer_samples: list[int] = []
    ai_samples: list[int] = []
    for customer_amplitude, ai_amplitude in zip(
        [2600] * 16,
        [9000] * 16,
        strict=True,
    ):
        customer_samples.extend([customer_amplitude] * frame_samples)
        ai_samples.extend([ai_amplitude] * frame_samples)

    customer_path = tmp_path / "customer.wav"
    ai_path = tmp_path / "ai.wav"
    for path, samples in ((customer_path, customer_samples), (ai_path, ai_samples)):
        with wave.open(str(path), "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(sample_rate_hz)
            wav_file.writeframes(struct.pack("<" + "h" * len(samples), *samples))

    matrix_path = tmp_path / "p1_matrix_audio_authority_ai_echo.json"
    matrix_path.write_text(
        json.dumps(
            {
                "fixtureCoverageGates": {
                    "minSamples": 1,
                    "requiredCategories": {"opening_fan_noise": 1},
                    "requiredSourceTypes": {"audio_authority_fixture": 1},
                    "requiredExpectations": {"must_defer": 1},
                },
                "audioFixtures": {
                    "fixture_audio_authority_ai_echo": {
                        "authorityReplay": True,
                        "startedAt": "2026-07-12T12:10:00Z",
                        "sampleRateHz": sample_rate_hz,
                        "frameMs": frame_ms,
                        "playbackTarget": True,
                        "responseId": "resp_audio_authority_ai_echo",
                        "generation": 2,
                        "wavPath": customer_path.name,
                        "aiWavPath": ai_path.name,
                        "vadWindows": [{"startMs": 0, "endMs": 320}],
                    }
                },
                "samples": [
                    {
                        "id": "audio_authority_ai_echo_must_defer",
                        "callId": "fixture_audio_authority_ai_echo",
                        "category": "opening_fan_noise",
                        "expectation": "must_defer",
                        "windowStartTime": "2026-07-12T12:10:00.000Z",
                        "windowEndTime": "2026-07-12T12:10:00.320Z",
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def fake_get_json(url: str, _timeout_seconds: float) -> dict[str, Any]:
        raise AssertionError(f"unexpected API fetch for audio authority fixture: {url}")

    stdout = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--sample-matrix",
            str(matrix_path),
            "--fixture-only",
            "--json",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    payload = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert payload["summary"]["failed"] == 0
    assert payload["summary"]["coverage"]["passed"] is True
    evidence = payload["samples"][0]["evidence"]
    assert evidence["preStopTime"] is None
    assert evidence["decisionEventType"] == "sip_ai_playback_echo_deferred"
    assert evidence["decisionReason"] == "awaiting_ai_playback_echo_guard"


def test_p1_eval_cli_builds_sample_matrix_from_authority_fixtures_without_fetching_api(
    tmp_path,
) -> None:
    matrix_path = tmp_path / "p1_matrix_authority_fixtures.json"
    matrix_path.write_text(
        json.dumps(
            {
                "fixtureCoverageGates": {
                    "minSamples": 3,
                    "requiredCategories": {
                        "opening_echo_guard": 1,
                        "opening_fan_noise": 1,
                        "sustained_near_end_speech": 1,
                    },
                    "requiredSourceTypes": {"authority_fixture": 3},
                    "requiredExpectations": {
                        "must_defer": 2,
                        "must_pre_stop_after_candidate": 1,
                    },
                },
                "authorityFixtures": {
                    "fixture_authority_echo_local_only": {
                        "startedAt": "2026-07-11T10:00:00Z",
                        "decisionOffsetMs": 260,
                        "responseId": "resp_echo_guarded",
                        "generation": 3,
                        "playbackTarget": True,
                        "opening": False,
                        "recentAiAudio": {"rmsDbfs": -22.0, "ageMs": 20},
                        "detector": {
                            "singleShort": False,
                            "fastLocal": False,
                            "preStopLocal": False,
                            "payload": {
                                "maxSnrDb": 18.5,
                                "rmsRangeDb": 7.0,
                                "rmsDirectionChanges": 1,
                                "largeRmsJumpCount": 0,
                                "speechQualityRejection": None,
                            },
                        },
                        "observation": {
                            "active": True,
                            "candidate": True,
                            "rmsDbfs": -20.5,
                            "noiseFloorDbfs": -40.0,
                            "snrDb": 18.5,
                            "peakDbfs": -12.0,
                            "vadVoicedMs": 260,
                            "candidateDurationMs": 260,
                            "speechDurationMs": 260,
                            "frameDurationMs": 20,
                            "candidateClass": "stable_speech_candidate",
                            "reason": "fixture_echo_guarded_local_only",
                        },
                    },
                    "fixture_authority_opening_stale_lifecycle_noise": {
                        "startedAt": "2026-07-11T02:54:10Z",
                        "decisionOffsetMs": 6066,
                        "responseId": "resp_opening",
                        "generation": 0,
                        "playbackTarget": True,
                        "opening": False,
                        "openingStarted": True,
                        "detector": {
                            "singleShort": False,
                            "fastLocal": True,
                            "preStopLocal": True,
                            "payload": {
                                "maxSnrDb": 20.54,
                                "rmsRangeDb": 11.63,
                                "rmsDirectionChanges": 19,
                                "largeRmsJumpCount": 10,
                                "speechQualityRejection": None,
                            },
                        },
                        "observation": {
                            "active": True,
                            "candidate": True,
                            "rmsDbfs": -16.7,
                            "noiseFloorDbfs": -37.24,
                            "snrDb": 20.54,
                            "peakDbfs": -11.84,
                            "vadVoicedMs": 860,
                            "candidateDurationMs": 860,
                            "speechDurationMs": 860,
                            "frameDurationMs": 20,
                            "candidateClass": "stable_speech_candidate",
                            "reason": "fixture_opening_stale_lifecycle_noise",
                        },
                    },
                    "fixture_authority_sustained_stable_local": {
                        "startedAt": "2026-07-11T10:01:00Z",
                        "decisionOffsetMs": 480,
                        "responseId": "resp_sustained",
                        "generation": 4,
                        "playbackTarget": True,
                        "opening": False,
                        "detector": {
                            "singleShort": False,
                            "fastLocal": True,
                            "preStopLocal": True,
                            "payload": {
                                "maxSnrDb": 24.0,
                                "rmsRangeDb": 7.0,
                                "rmsDirectionChanges": 2,
                                "largeRmsJumpCount": 0,
                                "speechQualityRejection": None,
                            },
                        },
                        "observation": {
                            "active": True,
                            "candidate": True,
                            "rmsDbfs": -24.0,
                            "noiseFloorDbfs": -50.0,
                            "snrDb": 24.0,
                            "peakDbfs": -15.0,
                            "vadVoicedMs": 480,
                            "candidateDurationMs": 480,
                            "speechDurationMs": 480,
                            "frameDurationMs": 20,
                            "candidateClass": "stable_speech_candidate",
                            "reason": "fixture_sustained_stable_local",
                        },
                    },
                },
                "samples": [
                    {
                        "id": "authority_echo_local_only_deferred",
                        "callId": "fixture_authority_echo_local_only",
                        "category": "opening_echo_guard",
                        "expectation": "must_defer",
                        "windowStartTime": "2026-07-11T10:00:00.000Z",
                        "windowEndTime": "2026-07-11T10:00:00.500Z",
                    },
                    {
                        "id": "authority_opening_stale_lifecycle_noise_deferred",
                        "callId": "fixture_authority_opening_stale_lifecycle_noise",
                        "category": "opening_fan_noise",
                        "expectation": "must_defer",
                        "windowStartTime": "2026-07-11T02:54:16.000Z",
                        "windowEndTime": "2026-07-11T02:54:16.200Z",
                    },
                    {
                        "id": "authority_sustained_local_pre_stop",
                        "callId": "fixture_authority_sustained_stable_local",
                        "category": "sustained_near_end_speech",
                        "expectation": "must_pre_stop_after_candidate",
                        "candidateTime": "2026-07-11T10:01:00.480Z",
                        "maxCandidateToPreStopMs": 20,
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def fake_get_json(url: str, _timeout_seconds: float) -> dict[str, Any]:
        raise AssertionError(f"unexpected API fetch for authority fixture matrix: {url}")

    stdout = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--sample-matrix",
            str(matrix_path),
            "--fixture-only",
            "--json",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    payload = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert payload["summary"]["samples"] == 3
    assert payload["summary"]["failed"] == 0
    assert payload["summary"]["coverage"]["passed"] is True
    assert payload["summary"]["evaluationSources"] == {"authority_fixture": 3}
    assert [sample["sourceType"] for sample in payload["samples"]] == [
        "authority_fixture",
        "authority_fixture",
        "authority_fixture",
    ]
    assert [sample["evaluationSource"] for sample in payload["samples"]] == [
        "authority_fixture",
        "authority_fixture",
        "authority_fixture",
    ]
    assert payload["samples"][0]["evidence"]["outcome"] == "candidate_without_pre_stop"
    assert payload["samples"][0]["evidence"]["decisionReason"] == (
        "awaiting_ai_playback_echo_guard"
    )
    assert payload["samples"][1]["evidence"]["outcome"] == "candidate_without_pre_stop"
    assert payload["samples"][1]["evidence"]["decisionReason"] == (
        "awaiting_opening_pre_stop_authority"
    )
    assert payload["samples"][2]["evidence"]["candidateToPreStopMs"] == 0
    assert payload["samples"][2]["evidence"]["outcome"] == "pre_stop_pending"


def test_p1_eval_authority_fixture_detector_implements_runner_reset_hooks() -> None:
    from tools.ai_call_p1_eval import _AuthorityFixtureDetector

    detector = _AuthorityFixtureDetector({})

    detector.reset_activity("call_contract")
    detector.reset("call_contract")


def test_p1_eval_cli_replays_preloaded_echo_guarded_turn_state(tmp_path) -> None:
    matrix_path = tmp_path / "p1_matrix_preloaded_echo_guarded_turn.json"
    matrix_path.write_text(
        json.dumps(
            {
                "fixtureCoverageGates": {
                    "minSamples": 1,
                    "requiredCategories": {"echo_guarded_near_end_speech": 1},
                    "requiredSourceTypes": {"authority_fixture": 1},
                    "requiredExpectations": {"must_pre_stop_after_candidate": 1},
                },
                "authorityFixtures": {
                    "fixture_preloaded_echo_guarded_turn": {
                        "startedAt": "2026-07-11T12:00:00Z",
                        "decisionOffsetMs": 360,
                        "responseId": "resp_preloaded_echo",
                        "generation": 2,
                        "playbackTarget": True,
                        "opening": False,
                        "recentAiAudio": {"rmsDbfs": -18.0, "ageMs": 4},
                        "turn": {
                            "echoGuardedTurn": {
                                "responseId": "resp_preloaded_echo",
                                "generation": 2,
                                "firstOffsetMs": 0,
                                "lastOffsetMs": 0,
                                "burstCount": 1,
                                "voicedMs": 180,
                                "currentBurstVoicedMs": 180,
                                "minRmsDbfs": -25.38,
                                "maxRmsDbfs": -25.38,
                                "maxSnrDb": 20.5,
                                "maxRmsRangeDb": 5.5,
                            }
                        },
                        "detector": {
                            "singleShort": False,
                            "fastLocal": False,
                            "preStopLocal": False,
                            "payload": {
                                "maxSnrDb": 20.5,
                                "rmsRangeDb": 5.93,
                                "rmsDirectionChanges": 3,
                                "largeRmsJumpCount": 0,
                                "speechQualityRejection": None,
                            },
                        },
                        "observation": {
                            "active": True,
                            "candidate": True,
                            "rmsDbfs": -25.08,
                            "noiseFloorDbfs": None,
                            "snrDb": 10.92,
                            "peakDbfs": -18.7,
                            "vadVoicedMs": 240,
                            "candidateDurationMs": 240,
                            "speechDurationMs": 240,
                            "frameDurationMs": 20,
                            "candidateClass": "stable_speech_candidate",
                            "reason": "fixture_preloaded_echo_guarded_turn",
                        },
                    }
                },
                "samples": [
                    {
                        "id": "preloaded_echo_guarded_turn_must_pre_stop",
                        "callId": "fixture_preloaded_echo_guarded_turn",
                        "category": "echo_guarded_near_end_speech",
                        "expectation": "must_pre_stop_after_candidate",
                        "candidateTime": "2026-07-11T12:00:00.360Z",
                        "maxCandidateToPreStopMs": 20,
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def fake_get_json(url: str, _timeout_seconds: float) -> dict[str, Any]:
        raise AssertionError(f"unexpected API fetch for authority fixture matrix: {url}")

    stdout = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--sample-matrix",
            str(matrix_path),
            "--fixture-only",
            "--json",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    payload = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert payload["summary"]["failed"] == 0
    assert payload["samples"][0]["evidence"]["candidateToPreStopMs"] == 0
    assert payload["samples"][0]["evidence"]["outcome"] == "pre_stop_pending"


def test_p1_eval_cli_replays_preloaded_deferred_episode_state(tmp_path) -> None:
    matrix_path = tmp_path / "p1_matrix_haode_keyi_deferred_episode_final.json"
    matrix_path.write_text(
        json.dumps(
            {
                "fixtureCoverageGates": {
                    "minSamples": 1,
                    "requiredCategories": {"short_ack_candidate": 1},
                    "requiredSourceTypes": {"authority_fixture": 1},
                    "requiredExpectations": {"must_pre_stop_after_candidate": 1},
                },
                "authorityFixtures": {
                    "fixture_haode_keyi_deferred_episode_final": {
                        "startedAt": "2026-07-10T03:26:04.490Z",
                        "decisionOffsetMs": 3602,
                        "responseId": "resp_D57VkiQeuzqclhZ7Boqcs",
                        "generation": 4,
                        "playbackTarget": True,
                        "aiPlaybackFrames": [{"offsetMs": 3602, "rmsDbfs": -17.0}],
                        "turn": {
                            "deferredEpisode": {
                                "responseId": "resp_D57VkiQeuzqclhZ7Boqcs",
                                "generation": 4,
                                "firstOffsetMs": 0,
                                "lastOffsetMs": 3601,
                                "burstCount": 3,
                                "voicedMs": 600,
                                "currentBurstVoicedMs": 180,
                                "minRmsDbfs": -24.0,
                                "maxRmsDbfs": -16.59,
                                "maxSnrDb": 19.03,
                                "maxRmsRangeDb": 7.52,
                                "maxGapMs": 1921,
                            }
                        },
                        "detector": {
                            "singleShort": False,
                            "fastLocal": False,
                            "preStopLocal": False,
                            "payload": {
                                "maxSnrDb": 19.03,
                                "rmsRangeDb": 2.82,
                                "rmsDirectionChanges": 0,
                                "largeRmsJumpCount": 0,
                                "speechQualityRejection": None,
                            },
                        },
                        "observation": {
                            "active": True,
                            "candidate": True,
                            "rmsDbfs": -18.43,
                            "noiseFloorDbfs": -33.0,
                            "snrDb": 14.57,
                            "peakDbfs": -12.5,
                            "vadVoicedMs": 180,
                            "candidateDurationMs": 180,
                            "speechDurationMs": 180,
                            "frameDurationMs": 20,
                            "candidateClass": "stable_speech_candidate",
                            "reason": "fixture_haode_keyi_deferred_episode_final",
                        },
                    }
                },
                "samples": [
                    {
                        "id": "haode_keyi_deferred_episode_final_must_pre_stop",
                        "callId": "fixture_haode_keyi_deferred_episode_final",
                        "category": "short_ack_candidate",
                        "expectation": "must_pre_stop_after_candidate",
                        "candidateTime": "2026-07-10T03:26:08.092Z",
                        "maxCandidateToPreStopMs": 20,
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def fake_get_json(url: str, _timeout_seconds: float) -> dict[str, Any]:
        raise AssertionError(f"unexpected API fetch for authority fixture matrix: {url}")

    stdout = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--sample-matrix",
            str(matrix_path),
            "--fixture-only",
            "--json",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    payload = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert payload["summary"]["failed"] == 0
    assert payload["samples"][0]["evidence"]["candidateToPreStopMs"] == 0
    assert payload["samples"][0]["evidence"]["outcome"] == "pre_stop_pending"


def test_p1_eval_cli_builds_authority_fixture_from_sparse_observation_episode(
    tmp_path,
) -> None:
    matrix_path = tmp_path / "p1_matrix_sparse_authority_fixture.json"
    matrix_path.write_text(
        json.dumps(
            {
                "fixtureCoverageGates": {
                    "minSamples": 1,
                    "requiredCategories": {"sparse_short_phrase": 1},
                    "requiredSourceTypes": {"authority_fixture": 1},
                    "requiredExpectations": {"must_pre_stop_after_candidate": 1},
                },
                "authorityFixtures": {
                    "fixture_authority_good_ok_understood_sparse_turn": {
                        "startedAt": "2026-07-11T03:22:01.611826Z",
                        "responseId": "resp_YA2lqDw9QH9ZEWwn22RcU",
                        "generation": 6,
                        "playbackTarget": True,
                        "observations": [
                            {
                                "offsetMs": 0,
                                "recentAiAudio": {"rmsDbfs": -26.0, "ageMs": 0},
                                "detector": {
                                    "singleShort": False,
                                    "fastLocal": False,
                                    "preStopLocal": True,
                                    "payload": {
                                        "maxSnrDb": 23.15,
                                        "rmsRangeDb": 5.02,
                                        "rmsDirectionChanges": 2,
                                        "largeRmsJumpCount": 0,
                                        "speechQualityRejection": None,
                                    },
                                },
                                "observation": {
                                    "active": True,
                                    "candidate": True,
                                    "rmsDbfs": -12.87,
                                    "noiseFloorDbfs": -36.01,
                                    "snrDb": 23.15,
                                    "peakDbfs": -4.94,
                                    "vadVoicedMs": 180,
                                    "candidateDurationMs": 180,
                                    "speechDurationMs": 180,
                                    "frameDurationMs": 20,
                                    "candidateClass": "stable_speech_candidate",
                                    "reason": "fixture_good_ok_understood_part_1",
                                },
                            },
                            {
                                "offsetMs": 3741,
                                "recentAiAudio": {"rmsDbfs": -24.0, "ageMs": 0},
                                "detector": {
                                    "singleShort": False,
                                    "fastLocal": False,
                                    "preStopLocal": True,
                                    "payload": {
                                        "maxSnrDb": 14.53,
                                        "rmsRangeDb": 6.58,
                                        "rmsDirectionChanges": 1,
                                        "largeRmsJumpCount": 1,
                                        "speechQualityRejection": None,
                                    },
                                },
                                "observation": {
                                    "active": True,
                                    "candidate": True,
                                    "rmsDbfs": -21.49,
                                    "noiseFloorDbfs": -36.01,
                                    "snrDb": 14.53,
                                    "peakDbfs": -15.23,
                                    "vadVoicedMs": 180,
                                    "candidateDurationMs": 180,
                                    "speechDurationMs": 180,
                                    "frameDurationMs": 20,
                                    "candidateClass": "stable_speech_candidate",
                                    "reason": "fixture_good_ok_understood_part_2",
                                },
                            },
                            {
                                "offsetMs": 6405,
                                "recentAiAudio": {"rmsDbfs": -24.0, "ageMs": 0},
                                "detector": {
                                    "singleShort": False,
                                    "fastLocal": False,
                                    "preStopLocal": True,
                                    "payload": {
                                        "maxSnrDb": 16.32,
                                        "rmsRangeDb": 7.09,
                                        "rmsDirectionChanges": 2,
                                        "largeRmsJumpCount": 1,
                                        "speechQualityRejection": None,
                                    },
                                },
                                "observation": {
                                    "active": True,
                                    "candidate": True,
                                    "rmsDbfs": -19.7,
                                    "noiseFloorDbfs": -36.01,
                                    "snrDb": 16.32,
                                    "peakDbfs": -12.97,
                                    "vadVoicedMs": 180,
                                    "candidateDurationMs": 180,
                                    "speechDurationMs": 180,
                                    "frameDurationMs": 20,
                                    "candidateClass": "stable_speech_candidate",
                                    "reason": "fixture_good_ok_understood_part_3",
                                },
                            },
                        ],
                    }
                },
                "samples": [
                    {
                        "id": "authority_good_ok_understood_sparse_turn_pre_stop",
                        "callId": "fixture_authority_good_ok_understood_sparse_turn",
                        "category": "sparse_short_phrase",
                        "expectation": "must_pre_stop_after_candidate",
                        "candidateTime": "2026-07-11T03:22:08.016Z",
                        "maxCandidateToPreStopMs": 20,
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def fake_get_json(url: str, _timeout_seconds: float) -> dict[str, Any]:
        raise AssertionError(f"unexpected API fetch for authority fixture matrix: {url}")

    stdout = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--sample-matrix",
            str(matrix_path),
            "--fixture-only",
            "--json",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    payload = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert payload["summary"]["failed"] == 0
    assert payload["summary"]["coverage"]["passed"] is True
    assert payload["samples"][0]["evidence"]["candidateToPreStopMs"] == 0
    assert payload["samples"][0]["evidence"]["outcome"] == "pre_stop_pending"


def test_p1_eval_cli_replays_authority_fixture_ai_playback_frame_timeline(
    tmp_path,
) -> None:
    matrix_path = tmp_path / "p1_matrix_authority_ai_playback_frames.json"
    matrix_path.write_text(
        json.dumps(
            {
                "fixtureCoverageGates": {
                    "minSamples": 2,
                    "requiredCategories": {"short_ack_candidate": 2},
                    "requiredSourceTypes": {"authority_fixture": 2},
                    "requiredExpectations": {"must_defer": 2},
                },
                "authorityFixtures": {
                    "fixture_authority_haode_keyi_ai_playback_frames": {
                        "startedAt": "2026-07-10T03:26:04.000Z",
                        "responseId": "resp_D57VkiQeuzqclhZ7Boqcs",
                        "generation": 4,
                        "playbackTarget": True,
                        "aiPlaybackFrames": [
                            {"offsetMs": 500, "rmsDbfs": -13.97},
                            {"offsetMs": 2120, "rmsDbfs": -17.32},
                        ],
                        "observations": [
                            {
                                "offsetMs": 500,
                                "detector": {
                                    "singleShort": False,
                                    "fastLocal": False,
                                    "preStopLocal": True,
                                    "payload": {
                                        "maxSnrDb": 16.41,
                                        "rmsRangeDb": 5.92,
                                        "rmsDirectionChanges": 3,
                                        "largeRmsJumpCount": 0,
                                        "speechQualityRejection": None,
                                    },
                                },
                                "observation": {
                                    "active": True,
                                    "candidate": True,
                                    "rmsDbfs": -19.52,
                                    "noiseFloorDbfs": -33.0,
                                    "snrDb": 13.48,
                                    "peakDbfs": -12.0,
                                    "vadVoicedMs": 180,
                                    "candidateDurationMs": 220,
                                    "speechDurationMs": 220,
                                    "frameDurationMs": 20,
                                    "candidateClass": "stable_speech_candidate",
                                    "reason": "fixture_haode_keyi_part_1",
                                },
                            },
                            {
                                "offsetMs": 2120,
                                "detector": {
                                    "singleShort": False,
                                    "fastLocal": False,
                                    "preStopLocal": True,
                                    "payload": {
                                        "maxSnrDb": 13.57,
                                        "rmsRangeDb": 5.46,
                                        "rmsDirectionChanges": 0,
                                        "largeRmsJumpCount": 0,
                                        "speechQualityRejection": None,
                                    },
                                },
                                "observation": {
                                    "active": True,
                                    "candidate": True,
                                    "rmsDbfs": -19.43,
                                    "noiseFloorDbfs": -33.0,
                                    "snrDb": 13.57,
                                    "peakDbfs": -13.0,
                                    "vadVoicedMs": 180,
                                    "candidateDurationMs": 180,
                                    "speechDurationMs": 180,
                                    "frameDurationMs": 20,
                                    "candidateClass": "stable_speech_candidate",
                                    "reason": "fixture_haode_keyi_part_2",
                                },
                            },
                        ],
                    }
                },
                "samples": [
                    {
                        "id": "authority_haode_keyi_ai_playback_frames_echo_defer",
                        "callId": "fixture_authority_haode_keyi_ai_playback_frames",
                        "category": "short_ack_candidate",
                        "expectation": "must_defer",
                        "windowStartTime": "2026-07-10T03:26:04.400Z",
                        "windowEndTime": "2026-07-10T03:26:04.700Z",
                    },
                    {
                        "id": "authority_haode_keyi_ai_playback_frames_second_echo_defer",
                        "callId": "fixture_authority_haode_keyi_ai_playback_frames",
                        "category": "short_ack_candidate",
                        "expectation": "must_defer",
                        "windowStartTime": "2026-07-10T03:26:06.000Z",
                        "windowEndTime": "2026-07-10T03:26:06.300Z",
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def fake_get_json(url: str, _timeout_seconds: float) -> dict[str, Any]:
        raise AssertionError(f"unexpected API fetch for authority fixture matrix: {url}")

    stdout = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--sample-matrix",
            str(matrix_path),
            "--fixture-only",
            "--json",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    payload = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert payload["summary"]["failed"] == 0
    defer_evidence = payload["samples"][0]["evidence"]
    assert defer_evidence["outcome"] == "candidate_without_pre_stop"
    assert defer_evidence["decisionReason"] == "awaiting_ai_playback_echo_guard"
    second_defer_evidence = payload["samples"][1]["evidence"]
    assert second_defer_evidence["outcome"] == "candidate_without_pre_stop"
    assert second_defer_evidence["decisionReason"] == "awaiting_ai_playback_echo_guard"


def test_p1_eval_cli_resolves_call_334205_echo_guarded_red_fixtures() -> None:
    matrix_path = Path(
        "docs/livekit-ai-outbound/reports/"
        "phase-e-p1-authority-red-fixtures-call_334205544210567168-2026-07-11.json"
    )

    def fake_get_json(url: str, _timeout_seconds: float) -> dict[str, Any]:
        raise AssertionError(f"unexpected API fetch for authority fixture matrix: {url}")

    stdout = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--sample-matrix",
            str(matrix_path),
            "--fixture-only",
            "--json",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    payload = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert payload["summary"]["failed"] == 0
    assert payload["summary"]["coverage"]["passed"] is True
    assert [sample["passed"] for sample in payload["samples"]] == [True, True]


def test_p1_eval_cli_resolves_call_334214_sparse_ack_red_fixtures() -> None:
    matrix_path = Path(
        "docs/livekit-ai-outbound/reports/"
        "phase-e-p1-authority-red-fixtures-call_334214760864866304-2026-07-11.json"
    )

    def fake_get_json(url: str, _timeout_seconds: float) -> dict[str, Any]:
        raise AssertionError(f"unexpected API fetch for authority fixture matrix: {url}")

    stdout = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--sample-matrix",
            str(matrix_path),
            "--fixture-only",
            "--json",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    payload = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert payload["summary"]["failed"] == 0
    assert payload["summary"]["coverage"]["passed"] is True
    assert [sample["passed"] for sample in payload["samples"]] == [True, True]


def test_p1_eval_cli_resolves_call_333517_bieshuole_gualeba_red_fixture() -> None:
    matrix_path = Path(
        "docs/livekit-ai-outbound/reports/"
        "phase-e-p1-authority-red-fixtures-call_333517350307270656-2026-07-12.json"
    )

    def fake_get_json(url: str, _timeout_seconds: float) -> dict[str, Any]:
        raise AssertionError(f"unexpected API fetch for authority fixture matrix: {url}")

    stdout = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--sample-matrix",
            str(matrix_path),
            "--fixture-only",
            "--json",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    payload = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert payload["summary"]["failed"] == 0
    assert payload["summary"]["coverage"]["passed"] is True
    assert payload["samples"][0]["evidence"]["candidateToPreStopMs"] == 1660


def test_p1_eval_cli_keeps_echo_guarded_periodic_noise_deferred(tmp_path) -> None:
    matrix_path = tmp_path / "p1_matrix_echo_guarded_periodic_noise.json"
    matrix_path.write_text(
        json.dumps(
            {
                "fixtureCoverageGates": {
                    "minSamples": 1,
                    "requiredCategories": {"echo_guarded_periodic_noise": 1},
                    "requiredSourceTypes": {"authority_fixture": 1},
                    "requiredExpectations": {"must_defer": 1},
                },
                "authorityFixtures": {
                    "fixture_echo_guarded_periodic_noise": {
                        "startedAt": "2026-07-11T11:00:00Z",
                        "responseId": "resp_periodic_noise",
                        "generation": 8,
                        "playbackTarget": True,
                        "observations": [
                            {
                                "offsetMs": 0,
                                "recentAiAudio": {"rmsDbfs": -16.0, "ageMs": 20},
                                "detector": {
                                    "singleShort": False,
                                    "fastLocal": False,
                                    "preStopLocal": True,
                                    "payload": {
                                        "maxSnrDb": 10.8,
                                        "rmsRangeDb": 2.2,
                                        "rmsDirectionChanges": 0,
                                        "largeRmsJumpCount": 0,
                                        "speechQualityRejection": None,
                                    },
                                },
                                "observation": {
                                    "active": True,
                                    "candidate": True,
                                    "rmsDbfs": -25.2,
                                    "noiseFloorDbfs": -35.8,
                                    "snrDb": 10.6,
                                    "peakDbfs": -20.0,
                                    "vadVoicedMs": 180,
                                    "candidateDurationMs": 180,
                                    "speechDurationMs": 180,
                                    "frameDurationMs": 20,
                                    "candidateClass": "stable_speech_candidate",
                                    "reason": "fixture_periodic_noise_1",
                                },
                            },
                            {
                                "offsetMs": 3800,
                                "recentAiAudio": {"rmsDbfs": -16.0, "ageMs": 20},
                                "detector": {
                                    "singleShort": False,
                                    "fastLocal": False,
                                    "preStopLocal": True,
                                    "payload": {
                                        "maxSnrDb": 10.9,
                                        "rmsRangeDb": 2.0,
                                        "rmsDirectionChanges": 0,
                                        "largeRmsJumpCount": 0,
                                        "speechQualityRejection": None,
                                    },
                                },
                                "observation": {
                                    "active": True,
                                    "candidate": True,
                                    "rmsDbfs": -25.0,
                                    "noiseFloorDbfs": -35.8,
                                    "snrDb": 10.8,
                                    "peakDbfs": -19.8,
                                    "vadVoicedMs": 180,
                                    "candidateDurationMs": 180,
                                    "speechDurationMs": 180,
                                    "frameDurationMs": 20,
                                    "candidateClass": "stable_speech_candidate",
                                    "reason": "fixture_periodic_noise_2",
                                },
                            },
                            {
                                "offsetMs": 7800,
                                "recentAiAudio": {"rmsDbfs": -16.0, "ageMs": 20},
                                "detector": {
                                    "singleShort": False,
                                    "fastLocal": False,
                                    "preStopLocal": True,
                                    "payload": {
                                        "maxSnrDb": 11.0,
                                        "rmsRangeDb": 2.1,
                                        "rmsDirectionChanges": 0,
                                        "largeRmsJumpCount": 0,
                                        "speechQualityRejection": None,
                                    },
                                },
                                "observation": {
                                    "active": True,
                                    "candidate": True,
                                    "rmsDbfs": -24.9,
                                    "noiseFloorDbfs": -35.8,
                                    "snrDb": 10.9,
                                    "peakDbfs": -19.7,
                                    "vadVoicedMs": 180,
                                    "candidateDurationMs": 180,
                                    "speechDurationMs": 180,
                                    "frameDurationMs": 20,
                                    "candidateClass": "stable_speech_candidate",
                                    "reason": "fixture_periodic_noise_3",
                                },
                            },
                        ],
                    }
                },
                "samples": [
                    {
                        "id": "echo_guarded_periodic_noise_must_defer",
                        "callId": "fixture_echo_guarded_periodic_noise",
                        "category": "echo_guarded_periodic_noise",
                        "expectation": "must_defer",
                        "windowStartTime": "2026-07-11T11:00:07.700Z",
                        "windowEndTime": "2026-07-11T11:00:08.100Z",
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def fake_get_json(url: str, _timeout_seconds: float) -> dict[str, Any]:
        raise AssertionError(f"unexpected API fetch for authority fixture matrix: {url}")

    stdout = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--sample-matrix",
            str(matrix_path),
            "--fixture-only",
            "--json",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    payload = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert payload["summary"]["failed"] == 0
    assert payload["samples"][0]["evidence"]["outcome"] == "candidate_without_pre_stop"


def test_sip_authority_defers_midcall_fan_noise_below_turn_cluster_snr() -> None:
    from datetime import datetime, timedelta

    from app.services.ai_call.agent_runner import PendingUserTurn, RealtimeCallAgentRunner
    from app.services.ai_call.event_store import InMemoryEventStore
    from app.services.ai_call.session_registry import InMemorySessionRegistry
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInObservation
    from tools.ai_call_p1_eval import _AuthorityFixtureDetector

    call_id = "fixture_midcall_fan_echo_guarded_turn"
    response_id = "resp_DfKd1bIwNlRUP2okBsQE6"
    generation = 1
    started_at = datetime.fromisoformat("2026-07-10T03:25:16.010408+00:00")
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: None,
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(),
    )
    guard = runner._playback_guard(call_id)
    guard.current_response_id = response_id
    guard.current_response_generation = generation
    guard.generation = generation
    guard.current_response_audio_published = True
    turn = PendingUserTurn(started_at=started_at)

    def mark_recent_ai_audio(timestamp):
        runner._last_ai_audio_rms_dbfs[call_id] = -18.61
        runner._last_ai_audio_published_at[call_id] = timestamp - timedelta(milliseconds=4)

    first_observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-25.38,
        noise_floor_dbfs=None,
        snr_db=10.62,
        peak_dbfs=-17.01,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="fixture_midcall_fan_part_1",
    )
    runner._sip_barge_in_detector = _AuthorityFixtureDetector({
        "singleShort": False,
        "fastLocal": False,
        "preStopLocal": False,
        "payload": {
            "maxSnrDb": 10.62,
            "rmsRangeDb": 5.37,
            "rmsDirectionChanges": 3,
            "largeRmsJumpCount": 0,
            "speechQualityRejection": None,
        },
    })
    mark_recent_ai_audio(started_at)

    first_decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=started_at,
        observation=first_observation,
    )

    assert first_decision.action == "defer"
    assert first_decision.reason == "awaiting_ai_playback_echo_guard"

    second_timestamp = started_at + timedelta(milliseconds=360)
    second_observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-25.08,
        noise_floor_dbfs=None,
        snr_db=10.92,
        peak_dbfs=-18.7,
        vad_voiced_ms=240,
        candidate_duration_ms=240,
        speech_duration_ms=240,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="fixture_midcall_fan_part_2",
    )
    runner._sip_barge_in_detector = _AuthorityFixtureDetector({
        "singleShort": False,
        "fastLocal": False,
        "preStopLocal": False,
        "payload": {
            "maxSnrDb": 15.98,
            "rmsRangeDb": 5.93,
            "rmsDirectionChanges": 5,
            "largeRmsJumpCount": 0,
            "speechQualityRejection": None,
        },
    })
    mark_recent_ai_audio(second_timestamp)

    second_decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=second_timestamp,
        observation=second_observation,
    )

    assert second_decision.action == "defer"
    assert second_decision.reason == "awaiting_ai_playback_echo_guard"


def test_p1_eval_cli_fixture_only_filters_sample_matrix_to_local_fixtures(tmp_path) -> None:
    matrix_path = tmp_path / "p1_matrix_fixture_only.json"
    matrix_path.write_text(
        json.dumps(
            {
                "coverageGates": {
                    "minSamples": 20,
                    "requiredSourceTypes": {"live_call": 20},
                },
                "fixtureCoverageGates": {
                    "minSamples": 1,
                    "requiredCategories": {"short_command_candidate": 1},
                    "requiredSourceTypes": {"synthetic": 1},
                    "requiredExpectations": {"must_interrupt": 1},
                },
                "audioFixtures": {
                    "fixture_audio_short_stop": {
                        "startedAt": "2026-07-10T09:20:00Z",
                        "sampleRateHz": 8000,
                        "frameMs": 20,
                        "pcmSegments": [
                            {"durationMs": 400, "amplitude": 3000}
                        ],
                        "vadWindows": [{"startMs": 0, "endMs": 400}],
                    }
                },
                "samples": [
                    {
                        "id": "fixture_short_stop",
                        "callId": "fixture_audio_short_stop",
                        "sourceType": "synthetic",
                        "category": "short_command_candidate",
                        "expectation": "must_interrupt",
                        "speechStartTime": "2026-07-10T09:20:00.000Z",
                        "maxPreStopLatencyMs": 500,
                    },
                    {
                        "id": "live_call_sample_should_be_skipped",
                        "callId": "call_live_would_require_api",
                        "sourceType": "live_call",
                        "category": "near_end_speech",
                        "expectation": "must_interrupt",
                        "speechStartTime": "2026-07-10T09:21:00.000Z",
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def fake_get_json(url: str, _timeout_seconds: float) -> dict[str, Any]:
        raise AssertionError(f"unexpected API fetch for fixture-only matrix: {url}")

    stdout = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--sample-matrix",
            str(matrix_path),
            "--fixture-only",
            "--json",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    payload = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert payload["summary"]["samples"] == 1
    assert payload["summary"]["coverage"]["passed"] is True
    assert payload["summary"]["evaluationSources"] == {"audio_fixture": 1}
    assert [sample["id"] for sample in payload["samples"]] == ["fixture_short_stop"]
    assert [sample["evaluationSource"] for sample in payload["samples"]] == [
        "audio_fixture"
    ]


def test_p1_audio_fixture_export_writes_customer_wav_and_matrix_fragment(tmp_path) -> None:
    requested_urls: list[str] = []
    exported: list[dict[str, Any]] = []

    def fake_get_json(url: str, timeout_seconds: float) -> dict[str, Any]:
        requested_urls.append(url)
        assert timeout_seconds == 7.0
        if url.endswith("/ai-call/records/call_export/recording"):
            return {
                "code": 200,
                "data": {
                    "callId": "call_export",
                    "tracks": [
                        {
                            "trackRole": "customer",
                            "playUrl": "https://audio.test/customer.ogg",
                            "startedAt": "2026-07-10T09:00:00Z",
                        },
                        {
                            "trackRole": "ai",
                            "playUrl": "https://audio.test/ai.ogg",
                            "startedAt": "2026-07-10T09:00:00Z",
                        },
                    ],
                },
            }
        raise AssertionError(f"unexpected url: {url}")

    def fake_export_wav(
        play_url: str,
        output_path,
        sample_rate_hz: int,
        timeout_seconds: float,
        start_ms: int,
        end_ms: int,
    ) -> None:
        exported.append(
            {
                "playUrl": play_url,
                "outputPath": str(output_path),
                "sampleRateHz": sample_rate_hz,
                "timeoutSeconds": timeout_seconds,
                "startMs": start_ms,
                "endMs": end_ms,
            }
        )
        output_path.write_bytes(b"fake wav")

    stdout = io.StringIO()
    exit_code = run_export_p1_audio_fixture(
        [
            "--base-url",
            "http://127.0.0.1:19011",
            "--call-id",
            "call_export",
            "--start-ms",
            "1000",
            "--end-ms",
            "1600",
            "--category",
            "opening_fan_noise",
            "--expectation",
            "must_defer",
            "--output-dir",
            str(tmp_path),
            "--timeout-seconds",
            "7",
            "--json",
        ],
        get_json=fake_get_json,
        export_wav=fake_export_wav,
        stdout=stdout,
    )

    payload = json.loads(stdout.getvalue())
    fixture_path = tmp_path / "call_export_customer_1000_1600.wav"
    fragment_path = tmp_path / "call_export_customer_1000_1600.fixture.json"
    fragment = json.loads(fragment_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert requested_urls == [
        "http://127.0.0.1:19011/ai-call/records/call_export/recording"
    ]
    assert exported == [
        {
            "playUrl": "https://audio.test/customer.ogg",
            "outputPath": str(fixture_path),
            "sampleRateHz": 16000,
            "timeoutSeconds": 7.0,
            "startMs": 1000,
            "endMs": 1600,
        }
    ]
    assert fixture_path.read_bytes() == b"fake wav"
    assert payload["fixturePath"] == str(fixture_path)
    assert payload["fragmentPath"] == str(fragment_path)
    assert list(fragment["audioFixtures"]) == ["call_export_customer_1000_1600"]
    assert fragment["audioFixtures"]["call_export_customer_1000_1600"] == {
        "startedAt": "2026-07-10T09:00:01.000Z",
        "sampleRateHz": 16000,
        "frameMs": 20,
        "wavPath": "call_export_customer_1000_1600.wav",
        "vadWindows": [{"startMs": 0, "endMs": 600}],
    }
    assert fragment["samples"] == [
        {
            "id": "call_export_customer_1000_1600_opening_fan_noise_must_defer",
            "callId": "call_export_customer_1000_1600",
            "sourceType": "live_call",
            "evaluationSource": "audio_fixture",
            "category": "opening_fan_noise",
            "expectation": "must_defer",
            "windowStartTime": "2026-07-10T09:00:01.000Z",
            "windowEndTime": "2026-07-10T09:00:01.600Z",
        }
    ]


def test_p1_audio_fixture_export_can_include_aligned_ai_wav(tmp_path) -> None:
    exported: list[dict[str, Any]] = []

    def fake_get_json(url: str, _timeout_seconds: float) -> dict[str, Any]:
        if url.endswith("/ai-call/records/call_export_with_ai/recording"):
            return {
                "code": 200,
                "data": {
                    "callId": "call_export_with_ai",
                    "tracks": [
                        {
                            "trackRole": "customer",
                            "playUrl": "https://audio.test/customer.ogg",
                            "startedAt": "2026-07-10T09:00:00Z",
                        },
                        {
                            "trackRole": "ai",
                            "playUrl": "https://audio.test/ai.ogg",
                            "startedAt": "2026-07-10T09:00:00.200Z",
                        },
                    ],
                },
            }
        raise AssertionError(f"unexpected url: {url}")

    def fake_export_wav(
        play_url: str,
        output_path,
        sample_rate_hz: int,
        _timeout_seconds: float,
        start_ms: int,
        end_ms: int,
    ) -> None:
        exported.append(
            {
                "playUrl": play_url,
                "outputPath": output_path.name,
                "sampleRateHz": sample_rate_hz,
                "startMs": start_ms,
                "endMs": end_ms,
            }
        )
        quiet_samples = [180] * (sample_rate_hz * 200 // 1000)
        speech_samples = [4000] * (sample_rate_hz * 500 // 1000)
        samples = quiet_samples + speech_samples
        with wave.open(str(output_path), "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(sample_rate_hz)
            wav_file.writeframes(struct.pack("<" + "h" * len(samples), *samples))

    exit_code = run_export_p1_audio_fixture(
        [
            "--base-url",
            "http://127.0.0.1:19011",
            "--call-id",
            "call_export_with_ai",
            "--start-ms",
            "1000",
            "--end-ms",
            "1800",
            "--category",
            "short_ack_candidate",
            "--expectation",
            "must_pre_stop_after_candidate",
            "--include-ai-track",
            "--output-dir",
            str(tmp_path),
        ],
        get_json=fake_get_json,
        export_wav=fake_export_wav,
        stdout=io.StringIO(),
    )

    fragment_path = tmp_path / "call_export_with_ai_customer_1000_1800.fixture.json"
    fragment = json.loads(fragment_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert exported == [
        {
            "playUrl": "https://audio.test/customer.ogg",
            "outputPath": "call_export_with_ai_customer_1000_1800.wav",
            "sampleRateHz": 16000,
            "startMs": 1000,
            "endMs": 1800,
        },
        {
            "playUrl": "https://audio.test/ai.ogg",
            "outputPath": "call_export_with_ai_customer_1000_1800.ai.wav",
            "sampleRateHz": 16000,
            "startMs": 800,
            "endMs": 1600,
        },
    ]
    assert fragment["audioFixtures"]["call_export_with_ai_customer_1000_1800"][
        "aiWavPath"
    ] == "call_export_with_ai_customer_1000_1800.ai.wav"


def test_p1_audio_fixture_export_uses_acoustic_start_for_interrupt_sample(tmp_path) -> None:
    def fake_get_json(url: str, _timeout_seconds: float) -> dict[str, Any]:
        if url.endswith("/ai-call/records/call_export_speech_start/recording"):
            return {
                "code": 200,
                "data": {
                    "callId": "call_export_speech_start",
                    "tracks": [
                        {
                            "trackRole": "customer",
                            "playUrl": "https://audio.test/customer.ogg",
                            "startedAt": "2026-07-10T09:00:00Z",
                        }
                    ],
                },
            }
        raise AssertionError(f"unexpected url: {url}")

    def fake_export_wav(
        _play_url: str,
        output_path,
        sample_rate_hz: int,
        _timeout_seconds: float,
        _start_ms: int,
        _end_ms: int,
    ) -> None:
        quiet_samples = [180] * (sample_rate_hz * 300 // 1000)
        speech_samples = [4000] * (sample_rate_hz * 500 // 1000)
        samples = quiet_samples + speech_samples
        with wave.open(str(output_path), "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(sample_rate_hz)
            wav_file.writeframes(struct.pack("<" + "h" * len(samples), *samples))

    exit_code = run_export_p1_audio_fixture(
        [
            "--base-url",
            "http://127.0.0.1:19011",
            "--call-id",
            "call_export_speech_start",
            "--start-ms",
            "1000",
            "--end-ms",
            "1800",
            "--category",
            "short_command_candidate",
            "--expectation",
            "must_interrupt",
            "--output-dir",
            str(tmp_path),
        ],
        get_json=fake_get_json,
        export_wav=fake_export_wav,
        stdout=io.StringIO(),
    )

    fragment_path = tmp_path / "call_export_speech_start_customer_1000_1800.fixture.json"
    fragment = json.loads(fragment_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert fragment["audioFixtures"]["call_export_speech_start_customer_1000_1800"][
        "vadWindows"
    ] == [{"startMs": 300, "endMs": 800}]
    assert fragment["samples"][0]["speechStartTime"] == "2026-07-10T09:00:01.300Z"


def test_p1_audio_fixture_export_ignores_short_bursts_before_stable_speech(
    tmp_path,
) -> None:
    def fake_get_json(url: str, _timeout_seconds: float) -> dict[str, Any]:
        if url.endswith("/ai-call/records/call_export_stable_speech/recording"):
            return {
                "code": 200,
                "data": {
                    "callId": "call_export_stable_speech",
                    "tracks": [
                        {
                            "trackRole": "customer",
                            "playUrl": "https://audio.test/customer.ogg",
                            "startedAt": "2026-07-10T09:00:00Z",
                        }
                    ],
                },
            }
        raise AssertionError(f"unexpected url: {url}")

    def fake_export_wav(
        _play_url: str,
        output_path,
        sample_rate_hz: int,
        _timeout_seconds: float,
        _start_ms: int,
        _end_ms: int,
    ) -> None:
        frame_samples = sample_rate_hz * 20 // 1000
        frame_amplitudes = (
            [3000]
            + [120] * 12
            + [4500, 4500, 4500]
            + [120] * 22
            + [5000] * 25
        )
        samples: list[int] = []
        for amplitude in frame_amplitudes:
            samples.extend([amplitude] * frame_samples)
        with wave.open(str(output_path), "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(sample_rate_hz)
            wav_file.writeframes(struct.pack("<" + "h" * len(samples), *samples))

    exit_code = run_export_p1_audio_fixture(
        [
            "--base-url",
            "http://127.0.0.1:19011",
            "--call-id",
            "call_export_stable_speech",
            "--start-ms",
            "43000",
            "--end-ms",
            "44000",
            "--category",
            "near_end_speech",
            "--expectation",
            "must_interrupt",
            "--output-dir",
            str(tmp_path),
        ],
        get_json=fake_get_json,
        export_wav=fake_export_wav,
        stdout=io.StringIO(),
    )

    fragment_path = tmp_path / "call_export_stable_speech_customer_43000_44000.fixture.json"
    fragment = json.loads(fragment_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert fragment["audioFixtures"]["call_export_stable_speech_customer_43000_44000"][
        "vadWindows"
    ] == [{"startMs": 760, "endMs": 1000}]
    assert fragment["samples"][0]["speechStartTime"] == "2026-07-10T09:00:43.760Z"


def test_p1_audio_fixture_export_uses_acoustic_start_for_candidate_sample(tmp_path) -> None:
    def fake_get_json(url: str, _timeout_seconds: float) -> dict[str, Any]:
        if url.endswith("/ai-call/records/call_export_candidate/recording"):
            return {
                "code": 200,
                "data": {
                    "callId": "call_export_candidate",
                    "tracks": [
                        {
                            "trackRole": "customer",
                            "playUrl": "https://audio.test/customer.ogg",
                            "startedAt": "2026-07-10T09:00:00Z",
                        }
                    ],
                },
            }
        raise AssertionError(f"unexpected url: {url}")

    def fake_export_wav(
        _play_url: str,
        output_path,
        sample_rate_hz: int,
        _timeout_seconds: float,
        _start_ms: int,
        _end_ms: int,
    ) -> None:
        quiet_samples = [180] * (sample_rate_hz * 260 // 1000)
        speech_samples = [4000] * (sample_rate_hz * 500 // 1000)
        samples = quiet_samples + speech_samples
        with wave.open(str(output_path), "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(sample_rate_hz)
            wav_file.writeframes(struct.pack("<" + "h" * len(samples), *samples))

    exit_code = run_export_p1_audio_fixture(
        [
            "--base-url",
            "http://127.0.0.1:19011",
            "--call-id",
            "call_export_candidate",
            "--start-ms",
            "2000",
            "--end-ms",
            "2800",
            "--category",
            "short_command_candidate",
            "--expectation",
            "must_pre_stop_after_candidate",
            "--output-dir",
            str(tmp_path),
        ],
        get_json=fake_get_json,
        export_wav=fake_export_wav,
        stdout=io.StringIO(),
    )

    fragment_path = tmp_path / "call_export_candidate_customer_2000_2800.fixture.json"
    fragment = json.loads(fragment_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert fragment["audioFixtures"]["call_export_candidate_customer_2000_2800"][
        "vadWindows"
    ] == [{"startMs": 260, "endMs": 800}]
    assert fragment["samples"] == [
        {
            "id": (
                "call_export_candidate_customer_2000_2800_"
                "short_command_candidate_must_pre_stop_after_candidate"
            ),
            "callId": "call_export_candidate_customer_2000_2800",
            "sourceType": "live_call",
            "evaluationSource": "audio_fixture",
            "category": "short_command_candidate",
            "expectation": "must_pre_stop_after_candidate",
            "candidateTime": "2026-07-10T09:00:02.260Z",
            "maxCandidateToPreStopMs": 500,
        }
    ]


def test_p1_audio_fixture_export_rejects_candidate_sample_without_stable_speech(
    tmp_path,
) -> None:
    def fake_get_json(url: str, _timeout_seconds: float) -> dict[str, Any]:
        if url.endswith("/ai-call/records/call_export_short_burst/recording"):
            return {
                "code": 200,
                "data": {
                    "callId": "call_export_short_burst",
                    "tracks": [
                        {
                            "trackRole": "customer",
                            "playUrl": "https://audio.test/customer.ogg",
                            "startedAt": "2026-07-10T09:00:00Z",
                        }
                    ],
                },
            }
        raise AssertionError(f"unexpected url: {url}")

    def fake_export_wav(
        _play_url: str,
        output_path,
        sample_rate_hz: int,
        _timeout_seconds: float,
        _start_ms: int,
        _end_ms: int,
    ) -> None:
        frame_samples = sample_rate_hz * 20 // 1000
        frame_amplitudes = [4200, 4200, 4200] + [120] * 37
        samples: list[int] = []
        for amplitude in frame_amplitudes:
            samples.extend([amplitude] * frame_samples)
        with wave.open(str(output_path), "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(sample_rate_hz)
            wav_file.writeframes(struct.pack("<" + "h" * len(samples), *samples))

    stderr = io.StringIO()
    exit_code = run_export_p1_audio_fixture(
        [
            "--base-url",
            "http://127.0.0.1:19011",
            "--call-id",
            "call_export_short_burst",
            "--start-ms",
            "31000",
            "--end-ms",
            "31800",
            "--category",
            "short_command_candidate",
            "--expectation",
            "must_pre_stop_after_candidate",
            "--output-dir",
            str(tmp_path),
        ],
        get_json=fake_get_json,
        export_wav=fake_export_wav,
        stdout=io.StringIO(),
        stderr=stderr,
    )

    assert exit_code == 1
    assert "stable acoustic speech" in stderr.getvalue()


def test_p1_eval_cli_prints_sample_matrix_failure_evidence(tmp_path) -> None:
    matrix_path = tmp_path / "p1_matrix.json"
    matrix_path.write_text(
        json.dumps(
            {
                "samples": [
                    {
                        "id": "opening_noise",
                        "callId": "call_matrix_evidence",
                        "category": "opening_noise",
                        "expectation": "must_not_interrupt",
                        "windowStartTime": "2026-07-08T09:00:01.000Z",
                        "windowEndTime": "2026-07-08T09:00:02.000Z",
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def fake_get_json(url: str, timeout_seconds: float) -> dict[str, Any]:
        assert timeout_seconds == 7.0
        if url.endswith("/ai-call/records/call_matrix_evidence"):
            return {
                "code": 200,
                "data": {
                    "record": {
                        "callId": "call_matrix_evidence",
                        "status": "completed",
                        "startedAt": "2026-07-08T09:00:00Z",
                    }
                },
            }
        if url.endswith("/ai-call/records/call_matrix_evidence/events?limit=1000"):
            return {
                "code": 200,
                "data": {
                    "rows": [
                        _event(
                            "sip_interrupt_candidate",
                            "2026-07-08T09:00:01.100Z",
                            response_id="resp_opening",
                            generation=0,
                        ),
                        _event(
                            "sip_pre_stop",
                            "2026-07-08T09:00:01.220Z",
                            response_id="resp_opening",
                            generation=1,
                        ),
                        _event(
                            "sip_interrupt_rejected",
                            "2026-07-08T09:00:01.520Z",
                            response_id="resp_opening",
                            generation=1,
                            reason="rejected_noise",
                        ),
                    ]
                },
            }
        if url.endswith("/ai-call/records/call_matrix_evidence/dialogue-segments?limit=1000"):
            return {"code": 200, "data": {"rows": [], "total": 0}}
        raise AssertionError(f"unexpected url: {url}")

    stdout = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--base-url",
            "http://127.0.0.1:19011",
            "--sample-matrix",
            str(matrix_path),
            "--timeout-seconds",
            "7",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    output = stdout.getvalue()
    assert exit_code == 2
    assert "id=opening_noise" in output
    assert "preStopTime=2026-07-08T09:00:01.220Z" in output
    assert "outcome=false_pre_stop_rejected" in output
    assert "decisionReason=rejected_noise" in output


def test_p1_eval_cli_marks_missing_sample_matrix_call_report(tmp_path) -> None:
    matrix_path = tmp_path / "p1_matrix.json"
    matrix_path.write_text(
        json.dumps(
            {
                "samples": [
                    {
                        "id": "missing_call_sample",
                        "callId": "call_missing_from_local_db",
                        "category": "near_end_speech",
                        "expectation": "must_interrupt",
                        "speechStartTime": "2026-07-08T09:00:01.000Z",
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def fake_get_json(url: str, _timeout_seconds: float) -> dict[str, Any]:
        if url.endswith("/ai-call/records/call_missing_from_local_db"):
            raise RuntimeError('{"code":500,"msg":"通话记录不存在","data":null}')
        raise AssertionError(f"unexpected url: {url}")

    stdout = io.StringIO()
    stderr = io.StringIO()
    exit_code = run_p1_eval(
        [
            "--base-url",
            "http://127.0.0.1:19011",
            "--sample-matrix",
            str(matrix_path),
            "--json",
        ],
        get_json=fake_get_json,
        stdout=stdout,
        stderr=stderr,
    )

    payload = json.loads(stdout.getvalue())
    assert exit_code == 2
    assert stderr.getvalue() == ""
    assert payload["summary"]["missingReports"] == 1
    assert payload["failedCalls"] == [
        {
            "callId": "call_missing_from_local_db",
            "error": '{"code":500,"msg":"通话记录不存在","data":null}',
        }
    ]
    assert payload["samples"][0]["reason"] == "missing_report"


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
