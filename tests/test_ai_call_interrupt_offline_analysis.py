from __future__ import annotations

import io
import json
import math
import subprocess
import sys
from typing import Any

from app.services.ai_call.interrupt_offline_analysis import build_offline_interrupt_report
from tools.ai_call_interrupt_replay import run


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
