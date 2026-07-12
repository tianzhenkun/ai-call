from __future__ import annotations

import io
import json

from app.services.ai_call.interrupt_vad_shadow import build_vad_shadow_report
from tools.ai_call_vad_shadow import run as run_vad_shadow


def test_vad_shadow_classifies_windows_by_existing_evidence() -> None:
    report = build_vad_shadow_report(
        call_id="call_shadow",
        record={"callId": "call_shadow"},
        recording={
            "tracks": [
                {
                    "trackRole": "customer",
                    "startedAt": "2026-07-03T09:00:00.000Z",
                    "playUrl": "https://example.invalid/customer.ogg",
                }
            ]
        },
        events=[
            {
                "eventType": "user_speech_started",
                "eventTime": "2026-07-03T09:00:03.350Z",
                "payload": {},
            },
            {
                "eventType": "sip_interrupt_candidate",
                "eventTime": "2026-07-03T09:00:05.220Z",
                "payload": {"snrDb": 18.0},
            },
        ],
        dialogue_segments=[
            {
                "speakerType": "customer",
                "source": "offline_asr",
                "text": "行。",
                "startedAt": "2026-07-03T09:00:01.100Z",
                "endedAt": "2026-07-03T09:00:01.900Z",
            }
        ],
        vad_windows=[
            {"startMs": 1_000, "endMs": 1_650},
            {"startMs": 3_000, "endMs": 3_400},
            {"startMs": 5_000, "endMs": 5_300},
            {"startMs": 8_000, "endMs": 8_240},
        ],
    )

    assert report["summary"]["classifications"] == {
        "offline_asr_speech": 1,
        "realtime_speech_or_provider": 1,
        "p1_candidate_without_transcript": 1,
        "unexplained_by_events": 1,
    }
    assert [window["classification"] for window in report["vadWindows"]] == [
        "offline_asr_speech",
        "realtime_speech_or_provider",
        "p1_candidate_without_transcript",
        "unexplained_by_events",
    ]
    assert report["offlineSpeech"]["segments"] == 1
    assert report["offlineSpeech"]["detected"] == 1
    assert report["offlineSpeech"]["missed"] == 0
    assert report["offlineSpeech"]["startLagMs"]["avg"] == -100


def test_vad_shadow_flags_slow_or_missing_offline_speech_detection() -> None:
    report = build_vad_shadow_report(
        call_id="call_shadow_miss",
        record={"callId": "call_shadow_miss"},
        recording={
            "tracks": [
                {
                    "trackRole": "customer",
                    "startedAt": "2026-07-03T09:00:00.000Z",
                    "playUrl": "https://example.invalid/customer.ogg",
                }
            ]
        },
        events=[],
        dialogue_segments=[
            {
                "speakerType": "customer",
                "source": "offline_asr",
                "text": "你说什么？",
                "startedAt": "2026-07-03T09:00:01.000Z",
                "endedAt": "2026-07-03T09:00:02.000Z",
            },
            {
                "speakerType": "customer",
                "source": "offline_asr",
                "text": "说快点。",
                "startedAt": "2026-07-03T09:00:05.000Z",
                "endedAt": "2026-07-03T09:00:06.000Z",
            },
        ],
        vad_windows=[
            {"startMs": 1_700, "endMs": 2_100},
        ],
        max_detection_lag_ms=500,
    )

    assert report["offlineSpeech"]["segments"] == 2
    assert report["offlineSpeech"]["detected"] == 1
    assert report["offlineSpeech"]["missed"] == 1
    assert report["offlineSpeech"]["slow"] == 1
    assert report["offlineSpeech"]["withinMaxLag"] == 0
    assert report["offlineSpeech"]["missedSegments"] == [
        {
            "text": "说快点。",
            "startedAt": "2026-07-03T09:00:05.000Z",
            "endedAt": "2026-07-03T09:00:06.000Z",
            "reason": "missing_vad_window",
        }
    ]
    assert report["offlineSpeech"]["slowSegments"][0]["text"] == "你说什么？"
    assert report["offlineSpeech"]["slowSegments"][0]["vadStartLagMs"] == 700


def test_vad_shadow_aligns_realtime_shadow_events_with_offline_speech() -> None:
    report = build_vad_shadow_report(
        call_id="call_shadow_realtime",
        record={"callId": "call_shadow_realtime"},
        recording={
            "tracks": [
                {
                    "trackRole": "customer",
                    "startedAt": "2026-07-03T09:00:00.000Z",
                    "playUrl": "https://example.invalid/customer.ogg",
                }
            ]
        },
        events=[
            {
                "eventType": "sip_vad_shadow_started",
                "eventTime": "2026-07-03T09:00:01.180Z",
                "payload": {"detector": "fsmn_shadow", "confidence": 0.91},
            },
            {
                "eventType": "sip_vad_shadow_ended",
                "eventTime": "2026-07-03T09:00:01.780Z",
                "payload": {"detector": "fsmn_shadow", "durationMs": 600},
            },
            {
                "eventType": "sip_vad_shadow_started",
                "eventTime": "2026-07-03T09:00:04.800Z",
                "payload": {"detector": "fsmn_shadow", "confidence": 0.84},
            },
            {
                "eventType": "sip_vad_shadow_ended",
                "eventTime": "2026-07-03T09:00:05.200Z",
                "payload": {"detector": "fsmn_shadow", "durationMs": 400},
            },
        ],
        dialogue_segments=[
            {
                "speakerType": "customer",
                "source": "offline_asr",
                "text": "你说快点。",
                "startedAt": "2026-07-03T09:00:01.000Z",
                "endedAt": "2026-07-03T09:00:01.900Z",
            },
            {
                "speakerType": "customer",
                "source": "offline_asr",
                "text": "不用了。",
                "startedAt": "2026-07-03T09:00:03.000Z",
                "endedAt": "2026-07-03T09:00:03.500Z",
            },
            {
                "speakerType": "customer",
                "source": "offline_asr",
                "text": "停一下。",
                "startedAt": "2026-07-03T09:00:04.000Z",
                "endedAt": "2026-07-03T09:00:05.000Z",
            },
        ],
        vad_windows=[],
        max_detection_lag_ms=500,
    )

    assert report["realtimeShadowSpeech"]["segments"] == 3
    assert report["realtimeShadowSpeech"]["detected"] == 2
    assert report["realtimeShadowSpeech"]["missed"] == 1
    assert report["realtimeShadowSpeech"]["slow"] == 1
    assert report["realtimeShadowSpeech"]["withinMaxLag"] == 1
    assert report["realtimeShadowSpeech"]["startLagMs"] == {
        "avg": 490,
        "min": 180,
        "max": 800,
    }
    assert report["realtimeShadowSpeech"]["missedSegments"] == [
        {
            "text": "不用了。",
            "startedAt": "2026-07-03T09:00:03.000Z",
            "endedAt": "2026-07-03T09:00:03.500Z",
            "reason": "missing_vad_window",
        }
    ]
    assert report["realtimeShadowWindows"] == [
        {
            "index": 0,
            "startMs": 1180,
            "endMs": 1780,
            "durationMs": 600,
            "startedAtRaw": "2026-07-03T09:00:01.180Z",
            "endedAtRaw": "2026-07-03T09:00:01.780Z",
            "detector": "fsmn_shadow",
            "confidence": 0.91,
        },
        {
            "index": 1,
            "startMs": 4800,
            "endMs": 5200,
            "durationMs": 400,
            "startedAtRaw": "2026-07-03T09:00:04.800Z",
            "endedAtRaw": "2026-07-03T09:00:05.200Z",
            "detector": "fsmn_shadow",
            "confidence": 0.84,
        },
    ]


def test_vad_shadow_reports_realtime_shadow_speech_by_detector() -> None:
    report = build_vad_shadow_report(
        call_id="call_shadow_by_detector",
        record={"callId": "call_shadow_by_detector"},
        recording={
            "tracks": [
                {
                    "trackRole": "customer",
                    "startedAt": "2026-07-03T09:00:00.000Z",
                    "playUrl": "https://example.invalid/customer.ogg",
                }
            ]
        },
        events=[
            {
                "eventType": "sip_vad_shadow_started",
                "eventTime": "2026-07-03T09:00:01.100Z",
                "payload": {"detector": "webrtc_shadow"},
            },
            {
                "eventType": "sip_vad_shadow_ended",
                "eventTime": "2026-07-03T09:00:01.700Z",
                "payload": {"detector": "webrtc_shadow"},
            },
            {
                "eventType": "sip_vad_shadow_started",
                "eventTime": "2026-07-03T09:00:05.050Z",
                "payload": {"detector": "webrtc_shadow"},
            },
            {
                "eventType": "sip_vad_shadow_ended",
                "eventTime": "2026-07-03T09:00:05.400Z",
                "payload": {"detector": "webrtc_shadow"},
            },
            {
                "eventType": "sip_vad_shadow_started",
                "eventTime": "2026-07-03T09:00:01.700Z",
                "payload": {
                    "detector": "fsmn_shadow",
                    "detectionLagMs": 500,
                    "speechEndLagMs": 100,
                    "bufferDurationMs": 1200,
                    "windowStartMs": 700,
                    "windowEndMs": 1100,
                },
            },
            {
                "eventType": "sip_vad_shadow_ended",
                "eventTime": "2026-07-03T09:00:02.000Z",
                "payload": {"detector": "fsmn_shadow"},
            },
        ],
        dialogue_segments=[
            {
                "speakerType": "customer",
                "source": "offline_asr",
                "text": "你说快点。",
                "startedAt": "2026-07-03T09:00:01.000Z",
                "endedAt": "2026-07-03T09:00:01.900Z",
            },
            {
                "speakerType": "customer",
                "source": "offline_asr",
                "text": "挂了吧。",
                "startedAt": "2026-07-03T09:00:05.000Z",
                "endedAt": "2026-07-03T09:00:05.600Z",
            },
        ],
        vad_windows=[],
        max_detection_lag_ms=500,
    )

    assert report["realtimeShadowSpeechByDetector"]["webrtc_shadow"][
        "detected"
    ] == 2
    assert report["realtimeShadowSpeechByDetector"]["webrtc_shadow"][
        "withinMaxLag"
    ] == 2
    assert report["realtimeShadowSpeechByDetector"]["fsmn_shadow"]["detected"] == 1
    assert report["realtimeShadowSpeechByDetector"]["fsmn_shadow"]["missed"] == 1
    assert report["realtimeShadowSpeechByDetector"]["fsmn_shadow"][
        "missedSegments"
    ] == [
        {
            "text": "挂了吧。",
            "startedAt": "2026-07-03T09:00:05.000Z",
            "endedAt": "2026-07-03T09:00:05.600Z",
            "reason": "missing_vad_window",
        }
    ]


def test_vad_shadow_uses_realtime_shadow_payload_lag_for_model_window_time() -> None:
    report = build_vad_shadow_report(
        call_id="call_shadow_realtime_lag",
        record={"callId": "call_shadow_realtime_lag"},
        recording={
            "tracks": [
                {
                    "trackRole": "customer",
                    "startedAt": "2026-07-03T09:00:00.000Z",
                    "playUrl": "https://example.invalid/customer.ogg",
                }
            ]
        },
        events=[
            {
                "eventType": "sip_vad_shadow_started",
                "eventTime": "2026-07-03T09:00:01.500Z",
                "payload": {
                    "detector": "fsmn_shadow",
                    "confidence": 0.91,
                    "detectionLagMs": 500,
                    "speechEndLagMs": 100,
                    "bufferDurationMs": 1200,
                    "windowStartMs": 700,
                    "windowEndMs": 1100,
                },
            },
            {
                "eventType": "sip_vad_shadow_ended",
                "eventTime": "2026-07-03T09:00:02.100Z",
                "payload": {"detector": "fsmn_shadow", "durationMs": 600},
            },
        ],
        dialogue_segments=[
            {
                "speakerType": "customer",
                "source": "offline_asr",
                "text": "你说快点。",
                "startedAt": "2026-07-03T09:00:01.000Z",
                "endedAt": "2026-07-03T09:00:01.900Z",
            },
        ],
        vad_windows=[],
        max_detection_lag_ms=500,
    )

    assert report["realtimeShadowSpeech"]["startLagMs"] == {
        "avg": 0,
        "min": 0,
        "max": 0,
    }
    assert report["realtimeShadowWindows"] == [
        {
            "index": 0,
            "startMs": 1000,
            "endMs": 1400,
            "durationMs": 400,
            "startedAtRaw": "2026-07-03T09:00:01.000Z",
            "endedAtRaw": "2026-07-03T09:00:01.400Z",
            "detector": "fsmn_shadow",
            "confidence": 0.91,
            "detectionLagMs": 500,
            "speechEndLagMs": 100,
            "bufferDurationMs": 1200,
            "windowStartMs": 700,
            "windowEndMs": 1100,
        },
    ]


def test_vad_shadow_reports_deferred_pre_stop_inside_offline_speech() -> None:
    report = build_vad_shadow_report(
        call_id="call_shadow_deferred",
        record={"callId": "call_shadow_deferred"},
        recording={
            "tracks": [
                {
                    "trackRole": "customer",
                    "startedAt": "2026-07-03T09:00:00.000Z",
                    "playUrl": "https://example.invalid/customer.ogg",
                }
            ]
        },
        events=[
            {
                "eventType": "sip_interrupt_candidate",
                "eventTime": "2026-07-03T09:00:01.200Z",
                "payload": {
                    "responseId": "resp_current",
                    "generation": 3,
                    "candidateClass": "stable_speech_candidate",
                    "candidateDurationMs": 180,
                    "snrDb": 28.0,
                    "rmsDbfs": -12.6,
                },
            },
            {
                "eventType": "sip_pre_stop_deferred",
                "eventTime": "2026-07-03T09:00:01.201Z",
                "payload": {
                    "responseId": "resp_current",
                    "generation": 3,
                    "reason": "awaiting_pre_stop_authority",
                    "requiredPreStopDurationMs": 240,
                    "candidateDurationMs": 180,
                    "snrDb": 28.0,
                    "rmsDbfs": -12.6,
                    "speechQualityRejection": None,
                },
            },
            {
                "eventType": "sip_ai_playback_echo_deferred",
                "eventTime": "2026-07-03T09:00:01.260Z",
                "payload": {
                    "responseId": "resp_current",
                    "generation": 3,
                    "reason": "awaiting_ai_playback_echo_guard",
                    "candidateDurationMs": 240,
                    "snrDb": 18.9,
                    "rmsDbfs": -21.7,
                    "aiPlaybackRmsDbfs": -12.7,
                },
            },
            {
                "eventType": "sip_interrupt_candidate_expired",
                "eventTime": "2026-07-03T09:00:02.200Z",
                "payload": {
                    "responseId": "resp_current",
                    "generation": 3,
                    "reason": "stale_deferred_pre_stop_candidate",
                },
            },
            {
                "eventType": "sip_vad_shadow_started",
                "eventTime": "2026-07-03T09:00:01.520Z",
                "payload": {
                    "detector": "fsmn_shadow",
                    "detectionLagMs": 520,
                    "speechEndLagMs": 20,
                    "bufferDurationMs": 1200,
                    "windowStartMs": 660,
                    "windowEndMs": 1180,
                },
            },
            {
                "eventType": "sip_vad_shadow_ended",
                "eventTime": "2026-07-03T09:00:02.000Z",
                "payload": {"detector": "fsmn_shadow"},
            },
        ],
        dialogue_segments=[
            {
                "speakerType": "customer",
                "source": "offline_asr",
                "text": "好的好的。",
                "startedAt": "2026-07-03T09:00:01.000Z",
                "endedAt": "2026-07-03T09:00:02.400Z",
            }
        ],
        vad_windows=[{"startMs": 1_000, "endMs": 2_100}],
    )

    assert report["deferredPreStops"]["segments"] == 1
    assert report["deferredPreStops"]["blockedSegments"] == 1
    assert report["deferredPreStops"]["deferredEvents"] == 2
    assert report["deferredPreStops"]["deferredDuringOfflineSpeech"] == 2
    assert report["deferredPreStops"]["reasons"] == {
        "awaiting_pre_stop_authority": 1,
        "awaiting_ai_playback_echo_guard": 1,
    }
    assert report["deferredPreStops"]["blockedSpeechSegments"] == [
        {
            "text": "好的好的。",
            "startedAt": "2026-07-03T09:00:01.000Z",
            "endedAt": "2026-07-03T09:00:02.400Z",
            "firstDeferredTime": "2026-07-03T09:00:01.201Z",
            "firstDeferredLagMs": 201,
            "preStopTime": None,
            "expired": True,
            "deferredReasons": [
                "awaiting_pre_stop_authority",
                "awaiting_ai_playback_echo_guard",
            ],
            "realtimeShadowDetectors": ["fsmn_shadow"],
        }
    ]


def test_vad_shadow_cli_builds_report_from_windows_file(tmp_path) -> None:
    windows_path = tmp_path / "windows.json"
    windows_path.write_text(
        json.dumps(
            {
                "windowsByCallId": {
                    "call_shadow": [{"startMs": 1_000, "endMs": 1_800}]
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    requested_urls: list[str] = []

    def fake_get_json(url: str, timeout_seconds: float) -> dict:
        requested_urls.append(url)
        assert timeout_seconds == 7.0
        if url.endswith("/ai-call/records/call_shadow"):
            return {
                "code": 200,
                "data": {
                    "record": {
                        "callId": "call_shadow",
                        "entryType": "sip_outbound",
                        "status": "completed",
                    }
                },
            }
        if url.endswith("/ai-call/records/call_shadow/events?limit=1000"):
            return {"code": 200, "data": {"rows": []}}
        if url.endswith("/ai-call/records/call_shadow/dialogue-segments?limit=1000"):
            return {
                "code": 200,
                "data": {
                    "rows": [
                        {
                            "speakerType": "customer",
                            "source": "offline_asr",
                            "text": "行。",
                            "startedAt": "2026-07-03T09:00:01.100Z",
                            "endedAt": "2026-07-03T09:00:01.900Z",
                        }
                    ]
                },
            }
        if url.endswith("/ai-call/records/call_shadow/recording"):
            return {
                "code": 200,
                "data": {
                    "tracks": [
                        {
                            "trackRole": "customer",
                            "startedAt": "2026-07-03T09:00:00.000Z",
                            "playUrl": "https://example.invalid/customer.ogg",
                        }
                    ]
                },
            }
        raise AssertionError(f"unexpected url: {url}")

    stdout = io.StringIO()
    exit_code = run_vad_shadow(
        [
            "--base-url",
            "http://127.0.0.1:19011",
            "--call-id",
            "call_shadow",
            "--vad-windows-file",
            str(windows_path),
            "--timeout-seconds",
            "7",
            "--json",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    assert exit_code == 0
    payload = json.loads(stdout.getvalue())
    assert payload["mode"] == "vad_shadow"
    assert payload["summary"]["classifications"] == {"offline_asr_speech": 1}
    assert requested_urls == [
        "http://127.0.0.1:19011/ai-call/records/call_shadow",
        "http://127.0.0.1:19011/ai-call/records/call_shadow/events?limit=1000",
        "http://127.0.0.1:19011/ai-call/records/call_shadow/dialogue-segments?limit=1000",
        "http://127.0.0.1:19011/ai-call/records/call_shadow/recording",
    ]


def test_vad_shadow_cli_summarizes_realtime_shadow_in_recent_suite(tmp_path) -> None:
    windows_path = tmp_path / "windows.json"
    windows_path.write_text(
        json.dumps(
            {
                "windowsByCallId": {
                    "call_shadow_a": [{"startMs": 1_000, "endMs": 1_800}],
                    "call_shadow_b": [],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def fake_get_json(url: str, _timeout_seconds: float) -> dict:
        if url.endswith("/ai-call/records?entryType=sip_outbound&pageSize=2"):
            return {
                "code": 200,
                "data": {
                    "total": 2,
                    "rows": [
                        {"callId": "call_shadow_a"},
                        {"callId": "call_shadow_b"},
                    ],
                },
            }
        call_id = "call_shadow_a" if "call_shadow_a" in url else "call_shadow_b"
        if url.endswith(f"/ai-call/records/{call_id}"):
            return {
                "code": 200,
                "data": {
                    "record": {
                        "callId": call_id,
                        "entryType": "sip_outbound",
                        "status": "completed",
                    }
                },
            }
        if url.endswith(f"/ai-call/records/{call_id}/events?limit=1000"):
            rows = (
                [
                    {
                        "eventType": "sip_vad_shadow_started",
                        "eventTime": "2026-07-03T09:00:01.100Z",
                        "payload": {"detector": "fsmn_shadow"},
                    },
                    {
                        "eventType": "sip_vad_shadow_ended",
                        "eventTime": "2026-07-03T09:00:01.700Z",
                        "payload": {"detector": "fsmn_shadow"},
                    },
                ]
                if call_id == "call_shadow_a"
                else []
            )
            return {"code": 200, "data": {"rows": rows}}
        if url.endswith(f"/ai-call/records/{call_id}/dialogue-segments?limit=1000"):
            return {
                "code": 200,
                "data": {
                    "rows": [
                        {
                            "speakerType": "customer",
                            "source": "offline_asr",
                            "text": "你说快点。",
                            "startedAt": "2026-07-03T09:00:01.000Z",
                            "endedAt": "2026-07-03T09:00:01.900Z",
                        }
                    ]
                },
            }
        if url.endswith(f"/ai-call/records/{call_id}/recording"):
            return {
                "code": 200,
                "data": {
                    "tracks": [
                        {
                            "trackRole": "customer",
                            "startedAt": "2026-07-03T09:00:00.000Z",
                            "playUrl": "https://example.invalid/customer.ogg",
                        }
                    ]
                },
            }
        raise AssertionError(f"unexpected url: {url}")

    stdout = io.StringIO()
    exit_code = run_vad_shadow(
        [
            "--base-url",
            "http://127.0.0.1:19011",
            "--recent",
            "2",
            "--vad-windows-file",
            str(windows_path),
            "--json",
        ],
        get_json=fake_get_json,
        stdout=stdout,
    )

    assert exit_code == 0
    payload = json.loads(stdout.getvalue())
    assert payload["summary"]["shadowSegments"] == 2
    assert payload["summary"]["shadowDetected"] == 1
    assert payload["summary"]["shadowMissed"] == 1
    assert payload["summary"]["shadowSlow"] == 0
    assert payload["summary"]["shadowByDetector"] == {
        "fsmn_shadow": {
            "segments": 2,
            "detected": 1,
            "missed": 1,
            "slow": 0,
            "withinMaxLag": 1,
        }
    }
