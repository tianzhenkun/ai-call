from __future__ import annotations

from tools.ai_call_p1_realtime_shadow_compare import build_comparison_report


def test_realtime_shadow_compare_reports_detector_and_prestop_lag() -> None:
    report = build_comparison_report(
        call_id="call_compare",
        track_started_at="2026-07-03T09:00:00.000Z",
        asr_sentences=[
            {"text": "你说快点。", "beginMs": 1_000, "endMs": 1_800},
            {"text": "挂了吧。", "beginMs": 5_000, "endMs": 5_600},
        ],
        ai_segments=[
            {
                "startedAt": "2026-07-03T09:00:00.500Z",
                "endedAt": "2026-07-03T09:00:06.000Z",
            }
        ],
        events=[
            {
                "eventType": "sip_vad_shadow_started",
                "eventTime": "2026-07-03T09:00:01.120Z",
                "payload": {"detector": "webrtc_shadow"},
            },
            {
                "eventType": "sip_vad_shadow_ended",
                "eventTime": "2026-07-03T09:00:01.700Z",
                "payload": {"detector": "webrtc_shadow"},
            },
            {
                "eventType": "sip_vad_shadow_started",
                "eventTime": "2026-07-03T09:00:05.080Z",
                "payload": {"detector": "webrtc_shadow"},
            },
            {
                "eventType": "sip_vad_shadow_ended",
                "eventTime": "2026-07-03T09:00:05.380Z",
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
            {
                "eventType": "sip_pre_stop",
                "eventTime": "2026-07-03T09:00:05.940Z",
                "payload": {"reason": "sip_uplink_speech_during_ai_audio"},
            },
        ],
    )

    assert report["summary"]["segments"] == 2
    assert report["summary"]["bargeInSegments"] == 2
    assert report["summary"]["providers"]["webrtc_shadow"]["detected"] == 2
    assert report["summary"]["providers"]["webrtc_shadow"]["withinMaxLag"] == 2
    assert report["summary"]["providers"]["fsmn_shadow"]["detected"] == 1
    assert report["summary"]["providers"]["fsmn_shadow"]["missed"] == 1
    assert report["summary"]["primaryPreStop"]["detected"] == 1
    assert report["summary"]["primaryPreStop"]["missed"] == 1
    assert report["segments"][1]["providers"]["fsmn_shadow"]["detected"] is False
    assert report["segments"][1]["preStop"]["startLagMs"] == 940
