from __future__ import annotations

import inspect
import sqlite3

import tools.ai_call_p1_realtime_shadow_compare as compare_tool
from tools.ai_call_p1_realtime_shadow_compare import (
    _build_report_from_db,
    _recent_call_ids,
    build_comparison_report,
)


def test_realtime_shadow_compare_recording_track_queries_match_tenant() -> None:
    report_query = inspect.getsource(_build_report_from_db).lower()
    recent_query = inspect.getsource(_recent_call_ids).lower()

    assert "record.tenant_id = track.tenant_id" in report_query
    assert "t.tenant_id = r.tenant_id" in recent_query


def test_realtime_shadow_compare_asr_job_uses_selected_tenant_track(monkeypatch) -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        create table ai_call_record (
            id integer primary key,
            tenant_id text not null,
            call_id text not null
        );
        create table ai_call_recording_track (
            id integer primary key,
            tenant_id text not null,
            call_id text not null,
            track_role text not null,
            started_at text not null
        );
        create table ai_call_asr_job (
            id integer primary key,
            call_id text not null,
            track_id integer not null,
            track_role text not null,
            status text not null,
            transcription_url text
        );
        insert into ai_call_record values (1, 'tenant-a', 'shared-call');
        insert into ai_call_record values (2, 'tenant-b', 'shared-call');
        insert into ai_call_recording_track
            values (20, 'tenant-a', 'shared-call', 'customer', '2026-07-03T09:00:00Z');
        insert into ai_call_recording_track
            values (10, 'tenant-b', 'shared-call', 'customer', '2026-07-03T09:00:00Z');
        insert into ai_call_asr_job
            values (100, 'shared-call', 20, 'customer', 'completed',
                    'https://asr.test/tenant-a.json');
        insert into ai_call_asr_job
            values (200, 'shared-call', 10, 'customer', 'completed',
                    'https://asr.test/tenant-b.json');
        """
    )
    fetched_urls: list[str] = []

    def fake_asr_sentences(url: str) -> list[dict]:
        fetched_urls.append(url)
        return []

    monkeypatch.setattr(compare_tool, "_asr_sentences_from_url", fake_asr_sentences)
    monkeypatch.setattr(compare_tool, "_ai_segments_for_call", lambda *_: [])
    monkeypatch.setattr(compare_tool, "_events_for_call", lambda *_: [])

    compare_tool._build_report_from_db(
        conn=conn,
        call_id="shared-call",
        max_detection_lag_ms=500,
        max_pre_stop_latency_ms=800,
    )

    assert fetched_urls == ["https://asr.test/tenant-a.json"]
    conn.close()


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
