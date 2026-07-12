from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import urllib.request
from collections.abc import Sequence
from datetime import datetime, timedelta
from statistics import median
from typing import Any, TextIO


def build_comparison_report(
    *,
    call_id: str,
    track_started_at: str,
    asr_sentences: list[dict[str, Any]],
    ai_segments: list[dict[str, Any]],
    events: list[dict[str, Any]],
    max_detection_lag_ms: int = 500,
    max_pre_stop_latency_ms: int = 800,
) -> dict[str, Any]:
    track_start = _parse_time(track_started_at)
    normalized_segments = [
        _normalize_asr_sentence(index, sentence, track_start)
        for index, sentence in enumerate(asr_sentences)
    ]
    normalized_ai_segments = [
        _normalize_ai_segment(index, segment)
        for index, segment in enumerate(ai_segments)
    ]
    normalized_events = [_normalize_event(index, event) for index, event in enumerate(events)]
    normalized_events.sort(key=lambda event: (event["timestamp"] or datetime.max, event["index"]))
    shadow_windows = _shadow_windows(normalized_events)
    provider_names = sorted({window["detector"] for window in shadow_windows})
    pre_stop_events = [
        event for event in normalized_events if event["eventType"] == "sip_pre_stop"
    ]

    segment_reports = [
        _segment_report(
            segment=segment,
            ai_segments=normalized_ai_segments,
            provider_names=provider_names,
            shadow_windows=shadow_windows,
            pre_stop_events=pre_stop_events,
            max_detection_lag_ms=max_detection_lag_ms,
            max_pre_stop_latency_ms=max_pre_stop_latency_ms,
        )
        for segment in normalized_segments
    ]
    barge_in_segments = [
        segment for segment in segment_reports if segment["bargeInRelevant"]
    ]
    return {
        "mode": "p1_realtime_shadow_compare",
        "callId": call_id,
        "summary": _summary(
            segment_reports=segment_reports,
            barge_in_segments=barge_in_segments,
            provider_names=provider_names,
            max_detection_lag_ms=max_detection_lag_ms,
            max_pre_stop_latency_ms=max_pre_stop_latency_ms,
        ),
        "segments": segment_reports,
    }


def build_suite_report(reports: list[dict[str, Any]]) -> dict[str, Any]:
    provider_names = sorted({
        provider
        for report in reports
        for provider in (report.get("summary", {}).get("providers") or {})
    })
    summary = {
        "calls": len(reports),
        "segments": 0,
        "bargeInSegments": 0,
        "providers": {
            provider: _empty_provider_summary() for provider in provider_names
        },
        "primaryPreStop": _empty_provider_summary(),
    }
    provider_lags: dict[str, list[int]] = {provider: [] for provider in provider_names}
    pre_stop_lags: list[int] = []
    for report in reports:
        report_summary = report.get("summary") or {}
        summary["segments"] += int(report_summary.get("segments") or 0)
        barge_in_count = int(report_summary.get("bargeInSegments") or 0)
        summary["bargeInSegments"] += barge_in_count
        report_providers = report_summary.get("providers") or {}
        for provider in provider_names:
            source = report_providers.get(provider)
            bucket = summary["providers"][provider]
            if source is None:
                bucket["segments"] += barge_in_count
                bucket["missed"] += barge_in_count
                continue
            _add_provider_summary(bucket, source)
            provider_lags[provider].extend(source.get("lagsMs") or [])
        _add_provider_summary(
            summary["primaryPreStop"],
            report_summary.get("primaryPreStop") or {},
        )
        pre_stop_lags.extend((report_summary.get("primaryPreStop") or {}).get("lagsMs") or [])

    for provider, lags in provider_lags.items():
        summary["providers"][provider]["startLagMs"] = _lag_summary(lags)
        summary["providers"][provider].pop("lagsMs", None)
    summary["primaryPreStop"]["startLagMs"] = _lag_summary(pre_stop_lags)
    summary["primaryPreStop"].pop("lagsMs", None)
    return {
        "mode": "p1_realtime_shadow_compare_suite",
        "summary": summary,
        "calls": reports,
    }


def main(argv: Sequence[str] | None = None) -> None:
    raise SystemExit(run(argv))


def run(
    argv: Sequence[str] | None = None,
    *,
    stdout: TextIO = sys.stdout,
    stderr: TextIO = sys.stderr,
) -> int:
    parser = argparse.ArgumentParser(
        description="Compare realtime WebRTC/FSMN shadow events against raw ASR sentences.",
    )
    parser.add_argument("--sqlite-db", default="/tmp/ai_call_ed81_local.db")
    parser.add_argument("--call-id")
    parser.add_argument("--recent", type=int)
    parser.add_argument("--max-detection-lag-ms", type=int, default=500)
    parser.add_argument("--max-pre-stop-latency-ms", type=int, default=800)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    if bool(args.call_id) == bool(args.recent):
        parser.error("exactly one of --call-id or --recent is required")
    if args.recent is not None and args.recent < 1:
        parser.error("--recent must be greater than 0")

    try:
        conn = sqlite3.connect(args.sqlite_db)
        conn.row_factory = sqlite3.Row
        reports = [
            _build_report_from_db(
                conn=conn,
                call_id=call_id,
                max_detection_lag_ms=args.max_detection_lag_ms,
                max_pre_stop_latency_ms=args.max_pre_stop_latency_ms,
            )
            for call_id in (
                _recent_call_ids(conn, args.recent)
                if args.recent is not None
                else [str(args.call_id)]
            )
        ]
        report = build_suite_report(reports) if args.recent is not None else reports[0]
    except Exception as exc:
        print(f"compare failed: {exc!s}", file=stderr)
        return 1

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2), file=stdout)
    else:
        _print_text_report(report, stdout)
    return 0


def _build_report_from_db(
    *,
    conn: sqlite3.Connection,
    call_id: str,
    max_detection_lag_ms: int,
    max_pre_stop_latency_ms: int,
) -> dict[str, Any]:
    track = conn.execute(
        """
        select started_at
        from ai_call_recording_track
        where call_id = ? and track_role = 'customer'
        order by id desc
        limit 1
        """,
        (call_id,),
    ).fetchone()
    if track is None:
        raise RuntimeError(f"{call_id}: missing customer recording track")
    asr_job = conn.execute(
        """
        select transcription_url
        from ai_call_asr_job
        where call_id = ? and track_role = 'customer' and status = 'completed'
        order by id desc
        limit 1
        """,
        (call_id,),
    ).fetchone()
    if asr_job is None or not asr_job["transcription_url"]:
        raise RuntimeError(f"{call_id}: missing completed customer ASR job")

    return build_comparison_report(
        call_id=call_id,
        track_started_at=str(track["started_at"]),
        asr_sentences=_asr_sentences_from_url(str(asr_job["transcription_url"])),
        ai_segments=_ai_segments_for_call(conn, call_id),
        events=_events_for_call(conn, call_id),
        max_detection_lag_ms=max_detection_lag_ms,
        max_pre_stop_latency_ms=max_pre_stop_latency_ms,
    )


def _recent_call_ids(conn: sqlite3.Connection, recent: int) -> list[str]:
    rows = conn.execute(
        """
        select r.call_id
        from ai_call_record r
        where r.entry_type = 'sip_outbound'
          and exists (
              select 1 from ai_call_recording_track t
              where t.call_id = r.call_id and t.track_role = 'customer'
          )
          and exists (
              select 1 from ai_call_asr_job j
              where j.call_id = r.call_id
                and j.track_role = 'customer'
                and j.status = 'completed'
          )
        order by r.started_at desc
        limit ?
        """,
        (recent,),
    ).fetchall()
    return [str(row["call_id"]) for row in rows]


def _asr_sentences_from_url(url: str) -> list[dict[str, Any]]:
    with urllib.request.urlopen(url, timeout=20) as response:
        payload = json.loads(response.read().decode("utf-8"))
    sentences: list[dict[str, Any]] = []
    for transcript in payload.get("transcripts") or []:
        if not isinstance(transcript, dict):
            continue
        for sentence in transcript.get("sentences") or []:
            if not isinstance(sentence, dict):
                continue
            begin_ms = _first_present(sentence.get("begin_time"), sentence.get("beginMs"))
            end_ms = _first_present(sentence.get("end_time"), sentence.get("endMs"))
            if begin_ms is None or end_ms is None:
                continue
            sentences.append({
                "text": str(sentence.get("text") or ""),
                "beginMs": int(begin_ms),
                "endMs": int(end_ms),
            })
    return sentences


def _events_for_call(conn: sqlite3.Connection, call_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        select event_time, event_type, payload_json
        from ai_call_event
        where call_id = ?
        order by event_time, id
        """,
        (call_id,),
    ).fetchall()
    events: list[dict[str, Any]] = []
    for row in rows:
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except json.JSONDecodeError:
            payload = {}
        events.append({
            "eventType": row["event_type"],
            "eventTime": row["event_time"],
            "payload": payload,
        })
    return events


def _ai_segments_for_call(conn: sqlite3.Connection, call_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        select started_at, ended_at, segment_text
        from ai_call_dialogue_segment
        where call_id = ?
          and speaker_type = 'ai'
          and started_at is not null
          and ended_at is not null
        order by started_at, segment_no
        """,
        (call_id,),
    ).fetchall()
    return [
        {
            "startedAt": row["started_at"],
            "endedAt": row["ended_at"],
            "text": row["segment_text"],
        }
        for row in rows
    ]


def _normalize_asr_sentence(
    index: int,
    sentence: dict[str, Any],
    track_start: datetime,
) -> dict[str, Any]:
    start_ms = int(_first_present(sentence.get("beginMs"), sentence.get("begin_ms")) or 0)
    end_ms = int(_first_present(sentence.get("endMs"), sentence.get("end_ms")) or 0)
    started_at = track_start + timedelta(milliseconds=start_ms)
    ended_at = track_start + timedelta(milliseconds=end_ms)
    return {
        "index": index,
        "text": str(sentence.get("text") or ""),
        "startMs": start_ms,
        "endMs": end_ms,
        "startedAt": started_at,
        "endedAt": ended_at,
        "startedAtRaw": _format_time(started_at),
        "endedAtRaw": _format_time(ended_at),
    }


def _normalize_ai_segment(index: int, segment: dict[str, Any]) -> dict[str, Any]:
    started_at = _parse_time(segment.get("startedAt") or segment.get("started_at"))
    ended_at = _parse_time(segment.get("endedAt") or segment.get("ended_at"))
    return {
        "index": index,
        "startedAt": started_at,
        "endedAt": ended_at,
    }


def _normalize_event(index: int, event: dict[str, Any]) -> dict[str, Any]:
    event_time = event.get("eventTime") or event.get("event_time")
    payload = event.get("payload")
    return {
        "index": index,
        "eventType": str(event.get("eventType") or event.get("event_type") or ""),
        "eventTime": event_time,
        "timestamp": _parse_time(event_time),
        "payload": payload if isinstance(payload, dict) else {},
    }


def _shadow_windows(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    active: dict[str, dict[str, Any]] = {}
    windows: list[dict[str, Any]] = []
    for event in events:
        if event["eventType"] not in {"sip_vad_shadow_started", "sip_vad_shadow_ended"}:
            continue
        timestamp = event["timestamp"]
        if timestamp is None:
            continue
        detector = str(event["payload"].get("detector") or "unknown")
        if event["eventType"] == "sip_vad_shadow_started":
            active[detector] = event
            continue
        started_event = active.pop(detector, None)
        if started_event is None:
            continue
        started_at = _time_minus_lag(
            started_event["timestamp"],
            started_event["payload"].get("detectionLagMs"),
        ) or started_event["timestamp"]
        ended_at = _time_minus_lag(
            started_event["timestamp"],
            started_event["payload"].get("speechEndLagMs"),
        ) or timestamp
        if ended_at < started_at:
            ended_at = timestamp
        windows.append({
            "detector": detector,
            "startedAt": started_at,
            "endedAt": ended_at,
            "startedAtRaw": _format_time(started_at),
            "endedAtRaw": _format_time(ended_at),
        })
    return windows


def _segment_report(
    *,
    segment: dict[str, Any],
    ai_segments: list[dict[str, Any]],
    provider_names: list[str],
    shadow_windows: list[dict[str, Any]],
    pre_stop_events: list[dict[str, Any]],
    max_detection_lag_ms: int,
    max_pre_stop_latency_ms: int,
) -> dict[str, Any]:
    barge_in_relevant = any(
        _overlaps(segment["startedAt"], segment["endedAt"], item["startedAt"], item["endedAt"])
        for item in ai_segments
    )
    providers = {
        provider: _provider_segment_result(
            segment=segment,
            windows=[
                window for window in shadow_windows if window["detector"] == provider
            ],
            max_detection_lag_ms=max_detection_lag_ms,
        )
        for provider in provider_names
    }
    pre_stop = _pre_stop_segment_result(
        segment=segment,
        pre_stop_events=pre_stop_events,
        max_pre_stop_latency_ms=max_pre_stop_latency_ms,
    )
    return {
        "text": segment["text"],
        "startedAt": segment["startedAtRaw"],
        "endedAt": segment["endedAtRaw"],
        "bargeInRelevant": barge_in_relevant,
        "providers": providers,
        "preStop": pre_stop,
    }


def _provider_segment_result(
    *,
    segment: dict[str, Any],
    windows: list[dict[str, Any]],
    max_detection_lag_ms: int,
) -> dict[str, Any]:
    matches = [
        window
        for window in windows
        if _overlaps(
            window["startedAt"],
            window["endedAt"],
            segment["startedAt"],
            segment["endedAt"],
        )
    ]
    if not matches:
        return {"detected": False, "startLagMs": None, "withinMaxLag": False}
    first = min(
        matches,
        key=lambda window: abs(
            (window["startedAt"] - segment["startedAt"]).total_seconds()
        ),
    )
    lag_ms = _elapsed_ms(segment["startedAt"], first["startedAt"])
    return {
        "detected": True,
        "startLagMs": lag_ms,
        "withinMaxLag": lag_ms <= max_detection_lag_ms,
        "windowStartedAt": first["startedAtRaw"],
        "windowEndedAt": first["endedAtRaw"],
    }


def _pre_stop_segment_result(
    *,
    segment: dict[str, Any],
    pre_stop_events: list[dict[str, Any]],
    max_pre_stop_latency_ms: int,
) -> dict[str, Any]:
    lower = segment["startedAt"] - timedelta(milliseconds=300)
    upper = segment["endedAt"] + timedelta(milliseconds=2_500)
    eligible = [
        event
        for event in pre_stop_events
        if event["timestamp"] is not None and lower <= event["timestamp"] <= upper
    ]
    if not eligible:
        return {"detected": False, "startLagMs": None, "withinMaxLag": False}
    first = min(eligible, key=lambda event: (event["timestamp"], event["index"]))
    lag_ms = _elapsed_ms(segment["startedAt"], first["timestamp"])
    return {
        "detected": True,
        "startLagMs": lag_ms,
        "withinMaxLag": lag_ms <= max_pre_stop_latency_ms,
        "eventTime": first["eventTime"],
    }


def _summary(
    *,
    segment_reports: list[dict[str, Any]],
    barge_in_segments: list[dict[str, Any]],
    provider_names: list[str],
    max_detection_lag_ms: int,
    max_pre_stop_latency_ms: int,
) -> dict[str, Any]:
    return {
        "segments": len(segment_reports),
        "bargeInSegments": len(barge_in_segments),
        "providers": {
            provider: _provider_summary(
                [
                    segment["providers"][provider]
                    for segment in barge_in_segments
                    if provider in segment["providers"]
                ],
                max_lag_ms=max_detection_lag_ms,
            )
            for provider in provider_names
        },
        "primaryPreStop": _provider_summary(
            [segment["preStop"] for segment in barge_in_segments],
            max_lag_ms=max_pre_stop_latency_ms,
        ),
    }


def _provider_summary(results: list[dict[str, Any]], *, max_lag_ms: int) -> dict[str, Any]:
    lags = [
        int(result["startLagMs"])
        for result in results
        if result.get("detected") and result.get("startLagMs") is not None
    ]
    detected = len(lags)
    segments = len(results)
    return {
        "segments": segments,
        "detected": detected,
        "missed": segments - detected,
        "withinMaxLag": sum(1 for lag in lags if lag <= max_lag_ms),
        "slow": sum(1 for lag in lags if lag > max_lag_ms),
        "maxLagMs": max_lag_ms,
        "startLagMs": _lag_summary(lags),
        "lagsMs": lags,
    }


def _empty_provider_summary() -> dict[str, Any]:
    return {
        "segments": 0,
        "detected": 0,
        "missed": 0,
        "withinMaxLag": 0,
        "slow": 0,
        "lagsMs": [],
    }


def _add_provider_summary(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key in ("segments", "detected", "missed", "withinMaxLag", "slow"):
        target[key] += int(source.get(key) or 0)


def _lag_summary(values: list[int]) -> dict[str, Any]:
    if not values:
        return {"min": None, "p50": None, "p90": None, "max": None, "avg": None}
    ordered = sorted(values)
    p90_index = max(0, min(len(ordered) - 1, int(len(ordered) * 0.9) - 1))
    return {
        "min": ordered[0],
        "p50": int(median(ordered)),
        "p90": ordered[p90_index],
        "max": ordered[-1],
        "avg": round(sum(ordered) / len(ordered)),
    }


def _print_text_report(report: dict[str, Any], stdout: TextIO) -> None:
    summary = report.get("summary") or {}
    print(
        f"{report.get('mode')} "
        f"calls={summary.get('calls', 1)} "
        f"segments={summary.get('segments')} "
        f"bargeInSegments={summary.get('bargeInSegments')}",
        file=stdout,
    )
    for provider, item in (summary.get("providers") or {}).items():
        print(
            f"provider {provider} "
            f"detected={item.get('detected')} "
            f"missed={item.get('missed')} "
            f"withinMaxLag={item.get('withinMaxLag')} "
            f"slow={item.get('slow')} "
            f"lag={item.get('startLagMs')}",
            file=stdout,
        )
    pre_stop = summary.get("primaryPreStop") or {}
    print(
        "primaryPreStop "
        f"detected={pre_stop.get('detected')} "
        f"missed={pre_stop.get('missed')} "
        f"withinMaxLag={pre_stop.get('withinMaxLag')} "
        f"slow={pre_stop.get('slow')} "
        f"lag={pre_stop.get('startLagMs')}",
        file=stdout,
    )


def _overlaps(
    first_started: datetime | None,
    first_ended: datetime | None,
    second_started: datetime | None,
    second_ended: datetime | None,
) -> bool:
    if None in (first_started, first_ended, second_started, second_ended):
        return False
    return first_started < second_ended and first_ended > second_started


def _time_minus_lag(timestamp: datetime | None, value: Any) -> datetime | None:
    if timestamp is None:
        return None
    try:
        lag_ms = int(value)
    except (TypeError, ValueError):
        return None
    return timestamp - timedelta(milliseconds=max(0, lag_ms))


def _elapsed_ms(first: datetime, second: datetime) -> int:
    return round((second - first).total_seconds() * 1000)


def _parse_time(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    return datetime.fromisoformat(text)


def _format_time(value: datetime | None) -> str | None:
    if value is None:
        return None
    text = value.isoformat(timespec="milliseconds")
    return text.replace("+00:00", "Z")


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


if __name__ == "__main__":
    main()
