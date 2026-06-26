from __future__ import annotations

import math
import sys
from array import array
from datetime import datetime
from typing import Any

MIN_DBFS = -45.0
MIN_SNR_DB = 6.0
MIN_SEGMENT_MS = 120
WINDOW_MS = 100
EVENT_ALIGNMENT_WINDOW_MS = 800

CANDIDATE_EVENT_TYPES = {"interrupt_candidate", "sip_interrupt_candidate"}
CONFIRMED_EVENT_TYPES = {"interrupt_confirmed", "sip_interrupt_candidate_confirmed"}
PROVIDER_SPEECH_EVENT_TYPES = {"user_speech_started"}


def build_offline_interrupt_report(
    *,
    call_id: str,
    record: dict[str, Any],
    events: list[dict[str, Any]],
    recording: dict[str, Any] | None,
    pcm_by_role: dict[str, bytes],
    sample_rate: int = 16_000,
    window_ms: int = WINDOW_MS,
    min_rms_dbfs: float = MIN_DBFS,
    min_snr_db: float = MIN_SNR_DB,
    min_segment_ms: int = MIN_SEGMENT_MS,
    event_alignment_window_ms: int = EVENT_ALIGNMENT_WINDOW_MS,
) -> dict[str, Any]:
    recording = recording or {}
    record_started_at = _parse_optional_time(record.get("startedAt"))
    tracks = _tracks_by_role(recording)

    customer_segments = _segments_for_role(
        pcm_by_role.get("customer", b""),
        sample_rate=sample_rate,
        window_ms=window_ms,
        min_rms_dbfs=min_rms_dbfs,
        min_snr_db=min_snr_db,
        min_segment_ms=min_segment_ms,
        track_offset_ms=_track_offset_ms(tracks.get("customer"), record_started_at),
    )
    ai_segments = _segments_for_role(
        pcm_by_role.get("ai", b""),
        sample_rate=sample_rate,
        window_ms=window_ms,
        min_rms_dbfs=min_rms_dbfs,
        min_snr_db=min_snr_db,
        min_segment_ms=min_segment_ms,
        track_offset_ms=_track_offset_ms(tracks.get("ai"), record_started_at),
    )
    event_points = _event_points(events, record_started_at)

    return {
        "callId": call_id,
        "record": _record_summary(record),
        "eventSummary": _event_summary(events),
        "recordings": _recording_summary(recording),
        "thresholds": {
            "sampleRate": sample_rate,
            "windowMs": window_ms,
            "minRmsDbfs": min_rms_dbfs,
            "minSnrDb": min_snr_db,
            "minSegmentMs": min_segment_ms,
            "eventAlignmentWindowMs": event_alignment_window_ms,
        },
        "audioAnalysis": {
            "customerSegments": customer_segments,
            "aiActiveSegments": ai_segments,
        },
        "possibleInterruptWindows": _overlap_windows(
            customer_segments,
            ai_segments,
            event_points=event_points,
            event_alignment_window_ms=event_alignment_window_ms,
        ),
    }


def _segments_for_role(
    pcm: bytes,
    *,
    sample_rate: int,
    window_ms: int,
    min_rms_dbfs: float,
    min_snr_db: float,
    min_segment_ms: int,
    track_offset_ms: int,
) -> list[dict[str, Any]]:
    windows = _rms_windows(pcm, sample_rate=sample_rate, window_ms=window_ms)
    if not windows:
        return []
    noise_floor = _percentile([window["rmsDbfs"] for window in windows], 0.2)
    active_windows = [
        window
        for window in windows
        if window["rmsDbfs"] >= min_rms_dbfs
        and window["rmsDbfs"] - noise_floor >= min_snr_db
    ]
    return _merge_windows(
        active_windows,
        min_segment_ms=min_segment_ms,
        track_offset_ms=track_offset_ms,
    )


def _rms_windows(
    pcm: bytes,
    *,
    sample_rate: int,
    window_ms: int,
) -> list[dict[str, Any]]:
    if not pcm:
        return []
    samples = array("h")
    samples.frombytes(pcm[: len(pcm) - (len(pcm) % 2)])
    if sys.byteorder != "little":
        samples.byteswap()
    window_samples = max(1, sample_rate * window_ms // 1000)
    windows: list[dict[str, Any]] = []
    for start_sample in range(0, len(samples), window_samples):
        chunk = samples[start_sample : start_sample + window_samples]
        if not chunk:
            continue
        rms = math.sqrt(sum(sample * sample for sample in chunk) / len(chunk))
        rms_dbfs = _rms_to_dbfs(rms)
        start_ms = round(start_sample * 1000 / sample_rate)
        end_ms = round((start_sample + len(chunk)) * 1000 / sample_rate)
        windows.append({
            "startMs": start_ms,
            "endMs": end_ms,
            "rmsDbfs": rms_dbfs,
        })
    return windows


def _merge_windows(
    windows: list[dict[str, Any]],
    *,
    min_segment_ms: int,
    track_offset_ms: int,
) -> list[dict[str, Any]]:
    if not windows:
        return []
    segments: list[dict[str, Any]] = []
    current_start = windows[0]["startMs"]
    current_end = windows[0]["endMs"]
    peak = windows[0]["rmsDbfs"]
    for window in windows[1:]:
        if window["startMs"] <= current_end:
            current_end = max(current_end, window["endMs"])
            peak = max(peak, window["rmsDbfs"])
            continue
        _append_segment(
            segments,
            start_ms=current_start,
            end_ms=current_end,
            peak_rms_dbfs=peak,
            min_segment_ms=min_segment_ms,
            track_offset_ms=track_offset_ms,
        )
        current_start = window["startMs"]
        current_end = window["endMs"]
        peak = window["rmsDbfs"]
    _append_segment(
        segments,
        start_ms=current_start,
        end_ms=current_end,
        peak_rms_dbfs=peak,
        min_segment_ms=min_segment_ms,
        track_offset_ms=track_offset_ms,
    )
    return segments


def _append_segment(
    segments: list[dict[str, Any]],
    *,
    start_ms: int,
    end_ms: int,
    peak_rms_dbfs: float,
    min_segment_ms: int,
    track_offset_ms: int,
) -> None:
    duration_ms = end_ms - start_ms
    if duration_ms < min_segment_ms:
        return
    shifted_start = start_ms + track_offset_ms
    shifted_end = end_ms + track_offset_ms
    segments.append({
        "startMs": shifted_start,
        "endMs": shifted_end,
        "durationMs": shifted_end - shifted_start,
        "peakRmsDbfs": round(peak_rms_dbfs, 2),
    })


def _overlap_windows(
    customer_segments: list[dict[str, Any]],
    ai_segments: list[dict[str, Any]],
    *,
    event_points: list[dict[str, Any]],
    event_alignment_window_ms: int,
) -> list[dict[str, Any]]:
    windows: list[dict[str, Any]] = []
    for customer in customer_segments:
        for ai_segment in ai_segments:
            start_ms = max(customer["startMs"], ai_segment["startMs"])
            end_ms = min(customer["endMs"], ai_segment["endMs"])
            if end_ms <= start_ms:
                continue
            windows.append({
                "startMs": start_ms,
                "endMs": end_ms,
                "durationMs": end_ms - start_ms,
                "reason": "customer_audio_over_ai_audio",
                "eventAlignment": _align_events_to_window(
                    start_ms=start_ms,
                    end_ms=end_ms,
                    event_points=event_points,
                    event_alignment_window_ms=event_alignment_window_ms,
                ),
            })
    return windows


def _event_points(
    events: list[dict[str, Any]],
    record_started_at: datetime | None,
) -> list[dict[str, Any]]:
    if record_started_at is None:
        return []
    points: list[dict[str, Any]] = []
    for event in events:
        event_time = _parse_optional_time(event.get("eventTime") or event.get("timestamp"))
        if event_time is None:
            continue
        event_type = str(event.get("eventType") or event.get("type") or "")
        if not event_type:
            continue
        points.append({
            "eventType": event_type,
            "source": event.get("source"),
            "eventTime": _json_value(event.get("eventTime") or event.get("timestamp")),
            "offsetMs": round((event_time - record_started_at).total_seconds() * 1000),
            "payload": event.get("payload") if isinstance(event.get("payload"), dict) else {},
        })
    return points


def _align_events_to_window(
    *,
    start_ms: int,
    end_ms: int,
    event_points: list[dict[str, Any]],
    event_alignment_window_ms: int,
) -> dict[str, Any]:
    nearby = [
        _with_distance(event, start_ms=start_ms, end_ms=end_ms)
        for event in event_points
        if _distance_to_window(event["offsetMs"], start_ms=start_ms, end_ms=end_ms)
        <= event_alignment_window_ms
    ]
    candidate_events = [
        event for event in nearby if event["eventType"] in CANDIDATE_EVENT_TYPES
    ]
    provider_events = [
        event for event in nearby if event["eventType"] in PROVIDER_SPEECH_EVENT_TYPES
    ]
    confirmed_events = [
        event for event in nearby if event["eventType"] in CONFIRMED_EVENT_TYPES
    ]
    return {
        "alignmentWindowMs": event_alignment_window_ms,
        "verdict": _alignment_verdict(
            candidate_events=candidate_events,
            provider_events=provider_events,
            confirmed_events=confirmed_events,
        ),
        "candidateEvents": candidate_events,
        "providerSpeechStartedEvents": provider_events,
        "confirmedEvents": confirmed_events,
    }


def _alignment_verdict(
    *,
    candidate_events: list[dict[str, Any]],
    provider_events: list[dict[str, Any]],
    confirmed_events: list[dict[str, Any]],
) -> str:
    if confirmed_events:
        return "confirmed"
    if candidate_events:
        return "candidate_not_confirmed"
    if provider_events:
        return "missed_candidate"
    return "likely_noise"


def _with_distance(
    event: dict[str, Any],
    *,
    start_ms: int,
    end_ms: int,
) -> dict[str, Any]:
    result = dict(event)
    result["distanceMs"] = _distance_to_window(event["offsetMs"], start_ms=start_ms, end_ms=end_ms)
    return result


def _distance_to_window(offset_ms: int, *, start_ms: int, end_ms: int) -> int:
    if start_ms <= offset_ms <= end_ms:
        return 0
    if offset_ms < start_ms:
        return start_ms - offset_ms
    return offset_ms - end_ms


def _record_summary(record: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "callId",
        "status",
        "entryType",
        "endReason",
        "startedAt",
        "answeredAt",
        "endedAt",
        "durationMs",
    )
    return {key: _json_value(record.get(key)) for key in keys if key in record}


def _event_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    key_events: list[dict[str, Any]] = []
    counts = {
        "interruptCandidateCount": 0,
        "interruptConfirmedCount": 0,
        "sipHangupCount": 0,
        "providerSpeechStartedCount": 0,
    }
    key_types = {
        "interrupt_candidate",
        "sip_interrupt_candidate",
        "sip_interrupt_candidate_confirmed",
        "interrupt_confirmed",
        "user_speech_started",
        "sip_hangup",
        "session_completed",
        "handoff_requested",
        "handoff_connected",
        "handoff_completed",
    }
    for event in events:
        event_type = str(event.get("eventType") or event.get("type") or "")
        if event_type in {"interrupt_candidate", "sip_interrupt_candidate"}:
            counts["interruptCandidateCount"] += 1
        elif event_type in {"interrupt_confirmed", "sip_interrupt_candidate_confirmed"}:
            counts["interruptConfirmedCount"] += 1
        elif event_type == "sip_hangup":
            counts["sipHangupCount"] += 1
        elif event_type == "user_speech_started":
            counts["providerSpeechStartedCount"] += 1
        if event_type in key_types:
            key_events.append({
                "eventType": event_type,
                "source": event.get("source"),
                "eventTime": _json_value(event.get("eventTime") or event.get("timestamp")),
                "payload": event.get("payload") if isinstance(event.get("payload"), dict) else {},
            })
    counts["keyEvents"] = key_events
    return counts


def _recording_summary(recording: dict[str, Any]) -> dict[str, Any]:
    if not recording:
        return {"main": None, "tracks": []}
    return {
        "main": _media_summary("main", recording),
        "tracks": [
            _media_summary(str(track.get("trackRole") or ""), track)
            for track in recording.get("tracks", [])
            if isinstance(track, dict)
        ],
    }


def _media_summary(role: str, item: dict[str, Any]) -> dict[str, Any]:
    return {
        "role": role,
        "status": item.get("status"),
        "ossId": item.get("ossId"),
        "durationMs": item.get("durationMs"),
        "playUrl": item.get("playUrl"),
        "startedAt": _json_value(item.get("startedAt")),
        "endedAt": _json_value(item.get("endedAt")),
    }


def _tracks_by_role(recording: dict[str, Any]) -> dict[str, dict[str, Any]]:
    tracks: dict[str, dict[str, Any]] = {}
    for track in recording.get("tracks", []):
        if isinstance(track, dict) and track.get("trackRole"):
            tracks[str(track["trackRole"])] = track
    return tracks


def _track_offset_ms(track: dict[str, Any] | None, record_started_at: datetime | None) -> int:
    if track is None or record_started_at is None:
        return 0
    track_started_at = _parse_optional_time(track.get("startedAt"))
    if track_started_at is None:
        return 0
    return round((track_started_at - record_started_at).total_seconds() * 1000)


def _parse_optional_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _rms_to_dbfs(rms: float) -> float:
    if rms <= 0:
        return -120.0
    return 20 * math.log10(rms / 32768.0)


def _percentile(values: list[float], ratio: float) -> float:
    if not values:
        return -120.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * ratio)))
    return ordered[index]
