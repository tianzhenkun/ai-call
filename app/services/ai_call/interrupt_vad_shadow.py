from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

OFFLINE_ASR_CLASSIFICATION = "offline_asr_speech"
REALTIME_OR_PROVIDER_CLASSIFICATION = "realtime_speech_or_provider"
P1_CANDIDATE_CLASSIFICATION = "p1_candidate_without_transcript"
UNEXPLAINED_CLASSIFICATION = "unexplained_by_events"


def build_vad_shadow_report(
    *,
    call_id: str,
    record: dict[str, Any] | None,
    recording: dict[str, Any] | None,
    events: list[dict[str, Any]],
    dialogue_segments: list[dict[str, Any]],
    vad_windows: list[dict[str, Any]],
    max_detection_lag_ms: int = 500,
    evidence_tolerance_ms: int = 600,
) -> dict[str, Any]:
    """Align offline VAD windows with existing P1/Qwen/ASR evidence.

    This report is intentionally detector-agnostic. The caller provides VAD
    windows in customer-track milliseconds, and this function answers whether
    those windows map to evidence already stored by the call pipeline.
    """

    customer_track = _customer_track(recording or {})
    track_started_at = _parse_optional_time(customer_track.get("startedAt"))
    normalized_windows = _normalize_vad_windows(
        vad_windows=vad_windows,
        track_started_at=track_started_at,
    )
    normalized_segments = [
        _normalize_dialogue_segment(index, row)
        for index, row in enumerate(dialogue_segments)
    ]
    offline_segments = [
        segment
        for segment in normalized_segments
        if segment["speakerType"] == "customer"
        and segment["source"] == "offline_asr"
        and segment["startedAt"] is not None
        and segment["endedAt"] is not None
    ]
    realtime_segments = [
        segment
        for segment in normalized_segments
        if segment["speakerType"] == "customer"
        and segment["source"] != "offline_asr"
        and segment["startedAt"] is not None
        and segment["endedAt"] is not None
    ]
    normalized_events = [
        _normalize_event(index, row) for index, row in enumerate(events)
    ]
    realtime_shadow_windows = _realtime_shadow_windows(
        events=normalized_events,
        track_started_at=track_started_at,
    )

    classified_windows = [
        _classify_window(
            window=window,
            offline_segments=offline_segments,
            realtime_segments=realtime_segments,
            events=normalized_events,
            evidence_tolerance_ms=evidence_tolerance_ms,
        )
        for window in normalized_windows
    ]
    classification_counts = Counter(
        window["classification"] for window in classified_windows
    )

    return {
        "mode": "vad_shadow",
        "callId": call_id,
        "record": _record_summary(record or {}),
        "track": {
            "startedAt": customer_track.get("startedAt"),
            "durationMs": customer_track.get("durationMs"),
            "playUrl": customer_track.get("playUrl"),
            "objectName": customer_track.get("objectName"),
        },
        "summary": {
            "vadWindows": len(classified_windows),
            "classifications": _ordered_nonzero_counts(classification_counts),
            "unexplainedWindows": classification_counts[UNEXPLAINED_CLASSIFICATION],
        },
        "offlineSpeech": _offline_speech_report(
            offline_segments=offline_segments,
            vad_windows=normalized_windows,
            max_detection_lag_ms=max_detection_lag_ms,
        ),
        "realtimeShadowSpeech": _offline_speech_report(
            offline_segments=offline_segments,
            vad_windows=realtime_shadow_windows,
            max_detection_lag_ms=max_detection_lag_ms,
        ),
        "realtimeShadowSpeechByDetector": _realtime_shadow_speech_by_detector(
            offline_segments=offline_segments,
            realtime_shadow_windows=realtime_shadow_windows,
            max_detection_lag_ms=max_detection_lag_ms,
        ),
        "deferredPreStops": _deferred_pre_stop_report(
            offline_segments=offline_segments,
            events=normalized_events,
            realtime_shadow_windows=realtime_shadow_windows,
            max_pre_stop_latency_ms=max_detection_lag_ms,
            evidence_tolerance_ms=evidence_tolerance_ms,
        ),
        "vadWindows": [_public_window(window) for window in classified_windows],
        "realtimeShadowWindows": [
            _public_window(window) for window in realtime_shadow_windows
        ],
    }


def _realtime_shadow_speech_by_detector(
    *,
    offline_segments: list[dict[str, Any]],
    realtime_shadow_windows: list[dict[str, Any]],
    max_detection_lag_ms: int,
) -> dict[str, Any]:
    detectors = sorted(
        {
            str(window.get("detector") or "unknown")
            for window in realtime_shadow_windows
        }
    )
    return {
        detector: _offline_speech_report(
            offline_segments=offline_segments,
            vad_windows=[
                window
                for window in realtime_shadow_windows
                if str(window.get("detector") or "unknown") == detector
            ],
            max_detection_lag_ms=max_detection_lag_ms,
        )
        for detector in detectors
    }


def _classify_window(
    *,
    window: dict[str, Any],
    offline_segments: list[dict[str, Any]],
    realtime_segments: list[dict[str, Any]],
    events: list[dict[str, Any]],
    evidence_tolerance_ms: int,
) -> dict[str, Any]:
    offline_matches = [
        segment for segment in offline_segments if _window_overlaps_segment(window, segment)
    ]
    if offline_matches:
        return {
            **window,
            "classification": OFFLINE_ASR_CLASSIFICATION,
            "evidence": {
                "offlineAsr": [
                    _segment_evidence(segment) for segment in offline_matches
                ],
            },
        }

    realtime_matches = [
        segment for segment in realtime_segments if _window_overlaps_segment(window, segment)
    ]
    provider_matches = _events_near_window(
        events,
        window=window,
        event_types={"user_speech_started"},
        tolerance_ms=evidence_tolerance_ms,
    )
    if realtime_matches or provider_matches:
        return {
            **window,
            "classification": REALTIME_OR_PROVIDER_CLASSIFICATION,
            "evidence": {
                "realtimeSegments": [
                    _segment_evidence(segment) for segment in realtime_matches
                ],
                "providerSpeechStarted": [
                    _event_evidence(event) for event in provider_matches
                ],
            },
        }

    candidate_matches = _events_near_window(
        events,
        window=window,
        event_types={"sip_interrupt_candidate"},
        tolerance_ms=evidence_tolerance_ms,
    )
    if candidate_matches:
        return {
            **window,
            "classification": P1_CANDIDATE_CLASSIFICATION,
            "evidence": {
                "p1Candidates": [
                    _event_evidence(event) for event in candidate_matches
                ],
            },
        }

    return {
        **window,
        "classification": UNEXPLAINED_CLASSIFICATION,
        "evidence": {},
    }


def _offline_speech_report(
    *,
    offline_segments: list[dict[str, Any]],
    vad_windows: list[dict[str, Any]],
    max_detection_lag_ms: int,
) -> dict[str, Any]:
    detected = 0
    within_max_lag = 0
    missed_segments: list[dict[str, Any]] = []
    slow_segments: list[dict[str, Any]] = []
    lags: list[int] = []

    for segment in offline_segments:
        matching_window = _first_overlapping_window(vad_windows, segment)
        if matching_window is None:
            missed_segments.append({
                "text": segment["text"],
                "startedAt": segment["startedAtRaw"],
                "endedAt": segment["endedAtRaw"],
                "reason": "missing_vad_window",
            })
            continue

        detected += 1
        lag_ms = _elapsed_between_times_ms(
            segment["startedAt"],
            matching_window["startedAt"],
        )
        if lag_ms is not None:
            lags.append(lag_ms)
            if lag_ms <= max_detection_lag_ms:
                within_max_lag += 1
            else:
                slow_segments.append({
                    "text": segment["text"],
                    "startedAt": segment["startedAtRaw"],
                    "endedAt": segment["endedAtRaw"],
                    "vadStartTime": matching_window["startedAtRaw"],
                    "vadStartLagMs": lag_ms,
                    "maxDetectionLagMs": max_detection_lag_ms,
                    "reason": "vad_start_too_late",
                })

    return {
        "segments": len(offline_segments),
        "detected": detected,
        "missed": len(missed_segments),
        "slow": len(slow_segments),
        "withinMaxLag": within_max_lag,
        "maxDetectionLagMs": max_detection_lag_ms,
        "startLagMs": _lag_summary(lags),
        "missedSegments": missed_segments,
        "slowSegments": slow_segments,
    }


def _deferred_pre_stop_report(
    *,
    offline_segments: list[dict[str, Any]],
    events: list[dict[str, Any]],
    realtime_shadow_windows: list[dict[str, Any]],
    max_pre_stop_latency_ms: int,
    evidence_tolerance_ms: int,
) -> dict[str, Any]:
    deferred_events = [
        event
        for event in events
        if event["eventType"] in {"sip_pre_stop_deferred", "sip_ai_playback_echo_deferred"}
    ]
    deferred_during_speech: list[dict[str, Any]] = []
    blocked_segments: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()

    for event in deferred_events:
        reason = str(event["payload"].get("reason") or "unknown")
        reason_counts[reason] += 1
        if any(_event_overlaps_segment(event, segment, tolerance_ms=0) for segment in offline_segments):
            deferred_during_speech.append(event)

    for segment in offline_segments:
        segment_deferred_events = [
            event
            for event in deferred_events
            if _event_overlaps_segment(
                event,
                segment,
                tolerance_ms=evidence_tolerance_ms,
            )
        ]
        if not segment_deferred_events:
            continue
        fast_pre_stop = _first_pre_stop_for_segment(
            events=events,
            segment=segment,
            max_pre_stop_latency_ms=max_pre_stop_latency_ms,
        )
        if fast_pre_stop is not None:
            continue
        expired = any(
            _event_overlaps_segment(
                event,
                segment,
                tolerance_ms=evidence_tolerance_ms,
            )
            for event in events
            if event["eventType"] == "sip_interrupt_candidate_expired"
        )
        shadow_detectors = _shadow_detectors_for_segment(
            realtime_shadow_windows,
            segment,
        )
        first_deferred = min(
            segment_deferred_events,
            key=lambda event: event.get("timestamp") or datetime.max,
        )
        blocked_segments.append({
            "text": segment["text"],
            "startedAt": segment["startedAtRaw"],
            "endedAt": segment["endedAtRaw"],
            "firstDeferredTime": first_deferred["eventTime"],
            "firstDeferredLagMs": _elapsed_between_times_ms(
                segment["startedAt"],
                first_deferred["timestamp"],
            ),
            "preStopTime": None,
            "expired": expired,
            "deferredReasons": _ordered_unique(
                str(event["payload"].get("reason") or "unknown")
                for event in segment_deferred_events
            ),
            "realtimeShadowDetectors": shadow_detectors,
        })

    return {
        "segments": len(offline_segments),
        "blockedSegments": len(blocked_segments),
        "deferredEvents": len(deferred_events),
        "deferredDuringOfflineSpeech": len(deferred_during_speech),
        "reasons": _ordered_nonzero_counts(reason_counts),
        "blockedSpeechSegments": blocked_segments,
    }


def _normalize_vad_windows(
    *,
    vad_windows: list[dict[str, Any]],
    track_started_at: datetime | None,
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for index, window in enumerate(vad_windows):
        start_ms = _first_present(window.get("startMs"), window.get("start_ms"))
        end_ms = _first_present(window.get("endMs"), window.get("end_ms"))
        if start_ms is None or end_ms is None:
            continue
        start_ms_int = int(start_ms)
        end_ms_int = int(end_ms)
        started_at = (
            track_started_at + timedelta(milliseconds=start_ms_int)
            if track_started_at is not None
            else None
        )
        ended_at = (
            track_started_at + timedelta(milliseconds=end_ms_int)
            if track_started_at is not None
            else None
        )
        normalized.append({
            "index": index,
            "startMs": start_ms_int,
            "endMs": end_ms_int,
            "durationMs": max(0, end_ms_int - start_ms_int),
            "startedAt": started_at,
            "endedAt": ended_at,
            "startedAtRaw": _format_time(started_at),
            "endedAtRaw": _format_time(ended_at),
        })
    return normalized


def _realtime_shadow_windows(
    *,
    events: list[dict[str, Any]],
    track_started_at: datetime | None,
) -> list[dict[str, Any]]:
    windows: list[dict[str, Any]] = []
    active_by_detector: dict[str, dict[str, Any]] = {}
    ordered_events = sorted(
        events,
        key=lambda event: (event["timestamp"] or datetime.max, event["index"]),
    )
    for event in ordered_events:
        event_type = event["eventType"]
        if event_type not in {"sip_vad_shadow_started", "sip_vad_shadow_ended"}:
            continue
        timestamp = event["timestamp"]
        if timestamp is None:
            continue
        payload = event["payload"]
        detector = str(payload.get("detector") or "unknown")
        if event_type == "sip_vad_shadow_started":
            active_by_detector[detector] = event
            continue
        started_event = active_by_detector.pop(detector, None)
        if started_event is None or started_event["timestamp"] is None:
            continue
        windows.append(
            _shadow_window(
                index=len(windows),
                started_event=started_event,
                ended_event=event,
                track_started_at=track_started_at,
                detector=detector,
            )
        )
    return windows


def _shadow_window(
    *,
    index: int,
    started_event: dict[str, Any],
    ended_event: dict[str, Any],
    track_started_at: datetime | None,
    detector: str,
) -> dict[str, Any]:
    started_payload = started_event["payload"]
    started_event_at = started_event["timestamp"]
    started_at = (
        _time_minus_payload_lag(started_event_at, started_payload, "detectionLagMs")
        or started_event_at
    )
    ended_at = (
        _time_minus_payload_lag(started_event_at, started_payload, "speechEndLagMs")
        or ended_event["timestamp"]
    )
    start_ms = _elapsed_between_times_ms(track_started_at, started_at)
    end_ms = _elapsed_between_times_ms(track_started_at, ended_at)
    duration_ms = _elapsed_between_times_ms(started_at, ended_at)
    window = {
        "index": index,
        "startMs": start_ms,
        "endMs": end_ms,
        "durationMs": max(0, duration_ms or 0),
        "startedAt": started_at,
        "endedAt": ended_at,
        "startedAtRaw": _format_time(started_at),
        "endedAtRaw": _format_time(ended_at),
        "detector": detector,
        "confidence": started_event["payload"].get("confidence"),
    }
    for output_key, payload_key in (
        ("detectionLagMs", "detectionLagMs"),
        ("speechEndLagMs", "speechEndLagMs"),
        ("bufferDurationMs", "bufferDurationMs"),
        ("windowStartMs", "windowStartMs"),
        ("windowEndMs", "windowEndMs"),
    ):
        value = _optional_int(started_payload.get(payload_key))
        if value is not None:
            window[output_key] = value
    return window


def _time_minus_payload_lag(
    timestamp: datetime | None,
    payload: dict[str, Any],
    key: str,
) -> datetime | None:
    lag_ms = _optional_int(payload.get(key))
    if timestamp is None or lag_ms is None:
        return None
    return timestamp - timedelta(milliseconds=max(0, lag_ms))


def _public_window(window: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in window.items()
        if key not in {"startedAt", "endedAt"}
    }


def _normalize_dialogue_segment(index: int, row: dict[str, Any]) -> dict[str, Any]:
    started_at_raw = _first_present(row.get("startedAt"), row.get("started_at"))
    ended_at_raw = _first_present(row.get("endedAt"), row.get("ended_at"))
    return {
        "index": index,
        "speakerType": str(
            _first_present(row.get("speakerType"), row.get("speaker_type"), "")
        ),
        "source": str(row.get("source") or ""),
        "text": str(_first_present(row.get("text"), row.get("segmentText"), row.get("segment_text"), "")),
        "startedAtRaw": started_at_raw,
        "endedAtRaw": ended_at_raw,
        "startedAt": _parse_optional_time(started_at_raw),
        "endedAt": _parse_optional_time(ended_at_raw),
    }


def _normalize_event(index: int, row: dict[str, Any]) -> dict[str, Any]:
    event_time_raw = _first_present(row.get("eventTime"), row.get("timestamp"))
    payload = row.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    return {
        "index": index,
        "eventType": str(_first_present(row.get("eventType"), row.get("type"), "")),
        "eventTime": event_time_raw,
        "timestamp": _parse_optional_time(event_time_raw),
        "payload": payload,
    }


def _customer_track(recording: dict[str, Any]) -> dict[str, Any]:
    tracks = recording.get("tracks")
    if not isinstance(tracks, list):
        return {}
    for track in tracks:
        if isinstance(track, dict) and track.get("trackRole") == "customer":
            return track
    return {}


def _record_summary(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "callId": record.get("callId"),
        "entryType": record.get("entryType"),
        "status": record.get("status"),
        "startedAt": record.get("startedAt"),
        "endedAt": record.get("endedAt"),
        "durationMs": record.get("durationMs"),
    }


def _first_overlapping_window(
    windows: list[dict[str, Any]],
    segment: dict[str, Any],
) -> dict[str, Any] | None:
    matches = [
        window for window in windows if _window_overlaps_segment(window, segment)
    ]
    if not matches:
        return None
    return min(matches, key=lambda window: window.get("startedAt") or datetime.max)


def _window_overlaps_segment(
    window: dict[str, Any],
    segment: dict[str, Any],
) -> bool:
    window_start = window.get("startedAt")
    window_end = window.get("endedAt")
    segment_start = segment.get("startedAt")
    segment_end = segment.get("endedAt")
    if (
        window_start is None
        or window_end is None
        or segment_start is None
        or segment_end is None
    ):
        return False
    return window_start <= segment_end and segment_start <= window_end


def _events_near_window(
    events: list[dict[str, Any]],
    *,
    window: dict[str, Any],
    event_types: set[str],
    tolerance_ms: int,
) -> list[dict[str, Any]]:
    window_start = window.get("startedAt")
    window_end = window.get("endedAt")
    if window_start is None or window_end is None:
        return []
    tolerance = timedelta(milliseconds=tolerance_ms)
    return [
        event
        for event in events
        if event["eventType"] in event_types
        and event["timestamp"] is not None
        and window_start - tolerance <= event["timestamp"] <= window_end + tolerance
    ]


def _event_overlaps_segment(
    event: dict[str, Any],
    segment: dict[str, Any],
    *,
    tolerance_ms: int,
) -> bool:
    event_time = event.get("timestamp")
    segment_start = segment.get("startedAt")
    segment_end = segment.get("endedAt")
    if event_time is None or segment_start is None or segment_end is None:
        return False
    tolerance = timedelta(milliseconds=tolerance_ms)
    return segment_start - tolerance <= event_time <= segment_end + tolerance


def _first_pre_stop_for_segment(
    *,
    events: list[dict[str, Any]],
    segment: dict[str, Any],
    max_pre_stop_latency_ms: int,
) -> dict[str, Any] | None:
    segment_start = segment.get("startedAt")
    if segment_start is None:
        return None
    deadline = segment_start + timedelta(milliseconds=max_pre_stop_latency_ms)
    matches = [
        event
        for event in events
        if event["eventType"] == "sip_pre_stop"
        and event["timestamp"] is not None
        and segment_start <= event["timestamp"] <= deadline
    ]
    if not matches:
        return None
    return min(matches, key=lambda event: event["timestamp"])


def _shadow_detectors_for_segment(
    realtime_shadow_windows: list[dict[str, Any]],
    segment: dict[str, Any],
) -> list[str]:
    detectors: list[str] = []
    for window in realtime_shadow_windows:
        if not _window_overlaps_segment(window, segment):
            continue
        detector = str(window.get("detector") or "unknown")
        if detector not in detectors:
            detectors.append(detector)
    return detectors


def _segment_evidence(segment: dict[str, Any]) -> dict[str, Any]:
    return {
        "text": segment["text"],
        "source": segment["source"],
        "startedAt": segment["startedAtRaw"],
        "endedAt": segment["endedAtRaw"],
    }


def _event_evidence(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "eventType": event["eventType"],
        "eventTime": event["eventTime"],
        "snrDb": event["payload"].get("snrDb"),
        "rmsDbfs": event["payload"].get("rmsDbfs"),
        "candidateDurationMs": event["payload"].get("candidateDurationMs"),
    }


def _ordered_nonzero_counts(counter: Counter[str]) -> dict[str, int]:
    labels = [
        OFFLINE_ASR_CLASSIFICATION,
        REALTIME_OR_PROVIDER_CLASSIFICATION,
        P1_CANDIDATE_CLASSIFICATION,
        UNEXPLAINED_CLASSIFICATION,
    ]
    ordered = {label: counter[label] for label in labels if counter[label]}
    for label, count in counter.items():
        if count and label not in ordered:
            ordered[label] = count
    return ordered


def _ordered_unique(values: Any) -> list[str]:
    output: list[str] = []
    for value in values:
        if value not in output:
            output.append(value)
    return output


def _lag_summary(lags: list[int]) -> dict[str, int | None]:
    if not lags:
        return {"avg": None, "min": None, "max": None}
    return {
        "avg": round(sum(lags) / len(lags)),
        "min": min(lags),
        "max": max(lags),
    }


def _elapsed_between_times_ms(
    start: datetime | None,
    end: datetime | None,
) -> int | None:
    if start is None or end is None:
        return None
    return round((end - start).total_seconds() * 1000)


def _parse_optional_time(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value)
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _format_time(value: datetime | None) -> str | None:
    if value is None:
        return None
    return f"{value.isoformat(timespec='milliseconds')}Z"


def _optional_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None
