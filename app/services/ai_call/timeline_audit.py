from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

EXPECTED_STALE_AUDIO_DROP_REASONS = frozenset({
    "awaiting_response_start_after_interrupt",
    "cancelled_response",
    "interrupt_pending",
    "non_current_response",
    "stale_generation",
    "suppressed_after_interrupt",
    "user_speech_active",
})
MAX_CUSTOMER_STOP_TO_FIRST_AUDIO_MS = 5_000


@dataclass(frozen=True)
class NormalizedEvent:
    index: int
    event_type: str
    event_time: str | None
    timestamp: datetime | None
    payload: dict[str, Any]
    source: str | None


@dataclass(frozen=True)
class SpeechInterval:
    start: NormalizedEvent
    stop: NormalizedEvent | None


def build_timeline_audit(
    *,
    call_id: str,
    record: dict[str, Any] | None,
    events: list[dict[str, Any]],
    dialogue_segments: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    normalized = [_normalize_event(index, event) for index, event in enumerate(events)]
    normalized.sort(key=_event_sort_key)
    speech_intervals = _customer_speech_intervals(normalized)
    model_starts = [
        event for event in normalized if event.event_type == "model_response_started"
    ]
    stale_audio_drops = [
        event for event in normalized if event.event_type == "stale_audio_dropped"
    ]

    issues: list[dict[str, Any]] = []
    covered_overlaps: list[dict[str, Any]] = []
    for model_start in model_starts:
        interval = _active_interval_at(speech_intervals, model_start)
        if interval is None:
            continue
        covered = _covered_by_defer_or_cancel(
            model_start,
            events=normalized,
            speech_interval=interval,
        )
        overlap = _overlap_window(
            call_id=call_id,
            model_start=model_start,
            speech_interval=interval,
        )
        if covered:
            overlap["coveredBy"] = covered
            covered_overlaps.append(overlap)
        else:
            overlap["type"] = "ai_started_during_customer_speech"
            overlap["severity"] = "high"
            issues.append(overlap)

    issues.extend(_unexpected_stale_audio_issues(call_id, stale_audio_drops))
    slow_first_audio_issues = _slow_first_audio_issues(call_id, normalized)
    issues.extend(slow_first_audio_issues)

    high_severity_count = sum(1 for issue in issues if issue.get("severity") == "high")
    summary = {
        "issueCount": len(issues),
        "highSeverityCount": high_severity_count,
        "coveredOverlapCount": len(covered_overlaps),
        "speechIntervalCount": len(speech_intervals),
        "modelResponseCount": len(model_starts),
        "staleAudioDropCount": len(stale_audio_drops),
        "slowFirstAudioCount": len(slow_first_audio_issues),
        "dialogueSegmentCount": len(dialogue_segments or []),
    }
    return {
        "callId": call_id,
        "passed": high_severity_count == 0,
        "record": _record_summary(record or {}),
        "summary": summary,
        "issues": issues,
        "coveredOverlaps": covered_overlaps,
    }


def build_timeline_audit_suite(
    reports: list[dict[str, Any]],
    failed_calls: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    failed_calls = failed_calls or []
    summary = {
        "calls": len(reports),
        "passed": sum(1 for report in reports if report.get("passed") is True),
        "failed": sum(1 for report in reports if report.get("passed") is False),
        "issues": 0,
        "highSeverity": 0,
        "coveredOverlaps": 0,
        "failedCalls": len(failed_calls),
    }
    for report in reports:
        report_summary = report.get("summary") or {}
        summary["issues"] += int(report_summary.get("issueCount") or 0)
        summary["highSeverity"] += int(report_summary.get("highSeverityCount") or 0)
        summary["coveredOverlaps"] += int(report_summary.get("coveredOverlapCount") or 0)
    return {
        "mode": "suite",
        "summary": summary,
        "calls": reports,
        "failedCalls": failed_calls,
    }


def _customer_speech_intervals(events: list[NormalizedEvent]) -> list[SpeechInterval]:
    intervals: list[SpeechInterval] = []
    active_start: NormalizedEvent | None = None
    for event in events:
        if event.event_type == "user_speech_started":
            if active_start is not None:
                intervals.append(SpeechInterval(start=active_start, stop=None))
            active_start = event
        elif event.event_type == "user_speech_stopped" and active_start is not None:
            intervals.append(SpeechInterval(start=active_start, stop=event))
            active_start = None
    if active_start is not None:
        intervals.append(SpeechInterval(start=active_start, stop=None))
    return intervals


def _active_interval_at(
    intervals: list[SpeechInterval],
    event: NormalizedEvent,
) -> SpeechInterval | None:
    if event.timestamp is None:
        return None
    for interval in intervals:
        if interval.start.timestamp is None:
            continue
        if event.timestamp < interval.start.timestamp:
            continue
        if interval.stop is not None and interval.stop.timestamp is not None:
            if event.timestamp >= interval.stop.timestamp:
                continue
        return interval
    return None


def _covered_by_defer_or_cancel(
    model_start: NormalizedEvent,
    *,
    events: list[NormalizedEvent],
    speech_interval: SpeechInterval,
) -> list[dict[str, Any]]:
    response_id = _response_id(model_start)
    evidence: list[dict[str, Any]] = []
    for event in events:
        if event.timestamp is None or model_start.timestamp is None:
            continue
        if event.timestamp < model_start.timestamp:
            continue
        if speech_interval.stop is not None and speech_interval.stop.timestamp is not None:
            if event.timestamp > speech_interval.stop.timestamp:
                continue
        if response_id and _response_id(event) not in {None, response_id}:
            continue
        if event.event_type == "no_barge_unstarted_response_deferred":
            evidence.append({
                "eventType": event.event_type,
                "eventTime": event.event_time,
                "reason": _payload_text(event.payload, "reason"),
            })
        elif event.event_type == "stale_audio_dropped" and (
            _payload_text(event.payload, "reason") == "cancelled_response"
        ):
            evidence.append({
                "eventType": event.event_type,
                "eventTime": event.event_time,
                "reason": _payload_text(event.payload, "reason"),
            })
    return evidence


def _overlap_window(
    *,
    call_id: str,
    model_start: NormalizedEvent,
    speech_interval: SpeechInterval,
) -> dict[str, Any]:
    return {
        "callId": call_id,
        "eventTime": model_start.event_time,
        "responseId": _response_id(model_start),
        "speechStartTime": speech_interval.start.event_time,
        "speechStopTime": speech_interval.stop.event_time if speech_interval.stop else None,
        "deltaMsFromSpeechStart": _elapsed_ms(speech_interval.start, model_start),
    }


def _unexpected_stale_audio_issues(
    call_id: str,
    stale_audio_drops: list[NormalizedEvent],
) -> list[dict[str, Any]]:
    groups: dict[tuple[str | None, str | None], dict[str, Any]] = {}
    for stale_audio_drop in stale_audio_drops:
        reason = _payload_text(stale_audio_drop.payload, "reason")
        if reason in EXPECTED_STALE_AUDIO_DROP_REASONS:
            continue
        response_id = _response_id(stale_audio_drop)
        key = (reason, response_id)
        issue = groups.get(key)
        if issue is None:
            issue = {
                "type": "unexpected_stale_audio_drop",
                "severity": "high" if reason == "session_not_ai_speaking" else "medium",
                "callId": call_id,
                "eventTime": stale_audio_drop.event_time,
                "firstEventTime": stale_audio_drop.event_time,
                "lastEventTime": stale_audio_drop.event_time,
                "responseId": response_id,
                "reason": reason,
                "dropCount": 0,
                "deltaBytes": 0,
            }
            groups[key] = issue
        issue["dropCount"] += 1
        issue["lastEventTime"] = stale_audio_drop.event_time
        issue["deltaBytes"] += _payload_int(stale_audio_drop.payload, "deltaBytes") or 0
    return list(groups.values())


def _slow_first_audio_issues(
    call_id: str,
    events: list[NormalizedEvent],
) -> list[dict[str, Any]]:
    stops = [event for event in events if event.event_type == "user_speech_stopped"]
    first_audio_events = [
        event for event in events if event.event_type == "browser_first_audio"
    ]
    user_start_events = [
        event for event in events if event.event_type == "user_speech_started"
    ]
    issues: list[dict[str, Any]] = []
    for stop in stops:
        next_audio = _first_event_after(first_audio_events, stop)
        next_user_start = _first_event_after(user_start_events, stop)
        next_observable = _earlier_event(next_audio, next_user_start)
        elapsed_to_observable_ms = (
            _elapsed_ms(stop, next_observable) if next_observable is not None else None
        )
        if (
            next_audio is None
            or elapsed_to_observable_ms is None
            or elapsed_to_observable_ms <= MAX_CUSTOMER_STOP_TO_FIRST_AUDIO_MS
        ):
            continue
        ignored_tool = _first_between(events, "call_end_tool_ignored", stop, next_audio)
        reason = (
            "call_end_tool_ignored_before_next_audio"
            if ignored_tool is not None
            else "slow_response_audio_after_customer_turn"
        )
        elapsed_to_audio_ms = _elapsed_ms(stop, next_audio)
        issues.append({
            "type": "slow_ai_first_audio_after_customer_turn",
            "severity": "high",
            "callId": call_id,
            "eventTime": stop.event_time,
            "customerStopTime": stop.event_time,
            "firstAudioTime": next_audio.event_time,
            "nextCustomerSpeechTime": next_user_start.event_time if next_user_start else None,
            "customerStopToFirstAudioMs": elapsed_to_audio_ms,
            "customerStopToNextActivityMs": elapsed_to_observable_ms,
            "reason": reason,
        })
    return issues


def _earlier_event(
    first: NormalizedEvent | None,
    second: NormalizedEvent | None,
) -> NormalizedEvent | None:
    if first is None:
        return second
    if second is None:
        return first
    if first.timestamp is None:
        return second
    if second.timestamp is None:
        return first
    return first if first.timestamp <= second.timestamp else second


def _first_event_after(
    events: list[NormalizedEvent],
    anchor: NormalizedEvent,
) -> NormalizedEvent | None:
    if anchor.timestamp is None:
        return None
    for event in events:
        if event.timestamp is not None and event.timestamp > anchor.timestamp:
            return event
    return None


def _first_between(
    events: list[NormalizedEvent],
    event_type: str,
    start: NormalizedEvent,
    end: NormalizedEvent,
) -> NormalizedEvent | None:
    if start.timestamp is None or end.timestamp is None:
        return None
    for event in events:
        if (
            event.event_type == event_type
            and event.timestamp is not None
            and start.timestamp < event.timestamp < end.timestamp
        ):
            return event
    return None


def _normalize_event(index: int, event: dict[str, Any]) -> NormalizedEvent:
    event_type = str(event.get("eventType") or event.get("type") or event.get("event_type") or "")
    event_time = event.get("eventTime") or event.get("timestamp") or event.get("event_time")
    payload = _payload(event)
    return NormalizedEvent(
        index=index,
        event_type=event_type,
        event_time=event_time if isinstance(event_time, str) else None,
        timestamp=_parse_optional_time(event_time),
        payload=payload,
        source=_optional_text(event.get("source")),
    )


def _event_sort_key(event: NormalizedEvent) -> tuple[int, float, int]:
    if event.timestamp is None:
        return (1, 0.0, event.index)
    return (0, event.timestamp.timestamp(), event.index)


def _payload(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload")
    if isinstance(payload, dict):
        return payload
    payload_json = event.get("payload_json") or event.get("payloadJson")
    if isinstance(payload_json, dict):
        return payload_json
    if isinstance(payload_json, str) and payload_json:
        try:
            decoded = json.loads(payload_json)
        except json.JSONDecodeError:
            return {}
        if isinstance(decoded, dict):
            return decoded
    return {}


def _response_id(event: NormalizedEvent) -> str | None:
    payload = event.payload
    direct = (
        payload.get("responseId")
        or payload.get("response_id")
        or payload.get("currentResponseId")
    )
    if isinstance(direct, str) and direct:
        return direct
    response = payload.get("response")
    if isinstance(response, dict):
        response_id = response.get("id") or response.get("responseId")
        if isinstance(response_id, str) and response_id:
            return response_id
    return None


def _record_summary(record: dict[str, Any]) -> dict[str, Any]:
    return {
        key: record.get(key)
        for key in (
            "callId",
            "entryType",
            "sceneCode",
            "promptSourceKey",
            "status",
            "endReason",
            "startedAt",
            "endedAt",
            "durationMs",
        )
        if record.get(key) is not None
    }


def _payload_text(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if isinstance(value, str) and value:
        return value
    return None


def _payload_int(payload: dict[str, Any], key: str) -> int | None:
    value = payload.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(value)
    if isinstance(value, str) and value.isdecimal():
        return int(value)
    return None


def _elapsed_ms(first: NormalizedEvent, second: NormalizedEvent) -> int | None:
    if first.timestamp is None or second.timestamp is None:
        return None
    return round((second.timestamp - first.timestamp).total_seconds() * 1000)


def _optional_text(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


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
