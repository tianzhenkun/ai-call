from __future__ import annotations

from datetime import datetime
from typing import Any

CONFIRMED_EVENT_TYPES = {
    "interrupt_confirmed",
    "sip_interrupt_candidate_confirmed",
    "sip_interrupt_confirmed",
}
PROVIDER_SPEECH_EVENT_TYPES = {"user_speech_started"}
REJECTED_EVENT_TYPES = {"sip_interrupt_rejected"}
DEFERRED_EVENT_TYPES = {"sip_pre_stop_deferred", "sip_ai_playback_echo_deferred"}
PROVIDER_CONFIRMED_CANDIDATE_WINDOW_MS = 1_200
OFFLINE_ASR_CANDIDATE_SKEW_TOLERANCE_MS = 1_500


def build_p1_evaluation(
    *,
    call_id: str,
    record: dict[str, Any] | None,
    events: list[dict[str, Any]],
    dialogue_segments: list[dict[str, Any]] | None = None,
    max_pre_stop_latency_ms: int = 500,
    max_end_completion_ms: int = 1_000,
    max_call_end_intent_schedule_ms: int = 1_000,
) -> dict[str, Any]:
    """Build an event-only P1 SIP barge-in evaluation report."""

    normalized = [_normalize_event(index, event) for index, event in enumerate(events)]
    normalized.sort(key=lambda event: (event["timestamp"] or datetime.min, event["index"]))

    candidates = [event for event in normalized if event["eventType"] == "sip_interrupt_candidate"]
    pre_stops = [event for event in normalized if event["eventType"] == "sip_pre_stop"]
    decisions = [
        event
        for event in normalized
        if event["eventType"] in CONFIRMED_EVENT_TYPES or event["eventType"] in REJECTED_EVENT_TYPES
    ]
    deferred_decisions = [
        event for event in normalized if event["eventType"] in DEFERRED_EVENT_TYPES
    ]
    provider_speech_events = [
        event for event in normalized if event["eventType"] in PROVIDER_SPEECH_EVENT_TYPES
    ]

    matched_candidate_indexes: set[int] = set()
    windows: list[dict[str, Any]] = []

    for pre_stop in pre_stops:
        candidate = _find_candidate_for_pre_stop(pre_stop, candidates, matched_candidate_indexes)
        if candidate is not None:
            matched_candidate_indexes.add(candidate["index"])
        decision = _find_decision_for_pre_stop(pre_stop, decisions)
        provider_speech = _has_provider_speech_near_pre_stop(
            pre_stop,
            candidate=candidate,
            provider_speech_events=provider_speech_events,
        )
        windows.append(
            _pre_stop_window(
                call_id=call_id,
                candidate=candidate,
                pre_stop=pre_stop,
                decision=decision,
                provider_speech=provider_speech,
            )
        )

    for candidate in candidates:
        if candidate["index"] in matched_candidate_indexes:
            continue
        windows.append(
            _candidate_without_pre_stop_window(
                call_id=call_id,
                candidate=candidate,
                decisions=decisions + deferred_decisions,
                provider_speech_events=provider_speech_events,
            )
        )

    windows.sort(key=lambda window: window.get("candidateTime") or window.get("preStopTime") or "")
    return {
        "callId": call_id,
        "record": _record_summary(record or {}),
        "summary": _summary(windows, provider_speech_events),
        "quality": _quality_report(
            normalized_events=normalized,
            dialogue_segments=dialogue_segments or [],
            max_pre_stop_latency_ms=max_pre_stop_latency_ms,
            max_end_completion_ms=max_end_completion_ms,
            max_call_end_intent_schedule_ms=max_call_end_intent_schedule_ms,
        ),
        "windows": windows,
    }


def build_p1_evaluation_suite(reports: list[dict[str, Any]], failed_calls: list[dict[str, str]] | None = None) -> dict[str, Any]:
    failed_calls = failed_calls or []
    summary = {
        "calls": len(reports),
        "windows": 0,
        "confirmedPreStops": 0,
        "confirmedWithoutPreStop": 0,
        "falsePreStops": 0,
        "candidateOnly": 0,
        "preStopPending": 0,
        "providerSpeechStarted": 0,
        "failedCalls": len(failed_calls),
    }
    for report in reports:
        report_summary = report.get("summary") or {}
        summary["windows"] += len(report.get("windows") or [])
        summary["confirmedPreStops"] += int(report_summary.get("confirmedPreStops") or 0)
        summary["confirmedWithoutPreStop"] += int(
            report_summary.get("confirmedWithoutPreStop") or 0
        )
        summary["falsePreStops"] += int(report_summary.get("falsePreStops") or 0)
        summary["candidateOnly"] += int(report_summary.get("candidateOnly") or 0)
        summary["preStopPending"] += int(report_summary.get("preStopPending") or 0)
        summary["providerSpeechStarted"] += int(report_summary.get("providerSpeechStarted") or 0)
    return {
        "mode": "suite",
        "summary": summary,
        "calls": reports,
        "failedCalls": failed_calls,
    }


def _pre_stop_window(
    *,
    call_id: str,
    candidate: dict[str, Any] | None,
    pre_stop: dict[str, Any],
    decision: dict[str, Any] | None,
    provider_speech: bool,
) -> dict[str, Any]:
    decision_type = decision["eventType"] if decision is not None else None
    if decision_type in CONFIRMED_EVENT_TYPES or provider_speech:
        outcome = "confirmed_pre_stop"
        severity = "pass"
    elif decision_type in REJECTED_EVENT_TYPES:
        outcome = "false_pre_stop_rejected"
        severity = "fail"
    else:
        outcome = "pre_stop_pending"
        severity = "warn"

    candidate_payload = candidate["payload"] if candidate is not None else {}
    pre_stop_payload = pre_stop["payload"]
    decision_payload = decision["payload"] if decision is not None else {}
    return {
        "callId": call_id,
        "outcome": outcome,
        "severity": severity,
        "responseId": pre_stop_payload.get("responseId") or candidate_payload.get("responseId"),
        "generation": pre_stop_payload.get("generation"),
        "candidateTime": candidate["eventTime"] if candidate is not None else None,
        "preStopTime": pre_stop["eventTime"],
        "decisionTime": decision["eventTime"] if decision is not None else None,
        "decisionEventType": decision_type,
        "decisionReason": decision_payload.get("reason"),
        "providerSpeechStarted": provider_speech,
        "candidateToPreStopMs": _elapsed_ms(candidate, pre_stop),
        "preStopToDecisionMs": (
            decision_payload.get("preStopToDecisionMs")
            if decision_payload.get("preStopToDecisionMs") is not None
            else _elapsed_ms(pre_stop, decision)
        ),
        "candidateDurationMs": _first_present(
            pre_stop_payload.get("candidateDurationMs"),
            candidate_payload.get("candidateDurationMs"),
        ),
        "wallClockSpeechMs": _first_present(
            pre_stop_payload.get("wallClockSpeechMs"),
            candidate_payload.get("wallClockSpeechMs"),
        ),
        "rmsDbfs": _first_present(pre_stop_payload.get("rmsDbfs"), candidate_payload.get("rmsDbfs")),
        "snrDb": _first_present(pre_stop_payload.get("snrDb"), candidate_payload.get("snrDb")),
        "peakDbfs": _first_present(pre_stop_payload.get("peakDbfs"), candidate_payload.get("peakDbfs")),
        "speechQualityRejection": _first_present(
            candidate_payload.get("speechQualityRejection"),
            pre_stop_payload.get("speechQualityRejection"),
        ),
    }


def _candidate_without_pre_stop_window(
    *,
    call_id: str,
    candidate: dict[str, Any],
    decisions: list[dict[str, Any]],
    provider_speech_events: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = candidate["payload"]
    terminal_decision = _find_decision_for_candidate_without_pre_stop(
        candidate,
        [event for event in decisions if event["eventType"] not in DEFERRED_EVENT_TYPES],
    )
    deferred_decision = _find_decision_for_candidate_without_pre_stop(
        candidate,
        [event for event in decisions if event["eventType"] in DEFERRED_EVENT_TYPES],
    )
    provider_speech = _has_provider_speech_near_candidate(
        candidate,
        provider_speech_events=provider_speech_events,
    )
    decision = terminal_decision or deferred_decision
    decision_type = decision["eventType"] if decision is not None else None
    decision_payload = decision["payload"] if decision is not None else {}
    if decision_type in CONFIRMED_EVENT_TYPES or provider_speech:
        outcome = "confirmed_without_pre_stop"
        severity = "warn"
    elif decision_type in REJECTED_EVENT_TYPES:
        outcome = "rejected_without_pre_stop"
        severity = "info"
    else:
        outcome = "candidate_without_pre_stop"
        severity = "info"
    return {
        "callId": call_id,
        "outcome": outcome,
        "severity": severity,
        "responseId": payload.get("responseId"),
        "generation": payload.get("generation"),
        "candidateTime": candidate["eventTime"],
        "preStopTime": None,
        "decisionTime": decision["eventTime"] if decision is not None else None,
        "decisionEventType": decision_type,
        "decisionReason": decision_payload.get("reason"),
        "providerSpeechStarted": provider_speech,
        "candidateToPreStopMs": None,
        "candidateToDecisionMs": _elapsed_ms(candidate, decision),
        "preStopToDecisionMs": None,
        "candidateDurationMs": payload.get("candidateDurationMs"),
        "wallClockSpeechMs": payload.get("wallClockSpeechMs"),
        "rmsDbfs": payload.get("rmsDbfs"),
        "snrDb": payload.get("snrDb"),
        "peakDbfs": payload.get("peakDbfs"),
        "speechQualityRejection": payload.get("speechQualityRejection"),
    }


def _find_decision_for_candidate_without_pre_stop(
    candidate: dict[str, Any],
    decisions: list[dict[str, Any]],
    *,
    window_ms: int = PROVIDER_CONFIRMED_CANDIDATE_WINDOW_MS,
) -> dict[str, Any] | None:
    candidate_response_id = candidate["payload"].get("responseId")
    eligible = [
        decision
        for decision in decisions
        if _event_time_lte(candidate, decision)
        and _elapsed_ms(candidate, decision) is not None
        and (_elapsed_ms(candidate, decision) or 0) <= window_ms
        and (
            candidate_response_id is None
            or decision["payload"].get("responseId") in (None, candidate_response_id)
        )
    ]
    if not eligible:
        return None
    return min(eligible, key=lambda event: (event["timestamp"] or datetime.max, event["index"]))


def _find_candidate_for_pre_stop(
    pre_stop: dict[str, Any],
    candidates: list[dict[str, Any]],
    matched_candidate_indexes: set[int],
) -> dict[str, Any] | None:
    pre_stop_response_id = pre_stop["payload"].get("responseId")
    eligible = [
        candidate
        for candidate in candidates
        if candidate["index"] not in matched_candidate_indexes
        and _event_time_lte(candidate, pre_stop)
        and (
            pre_stop_response_id is None
            or candidate["payload"].get("responseId") in (None, pre_stop_response_id)
        )
    ]
    if not eligible:
        return None
    return max(eligible, key=lambda event: (event["timestamp"] or datetime.min, event["index"]))


def _find_decision_for_pre_stop(
    pre_stop: dict[str, Any],
    decisions: list[dict[str, Any]],
) -> dict[str, Any] | None:
    pre_stop_response_id = pre_stop["payload"].get("responseId")
    eligible = [
        decision
        for decision in decisions
        if _event_time_lte(pre_stop, decision)
        and (
            pre_stop_response_id is None
            or decision["payload"].get("responseId") in (None, pre_stop_response_id)
        )
    ]
    if not eligible:
        return None
    return min(eligible, key=lambda event: (event["timestamp"] or datetime.max, event["index"]))


def _has_provider_speech_near_pre_stop(
    pre_stop: dict[str, Any],
    *,
    candidate: dict[str, Any] | None,
    provider_speech_events: list[dict[str, Any]],
    window_ms: int = 1_200,
) -> bool:
    if not provider_speech_events:
        return False
    start = candidate or pre_stop
    for provider_event in provider_speech_events:
        elapsed_from_start = _elapsed_ms(start, provider_event)
        elapsed_from_pre_stop = _elapsed_ms(pre_stop, provider_event)
        if elapsed_from_start is not None and 0 <= elapsed_from_start <= window_ms:
            return True
        if elapsed_from_pre_stop is not None and 0 <= elapsed_from_pre_stop <= window_ms:
            return True
    return False


def _has_provider_speech_near_candidate(
    candidate: dict[str, Any],
    *,
    provider_speech_events: list[dict[str, Any]],
    window_ms: int = PROVIDER_CONFIRMED_CANDIDATE_WINDOW_MS,
) -> bool:
    for provider_event in provider_speech_events:
        elapsed = _elapsed_ms(candidate, provider_event)
        if elapsed is not None and 0 <= elapsed <= window_ms:
            return True
    return False


def _summary(windows: list[dict[str, Any]], provider_speech_events: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "confirmedPreStops": sum(1 for window in windows if window["outcome"] == "confirmed_pre_stop"),
        "confirmedWithoutPreStop": sum(
            1 for window in windows if window["outcome"] == "confirmed_without_pre_stop"
        ),
        "falsePreStops": sum(1 for window in windows if window["outcome"] == "false_pre_stop_rejected"),
        "candidateOnly": sum(1 for window in windows if window["outcome"] == "candidate_without_pre_stop"),
        "preStopPending": sum(1 for window in windows if window["outcome"] == "pre_stop_pending"),
        "providerSpeechStarted": len(provider_speech_events),
    }


def _quality_report(
    *,
    normalized_events: list[dict[str, Any]],
    dialogue_segments: list[dict[str, Any]],
    max_pre_stop_latency_ms: int,
    max_end_completion_ms: int,
    max_call_end_intent_schedule_ms: int,
) -> dict[str, Any]:
    missed_speech = _missed_customer_speech_windows(
        normalized_events=normalized_events,
        dialogue_segments=dialogue_segments,
        max_pre_stop_latency_ms=max_pre_stop_latency_ms,
    )
    slow_ends = _slow_session_end_windows(
        normalized_events=normalized_events,
        max_end_completion_ms=max_end_completion_ms,
    )
    call_end_intent_windows = _call_end_intent_windows(
        normalized_events=normalized_events,
        max_call_end_intent_schedule_ms=max_call_end_intent_schedule_ms,
    )
    slow_call_end_intents = [
        window for window in call_end_intent_windows if not window["passed"]
    ]
    return {
        "passed": not missed_speech and not slow_ends and not slow_call_end_intents,
        "missedCustomerSpeech": len(missed_speech),
        "slowSessionEnds": len(slow_ends),
        "callEndIntents": len(call_end_intent_windows),
        "slowCallEndIntents": len(slow_call_end_intents),
        "missedCustomerSpeechWindows": missed_speech,
        "slowSessionEndWindows": slow_ends,
        "callEndIntentWindows": call_end_intent_windows,
        "slowCallEndIntentWindows": slow_call_end_intents,
        "thresholds": {
            "maxPreStopLatencyMs": max_pre_stop_latency_ms,
            "maxEndCompletionMs": max_end_completion_ms,
            "maxCallEndIntentScheduleMs": max_call_end_intent_schedule_ms,
        },
    }


def _missed_customer_speech_windows(
    *,
    normalized_events: list[dict[str, Any]],
    dialogue_segments: list[dict[str, Any]],
    max_pre_stop_latency_ms: int,
) -> list[dict[str, Any]]:
    ai_segments = [
        segment
        for segment in (_normalize_dialogue_segment(index, row) for index, row in enumerate(dialogue_segments))
        if segment["speakerType"] == "ai"
        and segment["startedAt"] is not None
        and segment["endedAt"] is not None
    ]
    customer_segments = [
        segment
        for segment in (_normalize_dialogue_segment(index, row) for index, row in enumerate(dialogue_segments))
        if segment["speakerType"] == "customer"
        and segment["source"] == "offline_asr"
        and segment["startedAt"] is not None
        and segment["endedAt"] is not None
    ]
    candidates = [
        event for event in normalized_events if event["eventType"] == "sip_interrupt_candidate"
    ]
    pre_stops = [event for event in normalized_events if event["eventType"] == "sip_pre_stop"]
    decisions = [
        event
        for event in normalized_events
        if event["eventType"] in CONFIRMED_EVENT_TYPES or event["eventType"] in REJECTED_EVENT_TYPES
    ]
    provider_speech_events = [
        event for event in normalized_events if event["eventType"] in PROVIDER_SPEECH_EVENT_TYPES
    ]

    missed: list[dict[str, Any]] = []
    for segment in customer_segments:
        if not any(_segments_overlap(segment, ai_segment) for ai_segment in ai_segments):
            continue
        first_pre_stop = _first_event_at_or_after(pre_stops, segment["startedAt"])
        speech_start_to_pre_stop_ms = _elapsed_from_time_ms(segment["startedAt"], first_pre_stop)
        if (
            speech_start_to_pre_stop_ms is not None
            and speech_start_to_pre_stop_ms <= max_pre_stop_latency_ms
        ):
            continue
        nearest_candidate = _nearest_event_at_or_after(candidates, segment["startedAt"])
        candidate_lag_ms = _elapsed_from_time_ms(segment["startedAt"], nearest_candidate)
        fast_candidate_resolution = _candidate_has_fast_resolution(
            candidate=nearest_candidate,
            pre_stops=pre_stops,
            decisions=decisions,
            provider_speech_events=provider_speech_events,
            max_resolution_ms=max_pre_stop_latency_ms,
        )
        if (
            fast_candidate_resolution
            and candidate_lag_ms is not None
            and candidate_lag_ms <= OFFLINE_ASR_CANDIDATE_SKEW_TOLERANCE_MS
        ):
            continue
        missed.append({
            "severity": "fail",
            "reason": "offline_customer_speech_without_fast_pre_stop",
            "text": segment["text"],
            "speechStartTime": segment["startedAtRaw"],
            "speechEndTime": segment["endedAtRaw"],
            "speechDurationMs": _elapsed_between_times_ms(
                segment["startedAt"],
                segment["endedAt"],
            ),
            "nearestCandidateTime": (
                nearest_candidate["eventTime"] if nearest_candidate is not None else None
            ),
            "nearestCandidateToSpeechStartMs": candidate_lag_ms,
            "speechStartToPreStopMs": speech_start_to_pre_stop_ms,
            "maxPreStopLatencyMs": max_pre_stop_latency_ms,
        })
    return missed


def _candidate_has_fast_resolution(
    *,
    candidate: dict[str, Any] | None,
    pre_stops: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
    provider_speech_events: list[dict[str, Any]],
    max_resolution_ms: int,
) -> bool:
    if candidate is None:
        return False
    first_pre_stop = _first_event_at_or_after(pre_stops, candidate["timestamp"])
    candidate_to_pre_stop_ms = _elapsed_ms(candidate, first_pre_stop)
    if (
        candidate_to_pre_stop_ms is not None
        and candidate_to_pre_stop_ms <= max_resolution_ms
    ):
        return True
    decision = _find_decision_for_candidate_without_pre_stop(
        candidate,
        decisions,
        window_ms=max_resolution_ms,
    )
    if decision is not None:
        return True
    return _has_provider_speech_near_candidate(
        candidate,
        provider_speech_events=provider_speech_events,
        window_ms=max_resolution_ms,
    )


def _slow_session_end_windows(
    *,
    normalized_events: list[dict[str, Any]],
    max_end_completion_ms: int,
) -> list[dict[str, Any]]:
    endings = [event for event in normalized_events if event["eventType"] == "session_ending"]
    completions = [
        event for event in normalized_events if event["eventType"] == "session_completed"
    ]
    slow: list[dict[str, Any]] = []
    for ending in endings:
        completed = _first_event_at_or_after(completions, ending["timestamp"])
        ending_to_completed_ms = _elapsed_ms(ending, completed)
        if ending_to_completed_ms is None or ending_to_completed_ms <= max_end_completion_ms:
            continue
        payload = completed["payload"] if completed is not None else {}
        slow.append({
            "severity": "fail",
            "reason": "session_end_completion_too_slow",
            "endingTime": ending["eventTime"],
            "completedTime": completed["eventTime"] if completed is not None else None,
            "endingToCompletedMs": ending_to_completed_ms,
            "maxEndCompletionMs": max_end_completion_ms,
            "endReason": payload.get("endReason"),
        })
    return slow


def _call_end_intent_windows(
    *,
    normalized_events: list[dict[str, Any]],
    max_call_end_intent_schedule_ms: int,
) -> list[dict[str, Any]]:
    intents = [
        event for event in normalized_events if event["eventType"] == "call_end_intent_detected"
    ]
    scheduled_events = [
        event for event in normalized_events if event["eventType"] == "call_end_scheduled"
    ]
    windows: list[dict[str, Any]] = []
    for intent in intents:
        scheduled = _first_event_at_or_after(scheduled_events, intent["timestamp"])
        intent_to_schedule_ms = _elapsed_ms(intent, scheduled)
        passed = (
            intent_to_schedule_ms is not None
            and intent_to_schedule_ms <= max_call_end_intent_schedule_ms
        )
        if intent_to_schedule_ms is None:
            reason = "missing_call_end_scheduled"
        elif passed:
            reason = "passed"
        else:
            reason = "call_end_schedule_too_slow"
        intent_payload = intent["payload"]
        scheduled_payload = scheduled["payload"] if scheduled is not None else {}
        windows.append({
            "passed": passed,
            "severity": "pass" if passed else "fail",
            "reason": reason,
            "intentTime": intent["eventTime"],
            "scheduledTime": scheduled["eventTime"] if scheduled is not None else None,
            "intentToScheduleMs": intent_to_schedule_ms,
            "maxCallEndIntentScheduleMs": max_call_end_intent_schedule_ms,
            "endReason": scheduled_payload.get("endReason"),
            "transcriptPreview": intent_payload.get("transcriptPreview"),
        })
    return windows


def _normalize_event(index: int, event: dict[str, Any]) -> dict[str, Any]:
    event_time = event.get("eventTime") or event.get("timestamp")
    payload = event.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    return {
        "index": index,
        "eventType": str(event.get("eventType") or event.get("type") or ""),
        "eventTime": event_time,
        "timestamp": _parse_optional_time(event_time),
        "payload": payload,
    }


def _normalize_dialogue_segment(index: int, row: dict[str, Any]) -> dict[str, Any]:
    started_at = _first_present(row.get("startedAt"), row.get("started_at"))
    ended_at = _first_present(row.get("endedAt"), row.get("ended_at"))
    return {
        "index": index,
        "speakerType": str(
            _first_present(row.get("speakerType"), row.get("speaker_type"), "")
        ),
        "source": str(row.get("source") or ""),
        "startedAtRaw": started_at,
        "endedAtRaw": ended_at,
        "startedAt": _parse_optional_time(started_at),
        "endedAt": _parse_optional_time(ended_at),
        "text": str(
            _first_present(
                row.get("text"),
                row.get("segmentText"),
                row.get("segment_text"),
                "",
            )
        ),
    }


def _segments_overlap(first: dict[str, Any], second: dict[str, Any]) -> bool:
    first_started = first["startedAt"]
    first_ended = first["endedAt"]
    second_started = second["startedAt"]
    second_ended = second["endedAt"]
    if None in (first_started, first_ended, second_started, second_ended):
        return False
    return first_started < second_ended and first_ended > second_started


def _first_event_at_or_after(
    events: list[dict[str, Any]],
    timestamp: datetime | None,
) -> dict[str, Any] | None:
    if timestamp is None:
        return None
    eligible = [
        event
        for event in events
        if event.get("timestamp") is not None and event["timestamp"] >= timestamp
    ]
    if not eligible:
        return None
    return min(eligible, key=lambda event: (event["timestamp"], event["index"]))


def _nearest_event_at_or_after(
    events: list[dict[str, Any]],
    timestamp: datetime | None,
) -> dict[str, Any] | None:
    return _first_event_at_or_after(events, timestamp)


def _event_time_lte(first: dict[str, Any], second: dict[str, Any]) -> bool:
    first_time = first.get("timestamp")
    second_time = second.get("timestamp")
    if first_time is None or second_time is None:
        return first["index"] <= second["index"]
    return first_time <= second_time


def _elapsed_ms(first: dict[str, Any] | None, second: dict[str, Any] | None) -> int | None:
    if first is None or second is None:
        return None
    first_time = first.get("timestamp")
    second_time = second.get("timestamp")
    if first_time is None or second_time is None:
        return None
    return round((second_time - first_time).total_seconds() * 1000)


def _elapsed_from_time_ms(
    first_time: datetime | None,
    second: dict[str, Any] | None,
) -> int | None:
    if first_time is None or second is None:
        return None
    second_time = second.get("timestamp")
    if second_time is None:
        return None
    return round((second_time - first_time).total_seconds() * 1000)


def _elapsed_between_times_ms(
    first_time: datetime | None,
    second_time: datetime | None,
) -> int | None:
    if first_time is None or second_time is None:
        return None
    return round((second_time - first_time).total_seconds() * 1000)


def _parse_optional_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return parsed.replace(tzinfo=None)


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


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
    return {key: record.get(key) for key in keys if key in record}
