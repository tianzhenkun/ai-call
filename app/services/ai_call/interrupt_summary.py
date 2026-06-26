from __future__ import annotations

from datetime import datetime
from typing import Any

SLOW_BROWSER_TO_CONFIRMED_MS = 800
SLOW_PROVIDER_TO_CONFIRMED_MS = 500


def build_interrupt_summary(call_id: str, events: list[dict[str, Any]]) -> dict[str, Any]:
    candidate_count = 0
    confirmed_count = 0
    stale_audio_dropped_count = 0
    stale_audio_dropped_bytes = 0
    playout_flush_count = 0
    session_ending_count = 0
    session_completed_count = 0
    agent_start_failed = False
    session_failed = False
    stale_audio_risk = False

    current_browser_started_at: datetime | None = None
    current_provider_started_at: datetime | None = None
    browser_to_provider_samples: list[int] = []
    provider_to_confirmed_samples: list[int] = []
    browser_to_confirmed_samples: list[int] = []
    has_open_candidate = False

    for event in events:
        event_type = str(event.get("eventType") or event.get("type") or "")
        event_time = _event_time(event)
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}

        if event_type in {"browser_user_speech_started", "browser_user_speech_segment"}:
            current_browser_started_at = event_time
            current_provider_started_at = None
        elif event_type == "user_speech_started":
            current_provider_started_at = event_time
            if current_browser_started_at is not None:
                browser_to_provider_samples.append(
                    _milliseconds_between(current_browser_started_at, event_time)
                )
        elif event_type in {"interrupt_confirmed", "sip_interrupt_candidate_confirmed"}:
            if has_open_candidate:
                confirmed_count += 1
                has_open_candidate = False
                if current_provider_started_at is not None:
                    provider_to_confirmed_samples.append(
                        _milliseconds_between(current_provider_started_at, event_time)
                    )
                if current_browser_started_at is not None:
                    browser_to_confirmed_samples.append(
                        _milliseconds_between(current_browser_started_at, event_time)
                    )
                current_browser_started_at = None
                current_provider_started_at = None
        elif event_type == "interrupt_candidate":
            candidate_count += 1
            has_open_candidate = True
        elif event_type == "stale_audio_dropped":
            stale_audio_dropped_count += 1
            stale_audio_dropped_bytes += _payload_int(payload, "deltaBytes")
            if payload.get("reason") != "interrupt_pending":
                stale_audio_risk = True
        elif event_type == "playout_queue_flushed":
            playout_flush_count += 1
        elif event_type == "agent_start_failed":
            agent_start_failed = True
        elif event_type == "session_failed":
            session_failed = True
            if payload.get("endReason") == "agent_start_failed":
                agent_start_failed = True
        elif event_type == "session_ending":
            session_ending_count += 1
        elif event_type == "session_completed":
            session_completed_count += 1

    browser_to_provider_ms = _max_or_none(browser_to_provider_samples)
    provider_to_confirmed_ms = _max_or_none(provider_to_confirmed_samples)
    browser_to_confirmed_ms = _max_or_none(browser_to_confirmed_samples)
    candidate_not_confirmed_count = max(0, candidate_count - confirmed_count)
    duplicate_end_request = session_ending_count > 1 or session_completed_count > 1
    slow_confirm = (
        browser_to_confirmed_ms is not None
        and browser_to_confirmed_ms > SLOW_BROWSER_TO_CONFIRMED_MS
    ) or (
        provider_to_confirmed_ms is not None
        and provider_to_confirmed_ms > SLOW_PROVIDER_TO_CONFIRMED_MS
    )

    issues: list[str] = []
    if agent_start_failed:
        issues.append("agent_start_failed")
    if session_failed:
        issues.append("session_failed")
    if stale_audio_risk:
        issues.append("stale_audio_risk")
    if candidate_not_confirmed_count > 0:
        issues.append("candidate_not_confirmed")
    if slow_confirm:
        issues.append("slow_confirm")
    if duplicate_end_request:
        issues.append("duplicate_end_request")

    if session_failed or agent_start_failed:
        verdict = "session_failed"
    elif stale_audio_risk:
        verdict = "stale_audio_risk"
    elif candidate_not_confirmed_count > 0:
        verdict = "candidate_not_confirmed"
    elif slow_confirm:
        verdict = "slow_confirm"
    else:
        verdict = "normal"

    return {
        "callId": call_id,
        "interruptCandidateCount": candidate_count,
        "interruptConfirmedCount": confirmed_count,
        "candidateNotConfirmedCount": candidate_not_confirmed_count,
        "browserToProviderMs": browser_to_provider_ms,
        "providerToConfirmedMs": provider_to_confirmed_ms,
        "browserToConfirmedMs": browser_to_confirmed_ms,
        "staleAudioDroppedCount": stale_audio_dropped_count,
        "staleAudioDroppedBytes": stale_audio_dropped_bytes,
        "playoutFlushCount": playout_flush_count,
        "duplicateEndRequest": duplicate_end_request,
        "agentStartFailed": agent_start_failed,
        "verdict": verdict,
        "issues": issues,
    }


def _event_time(event: dict[str, Any]) -> datetime:
    value = event.get("eventTime") or event.get("timestamp")
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise ValueError("event time is required")


def _milliseconds_between(start: datetime, end: datetime) -> int:
    return round((end - start).total_seconds() * 1000)


def _max_or_none(values: list[int]) -> int | None:
    return max(values) if values else None


def _payload_int(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(value)
    if isinstance(value, str) and value.isdecimal():
        return int(value)
    return 0
