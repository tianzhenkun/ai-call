from __future__ import annotations

from datetime import datetime
from typing import Any

PRE_STOP_OUTCOMES = {
    "confirmed_pre_stop",
    "false_pre_stop_rejected",
    "pre_stop_pending",
}
RESOLVED_PRE_STOP_OUTCOMES = {
    "confirmed_pre_stop",
    "false_pre_stop_rejected",
}


def build_p1_sample_matrix_evaluation(
    *,
    reports_by_call_id: dict[str, dict[str, Any]],
    samples: list[dict[str, Any]],
    coverage_gates: dict[str, Any] | None = None,
) -> dict[str, Any]:
    results = [
        _evaluate_sample(reports_by_call_id=reports_by_call_id, sample=sample)
        for sample in samples
    ]
    return {
        "mode": "sample_matrix",
        "summary": _summary(results, samples=samples, coverage_gates=coverage_gates),
        "samples": results,
    }


def _copy_structured_annotations(
    target: dict[str, Any],
    sample: dict[str, Any],
) -> None:
    for key in ("utterance", "turnEvidence", "acousticContext"):
        value = sample.get(key)
        if isinstance(value, dict):
            target[key] = value


def _evaluate_sample(
    *,
    reports_by_call_id: dict[str, dict[str, Any]],
    sample: dict[str, Any],
) -> dict[str, Any]:
    call_id = str(sample.get("callId") or "")
    category = str(sample.get("category") or "uncategorized")
    expectation = str(sample.get("expectation") or "")
    base = {
        "id": str(sample.get("id") or call_id),
        "callId": call_id,
        "sourceType": str(sample.get("sourceType") or "unspecified"),
        "evaluationSource": str(sample.get("evaluationSource") or "unspecified"),
        "category": category,
        "expectation": expectation,
    }
    _copy_structured_annotations(base, sample)
    report = reports_by_call_id.get(call_id)
    if report is None:
        return {
            **base,
            "passed": False,
            "reason": "missing_report",
            "evidence": {},
        }

    if expectation == "must_interrupt":
        return _evaluate_must_interrupt(base=base, sample=sample, report=report)
    if expectation == "must_not_interrupt":
        return _evaluate_must_not_interrupt(base=base, sample=sample, report=report)
    if expectation == "must_defer":
        return _evaluate_must_defer(base=base, sample=sample, report=report)
    if expectation == "must_confirm_or_reject":
        return _evaluate_must_confirm_or_reject(base=base, sample=sample, report=report)
    if expectation == "must_pre_stop_after_candidate":
        return _evaluate_must_pre_stop_after_candidate(base=base, sample=sample, report=report)
    if expectation == "must_resolve_after_candidate":
        return _evaluate_must_resolve_after_candidate(base=base, sample=sample, report=report)
    if expectation == "must_schedule_call_end":
        return _evaluate_must_schedule_call_end(base=base, sample=sample, report=report)

    return {
        **base,
        "passed": False,
        "reason": "unsupported_expectation",
        "evidence": {
            "supportedExpectations": [
                "must_interrupt",
                "must_not_interrupt",
                "must_defer",
                "must_confirm_or_reject",
                "must_pre_stop_after_candidate",
                "must_resolve_after_candidate",
                "must_schedule_call_end",
            ]
        },
    }


def _evaluate_must_interrupt(
    *,
    base: dict[str, Any],
    sample: dict[str, Any],
    report: dict[str, Any],
) -> dict[str, Any]:
    speech_start = _parse_optional_time(sample.get("speechStartTime"))
    max_latency_ms = int(sample.get("maxPreStopLatencyMs") or 500)
    if speech_start is None:
        return {
            **base,
            "passed": False,
            "reason": "missing_speech_start_time",
            "evidence": {},
        }

    pre_stop_window = _first_window_at_or_after(
        report.get("windows") or [],
        timestamp=speech_start,
        time_key="preStopTime",
    )
    latency_ms = _elapsed_from_time_ms(
        speech_start,
        pre_stop_window.get("preStopTime") if pre_stop_window is not None else None,
    )
    evidence = {
        "speechStartTime": sample.get("speechStartTime"),
        "preStopTime": pre_stop_window.get("preStopTime") if pre_stop_window else None,
        "speechStartToPreStopMs": latency_ms,
        "maxPreStopLatencyMs": max_latency_ms,
        "outcome": pre_stop_window.get("outcome") if pre_stop_window else None,
    }
    if latency_ms is None:
        return {**base, "passed": False, "reason": "missing_pre_stop", "evidence": evidence}
    if latency_ms > max_latency_ms:
        return {**base, "passed": False, "reason": "pre_stop_too_slow", "evidence": evidence}
    return {**base, "passed": True, "reason": "passed", "evidence": evidence}


def _evaluate_must_not_interrupt(
    *,
    base: dict[str, Any],
    sample: dict[str, Any],
    report: dict[str, Any],
) -> dict[str, Any]:
    window_start = _parse_optional_time(sample.get("windowStartTime"))
    window_end = _parse_optional_time(sample.get("windowEndTime"))
    if window_start is None or window_end is None:
        return {
            **base,
            "passed": False,
            "reason": "missing_time_window",
            "evidence": {},
        }

    for window in report.get("windows") or []:
        pre_stop_time = _parse_optional_time(window.get("preStopTime"))
        if pre_stop_time is None or not (window_start <= pre_stop_time <= window_end):
            continue
        if window.get("outcome") not in PRE_STOP_OUTCOMES:
            continue
        return {
            **base,
            "passed": False,
            "reason": "unexpected_pre_stop",
            "evidence": _window_evidence(sample=sample, window=window),
        }

    return {
        **base,
        "passed": True,
        "reason": "passed",
        "evidence": {
            "windowStartTime": sample.get("windowStartTime"),
            "windowEndTime": sample.get("windowEndTime"),
        },
    }


def _evaluate_must_defer(
    *,
    base: dict[str, Any],
    sample: dict[str, Any],
    report: dict[str, Any],
) -> dict[str, Any]:
    window_start = _parse_optional_time(sample.get("windowStartTime"))
    window_end = _parse_optional_time(sample.get("windowEndTime"))
    if window_start is None or window_end is None:
        return {
            **base,
            "passed": False,
            "reason": "missing_time_window",
            "evidence": {},
        }

    for window in report.get("windows") or []:
        pre_stop_time = _parse_optional_time(window.get("preStopTime"))
        if pre_stop_time is None or not (window_start <= pre_stop_time <= window_end):
            continue
        if window.get("outcome") not in PRE_STOP_OUTCOMES:
            continue
        return {
            **base,
            "passed": False,
            "reason": "unexpected_pre_stop",
            "evidence": _window_evidence(sample=sample, window=window),
        }

    candidate_window = _first_window_in_range(
        report.get("windows") or [],
        time_key="candidateTime",
        window_start=window_start,
        window_end=window_end,
        outcomes={"candidate_without_pre_stop"},
    )
    if candidate_window is None:
        return {
            **base,
            "passed": False,
            "reason": "missing_deferred_candidate",
            "evidence": {
                "windowStartTime": sample.get("windowStartTime"),
                "windowEndTime": sample.get("windowEndTime"),
            },
        }

    return {
        **base,
        "passed": True,
        "reason": "passed",
        "evidence": _window_evidence(sample=sample, window=candidate_window),
    }


def _evaluate_must_confirm_or_reject(
    *,
    base: dict[str, Any],
    sample: dict[str, Any],
    report: dict[str, Any],
) -> dict[str, Any]:
    window_start = _parse_optional_time(sample.get("windowStartTime"))
    window_end = _parse_optional_time(sample.get("windowEndTime"))
    if window_start is None or window_end is None:
        return {
            **base,
            "passed": False,
            "reason": "missing_time_window",
            "evidence": {},
        }

    pre_stop_window = _first_window_in_range(
        report.get("windows") or [],
        time_key="preStopTime",
        window_start=window_start,
        window_end=window_end,
        outcomes=PRE_STOP_OUTCOMES,
    )
    if pre_stop_window is None:
        return {
            **base,
            "passed": False,
            "reason": "missing_pre_stop",
            "evidence": {
                "windowStartTime": sample.get("windowStartTime"),
                "windowEndTime": sample.get("windowEndTime"),
            },
        }

    evidence = _window_evidence(sample=sample, window=pre_stop_window)
    outcome = pre_stop_window.get("outcome")
    if outcome not in RESOLVED_PRE_STOP_OUTCOMES:
        return {
            **base,
            "passed": False,
            "reason": str(outcome or "pre_stop_pending"),
            "evidence": evidence,
        }

    return {**base, "passed": True, "reason": "passed", "evidence": evidence}


def _evaluate_must_pre_stop_after_candidate(
    *,
    base: dict[str, Any],
    sample: dict[str, Any],
    report: dict[str, Any],
) -> dict[str, Any]:
    candidate_time = _parse_optional_time(sample.get("candidateTime"))
    max_latency_ms = int(sample.get("maxCandidateToPreStopMs") or 500)
    if candidate_time is None:
        return {
            **base,
            "passed": False,
            "reason": "missing_candidate_time",
            "evidence": {},
        }

    windows = report.get("windows") or []
    candidate_window = _first_window_at_or_after(
        windows,
        timestamp=candidate_time,
        time_key="candidateTime",
    )
    evidence = {
        "candidateTime": candidate_window.get("candidateTime") if candidate_window else sample.get("candidateTime"),
        "preStopTime": None,
        "candidateToPreStopMs": None,
        "maxCandidateToPreStopMs": max_latency_ms,
        "outcome": candidate_window.get("outcome") if candidate_window else None,
    }
    if candidate_window is None:
        return {**base, "passed": False, "reason": "missing_candidate", "evidence": evidence}

    pre_stop_window = _first_window_at_or_after(
        windows,
        timestamp=candidate_time,
        time_key="preStopTime",
    )
    if pre_stop_window is None:
        return {**base, "passed": False, "reason": "missing_pre_stop", "evidence": evidence}

    latency_ms = _elapsed_from_time_ms(
        candidate_time,
        pre_stop_window.get("preStopTime"),
    )
    evidence.update(
        {
            "preStopTime": pre_stop_window.get("preStopTime"),
            "candidateToPreStopMs": latency_ms,
            "outcome": pre_stop_window.get("outcome"),
            "decisionEventType": pre_stop_window.get("decisionEventType"),
            "decisionReason": pre_stop_window.get("decisionReason"),
        }
    )
    if latency_ms is None:
        return {**base, "passed": False, "reason": "missing_pre_stop", "evidence": evidence}
    if latency_ms > max_latency_ms:
        return {**base, "passed": False, "reason": "pre_stop_too_slow", "evidence": evidence}
    return {**base, "passed": True, "reason": "passed", "evidence": evidence}


def _evaluate_must_resolve_after_candidate(
    *,
    base: dict[str, Any],
    sample: dict[str, Any],
    report: dict[str, Any],
) -> dict[str, Any]:
    candidate_time = _parse_optional_time(sample.get("candidateTime"))
    max_latency_ms = int(sample.get("maxCandidateToResolutionMs") or 500)
    if candidate_time is None:
        return {
            **base,
            "passed": False,
            "reason": "missing_candidate_time",
            "evidence": {},
        }

    candidate_window = _first_window_at_or_after(
        report.get("windows") or [],
        timestamp=candidate_time,
        time_key="candidateTime",
    )
    evidence = {
        "candidateTime": candidate_window.get("candidateTime") if candidate_window else sample.get("candidateTime"),
        "resolutionTime": None,
        "candidateToResolutionMs": None,
        "maxCandidateToResolutionMs": max_latency_ms,
        "outcome": candidate_window.get("outcome") if candidate_window else None,
    }
    if candidate_window is None:
        return {**base, "passed": False, "reason": "missing_candidate", "evidence": evidence}

    resolution_time = candidate_window.get("preStopTime") or candidate_window.get("decisionTime")
    latency_ms = _elapsed_from_time_ms(candidate_time, resolution_time)
    evidence.update(
        {
            "resolutionTime": resolution_time,
            "candidateToResolutionMs": latency_ms,
            "decisionEventType": candidate_window.get("decisionEventType"),
            "decisionReason": candidate_window.get("decisionReason"),
        }
    )
    if candidate_window.get("outcome") not in {
        "confirmed_pre_stop",
        "false_pre_stop_rejected",
        "confirmed_without_pre_stop",
        "rejected_without_pre_stop",
    }:
        return {**base, "passed": False, "reason": "candidate_not_resolved", "evidence": evidence}
    if latency_ms is None:
        return {**base, "passed": False, "reason": "missing_resolution", "evidence": evidence}
    if latency_ms > max_latency_ms:
        return {**base, "passed": False, "reason": "resolution_too_slow", "evidence": evidence}
    return {**base, "passed": True, "reason": "passed", "evidence": evidence}


def _evaluate_must_schedule_call_end(
    *,
    base: dict[str, Any],
    sample: dict[str, Any],
    report: dict[str, Any],
) -> dict[str, Any]:
    intent_time = _parse_optional_time(sample.get("intentTime"))
    max_latency_ms = int(sample.get("maxScheduleLatencyMs") or 1_000)
    if intent_time is None:
        return {
            **base,
            "passed": False,
            "reason": "missing_intent_time",
            "evidence": {},
        }

    window = _first_call_end_window_at_or_after(
        report.get("quality", {}).get("callEndIntentWindows") or [],
        timestamp=intent_time,
    )
    if window is None:
        return {
            **base,
            "passed": False,
            "reason": "missing_call_end_intent",
            "evidence": {
                "intentTime": sample.get("intentTime"),
                "maxScheduleLatencyMs": max_latency_ms,
            },
        }

    intent_to_schedule_ms = window.get("intentToScheduleMs")
    evidence = {
        "intentTime": window.get("intentTime"),
        "scheduledTime": window.get("scheduledTime"),
        "intentToScheduleMs": intent_to_schedule_ms,
        "maxScheduleLatencyMs": max_latency_ms,
        "transcriptPreview": window.get("transcriptPreview"),
    }
    if intent_to_schedule_ms is None:
        return {
            **base,
            "passed": False,
            "reason": str(window.get("reason") or "missing_call_end_scheduled"),
            "evidence": evidence,
        }
    if int(intent_to_schedule_ms) > max_latency_ms:
        return {
            **base,
            "passed": False,
            "reason": "call_end_schedule_too_slow",
            "evidence": evidence,
        }

    return {
        **base,
        "passed": True,
        "reason": "passed",
        "evidence": evidence,
    }


def _summary(
    results: list[dict[str, Any]],
    *,
    samples: list[dict[str, Any]],
    coverage_gates: dict[str, Any] | None,
) -> dict[str, Any]:
    categories: dict[str, dict[str, int]] = {}
    for result in results:
        category = result["category"]
        bucket = categories.setdefault(
            category,
            {"samples": 0, "passed": 0, "failed": 0},
        )
        bucket["samples"] += 1
        if result["passed"]:
            bucket["passed"] += 1
        else:
            bucket["failed"] += 1
    failed = sum(1 for result in results if not result["passed"])
    summary = {
        "samples": len(results),
        "passed": len(results) - failed,
        "failed": failed,
        "missingReports": sum(1 for result in results if result["reason"] == "missing_report"),
        "categories": categories,
    }
    evaluation_sources = _count_sample_field(
        samples,
        "evaluationSource",
        default="unspecified",
    )
    if evaluation_sources != {"unspecified": len(samples)}:
        summary["evaluationSources"] = evaluation_sources
    annotations = _annotation_summary(samples)
    if annotations:
        summary["annotations"] = annotations
    if coverage_gates:
        summary["coverage"] = _coverage_summary(
            samples=samples,
            coverage_gates=coverage_gates,
        )
    return summary


def _annotation_summary(samples: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    summary: dict[str, dict[str, int]] = {}
    utterance_sources = _count_nested_sample_field(samples, "utterance", "source")
    if utterance_sources:
        summary["utteranceSources"] = utterance_sources
    speech_patterns = _count_nested_sample_field(
        samples,
        "acousticContext",
        "speechPattern",
    )
    if speech_patterns:
        summary["speechPatterns"] = speech_patterns
    expired_reasons = _count_nested_sample_field(
        samples,
        "turnEvidence",
        "expiredReason",
    )
    if expired_reasons:
        summary["turnEvidenceExpiredReasons"] = expired_reasons
    return summary


def _coverage_summary(
    *,
    samples: list[dict[str, Any]],
    coverage_gates: dict[str, Any],
) -> dict[str, Any]:
    actual = {
        "samples": len(samples),
        "categories": _count_sample_field(samples, "category", default="uncategorized"),
        "sourceTypes": _count_sample_field(samples, "sourceType", default="unspecified"),
        "expectations": _count_sample_field(samples, "expectation", default="unspecified"),
    }
    failures: list[dict[str, Any]] = []
    min_samples = _optional_positive_int(coverage_gates.get("minSamples"))
    if min_samples is not None and actual["samples"] < min_samples:
        failures.append({
            "gate": "min_samples",
            "required": min_samples,
            "actual": actual["samples"],
        })
    failures.extend(
        _required_count_failures(
            gate="category",
            required=coverage_gates.get("requiredCategories"),
            actual=actual["categories"],
        )
    )
    failures.extend(
        _required_count_failures(
            gate="source_type",
            required=coverage_gates.get("requiredSourceTypes"),
            actual=actual["sourceTypes"],
        )
    )
    failures.extend(
        _required_count_failures(
            gate="expectation",
            required=coverage_gates.get("requiredExpectations"),
            actual=actual["expectations"],
        )
    )
    return {
        "passed": not failures,
        "failureCount": len(failures),
        "failures": failures,
        "actual": actual,
    }


def _count_sample_field(
    samples: list[dict[str, Any]],
    field: str,
    *,
    default: str,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for sample in samples:
        value = sample.get(field)
        key = value if isinstance(value, str) and value else default
        counts[key] = counts.get(key, 0) + 1
    return counts


def _count_nested_sample_field(
    samples: list[dict[str, Any]],
    container_field: str,
    field: str,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for sample in samples:
        container = sample.get(container_field)
        if not isinstance(container, dict):
            continue
        value = container.get(field)
        if not isinstance(value, str) or not value:
            continue
        counts[value] = counts.get(value, 0) + 1
    return counts


def _required_count_failures(
    *,
    gate: str,
    required: Any,
    actual: dict[str, int],
) -> list[dict[str, Any]]:
    if not isinstance(required, dict):
        return []
    failures: list[dict[str, Any]] = []
    for key, raw_required_count in required.items():
        if not isinstance(key, str) or not key:
            continue
        required_count = _optional_positive_int(raw_required_count)
        if required_count is None:
            continue
        actual_count = actual.get(key, 0)
        if actual_count < required_count:
            failures.append({
                "gate": gate,
                "key": key,
                "required": required_count,
                "actual": actual_count,
            })
    return failures


def _optional_positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    return None


def _first_window_at_or_after(
    windows: list[dict[str, Any]],
    *,
    timestamp: datetime,
    time_key: str,
) -> dict[str, Any] | None:
    eligible = [
        window
        for window in windows
        if _parse_optional_time(window.get(time_key)) is not None
        and _parse_optional_time(window.get(time_key)) >= timestamp
    ]
    if not eligible:
        return None
    return min(eligible, key=lambda window: _parse_optional_time(window.get(time_key)) or datetime.max)


def _first_window_in_range(
    windows: list[dict[str, Any]],
    *,
    time_key: str,
    window_start: datetime,
    window_end: datetime,
    outcomes: set[str],
) -> dict[str, Any] | None:
    eligible = [
        window
        for window in windows
        if window.get("outcome") in outcomes
        and _parse_optional_time(window.get(time_key)) is not None
        and window_start <= (_parse_optional_time(window.get(time_key)) or datetime.min) <= window_end
    ]
    if not eligible:
        return None
    return min(eligible, key=lambda window: _parse_optional_time(window.get(time_key)) or datetime.max)


def _window_evidence(*, sample: dict[str, Any], window: dict[str, Any]) -> dict[str, Any]:
    return {
        "windowStartTime": sample.get("windowStartTime"),
        "windowEndTime": sample.get("windowEndTime"),
        "candidateTime": window.get("candidateTime"),
        "preStopTime": window.get("preStopTime"),
        "decisionTime": window.get("decisionTime"),
        "outcome": window.get("outcome"),
        "decisionEventType": window.get("decisionEventType"),
        "decisionReason": window.get("decisionReason"),
    }


def _first_call_end_window_at_or_after(
    windows: list[dict[str, Any]],
    *,
    timestamp: datetime,
) -> dict[str, Any] | None:
    eligible = [
        window
        for window in windows
        if _parse_optional_time(window.get("intentTime")) is not None
        and _parse_optional_time(window.get("intentTime")) >= timestamp
    ]
    if not eligible:
        return None
    return min(
        eligible,
        key=lambda window: _parse_optional_time(window.get("intentTime")) or datetime.max,
    )


def _elapsed_from_time_ms(first_time: datetime, second_time_raw: Any) -> int | None:
    second_time = _parse_optional_time(second_time_raw)
    if second_time is None:
        return None
    return round((second_time - first_time).total_seconds() * 1000)


def _parse_optional_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None
