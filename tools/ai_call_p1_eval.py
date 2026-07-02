from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, TextIO

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.ai_call.interrupt_p1_evaluation import (
    build_p1_evaluation,
    build_p1_evaluation_suite,
)

GetJson = Callable[[str, float], dict[str, Any]]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="P1 SIP barge-in evaluation from AI Call events.",
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:19011",
        help="AI Call API base URL, without trailing slash.",
    )
    parser.add_argument("--call-id", help="Call ID to evaluate.")
    parser.add_argument(
        "--recent",
        type=int,
        help="Evaluate the latest N sip_outbound calls instead of a single call.",
    )
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--json", action="store_true", help="Print raw JSON report.")
    return parser


def run(
    argv: Sequence[str] | None = None,
    *,
    get_json: GetJson | None = None,
    stdout: TextIO = sys.stdout,
    stderr: TextIO = sys.stderr,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    get_json = get_json or _get_json
    base_url = args.base_url.rstrip("/")

    if bool(args.call_id) == bool(args.recent):
        parser.error("exactly one of --call-id or --recent is required")
    if args.recent is not None and args.recent < 1:
        parser.error("--recent must be greater than 0")

    try:
        report = (
            _build_recent_report(
                base_url=base_url,
                recent=args.recent,
                timeout_seconds=args.timeout_seconds,
                get_json=get_json,
            )
            if args.recent is not None
            else _build_single_report(
                base_url=base_url,
                call_id=str(args.call_id),
                timeout_seconds=args.timeout_seconds,
                get_json=get_json,
            )
        )
    except Exception as exc:
        print(f"fetch failed: {exc!s}", file=stderr)
        return 1

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2), file=stdout)
    else:
        _print_text_report(report, stdout)
    return 0


def main(argv: Sequence[str] | None = None) -> None:
    raise SystemExit(run(argv))


def _build_single_report(
    *,
    base_url: str,
    call_id: str,
    timeout_seconds: float,
    get_json: GetJson,
) -> dict[str, Any]:
    record = _record_for_call(base_url, call_id, timeout_seconds, get_json)
    events = _events_for_call(base_url, call_id, timeout_seconds, get_json)
    dialogue_segments = _dialogue_segments_for_call(
        base_url,
        call_id,
        timeout_seconds,
        get_json,
    )
    return build_p1_evaluation(
        call_id=call_id,
        record=record,
        events=events,
        dialogue_segments=dialogue_segments,
    )


def _build_recent_report(
    *,
    base_url: str,
    recent: int,
    timeout_seconds: float,
    get_json: GetJson,
) -> dict[str, Any]:
    records_response = get_json(
        f"{base_url}/ai-call/records?entryType=sip_outbound&pageSize={recent}",
        timeout_seconds,
    )
    reports: list[dict[str, Any]] = []
    failed_calls: list[dict[str, str]] = []
    for row in _record_rows(records_response):
        call_id = str(row.get("callId") or "")
        if not call_id:
            continue
        try:
            reports.append(
                _build_single_report(
                    base_url=base_url,
                    call_id=call_id,
                    timeout_seconds=timeout_seconds,
                    get_json=get_json,
                )
            )
        except Exception as exc:
            failed_calls.append({"callId": call_id, "error": str(exc)})
    report = build_p1_evaluation_suite(reports, failed_calls)
    report["mode"] = "recent"
    report["requested"] = recent
    report["sourceTotal"] = records_response.get("total")
    return report


def _record_for_call(
    base_url: str,
    call_id: str,
    timeout_seconds: float,
    get_json: GetJson,
) -> dict[str, Any]:
    detail = _unwrap_data(get_json(f"{base_url}/ai-call/records/{call_id}", timeout_seconds))
    record = detail.get("record") if isinstance(detail, dict) else None
    if not isinstance(record, dict):
        raise RuntimeError("record detail response missing data.record")
    return record


def _events_for_call(
    base_url: str,
    call_id: str,
    timeout_seconds: float,
    get_json: GetJson,
) -> list[dict[str, Any]]:
    response = _unwrap_data(
        get_json(f"{base_url}/ai-call/records/{call_id}/events?limit=1000", timeout_seconds)
    )
    rows = response.get("rows") if isinstance(response, dict) else None
    if not isinstance(rows, list):
        raise RuntimeError("event response missing data.rows")
    return [row for row in rows if isinstance(row, dict)]


def _dialogue_segments_for_call(
    base_url: str,
    call_id: str,
    timeout_seconds: float,
    get_json: GetJson,
) -> list[dict[str, Any]]:
    response = _unwrap_data(
        get_json(
            f"{base_url}/ai-call/records/{call_id}/dialogue-segments?limit=1000",
            timeout_seconds,
        )
    )
    rows = response.get("rows") if isinstance(response, dict) else None
    if not isinstance(rows, list):
        raise RuntimeError("dialogue segment response missing data.rows")
    return [row for row in rows if isinstance(row, dict)]


def _record_rows(response: dict[str, Any]) -> list[dict[str, Any]]:
    rows = response.get("rows")
    if rows is None and isinstance(response.get("data"), dict):
        rows = response["data"].get("rows")
    if not isinstance(rows, list):
        raise RuntimeError("record list response missing rows")
    return [row for row in rows if isinstance(row, dict)]


def _get_json(url: str, timeout_seconds: float) -> dict[str, Any]:
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(raw_body) from exc


def _unwrap_data(response: dict[str, Any]) -> Any:
    if response.get("code") not in (None, 200):
        raise RuntimeError(json.dumps(response, ensure_ascii=False))
    return response.get("data")


def _print_text_report(report: dict[str, Any], stdout: TextIO) -> None:
    if report.get("mode") == "recent":
        _print_recent_report(report, stdout)
        return
    _print_call_report(report, stdout)


def _print_recent_report(report: dict[str, Any], stdout: TextIO) -> None:
    summary = report["summary"]
    print(
        "p1_eval "
        f"calls={summary['calls']} "
        f"windows={summary['windows']} "
        f"confirmedPreStops={summary['confirmedPreStops']} "
        f"falsePreStops={summary['falsePreStops']} "
        f"candidateOnly={summary['candidateOnly']} "
        f"preStopPending={summary['preStopPending']} "
        f"providerSpeechStarted={summary['providerSpeechStarted']} "
        f"failed={summary['failedCalls']}",
        file=stdout,
    )
    for call in report.get("calls", []):
        _print_call_summary(call, stdout)
    for failed in report.get("failedCalls", []):
        print(
            f"failed callId={failed.get('callId')} error={failed.get('error')}",
            file=stdout,
        )


def _print_call_report(report: dict[str, Any], stdout: TextIO) -> None:
    _print_call_summary(report, stdout)
    for window in report.get("windows", []):
        print(
            "window "
            f"outcome={window.get('outcome')} "
            f"severity={window.get('severity')} "
            f"responseId={window.get('responseId')} "
            f"candidateTime={window.get('candidateTime')} "
            f"preStopTime={window.get('preStopTime')} "
            f"candidateToPreStopMs={window.get('candidateToPreStopMs')} "
            f"decision={window.get('decisionEventType')} "
            f"reason={window.get('decisionReason')} "
            f"rmsDbfs={window.get('rmsDbfs')} "
            f"snrDb={window.get('snrDb')} "
            f"quality={window.get('speechQualityRejection')}",
            file=stdout,
        )


def _print_call_summary(report: dict[str, Any], stdout: TextIO) -> None:
    summary = report.get("summary") or {}
    print(
        "call "
        f"callId={report.get('callId')} "
        f"windows={len(report.get('windows') or [])} "
        f"confirmedPreStops={summary.get('confirmedPreStops', 0)} "
        f"falsePreStops={summary.get('falsePreStops', 0)} "
        f"candidateOnly={summary.get('candidateOnly', 0)} "
        f"preStopPending={summary.get('preStopPending', 0)} "
        f"providerSpeechStarted={summary.get('providerSpeechStarted', 0)}",
        file=stdout,
    )


if __name__ == "__main__":
    main()
