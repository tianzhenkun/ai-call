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

from app.services.ai_call.timeline_audit import (
    build_timeline_audit,
    build_timeline_audit_suite,
)

GetJson = Callable[[str, float], dict[str, Any]]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Timeline audit for AI Call no-barge and stale-audio diagnostics.",
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:19012",
        help="AI Call API base URL, without trailing slash.",
    )
    parser.add_argument("--call-id", help="Call ID to audit.")
    parser.add_argument(
        "--recent",
        type=int,
        help="Audit the latest N calls instead of a single call.",
    )
    parser.add_argument(
        "--entry-type",
        default="web",
        help="Record entryType used with --recent.",
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
                entry_type=args.entry_type,
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
    return build_timeline_audit(
        call_id=call_id,
        record=record,
        events=events,
        dialogue_segments=dialogue_segments,
    )


def _build_recent_report(
    *,
    base_url: str,
    recent: int,
    entry_type: str,
    timeout_seconds: float,
    get_json: GetJson,
) -> dict[str, Any]:
    records_response = get_json(
        f"{base_url}/ai-call/records?entryType={entry_type}&pageSize={recent}",
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
    report = build_timeline_audit_suite(reports, failed_calls)
    report["mode"] = "recent"
    report["requested"] = recent
    report["entryType"] = entry_type
    report["sourceTotal"] = _source_total(records_response)
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


def _source_total(response: dict[str, Any]) -> Any:
    if "total" in response:
        return response.get("total")
    data = response.get("data")
    if isinstance(data, dict):
        return data.get("total")
    return None


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
        "timeline_audit "
        f"calls={summary['calls']} "
        f"passed={summary['passed']} "
        f"failed={summary['failed']} "
        f"issues={summary['issues']} "
        f"high={summary['highSeverity']} "
        f"covered={summary['coveredOverlaps']} "
        f"fetchFailed={summary['failedCalls']}",
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
    _print_call_summary(report, stdout, prefix="timeline_audit")
    for issue in report.get("issues", []):
        print(
            "issue "
            f"type={issue.get('type')} "
            f"severity={issue.get('severity')} "
            f"responseId={issue.get('responseId')} "
            f"eventTime={issue.get('eventTime')} "
            f"reason={issue.get('reason')} "
            f"deltaMsFromSpeechStart={issue.get('deltaMsFromSpeechStart')} "
            f"customerStopToFirstAudioMs={issue.get('customerStopToFirstAudioMs')} "
            f"dropCount={issue.get('dropCount')}",
            file=stdout,
        )


def _print_call_summary(
    report: dict[str, Any],
    stdout: TextIO,
    *,
    prefix: str = "call",
) -> None:
    summary = report.get("summary") or {}
    print(
        f"{prefix} "
        f"callId={report.get('callId')} "
        f"passed={str(report.get('passed')).lower()} "
        f"issues={summary.get('issueCount', 0)} "
        f"high={summary.get('highSeverityCount', 0)} "
        f"covered={summary.get('coveredOverlapCount', 0)} "
        f"staleDrops={summary.get('staleAudioDropCount', 0)}",
        file=stdout,
    )


if __name__ == "__main__":
    main()
