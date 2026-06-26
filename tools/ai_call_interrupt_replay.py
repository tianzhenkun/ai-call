from __future__ import annotations

import argparse
import json
import subprocess
import sys
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, TextIO

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.ai_call.interrupt_offline_analysis import build_offline_interrupt_report

GetJson = Callable[[str, float], dict[str, Any]]
DecodeAudio = Callable[[str, int, float], bytes]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Offline replay report for AI Call interrupt diagnostics.",
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:19012",
        help="AI Call API base URL, without trailing slash.",
    )
    parser.add_argument("--call-id", help="Call ID to replay.")
    parser.add_argument(
        "--recent",
        type=int,
        help="Analyze the latest N sip_outbound calls instead of a single call.",
    )
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--sample-rate", type=int, default=16_000)
    parser.add_argument("--window-ms", type=int, default=100)
    parser.add_argument("--min-rms-dbfs", type=float, default=-45.0)
    parser.add_argument("--min-snr-db", type=float, default=6.0)
    parser.add_argument("--min-segment-ms", type=int, default=120)
    parser.add_argument("--json", action="store_true", help="Print raw JSON report.")
    return parser


def run(
    argv: Sequence[str] | None = None,
    *,
    get_json: GetJson | None = None,
    decode_audio: DecodeAudio | None = None,
    stdout: TextIO = sys.stdout,
    stderr: TextIO = sys.stderr,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    get_json = get_json or _get_json
    decode_audio = decode_audio or _decode_audio
    base_url = args.base_url.rstrip("/")

    if bool(args.call_id) == bool(args.recent):
        parser.error("exactly one of --call-id or --recent is required")
    if args.recent is not None and args.recent < 1:
        parser.error("--recent must be greater than 0")

    if args.recent is not None:
        try:
            report = _build_recent_report(
                base_url=base_url,
                recent=args.recent,
                args=args,
                get_json=get_json,
                decode_audio=decode_audio,
            )
        except Exception as exc:
            print(f"fetch failed: {exc!s}", file=stderr)
            return 1
        if args.json:
            print(json.dumps(report, ensure_ascii=False, indent=2), file=stdout)
        else:
            _print_recent_report(report, stdout)
        return 0

    try:
        report = _build_single_report(
            base_url=base_url,
            call_id=args.call_id,
            args=args,
            get_json=get_json,
            decode_audio=decode_audio,
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
    args: argparse.Namespace,
    get_json: GetJson,
    decode_audio: DecodeAudio,
) -> dict[str, Any]:
    detail = _unwrap_data(get_json(f"{base_url}/ai-call/records/{call_id}", args.timeout_seconds))
    events = _unwrap_data(
        get_json(
            f"{base_url}/ai-call/records/{call_id}/events?limit=1000",
            args.timeout_seconds,
        )
    )
    recording = _unwrap_data(
        get_json(f"{base_url}/ai-call/records/{call_id}/recording", args.timeout_seconds)
    )

    record = detail.get("record") if isinstance(detail, dict) else None
    if not isinstance(record, dict):
        raise RuntimeError("record detail response missing data.record")
    event_rows = events.get("rows") if isinstance(events, dict) else None
    if not isinstance(event_rows, list):
        raise RuntimeError("event response missing data.rows")
    if recording is not None and not isinstance(recording, dict):
        raise RuntimeError("recording response data must be an object or null")

    pcm_by_role: dict[str, bytes] = {}
    decode_errors: dict[str, str] = {}
    for role, play_url in _track_urls_by_role(recording or {}).items():
        try:
            pcm_by_role[role] = decode_audio(play_url, args.sample_rate, args.timeout_seconds)
        except Exception as exc:
            decode_errors[role] = str(exc)

    report = build_offline_interrupt_report(
        call_id=call_id,
        record=record,
        events=event_rows,
        recording=recording,
        pcm_by_role=pcm_by_role,
        sample_rate=args.sample_rate,
        window_ms=args.window_ms,
        min_rms_dbfs=args.min_rms_dbfs,
        min_snr_db=args.min_snr_db,
        min_segment_ms=args.min_segment_ms,
    )
    if decode_errors:
        report["audioAnalysis"]["decodeErrors"] = decode_errors
    return report


def _build_recent_report(
    *,
    base_url: str,
    recent: int,
    args: argparse.Namespace,
    get_json: GetJson,
    decode_audio: DecodeAudio,
) -> dict[str, Any]:
    records_response = get_json(
        f"{base_url}/ai-call/records?entryType=sip_outbound&pageSize={recent}",
        args.timeout_seconds,
    )
    rows = _record_rows(records_response)
    calls: list[dict[str, Any]] = []
    failed_calls: list[dict[str, str]] = []
    verdict_counts = _empty_verdict_counts()
    window_count = 0
    for row in rows:
        call_id = str(row.get("callId") or "")
        if not call_id:
            continue
        try:
            report = _build_single_report(
                base_url=base_url,
                call_id=call_id,
                args=args,
                get_json=get_json,
                decode_audio=decode_audio,
            )
        except Exception as exc:
            failed_calls.append({"callId": call_id, "error": str(exc)})
            continue
        summary = _call_summary(report)
        calls.append(summary)
        window_count += summary["windowCount"]
        for verdict, count in summary["verdictCounts"].items():
            verdict_counts[verdict] += count
    return {
        "mode": "recent",
        "requested": recent,
        "totalCalls": len(calls),
        "sourceTotal": records_response.get("total"),
        "windowCount": window_count,
        "verdictCounts": verdict_counts,
        "calls": calls,
        "failedCalls": failed_calls,
    }


def _record_rows(response: dict[str, Any]) -> list[dict[str, Any]]:
    rows = response.get("rows")
    if rows is None and isinstance(response.get("data"), dict):
        rows = response["data"].get("rows")
    if not isinstance(rows, list):
        raise RuntimeError("record list response missing rows")
    return [row for row in rows if isinstance(row, dict)]


def _call_summary(report: dict[str, Any]) -> dict[str, Any]:
    windows = report.get("possibleInterruptWindows") or []
    verdict_counts = _empty_verdict_counts()
    compact_windows: list[dict[str, Any]] = []
    for window in windows:
        alignment = window.get("eventAlignment") or {}
        verdict = str(alignment.get("verdict") or "unknown")
        if verdict not in verdict_counts:
            verdict_counts[verdict] = 0
        verdict_counts[verdict] += 1
        compact_windows.append({
            "startMs": window.get("startMs"),
            "endMs": window.get("endMs"),
            "durationMs": window.get("durationMs"),
            "reason": window.get("reason"),
            "verdict": verdict,
            "candidateCount": len(alignment.get("candidateEvents") or []),
            "providerSpeechStartedCount": len(
                alignment.get("providerSpeechStartedEvents") or []
            ),
            "confirmedCount": len(alignment.get("confirmedEvents") or []),
        })
    return {
        "callId": report.get("callId"),
        "record": report.get("record") or {},
        "eventSummary": {
            key: value
            for key, value in (report.get("eventSummary") or {}).items()
            if key != "keyEvents"
        },
        "windowCount": len(windows),
        "verdictCounts": verdict_counts,
        "windows": compact_windows,
    }


def _empty_verdict_counts() -> dict[str, int]:
    return {
        "confirmed": 0,
        "candidate_not_confirmed": 0,
        "missed_candidate": 0,
        "likely_noise": 0,
    }


def _get_json(url: str, timeout_seconds: float) -> dict[str, Any]:
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(raw_body) from exc


def _decode_audio(url: str, sample_rate: int, timeout_seconds: float) -> bytes:
    result = subprocess.run(
        [
            "ffmpeg",
            "-v",
            "error",
            "-i",
            url,
            "-ac",
            "1",
            "-ar",
            str(sample_rate),
            "-f",
            "s16le",
            "-",
        ],
        capture_output=True,
        timeout=timeout_seconds,
    )
    if result.returncode != 0:
        message = result.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(message or f"ffmpeg exited with {result.returncode}")
    return result.stdout


def _unwrap_data(response: dict[str, Any]) -> Any:
    if response.get("code") not in (None, 200):
        raise RuntimeError(json.dumps(response, ensure_ascii=False))
    return response.get("data")


def _track_urls_by_role(recording: dict[str, Any]) -> dict[str, str]:
    urls: dict[str, str] = {}
    for track in recording.get("tracks", []):
        if not isinstance(track, dict):
            continue
        role = track.get("trackRole")
        play_url = track.get("playUrl")
        if role in {"customer", "ai"} and isinstance(play_url, str) and play_url:
            urls[str(role)] = play_url
    return urls


def _print_text_report(report: dict[str, Any], stdout: TextIO) -> None:
    record = report["record"]
    summary = report["eventSummary"]
    audio = report["audioAnalysis"]
    print(f"callId={report['callId']}", file=stdout)
    print(
        "record "
        f"status={record.get('status')} "
        f"endReason={record.get('endReason')} "
        f"durationMs={record.get('durationMs')}",
        file=stdout,
    )
    print(
        "events "
        f"interruptCandidates={summary.get('interruptCandidateCount')} "
        f"interruptConfirmed={summary.get('interruptConfirmedCount')} "
        f"sipHangup={summary.get('sipHangupCount')}",
        file=stdout,
    )
    print(
        "audio "
        f"customerSegments={len(audio.get('customerSegments', []))} "
        f"aiActiveSegments={len(audio.get('aiActiveSegments', []))} "
        f"possibleInterruptWindows={len(report.get('possibleInterruptWindows', []))}",
        file=stdout,
    )
    for window in report.get("possibleInterruptWindows", []):
        alignment = window.get("eventAlignment") or {}
        print(
            "window "
            f"startMs={window['startMs']} "
            f"endMs={window['endMs']} "
            f"durationMs={window['durationMs']} "
            f"reason={window['reason']} "
            f"verdict={alignment.get('verdict')} "
            f"candidates={len(alignment.get('candidateEvents') or [])} "
            f"providerSpeech={len(alignment.get('providerSpeechStartedEvents') or [])} "
            f"confirmed={len(alignment.get('confirmedEvents') or [])}",
            file=stdout,
        )


def _print_recent_report(report: dict[str, Any], stdout: TextIO) -> None:
    verdict_counts = report["verdictCounts"]
    print(
        "recent "
        f"calls={report['totalCalls']} "
        f"windows={report['windowCount']} "
        f"confirmed={verdict_counts.get('confirmed', 0)} "
        f"likely_noise={verdict_counts.get('likely_noise', 0)} "
        f"candidate_not_confirmed={verdict_counts.get('candidate_not_confirmed', 0)} "
        f"missed_candidate={verdict_counts.get('missed_candidate', 0)} "
        f"failed={len(report.get('failedCalls') or [])}",
        file=stdout,
    )
    for call in report.get("calls", []):
        record = call.get("record") or {}
        counts = call.get("verdictCounts") or {}
        print(
            "call "
            f"callId={call.get('callId')} "
            f"endReason={record.get('endReason')} "
            f"windows={call.get('windowCount')} "
            f"confirmed={counts.get('confirmed', 0)} "
            f"candidate_not_confirmed={counts.get('candidate_not_confirmed', 0)} "
            f"missed_candidate={counts.get('missed_candidate', 0)} "
            f"likely_noise={counts.get('likely_noise', 0)}",
            file=stdout,
        )
    for failed in report.get("failedCalls", []):
        print(
            f"failed callId={failed.get('callId')} error={failed.get('error')}",
            file=stdout,
        )


if __name__ == "__main__":
    main()
