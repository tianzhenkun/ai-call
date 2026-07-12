from __future__ import annotations

import argparse
import contextlib
import json
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from collections import Counter
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, TextIO

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.ai_call.interrupt_vad_shadow import build_vad_shadow_report

GetJson = Callable[[str, float], dict[str, Any]]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Offline VAD shadow report for AI Call SIP barge-in P1.",
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:19011/ai-call-api/v1",
        help="AI Call API base URL, without trailing slash.",
    )
    parser.add_argument("--call-id", help="Call ID to evaluate.")
    parser.add_argument(
        "--recent",
        type=int,
        help="Evaluate the latest N sip_outbound calls instead of a single call.",
    )
    parser.add_argument(
        "--vad-windows-file",
        help=(
            "Optional JSON with windowsByCallId. When omitted, the tool runs "
            "FunASR FSMN-VAD against the customer recording track."
        ),
    )
    parser.add_argument(
        "--model",
        default="iic/speech_fsmn_vad_zh-cn-16k-common-pytorch",
        help="FunASR VAD model name.",
    )
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--max-detection-lag-ms", type=int, default=500)
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
        detector = None
        windows_by_call_id = _load_windows_file(args.vad_windows_file)
        if windows_by_call_id is None:
            detector = FsmnVadDetector(model=args.model)
        report = (
            _build_recent_report(
                base_url=base_url,
                recent=args.recent,
                timeout_seconds=args.timeout_seconds,
                get_json=get_json,
                windows_by_call_id=windows_by_call_id,
                detector=detector,
                max_detection_lag_ms=args.max_detection_lag_ms,
            )
            if args.recent is not None
            else _build_single_report(
                base_url=base_url,
                call_id=str(args.call_id),
                timeout_seconds=args.timeout_seconds,
                get_json=get_json,
                windows_by_call_id=windows_by_call_id,
                detector=detector,
                max_detection_lag_ms=args.max_detection_lag_ms,
            )
        )
    except Exception as exc:
        print(f"vad shadow failed: {exc!s}", file=stderr)
        return 1

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2), file=stdout)
    else:
        _print_text_report(report, stdout)
    return 0


def main(argv: Sequence[str] | None = None) -> None:
    raise SystemExit(run(argv))


def _build_recent_report(
    *,
    base_url: str,
    recent: int,
    timeout_seconds: float,
    get_json: GetJson,
    windows_by_call_id: dict[str, list[dict[str, Any]]] | None,
    detector: FsmnVadDetector | None,
    max_detection_lag_ms: int,
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
                    windows_by_call_id=windows_by_call_id,
                    detector=detector,
                    max_detection_lag_ms=max_detection_lag_ms,
                )
            )
        except Exception as exc:
            failed_calls.append({"callId": call_id, "error": str(exc)})
    return _build_suite_report(reports, failed_calls, requested=recent, records_response=records_response)


def _build_single_report(
    *,
    base_url: str,
    call_id: str,
    timeout_seconds: float,
    get_json: GetJson,
    windows_by_call_id: dict[str, list[dict[str, Any]]] | None,
    detector: FsmnVadDetector | None,
    max_detection_lag_ms: int,
) -> dict[str, Any]:
    record = _record_for_call(base_url, call_id, timeout_seconds, get_json)
    events = _events_for_call(base_url, call_id, timeout_seconds, get_json)
    dialogue_segments = _dialogue_segments_for_call(base_url, call_id, timeout_seconds, get_json)
    recording = _recording_for_call(base_url, call_id, timeout_seconds, get_json)
    vad_windows = (
        windows_by_call_id.get(call_id, []) if windows_by_call_id is not None else None
    )
    if vad_windows is None:
        if detector is None:
            raise RuntimeError("missing VAD detector")
        customer_track = _customer_track(recording)
        play_url = customer_track.get("playUrl")
        if not play_url:
            raise RuntimeError("customer recording track missing playUrl")
        vad_windows = detector.detect(call_id=call_id, play_url=str(play_url))
    report = build_vad_shadow_report(
        call_id=call_id,
        record=record,
        recording=recording,
        events=events,
        dialogue_segments=dialogue_segments,
        vad_windows=vad_windows,
        max_detection_lag_ms=max_detection_lag_ms,
    )
    report["detector"] = {
        "name": "precomputed" if windows_by_call_id is not None else "funasr_fsmn_vad",
        "model": None if windows_by_call_id is not None else detector.model if detector else None,
    }
    return report


class FsmnVadDetector:
    def __init__(self, *, model: str) -> None:
        self.model = model
        try:
            with contextlib.redirect_stdout(sys.stderr):
                from funasr import AutoModel  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "FunASR is not installed. Run with: "
                "uv run --with funasr --with modelscope --with soundfile "
                "--with torch --with torchaudio python tools/ai_call_vad_shadow.py ..."
            ) from exc
        with contextlib.redirect_stdout(sys.stderr):
            self._auto_model = AutoModel(model=model, disable_update=True)

    def detect(self, *, call_id: str, play_url: str) -> list[dict[str, Any]]:
        with tempfile.TemporaryDirectory(prefix="ai-call-vad-shadow-") as tmp_dir:
            audio_path = Path(tmp_dir) / f"{call_id}-customer.ogg"
            wav_path = Path(tmp_dir) / f"{call_id}-customer.wav"
            _download_file(play_url, audio_path)
            _convert_to_wav(audio_path, wav_path)
            with contextlib.redirect_stdout(sys.stderr):
                result = self._auto_model.generate(input=str(wav_path))
        return _extract_fsmn_windows(result)


def _extract_fsmn_windows(result: Any) -> list[dict[str, Any]]:
    items = result if isinstance(result, list) else [result]
    windows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        value = item.get("value") or item.get("text") or []
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                continue
        if not isinstance(value, list):
            continue
        for segment in value:
            if (
                isinstance(segment, (list, tuple))
                and len(segment) >= 2
                and segment[0] is not None
                and segment[1] is not None
            ):
                windows.append({"startMs": int(segment[0]), "endMs": int(segment[1])})
    return windows


def _download_file(url: str, target: Path) -> None:
    request = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(request, timeout=60) as response:
        target.write_bytes(response.read())


def _convert_to_wav(source: Path, target: Path) -> None:
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            str(source),
            "-ac",
            "1",
            "-ar",
            "16000",
            str(target),
        ],
        check=True,
    )


def _load_windows_file(path: str | None) -> dict[str, list[dict[str, Any]]] | None:
    if path is None:
        return None
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("windowsByCallId"), dict):
        return {
            str(call_id): [window for window in windows if isinstance(window, dict)]
            for call_id, windows in payload["windowsByCallId"].items()
            if isinstance(windows, list)
        }
    raise RuntimeError("VAD windows file must contain windowsByCallId")


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


def _recording_for_call(
    base_url: str,
    call_id: str,
    timeout_seconds: float,
    get_json: GetJson,
) -> dict[str, Any]:
    response = _unwrap_data(
        get_json(f"{base_url}/ai-call/records/{call_id}/recording", timeout_seconds)
    )
    if not isinstance(response, dict):
        raise RuntimeError("recording response missing data")
    return response


def _record_rows(response: dict[str, Any]) -> list[dict[str, Any]]:
    rows = response.get("rows")
    if rows is None and isinstance(response.get("data"), dict):
        rows = response["data"].get("rows")
    if not isinstance(rows, list):
        raise RuntimeError("record list response missing rows")
    return [row for row in rows if isinstance(row, dict)]


def _customer_track(recording: dict[str, Any]) -> dict[str, Any]:
    tracks = recording.get("tracks")
    if not isinstance(tracks, list):
        return {}
    for track in tracks:
        if isinstance(track, dict) and track.get("trackRole") == "customer":
            return track
    return {}


def _build_suite_report(
    reports: list[dict[str, Any]],
    failed_calls: list[dict[str, str]],
    *,
    requested: int,
    records_response: dict[str, Any],
) -> dict[str, Any]:
    classifications: Counter[str] = Counter()
    detector_names = sorted(
        {
            str(detector)
            for report in reports
            for detector in (report.get("realtimeShadowSpeechByDetector") or {})
        }
    )
    shadow_by_detector: dict[str, dict[str, int]] = {
        detector: {
            "segments": 0,
            "detected": 0,
            "missed": 0,
            "slow": 0,
            "withinMaxLag": 0,
        }
        for detector in detector_names
    }
    summary = {
        "calls": len(reports),
        "vadWindows": 0,
        "offlineSegments": 0,
        "offlineDetected": 0,
        "offlineMissed": 0,
        "offlineSlow": 0,
        "shadowSegments": 0,
        "shadowDetected": 0,
        "shadowMissed": 0,
        "shadowSlow": 0,
        "unexplainedWindows": 0,
        "failedCalls": len(failed_calls),
    }
    for report in reports:
        report_summary = report.get("summary") or {}
        offline = report.get("offlineSpeech") or {}
        realtime_shadow = report.get("realtimeShadowSpeech") or {}
        classifications.update(report_summary.get("classifications") or {})
        summary["vadWindows"] += int(report_summary.get("vadWindows") or 0)
        summary["offlineSegments"] += int(offline.get("segments") or 0)
        summary["offlineDetected"] += int(offline.get("detected") or 0)
        summary["offlineMissed"] += int(offline.get("missed") or 0)
        summary["offlineSlow"] += int(offline.get("slow") or 0)
        summary["shadowSegments"] += int(realtime_shadow.get("segments") or 0)
        summary["shadowDetected"] += int(realtime_shadow.get("detected") or 0)
        summary["shadowMissed"] += int(realtime_shadow.get("missed") or 0)
        summary["shadowSlow"] += int(realtime_shadow.get("slow") or 0)
        summary["unexplainedWindows"] += int(report_summary.get("unexplainedWindows") or 0)
        by_detector = report.get("realtimeShadowSpeechByDetector") or {}
        for detector in detector_names:
            detector_report = by_detector.get(detector) or {
                "segments": offline.get("segments"),
                "detected": 0,
                "missed": offline.get("segments"),
                "slow": 0,
                "withinMaxLag": 0,
            }
            bucket = shadow_by_detector[detector]
            bucket["segments"] += int(detector_report.get("segments") or 0)
            bucket["detected"] += int(detector_report.get("detected") or 0)
            bucket["missed"] += int(detector_report.get("missed") or 0)
            bucket["slow"] += int(detector_report.get("slow") or 0)
            bucket["withinMaxLag"] += int(detector_report.get("withinMaxLag") or 0)
    summary["classifications"] = dict(classifications)
    summary["shadowByDetector"] = shadow_by_detector
    return {
        "mode": "vad_shadow_suite",
        "requested": requested,
        "sourceTotal": records_response.get("total"),
        "summary": summary,
        "calls": reports,
        "failedCalls": failed_calls,
    }


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
    if report.get("mode") == "vad_shadow_suite":
        summary = report["summary"]
        print(
            "vad_shadow "
            f"calls={summary['calls']} "
            f"vadWindows={summary['vadWindows']} "
            f"offlineSegments={summary['offlineSegments']} "
            f"offlineDetected={summary['offlineDetected']} "
            f"offlineMissed={summary['offlineMissed']} "
            f"offlineSlow={summary['offlineSlow']} "
            f"shadowDetected={summary['shadowDetected']} "
            f"shadowMissed={summary['shadowMissed']} "
            f"shadowSlow={summary['shadowSlow']} "
            f"unexplained={summary['unexplainedWindows']} "
            f"failed={summary['failedCalls']}",
            file=stdout,
        )
        for call in report.get("calls", []):
            _print_call_summary(call, stdout)
        for failed in report.get("failedCalls", []):
            print(f"failed callId={failed.get('callId')} error={failed.get('error')}", file=stdout)
        return
    _print_call_summary(report, stdout)
    for window in report.get("vadWindows", []):
        print(
            "window "
            f"classification={window.get('classification')} "
            f"startMs={window.get('startMs')} "
            f"endMs={window.get('endMs')} "
            f"startedAt={window.get('startedAtRaw')}",
            file=stdout,
        )


def _print_call_summary(report: dict[str, Any], stdout: TextIO) -> None:
    summary = report.get("summary") or {}
    offline = report.get("offlineSpeech") or {}
    realtime_shadow = report.get("realtimeShadowSpeech") or {}
    print(
        "call "
        f"callId={report.get('callId')} "
        f"vadWindows={summary.get('vadWindows', 0)} "
        f"offlineSegments={offline.get('segments', 0)} "
        f"offlineDetected={offline.get('detected', 0)} "
        f"offlineMissed={offline.get('missed', 0)} "
        f"offlineSlow={offline.get('slow', 0)} "
        f"shadowDetected={realtime_shadow.get('detected', 0)} "
        f"shadowMissed={realtime_shadow.get('missed', 0)} "
        f"shadowSlow={realtime_shadow.get('slow', 0)} "
        f"unexplained={summary.get('unexplainedWindows', 0)}",
        file=stdout,
    )


if __name__ == "__main__":
    main()
