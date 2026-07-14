from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol, TextIO

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.ai_call.sip_barge_in import (
    SipBargeInConfig,
    VoiceActivityDetectorProtocol,
    WebRtcVadAdapter,
)
from app.services.ai_call.sip_barge_in_replay import (
    SipBargeInProviderReplayReport,
    SipBargeInReplayFrame,
    SipVadReplayProvider,
    SpeechWindow,
    WindowVoiceActivityDetector,
    pcm16_mono_to_replay_frames,
    replay_sip_barge_in_vad_providers,
)

GetJson = Callable[[str, float], dict[str, Any]]
DecodeAudio = Callable[[str, int, float], bytes]
VadFactory = Callable[[], VoiceActivityDetectorProtocol]

LIVE_TIMELINE_AI_TAIL_MS = 600


class FsmnDetectorProtocol(Protocol):
    def detect(self, *, call_id: str, play_url: str) -> list[dict[str, Any]]: ...


FsmnDetectorFactory = Callable[[str], FsmnDetectorProtocol]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare WebRTC-main and FSMN-main SIP barge-in replay.",
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:19011/ai-call-api/v1",
        help="AI Call API base URL, without trailing slash.",
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--call-id", help="Call ID to replay.")
    source.add_argument(
        "--benchmark-file",
        help="Local JSON with labeled speech and non-speech wav samples.",
    )
    parser.add_argument(
        "--diagnose-sample-id",
        help="With --benchmark-file, emit per-frame onset diagnostics for one sample.",
    )
    parser.add_argument(
        "--diagnose-from-ms",
        type=int,
        help="Optional diagnosis window start. Defaults to speechStartMs or 0.",
    )
    parser.add_argument(
        "--diagnose-to-ms",
        type=int,
        help="Optional diagnosis window end. Defaults to first candidate plus 400ms.",
    )
    parser.add_argument(
        "--fsmn-report-file",
        help="Optional JSON from tools/ai_call_vad_shadow.py containing vadWindows.",
    )
    parser.add_argument(
        "--fsmn-model",
        default="iic/speech_fsmn_vad_zh-cn-16k-common-pytorch",
        help="FunASR FSMN-VAD model used when --fsmn-report-file is omitted.",
    )
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--sample-rate", type=int, default=16_000)
    parser.add_argument("--frame-ms", type=int, default=20)
    parser.add_argument("--webrtc-mode", type=int, default=2)
    parser.add_argument(
        "--live-timeline",
        action="store_true",
        help=(
            "With --call-id, replay only during model response windows reconstructed "
            "from live events."
        ),
    )
    parser.add_argument("--json", action="store_true", help="Print raw JSON report.")
    return parser


def run(
    argv: Sequence[str] | None = None,
    *,
    get_json: GetJson | None = None,
    decode_audio: DecodeAudio | None = None,
    webrtc_vad_factory: VadFactory | None = None,
    fsmn_detector_factory: FsmnDetectorFactory | None = None,
    stdout: TextIO = sys.stdout,
    stderr: TextIO = sys.stderr,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    get_json = get_json or _get_json
    decode_audio = decode_audio or _decode_audio
    base_url = args.base_url.rstrip("/")

    try:
        if args.benchmark_file:
            if args.live_timeline:
                raise RuntimeError("--live-timeline requires --call-id")
            report = (
                _build_benchmark_onset_diagnosis(
                    benchmark_path=Path(args.benchmark_file),
                    args=args,
                    decode_audio=decode_audio,
                    webrtc_vad_factory=webrtc_vad_factory,
                    fsmn_detector_factory=fsmn_detector_factory,
                )
                if args.diagnose_sample_id
                else _build_benchmark_report(
                    benchmark_path=Path(args.benchmark_file),
                    args=args,
                    decode_audio=decode_audio,
                    webrtc_vad_factory=webrtc_vad_factory,
                    fsmn_detector_factory=fsmn_detector_factory,
                )
            )
        else:
            if args.diagnose_sample_id:
                raise RuntimeError("--diagnose-sample-id requires --benchmark-file")
            report = _build_single_report(
                base_url=base_url,
                call_id=str(args.call_id),
                args=args,
                get_json=get_json,
                decode_audio=decode_audio,
                webrtc_vad_factory=webrtc_vad_factory,
                fsmn_detector_factory=fsmn_detector_factory,
            )
    except Exception as exc:
        print(f"provider compare failed: {exc!s}", file=stderr)
        return 1

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2), file=stdout)
    else:
        _print_text_report(report, stdout)
    gates = report.get("benchmarkGates")
    if isinstance(gates, dict) and not gates.get("passed", True):
        return 2
    return 0


def main(argv: Sequence[str] | None = None) -> None:
    raise SystemExit(run(argv))


def _load_benchmark_samples(benchmark_path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    payload = json.loads(benchmark_path.read_text(encoding="utf-8"))
    raw_samples = payload.get("samples") if isinstance(payload, dict) else None
    if not isinstance(raw_samples, list) or not raw_samples:
        raise RuntimeError("benchmark file samples must be a non-empty list")
    samples: list[dict[str, Any]] = []
    for index, raw_sample in enumerate(raw_samples):
        if not isinstance(raw_sample, dict):
            raise RuntimeError(f"benchmark sample[{index}] must be an object")
        sample_id = str(raw_sample.get("id") or "").strip()
        label = str(raw_sample.get("label") or "").strip()
        raw_wav_path = str(raw_sample.get("wavPath") or "").strip()
        if not sample_id or label not in {"speech", "non_speech"} or not raw_wav_path:
            raise RuntimeError(f"invalid benchmark sample[{index}]")
        samples.append(raw_sample)
    return payload, samples


def _build_benchmark_report(
    *,
    benchmark_path: Path,
    args: argparse.Namespace,
    decode_audio: DecodeAudio,
    webrtc_vad_factory: VadFactory | None,
    fsmn_detector_factory: FsmnDetectorFactory | None,
) -> dict[str, Any]:
    payload, raw_samples = _load_benchmark_samples(benchmark_path)
    agreement_tolerance_ms = int(payload.get("agreementToleranceMs") or 200)
    benchmark_gates = payload.get("benchmarkGates")
    if benchmark_gates is not None and not isinstance(benchmark_gates, dict):
        raise RuntimeError("benchmarkGates must be an object")

    if fsmn_detector_factory is not None:
        fsmn_detector = fsmn_detector_factory(args.fsmn_model)
    else:
        from tools.ai_call_vad_shadow import FsmnVadDetector

        fsmn_detector = FsmnVadDetector(model=args.fsmn_model)

    sample_reports: list[dict[str, Any]] = []
    for raw_sample in raw_samples:
        sample_id = str(raw_sample.get("id") or "").strip()
        label = str(raw_sample.get("label") or "").strip()
        raw_wav_path = str(raw_sample.get("wavPath") or "").strip()
        wav_path = Path(raw_wav_path)
        if not wav_path.is_absolute():
            wav_path = benchmark_path.parent / wav_path
        if not wav_path.is_file():
            raise RuntimeError(f"benchmark wav not found: {wav_path}")

        pcm16_mono = decode_audio(str(wav_path), args.sample_rate, args.timeout_seconds)
        frames = pcm16_mono_to_replay_frames(
            pcm16_mono,
            sample_rate_hz=args.sample_rate,
            frame_duration_ms=args.frame_ms,
        )
        fsmn_windows = fsmn_detector.detect(
            call_id=sample_id,
            play_url=wav_path.resolve().as_uri(),
        )
        speech_windows = [_speech_window(window) for window in fsmn_windows]
        web_rtc_factory = webrtc_vad_factory or (
            lambda: WebRtcVadAdapter(mode=args.webrtc_mode)
        )
        replay_report = replay_sip_barge_in_vad_providers(
            call_id=sample_id,
            frames=frames,
            providers=[
                SipVadReplayProvider(name="webrtc_main", vad_factory=web_rtc_factory),
                SipVadReplayProvider(
                    name="fsmn_main",
                    vad_factory=lambda windows=speech_windows: WindowVoiceActivityDetector(
                        windows
                    ),
                ),
            ],
            config=SipBargeInConfig(),
        )
        duration_ms = len(frames) * args.frame_ms
        provider_results = {
            name: _benchmark_provider_result(
                report=provider_report,
                sample=raw_sample,
                duration_ms=duration_ms,
            )
            for name, provider_report in replay_report.provider_reports.items()
        }
        web_rtc_offsets = [
            int(event.offset_ms)
            for event in replay_report.provider_reports["webrtc_main"].candidate_events
        ]
        fsmn_offsets = [
            int(event.offset_ms)
            for event in replay_report.provider_reports["fsmn_main"].candidate_events
        ]
        provider_results["webrtc_fsmn_agreement"] = _benchmark_candidate_result(
            candidate_offsets=_agreement_offsets(
                web_rtc_offsets,
                fsmn_offsets,
                tolerance_ms=agreement_tolerance_ms,
            ),
            sample=raw_sample,
            duration_ms=duration_ms,
        )
        sample_reports.append(
            {
                "id": sample_id,
                "label": label,
                "category": raw_sample.get("category"),
                "durationMs": duration_ms,
                "speechStartMs": raw_sample.get("speechStartMs"),
                "speechEndMs": raw_sample.get("speechEndMs"),
                "maxDetectionLagMs": int(raw_sample.get("maxDetectionLagMs") or 600),
                "providers": provider_results,
            }
        )

    summary = {
        "samples": len(sample_reports),
        "speechSamples": sum(sample["label"] == "speech" for sample in sample_reports),
        "nonSpeechSamples": sum(
            sample["label"] == "non_speech" for sample in sample_reports
        ),
        "providers": {
            name: _benchmark_provider_summary(sample_reports, name)
            for name in ("webrtc_main", "fsmn_main", "webrtc_fsmn_agreement")
        },
    }
    report = {
        "mode": "p1_vad_provider_benchmark",
        "benchmarkFile": str(benchmark_path),
        "samples": sample_reports,
        "summary": summary,
    }
    if benchmark_gates is not None:
        report["benchmarkGates"] = _evaluate_benchmark_gates(summary, benchmark_gates)
    return report


def _build_benchmark_onset_diagnosis(
    *,
    benchmark_path: Path,
    args: argparse.Namespace,
    decode_audio: DecodeAudio,
    webrtc_vad_factory: VadFactory | None,
    fsmn_detector_factory: FsmnDetectorFactory | None,
) -> dict[str, Any]:
    payload, samples = _load_benchmark_samples(benchmark_path)
    sample_id = str(args.diagnose_sample_id or "")
    sample = next((row for row in samples if str(row.get("id") or "") == sample_id), None)
    if sample is None:
        raise RuntimeError(f"diagnose sample not found: {sample_id}")
    raw_wav_path = str(sample.get("wavPath") or "").strip()
    wav_path = Path(raw_wav_path)
    if not wav_path.is_absolute():
        wav_path = benchmark_path.parent / wav_path
    if not wav_path.is_file():
        raise RuntimeError(f"benchmark wav not found: {wav_path}")

    if fsmn_detector_factory is not None:
        fsmn_detector = fsmn_detector_factory(args.fsmn_model)
    else:
        from tools.ai_call_vad_shadow import FsmnVadDetector

        fsmn_detector = FsmnVadDetector(model=args.fsmn_model)

    pcm16_mono = decode_audio(str(wav_path), args.sample_rate, args.timeout_seconds)
    frames = pcm16_mono_to_replay_frames(
        pcm16_mono,
        sample_rate_hz=args.sample_rate,
        frame_duration_ms=args.frame_ms,
    )
    fsmn_windows = fsmn_detector.detect(
        call_id=sample_id,
        play_url=wav_path.resolve().as_uri(),
    )
    speech_windows = [_speech_window(window) for window in fsmn_windows]
    web_rtc_factory = webrtc_vad_factory or (
        lambda: WebRtcVadAdapter(mode=args.webrtc_mode)
    )
    replay_report = replay_sip_barge_in_vad_providers(
        call_id=sample_id,
        frames=frames,
        providers=[
            SipVadReplayProvider(name="webrtc_main", vad_factory=web_rtc_factory),
            SipVadReplayProvider(
                name="fsmn_main",
                vad_factory=lambda windows=speech_windows: WindowVoiceActivityDetector(
                    windows
                ),
            ),
        ],
        config=SipBargeInConfig(),
    )
    web_rtc_offsets = [
        int(event.offset_ms)
        for event in replay_report.provider_reports["webrtc_main"].candidate_events
    ]
    fsmn_offsets = [
        int(event.offset_ms)
        for event in replay_report.provider_reports["fsmn_main"].candidate_events
    ]
    agreement_tolerance_ms = int(payload.get("agreementToleranceMs") or 200)
    agreement_offsets = _agreement_offsets(
        web_rtc_offsets,
        fsmn_offsets,
        tolerance_ms=agreement_tolerance_ms,
    )
    duration_ms = len(frames) * args.frame_ms
    from_ms = (
        args.diagnose_from_ms
        if args.diagnose_from_ms is not None
        else int(sample.get("speechStartMs") or 0)
    )
    first_candidate_ms = min(
        web_rtc_offsets + fsmn_offsets + agreement_offsets,
        default=duration_ms,
    )
    to_ms = (
        args.diagnose_to_ms
        if args.diagnose_to_ms is not None
        else min(duration_ms, max(from_ms, first_candidate_ms) + 400)
    )
    frame_rows = _diagnosis_frame_rows(
        call_id=sample_id,
        frames=frames,
        speech_windows=speech_windows,
        web_rtc_vad_factory=web_rtc_factory,
        from_ms=from_ms,
        to_ms=to_ms,
    )
    return {
        "mode": "p1_vad_onset_diagnosis",
        "benchmarkFile": str(benchmark_path),
        "sample": {
            "id": sample_id,
            "label": sample.get("label"),
            "category": sample.get("category"),
            "durationMs": duration_ms,
            "speechStartMs": sample.get("speechStartMs"),
            "speechEndMs": sample.get("speechEndMs"),
            "maxDetectionLagMs": int(sample.get("maxDetectionLagMs") or 600),
            "wavPath": str(wav_path),
        },
        "window": {
            "fromMs": from_ms,
            "toMs": to_ms,
            "frameMs": args.frame_ms,
            "agreementToleranceMs": agreement_tolerance_ms,
        },
        "providers": {
            "webrtc_main": _diagnosis_provider_summary(
                replay_report.provider_reports["webrtc_main"],
                sample=sample,
                duration_ms=duration_ms,
            ),
            "fsmn_main": _diagnosis_provider_summary(
                replay_report.provider_reports["fsmn_main"],
                sample=sample,
                duration_ms=duration_ms,
            ),
            "webrtc_fsmn_agreement": _benchmark_candidate_result(
                candidate_offsets=agreement_offsets,
                sample=sample,
                duration_ms=duration_ms,
            ),
        },
        "reasonSpans": _diagnosis_reason_spans(frame_rows),
        "frames": frame_rows,
    }


def _diagnosis_provider_summary(
    report: SipBargeInProviderReplayReport,
    *,
    sample: dict[str, Any],
    duration_ms: int,
) -> dict[str, Any]:
    result = _benchmark_provider_result(
        report=report,
        sample=sample,
        duration_ms=duration_ms,
    )
    result["firstPreStopMs"] = _first_offset(report.pre_stop_events)
    result["lastReason"] = report.last_reason
    return result


def _diagnosis_frame_rows(
    *,
    call_id: str,
    frames: Sequence[Any],
    speech_windows: Sequence[SpeechWindow],
    web_rtc_vad_factory: VadFactory,
    from_ms: int,
    to_ms: int,
) -> list[dict[str, Any]]:
    from app.services.ai_call.sip_barge_in import SipBargeInDetector

    base_time = datetime(1970, 1, 1, tzinfo=timezone.utc)
    web_rtc_vad_for_flags = web_rtc_vad_factory()
    fsmn_vad_for_flags = WindowVoiceActivityDetector(speech_windows)
    web_rtc_detector = SipBargeInDetector(
        config=SipBargeInConfig(),
        vad=web_rtc_vad_factory(),
    )
    fsmn_detector = SipBargeInDetector(
        config=SipBargeInConfig(),
        vad=WindowVoiceActivityDetector(speech_windows),
    )
    rows: list[dict[str, Any]] = []
    for frame in frames:
        offset_ms = int(frame.offset_ms)
        now = base_time + timedelta(milliseconds=offset_ms)
        web_rtc_speech = web_rtc_vad_for_flags.is_speech(frame.frame)
        fsmn_speech = fsmn_vad_for_flags.is_speech(frame.frame)
        web_rtc_observation = web_rtc_detector.observe(
            call_id,
            frame.frame,
            now=now,
            interruptible=frame.interruptible,
        )
        fsmn_observation = fsmn_detector.observe(
            call_id,
            frame.frame,
            now=now,
            interruptible=frame.interruptible,
        )
        if offset_ms < from_ms or offset_ms > to_ms:
            continue
        rows.append(
            {
                "offsetMs": offset_ms,
                "rmsDbfs": _round_float(web_rtc_observation.rms_dbfs),
                "snrDb": _round_float(web_rtc_observation.snr_db),
                "peakDbfs": _round_float(web_rtc_observation.peak_dbfs),
                "webrtcSpeech": web_rtc_speech,
                "fsmnSpeech": fsmn_speech,
                "agreementSpeech": web_rtc_speech and fsmn_speech,
                "webrtcReason": web_rtc_observation.reason,
                "webrtcCandidateClass": web_rtc_observation.candidate_class,
                "webrtcCandidateDurationMs": web_rtc_observation.candidate_duration_ms,
                "fsmnReason": fsmn_observation.reason,
                "fsmnCandidateClass": fsmn_observation.candidate_class,
                "fsmnCandidateDurationMs": fsmn_observation.candidate_duration_ms,
            }
        )
    return rows


def _diagnosis_reason_spans(frame_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    spans: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for row in frame_rows:
        key = (
            row.get("webrtcReason"),
            row.get("fsmnReason"),
            row.get("webrtcSpeech"),
            row.get("fsmnSpeech"),
            row.get("agreementSpeech"),
        )
        if current is None or current["key"] != key:
            if current is not None:
                spans.append(_format_diagnosis_span(current))
            current = {
                "key": key,
                "startMs": row["offsetMs"],
                "endMs": row["offsetMs"],
                "frameCount": 1,
                "minRmsDbfs": row.get("rmsDbfs"),
                "maxRmsDbfs": row.get("rmsDbfs"),
                "minSnrDb": row.get("snrDb"),
                "maxSnrDb": row.get("snrDb"),
            }
            continue
        current["endMs"] = row["offsetMs"]
        current["frameCount"] += 1
        current["minRmsDbfs"] = _min_present(current["minRmsDbfs"], row.get("rmsDbfs"))
        current["maxRmsDbfs"] = _max_present(current["maxRmsDbfs"], row.get("rmsDbfs"))
        current["minSnrDb"] = _min_present(current["minSnrDb"], row.get("snrDb"))
        current["maxSnrDb"] = _max_present(current["maxSnrDb"], row.get("snrDb"))
    if current is not None:
        spans.append(_format_diagnosis_span(current))
    return spans


def _format_diagnosis_span(raw_span: dict[str, Any]) -> dict[str, Any]:
    (
        web_rtc_reason,
        fsmn_reason,
        web_rtc_speech,
        fsmn_speech,
        agreement_speech,
    ) = raw_span["key"]
    return {
        "startMs": raw_span["startMs"],
        "endMs": raw_span["endMs"],
        "frameCount": raw_span["frameCount"],
        "webrtcSpeech": web_rtc_speech,
        "fsmnSpeech": fsmn_speech,
        "agreementSpeech": agreement_speech,
        "webrtcReason": web_rtc_reason,
        "fsmnReason": fsmn_reason,
        "rmsDbfsRange": [raw_span["minRmsDbfs"], raw_span["maxRmsDbfs"]],
        "snrDbRange": [raw_span["minSnrDb"], raw_span["maxSnrDb"]],
    }


def _min_present(left: Any, right: Any) -> Any:
    if left is None:
        return right
    if right is None:
        return left
    return min(left, right)


def _max_present(left: Any, right: Any) -> Any:
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)


def _round_float(value: float | None) -> float | None:
    return round(value, 2) if value is not None else None


def _benchmark_provider_result(
    *,
    report: SipBargeInProviderReplayReport,
    sample: dict[str, Any],
    duration_ms: int,
) -> dict[str, Any]:
    candidate_offsets = [int(event.offset_ms) for event in report.candidate_events]
    return _benchmark_candidate_result(
        candidate_offsets=candidate_offsets,
        sample=sample,
        duration_ms=duration_ms,
    )


def _benchmark_candidate_result(
    *,
    candidate_offsets: list[int],
    sample: dict[str, Any],
    duration_ms: int,
) -> dict[str, Any]:
    if sample.get("label") == "non_speech":
        return {
            "candidateCount": len(candidate_offsets),
            "falsePositive": bool(candidate_offsets),
            "firstCandidateMs": candidate_offsets[0] if candidate_offsets else None,
        }

    speech_start_ms = _required_int(sample.get("speechStartMs"), "speechStartMs")
    speech_end_ms = _required_int(sample.get("speechEndMs"), "speechEndMs")
    max_lag_ms = int(sample.get("maxDetectionLagMs") or 600)
    if speech_end_ms <= speech_start_ms or speech_end_ms > duration_ms:
        raise RuntimeError(f"invalid speech window for benchmark sample {sample.get('id')}")
    matching = [
        offset
        for offset in candidate_offsets
        if speech_start_ms <= offset <= speech_end_ms + max_lag_ms
    ]
    first_candidate_ms = matching[0] if matching else None
    lag_ms = first_candidate_ms - speech_start_ms if first_candidate_ms is not None else None
    return {
        "candidateCount": len(candidate_offsets),
        "detected": first_candidate_ms is not None,
        "withinMaxLag": lag_ms is not None and lag_ms <= max_lag_ms,
        "firstCandidateMs": first_candidate_ms,
        "detectionLagMs": lag_ms,
    }


def _agreement_offsets(
    web_rtc_offsets: list[int],
    fsmn_offsets: list[int],
    *,
    tolerance_ms: int,
) -> list[int]:
    return [
        web_rtc_offset
        for web_rtc_offset in web_rtc_offsets
        if any(abs(web_rtc_offset - fsmn_offset) <= tolerance_ms for fsmn_offset in fsmn_offsets)
    ]


def _benchmark_provider_summary(
    samples: list[dict[str, Any]],
    provider_name: str,
) -> dict[str, Any]:
    speech_results = [
        sample["providers"][provider_name]
        for sample in samples
        if sample["label"] == "speech"
    ]
    non_speech_samples = [sample for sample in samples if sample["label"] == "non_speech"]
    non_speech_results = [sample["providers"][provider_name] for sample in non_speech_samples]
    detected = sum(bool(result.get("detected")) for result in speech_results)
    within_max_lag = sum(bool(result.get("withinMaxLag")) for result in speech_results)
    lags = sorted(
        int(result["detectionLagMs"])
        for result in speech_results
        if result.get("detectionLagMs") is not None
    )
    false_positive_windows = sum(
        int(result.get("candidateCount") or 0) for result in non_speech_results
    )
    non_speech_duration_ms = sum(int(sample["durationMs"]) for sample in non_speech_samples)
    return {
        "speechSamples": len(speech_results),
        "detected": detected,
        "missed": len(speech_results) - detected,
        "withinMaxLag": within_max_lag,
        "speechRecall": round(detected / len(speech_results), 4) if speech_results else None,
        "detectionLagMs": _lag_summary(lags),
        "nonSpeechSamples": len(non_speech_results),
        "falsePositiveSamples": sum(
            bool(result.get("falsePositive")) for result in non_speech_results
        ),
        "falsePositiveWindows": false_positive_windows,
        "falsePositiveWindowsPerMinute": (
            round(false_positive_windows * 60_000 / non_speech_duration_ms, 3)
            if non_speech_duration_ms
            else None
        ),
    }


def _evaluate_benchmark_gates(
    summary: dict[str, Any],
    gates: dict[str, Any],
) -> dict[str, Any]:
    failures: list[dict[str, Any]] = []
    _check_min_gate(
        failures,
        gate="min_samples",
        actual=summary.get("samples"),
        required=gates.get("minSamples"),
    )
    _check_min_gate(
        failures,
        gate="min_speech_samples",
        actual=summary.get("speechSamples"),
        required=gates.get("minSpeechSamples"),
    )
    _check_min_gate(
        failures,
        gate="min_non_speech_samples",
        actual=summary.get("nonSpeechSamples"),
        required=gates.get("minNonSpeechSamples"),
    )

    provider_gates = gates.get("providers")
    if provider_gates is not None and not isinstance(provider_gates, dict):
        raise RuntimeError("benchmarkGates.providers must be an object")
    providers = summary.get("providers") or {}
    for provider_name, provider_gate in (provider_gates or {}).items():
        if not isinstance(provider_gate, dict):
            raise RuntimeError(f"benchmarkGates.providers.{provider_name} must be an object")
        provider = providers.get(provider_name)
        if not isinstance(provider, dict):
            failures.append(
                {
                    "gate": "provider_present",
                    "provider": provider_name,
                    "required": True,
                    "actual": False,
                }
            )
            continue
        _check_min_gate(
            failures,
            gate="provider_min_detected",
            provider=provider_name,
            actual=provider.get("detected"),
            required=provider_gate.get("minDetected"),
        )
        _check_min_gate(
            failures,
            gate="provider_min_within_max_lag",
            provider=provider_name,
            actual=provider.get("withinMaxLag"),
            required=provider_gate.get("minWithinMaxLag"),
        )
        _check_max_gate(
            failures,
            gate="provider_max_lag_p90_ms",
            provider=provider_name,
            actual=(provider.get("detectionLagMs") or {}).get("p90"),
            required=provider_gate.get("maxDetectionLagP90Ms"),
        )
        _check_max_gate(
            failures,
            gate="provider_max_false_positive_windows",
            provider=provider_name,
            actual=provider.get("falsePositiveWindows"),
            required=provider_gate.get("maxFalsePositiveWindows"),
        )
    return {
        "passed": not failures,
        "failureCount": len(failures),
        "failures": failures,
    }


def _check_min_gate(
    failures: list[dict[str, Any]],
    *,
    gate: str,
    actual: Any,
    required: Any,
    provider: str | None = None,
) -> None:
    if required is None:
        return
    if isinstance(required, bool) or not isinstance(required, (int, float)):
        raise RuntimeError(f"benchmark gate {gate} requires a number")
    if isinstance(actual, bool) or not isinstance(actual, (int, float)) or actual < required:
        failure = {"gate": gate, "required": required, "actual": actual}
        if provider is not None:
            failure["provider"] = provider
        failures.append(failure)


def _check_max_gate(
    failures: list[dict[str, Any]],
    *,
    gate: str,
    actual: Any,
    required: Any,
    provider: str | None = None,
) -> None:
    if required is None:
        return
    if isinstance(required, bool) or not isinstance(required, (int, float)):
        raise RuntimeError(f"benchmark gate {gate} requires a number")
    if isinstance(actual, bool) or not isinstance(actual, (int, float)) or actual > required:
        failure = {"gate": gate, "required": required, "actual": actual}
        if provider is not None:
            failure["provider"] = provider
        failures.append(failure)


def _required_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f"benchmark speech sample requires integer {field}")
    return value


def _lag_summary(lags: list[int]) -> dict[str, int | None]:
    if not lags:
        return {"p50": None, "p90": None, "max": None}
    return {
        "p50": _percentile(lags, 0.5),
        "p90": _percentile(lags, 0.9),
        "max": lags[-1],
    }


def _percentile(values: list[int], percentile: float) -> int:
    index = max(0, math.ceil(len(values) * percentile) - 1)
    return values[index]


def _build_single_report(
    *,
    base_url: str,
    call_id: str,
    args: argparse.Namespace,
    get_json: GetJson,
    decode_audio: DecodeAudio,
    webrtc_vad_factory: VadFactory | None,
    fsmn_detector_factory: FsmnDetectorFactory | None,
) -> dict[str, Any]:
    recording = _recording_for_call(base_url, call_id, args.timeout_seconds, get_json)
    customer_track = _customer_track(recording)
    play_url = customer_track.get("playUrl")
    if not play_url:
        raise RuntimeError("customer recording track missing playUrl")

    pcm16_mono = decode_audio(str(play_url), args.sample_rate, args.timeout_seconds)
    frames = pcm16_mono_to_replay_frames(
        pcm16_mono,
        sample_rate_hz=args.sample_rate,
        frame_duration_ms=args.frame_ms,
    )
    timeline = {"mode": "always_interruptible"}
    live_events: list[dict[str, Any]] = []
    if args.live_timeline:
        live_events = _events_for_call(
            base_url,
            call_id,
            args.timeout_seconds,
            get_json,
        )
        frames, timeline = _apply_live_interruptible_timeline(
            frames=frames,
            events=live_events,
            track_started_at=customer_track.get("startedAt"),
            frame_ms=args.frame_ms,
        )
    fsmn_windows = (
        _load_fsmn_windows(Path(args.fsmn_report_file), call_id=call_id)
        if args.fsmn_report_file
        else _detect_fsmn_windows(
            call_id=call_id,
            play_url=str(play_url),
            model=args.fsmn_model,
            fsmn_detector_factory=fsmn_detector_factory,
        )
    )
    speech_windows = [_speech_window(window) for window in fsmn_windows]
    web_rtc_factory = webrtc_vad_factory or (lambda: WebRtcVadAdapter(mode=args.webrtc_mode))
    replay_report = replay_sip_barge_in_vad_providers(
        call_id=call_id,
        frames=frames,
        providers=[
            SipVadReplayProvider(name="webrtc_main", vad_factory=web_rtc_factory),
            SipVadReplayProvider(
                name="fsmn_main",
                vad_factory=lambda: WindowVoiceActivityDetector(speech_windows),
            ),
        ],
        config=SipBargeInConfig(),
    )

    return {
        "mode": "p1_vad_provider_compare",
        "callId": call_id,
        "audio": {
            "sampleRateHz": args.sample_rate,
            "frameMs": args.frame_ms,
            "frameCount": len(frames),
            "customerTrackObjectName": customer_track.get("objectName"),
        },
        "timeline": timeline,
        "live": {
            "candidateOffsetsMs": _live_event_offsets_ms(
                live_events,
                event_type="sip_interrupt_candidate",
                track_started_at=customer_track.get("startedAt"),
            ),
            "preStopOffsetsMs": _live_event_offsets_ms(
                live_events,
                event_type="sip_pre_stop",
                track_started_at=customer_track.get("startedAt"),
            ),
        },
        "fsmn": {
            "windows": len(speech_windows),
            "source": "report_file" if args.fsmn_report_file else "funasr_fsmn_vad",
        },
        "providers": {
            name: _provider_report_to_dict(provider_report)
            for name, provider_report in replay_report.provider_reports.items()
        },
    }


def _detect_fsmn_windows(
    *,
    call_id: str,
    play_url: str,
    model: str,
    fsmn_detector_factory: FsmnDetectorFactory | None,
) -> list[dict[str, Any]]:
    if fsmn_detector_factory is not None:
        return fsmn_detector_factory(model).detect(call_id=call_id, play_url=play_url)

    from tools.ai_call_vad_shadow import FsmnVadDetector

    return FsmnVadDetector(model=model).detect(call_id=call_id, play_url=play_url)


def _provider_report_to_dict(report: SipBargeInProviderReplayReport) -> dict[str, Any]:
    return {
        "observedFrames": report.observed_frames,
        "candidateCount": len(report.candidate_events),
        "preStopCount": len(report.pre_stop_events),
        "firstCandidateMs": _first_offset(report.candidate_events),
        "firstPreStopMs": _first_offset(report.pre_stop_events),
        "lastReason": report.last_reason,
        "candidates": [_event_to_dict(event) for event in report.candidate_events],
        "preStops": [_event_to_dict(event) for event in report.pre_stop_events],
    }


def _event_to_dict(event: Any) -> dict[str, Any]:
    return {
        "offsetMs": event.offset_ms,
        "frameDurationMs": event.frame_duration_ms,
        "candidateClass": event.candidate_class,
        "reason": event.reason,
    }


def _first_offset(events: Sequence[Any]) -> int | None:
    if not events:
        return None
    return int(events[0].offset_ms)


def _load_fsmn_windows(path: Path, *, call_id: str) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return [window for window in payload if isinstance(window, dict)]
    if not isinstance(payload, dict):
        raise RuntimeError("FSMN report must be a JSON object or list")
    if isinstance(payload.get("vadWindows"), list):
        return [window for window in payload["vadWindows"] if isinstance(window, dict)]
    windows_by_call_id = payload.get("windowsByCallId")
    if isinstance(windows_by_call_id, dict) and isinstance(windows_by_call_id.get(call_id), list):
        return [window for window in windows_by_call_id[call_id] if isinstance(window, dict)]
    raise RuntimeError("FSMN report missing vadWindows")


def _speech_window(window: dict[str, Any]) -> SpeechWindow:
    start_ms = _first_present(window.get("startMs"), window.get("start_ms"))
    end_ms = _first_present(window.get("endMs"), window.get("end_ms"))
    if start_ms is None or end_ms is None:
        raise RuntimeError(f"invalid FSMN window: {window!r}")
    return SpeechWindow(start_ms=int(start_ms), end_ms=int(end_ms))


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


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


def _apply_live_interruptible_timeline(
    *,
    frames: Sequence[SipBargeInReplayFrame],
    events: Sequence[dict[str, Any]],
    track_started_at: Any,
    frame_ms: int,
) -> tuple[list[SipBargeInReplayFrame], dict[str, Any]]:
    track_start = _required_timestamp(track_started_at, field="customer track startedAt")
    duration_ms = len(frames) * frame_ms
    windows = _live_interruptible_windows(
        events=events,
        track_start=track_start,
        duration_ms=duration_ms,
    )
    timeline_frames: list[SipBargeInReplayFrame] = []
    window_index = 0
    for frame in frames:
        frame_end_ms = frame.offset_ms + frame_ms
        while window_index < len(windows) and windows[window_index][1] <= frame.offset_ms:
            window_index += 1
        interruptible = (
            window_index < len(windows)
            and windows[window_index][0] < frame_end_ms
            and windows[window_index][1] > frame.offset_ms
        )
        timeline_frames.append(
            SipBargeInReplayFrame(
                offset_ms=frame.offset_ms,
                frame=frame.frame,
                interruptible=interruptible,
            )
        )
    return timeline_frames, {
        "mode": "live_events",
        "aiTailMs": LIVE_TIMELINE_AI_TAIL_MS,
        "interruptibleWindowCount": len(windows),
        "interruptibleDurationMs": sum(end_ms - start_ms for start_ms, end_ms in windows),
        "interruptibleWindows": [
            {"startMs": start_ms, "endMs": end_ms} for start_ms, end_ms in windows
        ],
    }


def _live_interruptible_windows(
    *,
    events: Sequence[dict[str, Any]],
    track_start: datetime,
    duration_ms: int,
) -> list[tuple[int, int]]:
    active_start_ms: int | None = None
    windows: list[tuple[int, int]] = []
    sorted_events = sorted(events, key=lambda event: str(event.get("eventTime") or ""))
    for event in sorted_events:
        event_type = str(event.get("eventType") or "")
        if event_type not in {"model_response_started", "model_response_done"}:
            continue
        event_time = _optional_timestamp(event.get("eventTime"))
        if event_time is None:
            continue
        offset_ms = round((event_time - track_start).total_seconds() * 1000)
        if event_type == "model_response_started":
            if active_start_ms is None:
                active_start_ms = offset_ms
            continue
        if active_start_ms is None:
            continue
        windows.append((active_start_ms, offset_ms + LIVE_TIMELINE_AI_TAIL_MS))
        active_start_ms = None
    if active_start_ms is not None:
        windows.append((active_start_ms, duration_ms))

    clipped = [
        (max(0, start_ms), min(duration_ms, end_ms))
        for start_ms, end_ms in windows
        if end_ms > 0 and start_ms < duration_ms and end_ms > start_ms
    ]
    merged: list[tuple[int, int]] = []
    for start_ms, end_ms in clipped:
        if merged and start_ms <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end_ms))
        else:
            merged.append((start_ms, end_ms))
    return merged


def _live_event_offsets_ms(
    events: Sequence[dict[str, Any]],
    *,
    event_type: str,
    track_started_at: Any,
) -> list[int]:
    if not events:
        return []
    track_start = _required_timestamp(track_started_at, field="customer track startedAt")
    offsets: list[int] = []
    for event in events:
        if event.get("eventType") != event_type:
            continue
        event_time = _optional_timestamp(event.get("eventTime"))
        if event_time is not None:
            offsets.append(round((event_time - track_start).total_seconds() * 1000))
    return sorted(offset for offset in offsets if offset >= 0)


def _required_timestamp(value: Any, *, field: str) -> datetime:
    timestamp = _optional_timestamp(value)
    if timestamp is None:
        raise RuntimeError(f"{field} is required for --live-timeline")
    return timestamp


def _optional_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _customer_track(recording: dict[str, Any]) -> dict[str, Any]:
    tracks = recording.get("tracks")
    if not isinstance(tracks, list):
        return {}
    for track in tracks:
        if isinstance(track, dict) and track.get("trackRole") == "customer":
            return track
    return {}


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


def _print_text_report(report: dict[str, Any], stdout: TextIO) -> None:
    if report.get("mode") == "p1_vad_onset_diagnosis":
        sample = report.get("sample") or {}
        window = report.get("window") or {}
        print(
            "onset_diagnosis "
            f"sampleId={sample.get('id')} "
            f"label={sample.get('label')} "
            f"speechStartMs={sample.get('speechStartMs')} "
            f"windowMs={window.get('fromMs')}-{window.get('toMs')} "
            f"frames={len(report.get('frames') or [])}",
            file=stdout,
        )
        providers = report.get("providers") or {}
        for name in ("webrtc_main", "fsmn_main", "webrtc_fsmn_agreement"):
            provider = providers.get(name) or {}
            print(
                "provider "
                f"{name} "
                f"detected={provider.get('detected')} "
                f"withinMaxLag={provider.get('withinMaxLag')} "
                f"firstCandidateMs={provider.get('firstCandidateMs')} "
                f"detectionLagMs={provider.get('detectionLagMs')} "
                f"candidateCount={provider.get('candidateCount')}",
                file=stdout,
            )
        for span in (report.get("reasonSpans") or [])[:12]:
            print(
                "span "
                f"{span.get('startMs')}-{span.get('endMs')} "
                f"webrtcSpeech={span.get('webrtcSpeech')} "
                f"fsmnSpeech={span.get('fsmnSpeech')} "
                f"agreementSpeech={span.get('agreementSpeech')} "
                f"webrtcReason={span.get('webrtcReason')} "
                f"fsmnReason={span.get('fsmnReason')} "
                f"rmsDbfsRange={span.get('rmsDbfsRange')} "
                f"snrDbRange={span.get('snrDbRange')}",
                file=stdout,
            )
        return

    if report.get("mode") == "p1_vad_provider_benchmark":
        summary = report.get("summary") or {}
        print(
            "benchmark "
            f"samples={summary.get('samples')} "
            f"speech={summary.get('speechSamples')} "
            f"nonSpeech={summary.get('nonSpeechSamples')}",
            file=stdout,
        )
        for name in ("webrtc_main", "fsmn_main", "webrtc_fsmn_agreement"):
            provider = (summary.get("providers") or {}).get(name) or {}
            lag = provider.get("detectionLagMs") or {}
            print(
                "provider "
                f"{name} "
                f"recall={provider.get('speechRecall')} "
                f"detected={provider.get('detected')} "
                f"missed={provider.get('missed')} "
                f"withinMaxLag={provider.get('withinMaxLag')} "
                f"lagP50Ms={lag.get('p50')} "
                f"lagP90Ms={lag.get('p90')} "
                f"falsePositiveSamples={provider.get('falsePositiveSamples')} "
                f"falsePositiveWindowsPerMinute={provider.get('falsePositiveWindowsPerMinute')}",
                file=stdout,
            )
        gates = report.get("benchmarkGates")
        if isinstance(gates, dict):
            status = "pass" if gates.get("passed") else "fail"
            print(
                f"benchmark_gates status={status} failures={gates.get('failureCount', 0)}",
                file=stdout,
            )
            for failure in gates.get("failures") or []:
                if not isinstance(failure, dict):
                    continue
                print(
                    "benchmark_gate_failure "
                    f"gate={failure.get('gate')} "
                    f"provider={failure.get('provider')} "
                    f"required={failure.get('required')} "
                    f"actual={failure.get('actual')}",
                    file=stdout,
                )
        return

    audio = report.get("audio") or {}
    fsmn = report.get("fsmn") or {}
    print(
        "compare "
        f"callId={report.get('callId')} "
        f"frames={audio.get('frameCount')} "
        f"sampleRateHz={audio.get('sampleRateHz')} "
        f"fsmnWindows={fsmn.get('windows')} "
        f"fsmnSource={fsmn.get('source')}",
        file=stdout,
    )
    providers = report.get("providers") or {}
    for name in ("webrtc_main", "fsmn_main"):
        provider = providers.get(name) or {}
        print(
            "provider "
            f"{name} "
            f"candidates={provider.get('candidateCount')} "
            f"preStops={provider.get('preStopCount')} "
            f"firstCandidateMs={provider.get('firstCandidateMs')} "
            f"firstPreStopMs={provider.get('firstPreStopMs')} "
            f"lastReason={provider.get('lastReason')}",
            file=stdout,
        )


if __name__ == "__main__":
    main()
