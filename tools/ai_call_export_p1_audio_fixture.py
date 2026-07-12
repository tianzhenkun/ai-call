from __future__ import annotations

import argparse
import json
import math
import struct
import subprocess
import sys
import urllib.error
import urllib.request
import wave
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, TextIO

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

GetJson = Callable[[str, float], dict[str, Any]]
ExportWav = Callable[[str, Path, int, float, int, int], None]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export a customer recording slice as a local P1 audio fixture.",
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:19011",
        help="AI Call API base URL, without trailing slash.",
    )
    parser.add_argument("--call-id", required=True)
    parser.add_argument("--start-ms", type=int, required=True)
    parser.add_argument("--end-ms", type=int, required=True)
    parser.add_argument("--category", required=True)
    parser.add_argument("--expectation", required=True)
    parser.add_argument("--source-type", default="live_call")
    parser.add_argument("--output-dir", default="/tmp/ai_call_p1_audio_fixtures")
    parser.add_argument("--sample-rate", type=int, default=16_000)
    parser.add_argument("--frame-ms", type=int, default=20)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument(
        "--include-ai-track",
        action="store_true",
        help="Also export the aligned AI track slice and add aiWavPath.",
    )
    parser.add_argument("--json", action="store_true", help="Print raw JSON result.")
    return parser


def run(
    argv: Sequence[str] | None = None,
    *,
    get_json: GetJson | None = None,
    export_wav: ExportWav | None = None,
    stdout: TextIO = sys.stdout,
    stderr: TextIO = sys.stderr,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    get_json = get_json or _get_json
    export_wav = export_wav or _export_wav
    base_url = args.base_url.rstrip("/")

    try:
        result = _export_fixture(
            base_url=base_url,
            args=args,
            get_json=get_json,
            export_wav=export_wav,
        )
    except Exception as exc:
        print(f"export failed: {exc!s}", file=stderr)
        return 1

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2), file=stdout)
    else:
        _print_text_result(result, stdout)
    return 0


def main(argv: Sequence[str] | None = None) -> None:
    raise SystemExit(run(argv))


def _export_fixture(
    *,
    base_url: str,
    args: argparse.Namespace,
    get_json: GetJson,
    export_wav: ExportWav,
) -> dict[str, Any]:
    start_ms = _validate_ms(args.start_ms, name="start-ms")
    end_ms = _validate_ms(args.end_ms, name="end-ms")
    if end_ms <= start_ms:
        raise RuntimeError("--end-ms must be greater than --start-ms")
    if args.sample_rate <= 0:
        raise RuntimeError("--sample-rate must be greater than 0")
    if args.frame_ms <= 0:
        raise RuntimeError("--frame-ms must be greater than 0")

    recording = _recording_for_call(
        base_url=base_url,
        call_id=args.call_id,
        timeout_seconds=args.timeout_seconds,
        get_json=get_json,
    )
    customer_track = _track_by_role(recording, "customer")
    play_url = str(customer_track["playUrl"])

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    fixture_id = _fixture_id(args.call_id, start_ms=start_ms, end_ms=end_ms)
    wav_path = output_dir / f"{fixture_id}.wav"
    ai_wav_path: Path | None = None
    fragment_path = output_dir / f"{fixture_id}.fixture.json"

    export_wav(
        play_url,
        wav_path,
        int(args.sample_rate),
        float(args.timeout_seconds),
        start_ms,
        end_ms,
    )
    if bool(args.include_ai_track):
        ai_track = _track_by_role(recording, "ai")
        ai_start_ms, ai_end_ms = _aligned_track_offsets_ms(
            source_track=customer_track,
            target_track=ai_track,
            start_ms=start_ms,
            end_ms=end_ms,
        )
        ai_wav_path = output_dir / f"{fixture_id}.ai.wav"
        export_wav(
            str(ai_track["playUrl"]),
            ai_wav_path,
            int(args.sample_rate),
            float(args.timeout_seconds),
            ai_start_ms,
            ai_end_ms,
        )
    acoustic_start_offset_ms = _detect_acoustic_speech_offset_ms(
        wav_path,
        frame_ms=int(args.frame_ms),
    )

    fragment = _matrix_fragment(
        fixture_id=fixture_id,
        wav_path=wav_path,
        ai_wav_path=ai_wav_path,
        track=customer_track,
        start_ms=start_ms,
        end_ms=end_ms,
        sample_rate_hz=int(args.sample_rate),
        frame_ms=int(args.frame_ms),
        source_type=str(args.source_type),
        category=str(args.category),
        expectation=str(args.expectation),
        acoustic_start_offset_ms=acoustic_start_offset_ms,
    )
    fragment_path.write_text(
        json.dumps(fragment, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    return {
        "callId": args.call_id,
        "fixtureId": fixture_id,
        "fixturePath": str(wav_path),
        "aiFixturePath": str(ai_wav_path) if ai_wav_path is not None else None,
        "fragmentPath": str(fragment_path),
        "matrixFragment": fragment,
    }


def _recording_for_call(
    *,
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


def _track_by_role(recording: dict[str, Any], role: str) -> dict[str, Any]:
    for track in recording.get("tracks") or []:
        if not isinstance(track, dict):
            continue
        if track.get("trackRole") != role:
            continue
        play_url = track.get("playUrl")
        if isinstance(play_url, str) and play_url:
            return track
    raise RuntimeError(f"recording response missing {role} track playUrl")


def _matrix_fragment(
    *,
    fixture_id: str,
    wav_path: Path,
    ai_wav_path: Path | None,
    track: dict[str, Any],
    start_ms: int,
    end_ms: int,
    sample_rate_hz: int,
    frame_ms: int,
    source_type: str,
    category: str,
    expectation: str,
    acoustic_start_offset_ms: int | None,
) -> dict[str, Any]:
    duration_ms = end_ms - start_ms
    started_at = _track_slice_start(track, start_ms=start_ms)
    ended_at = started_at + timedelta(milliseconds=duration_ms)
    sample: dict[str, Any] = {
        "id": f"{fixture_id}_{category}_{expectation}",
        "callId": fixture_id,
        "sourceType": source_type,
        "evaluationSource": "audio_fixture",
        "category": category,
        "expectation": expectation,
    }
    uses_acoustic_start = expectation in {
        "must_interrupt",
        "must_pre_stop_after_candidate",
    }
    if uses_acoustic_start:
        if acoustic_start_offset_ms is None:
            raise RuntimeError(
                f"{expectation} sample requires stable acoustic speech in exported wav"
            )
        speech_start_offset_ms = min(max(0, acoustic_start_offset_ms), duration_ms)
        speech_started_at = started_at + timedelta(milliseconds=speech_start_offset_ms)
    else:
        speech_start_offset_ms = 0
        speech_started_at = started_at

    if expectation == "must_interrupt":
        sample["speechStartTime"] = _format_time(speech_started_at)
        sample["maxPreStopLatencyMs"] = 500
    elif expectation == "must_pre_stop_after_candidate":
        sample["candidateTime"] = _format_time(speech_started_at)
        sample["maxCandidateToPreStopMs"] = 500
    else:
        sample["windowStartTime"] = _format_time(started_at)
        sample["windowEndTime"] = _format_time(ended_at)

    vad_window_start_ms = speech_start_offset_ms if uses_acoustic_start else 0
    fixture_payload: dict[str, Any] = {
        "startedAt": _format_time(started_at),
        "sampleRateHz": sample_rate_hz,
        "frameMs": frame_ms,
        "wavPath": wav_path.name,
        "vadWindows": [{"startMs": vad_window_start_ms, "endMs": duration_ms}],
    }
    if ai_wav_path is not None:
        fixture_payload["aiWavPath"] = ai_wav_path.name
    return {
        "audioFixtures": {
            fixture_id: fixture_payload,
        },
        "samples": [sample],
    }


def _aligned_track_offsets_ms(
    *,
    source_track: dict[str, Any],
    target_track: dict[str, Any],
    start_ms: int,
    end_ms: int,
) -> tuple[int, int]:
    source_start = _track_slice_start(source_track, start_ms=start_ms)
    source_end = _track_slice_start(source_track, start_ms=end_ms)
    target_started_at = target_track.get("startedAt")
    if not isinstance(target_started_at, str) or not target_started_at:
        raise RuntimeError("target track missing startedAt")
    target_start = _parse_time(target_started_at)
    target_offset_ms = round((source_start - target_start).total_seconds() * 1000)
    target_end_ms = round((source_end - target_start).total_seconds() * 1000)
    if target_offset_ms < 0:
        raise RuntimeError("aligned target track starts after requested window")
    if target_end_ms <= target_offset_ms:
        raise RuntimeError("aligned target track window is empty")
    return target_offset_ms, target_end_ms


def _detect_acoustic_speech_offset_ms(
    wav_path: Path,
    *,
    frame_ms: int,
    rms_threshold_dbfs: float = -36.0,
    min_stable_ms: int = 160,
) -> int | None:
    if frame_ms <= 0:
        return None
    min_stable_frames = max(1, math.ceil(max(0, min_stable_ms) / frame_ms))
    try:
        with wave.open(str(wav_path), "rb") as wav_file:
            channels = wav_file.getnchannels()
            sample_width = wav_file.getsampwidth()
            sample_rate_hz = wav_file.getframerate()
            if channels != 1 or sample_width != 2 or sample_rate_hz <= 0:
                return None
            pcm = wav_file.readframes(wav_file.getnframes())
    except (EOFError, OSError, wave.Error):
        return None

    samples_per_frame = sample_rate_hz * frame_ms // 1000
    bytes_per_frame = samples_per_frame * sample_width
    if samples_per_frame <= 0 or bytes_per_frame <= 0:
        return None
    full_frame_count = len(pcm) // bytes_per_frame
    stable_start_index: int | None = None
    stable_frames = 0
    for index in range(full_frame_count):
        chunk = pcm[index * bytes_per_frame : (index + 1) * bytes_per_frame]
        samples = [sample for (sample,) in struct.iter_unpack("<h", chunk)]
        if not samples:
            stable_start_index = None
            stable_frames = 0
            continue
        rms = math.sqrt(sum(sample * sample for sample in samples) / len(samples))
        if rms <= 0:
            stable_start_index = None
            stable_frames = 0
            continue
        rms_dbfs = 20 * math.log10(rms / 32768.0)
        if rms_dbfs >= rms_threshold_dbfs:
            if stable_start_index is None:
                stable_start_index = index
            stable_frames += 1
            if stable_frames >= min_stable_frames:
                return stable_start_index * frame_ms
        else:
            stable_start_index = None
            stable_frames = 0
    return None


def _track_slice_start(track: dict[str, Any], *, start_ms: int) -> datetime:
    started_at = track.get("startedAt")
    if not isinstance(started_at, str) or not started_at:
        base_time = datetime(1970, 1, 1, tzinfo=timezone.utc)
    else:
        base_time = _parse_time(started_at)
    return base_time + timedelta(milliseconds=start_ms)


def _fixture_id(call_id: str, *, start_ms: int, end_ms: int) -> str:
    safe_call_id = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in call_id)
    return f"{safe_call_id}_customer_{start_ms}_{end_ms}"


def _validate_ms(value: int, *, name: str) -> int:
    if value < 0:
        raise RuntimeError(f"--{name} must be greater than or equal to 0")
    return int(value)


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00",
        "Z",
    )


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


def _export_wav(
    play_url: str,
    output_path: Path,
    sample_rate_hz: int,
    timeout_seconds: float,
    start_ms: int,
    end_ms: int,
) -> None:
    duration_ms = end_ms - start_ms
    result = subprocess.run(
        [
            "ffmpeg",
            "-v",
            "error",
            "-y",
            "-ss",
            f"{start_ms / 1000:.3f}",
            "-t",
            f"{duration_ms / 1000:.3f}",
            "-i",
            play_url,
            "-ac",
            "1",
            "-ar",
            str(sample_rate_hz),
            "-acodec",
            "pcm_s16le",
            str(output_path),
        ],
        capture_output=True,
        timeout=timeout_seconds,
    )
    if result.returncode != 0:
        message = result.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(message or f"ffmpeg exited with {result.returncode}")


def _print_text_result(result: dict[str, Any], stdout: TextIO) -> None:
    print(
        "exported "
        f"callId={result.get('callId')} "
        f"fixtureId={result.get('fixtureId')} "
        f"fixturePath={result.get('fixturePath')} "
        f"fragmentPath={result.get('fragmentPath')}",
        file=stdout,
    )


if __name__ == "__main__":
    main()
