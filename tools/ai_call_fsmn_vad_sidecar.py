from __future__ import annotations

import argparse
import base64
import binascii
import contextlib
import json
import sys
import tempfile
import threading
import time
import wave
from collections.abc import Sequence
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Protocol

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.ai_call.audio_bridge import AudioBridgeError, PcmAudioBridge, PcmAudioFrame

DEFAULT_MODEL = "iic/speech_fsmn_vad_zh-cn-16k-common-pytorch"


@dataclass(frozen=True, slots=True)
class FsmnVadSidecarRequest:
    call_id: str
    model: str
    sample_rate_hz: int
    channels: int
    sample_width_bytes: int
    audio: bytes
    interruptible: bool
    timestamp: str | None = None


class FsmnVadModelProtocol(Protocol):
    def detect_windows(
        self,
        *,
        pcm16_mono: bytes,
        sample_rate_hz: int,
    ) -> list[dict[str, Any]]: ...


@dataclass(slots=True)
class _BufferedCallState:
    pcm16_mono: bytearray = field(default_factory=bytearray)
    samples_since_analysis: int = 0
    has_analyzed: bool = False
    last_speech: bool = False
    last_confidence: float | None = None
    last_access_monotonic: float = 0.0


def parse_vad_request_payload(payload: Any) -> FsmnVadSidecarRequest:
    if not isinstance(payload, dict):
        raise ValueError("request payload must be a JSON object")
    call_id = str(payload.get("callId") or "").strip()
    if not call_id:
        raise ValueError("callId is required")

    audio_base64 = payload.get("audioBase64")
    if not isinstance(audio_base64, str) or not audio_base64:
        raise ValueError("audioBase64 is required")
    try:
        audio = base64.b64decode(audio_base64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("audioBase64 must be valid base64") from exc

    sample_rate_hz = _positive_int(payload.get("sampleRateHz"), "sampleRateHz")
    channels = _positive_int(payload.get("channels"), "channels")
    sample_width_bytes = _positive_int(payload.get("sampleWidthBytes"), "sampleWidthBytes")
    interruptible = bool(payload.get("interruptible", False))
    timestamp_value = payload.get("timestamp")
    timestamp = timestamp_value if isinstance(timestamp_value, str) else None
    model = str(payload.get("model") or DEFAULT_MODEL)

    return FsmnVadSidecarRequest(
        call_id=call_id,
        model=model,
        sample_rate_hz=sample_rate_hz,
        channels=channels,
        sample_width_bytes=sample_width_bytes,
        audio=audio,
        interruptible=interruptible,
        timestamp=timestamp,
    )


class BufferedFsmnVadSidecar:
    def __init__(
        self,
        *,
        model: FsmnVadModelProtocol,
        target_sample_rate_hz: int = 16000,
        min_window_ms: int = 320,
        analysis_interval_ms: int = 160,
        max_window_ms: int = 1200,
        decision_tail_ms: int = 260,
        idle_ttl_seconds: float = 600.0,
    ) -> None:
        self._model = model
        self._target_sample_rate_hz = target_sample_rate_hz
        self._min_window_ms = max(1, min_window_ms)
        self._analysis_interval_ms = max(0, analysis_interval_ms)
        self._max_window_ms = max(self._min_window_ms, max_window_ms)
        self._decision_tail_ms = max(1, decision_tail_ms)
        self._idle_ttl_seconds = max(1.0, idle_ttl_seconds)
        self._audio_bridge = PcmAudioBridge(qwen_input_sample_rate_hz=target_sample_rate_hz)
        self._states: dict[str, _BufferedCallState] = {}
        self._lock = threading.Lock()

    def detect(self, request: FsmnVadSidecarRequest) -> dict[str, Any]:
        frame = PcmAudioFrame(
            data=request.audio,
            sample_rate_hz=request.sample_rate_hz,
            channels=request.channels,
            sample_width_bytes=request.sample_width_bytes,
        )
        try:
            pcm16_mono = self._audio_bridge.normalize_qwen_input(frame)
        except AudioBridgeError as exc:
            raise ValueError(exc.reason) from exc

        with self._lock:
            now = time.monotonic()
            self._cleanup_idle_calls(now)
            state = self._states.setdefault(request.call_id, _BufferedCallState())
            state.last_access_monotonic = now
            state.pcm16_mono.extend(pcm16_mono)
            appended_samples = len(pcm16_mono) // 2
            state.samples_since_analysis += appended_samples
            self._trim_to_max_window(state)

            buffer_duration_ms = self._duration_ms(len(state.pcm16_mono) // 2)
            since_analysis_ms = self._duration_ms(state.samples_since_analysis)
            if buffer_duration_ms < self._min_window_ms:
                return self._response(
                    state,
                    analyzed=False,
                    buffer_duration_ms=buffer_duration_ms,
                    windows=[],
                )
            if state.has_analyzed and since_analysis_ms < self._analysis_interval_ms:
                return self._response(
                    state,
                    analyzed=False,
                    buffer_duration_ms=buffer_duration_ms,
                    windows=[],
                )
            pcm_for_model = bytes(state.pcm16_mono)

        windows = self._model.detect_windows(
            pcm16_mono=pcm_for_model,
            sample_rate_hz=self._target_sample_rate_hz,
        )
        speech = self._has_recent_speech(
            windows=windows,
            buffer_duration_ms=buffer_duration_ms,
        )

        with self._lock:
            state = self._states.setdefault(request.call_id, _BufferedCallState())
            state.last_access_monotonic = time.monotonic()
            state.samples_since_analysis = 0
            state.has_analyzed = True
            state.last_speech = speech
            state.last_confidence = None
            return self._response(
                state,
                analyzed=True,
                buffer_duration_ms=buffer_duration_ms,
                windows=windows,
            )

    def reset(self, call_id: str) -> None:
        with self._lock:
            self._states.pop(call_id, None)

    def _trim_to_max_window(self, state: _BufferedCallState) -> None:
        max_bytes = self._samples_for_ms(self._max_window_ms) * 2
        if len(state.pcm16_mono) > max_bytes:
            del state.pcm16_mono[: len(state.pcm16_mono) - max_bytes]

    def _cleanup_idle_calls(self, now: float) -> None:
        stale = [
            call_id
            for call_id, state in self._states.items()
            if state.last_access_monotonic and now - state.last_access_monotonic > self._idle_ttl_seconds
        ]
        for call_id in stale:
            self._states.pop(call_id, None)

    def _samples_for_ms(self, duration_ms: int) -> int:
        return self._target_sample_rate_hz * duration_ms // 1000

    def _duration_ms(self, sample_count: int) -> int:
        return round(sample_count / self._target_sample_rate_hz * 1000)

    def _has_recent_speech(
        self,
        *,
        windows: list[dict[str, Any]],
        buffer_duration_ms: int,
    ) -> bool:
        tail_start_ms = max(0, buffer_duration_ms - self._decision_tail_ms)
        for window in windows:
            start_ms = _optional_int(window.get("startMs"))
            end_ms = _optional_int(window.get("endMs"))
            if start_ms is None or end_ms is None:
                continue
            if start_ms < buffer_duration_ms and end_ms > tail_start_ms:
                return True
        return False

    @staticmethod
    def _response(
        state: _BufferedCallState,
        *,
        analyzed: bool,
        buffer_duration_ms: int,
        windows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "speech": state.last_speech,
            "confidence": state.last_confidence,
            "analyzed": analyzed,
            "bufferDurationMs": buffer_duration_ms,
            "windows": windows,
        }


class FunasrFsmnVadModel:
    def __init__(self, *, model: str) -> None:
        self.model = model
        try:
            with contextlib.redirect_stdout(sys.stderr):
                from funasr import AutoModel  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "FunASR is not installed. Run with: "
                "uv run --with funasr --with modelscope --with soundfile "
                "--with torch --with torchaudio python tools/ai_call_fsmn_vad_sidecar.py"
            ) from exc
        with contextlib.redirect_stdout(sys.stderr):
            self._auto_model = AutoModel(model=model, disable_update=True)
        self._lock = threading.Lock()

    def detect_windows(
        self,
        *,
        pcm16_mono: bytes,
        sample_rate_hz: int,
    ) -> list[dict[str, Any]]:
        with tempfile.TemporaryDirectory(prefix="ai-call-fsmn-vad-") as tmp_dir:
            wav_path = Path(tmp_dir) / "window.wav"
            _write_pcm16_mono_wav(wav_path, pcm16_mono, sample_rate_hz)
            with self._lock, contextlib.redirect_stdout(sys.stderr):
                result = self._auto_model.generate(input=str(wav_path))
        return _extract_fsmn_windows(result)


class _VadRequestHandler(BaseHTTPRequestHandler):
    server: _VadHttpServer

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self._write_json(200, {"ok": True, "model": self.server.model_name})
            return
        self._write_json(404, {"error": "not_found"})

    def do_POST(self) -> None:
        if self.path != "/vad":
            self._write_json(404, {"error": "not_found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
            request = parse_vad_request_payload(payload)
            result = self.server.sidecar.detect(request)
        except ValueError as exc:
            self._write_json(400, {"error": str(exc)})
            return
        except Exception as exc:
            self._write_json(500, {"error": type(exc).__name__, "message": str(exc)})
            return
        self._write_json(200, result)

    def log_message(self, format: str, *args: Any) -> None:
        print(f"[fsmn-vad-sidecar] {self.address_string()} {format % args}", file=sys.stderr)

    def _write_json(self, status_code: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class _VadHttpServer(ThreadingHTTPServer):
    def __init__(
        self,
        server_address: tuple[str, int],
        sidecar: BufferedFsmnVadSidecar,
        *,
        model_name: str,
    ) -> None:
        super().__init__(server_address, _VadRequestHandler)
        self.sidecar = sidecar
        self.model_name = model_name


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local FunASR FSMN-VAD sidecar for AI Call.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19111)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--min-window-ms", type=int, default=320)
    parser.add_argument("--analysis-interval-ms", type=int, default=160)
    parser.add_argument("--max-window-ms", type=int, default=1200)
    parser.add_argument("--decision-tail-ms", type=int, default=260)
    parser.add_argument("--idle-ttl-seconds", type=float, default=600.0)
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    model = FunasrFsmnVadModel(model=args.model)
    sidecar = BufferedFsmnVadSidecar(
        model=model,
        min_window_ms=args.min_window_ms,
        analysis_interval_ms=args.analysis_interval_ms,
        max_window_ms=args.max_window_ms,
        decision_tail_ms=args.decision_tail_ms,
        idle_ttl_seconds=args.idle_ttl_seconds,
    )
    server = _VadHttpServer((args.host, args.port), sidecar, model_name=args.model)
    print(
        f"FSMN VAD sidecar listening on http://{args.host}:{args.port}/vad model={args.model}",
        file=sys.stderr,
    )
    server.serve_forever()


def _positive_int(value: Any, field_name: str) -> int:
    if not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _optional_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


def _write_pcm16_mono_wav(path: Path, pcm16_mono: bytes, sample_rate_hz: int) -> None:
    with wave.open(str(path), "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(sample_rate_hz)
        wav_file.writeframes(pcm16_mono)


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


if __name__ == "__main__":
    main()
