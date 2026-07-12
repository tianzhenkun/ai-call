from __future__ import annotations

import base64
from typing import Any

from tools.ai_call_fsmn_vad_sidecar import (
    BufferedFsmnVadSidecar,
    parse_vad_request_payload,
)


class FakeFsmnVadModel:
    def __init__(self, windows: list[list[int]]) -> None:
        self.windows = windows
        self.calls: list[tuple[int, int]] = []

    def detect_windows(self, *, pcm16_mono: bytes, sample_rate_hz: int) -> list[dict[str, Any]]:
        self.calls.append((len(pcm16_mono), sample_rate_hz))
        return [{"startMs": start_ms, "endMs": end_ms} for start_ms, end_ms in self.windows]


def _payload(
    *,
    call_id: str = "call_fsmn_sidecar",
    sample_rate_hz: int = 48000,
    duration_ms: int = 20,
    amplitude: int = 1000,
) -> dict[str, Any]:
    sample_count = sample_rate_hz * duration_ms // 1000
    sample = int(amplitude).to_bytes(2, "little", signed=True)
    return {
        "callId": call_id,
        "model": "iic/speech_fsmn_vad_zh-cn-16k-common-pytorch",
        "timestamp": "2026-07-03T10:00:00+00:00",
        "sampleRateHz": sample_rate_hz,
        "channels": 1,
        "sampleWidthBytes": 2,
        "interruptible": True,
        "audioBase64": base64.b64encode(sample * sample_count).decode("ascii"),
    }


def test_parse_vad_request_payload_accepts_current_client_shape() -> None:
    request = parse_vad_request_payload(_payload(sample_rate_hz=48000, duration_ms=20))

    assert request.call_id == "call_fsmn_sidecar"
    assert request.sample_rate_hz == 48000
    assert request.channels == 1
    assert request.sample_width_bytes == 2
    assert request.audio


def test_buffered_fsmn_sidecar_resamples_livekit_frames_before_model_call() -> None:
    model = FakeFsmnVadModel([[0, 20]])
    sidecar = BufferedFsmnVadSidecar(
        model=model,
        target_sample_rate_hz=16000,
        min_window_ms=20,
        analysis_interval_ms=0,
        max_window_ms=1000,
        decision_tail_ms=40,
    )

    result = sidecar.detect(parse_vad_request_payload(_payload(sample_rate_hz=48000)))

    assert result["speech"] is True
    assert model.calls == [(640, 16000)]


def test_buffered_fsmn_sidecar_waits_for_enough_audio_and_reuses_last_decision() -> None:
    model = FakeFsmnVadModel([[20, 40]])
    sidecar = BufferedFsmnVadSidecar(
        model=model,
        target_sample_rate_hz=16000,
        min_window_ms=40,
        analysis_interval_ms=100,
        max_window_ms=1000,
        decision_tail_ms=40,
    )
    first = sidecar.detect(parse_vad_request_payload(_payload(duration_ms=20)))
    second = sidecar.detect(parse_vad_request_payload(_payload(duration_ms=20)))
    third = sidecar.detect(parse_vad_request_payload(_payload(duration_ms=20)))

    assert first["speech"] is False
    assert first["analyzed"] is False
    assert second["speech"] is True
    assert second["analyzed"] is True
    assert third["speech"] is True
    assert third["analyzed"] is False
    assert len(model.calls) == 1
