from __future__ import annotations

import io
import json
import struct
from pathlib import Path
from typing import Any

from app.services.ai_call.sip_barge_in_replay import SequenceVoiceActivityDetector
from tools.ai_call_p1_vad_provider_compare import run


def _pcm_for_amplitudes(amplitudes: list[int]) -> bytes:
    chunks: list[bytes] = []
    sample_rate_hz = 8000
    samples_per_frame = sample_rate_hz * 20 // 1000
    for amplitude in amplitudes:
        chunks.append(struct.pack("<" + "h" * samples_per_frame, *([amplitude] * samples_per_frame)))
    return b"".join(chunks)


def test_provider_compare_reports_webrtc_main_and_fsmn_main_replay(tmp_path: Path) -> None:
    windows_file = tmp_path / "fsmn-report.json"
    windows_file.write_text(
        json.dumps({"vadWindows": [{"startMs": 0, "endMs": 360}]}),
        encoding="utf-8",
    )

    def fake_get_json(url: str, timeout_seconds: float) -> dict[str, Any]:
        _ = timeout_seconds
        if url.endswith("/ai-call/records/call_compare/recording"):
            return {
                "code": 200,
                "data": {
                    "tracks": [
                        {
                            "trackRole": "customer",
                            "playUrl": "https://audio.test/customer.ogg",
                        }
                    ],
                },
            }
        raise AssertionError(f"unexpected url: {url}")

    def fake_decode_audio(url: str, sample_rate_hz: int, timeout_seconds: float) -> bytes:
        _ = timeout_seconds
        assert url == "https://audio.test/customer.ogg"
        assert sample_rate_hz == 8000
        return _pcm_for_amplitudes([
            830,
            3750,
            3600,
            1825,
            1285,
            880,
            2950,
            4520,
            4200,
            3600,
            3350,
            3100,
            2950,
            3300,
            3650,
            3900,
            3700,
            3500,
        ])

    stdout = io.StringIO()

    exit_code = run(
        [
            "--base-url",
            "http://127.0.0.1:19011/ai-call-api/v1",
            "--call-id",
            "call_compare",
            "--fsmn-report-file",
            str(windows_file),
            "--sample-rate",
            "8000",
        ],
        get_json=fake_get_json,
        decode_audio=fake_decode_audio,
        webrtc_vad_factory=lambda: SequenceVoiceActivityDetector(
            [True, False, True, False, True, False, True, False, True, False]
            + [True, False, True, False, True, False, False, False],
        ),
        stdout=stdout,
    )

    output = stdout.getvalue()
    assert exit_code == 0
    assert "provider webrtc_main candidates=0 preStops=0" in output
    assert (
        "provider fsmn_main candidates=1 preStops=1 firstCandidateMs=160 firstPreStopMs=220"
        in output
    )


def test_provider_compare_scores_labeled_local_benchmark(tmp_path: Path) -> None:
    speech_path = tmp_path / "speech.wav"
    noise_path = tmp_path / "noise.wav"
    speech_path.write_bytes(b"wav")
    noise_path.write_bytes(b"wav")
    benchmark_path = tmp_path / "benchmark.json"
    benchmark_path.write_text(
        json.dumps(
            {
                "benchmarkGates": {
                    "minSamples": 2,
                    "minSpeechSamples": 1,
                    "minNonSpeechSamples": 1,
                    "providers": {
                        "webrtc_main": {
                            "minDetected": 1,
                            "minWithinMaxLag": 1,
                            "maxDetectionLagP90Ms": 160,
                            "maxFalsePositiveWindows": 1,
                        },
                        "fsmn_main": {
                            "minDetected": 1,
                            "minWithinMaxLag": 1,
                            "maxDetectionLagP90Ms": 160,
                            "maxFalsePositiveWindows": 0,
                        },
                        "webrtc_fsmn_agreement": {
                            "minDetected": 1,
                            "minWithinMaxLag": 1,
                            "maxDetectionLagP90Ms": 160,
                            "maxFalsePositiveWindows": 0,
                        },
                    },
                },
                "samples": [
                    {
                        "id": "speech_sample",
                        "wavPath": speech_path.name,
                        "label": "speech",
                        "speechStartMs": 0,
                        "speechEndMs": 360,
                        "maxDetectionLagMs": 300,
                    },
                    {
                        "id": "noise_sample",
                        "wavPath": noise_path.name,
                        "label": "non_speech",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    class FakeFsmnDetector:
        def detect(self, *, call_id: str, play_url: str) -> list[dict[str, Any]]:
            assert play_url.startswith("file://")
            return [{"startMs": 0, "endMs": 360}] if call_id == "speech_sample" else []

    stdout = io.StringIO()
    exit_code = run(
        [
            "--benchmark-file",
            str(benchmark_path),
            "--sample-rate",
            "8000",
        ],
        decode_audio=lambda url, sample_rate_hz, timeout_seconds: _pcm_for_amplitudes(
            [3000] * 18
        ),
        webrtc_vad_factory=lambda: SequenceVoiceActivityDetector([True] * 18),
        fsmn_detector_factory=lambda model: FakeFsmnDetector(),
        stdout=stdout,
    )

    output = stdout.getvalue()
    assert exit_code == 0
    assert "benchmark samples=2 speech=1 nonSpeech=1" in output
    assert "provider webrtc_main recall=1.0 detected=1 missed=0" in output
    assert "falsePositiveSamples=1" in output
    assert "provider fsmn_main recall=1.0 detected=1 missed=0" in output
    assert "falsePositiveSamples=0" in output
    assert "provider webrtc_fsmn_agreement recall=1.0 detected=1 missed=0" in output
    assert "benchmark_gates status=pass failures=0" in output


def test_provider_compare_fails_when_benchmark_gate_regresses(tmp_path: Path) -> None:
    speech_path = tmp_path / "speech.wav"
    speech_path.write_bytes(b"wav")
    benchmark_path = tmp_path / "benchmark.json"
    benchmark_path.write_text(
        json.dumps(
            {
                "benchmarkGates": {
                    "minSamples": 2,
                    "providers": {
                        "webrtc_main": {
                            "minDetected": 1,
                            "maxFalsePositiveWindows": 0,
                        }
                    },
                },
                "samples": [
                    {
                        "id": "speech_sample",
                        "wavPath": speech_path.name,
                        "label": "speech",
                        "speechStartMs": 0,
                        "speechEndMs": 360,
                        "maxDetectionLagMs": 300,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    class FakeFsmnDetector:
        def detect(self, *, call_id: str, play_url: str) -> list[dict[str, Any]]:
            assert play_url.startswith("file://")
            return [{"startMs": 0, "endMs": 360}]

    stdout = io.StringIO()
    exit_code = run(
        [
            "--benchmark-file",
            str(benchmark_path),
            "--sample-rate",
            "8000",
        ],
        decode_audio=lambda url, sample_rate_hz, timeout_seconds: _pcm_for_amplitudes(
            [3000] * 18
        ),
        webrtc_vad_factory=lambda: SequenceVoiceActivityDetector([True] * 18),
        fsmn_detector_factory=lambda model: FakeFsmnDetector(),
        stdout=stdout,
    )

    output = stdout.getvalue()
    assert exit_code == 2
    assert "benchmark_gates status=fail failures=1" in output
    assert (
        "benchmark_gate_failure gate=min_samples provider=None required=2 actual=1"
        in output
    )


def test_provider_compare_diagnoses_labeled_sample_onset(tmp_path: Path) -> None:
    speech_path = tmp_path / "speech.wav"
    speech_path.write_bytes(b"wav")
    benchmark_path = tmp_path / "benchmark.json"
    benchmark_path.write_text(
        json.dumps(
            {
                "samples": [
                    {
                        "id": "speech_sample",
                        "wavPath": speech_path.name,
                        "label": "speech",
                        "speechStartMs": 0,
                        "speechEndMs": 360,
                        "maxDetectionLagMs": 300,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    class FakeFsmnDetector:
        def detect(self, *, call_id: str, play_url: str) -> list[dict[str, Any]]:
            assert call_id == "speech_sample"
            assert play_url.startswith("file://")
            return [{"startMs": 0, "endMs": 360}]

    stdout = io.StringIO()
    exit_code = run(
        [
            "--benchmark-file",
            str(benchmark_path),
            "--diagnose-sample-id",
            "speech_sample",
            "--diagnose-from-ms",
            "0",
            "--diagnose-to-ms",
            "240",
            "--sample-rate",
            "8000",
            "--json",
        ],
        decode_audio=lambda url, sample_rate_hz, timeout_seconds: _pcm_for_amplitudes(
            [3000] * 18
        ),
        webrtc_vad_factory=lambda: SequenceVoiceActivityDetector([True] * 18),
        fsmn_detector_factory=lambda model: FakeFsmnDetector(),
        stdout=stdout,
    )

    assert exit_code == 0
    report = json.loads(stdout.getvalue())
    assert report["mode"] == "p1_vad_onset_diagnosis"
    assert report["sample"]["id"] == "speech_sample"
    assert report["providers"]["webrtc_main"]["firstCandidateMs"] == 160
    assert report["providers"]["fsmn_main"]["firstCandidateMs"] == 160
    assert report["providers"]["webrtc_fsmn_agreement"]["detectionLagMs"] == 160
    assert report["reasonSpans"]
    assert report["frames"][0]["offsetMs"] == 0
    assert report["frames"][0]["webrtcReason"] in {
        "below_vad_voiced_duration",
        "candidate_pending_duration",
        "speech_active_below_candidate_duration",
    }
    assert report["frames"][0]["agreementSpeech"] is True
