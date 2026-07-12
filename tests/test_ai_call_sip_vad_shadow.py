from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from typing import Any

from app.services.ai_call.audio_bridge import PcmAudioFrame
from app.services.ai_call.sip_vad_shadow import (
    FsmnVadSidecarClient,
    MultiSipVadShadowDetector,
    QueuedSipVadShadowDetector,
    SipVadShadowDecision,
    SipVadShadowObservation,
)


def _frame(*, amplitude: int = 1000, duration_ms: int = 20) -> PcmAudioFrame:
    sample_rate_hz = 8000
    sample_count = sample_rate_hz * duration_ms // 1000
    sample = int(amplitude).to_bytes(2, "little", signed=True)
    return PcmAudioFrame(
        data=sample * sample_count,
        sample_rate_hz=sample_rate_hz,
        channels=1,
        sample_width_bytes=2,
    )


class FakeSidecarClient:
    def __init__(self, decisions: list[SipVadShadowDecision]) -> None:
        self.decisions = list(decisions)
        self.calls = 0
        self.called = threading.Event()
        self.release = threading.Event()
        self.release.set()

    def detect(
        self,
        *,
        call_id: str,
        frame: PcmAudioFrame,
        now: datetime,
        interruptible: bool,
    ) -> SipVadShadowDecision:
        _ = call_id, frame, now, interruptible
        self.called.set()
        self.release.wait(timeout=1)
        decision = self.decisions[min(self.calls, len(self.decisions) - 1)]
        self.calls += 1
        return decision


class StaticShadowDetector:
    def __init__(self, detector_name: str, observations: list[SipVadShadowObservation]) -> None:
        self.detector_name = detector_name
        self.observations = list(observations)
        self.calls = 0
        self.reset_call_ids: list[str] = []

    def observe(
        self,
        call_id: str,
        frame: PcmAudioFrame,
        *,
        now: datetime,
        interruptible: bool,
    ) -> SipVadShadowObservation:
        _ = call_id, frame, now, interruptible
        observation = self.observations[min(self.calls, len(self.observations) - 1)]
        self.calls += 1
        return observation

    def reset(self, call_id: str) -> None:
        self.reset_call_ids.append(call_id)


class FailingShadowDetector:
    def __init__(self) -> None:
        self.detector_name = "fsmn_shadow"
        self.calls = 0
        self.reset_call_ids: list[str] = []

    def observe(
        self,
        call_id: str,
        frame: PcmAudioFrame,
        *,
        now: datetime,
        interruptible: bool,
    ) -> SipVadShadowObservation:
        _ = call_id, frame, now, interruptible
        self.calls += 1
        raise RuntimeError("sidecar unavailable")

    def reset(self, call_id: str) -> None:
        self.reset_call_ids.append(call_id)


def test_multi_sip_vad_shadow_detector_returns_observations_from_all_children() -> None:
    detector = MultiSipVadShadowDetector([
        StaticShadowDetector(
            "webrtc_shadow",
            [
                SipVadShadowObservation(
                    active=True,
                    started=True,
                    ended=False,
                    duration_ms=20,
                    frame_duration_ms=20,
                    detector="webrtc_shadow",
                )
            ],
        ),
        StaticShadowDetector(
            "fsmn_shadow",
            [
                SipVadShadowObservation(
                    active=True,
                    started=True,
                    ended=False,
                    duration_ms=20,
                    frame_duration_ms=20,
                    confidence=0.89,
                    detector="fsmn_shadow",
                )
            ],
        ),
    ])

    observations = detector.observe(
        "call_multi_shadow",
        _frame(),
        now=datetime.now(timezone.utc),
        interruptible=True,
    )

    assert [observation.detector for observation in observations] == [
        "webrtc_shadow",
        "fsmn_shadow",
    ]
    assert all(observation.started for observation in observations)


def test_multi_sip_vad_shadow_detector_reports_one_child_error_without_stopping_others() -> None:
    healthy = StaticShadowDetector(
        "webrtc_shadow",
        [
            SipVadShadowObservation(
                active=True,
                started=True,
                ended=False,
                duration_ms=20,
                frame_duration_ms=20,
                detector="webrtc_shadow",
            ),
            SipVadShadowObservation(
                active=False,
                started=False,
                ended=True,
                duration_ms=20,
                frame_duration_ms=20,
                detector="webrtc_shadow",
            ),
        ],
    )
    failing = FailingShadowDetector()
    detector = MultiSipVadShadowDetector([healthy, failing])
    call_id = "call_multi_shadow_error"

    first = detector.observe(call_id, _frame(), now=datetime.now(timezone.utc), interruptible=True)
    second = detector.observe(call_id, _frame(), now=datetime.now(timezone.utc), interruptible=True)

    assert [(observation.detector, observation.error_type) for observation in first] == [
        ("webrtc_shadow", None),
        ("fsmn_shadow", "RuntimeError"),
    ]
    assert [(observation.detector, observation.error_type) for observation in second] == [
        ("webrtc_shadow", None),
    ]
    assert healthy.calls == 2
    assert failing.calls == 1


def test_queued_sip_vad_shadow_detector_returns_without_waiting_for_sidecar() -> None:
    client = FakeSidecarClient([SipVadShadowDecision(is_speech=True)])
    client.release.clear()
    detector = QueuedSipVadShadowDetector(
        client=client,
        detector_name="fsmn_shadow",
        max_queue_size=2,
    )

    started_at = time.monotonic()
    observation = detector.observe(
        "call_shadow_nonblocking",
        _frame(),
        now=datetime.now(timezone.utc),
        interruptible=True,
    )
    elapsed_ms = (time.monotonic() - started_at) * 1000

    try:
        assert elapsed_ms < 50
        assert observation.started is False
        assert observation.ended is False
        assert client.called.wait(timeout=1)
    finally:
        client.release.set()
        detector.close()


def test_queued_sip_vad_shadow_detector_emits_sidecar_speech_boundaries() -> None:
    client = FakeSidecarClient([
        SipVadShadowDecision(
            is_speech=True,
            confidence=0.88,
            analyzed=True,
            buffer_duration_ms=1200,
            window_start_ms=820,
            window_end_ms=1100,
            detection_lag_ms=380,
            speech_end_lag_ms=100,
        ),
        SipVadShadowDecision(is_speech=False, confidence=0.12),
    ])
    detector = QueuedSipVadShadowDetector(
        client=client,
        detector_name="fsmn_shadow",
        max_queue_size=4,
    )
    call_id = "call_shadow_boundaries"

    try:
        first = detector.observe(
            call_id,
            _frame(),
            now=datetime.now(timezone.utc),
            interruptible=True,
        )
        assert first.started is False
        assert client.called.wait(timeout=1)
        time.sleep(0.02)

        started = detector.observe(
            call_id,
            _frame(amplitude=1200),
            now=datetime.now(timezone.utc),
            interruptible=True,
        )
        assert started.started is True
        assert started.active is True
        assert started.duration_ms == 20
        assert started.confidence == 0.88
        assert started.detector == "fsmn_shadow"
        assert started.analyzed is True
        assert started.buffer_duration_ms == 1200
        assert started.window_start_ms == 820
        assert started.window_end_ms == 1100
        assert started.detection_lag_ms == 380
        assert started.speech_end_lag_ms == 100
        time.sleep(0.02)

        ended = detector.observe(
            call_id,
            _frame(amplitude=0),
            now=datetime.now(timezone.utc),
            interruptible=True,
        )
        assert ended.ended is True
        assert ended.active is False
        assert ended.duration_ms == 20
        assert ended.confidence == 0.12
    finally:
        detector.close()


def test_fsmn_vad_sidecar_client_posts_pcm_frame_and_parses_decision(
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    class FakeResponse:
        def __enter__(self) -> FakeResponse:
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

        def read(self) -> bytes:
            return (
                b'{"speech": true, "confidence": 0.91, "analyzed": true, '
                b'"bufferDurationMs": 1200, '
                b'"windows": [{"startMs": 300, "endMs": 400}, '
                b'{"startMs": 820, "endMs": 1100}]}'
            )

    def fake_urlopen(request, timeout: float):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        captured["method"] = request.get_method()
        captured["payload"] = json.loads(request.data.decode("utf-8"))
        return FakeResponse()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    client = FsmnVadSidecarClient(
        endpoint="http://127.0.0.1:19111/vad",
        model="iic/fsmn",
        timeout_seconds=0.2,
    )

    decision = client.detect(
        call_id="call_http_fsmn",
        frame=_frame(amplitude=1234),
        now=datetime(2026, 7, 3, tzinfo=timezone.utc),
        interruptible=True,
    )

    assert decision.is_speech is True
    assert decision.confidence == 0.91
    assert decision.analyzed is True
    assert decision.buffer_duration_ms == 1200
    assert decision.window_start_ms == 820
    assert decision.window_end_ms == 1100
    assert decision.detection_lag_ms == 380
    assert decision.speech_end_lag_ms == 100
    assert captured["url"] == "http://127.0.0.1:19111/vad"
    assert captured["timeout"] == 0.2
    assert captured["method"] == "POST"
    assert captured["payload"]["callId"] == "call_http_fsmn"
    assert captured["payload"]["model"] == "iic/fsmn"
    assert captured["payload"]["sampleRateHz"] == 8000
    assert captured["payload"]["channels"] == 1
    assert captured["payload"]["sampleWidthBytes"] == 2
    assert captured["payload"]["interruptible"] is True
    assert captured["payload"]["audioBase64"]
