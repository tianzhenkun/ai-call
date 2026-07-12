from __future__ import annotations

import base64
import json
import queue
import threading
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from app.services.ai_call.audio_bridge import PcmAudioFrame


@dataclass(frozen=True, slots=True)
class SipVadShadowObservation:
    active: bool
    started: bool
    ended: bool
    duration_ms: int
    frame_duration_ms: int
    confidence: float | None = None
    analyzed: bool | None = None
    buffer_duration_ms: int | None = None
    window_start_ms: int | None = None
    window_end_ms: int | None = None
    detection_lag_ms: int | None = None
    speech_end_lag_ms: int | None = None
    detector: str = "unknown"
    error_type: str | None = None
    error_message: str | None = None


@dataclass(frozen=True, slots=True)
class SipVadShadowDecision:
    is_speech: bool
    confidence: float | None = None
    analyzed: bool | None = None
    buffer_duration_ms: int | None = None
    window_start_ms: int | None = None
    window_end_ms: int | None = None
    detection_lag_ms: int | None = None
    speech_end_lag_ms: int | None = None


class SipVadShadowDetectorProtocol(Protocol):
    def observe(
        self,
        call_id: str,
        frame: PcmAudioFrame,
        *,
        now: datetime,
        interruptible: bool,
    ) -> SipVadShadowObservation | list[SipVadShadowObservation]: ...

    def reset(self, call_id: str) -> None: ...


class FrameVoiceActivityDetectorProtocol(Protocol):
    def is_speech(self, frame: PcmAudioFrame) -> bool: ...


class SipVadShadowSidecarClientProtocol(Protocol):
    def detect(
        self,
        *,
        call_id: str,
        frame: PcmAudioFrame,
        now: datetime,
        interruptible: bool,
    ) -> SipVadShadowDecision: ...


class UnavailableSipVadShadowDetector:
    def __init__(self, *, detector_name: str, reason: str) -> None:
        self.detector_name = detector_name
        self.reason = reason

    def observe(
        self,
        call_id: str,
        frame: PcmAudioFrame,
        *,
        now: datetime,
        interruptible: bool,
    ) -> SipVadShadowObservation:
        _ = call_id, frame, now, interruptible
        raise RuntimeError(self.reason)

    def reset(self, call_id: str) -> None:
        _ = call_id


@dataclass(slots=True)
class _SipVadShadowState:
    active: bool = False
    duration_ms: int = 0


@dataclass(frozen=True, slots=True)
class _QueuedVadWorkItem:
    call_id: str
    frame: PcmAudioFrame | None
    now: datetime | None
    interruptible: bool = False
    reset: bool = False


@dataclass(frozen=True, slots=True)
class _QueuedVadResultItem:
    call_id: str
    observation: SipVadShadowObservation | None = None
    error: BaseException | None = None


class SipFrameVadShadowDetector:
    def __init__(
        self,
        *,
        vad: FrameVoiceActivityDetectorProtocol,
        detector_name: str,
    ) -> None:
        self._vad = vad
        self._detector_name = detector_name
        self._states: dict[str, _SipVadShadowState] = {}

    @property
    def detector_name(self) -> str:
        return self._detector_name

    def observe(
        self,
        call_id: str,
        frame: PcmAudioFrame,
        *,
        now: datetime,
        interruptible: bool,
    ) -> SipVadShadowObservation:
        _ = now, interruptible
        frame_duration_ms = self._frame_duration_ms(frame)
        state = self._states.setdefault(call_id, _SipVadShadowState())
        is_speech = self._vad.is_speech(frame)
        if is_speech:
            state.duration_ms += frame_duration_ms
            started = not state.active
            state.active = True
            return SipVadShadowObservation(
                active=True,
                started=started,
                ended=False,
                duration_ms=state.duration_ms,
                frame_duration_ms=frame_duration_ms,
                detector=self._detector_name,
            )

        ended = state.active
        duration_ms = state.duration_ms
        state.active = False
        state.duration_ms = 0
        return SipVadShadowObservation(
            active=False,
            started=False,
            ended=ended,
            duration_ms=duration_ms,
            frame_duration_ms=frame_duration_ms,
            detector=self._detector_name,
        )

    def reset(self, call_id: str) -> None:
        self._states.pop(call_id, None)

    @staticmethod
    def _frame_duration_ms(frame: PcmAudioFrame) -> int:
        if frame.sample_rate_hz <= 0 or frame.channels <= 0 or frame.sample_width_bytes <= 0:
            return 0
        sample_count = len(frame.data) // (frame.channels * frame.sample_width_bytes)
        return round(sample_count / frame.sample_rate_hz * 1000)


class QueuedSipVadShadowDetector:
    def __init__(
        self,
        *,
        client: SipVadShadowSidecarClientProtocol,
        detector_name: str,
        max_queue_size: int = 50,
    ) -> None:
        self.detector_name = detector_name
        self._client = client
        self._work_queue: queue.Queue[_QueuedVadWorkItem | None] = queue.Queue(
            maxsize=max(1, max_queue_size),
        )
        self._result_queue: queue.Queue[_QueuedVadResultItem] = queue.Queue()
        self._pending_observations: dict[str, list[SipVadShadowObservation]] = {}
        self._pending_errors: dict[str, BaseException] = {}
        self._states: dict[str, _SipVadShadowState] = {}
        self._closed = False
        self._thread: threading.Thread | None = None
        self._thread_lock = threading.Lock()

    def observe(
        self,
        call_id: str,
        frame: PcmAudioFrame,
        *,
        now: datetime,
        interruptible: bool,
    ) -> SipVadShadowObservation:
        self._drain_results()
        error = self._pending_errors.pop(call_id, None)
        if error is not None:
            raise error

        self._enqueue_work(
            _QueuedVadWorkItem(
                call_id=call_id,
                frame=frame,
                now=now,
                interruptible=interruptible,
            )
        )
        pending = self._pending_observations.get(call_id)
        if pending:
            observation = pending.pop(0)
            if not pending:
                self._pending_observations.pop(call_id, None)
            return observation
        return self._neutral_observation(frame)

    def reset(self, call_id: str) -> None:
        self._pending_observations.pop(call_id, None)
        self._pending_errors.pop(call_id, None)
        if self._closed or self._thread is None:
            return
        self._enqueue_work(
            _QueuedVadWorkItem(
                call_id=call_id,
                frame=None,
                now=None,
                reset=True,
            )
        )

    def close(self) -> None:
        self._closed = True
        try:
            self._work_queue.put(None, timeout=0.1)
        except queue.Full:
            return
        thread = self._thread
        if thread is not None:
            thread.join(timeout=1)

    def _enqueue_work(self, item: _QueuedVadWorkItem) -> None:
        if self._closed:
            return
        self._ensure_started()
        try:
            self._work_queue.put_nowait(item)
        except queue.Full:
            return

    def _ensure_started(self) -> None:
        if self._closed or self._thread is not None:
            return
        with self._thread_lock:
            if self._closed or self._thread is not None:
                return
            self._thread = threading.Thread(
                target=self._run,
                name=f"{self.detector_name}-worker",
                daemon=True,
            )
            self._thread.start()

    def _run(self) -> None:
        while True:
            item = self._work_queue.get()
            if item is None:
                return
            if item.reset:
                self._states.pop(item.call_id, None)
                continue
            if item.frame is None or item.now is None:
                continue
            try:
                decision = self._client.detect(
                    call_id=item.call_id,
                    frame=item.frame,
                    now=item.now,
                    interruptible=item.interruptible,
                )
                observation = self._observation_for_decision(
                    call_id=item.call_id,
                    frame=item.frame,
                    decision=decision,
                )
            except Exception as exc:
                self._result_queue.put(_QueuedVadResultItem(call_id=item.call_id, error=exc))
                continue
            if observation.started or observation.ended:
                self._result_queue.put(
                    _QueuedVadResultItem(
                        call_id=item.call_id,
                        observation=observation,
                    )
                )

    def _drain_results(self) -> None:
        while True:
            try:
                result = self._result_queue.get_nowait()
            except queue.Empty:
                return
            if result.error is not None:
                self._pending_errors[result.call_id] = result.error
                continue
            if result.observation is None:
                continue
            self._pending_observations.setdefault(result.call_id, []).append(
                result.observation,
            )

    def _observation_for_decision(
        self,
        *,
        call_id: str,
        frame: PcmAudioFrame,
        decision: SipVadShadowDecision,
    ) -> SipVadShadowObservation:
        frame_duration_ms = SipFrameVadShadowDetector._frame_duration_ms(frame)
        state = self._states.setdefault(call_id, _SipVadShadowState())
        if decision.is_speech:
            state.duration_ms += frame_duration_ms
            started = not state.active
            state.active = True
            return SipVadShadowObservation(
                active=True,
                started=started,
                ended=False,
                duration_ms=state.duration_ms,
                frame_duration_ms=frame_duration_ms,
                confidence=decision.confidence,
                analyzed=decision.analyzed,
                buffer_duration_ms=decision.buffer_duration_ms,
                window_start_ms=decision.window_start_ms,
                window_end_ms=decision.window_end_ms,
                detection_lag_ms=decision.detection_lag_ms,
                speech_end_lag_ms=decision.speech_end_lag_ms,
                detector=self.detector_name,
            )

        ended = state.active
        duration_ms = state.duration_ms
        state.active = False
        state.duration_ms = 0
        return SipVadShadowObservation(
            active=False,
            started=False,
            ended=ended,
            duration_ms=duration_ms,
            frame_duration_ms=frame_duration_ms,
            confidence=decision.confidence,
            analyzed=decision.analyzed,
            buffer_duration_ms=decision.buffer_duration_ms,
            window_start_ms=decision.window_start_ms,
            window_end_ms=decision.window_end_ms,
            detection_lag_ms=decision.detection_lag_ms,
            speech_end_lag_ms=decision.speech_end_lag_ms,
            detector=self.detector_name,
        )

    def _neutral_observation(self, frame: PcmAudioFrame) -> SipVadShadowObservation:
        return SipVadShadowObservation(
            active=False,
            started=False,
            ended=False,
            duration_ms=0,
            frame_duration_ms=SipFrameVadShadowDetector._frame_duration_ms(frame),
            detector=self.detector_name,
        )


class MultiSipVadShadowDetector:
    detector_name = "multi_shadow"

    def __init__(self, detectors: list[SipVadShadowDetectorProtocol]) -> None:
        self._detectors = tuple(detectors)
        self._failed_detectors_by_call_id: dict[str, set[str]] = {}

    @property
    def detector_names(self) -> tuple[str, ...]:
        return tuple(self._detector_name(detector) for detector in self._detectors)

    def observe(
        self,
        call_id: str,
        frame: PcmAudioFrame,
        *,
        now: datetime,
        interruptible: bool,
    ) -> list[SipVadShadowObservation]:
        observations: list[SipVadShadowObservation] = []
        failed_detectors = self._failed_detectors_by_call_id.setdefault(call_id, set())
        for detector in self._detectors:
            detector_name = self._detector_name(detector)
            if detector_name in failed_detectors:
                continue
            try:
                result = detector.observe(
                    call_id,
                    frame,
                    now=now,
                    interruptible=interruptible,
                )
            except Exception as exc:
                failed_detectors.add(detector_name)
                observations.append(
                    SipVadShadowObservation(
                        active=False,
                        started=False,
                        ended=False,
                        duration_ms=0,
                        frame_duration_ms=SipFrameVadShadowDetector._frame_duration_ms(frame),
                        detector=detector_name,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                )
                continue
            observations.extend(self._observations_from_result(result))
        if not failed_detectors:
            self._failed_detectors_by_call_id.pop(call_id, None)
        return observations

    def reset(self, call_id: str) -> None:
        self._failed_detectors_by_call_id.pop(call_id, None)
        for detector in self._detectors:
            detector.reset(call_id)

    def close(self) -> None:
        for detector in self._detectors:
            close = getattr(detector, "close", None)
            if callable(close):
                close()

    @staticmethod
    def _observations_from_result(
        result: SipVadShadowObservation | list[SipVadShadowObservation],
    ) -> list[SipVadShadowObservation]:
        if isinstance(result, SipVadShadowObservation):
            return [result]
        return list(result)

    @staticmethod
    def _detector_name(detector: SipVadShadowDetectorProtocol) -> str:
        return str(getattr(detector, "detector_name", type(detector).__name__))


class FsmnVadSidecarClient:
    def __init__(self, *, endpoint: str, model: str, timeout_seconds: float) -> None:
        self.endpoint = endpoint
        self.model = model
        self.timeout_seconds = max(0.05, timeout_seconds)

    def detect(
        self,
        *,
        call_id: str,
        frame: PcmAudioFrame,
        now: datetime,
        interruptible: bool,
    ) -> SipVadShadowDecision:
        payload = {
            "callId": call_id,
            "model": self.model,
            "timestamp": now.isoformat(),
            "sampleRateHz": frame.sample_rate_hz,
            "channels": frame.channels,
            "sampleWidthBytes": frame.sample_width_bytes,
            "interruptible": interruptible,
            "audioBase64": base64.b64encode(frame.data).decode("ascii"),
        }
        request = urllib.request.Request(
            self.endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
            response_payload = json.loads(response.read().decode("utf-8") or "{}")
        return self._decision_from_payload(response_payload)

    @staticmethod
    def _decision_from_payload(payload: Any) -> SipVadShadowDecision:
        if not isinstance(payload, dict):
            raise RuntimeError("FSMN VAD sidecar response must be a JSON object")
        speech = payload.get("speech", payload.get("isSpeech", payload.get("active")))
        if not isinstance(speech, bool):
            raise RuntimeError("FSMN VAD sidecar response missing boolean speech")
        confidence_value = payload.get("confidence")
        confidence = (
            float(confidence_value)
            if isinstance(confidence_value, (int, float))
            else None
        )
        buffer_duration_ms = _optional_int(
            _first_present(payload.get("bufferDurationMs"), payload.get("buffer_duration_ms"))
        )
        window_start_ms = _optional_int(
            _first_present(payload.get("windowStartMs"), payload.get("window_start_ms"))
        )
        window_end_ms = _optional_int(
            _first_present(payload.get("windowEndMs"), payload.get("window_end_ms"))
        )
        if window_start_ms is None or window_end_ms is None:
            window_start_ms, window_end_ms = _latest_window(payload.get("windows"))
        detection_lag_ms = _lag_ms(buffer_duration_ms, window_start_ms)
        speech_end_lag_ms = _lag_ms(buffer_duration_ms, window_end_ms)
        analyzed_value = payload.get("analyzed")
        return SipVadShadowDecision(
            is_speech=speech,
            confidence=confidence,
            analyzed=analyzed_value if isinstance(analyzed_value, bool) else None,
            buffer_duration_ms=buffer_duration_ms,
            window_start_ms=window_start_ms,
            window_end_ms=window_end_ms,
            detection_lag_ms=detection_lag_ms,
            speech_end_lag_ms=speech_end_lag_ms,
        )


def _latest_window(value: Any) -> tuple[int | None, int | None]:
    if not isinstance(value, list):
        return None, None
    latest_start_ms: int | None = None
    latest_end_ms: int | None = None
    for item in value:
        if not isinstance(item, dict):
            continue
        start_ms = _optional_int(_first_present(item.get("startMs"), item.get("start_ms")))
        end_ms = _optional_int(_first_present(item.get("endMs"), item.get("end_ms")))
        if start_ms is None or end_ms is None:
            continue
        if latest_end_ms is None or end_ms >= latest_end_ms:
            latest_start_ms = start_ms
            latest_end_ms = end_ms
    return latest_start_ms, latest_end_ms


def _lag_ms(buffer_duration_ms: int | None, point_ms: int | None) -> int | None:
    if buffer_duration_ms is None or point_ms is None:
        return None
    return max(0, buffer_duration_ms - point_ms)


def _optional_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None
