from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from app.services.ai_call.audio_bridge import PcmAudioFrame
from app.services.ai_call.sip_barge_in import (
    SipBargeInConfig,
    SipBargeInDetector,
    SipBargeInObservation,
    VoiceActivityDetectorProtocol,
)


@dataclass(frozen=True, slots=True)
class SipBargeInReplayFrame:
    offset_ms: int
    frame: PcmAudioFrame
    interruptible: bool = True


@dataclass(frozen=True, slots=True)
class SpeechWindow:
    start_ms: int
    end_ms: int


class SequenceVoiceActivityDetector:
    def __init__(self, decisions: Sequence[bool]) -> None:
        if not decisions:
            raise ValueError("decisions must not be empty")
        self._decisions = list(decisions)
        self._calls = 0

    def is_speech(self, frame: PcmAudioFrame) -> bool:
        _ = frame
        decision = self._decisions[min(self._calls, len(self._decisions) - 1)]
        self._calls += 1
        return decision


class WindowVoiceActivityDetector:
    def __init__(self, windows: Sequence[SpeechWindow]) -> None:
        self._windows = list(windows)
        self._position_ms = 0

    def is_speech(self, frame: PcmAudioFrame) -> bool:
        frame_duration_ms = _frame_duration_ms(frame)
        start_ms = self._position_ms
        end_ms = start_ms + frame_duration_ms
        self._position_ms = end_ms
        return any(window.start_ms < end_ms and window.end_ms > start_ms for window in self._windows)


@dataclass(frozen=True, slots=True)
class SipVadReplayProvider:
    name: str
    vad_factory: Callable[[], VoiceActivityDetectorProtocol]


@dataclass(frozen=True, slots=True)
class SipBargeInReplayEvent:
    provider: str
    offset_ms: int
    frame_duration_ms: int
    candidate_class: str | None
    reason: str
    observation: SipBargeInObservation


@dataclass(frozen=True, slots=True)
class SipBargeInProviderReplayReport:
    provider: str
    observed_frames: int
    candidate_events: list[SipBargeInReplayEvent]
    pre_stop_events: list[SipBargeInReplayEvent]
    last_reason: str | None
    last_observation: SipBargeInObservation | None


@dataclass(frozen=True, slots=True)
class SipBargeInReplayReport:
    call_id: str
    provider_reports: dict[str, SipBargeInProviderReplayReport]


def pcm16_mono_to_replay_frames(
    pcm16_mono: bytes,
    *,
    sample_rate_hz: int,
    frame_duration_ms: int = 20,
) -> list[SipBargeInReplayFrame]:
    if sample_rate_hz <= 0:
        raise ValueError("sample_rate_hz must be greater than 0")
    if frame_duration_ms <= 0:
        raise ValueError("frame_duration_ms must be greater than 0")

    samples_per_frame = sample_rate_hz * frame_duration_ms // 1000
    if samples_per_frame <= 0:
        raise ValueError("frame_duration_ms is too small for sample_rate_hz")

    bytes_per_frame = samples_per_frame * 2
    full_frame_count = len(pcm16_mono) // bytes_per_frame
    frames: list[SipBargeInReplayFrame] = []
    for index in range(full_frame_count):
        start = index * bytes_per_frame
        end = start + bytes_per_frame
        frames.append(
            SipBargeInReplayFrame(
                offset_ms=index * frame_duration_ms,
                frame=PcmAudioFrame(
                    data=pcm16_mono[start:end],
                    sample_rate_hz=sample_rate_hz,
                    channels=1,
                    sample_width_bytes=2,
                ),
            )
        )
    return frames


def replay_sip_barge_in_vad_providers(
    *,
    call_id: str,
    frames: Sequence[SipBargeInReplayFrame],
    providers: Sequence[SipVadReplayProvider],
    config: SipBargeInConfig | None = None,
    started_at: datetime | None = None,
) -> SipBargeInReplayReport:
    base_time = started_at or datetime(1970, 1, 1, tzinfo=timezone.utc)
    replay_config = config or SipBargeInConfig()
    provider_reports: dict[str, SipBargeInProviderReplayReport] = {}

    for provider in providers:
        detector = SipBargeInDetector(
            config=replay_config,
            vad=provider.vad_factory(),
        )
        candidate_events: list[SipBargeInReplayEvent] = []
        pre_stop_events: list[SipBargeInReplayEvent] = []
        pre_stop_reported = False
        last_observation: SipBargeInObservation | None = None

        for replay_frame in frames:
            observation = detector.observe(
                call_id,
                replay_frame.frame,
                now=base_time + timedelta(milliseconds=replay_frame.offset_ms),
                interruptible=replay_frame.interruptible,
            )
            last_observation = observation

            if observation.candidate:
                candidate_events.append(
                    _replay_event(
                        provider=provider.name,
                        replay_frame=replay_frame,
                        observation=observation,
                    )
                )

            if not observation.active:
                pre_stop_reported = False
                continue

            if pre_stop_reported:
                continue

            if _has_pre_stop_evidence(detector, call_id, observation):
                pre_stop_events.append(
                    _replay_event(
                        provider=provider.name,
                        replay_frame=replay_frame,
                        observation=observation,
                    )
                )
                pre_stop_reported = True

        provider_reports[provider.name] = SipBargeInProviderReplayReport(
            provider=provider.name,
            observed_frames=len(frames),
            candidate_events=candidate_events,
            pre_stop_events=pre_stop_events,
            last_reason=last_observation.reason if last_observation is not None else None,
            last_observation=last_observation,
        )

    return SipBargeInReplayReport(
        call_id=call_id,
        provider_reports=provider_reports,
    )


def _has_pre_stop_evidence(
    detector: SipBargeInDetector,
    call_id: str,
    observation: SipBargeInObservation,
) -> bool:
    if observation.candidate_class not in {
        "stable_speech_candidate",
        "strong_short_speech_candidate",
    }:
        return False
    return detector.has_fast_pre_stop_local_speech(call_id) or detector.has_pre_stop_local_speech(
        call_id
    )


def _replay_event(
    *,
    provider: str,
    replay_frame: SipBargeInReplayFrame,
    observation: SipBargeInObservation,
) -> SipBargeInReplayEvent:
    return SipBargeInReplayEvent(
        provider=provider,
        offset_ms=replay_frame.offset_ms,
        frame_duration_ms=observation.frame_duration_ms,
        candidate_class=observation.candidate_class,
        reason=observation.reason,
        observation=observation,
    )


def _frame_duration_ms(frame: PcmAudioFrame) -> int:
    if frame.sample_rate_hz <= 0 or frame.channels <= 0 or frame.sample_width_bytes <= 0:
        return 0
    sample_count = len(frame.data) // (frame.channels * frame.sample_width_bytes)
    return round(sample_count * 1000 / frame.sample_rate_hz)
