from __future__ import annotations

import math
import struct
from dataclasses import dataclass
from datetime import datetime

from app.services.ai_call.audio_bridge import PcmAudioFrame


@dataclass(frozen=True, slots=True)
class SipBargeInObservation:
    active: bool
    candidate: bool
    rms_dbfs: float | None
    speech_duration_ms: int
    frame_duration_ms: int
    reason: str


@dataclass(slots=True)
class _SipBargeInState:
    speech_duration_ms: int = 0
    candidate_raised: bool = False
    last_candidate_at: datetime | None = None


class SipBargeInDetector:
    def __init__(
        self,
        *,
        min_rms_dbfs: float = -35.0,
        min_speech_duration_ms: int = 220,
    ) -> None:
        self.min_rms_dbfs = min_rms_dbfs
        self.min_speech_duration_ms = max(20, min_speech_duration_ms)
        self._states: dict[str, _SipBargeInState] = {}

    def observe(
        self,
        call_id: str,
        frame: PcmAudioFrame,
        *,
        now: datetime,
        interruptible: bool,
    ) -> SipBargeInObservation:
        frame_duration_ms = self._frame_duration_ms(frame)
        if not interruptible:
            self.reset(call_id)
            return SipBargeInObservation(
                active=False,
                candidate=False,
                rms_dbfs=None,
                speech_duration_ms=0,
                frame_duration_ms=frame_duration_ms,
                reason="not_interruptible",
            )

        rms_dbfs = self._pcm16_rms_dbfs(frame)
        if rms_dbfs is None:
            self.reset(call_id)
            return SipBargeInObservation(
                active=False,
                candidate=False,
                rms_dbfs=None,
                speech_duration_ms=0,
                frame_duration_ms=frame_duration_ms,
                reason="unsupported_frame",
            )

        if rms_dbfs < self.min_rms_dbfs:
            self.reset(call_id)
            return SipBargeInObservation(
                active=False,
                candidate=False,
                rms_dbfs=rms_dbfs,
                speech_duration_ms=0,
                frame_duration_ms=frame_duration_ms,
                reason="below_min_rms",
            )

        state = self._states.setdefault(call_id, _SipBargeInState())
        state.speech_duration_ms += frame_duration_ms
        candidate = (
            not state.candidate_raised
            and state.speech_duration_ms >= self.min_speech_duration_ms
        )
        if candidate:
            state.candidate_raised = True
            state.last_candidate_at = now

        return SipBargeInObservation(
            active=True,
            candidate=candidate,
            rms_dbfs=rms_dbfs,
            speech_duration_ms=state.speech_duration_ms,
            frame_duration_ms=frame_duration_ms,
            reason=(
                "sip_uplink_speech_during_ai_audio"
                if candidate
                else "speech_active_below_candidate_duration"
            ),
        )

    def reset(self, call_id: str) -> None:
        self._states.pop(call_id, None)

    @staticmethod
    def _frame_duration_ms(frame: PcmAudioFrame) -> int:
        if frame.sample_rate_hz <= 0 or frame.channels <= 0 or frame.sample_width_bytes <= 0:
            return 0
        sample_count = len(frame.data) // (frame.channels * frame.sample_width_bytes)
        return round(sample_count * 1000 / frame.sample_rate_hz)

    @staticmethod
    def _pcm16_rms_dbfs(frame: PcmAudioFrame) -> float | None:
        if frame.channels != 1 or frame.sample_width_bytes != 2 or len(frame.data) < 2:
            return None
        usable_size = len(frame.data) - (len(frame.data) % 2)
        if usable_size <= 0:
            return None

        total = 0
        count = 0
        for (sample,) in struct.iter_unpack("<h", frame.data[:usable_size]):
            total += sample * sample
            count += 1
        if count <= 0:
            return None
        rms = math.sqrt(total / count)
        if rms <= 0:
            return float("-inf")
        return 20 * math.log10(rms / 32768.0)
