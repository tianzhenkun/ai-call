from __future__ import annotations

import math
import struct
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from app.services.ai_call.audio_bridge import PcmAudioFrame


@dataclass(frozen=True, slots=True)
class SipBargeInConfig:
    rms_threshold_dbfs: float = -36.0
    snr_threshold_db: float = 10.0
    vad_voiced_duration_ms: int = 120
    candidate_min_duration_ms: int = 180
    pre_stop_min_duration_ms: int = 240
    short_speech_min_duration_ms: int = 180
    impulse_noise_max_duration_ms: int = 120
    clean_window_ms: int = 300
    max_hold_ms: int = 500
    echo_tail_window_ms: int = 500
    noise_floor_initial_dbfs: float = -50.0
    impulse_peak_rms_gap_db: float = 18.0
    strong_short_min_rms_dbfs: float = -16.0
    strong_short_max_rms_dbfs: float = -9.0
    strong_short_extra_snr_db: float = 10.0
    hot_burst_tail_rms_drop_db: float = 18.0
    clipped_hot_peak_dbfs: float = -3.0
    clipped_hot_min_rms_dbfs: float = -12.0
    low_confidence_flat_max_rms_dbfs: float = -29.0
    low_confidence_flat_max_snr_db: float = 20.0
    low_confidence_flat_max_range_db: float = 3.0
    weak_flat_turn_evidence_max_rms_dbfs: float = -29.0
    weak_flat_turn_evidence_max_snr_db: float = 24.0
    weak_flat_turn_evidence_max_range_db: float = 3.0
    turn_taking_min_range_db: float = 4.0
    turn_taking_high_confidence_rms_dbfs: float = -24.0
    turn_taking_high_confidence_snr_db: float = 26.0
    turn_taking_extended_duration_ms: int = 720
    non_speech_envelope_min_range_db: float = 7.0
    non_speech_envelope_min_jump_db: float = 5.0
    non_speech_envelope_min_jump_count: int = 2
    non_speech_envelope_min_direction_changes: int = 4


class VoiceActivityDetectorProtocol(Protocol):
    def is_speech(self, frame: PcmAudioFrame) -> bool: ...


class EnergyVoiceActivityDetector:
    def is_speech(self, frame: PcmAudioFrame) -> bool:
        _ = frame
        return True


class WebRtcVadAdapter:
    def __init__(self, mode: int = 2) -> None:
        import webrtcvad

        self._vad = webrtcvad.Vad(mode)

    def is_speech(self, frame: PcmAudioFrame) -> bool:
        if frame.channels != 1 or frame.sample_width_bytes != 2:
            return False
        return self._vad.is_speech(frame.data, frame.sample_rate_hz)


@dataclass(frozen=True, slots=True)
class SipBargeInObservation:
    active: bool
    candidate: bool
    rms_dbfs: float | None
    noise_floor_dbfs: float | None
    snr_db: float | None
    peak_dbfs: float | None
    vad_voiced_ms: int
    candidate_duration_ms: int
    speech_duration_ms: int
    frame_duration_ms: int
    candidate_class: str | None
    reason: str


@dataclass(slots=True)
class _SipBargeInState:
    speech_duration_ms: int = 0
    vad_voiced_ms: int = 0
    candidate_duration_ms: int = 0
    noise_floor_dbfs: float = -50.0
    latest_snr_db: float | None = None
    max_snr_db: float | None = None
    latest_rms_dbfs: float | None = None
    previous_rms_dbfs: float | None = None
    min_rms_dbfs: float | None = None
    max_rms_dbfs: float | None = None
    max_peak_dbfs: float | None = None
    latest_peak_dbfs: float | None = None
    latest_candidate_class: str | None = None
    large_rms_jump_count: int = 0
    rms_direction_changes: int = 0
    last_rms_direction: int = 0
    hot_onset_voiced_ms: int = 0
    clipped_hot_onset_detected: bool = False
    short_hot_onset_drop_detected: bool = False
    suppress_until_silence: bool = False
    suppression_reason: str | None = None
    candidate_raised: bool = False
    last_candidate_at: datetime | None = None
    first_voiced_at: datetime | None = None
    previous_voiced_at: datetime | None = None
    max_voiced_frame_gap_ms: int = 0


class SipBargeInDetector:
    def __init__(
        self,
        *,
        config: SipBargeInConfig | None = None,
        vad: VoiceActivityDetectorProtocol | None = None,
        min_rms_dbfs: float = -35.0,
        min_speech_duration_ms: int = 220,
    ) -> None:
        self.config = config or SipBargeInConfig(
            rms_threshold_dbfs=min_rms_dbfs,
            candidate_min_duration_ms=max(20, min_speech_duration_ms),
        )
        self.min_rms_dbfs = self.config.rms_threshold_dbfs
        self.min_speech_duration_ms = max(20, self.config.candidate_min_duration_ms)
        self.vad = vad or EnergyVoiceActivityDetector()
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
        rms_dbfs = self._pcm16_rms_dbfs(frame)
        peak_dbfs = self._pcm16_peak_dbfs(frame)
        if rms_dbfs is None:
            self.reset(call_id)
            return SipBargeInObservation(
                active=False,
                candidate=False,
                rms_dbfs=None,
                noise_floor_dbfs=None,
                snr_db=None,
                peak_dbfs=None,
                vad_voiced_ms=0,
                candidate_duration_ms=0,
                speech_duration_ms=0,
                frame_duration_ms=frame_duration_ms,
                candidate_class=None,
                reason="unsupported_frame",
            )

        state = self._states.setdefault(
            call_id,
            _SipBargeInState(noise_floor_dbfs=self.config.noise_floor_initial_dbfs),
        )
        noise_floor_dbfs = state.noise_floor_dbfs
        snr_db = self._db_delta(rms_dbfs, noise_floor_dbfs)
        is_voiced = self._is_vad_speech(frame)

        if not interruptible:
            if not is_voiced and not self._is_impulse_noise(
                frame_duration_ms=frame_duration_ms,
                rms_dbfs=rms_dbfs,
                peak_dbfs=peak_dbfs,
            ):
                self._update_noise_floor(state, rms_dbfs)
            self._reset_activity(call_id)
            state = self._states.get(call_id)
            noise_floor_dbfs = state.noise_floor_dbfs if state is not None else noise_floor_dbfs
            snr_db = self._db_delta(rms_dbfs, noise_floor_dbfs)
            return SipBargeInObservation(
                active=False,
                candidate=False,
                rms_dbfs=rms_dbfs,
                noise_floor_dbfs=noise_floor_dbfs,
                snr_db=snr_db,
                peak_dbfs=peak_dbfs,
                vad_voiced_ms=0,
                candidate_duration_ms=0,
                speech_duration_ms=0,
                frame_duration_ms=frame_duration_ms,
                candidate_class=None,
                reason="not_interruptible",
            )

        if self._is_impulse_noise(
            frame_duration_ms=frame_duration_ms,
            rms_dbfs=rms_dbfs,
            peak_dbfs=peak_dbfs,
        ):
            self._reset_activity(call_id)
            return SipBargeInObservation(
                active=False,
                candidate=False,
                rms_dbfs=rms_dbfs,
                noise_floor_dbfs=noise_floor_dbfs,
                snr_db=snr_db,
                peak_dbfs=peak_dbfs,
                vad_voiced_ms=0,
                candidate_duration_ms=0,
                speech_duration_ms=0,
                frame_duration_ms=frame_duration_ms,
                candidate_class="impulse_noise",
                reason="impulse_noise",
            )

        if rms_dbfs < self.min_rms_dbfs:
            if not is_voiced:
                self._update_noise_floor(state, rms_dbfs)
            self._reset_activity(call_id)
            return SipBargeInObservation(
                active=False,
                candidate=False,
                rms_dbfs=rms_dbfs,
                noise_floor_dbfs=noise_floor_dbfs,
                snr_db=snr_db,
                peak_dbfs=peak_dbfs,
                vad_voiced_ms=0,
                candidate_duration_ms=0,
                speech_duration_ms=0,
                frame_duration_ms=frame_duration_ms,
                candidate_class=None,
                reason="below_min_rms",
            )

        if snr_db is None or snr_db < self.config.snr_threshold_db:
            state.latest_rms_dbfs = rms_dbfs
            state.latest_snr_db = snr_db
            state.latest_peak_dbfs = peak_dbfs
            return SipBargeInObservation(
                active=False,
                candidate=False,
                rms_dbfs=rms_dbfs,
                noise_floor_dbfs=noise_floor_dbfs,
                snr_db=snr_db,
                peak_dbfs=peak_dbfs,
                vad_voiced_ms=state.vad_voiced_ms,
                candidate_duration_ms=state.candidate_duration_ms,
                speech_duration_ms=state.speech_duration_ms,
                frame_duration_ms=frame_duration_ms,
                candidate_class=None,
                reason="below_min_snr",
            )

        if not is_voiced:
            if state.suppress_until_silence:
                self._reset_activity(call_id)
            return SipBargeInObservation(
                active=False,
                candidate=False,
                rms_dbfs=rms_dbfs,
                noise_floor_dbfs=noise_floor_dbfs,
                snr_db=snr_db,
                peak_dbfs=peak_dbfs,
                vad_voiced_ms=state.vad_voiced_ms,
                candidate_duration_ms=state.candidate_duration_ms,
                speech_duration_ms=state.speech_duration_ms,
                frame_duration_ms=frame_duration_ms,
                candidate_class=None,
                reason="vad_not_voiced",
            )

        state.speech_duration_ms += frame_duration_ms
        state.vad_voiced_ms += frame_duration_ms
        state.candidate_duration_ms += frame_duration_ms
        state.latest_rms_dbfs = rms_dbfs
        state.latest_snr_db = snr_db
        state.max_snr_db = snr_db if state.max_snr_db is None else max(state.max_snr_db, snr_db)
        state.latest_peak_dbfs = peak_dbfs
        state.max_peak_dbfs = (
            peak_dbfs
            if state.max_peak_dbfs is None
            else max(state.max_peak_dbfs, peak_dbfs)
        )
        self._update_voice_timing(state, now)
        self._update_energy_envelope(state, rms_dbfs, frame_duration_ms=frame_duration_ms)
        if self._has_clipped_hot_onset(state):
            state.clipped_hot_onset_detected = True
        if (
            self._has_short_hot_onset_drop(state)
            and 0 < state.hot_onset_voiced_ms <= self.config.impulse_noise_max_duration_ms
        ):
            state.short_hot_onset_drop_detected = True

        suppression_reason = state.suppression_reason or self._non_speech_suppression_reason(state)
        if state.suppress_until_silence or suppression_reason is not None:
            state.suppress_until_silence = True
            state.suppression_reason = suppression_reason or state.suppression_reason
            return SipBargeInObservation(
                active=False,
                candidate=False,
                rms_dbfs=rms_dbfs,
                noise_floor_dbfs=noise_floor_dbfs,
                snr_db=snr_db,
                peak_dbfs=peak_dbfs,
                vad_voiced_ms=state.vad_voiced_ms,
                candidate_duration_ms=state.candidate_duration_ms,
                speech_duration_ms=state.speech_duration_ms,
                frame_duration_ms=frame_duration_ms,
                candidate_class=None,
                reason=state.suppression_reason or "non_speech_energy_envelope",
            )

        candidate_class = self._candidate_class(state)
        state.latest_candidate_class = candidate_class
        candidate = (
            not state.candidate_raised
            and candidate_class in {
                "stable_speech_candidate",
                "strong_short_speech_candidate",
            }
        )
        if candidate:
            state.candidate_raised = True
            state.last_candidate_at = now

        return SipBargeInObservation(
            active=True,
            candidate=candidate,
            rms_dbfs=rms_dbfs,
            noise_floor_dbfs=noise_floor_dbfs,
            snr_db=snr_db,
            peak_dbfs=peak_dbfs,
            vad_voiced_ms=state.vad_voiced_ms,
            candidate_duration_ms=state.candidate_duration_ms,
            speech_duration_ms=state.speech_duration_ms,
            frame_duration_ms=frame_duration_ms,
            candidate_class=candidate_class,
            reason=(
                "sip_uplink_speech_during_ai_audio"
                if candidate
                else "speech_active_below_candidate_duration"
            ),
        )

    def reset(self, call_id: str) -> None:
        self._states.pop(call_id, None)

    def reset_activity(self, call_id: str) -> None:
        self._reset_activity(call_id)

    def _reset_activity(self, call_id: str) -> None:
        state = self._states.get(call_id)
        if state is None:
            return
        self._states[call_id] = _SipBargeInState(noise_floor_dbfs=state.noise_floor_dbfs)

    def has_stable_voice_evidence(self, call_id: str) -> bool:
        state = self._states.get(call_id)
        if state is None:
            return False
        return self.has_stable_voice_evidence_from_state(state)

    def has_confirmable_local_speech(self, call_id: str) -> bool:
        state = self._states.get(call_id)
        if state is None:
            return False
        return self.has_confirmable_local_speech_from_state(state)

    def has_pre_stop_local_speech(self, call_id: str) -> bool:
        state = self._states.get(call_id)
        if state is None:
            return False
        return self.has_pre_stop_local_speech_from_state(state)

    def has_pre_stop_local_speech_from_state(self, state: _SipBargeInState) -> bool:
        return (
            self.has_stable_voice_evidence_from_state(state)
            and self._has_stable_turn_pre_stop_evidence(state)
            and self._local_speech_quality_rejection_reason(state) is None
        )

    def has_fast_pre_stop_local_speech(self, call_id: str) -> bool:
        state = self._states.get(call_id)
        if state is None:
            return False
        return self.has_fast_pre_stop_local_speech_from_state(state)

    def has_fast_pre_stop_local_speech_from_state(self, state: _SipBargeInState) -> bool:
        return (
            self.has_stable_voice_evidence_from_state(state)
            and (
                self._has_fast_turn_pre_stop_evidence(state)
                or self._has_clean_decaying_short_command_evidence(state)
            )
            and self._local_speech_quality_rejection_reason(state) is None
        )

    def has_single_short_pre_stop_local_speech(
        self,
        call_id: str,
        *,
        min_rms_dbfs: float,
        max_rms_dbfs: float,
        min_snr_db: float,
    ) -> bool:
        state = self._states.get(call_id)
        if state is None:
            return False
        return self.has_single_short_pre_stop_local_speech_from_state(
            state,
            min_rms_dbfs=min_rms_dbfs,
            max_rms_dbfs=max_rms_dbfs,
            min_snr_db=min_snr_db,
        )

    def has_single_short_pre_stop_local_speech_from_state(
        self,
        state: _SipBargeInState,
        *,
        min_rms_dbfs: float,
        max_rms_dbfs: float,
        min_snr_db: float,
    ) -> bool:
        if not self.has_stable_voice_evidence_from_state(state):
            return False
        if state.latest_rms_dbfs is None or state.latest_snr_db is None:
            return False
        if state.latest_rms_dbfs < min_rms_dbfs or state.latest_rms_dbfs > max_rms_dbfs:
            return False
        if state.latest_snr_db < min_snr_db:
            return False
        return (
            self._has_modulated_turn_taking_envelope(state)
            and self._local_speech_quality_rejection_reason(state) is None
        )

    def has_confirmable_local_speech_from_state(self, state: _SipBargeInState) -> bool:
        min_local_confirm_ms = max(
            self.config.max_hold_ms,
            self.config.candidate_min_duration_ms,
        )
        return (
            self.has_stable_voice_evidence_from_state(state)
            and state.candidate_duration_ms >= min_local_confirm_ms
            and self._has_turn_taking_speech_evidence(state)
            and self._local_speech_quality_rejection_reason(state) is None
        )

    def latest_observation_payload(self, call_id: str) -> dict[str, float | int | str | None]:
        state = self._states.get(call_id)
        if state is None:
            return {}
        return {
            "candidateClass": state.latest_candidate_class,
            "rmsDbfs": self._round_db(state.latest_rms_dbfs),
            "noiseFloorDbfs": self._round_db(state.noise_floor_dbfs),
            "snrDb": self._round_db(state.latest_snr_db),
            "maxSnrDb": self._round_db(state.max_snr_db),
            "peakDbfs": self._round_db(state.latest_peak_dbfs),
            "vadVoicedMs": state.vad_voiced_ms,
            "candidateDurationMs": state.candidate_duration_ms,
            "wallClockSpeechMs": self._wall_clock_speech_ms(state),
            "maxVoicedFrameGapMs": state.max_voiced_frame_gap_ms,
            "rmsRangeDb": self._rms_range_db(state),
            "rmsDirectionChanges": state.rms_direction_changes,
            "largeRmsJumpCount": state.large_rms_jump_count,
            "speechQualityRejection": self._local_speech_quality_rejection_reason(state),
        }

    def _candidate_class(self, state: _SipBargeInState) -> str | None:
        if self._has_clean_decaying_short_command_evidence(state):
            return "strong_short_speech_candidate"
        if self.has_stable_voice_evidence_from_state(state):
            return "stable_speech_candidate"
        if (
            state.vad_voiced_ms >= self.config.short_speech_min_duration_ms
            and state.latest_rms_dbfs is not None
            and state.latest_rms_dbfs >= self.config.strong_short_min_rms_dbfs
            and not self._is_too_hot_for_strong_short_candidate(state)
            and state.latest_snr_db is not None
            and state.latest_snr_db
            >= self.config.snr_threshold_db + self.config.strong_short_extra_snr_db
        ):
            return "strong_short_speech_candidate"
        return None

    def has_stable_voice_evidence_from_state(self, state: _SipBargeInState) -> bool:
        return (
            state.vad_voiced_ms >= self.config.vad_voiced_duration_ms
            and state.candidate_duration_ms >= self.config.candidate_min_duration_ms
            and state.latest_snr_db is not None
            and state.latest_snr_db >= self.config.snr_threshold_db
        )

    def _is_too_hot_for_strong_short_candidate(self, state: _SipBargeInState) -> bool:
        if state.latest_rms_dbfs is None:
            return False
        return (
            state.candidate_duration_ms < self.config.candidate_min_duration_ms
            and state.latest_rms_dbfs >= self.config.strong_short_max_rms_dbfs
        )

    def _is_decaying_hot_burst_tail(self, state: _SipBargeInState) -> bool:
        if state.latest_rms_dbfs is None or state.max_rms_dbfs is None:
            return False
        return (
            state.max_rms_dbfs >= self.config.strong_short_max_rms_dbfs
            and state.latest_rms_dbfs < self.config.strong_short_min_rms_dbfs
            and state.max_rms_dbfs - state.latest_rms_dbfs
            >= self.config.hot_burst_tail_rms_drop_db
        )

    def _non_speech_suppression_reason(self, state: _SipBargeInState) -> str | None:
        if state.candidate_raised and self._has_turn_taking_speech_evidence(state):
            return None
        if self._is_decaying_hot_burst_tail(state):
            return "decaying_hot_burst_tail"
        if self._has_non_speech_energy_envelope(state):
            return "non_speech_energy_envelope"
        return None

    def _local_speech_quality_rejection_reason(self, state: _SipBargeInState) -> str | None:
        if state.suppress_until_silence:
            return state.suppression_reason or "non_speech_suppressed"
        if self._has_low_confidence_flat_audio(state):
            return "low_confidence_flat_audio"
        if self._has_weak_flat_turn_evidence(state):
            return "weak_flat_turn_evidence"
        if self._has_rise_fall_tail_envelope(state):
            return "rise_fall_tail_envelope"
        if self._has_recovered_from_hot_onset_speech(state):
            return None
        clipped_hot_onset_recovered = self._has_recovered_from_clipped_hot_onset(state)
        if state.clipped_hot_onset_detected and not clipped_hot_onset_recovered:
            return "clipped_hot_onset"
        if not clipped_hot_onset_recovered and self._has_clipped_hot_onset(state):
            return "clipped_hot_onset"
        if state.short_hot_onset_drop_detected:
            return "short_hot_onset_drop"
        if self._has_short_hot_onset_drop(state):
            return "short_hot_onset_drop"
        if (
            self.has_stable_voice_evidence_from_state(state)
            and self._reached_stable_turn_pre_stop_duration(state)
            and not self._has_turn_taking_speech_evidence(state)
        ):
            return "insufficient_turn_taking_evidence"
        return None

    def _has_clipped_hot_onset(self, state: _SipBargeInState) -> bool:
        if state.max_peak_dbfs is None or state.max_rms_dbfs is None:
            return False
        if state.candidate_duration_ms > self.config.pre_stop_min_duration_ms:
            return False
        return (
            state.max_peak_dbfs >= self.config.clipped_hot_peak_dbfs
            and state.max_rms_dbfs >= self.config.clipped_hot_min_rms_dbfs
        )

    def _has_recovered_from_clipped_hot_onset(self, state: _SipBargeInState) -> bool:
        if (
            state.latest_peak_dbfs is None
            or state.latest_rms_dbfs is None
            or state.latest_snr_db is None
        ):
            return False
        return (
            state.candidate_duration_ms >= self.config.pre_stop_min_duration_ms
            and state.latest_peak_dbfs < self.config.clipped_hot_peak_dbfs
            and state.latest_rms_dbfs < self.config.clipped_hot_min_rms_dbfs
            and self.has_stable_voice_evidence_from_state(state)
        )

    def _has_short_hot_onset_drop(self, state: _SipBargeInState) -> bool:
        if state.latest_rms_dbfs is None or state.max_rms_dbfs is None:
            return False
        if state.candidate_duration_ms > self.config.pre_stop_min_duration_ms:
            return False
        hot_onset_dbfs = self.config.strong_short_max_rms_dbfs - 1.0
        return (
            state.max_rms_dbfs >= hot_onset_dbfs
            and state.max_rms_dbfs - state.latest_rms_dbfs
            >= self.config.non_speech_envelope_min_jump_db
        )

    def _has_recovered_from_hot_onset_speech(self, state: _SipBargeInState) -> bool:
        if state.latest_snr_db is None:
            return False
        return (
            state.candidate_duration_ms >= self.config.pre_stop_min_duration_ms
            and self.has_stable_voice_evidence_from_state(state)
            and self._has_modulated_turn_taking_envelope(state)
            and state.latest_snr_db
            >= max(self.config.snr_threshold_db + self.config.strong_short_extra_snr_db, 20.0)
        )

    def _has_stable_turn_pre_stop_evidence(self, state: _SipBargeInState) -> bool:
        if state.latest_snr_db is None:
            return False
        if not self._has_turn_taking_speech_evidence(state):
            return False
        stable_turn_min_duration_ms = max(
            self.config.pre_stop_min_duration_ms,
            self.config.candidate_min_duration_ms * 2,
        )
        stable_turn_min_snr_db = max(
            self.config.snr_threshold_db,
            self.config.snr_threshold_db + self.config.strong_short_extra_snr_db,
        )
        if (
            state.candidate_duration_ms >= stable_turn_min_duration_ms
            and state.latest_snr_db >= stable_turn_min_snr_db
        ):
            return True

        extended_turn_min_duration_ms = max(
            self.config.max_hold_ms,
            stable_turn_min_duration_ms,
        )
        return (
            state.candidate_duration_ms >= extended_turn_min_duration_ms
            and state.latest_snr_db >= self.config.snr_threshold_db + 5.0
        )

    def _has_fast_turn_pre_stop_evidence(self, state: _SipBargeInState) -> bool:
        if state.latest_snr_db is None:
            return False
        if state.candidate_duration_ms < self.config.pre_stop_min_duration_ms:
            return False
        if not self._has_modulated_turn_taking_envelope(state):
            return False
        fast_min_snr_db = max(
            self.config.snr_threshold_db + self.config.strong_short_extra_snr_db,
            20.0,
        )
        return state.latest_snr_db >= fast_min_snr_db

    def _has_clean_decaying_short_command_evidence(self, state: _SipBargeInState) -> bool:
        if (
            state.latest_snr_db is None
            or state.max_snr_db is None
            or state.latest_rms_dbfs is None
            or state.max_rms_dbfs is None
            or state.min_rms_dbfs is None
        ):
            return False
        if state.candidate_duration_ms < self.config.short_speech_min_duration_ms:
            return False
        if state.candidate_duration_ms > self.config.pre_stop_min_duration_ms:
            return False
        if state.max_rms_dbfs >= self.config.clipped_hot_min_rms_dbfs:
            return False
        if state.max_snr_db < max(
            self.config.snr_threshold_db + self.config.strong_short_extra_snr_db,
            20.0,
        ):
            return False
        decaying_tail_min_snr_db = (
            self.config.snr_threshold_db
            + max(5.0, self.config.strong_short_extra_snr_db - 3.0)
        )
        if state.latest_snr_db < decaying_tail_min_snr_db:
            return False
        rms_range_db = state.max_rms_dbfs - state.min_rms_dbfs
        if rms_range_db < self.config.non_speech_envelope_min_range_db:
            return False
        if state.max_rms_dbfs - state.latest_rms_dbfs < self.config.non_speech_envelope_min_jump_db:
            return False
        if state.rms_direction_changes < 1 or state.rms_direction_changes > 2:
            return False
        return state.large_rms_jump_count <= 1

    def _reached_stable_turn_pre_stop_duration(self, state: _SipBargeInState) -> bool:
        return state.candidate_duration_ms >= max(
            self.config.pre_stop_min_duration_ms,
            self.config.candidate_min_duration_ms * 2,
        )

    def _has_turn_taking_speech_evidence(self, state: _SipBargeInState) -> bool:
        if state.latest_snr_db is None or state.latest_rms_dbfs is None:
            return False
        if state.candidate_duration_ms < self.config.pre_stop_min_duration_ms:
            return False
        if self._has_modulated_turn_taking_envelope(state):
            return True
        if (
            state.candidate_duration_ms >= self.config.turn_taking_extended_duration_ms
            and state.latest_snr_db >= self.config.snr_threshold_db + 8.0
        ):
            return True
        return (
            state.latest_rms_dbfs >= self.config.turn_taking_high_confidence_rms_dbfs
            and state.latest_snr_db >= self.config.turn_taking_high_confidence_snr_db
            and self._reached_stable_turn_pre_stop_duration(state)
        )

    def _has_modulated_turn_taking_envelope(self, state: _SipBargeInState) -> bool:
        if state.min_rms_dbfs is None or state.max_rms_dbfs is None:
            return False
        rms_range_db = state.max_rms_dbfs - state.min_rms_dbfs
        return (
            rms_range_db >= self.config.turn_taking_min_range_db
            and state.rms_direction_changes >= 1
        )

    def _has_low_confidence_flat_audio(self, state: _SipBargeInState) -> bool:
        if (
            state.min_rms_dbfs is None
            or state.max_rms_dbfs is None
            or state.latest_snr_db is None
        ):
            return False
        if state.candidate_duration_ms < self.config.pre_stop_min_duration_ms:
            return False
        rms_range_db = state.max_rms_dbfs - state.min_rms_dbfs
        return (
            state.max_rms_dbfs <= self.config.low_confidence_flat_max_rms_dbfs
            and state.latest_snr_db <= self.config.low_confidence_flat_max_snr_db
            and rms_range_db <= self.config.low_confidence_flat_max_range_db
            and state.large_rms_jump_count == 0
            and state.rms_direction_changes == 0
        )

    def _has_weak_flat_turn_evidence(self, state: _SipBargeInState) -> bool:
        if (
            state.min_rms_dbfs is None
            or state.max_rms_dbfs is None
            or state.latest_snr_db is None
        ):
            return False
        if state.candidate_duration_ms < self.config.candidate_min_duration_ms:
            return False
        rms_range_db = state.max_rms_dbfs - state.min_rms_dbfs
        return (
            state.max_rms_dbfs <= self.config.weak_flat_turn_evidence_max_rms_dbfs
            and state.latest_snr_db <= self.config.weak_flat_turn_evidence_max_snr_db
            and rms_range_db <= self.config.weak_flat_turn_evidence_max_range_db
            and state.large_rms_jump_count == 0
            and state.rms_direction_changes == 0
        )

    def _has_rise_fall_tail_envelope(self, state: _SipBargeInState) -> bool:
        if (
            state.min_rms_dbfs is None
            or state.max_rms_dbfs is None
            or state.latest_rms_dbfs is None
        ):
            return False
        if state.candidate_duration_ms < self.config.pre_stop_min_duration_ms:
            return False
        rms_range_db = state.max_rms_dbfs - state.min_rms_dbfs
        return (
            rms_range_db >= self.config.non_speech_envelope_min_range_db
            and state.large_rms_jump_count >= self.config.non_speech_envelope_min_jump_count
            and state.rms_direction_changes >= 1
            and state.latest_rms_dbfs <= state.min_rms_dbfs + 1.0
            and state.max_rms_dbfs - state.latest_rms_dbfs
            >= self.config.non_speech_envelope_min_range_db
        )

    def _has_non_speech_energy_envelope(self, state: _SipBargeInState) -> bool:
        if state.min_rms_dbfs is None or state.max_rms_dbfs is None:
            return False
        if state.candidate_duration_ms < self.config.short_speech_min_duration_ms:
            return False
        rms_range_db = state.max_rms_dbfs - state.min_rms_dbfs
        return (
            rms_range_db >= self.config.non_speech_envelope_min_range_db
            and state.large_rms_jump_count >= self.config.non_speech_envelope_min_jump_count
            and state.rms_direction_changes
            >= self.config.non_speech_envelope_min_direction_changes
        )

    def _update_energy_envelope(
        self,
        state: _SipBargeInState,
        rms_dbfs: float,
        *,
        frame_duration_ms: int,
    ) -> None:
        hot_onset_dbfs = self.config.strong_short_max_rms_dbfs - 1.0
        if rms_dbfs >= hot_onset_dbfs:
            state.hot_onset_voiced_ms += frame_duration_ms
        state.min_rms_dbfs = (
            rms_dbfs
            if state.min_rms_dbfs is None
            else min(state.min_rms_dbfs, rms_dbfs)
        )
        state.max_rms_dbfs = (
            rms_dbfs
            if state.max_rms_dbfs is None
            else max(state.max_rms_dbfs, rms_dbfs)
        )
        previous_rms_dbfs = state.previous_rms_dbfs
        if previous_rms_dbfs is not None:
            delta_db = rms_dbfs - previous_rms_dbfs
            if abs(delta_db) >= self.config.non_speech_envelope_min_jump_db:
                state.large_rms_jump_count += 1
            direction = self._rms_delta_direction(delta_db)
            if direction:
                if state.last_rms_direction and state.last_rms_direction != direction:
                    state.rms_direction_changes += 1
                state.last_rms_direction = direction
        state.previous_rms_dbfs = rms_dbfs

    def _update_voice_timing(self, state: _SipBargeInState, now: datetime) -> None:
        if state.first_voiced_at is None:
            state.first_voiced_at = now
        if state.previous_voiced_at is not None:
            gap_ms = round(max(0.0, (now - state.previous_voiced_at).total_seconds() * 1000))
            state.max_voiced_frame_gap_ms = max(state.max_voiced_frame_gap_ms, gap_ms)
        state.previous_voiced_at = now

    def _wall_clock_speech_ms(self, state: _SipBargeInState) -> int | None:
        if state.first_voiced_at is None or state.previous_voiced_at is None:
            return None
        return round(
            max(
                0.0,
                (state.previous_voiced_at - state.first_voiced_at).total_seconds() * 1000,
            )
        )

    @staticmethod
    def _rms_delta_direction(delta_db: float) -> int:
        if delta_db >= 1.0:
            return 1
        if delta_db <= -1.0:
            return -1
        return 0

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

    @staticmethod
    def _pcm16_peak_dbfs(frame: PcmAudioFrame) -> float | None:
        if frame.channels != 1 or frame.sample_width_bytes != 2 or len(frame.data) < 2:
            return None
        usable_size = len(frame.data) - (len(frame.data) % 2)
        peak = 0
        for (sample,) in struct.iter_unpack("<h", frame.data[:usable_size]):
            peak = max(peak, abs(sample))
        if peak <= 0:
            return float("-inf")
        return 20 * math.log10(peak / 32768.0)

    @staticmethod
    def _db_delta(value_db: float | None, floor_db: float | None) -> float | None:
        if value_db is None or floor_db is None:
            return None
        if math.isinf(value_db) or math.isinf(floor_db):
            return None
        return value_db - floor_db

    @staticmethod
    def _update_noise_floor(state: _SipBargeInState, rms_dbfs: float) -> None:
        if math.isinf(rms_dbfs):
            return
        state.noise_floor_dbfs = min(
            max(rms_dbfs, state.noise_floor_dbfs),
            state.noise_floor_dbfs + 1.0,
        )

    def _is_vad_speech(self, frame: PcmAudioFrame) -> bool:
        try:
            return self.vad.is_speech(frame)
        except Exception:
            return False

    def _is_impulse_noise(
        self,
        *,
        frame_duration_ms: int,
        rms_dbfs: float,
        peak_dbfs: float | None,
    ) -> bool:
        if peak_dbfs is None or math.isinf(peak_dbfs) or math.isinf(rms_dbfs):
            return False
        return (
            rms_dbfs >= self.min_rms_dbfs
            and frame_duration_ms <= self.config.impulse_noise_max_duration_ms
            and peak_dbfs - rms_dbfs >= self.config.impulse_peak_rms_gap_db
        )

    @staticmethod
    def _round_db(value: float | None) -> float | None:
        return round(value, 2) if value is not None and not math.isinf(value) else value

    def _rms_range_db(self, state: _SipBargeInState) -> float | None:
        if state.min_rms_dbfs is None or state.max_rms_dbfs is None:
            return None
        return self._round_db(state.max_rms_dbfs - state.min_rms_dbfs)
