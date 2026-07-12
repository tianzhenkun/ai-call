from __future__ import annotations

import struct

from app.services.ai_call.audio_bridge import PcmAudioFrame
from app.services.ai_call.sip_barge_in import SipBargeInConfig
from app.services.ai_call.sip_barge_in_replay import (
    SequenceVoiceActivityDetector,
    SipBargeInReplayFrame,
    SipVadReplayProvider,
    SpeechWindow,
    WindowVoiceActivityDetector,
    pcm16_mono_to_replay_frames,
    replay_sip_barge_in_vad_providers,
)


def _pcm16_constant_frame(
    *,
    amplitude: int,
    sample_rate_hz: int = 8000,
    duration_ms: int = 20,
) -> PcmAudioFrame:
    sample_count = sample_rate_hz * duration_ms // 1000
    pcm = struct.pack("<" + "h" * sample_count, *([amplitude] * sample_count))
    return PcmAudioFrame(
        data=pcm,
        sample_rate_hz=sample_rate_hz,
        channels=1,
        sample_width_bytes=2,
    )


def _replay_frames(amplitudes: list[int]) -> list[SipBargeInReplayFrame]:
    return [
        SipBargeInReplayFrame(
            offset_ms=index * 20,
            frame=_pcm16_constant_frame(amplitude=amplitude),
        )
        for index, amplitude in enumerate(amplitudes)
    ]


def test_replay_compares_fsmn_main_against_webrtc_main_without_mixing_decisions() -> None:
    config = SipBargeInConfig(
        rms_threshold_dbfs=-36.0,
        snr_threshold_db=10.0,
        vad_voiced_duration_ms=120,
        candidate_min_duration_ms=180,
        pre_stop_min_duration_ms=240,
    )
    frames = _replay_frames([
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
    providers = [
        SipVadReplayProvider(
            name="webrtc_main",
            vad_factory=lambda: SequenceVoiceActivityDetector(
                [True, False, True, False, True, False, True, False, True, False]
                + [True, False, True, False, True, False, False, False],
            ),
        ),
        SipVadReplayProvider(
            name="fsmn_main",
            vad_factory=lambda: WindowVoiceActivityDetector(
                [SpeechWindow(start_ms=0, end_ms=360)],
            ),
        ),
    ]

    report = replay_sip_barge_in_vad_providers(
        call_id="call_replay_compare",
        frames=frames,
        providers=providers,
        config=config,
    )

    webrtc = report.provider_reports["webrtc_main"]
    fsmn = report.provider_reports["fsmn_main"]

    assert webrtc.candidate_events == []
    assert webrtc.pre_stop_events == []
    assert len(fsmn.candidate_events) == 1
    assert fsmn.candidate_events[0].candidate_class == "stable_speech_candidate"
    assert fsmn.candidate_events[0].offset_ms == 160
    assert len(fsmn.pre_stop_events) == 1
    assert fsmn.pre_stop_events[0].provider == "fsmn_main"
    assert fsmn.pre_stop_events[0].offset_ms == 220


def test_replay_keeps_rms_guard_when_fsmn_main_marks_low_energy_frames_as_speech() -> None:
    frames = _replay_frames([50] * 20)
    providers = [
        SipVadReplayProvider(
            name="fsmn_main",
            vad_factory=lambda: WindowVoiceActivityDetector(
                [SpeechWindow(start_ms=0, end_ms=400)],
            ),
        ),
    ]

    report = replay_sip_barge_in_vad_providers(
        call_id="call_replay_low_energy",
        frames=frames,
        providers=providers,
        config=SipBargeInConfig(rms_threshold_dbfs=-36.0),
    )

    fsmn = report.provider_reports["fsmn_main"]

    assert fsmn.candidate_events == []
    assert fsmn.pre_stop_events == []
    assert fsmn.last_reason == "below_min_rms"


def test_replay_pre_stops_clean_decaying_short_command_envelope() -> None:
    frames = _replay_frames([
        4000,
        4200,
        5000,
        4600,
        4900,
        4000,
        3500,
        2500,
        1400,
        850,
    ])
    providers = [
        SipVadReplayProvider(
            name="webrtc_main",
            vad_factory=lambda: WindowVoiceActivityDetector(
                [SpeechWindow(start_ms=0, end_ms=200)],
            ),
        )
    ]

    report = replay_sip_barge_in_vad_providers(
        call_id="call_replay_clean_decaying_short_command",
        frames=frames,
        providers=providers,
        config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )

    webrtc = report.provider_reports["webrtc_main"]

    assert len(webrtc.candidate_events) == 1
    assert webrtc.candidate_events[0].candidate_class == "strong_short_speech_candidate"
    assert len(webrtc.pre_stop_events) == 1
    assert webrtc.pre_stop_events[0].offset_ms <= 180


def test_replay_pre_stops_decaying_short_command_after_noise_floor_adaptation() -> None:
    frames = _replay_frames(
        [180] * 25
        + [
            4000,
            4200,
            5000,
            4600,
            4900,
            4000,
            3500,
            2500,
            1400,
            850,
        ]
    )
    providers = [
        SipVadReplayProvider(
            name="webrtc_main",
            vad_factory=lambda: SequenceVoiceActivityDetector([False] * 25 + [True] * 10),
        )
    ]

    report = replay_sip_barge_in_vad_providers(
        call_id="call_replay_decaying_short_after_noise_floor",
        frames=frames,
        providers=providers,
        config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )

    webrtc = report.provider_reports["webrtc_main"]

    assert len(webrtc.candidate_events) == 1
    assert webrtc.candidate_events[0].candidate_class == "strong_short_speech_candidate"
    assert len(webrtc.pre_stop_events) == 1
    assert webrtc.pre_stop_events[0].offset_ms <= 680


def test_pcm16_mono_to_replay_frames_splits_full_frames_and_drops_partial_tail() -> None:
    sample_rate_hz = 8000
    full_frame_samples = sample_rate_hz * 20 // 1000
    samples = [1000] * full_frame_samples + [2000] * full_frame_samples + [3000]
    pcm = struct.pack("<" + "h" * len(samples), *samples)

    frames = pcm16_mono_to_replay_frames(
        pcm,
        sample_rate_hz=sample_rate_hz,
        frame_duration_ms=20,
    )

    assert len(frames) == 2
    assert [frame.offset_ms for frame in frames] == [0, 20]
    assert frames[0].frame.data == struct.pack("<" + "h" * full_frame_samples, *([1000] * full_frame_samples))
    assert frames[1].frame.data == struct.pack("<" + "h" * full_frame_samples, *([2000] * full_frame_samples))
