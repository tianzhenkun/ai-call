import asyncio
import base64
import json
import shutil
import struct
import subprocess
import sys
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

from app.api.v1.ai_call import AiCallRouter
from app.api.v1.ai_call.controller import get_ai_call_service
from app.api.v1.ai_call.service import AiCallService
from app.config.setting import Settings
from app.core.logger import sanitize_log_message
from app.plugin import init_app
from app.services.ai_call.agent_runner import (
    PendingCallEndIntent,
    PendingUserTurn,
    RealtimeCallAgentRunner,
)
from app.services.ai_call.audio_bridge import AudioBridgeError, PcmAudioBridge, PcmAudioFrame
from app.services.ai_call.event_store import InMemoryEventStore
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.livekit_audio_transport import LiveKitRoomAudioTransport
from app.services.ai_call.livekit_room import BrowserRoomToken
from app.services.ai_call.metrics import CallMetrics
from app.services.ai_call.orchestrator import AiCallOrchestrator, AiCallRuntimeConfig
from app.services.ai_call.providers.aliyun_qwen_realtime import (
    DEFAULT_REALTIME_TOOLS,
    REQUEST_HANDOFF_TOOL,
    SCHEDULE_CALL_END_TOOL,
    AliyunQwenRealtimeProvider,
    QwenRealtimeSessionConfig,
    _default_websocket_factory,
    build_session_update_event,
    map_qwen_server_event,
)
from app.services.ai_call.providers.base import ProviderEvent
from app.services.ai_call.session_registry import (
    CallSession,
    CallSessionStatus,
    InMemorySessionRegistry,
)
from app.services.ai_call.sip_barge_in import SipBargeInObservation
from app.services.ai_call.sip_vad_shadow import (
    MultiSipVadShadowDetector,
    QueuedSipVadShadowDetector,
    SipFrameVadShadowDetector,
    SipVadShadowObservation,
    UnavailableSipVadShadowDetector,
)


class FakeLiveKitRoomManager:
    def __init__(self) -> None:
        self.created_rooms: list[str] = []
        self.deleted_rooms: list[str] = []
        self.issued_tokens: list[tuple[str, str]] = []

    async def create_room(self, room_name: str) -> None:
        self.created_rooms.append(room_name)

    def issue_browser_token(self, room_name: str, participant_identity: str) -> BrowserRoomToken:
        self.issued_tokens.append((room_name, participant_identity))
        return BrowserRoomToken(
            livekit_url="wss://livekit.test",
            participant_token=f"browser-token-for-{participant_identity}",
            participant_identity=participant_identity,
            expires_in_seconds=600,
        )

    async def delete_room(self, room_name: str) -> None:
        self.deleted_rooms.append(room_name)


class FakeAgentRunner:
    def __init__(self) -> None:
        self.started_call_ids: list[str] = []
        self.stopped_call_ids: list[str] = []
        self.started_opening_call_ids: list[str] = []
        self.browser_speech_candidates: list[tuple[str, datetime]] = []
        self.browser_speech_segments: list[tuple[str, datetime, dict[str, object]]] = []

    async def start(self, session: CallSession) -> None:
        self.started_call_ids.append(session.call_id)

    async def start_opening(self, call_id: str) -> None:
        self.started_opening_call_ids.append(call_id)

    async def stop(self, call_id: str) -> None:
        self.stopped_call_ids.append(call_id)

    async def record_browser_speech_candidate(
        self,
        call_id: str,
        trigger_timestamp: datetime,
    ) -> bool:
        self.browser_speech_candidates.append((call_id, trigger_timestamp))
        return False

    async def record_browser_speech_segment(
        self,
        call_id: str,
        trigger_timestamp: datetime,
        payload: dict[str, object],
    ) -> bool:
        self.browser_speech_segments.append((call_id, trigger_timestamp, payload))
        return False


class DiagnosticAgentRunner(FakeAgentRunner):
    def runtime_diagnostics(self) -> dict[str, object]:
        return {
            "diagnosticsVersion": "test-runtime",
            "runnerModule": "tests.fake_agent",
            "runnerSourceHash": "sha256:test",
        }


class FailingAgentRunner(FakeAgentRunner):
    async def start(self, session: CallSession) -> None:
        await super().start(session)
        raise RuntimeError("agent boom")


class BlockingStopAgentRunner(FakeAgentRunner):
    def __init__(self) -> None:
        super().__init__()
        self.stop_started = asyncio.Event()
        self.release_stop = asyncio.Event()

    async def stop(self, call_id: str) -> None:
        self.stopped_call_ids.append(call_id)
        self.stop_started.set()
        await self.release_stop.wait()


class FakeRealtimeProvider:
    def __init__(self, events: list[ProviderEvent]) -> None:
        self.events = events
        self.connected = False
        self.closed = False
        self.sent_audio: list[bytes] = []
        self.session_updates: list[QwenRealtimeSessionConfig] = []
        self.cancelled_response_count = 0
        self.cleared_input_count = 0
        self.created_responses: list[str | None] = []
        self.submitted_tool_results: list[tuple[str, str]] = []

    async def connect(self) -> None:
        self.connected = True

    async def update_session(self, config: QwenRealtimeSessionConfig) -> None:
        self.session_updates.append(config)

    async def send_audio(self, pcm_frame: bytes) -> None:
        self.sent_audio.append(pcm_frame)

    async def cancel_response(self) -> None:
        self.cancelled_response_count += 1

    async def clear_input_audio(self) -> None:
        self.cleared_input_count += 1

    async def create_response(self, input_text: str | None = None) -> None:
        self.created_responses.append(input_text)

    async def submit_tool_result(self, tool_call_id: str, output: str) -> None:
        self.submitted_tool_results.append((tool_call_id, output))

    async def receive_events(self):
        for event in self.events:
            await asyncio.sleep(0)
            yield event

    async def close(self) -> None:
        self.closed = True


class QueueRealtimeProvider(FakeRealtimeProvider):
    def __init__(self) -> None:
        super().__init__([])
        self._queue: asyncio.Queue[ProviderEvent | None] = asyncio.Queue()

    async def emit(self, event: ProviderEvent) -> None:
        await self._queue.put(event)

    async def close_events(self) -> None:
        await self._queue.put(None)

    async def receive_events(self):
        while True:
            event = await self._queue.get()
            if event is None:
                return
            yield event


class BlockingCreateResponseProvider(QueueRealtimeProvider):
    def __init__(self) -> None:
        super().__init__()
        self.create_started = asyncio.Event()
        self.release_create = asyncio.Event()

    async def create_response(self, input_text: str | None = None) -> None:
        self.created_responses.append(input_text)
        self.create_started.set()
        if len(self.created_responses) == 1:
            await self.release_create.wait()


class FakeAudioPublisher:
    def __init__(self) -> None:
        self.published: list[tuple[str, PcmAudioFrame]] = []
        self.stopped_call_ids: list[str] = []

    async def publish_audio(self, call_id: str, frame: PcmAudioFrame) -> None:
        self.published.append((call_id, frame))

    async def stop_audio(self, call_id: str) -> None:
        self.stopped_call_ids.append(call_id)


class FailingStopAudioPublisher(FakeAudioPublisher):
    async def stop_audio(self, call_id: str) -> None:
        await super().stop_audio(call_id)
        raise RuntimeError("stop audio failed")


async def _wait_until(predicate, attempts: int = 40) -> None:
    for _ in range(attempts):
        if predicate():
            return
        await asyncio.sleep(0)
    assert predicate()


def _seed_customer_end_intent(
    runner: RealtimeCallAgentRunner,
    call_id: str,
    transcript: str = "先这样吧。",
) -> None:
    runner._pending_call_end_intents[call_id] = PendingCallEndIntent(
        transcript=transcript,
        reason="explicit_customer_end",
        summary="用户明确要求结束通话",
        source="test",
        confidence=0.95,
    )


def _seed_task_completed_signal(
    runner: RealtimeCallAgentRunner,
    call_id: str,
    transcript: str = "可以，安排顾问联系我。",
) -> None:
    runner._pending_user_turns[call_id] = PendingUserTurn(
        stopped_at=datetime.now(timezone.utc),
        transcript_parts=[transcript],
    )


def test_interrupt_policy_matrix_upgrades_provider_after_browser_candidate() -> None:
    from app.services.ai_call.agent_runner import (
        InterruptDecisionContext,
        InterruptDecisionPolicy,
    )

    policy = InterruptDecisionPolicy()

    browser_decision = policy.decide_speech_started(
        InterruptDecisionContext(
            source="browser",
            session_status=CallSessionStatus.AI_SPEAKING,
        )
    )
    provider_decision = policy.decide_speech_started(
        InterruptDecisionContext(
            source="provider",
            session_status=CallSessionStatus.AI_SPEAKING,
            has_interrupt_candidate=True,
            candidate_reason="browser_user_speech_started_during_ai_audio",
        )
    )
    transcript_decision = policy.decide_transcript(
        InterruptDecisionContext(
            source="provider",
            session_status=CallSessionStatus.AI_SPEAKING,
            has_interrupt_candidate=True,
            has_valid_transcript=True,
        )
    )

    assert browser_decision.action == "candidate"
    assert browser_decision.reason == "browser_user_speech_started_during_ai_audio"
    assert provider_decision.action == "stop_only"
    assert provider_decision.reason == "user_speech_started_during_ai_audio"
    assert transcript_decision.action == "confirm"


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


def _pcm16_constant_frames(amplitudes: list[int]) -> list[PcmAudioFrame]:
    return [_pcm16_constant_frame(amplitude=amplitude) for amplitude in amplitudes]


class FakeVad:
    def __init__(self, decisions: list[bool]) -> None:
        self.decisions = list(decisions)
        self.calls = 0

    def is_speech(self, frame: PcmAudioFrame) -> bool:
        _ = frame
        decision = self.decisions[min(self.calls, len(self.decisions) - 1)]
        self.calls += 1
        return decision


class FakeSipVadShadowDetector:
    def __init__(self, observations: list["SipVadShadowObservation"]) -> None:
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
    ) -> "SipVadShadowObservation":
        _ = call_id, frame, now, interruptible
        observation = self.observations[min(self.calls, len(self.observations) - 1)]
        self.calls += 1
        return observation

    def reset(self, call_id: str) -> None:
        self.reset_call_ids.append(call_id)


class FakeMultiSipVadShadowDetector:
    def __init__(self, observations: list[SipVadShadowObservation]) -> None:
        self.detector_name = "fake_multi_shadow"
        self.observations = list(observations)
        self.calls = 0

    def observe(
        self,
        call_id: str,
        frame: PcmAudioFrame,
        *,
        now: datetime,
        interruptible: bool,
    ) -> list[SipVadShadowObservation]:
        _ = call_id, frame, now, interruptible
        self.calls += 1
        return list(self.observations)

    def reset(self, call_id: str) -> None:
        _ = call_id


class FailingSipVadShadowDetector:
    def __init__(self, message: str) -> None:
        self.detector_name = "fake_failing_shadow"
        self.message = message
        self.calls = 0
        self.reset_call_ids: list[str] = []

    def observe(
        self,
        call_id: str,
        frame: PcmAudioFrame,
        *,
        now: datetime,
        interruptible: bool,
    ) -> "SipVadShadowObservation":
        _ = call_id, frame, now, interruptible
        self.calls += 1
        raise RuntimeError(self.message)

    def reset(self, call_id: str) -> None:
        self.reset_call_ids.append(call_id)


def _pcm16_pulse_frame(
    *,
    amplitude: int,
    sample_rate_hz: int = 8000,
    duration_ms: int = 20,
) -> PcmAudioFrame:
    sample_count = sample_rate_hz * duration_ms // 1000
    samples = [0] * sample_count
    samples[0] = amplitude
    pcm = struct.pack("<" + "h" * sample_count, *samples)
    return PcmAudioFrame(
        data=pcm,
        sample_rate_hz=sample_rate_hz,
        channels=1,
        sample_width_bytes=2,
    )


def _pcm16_peak_with_body_frame(
    *,
    peak_amplitude: int,
    body_amplitude: int,
    sample_rate_hz: int = 8000,
    duration_ms: int = 20,
) -> PcmAudioFrame:
    sample_count = sample_rate_hz * duration_ms // 1000
    samples = [body_amplitude] * sample_count
    samples[0] = peak_amplitude
    pcm = struct.pack("<" + "h" * sample_count, *samples)
    return PcmAudioFrame(
        data=pcm,
        sample_rate_hz=sample_rate_hz,
        channels=1,
        sample_width_bytes=2,
    )


def test_sip_barge_in_detector_promotes_sustained_uplink_audio() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInDetector

    detector = SipBargeInDetector(
        min_rms_dbfs=-35.0,
        min_speech_duration_ms=200,
    )
    started_at = datetime.now(timezone.utc)
    loud_frame = _pcm16_constant_frame(amplitude=4000)

    observations = [
        detector.observe(
            "call_sip_detector",
            loud_frame,
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=True,
        )
        for index in range(10)
    ]

    assert observations[-1].active is True
    assert any(observation.candidate for observation in observations)
    assert observations[-1].speech_duration_ms >= 200
    assert observations[-1].rms_dbfs >= -35.0


def test_sip_barge_in_detector_ignores_silence_and_low_level_audio() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInDetector

    detector = SipBargeInDetector(
        min_rms_dbfs=-35.0,
        min_speech_duration_ms=200,
    )
    started_at = datetime.now(timezone.utc)
    quiet_frame = _pcm16_constant_frame(amplitude=50)

    observations = [
        detector.observe(
            "call_sip_detector",
            quiet_frame,
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=True,
        )
        for index in range(12)
    ]

    assert all(observation.candidate is False for observation in observations)
    assert observations[-1].active is False
    assert observations[-1].speech_duration_ms == 0


def test_sip_barge_in_detector_does_not_label_low_level_sparse_frame_as_impulse() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInDetector

    detector = SipBargeInDetector(
        config=SipBargeInConfig(
            rms_threshold_dbfs=-35.0,
            snr_threshold_db=10.0,
            impulse_noise_max_duration_ms=120,
        ),
        vad=FakeVad([True]),
    )

    observation = detector.observe(
        "call_low_sparse_frame",
        _pcm16_pulse_frame(amplitude=800),
        now=datetime.now(timezone.utc),
        interruptible=True,
    )

    assert observation.candidate is False
    assert observation.candidate_class is None
    assert observation.reason == "below_min_rms"
    assert observation.rms_dbfs is not None
    assert observation.rms_dbfs < -35.0


def test_sip_barge_in_detector_requires_snr_and_vad() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInDetector

    detector = SipBargeInDetector(
        config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=40.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
        ),
        vad=FakeVad([True] * 20),
    )
    started_at = datetime.now(timezone.utc)
    frame = _pcm16_constant_frame(amplitude=4000)

    observations = [
        detector.observe(
            "call_snr_detector",
            frame,
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=True,
        )
        for index in range(10)
    ]

    assert all(not observation.candidate for observation in observations)
    assert observations[-1].reason == "below_min_snr"


def test_sip_barge_in_detector_calibrates_floor_while_not_interruptible() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInDetector

    class SwitchableVad:
        voiced = False

        def is_speech(self, frame: PcmAudioFrame) -> bool:
            _ = frame
            return self.voiced

    vad = SwitchableVad()
    detector = SipBargeInDetector(
        config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
        ),
        vad=vad,
    )
    call_id = "call_high_floor_calibration"
    started_at = datetime.now(timezone.utc)
    high_floor_frame = _pcm16_constant_frame(amplitude=1200)

    for index in range(30):
        detector.observe(
            call_id,
            high_floor_frame,
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=False,
        )

    vad.voiced = True
    observations = [
        detector.observe(
            call_id,
            high_floor_frame,
            now=started_at + timedelta(milliseconds=20 * (30 + index)),
            interruptible=True,
        )
        for index in range(12)
    ]
    payload = detector.latest_observation_payload(call_id)

    assert all(not observation.candidate for observation in observations)
    assert observations[-1].reason == "below_min_snr"
    assert payload["noiseFloorDbfs"] is not None
    assert payload["noiseFloorDbfs"] > -36.0
    assert payload["snrDb"] is not None
    assert payload["snrDb"] < 10.0


def test_sip_barge_in_detector_preserves_floor_after_impulse_noise() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInDetector

    detector = SipBargeInDetector(
        config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
        ),
        vad=FakeVad([False] * 30 + [True] + [True] * 12),
    )
    call_id = "call_impulse_preserves_floor"
    started_at = datetime.now(timezone.utc)
    high_floor_frame = _pcm16_constant_frame(amplitude=1200)

    for index in range(30):
        detector.observe(
            call_id,
            high_floor_frame,
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=False,
        )

    impulse = detector.observe(
        call_id,
        _pcm16_pulse_frame(amplitude=30000),
        now=started_at + timedelta(milliseconds=600),
        interruptible=True,
    )
    observations = [
        detector.observe(
            call_id,
            high_floor_frame,
            now=started_at + timedelta(milliseconds=620 + 20 * index),
            interruptible=True,
        )
        for index in range(12)
    ]
    payload = detector.latest_observation_payload(call_id)

    assert impulse.candidate_class == "impulse_noise"
    assert all(not observation.candidate for observation in observations)
    assert observations[-1].reason == "below_min_snr"
    assert payload["noiseFloorDbfs"] is not None
    assert payload["noiseFloorDbfs"] > -36.0
    assert payload["snrDb"] is not None
    assert payload["snrDb"] < 10.0


def test_sip_barge_in_detector_does_not_learn_floor_from_loud_non_voiced_tail() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInDetector

    speech_frames = _pcm16_constant_frames([
        1850,
        2550,
        3150,
        3800,
        4700,
        5750,
        5200,
        5900,
        5600,
    ])
    detector = SipBargeInDetector(
        config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
        vad=FakeVad([False] * 50 + [True] * len(speech_frames)),
    )
    call_id = "call_loud_tail_floor_guard"
    started_at = datetime.now(timezone.utc)
    loud_non_voiced_tail = _pcm16_constant_frame(amplitude=12000)

    for index in range(50):
        detector.observe(
            call_id,
            loud_non_voiced_tail,
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=True,
        )

    tail_payload = detector.latest_observation_payload(call_id)
    observations = [
        detector.observe(
            call_id,
            frame,
            now=started_at + timedelta(milliseconds=1000 + 20 * index),
            interruptible=True,
        )
        for index, frame in enumerate(speech_frames)
    ]

    assert tail_payload["noiseFloorDbfs"] is not None
    assert tail_payload["noiseFloorDbfs"] <= -45.0
    assert any(observation.candidate for observation in observations)
    assert observations[-1].candidate_class == "stable_speech_candidate"


def test_sip_barge_in_detector_caps_interruptible_floor_from_fragmented_control_speech() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInDetector

    detector = SipBargeInDetector(
        config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
            noise_floor_initial_dbfs=-50.0,
            noise_floor_interruptible_max_dbfs=-44.0,
        ),
        vad=FakeVad([False] * 30 + [True] * 12),
    )
    call_id = "call_fragmented_control_speech_floor_guard"
    started_at = datetime.now(timezone.utc)

    for index in range(30):
        detector.observe(
            call_id,
            _pcm16_constant_frame(amplitude=400),
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=True,
        )

    floor_payload = detector.latest_observation_payload(call_id)
    speech_frames = _pcm16_constant_frames([
        944,
        1780,
        1780,
        1970,
        1970,
        1900,
        1980,
        1800,
        1000,
        830,
        740,
    ])
    observations = [
        detector.observe(
            call_id,
            frame,
            now=started_at + timedelta(milliseconds=600 + 20 * index),
            interruptible=True,
        )
        for index, frame in enumerate(speech_frames)
    ]

    assert floor_payload["noiseFloorDbfs"] == -44.0
    assert any(observation.candidate for observation in observations)
    assert observations[-1].candidate_class == "stable_speech_candidate"


def test_sip_barge_in_detector_ignores_impulse_noise_before_pre_stop() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInDetector

    detector = SipBargeInDetector(
        config=SipBargeInConfig(
            rms_threshold_dbfs=-60.0,
            snr_threshold_db=10.0,
            impulse_noise_max_duration_ms=120,
        ),
        vad=FakeVad([True, False, False, False, False, False]),
    )
    started_at = datetime.now(timezone.utc)

    observations = [
        detector.observe(
            "call_impulse_detector",
            _pcm16_pulse_frame(amplitude=30000 if index == 0 else 80),
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=True,
        )
        for index in range(6)
    ]

    assert observations[0].candidate is False
    assert observations[0].candidate_class == "impulse_noise"
    assert observations[0].reason == "impulse_noise"
    assert all(not observation.candidate for observation in observations)


def test_sip_barge_in_detector_filters_clipped_short_burst_before_pre_stop() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInDetector

    detector = SipBargeInDetector(
        config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            short_speech_min_duration_ms=120,
        ),
        vad=FakeVad([True] * 12),
    )
    started_at = datetime.now(timezone.utc)
    clipped_burst_frame = _pcm16_constant_frame(amplitude=16000)

    observations = [
        detector.observe(
            "call_clipped_burst_detector",
            clipped_burst_frame,
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=True,
        )
        for index in range(6)
    ]

    assert observations[-1].candidate is False
    assert observations[-1].candidate_class is None
    assert observations[-1].reason == "speech_active_below_candidate_duration"


def test_sip_barge_in_detector_suppresses_modulated_cough_like_short_burst() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInDetector

    detector = SipBargeInDetector(
        config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            short_speech_min_duration_ms=120,
        ),
        vad=FakeVad([True] * 12),
    )
    started_at = datetime.now(timezone.utc)
    frames = _pcm16_constant_frames([4000, 12000, 5000, 10000, 6000, 8500])

    observations = [
        detector.observe(
            "call_modulated_cough_detector",
            frame,
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=True,
        )
        for index, frame in enumerate(frames)
    ]

    assert all(not observation.candidate for observation in observations)
    assert observations[-1].candidate_class is None
    assert observations[-1].reason == "non_speech_energy_envelope"


def test_sip_barge_in_detector_preserves_modulated_phone_speech_candidate() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInDetector

    detector = SipBargeInDetector(
        config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            short_speech_min_duration_ms=120,
        ),
        vad=FakeVad([True] * 20),
    )
    started_at = datetime.now(timezone.utc)
    frames = _pcm16_constant_frames([4700, 5500, 2300, 1600, 1300, 3300, 5250, 8350, 7150])

    observations = [
        detector.observe(
            "call_modulated_phone_speech",
            frame,
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=True,
        )
        for index, frame in enumerate(frames)
    ]

    assert any(observation.candidate for observation in observations)
    assert observations[-1].candidate_class == "stable_speech_candidate"
    assert observations[-1].reason == "speech_active_below_candidate_duration"


def test_sip_barge_in_detector_releases_clipped_onset_after_stable_speech_recovers() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInDetector

    detector = SipBargeInDetector(
        config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
            short_speech_min_duration_ms=120,
        ),
        vad=FakeVad([True] * 20),
    )
    started_at = datetime.now(timezone.utc)
    frames = _pcm16_constant_frames([4700, 5500, 2300, 1600, 1300, 3300, 5250])
    frames.extend([
        _pcm16_peak_with_body_frame(peak_amplitude=30000, body_amplitude=8350),
        _pcm16_constant_frame(amplitude=7150),
        _pcm16_constant_frame(amplitude=6000),
        _pcm16_constant_frame(amplitude=5200),
        _pcm16_constant_frame(amplitude=5000),
    ])
    frames.extend([_pcm16_constant_frame(amplitude=5000)] * 6)

    observations = [
        detector.observe(
            "call_recovered_clipped_phone_speech",
            frame,
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=True,
        )
        for index, frame in enumerate(frames)
    ]

    assert any(observation.candidate for observation in observations)
    assert detector.has_pre_stop_local_speech("call_recovered_clipped_phone_speech") is True
    assert detector.latest_observation_payload("call_recovered_clipped_phone_speech")[
        "speechQualityRejection"
    ] is None


def test_sip_barge_in_detector_releases_short_hot_onset_after_modulated_speech() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInDetector

    detector = SipBargeInDetector(
        config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
            short_speech_min_duration_ms=120,
        ),
        vad=FakeVad([True] * 30),
    )
    started_at = datetime.now(timezone.utc)
    frames = _pcm16_constant_frames([
        11500,
        10000,
        7600,
        5200,
        4300,
        5600,
        7200,
        6100,
        5200,
        4800,
        6200,
        7600,
        6800,
        5900,
        5200,
        5000,
        5400,
        6000,
    ])

    observations = [
        detector.observe(
            "call_recovered_short_hot_phone_speech",
            frame,
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=True,
        )
        for index, frame in enumerate(frames)
    ]

    assert any(observation.candidate for observation in observations)
    assert detector.has_fast_pre_stop_local_speech("call_recovered_short_hot_phone_speech")
    assert detector.has_pre_stop_local_speech("call_recovered_short_hot_phone_speech")
    assert detector.latest_observation_payload("call_recovered_short_hot_phone_speech")[
        "speechQualityRejection"
    ] is None


def test_sip_barge_in_detector_allows_human_phone_modulation_after_stable_turn_evidence() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInDetector

    detector = SipBargeInDetector(
        config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
        vad=FakeVad([True] * 20),
    )
    started_at = datetime.now(timezone.utc)
    frames = _pcm16_constant_frames([
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

    observations = [
        detector.observe(
            "call_human_phone_modulation",
            frame,
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=True,
        )
        for index, frame in enumerate(frames)
    ]

    assert any(observation.candidate for observation in observations)
    assert observations[-1].candidate_class == "stable_speech_candidate"
    assert detector.latest_observation_payload("call_human_phone_modulation")[
        "speechQualityRejection"
    ] is None
    assert detector.has_fast_pre_stop_local_speech("call_human_phone_modulation") is True


def test_sip_barge_in_detector_keeps_modulated_speech_when_envelope_changes_after_candidate() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInDetector

    detector = SipBargeInDetector(
        config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
            noise_floor_initial_dbfs=-42.0,
        ),
        vad=FakeVad([True] * 30),
    )
    started_at = datetime.now(timezone.utc)
    frames = _pcm16_constant_frames([
        2197,
        3248,
        2356,
        1762,
        1819,
        4215,
        5869,
        8072,
        6440,
        4791,
        2210,
        3374,
        4725,
        6572,
        4725,
        5043,
        5492,
        5824,
        7314,
        6417,
        4841,
        3029,
    ])

    observations = [
        detector.observe(
            "call_real_modulated_phone_speech",
            frame,
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=True,
        )
        for index, frame in enumerate(frames)
    ]

    assert any(observation.candidate for observation in observations)
    assert observations[-1].active is True
    assert observations[-1].candidate_class == "stable_speech_candidate"
    assert detector.has_fast_pre_stop_local_speech("call_real_modulated_phone_speech") is True
    assert detector.latest_observation_payload("call_real_modulated_phone_speech")[
        "speechQualityRejection"
    ] is None


def test_sip_barge_in_detector_withholds_pre_stop_for_low_confidence_flat_audio() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInDetector

    detector = SipBargeInDetector(
        config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
        vad=FakeVad([True] * 20),
    )
    started_at = datetime.now(timezone.utc)
    flat_low_confidence_frame = _pcm16_constant_frame(amplitude=800)

    observations = [
        detector.observe(
            "call_low_confidence_flat_audio",
            flat_low_confidence_frame,
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=True,
        )
        for index in range(12)
    ]

    assert any(observation.candidate for observation in observations)
    assert observations[-1].candidate_class == "stable_speech_candidate"
    assert detector.has_pre_stop_local_speech("call_low_confidence_flat_audio") is False
    assert detector.latest_observation_payload("call_low_confidence_flat_audio")[
        "speechQualityRejection"
    ] == "low_confidence_flat_audio"


def test_sip_barge_in_detector_withholds_pre_stop_for_weak_flat_turn_evidence() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInDetector

    detector = SipBargeInDetector(
        config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
        vad=FakeVad([True] * 20),
    )
    started_at = datetime.now(timezone.utc)
    weak_flat_frame = _pcm16_constant_frame(amplitude=1050)

    observations = [
        detector.observe(
            "call_weak_flat_turn_evidence",
            weak_flat_frame,
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=True,
        )
        for index in range(12)
    ]

    assert any(observation.candidate for observation in observations)
    assert observations[-1].candidate_class == "stable_speech_candidate"
    assert detector.has_fast_pre_stop_local_speech("call_weak_flat_turn_evidence") is False
    assert detector.has_pre_stop_local_speech("call_weak_flat_turn_evidence") is False
    assert detector.latest_observation_payload("call_weak_flat_turn_evidence")[
        "speechQualityRejection"
    ] == "weak_flat_turn_evidence"


def test_sip_barge_in_detector_withholds_pre_stop_for_breath_like_flat_turn_shape() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInDetector

    detector = SipBargeInDetector(
        config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
        vad=FakeVad([True] * 20),
    )
    started_at = datetime.now(timezone.utc)
    breath_like_frames = _pcm16_constant_frames([1180] * 18)

    observations = [
        detector.observe(
            "call_flat_breath_like_audio",
            frame,
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=True,
        )
        for index, frame in enumerate(breath_like_frames)
    ]

    assert any(observation.candidate for observation in observations)
    assert observations[-1].candidate_class == "stable_speech_candidate"
    assert detector.has_pre_stop_local_speech("call_flat_breath_like_audio") is False
    assert detector.has_fast_pre_stop_local_speech("call_flat_breath_like_audio") is False
    assert detector.has_confirmable_local_speech("call_flat_breath_like_audio") is False
    assert detector.latest_observation_payload("call_flat_breath_like_audio")[
        "speechQualityRejection"
    ] == "insufficient_turn_taking_evidence"


def test_sip_barge_in_detector_suppresses_rhythmic_clap_like_sequence() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInDetector

    detector = SipBargeInDetector(
        config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            short_speech_min_duration_ms=120,
        ),
        vad=FakeVad([True] * 20),
    )
    started_at = datetime.now(timezone.utc)
    frames = _pcm16_constant_frames([900, 5000, 800, 4500, 900, 5200, 850, 4800, 900])

    observations = [
        detector.observe(
            "call_clap_sequence_detector",
            frame,
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=True,
        )
        for index, frame in enumerate(frames)
    ]

    assert all(not observation.candidate for observation in observations)
    assert observations[-1].candidate_class is None
    assert observations[-1].reason == "non_speech_energy_envelope"


def test_sip_barge_in_detector_preserves_moderate_strong_short_speech_candidate() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInDetector

    detector = SipBargeInDetector(
        config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            short_speech_min_duration_ms=120,
        ),
        vad=FakeVad([True] * 12),
    )
    started_at = datetime.now(timezone.utc)
    short_speech_frame = _pcm16_constant_frame(amplitude=7000)

    observations = [
        detector.observe(
            "call_short_speech_detector",
            short_speech_frame,
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=True,
        )
        for index in range(6)
    ]

    assert observations[-1].candidate is True
    assert observations[-1].candidate_class == "strong_short_speech_candidate"


def test_sip_barge_in_detector_promotes_hot_speech_after_stable_duration() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInDetector

    detector = SipBargeInDetector(
        config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            short_speech_min_duration_ms=120,
        ),
        vad=FakeVad([True] * 10),
    )
    started_at = datetime.now(timezone.utc)
    hot_speech_frame = _pcm16_constant_frame(amplitude=16000)

    observations = [
        detector.observe(
            "call_hot_speech_detector",
            hot_speech_frame,
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=True,
        )
        for index in range(9)
    ]

    assert all(not observation.candidate for observation in observations[:8])
    assert observations[-1].candidate is True
    assert observations[-1].candidate_class == "stable_speech_candidate"


def test_sip_barge_in_detector_promotes_stable_speech_candidate() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig, SipBargeInDetector

    detector = SipBargeInDetector(
        config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
        ),
        vad=FakeVad([True] * 20),
    )
    started_at = datetime.now(timezone.utc)
    frame = _pcm16_constant_frame(amplitude=4000)

    observations = [
        detector.observe(
            "call_stable_detector",
            frame,
            now=started_at + timedelta(milliseconds=20 * index),
            interruptible=True,
        )
        for index in range(9)
    ]

    assert observations[-1].candidate is True
    assert observations[-1].candidate_class == "stable_speech_candidate"
    assert observations[-1].vad_voiced_ms >= 120
    assert observations[-1].candidate_duration_ms >= 180


def test_sip_frame_vad_shadow_detector_emits_speech_boundaries() -> None:
    detector = SipFrameVadShadowDetector(
        vad=FakeVad([True, True, False]),
        detector_name="fake_fsmn",
    )
    call_id = "call_shadow_boundaries"
    frame = _pcm16_constant_frame(amplitude=4000)
    now = datetime.now(timezone.utc)

    first = detector.observe(call_id, frame, now=now, interruptible=True)
    second = detector.observe(
        call_id,
        frame,
        now=now + timedelta(milliseconds=20),
        interruptible=True,
    )
    ended = detector.observe(
        call_id,
        frame,
        now=now + timedelta(milliseconds=40),
        interruptible=True,
    )

    assert first.started is True
    assert first.ended is False
    assert first.duration_ms == 20
    assert first.detector == "fake_fsmn"
    assert second.started is False
    assert second.active is True
    assert second.duration_ms == 40
    assert ended.started is False
    assert ended.ended is True
    assert ended.active is False
    assert ended.duration_ms == 40


def _sip_session(call_id: str) -> CallSession:
    return CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"sip-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )


async def _started_sip_runner(
    *,
    vad: FakeVad,
    call_id: str = "call_sip_p1",
    clean_window_ms: int = 60,
    max_hold_ms: int = 100,
    hold_timeout_seconds: float = 5.0,
    short_speech_min_duration_ms: int = 180,
    pre_stop_min_duration_ms: int = 240,
    snr_threshold_db: float = 10.0,
    recovery_silence_ms: int = 20,
    recovery_max_per_turn: int = 1,
    sip_vad_shadow_enabled: bool = False,
    sip_vad_shadow_detector: Any | None = None,
) -> tuple[
    RealtimeCallAgentRunner,
    InMemorySessionRegistry,
    InMemoryEventStore,
    FakeRealtimeProvider,
    FakeAudioPublisher,
    str,
]:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    registry = InMemorySessionRegistry()
    provider = FakeRealtimeProvider([])
    store = InMemoryEventStore()
    publisher = FakeAudioPublisher()
    session = _sip_session(call_id)
    registry.add(session)
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        sip_barge_in_enabled=True,
        sip_barge_in_fast_stop_enabled=True,
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=snr_threshold_db,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=pre_stop_min_duration_ms,
            short_speech_min_duration_ms=short_speech_min_duration_ms,
            clean_window_ms=clean_window_ms,
            max_hold_ms=max_hold_ms,
        ),
        sip_barge_in_vad=vad,
        sip_barge_in_hold_timeout_seconds=hold_timeout_seconds,
        sip_barge_in_recovery_silence_ms=recovery_silence_ms,
        sip_barge_in_recovery_max_per_turn=recovery_max_per_turn,
        sip_vad_shadow_enabled=sip_vad_shadow_enabled,
        sip_vad_shadow_detector=sip_vad_shadow_detector,
        user_turn_stability_delay_seconds=0,
    )
    await runner.start(session)
    registry.transition(call_id, CallSessionStatus.CONNECTED)
    registry.transition(call_id, CallSessionStatus.AI_THINKING)
    registry.transition(call_id, CallSessionStatus.AI_SPEAKING)
    runner._mark_response_started(call_id, {"response_id": "resp_sip_opening"})
    runner._response_lifecycle(call_id).active = True
    runner._playback_guard(call_id).current_response_audio_published = True
    return runner, registry, store, provider, publisher, call_id


class WaitingAudioPublisher(FakeAudioPublisher):
    def __init__(self) -> None:
        super().__init__()
        self.playout_wait_started = asyncio.Event()
        self.playout_release = asyncio.Event()

    async def wait_for_playout(self, call_id: str) -> None:
        _ = call_id
        self.playout_wait_started.set()
        await self.playout_release.wait()


class ImmediatePlayoutAudioPublisher(FakeAudioPublisher):
    async def wait_for_playout(self, call_id: str) -> None:
        _ = call_id


class BlockingAudioPublisher(FakeAudioPublisher):
    def __init__(self) -> None:
        super().__init__()
        self.publish_started = asyncio.Event()
        self.publish_release = asyncio.Event()

    async def publish_audio(self, call_id: str, frame: PcmAudioFrame) -> None:
        self.publish_started.set()
        await self.publish_release.wait()
        await super().publish_audio(call_id, frame)


class FakeRoomAudioTransport(FakeAudioPublisher):
    def __init__(self, frames: list[PcmAudioFrame]) -> None:
        super().__init__()
        self.frames = frames
        self.closed_call_ids: list[str] = []
        self.receive_call_ids: list[str] = []
        self.started_call_ids: list[str] = []

    async def start(self, session: CallSession) -> None:
        self.started_call_ids.append(session.call_id)

    async def receive_audio_frames(self, call_id: str):
        self.receive_call_ids.append(call_id)
        for frame in self.frames:
            await asyncio.sleep(0)
            yield frame

    async def close(self, call_id: str) -> None:
        self.closed_call_ids.append(call_id)


class FakeRtcAudioFrame:
    def __init__(
        self,
        data: bytes,
        sample_rate: int,
        num_channels: int,
        samples_per_channel: int,
    ) -> None:
        self.data = memoryview(data)
        self.sample_rate = sample_rate
        self.num_channels = num_channels
        self.samples_per_channel = samples_per_channel


@dataclass(slots=True)
class FakeRtcAudioFrameEvent:
    frame: FakeRtcAudioFrame


class FakeRtcAudioStream:
    def __init__(
        self,
        track,
        sample_rate: int,
        num_channels: int,
        frame_size_ms: int,
    ) -> None:
        self.track = track
        self.sample_rate = sample_rate
        self.num_channels = num_channels
        self.frame_size_ms = frame_size_ms
        self._events = list(track.audio_events)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._events:
            raise StopAsyncIteration
        await asyncio.sleep(0)
        return self._events.pop(0)


class FakeRtcAudioSource:
    def __init__(self, sample_rate: int, num_channels: int, queue_size_ms: int = 1000) -> None:
        self.sample_rate = sample_rate
        self.num_channels = num_channels
        self.queue_size_ms = queue_size_ms
        self.captured_frames: list[FakeRtcAudioFrame] = []
        self.clear_count = 0
        self.fail_capture = False

    async def capture_frame(self, frame: FakeRtcAudioFrame) -> None:
        if self.fail_capture:
            raise RuntimeError("capture frame failed")
        self.captured_frames.append(frame)

    def clear_queue(self) -> None:
        self.clear_count += 1


class FakeRtcLocalAudioTrack:
    def __init__(self, name: str, source: FakeRtcAudioSource) -> None:
        self.name = name
        self.source = source

    @classmethod
    def create_audio_track(cls, name: str, source: FakeRtcAudioSource):
        return cls(name, source)


class FakeRtcTrackPublishOptions:
    def __init__(self, source=None) -> None:
        self.source = source


class FakeRtcTrackSource:
    SOURCE_MICROPHONE = "microphone"


class FakeRtcModule:
    AudioFrame = FakeRtcAudioFrame
    AudioSource = FakeRtcAudioSource
    AudioStream = FakeRtcAudioStream
    LocalAudioTrack = FakeRtcLocalAudioTrack
    TrackPublishOptions = FakeRtcTrackPublishOptions
    TrackSource = FakeRtcTrackSource


class FakeLiveKitLocalParticipant:
    def __init__(self) -> None:
        self.published: list[tuple[FakeRtcLocalAudioTrack, FakeRtcTrackPublishOptions]] = []

    async def publish_track(
        self,
        track: FakeRtcLocalAudioTrack,
        options: FakeRtcTrackPublishOptions,
    ) -> None:
        self.published.append((track, options))


class FakeLiveKitRoom:
    def __init__(self) -> None:
        self.local_participant = FakeLiveKitLocalParticipant()
        self.connected: tuple[str, str] | None = None
        self.callbacks: dict[str, object] = {}
        self.disconnected = False

    def on(self, event: str, callback) -> None:
        self.callbacks[event] = callback

    async def connect(self, url: str, token: str) -> None:
        self.connected = (url, token)

    async def disconnect(self) -> None:
        self.disconnected = True


@dataclass(slots=True)
class FakeLiveKitParticipant:
    identity: str


@dataclass(slots=True)
class FakeRemoteAudioTrack:
    audio_events: list[FakeRtcAudioFrameEvent]


def build_orchestrator(
    agent_runner: FakeAgentRunner | None = None,
) -> tuple[AiCallOrchestrator, FakeLiveKitRoomManager, FakeAgentRunner]:
    livekit = FakeLiveKitRoomManager()
    agent = agent_runner or FakeAgentRunner()
    orchestrator = AiCallOrchestrator(
        config=AiCallRuntimeConfig(
            livekit_url="wss://livekit.test",
            livekit_api_key="livekit-key",
            livekit_api_secret="livekit-secret",
            browser_token_ttl_seconds=600,
            dashscope_api_key="dashscope-secret",
            dashscope_realtime_url="wss://dashscope.test/api-ws/v1/realtime",
            qwen_realtime_model="qwen3.5-omni-plus-realtime",
            qwen_realtime_voice="Tina",
            default_prompt="你是一个电话外呼助手，回答要简短自然。",
            opening_message="您好，我是灵宸智能助手，请问现在方便简单沟通一下吗？",
            web_audio_echo_cancellation=True,
            web_audio_noise_suppression=True,
            web_audio_auto_gain_control=True,
            vad_type="server_vad",
            vad_threshold=0.5,
            vad_silence_duration_ms=800,
        ),
        livekit_room_manager=livekit,
        agent_runner=agent,
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    return orchestrator, livekit, agent


def test_ai_call_runtime_config_defaults_to_server_vad(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("QWEN_REALTIME_TURN_DETECTION_TYPE", raising=False)

    runtime_config = AiCallRuntimeConfig.from_settings(Settings(_env_file=None))

    assert runtime_config.vad_type == "server_vad"


def test_ai_call_runtime_config_reads_sip_barge_in_settings() -> None:
    runtime_config = AiCallRuntimeConfig.from_settings(
        Settings(
            _env_file=None,
            AI_CALL_BARGE_IN_ENABLED=False,
            AI_CALL_SIP_BARGE_IN_ENABLED=True,
            AI_CALL_SIP_BARGE_IN_MIN_RMS_DBFS=-32.5,
            AI_CALL_SIP_BARGE_IN_MIN_SPEECH_DURATION_MS=260,
            AI_CALL_SIP_BARGE_IN_HOLD_TIMEOUT_SECONDS=4.5,
            AI_CALL_SIP_BARGE_IN_FAST_STOP_ENABLED=True,
            AI_CALL_SIP_BARGE_IN_RMS_THRESHOLD_DBFS=-36.0,
            AI_CALL_SIP_BARGE_IN_SNR_THRESHOLD_DB=11.0,
            AI_CALL_SIP_BARGE_IN_VAD_VOICED_DURATION_MS=130,
            AI_CALL_SIP_BARGE_IN_CANDIDATE_MIN_DURATION_MS=190,
            AI_CALL_SIP_BARGE_IN_PRE_STOP_MIN_DURATION_MS=250,
            AI_CALL_SIP_BARGE_IN_SHORT_SPEECH_MIN_DURATION_MS=125,
            AI_CALL_SIP_BARGE_IN_IMPULSE_NOISE_MAX_DURATION_MS=115,
            AI_CALL_SIP_BARGE_IN_CLEAN_WINDOW_MS=310,
            AI_CALL_SIP_BARGE_IN_MAX_HOLD_MS=510,
            AI_CALL_SIP_BARGE_IN_ECHO_TAIL_WINDOW_MS=520,
            AI_CALL_SIP_BARGE_IN_RECOVERY_SILENCE_MS=650,
            AI_CALL_SIP_BARGE_IN_RECOVERY_MAX_PER_TURN=2,
            AI_CALL_SIP_VAD_SHADOW_ENABLED=True,
            AI_CALL_SIP_VAD_SHADOW_DETECTOR="fsmn",
            AI_CALL_SIP_VAD_SHADOW_FSMN_MODEL="custom/fsmn-vad",
            AI_CALL_SIP_VAD_SHADOW_FSMN_ENDPOINT="http://127.0.0.1:19111/vad",
            AI_CALL_SIP_VAD_SHADOW_FSMN_TIMEOUT_SECONDS=0.15,
            AI_CALL_SIP_VAD_SHADOW_QUEUE_SIZE=8,
        )
    )

    assert runtime_config.barge_in_enabled is False
    assert runtime_config.sip_barge_in_enabled is True
    assert runtime_config.sip_barge_in_min_rms_dbfs == -32.5
    assert runtime_config.sip_barge_in_min_speech_duration_ms == 260
    assert runtime_config.sip_barge_in_hold_timeout_seconds == 4.5
    assert runtime_config.sip_barge_in_fast_stop_enabled is True
    assert runtime_config.sip_barge_in_config.rms_threshold_dbfs == -36.0
    assert runtime_config.sip_barge_in_config.snr_threshold_db == 11.0
    assert runtime_config.sip_barge_in_config.vad_voiced_duration_ms == 130
    assert runtime_config.sip_barge_in_config.candidate_min_duration_ms == 190
    assert runtime_config.sip_barge_in_config.pre_stop_min_duration_ms == 250
    assert runtime_config.sip_barge_in_config.short_speech_min_duration_ms == 125
    assert runtime_config.sip_barge_in_config.impulse_noise_max_duration_ms == 115
    assert runtime_config.sip_barge_in_config.clean_window_ms == 310
    assert runtime_config.sip_barge_in_config.max_hold_ms == 510
    assert runtime_config.sip_barge_in_config.echo_tail_window_ms == 520
    assert runtime_config.sip_barge_in_recovery_silence_ms == 650
    assert runtime_config.sip_barge_in_recovery_max_per_turn == 2
    assert runtime_config.sip_vad_shadow_enabled is True
    assert runtime_config.sip_vad_shadow_detector == "fsmn"
    assert runtime_config.sip_vad_shadow_fsmn_model == "custom/fsmn-vad"
    assert runtime_config.sip_vad_shadow_fsmn_endpoint == "http://127.0.0.1:19111/vad"
    assert runtime_config.sip_vad_shadow_fsmn_timeout_seconds == 0.15
    assert runtime_config.sip_vad_shadow_queue_size == 8


def test_orchestrator_default_runner_enables_sip_vad_shadow_when_configured() -> None:
    orchestrator = AiCallOrchestrator(
        config=AiCallRuntimeConfig(
            livekit_url="ws://livekit.test",
            livekit_api_key="key",
            livekit_api_secret="secret",
            browser_token_ttl_seconds=600,
            dashscope_api_key="dashscope-key",
            dashscope_realtime_url="wss://dashscope.test",
            qwen_realtime_model="qwen-realtime",
            qwen_realtime_voice="Tina",
            default_prompt="简短回答",
            opening_message="您好",
            web_audio_echo_cancellation=True,
            web_audio_noise_suppression=True,
            web_audio_auto_gain_control=True,
            vad_type="server_vad",
            vad_threshold=0.5,
            vad_silence_duration_ms=800,
            sip_vad_shadow_enabled=True,
        ),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )

    runner = orchestrator._build_default_agent_runner()

    assert isinstance(runner, RealtimeCallAgentRunner)
    assert runner.sip_vad_shadow_enabled is True
    assert runner._sip_vad_shadow_detector is not None


def test_orchestrator_default_runner_uses_configured_fsmn_shadow_fallback() -> None:
    orchestrator = AiCallOrchestrator(
        config=AiCallRuntimeConfig(
            livekit_url="ws://livekit.test",
            livekit_api_key="key",
            livekit_api_secret="secret",
            browser_token_ttl_seconds=600,
            dashscope_api_key="dashscope-key",
            dashscope_realtime_url="wss://dashscope.test",
            qwen_realtime_model="qwen-realtime",
            qwen_realtime_voice="Tina",
            default_prompt="简短回答",
            opening_message="您好",
            web_audio_echo_cancellation=True,
            web_audio_noise_suppression=True,
            web_audio_auto_gain_control=True,
            vad_type="server_vad",
            vad_threshold=0.5,
            vad_silence_duration_ms=800,
            sip_vad_shadow_enabled=True,
            sip_vad_shadow_detector="fsmn",
            sip_vad_shadow_fsmn_model="custom/fsmn-vad",
        ),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )

    runner = orchestrator._build_default_agent_runner()

    assert isinstance(runner, RealtimeCallAgentRunner)
    assert runner.sip_vad_shadow_enabled is True
    assert isinstance(runner._sip_vad_shadow_detector, UnavailableSipVadShadowDetector)


def test_orchestrator_default_runner_uses_configured_fsmn_sidecar_shadow() -> None:
    orchestrator = AiCallOrchestrator(
        config=AiCallRuntimeConfig(
            livekit_url="ws://livekit.test",
            livekit_api_key="key",
            livekit_api_secret="secret",
            browser_token_ttl_seconds=600,
            dashscope_api_key="dashscope-key",
            dashscope_realtime_url="wss://dashscope.test",
            qwen_realtime_model="qwen-realtime",
            qwen_realtime_voice="Tina",
            default_prompt="简短回答",
            opening_message="您好",
            web_audio_echo_cancellation=True,
            web_audio_noise_suppression=True,
            web_audio_auto_gain_control=True,
            vad_type="server_vad",
            vad_threshold=0.5,
            vad_silence_duration_ms=800,
            sip_vad_shadow_enabled=True,
            sip_vad_shadow_detector="fsmn",
            sip_vad_shadow_fsmn_model="custom/fsmn-vad",
            sip_vad_shadow_fsmn_endpoint="http://127.0.0.1:19111/vad",
            sip_vad_shadow_fsmn_timeout_seconds=0.15,
            sip_vad_shadow_queue_size=8,
        ),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )

    runner = orchestrator._build_default_agent_runner()

    assert isinstance(runner, RealtimeCallAgentRunner)
    assert runner.sip_vad_shadow_enabled is True
    assert isinstance(runner._sip_vad_shadow_detector, QueuedSipVadShadowDetector)
    runner._sip_vad_shadow_detector.close()


def test_orchestrator_default_runner_uses_configured_webrtc_and_fsmn_shadow() -> None:
    orchestrator = AiCallOrchestrator(
        config=AiCallRuntimeConfig(
            livekit_url="ws://livekit.test",
            livekit_api_key="key",
            livekit_api_secret="secret",
            browser_token_ttl_seconds=600,
            dashscope_api_key="dashscope-key",
            dashscope_realtime_url="wss://dashscope.test",
            qwen_realtime_model="qwen-realtime",
            qwen_realtime_voice="Tina",
            default_prompt="简短回答",
            opening_message="您好",
            web_audio_echo_cancellation=True,
            web_audio_noise_suppression=True,
            web_audio_auto_gain_control=True,
            vad_type="server_vad",
            vad_threshold=0.5,
            vad_silence_duration_ms=800,
            sip_vad_shadow_enabled=True,
            sip_vad_shadow_detector="webrtc+fsmn",
            sip_vad_shadow_fsmn_model="custom/fsmn-vad",
            sip_vad_shadow_fsmn_endpoint="http://127.0.0.1:19111/vad",
            sip_vad_shadow_fsmn_timeout_seconds=0.15,
            sip_vad_shadow_queue_size=8,
        ),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )

    runner = orchestrator._build_default_agent_runner()

    assert isinstance(runner, RealtimeCallAgentRunner)
    assert runner.sip_vad_shadow_enabled is True
    assert isinstance(runner._sip_vad_shadow_detector, MultiSipVadShadowDetector)
    assert runner._sip_vad_shadow_detector.detector_names == ("webrtc_shadow", "fsmn_shadow")
    runner._sip_vad_shadow_detector.close()


def test_orchestrator_default_agent_runner_uses_realtime_qwen_livekit_stack() -> None:
    metrics_by_call_id: dict[str, CallMetrics] = {}
    orchestrator = AiCallOrchestrator(
        config=AiCallRuntimeConfig(
            livekit_url="wss://livekit.test",
            livekit_api_key="livekit-key",
            livekit_api_secret="livekit-secret",
            browser_token_ttl_seconds=600,
            dashscope_api_key="dashscope-secret",
            dashscope_realtime_url="wss://dashscope.test/api-ws/v1/realtime",
            qwen_realtime_model="qwen3.5-omni-plus-realtime",
            qwen_realtime_voice="Tina",
            default_prompt="你是一个电话外呼助手，回答要简短自然。",
            opening_message="您好，我是灵宸智能助手，请问现在方便简单沟通一下吗？",
            web_audio_echo_cancellation=True,
            web_audio_noise_suppression=True,
            web_audio_auto_gain_control=True,
            vad_type="server_vad",
            vad_threshold=0.5,
            vad_silence_duration_ms=800,
        ),
        metrics_by_call_id=metrics_by_call_id,
    )

    assert orchestrator.metrics_by_call_id is metrics_by_call_id
    assert isinstance(orchestrator.agent_runner, RealtimeCallAgentRunner)
    assert orchestrator.agent_runner.registry is orchestrator.registry
    assert orchestrator.agent_runner.event_store is orchestrator.event_store
    assert orchestrator.agent_runner.metrics_by_call_id is orchestrator.metrics_by_call_id
    assert isinstance(orchestrator.agent_runner.audio_transport, LiveKitRoomAudioTransport)


@pytest.mark.anyio
async def test_create_session_uses_defaults_and_keeps_secrets_out_of_browser_response() -> None:
    orchestrator, livekit, agent = build_orchestrator()

    result = await orchestrator.create_web_session(voice=None, prompt=None)

    assert result.call_id.startswith("call_")
    assert result.room_name == f"ai-call-{result.call_id}"
    assert result.livekit_url == "wss://livekit.test"
    assert result.participant_identity == f"browser-{result.call_id}"
    assert "dashscope-secret" not in result.participant_token
    assert "livekit-secret" not in result.participant_token
    assert result.status == CallSessionStatus.READY
    assert result.effective_config.model == "qwen3.5-omni-plus-realtime"
    assert result.effective_config.voice == "Tina"
    assert result.effective_config.prompt_hash.startswith("sha256:")
    assert result.effective_config.opening_message_hash.startswith("sha256:")
    assert livekit.created_rooms == [result.room_name]
    assert agent.started_call_ids == [result.call_id]

    events = await orchestrator.list_events(result.call_id)
    assert [event.type for event in events.rows] == [
        "session_created",
        "session_preparing",
        "room_created",
        "browser_token_issued",
        "agent_started",
        "session_ready",
    ]


@pytest.mark.anyio
async def test_create_session_records_agent_runner_runtime_diagnostics() -> None:
    orchestrator, _livekit, _agent = build_orchestrator()
    orchestrator.agent_runner = DiagnosticAgentRunner()

    result = await orchestrator.create_web_session(voice=None, prompt=None)
    events = await orchestrator.list_events(result.call_id)

    agent_started = next(event for event in events.rows if event.type == "agent_started")
    assert agent_started.payload == {
        "diagnosticsVersion": "test-runtime",
        "runnerModule": "tests.fake_agent",
        "runnerSourceHash": "sha256:test",
    }


@pytest.mark.anyio
async def test_create_session_rejects_empty_opening_message_before_room_create() -> None:
    orchestrator, livekit, _agent = build_orchestrator()
    orchestrator.config = replace(orchestrator.config, opening_message=" ")

    with pytest.raises(AiCallError) as exc_info:
        await orchestrator.create_web_session(voice=None, prompt=None)

    assert exc_info.value.error_id == "opening_message_empty"
    assert livekit.created_rooms == []


@pytest.mark.anyio
async def test_create_session_cleans_up_room_and_marks_failed_when_agent_start_fails() -> None:
    livekit = FakeLiveKitRoomManager()
    agent = FailingAgentRunner()
    orchestrator = AiCallOrchestrator(
        config=AiCallRuntimeConfig(
            livekit_url="wss://livekit.test",
            livekit_api_key="livekit-key",
            livekit_api_secret="livekit-secret",
            browser_token_ttl_seconds=600,
            dashscope_api_key="dashscope-secret",
            dashscope_realtime_url="wss://dashscope.test/api-ws/v1/realtime",
            qwen_realtime_model="qwen3.5-omni-plus-realtime",
            qwen_realtime_voice="Tina",
            default_prompt="你是一个电话外呼助手，回答要简短自然。",
            opening_message="您好，我是灵宸智能助手，请问现在方便简单沟通一下吗？",
            web_audio_echo_cancellation=True,
            web_audio_noise_suppression=True,
            web_audio_auto_gain_control=True,
            vad_type="server_vad",
            vad_threshold=0.5,
            vad_silence_duration_ms=800,
        ),
        livekit_room_manager=livekit,
        agent_runner=agent,
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )

    with pytest.raises(AiCallError) as exc_info:
        await orchestrator.create_web_session(voice=None, prompt=None)

    assert exc_info.value.error_id == "agent_start_failed"
    assert livekit.deleted_rooms == livekit.created_rooms
    call_id = livekit.created_rooms[0].removeprefix("ai-call-")
    assert agent.stopped_call_ids == [call_id]
    assert orchestrator.registry.get(call_id).status == CallSessionStatus.FAILED
    events = await orchestrator.list_events(call_id)
    assert [event.type for event in events.rows][-2:] == [
        "agent_start_failed",
        "session_failed",
    ]
    assert events.rows[-2].payload["errorType"] == "RuntimeError"
    assert events.rows[-2].payload["errorMessage"] == "agent boom"


@pytest.mark.anyio
async def test_token_reissue_does_not_start_new_agent_and_rejects_completed_session() -> None:
    orchestrator, _livekit, agent = build_orchestrator()
    created = await orchestrator.create_web_session(voice="Cindy", prompt="简短回答")

    token = await orchestrator.reissue_browser_token(created.call_id)

    assert token.call_id == created.call_id
    assert token.room_name == created.room_name
    assert token.participant_identity == f"browser-{created.call_id}"
    assert agent.started_call_ids == [created.call_id]

    ended = await orchestrator.end_session(created.call_id)
    assert ended.status == CallSessionStatus.COMPLETED
    assert agent.stopped_call_ids == [created.call_id]

    with pytest.raises(AiCallError) as exc_info:
        await orchestrator.reissue_browser_token(created.call_id)
    assert exc_info.value.error_id == "invalid_session_state"


@pytest.mark.anyio
async def test_end_failed_session_still_cleans_up_agent_and_room() -> None:
    orchestrator, livekit, agent = build_orchestrator()
    created = await orchestrator.create_web_session(voice=None, prompt=None)
    orchestrator.registry.transition(created.call_id, CallSessionStatus.FAILED)

    ended = await orchestrator.end_session(created.call_id)

    assert ended.status == CallSessionStatus.FAILED
    assert agent.stopped_call_ids == [created.call_id]
    assert livekit.deleted_rooms == [created.room_name]


@pytest.mark.anyio
async def test_orchestrator_auto_end_session_completes_with_model_reason() -> None:
    orchestrator, livekit, agent = build_orchestrator()
    created = await orchestrator.create_web_session(voice=None, prompt=None)

    orchestrator._schedule_auto_end_session(created.call_id, "customer_end")

    await asyncio.wait_for(
        _wait_until(
            lambda: orchestrator.registry.get(created.call_id).status == CallSessionStatus.COMPLETED
        ),
        timeout=1,
    )
    events = await orchestrator.list_events(created.call_id)

    assert agent.stopped_call_ids == [created.call_id]
    assert livekit.deleted_rooms == [created.room_name]
    assert events.rows[-1].type == "session_completed"
    assert events.rows[-1].payload == {"endReason": "customer_end"}


@pytest.mark.anyio
async def test_end_session_is_idempotent_while_session_is_ending() -> None:
    agent = BlockingStopAgentRunner()
    orchestrator, livekit, _agent = build_orchestrator(agent_runner=agent)
    created = await orchestrator.create_web_session(voice=None, prompt=None)

    first_end = asyncio.create_task(
        orchestrator.end_session(created.call_id, end_reason="customer_end")
    )
    await asyncio.wait_for(agent.stop_started.wait(), timeout=1)

    second_end = await orchestrator.end_session(created.call_id, end_reason="web_user_end")

    assert second_end.status == CallSessionStatus.ENDING
    assert orchestrator.registry.get(created.call_id).status == CallSessionStatus.ENDING

    agent.release_stop.set()
    first_result = await asyncio.wait_for(first_end, timeout=1)

    events = await orchestrator.list_events(created.call_id)
    event_types = [event.type for event in events.rows]
    assert first_result.status == CallSessionStatus.COMPLETED
    assert orchestrator.registry.get(created.call_id).status == CallSessionStatus.COMPLETED
    assert event_types.count("session_ending") == 1
    assert event_types.count("session_completed") == 1
    assert events.rows[-1].payload == {"endReason": "customer_end"}
    assert livekit.deleted_rooms == [created.room_name]


@pytest.mark.anyio
async def test_end_session_completes_even_when_agent_stop_blocks() -> None:
    agent = BlockingStopAgentRunner()
    livekit = FakeLiveKitRoomManager()
    orchestrator = AiCallOrchestrator(
        config=AiCallRuntimeConfig(
            livekit_url="wss://livekit.test",
            livekit_api_key="livekit-key",
            livekit_api_secret="livekit-secret",
            browser_token_ttl_seconds=600,
            dashscope_api_key="dashscope-secret",
            dashscope_realtime_url="wss://dashscope.test/api-ws/v1/realtime",
            qwen_realtime_model="qwen3.5-omni-plus-realtime",
            qwen_realtime_voice="Tina",
            default_prompt="你是一个电话外呼助手，回答要简短自然。",
            opening_message="您好，我是灵宸智能助手，请问现在方便简单沟通一下吗？",
            web_audio_echo_cancellation=True,
            web_audio_noise_suppression=True,
            web_audio_auto_gain_control=True,
            vad_type="server_vad",
            vad_threshold=0.5,
            vad_silence_duration_ms=800,
        ),
        livekit_room_manager=livekit,
        agent_runner=agent,
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        end_cleanup_timeout_seconds=0.01,
    )
    created = await orchestrator.create_web_session(voice=None, prompt=None)

    ended = await asyncio.wait_for(
        orchestrator.end_session(created.call_id, end_reason="web_user_end"),
        timeout=0.2,
    )

    events = await orchestrator.list_events(created.call_id)
    event_types = [event.type for event in events.rows]
    assert ended.status == CallSessionStatus.COMPLETED
    assert orchestrator.registry.get(created.call_id).status == CallSessionStatus.COMPLETED
    assert agent.stopped_call_ids == [created.call_id]
    assert livekit.deleted_rooms == [created.room_name]
    assert "session_cleanup_timeout" in event_types
    assert event_types.index("session_cleanup_timeout") < event_types.index("session_completed")


@pytest.mark.anyio
async def test_orchestrator_auto_end_session_cleans_room_on_model_error() -> None:
    orchestrator, livekit, _agent = build_orchestrator()
    provider = QueueRealtimeProvider()
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=orchestrator.registry,
        event_store=orchestrator.event_store,
        call_end_scheduler=orchestrator._schedule_auto_end_session,
    )
    orchestrator.agent_runner = runner
    created = await orchestrator.create_web_session(voice=None, prompt=None)

    await provider.emit(
        ProviderEvent(
            type="model_error",
            payload={"error": {"message": "insufficient balance"}},
        )
    )
    await _wait_until(lambda: livekit.deleted_rooms == [created.room_name])

    assert orchestrator.registry.get(created.call_id).status == CallSessionStatus.FAILED
    assert provider.closed is True
    events = await orchestrator.list_events(created.call_id)
    assert "session_failed" in [event.type for event in events.rows]


def test_event_store_returns_sorted_limited_incremental_events() -> None:
    store = InMemoryEventStore()
    first = store.append(call_id="call_1", type="session_created", source="orchestrator")
    second = store.append(call_id="call_1", type="room_created", source="livekit")
    store.append(call_id="call_2", type="session_created", source="orchestrator")
    third = store.append(call_id="call_1", type="agent_started", source="agent")

    rows = store.list(call_id="call_1", limit=2)
    assert [event.event_id for event in rows] == [first.event_id, second.event_id]

    rows_after = store.list(call_id="call_1", after_event_id=first.event_id, limit=1000)
    assert [event.event_id for event in rows_after] == [second.event_id, third.event_id]


def test_session_registry_rejects_illegal_state_transition() -> None:
    registry = InMemorySessionRegistry()
    session = CallSession(
        call_id="call_1",
        room_name="ai-call-call_1",
        participant_identity="browser-call_1",
        status=CallSessionStatus.CREATED,
        effective_config={},
    )
    registry.add(session)

    with pytest.raises(AiCallError) as exc_info:
        registry.transition("call_1", CallSessionStatus.AI_SPEAKING)

    assert exc_info.value.error_id == "invalid_session_state"


def test_metrics_calculates_first_audio_and_interrupt_latency() -> None:
    metrics = CallMetrics()
    start = datetime(2026, 6, 15, 9, 0, tzinfo=timezone.utc)

    metrics.mark_user_speech_stopped(start)
    metrics.mark_model_audio_delta(start + timedelta(milliseconds=820))
    metrics.mark_browser_first_audio(start + timedelta(milliseconds=960))
    metrics.mark_interrupt_confirmed(start + timedelta(seconds=2))
    metrics.mark_ai_audio_stopped(start + timedelta(seconds=2, milliseconds=180))

    snapshot = metrics.snapshot()
    assert snapshot["lastModelFirstAudioMs"] == 820
    assert snapshot["lastBrowserFirstAudioMs"] == 960
    assert snapshot["lastInterruptStopMs"] == 180
    assert snapshot["modelFirstAudioCount"] == 1
    assert snapshot["modelFirstAudioP50Ms"] == 820
    assert snapshot["modelFirstAudioP90Ms"] == 820
    assert snapshot["modelFirstAudioMaxMs"] == 820
    assert snapshot["browserFirstAudioCount"] == 1
    assert snapshot["browserFirstAudioP50Ms"] == 960
    assert snapshot["browserFirstAudioP90Ms"] == 960
    assert snapshot["browserFirstAudioMaxMs"] == 960


@pytest.mark.anyio
async def test_browser_first_audio_report_updates_metrics_and_events() -> None:
    orchestrator, _livekit, _agent = build_orchestrator()
    created = await orchestrator.create_web_session(voice=None, prompt=None)
    metrics = CallMetrics()
    speech_stopped_at = datetime(2026, 6, 15, 9, 0, tzinfo=timezone.utc)
    metrics.mark_user_speech_stopped(speech_stopped_at)
    orchestrator.metrics_by_call_id[created.call_id] = metrics

    await orchestrator.report_browser_event(
        call_id=created.call_id,
        event_type="browser_first_audio",
        timestamp=speech_stopped_at + timedelta(milliseconds=960),
    )

    status = await orchestrator.get_session(created.call_id)
    assert status.metrics["lastBrowserFirstAudioMs"] == 960
    events = await orchestrator.list_events(created.call_id)
    assert events.rows[-1].type == "browser_first_audio"
    assert events.rows[-1].source == "browser"


@pytest.mark.anyio
async def test_browser_ready_report_marks_session_connected() -> None:
    orchestrator, _livekit, agent = build_orchestrator()
    created = await orchestrator.create_web_session(voice=None, prompt=None)

    await orchestrator.report_browser_event(
        call_id=created.call_id,
        event_type="browser_ready",
    )

    status = await orchestrator.get_session(created.call_id)
    assert status.status == CallSessionStatus.CONNECTED
    assert agent.started_opening_call_ids == [created.call_id]
    events = await orchestrator.list_events(created.call_id)
    assert [event.type for event in events.rows][-2:] == ["browser_ready", "opening_started"]
    assert events.rows[-2].source == "browser"
    assert events.rows[-1].source == "agent"


@pytest.mark.anyio
async def test_browser_ready_opening_marks_model_response_timing_baseline() -> None:
    orchestrator, _livekit, _agent = build_orchestrator()
    created = await orchestrator.create_web_session(voice=None, prompt=None)

    await orchestrator.report_browser_event(
        call_id=created.call_id,
        event_type="browser_ready",
    )

    metrics = orchestrator.metrics_by_call_id[created.call_id]
    assert metrics.last_model_response_requested_at is not None


@pytest.mark.anyio
async def test_browser_user_speech_started_records_candidate_while_ai_speaking() -> None:
    orchestrator, _livekit, agent = build_orchestrator()
    created = await orchestrator.create_web_session(voice=None, prompt=None)
    orchestrator.registry.transition(created.call_id, CallSessionStatus.CONNECTED)
    orchestrator.registry.transition(created.call_id, CallSessionStatus.AI_SPEAKING)
    reported_at = datetime(2026, 6, 15, 9, 0, tzinfo=timezone.utc)

    await orchestrator.report_browser_event(
        call_id=created.call_id,
        event_type="browser_user_speech_started",
        timestamp=reported_at,
    )

    events = await orchestrator.list_events(created.call_id)
    assert events.rows[-1].type == "browser_user_speech_started"
    assert events.rows[-1].source == "browser"
    assert agent.browser_speech_candidates == [(created.call_id, events.rows[-1].timestamp)]
    assert events.rows[-1].payload["reportedAt"] == reported_at.isoformat()


@pytest.mark.anyio
async def test_browser_user_speech_started_is_forwarded_as_candidate() -> None:
    orchestrator, _livekit, agent = build_orchestrator()
    created = await orchestrator.create_web_session(voice=None, prompt=None)
    orchestrator.registry.transition(created.call_id, CallSessionStatus.CONNECTED)
    reported_at = datetime(2026, 6, 15, 9, 0, tzinfo=timezone.utc)

    await orchestrator.report_browser_event(
        call_id=created.call_id,
        event_type="browser_user_speech_started",
        timestamp=reported_at,
    )

    events = await orchestrator.list_events(created.call_id)
    assert events.rows[-1].type == "browser_user_speech_started"
    assert events.rows[-1].source == "browser"
    assert agent.browser_speech_candidates == [(created.call_id, events.rows[-1].timestamp)]
    assert events.rows[-1].payload["reportedAt"] == reported_at.isoformat()


@pytest.mark.anyio
async def test_browser_speech_segment_is_forwarded_with_quality_payload() -> None:
    orchestrator, _livekit, agent = build_orchestrator()
    created = await orchestrator.create_web_session(voice=None, prompt=None)
    orchestrator.registry.transition(created.call_id, CallSessionStatus.CONNECTED)
    orchestrator.registry.transition(created.call_id, CallSessionStatus.AI_SPEAKING)
    reported_at = datetime(2026, 6, 23, 9, 0, tzinfo=timezone.utc)
    payload = {
        "segmentId": "browser-seg-1",
        "phase": "updated",
        "durationMs": 440,
        "rmsDbfs": -30.0,
        "noiseFloorDbfs": -45.0,
        "snrDb": 15.0,
        "hotFrameCount": 11,
        "remoteAudioActive": True,
    }

    await orchestrator.report_browser_event(
        call_id=created.call_id,
        event_type="browser_user_speech_segment",
        timestamp=reported_at,
        payload=payload,
    )

    events = await orchestrator.list_events(created.call_id)
    assert events.rows[-1].type == "browser_user_speech_segment"
    assert events.rows[-1].source == "browser"
    assert events.rows[-1].payload["reportedAt"] == reported_at.isoformat()
    assert events.rows[-1].payload["segmentId"] == "browser-seg-1"
    assert events.rows[-1].payload["durationMs"] == 440
    assert agent.browser_speech_segments == [
        (created.call_id, events.rows[-1].timestamp, events.rows[-1].payload)
    ]


@pytest.mark.anyio
async def test_browser_disconnect_accepts_sqlite_lock_during_recording_stop() -> None:
    class LockedRecordingService:
        async def stop_for_session(self, call_id: str) -> None:
            raise OperationalError(
                "UPDATE ai_call_recording SET status=? WHERE ai_call_recording.id = ?",
                {},
                Exception("database is locked"),
            )

    orchestrator, _livekit, _agent = build_orchestrator()
    created = await orchestrator.create_web_session(voice=None, prompt=None)
    service = AiCallService(orchestrator, recording_service=LockedRecordingService())

    result = await service.report_browser_event(
        call_id=created.call_id,
        event_type="browser_disconnect",
        timestamp=None,
    )

    assert result.type == "browser_disconnect"
    status = await orchestrator.get_session(created.call_id)
    assert status.status == CallSessionStatus.COMPLETED


@pytest.mark.anyio
async def test_browser_disconnect_is_idempotent_after_failed_session() -> None:
    class RecordingServiceThatMustNotStop:
        async def stop_for_session(self, call_id: str) -> None:
            raise AssertionError("terminal browser disconnect should not stop recording inline")

    orchestrator, _livekit, _agent = build_orchestrator()
    created = await orchestrator.create_web_session(voice=None, prompt=None)
    orchestrator.registry.transition(created.call_id, CallSessionStatus.FAILED)
    service = AiCallService(
        orchestrator,
        recording_service=RecordingServiceThatMustNotStop(),
    )

    result = await service.report_browser_event(
        call_id=created.call_id,
        event_type="browser_disconnect",
        timestamp=None,
    )

    assert result.type == "browser_disconnect"
    assert result.payload["terminalSessionStatus"] == "failed"
    status = await orchestrator.get_session(created.call_id)
    assert status.status == CallSessionStatus.FAILED


@pytest.mark.anyio
async def test_late_browser_telemetry_is_idempotent_after_completed_session() -> None:
    orchestrator, _livekit, _agent = build_orchestrator()
    created = await orchestrator.create_web_session(voice=None, prompt=None)
    await orchestrator.end_session(created.call_id, end_reason="normal_completed")

    result = await orchestrator.report_browser_event(
        call_id=created.call_id,
        event_type="browser_first_audio",
        timestamp=None,
    )

    assert result.type == "browser_first_audio"
    assert result.payload["terminalSessionStatus"] == "completed"
    status = await orchestrator.get_session(created.call_id)
    assert status.status == CallSessionStatus.COMPLETED


@pytest.mark.anyio
async def test_browser_audio_input_diagnostics_is_recorded_without_agent_side_effects() -> None:
    orchestrator, _livekit, agent = build_orchestrator()
    created = await orchestrator.create_web_session(voice=None, prompt=None)
    orchestrator.registry.transition(created.call_id, CallSessionStatus.CONNECTED)
    reported_at = datetime(2026, 6, 24, 9, 0, tzinfo=timezone.utc)
    payload = {
        "diagnosticsVersion": "browser-audio-input-v1",
        "source": "livekit_local_audio_track",
        "trackLabel": "MacBook Pro Microphone",
        "trackState": {"enabled": True, "muted": False, "readyState": "live"},
        "requestedConstraints": {
            "echoCancellation": True,
            "noiseSuppression": True,
            "autoGainControl": True,
        },
        "trackSettings": {
            "deviceId": "mic-1",
            "echoCancellation": True,
            "noiseSuppression": True,
            "autoGainControl": True,
            "sampleRate": 48000,
            "channelCount": 1,
        },
        "audioContext": {"sampleRate": 48000},
    }

    await orchestrator.report_browser_event(
        call_id=created.call_id,
        event_type="browser_audio_input_diagnostics",
        timestamp=reported_at,
        payload=payload,
    )

    events = await orchestrator.list_events(created.call_id)
    assert events.rows[-1].type == "browser_audio_input_diagnostics"
    assert events.rows[-1].source == "browser"
    assert events.rows[-1].payload["reportedAt"] == reported_at.isoformat()
    assert events.rows[-1].payload["diagnosticsVersion"] == "browser-audio-input-v1"
    assert events.rows[-1].payload["trackSettings"]["echoCancellation"] is True
    assert agent.browser_speech_candidates == []
    assert agent.browser_speech_segments == []


def test_audio_bridge_downsamples_48k_mono_pcm_to_16k_20ms_frame() -> None:
    input_samples = list(range(960))
    input_pcm = struct.pack("<" + "h" * len(input_samples), *input_samples)
    frame = PcmAudioFrame(
        data=input_pcm,
        sample_rate_hz=48000,
        channels=1,
        sample_width_bytes=2,
    )

    chunks = list(PcmAudioBridge().iter_qwen_input_chunks(frame))

    assert len(chunks) == 1
    assert len(chunks[0]) == 640
    output_samples = struct.unpack("<" + "h" * 320, chunks[0])
    assert list(output_samples[:5]) == [0, 3, 6, 9, 12]
    assert output_samples[-1] == 957


def test_audio_bridge_rejects_non_mono_pcm_until_mixdown_is_defined() -> None:
    frame = PcmAudioFrame(
        data=b"\x00" * 1280,
        sample_rate_hz=48000,
        channels=2,
        sample_width_bytes=2,
    )

    with pytest.raises(AudioBridgeError) as exc_info:
        list(PcmAudioBridge().iter_qwen_input_chunks(frame))

    assert exc_info.value.reason == "unsupported_channel_count"


def test_audio_bridge_splits_qwen_output_into_40ms_playout_frames() -> None:
    pcm = b"\x01\x02" * 7680
    frame = PcmAudioFrame(data=pcm, sample_rate_hz=24000, channels=1)

    chunks = list(PcmAudioBridge().iter_output_playout_frames(frame))

    assert len(chunks) == 8
    assert {len(chunk.data) for chunk in chunks} == {1920}
    assert b"".join(chunk.data for chunk in chunks) == pcm


@pytest.mark.anyio
async def test_livekit_room_audio_transport_connects_publishes_and_receives_pcm_frames() -> None:
    room = FakeLiveKitRoom()
    transport = LiveKitRoomAudioTransport(
        livekit_url="wss://livekit.test",
        api_key="livekit-key",
        api_secret="livekit-secret",
        rtc_module=FakeRtcModule,
        room_factory=lambda: room,
    )
    session = CallSession(
        call_id="call_transport",
        room_name="ai-call-call_transport",
        participant_identity="browser-call_transport",
        status=CallSessionStatus.READY,
        effective_config={},
    )

    await transport.start(session)

    assert room.connected is not None
    assert room.connected[0] == "wss://livekit.test"
    assert "livekit-secret" not in room.connected[1]
    published_track, publish_options = room.local_participant.published[0]
    assert published_track.name == "ai_audio"
    assert published_track.source.queue_size_ms == 200
    assert publish_options.source == FakeRtcTrackSource.SOURCE_MICROPHONE

    await transport.publish_audio(
        "call_transport",
        PcmAudioFrame(data=b"\x01\x02" * 240, sample_rate_hz=24000, channels=1),
    )
    assert published_track.source.captured_frames[0].sample_rate == 24000
    assert bytes(published_track.source.captured_frames[0].data) == b"\x01\x02" * 240

    incoming_frame = FakeRtcAudioFrame(
        data=b"\x03\x04" * 480,
        sample_rate=48000,
        num_channels=1,
        samples_per_channel=480,
    )
    room.callbacks["track_subscribed"](
        FakeRemoteAudioTrack([FakeRtcAudioFrameEvent(incoming_frame)]),
        object(),
        FakeLiveKitParticipant(identity="browser-call_transport"),
    )
    receiver = transport.receive_audio_frames("call_transport")
    received = await anext(receiver)

    assert received.data == b"\x03\x04" * 480
    assert received.sample_rate_hz == 48000
    assert received.channels == 1

    await transport.stop_audio("call_transport")
    assert published_track.source.clear_count == 1

    await transport.close("call_transport")
    assert room.disconnected is True


@pytest.mark.anyio
async def test_livekit_room_audio_transport_filters_to_session_participant_identity() -> None:
    room = FakeLiveKitRoom()
    transport = LiveKitRoomAudioTransport(
        livekit_url="wss://livekit.test",
        api_key="livekit-key",
        api_secret="livekit-secret",
        rtc_module=FakeRtcModule,
        room_factory=lambda: room,
    )
    session = CallSession(
        call_id="call_sip_filter",
        room_name="ai-call-call_sip_filter",
        participant_identity="sip-call_sip_filter",
        status=CallSessionStatus.READY,
        effective_config={},
    )

    await transport.start(session)
    agent_track = FakeRemoteAudioTrack([
        FakeRtcAudioFrameEvent(FakeRtcAudioFrame(b"\x02\x00" * 160, 8000, 1, 160))
    ])
    sip_track = FakeRemoteAudioTrack([
        FakeRtcAudioFrameEvent(FakeRtcAudioFrame(b"\x01\x00" * 160, 8000, 1, 160))
    ])

    room.callbacks["track_subscribed"](
        agent_track,
        object(),
        FakeLiveKitParticipant(identity="agent-call_sip_filter"),
    )
    room.callbacks["track_subscribed"](
        sip_track,
        object(),
        FakeLiveKitParticipant(identity="sip-call_sip_filter"),
    )
    receiver = transport.receive_audio_frames("call_sip_filter")
    received = await anext(receiver)

    assert received.data == b"\x01\x00" * 160
    assert received.sample_rate_hz == 8000
    assert received.channels == 1


@pytest.mark.anyio
async def test_livekit_room_audio_transport_does_not_replay_old_tail_when_stopping_audio() -> None:
    room = FakeLiveKitRoom()
    transport = LiveKitRoomAudioTransport(
        livekit_url="wss://livekit.test",
        api_key="livekit-key",
        api_secret="livekit-secret",
        rtc_module=FakeRtcModule,
        room_factory=lambda: room,
    )
    session = CallSession(
        call_id="call_fade",
        room_name="ai-call-call_fade",
        participant_identity="browser-call_fade",
        status=CallSessionStatus.READY,
        effective_config={},
    )

    await transport.start(session)
    published_track, _publish_options = room.local_participant.published[0]
    samples = [12000] * 2400
    pcm = struct.pack("<" + "h" * len(samples), *samples)

    await transport.publish_audio(
        "call_fade",
        PcmAudioFrame(data=pcm, sample_rate_hz=24000, channels=1),
    )
    await transport.stop_audio("call_fade")

    assert published_track.source.clear_count == 1
    assert len(published_track.source.captured_frames) == 1


@pytest.mark.anyio
async def test_livekit_room_audio_transport_stop_audio_keeps_clear_when_fade_capture_fails() -> (
    None
):
    room = FakeLiveKitRoom()
    transport = LiveKitRoomAudioTransport(
        livekit_url="wss://livekit.test",
        api_key="livekit-key",
        api_secret="livekit-secret",
        rtc_module=FakeRtcModule,
        room_factory=lambda: room,
    )
    session = CallSession(
        call_id="call_fade_capture_failure",
        room_name="ai-call-call_fade_capture_failure",
        participant_identity="browser-call_fade_capture_failure",
        status=CallSessionStatus.READY,
        effective_config={},
    )

    await transport.start(session)
    published_track, _publish_options = room.local_participant.published[0]
    await transport.publish_audio(
        "call_fade_capture_failure",
        PcmAudioFrame(data=b"\x01\x02" * 2400, sample_rate_hz=24000, channels=1),
    )
    published_track.source.fail_capture = True

    await transport.stop_audio("call_fade_capture_failure")

    assert published_track.source.clear_count == 1


@pytest.mark.anyio
async def test_livekit_room_audio_transport_close_ignores_stop_audio_failure() -> None:
    room = FakeLiveKitRoom()
    transport = LiveKitRoomAudioTransport(
        livekit_url="wss://livekit.test",
        api_key="livekit-key",
        api_secret="livekit-secret",
        rtc_module=FakeRtcModule,
        room_factory=lambda: room,
    )
    session = CallSession(
        call_id="call_close_fade_failure",
        room_name="ai-call-call_close_fade_failure",
        participant_identity="browser-call_close_fade_failure",
        status=CallSessionStatus.READY,
        effective_config={},
    )

    await transport.start(session)
    published_track, _publish_options = room.local_participant.published[0]
    await transport.publish_audio(
        "call_close_fade_failure",
        PcmAudioFrame(data=b"\x01\x02" * 240, sample_rate_hz=24000, channels=1),
    )
    published_track.source.fail_capture = True

    await transport.close("call_close_fade_failure")

    assert published_track.source.clear_count == 1
    assert room.disconnected is True


def test_qwen_session_update_and_server_event_mapping() -> None:
    payload = build_session_update_event(
        QwenRealtimeSessionConfig(
            voice="Tina",
            instructions="你是一个电话外呼助手，回答要简短自然。",
            vad_type="server_vad",
            vad_threshold=0.5,
            vad_silence_duration_ms=800,
            temperature=0.7,
        )
    )

    assert payload["type"] == "session.update"
    assert payload["session"]["voice"] == "Tina"
    assert payload["session"]["input_audio_transcription"] == {"language": "zh"}
    assert payload["session"]["turn_detection"] == {
        "type": "server_vad",
        "threshold": 0.5,
        "silence_duration_ms": 800,
        "create_response": False,
        "interrupt_response": False,
    }
    assert "tools" not in payload["session"]

    tool_payload = build_session_update_event(
        QwenRealtimeSessionConfig(
            voice="Tina",
            instructions="你是一个电话外呼助手，回答要简短自然。",
            vad_type="server_vad",
            vad_threshold=0.5,
            vad_silence_duration_ms=800,
            tools=DEFAULT_REALTIME_TOOLS,
        )
    )
    assert tool_payload["session"]["tools"] == DEFAULT_REALTIME_TOOLS
    assert SCHEDULE_CALL_END_TOOL in DEFAULT_REALTIME_TOOLS
    assert REQUEST_HANDOFF_TOOL in DEFAULT_REALTIME_TOOLS

    assert map_qwen_server_event({"type": "response.audio.delta"}) == "model_audio_delta"
    assert map_qwen_server_event({"type": "response.function_call_arguments.done"}) == (
        "tool_call_done"
    )
    assert map_qwen_server_event({"type": "input_audio_buffer.speech_stopped"}) == (
        "user_speech_stopped"
    )
    assert map_qwen_server_event({"type": "conversation.item.created"}) == (
        "conversation_item_created"
    )
    assert map_qwen_server_event({"type": "input_audio_buffer.committed"}) == (
        "input_audio_committed"
    )
    assert (
        map_qwen_server_event({"type": "conversation.item.input_audio_transcription.failed"})
        == "user_transcript_failed"
    )
    assert map_qwen_server_event({"type": "unknown.event"}) is None


class FakeQwenWebSocket:
    def __init__(self, incoming: list[dict] | None = None) -> None:
        self.sent_json: list[dict] = []
        self.incoming = incoming or []
        self.closed = False

    async def send_json(self, payload: dict) -> None:
        self.sent_json.append(payload)

    async def receive_json(self) -> dict:
        if not self.incoming:
            raise StopAsyncIteration
        return self.incoming.pop(0)

    async def close(self) -> None:
        self.closed = True


@pytest.mark.anyio
async def test_qwen_provider_connects_and_sends_protocol_events() -> None:
    sockets: list[FakeQwenWebSocket] = []
    captured: dict[str, object] = {}

    async def websocket_factory(url: str, headers: dict[str, str]) -> FakeQwenWebSocket:
        captured["url"] = url
        captured["headers"] = headers
        socket = FakeQwenWebSocket()
        sockets.append(socket)
        return socket

    provider = AliyunQwenRealtimeProvider(
        realtime_url="wss://dashscope.test/api-ws/v1/realtime",
        api_key="dashscope-secret",
        model="qwen3.5-omni-plus-realtime",
        websocket_factory=websocket_factory,
    )

    await provider.connect()
    await provider.update_session(
        QwenRealtimeSessionConfig(
            voice="Tina",
            instructions="你是一个电话外呼助手，回答要简短自然。",
            vad_type="server_vad",
            vad_threshold=0.5,
            vad_silence_duration_ms=800,
        )
    )
    await provider.send_audio(b"\x01\x02\x03")
    await provider.create_response()
    await provider.create_response("请说一句开场白")
    await provider.cancel_response()
    await provider.clear_input_audio()
    await provider.submit_tool_result("call_tool_1", "ok")
    await provider.create_response()
    await provider.close()

    assert captured["url"] == (
        "wss://dashscope.test/api-ws/v1/realtime?model=qwen3.5-omni-plus-realtime"
    )
    assert captured["headers"] == {"Authorization": "Bearer dashscope-secret"}
    assert sockets[0].sent_json == [
        build_session_update_event(
            QwenRealtimeSessionConfig(
                voice="Tina",
                instructions="你是一个电话外呼助手，回答要简短自然。",
                vad_type="server_vad",
                vad_threshold=0.5,
                vad_silence_duration_ms=800,
            )
        ),
        {
            "type": "input_audio_buffer.append",
            "audio": "AQID",
        },
        {"type": "response.create"},
        {
            "type": "conversation.item.create",
            "item": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "请说一句开场白"}],
            },
        },
        {"type": "response.create"},
        {"type": "response.cancel"},
        {"type": "input_audio_buffer.clear"},
        {
            "type": "conversation.item.create",
            "item": {
                "type": "function_call_output",
                "call_id": "call_tool_1",
                "output": "ok",
            },
        },
        {"type": "response.create"},
    ]
    assert sockets[0].closed is True


@pytest.mark.anyio
async def test_qwen_provider_submit_tool_result_does_not_create_response_immediately() -> None:
    socket = FakeQwenWebSocket()

    async def websocket_factory(url: str, headers: dict[str, str]) -> FakeQwenWebSocket:
        return socket

    provider = AliyunQwenRealtimeProvider(
        realtime_url="wss://dashscope.test/api-ws/v1/realtime",
        api_key="dashscope-secret",
        model="qwen3.5-omni-plus-realtime",
        websocket_factory=websocket_factory,
    )

    await provider.connect()
    await provider.submit_tool_result("call_tool_1", "ok")

    assert socket.sent_json == [
        {
            "type": "conversation.item.create",
            "item": {
                "type": "function_call_output",
                "call_id": "call_tool_1",
                "output": "ok",
            },
        }
    ]


@pytest.mark.anyio
async def test_default_qwen_websocket_factory_disables_implicit_proxy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    fake_websocket = object()

    async def connect(
        url: str,
        *,
        additional_headers: dict[str, str],
        proxy: object,
        ping_interval: float | None = None,
        ping_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> object:
        captured["url"] = url
        captured["additional_headers"] = additional_headers
        captured["proxy"] = proxy
        captured["ping_interval"] = ping_interval
        captured["ping_timeout"] = ping_timeout
        captured["close_timeout"] = close_timeout
        return fake_websocket

    fake_websockets = type(
        "FakeWebsockets",
        (),
        {"connect": staticmethod(connect)},
    )
    monkeypatch.setitem(sys.modules, "websockets", fake_websockets)

    connection = await _default_websocket_factory(
        "wss://dashscope.test/api-ws/v1/realtime?model=qwen",
        {"Authorization": "Bearer dashscope-secret"},
    )

    assert connection.websocket is fake_websocket
    assert captured == {
        "url": "wss://dashscope.test/api-ws/v1/realtime?model=qwen",
        "additional_headers": {"Authorization": "Bearer dashscope-secret"},
        "proxy": None,
        "ping_interval": 20.0,
        "ping_timeout": 120.0,
        "close_timeout": 10.0,
    }


@pytest.mark.anyio
async def test_qwen_provider_receives_mapped_events_and_preserves_payload() -> None:
    socket = FakeQwenWebSocket(
        incoming=[
            {"type": "session.created", "session": {"id": "sess_1"}},
            {"type": "response.audio.delta", "delta": "AAAA"},
            {"type": "conversation.item.created", "item": {"id": "item_1"}},
            {"type": "input_audio_buffer.committed", "item_id": "item_1"},
            {
                "type": "response.function_call_arguments.done",
                "call_id": "call_tool_1",
                "name": "schedule_call_end",
                "arguments": '{"reason":"customer_end"}',
            },
            {
                "type": "conversation.item.input_audio_transcription.failed",
                "error": {"code": "asr_failed", "message": "no speech"},
            },
            {
                "type": "unknown.event",
                "authorization": "Bearer secret",
                "audio": "AAAA",
                "delta": "x" * 600,
                "value": 1,
            },
        ]
    )

    async def websocket_factory(url: str, headers: dict[str, str]) -> FakeQwenWebSocket:
        return socket

    provider = AliyunQwenRealtimeProvider(
        realtime_url="wss://dashscope.test/api-ws/v1/realtime",
        api_key="dashscope-secret",
        model="qwen3.5-omni-plus-realtime",
        websocket_factory=websocket_factory,
    )

    await provider.connect()
    events = []
    async for event in provider.receive_events():
        events.append(event)

    assert [event.type for event in events] == [
        "model_session_started",
        "model_audio_delta",
        "conversation_item_created",
        "input_audio_committed",
        "tool_call_done",
        "user_transcript_failed",
        "provider_event_unmapped",
    ]
    assert events[0].payload == {"type": "session.created", "session": {"id": "sess_1"}}
    assert events[1].payload == {"type": "response.audio.delta", "delta": "AAAA"}
    assert events[4].payload == {
        "type": "response.function_call_arguments.done",
        "call_id": "call_tool_1",
        "name": "schedule_call_end",
        "arguments": '{"reason":"customer_end"}',
    }
    assert events[6].payload == {
        "rawType": "unknown.event",
        "raw": {
            "type": "unknown.event",
            "authorization": "<redacted>",
            "audio": "<redacted_audio>",
            "delta": "<redacted_large_delta>",
            "value": 1,
        },
    }


@pytest.mark.anyio
async def test_realtime_agent_runner_records_provider_events_and_updates_session_state() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    metrics = CallMetrics()
    provider = FakeRealtimeProvider([
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}),
        ProviderEvent(type="user_speech_started", payload={}),
        ProviderEvent(type="user_transcript_delta", payload={"delta": "你好"}),
        ProviderEvent(type="user_speech_stopped", payload={}),
        ProviderEvent(type="model_audio_delta", payload={"delta": "AAAA"}),
        ProviderEvent(type="model_response_done", payload={}),
    ])
    session = CallSession(
        call_id="call_1",
        room_name="ai-call-call_1",
        participant_identity="browser-call_1",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "你是一个电话外呼助手，回答要简短自然。",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        metrics_by_call_id={"call_1": metrics},
        user_turn_stability_delay_seconds=0,
    )

    await runner.start(session)
    await runner.wait("call_1")

    assert provider.connected is True
    assert len(provider.session_updates) == 1
    session_update = provider.session_updates[0]
    assert session_update.voice == "Tina"
    assert "你是一个电话外呼助手，回答要简短自然。" in session_update.instructions
    assert "电话单轮回复约束" in session_update.instructions
    assert "10-15 秒" in session_update.instructions
    assert "不超过 2 句话" in session_update.instructions
    assert "schedule_call_end" in session_update.instructions
    assert session_update.vad_type == "server_vad"
    assert session_update.vad_threshold == 0.5
    assert session_update.vad_silence_duration_ms == 800
    assert session_update.tools == DEFAULT_REALTIME_TOOLS
    assert registry.get("call_1").status == CallSessionStatus.CONNECTED
    assert metrics.snapshot()["lastModelFirstAudioMs"] is not None
    assert [event.type for event in store.list("call_1")] == [
        "model_session_started",
        "user_speech_started",
        "user_transcript_delta",
        "user_speech_stopped",
        "model_audio_delta",
        "model_response_done",
    ]


@pytest.mark.anyio
async def test_realtime_agent_runner_schedules_end_on_model_error() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    scheduler_calls: list[tuple[str, str]] = []
    session = CallSession(
        call_id="call_model_error",
        room_name="ai-call-call_model_error",
        participant_identity="browser-call_model_error",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        call_end_scheduler=lambda call_id, reason: scheduler_calls.append((call_id, reason)),
    )

    await runner.start(session)
    await provider.emit(ProviderEvent(type="model_session_started", payload={}))
    await provider.emit(
        ProviderEvent(
            type="model_error",
            payload={"error": {"message": "insufficient balance"}},
        )
    )
    await _wait_until(lambda: bool(scheduler_calls))

    assert registry.get("call_model_error").status == CallSessionStatus.FAILED
    assert scheduler_calls == [("call_model_error", "model_error")]
    session_failed = [
        event for event in store.list("call_model_error") if event.type == "session_failed"
    ]
    assert session_failed[-1].payload == {
        "endReason": "model_error",
        "failureStage": "model",
        "failureMessage": "insufficient balance",
    }

    await runner.stop("call_model_error")


@pytest.mark.anyio
async def test_realtime_agent_runner_ignores_none_active_response_after_cancel_race() -> None:
    runner, registry, store, provider, _publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True]),
        call_id="call_model_cancel_race",
        clean_window_ms=40,
        max_hold_ms=100,
    )

    try:
        await runner._confirm_interrupt(
            call_id,
            provider,
            datetime.now(timezone.utc),
            reason="user_speech_started_during_ai_audio",
        )
        assert provider.cancelled_response_count == 1

        await runner._apply_provider_event(
            call_id,
            provider,
            "model_response_done",
            datetime.now(timezone.utc),
            {"response": {"id": "resp_sip_opening", "status": "completed"}},
        )
        await runner._apply_provider_event(
            call_id,
            provider,
            "model_error",
            datetime.now(timezone.utc),
            {
                "error": {
                    "message": "Conversation has none active response",
                    "type": "invalid_request_error",
                }
            },
        )

        event_types = [event.type for event in store.list(call_id)]
        assert registry.get(call_id).status != CallSessionStatus.FAILED
        assert "model_cancel_race_ignored" in event_types
        assert "session_failed" not in event_types
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_schedules_end_when_model_response_create_fails() -> None:
    class FailingCreateResponseProvider(QueueRealtimeProvider):
        async def create_response(self, input_text: str | None = None) -> None:
            _ = input_text
            raise RuntimeError("quota exceeded")

    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = FailingCreateResponseProvider()
    scheduler_calls: list[tuple[str, str]] = []
    session = CallSession(
        call_id="call_response_create_failed",
        room_name="ai-call-call_response_create_failed",
        participant_identity="browser-call_response_create_failed",
        status=CallSessionStatus.CONNECTED,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "opening_message": "您好",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        call_end_scheduler=lambda call_id, reason: scheduler_calls.append((call_id, reason)),
    )

    await runner.start(session)
    await runner.start_opening("call_response_create_failed")

    assert registry.get("call_response_create_failed").status == CallSessionStatus.FAILED
    assert scheduler_calls == [("call_response_create_failed", "model_error")]
    session_failed = [
        event
        for event in store.list("call_response_create_failed")
        if event.type == "session_failed"
    ]
    assert session_failed[-1].payload["failureStage"] == "model_response_create"
    assert "quota exceeded" in session_failed[-1].payload["failureMessage"]

    await runner.stop("call_response_create_failed")


@pytest.mark.anyio
async def test_realtime_agent_runner_schedules_end_when_provider_event_stream_fails() -> None:
    class FailingReceiveProvider(FakeRealtimeProvider):
        async def receive_events(self):
            yield ProviderEvent(type="model_session_started", payload={})
            raise RuntimeError("websocket closed")

    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = FailingReceiveProvider([])
    scheduler_calls: list[tuple[str, str]] = []
    session = CallSession(
        call_id="call_provider_stream_failed",
        room_name="ai-call-call_provider_stream_failed",
        participant_identity="browser-call_provider_stream_failed",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        call_end_scheduler=lambda call_id, reason: scheduler_calls.append((call_id, reason)),
    )

    await runner.start(session)
    await _wait_until(lambda: bool(scheduler_calls))
    await runner.wait("call_provider_stream_failed")

    assert registry.get("call_provider_stream_failed").status == CallSessionStatus.FAILED
    assert scheduler_calls == [("call_provider_stream_failed", "provider_transport_error")]
    session_failed = [
        event
        for event in store.list("call_provider_stream_failed")
        if event.type == "session_failed"
    ]
    assert session_failed[-1].payload["endReason"] == "provider_transport_error"
    assert session_failed[-1].payload["failureStage"] == "provider_event_stream"
    assert "websocket closed" in session_failed[-1].payload["failureMessage"]
    provider_transport = session_failed[-1].payload["providerTransport"]
    assert provider_transport["errorSource"] == "provider_event_stream"
    assert provider_transport["lastProviderEventType"] == "model_session_started"
    assert isinstance(provider_transport["lastProviderEventAt"], str)
    assert provider_transport["lastProviderEventStreamErrorType"] == "RuntimeError"
    assert "websocket closed" in provider_transport["lastProviderEventStreamErrorMessage"]

    await runner.stop("call_provider_stream_failed")


@pytest.mark.anyio
async def test_realtime_agent_runner_classifies_provider_audio_send_failure() -> None:
    class FailingSendProvider(FakeRealtimeProvider):
        async def send_audio(self, pcm_frame: bytes) -> None:
            raise RuntimeError("sent 1011 keepalive ping timeout")

    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = FailingSendProvider([])
    scheduler_calls: list[tuple[str, str]] = []
    input_samples = list(range(960))
    input_pcm = struct.pack("<" + "h" * len(input_samples), *input_samples)
    transport = FakeRoomAudioTransport([
        PcmAudioFrame(
            data=input_pcm,
            sample_rate_hz=48000,
            channels=1,
            sample_width_bytes=2,
        )
    ])
    session = CallSession(
        call_id="call_provider_audio_send_failed",
        room_name="ai-call-call_provider_audio_send_failed",
        participant_identity="browser-call_provider_audio_send_failed",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_transport=transport,
        call_end_scheduler=lambda call_id, reason: scheduler_calls.append((call_id, reason)),
    )

    await runner.start(session)
    await _wait_until(lambda: bool(scheduler_calls))
    await runner.wait("call_provider_audio_send_failed")

    assert registry.get("call_provider_audio_send_failed").status == CallSessionStatus.FAILED
    assert scheduler_calls == [
        ("call_provider_audio_send_failed", "provider_transport_error")
    ]
    session_failed = [
        event
        for event in store.list("call_provider_audio_send_failed")
        if event.type == "session_failed"
    ]
    assert session_failed[-1].payload["endReason"] == "provider_transport_error"
    assert session_failed[-1].payload["failureStage"] == "provider_audio_send"
    assert "keepalive ping timeout" in session_failed[-1].payload["failureMessage"]
    provider_transport = session_failed[-1].payload["providerTransport"]
    assert provider_transport["errorSource"] == "provider_audio_send"
    assert isinstance(provider_transport["lastProviderAudioSendAttemptAt"], str)
    assert isinstance(provider_transport["lastProviderAudioSendErrorAt"], str)
    assert provider_transport["lastProviderAudioSendErrorType"] == "RuntimeError"
    assert "keepalive ping timeout" in provider_transport["lastProviderAudioSendErrorMessage"]

    await runner.stop("call_provider_audio_send_failed")


@pytest.mark.anyio
async def test_realtime_agent_runner_schedules_customer_end_after_final_audio_playout() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    scheduler_calls: list[tuple[str, str]] = []
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_end_tool",
        room_name="ai-call-call_end_tool",
        participant_identity="browser-call_end_tool",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        ai_speaking_tail_grace_seconds=0,
        call_end_scheduler=lambda call_id, reason: scheduler_calls.append((call_id, reason)),
    )
    _seed_customer_end_intent(runner, "call_end_tool")

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(
        ProviderEvent(
            type="tool_call_done",
            payload={
                "call_id": "tool_1",
                "name": "schedule_call_end",
                "arguments": json.dumps({"reason": "customer_end"}),
            },
        )
    )
    await asyncio.wait_for(
        _wait_until(lambda: provider.submitted_tool_results),
        timeout=1,
    )
    assert scheduler_calls == []

    await provider.emit(ProviderEvent(type="model_response_done", payload={}))
    await asyncio.sleep(0)
    assert scheduler_calls == []

    await provider.emit(ProviderEvent(type="model_response_started", payload={}))
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={"delta": base64.b64encode(output_pcm).decode("ascii")},
        )
    )
    await provider.emit(ProviderEvent(type="model_response_done", payload={}))
    await asyncio.wait_for(
        _wait_until(lambda: scheduler_calls == [("call_end_tool", "customer_end")]),
        timeout=1,
    )
    await provider.close_events()
    await runner.wait("call_end_tool")

    assert provider.submitted_tool_results[0][0] == "tool_1"
    assert provider.submitted_tool_results[0][1] == (
        "请直接回复：“好的，那我先不打扰您了，祝您工作顺利。”"
        "不要添加其他内容，不要再提出问题。"
    )
    assert len(publisher.published) == 1
    assert [event.type for event in store.list("call_end_tool")] == [
        "model_session_started",
        "tool_call_done",
        "call_end_tool_requested",
        "model_response_done",
        "model_response_started",
        "model_audio_delta",
        "ai_audio_published",
        "model_response_done",
        "call_end_scheduled",
    ]


@pytest.mark.anyio
async def test_realtime_agent_runner_ignores_customer_end_tool_after_short_ack() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    scheduler_calls: list[tuple[str, str]] = []
    call_id = "call_end_short_ack_guard"
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"browser-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        user_turn_stability_delay_seconds=0,
        call_end_scheduler=lambda call_id, reason: scheduler_calls.append((call_id, reason)),
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.emit(
        ProviderEvent(type="user_transcript_done", payload={"transcript": "行。"})
    )
    await provider.emit(ProviderEvent(type="user_speech_stopped", payload={}))
    await asyncio.wait_for(_wait_until(lambda: provider.created_responses == [None]), timeout=1)
    await provider.emit(
        ProviderEvent(
            type="tool_call_done",
            payload={
                "call_id": "tool_short_ack",
                "name": "schedule_call_end",
                "arguments": json.dumps({"reason": "customer_end"}),
            },
        )
    )
    await asyncio.wait_for(_wait_until(lambda: provider.submitted_tool_results), timeout=1)
    await provider.close_events()
    await runner.wait(call_id)

    events = store.list(call_id)
    event_types = [event.type for event in events]
    ignored_event = next(event for event in events if event.type == "call_end_tool_ignored")
    assert scheduler_calls == []
    assert provider.submitted_tool_results == [
        (
            "tool_short_ack",
            "未确认用户要求结束通话。请继续按用户刚才的话推进对话，不要结束通话。",
        )
    ]
    assert ignored_event.payload["reason"] == "customer_end_without_explicit_customer_intent"
    assert ignored_event.payload["localDecisionReason"] == "not_explicit_call_end"
    assert ignored_event.payload["transcriptPreview"] == "行。"
    assert "call_end_tool_requested" not in event_types
    assert "call_end_scheduled" not in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_schedules_call_end_after_final_audio_hold_expires() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = WaitingAudioPublisher()
    scheduler_calls: list[tuple[str, str]] = []
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_end_audio_hold_expired",
        room_name="ai-call-call_end_audio_hold_expired",
        participant_identity="browser-call_end_audio_hold_expired",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        ai_speaking_tail_grace_seconds=0,
        browser_audio_hold_timeout_seconds=0.03,
        call_end_scheduler=lambda call_id, reason: scheduler_calls.append((call_id, reason)),
    )
    _seed_customer_end_intent(runner, "call_end_audio_hold_expired")

    try:
        await runner.start(session)
        await provider.emit(
            ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
        )
        await provider.emit(
            ProviderEvent(
                type="tool_call_done",
                payload={
                    "call_id": "tool_end_after_hold",
                    "name": "schedule_call_end",
                    "arguments": json.dumps({"reason": "customer_end"}),
                },
            )
        )
        await asyncio.wait_for(
            _wait_until(lambda: provider.submitted_tool_results),
            timeout=1,
        )
        await provider.emit(ProviderEvent(type="model_response_done", payload={}))
        await provider.emit(
            ProviderEvent(
                type="model_response_started",
                payload={"response": {"id": "resp_final_goodbye"}},
            )
        )
        await provider.emit(
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_final_goodbye",
                    "delta": base64.b64encode(output_pcm).decode("ascii"),
                },
            )
        )
        await provider.emit(
            ProviderEvent(
                type="model_response_done",
                payload={"response": {"id": "resp_final_goodbye", "status": "completed"}},
            )
        )
        await asyncio.wait_for(publisher.playout_wait_started.wait(), timeout=1)

        accepted = await runner.record_browser_speech_segment(
            "call_end_audio_hold_expired",
            datetime.now(timezone.utc),
            {
                "segmentId": "browser-seg-end-audio-hold",
                "phase": "updated",
                "durationMs": 280,
                "rmsDbfs": -19.9,
                "noiseFloorDbfs": -45.0,
                "snrDb": 25.1,
                "hotFrameCount": 8,
                "remoteAudioActive": True,
                "remoteAudioRmsDbfs": -64.5,
            },
        )
        await asyncio.sleep(0.05)
        await asyncio.wait_for(
            _wait_until(
                lambda: scheduler_calls
                == [("call_end_audio_hold_expired", "customer_end")],
                attempts=80,
            ),
            timeout=1,
        )
        await provider.close_events()
        await runner.wait("call_end_audio_hold_expired")

        event_types = [event.type for event in store.list("call_end_audio_hold_expired")]
        assert accepted is True
        assert publisher.stopped_call_ids == ["call_end_audio_hold_expired"]
        assert "browser_audio_hold_requested" in event_types
        assert "browser_audio_hold_expired" in event_types
        assert "interrupt_ignored" in event_types
        assert "call_end_scheduled" in event_types
        assert registry.get("call_end_audio_hold_expired").status == CallSessionStatus.CONNECTED
    finally:
        await runner.stop("call_end_audio_hold_expired")


@pytest.mark.anyio
async def test_realtime_agent_runner_rejects_customer_end_tool_for_non_terminal_user_text() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    scheduler_calls: list[tuple[str, str]] = []
    call_id = "call_end_reject_price_question"
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"browser-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        user_turn_stability_delay_seconds=0,
        call_end_scheduler=lambda call_id, reason: scheduler_calls.append((call_id, reason)),
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.emit(
        ProviderEvent(
            type="user_transcript_done",
            payload={"transcript": "好像有价格。"},
        )
    )
    await provider.emit(ProviderEvent(type="user_speech_stopped", payload={}))
    await provider.emit(
        ProviderEvent(
            type="tool_call_done",
            payload={
                "call_id": "tool_wrong_customer_end",
                "name": "schedule_call_end",
                "arguments": json.dumps({"reason": "customer_end"}),
            },
        )
    )
    await provider.close_events()
    await runner.wait(call_id)

    event_types = [event.type for event in store.list(call_id)]
    ignored_event = next(event for event in store.list(call_id) if event.type == "call_end_tool_ignored")
    assert scheduler_calls == []
    assert "call_end_tool_requested" not in event_types
    assert "call_end_scheduled" not in event_types
    assert ignored_event.payload["reason"] == "customer_end_without_terminal_user_signal"
    assert provider.submitted_tool_results == [
        (
            "tool_wrong_customer_end",
            "未确认客户要结束通话。请继续回应客户刚才的问题或做一个必要澄清。",
        )
    ]


@pytest.mark.anyio
async def test_realtime_agent_runner_recovers_with_audio_after_rejected_customer_end_tool() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    call_id = "call_end_rejected_tool_recovery"
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"browser-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        user_turn_stability_delay_seconds=0,
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_tool_only"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="tool_call_done",
            payload={
                "call_id": "tool_wrong_customer_end",
                "name": "schedule_call_end",
                "arguments": json.dumps({"reason": "customer_end"}),
            },
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_tool_only"}},
        )
    )
    await provider.close_events()
    await runner.wait(call_id)

    event_types = [event.type for event in store.list(call_id)]
    assert "call_end_tool_ignored" in event_types
    assert "call_end_tool_requested" not in event_types
    assert provider.submitted_tool_results == [
        (
            "tool_wrong_customer_end",
            "未确认客户要结束通话。请继续回应客户刚才的问题或做一个必要澄清。",
        )
    ]
    assert provider.created_responses == [None]


@pytest.mark.anyio
async def test_realtime_agent_runner_cancels_no_barge_call_end_when_user_continues() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = ImmediatePlayoutAudioPublisher()
    scheduler_calls: list[tuple[str, str]] = []
    output_pcm = b"\x01\x02" * 240
    call_id = "call_end_no_barge_user_continues"
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"browser-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
            "barge_in_enabled": False,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        ai_speaking_tail_grace_seconds=0,
        user_turn_stability_delay_seconds=0,
        call_end_scheduler=lambda call_id, reason: scheduler_calls.append((call_id, reason)),
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.emit(
        ProviderEvent(
            type="user_transcript_done",
            payload={"transcript": "先这样吧。"},
        )
    )
    await provider.emit(ProviderEvent(type="user_speech_stopped", payload={}))
    await provider.emit(
        ProviderEvent(
            type="tool_call_done",
            payload={
                "call_id": "tool_customer_end_then_resume",
                "name": "schedule_call_end",
                "arguments": json.dumps({"reason": "customer_end"}),
            },
        )
    )
    await asyncio.wait_for(_wait_until(lambda: provider.created_responses == [None]), timeout=1)
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_final_goodbye"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_final_goodbye",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
    )
    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.emit(
        ProviderEvent(
            type="user_transcript_done",
            payload={"transcript": "都会参与。"},
        )
    )
    await provider.emit(ProviderEvent(type="user_speech_stopped", payload={}))
    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_final_goodbye", "status": "completed"}},
        )
    )
    await asyncio.wait_for(_wait_until(lambda: provider.created_responses == [None, None]), timeout=1)
    await provider.close_events()
    await runner.wait(call_id)

    event_types = [event.type for event in store.list(call_id)]
    assert scheduler_calls == []
    assert "call_end_tool_requested" in event_types
    assert "call_end_interrupted" in event_types
    assert "call_end_tail_ignored" not in event_types
    assert "call_end_scheduled" not in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_queues_call_end_tool_response_until_active_response_done() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    session = CallSession(
        call_id="call_end_tool_queue",
        room_name="ai-call-call_end_tool_queue",
        participant_identity="browser-call_end_tool_queue",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
    )
    _seed_customer_end_intent(runner, "call_end_tool_queue")

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_tool_call"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="tool_call_done",
            payload={
                "call_id": "tool_queued",
                "name": "schedule_call_end",
                "arguments": json.dumps({"reason": "customer_end"}),
            },
        )
    )
    await asyncio.wait_for(
        _wait_until(lambda: provider.submitted_tool_results),
        timeout=1,
    )

    assert provider.created_responses == []

    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_tool_call", "status": "completed"}},
        )
    )
    await asyncio.wait_for(_wait_until(lambda: provider.created_responses == [None]), timeout=1)
    await provider.close_events()
    await runner.wait("call_end_tool_queue")

    event_types = [event.type for event in store.list("call_end_tool_queue")]
    assert event_types == [
        "model_session_started",
        "model_response_started",
        "tool_call_done",
        "call_end_tool_requested",
        "model_response_done",
    ]


@pytest.mark.anyio
async def test_realtime_agent_runner_does_not_create_extra_call_end_response_after_audio() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = ImmediatePlayoutAudioPublisher()
    scheduler_calls: list[tuple[str, str]] = []
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_end_audio_already_spoken",
        room_name="ai-call-call_end_audio_already_spoken",
        participant_identity="browser-call_end_audio_already_spoken",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        ai_speaking_tail_grace_seconds=0,
        call_end_scheduler=lambda call_id, reason: scheduler_calls.append((call_id, reason)),
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    _seed_task_completed_signal(runner, "call_end_audio_already_spoken")
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_final_goodbye"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_final_goodbye",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
    )
    await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)
    await provider.emit(
        ProviderEvent(
            type="tool_call_done",
            payload={
                "call_id": "tool_after_audio",
                "name": "schedule_call_end",
                "arguments": json.dumps({"reason": "task_completed"}),
            },
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_final_goodbye", "status": "completed"}},
        )
    )
    await asyncio.wait_for(
        _wait_until(
            lambda: scheduler_calls == [("call_end_audio_already_spoken", "normal_completed")]
        ),
        timeout=1,
    )
    await provider.close_events()
    await runner.wait("call_end_audio_already_spoken")

    assert provider.created_responses == []
    assert provider.submitted_tool_results == [
        ("tool_after_audio", "已记录。系统将结束通话，不要再生成额外回复。")
    ]
    assert [event.type for event in store.list("call_end_audio_already_spoken")] == [
        "model_session_started",
        "model_response_started",
        "model_audio_delta",
        "ai_audio_published",
        "tool_call_done",
        "call_end_tool_requested",
        "model_response_done",
        "call_end_scheduled",
    ]


@pytest.mark.anyio
async def test_realtime_agent_runner_cancels_pending_call_end_when_user_speaks_again() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    scheduler_calls: list[tuple[str, str]] = []
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_end_user_resumed",
        room_name="ai-call-call_end_user_resumed",
        participant_identity="browser-call_end_user_resumed",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        user_turn_stability_delay_seconds=0,
        call_end_scheduler=lambda call_id, reason: scheduler_calls.append((call_id, reason)),
    )
    _seed_customer_end_intent(runner, "call_end_user_resumed")

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(
        ProviderEvent(
            type="tool_call_done",
            payload={
                "call_id": "tool_resume",
                "name": "schedule_call_end",
                "arguments": json.dumps({"reason": "customer_end"}),
            },
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_final_goodbye"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_final_goodbye",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
    )
    await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)

    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.emit(
        ProviderEvent(
            type="user_transcript_delta",
            payload={"delta": "你帮我查一下今天几号。"},
        )
    )
    await provider.emit(ProviderEvent(type="user_speech_stopped", payload={}))
    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_final_goodbye", "status": "cancelled"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_date_answer"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_date_answer", "status": "completed"}},
        )
    )
    await provider.close_events()
    await runner.wait("call_end_user_resumed")

    event_types = [event.type for event in store.list("call_end_user_resumed")]
    assert scheduler_calls == []
    assert provider.created_responses == [None]
    assert "call_end_tool_requested" in event_types
    assert "call_end_interrupted" in event_types
    assert "call_end_scheduled" not in event_types
    assert "interrupt_confirmed" in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_schedules_explicit_customer_end_without_extra_response() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    scheduler_calls: list[tuple[str, str]] = []
    call_id = "call_end_explicit_without_extra_response"
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"browser-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        user_turn_stability_delay_seconds=0,
        call_end_scheduler=lambda call_id, reason: scheduler_calls.append((call_id, reason)),
    )

    try:
        await runner.start(session)
        await provider.emit(
            ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
        )
        await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
        await provider.emit(
            ProviderEvent(
                type="user_transcript_done",
                payload={"transcript": "好，挂了吧。"},
            )
        )
        await provider.emit(ProviderEvent(type="user_speech_stopped", payload={}))
        await asyncio.sleep(0)

        event_types = [event.type for event in store.list(call_id)]
        assert scheduler_calls == [(call_id, "customer_end")]
        assert provider.created_responses == []
        assert "call_end_intent_detected" in event_types
        assert "call_end_tool_missing" in event_types
        assert "call_end_scheduled" in event_types
        assert "model_response_started" not in event_types
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_fast_ends_explicit_customer_end_during_ai_audio() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    scheduler_calls: list[tuple[str, str]] = []
    call_id = "call_end_explicit_during_ai_audio"
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"browser-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        user_turn_stability_delay_seconds=0,
        call_end_scheduler=lambda call_id, reason: scheduler_calls.append((call_id, reason)),
    )

    try:
        await runner.start(session)
        await provider.emit(
            ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
        )
        await provider.emit(
            ProviderEvent(
                type="model_response_started",
                payload={"response": {"id": "resp_long_goodbye"}},
            )
        )
        await provider.emit(
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_long_goodbye",
                    "delta": base64.b64encode(output_pcm).decode("ascii"),
                },
            )
        )
        await provider.emit(
            ProviderEvent(type="user_transcript_done", payload={"transcript": "挂了吧。"})
        )
        await provider.emit(ProviderEvent(type="user_speech_stopped", payload={}))
        await asyncio.sleep(0)

        event_types = [event.type for event in store.list(call_id)]
        assert scheduler_calls == [(call_id, "customer_end")]
        assert provider.created_responses == []
        assert provider.cancelled_response_count == 1
        assert publisher.stopped_call_ids == [call_id]
        assert "call_end_intent_detected" in event_types
        assert "call_end_tool_missing" in event_types
        assert "response_generation_invalidated" in event_types
        assert "interrupt_audio_stop_requested" in event_types
        assert "call_end_scheduled" in event_types
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("transcript", "call_id"),
    [
        ("挂了吧。", "call_end_tool_missing"),
        ("挂断吧。", "call_end_tool_missing_hangup"),
    ],
)
async def test_realtime_agent_runner_schedules_customer_end_when_model_misses_call_end_tool(
    transcript: str,
    call_id: str,
) -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = ImmediatePlayoutAudioPublisher()
    scheduler_calls: list[tuple[str, str]] = []
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"browser-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        ai_speaking_tail_grace_seconds=0,
        user_turn_stability_delay_seconds=0,
        call_end_scheduler=lambda call_id, reason: scheduler_calls.append((call_id, reason)),
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.emit(
        ProviderEvent(
            type="user_transcript_done",
            payload={"transcript": transcript},
        )
    )
    await provider.emit(ProviderEvent(type="user_speech_stopped", payload={}))
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": f"resp_{call_id}"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": f"resp_{call_id}",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": f"resp_{call_id}", "status": "completed"}},
        )
    )
    await asyncio.wait_for(
        _wait_until(lambda: scheduler_calls == [(call_id, "customer_end")]),
        timeout=1,
    )
    await provider.close_events()
    await runner.wait(call_id)

    event_types = [event.type for event in store.list(call_id)]
    assert "call_end_intent_detected" in event_types
    assert "call_end_tool_missing" in event_types
    assert "call_end_scheduled" in event_types


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("transcript", "call_id"),
    [
        ("挂了吗？", "call_end_question"),
        ("先别挂。", "call_end_negated"),
    ],
)
async def test_realtime_agent_runner_does_not_schedule_call_end_for_non_end_control_text(
    transcript: str,
    call_id: str,
) -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = ImmediatePlayoutAudioPublisher()
    scheduler_calls: list[tuple[str, str]] = []
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"browser-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        ai_speaking_tail_grace_seconds=0,
        user_turn_stability_delay_seconds=0,
        call_end_scheduler=lambda call_id, reason: scheduler_calls.append((call_id, reason)),
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.emit(
        ProviderEvent(
            type="user_transcript_done",
            payload={"transcript": transcript},
        )
    )
    await provider.emit(ProviderEvent(type="user_speech_stopped", payload={}))
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": f"resp_{call_id}"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": f"resp_{call_id}",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": f"resp_{call_id}", "status": "completed"}},
        )
    )
    await asyncio.sleep(0)
    await provider.close_events()
    await runner.wait(call_id)

    event_types = [event.type for event in store.list(call_id)]
    assert scheduler_calls == []
    assert "call_end_intent_detected" not in event_types
    assert "call_end_tool_missing" not in event_types
    assert "call_end_scheduled" not in event_types


@pytest.mark.anyio
@pytest.mark.parametrize(
    "transcript",
    [
        "我。",
        "对。",
        "最后。",
        "法人。",
        "五七一八五。",
        "15000。",
    ],
)
async def test_realtime_agent_runner_rejects_low_trust_short_overlap_transcript(
    transcript: str,
) -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = ImmediatePlayoutAudioPublisher()
    call_id = "call_low_trust_transcript"
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"browser-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        ai_speaking_tail_grace_seconds=0,
        user_turn_stability_delay_seconds=0,
    )

    await runner.start(session)
    await provider.emit(ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}))
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_low_trust"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_low_trust",
                "delta": base64.b64encode(b"\x01\x02" * 240).decode("ascii"),
            },
        )
    )
    await provider.emit(
        ProviderEvent(
            type="user_speech_started",
            payload={"audio_start_ms": 1400},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="user_transcript_done",
            payload={"transcript": transcript, "audio_start_ms": 1400, "audio_end_ms": 2040},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="user_speech_stopped",
            payload={"audio_end_ms": 2040},
        )
    )
    await provider.close_events()
    await runner.wait(call_id)

    event_types = [event.type for event in store.list(call_id)]
    transcript_event = next(event for event in store.list(call_id) if event.type == "user_transcript_done")
    assert "user_transcript_semantic_rejected" in event_types
    assert transcript_event.payload["commitDecision"] == "candidate"
    assert "interrupt_audio_stop_requested" in event_types
    assert "interrupt_confirmed" not in event_types
    assert provider.created_responses == []


@pytest.mark.anyio
async def test_realtime_agent_runner_accepts_short_overlap_transcript_with_reliable_audio() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = ImmediatePlayoutAudioPublisher()
    call_id = "call_short_overlap_reliable_audio"
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"browser-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        ai_speaking_tail_grace_seconds=0,
        user_turn_stability_delay_seconds=0,
    )

    await runner.start(session)
    await provider.emit(ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}))
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_short_reliable_audio"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_short_reliable_audio",
                "delta": base64.b64encode(b"\x01\x02" * 240).decode("ascii"),
            },
        )
    )
    await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)
    accepted = await runner.record_browser_speech_segment(
        call_id,
        datetime.now(timezone.utc),
        {
            "segmentId": "browser-short-reliable",
            "phase": "ended",
            "durationMs": 440,
            "rmsDbfs": -20.9,
            "noiseFloorDbfs": -45.2,
            "snrDb": 24.3,
            "hotFrameCount": 8,
            "remoteAudioActive": True,
            "remoteAudioRmsDbfs": -34.3,
        },
    )
    assert accepted is True
    await provider.emit(
        ProviderEvent(
            type="user_speech_started",
            payload={"audio_start_ms": 2140},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="user_transcript_done",
            payload={"transcript": "你好。", "audio_start_ms": 2140, "audio_end_ms": 3160},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="user_speech_stopped",
            payload={"audio_end_ms": 3160},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_short_reliable_audio", "status": "cancelled"}},
        )
    )
    await provider.close_events()
    await runner.wait(call_id)

    event_types = [event.type for event in store.list(call_id)]
    transcript_event = next(event for event in store.list(call_id) if event.type == "user_transcript_done")
    assert "user_transcript_semantic_rejected" not in event_types
    assert transcript_event.payload["commitDecision"] == "commit"
    assert provider.created_responses == [None]


@pytest.mark.anyio
async def test_realtime_agent_runner_accepts_short_overlap_transcript_from_strong_browser_segment() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = ImmediatePlayoutAudioPublisher()
    call_id = "call_short_overlap_strong_browser_segment"
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"browser-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        ai_speaking_tail_grace_seconds=0,
        user_turn_stability_delay_seconds=0,
    )

    await runner.start(session)
    await provider.emit(ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}))
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_short_strong_segment"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_short_strong_segment",
                "delta": base64.b64encode(b"\x01\x02" * 240).decode("ascii"),
            },
        )
    )
    await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)
    accepted = await runner.record_browser_speech_segment(
        call_id,
        datetime.now(timezone.utc),
        {
            "segmentId": "browser-short-strong",
            "phase": "ended",
            "durationMs": 321,
            "rmsDbfs": -19.7,
            "noiseFloorDbfs": -43.5,
            "snrDb": 23.8,
            "hotFrameCount": 5,
            "remoteAudioActive": True,
            "remoteAudioRmsDbfs": -43.2,
        },
    )
    assert accepted is True
    await provider.emit(
        ProviderEvent(
            type="user_speech_started",
            payload={"audio_start_ms": 2140},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="user_transcript_done",
            payload={"transcript": "可以。", "audio_start_ms": 2140, "audio_end_ms": 2461},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="user_speech_stopped",
            payload={"audio_end_ms": 2461},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_short_strong_segment", "status": "cancelled"}},
        )
    )
    await provider.close_events()
    await runner.wait(call_id)

    event_types = [event.type for event in store.list(call_id)]
    transcript_event = next(event for event in store.list(call_id) if event.type == "user_transcript_done")
    assert "browser_interrupt_candidate_promoted" in event_types
    assert "user_transcript_semantic_rejected" not in event_types
    assert transcript_event.payload["commitDecision"] == "commit"
    assert provider.created_responses == [None]


@pytest.mark.anyio
async def test_realtime_agent_runner_trusts_late_short_transcript_from_completed_speech() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    call_id = "call_late_short_transcript_after_speech_done"
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"browser-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        user_turn_stability_delay_seconds=0,
    )

    await runner.start(session)
    await provider.emit(ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}))
    await provider.emit(
        ProviderEvent(
            type="user_speech_started",
            payload={"audio_start_ms": 11000},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="user_speech_stopped",
            payload={"audio_end_ms": 11840},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_started_after_user_speech"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_started_after_user_speech",
                "delta": base64.b64encode(b"\x01\x02" * 240).decode("ascii"),
            },
        )
    )
    await provider.emit(
        ProviderEvent(
            type="user_transcript_done",
            payload={
                "transcript": "方便。",
                "audio_start_ms": 11000,
                "audio_end_ms": 11840,
            },
        )
    )
    await provider.close_events()
    await runner.wait(call_id)

    events = store.list(call_id)
    event_types = [event.type for event in events]
    transcript_event = next(event for event in events if event.type == "user_transcript_done")
    assert "user_transcript_semantic_rejected" not in event_types
    assert transcript_event.payload["transcriptTrust"] == "trusted"
    assert transcript_event.payload["commitDecision"] == "commit"


@pytest.mark.anyio
async def test_realtime_agent_runner_trusts_no_barge_short_transcript_with_browser_end_before_ai_response() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    call_id = "call_no_barge_short_transcript_browser_end_before_ai"
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"browser-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
            "barge_in_enabled": False,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        user_turn_stability_delay_seconds=0,
    )

    await runner.start(session)
    await provider.emit(ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}))
    await provider.emit(
        ProviderEvent(
            type="user_speech_started",
            payload={"audio_start_ms": 11000},
        )
    )
    observed = await runner.record_browser_speech_segment(
        call_id,
        datetime.now(timezone.utc),
        {
            "segmentId": "browser-fangbian-ended-before-ai",
            "phase": "ended",
            "durationMs": 359,
            "rmsDbfs": -18.0,
            "noiseFloorDbfs": -42.7,
            "snrDb": 24.7,
            "hotFrameCount": 6,
            "remoteAudioActive": False,
            "remoteAudioRmsDbfs": -120.0,
        },
    )
    assert observed is False
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_started_after_browser_speech"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_started_after_browser_speech",
                "delta": base64.b64encode(b"\x01\x02" * 240).decode("ascii"),
            },
        )
    )
    await provider.emit(
        ProviderEvent(
            type="user_speech_stopped",
            payload={"audio_end_ms": 11840},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="user_transcript_done",
            payload={
                "transcript": "方便。",
                "audio_start_ms": 11000,
                "audio_end_ms": 11840,
            },
        )
    )
    await provider.close_events()
    await runner.wait(call_id)

    events = store.list(call_id)
    event_types = [event.type for event in events]
    transcript_event = next(event for event in events if event.type == "user_transcript_done")
    assert "user_transcript_semantic_rejected" not in event_types
    assert "browser_interrupt_candidate_deferred" not in event_types
    assert transcript_event.payload["transcriptTrust"] == "trusted"
    assert transcript_event.payload["commitDecision"] == "commit"


@pytest.mark.anyio
async def test_realtime_agent_runner_trusts_short_answer_after_ai_question_tail() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = ImmediatePlayoutAudioPublisher()
    call_id = "call_short_answer_after_ai_question_tail"
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"browser-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
            "barge_in_enabled": False,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        user_turn_stability_delay_seconds=0.01,
    )

    await runner.start(session)
    await provider.emit(ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}))
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_choice_question"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_choice_question",
                "delta": base64.b64encode(b"\x01\x02" * 240).decode("ascii"),
            },
        )
    )
    await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)
    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={
                "response": {
                    "id": "resp_choice_question",
                    "status": "completed",
                    "output": [
                        {
                            "content": [
                                {
                                    "type": "audio",
                                    "transcript": (
                                        "您主要想了解GEO的具体做法，还是想看看它对品牌曝光"
                                        "和推荐效果能带来哪些提升呢？"
                                    ),
                                }
                            ]
                        }
                    ],
                }
            },
        )
    )
    await provider.emit(
        ProviderEvent(
            type="user_speech_started",
            payload={"audio_start_ms": 30280},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="user_speech_stopped",
            payload={"audio_end_ms": 31160},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="user_transcript_done",
            payload={
                "transcript": "对。",
                "audio_start_ms": 30280,
                "audio_end_ms": 31160,
            },
        )
    )
    await provider.close_events()
    await runner.wait(call_id)

    events = store.list(call_id)
    event_types = [event.type for event in events]
    transcript_event = next(event for event in events if event.type == "user_transcript_done")
    assert "user_transcript_semantic_rejected" not in event_types
    assert transcript_event.payload["transcriptTrust"] == "trusted"
    assert transcript_event.payload["commitDecision"] == "commit"
    assert provider.created_responses == [None]


@pytest.mark.anyio
async def test_realtime_agent_runner_accepts_complete_overlap_question() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = ImmediatePlayoutAudioPublisher()
    call_id = "call_complete_overlap_question"
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"browser-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        ai_speaking_tail_grace_seconds=0,
        user_turn_stability_delay_seconds=0,
    )

    await runner.start(session)
    await provider.emit(ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}))
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_question"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_question",
                "delta": base64.b64encode(b"\x01\x02" * 240).decode("ascii"),
            },
        )
    )
    await provider.emit(
        ProviderEvent(
            type="user_speech_started",
            payload={"audio_start_ms": 1400},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="user_transcript_done",
            payload={
                "transcript": "最后是什么？",
                "audio_start_ms": 1400,
                "audio_end_ms": 2840,
            },
        )
    )
    await provider.emit(
        ProviderEvent(
            type="user_speech_stopped",
            payload={"audio_end_ms": 2840},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_question", "status": "cancelled"}},
        )
    )
    await provider.close_events()
    await runner.wait(call_id)

    event_types = [event.type for event in store.list(call_id)]
    transcript_event = next(event for event in store.list(call_id) if event.type == "user_transcript_done")
    assert "user_transcript_semantic_rejected" not in event_types
    assert transcript_event.payload["commitDecision"] == "commit"
    assert provider.created_responses == [None]


@pytest.mark.anyio
async def test_realtime_agent_runner_maps_task_completed_call_end_reason() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    scheduler_calls: list[tuple[str, str]] = []
    session = CallSession(
        call_id="call_task_completed",
        room_name="ai-call-call_task_completed",
        participant_identity="browser-call_task_completed",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        call_end_scheduler=lambda call_id, reason: scheduler_calls.append((call_id, reason)),
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.emit(
        ProviderEvent(
            type="user_transcript_done",
            payload={"transcript": "可以，安排顾问联系我。"},
        )
    )
    await provider.emit(ProviderEvent(type="user_speech_stopped", payload={}))
    await provider.emit(
        ProviderEvent(
            type="tool_call_done",
            payload={
                "call_id": "tool_2",
                "name": "schedule_call_end",
                "arguments": json.dumps({"reason": "task_completed"}),
            },
        )
    )
    await provider.emit(ProviderEvent(type="model_response_done", payload={}))
    await provider.emit(ProviderEvent(type="model_response_started", payload={}))
    await provider.emit(ProviderEvent(type="model_response_done", payload={}))
    await asyncio.wait_for(
        _wait_until(lambda: scheduler_calls == [("call_task_completed", "normal_completed")]),
        timeout=1,
    )
    await provider.close_events()
    await runner.wait("call_task_completed")

    assert provider.submitted_tool_results[0][0] == "tool_2"
    assert provider.submitted_tool_results[0][1] == (
        "请直接回复：“好的，我已经记录，稍后会有顾问联系您。”"
        "不要添加其他内容，不要再提出问题。"
    )
    assert "call_end_scheduled" in [event.type for event in store.list("call_task_completed")]


@pytest.mark.anyio
async def test_realtime_agent_runner_rejects_task_completed_without_next_step_signal() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    scheduler_calls: list[tuple[str, str]] = []
    call_id = "call_task_completed_without_next_step"
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"browser-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        user_turn_stability_delay_seconds=0,
        call_end_scheduler=lambda call_id, reason: scheduler_calls.append((call_id, reason)),
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.emit(
        ProviderEvent(type="user_transcript_done", payload={"transcript": "行，两人是吧。"})
    )
    await provider.emit(ProviderEvent(type="user_speech_stopped", payload={}))
    await provider.emit(
        ProviderEvent(
            type="tool_call_done",
            payload={
                "call_id": "tool_task_completed_ambiguous",
                "name": "schedule_call_end",
                "arguments": json.dumps({"reason": "task_completed"}),
            },
        )
    )
    await provider.close_events()
    await runner.wait(call_id)

    events = store.list(call_id)
    event_types = [event.type for event in events]
    ignored_event = next(event for event in events if event.type == "call_end_tool_ignored")
    assert scheduler_calls == []
    assert "call_end_tool_requested" not in event_types
    assert "call_end_scheduled" not in event_types
    assert ignored_event.payload["reason"] == "task_completed_without_next_step_signal"
    assert provider.submitted_tool_results == [
        (
            "tool_task_completed_ambiguous",
            "未确认客户已同意后续联系或演示。请继续澄清下一步，不要结束通话。",
        )
    ]


@pytest.mark.anyio
async def test_realtime_agent_runner_accepts_task_completed_with_online_meeting_signal() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    scheduler_calls: list[tuple[str, str]] = []
    call_id = "call_task_completed_online_meeting_signal"
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"browser-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        user_turn_stability_delay_seconds=0,
        call_end_scheduler=lambda call_id, reason: scheduler_calls.append((call_id, reason)),
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.emit(
        ProviderEvent(type="user_transcript_done", payload={"transcript": "可以，先约个线上沟通。"})
    )
    await provider.emit(ProviderEvent(type="user_speech_stopped", payload={}))
    await provider.emit(
        ProviderEvent(
            type="tool_call_done",
            payload={
                "call_id": "tool_task_completed_meeting",
                "name": "schedule_call_end",
                "arguments": json.dumps({"reason": "task_completed"}),
            },
        )
    )
    await provider.emit(ProviderEvent(type="model_response_done", payload={}))
    await provider.emit(ProviderEvent(type="model_response_started", payload={}))
    await provider.emit(ProviderEvent(type="model_response_done", payload={}))
    await asyncio.wait_for(
        _wait_until(lambda: scheduler_calls == [(call_id, "normal_completed")]),
        timeout=1,
    )
    await provider.close_events()
    await runner.wait(call_id)

    event_types = [event.type for event in store.list(call_id)]
    assert "call_end_tool_requested" in event_types
    assert "call_end_tool_ignored" not in event_types
    assert "call_end_scheduled" in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_records_handoff_tool_request() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    session = CallSession(
        call_id="call_handoff_tool",
        room_name="ai-call-call_handoff_tool",
        participant_identity="browser-call_handoff_tool",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(
        ProviderEvent(
            type="tool_call_done",
            payload={
                "call_id": "handoff_tool_1",
                "name": "request_handoff",
                "arguments": json.dumps({"reason": "customer_request"}),
            },
        )
    )
    await asyncio.wait_for(
        _wait_until(
            lambda: any(
                event.type == "handoff_tool_requested" for event in store.list("call_handoff_tool")
            )
        ),
        timeout=1,
    )
    await provider.close_events()
    await runner.wait("call_handoff_tool")

    assert provider.submitted_tool_results == []
    assert provider.created_responses == []
    event_types = [event.type for event in store.list("call_handoff_tool")]
    assert event_types == [
        "model_session_started",
        "tool_call_done",
        "handoff_tool_requested",
    ]
    handoff_event = store.list("call_handoff_tool")[-1]
    assert handoff_event.payload == {
        "toolCallId": "handoff_tool_1",
        "reason": "customer_request",
    }


@pytest.mark.anyio
async def test_realtime_agent_runner_asks_confirmation_after_business_handoff_tool() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    session = CallSession(
        call_id="call_handoff_tool_confirm",
        room_name="ai-call-call_handoff_tool_confirm",
        participant_identity="browser-call_handoff_tool_confirm",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_handoff_tool"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="tool_call_done",
            payload={
                "call_id": "handoff_tool_business",
                "name": "request_handoff",
                "arguments": json.dumps({"reason": "business_escalation"}),
            },
        )
    )
    await asyncio.wait_for(
        _wait_until(lambda: provider.submitted_tool_results),
        timeout=1,
    )

    assert provider.created_responses == []
    assert provider.submitted_tool_results == [
        (
            "handoff_tool_business",
            "系统尚未开始转人工。请先询问用户是否确认需要转人工，不得说正在转接、马上接入或已经接通。",
        )
    ]

    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_handoff_tool", "status": "completed"}},
        )
    )
    await asyncio.wait_for(_wait_until(lambda: provider.created_responses == [None]), timeout=1)
    await provider.close_events()
    await runner.wait("call_handoff_tool_confirm")

    event_types = [event.type for event in store.list("call_handoff_tool_confirm")]
    assert event_types == [
        "model_session_started",
        "model_response_started",
        "tool_call_done",
        "handoff_tool_requested",
        "model_response_done",
    ]


@pytest.mark.anyio
async def test_realtime_agent_runner_uses_qwen_text_stash_transcript_preview() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = FakeRealtimeProvider([
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}),
        ProviderEvent(type="user_speech_started", payload={}),
        ProviderEvent(
            type="user_transcript_delta",
            payload={"text": "", "stash": "今天"},
        ),
        ProviderEvent(
            type="user_transcript_delta",
            payload={"text": "今天", "stash": "天气怎么样"},
        ),
        ProviderEvent(type="user_speech_stopped", payload={}),
    ])
    session = CallSession(
        call_id="call_qwen_transcript",
        room_name="ai-call-call_qwen_transcript",
        participant_identity="browser-call_qwen_transcript",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
    )

    await runner.start(session)
    await runner.wait("call_qwen_transcript")

    assert provider.created_responses == [None]
    assert registry.get("call_qwen_transcript").status == CallSessionStatus.AI_THINKING
    transcript_events = [
        event
        for event in store.list("call_qwen_transcript")
        if event.type == "user_transcript_delta"
    ]
    assert transcript_events[-1].payload == {"text": "今天", "stash": "天气怎么样"}


@pytest.mark.anyio
async def test_realtime_agent_runner_records_transcription_failure_without_reply() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = FakeRealtimeProvider([
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}),
        ProviderEvent(type="user_speech_started", payload={}),
        ProviderEvent(
            type="user_transcript_failed",
            payload={"error": {"code": "asr_failed", "message": "no speech"}},
        ),
        ProviderEvent(type="user_speech_stopped", payload={}),
    ])
    session = CallSession(
        call_id="call_transcript_failed",
        room_name="ai-call-call_transcript_failed",
        participant_identity="browser-call_transcript_failed",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
    )

    await runner.start(session)
    await runner.wait("call_transcript_failed")

    assert provider.created_responses == []
    assert registry.get("call_transcript_failed").status == CallSessionStatus.CONNECTED
    assert [event.type for event in store.list("call_transcript_failed")] == [
        "model_session_started",
        "user_speech_started",
        "user_transcript_failed",
        "user_speech_stopped",
    ]


@pytest.mark.anyio
async def test_realtime_agent_runner_publishes_model_audio_delta_to_audio_sink() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    metrics = CallMetrics()
    output_pcm = b"\x01\x02" * 240
    provider = FakeRealtimeProvider([
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}),
        ProviderEvent(type="user_speech_started", payload={}),
        ProviderEvent(type="user_speech_stopped", payload={}),
        ProviderEvent(
            type="model_audio_delta",
            payload={"delta": base64.b64encode(output_pcm).decode("ascii")},
        ),
    ])
    publisher = FakeAudioPublisher()
    session = CallSession(
        call_id="call_publish",
        room_name="ai-call-call_publish",
        participant_identity="browser-call_publish",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        metrics_by_call_id={"call_publish": metrics},
        audio_publisher=publisher,
    )

    await runner.start(session)
    await runner.wait("call_publish")

    assert len(publisher.published) == 1
    call_id, frame = publisher.published[0]
    assert call_id == "call_publish"
    assert frame.data == output_pcm
    assert frame.sample_rate_hz == 24000
    assert frame.channels == 1
    assert "ai_audio_published" in [event.type for event in store.list("call_publish")]
    assert registry.get("call_publish").metrics["lastPublishDelayMs"] is not None


@pytest.mark.anyio
async def test_realtime_agent_runner_publishes_model_audio_when_response_starts_after_stopped_turn() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    output_pcm = b"\x01\x02" * 240
    provider = FakeRealtimeProvider([
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}),
        ProviderEvent(type="user_speech_started", payload={}),
        ProviderEvent(type="user_transcript_delta", payload={"delta": "你好"}),
        ProviderEvent(type="user_speech_stopped", payload={}),
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_auto_after_stop"}},
        ),
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_auto_after_stop",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        ),
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_auto_after_stop", "status": "completed"}},
        ),
    ])
    publisher = FakeAudioPublisher()
    session = CallSession(
        call_id="call_response_started_after_stop",
        room_name="ai-call-call_response_started_after_stop",
        participant_identity="browser-call_response_started_after_stop",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        user_turn_stability_delay_seconds=0.05,
    )

    await runner.start(session)
    await runner.wait("call_response_started_after_stop")

    assert len(publisher.published) == 1
    assert provider.created_responses == []
    event_types = [event.type for event in store.list("call_response_started_after_stop")]
    assert "ai_audio_published" in event_types
    assert "stale_audio_dropped" not in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_publishes_model_audio_when_user_turn_starts_from_ready() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    output_pcm = b"\x01\x02" * 240
    provider = FakeRealtimeProvider([
        ProviderEvent(type="user_speech_started", payload={}),
        ProviderEvent(type="user_transcript_delta", payload={"delta": "你好"}),
        ProviderEvent(type="user_speech_stopped", payload={}),
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_ready_turn"}},
        ),
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_ready_turn",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        ),
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_ready_turn", "status": "completed"}},
        ),
    ])
    publisher = FakeAudioPublisher()
    session = CallSession(
        call_id="call_ready_turn",
        room_name="ai-call-call_ready_turn",
        participant_identity="browser-call_ready_turn",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        user_turn_stability_delay_seconds=0.05,
    )

    await runner.start(session)
    await runner.wait("call_ready_turn")

    assert len(publisher.published) == 1
    assert provider.created_responses == []
    event_types = [event.type for event in store.list("call_ready_turn")]
    assert "ai_audio_published" in event_types
    assert "stale_audio_dropped" not in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_splits_large_model_audio_delta_before_publishing() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    output_pcm = b"\x01\x02" * 7680
    provider = FakeRealtimeProvider([
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}),
        ProviderEvent(type="user_speech_started", payload={}),
        ProviderEvent(type="user_speech_stopped", payload={}),
        ProviderEvent(
            type="model_audio_delta",
            payload={"delta": base64.b64encode(output_pcm).decode("ascii")},
        ),
    ])
    publisher = FakeAudioPublisher()
    session = CallSession(
        call_id="call_split_publish",
        room_name="ai-call-call_split_publish",
        participant_identity="browser-call_split_publish",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        user_turn_stability_delay_seconds=0.05,
    )

    await runner.start(session)
    await runner.wait("call_split_publish")

    assert len(publisher.published) == 8
    assert {len(frame.data) for _call_id, frame in publisher.published} == {1920}
    assert b"".join(frame.data for _call_id, frame in publisher.published) == output_pcm
    assert [call_id for call_id, _frame in publisher.published] == ["call_split_publish"] * 8


@pytest.mark.anyio
async def test_realtime_agent_runner_tracks_opening_audio_metrics_and_state() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    metrics = CallMetrics()
    metrics.mark_model_response_requested(datetime.now(timezone.utc))
    output_pcm = b"\x03\x04" * 240
    provider = FakeRealtimeProvider([
        ProviderEvent(
            type="model_audio_delta",
            payload={"delta": base64.b64encode(output_pcm).decode("ascii")},
        ),
        ProviderEvent(type="model_response_done", payload={}),
    ])
    publisher = FakeAudioPublisher()
    session = CallSession(
        call_id="call_opening_audio",
        room_name="ai-call-call_opening_audio",
        participant_identity="browser-call_opening_audio",
        status=CallSessionStatus.CONNECTED,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        metrics_by_call_id={"call_opening_audio": metrics},
        audio_publisher=publisher,
    )

    await runner.start(session)
    await runner.wait("call_opening_audio")

    snapshot = registry.get("call_opening_audio").metrics
    assert registry.get("call_opening_audio").status == CallSessionStatus.CONNECTED
    assert snapshot["lastModelFirstAudioMs"] is not None
    assert snapshot["lastPublishDelayMs"] is not None
    assert [event.type for event in store.list("call_opening_audio")] == [
        "model_audio_delta",
        "ai_audio_published",
        "model_response_done",
    ]


@pytest.mark.anyio
async def test_realtime_agent_runner_redacts_provider_session_instructions_in_events() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = FakeRealtimeProvider([
        ProviderEvent(
            type="model_session_updated",
            payload={
                "type": "session.updated",
                "session": {
                    "id": "sess_1",
                    "instructions": "不要把完整 prompt 暴露给前端",
                    "voice": "Tina",
                },
            },
        ),
    ])
    session = CallSession(
        call_id="call_redact",
        room_name="ai-call-call_redact",
        participant_identity="browser-call_redact",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "不要把完整 prompt 暴露给前端",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
    )

    await runner.start(session)
    await runner.wait("call_redact")

    event = store.list("call_redact")[0]
    assert event.payload["session"]["instructions"] == "<redacted>"
    assert "不要把完整 prompt" not in str(event.payload)


@pytest.mark.anyio
async def test_realtime_agent_runner_redacts_provider_audio_delta_in_events() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    raw_delta = base64.b64encode(b"\x00\x01\x02\x03").decode()
    provider = FakeRealtimeProvider([
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "type": "response.audio.delta",
                "delta": raw_delta,
            },
        ),
    ])
    session = CallSession(
        call_id="call_audio_redact",
        room_name="ai-call-call_audio_redact",
        participant_identity="browser-call_audio_redact",
        status=CallSessionStatus.CONNECTED,
        effective_config={
            "voice": "Tina",
            "prompt": "prompt",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
    )

    await runner.start(session)
    await runner.wait("call_audio_redact")

    event = store.list("call_audio_redact")[0]
    assert event.payload["delta"] == "<redacted_audio_delta>"
    assert event.payload["deltaBytes"] == 4
    assert raw_delta not in str(event.payload)


@pytest.mark.anyio
async def test_realtime_agent_runner_starts_opening_response_with_configured_message() -> None:
    registry = InMemorySessionRegistry()
    provider = FakeRealtimeProvider([])
    session = CallSession(
        call_id="call_opening",
        room_name="ai-call-call_opening",
        participant_identity="browser-call_opening",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "opening_message": "您好，我是灵宸智能助手，请问现在方便简单沟通一下吗？",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=InMemoryEventStore(),
    )

    await runner.start(session)
    await runner.start_opening("call_opening")

    assert provider.created_responses == [
        "请主动说出开场白：您好，我是灵宸智能助手，请问现在方便简单沟通一下吗？"
    ]
    assert "您好，我是灵宸智能助手" in provider.session_updates[0].instructions


@pytest.mark.anyio
async def test_realtime_agent_runner_prepends_fixed_handoff_capability_prompt() -> None:
    registry = InMemorySessionRegistry()
    provider = QueueRealtimeProvider()
    session = CallSession(
        call_id="call_handoff_prompt_constraint",
        room_name="ai-call-call_handoff_prompt_constraint",
        participant_identity="browser-call_handoff_prompt_constraint",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "只用一句话回答用户问题。",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=InMemoryEventStore(),
        handoff_prompt_constraint_enabled=True,
    )

    await runner.start(session)
    await provider.close_events()
    await runner.wait("call_handoff_prompt_constraint")

    instructions = provider.session_updates[0].instructions
    assert instructions.startswith("系统固定转人工能力约束")
    assert "request_handoff" in instructions
    assert "不要声称自己已经是人工客服" in instructions
    assert "业务话术：" in instructions
    assert "只用一句话回答用户问题。" in instructions


@pytest.mark.anyio
async def test_realtime_agent_runner_confirms_interrupt_when_user_speaks_during_ai_audio() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    output_pcm = b"\x01\x02" * 240
    provider = FakeRealtimeProvider([
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}),
        ProviderEvent(type="user_speech_started", payload={}),
        ProviderEvent(type="user_speech_stopped", payload={}),
        ProviderEvent(
            type="model_audio_delta",
            payload={"delta": base64.b64encode(output_pcm).decode("ascii")},
        ),
        ProviderEvent(type="user_speech_started", payload={}),
        ProviderEvent(type="user_transcript_delta", payload={"delta": "你好"}),
        ProviderEvent(type="user_speech_stopped", payload={}),
    ])
    publisher = FakeAudioPublisher()
    session = CallSession(
        call_id="call_interrupt",
        room_name="ai-call-call_interrupt",
        participant_identity="browser-call_interrupt",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        user_turn_stability_delay_seconds=0.05,
    )

    await runner.start(session)
    await runner.wait("call_interrupt")

    assert provider.cancelled_response_count == 1
    assert provider.cleared_input_count == 0
    assert provider.created_responses == [None]
    assert publisher.stopped_call_ids
    assert set(publisher.stopped_call_ids) == {"call_interrupt"}
    assert registry.get("call_interrupt").status == CallSessionStatus.AI_THINKING
    event_types = [event.type for event in store.list("call_interrupt")]
    assert "interrupt_candidate" in event_types
    assert "interrupt_confirmed" in event_types
    assert registry.get("call_interrupt").metrics["lastInterruptStopMs"] is not None


@pytest.mark.anyio
async def test_realtime_agent_runner_confirms_interrupt_until_published_audio_plays_out() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    output_pcm = b"\x01\x02" * 240
    provider = FakeRealtimeProvider([
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}),
        ProviderEvent(type="user_speech_started", payload={}),
        ProviderEvent(type="user_speech_stopped", payload={}),
        ProviderEvent(
            type="model_audio_delta",
            payload={"delta": base64.b64encode(output_pcm).decode("ascii")},
        ),
        ProviderEvent(type="model_response_done", payload={}),
        ProviderEvent(type="user_speech_started", payload={}),
        ProviderEvent(type="user_transcript_delta", payload={"delta": "你好"}),
        ProviderEvent(type="user_speech_stopped", payload={}),
    ])
    publisher = WaitingAudioPublisher()
    session = CallSession(
        call_id="call_playout_interrupt",
        room_name="ai-call-call_playout_interrupt",
        participant_identity="browser-call_playout_interrupt",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        user_turn_stability_delay_seconds=0.05,
    )

    await runner.start(session)
    await runner.wait("call_playout_interrupt")

    assert provider.cancelled_response_count == 1
    assert provider.cleared_input_count == 0
    assert provider.created_responses == [None]
    assert publisher.stopped_call_ids
    assert set(publisher.stopped_call_ids) == {"call_playout_interrupt"}
    assert registry.get("call_playout_interrupt").status == CallSessionStatus.AI_THINKING
    event_types = [event.type for event in store.list("call_playout_interrupt")]
    assert "interrupt_candidate" in event_types
    assert "interrupt_confirmed" in event_types
    assert registry.get("call_playout_interrupt").metrics["lastInterruptStopMs"] is not None


@pytest.mark.anyio
async def test_realtime_agent_runner_confirms_interrupt_during_ai_audio_tail_grace() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    output_pcm = b"\x01\x02" * 240
    provider = FakeRealtimeProvider([
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}),
        ProviderEvent(type="user_speech_started", payload={}),
        ProviderEvent(type="user_speech_stopped", payload={}),
        ProviderEvent(
            type="model_audio_delta",
            payload={"delta": base64.b64encode(output_pcm).decode("ascii")},
        ),
        ProviderEvent(type="model_response_done", payload={}),
        ProviderEvent(type="user_speech_started", payload={}),
        ProviderEvent(type="user_transcript_delta", payload={"delta": "你好"}),
        ProviderEvent(type="user_speech_stopped", payload={}),
    ])
    publisher = ImmediatePlayoutAudioPublisher()
    session = CallSession(
        call_id="call_tail_interrupt",
        room_name="ai-call-call_tail_interrupt",
        participant_identity="browser-call_tail_interrupt",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    await runner.start(session)
    await runner.wait("call_tail_interrupt")

    assert provider.cancelled_response_count == 1
    assert provider.created_responses == [None]
    assert publisher.stopped_call_ids
    assert set(publisher.stopped_call_ids) == {"call_tail_interrupt"}
    assert registry.get("call_tail_interrupt").status == CallSessionStatus.AI_THINKING
    event_types = [event.type for event in store.list("call_tail_interrupt")]
    assert "interrupt_candidate" in event_types
    assert "interrupt_confirmed" in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_keeps_audio_playing_on_browser_candidate() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_browser_candidate",
        room_name="ai-call-call_browser_candidate",
        participant_identity="browser-call_browser_candidate",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_browser_candidate"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_browser_candidate",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
    )
    await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)
    accepted = await runner.record_browser_speech_candidate(
        "call_browser_candidate",
        datetime.now(timezone.utc),
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_browser_candidate",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_browser_candidate", "status": "completed"}},
        )
    )
    await provider.close_events()
    await runner.wait("call_browser_candidate")

    assert accepted is True
    assert provider.cancelled_response_count == 0
    assert provider.cleared_input_count == 0
    assert provider.created_responses == []
    assert publisher.stopped_call_ids == []
    assert len(publisher.published) == 2
    assert registry.get("call_browser_candidate").status == CallSessionStatus.CONNECTED
    events = store.list("call_browser_candidate")
    event_types = [event.type for event in events]
    assert "interrupt_candidate" in event_types
    assert "browser_interrupt_candidate_deferred" in event_types
    assert "interrupt_pending" not in event_types
    assert "stale_audio_dropped" not in event_types
    assert "interrupt_confirmed" not in event_types
    assert "response_generation_invalidated" not in event_types
    assert "playout_queue_flushed" not in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_keeps_opening_audio_on_browser_candidate() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_opening_noise",
        room_name="ai-call-call_opening_noise",
        participant_identity="browser-call_opening_noise",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "opening_message": "您好，我是灵宸智能助手，请问现在方便简单沟通一下吗？",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await runner.start_opening("call_opening_noise")
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_opening"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_opening",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
    )
    await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)
    accepted = await runner.record_browser_speech_candidate(
        "call_opening_noise",
        datetime.now(timezone.utc),
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_opening",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_opening", "status": "completed"}},
        )
    )
    await provider.close_events()
    await runner.wait("call_opening_noise")

    assert accepted is True
    assert provider.cancelled_response_count == 0
    assert provider.cleared_input_count == 0
    assert provider.created_responses == [
        "请主动说出开场白：您好，我是灵宸智能助手，请问现在方便简单沟通一下吗？"
    ]
    assert publisher.stopped_call_ids == []
    assert len(publisher.published) == 2
    events = store.list("call_opening_noise")
    event_types = [event.type for event in events]
    assert "interrupt_candidate" in event_types
    assert "browser_interrupt_candidate_deferred" in event_types
    assert "interrupt_pending" not in event_types
    assert "stale_audio_dropped" not in event_types
    assert "interrupt_confirmed" not in event_types
    assert "response_generation_invalidated" not in event_types
    assert "playout_queue_flushed" not in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_keeps_audio_after_browser_candidate_before_confirm() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_browser_stale_before_confirm",
        room_name="ai-call-call_browser_stale_before_confirm",
        participant_identity="browser-call_browser_stale_before_confirm",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_stale"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_stale",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
    )
    await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)

    accepted = await runner.record_browser_speech_candidate(
        "call_browser_stale_before_confirm",
        datetime.now(timezone.utc),
    )
    await asyncio.sleep(0.65)
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_stale",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_stale", "status": "completed"}},
        )
    )
    await provider.close_events()
    await runner.wait("call_browser_stale_before_confirm")

    assert accepted is True
    assert provider.cancelled_response_count == 0
    assert publisher.stopped_call_ids == []
    assert len(publisher.published) == 2
    event_types = [event.type for event in store.list("call_browser_stale_before_confirm")]
    assert "interrupt_candidate" in event_types
    assert "browser_interrupt_candidate_deferred" in event_types
    assert "interrupt_confirmed" not in event_types
    assert "response_generation_invalidated" not in event_types
    assert "playout_queue_flushed" not in event_types
    assert "stale_audio_dropped" not in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_marks_repeated_browser_candidate_without_stopping_audio() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_repeated_browser_candidate",
        room_name="ai-call-call_repeated_browser_candidate",
        participant_identity="browser-call_repeated_browser_candidate",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_repeated_browser"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_repeated_browser",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
    )
    await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)

    first_at = datetime.now(timezone.utc)
    first_accepted = await runner.record_browser_speech_candidate(
        "call_repeated_browser_candidate",
        first_at,
    )
    second_accepted = await runner.record_browser_speech_candidate(
        "call_repeated_browser_candidate",
        first_at + timedelta(seconds=1.2),
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_repeated_browser", "status": "completed"}},
        )
    )
    await provider.close_events()
    await runner.wait("call_repeated_browser_candidate")

    event_types = [event.type for event in store.list("call_repeated_browser_candidate")]
    assert first_accepted is True
    assert second_accepted is True
    assert provider.cancelled_response_count == 0
    assert publisher.stopped_call_ids == []
    assert event_types.count("interrupt_candidate") == 1
    assert "browser_interrupt_candidate_deferred" in event_types
    assert "browser_interrupt_candidate_promoted" in event_types
    assert "response_generation_invalidated" not in event_types
    assert "interrupt_audio_stop_requested" not in event_types
    assert "interrupt_confirmed" not in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_defers_weak_browser_speech_segment() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_weak_browser_segment",
        room_name="ai-call-call_weak_browser_segment",
        participant_identity="browser-call_weak_browser_segment",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_weak_browser_segment"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_weak_browser_segment",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
    )
    await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)

    accepted = await runner.record_browser_speech_segment(
        "call_weak_browser_segment",
        datetime.now(timezone.utc),
        {
            "segmentId": "browser-seg-weak",
            "phase": "started",
            "durationMs": 120,
            "rmsDbfs": -34.0,
            "noiseFloorDbfs": -48.0,
            "snrDb": 14.0,
            "hotFrameCount": 3,
            "remoteAudioActive": True,
        },
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_weak_browser_segment",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_weak_browser_segment", "status": "completed"}},
        )
    )
    await provider.close_events()
    await runner.wait("call_weak_browser_segment")

    event_types = [event.type for event in store.list("call_weak_browser_segment")]
    assert accepted is True
    assert provider.cancelled_response_count == 0
    assert publisher.stopped_call_ids == []
    assert len(publisher.published) == 2
    assert "interrupt_candidate" in event_types
    assert "browser_interrupt_candidate_deferred" in event_types
    assert "browser_interrupt_candidate_promoted" not in event_types
    assert "response_generation_invalidated" not in event_types
    assert "interrupt_confirmed" not in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_marks_strong_browser_speech_segment_without_stopping_audio() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_strong_browser_segment",
        room_name="ai-call-call_strong_browser_segment",
        participant_identity="browser-call_strong_browser_segment",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_strong_browser_segment"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_strong_browser_segment",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
    )
    await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)

    accepted = await runner.record_browser_speech_segment(
        "call_strong_browser_segment",
        datetime.now(timezone.utc),
        {
            "segmentId": "browser-seg-strong",
            "phase": "updated",
            "durationMs": 440,
            "rmsDbfs": -30.0,
            "noiseFloorDbfs": -45.0,
            "snrDb": 15.0,
            "hotFrameCount": 11,
            "remoteAudioActive": True,
        },
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_strong_browser_segment", "status": "completed"}},
        )
    )
    await provider.close_events()
    await runner.wait("call_strong_browser_segment")

    events = store.list("call_strong_browser_segment")
    event_types = [event.type for event in events]
    promoted = next(event for event in events if event.type == "browser_interrupt_candidate_promoted")
    assert accepted is True
    assert provider.cancelled_response_count == 0
    assert publisher.stopped_call_ids == []
    assert event_types.count("interrupt_candidate") == 1
    assert "browser_interrupt_candidate_deferred" in event_types
    assert "browser_interrupt_candidate_promoted" in event_types
    assert "response_generation_invalidated" not in event_types
    assert "interrupt_audio_stop_requested" not in event_types
    assert "interrupt_confirmed" not in event_types
    assert promoted.payload["reason"] == "browser_speech_segment_strong_during_ai_audio"
    assert promoted.payload["segmentId"] == "browser-seg-strong"
    assert promoted.payload["durationMs"] == 440
    assert promoted.payload["snrDb"] == 15.0
    assert promoted.payload["hotFrameCount"] == 11


@pytest.mark.anyio
async def test_realtime_agent_runner_records_browser_pre_stop_skip_reason() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_browser_pre_stop_skip_reason",
        room_name="ai-call-call_browser_pre_stop_skip_reason",
        participant_identity="browser-call_browser_pre_stop_skip_reason",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    try:
        await runner.start(session)
        await provider.emit(
            ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
        )
        await provider.emit(
            ProviderEvent(
                type="model_response_started",
                payload={"response": {"id": "resp_browser_pre_stop_skip_reason"}},
            )
        )
        await provider.emit(
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_browser_pre_stop_skip_reason",
                    "delta": base64.b64encode(output_pcm).decode("ascii"),
                },
            )
        )
        await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)

        accepted = await runner.record_browser_speech_segment(
            "call_browser_pre_stop_skip_reason",
            datetime.now(timezone.utc),
            {
                "segmentId": "browser-seg-near-pre-stop",
                "phase": "updated",
                "durationMs": 479,
                "rmsDbfs": -17.7,
                "noiseFloorDbfs": -44.9,
                "snrDb": 27.2,
                "hotFrameCount": 13,
                "remoteAudioActive": True,
                "remoteAudioRmsDbfs": -15.0,
            },
        )
        await provider.close_events()
        await runner.wait("call_browser_pre_stop_skip_reason")

        events = store.list("call_browser_pre_stop_skip_reason")
        event_types = [event.type for event in events]
        skipped = next(event for event in events if event.type == "browser_pre_stop_skipped")
        assert accepted is True
        assert "browser_interrupt_candidate_promoted" in event_types
        assert "browser_pre_stop_requested" not in event_types
        assert skipped.payload["skipReason"] == "below_min_duration"
        assert skipped.payload["minDurationMs"] == 480
        assert skipped.payload["durationMs"] == 479
    finally:
        await runner.stop("call_browser_pre_stop_skip_reason")


@pytest.mark.anyio
async def test_realtime_agent_runner_marks_short_clear_browser_speech_segment_without_stopping_audio() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_short_clear_browser_segment",
        room_name="ai-call-call_short_clear_browser_segment",
        participant_identity="browser-call_short_clear_browser_segment",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_short_clear_browser_segment"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_short_clear_browser_segment",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
    )
    await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)

    accepted = await runner.record_browser_speech_segment(
        "call_short_clear_browser_segment",
        datetime.now(timezone.utc),
        {
            "segmentId": "browser-seg-short-clear",
            "phase": "ended",
            "durationMs": 321,
            "rmsDbfs": -20.5,
            "noiseFloorDbfs": -45.3,
            "snrDb": 24.8,
            "hotFrameCount": 5,
            "remoteAudioActive": False,
        },
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_short_clear_browser_segment", "status": "completed"}},
        )
    )
    await provider.close_events()
    await runner.wait("call_short_clear_browser_segment")

    event_types = [event.type for event in store.list("call_short_clear_browser_segment")]
    assert accepted is True
    assert provider.cancelled_response_count == 0
    assert publisher.stopped_call_ids == []
    assert "browser_interrupt_candidate_promoted" in event_types
    assert "response_generation_invalidated" not in event_types
    assert "interrupt_audio_stop_requested" not in event_types
    assert "interrupt_confirmed" not in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_audio_holds_medium_browser_speech_segment() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_browser_audio_hold",
        room_name="ai-call-call_browser_audio_hold",
        participant_identity="browser-call_browser_audio_hold",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    try:
        await runner.start(session)
        await provider.emit(
            ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
        )
        await provider.emit(
            ProviderEvent(
                type="model_response_started",
                payload={"response": {"id": "resp_browser_audio_hold"}},
            )
        )
        await provider.emit(
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_browser_audio_hold",
                    "delta": base64.b64encode(output_pcm).decode("ascii"),
                },
            )
        )
        await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)

        accepted = await runner.record_browser_speech_segment(
            "call_browser_audio_hold",
            datetime.now(timezone.utc),
            {
                "segmentId": "browser-seg-audio-hold",
                "phase": "updated",
                "durationMs": 281,
                "rmsDbfs": -18.0,
                "noiseFloorDbfs": -45.0,
                "snrDb": 27.0,
                "hotFrameCount": 7,
                "remoteAudioActive": True,
                "remoteAudioRmsDbfs": -42.0,
            },
        )
        await provider.emit(
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_browser_audio_hold",
                    "delta": base64.b64encode(output_pcm).decode("ascii"),
                },
            )
        )
        await provider.close_events()
        await runner.wait("call_browser_audio_hold")

        event_types = [event.type for event in store.list("call_browser_audio_hold")]
        assert accepted is True
        assert provider.cancelled_response_count == 0
        assert publisher.stopped_call_ids == ["call_browser_audio_hold"]
        assert len(publisher.published) == 1
        assert "browser_audio_hold_requested" in event_types
        assert "browser_audio_hold_completed" in event_types
        assert "browser_pre_stop_requested" not in event_types
        assert "stale_audio_dropped" in event_types
        assert "response_generation_invalidated" not in event_types
        assert "interrupt_audio_stop_requested" not in event_types
        assert "interrupt_confirmed" not in event_types
    finally:
        await runner.stop("call_browser_audio_hold")


@pytest.mark.anyio
async def test_realtime_agent_runner_audio_holds_sustained_low_snr_browser_speech() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_browser_audio_hold_low_snr",
        room_name="ai-call-call_browser_audio_hold_low_snr",
        participant_identity="browser-call_browser_audio_hold_low_snr",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    try:
        await runner.start(session)
        await provider.emit(
            ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
        )
        await provider.emit(
            ProviderEvent(
                type="model_response_started",
                payload={"response": {"id": "resp_browser_audio_hold_low_snr"}},
            )
        )
        await provider.emit(
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_browser_audio_hold_low_snr",
                    "delta": base64.b64encode(output_pcm).decode("ascii"),
                },
            )
        )
        await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)

        accepted = await runner.record_browser_speech_segment(
            "call_browser_audio_hold_low_snr",
            datetime.now(timezone.utc),
            {
                "segmentId": "browser-seg-audio-hold-low-snr",
                "phase": "ended",
                "durationMs": 600,
                "rmsDbfs": -19.3,
                "noiseFloorDbfs": -37.7,
                "snrDb": 18.4,
                "hotFrameCount": 10,
                "remoteAudioActive": True,
                "remoteAudioRmsDbfs": -25.8,
            },
        )
        await provider.emit(
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_browser_audio_hold_low_snr",
                    "delta": base64.b64encode(output_pcm).decode("ascii"),
                },
            )
        )
        await provider.close_events()
        await runner.wait("call_browser_audio_hold_low_snr")

        event_types = [event.type for event in store.list("call_browser_audio_hold_low_snr")]
        assert accepted is True
        assert provider.cancelled_response_count == 0
        assert publisher.stopped_call_ids == ["call_browser_audio_hold_low_snr"]
        assert len(publisher.published) == 1
        assert "browser_audio_hold_requested" in event_types
        assert "browser_audio_hold_completed" in event_types
        assert "browser_pre_stop_requested" not in event_types
        assert "stale_audio_dropped" in event_types
        assert "response_generation_invalidated" not in event_types
        assert "interrupt_audio_stop_requested" not in event_types
        assert "interrupt_confirmed" not in event_types
    finally:
        await runner.stop("call_browser_audio_hold_low_snr")


@pytest.mark.anyio
async def test_realtime_agent_runner_audio_holds_speech_like_browser_interruption_sample() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_browser_audio_hold_speech_like_sample",
        room_name="ai-call-call_browser_audio_hold_speech_like_sample",
        participant_identity="browser-call_browser_audio_hold_speech_like_sample",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    try:
        await runner.start(session)
        await provider.emit(
            ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
        )
        await provider.emit(
            ProviderEvent(
                type="model_response_started",
                payload={"response": {"id": "resp_browser_audio_hold_speech_like_sample"}},
            )
        )
        await provider.emit(
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_browser_audio_hold_speech_like_sample",
                    "delta": base64.b64encode(output_pcm).decode("ascii"),
                },
            )
        )
        await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)

        accepted = await runner.record_browser_speech_segment(
            "call_browser_audio_hold_speech_like_sample",
            datetime.now(timezone.utc),
            {
                "segmentId": "browser-seg-speech-like-sample",
                "phase": "ended",
                "durationMs": 441,
                "rmsDbfs": -10.0,
                "noiseFloorDbfs": -28.0,
                "snrDb": 18.0,
                "hotFrameCount": 8,
                "remoteAudioActive": True,
                "remoteAudioRmsDbfs": -37.6,
            },
        )
        await provider.emit(
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_browser_audio_hold_speech_like_sample",
                    "delta": base64.b64encode(output_pcm).decode("ascii"),
                },
            )
        )
        await provider.close_events()
        await runner.wait("call_browser_audio_hold_speech_like_sample")

        event_types = [
            event.type for event in store.list("call_browser_audio_hold_speech_like_sample")
        ]
        assert accepted is True
        assert provider.cancelled_response_count == 0
        assert publisher.stopped_call_ids == ["call_browser_audio_hold_speech_like_sample"]
        assert len(publisher.published) == 1
        assert "browser_audio_hold_requested" in event_types
        assert "browser_audio_hold_completed" in event_types
        assert "browser_pre_stop_requested" not in event_types
        assert "stale_audio_dropped" in event_types
        assert "response_generation_invalidated" not in event_types
        assert "interrupt_audio_stop_requested" not in event_types
        assert "interrupt_confirmed" not in event_types
    finally:
        await runner.stop("call_browser_audio_hold_speech_like_sample")


@pytest.mark.anyio
async def test_realtime_agent_runner_audio_holds_sustained_double_talk_browser_speech() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_browser_audio_hold_double_talk",
        room_name="ai-call-call_browser_audio_hold_double_talk",
        participant_identity="browser-call_browser_audio_hold_double_talk",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    try:
        await runner.start(session)
        await provider.emit(
            ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
        )
        await provider.emit(
            ProviderEvent(
                type="model_response_started",
                payload={"response": {"id": "resp_browser_audio_hold_double_talk"}},
            )
        )
        await provider.emit(
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_browser_audio_hold_double_talk",
                    "delta": base64.b64encode(output_pcm).decode("ascii"),
                },
            )
        )
        await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)

        accepted = await runner.record_browser_speech_segment(
            "call_browser_audio_hold_double_talk",
            datetime.now(timezone.utc),
            {
                "segmentId": "browser-seg-audio-hold-double-talk",
                "phase": "ended",
                "durationMs": 560,
                "rmsDbfs": -23.6,
                "noiseFloorDbfs": -44.7,
                "snrDb": 21.1,
                "hotFrameCount": 11,
                "remoteAudioActive": True,
                "remoteAudioRmsDbfs": -17.1,
            },
        )
        await provider.emit(
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_browser_audio_hold_double_talk",
                    "delta": base64.b64encode(output_pcm).decode("ascii"),
                },
            )
        )
        await provider.close_events()
        await runner.wait("call_browser_audio_hold_double_talk")

        event_types = [event.type for event in store.list("call_browser_audio_hold_double_talk")]
        assert accepted is True
        assert provider.cancelled_response_count == 0
        assert publisher.stopped_call_ids == ["call_browser_audio_hold_double_talk"]
        assert len(publisher.published) == 1
        assert "browser_audio_hold_requested" in event_types
        assert "browser_audio_hold_completed" in event_types
        assert "browser_audio_hold_rejected_echo" not in event_types
        assert "browser_pre_stop_requested" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert "interrupt_audio_stop_requested" not in event_types
        assert "interrupt_confirmed" not in event_types
    finally:
        await runner.stop("call_browser_audio_hold_double_talk")


@pytest.mark.anyio
async def test_realtime_agent_runner_provider_speech_confirms_browser_audio_hold() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_browser_audio_hold_provider",
        room_name="ai-call-call_browser_audio_hold_provider",
        participant_identity="browser-call_browser_audio_hold_provider",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    try:
        await runner.start(session)
        await provider.emit(
            ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
        )
        await provider.emit(
            ProviderEvent(
                type="model_response_started",
                payload={"response": {"id": "resp_browser_audio_hold_provider"}},
            )
        )
        await provider.emit(
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_browser_audio_hold_provider",
                    "delta": base64.b64encode(output_pcm).decode("ascii"),
                },
            )
        )
        await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)

        accepted = await runner.record_browser_speech_segment(
            "call_browser_audio_hold_provider",
            datetime.now(timezone.utc),
            {
                "segmentId": "browser-seg-audio-hold-provider",
                "phase": "updated",
                "durationMs": 281,
                "rmsDbfs": -18.0,
                "noiseFloorDbfs": -45.0,
                "snrDb": 27.0,
                "hotFrameCount": 7,
                "remoteAudioActive": True,
                "remoteAudioRmsDbfs": -42.0,
            },
        )
        await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
        await _wait_until(lambda: provider.cancelled_response_count == 1)
        await provider.close_events()
        await runner.wait("call_browser_audio_hold_provider")

        event_types = [event.type for event in store.list("call_browser_audio_hold_provider")]
        assert accepted is True
        assert provider.cancelled_response_count == 1
        assert publisher.stopped_call_ids == ["call_browser_audio_hold_provider"]
        assert "browser_audio_hold_requested" in event_types
        assert "browser_audio_hold_completed" in event_types
        assert "browser_audio_hold_confirmed" in event_types
        assert "response_generation_invalidated" in event_types
        assert "interrupt_audio_stop_requested" in event_types
        assert "interrupt_confirmed" not in event_types
    finally:
        await runner.stop("call_browser_audio_hold_provider")


@pytest.mark.anyio
async def test_realtime_agent_runner_pre_stops_clear_browser_speech_segment_without_cancelling_response() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_browser_pre_stop",
        room_name="ai-call-call_browser_pre_stop",
        participant_identity="browser-call_browser_pre_stop",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    try:
        await runner.start(session)
        await provider.emit(
            ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
        )
        await provider.emit(
            ProviderEvent(
                type="model_response_started",
                payload={"response": {"id": "resp_browser_pre_stop"}},
            )
        )
        await provider.emit(
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_browser_pre_stop",
                    "delta": base64.b64encode(output_pcm).decode("ascii"),
                },
            )
        )
        await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)

        accepted = await runner.record_browser_speech_segment(
            "call_browser_pre_stop",
            datetime.now(timezone.utc),
            {
                "segmentId": "browser-seg-pre-stop",
                "phase": "updated",
                "durationMs": 520,
                "rmsDbfs": -14.7,
                "noiseFloorDbfs": -45.2,
                "snrDb": 30.5,
                "hotFrameCount": 12,
                "remoteAudioActive": True,
                "remoteAudioRmsDbfs": -15.7,
            },
        )
        await provider.emit(
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_browser_pre_stop",
                    "delta": base64.b64encode(output_pcm).decode("ascii"),
                },
            )
        )
        await provider.emit(
            ProviderEvent(
                type="model_response_done",
                payload={"response": {"id": "resp_browser_pre_stop", "status": "completed"}},
            )
        )
        await provider.close_events()
        await runner.wait("call_browser_pre_stop")

        event_types = [event.type for event in store.list("call_browser_pre_stop")]
        assert accepted is True
        assert provider.cancelled_response_count == 0
        assert publisher.stopped_call_ids == ["call_browser_pre_stop"]
        assert len(publisher.published) == 1
        assert "browser_interrupt_candidate_promoted" in event_types
        assert "browser_pre_stop_requested" in event_types
        assert "browser_pre_stop_completed" in event_types
        assert "stale_audio_dropped" in event_types
        assert "response_generation_invalidated" not in event_types
        assert "interrupt_audio_stop_requested" not in event_types
        assert "interrupt_confirmed" not in event_types
    finally:
        await runner.stop("call_browser_pre_stop")


@pytest.mark.anyio
async def test_realtime_agent_runner_provider_speech_confirms_browser_pre_stop() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_browser_pre_stop_provider",
        room_name="ai-call-call_browser_pre_stop_provider",
        participant_identity="browser-call_browser_pre_stop_provider",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    try:
        await runner.start(session)
        await provider.emit(
            ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
        )
        await provider.emit(
            ProviderEvent(
                type="model_response_started",
                payload={"response": {"id": "resp_browser_pre_stop_provider"}},
            )
        )
        await provider.emit(
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_browser_pre_stop_provider",
                    "delta": base64.b64encode(output_pcm).decode("ascii"),
                },
            )
        )
        await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)

        accepted = await runner.record_browser_speech_segment(
            "call_browser_pre_stop_provider",
            datetime.now(timezone.utc),
            {
                "segmentId": "browser-seg-pre-stop-provider",
                "phase": "updated",
                "durationMs": 520,
                "rmsDbfs": -14.7,
                "noiseFloorDbfs": -45.2,
                "snrDb": 30.5,
                "hotFrameCount": 12,
                "remoteAudioActive": True,
                "remoteAudioRmsDbfs": -15.7,
            },
        )
        await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
        await _wait_until(lambda: provider.cancelled_response_count == 1)
        await provider.close_events()
        await runner.wait("call_browser_pre_stop_provider")

        event_types = [event.type for event in store.list("call_browser_pre_stop_provider")]
        assert accepted is True
        assert provider.cancelled_response_count == 1
        assert publisher.stopped_call_ids == ["call_browser_pre_stop_provider"]
        assert "browser_pre_stop_requested" in event_types
        assert "browser_pre_stop_completed" in event_types
        assert "browser_pre_stop_confirmed" in event_types
        assert "response_generation_invalidated" in event_types
        assert "interrupt_audio_stop_requested" in event_types
        assert "interrupt_confirmed" not in event_types
    finally:
        await runner.stop("call_browser_pre_stop_provider")


@pytest.mark.anyio
async def test_realtime_agent_runner_rejects_browser_pre_stop_when_remote_audio_dominates() -> (
    None
):
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_browser_pre_stop_echo",
        room_name="ai-call-call_browser_pre_stop_echo",
        participant_identity="browser-call_browser_pre_stop_echo",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    try:
        await runner.start(session)
        await provider.emit(
            ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
        )
        await provider.emit(
            ProviderEvent(
                type="model_response_started",
                payload={"response": {"id": "resp_browser_pre_stop_echo"}},
            )
        )
        await provider.emit(
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_browser_pre_stop_echo",
                    "delta": base64.b64encode(output_pcm).decode("ascii"),
                },
            )
        )
        await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)

        accepted = await runner.record_browser_speech_segment(
            "call_browser_pre_stop_echo",
            datetime.now(timezone.utc),
            {
                "segmentId": "browser-seg-pre-stop-echo",
                "phase": "updated",
                "durationMs": 520,
                "rmsDbfs": -28.0,
                "noiseFloorDbfs": -58.0,
                "snrDb": 30.0,
                "hotFrameCount": 12,
                "remoteAudioActive": True,
                "remoteAudioRmsDbfs": -12.0,
            },
        )
        await provider.close_events()
        await runner.wait("call_browser_pre_stop_echo")

        event_types = [event.type for event in store.list("call_browser_pre_stop_echo")]
        assert accepted is True
        assert provider.cancelled_response_count == 0
        assert publisher.stopped_call_ids == []
        assert "browser_interrupt_candidate_promoted" in event_types
        assert "browser_pre_stop_rejected_echo" in event_types
        assert "browser_audio_hold_rejected_echo" in event_types
        assert "browser_pre_stop_requested" not in event_types
        assert "browser_audio_hold_requested" not in event_types
        assert "interrupt_audio_stop_requested" not in event_types
    finally:
        await runner.stop("call_browser_pre_stop_echo")


@pytest.mark.anyio
async def test_realtime_agent_runner_expires_unconfirmed_browser_pre_stop() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_browser_pre_stop_expired",
        room_name="ai-call-call_browser_pre_stop_expired",
        participant_identity="browser-call_browser_pre_stop_expired",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        browser_pre_stop_timeout_seconds=0.03,
    )

    try:
        await runner.start(session)
        await provider.emit(
            ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
        )
        await provider.emit(
            ProviderEvent(
                type="model_response_started",
                payload={"response": {"id": "resp_browser_pre_stop_expired"}},
            )
        )
        await provider.emit(
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_browser_pre_stop_expired",
                    "delta": base64.b64encode(output_pcm).decode("ascii"),
                },
            )
        )
        await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)

        accepted = await runner.record_browser_speech_segment(
            "call_browser_pre_stop_expired",
            datetime.now(timezone.utc),
            {
                "segmentId": "browser-seg-pre-stop-expired",
                "phase": "updated",
                "durationMs": 520,
                "rmsDbfs": -14.7,
                "noiseFloorDbfs": -45.2,
                "snrDb": 30.5,
                "hotFrameCount": 12,
                "remoteAudioActive": True,
                "remoteAudioRmsDbfs": -15.7,
            },
        )
        await asyncio.sleep(0.05)
        await provider.close_events()
        await runner.wait("call_browser_pre_stop_expired")

        event_types = [event.type for event in store.list("call_browser_pre_stop_expired")]
        assert accepted is True
        assert provider.cancelled_response_count == 0
        assert publisher.stopped_call_ids == ["call_browser_pre_stop_expired"]
        assert "browser_pre_stop_requested" in event_types
        assert "browser_pre_stop_completed" in event_types
        assert "browser_pre_stop_expired" in event_types
        assert "interrupt_ignored" in event_types
        assert "browser_pre_stop_confirmed" not in event_types
        assert "interrupt_audio_stop_requested" not in event_types
    finally:
        await runner.stop("call_browser_pre_stop_expired")


@pytest.mark.anyio
async def test_realtime_agent_runner_upgrades_browser_candidate_on_provider_speech() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_browser_candidate_provider_upgrade",
        room_name="ai-call-call_browser_candidate_provider_upgrade",
        participant_identity="browser-call_browser_candidate_provider_upgrade",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_provider_upgrade"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_provider_upgrade",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
    )
    await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)

    accepted = await runner.record_browser_speech_candidate(
        "call_browser_candidate_provider_upgrade",
        datetime.now(timezone.utc),
    )
    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await _wait_until(lambda: provider.cancelled_response_count == 1)
    await provider.close_events()
    await runner.wait("call_browser_candidate_provider_upgrade")

    event_types = [event.type for event in store.list("call_browser_candidate_provider_upgrade")]
    assert accepted is True
    assert provider.cancelled_response_count == 1
    assert publisher.stopped_call_ids == ["call_browser_candidate_provider_upgrade"]
    assert event_types.count("interrupt_candidate") == 1
    assert "browser_interrupt_candidate_deferred" in event_types
    assert "response_generation_invalidated" in event_types
    assert "interrupt_audio_stop_requested" in event_types
    assert "interrupt_confirmed" not in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_provider_speech_upgrades_recently_expired_browser_candidate() -> (
    None
):
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    session = CallSession(
        call_id="call_recently_expired_browser_candidate",
        room_name="ai-call-call_recently_expired_browser_candidate",
        participant_identity="browser-call_recently_expired_browser_candidate",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    await runner.start(session)
    registry.transition("call_recently_expired_browser_candidate", CallSessionStatus.CONNECTED)
    registry.transition("call_recently_expired_browser_candidate", CallSessionStatus.AI_SPEAKING)
    accepted = await runner.record_browser_speech_candidate(
        "call_recently_expired_browser_candidate",
        datetime.now(timezone.utc) - timedelta(seconds=1.6),
    )
    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.close_events()
    await runner.wait("call_recently_expired_browser_candidate")

    event_types = [event.type for event in store.list("call_recently_expired_browser_candidate")]
    assert accepted is True
    assert provider.cancelled_response_count == 1
    assert publisher.stopped_call_ids == ["call_recently_expired_browser_candidate"]
    assert "interrupt_ignored" in event_types
    assert event_types.count("interrupt_candidate") == 2
    assert "response_generation_invalidated" in event_types
    assert "interrupt_audio_stop_requested" in event_types
    assert "interrupt_confirmed" not in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_defers_browser_interrupt_while_ai_speaking() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = FakeRealtimeProvider([])
    publisher = FakeAudioPublisher()
    session = CallSession(
        call_id="call_browser_interrupt",
        room_name="ai-call-call_browser_interrupt",
        participant_identity="browser-call_browser_interrupt",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    await runner.start(session)
    registry.transition("call_browser_interrupt", CallSessionStatus.CONNECTED)
    registry.transition("call_browser_interrupt", CallSessionStatus.AI_SPEAKING)
    accepted = await runner.record_browser_speech_candidate(
        "call_browser_interrupt",
        datetime(2026, 6, 15, 9, 0, tzinfo=timezone.utc),
    )

    assert accepted is True
    assert provider.cancelled_response_count == 0
    assert provider.cleared_input_count == 0
    assert publisher.stopped_call_ids == []
    assert registry.get("call_browser_interrupt").status == CallSessionStatus.AI_SPEAKING
    assert "interrupt_candidate" in [event.type for event in store.list("call_browser_interrupt")]
    assert "interrupt_confirmed" not in [
        event.type for event in store.list("call_browser_interrupt")
    ]


@pytest.mark.anyio
async def test_realtime_agent_runner_skips_browser_interrupt_when_barge_in_disabled() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = FakeRealtimeProvider([])
    session = CallSession(
        call_id="call_browser_barge_disabled",
        room_name="ai-call-call_browser_barge_disabled",
        participant_identity="browser-call_browser_barge_disabled",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
            "barge_in_enabled": False,
        },
    )
    registry.add(session)
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
    )

    await runner.start(session)
    registry.transition("call_browser_barge_disabled", CallSessionStatus.CONNECTED)
    registry.transition("call_browser_barge_disabled", CallSessionStatus.AI_SPEAKING)
    accepted = await runner.record_browser_speech_candidate(
        "call_browser_barge_disabled",
        datetime(2026, 6, 15, 9, 0, tzinfo=timezone.utc),
    )

    event_types = [event.type for event in store.list("call_browser_barge_disabled")]
    assert accepted is False
    assert "interrupt_candidate" not in event_types
    assert "browser_interrupt_candidate_deferred" not in event_types
    await runner.stop("call_browser_barge_disabled")


@pytest.mark.anyio
async def test_realtime_agent_runner_skips_provider_interrupt_when_barge_in_disabled() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_provider_barge_disabled",
        room_name="ai-call-call_provider_barge_disabled",
        participant_identity="sip-call_provider_barge_disabled",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
            "barge_in_enabled": False,
        },
    )
    registry.add(session)
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_no_barge"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_no_barge",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
    )
    await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)

    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.close_events()
    await runner.wait("call_provider_barge_disabled")

    event_types = [event.type for event in store.list("call_provider_barge_disabled")]
    assert "user_speech_started" in event_types
    assert "interrupt_candidate" not in event_types
    assert "response_generation_invalidated" not in event_types
    assert "interrupt_confirmed" not in event_types
    assert provider.cancelled_response_count == 0
    assert publisher.stopped_call_ids == []


@pytest.mark.anyio
async def test_realtime_agent_runner_defers_browser_interrupt_before_first_audio() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    session = CallSession(
        call_id="call_browser_pre_audio_interrupt",
        room_name="ai-call-call_browser_pre_audio_interrupt",
        participant_identity="browser-call_browser_pre_audio_interrupt",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "opening_message": "您好，我是灵宸智能助手。",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await asyncio.wait_for(
        _wait_until(
            lambda: (
                registry.get("call_browser_pre_audio_interrupt").status
                == CallSessionStatus.CONNECTED
            )
        ),
        timeout=1,
    )
    await runner.start_opening("call_browser_pre_audio_interrupt")
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_pre_audio"}},
        )
    )
    accepted = await runner.record_browser_speech_candidate(
        "call_browser_pre_audio_interrupt",
        datetime.now(timezone.utc),
    )
    await provider.close_events()
    await runner.wait("call_browser_pre_audio_interrupt")

    assert accepted is True
    assert provider.cancelled_response_count == 0
    assert publisher.stopped_call_ids == []
    event_types = [event.type for event in store.list("call_browser_pre_audio_interrupt")]
    assert "interrupt_candidate" in event_types
    assert "browser_interrupt_candidate_deferred" in event_types
    assert "response_generation_invalidated" not in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_cancels_active_response_when_user_speaks_before_audio() -> (
    None
):
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    session = CallSession(
        call_id="call_provider_pre_audio_interrupt",
        room_name="ai-call-call_provider_pre_audio_interrupt",
        participant_identity="browser-call_provider_pre_audio_interrupt",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "opening_message": "您好，我是灵宸智能助手。",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        user_turn_stability_delay_seconds=0.05,
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await runner.start_opening("call_provider_pre_audio_interrupt")
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_pre_audio"}},
        )
    )
    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.emit(ProviderEvent(type="user_transcript_delta", payload={"delta": "你好"}))
    await provider.emit(ProviderEvent(type="user_speech_stopped", payload={}))
    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_pre_audio", "status": "cancelled"}},
        )
    )
    await asyncio.sleep(0.06)
    await provider.close_events()
    await runner.wait("call_provider_pre_audio_interrupt")

    assert provider.cancelled_response_count == 1
    assert provider.created_responses == ["请主动说出开场白：您好，我是灵宸智能助手。", None]
    assert publisher.stopped_call_ids == ["call_provider_pre_audio_interrupt"]
    event_types = [event.type for event in store.list("call_provider_pre_audio_interrupt")]
    assert "interrupt_candidate" in event_types
    assert "interrupt_confirmed" in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_expires_stale_browser_candidate_before_late_speech() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    session = CallSession(
        call_id="call_stale_browser_candidate",
        room_name="ai-call-call_stale_browser_candidate",
        participant_identity="browser-call_stale_browser_candidate",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        user_turn_stability_delay_seconds=0.05,
    )

    await runner.start(session)
    registry.transition("call_stale_browser_candidate", CallSessionStatus.CONNECTED)
    registry.transition("call_stale_browser_candidate", CallSessionStatus.AI_SPEAKING)
    accepted = await runner.record_browser_speech_candidate(
        "call_stale_browser_candidate",
        datetime.now(timezone.utc) - timedelta(seconds=3),
    )
    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.emit(ProviderEvent(type="user_transcript_delta", payload={"delta": "你好"}))
    await provider.emit(ProviderEvent(type="user_speech_stopped", payload={}))
    await asyncio.sleep(0.06)
    await provider.close_events()
    await runner.wait("call_stale_browser_candidate")

    event_types = [event.type for event in store.list("call_stale_browser_candidate")]
    assert accepted is True
    assert provider.cancelled_response_count == 0
    assert provider.created_responses == [None]
    assert publisher.stopped_call_ids == []
    assert "interrupt_ignored" in event_types
    assert "interrupt_confirmed" not in event_types
    assert registry.get("call_stale_browser_candidate").status == CallSessionStatus.AI_THINKING


@pytest.mark.anyio
async def test_realtime_agent_runner_expires_stale_browser_speech_segment_before_late_speech() -> (
    None
):
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    session = CallSession(
        call_id="call_stale_browser_segment",
        room_name="ai-call-call_stale_browser_segment",
        participant_identity="browser-call_stale_browser_segment",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        user_turn_stability_delay_seconds=0.05,
    )

    await runner.start(session)
    registry.transition("call_stale_browser_segment", CallSessionStatus.CONNECTED)
    registry.transition("call_stale_browser_segment", CallSessionStatus.AI_SPEAKING)
    accepted = await runner.record_browser_speech_segment(
        "call_stale_browser_segment",
        datetime.now(timezone.utc) - timedelta(seconds=3),
        {
            "segmentId": "browser-seg-stale",
            "phase": "started",
            "durationMs": 120,
            "snrDb": 14.0,
            "hotFrameCount": 3,
            "remoteAudioActive": True,
        },
    )
    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.emit(ProviderEvent(type="user_transcript_delta", payload={"delta": "你好"}))
    await provider.emit(ProviderEvent(type="user_speech_stopped", payload={}))
    await asyncio.sleep(0.06)
    await provider.close_events()
    await runner.wait("call_stale_browser_segment")

    event_types = [event.type for event in store.list("call_stale_browser_segment")]
    assert accepted is True
    assert provider.cancelled_response_count == 0
    assert provider.created_responses == [None]
    assert publisher.stopped_call_ids == []
    assert "interrupt_ignored" in event_types
    assert "interrupt_confirmed" not in event_types
    assert registry.get("call_stale_browser_segment").status == CallSessionStatus.AI_THINKING


@pytest.mark.anyio
async def test_realtime_agent_runner_ignores_late_provider_speech_after_stale_browser_candidate() -> (
    None
):
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    session = CallSession(
        call_id="call_late_provider_after_noise",
        room_name="ai-call-call_late_provider_after_noise",
        participant_identity="browser-call_late_provider_after_noise",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    await runner.start(session)
    registry.transition("call_late_provider_after_noise", CallSessionStatus.CONNECTED)
    registry.transition("call_late_provider_after_noise", CallSessionStatus.AI_SPEAKING)
    accepted = await runner.record_browser_speech_candidate(
        "call_late_provider_after_noise",
        datetime.now(timezone.utc) - timedelta(seconds=3),
    )
    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.close_events()
    await runner.wait("call_late_provider_after_noise")

    event_types = [event.type for event in store.list("call_late_provider_after_noise")]
    assert accepted is True
    assert provider.cancelled_response_count == 0
    assert publisher.stopped_call_ids == []
    assert "interrupt_ignored" in event_types
    assert "interrupt_confirmed" not in event_types
    assert "session_failed" not in event_types
    assert registry.get("call_late_provider_after_noise").status == CallSessionStatus.CONNECTED


@pytest.mark.anyio
async def test_realtime_agent_runner_does_not_confirm_browser_interrupt_after_recent_ai_audio() -> (
    None
):
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_recent_audio_interrupt",
        room_name="ai-call-call_recent_audio_interrupt",
        participant_identity="browser-call_recent_audio_interrupt",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={"delta": base64.b64encode(output_pcm).decode("ascii")},
        )
    )
    await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)
    registry.transition("call_recent_audio_interrupt", CallSessionStatus.CONNECTED)

    accepted = await runner.record_browser_speech_candidate(
        "call_recent_audio_interrupt",
        datetime.now(timezone.utc),
    )
    await provider.close_events()
    await runner.wait("call_recent_audio_interrupt")

    assert accepted is True
    assert provider.cancelled_response_count == 0
    assert provider.cleared_input_count == 0
    assert publisher.stopped_call_ids == []
    assert registry.get("call_recent_audio_interrupt").status == CallSessionStatus.AI_SPEAKING
    assert "interrupt_candidate" in [
        event.type for event in store.list("call_recent_audio_interrupt")
    ]
    assert "interrupt_confirmed" not in [
        event.type for event in store.list("call_recent_audio_interrupt")
    ]


@pytest.mark.anyio
async def test_realtime_agent_runner_confirms_browser_candidate_after_provider_transcript() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    session = CallSession(
        call_id="call_browser_text_alone",
        room_name="ai-call-call_browser_text_alone",
        participant_identity="browser-call_browser_text_alone",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    await runner.start(session)
    registry.transition("call_browser_text_alone", CallSessionStatus.CONNECTED)
    registry.transition("call_browser_text_alone", CallSessionStatus.AI_SPEAKING)
    accepted = await runner.record_browser_speech_candidate(
        "call_browser_text_alone",
        datetime.now(timezone.utc),
    )
    await provider.emit(
        ProviderEvent(type="user_transcript_done", payload={"transcript": "明天再说。"})
    )
    await provider.close_events()
    await runner.wait("call_browser_text_alone")

    assert accepted is True
    assert provider.cancelled_response_count == 1
    assert publisher.stopped_call_ids
    assert set(publisher.stopped_call_ids) == {"call_browser_text_alone"}
    assert "interrupt_confirmed" in [event.type for event in store.list("call_browser_text_alone")]


@pytest.mark.anyio
async def test_realtime_agent_runner_recovers_from_stop_audio_failure_during_interrupt() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FailingStopAudioPublisher()
    session = CallSession(
        call_id="call_stop_audio_failure_interrupt",
        room_name="ai-call-call_stop_audio_failure_interrupt",
        participant_identity="browser-call_stop_audio_failure_interrupt",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    await runner.start(session)
    registry.transition("call_stop_audio_failure_interrupt", CallSessionStatus.CONNECTED)
    registry.transition("call_stop_audio_failure_interrupt", CallSessionStatus.AI_SPEAKING)
    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.emit(ProviderEvent(type="user_transcript_delta", payload={"delta": "你好"}))
    await provider.emit(ProviderEvent(type="user_speech_stopped", payload={}))
    await provider.close_events()
    await runner.wait("call_stop_audio_failure_interrupt")

    event_types = [event.type for event in store.list("call_stop_audio_failure_interrupt")]
    assert publisher.stopped_call_ids
    assert set(publisher.stopped_call_ids) == {"call_stop_audio_failure_interrupt"}
    assert provider.cancelled_response_count == 1
    assert provider.cleared_input_count == 0
    assert provider.created_responses == [None]
    assert "interrupt_cleanup_failed" in event_types
    assert "interrupt_candidate" in event_types
    assert "interrupt_confirmed" in event_types
    assert registry.get("call_stop_audio_failure_interrupt").status == CallSessionStatus.AI_THINKING


@pytest.mark.anyio
async def test_realtime_agent_runner_ignores_stale_audio_after_confirmed_interrupt() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_stale_audio",
        room_name="ai-call-call_stale_audio",
        participant_identity="browser-call_stale_audio",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={"delta": base64.b64encode(output_pcm).decode("ascii")},
        )
    )
    await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)

    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.emit(ProviderEvent(type="user_transcript_delta", payload={"delta": "你好"}))
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={"delta": base64.b64encode(output_pcm).decode("ascii")},
        )
    )
    await provider.close_events()
    await runner.wait("call_stale_audio")

    assert provider.cancelled_response_count == 1
    assert len(publisher.published) == 1
    assert "interrupt_confirmed" in [event.type for event in store.list("call_stale_audio")]
    assert [event.type for event in store.list("call_stale_audio")].count("ai_audio_published") == 1


@pytest.mark.anyio
async def test_realtime_agent_runner_blocks_unidentified_audio_until_new_response_starts_after_interrupt() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_unidentified_audio_after_interrupt",
        room_name="ai-call-call_unidentified_audio_after_interrupt",
        participant_identity="browser-call_unidentified_audio_after_interrupt",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        user_turn_stability_delay_seconds=0.05,
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(
        ProviderEvent(type="model_response_started", payload={"response": {"id": "resp_old"}})
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_old",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
    )
    await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)

    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.emit(ProviderEvent(type="user_transcript_delta", payload={"delta": "你好"}))
    await provider.emit(ProviderEvent(type="user_speech_stopped", payload={}))
    await asyncio.wait_for(_wait_until(lambda: provider.cancelled_response_count == 1), timeout=1)
    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_old", "status": "cancelled"}},
        )
    )
    await asyncio.sleep(0.06)
    await asyncio.wait_for(_wait_until(lambda: provider.created_responses == [None]), timeout=1)

    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={"delta": base64.b64encode(output_pcm).decode("ascii")},
        )
    )
    await provider.emit(
        ProviderEvent(type="model_response_started", payload={"response": {"id": "resp_new"}})
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_new",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
    )
    await provider.close_events()
    await runner.wait("call_unidentified_audio_after_interrupt")

    events = store.list("call_unidentified_audio_after_interrupt")
    assert len(publisher.published) == 2
    assert [event.type for event in events].count("ai_audio_published") == 2
    assert any(
        event.type == "stale_audio_dropped"
        and event.payload.get("reason") == "awaiting_response_start_after_interrupt"
        for event in events
    )


@pytest.mark.anyio
async def test_realtime_agent_runner_keeps_new_response_generation_after_duplicate_speech_start() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = FakeRealtimeProvider([])
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    call_id = "call_duplicate_speech_start_generation"
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"sip-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        user_turn_stability_delay_seconds=0,
    )

    try:
        await runner.start(session)
        registry.transition(call_id, CallSessionStatus.CONNECTED)
        registry.transition(call_id, CallSessionStatus.AI_THINKING)
        registry.transition(call_id, CallSessionStatus.AI_SPEAKING)
        runner._mark_response_started(call_id, {"response": {"id": "resp_old"}})
        runner._response_lifecycle(call_id).active = True
        await runner._publish_model_audio_delta(
            call_id,
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_old",
                    "delta": base64.b64encode(output_pcm).decode("ascii"),
                },
            ),
        )

        first_speech_at = datetime.now(timezone.utc)
        await runner._handle_user_speech_started(call_id, provider, first_speech_at)
        await runner._handle_user_transcript(
            call_id,
            provider,
            ProviderEvent(type="user_transcript_done", payload={"transcript": "是的。"}),
            first_speech_at + timedelta(milliseconds=200),
        )
        await runner._handle_user_speech_stopped(
            call_id,
            provider,
            first_speech_at + timedelta(milliseconds=260),
        )
        await runner._complete_response_and_flush_pending(call_id, provider)
        assert provider.created_responses == [None]

        await runner._handle_user_speech_started(
            call_id,
            provider,
            first_speech_at + timedelta(milliseconds=650),
        )
        await runner._apply_provider_event(
            call_id,
            provider,
            "model_response_started",
            first_speech_at + timedelta(milliseconds=700),
            {"response": {"id": "resp_new"}},
        )
        audio_event = ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_new",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
        await runner._apply_provider_event(
            call_id,
            provider,
            "model_audio_delta",
            first_speech_at + timedelta(milliseconds=720),
            audio_event.payload,
        )
        await runner._publish_model_audio_delta(
            call_id,
            audio_event,
        )

        events = store.list(call_id)
        event_types = [event.type for event in events]
        assert len(publisher.published) == 2
        assert "sip_provider_speech_started_ignored" in event_types
        assert not any(
            event.type == "response_generation_invalidated"
            and event.payload.get("reason") == "stale_response_started"
            and event.payload.get("responseId") == "resp_new"
            for event in events
        )
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_queues_response_until_cancelled_response_done() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = FakeAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_single_flight_response",
        room_name="ai-call-call_single_flight_response",
        participant_identity="browser-call_single_flight_response",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        user_turn_stability_delay_seconds=0.05,
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(
        ProviderEvent(type="model_response_started", payload={"response_id": "resp_1"})
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={"delta": base64.b64encode(output_pcm).decode("ascii")},
        )
    )
    await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)

    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.emit(ProviderEvent(type="user_transcript_delta", payload={"delta": "你好"}))
    await provider.emit(ProviderEvent(type="user_speech_stopped", payload={}))
    await asyncio.wait_for(
        _wait_until(lambda: provider.cancelled_response_count == 1),
        timeout=1,
    )

    assert provider.created_responses == []

    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_1", "status": "cancelled"}},
        )
    )
    await asyncio.sleep(0.06)
    assert provider.created_responses == [None]
    await provider.close_events()
    await runner.wait("call_single_flight_response")

    assert provider.cancelled_response_count == 1
    assert "interrupt_confirmed" in [
        event.type for event in store.list("call_single_flight_response")
    ]


@pytest.mark.anyio
async def test_realtime_agent_runner_waits_for_stable_user_turn_before_response() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    session = CallSession(
        call_id="call_stable_user_turn",
        room_name="ai-call-call_stable_user_turn",
        participant_identity="browser-call_stable_user_turn",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        user_turn_stability_delay_seconds=0.05,
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.emit(ProviderEvent(type="user_transcript_delta", payload={"delta": "嗯。"}))
    await provider.emit(ProviderEvent(type="user_speech_stopped", payload={}))
    await asyncio.sleep(0.01)
    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await asyncio.sleep(0.06)
    assert provider.created_responses == []

    await provider.emit(ProviderEvent(type="user_transcript_delta", payload={"delta": "你是谁？"}))
    await provider.emit(ProviderEvent(type="user_speech_stopped", payload={}))
    await asyncio.sleep(0.06)
    assert provider.created_responses == [None]
    await provider.close_events()
    await runner.wait("call_stable_user_turn")

    assert provider.created_responses == [None]


@pytest.mark.anyio
async def test_realtime_agent_runner_keeps_inflight_audio_for_browser_candidate() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = BlockingAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_browser_inflight_audio",
        room_name="ai-call-call_browser_inflight_audio",
        participant_identity="browser-call_browser_inflight_audio",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(
        ProviderEvent(
            type="model_response_started",
            payload={"response": {"id": "resp_inflight"}},
        )
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={
                "response_id": "resp_inflight",
                "delta": base64.b64encode(output_pcm).decode("ascii"),
            },
        )
    )
    await asyncio.wait_for(publisher.publish_started.wait(), timeout=1)

    accepted = await runner.record_browser_speech_candidate(
        "call_browser_inflight_audio",
        datetime.now(timezone.utc),
    )
    publisher.publish_release.set()
    await provider.emit(
        ProviderEvent(
            type="model_response_done",
            payload={"response": {"id": "resp_inflight", "status": "completed"}},
        )
    )
    await provider.close_events()
    await runner.wait("call_browser_inflight_audio")

    assert accepted is True
    assert provider.cancelled_response_count == 0
    assert len(publisher.published) == 1
    assert publisher.stopped_call_ids == []
    event_types = [event.type for event in store.list("call_browser_inflight_audio")]
    assert "browser_interrupt_candidate_deferred" in event_types
    assert "stale_audio_dropped" not in event_types
    assert "playout_queue_flushed" not in event_types
    assert event_types.count("ai_audio_published") == 1


@pytest.mark.anyio
async def test_realtime_agent_runner_confirms_interrupt_after_inflight_audio_publishes() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = QueueRealtimeProvider()
    publisher = BlockingAudioPublisher()
    output_pcm = b"\x01\x02" * 240
    session = CallSession(
        call_id="call_inflight_audio",
        room_name="ai-call-call_inflight_audio",
        participant_identity="browser-call_inflight_audio",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    await runner.start(session)
    await provider.emit(
        ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"})
    )
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={"delta": base64.b64encode(output_pcm).decode("ascii")},
        )
    )
    await asyncio.wait_for(publisher.publish_started.wait(), timeout=1)

    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.emit(ProviderEvent(type="user_transcript_delta", payload={"delta": "你好"}))
    publisher.publish_release.set()
    await provider.close_events()
    await runner.wait("call_inflight_audio")

    assert provider.cancelled_response_count == 1
    assert len(publisher.published) == 1
    assert publisher.stopped_call_ids
    assert set(publisher.stopped_call_ids) == {"call_inflight_audio"}
    event_types = [event.type for event in store.list("call_inflight_audio")]
    assert event_types.count("ai_audio_published") == 1
    assert "interrupt_confirmed" in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_stop_closes_provider() -> None:
    registry = InMemorySessionRegistry()
    provider = FakeRealtimeProvider([])
    session = CallSession(
        call_id="call_2",
        room_name="ai-call-call_2",
        participant_identity="browser-call_2",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=InMemoryEventStore(),
    )

    await runner.start(session)
    await runner.stop("call_2")

    assert provider.closed is True


@pytest.mark.anyio
async def test_realtime_agent_runner_sends_normalized_audio_frames_to_provider() -> None:
    registry = InMemorySessionRegistry()
    provider = FakeRealtimeProvider([])
    session = CallSession(
        call_id="call_3",
        room_name="ai-call-call_3",
        participant_identity="browser-call_3",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)
    input_samples = list(range(960))
    input_pcm = struct.pack("<" + "h" * len(input_samples), *input_samples)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=InMemoryEventStore(),
    )

    await runner.start(session)
    await runner.send_audio_frame(
        "call_3",
        PcmAudioFrame(
            data=input_pcm,
            sample_rate_hz=48000,
            channels=1,
            sample_width_bytes=2,
        ),
    )

    assert len(provider.sent_audio) == 1
    assert len(provider.sent_audio[0]) == 640


@pytest.mark.anyio
async def test_realtime_agent_runner_forwards_room_audio_frames_to_provider() -> None:
    registry = InMemorySessionRegistry()
    provider = FakeRealtimeProvider([])
    input_samples = list(range(960))
    input_pcm = struct.pack("<" + "h" * len(input_samples), *input_samples)
    transport = FakeRoomAudioTransport([
        PcmAudioFrame(
            data=input_pcm,
            sample_rate_hz=48000,
            channels=1,
            sample_width_bytes=2,
        )
    ])
    session = CallSession(
        call_id="call_room_audio",
        room_name="ai-call-call_room_audio",
        participant_identity="browser-call_room_audio",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=InMemoryEventStore(),
        audio_transport=transport,
    )

    await runner.start(session)
    await runner.wait("call_room_audio")
    await runner.stop("call_room_audio")

    assert transport.receive_call_ids == ["call_room_audio"]
    assert transport.closed_call_ids == ["call_room_audio"]
    assert len(provider.sent_audio) == 1
    assert len(provider.sent_audio[0]) == 640


@pytest.mark.anyio
async def test_realtime_agent_runner_pre_stops_sip_candidate_before_provider_vad() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    registry = InMemorySessionRegistry()
    provider = FakeRealtimeProvider([])
    store = InMemoryEventStore()
    publisher = FakeAudioPublisher()
    call_id = "call_sip_barge_in"
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"sip-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        sip_barge_in_enabled=True,
        sip_barge_in_fast_stop_enabled=True,
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
        ),
        sip_barge_in_vad=FakeVad([True] * 20),
    )
    deferred_frame = _pcm16_constant_frame(amplitude=4000)
    speech_frames = _pcm16_constant_frames([
        3200,
        5200,
        3900,
        6100,
        4400,
        5700,
        4700,
        6200,
        4100,
        5900,
        4800,
        6100,
        5200,
        5700,
        5000,
    ])
    output_pcm = b"\x01\x02" * 240

    try:
        await runner.start(session)
        registry.transition(call_id, CallSessionStatus.CONNECTED)
        registry.transition(call_id, CallSessionStatus.AI_THINKING)
        registry.transition(call_id, CallSessionStatus.AI_SPEAKING)
        runner._mark_response_started(
            call_id,
            {"response_id": "resp_sip_opening"},
            datetime.now(timezone.utc),
        )
        runner._response_lifecycle(call_id).active = True
        runner._playback_guard(call_id).current_response_audio_published = True

        for _ in range(9):
            await runner.send_audio_frame(call_id, deferred_frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        sip_candidate = next(event for event in events if event.type == "sip_interrupt_candidate")

        assert len(provider.sent_audio) == 9
        assert provider.cancelled_response_count == 0
        assert publisher.stopped_call_ids == []
        assert sip_candidate.payload["reason"] == "sip_uplink_speech_during_ai_audio"
        assert sip_candidate.payload["candidateClass"] == "stable_speech_candidate"
        assert sip_candidate.payload["vadVoicedMs"] >= 120
        assert sip_candidate.payload["candidateDurationMs"] >= 180
        assert "interrupt_candidate" in event_types
        assert "sip_interrupt_candidate" in event_types
        assert "sip_pre_stop_deferred" in event_types
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert "interrupt_audio_stop_requested" not in event_types
        assert "playout_queue_flushed" not in event_types
        assert "interrupt_audio_stop_completed" not in event_types
        assert "interrupt_confirmed" not in event_types

        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        pre_stop = next(event for event in events if event.type == "sip_pre_stop")
        assert len(provider.sent_audio) == 24
        assert publisher.stopped_call_ids == [call_id]
        assert pre_stop.payload["candidateClass"] == "stable_speech_candidate"
        assert pre_stop.payload["candidateDurationMs"] >= 360
        assert pre_stop.payload["sipPreStopAuthorityEvidence"] == "stable_local_speech"
        assert "sip_pre_stop" in event_types
        assert "response_generation_invalidated" in event_types
        assert "interrupt_audio_stop_requested" in event_types
        assert "playout_queue_flushed" in event_types
        assert "interrupt_audio_stop_completed" in event_types
        assert "interrupt_confirmed" not in event_types
        guard = runner._playback_guard(call_id)
        assert guard.user_speech_active is False
        assert guard.suppress_audio_until is not None

        await runner._publish_model_audio_delta(
            call_id,
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_sip_opening",
                    "delta": base64.b64encode(output_pcm).decode("ascii"),
                },
            ),
        )

        events = store.list(call_id)
        assert publisher.published == []
        assert "stale_audio_dropped" in [event.type for event in events]
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_defers_sip_pre_stop_for_recent_ai_playback_echo_like_audio() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 30),
        call_id="call_sip_ai_playback_echo",
        clean_window_ms=40,
        max_hold_ms=80,
    )
    ai_frame = _pcm16_constant_frame(amplitude=3200, sample_rate_hz=24000, duration_ms=40)
    echo_like_uplink_frame = _pcm16_constant_frame(amplitude=3000)

    try:
        await runner._publish_model_audio_delta(
            call_id,
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_sip_opening",
                    "delta": base64.b64encode(ai_frame.data).decode("ascii"),
                },
            ),
        )

        for _ in range(18):
            await runner.send_audio_frame(call_id, echo_like_uplink_frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        echo_deferred = next(
            event for event in events if event.type == "sip_ai_playback_echo_deferred"
        )

        assert len(provider.sent_audio) == 18
        assert len(publisher.published) == 1
        assert publisher.stopped_call_ids == []
        assert echo_deferred.payload["reason"] == "awaiting_ai_playback_echo_guard"
        assert echo_deferred.payload["candidateClass"] == "stable_speech_candidate"
        assert "sip_interrupt_candidate" in event_types
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert "interrupt_audio_stop_requested" not in event_types
        assert "playout_queue_flushed" not in event_types
        assert "interrupt_confirmed" not in event_types
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_defers_clear_short_sip_speech_under_recent_ai_playback() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 20),
        call_id="call_sip_clear_weak_short_speech_echo_guard",
        clean_window_ms=40,
        max_hold_ms=80,
    )
    ai_frame = _pcm16_constant_frame(amplitude=1000, sample_rate_hz=24000, duration_ms=40)
    speech_frames = _pcm16_constant_frames([
        900,
        1500,
        2300,
        1650,
        2600,
        1800,
        2450,
        1700,
        2250,
    ])

    try:
        await runner._publish_model_audio_delta(
            call_id,
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_sip_opening",
                    "delta": base64.b64encode(ai_frame.data).decode("ascii"),
                },
            ),
        )

        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        deferred = next(event for event in events if event.type == "sip_pre_stop_deferred")

        assert "sip_interrupt_candidate" in event_types
        assert "sip_ai_playback_echo_deferred" not in event_types
        assert deferred.payload["candidateClass"] == "stable_speech_candidate"
        assert deferred.payload["candidateDurationMs"] == 180
        assert deferred.payload["rmsDbfs"] < -20.0
        assert deferred.payload["rmsRangeDb"] >= 6.0
        assert deferred.payload["rmsDirectionChanges"] >= 1
        assert deferred.payload["speechQualityRejection"] is None
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_pre_stops_echo_like_sip_audio_with_local_turn_evidence() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 30),
        call_id="call_sip_echo_like_local_turn_evidence",
        clean_window_ms=40,
        max_hold_ms=80,
    )
    ai_frame = _pcm16_constant_frame(amplitude=6000, sample_rate_hz=24000, duration_ms=40)
    speech_frames = _pcm16_constant_frames([
        1500,
        2200,
        2700,
        2300,
        1600,
        2400,
        2800,
        2300,
        2600,
        2300,
        2100,
        1900,
        2300,
        2700,
        2400,
        2900,
        2500,
        2800,
        2400,
        2600,
        2300,
        2700,
        2500,
        2800,
    ])

    try:
        await runner._publish_model_audio_delta(
            call_id,
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_sip_opening",
                    "delta": base64.b64encode(ai_frame.data).decode("ascii"),
                },
            ),
        )

        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        pre_stop = next(event for event in events if event.type == "sip_pre_stop")

        assert "sip_interrupt_candidate" in event_types
        assert "sip_ai_playback_echo_deferred" in event_types
        assert pre_stop.payload["candidateClass"] == "stable_speech_candidate"
        assert pre_stop.payload["candidateDurationMs"] >= 360
        assert pre_stop.payload["speechQualityRejection"] is None
        assert publisher.stopped_call_ids == [call_id]
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_pre_stops_sip_when_uplink_exceeds_recent_ai_playback() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 30),
        call_id="call_sip_user_over_ai_playback",
        clean_window_ms=40,
        max_hold_ms=80,
    )
    ai_frame = _pcm16_constant_frame(amplitude=1200, sample_rate_hz=24000, duration_ms=40)
    user_uplink_frames = _pcm16_constant_frames([
        5000,
        7200,
        5600,
        7600,
        6100,
        7900,
        6400,
        8100,
        5900,
        7500,
        6200,
        7800,
        6600,
        8000,
        6300,
        7700,
        6000,
        7400,
        6500,
        7900,
        6300,
        7600,
        6100,
        7300,
    ])

    try:
        await runner._publish_model_audio_delta(
            call_id,
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_sip_opening",
                    "delta": base64.b64encode(ai_frame.data).decode("ascii"),
                },
            ),
        )

        for frame in user_uplink_frames:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        pre_stop = next(event for event in events if event.type == "sip_pre_stop")

        assert len(provider.sent_audio) == 24
        assert len(publisher.published) == 1
        assert publisher.stopped_call_ids == [call_id]
        assert pre_stop.payload["candidateClass"] == "stable_speech_candidate"
        assert "sip_ai_playback_echo_deferred" not in event_types
        assert "response_generation_invalidated" in event_types
        assert "interrupt_audio_stop_requested" in event_types
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_clears_sip_provisional_state_when_turn_is_ignored() -> None:
    runner, _registry, _store, _provider, _publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 20),
        call_id="call_sip_clear_provisional_on_ignore",
    )

    try:
        turn = runner._pending_turn(call_id)
        turn.interrupt_candidate = True
        turn.sip_barge_in_requested = True
        turn.sip_barge_in_confirmed = True
        turn.sip_barge_in_confirmed_by = "provider_speech_started"
        turn.sip_barge_in_expires_at = datetime.now(timezone.utc) + timedelta(seconds=5)
        turn.sip_pre_stop_requested = True
        turn.sip_pre_stop_deferred = True
        turn.sip_ai_playback_echo_deferred = True
        turn.sip_candidate_class = "stable_speech_candidate"
        turn.sip_candidate_response_id = "resp_sip_opening"
        turn.sip_candidate_generation = 1
        turn.sip_provider_speech_confirmable = True
        turn.sip_interrupt_rejected = True

        runner._ignore_empty_turn(call_id, turn, "no_valid_transcript")

        assert turn.sip_barge_in_requested is False
        assert turn.sip_barge_in_confirmed is False
        assert turn.sip_barge_in_confirmed_by is None
        assert turn.sip_barge_in_expires_at is None
        assert turn.sip_pre_stop_requested is False
        assert turn.sip_pre_stop_deferred is False
        assert turn.sip_ai_playback_echo_deferred is False
        assert turn.sip_candidate_class is None
        assert turn.sip_candidate_response_id is None
        assert turn.sip_candidate_generation is None
        assert turn.sip_provider_speech_confirmable is False
        assert turn.sip_interrupt_rejected is False
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_skips_sip_barge_in_when_scene_barge_in_disabled() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 20),
        call_id="call_sip_barge_disabled",
    )
    session = runner.registry.get(call_id)
    session.effective_config["barge_in_enabled"] = False
    speech_frames = _pcm16_constant_frames([
        3200,
        4300,
        5400,
        4700,
        3600,
        4400,
        5200,
        4700,
        5500,
        5200,
        4700,
        4300,
        5100,
        4600,
        5400,
        4900,
        5700,
        5000,
    ])

    try:
        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)

        event_types = [event.type for event in store.list(call_id)]
        assert len(provider.sent_audio) == 18
        assert publisher.stopped_call_ids == []
        assert "interrupt_candidate" not in event_types
        assert "sip_interrupt_candidate" not in event_types
        assert "sip_pre_stop" not in event_types
        assert "interrupt_confirmed" not in event_types
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_emits_sip_vad_shadow_without_pre_stop_side_effects() -> None:
    shadow = FakeSipVadShadowDetector([
        SipVadShadowObservation(
            active=True,
            started=True,
            ended=False,
            duration_ms=20,
            frame_duration_ms=20,
            confidence=0.91,
            detector="fake_fsmn",
            analyzed=True,
            buffer_duration_ms=1200,
            window_start_ms=820,
            window_end_ms=1100,
            detection_lag_ms=380,
            speech_end_lag_ms=100,
        ),
        SipVadShadowObservation(
            active=True,
            started=False,
            ended=False,
            duration_ms=40,
            frame_duration_ms=20,
            confidence=0.93,
            detector="fake_fsmn",
        ),
    ])
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([False] * 10),
        call_id="call_sip_vad_shadow_only",
        sip_vad_shadow_enabled=True,
        sip_vad_shadow_detector=shadow,
    )
    speech_frame = _pcm16_constant_frame(amplitude=4000)

    try:
        for _ in range(2):
            await runner.send_audio_frame(call_id, speech_frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        shadow_started = next(event for event in events if event.type == "sip_vad_shadow_started")
        assert shadow_started.payload["detector"] == "fake_fsmn"
        assert shadow_started.payload["confidence"] == 0.91
        assert shadow_started.payload["durationMs"] == 20
        assert shadow_started.payload["analyzed"] is True
        assert shadow_started.payload["bufferDurationMs"] == 1200
        assert shadow_started.payload["windowStartMs"] == 820
        assert shadow_started.payload["windowEndMs"] == 1100
        assert shadow_started.payload["detectionLagMs"] == 380
        assert shadow_started.payload["speechEndLagMs"] == 100
        assert shadow_started.payload["interruptible"] is True
        assert shadow_started.payload["responseId"] == "resp_sip_opening"
        assert len(provider.sent_audio) == 2
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
        assert "sip_interrupt_candidate" not in event_types
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_emits_multiple_sip_vad_shadow_observations() -> None:
    shadow = FakeMultiSipVadShadowDetector([
        SipVadShadowObservation(
            active=True,
            started=True,
            ended=False,
            duration_ms=20,
            frame_duration_ms=20,
            detector="webrtc_shadow",
        ),
        SipVadShadowObservation(
            active=True,
            started=True,
            ended=False,
            duration_ms=20,
            frame_duration_ms=20,
            confidence=0.92,
            detector="fsmn_shadow",
        ),
    ])
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([False] * 10),
        call_id="call_sip_multi_vad_shadow",
        sip_vad_shadow_enabled=True,
        sip_vad_shadow_detector=shadow,
    )
    speech_frame = _pcm16_constant_frame(amplitude=4000)

    try:
        await runner.send_audio_frame(call_id, speech_frame)

        shadow_started = [
            event for event in store.list(call_id) if event.type == "sip_vad_shadow_started"
        ]
        assert [event.payload["detector"] for event in shadow_started] == [
            "webrtc_shadow",
            "fsmn_shadow",
        ]
        assert shadow_started[1].payload["confidence"] == 0.92
        assert len(provider.sent_audio) == 1
        assert publisher.stopped_call_ids == []
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_disables_sip_vad_shadow_after_error_without_side_effects() -> None:
    shadow = FailingSipVadShadowDetector("FSMN shadow unavailable")
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([False] * 10),
        call_id="call_sip_vad_shadow_error",
        sip_vad_shadow_enabled=True,
        sip_vad_shadow_detector=shadow,
    )
    speech_frame = _pcm16_constant_frame(amplitude=4000)

    try:
        for _ in range(2):
            await runner.send_audio_frame(call_id, speech_frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        shadow_errors = [event for event in events if event.type == "sip_vad_shadow_error"]
        assert len(shadow_errors) == 1
        assert shadow_errors[0].payload["detector"] == "fake_failing_shadow"
        assert shadow_errors[0].payload["errorType"] == "RuntimeError"
        assert shadow_errors[0].payload["message"] == "FSMN shadow unavailable"
        assert shadow.calls == 1
        assert len(provider.sent_audio) == 2
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
        assert "sip_interrupt_candidate" not in event_types
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_skips_sip_vad_shadow_when_scene_barge_in_disabled() -> None:
    shadow = FakeSipVadShadowDetector([
        SipVadShadowObservation(
            active=True,
            started=True,
            ended=False,
            duration_ms=20,
            frame_duration_ms=20,
            confidence=0.91,
            detector="fake_fsmn",
        )
    ])
    runner, registry, store, _provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([False] * 10),
        call_id="call_sip_vad_shadow_scene_disabled",
        sip_vad_shadow_enabled=True,
        sip_vad_shadow_detector=shadow,
    )
    session = registry.get(call_id)
    session.effective_config["barge_in_enabled"] = False

    try:
        await runner.send_audio_frame(call_id, _pcm16_constant_frame(amplitude=4000))

        event_types = [event.type for event in store.list(call_id)]
        assert "sip_vad_shadow_started" not in event_types
        assert "sip_vad_shadow_ended" not in event_types
        assert "sip_interrupt_candidate" not in event_types
        assert "sip_pre_stop" not in event_types
        assert publisher.stopped_call_ids == []
        assert shadow.reset_call_ids == [call_id]
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_ignores_sip_impulse_noise_before_pre_stop() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    registry = InMemorySessionRegistry()
    provider = FakeRealtimeProvider([])
    store = InMemoryEventStore()
    publisher = FakeAudioPublisher()
    call_id = "call_sip_impulse"
    session = _sip_session(call_id)
    registry.add(session)
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        sip_barge_in_enabled=True,
        sip_barge_in_fast_stop_enabled=True,
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-60.0,
            snr_threshold_db=10.0,
            impulse_noise_max_duration_ms=120,
        ),
        sip_barge_in_vad=FakeVad([True, False, False]),
    )

    try:
        await runner.start(session)
        registry.transition(call_id, CallSessionStatus.CONNECTED)
        registry.transition(call_id, CallSessionStatus.AI_THINKING)
        registry.transition(call_id, CallSessionStatus.AI_SPEAKING)
        runner._mark_response_started(call_id, {"response_id": "resp_sip_opening"})
        runner._response_lifecycle(call_id).active = True
        runner._playback_guard(call_id).current_response_audio_published = True

        await runner.send_audio_frame(call_id, _pcm16_pulse_frame(amplitude=30000))

        events = store.list(call_id)
        event_types = [event.type for event in events]
        ignored = next(event for event in events if event.type == "sip_impulse_noise_ignored")
        assert ignored.payload["candidateClass"] == "impulse_noise"
        assert "sip_interrupt_candidate" not in event_types
        assert "sip_pre_stop" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_ignores_clipped_sip_burst_before_pre_stop() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 10),
        call_id="call_sip_clipped_burst",
        clean_window_ms=40,
        max_hold_ms=80,
        short_speech_min_duration_ms=120,
    )
    clipped_burst_frame = _pcm16_constant_frame(amplitude=16000)

    try:
        for _ in range(6):
            await runner.send_audio_frame(call_id, clipped_burst_frame)

        event_types = [event.type for event in store.list(call_id)]
        assert "sip_impulse_noise_ignored" not in event_types
        assert "sip_interrupt_candidate" not in event_types
        assert "sip_pre_stop" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_ignores_rhythmic_clap_sequence_before_pre_stop() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 20),
        call_id="call_sip_rhythmic_clap",
        clean_window_ms=40,
        max_hold_ms=80,
        short_speech_min_duration_ms=120,
    )
    frames = _pcm16_constant_frames([900, 5000, 800, 4500, 900, 5200, 850, 4800, 900])

    try:
        for frame in frames:
            await runner.send_audio_frame(call_id, frame)

        event_types = [event.type for event in store.list(call_id)]
        assert "sip_interrupt_candidate" not in event_types
        assert "sip_pre_stop" not in event_types
        assert "sip_interrupt_rejected" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_confirms_sip_clean_window_with_stable_voice() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 40),
        call_id="call_sip_clean_confirm",
        clean_window_ms=60,
        max_hold_ms=100,
    )
    speech_frames = _pcm16_constant_frames([
        3200,
        4300,
        5400,
        4700,
        3600,
        4400,
        5200,
        4700,
        5500,
        5200,
        4700,
        4300,
        5100,
        4600,
        5400,
        4900,
        5700,
        5000,
    ])

    try:
        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)
        await asyncio.sleep(0.14)

        event_types = [event.type for event in store.list(call_id)]
        assert "sip_pre_stop" in event_types
        assert "sip_interrupt_confirmed" in event_types
        assert "interrupt_confirmed" in event_types
        assert provider.cancelled_response_count == 1
        assert publisher.stopped_call_ids == [call_id]
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_recovers_local_sip_confirm_without_transcript() -> None:
    runner, registry, store, provider, _publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 40),
        call_id="call_sip_confirm_without_transcript",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    speech_frames = _pcm16_constant_frames([
        3200,
        4300,
        5400,
        4700,
        3600,
        4400,
        5200,
        4700,
        5500,
        5200,
        4700,
        4300,
        5100,
        4600,
        5400,
        4900,
        5700,
        5000,
    ])

    try:
        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)
        await asyncio.sleep(0.14)

        event_types = [event.type for event in store.list(call_id)]
        assert "sip_interrupt_confirmed" in event_types
        assert "interrupt_confirmed" in event_types
        assert provider.cancelled_response_count == 1
        assert provider.created_responses == []

        await runner._apply_provider_event(
            call_id,
            provider,
            "model_response_done",
            datetime.now(timezone.utc),
            {"response": {"id": "resp_sip_opening", "status": "cancelled"}},
        )

        event_types = [event.type for event in store.list(call_id)]
        assert "sip_recovery_started" in event_types
        assert provider.created_responses
        recovery_input = provider.created_responses[0]
        assert recovery_input is not None
        assert "一句简短自然" in recovery_input
        assert "不要重复整段内容" in recovery_input
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_holds_unmodulated_sip_noise_without_context() -> None:
    runner, registry, store, provider, _publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 18 + [False] * 20),
        call_id="call_sip_clean_reject",
        clean_window_ms=60,
        max_hold_ms=100,
    )
    noise_frame = _pcm16_constant_frame(amplitude=4000)
    quiet_frame = _pcm16_constant_frame(amplitude=50)

    try:
        for _ in range(18):
            await runner.send_audio_frame(call_id, noise_frame)
        for _ in range(4):
            await runner.send_audio_frame(call_id, quiet_frame)
        await asyncio.sleep(0.14)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        assert "sip_interrupt_candidate" in event_types
        assert "sip_pre_stop_deferred" in event_types
        assert "sip_pre_stop" not in event_types
        assert "sip_interrupt_rejected" not in event_types
        assert "sip_interrupt_confirmed" not in event_types
        assert "interrupt_confirmed" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_does_not_pre_stop_unmodulated_sip_candidate() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 18 + [False] * 20),
        call_id="call_sip_unmodulated_candidate_no_pre_stop",
        clean_window_ms=40,
        max_hold_ms=80,
        recovery_silence_ms=20,
    )
    flat_frame = _pcm16_constant_frame(amplitude=4000)
    quiet_frame = _pcm16_constant_frame(amplitude=50)

    try:
        for _ in range(18):
            await runner.send_audio_frame(call_id, flat_frame)
        for _ in range(4):
            await runner.send_audio_frame(call_id, quiet_frame)
        await asyncio.sleep(0.12)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        assert "sip_interrupt_candidate" in event_types
        assert "sip_pre_stop_deferred" in event_types
        assert "sip_pre_stop" not in event_types
        assert "sip_interrupt_rejected" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert "interrupt_audio_stop_requested" not in event_types
        assert "playout_queue_flushed" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
        assert provider.created_responses == []
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_holds_strong_short_sip_noise_before_pre_stop() -> None:
    runner, _registry, store, provider, _publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 6 + [False] * 20),
        call_id="call_sip_strong_short_noise",
        clean_window_ms=40,
        max_hold_ms=80,
        short_speech_min_duration_ms=120,
        recovery_silence_ms=20,
    )
    strong_frame = _pcm16_constant_frame(amplitude=8000)
    quiet_frame = _pcm16_constant_frame(amplitude=50)

    try:
        for _ in range(6):
            await runner.send_audio_frame(call_id, strong_frame)
        for _ in range(4):
            await runner.send_audio_frame(call_id, quiet_frame)
        await asyncio.sleep(0.12)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        deferred = next(event for event in events if event.type == "sip_pre_stop_deferred")
        assert deferred.payload["candidateClass"] == "strong_short_speech_candidate"
        assert deferred.payload["requiredPreStopDurationMs"] == 240
        assert "sip_pre_stop" not in event_types
        assert "sip_interrupt_confirmed" not in event_types
        assert "interrupt_confirmed" not in event_types
        assert "sip_interrupt_rejected" not in event_types
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_does_not_pre_stop_then_reject_short_noise() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 5 + [False] * 8),
        call_id="call_sip_no_pre_stop_then_reject_short_noise",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    noise_frames = _pcm16_constant_frames([800, 4200, 900, 3800, 750] + [40] * 8)

    try:
        for frame in noise_frames:
            await runner.send_audio_frame(call_id, frame)
        await asyncio.sleep(0.08)

        event_types = [event.type for event in store.list(call_id)]
        assert "sip_pre_stop" not in event_types
        assert "sip_interrupt_rejected" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_holds_ambiguous_short_sip_burst_before_pre_stop() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 6 + [False] * 20),
        call_id="call_sip_ambiguous_short_burst",
        clean_window_ms=40,
        max_hold_ms=80,
        recovery_silence_ms=20,
    )
    burst_frame = _pcm16_constant_frame(amplitude=6000)
    quiet_frame = _pcm16_constant_frame(amplitude=50)

    try:
        for _ in range(6):
            await runner.send_audio_frame(call_id, burst_frame)
        for _ in range(4):
            await runner.send_audio_frame(call_id, quiet_frame)
        await asyncio.sleep(0.12)

        event_types = [event.type for event in store.list(call_id)]
        assert "sip_interrupt_candidate" not in event_types
        assert "sip_pre_stop" not in event_types
        assert "sip_interrupt_rejected" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_holds_low_confidence_flat_sip_audio_before_pre_stop() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 12 + [False] * 20),
        call_id="call_sip_low_confidence_flat_audio",
        clean_window_ms=40,
        max_hold_ms=80,
        recovery_silence_ms=20,
    )
    flat_low_confidence_frame = _pcm16_constant_frame(amplitude=800)
    quiet_frame = _pcm16_constant_frame(amplitude=50)

    try:
        for _ in range(12):
            await runner.send_audio_frame(call_id, flat_low_confidence_frame)
        for _ in range(4):
            await runner.send_audio_frame(call_id, quiet_frame)
        await asyncio.sleep(0.12)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        assert "sip_interrupt_candidate" in event_types
        assert "sip_pre_stop_deferred" in event_types
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert "sip_interrupt_rejected" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_holds_weak_flat_sip_audio_before_pre_stop() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 12 + [False] * 20),
        call_id="call_sip_weak_flat_turn_evidence",
        clean_window_ms=40,
        max_hold_ms=80,
        recovery_silence_ms=20,
    )
    weak_flat_frame = _pcm16_constant_frame(amplitude=1050)
    quiet_frame = _pcm16_constant_frame(amplitude=50)

    try:
        for _ in range(12):
            await runner.send_audio_frame(call_id, weak_flat_frame)
        for _ in range(4):
            await runner.send_audio_frame(call_id, quiet_frame)
        await asyncio.sleep(0.12)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        assert "sip_interrupt_candidate" in event_types
        assert "sip_pre_stop_deferred" in event_types
        deferred = next(event for event in events if event.type == "sip_pre_stop_deferred")
        assert deferred.payload["speechQualityRejection"] == "weak_flat_turn_evidence"
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert "sip_interrupt_rejected" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_holds_breath_like_sip_candidate_before_pre_stop() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 12 + [False] * 20),
        call_id="call_sip_breath_like_candidate",
        clean_window_ms=40,
        max_hold_ms=80,
        recovery_silence_ms=20,
    )
    breath_like_frames = _pcm16_constant_frames([760] * 9 + [1120] * 3)
    quiet_frame = _pcm16_constant_frame(amplitude=50)

    try:
        for frame in breath_like_frames:
            await runner.send_audio_frame(call_id, frame)
        for _ in range(4):
            await runner.send_audio_frame(call_id, quiet_frame)
        await asyncio.sleep(0.12)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        deferred = next(event for event in events if event.type == "sip_pre_stop_deferred")
        assert "sip_interrupt_candidate" in event_types
        assert deferred.payload["candidateClass"] == "stable_speech_candidate"
        assert deferred.payload["candidateDurationMs"] == 180
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert "sip_interrupt_rejected" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_defers_single_short_sip_speech_without_turn_authority() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 12),
        call_id="call_sip_single_short_speech_micro_confirm",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    short_speech_frames = _pcm16_constant_frames([
        3200,
        4300,
        5400,
        4700,
        3600,
        4400,
        5200,
        4700,
        5500,
        5200,
        4700,
        4300,
    ])

    try:
        for frame in short_speech_frames[:9]:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        deferred = next(event for event in events if event.type == "sip_pre_stop_deferred")
        assert "sip_interrupt_candidate" in event_types
        assert deferred.payload["candidateDurationMs"] == 180
        assert deferred.payload["snrDb"] >= 16
        assert deferred.payload["rmsRangeDb"] >= 4
        assert deferred.payload["rmsDirectionChanges"] >= 1
        assert deferred.payload["speechQualityRejection"] is None
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert publisher.stopped_call_ids == []

        for frame in short_speech_frames[9:]:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_upgrades_deferred_sip_pre_stop_with_realtime_fsmn_shadow() -> None:
    shadow = FakeSipVadShadowDetector(
        [
            *[
                SipVadShadowObservation(
                    active=False,
                    started=False,
                    ended=False,
                    duration_ms=0,
                    frame_duration_ms=20,
                    detector="fsmn_shadow",
                )
                for _ in range(9)
            ],
            SipVadShadowObservation(
                active=True,
                started=True,
                ended=False,
                duration_ms=20,
                frame_duration_ms=20,
                detector="fsmn_shadow",
                analyzed=True,
                buffer_duration_ms=1200,
                window_start_ms=640,
                window_end_ms=1180,
                detection_lag_ms=560,
                speech_end_lag_ms=20,
            ),
        ]
    )
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 10),
        call_id="call_sip_deferred_shadow_upgrade",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
        sip_vad_shadow_enabled=True,
        sip_vad_shadow_detector=shadow,
    )
    speech_frames = _pcm16_constant_frames([
        2500,
        2700,
        2900,
        3100,
        3300,
        3500,
        3700,
        3900,
        4100,
        3000,
        3200,
        3400,
    ])

    try:
        for frame in speech_frames[:9]:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        deferred = next(event for event in events if event.type == "sip_pre_stop_deferred")
        assert deferred.payload["candidateDurationMs"] == 180
        assert "sip_pre_stop" not in event_types
        assert publisher.stopped_call_ids == []

        await runner.send_audio_frame(call_id, speech_frames[9])

        events = store.list(call_id)
        event_types = [event.type for event in events]
        assert "sip_vad_shadow_started" in event_types
        assert "sip_pre_stop" not in event_types

        for frame in speech_frames[10:]:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        pre_stop = next(event for event in events if event.type == "sip_pre_stop")
        assert pre_stop.payload["candidateDurationMs"] >= 240
        assert pre_stop.payload["sipVadShadowEvidence"] == "realtime_fsmn_shadow"
        assert pre_stop.payload["sipVadShadowWindowMs"] == 540
        assert publisher.stopped_call_ids == [call_id]
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_upgrades_soft_sip_speech_with_realtime_fsmn_shadow() -> None:
    shadow = FakeSipVadShadowDetector(
        [
            *[
                SipVadShadowObservation(
                    active=False,
                    started=False,
                    ended=False,
                    duration_ms=0,
                    frame_duration_ms=20,
                    detector="fsmn_shadow",
                )
                for _ in range(9)
            ],
            SipVadShadowObservation(
                active=True,
                started=True,
                ended=False,
                duration_ms=20,
                frame_duration_ms=20,
                detector="fsmn_shadow",
                analyzed=True,
                buffer_duration_ms=1200,
                window_start_ms=630,
                window_end_ms=1180,
                detection_lag_ms=570,
                speech_end_lag_ms=20,
            ),
        ]
    )
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 12),
        call_id="call_sip_soft_speech_shadow_upgrade",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
        sip_vad_shadow_enabled=True,
        sip_vad_shadow_detector=shadow,
    )
    speech_frames = _pcm16_constant_frames([
        1050,
        1700,
        2450,
        1900,
        2750,
        2150,
        3000,
        2350,
        3100,
        2300,
        2850,
        2450,
    ])

    try:
        for frame in speech_frames[:9]:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        deferred = next(event for event in events if event.type == "sip_pre_stop_deferred")
        assert deferred.payload["candidateDurationMs"] == 180
        assert deferred.payload["rmsDbfs"] < -20.0
        assert deferred.payload["speechQualityRejection"] is None
        assert "sip_pre_stop" not in event_types
        assert publisher.stopped_call_ids == []

        await runner.send_audio_frame(call_id, speech_frames[9])

        events = store.list(call_id)
        assert "sip_pre_stop" not in [event.type for event in events]

        for frame in speech_frames[10:]:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        pre_stop = next(event for event in events if event.type == "sip_pre_stop")
        assert pre_stop.payload["candidateDurationMs"] >= 240
        assert pre_stop.payload["rmsDbfs"] < -20.0
        assert pre_stop.payload["speechQualityRejection"] is None
        assert pre_stop.payload["sipVadShadowEvidence"] == "realtime_fsmn_shadow"
        assert pre_stop.payload["sipVadShadowWindowMs"] == 550
        assert "sipShortSpeechEvidence" not in pre_stop.payload
        assert publisher.stopped_call_ids == [call_id]
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_creates_shadow_assisted_sip_candidate_before_main_candidate() -> None:
    shadow = FakeSipVadShadowDetector(
        [
            *[
                SipVadShadowObservation(
                    active=False,
                    started=False,
                    ended=False,
                    duration_ms=0,
                    frame_duration_ms=20,
                    detector="fsmn_shadow",
                )
                for _ in range(5)
            ],
            SipVadShadowObservation(
                active=True,
                started=True,
                ended=False,
                duration_ms=20,
                frame_duration_ms=20,
                detector="fsmn_shadow",
                analyzed=True,
                buffer_duration_ms=1200,
                window_start_ms=640,
                window_end_ms=1180,
                detection_lag_ms=560,
                speech_end_lag_ms=20,
            ),
        ]
    )
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 6),
        call_id="call_sip_shadow_assisted_candidate",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
        sip_vad_shadow_enabled=True,
        sip_vad_shadow_detector=shadow,
    )
    speech_frames = _pcm16_constant_frames([
        1050,
        1700,
        2450,
        1900,
        2750,
        2150,
    ])

    try:
        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        candidate = next(event for event in events if event.type == "sip_interrupt_candidate")
        deferred = next(event for event in events if event.type == "sip_pre_stop_deferred")
        assert "sip_vad_shadow_started" in event_types
        assert candidate.payload["candidateDurationMs"] == 120
        assert candidate.payload["sipVadShadowCandidateEvidence"] == "realtime_fsmn_shadow"
        assert candidate.payload["sipVadShadowLocalEvidence"] == "local_modulated_candidate"
        assert deferred.payload["reason"] == "awaiting_pre_stop_authority"
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_defers_180ms_webrtc_shadow_without_stable_window() -> None:
    shadow = FakeSipVadShadowDetector(
        [
            SipVadShadowObservation(
                active=True,
                started=index == 0,
                ended=False,
                duration_ms=(index + 1) * 20,
                frame_duration_ms=20,
                detector="webrtc_shadow",
            )
            for index in range(9)
        ]
    )
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 9),
        call_id="call_sip_webrtc_shadow_180ms_deferred",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
        sip_vad_shadow_enabled=True,
        sip_vad_shadow_detector=shadow,
    )
    speech_frames = _pcm16_constant_frames([
        1050,
        1700,
        2450,
        1900,
        2750,
        2150,
        3000,
        2350,
        3100,
    ])

    try:
        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        deferred = next(event for event in events if event.type == "sip_pre_stop_deferred")
        assert "sip_interrupt_candidate" in event_types
        assert deferred.payload["candidateDurationMs"] == 180
        assert deferred.payload["requiredPreStopDurationMs"] > 180
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_does_not_let_continuous_webrtc_shadow_bypass_authority_guard() -> None:
    shadow = FakeSipVadShadowDetector(
        [
            SipVadShadowObservation(
                active=True,
                started=False,
                ended=False,
                duration_ms=2200 + index * 20,
                frame_duration_ms=20,
                detector="webrtc_shadow",
            )
            for index in range(9)
        ]
    )
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 9),
        call_id="call_sip_continuous_shadow_clear_local_modulation",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
        sip_vad_shadow_enabled=True,
        sip_vad_shadow_detector=shadow,
    )
    speech_frames = _pcm16_constant_frames([
        1600,
        2200,
        3000,
        2600,
        3400,
        2800,
        3200,
        2600,
        3000,
    ])

    try:
        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        deferred = next(event for event in events if event.type == "sip_pre_stop_deferred")
        assert deferred.payload["candidateDurationMs"] == 180
        assert deferred.payload["requiredPreStopDurationMs"] > 180
        assert "sip_pre_stop" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_defers_sip_pre_stop_without_playback_target() -> None:
    shadow = FakeSipVadShadowDetector(
        [
            SipVadShadowObservation(
                active=True,
                started=index == 0,
                ended=False,
                duration_ms=(index + 1) * 20,
                frame_duration_ms=20,
                detector="webrtc_shadow",
            )
            for index in range(18)
        ]
    )
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 18),
        call_id="call_sip_no_playback_target_shadow_deferred",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
        sip_vad_shadow_enabled=True,
        sip_vad_shadow_detector=shadow,
    )
    guard = runner._playback_guard(call_id)
    guard.current_response_id = None
    guard.current_response_audio_published = False
    speech_frames = _pcm16_constant_frames([
        1050,
        1700,
        2450,
        1900,
        2750,
        2150,
        3000,
        2350,
        3100,
        2500,
        3300,
        2700,
        3500,
        2850,
        3650,
        3000,
        3800,
        3150,
    ])

    try:
        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        deferred_events = [event for event in events if event.type == "sip_pre_stop_deferred"]
        assert "sip_interrupt_candidate" in event_types
        assert deferred_events[-1].payload["reason"] == "awaiting_ai_playback_target"
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_defers_recent_stable_webrtc_shadow_until_local_candidate_reaches_pre_stop_duration() -> None:
    shadow = FakeSipVadShadowDetector(
        [
            *[
                SipVadShadowObservation(
                    active=True,
                    started=index == 0,
                    ended=False,
                    duration_ms=(index + 1) * 20,
                    frame_duration_ms=20,
                    detector="webrtc_shadow",
                )
                for index in range(18)
            ],
            *[
                SipVadShadowObservation(
                    active=False,
                    started=False,
                    ended=False,
                    duration_ms=0,
                    frame_duration_ms=20,
                    detector="webrtc_shadow",
                )
                for _ in range(9)
            ],
        ]
    )
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([False] * 18 + [True] * 9),
        call_id="call_sip_recent_stable_webrtc_shadow_authority",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
        sip_vad_shadow_enabled=True,
        sip_vad_shadow_detector=shadow,
    )
    quiet_frame = _pcm16_constant_frame(amplitude=50)
    speech_frames = _pcm16_constant_frames([
        1050,
        1700,
        2450,
        1900,
        2750,
        2150,
        3000,
        2350,
        3100,
    ])

    try:
        for _ in range(18):
            await runner.send_audio_frame(call_id, quiet_frame)
        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        deferred = next(event for event in events if event.type == "sip_pre_stop_deferred")
        assert "sip_vad_shadow_started" in event_types
        assert "sip_interrupt_candidate" in event_types
        assert deferred.payload["candidateDurationMs"] == 180
        assert deferred.payload["reason"] == "awaiting_pre_stop_authority"
        assert "sip_pre_stop" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_uses_continuous_webrtc_shadow_authority_to_pre_stop() -> None:
    shadow = FakeSipVadShadowDetector(
        [
            SipVadShadowObservation(
                active=True,
                started=index == 0,
                ended=False,
                duration_ms=(index + 1) * 20,
                frame_duration_ms=20,
                detector="webrtc_shadow",
            )
            for index in range(18)
        ]
    )
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 18),
        call_id="call_sip_webrtc_shadow_upgrade",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
        pre_stop_min_duration_ms=360,
        sip_vad_shadow_enabled=True,
        sip_vad_shadow_detector=shadow,
    )
    speech_frames = _pcm16_constant_frames([
        1050,
        1700,
        2450,
        1900,
        2750,
        2150,
        3000,
        2350,
        3100,
        2500,
        3300,
        2700,
        3500,
        2850,
        3650,
        3000,
        3800,
        3150,
    ])

    try:
        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        pre_stop = next(event for event in events if event.type == "sip_pre_stop")
        assert "sip_interrupt_candidate" in event_types
        assert "sip_pre_stop_deferred" in event_types
        assert pre_stop.payload["candidateDurationMs"] == 360
        assert pre_stop.payload["sipPreStopAuthority"] == "local_speech"
        assert pre_stop.payload["sipPreStopAuthorityEvidence"] == (
            "realtime_webrtc_shadow_local_modulation"
        )
        assert pre_stop.payload["sipVadShadowEvidence"] == "realtime_webrtc_shadow"
        assert pre_stop.payload["sipVadShadowDetector"] == "webrtc_shadow"
        assert pre_stop.payload["sipVadShadowWindowMs"] == 360
        assert pre_stop.payload["sipVadShadowLocalEvidence"] == "local_modulated_candidate"
        assert publisher.stopped_call_ids == [call_id]
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_shadow_authority_pre_stops_sustained_modulated_speech() -> None:
    shadow = FakeSipVadShadowDetector(
        [
            SipVadShadowObservation(
                active=True,
                started=index == 0,
                ended=False,
                duration_ms=(index + 1) * 20,
                frame_duration_ms=20,
                detector="webrtc_shadow",
            )
            for index in range(18)
        ]
    )
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 18),
        call_id="call_sip_shadow_authority_echo_guard",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
        pre_stop_min_duration_ms=360,
        sip_vad_shadow_enabled=True,
        sip_vad_shadow_detector=shadow,
    )
    ai_frame = _pcm16_constant_frame(amplitude=6000, sample_rate_hz=24000, duration_ms=40)
    speech_frames = _pcm16_constant_frames([
        1050,
        1700,
        2450,
        1900,
        2750,
        2150,
        3000,
        2350,
        3100,
        2500,
        3300,
        2700,
        3500,
        2850,
        3650,
        3000,
        3800,
        3150,
    ])

    try:
        await runner._publish_model_audio_delta(
            call_id,
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_sip_opening",
                    "delta": base64.b64encode(ai_frame.data).decode("ascii"),
                },
            ),
        )

        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        pre_stop = next(event for event in events if event.type == "sip_pre_stop")
        assert "sip_interrupt_candidate" in event_types
        assert "sip_ai_playback_echo_deferred" in event_types
        assert pre_stop.payload["sipPreStopAuthority"] == "local_speech"
        assert pre_stop.payload["sipPreStopAuthorityEvidence"] == (
            "realtime_webrtc_shadow_local_modulation"
        )
        assert pre_stop.payload["candidateDurationMs"] == 360
        assert pre_stop.payload["sipVadShadowWindowMs"] == 360
        assert publisher.stopped_call_ids == [call_id]
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_shadow_authority_rejects_flat_breath_like_audio() -> None:
    shadow = FakeSipVadShadowDetector(
        [
            SipVadShadowObservation(
                active=True,
                started=index == 0,
                ended=False,
                duration_ms=(index + 1) * 20,
                frame_duration_ms=20,
                detector="webrtc_shadow",
            )
            for index in range(18)
        ]
    )
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 18),
        call_id="call_sip_shadow_authority_flat_audio",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
        sip_vad_shadow_enabled=True,
        sip_vad_shadow_detector=shadow,
    )
    flat_frames = _pcm16_constant_frames([1180] * 18)

    try:
        for frame in flat_frames:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        assert "sip_interrupt_candidate" in event_types
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_rejects_continuous_webrtc_shadow_without_local_modulation() -> None:
    shadow = FakeSipVadShadowDetector(
        [
            SipVadShadowObservation(
                active=True,
                started=index == 0,
                ended=False,
                duration_ms=(index + 1) * 20,
                frame_duration_ms=20,
                detector="webrtc_shadow",
            )
            for index in range(18)
        ]
    )
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 18),
        call_id="call_sip_webrtc_shadow_flat_rejected",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
        sip_vad_shadow_enabled=True,
        sip_vad_shadow_detector=shadow,
    )
    flat_frames = _pcm16_constant_frames([1180] * 18)

    try:
        for frame in flat_frames:
            await runner.send_audio_frame(call_id, frame)

        event_types = [event.type for event in store.list(call_id)]
        assert "sip_interrupt_candidate" in event_types
        assert "sip_pre_stop_deferred" in event_types
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_defers_sip_response_until_local_speech_clean() -> None:
    active_shadow = SipVadShadowObservation(
        active=True,
        started=True,
        ended=False,
        duration_ms=20,
        frame_duration_ms=20,
        detector="fsmn_shadow",
        analyzed=True,
        buffer_duration_ms=1200,
        window_start_ms=640,
        window_end_ms=1180,
        detection_lag_ms=560,
        speech_end_lag_ms=20,
    )
    ended_shadow = SipVadShadowObservation(
        active=False,
        started=False,
        ended=True,
        duration_ms=1200,
        frame_duration_ms=20,
        detector="fsmn_shadow",
        analyzed=True,
        buffer_duration_ms=1200,
        window_start_ms=0,
        window_end_ms=1180,
    )
    inactive_shadow = SipVadShadowObservation(
        active=False,
        started=False,
        ended=False,
        duration_ms=0,
        frame_duration_ms=20,
        detector="fsmn_shadow",
    )
    shadow = FakeSipVadShadowDetector(
        [active_shadow] * 12 + [ended_shadow] + [inactive_shadow] * 12
    )
    runner, registry, store, provider, _publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 12 + [False] * 12),
        call_id="call_sip_release_waits_for_local_clean_window",
        clean_window_ms=80,
        max_hold_ms=200,
        recovery_silence_ms=20,
        sip_vad_shadow_enabled=True,
        sip_vad_shadow_detector=shadow,
    )
    speech_frame = _pcm16_constant_frame(amplitude=4500)
    quiet_frame = _pcm16_constant_frame(amplitude=50)

    try:
        for _ in range(12):
            await runner.send_audio_frame(call_id, speech_frame)

        await runner._handle_user_transcript(
            call_id,
            provider,
            ProviderEvent(type="user_transcript_done", payload={"transcript": "你先别说。"}),
            datetime.now(timezone.utc),
        )
        await runner._handle_user_speech_stopped(call_id, provider, datetime.now(timezone.utc))

        assert provider.created_responses == []
        assert runner._response_lifecycle(call_id).pending_create is True

        await runner._complete_response_and_flush_pending(call_id, provider)

        assert provider.created_responses == []
        assert "sip_response_release_deferred" in [
            event.type for event in store.list(call_id)
        ]

        for _ in range(12):
            await runner.send_audio_frame(call_id, quiet_frame)
        await asyncio.sleep(0.1)
        assert provider.created_responses == [None]
        assert registry.get(call_id).status == CallSessionStatus.AI_THINKING
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_uses_shadow_turn_evidence_for_decaying_sip_speech() -> None:
    shadow = FakeSipVadShadowDetector(
        [
            *[
                SipVadShadowObservation(
                    active=False,
                    started=False,
                    ended=False,
                    duration_ms=0,
                    frame_duration_ms=20,
                    detector="fsmn_shadow",
                )
                for _ in range(8)
            ],
            SipVadShadowObservation(
                active=True,
                started=True,
                ended=False,
                duration_ms=20,
                frame_duration_ms=20,
                detector="fsmn_shadow",
                analyzed=True,
                buffer_duration_ms=1200,
                window_start_ms=620,
                window_end_ms=1180,
                detection_lag_ms=560,
                speech_end_lag_ms=20,
            ),
        ]
    )
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 12),
        call_id="call_sip_decaying_shadow_turn_evidence",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
        sip_vad_shadow_enabled=True,
        sip_vad_shadow_detector=shadow,
    )
    speech_frames = _pcm16_constant_frames([
        4000,
        3600,
        3100,
        2600,
        2100,
        1700,
        1300,
        1050,
        720,
        700,
        680,
        660,
    ])

    try:
        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        pre_stop = next(event for event in events if event.type == "sip_pre_stop")
        assert pre_stop.payload["candidateDurationMs"] >= 240
        assert pre_stop.payload["snrDb"] < 17.5
        assert pre_stop.payload["sipVadShadowEvidence"] == "realtime_fsmn_shadow"
        assert pre_stop.payload["sipVadShadowLocalEvidence"] == "shadow_turn_cluster"
        assert pre_stop.payload["sipTurnClusterBurstCount"] >= 2
        assert pre_stop.payload["sipTurnClusterMaxSnrDb"] >= 20
        assert pre_stop.payload["sipTurnClusterRmsRangeDb"] >= 3.0
        assert publisher.stopped_call_ids == [call_id]
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_rejects_shadow_turn_without_local_modulation() -> None:
    shadow = FakeSipVadShadowDetector(
        [
            *[
                SipVadShadowObservation(
                    active=False,
                    started=False,
                    ended=False,
                    duration_ms=0,
                    frame_duration_ms=20,
                    detector="fsmn_shadow",
                )
                for _ in range(8)
            ],
            SipVadShadowObservation(
                active=True,
                started=True,
                ended=False,
                duration_ms=20,
                frame_duration_ms=20,
                detector="fsmn_shadow",
                analyzed=True,
                buffer_duration_ms=1200,
                window_start_ms=620,
                window_end_ms=1180,
                detection_lag_ms=560,
                speech_end_lag_ms=20,
            ),
        ]
    )
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 12),
        call_id="call_sip_flat_shadow_turn_rejected",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
        sip_vad_shadow_enabled=True,
        sip_vad_shadow_detector=shadow,
    )
    flat_frames = _pcm16_constant_frames([900] * 9)

    try:
        for frame in flat_frames:
            await runner.send_audio_frame(call_id, frame)

        event_types = [event.type for event in store.list(call_id)]
        assert "sip_interrupt_candidate" in event_types
        assert "sip_pre_stop_deferred" in event_types
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_defers_clear_modulated_180ms_sip_speech_without_continuation() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 9 + [False] * 20),
        call_id="call_sip_clear_modulated_180ms_speech_without_continuation",
        clean_window_ms=40,
        max_hold_ms=80,
        recovery_silence_ms=20,
    )
    speech_frames = _pcm16_constant_frames([
        3200,
        4300,
        5400,
        4700,
        3600,
        4400,
        5200,
        4700,
        5500,
    ])
    quiet_frame = _pcm16_constant_frame(amplitude=50)

    try:
        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)
        for _ in range(4):
            await runner.send_audio_frame(call_id, quiet_frame)
        await asyncio.sleep(0.12)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        deferred = next(event for event in events if event.type == "sip_pre_stop_deferred")
        assert "sip_interrupt_candidate" in event_types
        assert deferred.payload["candidateDurationMs"] == 180
        assert deferred.payload["rmsRangeDb"] >= 4
        assert deferred.payload["rmsDirectionChanges"] >= 1
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


def _mark_sip_authority_playback_target(
    runner: RealtimeCallAgentRunner,
    call_id: str,
    *,
    response_id: str = "resp_sip_authority",
) -> None:
    guard = runner._playback_guard(call_id)
    guard.current_response_id = response_id
    guard.current_response_audio_published = True


class _StaticSipAuthorityDetector:
    def __init__(
        self,
        payload: dict[str, object],
        *,
        single_short: bool = False,
        fast_local: bool = False,
        pre_stop_local: bool = False,
    ) -> None:
        self.payload = payload
        self.single_short = single_short
        self.fast_local = fast_local
        self.pre_stop_local = pre_stop_local

    def has_single_short_pre_stop_local_speech(self, *_args, **_kwargs) -> bool:
        return self.single_short

    def has_fast_pre_stop_local_speech(self, _call_id: str) -> bool:
        return self.fast_local

    def has_pre_stop_local_speech(self, _call_id: str) -> bool:
        return self.pre_stop_local

    def latest_observation_payload(self, _call_id: str) -> dict[str, object]:
        return self.payload


def test_realtime_agent_runner_defers_single_short_local_only_micro_confirm() -> None:
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    call_id = "call_sip_single_short_local_only_micro_confirm"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-18.8,
        noise_floor_dbfs=-44.0,
        snr_db=25.2,
        peak_dbfs=-12.4,
        vad_voiced_ms=240,
        candidate_duration_ms=240,
        speech_duration_ms=240,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector(
        {
            "rmsRangeDb": 5.0,
            "rmsDirectionChanges": 0,
            "largeRmsJumpCount": 0,
            "speechQualityRejection": None,
        },
        single_short=True,
        fast_local=True,
        pre_stop_local=True,
    )

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_authorized_pre_stop_evidence"
    assert decision.extra_payload["sipShortSpeechEvidence"] == "single_high_confidence_burst"


def test_realtime_agent_runner_pre_stops_fast_strong_short_local_speech() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
            short_speech_min_duration_ms=180,
        ),
    )
    call_id = "call_sip_fast_strong_short_local_speech"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-19.0,
        noise_floor_dbfs=-50.0,
        snr_db=31.0,
        peak_dbfs=-13.0,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="strong_short_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector(
        {
            "rmsRangeDb": 8.0,
            "rmsDirectionChanges": 1,
            "largeRmsJumpCount": 1,
            "speechQualityRejection": None,
        },
        fast_local=True,
        pre_stop_local=True,
    )

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "pre_stop"
    assert decision.evidence == "strong_short_local_speech"
    assert decision.required_duration_ms == 180
    assert decision.extra_payload["sipShortSpeechEvidence"] == "strong_short_local_speech"


def test_realtime_agent_runner_defers_low_energy_clear_short_modulated_local_speech() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_clear_short_modulated_local_speech"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-30.1,
        noise_floor_dbfs=-44.0,
        snr_db=13.9,
        peak_dbfs=-21.0,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "rmsRangeDb": 6.43,
        "rmsDirectionChanges": 1,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_authorized_pre_stop_evidence"
    assert decision.required_duration_ms == 180
    assert decision.extra_payload["sipShortSpeechEvidence"] == "clear_short_modulated_burst"
    assert decision.extra_payload["sipClearShortNoiseRisk"] == "low_energy_short_burst"


def test_realtime_agent_runner_pre_stops_moderate_clear_short_continue_speech() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_moderate_clear_short_continue_speech"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-25.44,
        noise_floor_dbfs=-44.0,
        snr_db=18.56,
        peak_dbfs=-16.6,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "rmsRangeDb": 10.9,
        "rmsDirectionChanges": 2,
        "largeRmsJumpCount": 2,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "pre_stop"
    assert decision.evidence == "clear_short_modulated_burst"
    assert decision.required_duration_ms == 180
    assert decision.extra_payload["sipShortSpeechEvidence"] == "clear_short_modulated_burst"


def test_realtime_agent_runner_pre_stops_elevated_noise_clear_short_ack_speech() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_elevated_noise_clear_short_ack"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-23.15,
        noise_floor_dbfs=-36.76,
        snr_db=13.6,
        peak_dbfs=-13.34,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="fixture_call_334820_short_ack_part_1",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "maxSnrDb": 14.95,
        "rmsRangeDb": 8.49,
        "rmsDirectionChanges": 2,
        "largeRmsJumpCount": 0,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "pre_stop"
    assert decision.evidence == "elevated_noise_clear_short_modulated_burst"
    assert decision.required_duration_ms == 180
    assert decision.extra_payload["sipShortSpeechEvidence"] == "clear_short_modulated_burst"
    assert decision.extra_payload["sipElevatedNoiseClearShortEvidence"] is True


def test_realtime_agent_runner_pre_stops_echo_guarded_compact_short_phrase() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_echo_guarded_compact_short_phrase"
    started_at = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(
        runner,
        call_id,
        response_id="resp_echo_guarded_compact_short_phrase",
    )
    turn = PendingUserTurn()

    first_observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-19.08,
        noise_floor_dbfs=-33.27,
        snr_db=14.19,
        peak_dbfs=-9.13,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="call_334205_next_time_1",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "maxSnrDb": 14.19,
        "rmsRangeDb": 3.87,
        "rmsDirectionChanges": 2,
        "largeRmsJumpCount": 0,
        "speechQualityRejection": None,
    })
    runner._record_sip_deferred_episode_observation(
        call_id=call_id,
        turn=turn,
        timestamp=started_at,
        observation=first_observation,
    )

    second_timestamp = started_at + timedelta(milliseconds=2439)
    second_observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-14.53,
        noise_floor_dbfs=-33.27,
        snr_db=18.74,
        peak_dbfs=-6.05,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="call_334205_next_time_2",
    )
    runner._last_ai_audio_rms_dbfs[call_id] = -16.75
    runner._last_ai_audio_published_at[call_id] = second_timestamp - timedelta(
        milliseconds=8,
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "maxSnrDb": 18.74,
        "rmsRangeDb": 7.74,
        "rmsDirectionChanges": 3,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    })
    runner._record_sip_deferred_episode_observation(
        call_id=call_id,
        turn=turn,
        timestamp=second_timestamp,
        observation=second_observation,
    )

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=second_timestamp,
        observation=second_observation,
    )

    assert decision.action == "pre_stop"
    assert decision.evidence == "echo_guarded_compact_short_phrase"
    assert decision.required_duration_ms == 180
    assert decision.extra_payload["sipDeferredEpisodeEvidence"] == (
        "elevated_noise_compact_two_burst_turn"
    )
    assert decision.extra_payload["sipEchoGuardedCompactShortPhraseEvidence"] == (
        "loud_modulated_two_burst"
    )
    assert decision.extra_payload["sipUplinkAboveAiPlaybackDb"] == 2.22


def test_realtime_agent_runner_defers_echo_guarded_compact_short_phrase_when_ai_dominates() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_echo_guarded_compact_short_phrase_ai_dominates"
    started_at = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(
        runner,
        call_id,
        response_id="resp_echo_guarded_compact_short_phrase_ai_dominates",
    )
    turn = PendingUserTurn()

    first_observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-19.08,
        noise_floor_dbfs=-33.27,
        snr_db=14.19,
        peak_dbfs=-9.13,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="fixture_echo_compact_ai_dominates_part_1",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "maxSnrDb": 14.19,
        "rmsRangeDb": 3.87,
        "rmsDirectionChanges": 2,
        "largeRmsJumpCount": 0,
        "speechQualityRejection": None,
    })
    runner._record_sip_deferred_episode_observation(
        call_id=call_id,
        turn=turn,
        timestamp=started_at,
        observation=first_observation,
    )

    second_timestamp = started_at + timedelta(milliseconds=2439)
    second_observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-14.9,
        noise_floor_dbfs=-33.27,
        snr_db=18.74,
        peak_dbfs=-6.05,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="fixture_echo_compact_ai_dominates_part_2",
    )
    runner._last_ai_audio_rms_dbfs[call_id] = -14.0
    runner._last_ai_audio_published_at[call_id] = second_timestamp - timedelta(
        milliseconds=8,
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "maxSnrDb": 18.74,
        "rmsRangeDb": 7.74,
        "rmsDirectionChanges": 3,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    })
    runner._record_sip_deferred_episode_observation(
        call_id=call_id,
        turn=turn,
        timestamp=second_timestamp,
        observation=second_observation,
    )

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=second_timestamp,
        observation=second_observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_ai_playback_echo_guard"
    assert decision.extra_payload["sipAiPlaybackEchoGuardEscapedBy"] == (
        "elevated_noise_compact_two_burst_turn"
    )
    assert "sipEchoGuardedCompactShortPhraseEvidence" not in decision.extra_payload


def test_realtime_agent_runner_defers_midcall_fan_under_elevated_noise_floor() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_midcall_fan_elevated_noise_guard"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-25.38,
        noise_floor_dbfs=-36.0,
        snr_db=10.62,
        peak_dbfs=-17.01,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="fixture_midcall_fan_elevated_noise_part_1",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "maxSnrDb": 10.62,
        "rmsRangeDb": 5.37,
        "rmsDirectionChanges": 3,
        "largeRmsJumpCount": 0,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_pre_stop_authority"
    assert "sipElevatedNoiseClearShortEvidence" not in decision.extra_payload


def test_realtime_agent_runner_defers_clear_short_choppy_low_snr_noise() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_clear_short_choppy_low_snr_noise"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-30.27,
        noise_floor_dbfs=-44.0,
        snr_db=13.73,
        peak_dbfs=-21.07,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="fixture_call_334831_false_clear_short_noise_cluster",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "rmsRangeDb": 10.75,
        "rmsDirectionChanges": 2,
        "largeRmsJumpCount": 2,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_authorized_pre_stop_evidence"
    assert decision.extra_payload["sipShortSpeechEvidence"] == "clear_short_modulated_burst"
    assert decision.extra_payload["sipClearShortNoiseRisk"] == "choppy_low_snr_short_burst"


def test_realtime_agent_runner_defers_clear_short_loud_low_modulation_noise() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_clear_short_loud_low_modulation_noise"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-21.71,
        noise_floor_dbfs=-44.0,
        snr_db=22.29,
        peak_dbfs=-12.51,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="fixture_call_334831_false_clear_short_noise_loud",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "rmsRangeDb": 9.8,
        "rmsDirectionChanges": 2,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_authorized_pre_stop_evidence"
    assert decision.extra_payload["sipShortSpeechEvidence"] == "clear_short_modulated_burst"
    assert decision.extra_payload["sipClearShortNoiseRisk"] == "loud_low_modulation_short_burst"


def test_realtime_agent_runner_defers_clear_short_borderline_high_noise() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_clear_short_borderline_high_noise"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-21.21,
        noise_floor_dbfs=-39.04,
        snr_db=17.82,
        peak_dbfs=-14.33,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="fixture_call_334874_borderline_high_noise_clear_short",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "maxSnrDb": 17.82,
        "rmsRangeDb": 7.19,
        "rmsDirectionChanges": 3,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_authorized_pre_stop_evidence"
    assert decision.extra_payload["sipShortSpeechEvidence"] == "clear_short_modulated_burst"
    assert decision.extra_payload["sipClearShortNoiseRisk"] == (
        "borderline_high_noise_short_burst"
    )


def test_realtime_agent_runner_defers_high_snr_single_short_noise() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_high_snr_single_short_noise"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-19.93,
        noise_floor_dbfs=-48.53,
        snr_db=28.6,
        peak_dbfs=-13.81,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="fixture_single_short_noise_high_snr",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "maxSnrDb": 28.6,
        "rmsRangeDb": 9.61,
        "rmsDirectionChanges": 3,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_pre_stop_authority"
    assert "sipShortSpeechEvidence" not in decision.extra_payload


def test_realtime_agent_runner_defers_choppy_low_rms_single_short_noise() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_choppy_low_rms_single_short_noise"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-31.81,
        noise_floor_dbfs=-44.66,
        snr_db=12.85,
        peak_dbfs=-25.05,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="fixture_single_short_noise_low_rms",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "maxSnrDb": 12.85,
        "rmsRangeDb": 7.61,
        "rmsDirectionChanges": 5,
        "largeRmsJumpCount": 0,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_pre_stop_authority"
    assert "sipShortSpeechEvidence" not in decision.extra_payload


def test_realtime_agent_runner_defers_clear_short_modulated_local_speech_during_opening() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_clear_short_modulated_opening_guard"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    runner._response_lifecycle(call_id).current_response_is_opening = True
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-30.1,
        noise_floor_dbfs=-44.0,
        snr_db=13.9,
        peak_dbfs=-21.0,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "rmsRangeDb": 6.43,
        "rmsDirectionChanges": 1,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_opening_pre_stop_authority"
    assert decision.extra_payload["sipShortSpeechEvidence"] == "clear_short_modulated_burst"


def test_realtime_agent_runner_defers_opening_echo_guarded_turn_evidence() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_opening_echo_guarded_turn_evidence"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    runner._response_lifecycle(call_id).current_response_is_opening = True
    runner._last_ai_audio_rms_dbfs[call_id] = -23.99
    runner._last_ai_audio_published_at[call_id] = now - timedelta(milliseconds=2)
    guard = runner._playback_guard(call_id)
    turn = PendingUserTurn()
    turn.sip_echo_guarded_turn_response_id = guard.current_response_id
    turn.sip_echo_guarded_turn_generation = guard.generation
    turn.sip_echo_guarded_turn_first_at = now - timedelta(milliseconds=400)
    turn.sip_echo_guarded_turn_last_at = now - timedelta(milliseconds=400)
    turn.sip_echo_guarded_turn_burst_count = 1
    turn.sip_echo_guarded_turn_voiced_ms = 180
    turn.sip_echo_guarded_turn_current_burst_voiced_ms = 180
    turn.sip_echo_guarded_turn_min_rms_dbfs = -31.13
    turn.sip_echo_guarded_turn_max_rms_dbfs = -31.13
    turn.sip_echo_guarded_turn_max_snr_db = 16.87
    turn.sip_echo_guarded_turn_max_rms_range_db = 7.65
    observation = SipBargeInObservation(
        active=True,
        candidate=False,
        rms_dbfs=-28.85,
        noise_floor_dbfs=-48.0,
        snr_db=19.15,
        peak_dbfs=-20.81,
        vad_voiced_ms=240,
        candidate_duration_ms=240,
        speech_duration_ms=240,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="fixture_opening_fan_part_2",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "maxSnrDb": 19.15,
        "rmsRangeDb": 7.65,
        "rmsDirectionChanges": 4,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_opening_pre_stop_authority"
    assert decision.extra_payload["sipEchoGuardedTurnBurstCount"] == 2
    assert decision.extra_payload["sipEchoGuardedTurnVoicedMs"] == 420


def test_realtime_agent_runner_defers_sub_480ms_local_only_fast_speech() -> None:
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    call_id = "call_sip_sub_480ms_local_only_fast_speech"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-18.5,
        noise_floor_dbfs=-44.0,
        snr_db=30.0,
        peak_dbfs=-10.1,
        vad_voiced_ms=460,
        candidate_duration_ms=460,
        speech_duration_ms=460,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector(
        {
            "rmsRangeDb": 8.0,
            "rmsDirectionChanges": 1,
            "largeRmsJumpCount": 0,
            "speechQualityRejection": None,
        },
        fast_local=True,
        pre_stop_local=True,
    )

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_authorized_pre_stop_evidence"


def test_realtime_agent_runner_defers_noise_shaped_local_only_turn_cluster() -> None:
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    call_id = "call_sip_noise_shaped_turn_cluster"
    response_id = "resp_noise_cluster"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(
        runner,
        call_id,
        response_id=response_id,
    )
    turn = PendingUserTurn(
        sip_turn_cluster_response_id=response_id,
        sip_turn_cluster_first_at=now - timedelta(milliseconds=259),
        sip_turn_cluster_last_at=now,
        sip_turn_cluster_burst_count=2,
        sip_turn_cluster_voiced_ms=360,
        sip_turn_cluster_max_snr_db=25.96,
        sip_turn_cluster_max_rms_range_db=11.26,
    )
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-26.74,
        noise_floor_dbfs=-39.4,
        snr_db=12.66,
        peak_dbfs=-19.01,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "rmsRangeDb": 8.46,
        "rmsDirectionChanges": 5,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_pre_stop_authority"


def test_realtime_agent_runner_defers_contaminated_long_webrtc_shadow_authority() -> None:
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    call_id = "call_sip_contaminated_long_shadow"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-19.24,
        noise_floor_dbfs=-39.4,
        snr_db=20.16,
        peak_dbfs=-13.14,
        vad_voiced_ms=200,
        candidate_duration_ms=200,
        speech_duration_ms=200,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "rmsRangeDb": 7.2,
        "rmsDirectionChanges": 5,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
        shadow_observations=[
            SipVadShadowObservation(
                active=True,
                started=False,
                ended=False,
                duration_ms=6340,
                frame_duration_ms=20,
                detector="webrtc_shadow",
            ),
        ],
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_pre_stop_authority"
    assert "sipVadShadowEvidence" not in decision.extra_payload


def test_realtime_agent_runner_defers_short_continuous_shadow_context() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_short_continuous_shadow_context"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn(
        sip_turn_cluster_first_at=now - timedelta(milliseconds=458),
        sip_turn_cluster_last_at=now,
        sip_turn_cluster_burst_count=2,
        sip_turn_cluster_voiced_ms=360,
        sip_turn_cluster_max_snr_db=31.37,
        sip_turn_cluster_max_rms_range_db=17.9,
    )
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-29.65,
        noise_floor_dbfs=-49.0,
        snr_db=19.35,
        peak_dbfs=-23.27,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "rmsRangeDb": 14.29,
        "rmsDirectionChanges": 3,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
        shadow_observations=[
            SipVadShadowObservation(
                active=True,
                started=False,
                ended=False,
                duration_ms=3820,
                frame_duration_ms=20,
                detector="webrtc_shadow",
            ),
        ],
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_pre_stop_authority"
    assert decision.extra_payload["sipVadShadowEvidence"] == (
        "realtime_webrtc_shadow_continuous_context"
    )


def test_realtime_agent_runner_defers_unstable_shadow_local_modulation() -> None:
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    call_id = "call_sip_unstable_shadow_local_modulation"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-19.16,
        noise_floor_dbfs=-48.0,
        snr_db=28.84,
        peak_dbfs=-12.56,
        vad_voiced_ms=360,
        candidate_duration_ms=360,
        speech_duration_ms=360,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "rmsRangeDb": 18.16,
        "rmsDirectionChanges": 6,
        "largeRmsJumpCount": 5,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
        shadow_observations=[
            SipVadShadowObservation(
                active=True,
                started=True,
                ended=False,
                duration_ms=360,
                frame_duration_ms=20,
                detector="webrtc_shadow",
            ),
        ],
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_speech_like_continuity"
    assert decision.extra_payload["speechQualityRejection"] == "unstable_local_envelope"


def test_realtime_agent_runner_defers_short_shadow_turn_cluster_until_duration_gate() -> None:
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    call_id = "call_sip_short_shadow_turn_cluster_duration_gate"
    response_id = "resp_shadow_turn_cluster"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id, response_id=response_id)
    turn = PendingUserTurn(
        sip_turn_cluster_response_id=response_id,
        sip_turn_cluster_first_at=now - timedelta(milliseconds=260),
        sip_turn_cluster_last_at=now,
        sip_turn_cluster_burst_count=2,
        sip_turn_cluster_voiced_ms=360,
        sip_turn_cluster_shadow_burst_count=1,
        sip_turn_cluster_shadow_voiced_ms=360,
        sip_turn_cluster_shadow_detector="webrtc_shadow",
        sip_turn_cluster_shadow_window_ms=720,
        sip_turn_cluster_max_snr_db=21.0,
        sip_turn_cluster_max_rms_range_db=7.5,
    )
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-19.0,
        noise_floor_dbfs=-39.0,
        snr_db=20.0,
        peak_dbfs=-12.0,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "rmsRangeDb": 7.5,
        "rmsDirectionChanges": 1,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
        shadow_observations=[
            SipVadShadowObservation(
                active=True,
                started=True,
                ended=False,
                duration_ms=720,
                frame_duration_ms=20,
                detector="webrtc_shadow",
            ),
        ],
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_pre_stop_authority"


def test_realtime_agent_runner_defers_compact_two_burst_deferred_turn_at_live_snr() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=12.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_compact_two_burst_live_snr"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "rmsRangeDb": 7.2,
        "rmsDirectionChanges": 1,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    })
    observations = [
        (
            now - timedelta(milliseconds=2258),
            SipBargeInObservation(
                active=True,
                candidate=True,
                rms_dbfs=-22.13,
                noise_floor_dbfs=-38.55,
                snr_db=16.42,
                peak_dbfs=-14.86,
                vad_voiced_ms=180,
                candidate_duration_ms=180,
                speech_duration_ms=180,
                frame_duration_ms=20,
                candidate_class="stable_speech_candidate",
                reason="sip_uplink_speech_during_ai_audio",
            ),
        ),
        (
            now,
            SipBargeInObservation(
                active=True,
                candidate=True,
                rms_dbfs=-22.84,
                noise_floor_dbfs=-38.55,
                snr_db=15.71,
                peak_dbfs=-15.57,
                vad_voiced_ms=180,
                candidate_duration_ms=180,
                speech_duration_ms=180,
                frame_duration_ms=20,
                candidate_class="stable_speech_candidate",
                reason="sip_uplink_speech_during_ai_audio",
            ),
        ),
    ]
    for timestamp, observation in observations:
        runner._record_sip_deferred_episode_observation(
            call_id=call_id,
            turn=turn,
            timestamp=timestamp,
            observation=observation,
        )

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observations[-1][1],
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_pre_stop_authority"
    assert decision.extra_payload["sipDeferredEpisodeEvidence"] == "compact_two_burst_turn"
    assert decision.extra_payload["sipDeferredEpisodeBurstCount"] == 2
    assert decision.extra_payload["sipDeferredEpisodeVoicedMs"] == 360
    assert decision.extra_payload["sipDeferredEpisodeMaxGapMs"] >= 2200


def test_realtime_agent_runner_ignores_deferred_episode_seed_without_playback_target() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=12.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_deferred_episode_seed_without_playback_target"
    response_id = "resp_compact_false_seed"
    now = datetime.now(timezone.utc)
    guard = runner._playback_guard(call_id)
    guard.current_response_id = response_id
    guard.current_response_audio_published = False
    turn = PendingUserTurn()
    detector = _StaticSipAuthorityDetector({
        "maxSnrDb": 22.74,
        "rmsRangeDb": 12.62,
        "rmsDirectionChanges": 3,
        "largeRmsJumpCount": 2,
        "speechQualityRejection": None,
    })
    runner._sip_barge_in_detector = detector

    first_observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-27.89,
        noise_floor_dbfs=-40.14,
        snr_db=12.24,
        peak_dbfs=-21.5,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._record_sip_deferred_episode_observation(
        call_id=call_id,
        turn=turn,
        timestamp=now - timedelta(milliseconds=2179),
        observation=first_observation,
    )

    guard.current_response_audio_published = True
    detector.payload = {
        "maxSnrDb": 18.38,
        "rmsRangeDb": 7.55,
        "rmsDirectionChanges": 4,
        "largeRmsJumpCount": 0,
        "speechQualityRejection": None,
    }
    second_observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-21.76,
        noise_floor_dbfs=-40.14,
        snr_db=18.38,
        peak_dbfs=-15.03,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._record_sip_deferred_episode_observation(
        call_id=call_id,
        turn=turn,
        timestamp=now,
        observation=second_observation,
    )

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=second_observation,
    )

    assert decision.action == "defer"
    assert "sipDeferredEpisodeEvidence" not in decision.extra_payload
    assert turn.sip_deferred_episode_burst_count == 1


def test_realtime_agent_runner_defers_opening_local_only_stable_speech() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=12.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_opening_local_only_stable_speech"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id, response_id="resp_opening")
    runner._response_lifecycle(call_id).current_response_is_opening = True
    runner._last_ai_audio_rms_dbfs[call_id] = -23.0
    runner._last_ai_audio_published_at[call_id] = now - timedelta(milliseconds=80)
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-16.7,
        noise_floor_dbfs=-37.24,
        snr_db=20.54,
        peak_dbfs=-11.84,
        vad_voiced_ms=860,
        candidate_duration_ms=860,
        speech_duration_ms=860,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector(
        {
            "rmsRangeDb": 11.63,
            "rmsDirectionChanges": 19,
            "largeRmsJumpCount": 10,
            "speechQualityRejection": None,
        },
        fast_local=True,
        pre_stop_local=True,
    )

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_opening_pre_stop_authority"


def test_realtime_agent_runner_defers_opening_local_only_turn_cluster() -> None:
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    call_id = "call_sip_opening_local_only_turn_cluster"
    response_id = "resp_opening_cluster"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(
        runner,
        call_id,
        response_id=response_id,
    )
    runner._response_lifecycle(call_id).current_response_is_opening = True
    turn = PendingUserTurn(
        sip_turn_cluster_response_id=response_id,
        sip_turn_cluster_first_at=now - timedelta(milliseconds=60),
        sip_turn_cluster_last_at=now,
        sip_turn_cluster_burst_count=4,
        sip_turn_cluster_voiced_ms=840,
        sip_turn_cluster_max_snr_db=32.6,
        sip_turn_cluster_max_rms_range_db=6.5,
    )
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-23.05,
        noise_floor_dbfs=-50.0,
        snr_db=26.95,
        peak_dbfs=-15.54,
        vad_voiced_ms=240,
        candidate_duration_ms=240,
        speech_duration_ms=240,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "rmsRangeDb": 6.5,
        "rmsDirectionChanges": 4,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_opening_pre_stop_authority"
    assert decision.extra_payload["sipOpeningGuardedEvidence"] == "turn_cluster"


def test_realtime_agent_runner_defers_opening_strong_stable_local_speech() -> None:
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    call_id = "call_sip_opening_strong_stable_local"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(
        runner,
        call_id,
        response_id="resp_opening_stable",
    )
    runner._response_lifecycle(call_id).current_response_is_opening = True
    turn = PendingUserTurn(
        sip_deferred_episode_max_rms_dbfs=-17.96,
    )
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-24.24,
        noise_floor_dbfs=-50.0,
        snr_db=25.76,
        peak_dbfs=-18.98,
        vad_voiced_ms=440,
        candidate_duration_ms=440,
        speech_duration_ms=440,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector(
        {
            "rmsRangeDb": 8.46,
            "rmsDirectionChanges": 9,
            "largeRmsJumpCount": 4,
            "speechQualityRejection": None,
        },
        pre_stop_local=True,
    )

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_opening_pre_stop_authority"
    assert decision.extra_payload["sipOpeningGuardedEvidence"] == "stable_local_speech"


def test_realtime_agent_runner_defers_opening_local_only_when_lifecycle_flag_is_stale() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    store = InMemoryEventStore()
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=store,
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=12.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_opening_local_only_stale_lifecycle"
    now = datetime.now(timezone.utc)
    store.append(
        call_id,
        "opening_started",
        "agent",
        {"openingMessageHash": "sha256:test"},
        timestamp=now - timedelta(seconds=5),
    )
    _mark_sip_authority_playback_target(runner, call_id, response_id="resp_opening")
    runner._response_lifecycle(call_id).current_response_is_opening = False
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-16.7,
        noise_floor_dbfs=-37.24,
        snr_db=20.54,
        peak_dbfs=-11.84,
        vad_voiced_ms=860,
        candidate_duration_ms=860,
        speech_duration_ms=860,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector(
        {
            "rmsRangeDb": 11.63,
            "rmsDirectionChanges": 19,
            "largeRmsJumpCount": 10,
            "speechQualityRejection": None,
        },
        fast_local=True,
        pre_stop_local=True,
    )

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_opening_pre_stop_authority"


def test_realtime_agent_runner_defers_echo_guarded_compact_two_burst_deferred_turn() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=12.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_echo_guard_compact_two_burst_deferred"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    runner._last_ai_audio_rms_dbfs[call_id] = -20.0
    runner._last_ai_audio_published_at[call_id] = now - timedelta(milliseconds=30)
    turn = PendingUserTurn()
    detector = _StaticSipAuthorityDetector({
        "maxSnrDb": 17.96,
        "rmsRangeDb": 7.43,
        "rmsDirectionChanges": 1,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    })
    runner._sip_barge_in_detector = detector
    observations = [
        (
            now - timedelta(milliseconds=2559),
            SipBargeInObservation(
                active=True,
                candidate=True,
                rms_dbfs=-18.16,
                noise_floor_dbfs=-36.12,
                snr_db=17.96,
                peak_dbfs=-9.94,
                vad_voiced_ms=180,
                candidate_duration_ms=180,
                speech_duration_ms=180,
                frame_duration_ms=20,
                candidate_class="stable_speech_candidate",
                reason="sip_uplink_speech_during_ai_audio",
            ),
        ),
        (
            now,
            SipBargeInObservation(
                active=True,
                candidate=True,
                rms_dbfs=-16.8,
                noise_floor_dbfs=-36.12,
                snr_db=19.32,
                peak_dbfs=-7.94,
                vad_voiced_ms=180,
                candidate_duration_ms=180,
                speech_duration_ms=180,
                frame_duration_ms=20,
                candidate_class="stable_speech_candidate",
                reason="sip_uplink_speech_during_ai_audio",
            ),
        ),
    ]
    for index, (timestamp, observation) in enumerate(observations):
        if index == 1:
            detector.payload = {
                "maxSnrDb": 19.32,
                "rmsRangeDb": 5.98,
                "rmsDirectionChanges": 1,
                "largeRmsJumpCount": 1,
                "speechQualityRejection": None,
            }
        runner._record_sip_deferred_episode_observation(
            call_id=call_id,
            turn=turn,
            timestamp=timestamp,
            observation=observation,
        )

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observations[-1][1],
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_ai_playback_echo_guard"
    assert decision.extra_payload["sipDeferredEpisodeEvidence"] == "compact_two_burst_turn"
    assert decision.extra_payload["sipAiPlaybackEchoGuardEscapedBy"] == "compact_two_burst_turn"


def test_realtime_agent_runner_defers_elevated_noise_compact_two_burst_episode_without_cross_evidence() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=12.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_elevated_noise_compact_two_burst_deferred"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    detector = _StaticSipAuthorityDetector({
        "maxSnrDb": 14.37,
        "rmsRangeDb": 6.38,
        "rmsDirectionChanges": 1,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    })
    runner._sip_barge_in_detector = detector
    first_observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-23.51,
        noise_floor_dbfs=-37.88,
        snr_db=14.37,
        peak_dbfs=-16.43,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._record_sip_deferred_episode_observation(
        call_id=call_id,
        turn=turn,
        timestamp=now - timedelta(milliseconds=1200),
        observation=first_observation,
    )

    detector.payload = {
        "maxSnrDb": 20.75,
        "rmsRangeDb": 8.1,
        "rmsDirectionChanges": 2,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    }
    current_observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-17.13,
        noise_floor_dbfs=-37.88,
        snr_db=20.75,
        peak_dbfs=-8.65,
        vad_voiced_ms=300,
        candidate_duration_ms=300,
        speech_duration_ms=300,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._record_sip_deferred_episode_observation(
        call_id=call_id,
        turn=turn,
        timestamp=now,
        observation=current_observation,
    )

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=current_observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_pre_stop_authority"
    assert decision.extra_payload["sipDeferredEpisodeEvidence"] == (
        "elevated_noise_compact_two_burst_turn"
    )
    assert decision.extra_payload["sipDeferredEpisodeBurstCount"] == 2


def test_realtime_agent_runner_defers_ai_receded_compact_turn_under_high_noise() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_ai_receded_compact_turn_high_noise"
    started_at = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(
        runner,
        call_id,
        response_id="resp_ai_receded_compact_turn_high_noise",
    )
    turn = PendingUserTurn()
    detector = _StaticSipAuthorityDetector({
        "maxSnrDb": 11.8,
        "rmsRangeDb": 1.88,
        "rmsDirectionChanges": 4,
        "largeRmsJumpCount": 0,
        "speechQualityRejection": None,
    })
    runner._sip_barge_in_detector = detector
    first_observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-24.02,
        noise_floor_dbfs=-35.81,
        snr_db=11.8,
        peak_dbfs=-19.59,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="fixture_call_334874_ai_receded_part_1",
    )
    runner._record_sip_deferred_episode_observation(
        call_id=call_id,
        turn=turn,
        timestamp=started_at,
        observation=first_observation,
    )

    current_timestamp = started_at + timedelta(milliseconds=825)
    current_observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-23.61,
        noise_floor_dbfs=-35.81,
        snr_db=12.2,
        peak_dbfs=-18.92,
        vad_voiced_ms=220,
        candidate_duration_ms=220,
        speech_duration_ms=220,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="fixture_call_334874_ai_receded_part_2",
    )
    runner._last_ai_audio_rms_dbfs[call_id] = -31.0
    runner._last_ai_audio_published_at[call_id] = current_timestamp - timedelta(
        milliseconds=30,
    )
    detector.payload = {
        "maxSnrDb": 16.6,
        "rmsRangeDb": 5.64,
        "rmsDirectionChanges": 5,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    }
    runner._record_sip_deferred_episode_observation(
        call_id=call_id,
        turn=turn,
        timestamp=current_timestamp,
        observation=current_observation,
    )

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=current_timestamp,
        observation=current_observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_pre_stop_authority"
    assert decision.extra_payload["sipDeferredEpisodeEvidence"] == (
        "ai_receded_compact_two_burst_turn"
    )
    assert decision.extra_payload["sipAiRecededCompactTurnEvidence"] == (
        "two_burst_modulated_tail_after_echo_guard"
    )
    assert decision.extra_payload["sipAiRecededCompactTurnNoiseRisk"] == (
        "short_or_over_modulated_tail_under_elevated_noise"
    )


def test_realtime_agent_runner_promotes_sparse_deferred_sip_turn_evidence() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_sparse_deferred_turn"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "rmsRangeDb": 8.4,
        "rmsDirectionChanges": 3,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    })
    observations = [
        (
            now - timedelta(milliseconds=17900),
            SipBargeInObservation(
                active=True,
                candidate=True,
                rms_dbfs=-19.65,
                noise_floor_dbfs=-39.4,
                snr_db=13.97,
                peak_dbfs=-12.4,
                vad_voiced_ms=180,
                candidate_duration_ms=180,
                speech_duration_ms=180,
                frame_duration_ms=20,
                candidate_class="stable_speech_candidate",
                reason="sip_uplink_speech_during_ai_audio",
            ),
        ),
        (
            now - timedelta(milliseconds=9540),
            SipBargeInObservation(
                active=True,
                candidate=True,
                rms_dbfs=-23.36,
                noise_floor_dbfs=-39.4,
                snr_db=10.26,
                peak_dbfs=-16.2,
                vad_voiced_ms=180,
                candidate_duration_ms=180,
                speech_duration_ms=180,
                frame_duration_ms=20,
                candidate_class="stable_speech_candidate",
                reason="sip_uplink_speech_during_ai_audio",
            ),
        ),
        (
            now,
            SipBargeInObservation(
                active=True,
                candidate=True,
                rms_dbfs=-22.61,
                noise_floor_dbfs=-39.4,
                snr_db=11.01,
                peak_dbfs=-15.8,
                vad_voiced_ms=180,
                candidate_duration_ms=180,
                speech_duration_ms=180,
                frame_duration_ms=20,
                candidate_class="stable_speech_candidate",
                reason="sip_uplink_speech_during_ai_audio",
            ),
        ),
    ]
    for timestamp, observation in observations:
        runner._record_sip_deferred_episode_observation(
            call_id=call_id,
            turn=turn,
            timestamp=timestamp,
            observation=observation,
        )

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observations[-1][1],
    )

    assert decision.action == "pre_stop"
    assert decision.evidence == "deferred_multi_candidate_turn"
    assert decision.extra_payload["sipDeferredEpisodeBurstCount"] == 3
    assert decision.extra_payload["sipDeferredEpisodeVoicedMs"] == 540
    assert decision.extra_payload["sipDeferredEpisodeMaxGapMs"] >= 9000


def test_realtime_agent_runner_defers_sparse_deferred_turn_under_elevated_noise_floor() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_sparse_deferred_turn_high_noise"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "rmsRangeDb": 8.4,
        "rmsDirectionChanges": 3,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    })
    observations = [
        (
            now - timedelta(milliseconds=17900),
            SipBargeInObservation(
                active=True,
                candidate=True,
                rms_dbfs=-19.65,
                noise_floor_dbfs=-33.2,
                snr_db=13.97,
                peak_dbfs=-12.4,
                vad_voiced_ms=180,
                candidate_duration_ms=180,
                speech_duration_ms=180,
                frame_duration_ms=20,
                candidate_class="stable_speech_candidate",
                reason="sip_uplink_speech_during_ai_audio",
            ),
        ),
        (
            now - timedelta(milliseconds=9540),
            SipBargeInObservation(
                active=True,
                candidate=True,
                rms_dbfs=-23.36,
                noise_floor_dbfs=-33.2,
                snr_db=10.26,
                peak_dbfs=-16.2,
                vad_voiced_ms=180,
                candidate_duration_ms=180,
                speech_duration_ms=180,
                frame_duration_ms=20,
                candidate_class="stable_speech_candidate",
                reason="sip_uplink_speech_during_ai_audio",
            ),
        ),
        (
            now,
            SipBargeInObservation(
                active=True,
                candidate=True,
                rms_dbfs=-22.61,
                noise_floor_dbfs=-33.2,
                snr_db=11.01,
                peak_dbfs=-15.8,
                vad_voiced_ms=180,
                candidate_duration_ms=180,
                speech_duration_ms=180,
                frame_duration_ms=20,
                candidate_class="stable_speech_candidate",
                reason="sip_uplink_speech_during_ai_audio",
            ),
        ),
    ]
    for timestamp, observation in observations:
        runner._record_sip_deferred_episode_observation(
            call_id=call_id,
            turn=turn,
            timestamp=timestamp,
            observation=observation,
        )

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observations[-1][1],
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_pre_stop_authority"


def test_realtime_agent_runner_defers_high_noise_sparse_deferred_turn_with_marginal_current_snr() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_high_noise_sparse_deferred_turn_marginal_snr"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "rmsRangeDb": 10.3,
        "rmsDirectionChanges": 9,
        "largeRmsJumpCount": 5,
        "speechQualityRejection": None,
    })
    observations = [
        (
            now - timedelta(milliseconds=4560),
            SipBargeInObservation(
                active=True,
                candidate=True,
                rms_dbfs=-16.56,
                noise_floor_dbfs=-34.81,
                snr_db=18.25,
                peak_dbfs=-12.05,
                vad_voiced_ms=180,
                candidate_duration_ms=180,
                speech_duration_ms=180,
                frame_duration_ms=20,
                candidate_class="stable_speech_candidate",
                reason="sip_uplink_speech_during_ai_audio",
            ),
        ),
        (
            now - timedelta(milliseconds=1642),
            SipBargeInObservation(
                active=True,
                candidate=True,
                rms_dbfs=-15.37,
                noise_floor_dbfs=-34.81,
                snr_db=19.44,
                peak_dbfs=-10.3,
                vad_voiced_ms=180,
                candidate_duration_ms=180,
                speech_duration_ms=180,
                frame_duration_ms=20,
                candidate_class="stable_speech_candidate",
                reason="sip_uplink_speech_during_ai_audio",
            ),
        ),
        (
            now,
            SipBargeInObservation(
                active=True,
                candidate=True,
                rms_dbfs=-23.37,
                noise_floor_dbfs=-34.81,
                snr_db=11.45,
                peak_dbfs=-15.96,
                vad_voiced_ms=360,
                candidate_duration_ms=360,
                speech_duration_ms=360,
                frame_duration_ms=20,
                candidate_class="stable_speech_candidate",
                reason="sip_uplink_speech_during_ai_audio",
            ),
        ),
    ]
    for timestamp, observation in observations:
        runner._record_sip_deferred_episode_observation(
            call_id=call_id,
            turn=turn,
            timestamp=timestamp,
            observation=observation,
        )

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observations[-1][1],
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_speech_quality"


def test_realtime_agent_runner_promotes_elevated_noise_sparse_turn_with_strong_anchor() -> None:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=12.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_elevated_noise_sparse_turn_strong_anchor"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    detector = _StaticSipAuthorityDetector({
        "maxSnrDb": 23.15,
        "rmsRangeDb": 5.02,
        "rmsDirectionChanges": 2,
        "largeRmsJumpCount": 0,
        "speechQualityRejection": None,
    })
    runner._sip_barge_in_detector = detector
    observations = [
        (
            now - timedelta(milliseconds=6404),
            SipBargeInObservation(
                active=True,
                candidate=True,
                rms_dbfs=-12.87,
                noise_floor_dbfs=-36.01,
                snr_db=23.15,
                peak_dbfs=-4.94,
                vad_voiced_ms=180,
                candidate_duration_ms=180,
                speech_duration_ms=180,
                frame_duration_ms=20,
                candidate_class="stable_speech_candidate",
                reason="sip_uplink_speech_during_ai_audio",
            ),
            {
                "maxSnrDb": 23.15,
                "rmsRangeDb": 5.02,
                "rmsDirectionChanges": 2,
                "largeRmsJumpCount": 0,
                "speechQualityRejection": None,
            },
        ),
        (
            now - timedelta(milliseconds=2663),
            SipBargeInObservation(
                active=True,
                candidate=True,
                rms_dbfs=-21.49,
                noise_floor_dbfs=-36.01,
                snr_db=14.53,
                peak_dbfs=-15.23,
                vad_voiced_ms=180,
                candidate_duration_ms=180,
                speech_duration_ms=180,
                frame_duration_ms=20,
                candidate_class="stable_speech_candidate",
                reason="sip_uplink_speech_during_ai_audio",
            ),
            {
                "maxSnrDb": 14.53,
                "rmsRangeDb": 6.58,
                "rmsDirectionChanges": 1,
                "largeRmsJumpCount": 1,
                "speechQualityRejection": None,
            },
        ),
        (
            now,
            SipBargeInObservation(
                active=True,
                candidate=True,
                rms_dbfs=-19.7,
                noise_floor_dbfs=-36.01,
                snr_db=16.32,
                peak_dbfs=-12.97,
                vad_voiced_ms=180,
                candidate_duration_ms=180,
                speech_duration_ms=180,
                frame_duration_ms=20,
                candidate_class="stable_speech_candidate",
                reason="sip_uplink_speech_during_ai_audio",
            ),
            {
                "maxSnrDb": 16.32,
                "rmsRangeDb": 7.09,
                "rmsDirectionChanges": 2,
                "largeRmsJumpCount": 1,
                "speechQualityRejection": None,
            },
        ),
    ]
    for timestamp, observation, diagnostics in observations:
        detector.payload = diagnostics
        runner._record_sip_deferred_episode_observation(
            call_id=call_id,
            turn=turn,
            timestamp=timestamp,
            observation=observation,
        )

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observations[-1][1],
    )

    assert decision.action == "pre_stop"
    assert decision.evidence == "deferred_multi_candidate_turn"
    assert decision.extra_payload["sipDeferredEpisodeEvidence"] == "sparse_multi_candidate_turn"
    assert decision.extra_payload["sipDeferredEpisodeBurstCount"] == 3
    assert decision.extra_payload["sipDeferredEpisodeMaxGapMs"] >= 3700
    assert decision.extra_payload["sipElevatedNoiseSparseTurnEvidence"] == (
        "strong_anchor_current_modulation"
    )


def test_realtime_agent_runner_defers_echo_like_shadow_candidate_without_local_authority() -> None:
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    call_id = "call_sip_echo_like_shadow_candidate"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    runner._last_ai_audio_rms_dbfs[call_id] = -23.25
    runner._last_ai_audio_published_at[call_id] = now - timedelta(milliseconds=32)
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-20.47,
        noise_floor_dbfs=-39.4,
        snr_db=18.93,
        peak_dbfs=-12.44,
        vad_voiced_ms=300,
        candidate_duration_ms=300,
        speech_duration_ms=300,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "rmsRangeDb": 14.6,
        "rmsDirectionChanges": 4,
        "largeRmsJumpCount": 3,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
        shadow_observations=[
            SipVadShadowObservation(
                active=True,
                started=True,
                ended=False,
                duration_ms=720,
                frame_duration_ms=20,
                detector="webrtc_shadow",
            ),
        ],
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_ai_playback_echo_guard"


def test_realtime_agent_runner_defers_compact_modulated_echo_guarded_local_only_sip_turn() -> None:
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    call_id = "call_sip_compact_modulated_echo_guard"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    runner._last_ai_audio_rms_dbfs[call_id] = -24.74
    runner._last_ai_audio_published_at[call_id] = now - timedelta(milliseconds=2)
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=False,
        rms_dbfs=-20.44,
        noise_floor_dbfs=-39.85,
        snr_db=19.41,
        peak_dbfs=-12.12,
        vad_voiced_ms=260,
        candidate_duration_ms=260,
        speech_duration_ms=260,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="speech_active_below_candidate_duration",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "maxSnrDb": 19.41,
        "rmsRangeDb": 10.69,
        "rmsDirectionChanges": 5,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_ai_playback_echo_guard"
    assert decision.extra_payload["sipEchoGuardedLocalEvidence"] == (
        "compact_modulated_micro_confirmed"
    )
    assert decision.extra_payload["sipEchoGuardedTurnVoicedMs"] == 260
    assert decision.extra_payload["sipEchoGuardedTurnRmsRangeDb"] == 10.69
    assert decision.extra_payload["sipEchoGuardedTurnMaxSnrDb"] == 19.41


def test_realtime_agent_runner_defers_marginal_high_noise_echo_guarded_turn() -> None:
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    call_id = "call_sip_marginal_high_noise_echo_guarded_turn"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    guard = runner._playback_guard(call_id)
    runner._last_ai_audio_rms_dbfs[call_id] = -17.29
    runner._last_ai_audio_published_at[call_id] = now - timedelta(milliseconds=30)
    turn = PendingUserTurn(
        sip_echo_guarded_turn_response_id=guard.current_response_id,
        sip_echo_guarded_turn_generation=guard.generation,
        sip_echo_guarded_turn_first_at=now - timedelta(milliseconds=882),
        sip_echo_guarded_turn_last_at=now,
        sip_echo_guarded_turn_burst_count=2,
        sip_echo_guarded_turn_voiced_ms=480,
        sip_echo_guarded_turn_min_rms_dbfs=-28.95,
        sip_echo_guarded_turn_max_rms_dbfs=-21.95,
        sip_echo_guarded_turn_max_snr_db=17.04,
        sip_echo_guarded_turn_max_rms_range_db=7.0,
    )
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-21.95,
        noise_floor_dbfs=-34.81,
        snr_db=12.87,
        peak_dbfs=-14.33,
        vad_voiced_ms=240,
        candidate_duration_ms=240,
        speech_duration_ms=240,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "maxSnrDb": 17.04,
        "rmsRangeDb": 7.0,
        "rmsDirectionChanges": 6,
        "largeRmsJumpCount": 3,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_ai_playback_echo_guard"


@pytest.mark.anyio
async def test_realtime_agent_runner_defers_opening_echo_guarded_compact_local_micro_confirm() -> None:
    registry = InMemorySessionRegistry()
    provider = FakeRealtimeProvider([])
    call_id = "call_sip_opening_echo_guarded_compact"
    session = _sip_session(call_id)
    session.effective_config["opening_message"] = "您好张总，请问现在方便吗？"
    registry.add(session)
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=InMemoryEventStore(),
    )
    now = datetime.now(timezone.utc)

    await runner.start(session)
    await runner.start_opening(call_id)
    runner._mark_response_started(call_id, {"response_id": "resp_opening"})
    guard = runner._playback_guard(call_id)
    guard.current_response_audio_published = True
    runner._last_ai_audio_rms_dbfs[call_id] = -23.99
    runner._last_ai_audio_published_at[call_id] = now - timedelta(milliseconds=2)
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=False,
        rms_dbfs=-28.85,
        noise_floor_dbfs=-48.0,
        snr_db=19.15,
        peak_dbfs=-20.81,
        vad_voiced_ms=240,
        candidate_duration_ms=240,
        speech_duration_ms=240,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="speech_active_below_candidate_duration",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "maxSnrDb": 21.86,
        "rmsRangeDb": 7.65,
        "rmsDirectionChanges": 4,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_opening_pre_stop_authority"
    assert decision.extra_payload["sipEchoGuardedLocalEvidence"] == (
        "compact_modulated_micro_confirmed"
    )


def test_realtime_agent_runner_defers_high_noise_echo_guarded_single_short_local_only_micro_confirm() -> None:
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    call_id = "call_sip_high_noise_echo_guard_single_short"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    runner._last_ai_audio_rms_dbfs[call_id] = -19.86
    runner._last_ai_audio_published_at[call_id] = now - timedelta(milliseconds=29)
    turn = PendingUserTurn(sip_single_short_pre_stop_evidence=True)
    observation = SipBargeInObservation(
        active=True,
        candidate=False,
        rms_dbfs=-21.11,
        noise_floor_dbfs=-36.63,
        snr_db=15.52,
        peak_dbfs=-11.31,
        vad_voiced_ms=280,
        candidate_duration_ms=280,
        speech_duration_ms=280,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="speech_active_below_candidate_duration",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "maxSnrDb": 21.22,
        "rmsRangeDb": 6.69,
        "rmsDirectionChanges": 3,
        "largeRmsJumpCount": 1,
        "speechQualityRejection": None,
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_ai_playback_echo_guard"
    assert decision.extra_payload["sipEchoGuardedLocalEvidence"] == (
        "single_short_micro_confirmed"
    )
    assert decision.extra_payload["sipEchoGuardedTurnVoicedMs"] == 280
    assert decision.extra_payload["sipEchoGuardedTurnMaxSnrDb"] == 21.22


def _decide_low_snr_echo_guarded_deferred_episode(
    *,
    current_noise_floor_dbfs: float | None = -35.63,
    ai_rms_dbfs: float = -26.02,
) -> Any:
    from app.services.ai_call.sip_barge_in import SipBargeInConfig

    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
        sip_barge_in_config=SipBargeInConfig(
            rms_threshold_dbfs=-36.0,
            snr_threshold_db=10.0,
            vad_voiced_duration_ms=120,
            candidate_min_duration_ms=180,
            pre_stop_min_duration_ms=240,
        ),
    )
    call_id = "call_sip_low_snr_echo_guard_deferred_episode"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    runner._last_ai_audio_rms_dbfs[call_id] = ai_rms_dbfs
    runner._last_ai_audio_published_at[call_id] = now - timedelta(milliseconds=30)
    turn = PendingUserTurn()
    first_observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-19.47,
        noise_floor_dbfs=current_noise_floor_dbfs,
        snr_db=16.16,
        peak_dbfs=-12.5,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    current_observation = SipBargeInObservation(
        active=True,
        candidate=False,
        rms_dbfs=-20.08,
        noise_floor_dbfs=current_noise_floor_dbfs,
        snr_db=15.55,
        peak_dbfs=-10.92,
        vad_voiced_ms=240,
        candidate_duration_ms=240,
        speech_duration_ms=240,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="speech_active_below_candidate_duration",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "maxSnrDb": 16.16,
        "rmsRangeDb": 7.67,
        "rmsDirectionChanges": 2,
        "largeRmsJumpCount": 0,
        "speechQualityRejection": None,
    })
    runner._record_sip_deferred_episode_observation(
        call_id=call_id,
        turn=turn,
        timestamp=now - timedelta(milliseconds=1499),
        observation=first_observation,
    )
    runner._record_sip_deferred_episode_observation(
        call_id=call_id,
        turn=turn,
        timestamp=now,
        observation=current_observation,
    )

    return runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=current_observation,
    )


def test_realtime_agent_runner_promotes_low_snr_echo_guarded_deferred_episode() -> None:
    decision = _decide_low_snr_echo_guarded_deferred_episode()

    assert decision.action == "pre_stop"
    assert decision.evidence == "echo_guarded_local_speech"
    assert decision.extra_payload["sipEchoGuardedLocalEvidence"] == (
        "deferred_episode_micro_confirmed"
    )
    assert decision.extra_payload["sipDeferredEpisodeBurstCount"] == 2
    assert decision.extra_payload["sipDeferredEpisodeVoicedMs"] == 420
    assert decision.extra_payload["sipDeferredEpisodeMaxSnrDb"] == 16.16


def test_realtime_agent_runner_defers_echo_guarded_deferred_episode_without_noise_floor() -> None:
    decision = _decide_low_snr_echo_guarded_deferred_episode(
        current_noise_floor_dbfs=None,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_ai_playback_echo_guard"
    assert decision.extra_payload["sipEchoGuardedLocalEvidence"] == (
        "deferred_episode_micro_confirmed"
    )
    assert "sipAiPlaybackEchoGuardEscapedBy" not in decision.extra_payload


def test_realtime_agent_runner_defers_echo_guarded_deferred_episode_when_uplink_near_ai() -> None:
    decision = _decide_low_snr_echo_guarded_deferred_episode(ai_rms_dbfs=-22.0)

    assert decision.action == "defer"
    assert decision.reason == "awaiting_ai_playback_echo_guard"
    assert decision.extra_payload["sipEchoGuardedLocalEvidence"] == (
        "deferred_episode_micro_confirmed"
    )
    assert decision.extra_payload["sipUplinkAboveAiPlaybackDb"] < 3.0
    assert "sipAiPlaybackEchoGuardEscapedBy" not in decision.extra_payload


def test_realtime_agent_runner_defers_sparse_local_only_turn_cluster() -> None:
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    call_id = "call_sip_sparse_local_only_turn_cluster"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(
        runner,
        call_id,
        response_id="resp_sparse_cluster",
    )
    turn = PendingUserTurn(
        sip_turn_cluster_response_id="resp_sparse_cluster",
        sip_turn_cluster_first_at=now - timedelta(milliseconds=680),
        sip_turn_cluster_last_at=now,
        sip_turn_cluster_burst_count=2,
        sip_turn_cluster_voiced_ms=360,
        sip_turn_cluster_max_snr_db=20.7,
        sip_turn_cluster_max_rms_range_db=10.21,
    )
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-15.39,
        noise_floor_dbfs=-36.09,
        snr_db=20.7,
        peak_dbfs=-9.84,
        vad_voiced_ms=180,
        candidate_duration_ms=180,
        speech_duration_ms=180,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_pre_stop_authority"


def test_realtime_agent_runner_defers_continuous_non_speech_cluster_without_speech_like_quality() -> None:
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    call_id = "call_sip_continuous_non_speech_cluster"
    response_id = "resp_non_speech_cluster"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id, response_id=response_id)
    turn = PendingUserTurn(
        sip_turn_cluster_response_id=response_id,
        sip_turn_cluster_first_at=now - timedelta(milliseconds=260),
        sip_turn_cluster_last_at=now,
        sip_turn_cluster_burst_count=2,
        sip_turn_cluster_voiced_ms=360,
        sip_turn_cluster_max_snr_db=26.0,
        sip_turn_cluster_max_rms_range_db=12.0,
    )
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-18.0,
        noise_floor_dbfs=-39.0,
        snr_db=21.0,
        peak_dbfs=-8.0,
        vad_voiced_ms=240,
        candidate_duration_ms=240,
        speech_duration_ms=240,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector({
        "rmsRangeDb": 12.0,
        "rmsDirectionChanges": 6,
        "largeRmsJumpCount": 4,
        "speechQualityRejection": "non_speech_energy_envelope",
    })

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_speech_like_continuity"
    assert decision.extra_payload["speechQualityRejection"] == "non_speech_energy_envelope"


def test_realtime_agent_runner_defers_mid_duration_local_only_speech_without_authority() -> None:
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    call_id = "call_sip_mid_duration_local_only"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-15.17,
        noise_floor_dbfs=-36.09,
        snr_db=20.91,
        peak_dbfs=-7.19,
        vad_voiced_ms=440,
        candidate_duration_ms=440,
        speech_duration_ms=440,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )

    class LocalOnlyDetector:
        def has_single_short_pre_stop_local_speech(self, *_args, **_kwargs) -> bool:
            return False

        def has_fast_pre_stop_local_speech(self, _call_id: str) -> bool:
            return True

        def has_pre_stop_local_speech(self, _call_id: str) -> bool:
            return True

        def latest_observation_payload(self, _call_id: str) -> dict[str, object]:
            return {
                "rmsRangeDb": 10.89,
                "rmsDirectionChanges": 9,
                "largeRmsJumpCount": 3,
                "speechQualityRejection": None,
            }

    runner._sip_barge_in_detector = LocalOnlyDetector()

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_authorized_pre_stop_evidence"


def test_realtime_agent_runner_defers_flat_elevated_noise_local_only_fast_speech_without_cross_evidence() -> None:
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: FakeRealtimeProvider([]),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    call_id = "call_sip_elevated_noise_local_only_fast_speech"
    now = datetime.now(timezone.utc)
    _mark_sip_authority_playback_target(runner, call_id)
    turn = PendingUserTurn()
    observation = SipBargeInObservation(
        active=True,
        candidate=True,
        rms_dbfs=-16.4,
        noise_floor_dbfs=-35.2,
        snr_db=18.8,
        peak_dbfs=-8.5,
        vad_voiced_ms=480,
        candidate_duration_ms=480,
        speech_duration_ms=480,
        frame_duration_ms=20,
        candidate_class="stable_speech_candidate",
        reason="sip_uplink_speech_during_ai_audio",
    )
    runner._sip_barge_in_detector = _StaticSipAuthorityDetector(
        {
            "rmsRangeDb": 1.2,
            "rmsDirectionChanges": 0,
            "largeRmsJumpCount": 1,
            "speechQualityRejection": None,
        },
        fast_local=True,
        pre_stop_local=True,
    )

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=now,
        observation=observation,
    )

    assert decision.action == "defer"
    assert decision.reason == "awaiting_authorized_pre_stop_evidence"


@pytest.mark.anyio
async def test_realtime_agent_runner_holds_single_short_sip_burst_on_high_noise_floor() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([False] * 20 + [True] * 9 + [False] * 20),
        call_id="call_sip_high_noise_floor_single_short_burst",
        clean_window_ms=40,
        max_hold_ms=80,
        recovery_silence_ms=20,
    )
    fan_noise_frame = _pcm16_constant_frame(amplitude=490)
    burst_frames = _pcm16_constant_frames([
        3200,
        4300,
        5400,
        4700,
        3600,
        4400,
        5200,
        4700,
        5500,
    ])
    quiet_frame = _pcm16_constant_frame(amplitude=50)

    try:
        for _ in range(20):
            await runner.send_audio_frame(call_id, fan_noise_frame)
        for frame in burst_frames:
            await runner.send_audio_frame(call_id, frame)
        for _ in range(4):
            await runner.send_audio_frame(call_id, quiet_frame)
        await asyncio.sleep(0.12)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        deferred = next(event for event in events if event.type == "sip_pre_stop_deferred")
        assert "sip_interrupt_candidate" in event_types
        assert deferred.payload["candidateDurationMs"] == 180
        assert deferred.payload["noiseFloorDbfs"] == -44.0
        assert deferred.payload["reason"] == "awaiting_pre_stop_authority"
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert "sip_interrupt_rejected" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_does_not_confirm_breath_like_sip_audio_from_provider_speech() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 18),
        call_id="call_sip_breath_like_provider_speech_not_confirmed",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    breath_like_frames = _pcm16_constant_frames([1180] * 18)

    try:
        for frame in breath_like_frames:
            await runner.send_audio_frame(call_id, frame)
        await runner._handle_user_speech_started(call_id, provider, datetime.now(timezone.utc))
        provider_event = ProviderEvent(type="user_transcript_done", payload={"transcript": "唉。"})
        trust_decision = runner._decide_realtime_transcript_trust(call_id, provider_event)
        await runner._handle_user_transcript(
            call_id,
            provider,
            ProviderEvent(
                type=provider_event.type,
                payload={**provider_event.payload, **trust_decision.as_payload()},
            ),
            datetime.now(timezone.utc),
        )

        events = store.list(call_id)
        event_types = [event.type for event in events]
        assert "sip_interrupt_candidate" in event_types
        assert "sip_pre_stop_deferred" in event_types
        assert "sip_pre_stop" not in event_types
        assert "sip_interrupt_candidate_confirmed" not in event_types
        assert "interrupt_confirmed" not in event_types
        assert "user_transcript_semantic_rejected" in event_types
        assert provider.cancelled_response_count == 0
        assert provider.created_responses == []
        assert publisher.stopped_call_ids == []
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_pre_stops_syllabic_sip_speech_across_short_gap() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 9 + [False] * 10 + [True] * 12),
        call_id="call_sip_syllabic_short_gap_pre_stop",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    first_syllable_frame = _pcm16_constant_frame(amplitude=1200)
    second_syllable_frames = _pcm16_constant_frames([
        1700,
        2200,
        1900,
        2600,
        2100,
        2800,
        2300,
        2500,
        2000,
        2400,
        2100,
        2300,
    ])
    quiet_frame = _pcm16_constant_frame(amplitude=50)

    try:
        for _ in range(9):
            await runner.send_audio_frame(call_id, first_syllable_frame)
        for _ in range(10):
            await runner.send_audio_frame(call_id, quiet_frame)
        await asyncio.sleep(0.12)
        for frame in second_syllable_frames:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        pre_stop = next(event for event in events if event.type == "sip_pre_stop")
        assert event_types.count("sip_interrupt_candidate") == 1
        assert "sip_interrupt_candidate_expired" not in event_types
        assert pre_stop.payload["sipTurnClusterBurstCount"] == 2
        assert pre_stop.payload["sipTurnClusterVoicedMs"] >= 360
        assert pre_stop.payload["sipTurnClusterRmsRangeDb"] >= 3.0
        assert publisher.stopped_call_ids == [call_id]
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_pre_stops_recovered_hot_onset_syllabic_sip_speech() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 9 + [False] * 10 + [True] * 12),
        call_id="call_sip_recovered_hot_onset_syllabic_pre_stop",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    first_syllable_frames = _pcm16_constant_frames([11500] + [5850] * 8)
    second_syllable_frames = _pcm16_constant_frames([
        1800,
        2500,
        2100,
        2900,
        2300,
        3100,
        2400,
        2800,
        2200,
        2600,
        2300,
        2500,
    ])
    quiet_frame = _pcm16_constant_frame(amplitude=50)

    try:
        for frame in first_syllable_frames:
            await runner.send_audio_frame(call_id, frame)
        for _ in range(10):
            await runner.send_audio_frame(call_id, quiet_frame)
        await asyncio.sleep(0.12)
        for frame in second_syllable_frames:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        first_deferred = next(event for event in events if event.type == "sip_pre_stop_deferred")
        pre_stop = next(event for event in events if event.type == "sip_pre_stop")
        assert first_deferred.payload["speechQualityRejection"] == "short_hot_onset_drop"
        assert "sip_interrupt_candidate_expired" not in event_types
        assert pre_stop.payload["speechQualityRejection"] is None
        assert pre_stop.payload["candidateDurationMs"] >= 240
        assert pre_stop.payload["sipTurnClusterBurstCount"] == 2
        assert pre_stop.payload["sipTurnClusterVoicedMs"] >= 360
        assert pre_stop.payload["sipTurnClusterRmsRangeDb"] >= 3.0
        assert publisher.stopped_call_ids == [call_id]
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_holds_repeated_flat_breath_like_sip_bursts() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 9 + [False] * 10 + [True] * 9),
        call_id="call_sip_repeated_flat_breath_like_bursts",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    breath_like_frame = _pcm16_constant_frame(amplitude=1180)
    quiet_frame = _pcm16_constant_frame(amplitude=50)

    try:
        for _ in range(9):
            await runner.send_audio_frame(call_id, breath_like_frame)
        for _ in range(10):
            await runner.send_audio_frame(call_id, quiet_frame)
        await asyncio.sleep(1.05)
        for _ in range(9):
            await runner.send_audio_frame(call_id, breath_like_frame)

        event_types = [event.type for event in store.list(call_id)]
        assert event_types.count("sip_interrupt_candidate") == 2
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_defers_180ms_sip_burst_before_pre_stop() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 9 + [False] * 20),
        call_id="call_sip_180ms_burst",
        clean_window_ms=40,
        max_hold_ms=80,
        recovery_silence_ms=20,
    )
    burst_frame = _pcm16_constant_frame(amplitude=6000)
    quiet_frame = _pcm16_constant_frame(amplitude=50)

    try:
        for _ in range(9):
            await runner.send_audio_frame(call_id, burst_frame)
        for _ in range(4):
            await runner.send_audio_frame(call_id, quiet_frame)
        await asyncio.sleep(0.12)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        deferred = next(event for event in events if event.type == "sip_pre_stop_deferred")
        assert "sip_interrupt_candidate" in event_types
        assert deferred.payload["candidateClass"] == "stable_speech_candidate"
        assert deferred.payload["candidateDurationMs"] == 180
        assert deferred.payload["requiredPreStopDurationMs"] > 180
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert "sip_interrupt_rejected" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_pre_stops_moderate_sip_speech_after_sustained_stable_turn_evidence() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 24),
        call_id="call_sip_moderate_speech_stable_pre_stop",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    speech_frames = _pcm16_constant_frames([
        1180,
        1500,
        1350,
        1800,
        1600,
        2100,
        1750,
        2400,
        2000,
        2600,
        2200,
        2500,
        2100,
        2300,
        1900,
        2200,
        2000,
        2100,
        2300,
        2100,
        2400,
        2200,
        2500,
        2300,
    ])

    try:
        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        pre_stop = next(event for event in events if event.type == "sip_pre_stop")
        assert "sip_interrupt_candidate" in event_types
        assert "sip_pre_stop_deferred" in event_types
        assert pre_stop.payload["candidateDurationMs"] >= 480
        assert pre_stop.payload["vadVoicedMs"] >= 240
        assert "wallClockSpeechMs" in pre_stop.payload
        assert "maxVoicedFrameGapMs" in pre_stop.payload
        assert publisher.stopped_call_ids == [call_id]
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_defers_clear_modulated_sip_short_turn_without_turn_authority() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 14 + [False] * 10),
        call_id="call_sip_clear_modulated_short_turn",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    speech_frames = _pcm16_constant_frames([
        1850,
        2550,
        3150,
        3800,
        4700,
        5750,
        5200,
        5900,
        5600,
        6000,
        5100,
        4100,
        3200,
        2400,
    ])
    quiet_frame = _pcm16_constant_frame(amplitude=50)

    try:
        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)
        for _ in range(4):
            await runner.send_audio_frame(call_id, quiet_frame)
        await asyncio.sleep(0.16)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        assert "sip_interrupt_candidate" in event_types
        assert "sip_pre_stop_deferred" in event_types
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert "sip_interrupt_rejected" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_pre_stops_clear_louder_phone_speech_after_stable_turn_evidence() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 18),
        call_id="call_sip_clear_louder_phone_speech_stable_pre_stop",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    speech_frames = _pcm16_constant_frames([
        3600,
        5100,
        4200,
        5700,
        4600,
        6200,
        5000,
        5900,
        4400,
        5600,
        4700,
        6100,
        5200,
        5800,
        4500,
        5500,
        4900,
        6000,
    ])

    try:
        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        pre_stop = next(event for event in events if event.type == "sip_pre_stop")
        assert "sip_interrupt_candidate" in event_types
        assert "sip_pre_stop_deferred" in event_types
        assert pre_stop.payload["candidateDurationMs"] >= 220
        assert pre_stop.payload["speechQualityRejection"] is None
        assert publisher.stopped_call_ids == [call_id]
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_holds_sip_tail_after_recent_provider_speech() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 12),
        call_id="call_sip_post_speech_tail_guard",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    tail_frames = _pcm16_constant_frames([2330] * 9 + [1720] * 3)

    try:
        await runner._handle_user_speech_stopped(call_id, provider, datetime.now(timezone.utc))
        runner._mark_response_started(call_id, {"response_id": "resp_sip_after_user"})
        runner._response_lifecycle(call_id).active = True
        runner._playback_guard(call_id).current_response_audio_published = True

        for frame in tail_frames:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        deferred = next(event for event in events if event.type == "sip_pre_stop_deferred")
        assert "sip_interrupt_candidate" in event_types
        assert deferred.payload["reason"] == "awaiting_post_speech_tail_guard"
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_expires_deferred_sip_candidate_before_next_response() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 40),
        call_id="call_sip_deferred_candidate_next_response",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    tail_frames = [_pcm16_constant_frame(amplitude=3600) for _ in range(9)]

    try:
        runner._last_sip_provider_speech_stopped_at[call_id] = datetime.now(timezone.utc)
        lifecycle = runner._response_lifecycle(call_id)
        guard = runner._playback_guard(call_id)
        lifecycle.active = False
        guard.current_response_id = None
        guard.current_response_audio_published = False

        for frame in tail_frames:
            await runner.send_audio_frame(call_id, frame)

        runner._mark_response_started(call_id, {"response_id": "resp_sip_next"})
        runner._response_lifecycle(call_id).active = True
        runner._playback_guard(call_id).current_response_audio_published = True
        runner._last_sip_provider_speech_stopped_at[call_id] = (
            datetime.now(timezone.utc) - timedelta(seconds=2)
        )

        for frame in tail_frames * 2:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        expired = next(event for event in events if event.type == "sip_interrupt_candidate_expired")
        assert "sip_interrupt_candidate" in event_types
        assert expired.payload["reason"] == "candidate_response_mismatch"
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_holds_hot_cough_like_sip_burst_before_pre_stop() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 12 + [False] * 20),
        call_id="call_sip_hot_cough_like_burst",
        clean_window_ms=40,
        max_hold_ms=80,
        recovery_silence_ms=20,
    )
    frames = _pcm16_constant_frames([11500] * 9 + [5850] * 3 + [50] * 4)

    try:
        for frame in frames:
            await runner.send_audio_frame(call_id, frame)
        await asyncio.sleep(0.12)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        assert "sip_interrupt_candidate" in event_types
        assert "sip_pre_stop_deferred" in event_types
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert "sip_interrupt_confirmed" not in event_types
        assert "interrupt_confirmed" not in event_types
        assert "sip_interrupt_rejected" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_holds_short_loud_non_turn_tail_before_pre_stop() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 11 + [False] * 20),
        call_id="call_sip_short_loud_non_turn_tail",
        clean_window_ms=40,
        max_hold_ms=80,
        recovery_silence_ms=20,
    )
    frames = _pcm16_constant_frames([7300] * 9 + [5200] * 2 + [50] * 4)

    try:
        for frame in frames:
            await runner.send_audio_frame(call_id, frame)
        await asyncio.sleep(0.12)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        assert "sip_interrupt_candidate" in event_types
        assert "sip_pre_stop_deferred" in event_types
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert "sip_interrupt_confirmed" not in event_types
        assert "interrupt_confirmed" not in event_types
        assert "sip_interrupt_rejected" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_holds_short_hot_onset_tail_before_pre_stop() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 14),
        call_id="call_sip_short_hot_onset_tail",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    frames = _pcm16_constant_frames([11500] + [5850] * 12)

    try:
        for frame in frames:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        deferred = next(event for event in events if event.type == "sip_pre_stop_deferred")
        assert "sip_interrupt_candidate" in event_types
        assert deferred.payload["speechQualityRejection"] == "short_hot_onset_drop"
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_holds_clipped_hot_sip_burst_before_pre_stop() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 13),
        call_id="call_sip_clipped_hot_burst",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    clipped_frame = _pcm16_constant_frame(amplitude=30000)

    try:
        for _ in range(13):
            await runner.send_audio_frame(call_id, clipped_frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        deferred = next(event for event in events if event.type == "sip_pre_stop_deferred")
        assert "sip_interrupt_candidate" in event_types
        assert deferred.payload["speechQualityRejection"] == "clipped_hot_onset"
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_allows_continued_hot_onset_sip_speech() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 24),
        call_id="call_sip_continued_hot_onset_speech",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    frames = _pcm16_constant_frames([
        11500,
        8200,
        6200,
        7000,
        5600,
        7600,
        6100,
        7200,
        5800,
        6900,
        5400,
        7300,
        5900,
        7100,
        5600,
        6800,
        6200,
        7000,
    ])

    try:
        for frame in frames:
            await runner.send_audio_frame(call_id, frame)

        event_types = [event.type for event in store.list(call_id)]
        assert "sip_pre_stop" in event_types
        assert publisher.stopped_call_ids == [call_id]
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_holds_stale_deferred_sip_candidate_before_pre_stop() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 12),
        call_id="call_sip_stale_deferred_candidate",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    burst_frame = _pcm16_constant_frame(amplitude=6000)

    try:
        for _ in range(9):
            await runner.send_audio_frame(call_id, burst_frame)

        await asyncio.sleep(1.15)

        for _ in range(3):
            await runner.send_audio_frame(call_id, burst_frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        deferred = next(event for event in events if event.type == "sip_pre_stop_deferred")
        assert "sip_interrupt_candidate" in event_types
        assert deferred.payload["reason"] == "awaiting_pre_stop_authority"
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_pre_stops_modulated_two_burst_deferred_sip_episode() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 9 + [False] * 10 + [True] * 9),
        call_id="call_sip_modulated_two_burst_deferred_episode",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    quiet_frame = _pcm16_constant_frame(amplitude=50)
    first_burst = _pcm16_constant_frames([
        900,
        1100,
        1300,
        1550,
        1800,
        2100,
        2350,
        2600,
        2900,
    ])
    second_burst = _pcm16_constant_frames([
        1900,
        2300,
        2700,
        3200,
        3700,
        4200,
        4700,
        5200,
        5700,
    ])

    try:
        for frame in first_burst:
            await runner.send_audio_frame(call_id, frame)
        for _ in range(10):
            await runner.send_audio_frame(call_id, quiet_frame)
        await asyncio.sleep(1.05)
        for frame in second_burst:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        pre_stop = next(event for event in events if event.type == "sip_pre_stop")
        assert event_types.count("sip_interrupt_candidate") == 2
        assert event_types.count("sip_interrupt_candidate_expired") == 1
        assert pre_stop.payload["sipPreStopAuthorityEvidence"] == "deferred_speech_episode"
        assert pre_stop.payload["sipDeferredEpisodeBurstCount"] == 2
        assert pre_stop.payload["sipDeferredEpisodeVoicedMs"] >= 360
        assert pre_stop.payload["sipDeferredEpisodeRmsRangeDb"] >= 4.0
        assert publisher.stopped_call_ids == [call_id]
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_pre_stops_repeated_deferred_sip_speech_episode() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 9 + [False] * 4 + [True] * 9 + [False] * 4 + [True] * 9),
        call_id="call_sip_repeated_deferred_speech_episode",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    quiet_frame = _pcm16_constant_frame(amplitude=50)
    speech_bursts = [
        _pcm16_constant_frames([1280, 1480, 1360, 1720, 1510, 1880, 1660, 2050, 1820]),
        _pcm16_constant_frames([2240, 2840, 2510, 3180, 2690, 3420, 2950, 3660, 3130]),
        _pcm16_constant_frames([2460, 3320, 2810, 3920, 3190, 4300, 3560, 4660, 3810]),
    ]

    try:
        for index, burst in enumerate(speech_bursts):
            for frame in burst:
                await runner.send_audio_frame(call_id, frame)
            if index < len(speech_bursts) - 1:
                for _ in range(4):
                    await runner.send_audio_frame(call_id, quiet_frame)
                await asyncio.sleep(1.08)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        pre_stop = next(event for event in events if event.type == "sip_pre_stop")
        assert event_types.count("sip_interrupt_candidate") == 3
        assert event_types.count("sip_interrupt_candidate_expired") == 2
        assert event_types.count("sip_pre_stop_deferred") >= 2
        assert pre_stop.payload["sipPreStopAuthorityEvidence"] == "deferred_speech_episode"
        assert pre_stop.payload["sipDeferredEpisodeBurstCount"] >= 3
        assert pre_stop.payload["sipDeferredEpisodeVoicedMs"] >= 540
        assert publisher.stopped_call_ids == [call_id]
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_defers_repeated_episode_under_recent_ai_audio() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 9 + [False] * 4 + [True] * 9 + [False] * 4 + [True] * 9),
        call_id="call_sip_repeated_deferred_episode_recent_ai_audio",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    ai_frame = _pcm16_constant_frame(amplitude=4800, sample_rate_hz=24000, duration_ms=40)
    quiet_frame = _pcm16_constant_frame(amplitude=50)
    speech_bursts = [
        _pcm16_constant_frames([1280, 1480, 1360, 1720, 1510, 1880, 1660, 2050, 1820]),
        _pcm16_constant_frames([2240, 2840, 2510, 3180, 2690, 3420, 2950, 3660, 3130]),
        _pcm16_constant_frames([2460, 3320, 2810, 3920, 3190, 4300, 3560, 4660, 3810]),
    ]

    try:
        for index, burst in enumerate(speech_bursts):
            await runner._publish_model_audio_delta(
                call_id,
                ProviderEvent(
                    type="model_audio_delta",
                    payload={
                        "response_id": "resp_sip_opening",
                        "delta": base64.b64encode(ai_frame.data).decode("ascii"),
                    },
                ),
            )
            for frame in burst:
                await runner.send_audio_frame(call_id, frame)
            if index < len(speech_bursts) - 1:
                for _ in range(4):
                    await runner.send_audio_frame(call_id, quiet_frame)
                await asyncio.sleep(1.08)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        assert event_types.count("sip_interrupt_candidate") == 3
        assert "sip_ai_playback_echo_deferred" in event_types
        assert "sip_pre_stop" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_pre_stops_sparse_deferred_turn_across_echo_guard() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 9 + [False] * 4 + [True] * 9 + [False] * 4 + [True] * 9),
        call_id="call_sip_sparse_deferred_turn_echo_guard_pre_stop",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    ai_frame = _pcm16_constant_frame(amplitude=4800, sample_rate_hz=24000, duration_ms=40)
    quiet_frame = _pcm16_constant_frame(amplitude=50)
    speech_bursts = [
        _pcm16_constant_frames([2240, 2840, 2510, 3180, 2690, 3420, 2950, 3660, 3130]),
        _pcm16_constant_frames([2460, 3320, 2810, 3920, 3190, 4300, 3560, 4660, 3810]),
        _pcm16_constant_frames([2460, 3320, 2810, 3920, 3190, 4300, 3560, 4660, 3810]),
    ]

    try:
        for index, burst in enumerate(speech_bursts):
            await runner._publish_model_audio_delta(
                call_id,
                ProviderEvent(
                    type="model_audio_delta",
                    payload={
                        "response_id": "resp_sip_opening",
                        "delta": base64.b64encode(ai_frame.data).decode("ascii"),
                    },
                ),
            )
            for frame in burst:
                await runner.send_audio_frame(call_id, frame)
            if index < len(speech_bursts) - 1:
                for _ in range(4):
                    await runner.send_audio_frame(call_id, quiet_frame)
                await asyncio.sleep(1.65)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        pre_stop = next(event for event in events if event.type == "sip_pre_stop")
        assert event_types.count("sip_interrupt_candidate") == 3
        assert event_types.count("sip_interrupt_candidate_expired") == 2
        assert "sip_ai_playback_echo_deferred" in event_types
        assert pre_stop.payload["sipPreStopAuthorityEvidence"] in {
            "deferred_speech_episode",
            "deferred_multi_candidate_turn",
        }
        assert pre_stop.payload["sipDeferredEpisodeBurstCount"] >= 3
        assert pre_stop.payload["sipDeferredEpisodeWallMs"] >= 3000
        assert publisher.stopped_call_ids == [call_id]
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_promotes_repeated_echo_guarded_sip_turn_on_same_response() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([False] * 12 + [True] * 9 + [False] * 4 + [True] * 12),
        call_id="call_sip_repeated_echo_guarded_turn",
        clean_window_ms=40,
        max_hold_ms=100,
        hold_timeout_seconds=0.06,
        recovery_silence_ms=20,
    )
    ai_frame = _pcm16_constant_frame(amplitude=5200, sample_rate_hz=24000, duration_ms=40)
    noise_floor_frame = _pcm16_constant_frame(amplitude=520)
    quiet_frame = _pcm16_constant_frame(amplitude=50)
    first_burst = _pcm16_constant_frames([1480, 1840, 1650, 2110, 1760, 2320, 1880, 2480, 2030])
    second_burst = _pcm16_constant_frames([
        1680,
        2280,
        1910,
        2710,
        2140,
        3050,
        2390,
        3380,
        2630,
        3620,
        2860,
        3860,
    ])

    try:
        for _ in range(12):
            runner._sip_barge_in_detector.observe(
                call_id,
                noise_floor_frame,
                now=datetime.now(timezone.utc),
                interruptible=False,
            )
        await runner._publish_model_audio_delta(
            call_id,
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_sip_opening",
                    "delta": base64.b64encode(ai_frame.data).decode("ascii"),
                },
            ),
        )
        for frame in first_burst:
            await runner.send_audio_frame(call_id, frame)
        await asyncio.sleep(0.45)

        for _ in range(4):
            await runner.send_audio_frame(call_id, quiet_frame)
        await runner._publish_model_audio_delta(
            call_id,
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_sip_opening",
                    "delta": base64.b64encode(ai_frame.data).decode("ascii"),
                },
            ),
        )
        for frame in second_burst:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        pre_stop = next(event for event in events if event.type == "sip_pre_stop")

        assert event_types.count("sip_interrupt_candidate") >= 2
        assert "sip_ai_playback_echo_deferred" in event_types
        assert pre_stop.payload["sipPreStopAuthorityEvidence"] == "echo_guarded_turn_evidence"
        assert pre_stop.payload["candidateDurationMs"] >= 240
        assert pre_stop.payload["sipEchoGuardedTurnBurstCount"] >= 2
        assert pre_stop.payload["sipEchoGuardedTurnVoicedMs"] >= 420
        assert publisher.stopped_call_ids == [call_id]
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_defers_echo_guarded_sip_micro_confirmed_local_only_turn() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([False] * 12 + [True] * 14),
        call_id="call_sip_echo_guarded_micro_confirmed_turn",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    ai_frame = _pcm16_constant_frame(amplitude=5200, sample_rate_hz=24000, duration_ms=40)
    noise_floor_frame = _pcm16_constant_frame(amplitude=400)
    speech_frames = _pcm16_constant_frames([
        6100,
        7600,
        6900,
        8400,
        7200,
        9100,
        7800,
        6600,
        5200,
        3000,
        2400,
        1900,
        1700,
        1600,
    ])

    try:
        for _ in range(12):
            runner._sip_barge_in_detector.observe(
                call_id,
                noise_floor_frame,
                now=datetime.now(timezone.utc),
                interruptible=False,
            )
        await runner._publish_model_audio_delta(
            call_id,
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "response_id": "resp_sip_opening",
                    "delta": base64.b64encode(ai_frame.data).decode("ascii"),
                },
            ),
        )

        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        deferred = next(event for event in events if event.type == "sip_ai_playback_echo_deferred")

        assert "sip_interrupt_candidate" in event_types
        assert "sip_ai_playback_echo_deferred" in event_types
        assert "sip_interrupt_candidate_expired" not in event_types
        assert "sip_pre_stop" not in event_types
        assert deferred.payload["candidateDurationMs"] >= 240
        assert deferred.payload["candidateDurationMs"] <= 320
        assert deferred.payload["reason"] == "awaiting_ai_playback_echo_guard"
        assert deferred.payload["sipEchoGuardedTurnBurstCount"] == 1
        assert deferred.payload["sipEchoGuardedTurnVoicedMs"] >= 240
        assert deferred.payload["sipEchoGuardedTurnRmsRangeDb"] >= 6.0
        assert deferred.payload["sipEchoGuardedTurnMaxSnrDb"] >= 20.0
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_expires_stale_deferred_sip_candidate_before_fresh_candidate() -> None:
    runner, _registry, store, _provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 40),
        call_id="call_sip_stale_deferred_fresh_candidate",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    burst_frame = _pcm16_constant_frame(amplitude=6000)
    quiet_frame = _pcm16_constant_frame(amplitude=50)
    fresh_speech_frames = _pcm16_constant_frames([
        3200,
        5200,
        3900,
        6100,
        4400,
        5700,
        4700,
        6200,
        4100,
        5900,
        4500,
        5600,
        4800,
        6000,
        4300,
        5500,
        5000,
        5800,
        4600,
        6200,
        4900,
        5900,
        5100,
        6100,
    ])

    try:
        for _ in range(9):
            await runner.send_audio_frame(call_id, burst_frame)
        for _ in range(3):
            await runner.send_audio_frame(call_id, quiet_frame)

        await asyncio.sleep(1.15)

        for frame in fresh_speech_frames:
            await runner.send_audio_frame(call_id, frame)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        candidate_indexes = [
            index
            for index, event_type in enumerate(event_types)
            if event_type == "sip_interrupt_candidate"
        ]
        assert len(candidate_indexes) == 2
        assert event_types.index("sip_interrupt_candidate_expired") < candidate_indexes[1]
        assert candidate_indexes[1] < event_types.index("sip_pre_stop")
        assert publisher.stopped_call_ids == [call_id]
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_allows_fresh_sip_candidate_after_hold_expiry() -> None:
    runner, _registry, store, _provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 40),
        call_id="call_sip_hold_expiry_fresh_candidate",
        clean_window_ms=40,
        max_hold_ms=100,
        hold_timeout_seconds=0.05,
        recovery_silence_ms=20,
    )
    flat_frame = _pcm16_constant_frame(amplitude=4500)
    fresh_speech_frames = _pcm16_constant_frames([
        3200,
        5200,
        3900,
        6100,
        4400,
        5700,
        4700,
        6200,
        4100,
        5900,
        4500,
        5600,
        4800,
        6000,
        4300,
        5500,
        5000,
        5800,
        4600,
        6200,
        4900,
        5900,
        5100,
        6100,
    ])

    try:
        for _ in range(9):
            await runner.send_audio_frame(call_id, flat_frame)

        for _ in range(20):
            if any(event.type == "sip_interrupt_candidate_expired" for event in store.list(call_id)):
                break
            await asyncio.sleep(0.01)

        assert [event.type for event in store.list(call_id)].count("sip_interrupt_candidate") == 1
        assert "sip_interrupt_candidate_expired" in [event.type for event in store.list(call_id)]

        for frame in fresh_speech_frames:
            await runner.send_audio_frame(call_id, frame)

        event_types = [event.type for event in store.list(call_id)]
        candidate_indexes = [
            index
            for index, event_type in enumerate(event_types)
            if event_type == "sip_interrupt_candidate"
        ]
        assert len(candidate_indexes) == 2
        assert event_types.index("sip_interrupt_candidate_expired") < candidate_indexes[1]
        assert candidate_indexes[1] < event_types.index("sip_pre_stop")
        assert publisher.stopped_call_ids == [call_id]
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_holds_unstable_sip_envelope_before_pre_stop() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 40),
        call_id="call_sip_unstable_cough_tail",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    frames = _pcm16_constant_frames([2400] * 9 + [7700] * 3 + [1260] * 16)

    try:
        for frame in frames:
            await runner.send_audio_frame(call_id, frame)
        await asyncio.sleep(0.14)

        event_types = [event.type for event in store.list(call_id)]
        assert "sip_interrupt_candidate" in event_types
        assert "sip_pre_stop_deferred" in event_types
        assert "sip_pre_stop" not in event_types
        assert "sip_interrupt_rejected" not in event_types
        assert "sip_interrupt_confirmed" not in event_types
        assert "interrupt_confirmed" not in event_types
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_suppresses_cough_like_hot_burst_with_voiced_tail() -> None:
    runner, _registry, store, provider, _publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 30),
        call_id="call_sip_cough_like_burst",
        clean_window_ms=40,
        max_hold_ms=100,
        short_speech_min_duration_ms=120,
        recovery_silence_ms=20,
    )
    burst_frame = _pcm16_constant_frame(amplitude=24000)
    voiced_tail_frame = _pcm16_constant_frame(amplitude=900)

    try:
        for _ in range(6):
            await runner.send_audio_frame(call_id, burst_frame)
        for _ in range(15):
            await runner.send_audio_frame(call_id, voiced_tail_frame)
        await asyncio.sleep(0.14)

        event_types = [event.type for event in store.list(call_id)]
        assert "sip_interrupt_candidate" not in event_types
        assert "sip_pre_stop" not in event_types
        assert "sip_interrupt_confirmed" not in event_types
        assert "interrupt_confirmed" not in event_types
        assert "sip_interrupt_rejected" not in event_types
        assert "sip_recovery_started" not in event_types
        assert provider.cancelled_response_count == 0
        assert provider.created_responses == []
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_max_hold_defers_vad_without_snr() -> None:
    runner, _registry, store, provider, _publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 40),
        call_id="call_sip_max_hold",
        clean_window_ms=200,
        max_hold_ms=60,
        snr_threshold_db=25.0,
    )
    high_snr_frame = _pcm16_constant_frame(amplitude=4000)
    low_snr_frame = _pcm16_constant_frame(amplitude=900)

    try:
        for _ in range(12):
            await runner.send_audio_frame(call_id, high_snr_frame)
        for _ in range(4):
            await runner.send_audio_frame(call_id, low_snr_frame)
        await asyncio.sleep(0.08)

        event_types = [event.type for event in store.list(call_id)]
        assert "sip_pre_stop_deferred" in event_types
        assert "sip_pre_stop" not in event_types
        assert "sip_interrupt_rejected" not in event_types
        assert "sip_interrupt_confirmed" not in event_types
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_starts_one_short_recovery_after_sip_rejected() -> None:
    runner, _registry, store, provider, _publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 18 + [False] * 40),
        call_id="call_sip_recovery",
        clean_window_ms=40,
        max_hold_ms=80,
        recovery_silence_ms=20,
        recovery_max_per_turn=1,
    )
    speech_frames = _pcm16_constant_frames([
        3200,
        4300,
        5400,
        4700,
        3600,
        4400,
        5200,
        4700,
        5500,
        5200,
        4700,
        4300,
        5100,
        4600,
        5400,
        4900,
        5700,
        5000,
    ])
    quiet_frame = _pcm16_constant_frame(amplitude=50)

    try:
        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)
        for _ in range(4):
            await runner.send_audio_frame(call_id, quiet_frame)
        await asyncio.sleep(0.12)

        event_types = [event.type for event in store.list(call_id)]
        assert "sip_interrupt_rejected" in event_types
        assert "sip_recovery_started" in event_types
        assert provider.created_responses == []

        await runner._apply_provider_event(
            call_id,
            provider,
            "model_response_done",
            datetime.now(timezone.utc),
            {"response": {"id": "resp_sip_opening", "status": "cancelled"}},
        )

        assert provider.created_responses
        recovery_input = provider.created_responses[0]
        assert recovery_input is not None
        assert "一句简短自然" in recovery_input
        assert "不要重复整段内容" in recovery_input
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_confirms_recent_rejected_sip_pre_stop_from_late_provider_speech() -> None:
    runner, _registry, store, provider, _publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 18 + [False] * 40),
        call_id="call_sip_rejected_late_provider_confirm",
        clean_window_ms=40,
        max_hold_ms=80,
        recovery_silence_ms=20,
        recovery_max_per_turn=1,
    )
    speech_frames = _pcm16_constant_frames([
        3200,
        4300,
        5400,
        4700,
        3600,
        4400,
        5200,
        4700,
        5500,
        5200,
        4700,
        4300,
        5100,
        4600,
        5400,
        4900,
        5700,
        5000,
    ])
    quiet_frame = _pcm16_constant_frame(amplitude=50)

    try:
        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)
        for _ in range(4):
            await runner.send_audio_frame(call_id, quiet_frame)
        await asyncio.sleep(0.12)

        event_types = [event.type for event in store.list(call_id)]
        assert "sip_interrupt_rejected" in event_types
        assert "sip_recovery_started" in event_types
        assert provider.created_responses == []

        await runner._handle_user_speech_started(call_id, provider, datetime.now(timezone.utc))
        await runner._apply_provider_event(
            call_id,
            provider,
            "model_response_done",
            datetime.now(timezone.utc),
            {"response": {"id": "resp_sip_opening", "status": "cancelled"}},
        )

        event_types = [event.type for event in store.list(call_id)]
        assert "sip_rejected_pre_stop_late_provider_confirmed" in event_types
        assert "sip_interrupt_candidate_confirmed" in event_types
        assert "interrupt_confirmed" in event_types
        assert provider.created_responses == []
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_uses_short_reask_for_sip_confirmed_without_transcript() -> None:
    runner, _registry, store, provider, _publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 12),
        call_id="call_sip_confirmed_without_transcript_recovery",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
        recovery_max_per_turn=1,
    )

    try:
        turn = runner._pending_turn(call_id)
        turn.sip_barge_in_confirmed = True
        turn.sip_barge_in_confirmed_by = "sip_clean_window"
        turn.response_requested = False
        runner._response_lifecycle(call_id).active = True

        await runner._complete_response_and_flush_pending(call_id, provider)

        events = store.list(call_id)
        recovery = next(event for event in events if event.type == "sip_recovery_started")
        assert recovery.payload["reason"] == "sip_confirmed_without_transcript"
        assert provider.created_responses
        recovery_input = provider.created_responses[0]
        assert recovery_input is not None
        assert "一句简短自然" in recovery_input
        assert "不要重复整段内容" in recovery_input
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_defers_sip_tail_noise_on_same_response_without_pre_stop() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 14 + [False] * 4 + [True] * 9),
        call_id="call_sip_rejected_same_response_tail",
        clean_window_ms=40,
        max_hold_ms=80,
        recovery_silence_ms=10_000,
        recovery_max_per_turn=1,
    )
    speech_frames = _pcm16_constant_frames([
        3200,
        4300,
        5400,
        4700,
        3600,
        4400,
        5200,
        4700,
        5500,
        5200,
        4700,
        4300,
        5100,
        4600,
    ])
    quiet_frame = _pcm16_constant_frame(amplitude=50)
    tail_frame = _pcm16_constant_frame(amplitude=1300)

    try:
        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)
        for _ in range(4):
            await runner.send_audio_frame(call_id, quiet_frame)
        await asyncio.sleep(0.12)

        assert [event.type for event in store.list(call_id)].count("sip_pre_stop") == 0
        assert "sip_pre_stop_deferred" in [event.type for event in store.list(call_id)]
        assert "sip_interrupt_rejected" not in [event.type for event in store.list(call_id)]

        for _ in range(9):
            await runner.send_audio_frame(call_id, tail_frame)

        event_types = [event.type for event in store.list(call_id)]
        assert event_types.count("sip_pre_stop") == 0
        assert event_types.count("response_generation_invalidated") == 0
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_allows_new_sip_candidate_after_rejected_recovery() -> None:
    runner, _registry, store, provider, _publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 18 + [False] * 4 + [True] * 22 + [False] * 10),
        call_id="call_sip_rejected_then_new_candidate",
        clean_window_ms=40,
        max_hold_ms=80,
        recovery_silence_ms=20,
        recovery_max_per_turn=1,
    )
    first_speech_frames = _pcm16_constant_frames([
        3200,
        4300,
        5400,
        4700,
        3600,
        4400,
        5200,
        4700,
        5500,
        5200,
        4700,
        4300,
        5100,
        4600,
        5400,
        4900,
        5700,
        5000,
    ])
    second_speech_frames = _pcm16_constant_frames([
        3300,
        5200,
        4000,
        6100,
        4500,
        5700,
        4800,
        6200,
        4200,
        5900,
        4600,
        5600,
        4900,
        6000,
        4400,
        5500,
        5100,
        5800,
    ])
    quiet_frame = _pcm16_constant_frame(amplitude=50)
    ai_frame = _pcm16_constant_frame(amplitude=1000, sample_rate_hz=24000, duration_ms=40)

    try:
        for frame in first_speech_frames:
            await runner.send_audio_frame(call_id, frame)
        for _ in range(4):
            await runner.send_audio_frame(call_id, quiet_frame)
        await asyncio.sleep(0.12)

        assert [event.type for event in store.list(call_id)].count("sip_pre_stop") == 1

        await runner._apply_provider_event(
            call_id,
            provider,
            "model_response_done",
            datetime.now(timezone.utc),
            {"response": {"id": "resp_sip_opening", "status": "completed"}},
        )
        runner._response_lifecycle(call_id).response_generation = (
            runner._playback_guard(call_id).generation
        )
        await runner._apply_provider_event(
            call_id,
            provider,
            "model_response_started",
            datetime.now(timezone.utc),
            {"response": {"id": "resp_sip_recovery", "status": "in_progress"}},
        )
        await runner._apply_provider_event(
            call_id,
            provider,
            "model_audio_delta",
            datetime.now(timezone.utc),
            {
                "response_id": "resp_sip_recovery",
                "delta": base64.b64encode(ai_frame.data).decode("ascii"),
            },
        )
        runner._playback_guard(call_id).current_response_audio_published = True

        for frame in second_speech_frames:
            await runner.send_audio_frame(call_id, frame)

        event_types = [event.type for event in store.list(call_id)]
        assert event_types.count("sip_interrupt_candidate") == 2
        assert event_types.count("sip_pre_stop") == 2
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_defers_provider_speech_after_weak_local_noise_defer() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 14 + [False] * 20),
        call_id="call_sip_rejected_provider_speech_not_confirmed",
        clean_window_ms=40,
        max_hold_ms=80,
        recovery_silence_ms=10_000,
        recovery_max_per_turn=1,
    )
    speech_frames = _pcm16_constant_frames([
        3200,
        4300,
        5400,
        4700,
        3600,
        4400,
        5200,
        4700,
        5500,
        5200,
        4700,
        4300,
        5100,
        4600,
    ])
    quiet_frame = _pcm16_constant_frame(amplitude=50)

    try:
        for frame in speech_frames:
            await runner.send_audio_frame(call_id, frame)
        for _ in range(4):
            await runner.send_audio_frame(call_id, quiet_frame)
        await asyncio.sleep(0.12)

        event_types = [event.type for event in store.list(call_id)]
        assert "sip_pre_stop" not in event_types
        assert "sip_pre_stop_deferred" in event_types
        assert "sip_interrupt_rejected" not in event_types

        await runner._handle_user_speech_started(call_id, provider, datetime.now(timezone.utc))

        events = store.list(call_id)
        event_types = [event.type for event in events]
        assert "sip_interrupt_candidate_confirmed" not in event_types
        assert "interrupt_confirmed" not in event_types
        assert "sip_provider_speech_started_deferred" in event_types
        assert event_types.count("response_generation_invalidated") == 0
        assert publisher.stopped_call_ids == []
        assert provider.cancelled_response_count == 0
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_hard_stops_sip_candidate_after_provider_speech() -> None:
    registry = InMemorySessionRegistry()
    provider = FakeRealtimeProvider([])
    store = InMemoryEventStore()
    publisher = FakeAudioPublisher()
    call_id = "call_sip_barge_in_provider_confirmed"
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"sip-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        sip_barge_in_enabled=True,
        sip_barge_in_min_rms_dbfs=-35.0,
        sip_barge_in_min_speech_duration_ms=200,
    )
    loud_frame = _pcm16_constant_frame(amplitude=4000)

    try:
        await runner.start(session)
        registry.transition(call_id, CallSessionStatus.CONNECTED)
        registry.transition(call_id, CallSessionStatus.AI_THINKING)
        registry.transition(call_id, CallSessionStatus.AI_SPEAKING)
        runner._mark_response_started(
            call_id,
            {"response_id": "resp_sip_opening"},
            datetime.now(timezone.utc),
        )
        runner._response_lifecycle(call_id).active = True
        runner._playback_guard(call_id).current_response_audio_published = True

        for _ in range(10):
            await runner.send_audio_frame(call_id, loud_frame)
        await runner._handle_user_speech_started(call_id, provider, datetime.now(timezone.utc))

        events = store.list(call_id)
        event_types = [event.type for event in events]
        confirmed = next(
            event for event in events if event.type == "sip_interrupt_candidate_confirmed"
        )

        assert provider.cancelled_response_count == 1
        assert publisher.stopped_call_ids == [call_id]
        assert confirmed.payload["confirmedBy"] == "provider_speech_started"
        assert "sip_interrupt_candidate" in event_types
        assert "sip_interrupt_candidate_confirmed" in event_types
        assert "response_generation_invalidated" in event_types
        assert "interrupt_audio_stop_requested" in event_types
        assert "playout_queue_flushed" in event_types
        assert "interrupt_audio_stop_completed" in event_types
        assert "interrupt_confirmed" not in event_types
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_does_not_confirm_weak_sip_candidate_from_provider_speech() -> None:
    runner, _registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 12),
        call_id="call_sip_weak_provider_speech_not_confirmed",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    weak_flat_frame = _pcm16_constant_frame(amplitude=1050)

    try:
        for _ in range(12):
            await runner.send_audio_frame(call_id, weak_flat_frame)
        await runner._handle_user_speech_started(call_id, provider, datetime.now(timezone.utc))

        event_types = [event.type for event in store.list(call_id)]
        assert "sip_interrupt_candidate" in event_types
        assert "sip_interrupt_candidate_confirmed" not in event_types
        assert "interrupt_confirmed" not in event_types
        assert provider.cancelled_response_count == 0
        assert publisher.stopped_call_ids == []
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_rejects_short_non_turn_sip_transcript() -> None:
    runner, _registry, store, provider, _publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 16),
        call_id="call_sip_short_non_turn_transcript",
        clean_window_ms=40,
        max_hold_ms=100,
        recovery_silence_ms=20,
    )
    hot_frame = _pcm16_constant_frame(amplitude=16000)

    try:
        for _ in range(12):
            await runner.send_audio_frame(call_id, hot_frame)
        await runner._handle_user_speech_started(call_id, provider, datetime.now(timezone.utc))

        provider_event = ProviderEvent(type="user_transcript_done", payload={"transcript": "唉。"})
        trust_decision = runner._decide_realtime_transcript_trust(call_id, provider_event)
        await runner._handle_user_transcript(
            call_id,
            provider,
            ProviderEvent(
                type=provider_event.type,
                payload={**provider_event.payload, **trust_decision.as_payload()},
            ),
            datetime.now(timezone.utc),
        )

        event_types = [event.type for event in store.list(call_id)]
        assert trust_decision.commit_decision == "candidate"
        assert "user_transcript_semantic_rejected" in event_types
        assert "interrupt_confirmed" not in event_types
        assert provider.created_responses == []
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_trusts_short_transcript_after_sip_confirmed() -> None:
    runner, _registry, store, provider, _publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 20),
        call_id="call_sip_confirmed_short_transcript",
        clean_window_ms=40,
        max_hold_ms=100,
    )
    speech_frame = _pcm16_constant_frame(amplitude=8000)

    try:
        for _ in range(9):
            await runner.send_audio_frame(call_id, speech_frame)
        turn = runner._pending_turn(call_id)
        runner._confirm_sip_barge_in(
            call_id,
            turn,
            confirmed_by="provider_speech_started",
            reason="user_speech_started_during_ai_audio",
        )

        provider_event = ProviderEvent(type="user_transcript_done", payload={"transcript": "行。"})
        trust_decision = runner._decide_realtime_transcript_trust(call_id, provider_event)
        trusted_payload = trust_decision.as_payload()

        await runner._handle_user_transcript(
            call_id,
            provider,
            ProviderEvent(
                type=provider_event.type,
                payload={**provider_event.payload, **trusted_payload},
            ),
            datetime.now(timezone.utc),
        )

        event_types = [event.type for event in store.list(call_id)]
        assert trusted_payload["transcriptTrust"] == "trusted"
        assert trusted_payload["commitDecision"] == "commit"
        assert "sip_interrupt_candidate_confirmed" in event_types
        assert "user_transcript_semantic_rejected" not in event_types
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_confirms_sip_transcript_with_active_response_before_audio() -> None:
    runner, registry, store, provider, publisher, call_id = await _started_sip_runner(
        vad=FakeVad([True] * 12),
        call_id="call_sip_transcript_active_response_before_audio",
        clean_window_ms=40,
        max_hold_ms=100,
    )
    speech_frame = _pcm16_constant_frame(amplitude=4500)

    try:
        for _ in range(9):
            await runner.send_audio_frame(call_id, speech_frame)

        registry.transition(call_id, CallSessionStatus.CONNECTED)
        runner._response_lifecycle(call_id).active = True
        runner._playback_guard(call_id).current_response_audio_published = False

        provider_event = ProviderEvent(type="user_transcript_done", payload={"transcript": "说话呀。"})
        trust_decision = runner._decide_realtime_transcript_trust(call_id, provider_event)
        await runner._handle_user_transcript(
            call_id,
            provider,
            ProviderEvent(
                type=provider_event.type,
                payload={**provider_event.payload, **trust_decision.as_payload()},
            ),
            datetime.now(timezone.utc),
        )

        event_types = [event.type for event in store.list(call_id)]
        assert "sip_interrupt_candidate_confirmed" in event_types
        assert "interrupt_confirmed" in event_types
        assert "session_failed" not in event_types
        assert registry.get(call_id).status == CallSessionStatus.USER_SPEAKING
        assert publisher.stopped_call_ids == [call_id]
        assert provider.cancelled_response_count == 1
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_expires_unconfirmed_sip_candidate() -> None:
    registry = InMemorySessionRegistry()
    provider = FakeRealtimeProvider([])
    store = InMemoryEventStore()
    publisher = FakeAudioPublisher()
    call_id = "call_sip_barge_in_expired"
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"sip-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        sip_barge_in_enabled=True,
        sip_barge_in_min_rms_dbfs=-35.0,
        sip_barge_in_min_speech_duration_ms=200,
        sip_barge_in_hold_timeout_seconds=0.01,
    )
    loud_frame = _pcm16_constant_frame(amplitude=4000)

    try:
        await runner.start(session)
        registry.transition(call_id, CallSessionStatus.CONNECTED)
        registry.transition(call_id, CallSessionStatus.AI_THINKING)
        registry.transition(call_id, CallSessionStatus.AI_SPEAKING)
        runner._mark_response_started(
            call_id,
            {"response_id": "resp_sip_opening"},
            datetime.now(timezone.utc),
        )
        runner._response_lifecycle(call_id).active = True
        runner._playback_guard(call_id).current_response_audio_published = True

        for _ in range(10):
            await runner.send_audio_frame(call_id, loud_frame)
        for _ in range(10):
            if any(event.type == "sip_interrupt_candidate_expired" for event in store.list(call_id)):
                break
            await asyncio.sleep(0.01)

        events = store.list(call_id)
        event_types = [event.type for event in events]
        ignored = next(event for event in events if event.type == "interrupt_ignored")
        guard = runner._playback_guard(call_id)

        assert provider.cancelled_response_count == 0
        assert publisher.stopped_call_ids == []
        assert "sip_interrupt_candidate" in event_types
        assert "sip_interrupt_candidate_expired" in event_types
        assert "sip_interrupt_candidate_confirmed" not in event_types
        assert "response_generation_invalidated" not in event_types
        assert ignored.payload["reason"] == "sip_barge_in_expired"
        assert guard.user_speech_active is False
        assert guard.suppress_audio_until is None
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_treats_late_sip_transcript_as_user_turn() -> None:
    registry = InMemorySessionRegistry()
    provider = FakeRealtimeProvider([])
    store = InMemoryEventStore()
    publisher = FakeAudioPublisher()
    call_id = "call_sip_candidate_late_transcript"
    session = CallSession(
        call_id=call_id,
        room_name=f"ai-call-{call_id}",
        participant_identity=f"sip-{call_id}",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        sip_barge_in_enabled=True,
        sip_barge_in_min_rms_dbfs=-35.0,
        sip_barge_in_min_speech_duration_ms=200,
        user_turn_stability_delay_seconds=0,
    )
    loud_frame = _pcm16_constant_frame(amplitude=4000)

    try:
        await runner.start(session)
        registry.transition(call_id, CallSessionStatus.CONNECTED)
        registry.transition(call_id, CallSessionStatus.AI_THINKING)
        registry.transition(call_id, CallSessionStatus.AI_SPEAKING)
        runner._mark_response_started(
            call_id,
            {"response_id": "resp_sip_opening"},
            datetime.now(timezone.utc),
        )
        runner._response_lifecycle(call_id).active = True
        runner._playback_guard(call_id).current_response_audio_published = True

        for _ in range(10):
            await runner.send_audio_frame(call_id, loud_frame)

        await runner._apply_provider_event(
            call_id,
            provider,
            "model_response_done",
            datetime.now(timezone.utc),
            {"response": {"id": "resp_sip_opening", "status": "completed"}},
        )
        assert registry.get(call_id).status == CallSessionStatus.CONNECTED

        await runner._handle_user_speech_started(call_id, provider, datetime.now(timezone.utc))
        await runner._handle_user_transcript(
            call_id,
            provider,
            ProviderEvent(type="user_transcript_delta", payload={"delta": "你好"}),
            datetime.now(timezone.utc),
        )
        await runner._handle_user_speech_stopped(call_id, provider, datetime.now(timezone.utc))

        events = store.list(call_id)
        event_types = [event.type for event in events]
        assert registry.get(call_id).status == CallSessionStatus.AI_THINKING
        assert provider.cancelled_response_count == 0
        assert provider.created_responses == [None]
        assert "sip_interrupt_candidate" in event_types
        assert "sip_interrupt_candidate_confirmed" not in event_types
        assert "interrupt_audio_stop_requested" not in event_types
        assert "session_failed" not in event_types
        assert any(
            event.type == "interrupt_ignored" and event.payload["reason"] == "not_interrupt"
            for event in events
        )
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_realtime_agent_runner_does_not_play_fixed_handoff_prompt_before_stop() -> None:
    registry = InMemorySessionRegistry()
    provider = FakeRealtimeProvider([])
    store = InMemoryEventStore()
    publisher = ImmediatePlayoutAudioPublisher()
    session = CallSession(
        call_id="call_handoff_prompt",
        room_name="ai-call-call_handoff_prompt",
        participant_identity="browser-call_handoff_prompt",
        status=CallSessionStatus.READY,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )

    await runner.start(session)
    await runner.suspend_for_handoff("call_handoff_prompt")

    assert publisher.stopped_call_ids == ["call_handoff_prompt"]
    assert publisher.published == []
    assert provider.cancelled_response_count == 1
    assert provider.cleared_input_count == 1
    assert provider.closed is True
    event_types = [event.type for event in store.list("call_handoff_prompt")]
    assert "handoff_prompt_started" not in event_types
    assert "handoff_prompt_done" not in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_skips_model_handoff_prompt_before_stop() -> None:
    registry = InMemorySessionRegistry()
    provider = FakeRealtimeProvider([])
    store = InMemoryEventStore()
    publisher = ImmediatePlayoutAudioPublisher()
    session = CallSession(
        call_id="call_handoff_no_model_prompt",
        room_name="ai-call-call_handoff_no_model_prompt",
        participant_identity="browser-call_handoff_no_model_prompt",
        status=CallSessionStatus.CONNECTED,
        effective_config={
            "voice": "Tina",
            "prompt": "简短回答",
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
    )
    registry.add(session)
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
        ai_speaking_tail_grace_seconds=0,
    )

    await runner.start(session)
    await runner.suspend_for_handoff("call_handoff_no_model_prompt")

    assert provider.closed is True
    assert publisher.published == []
    assert provider.created_responses == []
    event_types = [event.type for event in store.list("call_handoff_no_model_prompt")]
    assert "handoff_model_prompt_started" not in event_types
    assert "handoff_model_prompt_done" not in event_types
    assert "handoff_prompt_started" not in event_types


def test_session_api_returns_unified_camel_case_response() -> None:
    orchestrator, _livekit, _agent = build_orchestrator()
    app = FastAPI()
    app.include_router(AiCallRouter)
    app.dependency_overrides[get_ai_call_service] = lambda: AiCallService(orchestrator)

    with TestClient(app) as client:
        create_response = client.post(
            "/ai-call/sessions",
            json={"voice": "Cindy", "sceneCode": "debt_promise_repay_reminder"},
        )
        assert create_response.status_code == 200
        create_body = create_response.json()
        assert create_body["code"] == 200
        assert create_body["msg"] == "创建成功"
        assert create_body["data"]["callId"].startswith("call_")
        assert create_body["data"]["effectiveConfig"]["voice"] == "Cindy"
        assert "participantToken" in create_body["data"]

        call_id = create_body["data"]["callId"]
        status_body = client.get(f"/ai-call/sessions/{call_id}").json()
        assert status_body["data"]["status"] == "ready"

        events_body = client.get(f"/ai-call/sessions/{call_id}/events").json()
        assert events_body["data"]["total"] == 6

        browser_event_body = client.post(
            f"/ai-call/sessions/{call_id}/browser-events",
            json={"type": "browser_first_audio"},
        ).json()
        assert browser_event_body["msg"] == "上报成功"
        assert browser_event_body["data"]["type"] == "browser_first_audio"

        end_body = client.post(f"/ai-call/sessions/{call_id}/end").json()
        assert end_body["msg"] == "结束成功"
        assert end_body["data"]["status"] == "completed"


def read_ai_call_web_asset(name: str) -> str:
    return (Path(__file__).parents[1] / "static/ai-call" / name).read_text(encoding="utf-8")


def test_phase_a_web_probe_page_wires_core_session_endpoints() -> None:
    html = read_ai_call_web_asset("customer.html")
    css = read_ai_call_web_asset("ai-call.css")
    script = read_ai_call_web_asset("customer.js")

    assert 'id="create-session"' in html
    assert 'rel="icon"' in html
    assert 'href="/static/ai-call/ai-call.css' in html or 'href="./ai-call.css' in html
    assert 'src="/static/ai-call/customer.js' in html or 'src="./customer.js' in html
    assert 'id="status-pill"' not in html
    assert 'id="end-session"' in html
    assert 'id="metric-model-stats"' in html
    assert 'id="metric-browser-stats"' in html
    assert 'class="control-actions"' in html
    assert 'id="request-handoff"' not in html
    assert "主动转人工" not in html
    assert "requestHandoff" not in script
    assert "发起转人工" not in html
    assert "麦克风：关" in html
    create_index = html.index('id="create-session"')
    mute_index = html.index('id="mute-room"')
    handoff_state_index = html.index('id="handoff-state"')
    assert create_index < mute_index < handoff_state_index
    assert "转人工状态" in html
    assert "转人工闭环" not in html
    assert '<div id="recording-state" class="recording-box">' in html
    assert '<div id="handoff-state" class="handoff-box">' in html
    assert '<div class="subtle">等待创建会话</div>' in html
    assert '<span class="constraint">等待创建会话</span>' not in html
    overview_index = html.index("overview-metrics-panel")
    log_index = html.index('id="log"')
    latency_index = html.index("latency-panel")
    recording_index = html.index("recording-panel")
    dialogue_index = html.index("dialogue-panel")
    assert overview_index < log_index < latency_index < recording_index < handoff_state_index
    assert handoff_state_index < dialogue_index
    assert ".overview-log .log" in css
    assert "max-height: 420px" in css
    assert "height: clamp(260px" in css
    assert "@media (min-width: 981px)" in css
    assert "height: clamp(420px, calc(100vh - 300px), 520px)" in css
    assert "min-height: 420px" in css
    assert "max-height: 520px" in css
    assert ".main-stack {\n  grid-template-columns: 1fr;" in css
    assert ".control-actions" in css
    assert "/ai-call/sessions" in script
    assert "/browser-events" in script
    assert "browser_ready" in script
    assert "browser_first_audio" in script
    assert "browser_disconnect" in script
    assert "navigator.sendBeacon" in script
    assert 'window.addEventListener("pagehide"' in script
    assert "suppressDisconnectReport" in script
    assert "if (!el.statusPill) return;" in script
    assert '"麦克风：开"' in script
    assert '"麦克风：关"' in script
    assert "暂无转人工记录" in script
    assert "暂无转人工请求" in script
    assert "LivekitClient" in script


def test_customer_page_wires_sip_outbound_mode_without_replacing_web_mode() -> None:
    html = read_ai_call_web_asset("customer.html")
    script = read_ai_call_web_asset("customer.js")

    assert 'id="call-mode-select"' in html
    assert 'value="web"' in html
    assert "浏览器通话" in html
    assert 'value="sip"' in html
    assert "电话外呼" in html
    assert 'id="sip-target-select"' in html
    assert 'value="linphone"' in html
    assert "本机软电话 Linphone" in html
    assert 'value="real_phone"' in html
    assert "真实电话" in html
    assert 'id="callee-phone-input"' in html
    assert 'id="ringing-timeout-input"' in html
    assert "/ai-call/sessions" in script
    assert "/ai-call/sip-sessions" in script
    assert "calleePhoneNumber" in script
    assert "ringingTimeoutSeconds" in script
    assert "createSipSession" in script
    assert "confirmRealPhoneCall" in script
    assert "sip_interrupt_candidate" in script
    assert "电话侧检测到插话候选" in script
    assert "sip_interrupt_candidate_confirmed" in script
    assert "sip_interrupt_candidate_expired" in script


def test_customer_page_defaults_linphone_callee_without_relaxing_real_phone_guard() -> None:
    html = read_ai_call_web_asset("customer.html")
    script = read_ai_call_web_asset("customer.js")

    assert "留空默认拨打本机 Linphone" in html
    assert "LINPHONE_DEFAULT_CALLEE_NUMBER" in script
    assert 'LINPHONE_DEFAULT_CALLEE_NUMBER = "19900001001"' in script
    assert "resolveCalleePhoneNumber" in script
    assert "return LINPHONE_DEFAULT_CALLEE_NUMBER;" in script
    assert "请填写真实电话被叫号码" in script
    assert "isRealPhoneTarget()" in script
    assert "confirmRealPhoneCall" in script


def test_phase_a_web_probe_requests_microphone_before_joining_room() -> None:
    script = read_ai_call_web_asset("customer.js")

    local_track_index = script.index("audioTrack = await createLocalAudioTrack")
    room_connect_index = script.index("await room.connect")
    catch_index = script.index("} catch (error) {")
    disconnect_index = script.index("room.disconnect();", catch_index)

    assert local_track_index < room_connect_index
    assert catch_index < disconnect_index


def test_phase_a_web_probe_reports_audio_input_diagnostics_before_browser_ready() -> None:
    script = read_ai_call_web_asset("customer.js")

    assert "function buildAudioInputDiagnostics" in script
    assert "async function reportBrowserAudioInputDiagnostics" in script
    assert 'type: "browser_audio_input_diagnostics"' in script
    assert "track.mediaStreamTrack.getSettings()" in script
    assert "track.mediaStreamTrack.getConstraints()" in script
    assert "state.session.webAudioConstraints" in script
    diagnostics_index = script.index("await reportBrowserAudioInputDiagnostics(audioTrack)")
    ready_index = script.index("await reportBrowserReady()")
    assert diagnostics_index < ready_index


def test_phase_a_web_probe_reports_local_speech_candidates() -> None:
    script = read_ai_call_web_asset("customer.js")

    assert "startLocalSpeechMonitor" in script
    assert "stopLocalSpeechMonitor" in script
    assert "checkLocalSpeechLevel" in script
    assert "reportBrowserSpeechSegment" in script
    assert 'type: "browser_user_speech_segment"' in script
    assert "durationMs" in script
    assert "snrDb" in script
    assert "hotFrameCount" in script
    assert "state.remoteAudioActive" in script


def test_customer_dialogue_labels_interrupted_ai_segments() -> None:
    script = read_ai_call_web_asset("customer.js")

    assert "function dialogueSpeakerName" in script
    assert 'segment.segmentStatus === "interrupted"' in script
    assert '"AI（已打断）"' in script
    assert "dialogueSpeakerName(segment)" in script


def test_customer_web_probe_reports_local_speech_before_remote_audio_active() -> None:
    if not shutil.which("node"):
        pytest.skip("node is required to execute the customer page VAD behavior test")

    script = read_ai_call_web_asset("customer.js").split(
        "async function reportBrowserDisconnect",
        maxsplit=1,
    )[0]
    harness = f"""
const reported = [];
function elementStub() {{
  return {{
    textContent: "",
    innerHTML: "",
    disabled: false,
    dataset: {{}},
    classList: {{ toggle() {{}}, add() {{}}, remove() {{}} }},
    setAttribute() {{}},
    addEventListener() {{}},
    appendChild() {{}},
    remove() {{}},
    value: "",
  }};
}}
globalThis.window = {{
  location: {{ pathname: "/static/ai-call/customer.html" }},
}};
globalThis.document = {{
  querySelector() {{ return elementStub(); }},
  createElement() {{ return elementStub(); }},
  body: {{ appendChild() {{}}, classList: {{ toggle() {{}} }} }},
  documentElement: {{ dataset: {{}} }},
}};
globalThis.fetch = async (_url, options) => {{
  reported.push(JSON.parse(options.body));
  return {{
    ok: true,
    json: async () => ({{ code: 200, data: {{}} }}),
  }};
}};
{script}
async function refreshStatus() {{}}
async function refreshEvents() {{}}
state.session = {{ callId: "call_opening" }};
state.localMuted = false;
state.localSpeechBaselineRms = 0.006;
state.localAudioSamples = new Uint8Array(512);
state.localAudioAnalyser = {{
  getByteTimeDomainData(samples) {{
    samples.fill(160);
  }},
}};
state.remoteAudioActive = false;
for (let index = 0; index < 3; index += 1) {{
  checkLocalSpeechLevel();
}}
await new Promise((resolve) => setTimeout(resolve, 0));
console.log(JSON.stringify(reported));
"""
    result = subprocess.run(
        ["node", "--input-type=module"],
        input=harness,
        text=True,
        capture_output=True,
        check=True,
    )
    events = json.loads(result.stdout)

    assert events
    assert events[0]["type"] == "browser_user_speech_segment"
    assert events[0]["phase"] == "started"
    assert events[0]["remoteAudioActive"] is False


def test_agent_join_failure_clears_local_room_state_and_refreshes_lists() -> None:
    if not shutil.which("node"):
        pytest.skip("node is required to execute the agent page behavior test")

    script = read_ai_call_web_asset("agent.js")
    harness = f"""
const apiCalls = [];
let disconnected = 0;
let trackStopped = 0;

function elementStub() {{
  return {{
    textContent: "",
    innerHTML: "",
    disabled: false,
    dataset: {{}},
    className: "",
    classList: {{ toggle() {{}}, add() {{}}, remove() {{}} }},
    setAttribute() {{}},
    addEventListener() {{}},
    appendChild() {{}},
    remove() {{}},
    pause() {{}},
    value: "online",
  }};
}}

globalThis.window = {{
  isSecureContext: true,
  location: {{ pathname: "/static/ai-call/agent.html" }},
  setTimeout: () => 0,
  setInterval: () => 0,
  clearInterval() {{}},
  addEventListener() {{}},
  LivekitClient: {{
    RoomEvent: {{
      TrackSubscribed: "TrackSubscribed",
      Disconnected: "Disconnected",
    }},
    Room: class {{
      constructor() {{
        this.localParticipant = {{
          publishTrack: async () => {{
            state.selectedHandoff = null;
          }},
        }};
      }}
      on() {{}}
      async connect() {{}}
      disconnect() {{
        disconnected += 1;
      }}
    }},
    createLocalAudioTrack: async () => ({{
      stop() {{
        trackStopped += 1;
      }},
      mediaStreamTrack: {{ enabled: true }},
    }}),
  }},
}};
globalThis.setTimeout = window.setTimeout;
globalThis.setInterval = window.setInterval;
globalThis.clearInterval = window.clearInterval;
Object.defineProperty(globalThis, "navigator", {{
  value: {{ mediaDevices: {{ getUserMedia() {{}} }} }},
  configurable: true,
}});
globalThis.document = {{
  querySelector() {{ return elementStub(); }},
  createElement() {{ return elementStub(); }},
  body: {{ appendChild() {{}}, classList: {{ toggle() {{}} }} }},
  documentElement: {{ dataset: {{}} }},
}};
globalThis.fetch = async (url, options = {{}}) => {{
  apiCalls.push({{ url, method: options.method || "GET" }});
  if (String(url).includes("/handoff-agents/")) {{
    return {{
      ok: true,
      json: async () => ({{
        code: 200,
        data: {{
          humanAgentIdentity: "agent-debug-001",
          status: "online",
          activeHandoffId: null,
        }},
      }}),
    }};
  }}
  if (String(url).includes("/handoffs/joinable")) {{
    return {{
      ok: true,
      json: async () => ({{ code: 200, data: {{ rows: [], total: 0 }} }}),
    }};
  }}
  if (String(url).endsWith("/accept")) {{
    return {{
      ok: true,
      json: async () => ({{
        code: 200,
        data: {{
          handoff: {{
            handoffId: "handoff_failed_connect",
            callId: "call_failed_connect",
            roomName: "ai-call-call_failed_connect",
            status: "accepted",
            humanAgentIdentity: "agent-debug-001",
            expiresAt: new Date(Date.now() + 30000).toISOString(),
          }},
          seatToken: {{
            livekitUrl: "wss://livekit.test",
            participantToken: "seat-token",
          }},
        }},
      }}),
    }};
  }}
  if (String(url).endsWith("/connected")) {{
    return {{
      ok: false,
      json: async () => ({{ code: 500, msg: "connected failed" }}),
    }};
  }}
  if (String(url).endsWith("/fail")) {{
    return {{
      ok: true,
      json: async () => ({{
        code: 200,
        data: {{
          handoffId: "handoff_failed_connect",
          callId: "call_failed_connect",
          roomName: "ai-call-call_failed_connect",
          status: "failed",
          failureStage: "agent_connect",
          failureMessage: "connected failed",
          expiresAt: new Date(Date.now() + 30000).toISOString(),
        }},
      }}),
    }};
  }}
  throw new Error(`unexpected api call: ${{url}}`);
}};

{script}

state.selectedHandoff = {{
  handoffId: "handoff_failed_connect",
  callId: "call_failed_connect",
  roomName: "ai-call-call_failed_connect",
  status: "requested",
  expiresAt: new Date(Date.now() + 30000).toISOString(),
}};
state.agentStatus = {{ status: "online", activeHandoffId: null }};
apiCalls.length = 0;

let errorMessage = "";
try {{
  await joinSelectedHandoff();
}} catch (error) {{
  errorMessage = error.message;
}}

console.log(JSON.stringify({{
  errorMessage,
  roomCleared: state.room === null,
  localTrackCleared: state.localTrack === null,
  tokenCleared: state.seatToken === null,
  disconnected,
  trackStopped,
  connectedCalls: apiCalls.filter((call) =>
    String(call.url).includes("/connected")
  ).length,
  failCalls: apiCalls.filter((call) =>
    String(call.url).includes("/fail")
  ).length,
  agentStatusRefreshes: apiCalls.filter((call) =>
    String(call.url).includes("/handoff-agents/")
  ).length,
  joinableRefreshes: apiCalls.filter((call) =>
    String(call.url).includes("/handoffs/joinable")
  ).length,
}}));
"""
    result = subprocess.run(
        ["node", "--input-type=module"],
        input=harness,
        text=True,
        capture_output=True,
        check=True,
    )
    outcome = json.loads(result.stdout)

    assert outcome["errorMessage"] == "connected failed"
    assert outcome["roomCleared"] is True
    assert outcome["localTrackCleared"] is True
    assert outcome["tokenCleared"] is True
    assert outcome["disconnected"] >= 1
    assert outcome["trackStopped"] >= 1
    assert outcome["connectedCalls"] == 1
    assert outcome["failCalls"] == 1
    assert outcome["agentStatusRefreshes"] >= 2
    assert outcome["joinableRefreshes"] >= 1


def test_agent_presence_select_change_saves_status() -> None:
    if not shutil.which("node"):
        pytest.skip("node is required to execute the agent page behavior test")

    script = read_ai_call_web_asset("agent.js")
    harness = """
const apiCalls = [];
const elements = new Map();

function elementStub(selector = "") {
  return {
    textContent: "",
    innerHTML: "",
    disabled: false,
    dataset: {},
    className: "",
    classList: { toggle() {}, add() {}, remove() {} },
    listeners: {},
    value: selector === "#agent-identity" ? "agent-debug-001" : "offline",
    setAttribute() {},
    addEventListener(type, handler) {
      this.listeners[type] = handler;
    },
    appendChild() {},
    remove() {},
    pause() {},
  };
}

function elementFor(selector) {
  if (!elements.has(selector)) {
    elements.set(selector, elementStub(selector));
  }
  return elements.get(selector);
}

globalThis.window = {
  isSecureContext: true,
  location: { pathname: "/static/ai-call/agent.html" },
  setTimeout: () => 0,
  setInterval: () => 0,
  clearInterval() {},
  addEventListener() {},
  LivekitClient: {
    RoomEvent: {
      TrackSubscribed: "TrackSubscribed",
      Disconnected: "Disconnected",
    },
    Room: class {
      constructor() {
        this.localParticipant = { publishTrack: async () => {} };
      }
      on() {}
      async connect() {}
      disconnect() {}
    },
    createLocalAudioTrack: async () => ({
      stop() {},
      mediaStreamTrack: { enabled: true },
    }),
  },
};
globalThis.setTimeout = window.setTimeout;
globalThis.setInterval = window.setInterval;
globalThis.clearInterval = window.clearInterval;
Object.defineProperty(globalThis, "navigator", {
  value: { mediaDevices: { getUserMedia() {} } },
  configurable: true,
});
globalThis.document = {
  querySelector(selector) { return elementFor(selector); },
  createElement() { return elementStub(); },
  body: { appendChild() {}, classList: { toggle() {} } },
  documentElement: { dataset: {} },
};
globalThis.fetch = async (url, options = {}) => {
  apiCalls.push({
    url: String(url),
    method: options.method || "GET",
    body: options.body || null,
  });
  if (String(url).includes("/handoff-agents/")) {
    const requestBody = options.body ? JSON.parse(options.body) : {};
    return {
      ok: true,
      json: async () => ({
        code: 200,
        data: {
          humanAgentIdentity: "agent-debug-001",
          status: requestBody.status || "offline",
          activeHandoffId: null,
        },
      }),
    };
  }
  if (String(url).includes("/handoffs/joinable")) {
    return {
      ok: true,
      json: async () => ({ code: 200, data: { rows: [], total: 0 } }),
    };
  }
  throw new Error(`unexpected api call: ${url}`);
};

""" + script + """

await Promise.resolve();
await Promise.resolve();
apiCalls.length = 0;

const presence = elements.get("#agent-presence");
const changeHandler = presence.listeners.change;
presence.value = "online";
if (changeHandler) {
  await changeHandler();
  await Promise.resolve();
}

const statusCalls = apiCalls.filter((call) =>
  call.method === "POST" && call.url.includes("/handoff-agents/agent-debug-001/status")
);

console.log(JSON.stringify({
  hasChangeHandler: Boolean(changeHandler),
  statusCallCount: statusCalls.length,
  lastStatusBody: statusCalls.length ? JSON.parse(statusCalls.at(-1).body) : null,
}));
"""
    result = subprocess.run(
        ["node", "--input-type=module"],
        input=harness,
        text=True,
        capture_output=True,
        check=True,
    )
    outcome = json.loads(result.stdout)

    assert outcome["hasChangeHandler"] is True
    assert outcome["statusCallCount"] == 1
    assert outcome["lastStatusBody"] == {"status": "online", "skillGroup": "default"}


def test_customer_web_probe_normalizes_audio_connect_errors() -> None:
    script = read_ai_call_web_asset("customer.js")

    assert "microphoneErrorMessage" in script
    assert "浏览器麦克风权限被拒绝" in script
    assert "没有检测到可用麦克风" in script
    assert "连接 LiveKit 房间失败" in script
    assert "发布麦克风到 LiveKit 房间失败" in script
    assert "麦克风已连接，但上报浏览器就绪失败" in script
    assert 'actionErrorMessage(error, "连接麦克风失败")' in script


def test_phase_a_web_probe_reports_browser_first_audio_from_remote_audio_level() -> None:
    script = read_ai_call_web_asset("customer.js")

    assert "startRemoteAudioMonitor(track)" in script
    assert "REMOTE_AUDIO_START_RMS = 0.015" in script
    assert "pendingBrowserFirstAudioTurnId" in script
    assert "const eventType = getEventType(event)" in script
    assert 'eventType === "opening_started"' in script
    assert 'eventType === "user_speech_stopped"' in script
    assert 'type: "browser_first_audio"' in script
    assert "media.onplaying" not in script


def test_phase_a_web_probe_fetches_events_incrementally() -> None:
    script = read_ai_call_web_asset("customer.js")

    assert "EVENT_RENDER_LIMIT = 300" in script
    assert "state.lastEventId = event.eventId" in script
    assert 'params.set("afterEventId", state.lastEventId)' in script
    assert "appendEvents(data.rows)" in script


def test_phase_a_web_probe_stops_polling_on_terminal_status() -> None:
    script = read_ai_call_web_asset("customer.js")

    assert 'return status === "completed" || status === "failed"' in script
    assert "stopPolling()" in script
    assert "stopClientAudioRuntime()" in script
    assert "disableSessionControls()" in script


def test_phase_a_web_probe_does_not_duck_remote_audio_for_local_speech() -> None:
    script = read_ai_call_web_asset("customer.js")

    assert "BROWSER_SPEECH_" not in script
    assert "duckRemoteAudioBriefly" not in script
    assert "restoreRemoteAudioDuck" not in script
    assert "REMOTE_AUDIO_DUCK_" not in script


@pytest.mark.anyio
async def test_standalone_lifespan_skips_system_service_startup(monkeypatch) -> None:
    app = FastAPI()
    monkeypatch.setattr(init_app.settings, "AI_CALL_STANDALONE_ENABLE", True, raising=False)
    monkeypatch.setattr(init_app.settings, "AI_CALL_RECORDING_ENABLED", False, raising=False)

    async def fail_import_modules_async(*args, **kwargs):
        raise AssertionError("standalone mode must not import system startup modules")

    monkeypatch.setattr(init_app, "import_modules_async", fail_import_modules_async)

    async with init_app.lifespan(app):
        assert not hasattr(app.state, "redis")


@pytest.mark.anyio
async def test_standalone_lifespan_initializes_oss_when_recording_enabled(
    monkeypatch,
) -> None:
    from app.api.v1.system.oss.service import OssService

    app = FastAPI()
    calls: list[str] = []

    async def no_start_worker():
        return None

    async def no_stop_worker(worker) -> None:
        return None

    async def fake_init_active_config() -> None:
        calls.append("init")

    monkeypatch.setattr(init_app.settings, "AI_CALL_STANDALONE_ENABLE", True, raising=False)
    monkeypatch.setattr(init_app.settings, "SQL_DB_ENABLE", True, raising=False)
    monkeypatch.setattr(init_app.settings, "AI_CALL_RECORDING_ENABLED", True, raising=False)
    monkeypatch.setattr(init_app, "_start_ai_call_event_worker", no_start_worker)
    monkeypatch.setattr(init_app, "_start_ai_call_dialogue_worker", no_start_worker)
    monkeypatch.setattr(init_app, "_start_ai_call_offline_asr_worker", no_start_worker)
    monkeypatch.setattr(init_app, "_start_ai_call_recording_reconcile_worker", no_start_worker)
    monkeypatch.setattr(init_app, "_start_ai_call_handoff_trigger_worker", no_start_worker)
    monkeypatch.setattr(init_app, "_stop_ai_call_event_worker", no_stop_worker)
    monkeypatch.setattr(init_app, "_stop_ai_call_dialogue_worker", no_stop_worker)
    monkeypatch.setattr(init_app, "_stop_ai_call_offline_asr_worker", no_stop_worker)
    monkeypatch.setattr(init_app, "_stop_ai_call_recording_reconcile_worker", no_stop_worker)
    monkeypatch.setattr(init_app, "_stop_ai_call_handoff_trigger_worker", no_stop_worker)
    monkeypatch.setattr(OssService, "init_active_config", fake_init_active_config)

    async with init_app.lifespan(app):
        assert calls == ["init"]


def test_standalone_router_registration_only_exposes_ai_call_routes(monkeypatch) -> None:
    app = FastAPI()
    monkeypatch.setattr(init_app.settings, "AI_CALL_STANDALONE_ENABLE", True, raising=False)

    init_app.register_routers(app)

    paths = {route.path for route in app.routes}
    assert "/ai-call/health" in paths
    assert not any(path.startswith("/system") for path in paths)


def test_standalone_settings_clear_root_path_for_local_static_assets() -> None:
    settings = Settings(
        AI_CALL_STANDALONE_ENABLE=True,
        ROOT_PATH="/ai-call-api/v1",
    )

    assert settings.FASTAPI_CONFIG["root_path"] == ""


def test_log_sanitizer_redacts_bearer_tokens() -> None:
    message = "Authorization: Bearer sk-secret-token"

    assert sanitize_log_message(message) == "Authorization: Bearer <redacted>"
