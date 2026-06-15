import asyncio
import base64
import struct
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.ai_call import AiCallRouter
from app.api.v1.ai_call.controller import get_ai_call_service
from app.api.v1.ai_call.service import AiCallService
from app.config.setting import Settings
from app.core.logger import sanitize_log_message
from app.plugin import init_app
from app.services.ai_call.agent_runner import RealtimeCallAgentRunner
from app.services.ai_call.audio_bridge import AudioBridgeError, PcmAudioBridge, PcmAudioFrame
from app.services.ai_call.event_store import InMemoryEventStore
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.livekit_audio_transport import LiveKitRoomAudioTransport
from app.services.ai_call.livekit_room import BrowserRoomToken
from app.services.ai_call.metrics import CallMetrics
from app.services.ai_call.orchestrator import AiCallOrchestrator, AiCallRuntimeConfig
from app.services.ai_call.providers.aliyun_qwen_realtime import (
    AliyunQwenRealtimeProvider,
    QwenRealtimeSessionConfig,
    build_session_update_event,
    map_qwen_server_event,
)
from app.services.ai_call.providers.base import ProviderEvent
from app.services.ai_call.session_registry import (
    CallSession,
    CallSessionStatus,
    InMemorySessionRegistry,
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


class FailingAgentRunner(FakeAgentRunner):
    async def start(self, session: CallSession) -> None:
        await super().start(session)
        raise RuntimeError("agent boom")


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
    def __init__(self, sample_rate: int, num_channels: int) -> None:
        self.sample_rate = sample_rate
        self.num_channels = num_channels
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
class FakeRemoteAudioTrack:
    audio_events: list[FakeRtcAudioFrameEvent]


def build_orchestrator() -> tuple[AiCallOrchestrator, FakeLiveKitRoomManager, FakeAgentRunner]:
    livekit = FakeLiveKitRoomManager()
    agent = FakeAgentRunner()
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
            opening_enabled=True,
            opening_message="您好，我是凌辰智能助手，请问现在方便简单沟通一下吗？",
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
            opening_enabled=True,
            opening_message="您好，我是凌辰智能助手，请问现在方便简单沟通一下吗？",
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
    assert result.effective_config.opening_enabled is True
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
            opening_enabled=True,
            opening_message="您好，我是凌辰智能助手，请问现在方便简单沟通一下吗？",
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

    assert agent.browser_speech_candidates == [(created.call_id, reported_at)]
    events = await orchestrator.list_events(created.call_id)
    assert events.rows[-1].type == "browser_user_speech_started"
    assert events.rows[-1].source == "browser"


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

    assert agent.browser_speech_candidates == [(created.call_id, reported_at)]
    events = await orchestrator.list_events(created.call_id)
    assert events.rows[-1].type == "browser_user_speech_started"
    assert events.rows[-1].source == "browser"


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
        object(),
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
async def test_livekit_room_audio_transport_adds_short_fade_out_when_stopping_audio() -> None:
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
    assert len(published_track.source.captured_frames) == 2
    fade_frame = published_track.source.captured_frames[-1]
    fade_samples = struct.unpack(
        "<" + "h" * fade_frame.samples_per_channel,
        bytes(fade_frame.data),
    )
    assert fade_frame.samples_per_channel == 1920
    assert fade_samples[0] == 12000
    assert abs(fade_samples[-1]) <= 10
    assert 75 <= fade_frame.samples_per_channel * 1000 // fade_frame.sample_rate <= 85


@pytest.mark.anyio
async def test_livekit_room_audio_transport_stop_audio_keeps_clear_when_fade_capture_fails() -> None:
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

    assert map_qwen_server_event({"type": "response.audio.delta"}) == "model_audio_delta"
    assert map_qwen_server_event({"type": "input_audio_buffer.speech_stopped"}) == (
        "user_speech_stopped"
    )
    assert map_qwen_server_event({"type": "conversation.item.created"}) == (
        "conversation_item_created"
    )
    assert map_qwen_server_event({"type": "input_audio_buffer.committed"}) == (
        "input_audio_committed"
    )
    assert map_qwen_server_event(
        {"type": "conversation.item.input_audio_transcription.failed"}
    ) == "user_transcript_failed"
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
    ]
    assert sockets[0].closed is True


@pytest.mark.anyio
async def test_qwen_provider_receives_mapped_events_and_preserves_payload() -> None:
    socket = FakeQwenWebSocket(
        incoming=[
            {"type": "session.created", "session": {"id": "sess_1"}},
            {"type": "response.audio.delta", "delta": "AAAA"},
            {"type": "conversation.item.created", "item": {"id": "item_1"}},
            {"type": "input_audio_buffer.committed", "item_id": "item_1"},
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
        "user_transcript_failed",
        "provider_event_unmapped",
    ]
    assert events[0].payload == {"type": "session.created", "session": {"id": "sess_1"}}
    assert events[1].payload == {"type": "response.audio.delta", "delta": "AAAA"}
    assert events[5].payload == {
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
    provider = FakeRealtimeProvider(
        [
            ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}),
            ProviderEvent(type="user_speech_started", payload={}),
            ProviderEvent(type="user_transcript_delta", payload={"delta": "你好"}),
            ProviderEvent(type="user_speech_stopped", payload={}),
            ProviderEvent(type="model_audio_delta", payload={"delta": "AAAA"}),
            ProviderEvent(type="model_response_done", payload={}),
        ]
    )
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
    )

    await runner.start(session)
    await runner.wait("call_1")

    assert provider.connected is True
    assert provider.session_updates == [
        QwenRealtimeSessionConfig(
            voice="Tina",
            instructions="你是一个电话外呼助手，回答要简短自然。",
            vad_type="server_vad",
            vad_threshold=0.5,
            vad_silence_duration_ms=800,
        )
    ]
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
async def test_realtime_agent_runner_uses_qwen_text_stash_transcript_preview() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = FakeRealtimeProvider(
        [
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
        ]
    )
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
        event for event in store.list("call_qwen_transcript") if event.type == "user_transcript_delta"
    ]
    assert transcript_events[-1].payload == {"text": "今天", "stash": "天气怎么样"}


@pytest.mark.anyio
async def test_realtime_agent_runner_records_transcription_failure_without_reply() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    provider = FakeRealtimeProvider(
        [
            ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}),
            ProviderEvent(type="user_speech_started", payload={}),
            ProviderEvent(
                type="user_transcript_failed",
                payload={"error": {"code": "asr_failed", "message": "no speech"}},
            ),
            ProviderEvent(type="user_speech_stopped", payload={}),
        ]
    )
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
    provider = FakeRealtimeProvider(
        [
            ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}),
            ProviderEvent(type="user_speech_started", payload={}),
            ProviderEvent(type="user_speech_stopped", payload={}),
            ProviderEvent(
                type="model_audio_delta",
                payload={"delta": base64.b64encode(output_pcm).decode("ascii")},
            ),
        ]
    )
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
async def test_realtime_agent_runner_splits_large_model_audio_delta_before_publishing() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    output_pcm = b"\x01\x02" * 7680
    provider = FakeRealtimeProvider(
        [
            ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}),
            ProviderEvent(type="user_speech_started", payload={}),
            ProviderEvent(type="user_speech_stopped", payload={}),
            ProviderEvent(
                type="model_audio_delta",
                payload={"delta": base64.b64encode(output_pcm).decode("ascii")},
            ),
        ]
    )
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
    provider = FakeRealtimeProvider(
        [
            ProviderEvent(
                type="model_audio_delta",
                payload={"delta": base64.b64encode(output_pcm).decode("ascii")},
            ),
            ProviderEvent(type="model_response_done", payload={}),
        ]
    )
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
    provider = FakeRealtimeProvider(
        [
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
        ]
    )
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
    provider = FakeRealtimeProvider(
        [
            ProviderEvent(
                type="model_audio_delta",
                payload={
                    "type": "response.audio.delta",
                    "delta": raw_delta,
                },
            ),
        ]
    )
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
            "opening_enabled": True,
            "opening_message": "您好，我是凌辰智能助手，请问现在方便简单沟通一下吗？",
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
        "请主动说出开场白：您好，我是凌辰智能助手，请问现在方便简单沟通一下吗？"
    ]
    assert "您好，我是凌辰智能助手" in provider.session_updates[0].instructions


@pytest.mark.anyio
async def test_realtime_agent_runner_confirms_interrupt_when_user_speaks_during_ai_audio() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    output_pcm = b"\x01\x02" * 240
    provider = FakeRealtimeProvider(
        [
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
        ]
    )
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
    )

    await runner.start(session)
    await runner.wait("call_interrupt")

    assert provider.cancelled_response_count == 1
    assert provider.cleared_input_count == 0
    assert provider.created_responses == [None]
    assert publisher.stopped_call_ids == ["call_interrupt"]
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
    provider = FakeRealtimeProvider(
        [
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
        ]
    )
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
    )

    await runner.start(session)
    await runner.wait("call_playout_interrupt")

    assert provider.cancelled_response_count == 1
    assert provider.cleared_input_count == 0
    assert provider.created_responses == [None]
    assert publisher.stopped_call_ids == ["call_playout_interrupt"]
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
    provider = FakeRealtimeProvider(
        [
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
        ]
    )
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
    assert publisher.stopped_call_ids == ["call_tail_interrupt"]
    assert registry.get("call_tail_interrupt").status == CallSessionStatus.AI_THINKING
    event_types = [event.type for event in store.list("call_tail_interrupt")]
    assert "interrupt_candidate" in event_types
    assert "interrupt_confirmed" in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_continues_ai_audio_for_noise_without_transcript() -> None:
    registry = InMemorySessionRegistry()
    store = InMemoryEventStore()
    output_pcm = b"\x01\x02" * 240
    provider = FakeRealtimeProvider(
        [
            ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}),
            ProviderEvent(
                type="model_audio_delta",
                payload={"delta": base64.b64encode(output_pcm).decode("ascii")},
            ),
            ProviderEvent(type="user_speech_started", payload={}),
            ProviderEvent(type="user_speech_stopped", payload={}),
            ProviderEvent(
                type="model_audio_delta",
                payload={"delta": base64.b64encode(output_pcm).decode("ascii")},
            ),
            ProviderEvent(type="model_response_done", payload={}),
        ]
    )
    publisher = FakeAudioPublisher()
    session = CallSession(
        call_id="call_noise",
        room_name="ai-call-call_noise",
        participant_identity="browser-call_noise",
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
    await runner.wait("call_noise")

    assert provider.cancelled_response_count == 0
    assert provider.created_responses == []
    assert publisher.stopped_call_ids == []
    assert len(publisher.published) == 2
    assert registry.get("call_noise").status == CallSessionStatus.CONNECTED
    event_types = [event.type for event in store.list("call_noise")]
    assert "interrupt_candidate" in event_types
    assert "interrupt_ignored" in event_types
    assert "interrupt_confirmed" not in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_does_not_pause_opening_for_noise_without_transcript() -> None:
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
            "opening_enabled": True,
            "opening_message": "您好，我是凌辰智能助手，请问现在方便简单沟通一下吗？",
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
    await provider.emit(ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}))
    await runner.start_opening("call_opening_noise")
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={"delta": base64.b64encode(output_pcm).decode("ascii")},
        )
    )
    await provider.emit(ProviderEvent(type="user_speech_started", payload={}))
    await provider.emit(ProviderEvent(type="user_speech_stopped", payload={}))
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={"delta": base64.b64encode(output_pcm).decode("ascii")},
        )
    )
    await provider.emit(ProviderEvent(type="model_response_done", payload={}))
    await provider.close_events()
    await runner.wait("call_opening_noise")

    assert provider.cancelled_response_count == 0
    assert provider.created_responses == [
        "请主动说出开场白：您好，我是凌辰智能助手，请问现在方便简单沟通一下吗？"
    ]
    assert publisher.stopped_call_ids == []
    assert len(publisher.published) == 2
    event_types = [event.type for event in store.list("call_opening_noise")]
    assert "interrupt_candidate" in event_types
    assert "interrupt_ignored" in event_types
    assert "interrupt_confirmed" not in event_types


@pytest.mark.anyio
async def test_realtime_agent_runner_records_browser_interrupt_candidate_while_ai_speaking() -> None:
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
    confirmed = await runner.record_browser_speech_candidate(
        "call_browser_interrupt",
        datetime(2026, 6, 15, 9, 0, tzinfo=timezone.utc),
    )

    assert confirmed is False
    assert provider.cancelled_response_count == 0
    assert provider.cleared_input_count == 0
    assert publisher.stopped_call_ids == []
    assert registry.get("call_browser_interrupt").status == CallSessionStatus.AI_SPEAKING
    candidate_event = [
        event for event in store.list("call_browser_interrupt") if event.type == "interrupt_candidate"
    ][-1]
    assert candidate_event.payload == {
        "source": "browser",
        "reason": "browser_user_speech_started_during_ai_audio"
    }


@pytest.mark.anyio
async def test_realtime_agent_runner_records_browser_candidate_after_recent_ai_audio() -> None:
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
    await provider.emit(ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}))
    await provider.emit(
        ProviderEvent(
            type="model_audio_delta",
            payload={"delta": base64.b64encode(output_pcm).decode("ascii")},
        )
    )
    await asyncio.wait_for(_wait_until(lambda: len(publisher.published) == 1), timeout=1)
    registry.transition("call_recent_audio_interrupt", CallSessionStatus.CONNECTED)

    confirmed = await runner.record_browser_speech_candidate(
        "call_recent_audio_interrupt",
        datetime.now(timezone.utc),
    )
    await provider.close_events()
    await runner.wait("call_recent_audio_interrupt")

    assert confirmed is False
    assert provider.cancelled_response_count == 0
    assert publisher.stopped_call_ids == []
    assert registry.get("call_recent_audio_interrupt").status == CallSessionStatus.CONNECTED
    assert "interrupt_candidate" in [
        event.type for event in store.list("call_recent_audio_interrupt")
    ]


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
    assert publisher.stopped_call_ids == ["call_stop_audio_failure_interrupt"]
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
    await provider.emit(ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}))
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
    assert "interrupt_confirmed" in [
        event.type for event in store.list("call_stale_audio")
    ]
    assert [event.type for event in store.list("call_stale_audio")].count(
        "ai_audio_published"
    ) == 1


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
    await provider.emit(ProviderEvent(type="model_session_started", payload={"sessionId": "sess_1"}))
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
    assert publisher.stopped_call_ids == ["call_inflight_audio"]
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
    transport = FakeRoomAudioTransport(
        [
            PcmAudioFrame(
                data=input_pcm,
                sample_rate_hz=48000,
                channels=1,
                sample_width_bytes=2,
            )
        ]
    )
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


def test_session_api_returns_unified_camel_case_response() -> None:
    orchestrator, _livekit, _agent = build_orchestrator()
    app = FastAPI()
    app.include_router(AiCallRouter)
    app.dependency_overrides[get_ai_call_service] = lambda: AiCallService(orchestrator)

    with TestClient(app) as client:
        create_response = client.post("/ai-call/sessions", json={"voice": "Cindy"})
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


def read_phase_a_web_asset(name: str) -> str:
    return (Path(__file__).parents[1] / "static/ai-call" / name).read_text(
        encoding="utf-8"
    )


def test_phase_a_web_probe_page_wires_core_session_endpoints() -> None:
    html = read_phase_a_web_asset("phase-a.html")
    script = read_phase_a_web_asset("phase-a.js")

    assert 'id="create-session"' in html
    assert 'rel="icon"' in html
    assert (
        'href="/static/ai-call/phase-a.css"' in html
        or 'href="./phase-a.css"' in html
    )
    assert (
        'src="/static/ai-call/phase-a.js"' in html
        or 'src="./phase-a.js"' in html
    )
    assert 'id="end-session"' in html
    assert 'id="metric-model-stats"' in html
    assert 'id="metric-browser-stats"' in html
    assert "/ai-call/sessions" in script
    assert "/browser-events" in script
    assert "browser_ready" in script
    assert "browser_first_audio" in script
    assert "browser_user_speech_started" in script
    assert "LivekitClient" in script


def test_phase_a_web_probe_requests_microphone_before_joining_room() -> None:
    script = read_phase_a_web_asset("phase-a.js")

    local_track_index = script.index("audioTrack = await createLocalAudioTrack")
    room_connect_index = script.index("await room.connect")
    catch_index = script.index("} catch (error) {")
    disconnect_index = script.index("room.disconnect();", catch_index)

    assert local_track_index < room_connect_index
    assert catch_index < disconnect_index


def test_phase_a_web_probe_reports_local_speech_for_fast_barge_in() -> None:
    script = read_phase_a_web_asset("phase-a.js")

    assert "startLocalSpeechMonitor(audioTrack)" in script
    assert "stopLocalSpeechMonitor()" in script
    assert "createMediaStreamSource" in script
    assert "getByteTimeDomainData" in script
    assert 'type: "browser_user_speech_started"' in script


def test_phase_a_web_probe_reports_browser_first_audio_from_remote_audio_level() -> None:
    script = read_phase_a_web_asset("phase-a.js")

    assert "startRemoteAudioMonitor(track)" in script
    assert "REMOTE_AUDIO_START_RMS = 0.015" in script
    assert "pendingBrowserFirstAudioTurnId" in script
    assert 'event.type === "opening_started"' in script
    assert 'event.type === "user_speech_stopped"' in script
    assert 'type: "browser_first_audio"' in script
    assert "media.onplaying" not in script


def test_phase_a_web_probe_fetches_events_incrementally() -> None:
    script = read_phase_a_web_asset("phase-a.js")

    assert "EVENT_RENDER_LIMIT = 300" in script
    assert "state.lastEventId = event.eventId" in script
    assert 'params.set("afterEventId", state.lastEventId)' in script
    assert "appendEvents(data.rows)" in script


def test_phase_a_web_probe_stops_polling_on_terminal_status() -> None:
    script = read_phase_a_web_asset("phase-a.js")

    assert 'return status === "completed" || status === "failed"' in script
    assert "stopPolling()" in script
    assert "stopClientAudioRuntime()" in script
    assert "disableSessionControls()" in script


def test_phase_a_web_probe_debounces_local_speech_before_reporting_barge_in() -> None:
    script = read_phase_a_web_asset("phase-a.js")

    assert "BROWSER_SPEECH_START_RMS = 0.055" in script
    assert "BROWSER_SPEECH_START_TICKS = 4" in script
    assert "BROWSER_SPEECH_RELEASE_TICKS = 8" in script
    assert "state.browserSpeechHotTicks >= BROWSER_SPEECH_START_TICKS" in script
    assert "state.browserSpeechQuietTicks >= BROWSER_SPEECH_RELEASE_TICKS" in script


@pytest.mark.anyio
async def test_standalone_lifespan_skips_system_service_startup(monkeypatch) -> None:
    app = FastAPI()
    monkeypatch.setattr(init_app.settings, "AI_CALL_STANDALONE_ENABLE", True, raising=False)

    async def fail_import_modules_async(*args, **kwargs):
        raise AssertionError("standalone mode must not import system startup modules")

    monkeypatch.setattr(init_app, "import_modules_async", fail_import_modules_async)

    async with init_app.lifespan(app):
        assert not hasattr(app.state, "redis")


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
