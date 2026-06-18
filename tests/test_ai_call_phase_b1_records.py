from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import jwt
import pytest
from sqlalchemy import inspect as sa_inspect
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import (
    AiCallAsrJobModel,
    AiCallDialogueSegmentModel,
    AiCallEventModel,
    AiCallHandoffModel,
    AiCallRecordingModel,
    AiCallRecordingTrackModel,
    AiCallRecordModel,
    AiCallVoiceProfileModel,
)
from app.api.v1.ai_call.service import AiCallService, configure_ai_call_offline_asr
from app.api.v1.system.oss.service import OssService
from app.core.base_model import MappedBase
from app.core.exceptions import CustomException
from app.services.ai_call.dialogue_service import (
    AiCallDialoguePersistenceWorker,
    AiCallDialogueRuntimeStore,
    AiCallDialogueService,
    DialogueSegmentSnapshot,
)
from app.services.ai_call.event_persistence import AiCallEventPersistenceWorker
from app.services.ai_call.event_store import InMemoryEventStore
from app.services.ai_call.handoff_exception_manager import AiCallHandoffExceptionManager
from app.services.ai_call.handoff_service import AiCallHandoffService
from app.services.ai_call.handoff_trigger_service import (
    AiCallHandoffTriggerService,
    AiCallHandoffTriggerWorker,
    HandoffIntentResult,
)
from app.services.ai_call.livekit_egress import (
    LiveKitEgressManager,
    LiveKitEgressRequestTimeout,
    LiveKitEgressStartResult,
    LiveKitEgressStopResult,
)
from app.services.ai_call.livekit_room import BrowserRoomToken
from app.services.ai_call.offline_asr_service import (
    AiCallOfflineAsrService,
    OfflineAsrResult,
    OfflineAsrSegment,
)
from app.services.ai_call.orchestrator import AiCallOrchestrator, AiCallRuntimeConfig
from app.services.ai_call.record_service import AiCallRecordService
from app.services.ai_call.recording_service import (
    AiCallRecordingReconcileWorker,
    AiCallRecordingService,
)
from app.services.ai_call.session_registry import (
    CallSession,
    CallSessionStatus,
    InMemorySessionRegistry,
)
from app.utils.minio_util import MinioUtil


class FakeLiveKitRoomManager:
    def __init__(self) -> None:
        self.created_rooms: list[str] = []
        self.deleted_rooms: list[str] = []
        self.issued_handoff_tokens: list[tuple[str, str, int | None]] = []

    async def create_room(self, room_name: str) -> None:
        self.created_rooms.append(room_name)

    def issue_browser_token(self, room_name: str, participant_identity: str) -> BrowserRoomToken:
        return BrowserRoomToken(
            livekit_url="wss://livekit.test",
            participant_token=f"browser-token-for-{participant_identity}",
            participant_identity=participant_identity,
            expires_in_seconds=600,
        )

    def issue_handoff_token(
        self,
        room_name: str,
        participant_identity: str,
        expires_in_seconds: int | None = None,
    ) -> BrowserRoomToken:
        self.issued_handoff_tokens.append((room_name, participant_identity, expires_in_seconds))
        return BrowserRoomToken(
            livekit_url="wss://livekit.test",
            participant_token=f"handoff-token-for-{participant_identity}",
            participant_identity=participant_identity,
            expires_in_seconds=expires_in_seconds or 600,
        )

    async def delete_room(self, room_name: str) -> None:
        self.deleted_rooms.append(room_name)


class FakeAgentRunner:
    def __init__(self) -> None:
        self.suspended_call_ids: list[str] = []

    async def start(self, session: CallSession) -> None:
        _ = session

    async def start_opening(self, call_id: str) -> None:
        _ = call_id

    async def record_browser_speech_candidate(self, call_id: str, trigger_timestamp) -> bool:
        _ = call_id, trigger_timestamp
        return False

    async def stop(self, call_id: str) -> None:
        _ = call_id

    async def suspend_for_handoff(self, call_id: str) -> None:
        self.suspended_call_ids.append(call_id)


class FakeSystemPromptPlayer:
    def __init__(self) -> None:
        self.played: list[tuple[str, str, str]] = []

    async def play(self, *, call_id: str, room_name: str, audio_path) -> None:
        self.played.append((call_id, room_name, str(audio_path)))


class BlockingSystemPromptPlayer(FakeSystemPromptPlayer):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def play(self, *, call_id: str, room_name: str, audio_path) -> None:
        self.played.append((call_id, room_name, str(audio_path)))
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


class FakeHandoffIntentClassifier:
    def __init__(self, result: HandoffIntentResult) -> None:
        self.result = result
        self.transcripts: list[str] = []

    async def classify(self, *, transcript: str) -> HandoffIntentResult:
        self.transcripts.append(transcript)
        return self.result


class FailingAgentRunner(FakeAgentRunner):
    async def start(self, session: CallSession) -> None:
        await super().start(session)
        raise RuntimeError("agent boom")


class FakeEgressManager:
    def __init__(
        self,
        room_manager: FakeLiveKitRoomManager | None = None,
        *,
        file_size: int | None = 2048,
    ) -> None:
        self.room_manager = room_manager
        self.file_size = file_size
        self.started: list[tuple[str, str]] = []
        self.started_participants: list[tuple[str, str, str, str]] = []
        self.stopped: list[str] = []
        self.stop_saw_deleted_room: bool | None = None

    def build_object_name(self, call_id: str) -> str:
        return f"ai-call/recordings/{call_id}.mp3"

    def build_participant_object_name(
        self,
        *,
        call_id: str,
        track_role: str,
        participant_identity: str,
    ) -> str:
        return f"ai-call/recordings/tracks/{call_id}/{track_role}-{participant_identity}.mp3"

    async def start_room_audio_recording(
        self,
        *,
        room_name: str,
        call_id: str,
        oss_config: dict,
    ) -> LiveKitEgressStartResult:
        assert oss_config["bucket_name"] == "recordings"
        self.started.append((room_name, call_id))
        return LiveKitEgressStartResult(
            egress_id=f"EG_{call_id}",
            object_name=self.build_object_name(call_id),
            status="EGRESS_ACTIVE",
        )

    async def start_participant_audio_recording(
        self,
        *,
        room_name: str,
        call_id: str,
        track_role: str,
        participant_identity: str,
        oss_config: dict,
    ) -> LiveKitEgressStartResult:
        assert oss_config["bucket_name"] == "recordings"
        self.started_participants.append((room_name, call_id, track_role, participant_identity))
        return LiveKitEgressStartResult(
            egress_id=f"EG_{call_id}_{track_role}_{participant_identity}",
            object_name=self.build_participant_object_name(
                call_id=call_id,
                track_role=track_role,
                participant_identity=participant_identity,
            ),
            status="EGRESS_ACTIVE",
        )

    async def stop_egress(self, egress_id: str) -> LiveKitEgressStopResult:
        self.stopped.append(egress_id)
        raw = egress_id.removeprefix("EG_")
        call_id = self._call_id_from_egress_id(egress_id, raw)
        if self.room_manager is not None:
            room_name = f"ai-call-{call_id}"
            self.stop_saw_deleted_room = room_name in self.room_manager.deleted_rooms
        object_name = self._object_name_from_egress_id(egress_id, call_id)
        return LiveKitEgressStopResult(
            egress_id=egress_id,
            status="EGRESS_COMPLETE",
            object_name=object_name,
            duration_ms=1200,
            file_size=self.file_size,
            location=f"s3://recordings/{object_name}",
        )

    def _call_id_from_egress_id(self, egress_id: str, fallback: str) -> str:
        for (
            _room_name,
            started_call_id,
            track_role,
            participant_identity,
        ) in self.started_participants:
            if egress_id == f"EG_{started_call_id}_{track_role}_{participant_identity}":
                return started_call_id
        return fallback

    def _object_name_from_egress_id(self, egress_id: str, call_id: str) -> str:
        for (
            _room_name,
            started_call_id,
            track_role,
            participant_identity,
        ) in self.started_participants:
            if egress_id == f"EG_{started_call_id}_{track_role}_{participant_identity}":
                return self.build_participant_object_name(
                    call_id=started_call_id,
                    track_role=track_role,
                    participant_identity=participant_identity,
                )
        return self.build_object_name(call_id)


class FlakyParticipantEgressManager(FakeEgressManager):
    def __init__(self, *, failures_before_success: int = 1) -> None:
        super().__init__()
        self.failures_before_success = failures_before_success
        self.participant_start_attempts: dict[tuple[str, str], int] = {}

    async def start_participant_audio_recording(
        self,
        *,
        room_name: str,
        call_id: str,
        track_role: str,
        participant_identity: str,
        oss_config: dict,
    ) -> LiveKitEgressStartResult:
        key = (track_role, participant_identity)
        attempts = self.participant_start_attempts.get(key, 0) + 1
        self.participant_start_attempts[key] = attempts
        if attempts <= self.failures_before_success:
            raise RuntimeError("participant not ready")
        return await super().start_participant_audio_recording(
            room_name=room_name,
            call_id=call_id,
            track_role=track_role,
            participant_identity=participant_identity,
            oss_config=oss_config,
        )


class FailedStopEgressManager(FakeEgressManager):
    async def stop_egress(self, egress_id: str) -> LiveKitEgressStopResult:
        self.stopped.append(egress_id)
        call_id = egress_id.removeprefix("EG_")
        if self.room_manager is not None:
            room_name = f"ai-call-{call_id}"
            self.stop_saw_deleted_room = room_name in self.room_manager.deleted_rooms
        return LiveKitEgressStopResult(
            egress_id=egress_id,
            status="EGRESS_FAILED",
            object_name=self.build_object_name(call_id),
            error="egress failed",
        )


class TimeoutMainStopEgressManager(FakeEgressManager):
    async def stop_egress(self, egress_id: str) -> LiveKitEgressStopResult:
        self.stopped.append(egress_id)
        raise LiveKitEgressRequestTimeout(method="StopEgress", timeout_seconds=2.0)


class BrokenMainStopEgressManager(FakeEgressManager):
    async def stop_egress(self, egress_id: str) -> LiveKitEgressStopResult:
        self.stopped.append(egress_id)
        raise RuntimeError("stop failed before result was accepted")


class AlreadyCompletedMainStopEgressManager(FakeEgressManager):
    async def stop_egress(self, egress_id: str) -> LiveKitEgressStopResult:
        self.stopped.append(egress_id)
        raise RuntimeError(
            'StopEgress HTTP 412: {"code":"failed_precondition",'
            '"msg":"egress with status EGRESS_COMPLETE cannot be stopped"}'
        )


class TimeoutAiTrackStopEgressManager(FakeEgressManager):
    async def stop_egress(self, egress_id: str) -> LiveKitEgressStopResult:
        if "_ai_" in egress_id:
            self.stopped.append(egress_id)
            raise LiveKitEgressRequestTimeout(method="StopEgress", timeout_seconds=2.0)
        return await super().stop_egress(egress_id)


class AlreadyCompletedAiTrackStopEgressManager(FakeEgressManager):
    async def stop_egress(self, egress_id: str) -> LiveKitEgressStopResult:
        if "_ai_" in egress_id:
            self.stopped.append(egress_id)
            raise RuntimeError(
                'StopEgress HTTP 412: {"code":"failed_precondition",'
                '"msg":"egress with status EGRESS_COMPLETE cannot be stopped"}'
            )
        return await super().stop_egress(egress_id)


class FakeOfflineAsrProvider:
    provider_name = "fake_asr"
    model_name = "fake-model"

    def __init__(self) -> None:
        self.audio_urls: list[str] = []

    async def transcribe(self, *, audio_url: str) -> OfflineAsrResult:
        self.audio_urls.append(audio_url)
        if "/customer-" in audio_url:
            return OfflineAsrResult(
                task_id="task-customer",
                transcription_url="https://asr.test/customer.json",
                segments=[
                    OfflineAsrSegment(
                        text="客户需要转人工",
                        begin_time_ms=100,
                        end_time_ms=520,
                    )
                ],
            )
        return OfflineAsrResult(
            task_id="task-agent",
            transcription_url="https://asr.test/agent.json",
            segments=[
                OfflineAsrSegment(
                    text="您好，我帮您接入人工",
                    begin_time_ms=600,
                    end_time_ms=1200,
                )
            ],
        )


class FakeOfflineAsrWorker:
    def __init__(self) -> None:
        self.call_ids: list[str] = []

    def enqueue(self, call_id: str) -> None:
        self.call_ids.append(call_id)


@dataclass(slots=True)
class B1ServiceContext:
    service: AiCallService
    record_service: AiCallRecordService
    event_worker: AiCallEventPersistenceWorker
    agent_runner: FakeAgentRunner
    room_manager: FakeLiveKitRoomManager
    session_maker: async_sessionmaker

    def __iter__(self):
        yield self.service
        yield self.record_service

    async def flush_events(self) -> None:
        await self.event_worker.flush_pending()


def build_b1_orchestrator(
    agent_runner: FakeAgentRunner | None = None,
    livekit_room_manager: FakeLiveKitRoomManager | None = None,
) -> AiCallOrchestrator:
    return AiCallOrchestrator(
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
        livekit_room_manager=livekit_room_manager or FakeLiveKitRoomManager(),
        agent_runner=agent_runner or FakeAgentRunner(),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )


async def wait_until(predicate, *, attempts: int = 40, delay_seconds: float = 0.05) -> None:
    for _ in range(attempts):
        if predicate():
            return
        await asyncio.sleep(delay_seconds)
    raise AssertionError("condition was not met in time")


@pytest.fixture
async def b1_service():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        agent_runner = FakeAgentRunner()
        room_manager = FakeLiveKitRoomManager()
        orchestrator = build_b1_orchestrator(
            agent_runner=agent_runner,
            livekit_room_manager=room_manager,
        )
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        event_worker = AiCallEventPersistenceWorker(
            session_maker,
            flush_interval_seconds=0.01,
        )
        await event_worker.start()
        event_worker.attach_event_store(orchestrator.event_store)
        try:
            yield B1ServiceContext(
                service=AiCallService(
                    orchestrator,
                    record_service,
                    handoff_service=AiCallHandoffService(repository),
                ),
                record_service=record_service,
                event_worker=event_worker,
                agent_runner=agent_runner,
                room_manager=room_manager,
                session_maker=session_maker,
            )
        finally:
            event_worker.detach_all()
            await event_worker.stop()

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


def test_phase_b1_models_do_not_use_physical_foreign_keys_or_relationships() -> None:
    assert not AiCallRecordModel.__table__.foreign_keys
    assert not AiCallEventModel.__table__.foreign_keys
    assert not AiCallRecordingModel.__table__.foreign_keys
    assert not AiCallRecordingTrackModel.__table__.foreign_keys
    assert not AiCallDialogueSegmentModel.__table__.foreign_keys
    assert not AiCallAsrJobModel.__table__.foreign_keys
    assert not AiCallHandoffModel.__table__.foreign_keys
    assert not AiCallVoiceProfileModel.__table__.foreign_keys
    assert not sa_inspect(AiCallRecordModel).relationships
    assert not sa_inspect(AiCallEventModel).relationships
    assert not sa_inspect(AiCallRecordingModel).relationships
    assert not sa_inspect(AiCallRecordingTrackModel).relationships
    assert not sa_inspect(AiCallDialogueSegmentModel).relationships
    assert not sa_inspect(AiCallAsrJobModel).relationships
    assert not sa_inspect(AiCallHandoffModel).relationships
    assert not sa_inspect(AiCallVoiceProfileModel).relationships


def test_livekit_egress_uses_ogg_participant_format_when_mp3_is_requested() -> None:
    manager = LiveKitEgressManager(
        livekit_url="ws://livekit.test",
        api_key="key",
        api_secret="secret",
        timeout_seconds=1,
        object_prefix="ai-call/recordings",
        file_type="MP3",
        participant_file_type="MP3",
    )

    assert manager.build_object_name("call_format") == "ai-call/recordings/call_format.mp3"
    assert (
        manager.build_participant_object_name(
            call_id="call_format",
            track_role="human_agent",
            participant_identity="human-agent-001",
        )
        == "ai-call/recordings/tracks/call_format/human_agent-human-agent-001.ogg"
    )


def test_livekit_egress_uses_separate_stop_timeout() -> None:
    manager = LiveKitEgressManager(
        livekit_url="ws://livekit.test",
        api_key="key",
        api_secret="secret",
        timeout_seconds=2,
        stop_timeout_seconds=10,
        object_prefix="ai-call/recordings",
    )

    assert manager.timeout_seconds == 2
    assert manager.stop_timeout_seconds == 10


def test_livekit_egress_room_admin_token_scopes_to_room_for_participant_lookup() -> None:
    manager = LiveKitEgressManager(
        livekit_url="ws://livekit.test",
        api_key="key",
        api_secret="secret",
        timeout_seconds=1,
        object_prefix="ai-call/recordings",
    )

    token = manager._issue_room_admin_token(room_name="room_format")
    payload = jwt.decode(token, "secret", algorithms=["HS256"])

    assert payload["video"]["roomAdmin"] is True
    assert payload["video"]["room"] == "room_format"


@pytest.mark.anyio
async def test_livekit_egress_uses_track_egress_for_participant_audio() -> None:
    class CapturingEgressManager(LiveKitEgressManager):
        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)
            self.calls: list[tuple[str, dict]] = []
            self.room_calls: list[tuple[str, dict]] = []

        async def _post_egress(
            self,
            method: str,
            payload: dict,
            *,
            timeout_seconds: float | None = None,
        ) -> dict:
            _ = timeout_seconds
            self.calls.append((method, payload))
            return {"egress_id": f"EG_{method}", "status": "EGRESS_ACTIVE"}

        async def _post_room_service(self, method: str, payload: dict) -> dict:
            self.room_calls.append((method, payload))
            return {
                "identity": payload["identity"],
                "tracks": [
                    {"sid": "TR_video", "type": "VIDEO", "source": "CAMERA"},
                    {"sid": "TR_audio", "type": "AUDIO", "source": "MICROPHONE"},
                ],
            }

    manager = CapturingEgressManager(
        livekit_url="ws://livekit.test",
        api_key="key",
        api_secret="secret",
        timeout_seconds=1,
        object_prefix="ai-call/recordings",
        file_type="MP4",
        participant_file_type="OGG",
    )

    await manager.start_room_audio_recording(
        room_name="room_format",
        call_id="call_format",
        oss_config={},
    )
    await manager.start_participant_audio_recording(
        room_name="room_format",
        call_id="call_format",
        track_role="customer",
        participant_identity="browser-call_format",
        oss_config={},
    )

    assert manager.calls[0][0] == "StartRoomCompositeEgress"
    assert manager.calls[0][1]["file_outputs"][0]["file_type"] == "MP4"
    assert manager.calls[0][1]["file_outputs"][0]["filepath"].endswith(".mp4")
    assert manager.room_calls == [
        (
            "GetParticipant",
            {"room": "room_format", "identity": "browser-call_format"},
        )
    ]
    assert manager.calls[1][0] == "StartTrackEgress"
    assert manager.calls[1][1]["track_id"] == "TR_audio"
    assert manager.calls[1][1]["file"]["filepath"].endswith(".ogg")
    assert "file_type" not in manager.calls[1][1]["file"]


def test_livekit_egress_selects_first_audio_track_when_source_is_numeric() -> None:
    assert LiveKitEgressManager._select_audio_track([
        {"sid": "TR_video", "type": 1, "source": 1},
        {"sid": "TR_audio", "type": 0, "source": 2},
    ]) == {"sid": "TR_audio", "type": 0, "source": 2}


@pytest.mark.anyio
async def test_create_web_session_persists_record_and_key_events(b1_service) -> None:
    service, record_service = b1_service

    result = await service.create_web_session(
        voice="Cindy",
        prompt=None,
        business_id="324800000000000001",
    )

    record = await record_service.get_record(result.call_id)
    assert record is not None
    assert record.id is not None
    assert record.call_id == result.call_id
    assert record.business_type is None
    assert record.business_id == "324800000000000001"
    assert record.entry_type == "web"
    assert record.room_name == result.room_name
    assert record.participant_identity == result.participant_identity
    assert record.status == CallSessionStatus.READY.value

    await b1_service.flush_events()
    events = await record_service.list_events(result.call_id)
    assert [event.event_type for event in events] == [
        "session_created",
        "session_preparing",
        "room_created",
        "browser_token_issued",
        "agent_started",
        "session_ready",
    ]
    assert all(event.call_id == result.call_id for event in events)


@pytest.mark.anyio
async def test_end_session_updates_record_terminal_state_and_reason(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )

    await service.report_browser_event(
        call_id=result.call_id,
        event_type="browser_ready",
        timestamp=None,
    )
    await service.end_session(result.call_id)

    record = await record_service.get_record(result.call_id)
    assert record is not None
    assert record.status == CallSessionStatus.COMPLETED.value
    assert record.end_reason == "web_user_end"
    assert record.answered_at is not None
    assert record.ended_at is not None
    assert record.duration_ms is not None
    assert record.duration_ms >= 0

    await b1_service.flush_events()
    events = await record_service.list_events(result.call_id)
    assert [event.event_type for event in events].count("session_completed") == 1
    terminal_event = events[-1]
    assert terminal_event.event_type == "session_completed"
    assert terminal_event.payload == {"endReason": "web_user_end"}


@pytest.mark.anyio
async def test_browser_disconnect_completes_record_with_disconnect_reason(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    await service.report_browser_event(
        call_id=result.call_id,
        event_type="browser_ready",
        timestamp=None,
    )

    browser_event = await service.report_browser_event(
        call_id=result.call_id,
        event_type="browser_disconnect",
        timestamp=None,
    )

    assert browser_event.type == "browser_disconnect"
    record = await record_service.get_record(result.call_id)
    assert record is not None
    assert record.status == CallSessionStatus.COMPLETED.value
    assert record.end_reason == "browser_disconnect"
    assert record.ended_at is not None
    assert record.duration_ms is not None

    await b1_service.flush_events()
    events = await record_service.list_events(result.call_id)
    assert [event.event_type for event in events][-3:] == [
        "browser_disconnect",
        "session_ending",
        "session_completed",
    ]
    assert events[-1].payload == {"endReason": "browser_disconnect"}


@pytest.mark.anyio
async def test_agent_start_failure_persists_failed_record_and_terminal_payload() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        orchestrator = build_b1_orchestrator(agent_runner=FailingAgentRunner())
        event_worker = AiCallEventPersistenceWorker(
            session_maker,
            flush_interval_seconds=0.01,
        )
        await event_worker.start()
        event_worker.attach_event_store(orchestrator.event_store)
        record_service = AiCallRecordService(AiCallRecordRepository(db))
        service = AiCallService(
            orchestrator,
            record_service,
        )
        try:
            with pytest.raises(CustomException) as exc_info:
                await service.create_web_session(
                    voice=None,
                    prompt=None,
                    business_id=None,
                )
            assert exc_info.value.msg == "Agent 启动失败"

            await event_worker.flush_pending()
            rows, total = await record_service.list_records()
            assert total == 1
            record = rows[0]
            assert record.status == CallSessionStatus.FAILED.value
            assert record.end_reason == "agent_start_failed"
            assert record.failure_stage == "agent_start"
            assert record.failure_message == "Agent 启动失败"

            events = await record_service.list_events(record.call_id)
            assert events[-1].event_type == "session_failed"
            assert events[-1].payload == {
                "endReason": "agent_start_failed",
                "failureMessage": "Agent 启动失败",
                "failureStage": "agent_start",
            }
        finally:
            event_worker.detach_all()
            await event_worker.stop()

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_record_query_outputs_bigint_ids_as_strings(b1_service) -> None:
    service, _record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id="324800000000000002",
    )

    await b1_service.flush_events()
    detail = await service.get_record_detail(result.call_id)
    assert detail["record"]["id"].isdigit()
    assert isinstance(detail["record"]["id"], str)
    assert detail["record"]["businessId"] == "324800000000000002"
    assert detail["lastEvent"]["id"].isdigit()
    assert isinstance(detail["lastEvent"]["id"], str)

    events = await service.list_record_events(result.call_id)
    assert events["total"] == 6
    assert isinstance(events["rows"][0]["id"], str)
    assert events["rows"][0]["eventType"] == "session_created"


@pytest.mark.anyio
async def test_create_handoff_persists_record_and_suspends_agent(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )

    handoff = await service.create_handoff(
        call_id=result.call_id,
        source="operator",
        reason="customer_request",
        request_message="验证页手工发起转人工",
    )

    assert handoff["id"].isdigit()
    assert isinstance(handoff["id"], str)
    assert handoff["handoffId"].startswith("handoff_")
    assert handoff["callId"] == result.call_id
    assert handoff["roomName"] == result.room_name
    assert handoff["status"] == "requested"
    assert handoff["requestSource"] == "operator"
    assert b1_service.agent_runner.suspended_call_ids == [result.call_id]
    assert service.orchestrator.registry.get(result.call_id).status == CallSessionStatus.WAITING

    await b1_service.flush_events()
    events = await record_service.list_events(result.call_id)
    event_types = [event.event_type for event in events]
    assert "handoff_requested" in event_types
    assert "agent_suspended_for_handoff" in event_types


@pytest.mark.anyio
async def test_duplicate_handoff_request_returns_active_one(b1_service) -> None:
    service, _record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )

    first = await service.create_handoff(
        call_id=result.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )
    second = await service.create_handoff(
        call_id=result.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )

    assert second["handoffId"] == first["handoffId"]
    assert b1_service.agent_runner.suspended_call_ids == [result.call_id]


@pytest.mark.anyio
async def test_handoff_trigger_worker_creates_customer_handoff_from_tool_request(
    b1_service,
) -> None:
    service, record_service = b1_service
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=False,
            confidence=0.0,
            reason="not_handoff",
            summary="不应进入分类器",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        threshold=0.8,
        timeout_seconds=0.2,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="handoff_tool_requested",
            source="agent",
            payload={
                "toolCallId": "handoff_tool_1",
                "reason": "business_escalation",
            },
        )

        await worker.flush_pending()
        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs["total"] == 1
        assert handoffs["rows"][0]["requestSource"] == "customer"
        assert handoffs["rows"][0]["requestReason"] == "business_escalation"
        assert handoffs["rows"][0]["status"] == "requested"
        assert handoffs["rows"][0]["requestMessage"] == "模型判断当前问题需要人工继续处理"
        assert b1_service.agent_runner.suspended_call_ids == [result.call_id]
        assert classifier.transcripts == []

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_tool_requested" in event_types
        assert "handoff_intent_detected" in event_types
        assert "handoff_auto_triggered" in event_types
        assert "handoff_requested" in event_types
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_handoff_trigger_worker_auto_creates_customer_handoff(b1_service) -> None:
    service, record_service = b1_service
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=True,
            confidence=0.92,
            reason="customer_request",
            summary="用户明确要求转人工",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        threshold=0.8,
        timeout_seconds=0.2,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service, transcript_trigger_enabled=True)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="user_transcript_done",
            source="provider",
            payload={"item_id": "item_handoff", "transcript": "我想找真人客服"},
        )

        await worker.flush_pending()
        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs["total"] == 1
        assert handoffs["rows"][0]["requestSource"] == "customer"
        assert handoffs["rows"][0]["requestReason"] == "customer_request"
        assert handoffs["rows"][0]["status"] == "requested"
        assert b1_service.agent_runner.suspended_call_ids == [result.call_id]
        assert classifier.transcripts == ["我想找真人客服"]

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_intent_detected" in event_types
        assert "handoff_auto_triggered" in event_types
        assert "handoff_requested" in event_types
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_handoff_trigger_worker_ignores_low_confidence_text(b1_service) -> None:
    service, record_service = b1_service
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=False,
            confidence=0.3,
            reason="not_handoff",
            summary="未识别到明确转人工意图",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        threshold=0.8,
        timeout_seconds=0.2,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service, transcript_trigger_enabled=True)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="user_transcript_done",
            source="provider",
            payload={"item_id": "item_no_handoff", "transcript": "人工智能是什么意思"},
        )

        await worker.flush_pending()
        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs == {"rows": [], "total": 0}
        assert b1_service.agent_runner.suspended_call_ids == []

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_intent_ignored" in event_types
        assert "handoff_requested" not in event_types
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_handoff_trigger_worker_does_not_duplicate_active_handoff(b1_service) -> None:
    service, record_service = b1_service
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=True,
            confidence=0.95,
            reason="customer_request",
            summary="用户明确要求转人工",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        threshold=0.8,
        timeout_seconds=0.2,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service, transcript_trigger_enabled=True)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.create_handoff(
            call_id=result.call_id,
            source="operator",
            reason="customer_request",
            request_message="验证页手工发起转人工",
        )
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="user_transcript_done",
            source="provider",
            payload={"item_id": "item_duplicate", "transcript": "帮我转人工"},
        )

        await worker.flush_pending()
        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs["total"] == 1
        assert handoffs["rows"][0]["requestSource"] == "operator"
        assert b1_service.agent_runner.suspended_call_ids == [result.call_id]
        assert classifier.transcripts == []

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_intent_ignored" in event_types
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_joinable_handoffs_only_return_requested_unexpired_rows(b1_service) -> None:
    service, _record_service = b1_service
    first_session = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    first_handoff = await service.create_handoff(
        call_id=first_session.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )

    joinable = await service.list_joinable_handoffs()
    assert joinable["total"] == 1
    assert joinable["rows"][0]["handoffId"] == first_handoff["handoffId"]

    await service.accept_handoff(
        handoff_id=first_handoff["handoffId"],
        human_agent_identity="agent-debug-001",
    )
    joinable = await service.list_joinable_handoffs()
    assert joinable["total"] == 0

    second_session = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    second_handoff = await service.create_handoff(
        call_id=second_session.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )
    await service.handoff_service.repository.update_handoff(
        second_handoff["handoffId"],
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )

    joinable = await service.list_joinable_handoffs()
    assert joinable["total"] == 0


@pytest.mark.anyio
async def test_handoff_accept_connect_and_complete_flow(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    handoff = await service.create_handoff(
        call_id=result.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )

    accepted = await service.accept_handoff(
        handoff_id=handoff["handoffId"],
        human_agent_identity="agent-debug-001",
    )
    assert accepted["handoff"]["status"] == "accepted"
    assert accepted["handoff"]["humanAgentIdentity"] == "agent-debug-001"
    assert accepted["seatToken"]["callId"] == result.call_id
    assert accepted["seatToken"]["roomName"] == result.room_name
    assert accepted["seatToken"]["participantIdentity"].startswith("human-agent-handoff_")
    assert accepted["seatToken"]["participantToken"].startswith("handoff-token-for-")

    connected = await service.mark_handoff_connected(handoff["handoffId"])
    assert connected["status"] == "connected"
    assert connected["connectedAt"] is not None

    completed = await service.complete_handoff(
        handoff_id=handoff["handoffId"],
        reason="agent_completed",
    )
    assert completed["status"] == "completed"
    assert completed["endReason"] == "agent_completed"
    assert completed["endedAt"] is not None

    history = await service.list_handoffs(result.call_id)
    assert history["total"] == 1
    assert history["rows"][0]["status"] == "completed"

    assert service.orchestrator.registry.get(result.call_id).status == CallSessionStatus.COMPLETED
    record_service.repository.db.expire_all()
    record = await record_service.get_record(result.call_id)
    assert record is not None
    assert record.status == CallSessionStatus.COMPLETED.value
    assert record.end_reason == "agent_completed"
    assert b1_service.room_manager.deleted_rooms == [result.room_name]

    await b1_service.flush_events()
    event_types = [event.event_type for event in await record_service.list_events(result.call_id)]
    assert "handoff_accepted" in event_types
    assert "handoff_connected" in event_types
    assert "handoff_completed" in event_types
    assert "session_completed" in event_types
    assert event_types.index("handoff_completed") < event_types.index("session_completed")


@pytest.mark.anyio
async def test_end_session_finalizes_active_handoff(b1_service) -> None:
    service, _record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    handoff = await service.create_handoff(
        call_id=result.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )

    await service.end_session(result.call_id)

    assert await service.get_current_handoff(result.call_id) is None
    history = await service.list_handoffs(result.call_id)
    assert history["total"] == 1
    assert history["rows"][0]["handoffId"] == handoff["handoffId"]
    assert history["rows"][0]["status"] == "canceled"
    assert history["rows"][0]["endReason"] == "web_user_end"


@pytest.mark.anyio
async def test_handoff_lazy_expire_records_expired_event(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    handoff = await service.create_handoff(
        call_id=result.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )
    await service.handoff_service.repository.update_handoff(
        handoff["handoffId"],
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )

    assert await service.get_current_handoff(result.call_id) is None
    history = await service.list_handoffs(result.call_id)
    assert history["rows"][0]["status"] == "expired"

    await b1_service.flush_events()
    event_types = [event.event_type for event in await record_service.list_events(result.call_id)]
    assert "handoff_expired" in event_types


@pytest.mark.anyio
async def test_handoff_timeout_auto_ends_without_unavailable_prompt(
    b1_service,
) -> None:
    service, record_service = b1_service
    prompt_player = FakeSystemPromptPlayer()
    manager = AiCallHandoffExceptionManager(
        orchestrator=service.orchestrator,
        session_factory=b1_service.session_maker,
        recording_service_factory=lambda _repository: None,
        system_prompt_player=prompt_player,
        timeout_seconds=1,
    )
    service.handoff_exception_manager = manager
    service.handoff_service.request_timeout_seconds = 1
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        handoff = await service.create_handoff(
            call_id=result.call_id,
            source="operator",
            reason="customer_request",
            request_message=None,
        )

        await wait_until(
            lambda: (
                service.orchestrator.registry.get(result.call_id).status
                == CallSessionStatus.COMPLETED
            ),
            attempts=50,
            delay_seconds=0.05,
        )
        record = None
        for _ in range(50):
            record_service.repository.db.expire_all()
            record = await record_service.get_record(result.call_id)
            if record is not None and record.status == "completed":
                break
            await asyncio.sleep(0.05)
        history = await service.list_handoffs(result.call_id)

        assert record is not None
        assert record.status == "completed"
        assert record.end_reason == "handoff_timeout"
        assert history["rows"][0]["handoffId"] == handoff["handoffId"]
        assert history["rows"][0]["status"] == "expired"
        assert prompt_player.played == []
        assert b1_service.room_manager.deleted_rooms == [result.room_name]

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_expired" in event_types
        assert "handoff_unavailable_prompt_started" not in event_types
        assert "handoff_unavailable_prompt_done" not in event_types
        assert "handoff_auto_ended" in event_types
    finally:
        await manager.shutdown()


@pytest.mark.anyio
async def test_handoff_connected_cancels_timeout_auto_end(
    b1_service,
) -> None:
    service, _record_service = b1_service
    prompt_player = FakeSystemPromptPlayer()
    manager = AiCallHandoffExceptionManager(
        orchestrator=service.orchestrator,
        session_factory=b1_service.session_maker,
        recording_service_factory=lambda _repository: None,
        system_prompt_player=prompt_player,
        timeout_seconds=1,
    )
    service.handoff_exception_manager = manager
    service.handoff_service.request_timeout_seconds = 1
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        handoff = await service.create_handoff(
            call_id=result.call_id,
            source="operator",
            reason="customer_request",
            request_message=None,
        )
        await service.accept_handoff(
            handoff_id=handoff["handoffId"],
            human_agent_identity="agent-debug-001",
        )
        connected = await service.mark_handoff_connected(handoff["handoffId"])

        await asyncio.sleep(1.2)
        assert connected["status"] == "connected"
        assert (
            service.orchestrator.registry.get(result.call_id).status != CallSessionStatus.COMPLETED
        )
        assert prompt_player.played == []
        assert b1_service.room_manager.deleted_rooms == []
    finally:
        await manager.shutdown()


@pytest.mark.anyio
async def test_handoff_waiting_tone_stops_when_agent_connected(
    b1_service,
    tmp_path,
) -> None:
    service, record_service = b1_service
    prompt_player = BlockingSystemPromptPlayer()
    manager = AiCallHandoffExceptionManager(
        orchestrator=service.orchestrator,
        session_factory=b1_service.session_maker,
        recording_service_factory=lambda _repository: None,
        system_prompt_player=prompt_player,
        timeout_seconds=30,
        waiting_tone_enabled=True,
        waiting_tone_audio_path=tmp_path / "handoff-ringback.wav",
    )
    service.handoff_exception_manager = manager
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        handoff = await service.create_handoff(
            call_id=result.call_id,
            source="operator",
            reason="customer_request",
            request_message=None,
        )

        await wait_until(lambda: prompt_player.started.is_set())
        assert prompt_player.played == [
            (result.call_id, result.room_name, str(tmp_path / "handoff-ringback.wav"))
        ]

        await service.accept_handoff(
            handoff_id=handoff["handoffId"],
            human_agent_identity="agent-debug-001",
        )
        await service.mark_handoff_connected(handoff["handoffId"])
        await wait_until(lambda: prompt_player.cancelled.is_set())

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_waiting_tone_started" in event_types
        assert "handoff_waiting_tone_stopped" in event_types
    finally:
        await manager.shutdown()


@pytest.mark.anyio
async def test_handoff_fail_auto_ends_without_unavailable_prompt(
    b1_service,
) -> None:
    service, record_service = b1_service
    prompt_player = FakeSystemPromptPlayer()
    manager = AiCallHandoffExceptionManager(
        orchestrator=service.orchestrator,
        session_factory=b1_service.session_maker,
        recording_service_factory=lambda _repository: None,
        system_prompt_player=prompt_player,
        timeout_seconds=30,
    )
    service.handoff_exception_manager = manager
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        handoff = await service.create_handoff(
            call_id=result.call_id,
            source="operator",
            reason="customer_request",
            request_message=None,
        )

        failed = await service.fail_handoff(
            handoff_id=handoff["handoffId"],
            failure_stage="agent_join",
            failure_message="坐席加入 Room 失败",
        )
        assert failed["status"] == "failed"

        await wait_until(
            lambda: (
                service.orchestrator.registry.get(result.call_id).status
                == CallSessionStatus.COMPLETED
            ),
            attempts=40,
            delay_seconds=0.05,
        )
        record_service.repository.db.expire_all()
        record = await record_service.get_record(result.call_id)

        assert record is not None
        assert record.status == "completed"
        assert record.end_reason == "handoff_failed"
        assert prompt_player.played == []

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_failed" in event_types
        assert "handoff_unavailable_prompt_started" not in event_types
        assert "handoff_unavailable_prompt_done" not in event_types
        assert "handoff_auto_ended" in event_types
    finally:
        await manager.shutdown()


@pytest.mark.anyio
async def test_append_event_is_idempotent_by_event_id(b1_service) -> None:
    _service, record_service = b1_service
    event_time = datetime.now(timezone.utc)

    first = await record_service.repository.append_event(
        event_id="evt_duplicate",
        call_id="call_duplicate",
        event_type="provider_event",
        source="provider",
        event_time=event_time,
        payload_json='{"sequence": 1}',
    )
    second = await record_service.repository.append_event(
        event_id="evt_duplicate",
        call_id="call_duplicate",
        event_type="provider_event_changed",
        source="provider",
        event_time=event_time,
        payload_json='{"sequence": 2}',
    )

    rows = await record_service.list_events("call_duplicate")
    assert len(rows) == 1
    assert first.id == second.id == rows[0].id
    assert rows[0].event_type == "provider_event"
    assert rows[0].payload == {"sequence": 1}


@pytest.mark.anyio
async def test_mirror_runtime_events_only_persists_new_events(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    await b1_service.flush_events()
    b1_service.event_worker.detach_all()

    runtime_events = service.orchestrator.event_store.list_all(call_id=result.call_id)
    assert await record_service.mirror_runtime_events(runtime_events) == []

    new_event = service.orchestrator.event_store.append(
        call_id=result.call_id,
        type="model_error",
        source="provider",
        payload={"sequence": 1},
    )
    mirrored = await record_service.mirror_runtime_events(
        service.orchestrator.event_store.list_all(call_id=result.call_id)
    )

    assert [event.event_id for event in mirrored] == [new_event.event_id]
    rows = await record_service.list_events(result.call_id)
    assert [event.event_id for event in rows].count(new_event.event_id) == 1


@pytest.mark.anyio
async def test_live_event_query_does_not_persist_runtime_events(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    await b1_service.flush_events()
    b1_service.event_worker.detach_all()
    new_event = service.orchestrator.event_store.append(
        call_id=result.call_id,
        type="model_error",
        source="provider",
        payload={"message": "provider failed"},
    )

    live_events = await service.list_events(
        call_id=result.call_id,
        limit=1000,
        after_event_id=None,
    )
    persisted_rows = await record_service.list_events(
        result.call_id,
        event_type="model_error",
    )

    assert [event.event_id for event in live_events.rows].count(new_event.event_id) == 1
    assert persisted_rows == []


@pytest.mark.anyio
async def test_mirror_runtime_events_filters_high_frequency_events_after_first_1000(
    b1_service,
) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    await b1_service.flush_events()
    b1_service.event_worker.detach_all()
    for index in range(1005):
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="model_audio_delta",
            source="provider",
            payload={"deltaBytes": index},
        )
    key_event = service.orchestrator.event_store.append(
        call_id=result.call_id,
        type="model_error",
        source="provider",
        payload={"message": "provider failed"},
    )

    await record_service.mirror_runtime_events(
        service.orchestrator.event_store.list_all(call_id=result.call_id)
    )

    persisted_key_events = await record_service.list_events(
        result.call_id,
        event_type="model_error",
    )
    persisted_audio_events = await record_service.list_events(
        result.call_id,
        event_type="model_audio_delta",
    )
    assert [event.event_id for event in persisted_key_events] == [key_event.event_id]
    assert persisted_audio_events == []


@pytest.mark.anyio
async def test_event_persistence_worker_flushes_events_without_service_mirror(
    b1_service,
) -> None:
    service, record_service = b1_service

    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )

    await b1_service.flush_events()
    events = await record_service.list_events(result.call_id)
    assert [event.event_type for event in events] == [
        "session_created",
        "session_preparing",
        "room_created",
        "browser_token_issued",
        "agent_started",
        "session_ready",
    ]


@pytest.mark.anyio
async def test_event_persistence_worker_closes_record_on_model_error(
    b1_service,
) -> None:
    service, _record_service = b1_service

    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    service.orchestrator.event_store.append(
        call_id=result.call_id,
        type="model_error",
        source="provider",
        payload={"error": {"message": "Conversation already has an active response"}},
    )
    await b1_service.flush_events()

    async with b1_service.event_worker.session_factory() as db:
        record = await AiCallRecordRepository(db).get_record(result.call_id)
        assert record is not None
        assert record.status == "failed"
        assert record.end_reason == "model_error"
        assert record.failure_stage == "model"
        assert record.failure_message == "Conversation already has an active response"
        assert record.ended_at is not None


@pytest.mark.anyio
async def test_event_worker_queue_full_does_not_block_runtime_path(b1_service) -> None:
    service, _record_service = b1_service
    b1_service.event_worker.detach_all()
    worker = AiCallEventPersistenceWorker(
        b1_service.event_worker.session_factory,
        queue_max_size=1,
    )
    service.orchestrator.event_store.add_listener(worker.enqueue)

    first = service.orchestrator.event_store.append(
        call_id="call_queue_full",
        type="model_error",
        source="provider",
        payload={"sequence": 1},
    )
    second = service.orchestrator.event_store.append(
        call_id="call_queue_full",
        type="model_error",
        source="provider",
        payload={"sequence": 2},
    )

    assert first.event_id != second.event_id
    assert worker.queue.qsize() == 1
    assert worker.dropped_count == 1
    service.orchestrator.event_store.remove_listener(worker.enqueue)


@pytest.mark.anyio
async def test_dialogue_worker_queue_full_does_not_block_runtime_path(b1_service) -> None:
    worker = AiCallDialoguePersistenceWorker(
        b1_service.event_worker.session_factory,
        queue_max_size=1,
    )
    now = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)

    first = DialogueSegmentSnapshot(
        call_id="call_dialogue_queue_full",
        segment_no=1,
        speaker_type="customer",
        speaker_identity=None,
        source="qwen_realtime",
        source_segment_id="item_1",
        text="第一句",
        segment_status="final",
        started_at=now,
        ended_at=now + timedelta(seconds=1),
        duration_ms=1000,
    )
    second = DialogueSegmentSnapshot(
        call_id="call_dialogue_queue_full",
        segment_no=2,
        speaker_type="customer",
        speaker_identity=None,
        source="qwen_realtime",
        source_segment_id="item_2",
        text="第二句",
        segment_status="final",
        started_at=now + timedelta(seconds=2),
        ended_at=now + timedelta(seconds=3),
        duration_ms=1000,
    )

    worker.enqueue(first)
    worker.enqueue(second)

    assert worker.queue.qsize() == 1
    assert worker.dropped_count == 1


@pytest.mark.anyio
async def test_recording_closure_starts_stops_and_registers_oss(monkeypatch) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        room_manager = FakeLiveKitRoomManager()
        fake_egress = FakeEgressManager(room_manager)
        service = AiCallService(
            build_b1_orchestrator(livekit_room_manager=room_manager),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
            ),
        )

        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        recording = await service.get_recording(result.call_id)
        assert recording is not None
        assert recording["status"] == "recording"
        assert recording["egressId"] == f"EG_{result.call_id}"

        await service.end_session(result.call_id)
        completed = await service.get_recording(result.call_id)
        assert completed is not None
        assert completed["status"] == "completed"
        assert completed["ossId"].isdigit()
        assert (
            completed["playUrl"]
            == f"https://files.test/recordings/ai-call/recordings/{result.call_id}.mp3"
        )
        assert completed["durationMs"] == 1200
        assert fake_egress.started == [(result.room_name, result.call_id)]
        assert fake_egress.stopped == [f"EG_{result.call_id}"]
        assert fake_egress.stop_saw_deleted_room is False
        assert room_manager.deleted_rooms == [result.room_name]

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_participant_recording_closure_records_customer_and_ai_tracks(monkeypatch) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = FakeEgressManager()
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
                participant_recording_enabled=True,
            ),
        )

        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        recording = await service.get_recording(result.call_id)
        assert recording is not None
        assert recording["tracks"] == []
        assert fake_egress.started_participants == []

        await service.report_browser_event(
            call_id=result.call_id,
            event_type="browser_ready",
            timestamp=None,
        )
        recording = await service.get_recording(result.call_id)
        assert recording is not None
        assert [track["trackRole"] for track in recording["tracks"]] == ["customer", "ai"]
        assert fake_egress.started_participants == [
            (result.room_name, result.call_id, "customer", result.participant_identity),
            (result.room_name, result.call_id, "ai", f"agent-{result.call_id}"),
        ]

        await service.end_session(result.call_id)
        completed = await service.get_recording(result.call_id)
        assert completed is not None
        assert completed["status"] == "completed"
        assert {track["status"] for track in completed["tracks"]} == {"completed"}
        assert {track["trackRole"] for track in completed["tracks"]} == {"customer", "ai"}
        assert all(track["ossId"] and track["playUrl"] for track in completed["tracks"])
        assert fake_egress.stopped == [
            f"EG_{result.call_id}",
            f"EG_{result.call_id}_customer_{result.participant_identity}",
            f"EG_{result.call_id}_ai_agent-{result.call_id}",
        ]

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_participant_recording_retries_until_participant_ready(monkeypatch) -> None:
    monkeypatch.setattr(
        AiCallRecordingService,
        "_PARTICIPANT_EGRESS_RETRY_DELAYS_SECONDS",
        (0.0, 0.0),
    )
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = FlakyParticipantEgressManager(failures_before_success=1)
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
                participant_recording_enabled=True,
            ),
        )

        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.report_browser_event(
            call_id=result.call_id,
            event_type="browser_ready",
            timestamp=None,
        )

        recording = await service.get_recording(result.call_id)
        assert recording is not None
        assert {track["status"] for track in recording["tracks"]} == {"recording"}
        assert (
            fake_egress.participant_start_attempts[("customer", result.participant_identity)] == 2
        )
        assert fake_egress.participant_start_attempts[("ai", f"agent-{result.call_id}")] == 2

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_handoff_connected_starts_human_agent_participant_recording(monkeypatch) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = FakeEgressManager()
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
                participant_recording_enabled=True,
            ),
            handoff_service=AiCallHandoffService(repository),
        )

        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.report_browser_event(
            call_id=result.call_id,
            event_type="browser_ready",
            timestamp=None,
        )
        handoff = await service.create_handoff(
            call_id=result.call_id,
            source="operator",
            reason="customer_request",
            request_message=None,
        )
        accepted = await service.accept_handoff(
            handoff_id=handoff["handoffId"],
            human_agent_identity="agent-debug-001",
        )
        human_participant_identity = accepted["seatToken"]["participantIdentity"]

        recording = await service.get_recording(result.call_id)
        assert recording is not None
        assert not [track for track in recording["tracks"] if track["trackRole"] == "human_agent"]

        await service.mark_handoff_connected(handoff["handoffId"])
        recording = await service.get_recording(result.call_id)
        assert recording is not None
        human_tracks = [
            track for track in recording["tracks"] if track["trackRole"] == "human_agent"
        ]
        assert len(human_tracks) == 1
        assert human_tracks[0]["participantIdentity"] == human_participant_identity
        assert human_tracks[0]["handoffId"] == handoff["handoffId"]

        await service.complete_handoff(
            handoff_id=handoff["handoffId"],
            reason="agent_completed",
        )
        completed = await service.get_recording(result.call_id)
        assert completed is not None
        assert {track["trackRole"] for track in completed["tracks"]} == {
            "customer",
            "ai",
            "human_agent",
        }
        assert {track["status"] for track in completed["tracks"]} == {"completed"}

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_offline_asr_persists_split_track_dialogue_segments(monkeypatch) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = FakeEgressManager()
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
                participant_recording_enabled=True,
            ),
            dialogue_service=AiCallDialogueService(repository),
        )

        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.report_browser_event(
            call_id=result.call_id,
            event_type="browser_ready",
            timestamp=None,
        )
        await service.dialogue_service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id=result.call_id,
                segment_no=1,
                speaker_type="customer",
                speaker_identity=result.participant_identity,
                source="qwen_realtime",
                source_segment_id="item_realtime_1",
                text="实时识别文本",
                segment_status="final",
                started_at=datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc),
                ended_at=datetime(2026, 6, 16, 10, 0, 1, tzinfo=timezone.utc),
                duration_ms=1000,
            )
        )
        await service.end_session(result.call_id)
        await db.commit()

        provider = FakeOfflineAsrProvider()
        asr_service = AiCallOfflineAsrService(repository, provider=provider)
        stats = await asr_service.process_call(result.call_id)

        assert stats["jobs"] == 1
        assert stats["segments"] == 1
        assert stats["skipped"] == 1
        assert len(provider.audio_urls) == 1
        rows = await service.list_record_dialogue_segments(result.call_id)
        assert rows["total"] == 2
        assert [(row["segmentNo"], row["source"], row["speakerType"]) for row in rows["rows"]] == [
            (1, "qwen_realtime", "customer"),
            (2, "offline_asr", "customer"),
        ]
        assert rows["rows"][1]["text"] == "客户需要转人工"
        assert rows["rows"][1]["audioStartMs"] == 100

        recording = await service.get_recording(result.call_id)
        assert recording is not None
        assert {job["status"] for job in recording["asrJobs"]} == {"completed"}
        assert {job["segmentCount"] for job in recording["asrJobs"]} == {1}
        assert {job["trackRole"] for job in recording["asrJobs"]} == {"customer"}

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_offline_asr_skips_duplicate_realtime_customer_segment(monkeypatch) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=FakeEgressManager(),
                participant_recording_enabled=True,
            ),
            dialogue_service=AiCallDialogueService(repository),
        )

        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.report_browser_event(
            call_id=result.call_id,
            event_type="browser_ready",
            timestamp=None,
        )
        now = datetime.now(timezone.utc)
        await service.dialogue_service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id=result.call_id,
                segment_no=1,
                speaker_type="customer",
                speaker_identity=result.participant_identity,
                source="qwen_realtime",
                source_segment_id="item_realtime_duplicate",
                text="客户需要转人工",
                segment_status="final",
                started_at=now - timedelta(seconds=10),
                ended_at=now + timedelta(seconds=10),
                duration_ms=20000,
            )
        )
        await service.end_session(result.call_id)
        await db.commit()

        provider = FakeOfflineAsrProvider()
        asr_service = AiCallOfflineAsrService(repository, provider=provider)
        stats = await asr_service.process_call(result.call_id)

        assert stats["jobs"] == 1
        assert stats["segments"] == 0
        assert stats["skipped"] == 1
        assert len(provider.audio_urls) == 1
        assert len(await repository.list_dialogue_segments(result.call_id)) == 1

        rows = await service.list_record_dialogue_segments(result.call_id)
        assert rows["total"] == 1
        assert rows["rows"][0]["source"] == "qwen_realtime"
        assert rows["rows"][0]["text"] == "客户需要转人工"

        recording = await service.get_recording(result.call_id)
        assert recording is not None
        assert len(recording["asrJobs"]) == 1
        assert recording["asrJobs"][0]["trackRole"] == "customer"
        assert recording["asrJobs"][0]["segmentCount"] == 0

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_end_session_enqueues_offline_asr(b1_service) -> None:
    service = b1_service.service
    worker = FakeOfflineAsrWorker()
    configure_ai_call_offline_asr(worker)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.end_session(result.call_id)
        assert result.call_id in worker.call_ids
    finally:
        configure_ai_call_offline_asr(None)


@pytest.mark.anyio
async def test_end_session_does_not_enqueue_offline_asr_while_recording_verifies(
    monkeypatch,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    worker = FakeOfflineAsrWorker()
    configure_ai_call_offline_asr(worker)
    try:
        async with session_maker() as db:
            repository = AiCallRecordRepository(db)
            record_service = AiCallRecordService(repository)
            service = AiCallService(
                build_b1_orchestrator(),
                record_service,
                recording_service=AiCallRecordingService(
                    repository,
                    enabled=True,
                    egress_manager=TimeoutMainStopEgressManager(),
                ),
            )

            result = await service.create_web_session(
                voice=None,
                prompt=None,
                business_id=None,
            )
            await service.end_session(result.call_id)

            recording = await service.get_recording(result.call_id)
            assert recording is not None
            assert recording["status"] == "verifying"
            assert worker.call_ids == []
    finally:
        configure_ai_call_offline_asr(None)

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_recording_reconcile_worker_enqueues_asr_after_verification(
    monkeypatch,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    async def fake_resolve_existing_object_size(config, object_name):
        assert config["bucket_name"] == "recordings"
        assert object_name.startswith("ai-call/recordings/")
        return 4096

    monkeypatch.setattr(
        OssService,
        "resolve_existing_object_size",
        staticmethod(fake_resolve_existing_object_size),
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    ready_for_asr: list[str] = []
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=TimeoutMainStopEgressManager(),
            ),
        )
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.end_session(result.call_id)
        await db.commit()

    def service_factory(repository: AiCallRecordRepository) -> AiCallRecordingService:
        return AiCallRecordingService(repository, enabled=True)

    worker = AiCallRecordingReconcileWorker(
        session_maker,
        service_factory,
        on_call_ready_for_asr=ready_for_asr.append,
    )
    ready_call_ids = await worker.flush_once()

    assert ready_call_ids == {result.call_id}
    assert ready_for_asr == [result.call_id]

    async with session_maker() as db:
        service = AiCallRecordingService(AiCallRecordRepository(db), enabled=True)
        recording = await service.get_recording(result.call_id)
        assert recording is not None
        assert recording.status == "completed"
        assert recording.oss_id is not None
        assert recording.verify_attempts == 1

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_recording_registers_head_object_size_when_egress_size_is_zero(monkeypatch) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    async def fake_head_object_size(config, object_name, timeout=5.0):
        _ = timeout
        assert config["bucket_name"] == "recordings"
        assert object_name.startswith("ai-call/recordings/")
        return 4096

    monkeypatch.setattr(MinioUtil, "head_object_size", fake_head_object_size)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = FakeEgressManager(file_size=0)
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
            ),
        )

        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.end_session(result.call_id)

        completed = await service.get_recording(result.call_id)
        assert completed is not None
        oss_row = await db.execute(
            text("select ext1 from sys_oss where oss_id = :oss_id"),
            {"oss_id": int(completed["ossId"])},
        )
        ext1 = json.loads(oss_row.scalar_one())
        assert ext1["fileSize"] == 4096

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_browser_disconnect_stops_recording_before_room_delete(monkeypatch) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        room_manager = FakeLiveKitRoomManager()
        fake_egress = FakeEgressManager(room_manager)
        service = AiCallService(
            build_b1_orchestrator(livekit_room_manager=room_manager),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
            ),
        )

        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.report_browser_event(
            call_id=result.call_id,
            event_type="browser_ready",
            timestamp=None,
        )
        await service.report_browser_event(
            call_id=result.call_id,
            event_type="browser_disconnect",
            timestamp=None,
        )

        completed = await service.get_recording(result.call_id)
        assert completed is not None
        assert completed["status"] == "completed"
        assert fake_egress.stopped == [f"EG_{result.call_id}"]
        assert fake_egress.stop_saw_deleted_room is False
        assert room_manager.deleted_rooms == [result.room_name]

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_recording_stop_failed_status_marks_recording_failed(monkeypatch) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = FailedStopEgressManager()
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
            ),
        )

        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.end_session(result.call_id)

        failed = await service.get_recording(result.call_id)
        assert failed is not None
        assert failed["status"] == "failed"
        assert failed["failureStage"] == "egress_stop"
        assert failed["ossId"] is None
        assert fake_egress.stopped == [f"EG_{result.call_id}"]

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_recording_stop_timeout_enters_verifying_then_reconciles(
    monkeypatch,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    async def fake_resolve_existing_object_size(config, object_name):
        assert config["bucket_name"] == "recordings"
        assert object_name.startswith("ai-call/recordings/")
        return 358253

    monkeypatch.setattr(
        OssService,
        "resolve_existing_object_size",
        staticmethod(fake_resolve_existing_object_size),
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = TimeoutMainStopEgressManager()
        recording_service = AiCallRecordingService(
            repository,
            enabled=True,
            egress_manager=fake_egress,
        )
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=recording_service,
        )

        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.end_session(result.call_id)

        pending = await service.get_recording(result.call_id)
        assert pending is not None
        assert pending["status"] == "verifying"
        assert pending["ossId"] is None
        assert pending["nextVerifyAt"] is not None
        assert pending["verifyDeadlineAt"] is not None
        assert pending["lastVerifyError"] is not None
        assert fake_egress.stopped == [f"EG_{result.call_id}"]

        ready_call_ids = await recording_service.reconcile_due_recordings()
        assert ready_call_ids == {result.call_id}

        completed = await service.get_recording(result.call_id)
        assert completed is not None
        assert completed["status"] == "completed"
        assert completed["failureStage"] is None
        assert completed["failureMessage"] is None
        assert completed["ossId"] is not None
        assert completed["playUrl"] == (
            f"https://files.test/recordings/ai-call/recordings/{result.call_id}.mp3"
        )
        assert completed["verifyAttempts"] == 1
        assert completed["nextVerifyAt"] is None

        oss_row = await db.execute(
            text("select ext1 from sys_oss where oss_id = :oss_id"),
            {"oss_id": int(completed["ossId"])},
        )
        ext1 = json.loads(oss_row.scalar_one())
        assert ext1["fileSize"] == 358253

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_recording_stop_already_completed_recovers_from_oss(
    monkeypatch,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    async def fake_resolve_existing_object_size(config, object_name):
        assert config["bucket_name"] == "recordings"
        assert object_name.startswith("ai-call/recordings/")
        return 245760

    monkeypatch.setattr(
        OssService,
        "resolve_existing_object_size",
        staticmethod(fake_resolve_existing_object_size),
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = AlreadyCompletedMainStopEgressManager()
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
            ),
        )

        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.end_session(result.call_id)

        completed = await service.get_recording(result.call_id)
        assert completed is not None
        assert completed["status"] == "completed"
        assert completed["failureStage"] is None
        assert completed["failureMessage"] is None
        assert completed["ossId"] is not None
        assert completed["lastVerifyError"] is None
        assert fake_egress.stopped == [f"EG_{result.call_id}"]

        oss_row = await db.execute(
            text("select ext1 from sys_oss where oss_id = :oss_id"),
            {"oss_id": int(completed["ossId"])},
        )
        ext1 = json.loads(oss_row.scalar_one())
        assert ext1["fileSize"] == 245760

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_recording_stop_non_timeout_exception_does_not_recover_from_oss(
    monkeypatch,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    async def fail_if_resolved(config, object_name):
        _ = config, object_name
        raise AssertionError("non-timeout stop failure must not be recovered from OSS")

    monkeypatch.setattr(
        OssService,
        "resolve_existing_object_size",
        staticmethod(fail_if_resolved),
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = BrokenMainStopEgressManager()
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
            ),
        )

        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.end_session(result.call_id)

        failed = await service.get_recording(result.call_id)
        assert failed is not None
        assert failed["status"] == "failed"
        assert failed["failureStage"] == "egress_stop"
        assert failed["ossId"] is None
        assert fake_egress.stopped == [f"EG_{result.call_id}"]

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_recording_verification_deadline_marks_missing_object_failed(
    monkeypatch,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    async def missing_object(config, object_name):
        _ = config, object_name
        return None

    monkeypatch.setattr(
        OssService,
        "resolve_existing_object_size",
        staticmethod(missing_object),
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        recording_service = AiCallRecordingService(
            repository,
            enabled=True,
            egress_manager=TimeoutMainStopEgressManager(),
        )
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=recording_service,
        )

        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.end_session(result.call_id)

        expired_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        await repository.update_recording(
            result.call_id,
            next_verify_at=expired_at,
            verify_deadline_at=expired_at,
        )

        ready_call_ids = await recording_service.reconcile_due_recordings()
        assert ready_call_ids == {result.call_id}

        failed = await service.get_recording(result.call_id)
        assert failed is not None
        assert failed["status"] == "failed"
        assert failed["failureStage"] == "oss_missing"
        assert failed["verifyAttempts"] == 1
        assert failed["nextVerifyAt"] is None

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_participant_stop_timeout_enters_verifying_then_reconciles(
    monkeypatch,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    async def fake_resolve_existing_object_size(config, object_name):
        assert config["bucket_name"] == "recordings"
        assert "/tracks/" in object_name
        return 102080

    monkeypatch.setattr(
        OssService,
        "resolve_existing_object_size",
        staticmethod(fake_resolve_existing_object_size),
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = TimeoutAiTrackStopEgressManager()
        recording_service = AiCallRecordingService(
            repository,
            enabled=True,
            egress_manager=fake_egress,
            participant_recording_enabled=True,
        )
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=recording_service,
        )

        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.report_browser_event(
            call_id=result.call_id,
            event_type="browser_ready",
            timestamp=None,
        )
        await service.end_session(result.call_id)

        pending = await service.get_recording(result.call_id)
        assert pending is not None
        tracks = {track["trackRole"]: track for track in pending["tracks"]}
        assert tracks["customer"]["status"] == "completed"
        assert tracks["ai"]["status"] == "verifying"
        assert tracks["ai"]["ossId"] is None
        assert tracks["ai"]["nextVerifyAt"] is not None

        ready_call_ids = await recording_service.reconcile_due_recordings()
        assert ready_call_ids == {result.call_id}

        completed = await service.get_recording(result.call_id)
        assert completed is not None
        assert completed["status"] == "completed"
        tracks = {track["trackRole"]: track for track in completed["tracks"]}
        assert tracks["ai"]["status"] == "completed"
        assert tracks["ai"]["failureStage"] is None
        assert tracks["ai"]["failureMessage"] is None
        assert tracks["ai"]["ossId"] is not None
        assert tracks["ai"]["playUrl"] == (
            f"https://files.test/recordings/ai-call/recordings/tracks/"
            f"{result.call_id}/ai-agent-{result.call_id}.mp3"
        )

        oss_row = await db.execute(
            text("select ext1 from sys_oss where oss_id = :oss_id"),
            {"oss_id": int(tracks["ai"]["ossId"])},
        )
        ext1 = json.loads(oss_row.scalar_one())
        assert ext1["fileSize"] == 102080

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_participant_stop_already_completed_recovers_from_oss(
    monkeypatch,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    async def fake_resolve_existing_object_size(config, object_name):
        assert config["bucket_name"] == "recordings"
        assert "/tracks/" in object_name
        return 88064

    monkeypatch.setattr(
        OssService,
        "resolve_existing_object_size",
        staticmethod(fake_resolve_existing_object_size),
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = AlreadyCompletedAiTrackStopEgressManager()
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
                participant_recording_enabled=True,
            ),
        )

        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.report_browser_event(
            call_id=result.call_id,
            event_type="browser_ready",
            timestamp=None,
        )
        await service.end_session(result.call_id)

        recording = await service.get_recording(result.call_id)
        assert recording is not None
        tracks = {track["trackRole"]: track for track in recording["tracks"]}
        assert tracks["customer"]["status"] == "completed"
        assert tracks["ai"]["status"] == "completed"
        assert tracks["ai"]["failureStage"] is None
        assert tracks["ai"]["failureMessage"] is None
        assert tracks["ai"]["ossId"] is not None
        assert tracks["ai"]["lastVerifyError"] is None

        oss_row = await db.execute(
            text("select ext1 from sys_oss where oss_id = :oss_id"),
            {"oss_id": int(tracks["ai"]["ossId"])},
        )
        ext1 = json.loads(oss_row.scalar_one())
        assert ext1["fileSize"] == 88064

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_recording_oss_register_failure_keeps_object_name(monkeypatch) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    async def fail_register(*args, **kwargs):
        _ = args, kwargs
        raise RuntimeError("oss register failed")

    monkeypatch.setattr(
        OssService,
        "register_existing_object_service",
        staticmethod(fail_register),
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = FakeEgressManager()
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
            ),
        )

        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.end_session(result.call_id)

        failed = await service.get_recording(result.call_id)
        assert failed is not None
        assert failed["status"] == "failed"
        assert failed["failureStage"] == "oss_register"
        assert failed["objectName"] == f"ai-call/recordings/{result.call_id}.mp3"
        assert failed["ossId"] is None
        assert failed["durationMs"] == 1200

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_dialogue_preview_keeps_partial_in_memory_and_persists_final() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        orchestrator = build_b1_orchestrator()
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        runtime_store = AiCallDialogueRuntimeStore()
        runtime_store.attach_event_store(orchestrator.event_store)
        dialogue_worker = AiCallDialoguePersistenceWorker(
            session_maker,
            flush_interval_seconds=0.01,
        )
        await dialogue_worker.start()
        dialogue_worker.attach_runtime_store(runtime_store)
        service = AiCallService(
            orchestrator,
            record_service,
            dialogue_service=AiCallDialogueService(repository, runtime_store),
        )
        try:
            result = await service.create_web_session(
                voice=None,
                prompt=None,
                business_id=None,
            )
            orchestrator.event_store.append(
                call_id=result.call_id,
                type="user_speech_started",
                source="provider",
                payload={"item_id": "item_customer_1", "audio_start_ms": 1000},
            )
            orchestrator.event_store.append(
                call_id=result.call_id,
                type="user_transcript_delta",
                source="provider",
                payload={
                    "item_id": "item_customer_1",
                    "text": "今天",
                    "stash": "需要还款吗",
                },
            )

            preview = await service.list_dialogue_preview(result.call_id)
            assert preview["total"] == 1
            assert preview["rows"][0]["segmentStatus"] == "partial"
            assert preview["rows"][0]["text"] == "今天需要还款吗"
            assert await service.list_record_dialogue_segments(result.call_id) == {
                "rows": [],
                "total": 0,
            }

            orchestrator.event_store.append(
                call_id=result.call_id,
                type="user_speech_stopped",
                source="provider",
                payload={"item_id": "item_customer_1", "audio_end_ms": 2200},
            )
            await dialogue_worker.flush_pending()
            assert await service.list_record_dialogue_segments(result.call_id) == {
                "rows": [],
                "total": 0,
            }

            orchestrator.event_store.append(
                call_id=result.call_id,
                type="user_transcript_done",
                source="provider",
                payload={
                    "item_id": "item_customer_1",
                    "transcript": "今天需要还款吗",
                },
            )
            await dialogue_worker.flush_pending()

            persisted = await service.list_record_dialogue_segments(result.call_id)
            assert persisted["total"] == 1
            assert persisted["rows"][0]["id"].isdigit()
            assert persisted["rows"][0]["speakerType"] == "customer"
            assert persisted["rows"][0]["source"] == "qwen_realtime"
            assert persisted["rows"][0]["sourceSegmentId"] == "item_customer_1"
            assert persisted["rows"][0]["segmentStatus"] == "final"
            assert persisted["rows"][0]["text"] == "今天需要还款吗"
            assert persisted["rows"][0]["audioStartMs"] == 1000
            assert persisted["rows"][0]["audioEndMs"] == 2200

            orchestrator.event_store.append(
                call_id=result.call_id,
                type="user_transcript_done",
                source="provider",
                payload={
                    "item_id": "item_customer_1",
                    "transcript": "今天需要还款吗",
                },
            )
            await dialogue_worker.flush_pending()

            persisted = await service.list_record_dialogue_segments(result.call_id)
            assert persisted["total"] == 1
        finally:
            dialogue_worker.detach_all()
            runtime_store.detach_all()
            await dialogue_worker.stop()

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_dialogue_persistence_upserts_by_source_segment_id() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        service = AiCallDialogueService(repository)
        first = DialogueSegmentSnapshot(
            call_id="call_dialogue_upsert",
            segment_no=1,
            speaker_type="customer",
            speaker_identity=None,
            source="qwen_realtime",
            source_segment_id="item_same",
            text="你叫什么？",
            segment_status="final",
            started_at=datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc),
            ended_at=datetime(2026, 6, 16, 10, 0, 1, tzinfo=timezone.utc),
            duration_ms=1000,
        )
        second = DialogueSegmentSnapshot(
            call_id="call_dialogue_upsert",
            segment_no=99,
            speaker_type="customer",
            speaker_identity=None,
            source="qwen_realtime",
            source_segment_id="item_same",
            text="你叫什么呀",
            segment_status="final",
            started_at=datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc),
            ended_at=datetime(2026, 6, 16, 10, 0, 2, tzinfo=timezone.utc),
            duration_ms=2000,
        )

        await service.persist_snapshot(first)
        await service.persist_snapshot(second)

        rows = await service.list_persisted_segments("call_dialogue_upsert")
        assert len(rows) == 1
        assert rows[0].segment_no == 1
        assert rows[0].source_segment_id == "item_same"
        assert rows[0].segment_text == "你叫什么呀"

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_dialogue_persistence_separates_same_source_segment_id_by_speaker() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        service = AiCallDialogueService(repository)
        now = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)

        await service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id="call_same_item_id",
                segment_no=1,
                speaker_type="customer",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_reused",
                text="可以啊",
                segment_status="final",
                started_at=now,
                ended_at=now + timedelta(milliseconds=500),
                duration_ms=500,
            )
        )
        await service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id="call_same_item_id",
                segment_no=2,
                speaker_type="ai",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_reused",
                text="好的，那我简单问两句。",
                segment_status="final",
                started_at=now + timedelta(milliseconds=600),
                ended_at=now + timedelta(milliseconds=1600),
                duration_ms=1000,
            )
        )

        rows = await service.list_persisted_segments("call_same_item_id")
        assert [(row.speaker_type, row.source_segment_id) for row in rows] == [
            ("customer", "item_reused"),
            ("ai", "item_reused"),
        ]

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_dialogue_query_hides_duplicate_offline_asr_segment() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        service = AiCallDialogueService(repository)
        started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)
        ended_at = started_at + timedelta(milliseconds=1600)

        await service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id="call_dialogue_duplicate_sources",
                segment_no=1,
                speaker_type="customer",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_customer_1",
                text="你能做什么？",
                segment_status="final",
                started_at=started_at,
                ended_at=ended_at,
                duration_ms=1600,
            )
        )
        await repository.upsert_dialogue_segment(
            call_id="call_dialogue_duplicate_sources",
            segment_no=2,
            speaker_type="customer",
            speaker_identity="browser-call_dialogue_duplicate_sources",
            source="offline_asr",
            source_segment_id="track_customer_1",
            segment_text="你能做什么",
            segment_status="final",
            started_at=started_at - timedelta(milliseconds=400),
            ended_at=ended_at - timedelta(milliseconds=100),
            duration_ms=1500,
        )

        raw_rows = await repository.list_dialogue_segments("call_dialogue_duplicate_sources")
        rows = await service.list_persisted_segments("call_dialogue_duplicate_sources")

        assert len(raw_rows) == 2
        assert len(rows) == 1
        assert rows[0].source == "qwen_realtime"
        assert rows[0].segment_text == "你能做什么？"

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


def test_dialogue_runtime_merges_adjacent_customer_fragments() -> None:
    runtime_store = AiCallDialogueRuntimeStore()
    event_store = InMemoryEventStore()
    runtime_store.attach_event_store(event_store)
    persisted: list[DialogueSegmentSnapshot] = []
    runtime_store.add_persist_listener(persisted.append)
    call_id = "call_fragment_merge"
    started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)

    event_store.append(
        call_id=call_id,
        type="user_speech_started",
        source="provider",
        payload={"item_id": "item_1", "audio_start_ms": 1000},
        timestamp=started_at,
    )
    event_store.append(
        call_id=call_id,
        type="user_transcript_delta",
        source="provider",
        payload={"item_id": "item_1", "stash": "你叫什么？"},
        timestamp=started_at + timedelta(milliseconds=2),
    )
    event_store.append(
        call_id=call_id,
        type="user_speech_stopped",
        source="provider",
        payload={"item_id": "item_1", "audio_end_ms": 1500},
        timestamp=started_at + timedelta(milliseconds=4),
    )
    event_store.append(
        call_id=call_id,
        type="user_transcript_done",
        source="provider",
        payload={"item_id": "item_1", "transcript": "你叫什么？"},
        timestamp=started_at + timedelta(milliseconds=5),
    )
    event_store.append(
        call_id=call_id,
        type="user_speech_started",
        source="provider",
        payload={"item_id": "item_2", "audio_start_ms": 1501},
        timestamp=started_at + timedelta(milliseconds=6),
    )
    event_store.append(
        call_id=call_id,
        type="user_transcript_delta",
        source="provider",
        payload={"item_id": "item_2", "stash": "你叫什么呀"},
        timestamp=started_at + timedelta(milliseconds=8),
    )
    event_store.append(
        call_id=call_id,
        type="user_speech_stopped",
        source="provider",
        payload={"item_id": "item_2", "audio_end_ms": 1800},
        timestamp=started_at + timedelta(milliseconds=10),
    )
    event_store.append(
        call_id=call_id,
        type="session_completed",
        source="orchestrator",
        payload={},
        timestamp=started_at + timedelta(milliseconds=20),
    )

    preview = runtime_store.list_preview(call_id)
    assert len(preview) == 1
    assert preview[0].source_segment_id == "item_1"
    assert preview[0].text == "你叫什么呀"
    assert preview[0].audio_start_ms == 1000
    assert preview[0].audio_end_ms == 1800
    assert persisted[-1].text == "你叫什么呀"


def test_dialogue_runtime_suppresses_interrupted_ai_done_duplicate() -> None:
    runtime_store = AiCallDialogueRuntimeStore()
    event_store = InMemoryEventStore()
    runtime_store.attach_event_store(event_store)
    persisted: list[DialogueSegmentSnapshot] = []
    runtime_store.add_persist_listener(persisted.append)
    call_id = "call_ai_duplicate_after_interrupt"
    started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)

    event_store.append(
        call_id=call_id,
        type="ai_transcript_delta",
        source="provider",
        payload={"item_id": "item_ai_interrupted", "delta": "您好，我是灵宸智能助手。"},
        timestamp=started_at,
    )
    event_store.append(
        call_id=call_id,
        type="interrupt_confirmed",
        source="agent",
        payload={},
        timestamp=started_at + timedelta(milliseconds=300),
    )
    event_store.append(
        call_id=call_id,
        type="ai_transcript_done",
        source="provider",
        payload={"item_id": "item_ai_late", "transcript": "您好，我是灵宸智能助手。"},
        timestamp=started_at + timedelta(milliseconds=800),
    )

    preview = runtime_store.list_preview(call_id)
    assert len(preview) == 1
    assert preview[0].source_segment_id == "item_ai_interrupted"
    assert preview[0].segment_status == "interrupted"
    assert preview[0].text == "您好，我是灵宸智能助手。"
    assert len(persisted) == 1
