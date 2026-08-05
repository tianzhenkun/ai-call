from __future__ import annotations

import asyncio
import base64
import importlib
import io
import json
import wave
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.api.v1.ai_call import AiCallRouter
from app.api.v1.ai_call.model import AiCallVoiceProfileModel
from app.api.v1.ai_call.voice import controller as voice_controller
from app.api.v1.ai_call.voice.model import AiCallTenantVoiceProfileModel
from app.api.v1.ai_call.voice.service import (
    VOICE_PREVIEW_OPENING_MESSAGE,
    VoicePreviewService,
    get_app_voice_preview_service,
)
from app.api.v1.system.auth.schema import AuthSchema
from app.api.v1.system.user.model import UserModel
from app.config.setting import settings
from app.core.base_model import MappedBase
from app.core.dependencies import get_current_user
from app.core.exceptions import CustomException, handle_exception
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.livekit_room import BrowserRoomToken
from app.services.ai_call.orchestrator import (
    AiCallOrchestrator,
    AiCallRuntimeConfig,
    BrowserEventReportResult,
    CreateSessionResult,
    EffectiveConfig,
    EndSessionResult,
    WebAudioConstraints,
)
from app.services.ai_call.providers.aliyun_qwen_realtime import QwenRealtimeSessionConfig
from app.services.ai_call.providers.base import ProviderEvent
from app.services.ai_call.session_registry import CallSession, CallSessionStatus

TARGET_MODEL = "qwen3.5-omni-plus-realtime"
OTHER_MODEL = "qwen-omni-turbo-realtime"
NOW = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)


class SequenceIds:
    def __init__(self, *values: int) -> None:
        self._values = iter(values)

    def __call__(self) -> int:
        return next(self._values)


class FakeLiveKitRoomManager:
    def __init__(self) -> None:
        self.created_rooms: list[str] = []
        self.deleted_rooms: list[str] = []

    async def create_room(self, room_name: str) -> None:
        self.created_rooms.append(room_name)

    def issue_browser_token(
        self,
        room_name: str,
        participant_identity: str,
    ) -> BrowserRoomToken:
        return BrowserRoomToken(
            livekit_url="wss://livekit.preview.test",
            participant_token="preview-participant-token",
            participant_identity=participant_identity,
            expires_in_seconds=60,
        )

    async def delete_room(self, room_name: str) -> None:
        self.deleted_rooms.append(room_name)


class FakeAgentRunner:
    def __init__(self) -> None:
        self.started_call_ids: list[str] = []
        self.started_opening_call_ids: list[str] = []
        self.stopped_call_ids: list[str] = []

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
        return False

    async def record_browser_speech_segment(
        self,
        call_id: str,
        trigger_timestamp: datetime,
        payload: dict[str, object],
    ) -> bool:
        return False


class BlockingEndOrchestrator(AiCallOrchestrator):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.end_calls: list[str] = []
        self.end_started = asyncio.Event()
        self.release_end = asyncio.Event()

    async def abort_session(
        self,
        call_id: str,
        *,
        end_reason: str = "session_aborted",
        strict_agent_stop: bool = True,
    ):
        self.end_calls.append(call_id)
        self.end_started.set()
        await self.release_end.wait()
        return await super().abort_session(
            call_id,
            end_reason=end_reason,
            strict_agent_stop=strict_agent_stop,
        )


class BlockingReadyOrchestrator(AiCallOrchestrator):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.ready_started = asyncio.Event()
        self.release_ready = asyncio.Event()
        self.ready_calls = 0
        self.abort_calls: list[str] = []

    async def report_browser_event(self, *args, **kwargs):
        self.ready_calls += 1
        self.ready_started.set()
        await self.release_ready.wait()
        return await super().report_browser_event(*args, **kwargs)

    async def abort_session(
        self,
        call_id: str,
        *,
        end_reason: str = "session_aborted",
        strict_agent_stop: bool = True,
    ):
        self.abort_calls.append(call_id)
        return await super().abort_session(
            call_id,
            end_reason=end_reason,
            strict_agent_stop=strict_agent_stop,
        )


class BlockingCreateRoomManager(FakeLiveKitRoomManager):
    def __init__(self) -> None:
        super().__init__()
        self.create_started = asyncio.Event()

    async def create_room(self, room_name: str) -> None:
        self.created_rooms.append(room_name)
        self.create_started.set()
        await asyncio.Event().wait()


class LateCompletingCreateRoomManager(FakeLiveKitRoomManager):
    def __init__(self) -> None:
        super().__init__()
        self.create_started = asyncio.Event()
        self.release_create = asyncio.Event()
        self.external_rooms: set[str] = set()
        self.operations: list[str] = []

    async def create_room(self, room_name: str) -> None:
        self.create_started.set()
        try:
            await self.release_create.wait()
        except asyncio.CancelledError:
            self.operations.append("create_cancelled")
        self.created_rooms.append(room_name)
        self.external_rooms.add(room_name)
        self.operations.append("create_completed")

    async def delete_room(self, room_name: str) -> None:
        self.deleted_rooms.append(room_name)
        self.external_rooms.discard(room_name)
        self.operations.append("delete_completed")


class FailingTokenRoomManager(FakeLiveKitRoomManager):
    def issue_browser_token(
        self,
        room_name: str,
        participant_identity: str,
    ) -> BrowserRoomToken:
        raise RuntimeError("token failed api-key=secret-value object=private/sample.wav")


class FailingCleanupRoomManager(FakeLiveKitRoomManager):
    async def delete_room(self, room_name: str) -> None:
        raise RuntimeError("delete failed api-key=secret-value object=private/sample.wav")


class FlakyCleanupRoomManager(FakeLiveKitRoomManager):
    def __init__(
        self,
        *,
        delete_failures: int,
        token_failure: bool = False,
    ) -> None:
        super().__init__()
        self.delete_failures = delete_failures
        self.token_failure = token_failure
        self.delete_attempts = 0
        self.external_rooms: set[str] = set()

    async def create_room(self, room_name: str) -> None:
        await super().create_room(room_name)
        self.external_rooms.add(room_name)

    def issue_browser_token(
        self,
        room_name: str,
        participant_identity: str,
    ) -> BrowserRoomToken:
        if self.token_failure:
            raise RuntimeError("token failed api-key=secret-value object=private/sample.wav")
        return super().issue_browser_token(room_name, participant_identity)

    async def delete_room(self, room_name: str) -> None:
        self.delete_attempts += 1
        if self.delete_attempts <= self.delete_failures:
            raise RuntimeError("delete failed api-key=secret-value object=private/sample.wav")
        await super().delete_room(room_name)
        self.external_rooms.discard(room_name)


class PartialCreateFailureRoomManager(FlakyCleanupRoomManager):
    async def create_room(self, room_name: str) -> None:
        self.created_rooms.append(room_name)
        self.external_rooms.add(room_name)
        raise RuntimeError("create failed api-key=secret-value object=private/sample.wav")


class FailingAgentRunner(FakeAgentRunner):
    async def start(self, session: CallSession) -> None:
        raise RuntimeError("agent failed api-key=secret-value object=private/sample.wav")


class SlowStopAgentRunner(FakeAgentRunner):
    async def stop(self, call_id: str) -> None:
        self.stopped_call_ids.append(call_id)
        await asyncio.Event().wait()


class FailingCreateOrchestrator:
    def __init__(self) -> None:
        self.created_call_ids: list[str] = []
        self.ended_call_ids: list[str] = []

    async def create_web_session(self, *, call_id: str, **kwargs):
        self.created_call_ids.append(call_id)
        raise RuntimeError("provider failed with api-key=secret-value")

    async def abort_session(
        self,
        call_id: str,
        *,
        end_reason: str,
        strict_agent_stop: bool = True,
    ):
        self.ended_call_ids.append(call_id)
        return EndSessionResult(
            call_id=call_id,
            status=CallSessionStatus.FAILED,
        )

    def dispose_session(self, call_id: str) -> None:
        return None

    async def shutdown(self) -> None:
        return None


class FakeDefaultPreviewService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    async def create_preview_session(self, db, **values):
        self.calls.append(("create", (db, values)))
        return CreateSessionResult(
            call_id="preview_http",
            room_name="ai-call-preview_http",
            livekit_url="wss://livekit.preview.test",
            participant_token="preview-token",
            participant_identity="browser-preview_http",
            status=CallSessionStatus.READY,
            effective_config=EffectiveConfig(
                model=TARGET_MODEL,
                voice=values["voice"],
                prompt="internal preview instructions",
                prompt_hash="prompt-hash",
                opening_message=VOICE_PREVIEW_OPENING_MESSAGE,
                opening_message_hash="opening-hash",
                prompt_source_key="voice_preview",
                vad_type="server_vad",
                vad_threshold=0.5,
                vad_silence_duration_ms=500,
            ),
            web_audio_constraints=WebAudioConstraints(
                echo_cancellation=True,
                noise_suppression=True,
                auto_gain_control=True,
            ),
        )

    async def create_preview_audio(self, db, **values):
        self.calls.append(("audio", (db, values)))
        return {
            "audioUrl": "data:audio/wav;base64,UklGRg==",
            "text": VOICE_PREVIEW_OPENING_MESSAGE,
        }

    async def ready_preview_session(self, **values):
        self.calls.append(("ready", values))
        return BrowserEventReportResult(
            event_id="event-1",
            call_id=values["call_id"],
            type="browser_ready",
            timestamp=NOW,
            source="browser",
            payload={},
        )

    async def close_preview_session(self, **values):
        self.calls.append(("close", values))
        return EndSessionResult(
            call_id=values["call_id"],
            status=CallSessionStatus.COMPLETED,
        )


class FakePreviewAudioProvider:
    def __init__(self, pcm: bytes = b"\x01\x02\x03\x04") -> None:
        self.pcm = pcm
        self.connected = False
        self.closed = False
        self.session_configs: list[QwenRealtimeSessionConfig] = []
        self.response_inputs: list[str | None] = []

    async def connect(self) -> None:
        self.connected = True

    async def update_session(self, config: QwenRealtimeSessionConfig) -> None:
        self.session_configs.append(config)

    async def create_response(self, input_text: str | None = None) -> None:
        self.response_inputs.append(input_text)

    async def receive_events(self):
        yield ProviderEvent(
            type="model_audio_delta",
            payload={"delta": base64.b64encode(self.pcm).decode("ascii")},
        )
        yield ProviderEvent(type="model_audio_done", payload={})

    async def close(self) -> None:
        self.closed = True


async def _wait_until(predicate, *, attempts: int = 100) -> None:
    for _attempt in range(attempts):
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("condition was not met")


def _runtime_config() -> AiCallRuntimeConfig:
    return AiCallRuntimeConfig(
        livekit_url="wss://livekit.preview.test",
        livekit_api_key="preview-key",
        livekit_api_secret="preview-secret",
        browser_token_ttl_seconds=60,
        dashscope_api_key="dashscope-key",
        dashscope_realtime_url="wss://dashscope.preview.test",
        qwen_realtime_model=TARGET_MODEL,
        qwen_realtime_voice="Tina",
        default_prompt="你是智能语音助手。",
        opening_message="正式通话默认开场白",
        web_audio_echo_cancellation=True,
        web_audio_noise_suppression=True,
        web_audio_auto_gain_control=True,
        vad_type="server_vad",
        vad_threshold=0.5,
        vad_silence_duration_ms=500,
    )


def _orchestrator(
    *,
    blocking_end: bool = False,
) -> tuple[AiCallOrchestrator, FakeLiveKitRoomManager, FakeAgentRunner]:
    room_manager = FakeLiveKitRoomManager()
    agent_runner = FakeAgentRunner()
    orchestrator_type = BlockingEndOrchestrator if blocking_end else AiCallOrchestrator
    orchestrator = orchestrator_type(
        config=_runtime_config(),
        livekit_room_manager=room_manager,
        agent_runner=agent_runner,
        browser_ready_timeout_seconds=60,
    )
    return orchestrator, room_manager, agent_runner


def _blocking_ready_orchestrator() -> tuple[
    BlockingReadyOrchestrator,
    FakeLiveKitRoomManager,
    FakeAgentRunner,
]:
    room_manager = FakeLiveKitRoomManager()
    agent_runner = FakeAgentRunner()
    orchestrator = BlockingReadyOrchestrator(
        config=_runtime_config(),
        livekit_room_manager=room_manager,
        agent_runner=agent_runner,
        browser_ready_timeout_seconds=60,
    )
    return orchestrator, room_manager, agent_runner


def _global_voice(
    *,
    profile_id: int,
    voice: str,
    target_model: str = TARGET_MODEL,
) -> AiCallVoiceProfileModel:
    return AiCallVoiceProfileModel(
        id=profile_id,
        voice=voice,
        display_name=f"内置 {voice}",
        voice_type="内置",
        gender="女声",
        target_model=target_model,
        description=None,
        sort_order=profile_id,
        remark="",
        created_at=NOW,
        updated_at=NOW,
    )


def _tenant_voice(
    *,
    profile_id: int,
    tenant_id: str,
    voice: str,
    status: str = "ENABLED",
    target_model: str = TARGET_MODEL,
) -> AiCallTenantVoiceProfileModel:
    return AiCallTenantVoiceProfileModel(
        id=profile_id,
        tenant_id=tenant_id,
        display_name=f"租户 {voice}",
        voice=voice,
        voice_type="自定义复刻",
        gender="女声",
        language="zh",
        target_model=target_model,
        provider="aliyun_qwen",
        status=status,
        latest_enrollment_id=None,
        provider_created_at=NOW if status == "ENABLED" else None,
        error_message=None,
        created_by=7,
        deleted_by=None,
        deleted_at=None,
        created_at=NOW,
        updated_at=NOW,
    )


@pytest.fixture
async def preview_database():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    tables = [
        AiCallVoiceProfileModel.__table__,
        AiCallTenantVoiceProfileModel.__table__,
    ]
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: MappedBase.metadata.create_all(
                sync_connection,
                tables=tables,
            )
        )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as db:
            db.add_all([
                _global_voice(profile_id=1, voice="Tina"),
                _tenant_voice(
                    profile_id=2,
                    tenant_id="tenant-a",
                    voice="tenant-a-enabled",
                ),
                _tenant_voice(
                    profile_id=3,
                    tenant_id="tenant-b",
                    voice="tenant-b-enabled",
                ),
                _tenant_voice(
                    profile_id=4,
                    tenant_id="tenant-a",
                    voice="tenant-a-failed",
                    status="CREATE_FAILED",
                ),
                _tenant_voice(
                    profile_id=5,
                    tenant_id="tenant-a",
                    voice="tenant-a-wrong-model",
                    target_model=OTHER_MODEL,
                ),
            ])
            await db.commit()
        yield factory
    finally:
        await engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize("voice", ["Tina", "tenant-a-enabled"])
async def test_preview_allows_builtin_and_enabled_tenant_voice_with_fixed_config(
    preview_database,
    voice: str,
) -> None:
    orchestrator, _rooms, _runner = _orchestrator()
    service = VoicePreviewService(
        orchestrator=orchestrator,
        target_model=TARGET_MODEL,
        timeout_seconds=30,
        id_generator=SequenceIds(101),
    )

    async with preview_database() as db:
        result = await service.create_preview_session(
            db,
            tenant_id="tenant-a",
            user_id=7,
            voice=voice,
        )

    assert result.call_id == "preview_101"
    assert result.participant_identity == "browser-preview_101"
    assert result.effective_config.voice == voice
    assert result.effective_config.opening_message == VOICE_PREVIEW_OPENING_MESSAGE
    assert result.effective_config.prompt_source_key == "voice_preview"

    await service.close_preview_session(
        tenant_id="tenant-a",
        user_id=7,
        call_id=result.call_id,
    )


@pytest.mark.anyio
async def test_preview_audio_generates_direct_wav_data_url_without_livekit(
    preview_database,
) -> None:
    provider = FakePreviewAudioProvider(pcm=b"\x01\x02\x03\x04")
    orchestrator, _rooms, _runner = _orchestrator()
    service = VoicePreviewService(
        orchestrator=orchestrator,
        target_model=TARGET_MODEL,
        preview_audio_provider_factory=lambda: provider,
    )

    async with preview_database() as db:
        result = await service.create_preview_audio(
            db,
            tenant_id="tenant-a",
            user_id=7,
            voice="tenant-a-enabled",
        )

    assert provider.connected is True
    assert provider.closed is True
    assert provider.session_configs[0].voice == "tenant-a-enabled"
    assert provider.response_inputs == [VOICE_PREVIEW_OPENING_MESSAGE]
    assert result["text"] == VOICE_PREVIEW_OPENING_MESSAGE
    assert result["audioUrl"].startswith("data:audio/wav;base64,")

    wav_bytes = base64.b64decode(result["audioUrl"].split(",", 1)[1])
    with wave.open(io.BytesIO(wav_bytes), "rb") as wav_file:
        assert wav_file.getnchannels() == 1
        assert wav_file.getsampwidth() == 2
        assert wav_file.getframerate() == 24000
        assert wav_file.readframes(wav_file.getnframes()) == b"\x01\x02\x03\x04"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "voice",
    [
        "tenant-b-enabled",
        "tenant-a-failed",
        "tenant-a-wrong-model",
        "missing-voice",
    ],
)
async def test_preview_rejects_cross_tenant_non_enabled_or_wrong_model_voice(
    preview_database,
    voice: str,
) -> None:
    orchestrator, rooms, _runner = _orchestrator()
    service = VoicePreviewService(
        orchestrator=orchestrator,
        target_model=TARGET_MODEL,
        id_generator=SequenceIds(102),
    )

    async with preview_database() as db:
        with pytest.raises(CustomException) as error:
            await service.create_preview_session(
                db,
                tenant_id="tenant-a",
                user_id=7,
                voice=voice,
            )

    assert error.value.status_code == 404
    assert error.value.msg == "音色不可用"
    assert voice not in error.value.msg
    assert rooms.created_rooms == []
    assert service.pending_timeout_count == 0


@pytest.mark.anyio
async def test_ready_calls_orchestrator_directly_and_starts_opening(
    preview_database,
) -> None:
    orchestrator, _rooms, runner = _orchestrator()
    service = VoicePreviewService(
        orchestrator=orchestrator,
        target_model=TARGET_MODEL,
        id_generator=SequenceIds(103),
    )
    async with preview_database() as db:
        preview = await service.create_preview_session(
            db,
            tenant_id="tenant-a",
            user_id=7,
            voice="tenant-a-enabled",
        )

    event = await service.ready_preview_session(
        tenant_id="tenant-a",
        user_id=7,
        call_id=preview.call_id,
    )

    assert event.type == "browser_ready"
    assert runner.started_opening_call_ids == [preview.call_id]
    assert [item.type for item in orchestrator.event_store.list_all(preview.call_id)].count(
        "opening_started"
    ) == 1

    await service.close_preview_session(
        tenant_id="tenant-a",
        user_id=7,
        call_id=preview.call_id,
    )


@pytest.mark.anyio
async def test_preview_and_formal_session_namespaces_are_isolated(
    preview_database,
) -> None:
    preview_orchestrator, _rooms, _runner = _orchestrator()
    formal_orchestrator, _formal_rooms, _formal_runner = _orchestrator()
    service = VoicePreviewService(
        orchestrator=preview_orchestrator,
        target_model=TARGET_MODEL,
        id_generator=SequenceIds(104),
    )
    async with preview_database() as db:
        preview = await service.create_preview_session(
            db,
            tenant_id="tenant-a",
            user_id=7,
            voice="Tina",
        )

    with pytest.raises(AiCallError):
        await formal_orchestrator.get_session(preview.call_id)
    with pytest.raises(CustomException) as foreign_tenant:
        await service.ready_preview_session(
            tenant_id="tenant-b",
            user_id=8,
            call_id=preview.call_id,
        )
    with pytest.raises(CustomException) as formal_call:
        await service.ready_preview_session(
            tenant_id="tenant-a",
            user_id=7,
            call_id="call_999",
        )

    assert foreign_tenant.value.status_code == 404
    assert formal_call.value.status_code == 404

    await service.close_preview_session(
        tenant_id="tenant-a",
        user_id=7,
        call_id=preview.call_id,
    )


@pytest.mark.anyio
async def test_timeout_and_delete_race_releases_once_without_task_leak(
    preview_database,
) -> None:
    orchestrator, rooms, runner = _orchestrator(blocking_end=True)
    assert isinstance(orchestrator, BlockingEndOrchestrator)
    service = VoicePreviewService(
        orchestrator=orchestrator,
        target_model=TARGET_MODEL,
        timeout_seconds=0.01,
        id_generator=SequenceIds(105),
    )
    async with preview_database() as db:
        preview = await service.create_preview_session(
            db,
            tenant_id="tenant-a",
            user_id=7,
            voice="Tina",
        )

    await asyncio.wait_for(orchestrator.end_started.wait(), timeout=1)
    delete_task = asyncio.create_task(
        service.close_preview_session(
            tenant_id="tenant-a",
            user_id=7,
            call_id=preview.call_id,
        )
    )
    await asyncio.sleep(0)
    orchestrator.release_end.set()
    first_result = await asyncio.wait_for(delete_task, timeout=1)
    second_result = await service.close_preview_session(
        tenant_id="tenant-a",
        user_id=7,
        call_id=preview.call_id,
    )
    await asyncio.sleep(0)

    assert first_result.call_id == preview.call_id
    assert second_result.call_id == preview.call_id
    assert orchestrator.end_calls == [preview.call_id]
    assert rooms.deleted_rooms == [preview.room_name]
    assert runner.stopped_call_ids == [preview.call_id]
    assert service.pending_timeout_count == 0


@pytest.mark.anyio
async def test_preview_close_continues_after_agent_stop_timeout(
    preview_database,
) -> None:
    room_manager = FakeLiveKitRoomManager()
    runner = SlowStopAgentRunner()
    orchestrator = AiCallOrchestrator(
        config=_runtime_config(),
        livekit_room_manager=room_manager,
        agent_runner=runner,
        browser_ready_timeout_seconds=60,
        end_cleanup_timeout_seconds=0.01,
    )
    service = VoicePreviewService(
        orchestrator=orchestrator,
        target_model=TARGET_MODEL,
        id_generator=SequenceIds(215),
    )
    async with preview_database() as db:
        preview = await service.create_preview_session(
            db,
            tenant_id="tenant-a",
            user_id=7,
            voice="Tina",
        )

    result = await service.close_preview_session(
        tenant_id="tenant-a",
        user_id=7,
        call_id=preview.call_id,
    )

    assert result.status == CallSessionStatus.COMPLETED
    assert runner.stopped_call_ids == [preview.call_id]
    assert room_manager.deleted_rooms == [preview.room_name]
    assert service.active_session_count == 0


@pytest.mark.anyio
async def test_create_failure_attempts_cleanup_and_hides_provider_error(
    preview_database,
) -> None:
    orchestrator = FailingCreateOrchestrator()
    service = VoicePreviewService(
        orchestrator=orchestrator,
        target_model=TARGET_MODEL,
        id_generator=SequenceIds(106),
    )

    async with preview_database() as db:
        with pytest.raises(CustomException) as error:
            await service.create_preview_session(
                db,
                tenant_id="tenant-a",
                user_id=7,
                voice="Tina",
            )

    assert error.value.status_code == 502
    assert "secret-value" not in error.value.msg
    assert "provider failed" not in error.value.msg
    assert orchestrator.created_call_ids == ["preview_106"]
    assert orchestrator.ended_call_ids == ["preview_106"]
    assert service.pending_timeout_count == 0


def test_http_preview_routes_use_default_isolated_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", True)
    preview_service = FakeDefaultPreviewService()
    monkeypatch.setattr(
        voice_controller,
        "get_app_voice_preview_service",
        lambda _app: preview_service,
    )
    user = UserModel(
        user_id=7,
        tenant_id="tenant-a",
        user_name="tenant-user",
        nick_name="租户用户",
        user_type="sys_user",
    )
    auth = AuthSchema(
        db=AsyncSession(),
        user=user,
        check_data_scope=False,
        permissions=frozenset({"ai_call:voice:manage"}),
    )
    app = FastAPI()
    handle_exception(app)
    app.include_router(AiCallRouter)
    app.dependency_overrides[get_current_user] = lambda: auth

    with TestClient(app) as client:
        audio = client.post(
            "/ai-call/voice-preview-audio",
            json={"voice": "Tina"},
        )
        created = client.post(
            "/ai-call/voice-preview-sessions",
            json={"voice": "Tina"},
        )
        ready = client.post("/ai-call/voice-preview-sessions/preview_http/ready")
        closed = client.delete("/ai-call/voice-preview-sessions/preview_http")

    assert audio.status_code == 200
    assert created.status_code == 200
    assert ready.status_code == 200
    assert closed.status_code == 200
    assert audio.json()["data"] == {
        "audioUrl": "data:audio/wav;base64,UklGRg==",
        "text": VOICE_PREVIEW_OPENING_MESSAGE,
    }
    assert created.json()["data"]["participantIdentity"] == "browser-preview_http"
    assert created.json()["data"]["effectiveConfig"]["voice"] == "Tina"
    assert "prompt" not in created.json()["data"]["effectiveConfig"]
    assert "openingMessage" not in created.json()["data"]["effectiveConfig"]
    assert preview_service.calls[0][0] == "audio"
    audio_db, audio_values = preview_service.calls[0][1]
    assert audio_db is auth.db
    assert audio_values == {
        "tenant_id": "tenant-a",
        "user_id": 7,
        "voice": "Tina",
    }
    assert preview_service.calls[1][0] == "create"
    create_db, create_values = preview_service.calls[1][1]
    assert create_db is auth.db
    assert create_values == {
        "tenant_id": "tenant-a",
        "user_id": 7,
        "voice": "Tina",
    }


@pytest.mark.anyio
async def test_blocked_ready_does_not_block_timeout_cleanup(
    preview_database,
) -> None:
    orchestrator, rooms, runner = _blocking_ready_orchestrator()
    service = VoicePreviewService(
        orchestrator=orchestrator,
        target_model=TARGET_MODEL,
        timeout_seconds=0.01,
        id_generator=SequenceIds(201),
    )
    async with preview_database() as db:
        preview = await service.create_preview_session(
            db,
            tenant_id="tenant-a",
            user_id=7,
            voice="Tina",
        )

    ready_request = asyncio.create_task(
        service.ready_preview_session(
            tenant_id="tenant-a",
            user_id=7,
            call_id=preview.call_id,
        )
    )
    await asyncio.wait_for(orchestrator.ready_started.wait(), timeout=1)
    await asyncio.sleep(0.03)

    assert orchestrator.abort_calls == [preview.call_id]
    assert rooms.deleted_rooms == [preview.room_name]
    assert runner.stopped_call_ids == [preview.call_id]
    assert service.pending_timeout_count == 0
    ready_request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await ready_request


@pytest.mark.anyio
async def test_cancelled_delete_keeps_shared_cleanup_alive_and_retryable(
    preview_database,
) -> None:
    orchestrator, rooms, _runner = _orchestrator(blocking_end=True)
    assert isinstance(orchestrator, BlockingEndOrchestrator)
    service = VoicePreviewService(
        orchestrator=orchestrator,
        target_model=TARGET_MODEL,
        id_generator=SequenceIds(202),
    )
    async with preview_database() as db:
        preview = await service.create_preview_session(
            db,
            tenant_id="tenant-a",
            user_id=7,
            voice="Tina",
        )

    first_delete = asyncio.create_task(
        service.close_preview_session(
            tenant_id="tenant-a",
            user_id=7,
            call_id=preview.call_id,
        )
    )
    await asyncio.wait_for(orchestrator.end_started.wait(), timeout=1)
    first_delete.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first_delete
    orchestrator.release_end.set()

    result = await asyncio.wait_for(
        service.close_preview_session(
            tenant_id="tenant-a",
            user_id=7,
            call_id=preview.call_id,
        ),
        timeout=1,
    )

    assert result.call_id == preview.call_id
    assert orchestrator.end_calls == [preview.call_id]
    assert rooms.deleted_rooms == [preview.room_name]


@pytest.mark.anyio
async def test_cancelled_ready_request_does_not_cancel_shared_ready_task(
    preview_database,
) -> None:
    orchestrator, _rooms, _runner = _blocking_ready_orchestrator()
    service = VoicePreviewService(
        orchestrator=orchestrator,
        target_model=TARGET_MODEL,
        id_generator=SequenceIds(210),
    )
    async with preview_database() as db:
        preview = await service.create_preview_session(
            db,
            tenant_id="tenant-a",
            user_id=7,
            voice="Tina",
        )

    first_request = asyncio.create_task(
        service.ready_preview_session(
            tenant_id="tenant-a",
            user_id=7,
            call_id=preview.call_id,
        )
    )
    await asyncio.wait_for(orchestrator.ready_started.wait(), timeout=1)
    first_request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first_request

    orchestrator.release_ready.set()
    result = await service.ready_preview_session(
        tenant_id="tenant-a",
        user_id=7,
        call_id=preview.call_id,
    )

    assert result.type == "browser_ready"
    assert orchestrator.ready_calls == 1
    await service.close_preview_session(
        tenant_id="tenant-a",
        user_id=7,
        call_id=preview.call_id,
    )


@pytest.mark.anyio
async def test_same_tenant_different_user_cannot_access_preview(
    preview_database,
) -> None:
    orchestrator, _rooms, _runner = _orchestrator()
    service = VoicePreviewService(
        orchestrator=orchestrator,
        target_model=TARGET_MODEL,
        id_generator=SequenceIds(203),
    )
    async with preview_database() as db:
        preview = await service.create_preview_session(
            db,
            tenant_id="tenant-a",
            user_id=7,
            voice="Tina",
        )

    with pytest.raises(CustomException) as error:
        await service.ready_preview_session(
            tenant_id="tenant-a",
            user_id=8,
            call_id=preview.call_id,
        )

    assert error.value.status_code == 404
    await service.close_preview_session(
        tenant_id="tenant-a",
        user_id=7,
        call_id=preview.call_id,
    )


@pytest.mark.anyio
async def test_successful_cleanup_disposes_runtime_and_uses_bounded_tombstones(
    preview_database,
) -> None:
    clock = [100.0]
    orchestrator, _rooms, _runner = _orchestrator()
    service = VoicePreviewService(
        orchestrator=orchestrator,
        target_model=TARGET_MODEL,
        tombstone_ttl_seconds=5,
        tombstone_capacity=2,
        monotonic=lambda: clock[0],
        id_generator=SequenceIds(204, 205, 206),
    )
    call_ids: list[str] = []
    async with preview_database() as db:
        for _index in range(3):
            preview = await service.create_preview_session(
                db,
                tenant_id="tenant-a",
                user_id=7,
                voice="Tina",
            )
            call_ids.append(preview.call_id)
            await service.close_preview_session(
                tenant_id="tenant-a",
                user_id=7,
                call_id=preview.call_id,
            )

    assert service.active_session_count == 0
    assert service.tombstone_count == 2
    assert orchestrator.metrics_by_call_id == {}
    assert orchestrator.event_store.list_all(call_ids[-1]) == []
    with pytest.raises(AiCallError):
        orchestrator.registry.get(call_ids[-1])

    repeated = await service.close_preview_session(
        tenant_id="tenant-a",
        user_id=7,
        call_id=call_ids[-1],
    )
    assert repeated.call_id == call_ids[-1]

    clock[0] += 6
    with pytest.raises(CustomException) as expired:
        await service.close_preview_session(
            tenant_id="tenant-a",
            user_id=7,
            call_id=call_ids[-1],
        )
    assert expired.value.status_code == 404
    assert service.tombstone_count == 0


@pytest.mark.anyio
async def test_cancelled_create_shields_abort_and_preserves_cancellation(
    preview_database,
) -> None:
    room_manager = BlockingCreateRoomManager()
    orchestrator = AiCallOrchestrator(
        config=_runtime_config(),
        livekit_room_manager=room_manager,
        agent_runner=FakeAgentRunner(),
        browser_ready_timeout_seconds=60,
    )
    service = VoicePreviewService(
        orchestrator=orchestrator,
        target_model=TARGET_MODEL,
        id_generator=SequenceIds(207),
    )
    async with preview_database() as db:
        create_task = asyncio.create_task(
            service.create_preview_session(
                db,
                tenant_id="tenant-a",
                user_id=7,
                voice="Tina",
            )
        )
        await asyncio.wait_for(room_manager.create_started.wait(), timeout=1)
        create_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await create_task

    await asyncio.sleep(0)
    assert room_manager.deleted_rooms == ["ai-call-preview_207"]
    assert service.active_session_count == 0
    with pytest.raises(AiCallError):
        orchestrator.registry.get("preview_207")


@pytest.mark.anyio
async def test_shutdown_waits_for_inflight_create_before_cleanup(
    preview_database,
) -> None:
    room_manager = LateCompletingCreateRoomManager()
    orchestrator = AiCallOrchestrator(
        config=_runtime_config(),
        livekit_room_manager=room_manager,
        agent_runner=FakeAgentRunner(),
        browser_ready_timeout_seconds=60,
    )
    service = VoicePreviewService(
        orchestrator=orchestrator,
        target_model=TARGET_MODEL,
        id_generator=SequenceIds(211),
    )
    async with preview_database() as db:
        create_request = asyncio.create_task(
            service.create_preview_session(
                db,
                tenant_id="tenant-a",
                user_id=7,
                voice="Tina",
            )
        )
        await asyncio.wait_for(room_manager.create_started.wait(), timeout=1)
        shutdown_task = asyncio.create_task(service.shutdown())
        await asyncio.sleep(0.01)
        room_manager.release_create.set()
        await asyncio.wait_for(shutdown_task, timeout=1)
        await asyncio.gather(create_request, return_exceptions=True)

    assert room_manager.operations.index("create_completed") < room_manager.operations.index(
        "delete_completed"
    )
    assert room_manager.external_rooms == set()
    assert service.active_session_count == 0
    assert service.pending_create_count == 0
    assert service.pending_cleanup_retry_count == 0
    with pytest.raises(AiCallError):
        orchestrator.registry.get("preview_211")


@pytest.mark.anyio
async def test_create_failure_retries_hidden_cleanup_until_runtime_is_gone(
    preview_database,
) -> None:
    room_manager = FlakyCleanupRoomManager(
        delete_failures=2,
        token_failure=True,
    )
    orchestrator = AiCallOrchestrator(
        config=_runtime_config(),
        livekit_room_manager=room_manager,
        agent_runner=FakeAgentRunner(),
        browser_ready_timeout_seconds=60,
    )
    service = VoicePreviewService(
        orchestrator=orchestrator,
        target_model=TARGET_MODEL,
        cleanup_retry_delays=(0, 0.01),
        id_generator=SequenceIds(212),
    )

    async with preview_database() as db:
        with pytest.raises(CustomException) as create_error:
            await service.create_preview_session(
                db,
                tenant_id="tenant-a",
                user_id=7,
                voice="Tina",
            )

    assert "preview_212" not in create_error.value.msg
    await _wait_until(lambda: service.active_session_count == 0)
    assert room_manager.delete_attempts == 3
    assert room_manager.external_rooms == set()
    assert service.pending_cleanup_retry_count == 0
    with pytest.raises(AiCallError):
        orchestrator.registry.get("preview_212")


@pytest.mark.anyio
async def test_timeout_cleanup_failure_is_retried_in_background(
    preview_database,
) -> None:
    room_manager = FlakyCleanupRoomManager(delete_failures=1)
    orchestrator = AiCallOrchestrator(
        config=_runtime_config(),
        livekit_room_manager=room_manager,
        agent_runner=FakeAgentRunner(),
        browser_ready_timeout_seconds=60,
    )
    service = VoicePreviewService(
        orchestrator=orchestrator,
        target_model=TARGET_MODEL,
        timeout_seconds=0.01,
        cleanup_retry_delays=(0,),
        id_generator=SequenceIds(213),
    )
    async with preview_database() as db:
        preview = await service.create_preview_session(
            db,
            tenant_id="tenant-a",
            user_id=7,
            voice="Tina",
        )

    await _wait_until(
        lambda: service.active_session_count == 0 and service.pending_cleanup_retry_count == 0
    )
    assert room_manager.delete_attempts == 2
    assert room_manager.external_rooms == set()
    assert service.pending_timeout_count == 0
    assert service.pending_cleanup_retry_count == 0
    with pytest.raises(AiCallError):
        orchestrator.registry.get(preview.call_id)


@pytest.mark.anyio
async def test_exhausted_cleanup_remains_observable_and_shutdown_retries(
    preview_database,
) -> None:
    room_manager = FlakyCleanupRoomManager(delete_failures=2)
    orchestrator = AiCallOrchestrator(
        config=_runtime_config(),
        livekit_room_manager=room_manager,
        agent_runner=FakeAgentRunner(),
        browser_ready_timeout_seconds=60,
    )
    service = VoicePreviewService(
        orchestrator=orchestrator,
        target_model=TARGET_MODEL,
        cleanup_retry_delays=(0,),
        id_generator=SequenceIds(214),
    )
    async with preview_database() as db:
        preview = await service.create_preview_session(
            db,
            tenant_id="tenant-a",
            user_id=7,
            voice="Tina",
        )
        with pytest.raises(CustomException):
            await service.close_preview_session(
                tenant_id="tenant-a",
                user_id=7,
                call_id=preview.call_id,
            )

    await _wait_until(lambda: service.pending_cleanup_retry_count == 0)
    assert service.active_session_count == 1
    assert service.failed_cleanup_count == 1
    assert room_manager.external_rooms == {preview.room_name}

    await service.shutdown()

    assert service.active_session_count == 0
    assert service.failed_cleanup_count == 0
    assert room_manager.delete_attempts == 3
    assert room_manager.external_rooms == set()


@pytest.mark.anyio
async def test_orchestrator_abort_cleans_preparing_session_and_redacts_secrets() -> None:
    room_manager = FailingTokenRoomManager()
    orchestrator = AiCallOrchestrator(
        config=_runtime_config(),
        livekit_room_manager=room_manager,
        agent_runner=FakeAgentRunner(),
        browser_ready_timeout_seconds=60,
    )

    with pytest.raises(AiCallError) as create_error:
        await orchestrator.create_web_session(
            voice="Tina",
            prompt=None,
            call_id="preview_abort",
        )
    assert "secret-value" not in create_error.value.msg

    result = await orchestrator.abort_session(
        "preview_abort",
        end_reason="voice_preview_create_failed",
    )
    serialized_events = json.dumps([
        {"type": event.type, "payload": event.payload}
        for event in orchestrator.event_store.list_all("preview_abort")
    ])

    assert result.call_id == "preview_abort"
    assert room_manager.deleted_rooms == ["ai-call-preview_abort"]
    assert "secret-value" not in serialized_events
    assert "private/sample.wav" not in serialized_events


@pytest.mark.anyio
async def test_web_room_create_and_cleanup_failure_is_failed_and_redacted() -> None:
    room_manager = PartialCreateFailureRoomManager(delete_failures=1)
    orchestrator = AiCallOrchestrator(
        config=_runtime_config(),
        livekit_room_manager=room_manager,
        agent_runner=FakeAgentRunner(),
        browser_ready_timeout_seconds=60,
    )

    with patch("app.services.ai_call.orchestrator.log.error") as error_log:
        with pytest.raises(AiCallError) as create_error:
            await orchestrator.create_web_session(
                voice="Tina",
                prompt=None,
                call_id="formal_web_create_failed",
            )

    session = orchestrator.registry.get("formal_web_create_failed")
    serialized_events = json.dumps([
        {"type": event.type, "payload": event.payload}
        for event in orchestrator.event_store.list_all("formal_web_create_failed")
    ])
    assert session.status == CallSessionStatus.FAILED
    assert "session_cleanup_failed" in serialized_events
    assert "secret-value" not in create_error.value.msg
    assert "secret-value" not in serialized_events
    assert "private/sample.wav" not in serialized_events
    assert "secret-value" not in repr(error_log.call_args_list)

    await orchestrator.abort_session("formal_web_create_failed")
    assert room_manager.external_rooms == set()


@pytest.mark.anyio
async def test_sip_agent_start_and_cleanup_failure_is_failed_and_redacted() -> None:
    room_manager = FlakyCleanupRoomManager(delete_failures=1)
    orchestrator = AiCallOrchestrator(
        config=_runtime_config(),
        livekit_room_manager=room_manager,
        agent_runner=FailingAgentRunner(),
        browser_ready_timeout_seconds=60,
    )

    with patch("app.services.ai_call.orchestrator.log.error") as error_log:
        with pytest.raises(AiCallError) as create_error:
            await orchestrator.create_sip_session(
                voice="Tina",
                prompt=None,
                call_id="formal_sip_agent_failed",
            )

    session = orchestrator.registry.get("formal_sip_agent_failed")
    serialized_events = json.dumps([
        {"type": event.type, "payload": event.payload}
        for event in orchestrator.event_store.list_all("formal_sip_agent_failed")
    ])
    serialized_logs = repr(error_log.call_args_list)
    assert session.status == CallSessionStatus.FAILED
    assert "session_cleanup_failed" in serialized_events
    assert "secret-value" not in create_error.value.msg
    assert "secret-value" not in serialized_events
    assert "private/sample.wav" not in serialized_events
    assert "secret-value" not in serialized_logs
    assert "private/sample.wav" not in serialized_logs

    await orchestrator.abort_session("formal_sip_agent_failed")
    assert room_manager.external_rooms == set()


@pytest.mark.anyio
async def test_cleanup_failure_is_redacted_and_retryable(
    preview_database,
) -> None:
    room_manager = FlakyCleanupRoomManager(delete_failures=2)
    orchestrator = AiCallOrchestrator(
        config=_runtime_config(),
        livekit_room_manager=room_manager,
        agent_runner=FakeAgentRunner(),
        browser_ready_timeout_seconds=60,
    )
    service = VoicePreviewService(
        orchestrator=orchestrator,
        target_model=TARGET_MODEL,
        cleanup_retry_delays=(),
        id_generator=SequenceIds(208),
    )
    async with preview_database() as db:
        preview = await service.create_preview_session(
            db,
            tenant_id="tenant-a",
            user_id=7,
            voice="Tina",
        )

    for _attempt in range(2):
        with pytest.raises(CustomException) as cleanup_error:
            await service.close_preview_session(
                tenant_id="tenant-a",
                user_id=7,
                call_id=preview.call_id,
            )
        assert cleanup_error.value.status_code == 502
        assert "secret-value" not in cleanup_error.value.msg

    serialized_events = json.dumps([
        {"type": event.type, "payload": event.payload}
        for event in orchestrator.event_store.list_all(preview.call_id)
    ])
    assert "secret-value" not in serialized_events
    assert "private/sample.wav" not in serialized_events
    assert service.active_session_count == 1

    await service.shutdown()
    assert service.active_session_count == 0


@pytest.mark.anyio
async def test_shutdown_releases_active_sessions_and_rejects_new_creation(
    preview_database,
) -> None:
    orchestrator, rooms, runner = _orchestrator()
    service = VoicePreviewService(
        orchestrator=orchestrator,
        target_model=TARGET_MODEL,
        id_generator=SequenceIds(209),
    )
    async with preview_database() as db:
        preview = await service.create_preview_session(
            db,
            tenant_id="tenant-a",
            user_id=7,
            voice="Tina",
        )
        await service.shutdown()
        with pytest.raises(CustomException) as stopped:
            await service.create_preview_session(
                db,
                tenant_id="tenant-a",
                user_id=7,
                voice="Tina",
            )

    assert stopped.value.status_code == 503
    assert rooms.deleted_rooms == [preview.room_name]
    assert runner.stopped_call_ids == [preview.call_id]
    assert service.active_session_count == 0
    assert service.pending_timeout_count == 0
    assert service.tombstone_count == 0


def test_default_preview_ids_are_high_entropy_and_not_sequential() -> None:
    orchestrator, _rooms, _runner = _orchestrator()
    service = VoicePreviewService(
        orchestrator=orchestrator,
        target_model=TARGET_MODEL,
    )

    first = service._new_call_id()
    second = service._new_call_id()

    assert first.startswith("preview_")
    assert second.startswith("preview_")
    assert first != second
    assert len(first.removeprefix("preview_")) >= 32
    assert len(second.removeprefix("preview_")) >= 32


def test_preview_service_is_scoped_to_each_app(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    services = [object(), object()]
    monkeypatch.setattr(
        "app.api.v1.ai_call.voice.service.build_default_voice_preview_service",
        lambda: services.pop(0),
    )
    first_app = FastAPI()
    second_app = FastAPI()

    first = get_app_voice_preview_service(first_app)
    repeated = get_app_voice_preview_service(first_app)
    second = get_app_voice_preview_service(second_app)

    assert first is repeated
    assert first is not second


@pytest.mark.anyio
async def test_app_lifespan_starts_and_stops_preview_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_init = importlib.import_module("app.plugin.init_app")

    class ShutdownTrackingService:
        def __init__(self) -> None:
            self.shutdown_calls = 0

        async def shutdown(self) -> None:
            self.shutdown_calls += 1

    preview_service = ShutdownTrackingService()
    monkeypatch.setattr(settings, "AI_CALL_STANDALONE_ENABLE", True)
    monkeypatch.setattr(
        "app.api.v1.ai_call.voice.service.build_default_voice_preview_service",
        lambda: preview_service,
    )
    start_names = [
        "_start_ai_call_event_worker",
        "_start_ai_call_dialogue_worker",
        "_start_ai_call_semantic_analysis_worker",
        "_start_ai_call_quality_scoring_worker",
        "_start_ai_call_offline_asr_worker",
        "_start_ai_call_recording_reconcile_worker",
        "_start_ai_call_handoff_trigger_worker",
        "_start_ai_call_outbound_task_worker",
        "_start_ai_call_linphone_test_worker",
        "_start_ai_call_voice_worker",
    ]
    stop_names = [
        "_stop_ai_call_event_worker",
        "_stop_ai_call_dialogue_worker",
        "_stop_ai_call_semantic_analysis_worker",
        "_stop_ai_call_quality_scoring_worker",
        "_stop_ai_call_offline_asr_worker",
        "_stop_ai_call_recording_reconcile_worker",
        "_stop_ai_call_handoff_trigger_worker",
        "_stop_ai_call_outbound_task_worker",
        "_stop_ai_call_linphone_test_worker",
        "_stop_ai_call_voice_worker",
    ]
    for name in start_names:
        monkeypatch.setattr(app_init, name, AsyncMock(return_value=None))
    for name in stop_names:
        monkeypatch.setattr(app_init, name, AsyncMock(return_value=None))
    monkeypatch.setattr(
        app_init,
        "_recover_ai_call_outbound_validations",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        app_init,
        "_init_ai_call_standalone_oss_config",
        AsyncMock(return_value=None),
    )
    app = FastAPI()

    async with app_init.lifespan(app):
        assert app.state.voice_preview_service is preview_service
        assert preview_service.shutdown_calls == 0

    assert not hasattr(app.state, "voice_preview_service")
    assert preview_service.shutdown_calls == 1


@pytest.mark.anyio
async def test_agent_start_failure_redacts_response_events_and_logs() -> None:
    room_manager = FakeLiveKitRoomManager()
    orchestrator = AiCallOrchestrator(
        config=_runtime_config(),
        livekit_room_manager=room_manager,
        agent_runner=FailingAgentRunner(),
        browser_ready_timeout_seconds=60,
    )

    with patch("app.services.ai_call.orchestrator.log.error") as error_log:
        with pytest.raises(AiCallError) as create_error:
            await orchestrator.create_web_session(
                voice="Tina",
                prompt=None,
                call_id="preview_secret_log",
            )

    serialized_events = json.dumps([
        {"type": event.type, "payload": event.payload}
        for event in orchestrator.event_store.list_all("preview_secret_log")
    ])
    serialized_logs = repr(error_log.call_args_list)
    assert "secret-value" not in create_error.value.msg
    assert "private/sample.wav" not in create_error.value.msg
    assert "secret-value" not in serialized_events
    assert "private/sample.wav" not in serialized_events
    assert "secret-value" not in serialized_logs
    assert "private/sample.wav" not in serialized_logs
