from __future__ import annotations

import asyncio
from datetime import datetime, timezone

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

    async def end_session(self, call_id: str, *, end_reason: str = "web_user_end"):
        self.end_calls.append(call_id)
        self.end_started.set()
        await self.release_end.wait()
        return await super().end_session(call_id, end_reason=end_reason)


class FailingCreateOrchestrator:
    def __init__(self) -> None:
        self.created_call_ids: list[str] = []
        self.ended_call_ids: list[str] = []

    async def create_web_session(self, *, call_id: str, **kwargs):
        self.created_call_ids.append(call_id)
        raise RuntimeError("provider failed with api-key=secret-value")

    async def end_session(self, call_id: str, *, end_reason: str):
        self.ended_call_ids.append(call_id)


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
        "get_default_voice_preview_service",
        lambda: preview_service,
        raising=False,
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
        created = client.post(
            "/ai-call/voice-preview-sessions",
            json={"voice": "Tina"},
        )
        ready = client.post("/ai-call/voice-preview-sessions/preview_http/ready")
        closed = client.delete("/ai-call/voice-preview-sessions/preview_http")

    assert created.status_code == 200
    assert ready.status_code == 200
    assert closed.status_code == 200
    assert created.json()["data"]["participantIdentity"] == "browser-preview_http"
    assert created.json()["data"]["effectiveConfig"]["voice"] == "Tina"
    assert "prompt" not in created.json()["data"]["effectiveConfig"]
    assert "openingMessage" not in created.json()["data"]["effectiveConfig"]
    assert preview_service.calls[0][0] == "create"
    create_db, create_values = preview_service.calls[0][1]
    assert create_db is auth.db
    assert create_values == {
        "tenant_id": "tenant-a",
        "user_id": 7,
        "voice": "Tina",
    }
