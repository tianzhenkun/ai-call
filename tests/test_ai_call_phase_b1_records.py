from __future__ import annotations

import pytest
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import AiCallEventModel, AiCallRecordModel
from app.api.v1.ai_call.service import AiCallService
from app.core.base_model import MappedBase
from app.services.ai_call.event_store import InMemoryEventStore
from app.services.ai_call.livekit_room import BrowserRoomToken
from app.services.ai_call.orchestrator import AiCallOrchestrator, AiCallRuntimeConfig
from app.services.ai_call.record_service import AiCallRecordService
from app.services.ai_call.session_registry import (
    CallSession,
    CallSessionStatus,
    InMemorySessionRegistry,
)


class FakeLiveKitRoomManager:
    async def create_room(self, room_name: str) -> None:
        _ = room_name

    def issue_browser_token(self, room_name: str, participant_identity: str) -> BrowserRoomToken:
        return BrowserRoomToken(
            livekit_url="wss://livekit.test",
            participant_token=f"browser-token-for-{participant_identity}",
            participant_identity=participant_identity,
            expires_in_seconds=600,
        )

    async def delete_room(self, room_name: str) -> None:
        _ = room_name


class FakeAgentRunner:
    async def start(self, session: CallSession) -> None:
        _ = session

    async def start_opening(self, call_id: str) -> None:
        _ = call_id

    async def record_browser_speech_candidate(self, call_id: str, trigger_timestamp) -> bool:
        _ = call_id, trigger_timestamp
        return False

    async def stop(self, call_id: str) -> None:
        _ = call_id


def build_b1_orchestrator() -> AiCallOrchestrator:
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
            opening_enabled=True,
            opening_message="您好，我是凌辰智能助手，请问现在方便简单沟通一下吗？",
            web_audio_echo_cancellation=True,
            web_audio_noise_suppression=True,
            web_audio_auto_gain_control=True,
            vad_type="server_vad",
            vad_threshold=0.5,
            vad_silence_duration_ms=800,
        ),
        livekit_room_manager=FakeLiveKitRoomManager(),
        agent_runner=FakeAgentRunner(),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )


@pytest.fixture
async def b1_service():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        orchestrator = build_b1_orchestrator()
        record_service = AiCallRecordService(AiCallRecordRepository(db))
        yield AiCallService(orchestrator, record_service), record_service

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


def test_phase_b1_models_do_not_use_physical_foreign_keys_or_relationships() -> None:
    assert not AiCallRecordModel.__table__.foreign_keys
    assert not AiCallEventModel.__table__.foreign_keys
    assert not sa_inspect(AiCallRecordModel).relationships
    assert not sa_inspect(AiCallEventModel).relationships


@pytest.mark.anyio
async def test_create_web_session_persists_record_and_key_events(b1_service) -> None:
    service, record_service = b1_service

    result = await service.create_web_session(
        voice="Cindy",
        prompt=None,
        business_type="debt",
        business_id="324800000000000001",
    )

    record = await record_service.get_record(result.call_id)
    assert record is not None
    assert record.id is not None
    assert record.call_id == result.call_id
    assert record.business_type == "debt"
    assert record.business_id == "324800000000000001"
    assert record.entry_type == "web"
    assert record.room_name == result.room_name
    assert record.participant_identity == result.participant_identity
    assert record.status == CallSessionStatus.READY.value

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
        business_type=None,
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

    events = await record_service.list_events(result.call_id)
    terminal_event = events[-1]
    assert terminal_event.event_type == "session_completed"
    assert terminal_event.payload == {"endReason": "web_user_end"}


@pytest.mark.anyio
async def test_record_query_outputs_bigint_ids_as_strings(b1_service) -> None:
    service, _record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_type="task",
        business_id="324800000000000002",
    )

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
