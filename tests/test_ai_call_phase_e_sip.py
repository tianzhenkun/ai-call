from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.ai_call.controller import AiCallRouter, get_ai_call_service
from app.api.v1.ai_call.service import AiCallService
from app.config.setting import Settings
from app.core.exceptions import CustomException
from app.services.ai_call.event_store import InMemoryEventStore
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.livekit_room import BrowserRoomToken
from app.services.ai_call.livekit_sip import (
    CreateSipParticipantPayload,
    CreateSipParticipantResult,
    LiveKitSipClient,
    SipOutboundConfig,
    SipOutboundPreflightResult,
    validate_sip_outbound_preflight,
)
from app.services.ai_call.orchestrator import AiCallOrchestrator, AiCallRuntimeConfig
from app.services.ai_call.prompt_config import BusinessPromptResult, PromptComposer
from app.services.ai_call.session_registry import (
    CallSession,
    CallSessionStatus,
    InMemorySessionRegistry,
)


class FakeLiveKitRoomManager:
    def __init__(self) -> None:
        self.created_rooms: list[str] = []
        self.issued_browser_tokens: list[tuple[str, str]] = []

    async def create_room(self, room_name: str) -> None:
        self.created_rooms.append(room_name)

    def issue_browser_token(self, room_name: str, participant_identity: str) -> BrowserRoomToken:
        self.issued_browser_tokens.append((room_name, participant_identity))
        return BrowserRoomToken(
            livekit_url="wss://livekit.test",
            participant_token=f"browser-token-for-{participant_identity}",
            participant_identity=participant_identity,
            expires_in_seconds=600,
        )

    async def delete_room(self, room_name: str) -> None:
        _ = room_name


class CapturingAgentRunner:
    def __init__(self) -> None:
        self.started_sessions: list[CallSession] = []

    async def start(self, session: CallSession) -> None:
        self.started_sessions.append(session)

    async def start_opening(self, call_id: str) -> None:
        _ = call_id

    async def record_browser_speech_candidate(self, call_id: str, trigger_timestamp) -> bool:
        _ = call_id, trigger_timestamp
        return False

    async def suspend_for_handoff(self, call_id: str) -> None:
        _ = call_id

    async def stop(self, call_id: str) -> None:
        _ = call_id


class FakeSipClient:
    def __init__(
        self,
        error: AiCallError | None = None,
        preflight_result: SipOutboundPreflightResult | None = None,
    ) -> None:
        self.created: list[dict[str, object]] = []
        self.error = error
        self.preflight_result = preflight_result
        self.preflighted_callees: list[str] = []

    def preflight(self, *, callee_phone_number: str) -> SipOutboundPreflightResult:
        self.preflighted_callees.append(callee_phone_number)
        if self.preflight_result is not None:
            return self.preflight_result
        return SipOutboundPreflightResult(ok=True)

    async def create_participant(
        self,
        *,
        room_name: str,
        participant_identity: str,
        callee_phone_number: str,
        ringing_timeout_seconds: int | None = None,
        wait_until_answered: bool = True,
    ) -> CreateSipParticipantResult:
        self.created.append({
            "room_name": room_name,
            "participant_identity": participant_identity,
            "callee_phone_number": callee_phone_number,
            "ringing_timeout_seconds": ringing_timeout_seconds,
            "wait_until_answered": wait_until_answered,
        })
        if self.error is not None:
            raise self.error
        return CreateSipParticipantResult(
            room_name=room_name,
            participant_identity=participant_identity,
            sip_call_id="short-call-id",
            sip_call_id_full="full-call-id",
            sip_trunk_id="trunk_123",
            sip_call_status="active",
            raw_status="created",
        )


class FakeRecordService:
    repository = None

    def __init__(self) -> None:
        self.created_sip_records: list[dict[str, object]] = []
        self.status_updates: list[tuple[str, str]] = []
        self.failed_sessions: list[dict[str, object]] = []

    async def create_sip_record(
        self,
        *,
        call_id: str,
        business_id: str | None,
        room_name: str,
        participant_identity: str,
    ) -> None:
        self.created_sip_records.append({
            "call_id": call_id,
            "business_id": business_id,
            "room_name": room_name,
            "participant_identity": participant_identity,
        })

    async def mark_status(self, call_id: str, status: str | CallSessionStatus) -> None:
        status_value = status.value if isinstance(status, CallSessionStatus) else status
        self.status_updates.append((call_id, status_value))

    async def fail_session(
        self,
        call_id: str,
        *,
        end_reason: str,
        failure_stage: str | None,
        failure_message: str | None,
    ) -> None:
        self.failed_sessions.append({
            "call_id": call_id,
            "end_reason": end_reason,
            "failure_stage": failure_stage,
            "failure_message": failure_message,
        })


class FakePromptResolver:
    def __init__(self) -> None:
        self.contexts = []

    async def resolve(self, context) -> BusinessPromptResult:
        self.contexts.append(context)
        return BusinessPromptResult(
            prompt="请用自然语气介绍 GEO 产品。",
            opening_message="您好，我是灵宸智能助手。",
            source_key=context.scene_code,
        )


def build_runtime_config() -> AiCallRuntimeConfig:
    return AiCallRuntimeConfig(
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
    )


def build_service_with_sip_fakes() -> tuple[
    AiCallService,
    FakeLiveKitRoomManager,
    CapturingAgentRunner,
    FakeSipClient,
    FakeRecordService,
    FakePromptResolver,
]:
    room_manager = FakeLiveKitRoomManager()
    agent_runner = CapturingAgentRunner()
    orchestrator = AiCallOrchestrator(
        config=build_runtime_config(),
        livekit_room_manager=room_manager,
        agent_runner=agent_runner,
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    record_service = FakeRecordService()
    sip_client = FakeSipClient()
    prompt_resolver = FakePromptResolver()
    service = AiCallService(
        orchestrator,
        record_service=record_service,
        sip_client=sip_client,
        prompt_resolver=prompt_resolver,
        prompt_composer=PromptComposer(handoff_component_enabled=True),
    )
    return service, room_manager, agent_runner, sip_client, record_service, prompt_resolver


def build_service_with_failing_sip_client() -> tuple[
    AiCallService,
    FakeSipClient,
    FakeRecordService,
]:
    room_manager = FakeLiveKitRoomManager()
    agent_runner = CapturingAgentRunner()
    orchestrator = AiCallOrchestrator(
        config=build_runtime_config(),
        livekit_room_manager=room_manager,
        agent_runner=agent_runner,
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    record_service = FakeRecordService()
    sip_client = FakeSipClient(
        error=AiCallError(
            error_id="sip_create_participant_failed",
            msg="LiveKit SIP Participant 创建失败",
            status_code=502,
        )
    )
    service = AiCallService(
        orchestrator,
        record_service=record_service,
        sip_client=sip_client,
        prompt_resolver=FakePromptResolver(),
        prompt_composer=PromptComposer(handoff_component_enabled=True),
    )
    return service, sip_client, record_service


def build_service_with_failing_sip_preflight() -> tuple[
    AiCallService,
    FakeLiveKitRoomManager,
    CapturingAgentRunner,
    FakeSipClient,
    FakeRecordService,
]:
    room_manager = FakeLiveKitRoomManager()
    agent_runner = CapturingAgentRunner()
    orchestrator = AiCallOrchestrator(
        config=build_runtime_config(),
        livekit_room_manager=room_manager,
        agent_runner=agent_runner,
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    record_service = FakeRecordService()
    sip_client = FakeSipClient(
        preflight_result=SipOutboundPreflightResult(
            ok=False,
            failure_reason="sip_outbound_disabled",
            stage="sip_config",
            message="SIP 真实外呼未启用",
        )
    )
    service = AiCallService(
        orchestrator,
        record_service=record_service,
        sip_client=sip_client,
        prompt_resolver=FakePromptResolver(),
        prompt_composer=PromptComposer(handoff_component_enabled=True),
    )
    return service, room_manager, agent_runner, sip_client, record_service


def test_livekit_api_dependency_is_declared() -> None:
    pyproject = Path("pyproject.toml").read_text(encoding="utf-8")

    assert '"livekit-api>=1.0,<2.0"' in pyproject


def test_self_hosted_livekit_sip_templates_are_declared() -> None:
    compose = Path("deploy/livekit-egress/docker-compose.yml").read_text(encoding="utf-8")
    sip_config = Path("deploy/livekit-egress/sip.yaml.example").read_text(encoding="utf-8")

    assert "livekit-sip:" in compose
    assert "livekit/sip:latest" in compose
    assert "./sip.yaml:/etc/sip.yaml:ro" in compose
    assert "${SIP_SIGNALING_PORT:-5060}:${SIP_SIGNALING_PORT:-5060}/udp" in compose
    assert "${SIP_RTP_RANGE:-10000-20000}:${SIP_RTP_RANGE:-10000-20000}/udp" in compose
    assert "redis:" in sip_config
    assert "sip_port: 5060" in sip_config
    assert "rtp_port: 10000-20000" in sip_config
    assert "use_external_ip: true" in sip_config


def test_settings_expose_sip_outbound_defaults() -> None:
    settings = Settings(_env_file=None)

    assert settings.AI_CALL_SIP_OUTBOUND_ENABLED is False
    assert settings.AI_CALL_SIP_ALLOWED_CALLEE_PREFIXES == ""
    assert settings.AI_CALL_SIP_DEFAULT_RINGING_TIMEOUT_SECONDS == 45
    assert settings.AI_CALL_SIP_MAX_RINGING_TIMEOUT_SECONDS == 120
    assert settings.AI_CALL_SIP_MAX_CALL_DURATION_SECONDS == 600
    assert settings.LIVEKIT_SIP_OUTBOUND_TRUNK_ID == ""
    assert settings.LIVEKIT_SIP_OUTBOUND_TRUNK_HOSTNAME == ""
    assert settings.LIVEKIT_SIP_OUTBOUND_DESTINATION_COUNTRY == "CN"
    assert settings.LIVEKIT_SIP_AUTH_USERNAME == ""
    assert settings.LIVEKIT_SIP_AUTH_PASSWORD == ""
    assert settings.SIP_PUBLIC_IP == ""
    assert settings.SIP_USE_EXTERNAL_IP is True


def test_sip_preflight_accepts_ip_allowlist_trunk_without_password() -> None:
    config = SipOutboundConfig(
        enabled=True,
        allowed_callee_prefixes="13,15,+86",
        trunk_hostname="sip-provider.example.com:5060",
        caller_number="037100000000",
        auth_username="037100000000",
        auth_password="",
        signaling_port=5060,
        rtp_range="10000-20000",
        public_ip="203.0.113.10",
        use_external_ip=True,
    )

    result = validate_sip_outbound_preflight(
        config,
        callee_phone_number="13800000000",
    )

    assert result.ok is True
    assert result.failure_reason is None


def test_sip_preflight_rejects_disabled_outbound_before_real_call() -> None:
    config = SipOutboundConfig(
        enabled=False,
        trunk_hostname="sip-provider.example.com:5060",
        caller_number="037100000000",
        signaling_port=5060,
        rtp_range="10000-20000",
        public_ip="203.0.113.10",
    )

    result = validate_sip_outbound_preflight(
        config,
        callee_phone_number="13800000000",
    )

    assert result.ok is False
    assert result.failure_reason == "sip_outbound_disabled"
    assert result.stage == "sip_config"


def test_sip_preflight_rejects_callee_outside_allowed_prefixes() -> None:
    config = SipOutboundConfig(
        enabled=True,
        allowed_callee_prefixes="13,15",
        trunk_hostname="sip-provider.example.com:5060",
        caller_number="037100000000",
        signaling_port=5060,
        rtp_range="10000-20000",
        public_ip="203.0.113.10",
    )

    result = validate_sip_outbound_preflight(
        config,
        callee_phone_number="18800000000",
    )

    assert result.ok is False
    assert result.failure_reason == "callee_prefix_not_allowed"
    assert result.stage == "callee_number"


@pytest.mark.anyio
async def test_livekit_sip_client_builds_create_participant_payload_for_fake_sdk() -> None:
    captured_payloads: list[CreateSipParticipantPayload] = []

    async def fake_create_participant(payload: CreateSipParticipantPayload) -> dict:
        captured_payloads.append(payload)
        return {
            "identity": payload.participant_identity,
            "attributes": {
                "sip.callID": "short-call-id",
                "sip.callIDFull": "full-call-id",
                "sip.trunkID": "trunk_123",
                "sip.callStatus": "active",
            },
        }

    config = SipOutboundConfig(
        enabled=True,
        trunk_id="trunk_123",
        caller_number="037100000000",
        signaling_port=5060,
        rtp_range="10000-20000",
        public_ip="203.0.113.10",
    )
    client = LiveKitSipClient(
        config=config,
        create_participant=fake_create_participant,
    )

    result = await client.create_participant(
        room_name="ai-call-call_1",
        participant_identity="sip-call_1",
        callee_phone_number="13800000000",
        ringing_timeout_seconds=30,
    )

    assert captured_payloads == [
        CreateSipParticipantPayload(
            room_name="ai-call-call_1",
            participant_identity="sip-call_1",
            sip_call_to="13800000000",
            sip_number="037100000000",
            sip_trunk_id="trunk_123",
            trunk_hostname="",
            auth_username="",
            auth_password="",
            destination_country="CN",
            wait_until_answered=True,
            ringing_timeout_seconds=30,
        )
    ]
    assert result.participant_identity == "sip-call_1"
    assert result.sip_call_id == "short-call-id"
    assert result.sip_call_id_full == "full-call-id"
    assert result.sip_trunk_id == "trunk_123"
    assert result.sip_call_status == "active"


@pytest.mark.anyio
async def test_livekit_sip_client_raises_aicall_error_when_preflight_fails() -> None:
    client = LiveKitSipClient(
        config=SipOutboundConfig(enabled=False),
        create_participant=lambda payload: None,
    )

    with pytest.raises(AiCallError) as exc_info:
        await client.create_participant(
            room_name="ai-call-call_1",
            participant_identity="sip-call_1",
            callee_phone_number="13800000000",
        )

    assert exc_info.value.error_id == "sip_outbound_disabled"


@pytest.mark.anyio
async def test_create_sip_session_reuses_room_agent_prompt_and_records_sip_events() -> None:
    (
        service,
        room_manager,
        agent_runner,
        sip_client,
        record_service,
        prompt_resolver,
    ) = build_service_with_sip_fakes()

    result = await service.create_sip_session(
        callee_phone_number="13800000000",
        voice=None,
        business_id="geo_task_001",
        scene_code="intro_geo",
        business_params={"customerId": "customer_001"},
        ringing_timeout_seconds=30,
    )

    assert result.call_id.startswith("call_")
    assert result.room_name == f"ai-call-{result.call_id}"
    assert result.participant_identity == f"sip-{result.call_id}"
    assert result.status == CallSessionStatus.READY
    assert result.sip_call_id == "short-call-id"
    assert result.sip_call_status == "active"
    assert room_manager.created_rooms == [result.room_name]
    assert room_manager.issued_browser_tokens == []
    assert record_service.created_sip_records == [
        {
            "call_id": result.call_id,
            "business_id": "geo_task_001",
            "room_name": result.room_name,
            "participant_identity": result.participant_identity,
        }
    ]
    assert record_service.status_updates == [(result.call_id, "ready")]
    assert record_service.failed_sessions == []
    assert len(agent_runner.started_sessions) == 1
    assert agent_runner.started_sessions[0].participant_identity == result.participant_identity
    assert prompt_resolver.contexts[0].scene_code == "intro_geo"
    assert prompt_resolver.contexts[0].business_params == {"customerId": "customer_001"}
    assert sip_client.created == [
        {
            "room_name": result.room_name,
            "participant_identity": result.participant_identity,
            "callee_phone_number": "13800000000",
            "ringing_timeout_seconds": 30,
            "wait_until_answered": True,
        }
    ]

    events = service.orchestrator.event_store.list_all(result.call_id)
    event_types = [event.type for event in events]
    assert "session_created" in event_types
    assert "room_created" in event_types
    assert "agent_started" in event_types
    assert "session_ready" in event_types
    assert "sip_invite_sent" in event_types
    assert "sip_answered" in event_types
    assert "media_connected" in event_types
    sip_invite = next(event for event in events if event.type == "sip_invite_sent")
    sip_answered = next(event for event in events if event.type == "sip_answered")
    media_connected = next(event for event in events if event.type == "media_connected")
    assert sip_invite.payload == {
        "participantIdentity": result.participant_identity,
        "calleePhoneNumberMasked": "138****0000",
        "ringingTimeoutSeconds": 30,
    }
    assert sip_answered.source == "sip"
    assert media_connected.source == "livekit"


@pytest.mark.anyio
async def test_create_sip_session_marks_failed_when_sip_participant_creation_fails() -> None:
    service, sip_client, record_service = build_service_with_failing_sip_client()

    with pytest.raises(CustomException) as exc_info:
        await service.create_sip_session(
            callee_phone_number="13800000000",
            voice=None,
            business_id="geo_task_001",
            scene_code="intro_geo",
            business_params={},
            ringing_timeout_seconds=30,
        )

    assert exc_info.value.msg == "LiveKit SIP Participant 创建失败"
    assert len(sip_client.created) == 1
    assert len(record_service.failed_sessions) == 1
    failed = record_service.failed_sessions[0]
    assert failed["end_reason"] == "sip_create_participant_failed"
    assert failed["failure_stage"] == "sip"

    call_id = str(failed["call_id"])
    events = service.orchestrator.event_store.list_all(call_id)
    assert [event.type for event in events if event.type.startswith("sip_")] == [
        "sip_preflight_passed",
        "sip_invite_sent",
        "sip_failed",
    ]
    sip_failed = next(event for event in events if event.type == "sip_failed")
    assert sip_failed.payload == {
        "errorId": "sip_create_participant_failed",
        "message": "LiveKit SIP Participant 创建失败",
        "calleePhoneNumberMasked": "138****0000",
    }


@pytest.mark.anyio
async def test_create_sip_session_stops_before_room_when_sip_preflight_fails() -> None:
    service, room_manager, agent_runner, sip_client, record_service = (
        build_service_with_failing_sip_preflight()
    )

    with pytest.raises(CustomException) as exc_info:
        await service.create_sip_session(
            callee_phone_number="13800000000",
            voice=None,
            business_id="geo_task_001",
            scene_code="intro_geo",
            business_params={},
            ringing_timeout_seconds=30,
        )

    assert exc_info.value.msg == "SIP 真实外呼未启用"
    assert sip_client.preflighted_callees == ["13800000000"]
    assert sip_client.created == []
    assert room_manager.created_rooms == []
    assert agent_runner.started_sessions == []
    assert len(record_service.failed_sessions) == 1
    failed = record_service.failed_sessions[0]
    assert failed["end_reason"] == "sip_outbound_disabled"
    assert failed["failure_stage"] == "sip_config"

    call_id = str(failed["call_id"])
    events = service.orchestrator.event_store.list_all(call_id)
    assert [event.type for event in events if event.type.startswith("sip_")] == [
        "sip_preflight_failed",
    ]
    assert events[0].payload == {
        "errorId": "sip_outbound_disabled",
        "message": "SIP 真实外呼未启用",
        "calleePhoneNumberMasked": "138****0000",
    }


def test_create_sip_session_controller_accepts_dynamic_callee_without_browser_token() -> None:
    service, _room_manager, _agent_runner, sip_client, _record_service, _prompt_resolver = (
        build_service_with_sip_fakes()
    )
    app = FastAPI()
    app.include_router(AiCallRouter)
    app.dependency_overrides[get_ai_call_service] = lambda: service

    with TestClient(app) as client:
        response = client.post(
            "/ai-call/sip-sessions",
            json={
                "calleePhoneNumber": "13800000000",
                "businessId": "geo_task_001",
                "sceneCode": "intro_geo",
                "businessParams": {"customerId": "customer_001"},
                "ringingTimeoutSeconds": 30,
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["code"] == 200
    assert body["msg"] == "创建成功"
    data = body["data"]
    assert data["callId"].startswith("call_")
    assert data["roomName"] == f"ai-call-{data['callId']}"
    assert data["participantIdentity"] == f"sip-{data['callId']}"
    assert data["status"] == "ready"
    assert data["effectiveConfig"]["voice"] == "Tina"
    assert data["sipCallId"] == "short-call-id"
    assert data["sipTrunkId"] == "trunk_123"
    assert data["sipCallStatus"] == "active"
    assert "participantToken" not in data
    assert "livekitUrl" not in data
    assert "sipCallIdFull" not in data
    assert "calleePhoneNumber" not in data
    assert sip_client.created[0]["callee_phone_number"] == "13800000000"
