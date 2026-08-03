from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from livekit import api
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call import controller as ai_call_controller
from app.api.v1.ai_call import service as ai_call_service_module
from app.api.v1.ai_call.controller import AiCallRouter, get_ai_call_service
from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.schema import CreateSipSessionRequest
from app.api.v1.ai_call.service import AiCallService
from app.config.setting import Settings, settings
from app.core.base_model import MappedBase
from app.core.exceptions import CustomException
from app.services.ai_call import livekit_sip as livekit_sip_module
from app.services.ai_call.event_store import InMemoryEventStore
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.livekit_room import BrowserRoomToken
from app.services.ai_call.livekit_sip import (
    CreateSipParticipantPayload,
    CreateSipParticipantResult,
    LiveKitSipClient,
    SipOutboundConfig,
    SipOutboundPreflightResult,
    validate_sip_outbound_line_config,
    validate_sip_outbound_preflight,
)
from app.services.ai_call.orchestrator import AiCallOrchestrator, AiCallRuntimeConfig
from app.services.ai_call.prompt_config import BusinessPromptResult, PromptComposer
from app.services.ai_call.record_service import AiCallRecordService
from app.services.ai_call.session_registry import (
    CallSession,
    CallSessionStatus,
    InMemorySessionRegistry,
)


class FakeLiveKitRoomManager:
    def __init__(self) -> None:
        self.created_rooms: list[str] = []
        self.issued_browser_tokens: list[tuple[str, str]] = []
        self.deleted_rooms: list[str] = []

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
        self.deleted_rooms.append(room_name)


class CapturingAgentRunner:
    def __init__(self) -> None:
        self.started_sessions: list[CallSession] = []
        self.started_opening_call_ids: list[str] = []
        self.stopped_call_ids: list[str] = []
        self.stop_error: Exception | None = None

    async def start(self, session: CallSession) -> None:
        self.started_sessions.append(session)

    async def start_opening(self, call_id: str) -> None:
        self.started_opening_call_ids.append(call_id)

    async def record_browser_speech_candidate(self, call_id: str, trigger_timestamp) -> bool:
        _ = call_id, trigger_timestamp
        return False

    async def suspend_for_handoff(self, call_id: str) -> None:
        _ = call_id

    async def stop(self, call_id: str) -> None:
        self.stopped_call_ids.append(call_id)
        if self.stop_error is not None:
            raise self.stop_error


class FakeSipClient:
    def __init__(
        self,
        error: AiCallError | None = None,
        preflight_result: SipOutboundPreflightResult | None = None,
        sip_call_status: str = "active",
    ) -> None:
        self.created: list[dict[str, object]] = []
        self.error = error
        self.preflight_result = preflight_result
        self.sip_call_status = sip_call_status
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
            sip_call_status=self.sip_call_status,
            raw_status="created",
        )


class FakeRecordService:
    repository = None

    def __init__(self) -> None:
        self.created_sip_records: list[dict[str, object]] = []
        self.tenant_by_call: dict[str, str | None] = {}
        self.status_updates: list[tuple[str, str]] = []
        self.answered_updates: list[tuple[str, object]] = []
        self.completed_sessions: list[dict[str, object]] = []
        self.failed_sessions: list[dict[str, object]] = []
        self.active_sip_records_by_callee_hash: dict[str, object] = {}
        self.active_sip_record_lookups: list[str] = []
        self.prompt_context_updates: list[dict[str, object]] = []
        self.persisted_events: list[object] = []

    async def create_sip_record(
        self,
        *,
        tenant_id: str | None = None,
        call_id: str,
        business_id: str | None,
        room_name: str,
        participant_identity: str,
        business_type: str | None = None,
        callee_phone_number_hash: str | None = None,
        callee_phone_number_masked: str | None = None,
    ) -> None:
        self.tenant_by_call[call_id] = tenant_id
        self.created_sip_records.append({
            "call_id": call_id,
            "business_type": business_type,
            "business_id": business_id,
            "room_name": room_name,
            "participant_identity": participant_identity,
            "callee_phone_number_hash": callee_phone_number_hash,
            "callee_phone_number_masked": callee_phone_number_masked,
        })

    async def mark_status(self, call_id: str, status: str | CallSessionStatus) -> None:
        status_value = status.value if isinstance(status, CallSessionStatus) else status
        self.status_updates.append((call_id, status_value))

    async def update_prompt_context(
        self,
        call_id: str,
        *,
        scene_code: str | None,
        prompt_source_key: str | None,
    ) -> None:
        self.prompt_context_updates.append({
            "call_id": call_id,
            "scene_code": scene_code,
            "prompt_source_key": prompt_source_key,
        })

    async def mark_answered(self, call_id: str, answered_at) -> None:
        self.answered_updates.append((call_id, answered_at))

    async def complete_session(
        self,
        call_id: str,
        *,
        end_reason: str,
        ended_at=None,
    ) -> None:
        self.completed_sessions.append({
            "call_id": call_id,
            "end_reason": end_reason,
            "ended_at": ended_at,
        })

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

    async def get_active_sip_record_by_callee_hash(self, callee_phone_number_hash: str):
        self.active_sip_record_lookups.append(callee_phone_number_hash)
        return self.active_sip_records_by_callee_hash.get(callee_phone_number_hash)

    async def get_record(self, call_id: str):
        row = next(
            (
                item
                for item in self.created_sip_records
                if item["call_id"] == call_id
            ),
            None,
        )
        if row is None:
            return None
        status = next(
            (
                value
                for updated_call_id, value in reversed(self.status_updates)
                if updated_call_id == call_id
            ),
            CallSessionStatus.CREATED.value,
        )
        return SimpleNamespace(
            tenant_id=self.tenant_by_call.get(call_id),
            call_id=call_id,
            room_name=row["room_name"],
            participant_identity=row["participant_identity"],
            entry_type="sip_outbound",
            status=status,
        )

    async def mirror_runtime_events(self, events):
        self.persisted_events.extend(events)
        return events


class FakeRecordingService:
    def __init__(self) -> None:
        self.started_sessions: list[dict[str, object]] = []
        self.started_participants: list[dict[str, object]] = []
        self.stopped_call_ids: list[str] = []

    async def start_for_session(
        self,
        *,
        tenant_id: str,
        call_id: str,
        room_name: str,
        customer_participant_identity: str | None = None,
        ai_participant_identity: str | None = None,
    ) -> None:
        _ = tenant_id
        self.started_sessions.append({
            "call_id": call_id,
            "room_name": room_name,
            "customer_participant_identity": customer_participant_identity,
            "ai_participant_identity": ai_participant_identity,
        })

    async def start_session_participant_recordings(
        self,
        *,
        call_id: str,
        room_name: str,
        customer_participant_identity: str | None = None,
        ai_participant_identity: str | None = None,
    ) -> None:
        self.started_participants.append({
            "call_id": call_id,
            "room_name": room_name,
            "customer_participant_identity": customer_participant_identity,
            "ai_participant_identity": ai_participant_identity,
        })

    async def stop_for_session(self, *, tenant_id: str, call_id: str) -> None:
        _ = tenant_id
        self.stopped_call_ids.append(call_id)

    async def is_ready_for_offline_asr(self, *, tenant_id: str, call_id: str) -> bool:
        _ = (tenant_id, call_id)
        return True


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
            details={
                "rawErrorType": "TwirpError",
                "rawErrorMessage": "callee already has an active SIP call",
            },
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


def _callee_hash(phone_number: str) -> str:
    normalized = "".join(ch for ch in str(phone_number or "") if ch.isdigit())
    return f"sha256:{hashlib.sha256(normalized.encode('utf-8')).hexdigest()}"


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
    assert settings.AI_CALL_SIP_BARGE_IN_FAST_STOP_ENABLED is False
    assert settings.AI_CALL_SIP_BARGE_IN_RMS_THRESHOLD_DBFS == -36.0
    assert settings.AI_CALL_SIP_BARGE_IN_SNR_THRESHOLD_DB == 10.0
    assert settings.AI_CALL_SIP_BARGE_IN_VAD_VOICED_DURATION_MS == 120
    assert settings.AI_CALL_SIP_BARGE_IN_CANDIDATE_MIN_DURATION_MS == 180
    assert settings.AI_CALL_SIP_BARGE_IN_PRE_STOP_MIN_DURATION_MS == 240
    assert settings.AI_CALL_SIP_BARGE_IN_SHORT_SPEECH_MIN_DURATION_MS == 180
    assert settings.AI_CALL_SIP_BARGE_IN_IMPULSE_NOISE_MAX_DURATION_MS == 120
    assert settings.AI_CALL_SIP_BARGE_IN_CLEAN_WINDOW_MS == 300
    assert settings.AI_CALL_SIP_BARGE_IN_MAX_HOLD_MS == 500
    assert settings.AI_CALL_SIP_BARGE_IN_ECHO_TAIL_WINDOW_MS == 500
    assert settings.AI_CALL_SIP_BARGE_IN_RECOVERY_SILENCE_MS == 600
    assert settings.AI_CALL_SIP_BARGE_IN_RECOVERY_MAX_PER_TURN == 1
    assert settings.AI_CALL_SIP_VAD_SHADOW_ENABLED is False
    assert settings.AI_CALL_SIP_VAD_SHADOW_DETECTOR == "webrtc"
    assert (
        settings.AI_CALL_SIP_VAD_SHADOW_FSMN_MODEL
        == "iic/speech_fsmn_vad_zh-cn-16k-common-pytorch"
    )
    assert settings.AI_CALL_SIP_VAD_SHADOW_FSMN_ENDPOINT == ""
    assert settings.AI_CALL_SIP_VAD_SHADOW_FSMN_TIMEOUT_SECONDS == 0.2
    assert settings.AI_CALL_SIP_VAD_SHADOW_QUEUE_SIZE == 50


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


def test_managed_trunk_line_preflight_does_not_require_inline_network() -> None:
    result = validate_sip_outbound_line_config(
        SipOutboundConfig(
            enabled=True,
            trunk_id="ST_provider",
            caller_number="1000",
            public_ip="",
            rtp_range="",
        )
    )

    assert result.ok is True


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
async def test_livekit_sip_client_treats_successful_sync_sdk_result_as_answered() -> None:
    async def fake_create_participant(
        payload: CreateSipParticipantPayload,
    ) -> SimpleNamespace:
        assert payload.wait_until_answered is True
        return SimpleNamespace(
            participant_identity=payload.participant_identity,
            room_name=payload.room_name,
            sip_call_id="sdk-call-id",
        )

    client = LiveKitSipClient(
        config=SipOutboundConfig(
            enabled=True,
            trunk_id="trunk_123",
            caller_number="037100000000",
        ),
        create_participant=fake_create_participant,
    )

    result = await client.create_participant(
        room_name="ai-call-call_1",
        participant_identity="sip-call_1",
        callee_phone_number="13800000000",
        wait_until_answered=True,
    )

    assert result.sip_call_status == "answered"


def test_livekit_sip_client_extends_sdk_timeout_when_waiting_until_answered() -> None:
    client = LiveKitSipClient(
        config=SipOutboundConfig(enabled=True),
        timeout_seconds=10,
    )
    payload = CreateSipParticipantPayload(
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

    assert client._request_timeout_seconds(payload) == 35


def test_livekit_sip_client_embeds_inline_trunk_in_official_request() -> None:
    payload = CreateSipParticipantPayload(
        room_name="ai-call-call_1",
        participant_identity="sip-call_1",
        sip_call_to="13800000000",
        sip_number="037100000000",
        sip_trunk_id="",
        trunk_hostname="freeswitch-local:5089",
        auth_username="037100000000",
        auth_password="",
        destination_country="CN",
        wait_until_answered=True,
        ringing_timeout_seconds=12,
    )

    request = livekit_sip_module._build_official_create_sip_participant_request(payload)

    assert request.room_name == "ai-call-call_1"
    assert request.participant_identity == "sip-call_1"
    assert request.sip_call_to == "13800000000"
    assert request.sip_number == "037100000000"
    assert request.sip_trunk_id == ""
    assert request.HasField("trunk")
    assert request.trunk.hostname == "freeswitch-local:5089"
    assert request.trunk.destination_country == "CN"
    assert request.trunk.auth_username == "037100000000"


def test_sip_sdk_error_details_extract_provider_diagnostics() -> None:
    details = livekit_sip_module._safe_exception_details(
        RuntimeError("SIP 486 Busy Here; hangup_cause=USER_BUSY")
    )

    assert details["providerStatusCode"] == "486"
    assert details["providerReason"] == "SIP 486 Busy Here"
    assert details["hangupCause"] == "USER_BUSY"


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
            "business_type": None,
            "business_id": "geo_task_001",
            "room_name": result.room_name,
            "participant_identity": result.participant_identity,
            "callee_phone_number_hash": _callee_hash("13800000000"),
            "callee_phone_number_masked": "138****0000",
        }
    ]
    assert record_service.status_updates == [(result.call_id, "ready")]
    assert record_service.prompt_context_updates == [
        {
            "call_id": result.call_id,
            "scene_code": "intro_geo",
            "prompt_source_key": "intro_geo",
        }
    ]
    assert record_service.answered_updates
    assert record_service.answered_updates[0][0] == result.call_id
    assert record_service.failed_sessions == []
    assert len(agent_runner.started_sessions) == 1
    assert agent_runner.started_sessions[0].participant_identity == result.participant_identity
    assert agent_runner.started_opening_call_ids == [result.call_id]
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
    assert "media_connected" not in event_types
    assert "opening_started" in event_types
    sip_invite = next(event for event in events if event.type == "sip_invite_sent")
    sip_answered = next(event for event in events if event.type == "sip_answered")
    assert sip_invite.payload == {
        "participantIdentity": result.participant_identity,
        "calleePhoneNumberMasked": "138****0000",
        "calleePhoneNumberHash": _callee_hash("13800000000"),
        "ringingTimeoutSeconds": 30,
    }
    assert sip_answered.source == "sip"


@pytest.mark.anyio
async def test_create_sip_session_persists_record_before_external_sip_invite() -> None:
    (
        service,
        _room_manager,
        _agent_runner,
        sip_client,
        _record_service,
        _prompt_resolver,
    ) = build_service_with_sip_fakes()
    persisted_before_invite = False
    original_create_participant = sip_client.create_participant

    async def persist_before_invite() -> None:
        nonlocal persisted_before_invite
        persisted_before_invite = True

    async def create_participant(**kwargs):
        assert persisted_before_invite is True
        return await original_create_participant(**kwargs)

    sip_client.create_participant = create_participant

    await service.create_sip_session(
        callee_phone_number="13800000000",
        voice=None,
        business_id="geo_task_001",
        scene_code="intro_geo",
        business_params={},
        before_sip_invite=persist_before_invite,
    )


@pytest.mark.anyio
async def test_sip_audio_track_webhook_records_independent_media_evidence() -> None:
    service, _room_manager, _agent_runner, _sip_client, record_service, _resolver = (
        build_service_with_sip_fakes()
    )
    result = await service.create_sip_session(
        callee_phone_number="13800000000",
        voice=None,
        business_id="geo_task_001",
        scene_code="intro_geo",
        business_params={},
    )

    handled = await service.handle_livekit_webhook_event(
        event_type="track_published",
        room_name=result.room_name,
        participant_identity=result.participant_identity,
        payload={"track": {"type": "AUDIO", "sid": "TR_audio"}},
    )

    assert handled == {
        "handled": True,
        "action": "record_media_connected",
        "callId": result.call_id,
    }
    events = service.orchestrator.event_store.list_all(result.call_id)
    media_connected = [
        event for event in events if event.type == "media_connected"
    ]
    assert len(media_connected) == 1
    assert media_connected[0].source == "livekit"
    assert record_service.persisted_events == media_connected


@pytest.mark.anyio
async def test_sip_audio_track_webhook_uses_persisted_record_across_workers() -> None:
    (
        creator_service,
        _creator_room_manager,
        _creator_agent_runner,
        sip_client,
        record_service,
        prompt_resolver,
    ) = build_service_with_sip_fakes()
    result = await creator_service.create_sip_session(
        callee_phone_number="13800000000",
        voice=None,
        business_id="geo_task_001",
        scene_code="intro_geo",
        business_params={},
    )
    webhook_orchestrator = AiCallOrchestrator(
        config=build_runtime_config(),
        livekit_room_manager=FakeLiveKitRoomManager(),
        agent_runner=CapturingAgentRunner(),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    webhook_service = AiCallService(
        webhook_orchestrator,
        record_service=record_service,
        sip_client=sip_client,
        prompt_resolver=prompt_resolver,
        prompt_composer=PromptComposer(handoff_component_enabled=True),
    )

    handled = await webhook_service.handle_livekit_webhook_event(
        event_type="track_published",
        room_name=result.room_name,
        participant_identity=result.participant_identity,
        payload={"track": {"type": "AUDIO", "sid": "TR_cross_worker"}},
    )

    assert handled == {
        "handled": True,
        "action": "record_media_connected",
        "callId": result.call_id,
    }
    assert len(record_service.persisted_events) == 1
    assert record_service.persisted_events[0].call_id == result.call_id
    assert record_service.persisted_events[0].type == "media_connected"


@pytest.mark.anyio
async def test_create_sip_session_reuses_internal_call_id_for_outbound_task() -> None:
    (
        service,
        room_manager,
        agent_runner,
        sip_client,
        record_service,
        _prompt_resolver,
    ) = build_service_with_sip_fakes()

    result = await service.create_sip_session(
        callee_phone_number="13800000000",
        voice=None,
        call_id="call-task-1",
        business_type="outbound_task",
        business_id="1001",
        scene_code="intro_geo",
    )

    assert result.call_id == "call-task-1"
    assert result.room_name == "ai-call-call-task-1"
    assert result.participant_identity == "sip-call-task-1"
    assert room_manager.created_rooms == ["ai-call-call-task-1"]
    assert agent_runner.started_sessions[0].call_id == "call-task-1"
    assert agent_runner.started_sessions[0].participant_identity == "sip-call-task-1"
    assert sip_client.created[0]["room_name"] == "ai-call-call-task-1"
    assert sip_client.created[0]["participant_identity"] == "sip-call-task-1"
    assert record_service.created_sip_records == [
        {
            "call_id": "call-task-1",
            "business_type": "outbound_task",
            "business_id": "1001",
            "room_name": "ai-call-call-task-1",
            "participant_identity": "sip-call-task-1",
            "callee_phone_number_hash": _callee_hash("13800000000"),
            "callee_phone_number_masked": "138****0000",
        }
    ]
    events = service.orchestrator.event_store.list_all("call-task-1")
    assert events
    assert {event.call_id for event in events} == {"call-task-1"}


@pytest.mark.anyio
async def test_create_sip_session_rejects_duplicate_active_callee_before_room_creation() -> None:
    (
        service,
        room_manager,
        agent_runner,
        sip_client,
        record_service,
        _prompt_resolver,
    ) = build_service_with_sip_fakes()
    callee_hash = _callee_hash("13800000000")
    record_service.active_sip_records_by_callee_hash[callee_hash] = SimpleNamespace(
        call_id="call_active",
        status="connected",
        room_name="ai-call-call_active",
    )

    with pytest.raises(CustomException) as exc_info:
        await service.create_sip_session(
            callee_phone_number="13800000000",
            voice=None,
            business_id="geo_task_002",
            scene_code="intro_geo",
            business_params={},
            ringing_timeout_seconds=30,
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.code == 409
    assert exc_info.value.msg == "该号码已有进行中的电话外呼，请先结束当前通话后再重试"
    assert exc_info.value.data == {
        "activeCallId": "call_active",
        "activeStatus": "connected",
        "calleePhoneNumberMasked": "138****0000",
    }
    assert record_service.active_sip_record_lookups == [callee_hash]
    assert record_service.created_sip_records == []
    assert room_manager.created_rooms == []
    assert agent_runner.started_sessions == []
    assert sip_client.created == []


@pytest.mark.anyio
async def test_create_sip_session_starts_customer_and_ai_participant_recordings() -> None:
    (
        service,
        _room_manager,
        _agent_runner,
        _sip_client,
        _record_service,
        _prompt_resolver,
    ) = build_service_with_sip_fakes()
    recording_service = FakeRecordingService()
    service.recording_service = recording_service

    result = await service.create_sip_session(
        tenant_id="000000",
        callee_phone_number="13800000000",
        voice=None,
        business_id="geo_task_001",
        scene_code="intro_geo",
        business_params={},
    )

    assert recording_service.started_sessions == [
        {
            "call_id": result.call_id,
            "room_name": result.room_name,
            "customer_participant_identity": result.participant_identity,
            "ai_participant_identity": f"agent-{result.call_id}",
        }
    ]
    assert recording_service.started_participants == [
        {
            "call_id": result.call_id,
            "room_name": result.room_name,
            "customer_participant_identity": result.participant_identity,
            "ai_participant_identity": f"agent-{result.call_id}",
        }
    ]


@pytest.mark.anyio
async def test_livekit_sip_participant_left_auto_ends_session_and_stops_recording() -> None:
    (
        service,
        room_manager,
        agent_runner,
        _sip_client,
        record_service,
        _prompt_resolver,
    ) = build_service_with_sip_fakes()
    recording_service = FakeRecordingService()
    service.recording_service = recording_service

    result = await service.create_sip_session(
        tenant_id="000000",
        callee_phone_number="13800000000",
        voice=None,
        business_id="geo_task_001",
        scene_code="intro_geo",
        business_params={},
    )

    handled = await service.handle_livekit_webhook_event(
        event_type="participant_left",
        room_name=result.room_name,
        participant_identity=result.participant_identity,
        payload={"disconnectReason": "CLIENT_INITIATED"},
    )

    assert handled == {
        "handled": True,
        "action": "end_session",
        "callId": result.call_id,
        "endReason": "remote_hangup",
    }
    status = await service.get_session(result.call_id)
    assert status.status == CallSessionStatus.COMPLETED
    assert room_manager.deleted_rooms == [result.room_name]
    assert agent_runner.stopped_call_ids == [result.call_id]
    assert recording_service.stopped_call_ids == [result.call_id]
    assert record_service.completed_sessions == [
        {
            "call_id": result.call_id,
            "end_reason": "remote_hangup",
            "ended_at": None,
        }
    ]
    event_types = [
        event.type for event in service.orchestrator.event_store.list_all(result.call_id)
    ]
    assert "sip_hangup" in event_types
    assert event_types.index("sip_hangup") < event_types.index("session_completed")


@pytest.mark.anyio
async def test_livekit_sip_participant_left_ends_persisted_session_without_local_runtime() -> None:
    (
        service,
        room_manager,
        agent_runner,
        _sip_client,
        record_service,
        _prompt_resolver,
    ) = build_service_with_sip_fakes()
    recording_service = FakeRecordingService()
    service.recording_service = recording_service

    result = await service.create_sip_session(
        tenant_id="000000",
        callee_phone_number="13800000000",
        voice=None,
        business_id="geo_task_001",
        scene_code="intro_geo",
        business_params={},
    )
    service.orchestrator.registry._sessions.pop(result.call_id)

    handled = await service.handle_livekit_webhook_event(
        event_type="participant_left",
        room_name=result.room_name,
        participant_identity=result.participant_identity,
        payload={"disconnectReason": "CLIENT_INITIATED"},
    )

    assert handled == {
        "handled": True,
        "action": "end_persisted_session",
        "callId": result.call_id,
        "endReason": "remote_hangup",
    }
    assert room_manager.deleted_rooms == [result.room_name]
    assert agent_runner.stopped_call_ids == [result.call_id]
    assert recording_service.stopped_call_ids == [result.call_id]
    assert record_service.completed_sessions == [
        {
            "call_id": result.call_id,
            "end_reason": "remote_hangup",
            "ended_at": None,
        }
    ]
    event_types = [
        event.type for event in service.orchestrator.event_store.list_all(result.call_id)
    ]
    assert "sip_hangup" in event_types
    assert event_types.index("sip_hangup") < event_types.index("session_completed")


@pytest.mark.anyio
async def test_livekit_non_sip_participant_left_does_not_end_session() -> None:
    (
        service,
        room_manager,
        agent_runner,
        _sip_client,
        record_service,
        _prompt_resolver,
    ) = build_service_with_sip_fakes()

    result = await service.create_sip_session(
        callee_phone_number="13800000000",
        voice=None,
        business_id="geo_task_001",
        scene_code="intro_geo",
        business_params={},
    )

    handled = await service.handle_livekit_webhook_event(
        event_type="participant_left",
        room_name=result.room_name,
        participant_identity=f"human-agent-handoff_{result.call_id}",
        payload={},
    )

    assert handled == {
        "handled": False,
        "reason": "non_sip_participant",
    }
    status = await service.get_session(result.call_id)
    assert status.status == CallSessionStatus.CONNECTED
    assert room_manager.deleted_rooms == []
    assert agent_runner.stopped_call_ids == []
    assert record_service.completed_sessions == []
    event_types = [
        event.type for event in service.orchestrator.event_store.list_all(result.call_id)
    ]
    assert "sip_hangup" not in event_types
    assert "session_completed" not in event_types


def _livekit_webhook_auth(body: str) -> str:
    body_hash = base64.b64encode(hashlib.sha256(body.encode()).digest()).decode()
    return api.AccessToken("livekit-key", "livekit-secret").with_sha256(body_hash).to_jwt()


def test_livekit_webhook_controller_commits_before_legacy_queue(monkeypatch) -> None:
    monkeypatch.setattr(settings, "LIVEKIT_API_KEY", "livekit-key")
    monkeypatch.setattr(settings, "LIVEKIT_API_SECRET", "livekit-secret")
    scheduled: list[dict[str, object]] = []

    class FakeDb:
        committed = False

        async def commit(self) -> None:
            self.committed = True

    fake_db = FakeDb()

    class FakeIngressService:
        async def receive_livekit(self, **_kwargs):
            return SimpleNamespace(disposition="LEGACY", row_id=None, status=None)

    def fake_schedule_livekit_webhook_event(**kwargs):
        scheduled.append(kwargs)
        return {
            "queued": True,
            "eventType": kwargs["event_type"],
            "roomName": kwargs["room_name"],
            "participantIdentity": kwargs["participant_identity"],
        }

    monkeypatch.setattr(
        ai_call_controller,
        "schedule_livekit_webhook_event",
        fake_schedule_livekit_webhook_event,
        raising=False,
    )
    app = FastAPI()
    app.include_router(AiCallRouter)
    app.dependency_overrides[ai_call_controller.ai_call_db_getter] = lambda: fake_db
    app.dependency_overrides[
        ai_call_controller.get_runtime_webhook_ingress_service
    ] = lambda: FakeIngressService()

    with TestClient(app) as client:
        body = json.dumps(
            {
                "event": "participant_left",
                "room": {"name": "ai-call-call_queued"},
                "participant": {"identity": "sip-call_queued"},
                "id": "EV_test",
                "createdAt": 1760000000,
            },
            separators=(",", ":"),
        )
        response = client.post(
            "/ai-call/livekit-webhook",
            content=body,
            headers={"Authorization": f"Bearer {_livekit_webhook_auth(body)}"},
        )

    assert response.status_code == 200
    assert response.json()["data"] == {
        "queued": True,
        "eventType": "participant_left",
        "roomName": "ai-call-call_queued",
        "participantIdentity": "sip-call_queued",
    }
    assert scheduled
    assert fake_db.committed is True
    assert scheduled[0]["event_type"] == "participant_left"
    assert scheduled[0]["room_name"] == "ai-call-call_queued"
    assert scheduled[0]["participant_identity"] == "sip-call_queued"


def test_livekit_webhook_controller_preserves_audio_track_type(monkeypatch) -> None:
    monkeypatch.setattr(settings, "LIVEKIT_API_KEY", "livekit-key")
    monkeypatch.setattr(settings, "LIVEKIT_API_SECRET", "livekit-secret")
    scheduled: list[dict[str, object]] = []

    class FakeDb:
        committed = False

        async def commit(self) -> None:
            self.committed = True

    fake_db = FakeDb()

    class FakeIngressService:
        async def receive_livekit(self, **kwargs):
            scheduled.append(kwargs)
            return SimpleNamespace(disposition="INBOX", row_id=901, status="RECEIVED")

    def fake_schedule_livekit_webhook_event(**_kwargs):
        raise AssertionError("owner webhook must not use the legacy in-process queue")

    monkeypatch.setattr(
        ai_call_controller,
        "schedule_livekit_webhook_event",
        fake_schedule_livekit_webhook_event,
        raising=False,
    )
    app = FastAPI()
    app.include_router(AiCallRouter)
    app.dependency_overrides[ai_call_controller.ai_call_db_getter] = lambda: fake_db
    app.dependency_overrides[
        ai_call_controller.get_runtime_webhook_ingress_service
    ] = lambda: FakeIngressService()

    with TestClient(app) as client:
        body = json.dumps(
            {
                "event": "track_published",
                "room": {"name": "ai-call-call_audio"},
                "participant": {"identity": "sip-call_audio"},
                "track": {"sid": "TR_audio", "type": "AUDIO"},
                "id": "EV_audio",
                "createdAt": 1760000000,
            },
            separators=(",", ":"),
        )
        response = client.post(
            "/ai-call/livekit-webhook",
            content=body,
            headers={"Authorization": f"Bearer {_livekit_webhook_auth(body)}"},
        )

    assert response.status_code == 200
    assert response.json()["data"] == {
        "persisted": True,
        "disposition": "INBOX",
        "rowId": "901",
        "status": "RECEIVED",
    }
    assert fake_db.committed is True
    assert scheduled[0]["payload"]["track"] == {
        "sid": "TR_audio",
        "type": "AUDIO",
    }


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
    assert service.orchestrator.livekit_room_manager.deleted_rooms == [
        f"ai-call-{failed['call_id']}"
    ]

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
        "calleePhoneNumberHash": _callee_hash("13800000000"),
        "rawErrorType": "TwirpError",
        "rawErrorMessage": "callee already has an active SIP call",
    }


@pytest.mark.anyio
async def test_create_sip_session_cleans_room_when_answer_status_is_uncertain() -> None:
    (
        service,
        room_manager,
        agent_runner,
        _sip_client,
        record_service,
        _prompt_resolver,
    ) = build_service_with_sip_fakes()
    service.sip_client = FakeSipClient(sip_call_status="ringing")

    with pytest.raises(CustomException) as exc_info:
        await service.create_sip_session(
            callee_phone_number="13800000000",
            voice=None,
            business_id="geo_task_001",
            scene_code="intro_geo",
            business_params={},
            ringing_timeout_seconds=30,
        )

    assert exc_info.value.msg == "SIP Participant 未返回已接听状态"
    assert len(record_service.failed_sessions) == 1
    call_id = str(record_service.failed_sessions[0]["call_id"])
    assert room_manager.deleted_rooms == [f"ai-call-{call_id}"]
    assert agent_runner.stopped_call_ids == [call_id]


@pytest.mark.anyio
async def test_create_sip_session_keeps_record_pending_when_cleanup_is_incomplete() -> None:
    (
        service,
        room_manager,
        agent_runner,
        _sip_client,
        record_service,
        _prompt_resolver,
    ) = build_service_with_sip_fakes()
    service.sip_client = FakeSipClient(sip_call_status="ringing")
    agent_runner.stop_error = RuntimeError("agent stop failed")

    with pytest.raises(CustomException) as exc_info:
        await service.create_sip_session(
            callee_phone_number="13800000000",
            voice=None,
            business_id="geo_task_001",
            scene_code="intro_geo",
            business_params={},
            ringing_timeout_seconds=30,
        )

    assert exc_info.value.msg.endswith("SIP 资源清理失败，保持待对账")
    assert room_manager.deleted_rooms
    assert agent_runner.stopped_call_ids
    assert record_service.failed_sessions == []


@pytest.mark.anyio
async def test_create_sip_session_controller_commits_failed_record_before_error_response() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with session_maker() as db:
            with pytest.raises(CustomException):
                async with db.begin():
                    room_manager = FakeLiveKitRoomManager()
                    agent_runner = CapturingAgentRunner()
                    orchestrator = AiCallOrchestrator(
                        config=build_runtime_config(),
                        livekit_room_manager=room_manager,
                        agent_runner=agent_runner,
                        registry=InMemorySessionRegistry(),
                        event_store=InMemoryEventStore(),
                    )
                    record_service = AiCallRecordService(AiCallRecordRepository(db))
                    service = AiCallService(
                        orchestrator,
                        record_service=record_service,
                        sip_client=FakeSipClient(
                            error=AiCallError(
                                error_id="sip_create_participant_failed",
                                msg="LiveKit SIP Participant 创建失败",
                                status_code=502,
                            )
                        ),
                        prompt_resolver=FakePromptResolver(),
                        prompt_composer=PromptComposer(handoff_component_enabled=True),
                    )
                    await ai_call_controller.create_sip_session_controller(
                        service=service,
                        request=CreateSipSessionRequest(
                            calleePhoneNumber="13800000000",
                            sceneCode="intro_geo",
                            ringingTimeoutSeconds=30,
                        ),
                    )

        async with session_maker() as verify_db:
            verify_service = AiCallRecordService(AiCallRecordRepository(verify_db))
            rows, total = await verify_service.list_records(entry_type="sip_outbound")

        assert total == 1
        record = rows[0]
        assert record.status == CallSessionStatus.FAILED.value
        assert record.end_reason == "sip_create_participant_failed"
        assert record.failure_stage == "sip"
        assert record.failure_message == "LiveKit SIP Participant 创建失败"
        assert record.callee_phone_number_hash == _callee_hash("13800000000")
        assert record.callee_phone_number_masked == "138****0000"
    finally:
        async with engine.begin() as conn:
            await conn.run_sync(MappedBase.metadata.drop_all)
        await engine.dispose()


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
        "calleePhoneNumberHash": _callee_hash("13800000000"),
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
    assert "call_id" not in CreateSipSessionRequest.model_fields

    with TestClient(app) as client:
        rejected = client.post(
            "/ai-call/sip-sessions",
            json={
                "calleePhoneNumber": "13800000000",
                "callId": "call-external-1",
                "sceneCode": "intro_geo",
            },
        )

    assert rejected.status_code == 422
    assert len(sip_client.created) == 1


def test_build_sip_client_accepts_request_level_config() -> None:
    config = SipOutboundConfig(
        enabled=True,
        trunk_hostname="127.0.0.1:5089",
        caller_number="1000",
    )

    client = ai_call_service_module._build_sip_client(config=config)

    assert client.config is config


def test_build_sip_client_keeps_settings_default(monkeypatch) -> None:
    monkeypatch.setattr(settings, "AI_CALL_SIP_OUTBOUND_ENABLED", True)
    monkeypatch.setattr(
        settings,
        "LIVEKIT_SIP_OUTBOUND_TRUNK_HOSTNAME",
        "sip.default.test:5060",
    )

    client = ai_call_service_module._build_sip_client()

    assert client.config.enabled is True
    assert client.config.trunk_hostname == "sip.default.test:5060"
