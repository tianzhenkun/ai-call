from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.services.ai_call.runtime_control.customer_track import customer_track_keys
from app.services.ai_call.runtime_control.dialogue_bridge import (
    OwnerDialogueFinalizeResult,
)
from app.services.ai_call.runtime_control.dialogue_repository import OwnerDialogueFence
from app.services.ai_call.runtime_control.effect_repository import (
    EffectClaim,
    ProviderObservationKind,
)
from app.services.ai_call.runtime_control.runtime_service import RuntimeRegistry
from app.services.ai_call.session_registry import InMemorySessionRegistry


def _effect(effect_type: str, *, reconcile_only: bool = False) -> EffectClaim:
    return EffectClaim(
        effect_id=101,
        tenant_id="tenant-a",
        call_id="call-1",
        effect_type=effect_type,
        processing_owner_id="runtime-1",
        processing_fencing_token=7,
        processing_token="effect-token",
        processing_expires_at=datetime(2026, 8, 2, 6, tzinfo=timezone.utc)
        + timedelta(minutes=1),
        source_create_effect_id=None,
        create_protection_deadline_at=None,
        attempt_count=1,
        reconcile_only=reconcile_only,
        provider_namespace="livekit:test",
        resource_key=f"{effect_type.lower()}:call-1:g7",
        reservation_token="reservation-1",
    )


def _customer_track_effect(
    effect_type: str,
    *,
    reconcile_only: bool = False,
) -> EffectClaim:
    _, _, resource_key = customer_track_keys("call-1", "sip-call-1")
    return replace(
        _effect(effect_type, reconcile_only=reconcile_only),
        effect_id=202,
        resource_key=resource_key,
        source_create_effect_id=101 if effect_type == "STOP_TRACK_EGRESS" else None,
    )


class FakeResolver:
    async def resolve(self, _effect):
        from app.services.ai_call.runtime_control.livekit_provider import (
            RuntimeProviderResource,
        )

        return RuntimeProviderResource(
            call_id="call-1",
            room_name="ai-call-call-1",
            customer_participant_identity="sip-call-1",
            agent_participant_identity="ai-agent-call-1-g7",
            callee_phone_number="19900001001",
            egress_id="EG_1",
            voice="Cherry",
        )


class FakeRoomManager:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.rooms = {"ai-call-call-1"}
        self.participants = {"sip-call-1", "ai-agent-call-1-g7"}

    async def create_room(self, room_name: str) -> None:
        self.calls.append(("create_room", room_name))
        self.rooms.add(room_name)

    async def room_exists(self, room_name: str) -> bool:
        self.calls.append(("room_exists", room_name))
        return room_name in self.rooms

    async def delete_room(self, room_name: str) -> None:
        self.calls.append(("delete_room", room_name))
        self.rooms.discard(room_name)

    async def remove_participant(self, room_name: str, identity: str) -> None:
        self.calls.append(("remove_participant", f"{room_name}|{identity}"))
        self.participants.discard(identity)

    async def participant_exists(self, room_name: str, identity: str) -> bool:
        self.calls.append(("participant_exists", f"{room_name}|{identity}"))
        return identity in self.participants

    async def get_participant_media(self, room_name: str, identity: str):
        self.calls.append(("get_participant_media", f"{room_name}|{identity}"))
        if identity not in self.participants:
            return None
        return SimpleNamespace(
            participant_identity=identity,
            participant_sid="PA_1",
            track_sid="TR_1",
            microphone_ready=True,
        )


class FakeAgentManager:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.active: set[str] = set()

    async def start(self, resource) -> str:
        self.calls.append(("start", resource.call_id))
        self.active.add(resource.call_id)
        return f"agent:{resource.call_id}"

    async def exists(self, call_id: str) -> bool:
        self.calls.append(("exists", call_id))
        return call_id in self.active

    async def stop(self, call_id: str) -> None:
        self.calls.append(("stop", call_id))
        self.active.discard(call_id)


class FakeSipClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def create_participant(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(sip_call_id="SIP_1")


class FakeEgressManager:
    def __init__(self, *, status: str = "EGRESS_COMPLETE") -> None:
        self.status = status
        self.calls: list[str] = []

    async def stop_egress(self, egress_id: str):
        self.calls.append(egress_id)
        return SimpleNamespace(egress_id=egress_id, status=self.status)

    async def get_egress_status(self, egress_id: str) -> str | None:
        self.calls.append(f"query:{egress_id}")
        return self.status

    async def get_egress(self, egress_id: str):
        self.calls.append(f"query:{egress_id}")
        return SimpleNamespace(
            egress_id=egress_id,
            status=self.status,
            object_name=None,
            started_at=None,
            ended_at=None,
            duration_ms=None,
            file_size=None,
        )

    def build_object_name(self, call_id: str) -> str:
        return f"ai-call/recordings/{call_id}.ogg"

    async def find_room_audio_recording(self, room_name: str, object_name: str):
        _ = (room_name, object_name)
        return None


class RecordingEgressManager:
    def __init__(self) -> None:
        self.start_calls: list[dict[str, object]] = []
        self.participant_start_calls: list[dict[str, object]] = []
        self.stop_calls: list[str] = []
        self.query_calls: list[str] = []
        self.find_calls: list[tuple[str, str]] = []
        self.start_error: Exception | None = None
        self.query_observation = None
        self.find_observation = None
        self.stop_result = SimpleNamespace(egress_id="EG_main", status="EGRESS_ACTIVE")
        self.participant_start_error: Exception | None = None

    def build_object_name(self, call_id: str) -> str:
        return f"ai-call/recordings/{call_id}.ogg"

    async def start_room_audio_recording(self, **kwargs):
        self.start_calls.append(kwargs)
        if self.start_error is not None:
            raise self.start_error
        return SimpleNamespace(
            egress_id="EG_main",
            object_name=self.build_object_name(str(kwargs["call_id"])),
            status="EGRESS_ACTIVE",
            started_at=datetime(2026, 8, 3, 9, tzinfo=timezone.utc),
        )

    def build_participant_object_name(
        self,
        *,
        call_id: str,
        track_role: str,
        participant_identity: str,
    ) -> str:
        return f"ai-call/recordings/tracks/{call_id}/{track_role}-{participant_identity}.ogg"

    async def start_participant_audio_recording(self, **kwargs):
        self.participant_start_calls.append(kwargs)
        if self.participant_start_error is not None:
            raise self.participant_start_error
        return SimpleNamespace(
            egress_id="EG_customer",
            object_name=self.build_participant_object_name(
                call_id=str(kwargs["call_id"]),
                track_role=str(kwargs["track_role"]),
                participant_identity=str(kwargs["participant_identity"]),
            ),
            status="EGRESS_ACTIVE",
            started_at=datetime(2026, 8, 3, 9, tzinfo=timezone.utc),
        )

    async def stop_egress(self, egress_id: str):
        self.stop_calls.append(egress_id)
        return self.stop_result

    async def get_egress(self, egress_id: str):
        self.query_calls.append(egress_id)
        return self.query_observation

    async def find_room_audio_recording(self, room_name: str, object_name: str):
        self.find_calls.append((room_name, object_name))
        return self.find_observation


def _provider(
    *,
    room_manager=None,
    agent_manager=None,
    sip_client=None,
    egress_manager=None,
    oss_config_provider=None,
):
    from app.services.ai_call.runtime_control.livekit_provider import (
        LiveKitRuntimeProvider,
    )

    return LiveKitRuntimeProvider(
        resolver=FakeResolver(),
        room_manager=room_manager or FakeRoomManager(),
        agent_manager=agent_manager or FakeAgentManager(),
        sip_client=sip_client or FakeSipClient(),
        egress_manager=egress_manager or FakeEgressManager(),
        oss_config_provider=oss_config_provider,
    )


def test_livekit_owner_provider_routes_final_customer_speech_to_handoff_worker() -> None:
    from app.config.setting import settings
    from app.services.ai_call.runtime_control.livekit_provider import (
        build_livekit_runtime_provider,
    )

    runtime_settings = settings.model_copy(
        update={"AI_CALL_HANDOFF_AUTO_TRIGGER_ENABLED": True}
    )
    provider = build_livekit_runtime_provider(
        settings=runtime_settings,
        session_factory=object(),
        registry=RuntimeRegistry(),
    )
    worker = provider._handoff_trigger_worker
    assert worker is not None
    owner_orchestrator = provider._agent_manager._orchestrator
    handoff_service = worker.trigger_service.service_factory(object())
    assert handoff_service.orchestrator is owner_orchestrator
    assert handoff_service.handoff_exception_manager.orchestrator is owner_orchestrator

    owner_orchestrator.event_store.append(
        "call-owner",
        "user_transcript_done",
        "provider",
        {"transcript": "转人工"},
    )

    assert worker.queue.qsize() == 1
    worker.detach_all()


def test_livekit_owner_provider_attaches_event_persistence_worker() -> None:
    from app.api.v1.ai_call.service import configure_ai_call_event_persistence
    from app.config.setting import settings
    from app.services.ai_call.runtime_control.livekit_provider import (
        build_livekit_runtime_provider,
    )

    class EventWorker:
        def __init__(self) -> None:
            self.stores = []

        def attach_event_store(self, event_store) -> None:
            self.stores.append(event_store)

    worker = EventWorker()
    configure_ai_call_event_persistence(worker)
    try:
        provider = build_livekit_runtime_provider(
            settings=settings,
            session_factory=object(),
            registry=RuntimeRegistry(),
        )
    finally:
        configure_ai_call_event_persistence(None)

    assert provider._agent_manager._orchestrator.event_store in worker.stores


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("effect_type", "expected_kind"),
    (
        ("CREATE_ROOM", ProviderObservationKind.RESOURCE_PRESENT),
        ("ATTACH_AGENT_PARTICIPANT", ProviderObservationKind.RESOURCE_PRESENT),
        ("CREATE_SIP_PARTICIPANT", ProviderObservationKind.RESOURCE_PRESENT),
        ("HANGUP_SIP", ProviderObservationKind.TERMINAL_CONFIRMED),
        (
            "DISCONNECT_AGENT_PARTICIPANT",
            ProviderObservationKind.TERMINAL_CONFIRMED,
        ),
        ("STOP_EGRESS", ProviderObservationKind.TERMINAL_CONFIRMED),
        ("DELETE_ROOM", ProviderObservationKind.TERMINAL_CONFIRMED),
    ),
)
async def test_livekit_provider_maps_effect_to_mutation_then_fact_query(
    effect_type: str,
    expected_kind: ProviderObservationKind,
) -> None:
    provider = _provider()

    observation = await provider.apply(_effect(effect_type))

    assert observation.kind == expected_kind


@pytest.mark.anyio
async def test_livekit_provider_timeout_after_call_is_uncertain_not_applied() -> None:
    class TimeoutRoomManager(FakeRoomManager):
        async def create_room(self, room_name: str) -> None:
            self.calls.append(("create_room", room_name))
            raise TimeoutError("result unknown")

    observation = await _provider(
        room_manager=TimeoutRoomManager()
    ).apply(_effect("CREATE_ROOM"))

    assert observation.kind == ProviderObservationKind.UNCERTAIN
    assert "TimeoutError" in (observation.error_message or "")


@pytest.mark.anyio
async def test_livekit_provider_rejects_callee_outside_single_number_allowlist() -> None:
    from app.services.ai_call.runtime_control.livekit_provider import (
        LiveKitRuntimeProvider,
    )

    sip_client = FakeSipClient()
    provider = LiveKitRuntimeProvider(
        resolver=FakeResolver(),
        room_manager=FakeRoomManager(),
        agent_manager=FakeAgentManager(),
        sip_client=sip_client,
        egress_manager=FakeEgressManager(),
        allowed_callee_phone_number="19900001002",
    )

    observation = await provider.apply(_effect("CREATE_SIP_PARTICIPANT"))

    assert observation.kind == ProviderObservationKind.PERMANENT_NO_RESOURCE
    assert observation.error_message == "callee_not_allowed"
    assert sip_client.calls == []


@pytest.mark.anyio
async def test_livekit_provider_missing_callee_is_permanent_no_resource() -> None:
    class MissingCalleeResolver(FakeResolver):
        async def resolve(self, effect):
            resource = await super().resolve(effect)
            return replace(resource, callee_phone_number=None)

    from app.services.ai_call.runtime_control.livekit_provider import (
        LiveKitRuntimeProvider,
    )

    sip_client = FakeSipClient()
    provider = LiveKitRuntimeProvider(
        resolver=MissingCalleeResolver(),
        room_manager=FakeRoomManager(),
        agent_manager=FakeAgentManager(),
        sip_client=sip_client,
        egress_manager=FakeEgressManager(),
    )

    observation = await provider.apply(_effect("CREATE_SIP_PARTICIPANT"))

    assert observation.kind == ProviderObservationKind.PERMANENT_NO_RESOURCE
    assert observation.error_message == "callee_phone_number_missing"
    assert sip_client.calls == []


@pytest.mark.anyio
async def test_livekit_provider_passes_selected_line_config_to_sip_client() -> None:
    from app.services.ai_call.livekit_sip import SipOutboundConfig
    from app.services.ai_call.runtime_control.livekit_provider import (
        LiveKitRuntimeProvider,
    )

    selected_config = SipOutboundConfig(
        enabled=True,
        allowed_callee_prefixes="199",
        trunk_hostname="47.94.86.132:5089",
        caller_number="037123124845",
    )

    class SelectedLineResolver(FakeResolver):
        async def resolve(self, effect):
            resource = await super().resolve(effect)
            return replace(resource, sip_config=selected_config)

    sip_client = FakeSipClient()
    provider = LiveKitRuntimeProvider(
        resolver=SelectedLineResolver(),
        room_manager=FakeRoomManager(),
        agent_manager=FakeAgentManager(),
        sip_client=sip_client,
        egress_manager=FakeEgressManager(),
    )

    observation = await provider.apply(_effect("CREATE_SIP_PARTICIPANT"))

    assert observation.kind == ProviderObservationKind.RESOURCE_PRESENT
    assert sip_client.calls[0]["config"] is selected_config


@pytest.mark.anyio
async def test_resource_resolver_builds_selected_line_config_for_outbound_sip() -> None:
    from app.api.v1.ai_call.model import AiCallRecordModel
    from app.config.setting import Settings
    from app.services.ai_call.runtime_control.livekit_provider import (
        DatabaseRuntimeProviderResourceResolver,
        ProviderPreconditionError,
    )
    from app.services.ai_call.runtime_control.models import (
        AiCallRuntimeCommandModel,
        AiCallRuntimeEffectModel,
    )

    command_payload = {
        "line_code": "provider-a",
        "line_id": "340700000000000001",
        "sip_line": {
            "lineId": "340700000000000001",
            "lineCode": "provider-a",
            "lineName": "供应商 A",
            "adapterType": "livekit_sip",
            "routeMode": "inline_hostname",
            "trunkId": None,
            "proxyHost": "47.94.86.132",
            "proxyPort": 5089,
            "authMode": "ip_allowlist",
            "callerNumber": "037123124845",
            "destinationCountry": "CN",
            "maxConcurrency": 1,
            "originateTimeoutSeconds": 45,
        },
    }
    rows = {
        AiCallRecordModel: SimpleNamespace(
            tenant_id="tenant-a",
            call_id="call-1",
            room_name="ai-call-call-1",
            participant_identity="sip-call-1",
            callee_phone_number="19900001001",
            business_type="outbound_attempt",
        ),
        AiCallRuntimeCommandModel: SimpleNamespace(
            payload_json=json.dumps(command_payload)
        ),
        AiCallRuntimeEffectModel: SimpleNamespace(
            id=101,
            tenant_id="tenant-a",
            call_id="call-1",
            command_id=77,
            effect_type="CREATE_SIP_PARTICIPANT",
            provider_namespace="livekit:test",
            resource_generation=5,
            provider_reference=None,
        ),
    }

    class FakeSession:
        async def scalar(self, statement):
            return rows.get(statement.column_descriptions[0]["entity"])

    class SessionContext:
        async def __aenter__(self):
            return FakeSession()

        async def __aexit__(self, *_args):
            return None

    resolver = DatabaseRuntimeProviderResourceResolver(
        lambda: SessionContext(),
        sip_settings=Settings(
            AI_CALL_SIP_OUTBOUND_ENABLED=True,
            AI_CALL_SIP_ALLOWED_CALLEE_PREFIXES="199",
            SIP_PUBLIC_IP="118.25.125.221",
        ),
    )

    resource = await resolver.resolve(_effect("CREATE_SIP_PARTICIPANT"))

    assert resource.sip_config is not None
    assert resource.sip_config.trunk_hostname == "47.94.86.132:5089"
    assert resource.sip_config.caller_number == "037123124845"
    assert resource.sip_config.allowed_callee_prefixes == "199"

    rows[AiCallRuntimeCommandModel].payload_json = "{}"
    with pytest.raises(ProviderPreconditionError, match="sip_line_snapshot_missing"):
        await resolver.resolve(_effect("CREATE_SIP_PARTICIPANT"))


@pytest.mark.anyio
async def test_livekit_provider_missing_callee_reconcile_closes_absent_resource() -> None:
    class MissingCalleeResolver(FakeResolver):
        async def resolve(self, effect):
            resource = await super().resolve(effect)
            return replace(resource, callee_phone_number=None)

    from app.services.ai_call.runtime_control.livekit_provider import (
        LiveKitRuntimeProvider,
    )

    room_manager = FakeRoomManager()
    room_manager.participants.discard("sip-call-1")
    sip_client = FakeSipClient()
    provider = LiveKitRuntimeProvider(
        resolver=MissingCalleeResolver(),
        room_manager=room_manager,
        agent_manager=FakeAgentManager(),
        sip_client=sip_client,
        egress_manager=FakeEgressManager(),
    )

    observation = await provider.apply(
        _effect("CREATE_SIP_PARTICIPANT", reconcile_only=True)
    )

    assert observation.kind == ProviderObservationKind.PERMANENT_NO_RESOURCE
    assert observation.error_message == "callee_phone_number_missing"
    assert sip_client.calls == []


@pytest.mark.anyio
async def test_livekit_provider_reconcile_only_queries_without_repeating_mutation() -> None:
    room_manager = FakeRoomManager()
    provider = _provider(room_manager=room_manager)

    observation = await provider.apply(_effect("DELETE_ROOM", reconcile_only=True))

    assert observation.kind == ProviderObservationKind.ACCEPTED
    assert room_manager.calls == [("room_exists", "ai-call-call-1")]


@pytest.mark.anyio
async def test_livekit_provider_reconcile_only_reports_missing_create_resource() -> None:
    room_manager = FakeRoomManager()
    room_manager.rooms.clear()

    observation = await _provider(room_manager=room_manager).apply(
        _effect("CREATE_ROOM", reconcile_only=True)
    )

    assert observation.kind == ProviderObservationKind.RESOURCE_ABSENT


@pytest.mark.anyio
async def test_livekit_provider_missing_egress_reference_queries_stable_key() -> None:
    class MissingEgressResolver(FakeResolver):
        async def resolve(self, effect):
            resource = await super().resolve(effect)
            return replace(resource, egress_id=None)

    from app.services.ai_call.runtime_control.livekit_provider import (
        LiveKitRuntimeProvider,
    )

    provider = LiveKitRuntimeProvider(
        resolver=MissingEgressResolver(),
        room_manager=FakeRoomManager(),
        agent_manager=FakeAgentManager(),
        sip_client=FakeSipClient(),
        egress_manager=FakeEgressManager(),
    )

    observation = await provider.apply(_effect("STOP_EGRESS"))

    assert observation.kind == ProviderObservationKind.RESOURCE_ABSENT


@pytest.mark.anyio
async def test_livekit_provider_maps_egress_404_to_terminal_confirmation() -> None:
    from app.services.ai_call.livekit_egress import LiveKitEgressNotFoundError

    class MissingEgressManager(FakeEgressManager):
        async def stop_egress(self, egress_id: str):
            raise LiveKitEgressNotFoundError(egress_id)

    observation = await _provider(
        egress_manager=MissingEgressManager()
    ).apply(_effect("STOP_EGRESS"))

    assert observation.kind == ProviderObservationKind.TERMINAL_CONFIRMED


@pytest.mark.anyio
@pytest.mark.parametrize("effect_type", ("STOP_EGRESS", "STOP_TRACK_EGRESS"))
async def test_livekit_provider_maps_already_completed_stop_to_terminal_confirmation(
    effect_type: str,
) -> None:
    from app.services.ai_call.livekit_egress import (
        LiveKitEgressAlreadyCompleteError,
    )

    class AlreadyCompletedEgressManager(RecordingEgressManager):
        def __init__(self) -> None:
            super().__init__()
            self.query_observation = SimpleNamespace(
                egress_id="EG_done",
                status="EGRESS_COMPLETE",
                object_name=(
                    "ai-call/recordings/call-1.ogg"
                    if effect_type == "STOP_EGRESS"
                    else "ai-call/recordings/tracks/call-1/customer-sip-call-1.ogg"
                ),
                started_at=None,
                ended_at=datetime(2026, 8, 3, 9, 1, tzinfo=timezone.utc),
                duration_ms=60_000,
                file_size=1024,
            )

        async def stop_egress(self, egress_id: str):
            self.stop_calls.append(egress_id)
            raise LiveKitEgressAlreadyCompleteError(egress_id)

    manager = AlreadyCompletedEgressManager()
    effect = (
        _effect(effect_type)
        if effect_type == "STOP_EGRESS"
        else _customer_track_effect(effect_type)
    )

    observation = await _provider(egress_manager=manager).apply(effect)

    assert observation.kind is ProviderObservationKind.TERMINAL_CONFIRMED
    assert observation.provider_reference == "EG_done"
    assert observation.duration_ms == 60_000
    assert manager.stop_calls == ["EG_1"]
    assert manager.query_calls == ["EG_1"]


@pytest.mark.anyio
async def test_livekit_egress_stop_maps_completed_412_to_typed_error(
    monkeypatch,
) -> None:
    import httpx

    from app.services.ai_call.livekit_egress import (
        LiveKitEgressAlreadyCompleteError,
        LiveKitEgressManager,
    )

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            _ = (args, kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args) -> None:
            _ = args

        async def post(self, url, *, headers, json):
            _ = (url, headers, json)
            return httpx.Response(
                412,
                request=httpx.Request("POST", "http://livekit.test"),
                json={
                    "code": "failed_precondition",
                    "msg": "egress with status EGRESS_COMPLETE cannot be stopped",
                },
            )

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)
    manager = LiveKitEgressManager(
        livekit_url="ws://livekit.test",
        api_key="key",
        api_secret="secret",
        timeout_seconds=1,
        object_prefix="ai-call/recordings",
    )

    with pytest.raises(LiveKitEgressAlreadyCompleteError) as exc_info:
        await manager.stop_egress("EG_done")

    assert exc_info.value.egress_id == "EG_done"


@pytest.mark.anyio
async def test_livekit_provider_starts_main_egress_with_active_oss_config() -> None:
    from app.services.ai_call.runtime_control.livekit_provider import (
        LiveKitRuntimeProvider,
    )

    manager = RecordingEgressManager()
    manager.query_observation = SimpleNamespace(
        egress_id="EG_main",
        status="EGRESS_ACTIVE",
        object_name="ai-call/recordings/call-1.ogg",
        started_at=datetime(2026, 8, 3, 9, tzinfo=timezone.utc),
        ended_at=None,
        duration_ms=None,
        file_size=None,
    )
    provider = LiveKitRuntimeProvider(
        resolver=FakeResolver(),
        room_manager=FakeRoomManager(),
        agent_manager=FakeAgentManager(),
        sip_client=FakeSipClient(),
        egress_manager=manager,
        main_recording_enabled=True,
        oss_config_provider=lambda: {"bucket_name": "recordings"},
    )

    observation = await provider.apply(_effect("START_EGRESS"))

    assert observation.kind is ProviderObservationKind.RESOURCE_PRESENT
    assert observation.provider_reference == "EG_main"
    assert observation.object_name == "ai-call/recordings/call-1.ogg"
    assert observation.provider_status == "EGRESS_ACTIVE"
    assert manager.start_calls == [
        {
            "room_name": "ai-call-call-1",
            "call_id": "call-1",
            "oss_config": {"bucket_name": "recordings"},
        }
    ]
    assert manager.query_calls == ["EG_main"]


@pytest.mark.anyio
async def test_livekit_provider_starts_customer_track_with_participant_identity() -> None:
    manager = RecordingEgressManager()
    manager.query_observation = SimpleNamespace(
        egress_id="EG_customer",
        status="EGRESS_ACTIVE",
        object_name="ai-call/recordings/tracks/call-1/customer-sip-call-1.ogg",
        started_at=datetime(2026, 8, 3, 9, tzinfo=timezone.utc),
        ended_at=None,
        duration_ms=None,
        file_size=None,
    )
    provider = _provider(
        egress_manager=manager,
        oss_config_provider=lambda: {"bucket_name": "recordings"},
    )

    observation = await provider.apply(
        _customer_track_effect("START_TRACK_EGRESS")
    )

    assert observation.kind is ProviderObservationKind.RESOURCE_PRESENT
    assert observation.provider_reference == "EG_customer"
    assert manager.participant_start_calls == [
        {
            "room_name": "ai-call-call-1",
            "call_id": "call-1",
            "track_role": "customer",
            "participant_identity": "sip-call-1",
            "oss_config": {"bucket_name": "recordings"},
        }
    ]
    assert manager.start_calls == []
    assert manager.query_calls == ["EG_customer"]


@pytest.mark.anyio
async def test_livekit_provider_does_not_start_customer_track_without_oss_config() -> None:
    manager = RecordingEgressManager()
    provider = _provider(egress_manager=manager)

    observation = await provider.apply(
        _customer_track_effect("START_TRACK_EGRESS")
    )

    assert observation.kind is ProviderObservationKind.PERMANENT_NO_RESOURCE
    assert observation.failure_code == "oss_config_missing"
    assert manager.participant_start_calls == []


@pytest.mark.anyio
async def test_livekit_provider_customer_track_timeout_recovery_never_restarts() -> None:
    class MissingEgressResolver(FakeResolver):
        async def resolve(self, effect):
            return replace(await super().resolve(effect), egress_id=None)

    manager = RecordingEgressManager()
    manager.participant_start_error = TimeoutError("response lost")
    manager.find_observation = SimpleNamespace(
        egress_id="EG_late_customer",
        status="EGRESS_ACTIVE",
        object_name="ai-call/recordings/tracks/call-1/customer-sip-call-1.ogg",
        started_at=datetime(2026, 8, 3, 9, tzinfo=timezone.utc),
        ended_at=None,
        duration_ms=None,
        file_size=None,
    )
    from app.services.ai_call.runtime_control.livekit_provider import (
        LiveKitRuntimeProvider,
    )

    provider = LiveKitRuntimeProvider(
        resolver=MissingEgressResolver(),
        room_manager=FakeRoomManager(),
        agent_manager=FakeAgentManager(),
        sip_client=FakeSipClient(),
        egress_manager=manager,
        oss_config_provider=lambda: {"bucket_name": "recordings"},
    )

    first = await provider.apply(_customer_track_effect("START_TRACK_EGRESS"))
    recovered = await provider.apply(
        _customer_track_effect("START_TRACK_EGRESS", reconcile_only=True)
    )

    assert first.kind is ProviderObservationKind.UNCERTAIN
    assert first.failure_code == "egress_start_timeout"
    assert recovered.kind is ProviderObservationKind.RESOURCE_PRESENT
    assert recovered.provider_reference == "EG_late_customer"
    assert len(manager.participant_start_calls) == 1
    assert manager.find_calls == [
        (
            "ai-call-call-1",
            "ai-call/recordings/tracks/call-1/customer-sip-call-1.ogg",
        )
    ]


@pytest.mark.anyio
async def test_livekit_provider_customer_track_stop_without_reference_finds_then_stops() -> None:
    class MissingEgressResolver(FakeResolver):
        async def resolve(self, effect):
            return replace(await super().resolve(effect), egress_id=None)

    manager = RecordingEgressManager()
    manager.find_observation = SimpleNamespace(
        egress_id="EG_customer",
        status="EGRESS_ACTIVE",
        object_name="ai-call/recordings/tracks/call-1/customer-sip-call-1.ogg",
        started_at=None,
        ended_at=None,
        duration_ms=None,
        file_size=None,
    )
    from app.services.ai_call.runtime_control.livekit_provider import (
        LiveKitRuntimeProvider,
    )

    provider = LiveKitRuntimeProvider(
        resolver=MissingEgressResolver(),
        room_manager=FakeRoomManager(),
        agent_manager=FakeAgentManager(),
        sip_client=FakeSipClient(),
        egress_manager=manager,
    )

    observation = await provider.apply(
        _customer_track_effect("STOP_TRACK_EGRESS")
    )

    assert observation.kind is ProviderObservationKind.ACCEPTED
    assert manager.find_calls == [
        (
            "ai-call-call-1",
            "ai-call/recordings/tracks/call-1/customer-sip-call-1.ogg",
        )
    ]
    assert manager.stop_calls == ["EG_customer"]


@pytest.mark.anyio
async def test_livekit_provider_customer_track_stop_terminal_is_confirmed() -> None:
    manager = RecordingEgressManager()
    manager.stop_result = SimpleNamespace(
        egress_id="EG_customer", status="EGRESS_COMPLETE"
    )
    manager.query_observation = SimpleNamespace(
        egress_id="EG_customer",
        status="EGRESS_COMPLETE",
        object_name="ai-call/recordings/tracks/call-1/customer-sip-call-1.ogg",
        started_at=None,
        ended_at=datetime(2026, 8, 3, 9, 1, tzinfo=timezone.utc),
        duration_ms=60_000,
        file_size=1024,
    )

    observation = await _provider(egress_manager=manager).apply(
        _customer_track_effect("STOP_TRACK_EGRESS")
    )

    assert observation.kind is ProviderObservationKind.TERMINAL_CONFIRMED
    assert observation.object_name == (
        "ai-call/recordings/tracks/call-1/customer-sip-call-1.ogg"
    )
    assert observation.duration_ms == 60_000


@pytest.mark.anyio
async def test_livekit_provider_does_not_start_egress_without_oss_config() -> None:
    from app.services.ai_call.runtime_control.livekit_provider import (
        LiveKitRuntimeProvider,
    )

    manager = RecordingEgressManager()
    provider = LiveKitRuntimeProvider(
        resolver=FakeResolver(),
        room_manager=FakeRoomManager(),
        agent_manager=FakeAgentManager(),
        sip_client=FakeSipClient(),
        egress_manager=manager,
        main_recording_enabled=True,
        oss_config_provider=lambda: None,
    )

    observation = await provider.apply(_effect("START_EGRESS"))

    assert observation.kind is ProviderObservationKind.PERMANENT_NO_RESOURCE
    assert observation.failure_code == "oss_config_missing"
    assert manager.start_calls == []


@pytest.mark.anyio
async def test_livekit_provider_start_timeout_recovery_never_restarts_egress() -> None:
    from app.services.ai_call.runtime_control.livekit_provider import (
        LiveKitRuntimeProvider,
    )

    manager = RecordingEgressManager()
    manager.start_error = TimeoutError("response lost")
    provider = LiveKitRuntimeProvider(
        resolver=FakeResolver(),
        room_manager=FakeRoomManager(),
        agent_manager=FakeAgentManager(),
        sip_client=FakeSipClient(),
        egress_manager=manager,
        main_recording_enabled=True,
        oss_config_provider=lambda: {"bucket_name": "recordings"},
    )

    first = await provider.apply(_effect("START_EGRESS"))
    second = await provider.apply(_effect("START_EGRESS", reconcile_only=True))
    third = await provider.apply(_effect("START_EGRESS", reconcile_only=True))

    assert first.kind is ProviderObservationKind.UNCERTAIN
    assert first.failure_code == "egress_start_timeout"
    assert first.error_message == "egress_start_timeout"
    assert second.kind is ProviderObservationKind.RESOURCE_ABSENT
    assert third.kind is ProviderObservationKind.RESOURCE_ABSENT
    assert len(manager.start_calls) == 1


@pytest.mark.anyio
async def test_livekit_provider_recovers_lost_start_response_by_stable_object() -> None:
    class MissingEgressResolver(FakeResolver):
        async def resolve(self, effect):
            return replace(await super().resolve(effect), egress_id=None)

    from app.services.ai_call.runtime_control.livekit_provider import (
        LiveKitRuntimeProvider,
    )

    manager = RecordingEgressManager()
    manager.start_error = TimeoutError("response lost")
    manager.find_observation = SimpleNamespace(
        egress_id="EG_late",
        status="EGRESS_ACTIVE",
        object_name="ai-call/recordings/call-1.ogg",
        started_at=datetime(2026, 8, 3, 9, tzinfo=timezone.utc),
        ended_at=None,
        duration_ms=None,
        file_size=None,
    )
    provider = LiveKitRuntimeProvider(
        resolver=MissingEgressResolver(),
        room_manager=FakeRoomManager(),
        agent_manager=FakeAgentManager(),
        sip_client=FakeSipClient(),
        egress_manager=manager,
        main_recording_enabled=True,
        oss_config_provider=lambda: {"bucket_name": "recordings"},
    )

    assert (
        await provider.apply(_effect("START_EGRESS"))
    ).kind is ProviderObservationKind.UNCERTAIN
    recovered = await provider.apply(_effect("START_EGRESS", reconcile_only=True))

    assert recovered.kind is ProviderObservationKind.RESOURCE_PRESENT
    assert recovered.provider_reference == "EG_late"
    assert len(manager.start_calls) == 1
    assert manager.find_calls == [
        ("ai-call-call-1", "ai-call/recordings/call-1.ogg")
    ]


@pytest.mark.anyio
async def test_livekit_provider_stop_accepted_stays_non_terminal() -> None:
    manager = RecordingEgressManager()
    manager.stop_result = SimpleNamespace(egress_id="EG_1", status="EGRESS_ACTIVE")
    manager.query_observation = SimpleNamespace(
        egress_id="EG_1",
        status="EGRESS_ACTIVE",
        object_name="ai-call/recordings/call-1.ogg",
        started_at=None,
        ended_at=None,
        duration_ms=None,
        file_size=None,
    )

    observation = await _provider(egress_manager=manager).apply(
        _effect("STOP_EGRESS")
    )

    assert observation.kind is ProviderObservationKind.ACCEPTED
    assert manager.stop_calls == ["EG_1"]


@pytest.mark.anyio
async def test_livekit_provider_stop_response_active_stays_non_terminal_when_query_empty() -> None:
    manager = RecordingEgressManager()
    manager.stop_result = SimpleNamespace(
        egress_id="EG_1",
        status="EGRESS_ACTIVE",
        object_name="ai-call/recordings/call-1.ogg",
        started_at=None,
        ended_at=None,
        duration_ms=None,
        file_size=None,
    )

    observation = await _provider(egress_manager=manager).apply(
        _effect("STOP_EGRESS")
    )

    assert observation.kind is ProviderObservationKind.ACCEPTED
    assert observation.provider_status == "EGRESS_ACTIVE"


@pytest.mark.anyio
async def test_livekit_provider_stop_terminal_returns_recording_metadata() -> None:
    manager = RecordingEgressManager()
    manager.stop_result = SimpleNamespace(egress_id="EG_1", status="EGRESS_COMPLETE")
    manager.query_observation = SimpleNamespace(
        egress_id="EG_1",
        status="EGRESS_COMPLETE",
        object_name="ai-call/recordings/call-1.ogg",
        started_at=datetime(2026, 8, 3, 9, tzinfo=timezone.utc),
        ended_at=datetime(2026, 8, 3, 9, 1, tzinfo=timezone.utc),
        duration_ms=60_000,
        file_size=1024,
    )

    observation = await _provider(egress_manager=manager).apply(
        _effect("STOP_EGRESS")
    )

    assert observation.kind is ProviderObservationKind.TERMINAL_CONFIRMED
    assert observation.object_name == "ai-call/recordings/call-1.ogg"
    assert observation.duration_ms == 60_000
    assert observation.file_size == 1024


@pytest.mark.anyio
async def test_livekit_provider_query_agent_media_returns_current_track_fact() -> None:
    provider = _provider()

    observation = await provider.query_agent_media(
        "ai-call-call-1",
        "ai-agent-call-1-g7",
    )

    assert observation.ready is True
    assert observation.participant_sid == "PA_1"
    assert observation.track_sid == "TR_1"


def test_real_provider_gate_defaults_are_closed() -> None:
    from app.config.setting import Settings

    assert Settings.model_fields["AI_CALL_RUNTIME_PROVIDER_MODE"].default == "stub"
    assert Settings.model_fields["AI_CALL_RUNTIME_REAL_PROVIDER_ALLOWED"].default is False


@pytest.mark.anyio
async def test_resource_resolver_uses_effect_generation_and_source_reference() -> None:
    from app.api.v1.ai_call.model import AiCallRecordModel
    from app.services.ai_call.runtime_control.livekit_provider import (
        DatabaseRuntimeProviderResourceResolver,
    )
    from app.services.ai_call.runtime_control.models import (
        AiCallRuntimeCommandModel,
        AiCallRuntimeEffectModel,
    )

    rows = {
        AiCallRecordModel: SimpleNamespace(
            tenant_id="tenant-a",
            call_id="call-1",
            room_name="ai-call-call-1",
            participant_identity="sip-call-1",
            callee_phone_number="19900001001",
        ),
        AiCallRuntimeCommandModel: SimpleNamespace(payload_json='{"voice":"Cherry"}'),
    }
    effects = iter(
        (
            SimpleNamespace(
                id=101,
                tenant_id="tenant-a",
                call_id="call-1",
                command_id=77,
                provider_namespace="livekit:test",
                resource_generation=5,
            ),
            SimpleNamespace(
                id=88,
                tenant_id="tenant-a",
                call_id="call-1",
                provider_reference="EG_1",
            ),
        )
    )

    class FakeSession:
        async def scalar(self, statement):
            entity = statement.column_descriptions[0]["entity"]
            if entity is AiCallRuntimeEffectModel:
                return next(effects)
            return rows.get(entity)

    class SessionContext:
        async def __aenter__(self):
            return FakeSession()

        async def __aexit__(self, *_args):
            return None

    resolver = DatabaseRuntimeProviderResourceResolver(lambda: SessionContext())
    claim = replace(_effect("STOP_EGRESS"), source_create_effect_id=88)

    resource = await resolver.resolve(claim)

    assert resource.agent_participant_identity == "agent-call-1-g5"
    assert resource.egress_id == "EG_1"
    assert resource.voice == "Cherry"
    assert resource.callee_phone_number == "19900001001"


@pytest.mark.anyio
async def test_resource_resolver_composes_task_prompt_for_agent_attach(monkeypatch) -> None:
    from app.api.v1.ai_call import service as ai_call_service
    from app.api.v1.ai_call.model import AiCallRecordModel
    from app.services.ai_call.runtime_control.livekit_provider import (
        DatabaseRuntimeProviderResourceResolver,
    )
    from app.services.ai_call.runtime_control.models import (
        AiCallRuntimeCommandModel,
        AiCallRuntimeEffectModel,
    )

    contexts = []
    prompt_config = SimpleNamespace(opening_message="您好刘先生，想介绍一下 GEO。")

    class PromptResolver:
        async def resolve(self, context):
            contexts.append(context)
            return SimpleNamespace()

    monkeypatch.setattr(
        ai_call_service,
        "_build_prompt_resolver",
        lambda _repository, _orchestrator: PromptResolver(),
    )
    monkeypatch.setattr(
        ai_call_service,
        "_build_prompt_composer",
        lambda _orchestrator: SimpleNamespace(compose=lambda _result: prompt_config),
    )
    rows = {
        AiCallRecordModel: SimpleNamespace(
            tenant_id="tenant-a",
            call_id="call-1",
            room_name="ai-call-call-1",
            participant_identity="browser-call-1",
            callee_phone_number=None,
            business_id="attempt-1",
            scene_code="intro_geo",
        ),
        AiCallRuntimeCommandModel: SimpleNamespace(
            payload_json=(
                '{"business_params":{"customerName":"刘先生"},"voice":"Cherry"}'
            )
        ),
    }

    class FakeSession:
        async def scalar(self, statement):
            entity = statement.column_descriptions[0]["entity"]
            if entity is AiCallRuntimeEffectModel:
                return SimpleNamespace(
                    id=101,
                    tenant_id="tenant-a",
                    call_id="call-1",
                    command_id=77,
                    provider_namespace="livekit:test",
                    resource_generation=5,
                    provider_reference=None,
                )
            return rows.get(entity)

    class SessionContext:
        async def __aenter__(self):
            return FakeSession()

        async def __aexit__(self, *_args):
            return None

    resource = await DatabaseRuntimeProviderResourceResolver(
        lambda: SessionContext(),
        orchestrator=object(),
    ).resolve(_effect("ATTACH_AGENT_PARTICIPANT"))

    assert resource.prompt_effective_config is prompt_config
    assert contexts[0].scene_code == "intro_geo"
    assert contexts[0].business_params == {"customerName": "刘先生"}


@pytest.mark.anyio
async def test_resource_resolver_validates_customer_track_scope_and_source_reference() -> None:
    from app.api.v1.ai_call.model import AiCallRecordModel
    from app.services.ai_call.runtime_control.livekit_provider import (
        DatabaseRuntimeProviderResourceResolver,
    )
    from app.services.ai_call.runtime_control.models import (
        AiCallRuntimeCommandModel,
        AiCallRuntimeEffectModel,
    )

    _, provider_key, resource_key = customer_track_keys("call-1", "sip-call-1")
    rows = {
        AiCallRecordModel: SimpleNamespace(
            tenant_id="tenant-a",
            call_id="call-1",
            room_name="ai-call-call-1",
            participant_identity="sip-call-1",
            callee_phone_number="19900001001",
        ),
        AiCallRuntimeCommandModel: SimpleNamespace(payload_json=None),
    }
    effects = iter(
        (
            SimpleNamespace(
                id=202,
                tenant_id="tenant-a",
                call_id="call-1",
                command_id=77,
                effect_type="STOP_TRACK_EGRESS",
                provider_namespace="livekit:test",
                provider_idempotency_key="destroy:customer",
                resource_key=resource_key,
                resource_generation=1,
                provider_reference=None,
            ),
            SimpleNamespace(
                id=101,
                tenant_id="tenant-a",
                call_id="call-1",
                effect_type="START_TRACK_EGRESS",
                provider_namespace="livekit:test",
                provider_idempotency_key=provider_key,
                resource_key=resource_key,
                resource_generation=1,
                provider_reference="EG_customer",
            ),
        )
    )

    class FakeSession:
        async def scalar(self, statement):
            entity = statement.column_descriptions[0]["entity"]
            if entity is AiCallRuntimeEffectModel:
                return next(effects)
            return rows.get(entity)

    class SessionContext:
        async def __aenter__(self):
            return FakeSession()

        async def __aexit__(self, *_args):
            return None

    resolver = DatabaseRuntimeProviderResourceResolver(lambda: SessionContext())
    resource = await resolver.resolve(
        _customer_track_effect("STOP_TRACK_EGRESS")
    )

    assert resource.egress_scope == "participant"
    assert resource.customer_participant_identity == "sip-call-1"
    assert resource.egress_id == "EG_customer"


@pytest.mark.anyio
async def test_resource_resolver_rejects_customer_track_key_mismatch() -> None:
    from app.api.v1.ai_call.model import AiCallRecordModel
    from app.services.ai_call.runtime_control.livekit_provider import (
        DatabaseRuntimeProviderResourceResolver,
        ProviderPreconditionError,
    )
    from app.services.ai_call.runtime_control.models import (
        AiCallRuntimeCommandModel,
        AiCallRuntimeEffectModel,
    )

    _, provider_key, _resource_key = customer_track_keys("call-1", "sip-call-1")
    rows = {
        AiCallRecordModel: SimpleNamespace(
            tenant_id="tenant-a",
            call_id="call-1",
            room_name="ai-call-call-1",
            participant_identity="sip-call-1",
            callee_phone_number="19900001001",
        ),
        AiCallRuntimeCommandModel: SimpleNamespace(payload_json=None),
    }
    effects = iter(
        (
            SimpleNamespace(
                id=202,
                tenant_id="tenant-a",
                call_id="call-1",
                command_id=77,
                effect_type="START_TRACK_EGRESS",
                provider_namespace="livekit:test",
                provider_idempotency_key=provider_key,
                resource_key="wrong-resource",
                resource_generation=1,
                provider_reference=None,
            ),
        )
    )

    class FakeSession:
        async def scalar(self, statement):
            entity = statement.column_descriptions[0]["entity"]
            if entity is AiCallRuntimeEffectModel:
                return next(effects)
            return rows.get(entity)

    class SessionContext:
        async def __aenter__(self):
            return FakeSession()

        async def __aexit__(self, *_args):
            return None

    resolver = DatabaseRuntimeProviderResourceResolver(lambda: SessionContext())
    with pytest.raises(ProviderPreconditionError, match="customer_track_resource_mismatch"):
        await resolver.resolve(_customer_track_effect("START_TRACK_EGRESS"))


@pytest.mark.anyio
async def test_owner_agent_manager_registers_generation_identity_and_fail_closed_handle() -> None:
    from app.services.ai_call.runtime_control.livekit_provider import (
        OwnerRuntimeAgentManager,
        RuntimeProviderResource,
    )

    class FakeRunner:
        def __init__(self) -> None:
            self.started = []
            self.stopped = []

        async def start(self, session) -> None:
            self.started.append(session)

        async def stop(self, call_id: str) -> None:
            self.stopped.append(call_id)

    runner = FakeRunner()
    prompt_config = SimpleNamespace(
        instructions="GEO 业务话术",
        opening_message="您好刘先生，我是灵宸智能助手。",
    )
    orchestrator = SimpleNamespace(
        registry=InMemorySessionRegistry(),
        agent_runner=runner,
        _build_effective_config=lambda voice, prompt, prompt_effective_config=None: {
            "voice": voice,
            "prompt": prompt,
            "prompt_effective_config": prompt_effective_config,
        },
    )
    runtime_registry = RuntimeRegistry()
    manager = OwnerRuntimeAgentManager(
        orchestrator=orchestrator,
        runtime_registry=runtime_registry,
    )
    resource = RuntimeProviderResource(
        call_id="call-1",
        room_name="ai-call-call-1",
        customer_participant_identity="sip-call-1",
        agent_participant_identity="agent-call-1-g7",
        voice="Cherry",
        prompt_effective_config=prompt_config,
    )

    reference = await manager.start(resource)

    assert reference == "agent-call-1-g7"
    assert runner.started[0].local_participant_identity == "agent-call-1-g7"
    assert runner.started[0].effective_config["prompt_effective_config"] is prompt_config
    assert "call-1" in runtime_registry.local_handles

    await runtime_registry.local_handles["call-1"].fail_closed()

    assert runner.stopped == ["call-1"]
    assert await manager.exists("call-1") is False
    assert "call-1" not in runtime_registry.local_handles


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("accepted", "expected_openings"),
    [(True, ["call-1"]), (False, [])],
)
async def test_owner_agent_manager_binds_connected_fact_to_effect_owner_fencing(
    monkeypatch: pytest.MonkeyPatch,
    accepted: bool,
    expected_openings: list[str],
) -> None:
    from app.services.ai_call.runtime_control import livekit_provider
    from app.services.ai_call.runtime_control.livekit_provider import (
        OwnerRuntimeAgentManager,
        RuntimeProviderResource,
    )

    recorded: list[tuple[str, str, str, int, str]] = []
    openings: list[str] = []

    async def start_opening(call_id: str) -> None:
        openings.append(call_id)

    class FakeConnectedRepository:
        def __init__(self, _session) -> None:
            pass

        async def record_sip_connected(
            self,
            *,
            tenant_id: str,
            call_id: str,
            owner_id: str,
            fencing_token: int,
            sip_call_status: str,
        ) -> bool:
            recorded.append(
                (tenant_id, call_id, owner_id, fencing_token, sip_call_status)
            )
            return accepted

    class SessionFactory:
        def begin(self):
            return self

        async def __aenter__(self):
            return object()

        async def __aexit__(self, *_args):
            return None

    class FakeAudioTransport:
        def __init__(self) -> None:
            self.observers = {}

        def bind_sip_connected_observer(self, call_id, observer) -> None:
            self.observers[call_id] = observer

        def unbind_sip_connected_observer(self, call_id) -> None:
            self.observers.pop(call_id, None)

    class FakeRunner:
        def __init__(self) -> None:
            self.audio_transport = FakeAudioTransport()

        async def start(self, _session) -> None:
            return None

        async def stop(self, _call_id: str) -> None:
            return None

    monkeypatch.setattr(
        livekit_provider,
        "RuntimeOwnerRepository",
        FakeConnectedRepository,
    )
    runner = FakeRunner()
    manager = OwnerRuntimeAgentManager(
        orchestrator=SimpleNamespace(
            registry=InMemorySessionRegistry(),
            agent_runner=runner,
            _build_effective_config=lambda _voice, _prompt: {},
            start_opening=start_opening,
        ),
        runtime_registry=RuntimeRegistry(),
        session_factory=SessionFactory(),
    )

    await manager.start(
        RuntimeProviderResource(
            call_id="call-1",
            room_name="ai-call-call-1",
            customer_participant_identity="sip-call-1",
            agent_participant_identity="agent-call-1-g7",
            tenant_id="tenant-a",
            runtime_owner_id="runtime-1",
            runtime_fencing_token=7,
        )
    )
    assert "call-1" in runner.audio_transport.observers

    assert await runner.audio_transport.observers["call-1"]("active") is accepted
    assert recorded == [("tenant-a", "call-1", "runtime-1", 7, "active")]
    assert openings == expected_openings

    await manager.stop("call-1")

    assert "call-1" not in runner.audio_transport.observers


@pytest.mark.anyio
async def test_owner_agent_manager_binds_dialogue_before_agent_and_finalizes_after_stop() -> None:
    from app.services.ai_call.runtime_control.livekit_provider import (
        OwnerRuntimeAgentManager,
        RuntimeProviderResource,
    )

    calls: list[tuple[str, object]] = []

    class FakeDialogueBridge:
        async def bind_call(self, fence: OwnerDialogueFence) -> bool:
            calls.append(("bind_dialogue", fence))
            return True

        async def finalize_call(
            self,
            fence: OwnerDialogueFence,
            *,
            ended_at: datetime,
        ) -> OwnerDialogueFinalizeResult:
            calls.append(("finalize_dialogue", (fence, ended_at)))
            return OwnerDialogueFinalizeResult("complete", 1, 0)

        def abandon_call(self, fence: OwnerDialogueFence) -> None:
            calls.append(("abandon_dialogue", fence))

    class FakeAudioTransport:
        def bind_sip_connected_observer(self, _call_id, _observer) -> None:
            return None

        def unbind_sip_connected_observer(self, _call_id) -> None:
            return None

    class FakeRunner:
        def __init__(self) -> None:
            self.audio_transport = FakeAudioTransport()

        async def start(self, _session) -> None:
            calls.append(("start_agent", None))

        async def stop(self, _call_id: str) -> None:
            calls.append(("stop_agent", None))

    resource = RuntimeProviderResource(
        call_id="call-1",
        room_name="ai-call-call-1",
        customer_participant_identity="sip-call-1",
        agent_participant_identity="agent-call-1-g7",
        tenant_id="tenant-a",
        runtime_owner_id="runtime-1",
        runtime_fencing_token=7,
    )
    runtime_registry = RuntimeRegistry()
    manager = OwnerRuntimeAgentManager(
        orchestrator=SimpleNamespace(
            registry=InMemorySessionRegistry(),
            agent_runner=FakeRunner(),
            _build_effective_config=lambda _voice, _prompt: {},
        ),
        runtime_registry=runtime_registry,
        session_factory=object(),
        dialogue_bridge=FakeDialogueBridge(),
    )

    await manager.start(resource)
    await runtime_registry.local_handles[resource.call_id].shutdown()

    assert [name for name, _value in calls] == [
        "bind_dialogue",
        "start_agent",
        "stop_agent",
        "finalize_dialogue",
    ]


@pytest.mark.anyio
async def test_owner_agent_manager_refuses_dialogue_complete_while_agent_is_active() -> None:
    from app.services.ai_call.runtime_control.livekit_provider import (
        OwnerRuntimeAgentManager,
        RuntimeProviderResource,
    )

    finalized: list[OwnerDialogueFence] = []

    class FakeDialogueBridge:
        async def bind_call(self, _fence: OwnerDialogueFence) -> bool:
            return True

        async def finalize_call(
            self,
            fence: OwnerDialogueFence,
            *,
            ended_at: datetime,
        ) -> OwnerDialogueFinalizeResult:
            del ended_at
            finalized.append(fence)
            return OwnerDialogueFinalizeResult("complete", 0, 0)

        def abandon_call(self, _fence: OwnerDialogueFence) -> None:
            return None

    class FakeAudioTransport:
        def bind_sip_connected_observer(self, _call_id, _observer) -> None:
            return None

        def unbind_sip_connected_observer(self, _call_id) -> None:
            return None

    class FakeRunner:
        def __init__(self) -> None:
            self.audio_transport = FakeAudioTransport()

        async def start(self, _session) -> None:
            return None

        async def stop(self, _call_id: str) -> None:
            return None

    manager = OwnerRuntimeAgentManager(
        orchestrator=SimpleNamespace(
            registry=InMemorySessionRegistry(),
            agent_runner=FakeRunner(),
            _build_effective_config=lambda _voice, _prompt: {},
        ),
        runtime_registry=RuntimeRegistry(),
        session_factory=object(),
        dialogue_bridge=FakeDialogueBridge(),
    )
    resource = RuntimeProviderResource(
        call_id="call-1",
        room_name="ai-call-call-1",
        customer_participant_identity="sip-call-1",
        agent_participant_identity="agent-call-1-g7",
        tenant_id="tenant-a",
        runtime_owner_id="runtime-1",
        runtime_fencing_token=7,
    )
    ended_at = datetime(2026, 8, 3, 8, tzinfo=timezone.utc)

    await manager.start(resource)
    before_stop = await manager.finalize_dialogue("call-1", ended_at=ended_at)
    await manager.stop("call-1")
    after_stop = await manager.finalize_dialogue("call-1", ended_at=ended_at)

    assert before_stop.status == "pending"
    assert after_stop.status == "complete"
    assert finalized == [OwnerDialogueFence("tenant-a", "call-1", "runtime-1", 7)]


@pytest.mark.anyio
async def test_owner_agent_stop_failure_keeps_handle_for_retry_before_finalize() -> None:
    from app.services.ai_call.runtime_control.livekit_provider import (
        OwnerRuntimeAgentManager,
        RuntimeProviderResource,
    )

    failed: list[tuple[OwnerDialogueFence, str]] = []
    finalized_statuses: list[str] = []

    class FakeDialogueBridge:
        async def bind_call(self, _fence: OwnerDialogueFence) -> bool:
            return True

        def mark_failed(self, fence: OwnerDialogueFence, *, error: str) -> None:
            failed.append((fence, error))

        async def finalize_call(
            self,
            _fence: OwnerDialogueFence,
            *,
            ended_at: datetime,
        ) -> OwnerDialogueFinalizeResult:
            del ended_at
            status = "uncertain" if failed else "complete"
            finalized_statuses.append(status)
            return OwnerDialogueFinalizeResult(status, 0, 0)

        def abandon_call(self, _fence: OwnerDialogueFence) -> None:
            return None

    class FakeAudioTransport:
        def bind_sip_connected_observer(self, _call_id, _observer) -> None:
            return None

        def unbind_sip_connected_observer(self, _call_id) -> None:
            return None

    class FakeRunner:
        def __init__(self) -> None:
            self.audio_transport = FakeAudioTransport()
            self.stop_attempts = 0

        async def start(self, _session) -> None:
            return None

        async def stop(self, _call_id: str) -> None:
            self.stop_attempts += 1
            if self.stop_attempts == 1:
                raise RuntimeError("runner stop failed")

    runtime_registry = RuntimeRegistry()
    runner = FakeRunner()
    manager = OwnerRuntimeAgentManager(
        orchestrator=SimpleNamespace(
            registry=InMemorySessionRegistry(),
            agent_runner=runner,
            _build_effective_config=lambda _voice, _prompt: {},
        ),
        runtime_registry=runtime_registry,
        session_factory=object(),
        dialogue_bridge=FakeDialogueBridge(),
    )
    resource = RuntimeProviderResource(
        call_id="call-1",
        room_name="ai-call-call-1",
        customer_participant_identity="sip-call-1",
        agent_participant_identity="agent-call-1-g7",
        tenant_id="tenant-a",
        runtime_owner_id="runtime-1",
        runtime_fencing_token=7,
    )

    await manager.start(resource)
    handle = runtime_registry.local_handles[resource.call_id]
    with pytest.raises(RuntimeError, match="runner stop failed"):
        await handle.shutdown()
    assert await manager.exists(resource.call_id) is True
    assert runtime_registry.local_handles[resource.call_id] is handle
    assert finalized_statuses == []

    await handle.shutdown()

    assert finalized_statuses == ["uncertain"]
    assert runner.stop_attempts == 2
    assert resource.call_id not in runtime_registry.local_handles
    assert failed == [
        (
            OwnerDialogueFence("tenant-a", "call-1", "runtime-1", 7),
            "agent_stop_failed",
        )
    ]


@pytest.mark.anyio
async def test_owner_agent_fail_closed_abandons_dialogue_without_complete() -> None:
    from app.services.ai_call.runtime_control.livekit_provider import (
        OwnerRuntimeAgentManager,
        RuntimeProviderResource,
    )

    finalized: list[OwnerDialogueFence] = []
    abandoned: list[OwnerDialogueFence] = []

    class FakeDialogueBridge:
        async def bind_call(self, _fence: OwnerDialogueFence) -> bool:
            return True

        async def finalize_call(
            self,
            fence: OwnerDialogueFence,
            *,
            ended_at: datetime,
        ) -> OwnerDialogueFinalizeResult:
            del ended_at
            finalized.append(fence)
            return OwnerDialogueFinalizeResult("complete", 0, 0)

        def abandon_call(self, fence: OwnerDialogueFence) -> None:
            abandoned.append(fence)

    class FakeAudioTransport:
        def bind_sip_connected_observer(self, _call_id, _observer) -> None:
            return None

        def unbind_sip_connected_observer(self, _call_id) -> None:
            return None

    class FakeRunner:
        def __init__(self) -> None:
            self.audio_transport = FakeAudioTransport()

        async def start(self, _session) -> None:
            return None

        async def stop(self, _call_id: str) -> None:
            return None

    registry = RuntimeRegistry()
    manager = OwnerRuntimeAgentManager(
        orchestrator=SimpleNamespace(
            registry=InMemorySessionRegistry(),
            agent_runner=FakeRunner(),
            _build_effective_config=lambda _voice, _prompt: {},
        ),
        runtime_registry=registry,
        session_factory=object(),
        dialogue_bridge=FakeDialogueBridge(),
    )
    resource = RuntimeProviderResource(
        call_id="call-1",
        room_name="ai-call-call-1",
        customer_participant_identity="sip-call-1",
        agent_participant_identity="agent-call-1-g7",
        tenant_id="tenant-a",
        runtime_owner_id="runtime-1",
        runtime_fencing_token=7,
    )

    await manager.start(resource)
    await registry.local_handles[resource.call_id].fail_closed()

    assert finalized == []
    assert abandoned == [OwnerDialogueFence("tenant-a", "call-1", "runtime-1", 7)]


@pytest.mark.anyio
async def test_livekit_provider_owns_bridge_lifecycle_and_dialogue_finalization() -> None:
    from app.services.ai_call.runtime_control.livekit_provider import (
        LiveKitRuntimeProvider,
    )

    calls: list[tuple[str, object]] = []

    class FakeDialogueBridge:
        async def start(self) -> None:
            calls.append(("start_bridge", None))

        async def stop(self) -> None:
            calls.append(("stop_bridge", None))

    class FakeHandoffTriggerWorker:
        async def start(self) -> None:
            calls.append(("start_handoff_trigger", None))

        def detach_all(self) -> None:
            calls.append(("detach_handoff_trigger", None))

        async def stop(self) -> None:
            calls.append(("stop_handoff_trigger", None))

    class FinalizingAgentManager(FakeAgentManager):
        async def finalize_dialogue(
            self,
            call_id: str,
            *,
            ended_at: datetime,
        ) -> OwnerDialogueFinalizeResult:
            calls.append(("finalize_dialogue", (call_id, ended_at)))
            return OwnerDialogueFinalizeResult("complete", 2, 0)

        async def shutdown(self) -> None:
            calls.append(("shutdown_agents", None))

    bridge = FakeDialogueBridge()
    handoff_trigger_worker = FakeHandoffTriggerWorker()
    provider = LiveKitRuntimeProvider(
        resolver=FakeResolver(),
        room_manager=FakeRoomManager(),
        agent_manager=FinalizingAgentManager(),
        sip_client=FakeSipClient(),
        egress_manager=FakeEgressManager(),
        dialogue_bridge=bridge,
        handoff_trigger_worker=handoff_trigger_worker,
    )
    ended_at = datetime(2026, 8, 3, 9, tzinfo=timezone.utc)

    await provider.start()
    result = await provider.finalize_dialogue(
        SimpleNamespace(call_id="call-1"),
        ended_at=ended_at,
    )
    await provider.stop()

    assert result == OwnerDialogueFinalizeResult("complete", 2, 0)
    assert calls == [
        ("start_handoff_trigger", None),
        ("start_bridge", None),
        ("finalize_dialogue", ("call-1", ended_at)),
        ("detach_handoff_trigger", None),
        ("stop_handoff_trigger", None),
        ("shutdown_agents", None),
        ("stop_bridge", None),
    ]
