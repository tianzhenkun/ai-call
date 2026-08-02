from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

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


def _provider(
    *,
    room_manager=None,
    agent_manager=None,
    sip_client=None,
    egress_manager=None,
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
    )


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
async def test_livekit_provider_missing_egress_reference_is_uncertain() -> None:
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

    assert observation.kind == ProviderObservationKind.UNCERTAIN
    assert observation.error_message == "provider_reference_missing"


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
    orchestrator = SimpleNamespace(
        registry=InMemorySessionRegistry(),
        agent_runner=runner,
        _build_effective_config=lambda voice, prompt: {
            "voice": voice,
            "prompt": prompt,
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
    )

    reference = await manager.start(resource)

    assert reference == "agent-call-1-g7"
    assert runner.started[0].local_participant_identity == "agent-call-1-g7"
    assert "call-1" in runtime_registry.local_handles

    await runtime_registry.local_handles["call-1"].fail_closed()

    assert runner.stopped == ["call-1"]
    assert await manager.exists("call-1") is False
    assert "call-1" not in runtime_registry.local_handles


@pytest.mark.anyio
async def test_owner_agent_manager_binds_connected_fact_to_effect_owner_fencing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.services.ai_call.runtime_control import livekit_provider
    from app.services.ai_call.runtime_control.livekit_provider import (
        OwnerRuntimeAgentManager,
        RuntimeProviderResource,
    )

    recorded: list[tuple[str, str, str, int, str]] = []

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
            return True

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

    assert await runner.audio_transport.observers["call-1"]("active") is True
    assert recorded == [("tenant-a", "call-1", "runtime-1", 7, "active")]

    await manager.stop("call-1")

    assert "call-1" not in runner.audio_transport.observers
