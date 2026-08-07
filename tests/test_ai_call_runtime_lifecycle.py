from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from app.common.enums import EnvironmentEnum
from app.services.ai_call.runtime_control.dialogue_bridge import (
    OwnerDialogueFinalizeResult,
)
from app.services.ai_call.runtime_control.lifecycle import (
    AiCallRuntimeTimingPolicy,
    start_dispatcher_control_lifecycle,
    start_recovery_control_lifecycle,
    start_runtime_control_lifecycle,
)
from app.services.ai_call.runtime_control.owner_repository import OwnerFailClosedWatchdog
from app.services.ai_call.runtime_control.provider_stub import (
    DeterministicDbOnlyProviderStub,
    DeterministicWebProviderStub,
)
from app.services.ai_call.runtime_control.runtime_service import (
    RuntimeControlService,
    RuntimeRegistry,
    _default_start_specs,
    _FailClosedProvider,
)


def _lease(*, fencing_token: int = 7):
    return SimpleNamespace(fencing_token=fencing_token)


@pytest.mark.parametrize("entry_type", ["direct_sip", "outbound"])
def test_sip_entry_default_specs_include_sip_participant(entry_type: str) -> None:
    specs = _default_start_specs(
        "call-a",
        _lease(fencing_token=7),
        "runtime-a",
        entry_type=entry_type,
    )

    assert [spec.effect_type for spec in specs] == [
        "CREATE_ROOM",
        "ATTACH_AGENT_PARTICIPANT",
        "CREATE_SIP_PARTICIPANT",
    ]
    assert specs[-1].resource_key == "sip:call-a:g7"


def test_default_start_specs_fail_closed_for_unknown_entry() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        _default_start_specs(
            "call-a",
            _lease(),
            "runtime-a",
            entry_type="preview",
        )


def test_start_specs_use_real_provider_namespace_when_configured() -> None:
    specs = _default_start_specs(
        "call-a",
        _lease(),
        "runtime-a",
        entry_type="web",
        provider_namespace="livekit:isolated-test",
    )

    assert {spec.provider_namespace for spec in specs} == {
        "livekit:isolated-test"
    }


def test_main_egress_spec_is_stable_across_owner_fencing() -> None:
    first = _default_start_specs(
        "call-a",
        _lease(fencing_token=7),
        "runtime-a",
        entry_type="web",
        provider_namespace="livekit:isolated-test",
        main_recording_enabled=True,
        participant_identity="customer-a",
    )
    takeover = _default_start_specs(
        "call-a",
        _lease(fencing_token=8),
        "runtime-b",
        entry_type="web",
        provider_namespace="livekit:isolated-test",
        main_recording_enabled=True,
        participant_identity="customer-a",
    )

    first_egress = next(spec for spec in first if spec.effect_type == "START_EGRESS")
    takeover_egress = next(
        spec for spec in takeover if spec.effect_type == "START_EGRESS"
    )
    assert first_egress == takeover_egress
    assert first_egress.idempotency_key == "start:call-a:start-main-egress"
    assert first_egress.provider_idempotency_key == "egress:main:call-a"
    assert first_egress.resource_key == "egress:main:call-a"
    assert first_egress.resource_generation == 1


def test_recording_capability_registers_main_and_customer_track_only() -> None:
    specs = _default_start_specs(
        "call-a",
        _lease(fencing_token=7),
        "runtime-a",
        entry_type="outbound",
        provider_namespace="livekit:isolated-test",
        main_recording_enabled=True,
        participant_identity="customer-a",
    )

    recording_specs = [
        spec
        for spec in specs
        if spec.effect_type in {"START_EGRESS", "START_TRACK_EGRESS"}
    ]
    assert [spec.effect_type for spec in recording_specs] == [
        "START_EGRESS",
        "START_TRACK_EGRESS",
    ]
    track = recording_specs[-1]
    assert track.resource_generation == 1
    assert "customer" in track.resource_key


def test_recording_capability_requires_customer_identity() -> None:
    with pytest.raises(ValueError, match="participant_identity"):
        _default_start_specs(
            "call-a",
            _lease(),
            "runtime-a",
            entry_type="web",
            provider_namespace="livekit:isolated-test",
            main_recording_enabled=True,
        )


def test_runtime_services_do_not_share_local_registry() -> None:
    runtime_a = RuntimeControlService(
        worker_id="pod-a:12345678-1234-5678-1234-567812345678",
        registry=RuntimeRegistry(),
        session_factory=None,
        provider=None,
    )
    runtime_b = RuntimeControlService(
        worker_id="pod-b:87654321-4321-8765-4321-876543218765",
        registry=RuntimeRegistry(),
        session_factory=None,
        provider=None,
    )

    assert runtime_a.registry is not runtime_b.registry


def test_db_only_timing_policy_freezes_scan_intervals() -> None:
    policy = AiCallRuntimeTimingPolicy()

    assert policy.end_scan_interval_seconds == 0.5
    assert policy.command_scan_interval_seconds == 1.0


@pytest.mark.anyio
async def test_runtime_lifecycle_uses_deterministic_offline_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.services.ai_call.runtime_control import lifecycle

    calls: list[str] = []

    async def _validate_database(_session_factory):
        calls.append("validate_database")
        return "runtime-test", "public"

    class _RuntimeService:
        def __init__(self, **kwargs) -> None:
            self.provider = kwargs["provider"]
            calls.append("construct")

        async def start(self) -> None:
            calls.append("start")

        async def stop(self) -> None:
            calls.append("stop")

    monkeypatch.setattr(lifecycle, "validate_db_only_runtime_database", _validate_database)
    monkeypatch.setattr(lifecycle, "RuntimeControlService", _RuntimeService)

    session_factory = SimpleNamespace(kw={"bind": object()})

    service = await start_runtime_control_lifecycle(
        SimpleNamespace(
            AI_CALL_RUNTIME_INSTANCE_ID="runtime-test",
            AI_CALL_RUNTIME_CAPACITY=2,
            AI_CALL_RUNTIME_CLEANUP_CAPACITY=1,
            AI_CALL_RUNTIME_WORKER_LEASE_SECONDS=15,
            AI_CALL_RUNTIME_OWNER_LEASE_SECONDS=15,
            AI_CALL_RUNTIME_FAIL_CLOSED_MARGIN_SECONDS=3,
            AI_CALL_RUNTIME_END_SCAN_INTERVAL_SECONDS=0.01,
        ),
        session_factory,
    )
    await service.stop()

    assert isinstance(service.provider, DeterministicDbOnlyProviderStub)
    assert isinstance(service.provider, DeterministicWebProviderStub)
    assert service.provider.calls == []
    assert calls == ["validate_database", "construct", "start", "stop"]


@pytest.mark.anyio
async def test_runtime_service_starts_provider_before_scan_and_stops_it_last(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.services.ai_call.runtime_control import runtime_service

    calls: list[str] = []

    class LocalHandle:
        async def shutdown(self) -> None:
            calls.append("handle_shutdown")

        async def fail_closed(self) -> None:
            calls.append("handle_fail_closed")

    class Provider:
        async def start(self) -> None:
            calls.append("provider_start")

        async def stop(self) -> None:
            calls.append("provider_stop")

    class Session:
        async def execute(self, _statement) -> None:
            return None

    class Transaction:
        async def __aenter__(self):
            return Session()

        async def __aexit__(self, *_args) -> None:
            return None

    class SessionFactory:
        def begin(self):
            return Transaction()

    class WorkerRepository:
        def __init__(self, _session, **_kwargs) -> None:
            return None

        async def register(self, registration):
            del registration
            return SimpleNamespace(
                worker_id="runtime-test:12345678-1234-5678-1234-567812345678"
            )

    monkeypatch.setattr(runtime_service, "WorkerRegistryRepository", WorkerRepository)
    registry = RuntimeRegistry()
    registry.local_handles["call-1"] = LocalHandle()
    service = RuntimeControlService(
        worker_id="runtime-test:12345678-1234-5678-1234-567812345678",
        registry=registry,
        session_factory=SessionFactory(),
        provider=Provider(),
    )

    async def request_local_end(call_id: str, *, source: str, end_reason: str) -> bool:
        assert call_id == "call-1"
        assert source == "runtime_shutdown"
        assert end_reason == "runtime_shutdown"
        calls.append("terminal_barrier")
        return True

    monkeypatch.setattr(service, "_request_local_end", request_local_end, raising=False)

    async def idle_loop() -> None:
        calls.append("runtime_scan")
        await service._stop_event.wait()

    monkeypatch.setattr(service, "_run_loop", idle_loop)

    await service.start()
    await asyncio.sleep(0)
    await service.stop()

    assert calls == [
        "provider_start",
        "runtime_scan",
        "terminal_barrier",
        "handle_shutdown",
        "provider_stop",
    ]


@pytest.mark.anyio
async def test_runtime_task_exit_establishes_end_before_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class LocalHandle:
        async def shutdown(self) -> None:
            raise AssertionError("unexpected exit must fail closed")

        async def fail_closed(self) -> None:
            calls.append("handle_fail_closed")

    class Provider:
        async def stop(self) -> None:
            calls.append("provider_stop")

    registry = RuntimeRegistry()
    registry.local_handles["call-1"] = LocalHandle()
    registry.owner_fencing_tokens["call-1"] = 7
    service = RuntimeControlService(
        worker_id="runtime-test:12345678-1234-5678-1234-567812345678",
        registry=registry,
        session_factory=None,
        provider=Provider(),
    )

    async def request_local_end(call_id: str, *, source: str, end_reason: str) -> bool:
        assert call_id == "call-1"
        assert source == "runtime_task_exit"
        assert end_reason == "runtime_task_failed"
        calls.append("terminal_barrier")
        return True

    monkeypatch.setattr(service, "_request_local_end", request_local_end, raising=False)

    await service._fail_closed_after_runtime_exit("runtime_task_failed")

    assert calls == ["terminal_barrier", "handle_fail_closed", "provider_stop"]


@pytest.mark.anyio
async def test_real_provider_gate_requires_isolated_allowlist_and_explicit_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.services.ai_call.runtime_control import lifecycle

    marker = object()

    def build_real_provider(**_kwargs):
        return marker

    monkeypatch.setattr(
        lifecycle,
        "build_livekit_runtime_provider",
        build_real_provider,
        raising=False,
    )
    base = {
        "AI_CALL_RUNTIME_PROVIDER_MODE": "livekit",
        "AI_CALL_RUNTIME_REAL_PROVIDER_ALLOWED": True,
        "AI_CALL_OWNER_COMMAND_V1_ENTRIES": "direct_sip",
        "AI_CALL_OUTBOUND_LINPHONE_TEST_ENABLED": True,
        "AI_CALL_OUTBOUND_LINPHONE_ALLOWED_CALLEE": "19900001001",
        "DATABASE_TYPE": "postgres",
        "ENVIRONMENT": EnvironmentEnum.DEV,
    }

    selected = lifecycle.select_runtime_provider(
        SimpleNamespace(**base),
        session_factory=object(),
        registry=RuntimeRegistry(),
    )
    assert selected is marker

    for override in (
        {"AI_CALL_RUNTIME_REAL_PROVIDER_ALLOWED": False},
        {"AI_CALL_OWNER_COMMAND_V1_ENTRIES": ""},
        {"AI_CALL_OUTBOUND_LINPHONE_TEST_ENABLED": False},
        {"AI_CALL_OUTBOUND_LINPHONE_ALLOWED_CALLEE": ""},
        {"DATABASE_TYPE": "mysql"},
        {"ENVIRONMENT": EnvironmentEnum.PROD},
    ):
        with pytest.raises(RuntimeError, match="真实 Provider"):
            lifecycle.select_runtime_provider(
                SimpleNamespace(**(base | override)),
                session_factory=object(),
                registry=RuntimeRegistry(),
            )


@pytest.mark.anyio
async def test_lifecycle_constructs_independent_runtime_and_dispatcher_listeners(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.services.ai_call.runtime_control import lifecycle

    constructed_listeners: list[object] = []
    service_listeners: list[object] = []

    async def _validate_database(_session_factory):
        return "runtime-test", "public"

    class _Listener:
        def __init__(self, engine: object) -> None:
            self.engine = engine
            constructed_listeners.append(self)

    class _Service:
        def __init__(self, *args: object, **kwargs: object) -> None:
            service_listeners.append(kwargs["wakeup_listener"])

        async def start(self) -> None:
            return None

    class _RecoveryService:
        def __init__(self, *args: object, **kwargs: object) -> None:
            assert "wakeup_listener" not in kwargs

        async def start(self) -> None:
            return None

    monkeypatch.setattr(lifecycle, "validate_db_only_runtime_database", _validate_database)
    monkeypatch.setattr(lifecycle, "PostgresWakeupListener", _Listener)
    monkeypatch.setattr(lifecycle, "RuntimeControlService", _Service)
    monkeypatch.setattr(lifecycle, "DispatcherControlService", _Service)
    monkeypatch.setattr(lifecycle, "RecoveryControlService", _RecoveryService)
    settings = SimpleNamespace(
        AI_CALL_RUNTIME_INSTANCE_ID="runtime-test",
        AI_CALL_RUNTIME_CAPACITY=2,
        AI_CALL_RUNTIME_CLEANUP_CAPACITY=1,
        AI_CALL_RUNTIME_WORKER_LEASE_SECONDS=15,
        AI_CALL_RUNTIME_OWNER_LEASE_SECONDS=15,
        AI_CALL_RUNTIME_FAIL_CLOSED_MARGIN_SECONDS=3,
        AI_CALL_RUNTIME_END_SCAN_INTERVAL_SECONDS=0.5,
        AI_CALL_RUNTIME_COMMAND_SCAN_INTERVAL_SECONDS=1.0,
    )
    engine = object()
    session_factory = SimpleNamespace(kw={"bind": engine})

    await start_runtime_control_lifecycle(settings, session_factory)
    await start_dispatcher_control_lifecycle(settings, session_factory)
    await start_recovery_control_lifecycle(settings, session_factory)

    assert len(constructed_listeners) == 2
    assert constructed_listeners[0] is not constructed_listeners[1]
    assert service_listeners == constructed_listeners
    assert all(listener.engine is engine for listener in constructed_listeners)


@pytest.mark.anyio
async def test_provider_timeout_before_owner_deadline_does_not_trip_watchdog() -> None:
    class TimeoutProvider:
        async def apply(self, effect):
            raise TimeoutError("provider timeout")

    fail_closed = asyncio.Event()

    async def mark_fail_closed() -> None:
        fail_closed.set()

    watchdog = OwnerFailClosedWatchdog(
        lease_ttl_seconds=15,
        safety_margin_seconds=3,
    )
    watchdog.observe_renewal()

    with pytest.raises(TimeoutError, match="provider timeout"):
        await _FailClosedProvider(
            TimeoutProvider(), watchdog, mark_fail_closed
        ).apply(object())

    assert watchdog.creation_allowed() is True
    assert fail_closed.is_set() is False


@pytest.mark.anyio
async def test_media_query_timeout_before_owner_deadline_does_not_trip_watchdog() -> None:
    class TimeoutProvider:
        async def query_agent_media(self, _room_name, _participant_identity):
            raise TimeoutError("provider query timeout")

    fail_closed = asyncio.Event()

    async def mark_fail_closed() -> None:
        fail_closed.set()

    watchdog = OwnerFailClosedWatchdog(
        lease_ttl_seconds=15,
        safety_margin_seconds=3,
    )
    watchdog.observe_renewal()

    with pytest.raises(TimeoutError, match="provider query timeout"):
        await _FailClosedProvider(
            TimeoutProvider(), watchdog, mark_fail_closed
        ).query_agent_media("room-1", "agent-1")

    assert watchdog.creation_allowed() is True
    assert fail_closed.is_set() is False


@pytest.mark.anyio
async def test_fail_closed_provider_guards_dialogue_finalize_with_owner_deadline() -> None:
    calls: list[tuple[object, object]] = []

    class Provider:
        async def finalize_dialogue(self, owner_lease, *, ended_at):
            calls.append((owner_lease, ended_at))
            return OwnerDialogueFinalizeResult("complete", 0, 0)

    async def fail_closed() -> None:
        raise AssertionError("valid deadline must not fail closed")

    watchdog = OwnerFailClosedWatchdog(
        lease_ttl_seconds=15,
        safety_margin_seconds=3,
    )
    watchdog.observe_renewal()
    owner_lease = SimpleNamespace(call_id="call-1")
    ended_at = object()

    result = await _FailClosedProvider(
        Provider(), watchdog, fail_closed
    ).finalize_dialogue(owner_lease, ended_at=ended_at)

    assert result == OwnerDialogueFinalizeResult("complete", 0, 0)
    assert calls == [(owner_lease, ended_at)]


@pytest.mark.anyio
async def test_runtime_routes_media_ready_to_specialized_handler(monkeypatch) -> None:
    from app.services.ai_call.runtime_control import runtime_service

    routed: list[tuple[object, object]] = []
    claim = SimpleNamespace(
        command_type="AGENT_MEDIA_READY",
        call_id="call-1",
    )
    lease = SimpleNamespace(call_id="call-1")

    class _Transaction:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, _exc_type, _exc, _traceback) -> None:
            return None

    class _SessionFactory:
        def begin(self):
            return _Transaction()

    class _CommandRepository:
        claim_count = 0

        def __init__(self, _session) -> None:
            return None

        async def claim_pending_end(self, _lease):
            return None

        async def claim_next_for_owner(self, _lease):
            return claim

        async def complete(self, *_args, **_kwargs):
            raise AssertionError("media command must not use the generic success path")

    class _EffectRepository:
        def __init__(self, _session) -> None:
            return None

        async def claim_next(self, _lease):
            return None

        async def mark_cleanup_clean(self, _lease):
            return False

    class _ReadyHandler:
        def __init__(self, session_factory, provider) -> None:
            routed.append((session_factory, provider))

        async def handle(self, received_claim, received_lease):
            assert received_claim is claim
            assert received_lease is lease
            return SimpleNamespace(command_completed=True, state_changed=True)

    class _Provider:
        async def apply(self, _effect):
            raise AssertionError("no effect should be applied")

        async def query_agent_media(self, _room_name, _participant_identity):
            raise AssertionError("specialized handler is mocked")

    monkeypatch.setattr(runtime_service, "RuntimeCommandRepository", _CommandRepository)
    monkeypatch.setattr(runtime_service, "RuntimeEffectRepository", _EffectRepository)
    monkeypatch.setattr(
        runtime_service,
        "AgentMediaReadyHandler",
        _ReadyHandler,
        raising=False,
    )
    service = RuntimeControlService(
        worker_id="runtime-test:12345678-1234-5678-1234-567812345678",
        registry=RuntimeRegistry(),
        session_factory=None,
        provider=None,
    )
    watchdog = OwnerFailClosedWatchdog(
        lease_ttl_seconds=15,
        safety_margin_seconds=3,
    )
    watchdog.observe_renewal()

    processed = await service._process_owned_call(
        _SessionFactory(),
        _Provider(),
        lease,
        watchdog,
    )

    assert processed is True
    assert len(routed) == 1


@pytest.mark.anyio
async def test_runtime_routes_start_opening_to_owner_local_handle(monkeypatch) -> None:
    from app.services.ai_call.runtime_control import runtime_service

    claim = SimpleNamespace(command_type="START_OPENING", call_id="call-1")
    lease = SimpleNamespace(call_id="call-1")
    decisions = []
    openings = []

    class _Transaction:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, _exc_type, _exc, _traceback) -> None:
            return None

    class _SessionFactory:
        def begin(self):
            return _Transaction()

    class _CommandRepository:
        def __init__(self, _session) -> None:
            return None

        async def claim_pending_end(self, _lease):
            return None

        async def claim_next_for_owner(self, _lease):
            return claim

        async def complete(self, received_claim, decision):
            assert received_claim is claim
            decisions.append(decision)

    class _EffectRepository:
        def __init__(self, _session) -> None:
            return None

        async def claim_next(self, _lease):
            return None

        async def mark_cleanup_clean(self, _lease):
            return False

    class _Handle:
        async def start_opening(self):
            openings.append("call-1")

    monkeypatch.setattr(runtime_service, "RuntimeCommandRepository", _CommandRepository)
    monkeypatch.setattr(runtime_service, "RuntimeEffectRepository", _EffectRepository)
    service = RuntimeControlService(
        worker_id="runtime-test:12345678-1234-5678-1234-567812345678",
        registry=RuntimeRegistry(local_handles={"call-1": _Handle()}),
        session_factory=None,
        provider=None,
    )
    watchdog = OwnerFailClosedWatchdog(
        lease_ttl_seconds=15,
        safety_margin_seconds=3,
    )
    watchdog.observe_renewal()

    processed = await service._process_owned_call(
        _SessionFactory(),
        SimpleNamespace(),
        lease,
        watchdog,
    )

    assert processed is True
    assert openings == ["call-1"]
    assert decisions[0].status.value == "SUCCEEDED"
