from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

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
