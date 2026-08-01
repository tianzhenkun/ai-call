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
    DeterministicWebProviderStub,
)
from app.services.ai_call.runtime_control.runtime_service import (
    RuntimeControlService,
    RuntimeRegistry,
    _FailClosedProvider,
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
