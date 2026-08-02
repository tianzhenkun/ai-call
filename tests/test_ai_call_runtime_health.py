from __future__ import annotations

import asyncio
import json
from importlib import import_module
from inspect import signature
from types import SimpleNamespace

import pytest

from app.api.v1.ai_call.controller import ai_call_health
from app.services.ai_call.runtime_control.health import (
    RuntimeTaskState,
    RuntimeWorkerHealth,
)
from app.services.ai_call.runtime_control.runtime_service import (
    RuntimeControlService,
    RuntimeRegistry,
)

WORKER_ID = "runtime-test:12345678-1234-5678-1234-567812345678"


class FakeLocalHandle:
    def __init__(self) -> None:
        self.fail_closed_count = 0

    async def fail_closed(self) -> None:
        self.fail_closed_count += 1


async def _return_unexpectedly() -> None:
    return None


def _runtime_worker_health_type():
    module = import_module("app.services.ai_call.runtime_control.health")
    health_type = getattr(module, "RuntimeWorkerHealth", None)
    assert health_type is not None, "RuntimeWorkerHealth is not implemented"
    return health_type


def test_runtime_worker_health_records_sanitized_failure() -> None:
    health = _runtime_worker_health_type()()
    health.mark_failed("runtime-a:uuid", "runtime_task_exited")

    snapshot = health.snapshot()

    assert snapshot.worker_id == "runtime-a:uuid"
    assert snapshot.state.value == "failed"
    assert snapshot.error_code == "runtime_task_exited"


@pytest.mark.anyio
async def test_ai_call_health_returns_503_when_runtime_task_failed() -> None:
    assert "runtime_health" in signature(ai_call_health).parameters
    health = _runtime_worker_health_type()()
    health.mark_failed("runtime-a:uuid", "runtime_task_exited")

    response = await ai_call_health(health)

    assert response.status_code == 503
    assert json.loads(response.body) == {
        "status": "error",
        "runtime": "failed",
        "errorCode": "runtime_task_exited",
    }


@pytest.mark.anyio
async def test_ai_call_health_keeps_ok_contract_when_runtime_not_configured() -> None:
    assert "runtime_health" in signature(ai_call_health).parameters

    response = await ai_call_health(_runtime_worker_health_type()())

    assert response == {"status": "ok"}


@pytest.mark.anyio
async def test_runtime_task_exit_marks_failed_and_fail_closes_handles() -> None:
    health = RuntimeWorkerHealth()
    handle = FakeLocalHandle()
    registry = RuntimeRegistry(
        owner_fencing_tokens={"call-1": 7},
        local_handles={"call-1": handle},
    )
    service = RuntimeControlService(
        worker_id=WORKER_ID,
        registry=registry,
        session_factory=None,
        provider=None,
        health=health,
    )
    health.mark_running(WORKER_ID)
    task = asyncio.create_task(_return_unexpectedly())
    service._task = task

    service._monitor_runtime_task(task)
    await task
    await asyncio.sleep(0)
    assert service._supervision_task is not None
    await service._supervision_task

    assert health.snapshot().state == RuntimeTaskState.FAILED
    assert health.snapshot().error_code == "runtime_task_exited"
    assert handle.fail_closed_count == 1
    assert registry.owner_fencing_tokens == {}
    assert registry.local_handles == {}


@pytest.mark.anyio
async def test_runtime_task_unexpected_cancellation_is_unhealthy() -> None:
    health = RuntimeWorkerHealth()
    service = RuntimeControlService(
        worker_id=WORKER_ID,
        registry=RuntimeRegistry(),
        session_factory=None,
        provider=None,
        health=health,
    )
    health.mark_running(WORKER_ID)
    task = asyncio.create_task(asyncio.Event().wait())
    service._task = task
    service._monitor_runtime_task(task)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0)
    assert service._supervision_task is not None
    await service._supervision_task

    assert health.snapshot().state == RuntimeTaskState.FAILED
    assert health.snapshot().error_code == "runtime_task_cancelled"


@pytest.mark.anyio
async def test_runtime_stop_does_not_report_expected_task_exit_as_failure() -> None:
    health = RuntimeWorkerHealth()
    service = RuntimeControlService(
        worker_id=WORKER_ID,
        registry=RuntimeRegistry(),
        session_factory=None,
        provider=None,
        health=health,
    )
    health.mark_running(WORKER_ID)
    task = asyncio.create_task(service._stop_event.wait())
    service._task = task
    service._monitor_runtime_task(task)

    await service.stop()

    assert health.snapshot().state == RuntimeTaskState.STOPPED
    assert health.snapshot().error_code is None
    assert service._supervision_task is None


@pytest.mark.anyio
async def test_runtime_lifecycle_injects_process_health(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.services.ai_call.runtime_control import lifecycle

    captured: dict[str, object] = {}

    async def valid_database(_session_factory) -> tuple[str, str]:
        return "runtime-test", "public"

    class FakeService:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        async def start(self) -> None:
            return None

    monkeypatch.setattr(lifecycle, "RuntimeControlService", FakeService)
    monkeypatch.setattr(
        lifecycle,
        "validate_db_only_runtime_database",
        valid_database,
    )
    settings = SimpleNamespace(
        AI_CALL_RUNTIME_INSTANCE_ID="runtime-test",
        AI_CALL_RUNTIME_CAPACITY=2,
        AI_CALL_RUNTIME_CLEANUP_CAPACITY=1,
        AI_CALL_RUNTIME_WORKER_LEASE_SECONDS=15,
        AI_CALL_RUNTIME_OWNER_LEASE_SECONDS=15,
        AI_CALL_RUNTIME_FAIL_CLOSED_MARGIN_SECONDS=3,
        AI_CALL_RUNTIME_END_SCAN_INTERVAL_SECONDS=0.5,
    )
    session_factory = SimpleNamespace(kw={"bind": object()})

    await lifecycle.start_runtime_control_lifecycle(settings, session_factory)

    assert captured["health"] is lifecycle.default_runtime_worker_health
