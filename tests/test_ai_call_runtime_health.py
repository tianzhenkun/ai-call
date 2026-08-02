from __future__ import annotations

import json
from importlib import import_module
from inspect import signature

import pytest

from app.api.v1.ai_call.controller import ai_call_health


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
