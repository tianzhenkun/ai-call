from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.services.ai_call.event_store import InMemoryEventStore
from app.services.ai_call.handoff_trigger_service import (
    AiCallHandoffTriggerService,
    AiCallHandoffTriggerWorker,
)


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def handoff_dependencies():
    db = MagicMock()
    session_factory = MagicMock()
    session_factory.return_value.__aenter__.return_value = db
    handoff_service = SimpleNamespace(
        get_current_handoff=AsyncMock(return_value=None),
        create_handoff=AsyncMock(return_value={"handoffId": "handoff-1", "status": "requested"}),
    )
    trigger_service = AiCallHandoffTriggerService(
        session_factory,
        lambda _db: handoff_service,
        SimpleNamespace(classify=AsyncMock(side_effect=AssertionError("工具请求不应进入意图分类"))),
    )
    return trigger_service, handoff_service


@asynccontextmanager
async def _running_worker(trigger_service, *, store=None, **options):
    store = store or InMemoryEventStore()
    worker = AiCallHandoffTriggerWorker(trigger_service, **options)
    worker.attach_event_store(store)
    await worker.start()
    try:
        yield worker, store
    finally:
        worker.detach_all()
        await worker.stop()


def _request(store, tool_call_id="tool-1", *, reason="customer_request", call_id="call-1"):
    return store.append(
        call_id,
        "handoff_tool_requested",
        "agent",
        {"toolCallId": tool_call_id, "reason": reason},
    )


def _outcome(store, event_type, tool_call_id="tool-1"):
    events = [event for event in store.list_all("call-1") if event.type == event_type]
    assert len(events) == 1
    assert events[0].source == "handoff"
    assert events[0].payload.get("toolCallId") == tool_call_id
    return events[0].payload


@pytest.mark.anyio
@pytest.mark.parametrize("reason", ["disabled", "duplicate_trigger", "active_handoff_exists"])
async def test_ignored_tool_request_keeps_tool_call_id(handoff_dependencies, reason):
    trigger_service, handoff_service = handoff_dependencies
    store = InMemoryEventStore()
    if reason == "disabled":
        trigger_service.enabled = False
    elif reason == "duplicate_trigger":
        store.append("call-1", "handoff_auto_triggered", "handoff", {"status": "requested"})
    else:
        handoff_service.get_current_handoff.return_value = {"handoffId": "existing-handoff"}

    async with _running_worker(trigger_service, store=store) as (worker, store):
        _request(store)
        await asyncio.wait_for(worker.flush_pending(), timeout=1)

    assert _outcome(store, "handoff_intent_ignored")["reason"] == reason
    handoff_service.create_handoff.assert_not_awaited()


@pytest.mark.anyio
async def test_create_handoff_failure_keeps_tool_call_id(handoff_dependencies):
    trigger_service, handoff_service = handoff_dependencies
    handoff_service.create_handoff.side_effect = RuntimeError("创建转人工事务失败")

    async with _running_worker(trigger_service) as (worker, store):
        _request(store)
        await asyncio.wait_for(worker.flush_pending(), timeout=1)

    payload = _outcome(store, "handoff_auto_trigger_failed")
    assert payload["stage"] == "create_handoff"
    assert payload["errorType"] == "RuntimeError"
    assert payload["message"] == "创建转人工事务失败"


@pytest.mark.anyio
@pytest.mark.parametrize("transcript", ["可以", "请再解释一下"])
async def test_confirmation_outcome_keeps_original_tool_call_id(handoff_dependencies, transcript):
    trigger_service, handoff_service = handoff_dependencies
    handoff_service.create_handoff.side_effect = RuntimeError("确认后创建转人工失败")

    async with _running_worker(trigger_service, transcript_trigger_enabled=True) as (worker, store):
        _request(store, reason="business_escalation")
        await asyncio.wait_for(worker.flush_pending(), timeout=1)
        _outcome(store, "handoff_confirmation_requested")
        store.append("call-1", "user_transcript_done", "agent", {"text": transcript})
        await asyncio.wait_for(worker.flush_pending(), timeout=1)

    if transcript == "可以":
        payload = _outcome(store, "handoff_auto_trigger_failed")
        assert payload["stage"] == "create_handoff"
        assert payload["message"] == "确认后创建转人工失败"
    else:
        assert _outcome(store, "handoff_intent_ignored")["reason"] == "handoff_confirmation_unresolved"
        handoff_service.create_handoff.assert_not_awaited()


def test_full_queue_emits_correlated_failure(handoff_dependencies):
    trigger_service, _ = handoff_dependencies
    store = InMemoryEventStore()
    worker = AiCallHandoffTriggerWorker(trigger_service, queue_max_size=1)
    worker.attach_event_store(store)
    try:
        _request(store, "tool-first")
        _request(store)
    finally:
        worker.detach_all()

    payload = _outcome(store, "handoff_auto_trigger_failed")
    assert payload["stage"] == "enqueue"
    assert payload["errorType"] == "QueueFull"
    assert payload["message"]
    assert worker.dropped_count == 1


@pytest.mark.anyio
async def test_exhausted_worker_retries_emit_one_correlated_failure(handoff_dependencies):
    trigger_service, handoff_service = handoff_dependencies
    handoff_service.get_current_handoff.side_effect = RuntimeError("查询转人工状态失败")

    async with _running_worker(trigger_service) as (worker, store):
        _request(store)
        await asyncio.wait_for(worker.flush_pending(), timeout=1)

    payload = _outcome(store, "handoff_auto_trigger_failed")
    assert payload["stage"] == "retry_exhausted"
    assert payload["errorType"] == "RuntimeError"
    assert payload["message"] == "查询转人工状态失败"
    assert handoff_service.get_current_handoff.await_count == 2
    assert worker.failed_count == 2


@pytest.mark.anyio
async def test_full_retry_queue_emits_correlated_failure(handoff_dependencies):
    trigger_service, handoff_service = handoff_dependencies

    async with _running_worker(trigger_service, queue_max_size=1) as (worker, store):
        async def get_current_handoff(call_id):
            if call_id == "call-1":
                _request(store, "tool-next", call_id="call-next")
                raise RuntimeError("查询转人工状态失败")
            return None

        handoff_service.get_current_handoff.side_effect = get_current_handoff
        _request(store)
        await asyncio.wait_for(worker.flush_pending(), timeout=1)

    payload = _outcome(store, "handoff_auto_trigger_failed")
    assert payload["stage"] == "retry_enqueue"
    assert payload["errorType"] == "QueueFull"
    assert "查询转人工状态失败" in payload["message"]
    assert worker.dropped_count == 1


@pytest.mark.anyio
async def test_recovered_retry_does_not_emit_terminal_failure(handoff_dependencies):
    trigger_service, handoff_service = handoff_dependencies
    handoff_service.get_current_handoff.side_effect = [RuntimeError("临时查询失败"), None]

    async with _running_worker(trigger_service) as (worker, store):
        _request(store)
        await asyncio.wait_for(worker.flush_pending(), timeout=1)

    assert _outcome(store, "handoff_auto_triggered")["status"] == "requested"
    assert not any(event.type == "handoff_auto_trigger_failed" for event in store.list_all("call-1"))
    assert handoff_service.get_current_handoff.await_count == 2


@pytest.mark.anyio
async def test_failed_handoff_preserves_failure_details_in_trigger_outcome(handoff_dependencies):
    trigger_service, handoff_service = handoff_dependencies
    handoff_service.create_handoff.return_value = {
        "handoffId": "handoff-failed",
        "status": "failed",
        "failureStage": "availability_check",
        "failureMessage": "当前场景没有在线可接范围坐席",
    }

    async with _running_worker(trigger_service) as (worker, store):
        _request(store)
        await asyncio.wait_for(worker.flush_pending(), timeout=1)

    payload = _outcome(store, "handoff_auto_triggered")
    assert payload["status"] == "failed"
    assert payload["failureStage"] == "availability_check"
    assert payload["failureMessage"] == "当前场景没有在线可接范围坐席"
