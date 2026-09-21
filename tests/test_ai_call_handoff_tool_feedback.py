from __future__ import annotations

import asyncio
import base64
from datetime import datetime, timezone

import pytest

from app.services.ai_call import agent_runner as runner_module
from app.services.ai_call.agent_runner import RealtimeCallAgentRunner
from app.services.ai_call.event_store import InMemoryEventStore
from app.services.ai_call.handoff_trigger_service import (
    AiCallHandoffTriggerService,
    AiCallHandoffTriggerWorker,
    RuleBasedHandoffIntentClassifier,
)
from app.services.ai_call.providers.base import ProviderEvent
from app.services.ai_call.session_registry import (
    CallSession,
    CallSessionStatus,
    InMemorySessionRegistry,
)


class Provider:
    def __init__(self):
        self.results = []
        self.responses = []
        self.closed = False
        self.result_received = asyncio.Event()

    async def submit_tool_result(self, tool_call_id, output):
        assert not self.closed
        self.results.append((tool_call_id, output))
        self.result_received.set()

    async def create_response(self, input_text=None):
        assert not self.closed
        self.responses.append(input_text)

    async def cancel_response(self):
        pass

    async def close(self):
        self.closed = True


class Publisher:
    def __init__(self):
        self.frames = []

    async def publish_audio(self, call_id, frame):
        self.frames.append(frame)

    async def stop_audio(self, call_id):
        pass


def make_runner():
    registry = InMemorySessionRegistry()
    registry.add(CallSession(
        call_id="call-handoff-result",
        room_name="room-handoff-result",
        participant_identity="sip-customer",
        status=CallSessionStatus.AI_SPEAKING,
        effective_config={"barge_in_enabled": True},
    ))
    provider = Provider()
    publisher = Publisher()
    store = InMemoryEventStore()
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=store,
        audio_publisher=publisher,
    )
    runner._providers["call-handoff-result"] = provider
    turn = runner._pending_turn("call-handoff-result")
    turn.transcript_parts = ["请转人工。"]
    turn.stopped_at = datetime.now(timezone.utc)
    turn.response_requested = True
    runner._mark_response_started("call-handoff-result", {"response_id": "response-handoff"})
    return runner, provider, publisher, store


async def request_handoff(runner, provider):
    await runner._handle_handoff_tool_done(
        "call-handoff-result",
        provider,
        ProviderEvent(type="tool_call_done", payload={
            "call_id": "tool-handoff",
            "name": "request_handoff",
            "arguments": {"reason": "customer_request"},
        }),
    )


@pytest.mark.anyio
@pytest.mark.parametrize("event_type,payload", [
    ("handoff_auto_trigger_failed", {"stage": "create_handoff"}),
    ("handoff_auto_trigger_failed", {"stage": "enqueue"}),
    ("handoff_intent_ignored", {"reason": "disabled"}),
    ("handoff_auto_triggered", {"status": "failed", "failureStage": "availability_check"}),
])
async def test_tool_failure_reaches_live_provider_and_resumes_answer(event_type, payload):
    runner, provider, _publisher, store = make_runner()
    try:
        await request_handoff(runner, provider)
        assert provider.results == []
        store.append("call-handoff-result", event_type, "handoff", {
            "toolCallId": "tool-handoff",
            **payload,
        })
        await asyncio.wait_for(provider.result_received.wait(), timeout=0.2)
        await runner._complete_response_and_flush_pending("call-handoff-result", provider)
        assert len(provider.results) == 1
        assert provider.results[0][0] == "tool-handoff"
        assert "未完成转人工" in provider.results[0][1]
        assert len(provider.responses) == 1
    finally:
        await runner.stop("call-handoff-result")


@pytest.mark.anyio
async def test_tool_request_stops_model_audio_while_system_confirms_transfer():
    runner, provider, publisher, _store = make_runner()
    try:
        await request_handoff(runner, provider)
        await runner._publish_model_audio_delta(
            "call-handoff-result",
            ProviderEvent(type="model_audio_delta", payload={
                "response_id": "response-handoff",
                "delta": base64.b64encode(b"\x01\x02" * 240).decode("ascii"),
            }),
        )
        await asyncio.sleep(0)
        assert publisher.frames == []
        assert not await runner._request_response("call-handoff-result", provider)
        assert provider.responses == []
    finally:
        await runner.stop("call-handoff-result")


@pytest.mark.anyio
async def test_original_audio_stays_invalid_after_failure_before_recovery_response_starts():
    runner, provider, publisher, store = make_runner()
    runner._playback_guard("call-handoff-result").current_response_id = None
    try:
        await request_handoff(runner, provider)
        store.append("call-handoff-result", "handoff_auto_trigger_failed", "handoff", {
            "toolCallId": "tool-handoff", "stage": "create_handoff",
        })
        await asyncio.wait_for(provider.result_received.wait(), timeout=0.2)
        await runner._publish_model_audio_delta(
            "call-handoff-result",
            ProviderEvent(type="model_audio_delta", payload={
                "delta": base64.b64encode(b"\x01\x02" * 240).decode("ascii"),
            }),
        )
        await asyncio.sleep(0)
        assert publisher.frames == []
        await runner._complete_response_and_flush_pending("call-handoff-result", provider)
        runner._mark_response_started("call-handoff-result", {"response_id": "response-recovered"})
        await runner._publish_model_audio_delta(
            "call-handoff-result",
            ProviderEvent(type="model_audio_delta", payload={
                "response_id": "response-recovered",
                "delta": base64.b64encode(b"\x01\x02" * 240).decode("ascii"),
            }),
        )
        await asyncio.sleep(0)
        assert len(publisher.frames) == 1
    finally:
        await runner.stop("call-handoff-result")


@pytest.mark.anyio
async def test_unrelated_and_duplicate_outcomes_do_not_resolve_another_tool():
    runner, provider, _publisher, store = make_runner()
    try:
        await request_handoff(runner, provider)
        store.append("call-handoff-result", "handoff_auto_trigger_failed", "handoff", {
            "toolCallId": "other-tool", "stage": "create_handoff",
        })
        await asyncio.sleep(0)
        assert provider.results == []
        for _ in range(2):
            store.append("call-handoff-result", "handoff_auto_trigger_failed", "handoff", {
                "toolCallId": "tool-handoff", "stage": "create_handoff",
            })
        await asyncio.wait_for(provider.result_received.wait(), timeout=0.2)
        assert len(provider.results) == 1
    finally:
        await runner.stop("call-handoff-result")


@pytest.mark.anyio
@pytest.mark.parametrize("reason", ["duplicate_trigger", "active_handoff_exists"])
async def test_existing_handoff_is_not_misreported_as_creation_success_or_failure(reason):
    runner, provider, _publisher, store = make_runner()
    try:
        await request_handoff(runner, provider)
        store.append("call-handoff-result", "handoff_intent_ignored", "handoff", {
            "toolCallId": "tool-handoff", "reason": reason,
        })
        await asyncio.wait_for(provider.result_received.wait(), timeout=0.2)
        assert "未重复创建" in provider.results[0][1]
        assert "是否接通仍以系统状态为准" in provider.results[0][1]
    finally:
        await runner.stop("call-handoff-result")


@pytest.mark.anyio
async def test_successful_transfer_uses_system_prompt_not_closed_provider():
    runner, provider, _publisher, store = make_runner()
    await request_handoff(runner, provider)
    await runner.stop("call-handoff-result")
    store.append("call-handoff-result", "handoff_auto_triggered", "handoff", {
        "toolCallId": "tool-handoff", "status": "requested",
    })
    await asyncio.sleep(0)
    assert provider.closed
    assert provider.results == []
    assert provider.responses == []
    assert runner._handoff_tool_results == {}
    assert runner._handoff_tool_tasks == {}
    assert runner._receive_handoff_tool_result not in store._listeners


@pytest.mark.anyio
@pytest.mark.parametrize("late_error", [False, True])
@pytest.mark.parametrize("replace_provider", [False, True])
async def test_inflight_response_cannot_fail_or_reactivate_agent_after_handoff(
    late_error, replace_provider
):
    runner, provider, _publisher, store = make_runner()
    call_id = "call-handoff-result"
    entered = asyncio.Event()
    released = asyncio.Event()
    scheduled_ends = []
    runner.call_end_scheduler = lambda *args: scheduled_ends.append(args)

    async def create_response(_input=None):
        assert not provider.closed
        entered.set()
        await released.wait()
        if late_error:
            raise RuntimeError("sent 1000 (OK); then received 1000 (OK)")

    provider.create_response = create_response
    runner._clear_response_lifecycle(call_id)
    pending = asyncio.create_task(runner._request_response(call_id, provider))
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        await runner.suspend_for_handoff(call_id)
        if replace_provider:
            runner._providers[call_id] = Provider()
            runner._mark_response_started(call_id, {"response_id": "new-response"})
        released.set()
        assert not await asyncio.wait_for(pending, timeout=1)
        assert not await runner._request_response(call_id, provider)
        assert runner.registry.get(call_id).status != CallSessionStatus.FAILED
        assert scheduled_ends == []
        assert not any(event.type == "session_failed" for event in store.list_all(call_id))
        if replace_provider:
            assert runner._response_lifecycles[call_id].active
            assert runner._playback_guards[call_id].current_response_id == "new-response"
        else:
            assert call_id not in runner._response_lifecycles
            assert call_id not in runner._playback_guards
        if late_error:
            discarded = [event for event in store.list_all(call_id)
                         if event.type == "model_response_create_discarded"]
            assert len(discarded) == 1
            assert "sent 1000 (OK)" in discarded[0].payload["message"]
    finally:
        released.set()
        await asyncio.gather(pending, return_exceptions=True)
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_missing_worker_result_reports_unconfirmed_instead_of_success(monkeypatch):
    monkeypatch.setattr(runner_module, "HANDOFF_TOOL_RESULT_TIMEOUT_SECONDS", 0)
    runner, provider, _publisher, store = make_runner()
    try:
        await request_handoff(runner, provider)
        await asyncio.wait_for(provider.result_received.wait(), timeout=0.2)
        assert "尚未确认" in provider.results[0][1]
        assert any(event.type == "handoff_tool_result_timeout"
                   for event in store.list_all("call-handoff-result"))
    finally:
        await runner.stop("call-handoff-result")


@pytest.mark.anyio
@pytest.mark.parametrize("queue_full", [False, True])
async def test_real_worker_failure_is_delivered_even_when_emitted_during_enqueue(queue_full):
    runner, provider, _publisher, store = make_runner()

    def unexpected_database(*_args):
        raise AssertionError("禁用或队列已满的请求不应访问数据库")

    service = AiCallHandoffTriggerService(
        unexpected_database, unexpected_database, RuleBasedHandoffIntentClassifier(), enabled=False
    )
    worker = AiCallHandoffTriggerWorker(service, queue_max_size=1)
    worker.attach_event_store(store)
    try:
        if queue_full:
            store.append("another-call", "handoff_tool_requested", "agent", {
                "toolCallId": "other-tool", "reason": "customer_request",
            })
        else:
            await worker.start()
        await request_handoff(runner, provider)
        await asyncio.wait_for(provider.result_received.wait(), timeout=0.2)
        assert "未完成转人工" in provider.results[0][1]
        await runner._complete_response_and_flush_pending("call-handoff-result", provider)
        assert len(provider.responses) == 1
    finally:
        worker.detach_all()
        await worker.stop()
        await runner.stop("call-handoff-result")


@pytest.mark.anyio
async def test_valid_handoff_request_cancels_pending_policy_goodbye():
    runner, provider, _publisher, _store = make_runner()
    scheduled = []
    runner.call_end_scheduler = lambda call_id, reason: scheduled.append((call_id, reason))
    runner._prepare_policy_call_end("call-handoff-result", end_reason="policy_turn_limit")
    runner._pending_call_ends["call-handoff-result"].final_response_started = True
    try:
        await request_handoff(runner, provider)
        await runner._apply_provider_event(
            "call-handoff-result", provider, "model_response_done", datetime.now(timezone.utc), {}
        )
        assert "call-handoff-result" not in runner._pending_call_ends
        assert scheduled == []
    finally:
        await runner.stop("call-handoff-result")
