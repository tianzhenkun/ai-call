from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import httpx
import pytest

from app.services.ai_call import agent_runner as agent_runner_module
from app.services.ai_call.agent_runner import (
    CALL_POLICY_FINAL_INPUT,
    RealtimeCallAgentRunner,
)
from app.services.ai_call.event_store import InMemoryEventStore
from app.services.ai_call.providers.base import ProviderEvent
from app.services.ai_call.record_service import PERSISTED_EVENT_TYPES
from app.services.ai_call.session_registry import (
    CallSession,
    CallSessionStatus,
    InMemorySessionRegistry,
)
from app.services.ai_call.transcript_trust import CustomerSpeechClassifier, CustomerSpeechDecision


class FakeProvider:
    def __init__(self) -> None:
        self.created_responses: list[str | None] = []
        self.tool_results: list[tuple[str, str]] = []

    async def create_response(self, input_text: str | None = None) -> None:
        self.created_responses.append(input_text)

    async def submit_tool_result(self, tool_call_id: str, output: str) -> None:
        self.tool_results.append((tool_call_id, output))

    async def close(self) -> None:
        pass


def _runner(classifier=None, *, entry_type=None, participant_identity="sip-customer") -> tuple[RealtimeCallAgentRunner, FakeProvider, list[tuple[str, str]]]:
    registry = InMemorySessionRegistry()
    registry.add(
        CallSession(
            call_id="call-policy",
            room_name="room-policy",
            participant_identity=participant_identity,
            status=CallSessionStatus.CONNECTED,
            effective_config={"barge_in_enabled": True},
            entry_type=entry_type,
        )
    )
    provider = FakeProvider()
    scheduled: list[tuple[str, str]] = []
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=InMemoryEventStore(),
        call_end_scheduler=lambda call_id, reason: scheduled.append((call_id, reason)),
        customer_speech_classifier=classifier,
    )
    runner._providers["call-policy"] = provider
    return runner, provider, scheduled


class SpeechClassifier:
    def __init__(self, speech="customer", topic="related", customer_text=None, error=None):
        self.speech, self.topic, self.customer_text, self.error = speech, topic, customer_text, error
        self.inputs = []

    async def classify(self, **kwargs):
        self.inputs.append(kwargs)
        if self.error:
            raise self.error
        return CustomerSpeechDecision(
            speech=self.speech,
            customer_text=(self.customer_text or kwargs["transcript"]) if self.speech == "customer" else "",
            topic=self.topic, confidence=0.95,
        )


async def _review_turn(runner, provider, text, item_id="latest"):
    turn = runner._pending_turn("call-policy", reset_if_finished=True)
    turn.stopped_at = datetime.now(timezone.utc)
    runner._queue_transcript_review("call-policy", provider, ProviderEvent(
        type="user_transcript_done", payload={"transcript": text, "item_id": item_id},
    ))
    task = runner._transcript_review_tasks.get("call-policy")
    if task:
        await task
    await runner.wait("call-policy")
    return turn


@pytest.mark.anyio
@pytest.mark.parametrize("entry_type,active_response", [("outbound", False), ("web", False), ("outbound", True)])
async def test_no_review_empty_turn_restores_outbound_silence_policy(monkeypatch, entry_type, active_response):
    monkeypatch.setattr(agent_runner_module, "CALL_POLICY_SILENCE_SECONDS", .01)
    runner, provider, _ = _runner(entry_type=entry_type, participant_identity="caller-test")
    now = datetime.now(timezone.utc)
    try:
        runner._arm_silence_watchdog("call-policy")
        await runner._handle_user_speech_started("call-policy", provider, now)
        assert "call-policy" not in runner._silence_watchdog_tasks
        runner._response_lifecycle("call-policy").active = active_response
        await runner._handle_user_speech_stopped("call-policy", provider, now)
        await asyncio.sleep(.03)
        expected = [agent_runner_module.CALL_POLICY_SILENCE_INPUTS[0]] if entry_type == "outbound" and not active_response else []
        assert provider.created_responses == expected
        assert runner._customer_turn_counts.get("call-policy", 0) == 0
    finally:
        await runner.stop("call-policy")


@pytest.mark.anyio
@pytest.mark.parametrize("speech,error", [("background", None), ("uncertain", None), ("uncertain", TimeoutError())])
async def test_background_and_uncertain_turns_cannot_spend_budget_or_authorize_end(speech, error):
    runner, provider, scheduled = _runner(SpeechClassifier(speech=speech, error=error))
    runner._customer_turn_counts["call-policy"] = 14
    runner._off_topic_turn_event_ids["call-policy"] = ["previous-1", "previous-2"]
    await _review_turn(runner, provider, "本次列车终点站。")
    assert runner._customer_turn_counts["call-policy"] == 14
    assert not runner._off_topic_turn_event_ids.get("call-policy")
    assert not runner._pending_call_ends and not scheduled
    assert all(not runner._accepts_call_end_tool("call-policy", reason)
               for reason in ("policy_limit", "customer_end", "task_completed"))
    if speech == "uncertain":
        assert len(provider.created_responses) == 1
        assert "再说一遍" in provider.created_responses[0]
    else:
        assert provider.created_responses == []
    await runner.stop("call-policy")


@pytest.mark.anyio
async def test_mixed_speech_commits_only_customer_span_with_original_evidence():
    classifier = SpeechClassifier(customer_text="啊，没事，你说。")
    runner, provider, _ = _runner(classifier)
    turn = await _review_turn(runner, provider, "使用电子设备时。啊，没事，你说。")
    assert turn.transcript == "啊，没事，你说。"
    assert runner._customer_turn_counts["call-policy"] == 1
    assert runner._off_topic_turn_event_ids["call-policy"] == []
    events = runner.event_store.list_all("call-policy")
    admitted = next(event for event in events if event.type == "user_transcript_done")
    assert admitted.payload["originalTranscript"] == "使用电子设备时。啊，没事，你说。"
    assert admitted.payload["transcript"] == "啊，没事，你说。"
    assert "customer_speech_classified" in PERSISTED_EVENT_TYPES
    assert "call_policy_customer_turn" in PERSISTED_EVENT_TYPES
    assert classifier.inputs[0]["transcript"] == admitted.payload["originalTranscript"]
    assert "啊，没事，你说。" in provider.created_responses[0]
    assert "使用电子设备" not in provider.created_responses[0]


@pytest.mark.anyio
async def test_rejected_background_recovers_pre_stopped_audio_without_confirming_interrupt():
    runner, provider, _ = _runner(SpeechClassifier(speech="background"))
    turn = runner._pending_turn("call-policy")
    turn.interrupt_candidate = True
    runner._playback_guard("call-policy").audio_stop_requested = True
    await _review_turn(runner, provider, "本次列车终点站。", "background")
    assert not turn.interrupt_confirmed
    assert not runner._customer_turn_counts
    assert len(provider.created_responses) == 1
    assert "接着被打断的内容" in provider.created_responses[0]
    assert not any(event.type == "interrupt_confirmed"
                   for event in runner.event_store.list_all("call-policy"))


@pytest.mark.anyio
async def test_only_three_consecutive_committed_off_topic_turns_authorize_policy_end():
    classifier = SpeechClassifier(topic="off_topic")
    runner, provider, _ = _runner(classifier)
    for index in range(3):
        await _review_turn(runner, provider, f"先别介绍产品，跟我聊聊第{index}部电影。", str(index))
        assert runner._accepts_call_end_tool("call-policy", "policy_limit") == (index == 2)
        await runner._complete_response_and_flush_pending("call-policy", provider)
    # 回到业务后不沿用上一段离题证据。
    classifier.topic = "related"
    await _review_turn(runner, provider, "你们产品怎么收费？", "back-to-business")
    assert not runner._accepts_call_end_tool("call-policy", "policy_limit")
    assert runner._customer_turn_counts["call-policy"] == 4


@pytest.mark.anyio
async def test_transcript_review_does_not_block_provider_events_or_survive_handoff():
    started, release = asyncio.Event(), asyncio.Event()

    class SlowClassifier:
        async def classify(self, **kwargs):
            started.set()
            await release.wait()
            return CustomerSpeechDecision(speech="customer", customer_text=kwargs["transcript"])

    runner, provider, _ = _runner(SlowClassifier())

    async def receive_events():
        yield ProviderEvent(type="user_transcript_done", payload={"transcript": "挂了吧。"})
        await started.wait()
        yield ProviderEvent(type="model_session_updated", payload={"marker": "still-receiving"})

    provider.receive_events = receive_events
    await asyncio.wait_for(runner._consume_provider_events("call-policy", provider), timeout=1)
    assert any(event.payload.get("marker") == "still-receiving"
               for event in runner.event_store.list_all("call-policy"))
    task = runner._transcript_review_tasks["call-policy"]
    await runner.stop("call-policy")
    release.set()
    assert task.cancelled()
    assert provider.created_responses == []
    assert not any(event.type == "call_end_intent_detected"
                   for event in runner.event_store.list_all("call-policy"))


@pytest.mark.anyio
async def test_review_combines_new_final_fragments_and_discards_superseded_result():
    started, release = asyncio.Event(), asyncio.Event()

    class UpdatingClassifier:
        async def classify(self, **kwargs):
            if kwargs["transcript"] == "本次列车终点站。":
                started.set()
                await release.wait()
            return CustomerSpeechDecision(speech="customer", customer_text="你继续说。", topic="related")

    runner, provider, _ = _runner(UpdatingClassifier())
    turn = runner._pending_turn("call-policy")
    turn.stopped_at = datetime.now(timezone.utc)
    runner._queue_transcript_review("call-policy", provider, ProviderEvent(
        type="user_transcript_done", payload={"transcript": "本次列车终点站。", "item_id": "a"},
    ))
    await started.wait()
    old_task = runner._transcript_review_tasks["call-policy"]
    await _review_turn(runner, provider, "你继续说。", "b")
    release.set()
    assert old_task.cancelled()
    assert turn.transcript == "你继续说。"
    assert runner._customer_turn_counts["call-policy"] == 1
    admitted = [event for event in runner.event_store.list_all("call-policy") if event.type == "user_transcript_done"]
    assert len(admitted) == 1
    assert admitted[0].payload["originalTranscript"] == "本次列车终点站。 你继续说。"


@pytest.mark.parametrize("data", [
    {"sources": ["customer", "customer"], "topic": "related", "confidence": .99},
    {"sources": ["customer"], "topic": "off_topic", "confidence": True},
    {"sources": ["customer"], "topic": "off_topic", "confidence": float("nan")},
])
def test_speech_decision_rejects_fabricated_or_invalid_evidence(data):
    with pytest.raises(ValueError):
        CustomerSpeechClassifier.parse_decision(data, transcript="你说")


@pytest.mark.anyio
async def test_new_speech_waits_for_its_own_final_and_old_classification_cannot_end_call():
    started = asyncio.Event()

    class InterruptibleClassifier:
        async def classify(self, **kwargs):
            if kwargs["transcript"] == "挂了吧。":
                started.set()
                await asyncio.Event().wait()
            return CustomerSpeechDecision(speech="customer", customer_text="别挂，我还要问。", topic="related")

    runner, provider, scheduled = _runner(InterruptibleClassifier())
    now = datetime.now(timezone.utc)
    await runner._handle_user_speech_started("call-policy", provider, now, speech_item_id="first")
    await runner._handle_user_speech_stopped("call-policy", provider, now)
    runner._queue_transcript_review("call-policy", provider, ProviderEvent(
        type="user_transcript_done", payload={"transcript": "挂了吧。", "item_id": "first"},
    ))
    await started.wait()
    await runner._handle_user_speech_started("call-policy", provider, datetime.now(timezone.utc), speech_item_id="second")
    await runner._handle_user_speech_stopped("call-policy", provider, datetime.now(timezone.utc))
    assert not runner._pending_call_ends
    assert provider.created_responses == []
    await _review_turn(runner, provider, "别挂，我还要问。", "second")
    assert not runner._pending_call_ends and not scheduled
    assert runner._customer_turn_counts["call-policy"] == 1
    assert not any(event.type == "call_end_intent_detected" for event in runner.event_store.list_all("call-policy"))


@pytest.mark.anyio
async def test_missing_final_transcript_clarifies_without_spending_budget(monkeypatch):
    monkeypatch.setattr(CustomerSpeechClassifier, "TIMEOUT_SECONDS", 0)
    monkeypatch.setattr(agent_runner_module, "FINAL_TRANSCRIPT_TIMEOUT_SECONDS", 0.01)
    classifier = SpeechClassifier()
    runner, provider, _ = _runner(classifier)
    now = datetime.now(timezone.utc)
    await runner._handle_user_speech_started("call-policy", provider, now, speech_item_id="missing")
    await runner._handle_user_speech_stopped("call-policy", provider, now)
    await runner._transcript_review_tasks["call-policy"]
    await runner.wait("call-policy")
    assert classifier.inputs == []
    assert len(provider.created_responses) == 1
    assert "再说一遍" in provider.created_responses[0]
    assert not runner._customer_turn_counts
    assert not runner._pending_call_ends
    assert any(event.payload.get("semanticReason") == "final_transcript_timeout"
               for event in runner.event_store.list_all("call-policy"))


@pytest.mark.anyio
async def test_classifier_deadline_and_background_context_are_enforced(monkeypatch):
    monkeypatch.setattr(CustomerSpeechClassifier, "TIMEOUT_SECONDS", 0.01)
    received = []

    class NeverReturnsClassifier:
        async def classify(self, **kwargs):
            received.append(kwargs)
            await asyncio.Event().wait()

    runner, provider, _ = _runner(NeverReturnsClassifier())
    runner.event_store.append("call-policy", "user_transcript_done", "provider", {
        "transcript": "广播样本", "speechSource": "background", "semanticAction": "reject",
    })
    await asyncio.wait_for(_review_turn(runner, provider, "刚才说的是什么？"), timeout=1)
    assert received[0]["recent_dialogue"][-1]["role"] == "background"
    assert not runner._customer_turn_counts and not runner._pending_call_ends
    classified = next(event for event in runner.event_store.list_all("call-policy")
                      if event.type == "customer_speech_classified")
    assert classified.payload["classificationError"]["type"] == "TimeoutError"
    assert "再说一遍" in provider.created_responses[0]


@pytest.mark.anyio
async def test_duplicate_final_from_committed_turn_does_not_join_next_turn():
    runner, provider, _ = _runner(SpeechClassifier())
    await _review_turn(runner, provider, "你们有知识库吗？", "first")
    await runner._complete_response_and_flush_pending("call-policy", provider)
    now = datetime.now(timezone.utc)
    await runner._handle_user_speech_started("call-policy", provider, now, speech_item_id="second")
    runner._queue_transcript_review("call-policy", provider, ProviderEvent(
        type="user_transcript_done", payload={"transcript": "你们有知识库吗？", "item_id": "first"},
    ))
    assert runner._pending_turn("call-policy").transcript_candidates == {}
    await runner._handle_user_speech_stopped("call-policy", provider, now)
    turn = await _review_turn(runner, provider, "怎么收费？", "second")
    assert turn.transcript == "怎么收费？"
    assert runner._customer_turn_counts["call-policy"] == 2


@pytest.mark.anyio
async def test_verified_customer_end_still_speaks_goodbye_before_hangup():
    runner, provider, scheduled = _runner(SpeechClassifier())
    await _review_turn(runner, provider, "不聊了，挂了吧。", "end")
    assert runner._pending_call_ends["call-policy"].end_reason == "customer_end"
    assert scheduled == []
    assert provider.created_responses


@pytest.mark.anyio
async def test_unreviewed_transcript_delta_never_confirms_interrupt_or_end():
    runner, provider, _ = _runner(SpeechClassifier())
    turn = runner._pending_turn("call-policy")
    turn.interrupt_candidate = True
    runner._queue_transcript_review("call-policy", provider, ProviderEvent(
        type="user_transcript_delta", payload={"text": "结束通话", "item_id": "incomplete"},
    ))
    assert not turn.interrupt_confirmed
    assert not runner._pending_call_end_intents
    assert not runner._customer_turn_counts
    assert not any(event.type == "user_transcript_done"
                   for event in runner.event_store.list_all("call-policy"))


@pytest.mark.anyio
async def test_classifier_http_contract_keeps_context_and_uses_only_original_spans(monkeypatch):
    requests = []
    clients = []

    def respond(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps({
            "sources": ["customer"], "customer_text": "我要挂断", "topic": "related", "confidence": .99,
        })}}]})

    client_class = httpx.AsyncClient

    def create_client(**kwargs):
        client = client_class(**kwargs, transport=httpx.MockTransport(respond))
        clients.append(client)
        return client

    monkeypatch.setattr(httpx, "AsyncClient", create_client)
    classifier = CustomerSpeechClassifier(base_url="https://model.example/v1", api_key="test-only", model="qwen-plus")
    for _ in range(2):
        decision = await classifier.classify(
            transcript="你继续说。", business_prompt="GEO 服务",
            recent_dialogue=[{"role": "assistant", "text": "方便聊聊吗？"}], audio_evidence={},
        )
    assert len(clients) == 1
    assert not clients[0].is_closed
    await classifier.aclose()
    assert clients[0].is_closed
    assert requests[0] == requests[1]
    assert decision.customer_text == "你继续说。"
    assert requests[0]["enable_thinking"] is False
    assert requests[0]["response_format"] == {"type": "json_object"}
    data = json.loads(requests[0]["messages"][1]["content"])
    assert data["transcript"] == "你继续说。"
    assert data["clauses"] == ["你继续说。"]
    assert data["recent_dialogue"][0]["text"] == "方便聊聊吗？"


@pytest.mark.anyio
@pytest.mark.parametrize("review_elapsed", [.02, .30])
async def test_review_overlaps_stability_without_releasing_unreviewed_reply(monkeypatch, review_elapsed):
    entered, release = asyncio.Event(), asyncio.Event()
    sleeps = []
    original_sleep = asyncio.sleep

    class GatedClassifier(SpeechClassifier):
        async def classify(self, **kwargs):
            entered.set()
            await release.wait()
            return await super().classify(**kwargs)

    async def observe_stability(seconds):
        if asyncio.current_task().get_name().startswith("ai-call-turn-response-"):
            sleeps.append(seconds)
            await original_sleep(0)
        else:
            await original_sleep(seconds)

    monkeypatch.setattr(agent_runner_module.asyncio, "sleep", observe_stability)
    runner, provider, _ = _runner(GatedClassifier())
    runner.user_turn_stability_delay_seconds = .15
    now = datetime.now(timezone.utc)
    try:
        await runner._handle_user_speech_started("call-policy", provider, now, speech_item_id="turn")
        await runner._handle_user_speech_stopped("call-policy", provider, now)
        runner._queue_transcript_review("call-policy", provider, ProviderEvent(
            type="user_transcript_done", payload={"transcript": "怎么收费？", "item_id": "turn"},
        ))
        await entered.wait()
        turn = runner._pending_turn("call-policy")
        assert turn.stability_updated_at is not None
        assert provider.created_responses == []
        # 只替换已流逝的审核时间，回复调度和所有权限检查仍走真实 runner。
        turn.stability_updated_at = asyncio.get_running_loop().time() - review_elapsed
        release.set()
        await runner._transcript_review_tasks["call-policy"]
        await runner.wait("call-policy")
        assert len(provider.created_responses) == 1
        assert sleeps == [pytest.approx(max(0, .15 - review_elapsed), abs=.03)]
    finally:
        await runner.stop("call-policy")


@pytest.mark.anyio
async def test_new_speech_cancels_remaining_stability_wait():
    runner, provider, _ = _runner(SpeechClassifier())
    runner.user_turn_stability_delay_seconds = .20
    now = datetime.now(timezone.utc)
    try:
        await runner._handle_user_speech_started("call-policy", provider, now, speech_item_id="old")
        await runner._handle_user_speech_stopped("call-policy", provider, now)
        runner._queue_transcript_review("call-policy", provider, ProviderEvent(
            type="user_transcript_done", payload={"transcript": "介绍一下产品。", "item_id": "old"},
        ))
        await runner._transcript_review_tasks["call-policy"]
        response = runner._turn_response_tasks["call-policy"]
        await runner._handle_user_speech_started("call-policy", provider, datetime.now(timezone.utc), speech_item_id="new")
        await asyncio.sleep(.22)
        assert response.cancelled()
        assert provider.created_responses == []
        assert runner._pending_turn("call-policy").stability_updated_at is None
    finally:
        await runner.stop("call-policy")


def test_mixed_source_decision_preserves_multiple_customer_spans_and_uncertain_corrections():
    transcript = "这个怎么收费？欢迎收看新闻。帮我转人工。"
    data = {"sources": ["customer", "background", "customer"], "topic": "related", "confidence": .95}
    decision = CustomerSpeechClassifier.parse_decision(data, transcript=transcript)
    assert decision.customer_text == "这个怎么收费？ 帮我转人工。"
    data["sources"][-1] = "uncertain"
    assert not CustomerSpeechClassifier.parse_decision(data, transcript=transcript).accepted


@pytest.mark.anyio
async def test_three_business_turns_do_not_authorize_off_topic_hangup() -> None:
    runner, provider, scheduled = _runner()
    for text in ("方便，你说。", "有关注。", "主要是销售获客这一块儿吧。"):
        turn = runner._pending_turn("call-policy", reset_if_finished=True)
        await runner._handle_user_transcript(
            "call-policy", provider,
            ProviderEvent(type="user_transcript_done", payload={"transcript": text}),
            datetime.now(timezone.utc),
        )
        turn.stopped_at = datetime.now(timezone.utc)
        await runner._request_response_from_turn("call-policy", provider, turn)
        await runner._complete_response_and_flush_pending("call-policy", provider)

    assert runner._customer_turn_counts["call-policy"] == 3
    await runner._handle_tool_call_done(
        "call-policy", provider,
        ProviderEvent(type="tool_call_done", payload={
            "name": "schedule_call_end", "call_id": "policy-from-background",
            "arguments": {"reason": "policy_limit"},
        }),
    )
    assert "call-policy" not in runner._pending_call_ends
    assert scheduled == []
    assert provider.tool_results


@pytest.mark.anyio
async def test_policy_end_speaks_before_scheduling_hangup() -> None:
    runner, provider, scheduled = _runner()

    await runner._begin_policy_call_end(
        "call-policy",
        provider,
        end_reason="policy_duration_limit",
    )
    assert provider.created_responses == [CALL_POLICY_FINAL_INPUT]
    assert scheduled == []

    runner._mark_response_started("call-policy", {"response_id": "final-response"})
    runner.registry.transition("call-policy", CallSessionStatus.AI_THINKING)
    runner.registry.transition("call-policy", CallSessionStatus.AI_SPEAKING)
    await runner._apply_provider_event(
        "call-policy",
        provider,
        "model_response_done",
        datetime.now(timezone.utc),
        {},
    )

    assert scheduled == [("call-policy", "policy_duration_limit")]


@pytest.mark.anyio
@pytest.mark.parametrize("text", ["好的。", "好的，再见。", "谢谢，再见。", "拜拜。"])
async def test_final_closing_ack_does_not_create_another_response(text) -> None:
    runner, provider, scheduled = _runner()
    call_id = "call-policy"
    runner._prepare_policy_call_end(call_id, end_reason="policy_duration_limit")
    runner._pending_call_ends[call_id].final_response_started = True
    runner._response_lifecycle(call_id).active = True
    now = datetime.now(timezone.utc)
    await runner._handle_user_transcript(call_id, provider, ProviderEvent(
        type="user_transcript_done", payload={"transcript": text},
    ), now)
    await runner._handle_user_speech_stopped(call_id, provider, now)
    await runner._complete_response_and_flush_pending(call_id, provider)

    assert runner._pending_turn(call_id).call_end_acknowledged
    assert provider.created_responses == []
    assert scheduled == [(call_id, "policy_duration_limit")]
    await runner.stop(call_id)


@pytest.mark.anyio
async def test_reviewed_closing_ack_after_speech_stop_releases_pending_hangup() -> None:
    runner, provider, scheduled = _runner(SpeechClassifier())
    call_id = "call-policy"
    runner._prepare_policy_call_end(call_id, end_reason="policy_duration_limit")
    runner._pending_call_ends[call_id].final_response_started = True
    runner.registry.transition(call_id, CallSessionStatus.USER_SPEAKING)
    await _review_turn(runner, provider, "好的，再见。")
    assert provider.created_responses == []
    assert scheduled == [(call_id, "policy_duration_limit")]
    await runner.stop(call_id)


@pytest.mark.parametrize("prompt_key", ["prompt", "instructions"])
def test_runtime_closing_constraints_apply_to_legacy_and_frozen_prompts(prompt_key) -> None:
    runner, _, _ = _runner()
    session = runner.registry.get("call-policy")
    session.effective_config[prompt_key] = "客户同意后说稍后安排顾问联系。"
    config = runner._session_config(session)
    assert "先调用 schedule_call_end 并等待工具结果确认" in config.instructions
    assert "当前通话没有创建回访任务或安排顾问的工具" in config.instructions
    tool = next(item for item in config.tools if item["function"]["name"] == "schedule_call_end")
    assert "此工具不创建回访任务" in tool["function"]["description"]


@pytest.mark.anyio
async def test_partial_ack_cannot_hide_the_question_in_final_transcript() -> None:
    runner, provider, scheduled = _runner()
    call_id = "call-policy"
    runner.user_turn_stability_delay_seconds = 0
    runner._prepare_policy_call_end(call_id, end_reason="policy_duration_limit")
    now = datetime.now(timezone.utc)
    await runner._handle_user_transcript(call_id, provider, ProviderEvent(
        type="user_transcript_delta", payload={"text": "好的"},
    ), now)
    assert not runner._pending_turn(call_id).call_end_acknowledged
    assert call_id in runner._pending_call_ends

    await runner._handle_user_transcript(call_id, provider, ProviderEvent(
        type="user_transcript_done", payload={"transcript": "好的，再见之前还有个问题，多少钱？"},
    ), now)
    await runner._handle_user_speech_stopped(call_id, provider, now)
    assert call_id not in runner._pending_call_ends
    assert not runner._pending_turn(call_id).call_end_acknowledged
    assert provider.created_responses == [None]
    assert scheduled == []
    await runner.stop(call_id)


@pytest.mark.anyio
async def test_new_question_replaces_queued_policy_goodbye() -> None:
    runner, provider, scheduled = _runner()
    call_id = "call-policy"
    runner.user_turn_stability_delay_seconds = 0
    runner._mark_response_started(call_id, {"response_id": "previous-answer"})
    await runner._begin_policy_call_end(call_id, provider, end_reason="policy_duration_limit")
    assert runner._response_lifecycle(call_id).pending_input_text == CALL_POLICY_FINAL_INPUT

    now = datetime.now(timezone.utc)
    await runner._handle_user_transcript(call_id, provider, ProviderEvent(
        type="user_transcript_done", payload={"transcript": "另外，你们有试用吗？"},
    ), now)
    await runner._handle_user_speech_stopped(call_id, provider, now)
    await runner._complete_response_and_flush_pending(call_id, provider)
    assert call_id not in runner._pending_call_ends
    assert provider.created_responses == [None]
    assert scheduled == []
    await runner.stop(call_id)


@pytest.mark.anyio
async def test_transcript_fragments_do_not_exhaust_customer_turn_budget() -> None:
    runner, provider, scheduled = _runner()
    for index in range(15):
        await runner._handle_user_transcript(
            "call-policy",
            provider,
            ProviderEvent(
                type="user_transcript_done",
                payload={"transcript": f"同一句话的第 {index + 1} 个识别片段"},
            ),
            datetime.now(timezone.utc),
        )

    assert "call-policy" not in runner._pending_call_ends
    assert scheduled == []
    assert CALL_POLICY_FINAL_INPUT not in provider.created_responses


@pytest.mark.anyio
@pytest.mark.parametrize("duplicate_final_transcript", [False, True])
async def test_fifteenth_committed_turn_answers_question_before_polite_end(
    duplicate_final_transcript: bool,
) -> None:
    runner, provider, scheduled = _runner()
    for index in range(15):
        turn = runner._pending_turn("call-policy", reset_if_finished=True)
        await runner._handle_user_transcript(
            "call-policy",
            provider,
            ProviderEvent(
                type="user_transcript_done",
                payload={"transcript": "你们有知识库吗？"},
            ),
            datetime.now(timezone.utc),
        )
        turn.stopped_at = datetime.now(timezone.utc)
        await runner._request_response_from_turn("call-policy", provider, turn)
        if index < 14:
            await runner._complete_response_and_flush_pending("call-policy", provider)

    assert runner._customer_turn_counts["call-policy"] == 15
    assert provider.created_responses[-1] != CALL_POLICY_FINAL_INPUT
    assert "当前问题" in provider.created_responses[-1]
    assert "收尾" in provider.created_responses[-1]
    assert runner._pending_call_ends["call-policy"].end_reason == "policy_turn_limit"
    assert scheduled == []

    runner._mark_response_started("call-policy", {"response_id": "knowledge-tool"})
    runner._queue_response_create("call-policy")
    await runner._complete_response_and_flush_pending("call-policy", provider)
    assert scheduled == []
    assert runner._customer_turn_counts["call-policy"] == 15
    assert "当前问题" in provider.created_responses[-1]
    runner._mark_response_started("call-policy", {"response_id": "answer-and-goodbye"})
    if duplicate_final_transcript:
        await runner._handle_user_transcript(
            "call-policy",
            provider,
            ProviderEvent(type="user_transcript_done", payload={"transcript": "你们有知识库吗？"}),
            datetime.now(timezone.utc),
        )
    await runner._apply_provider_event(
        "call-policy", provider, "model_response_done", datetime.now(timezone.utc), {}
    )
    assert scheduled == [("call-policy", "policy_turn_limit")]


@pytest.mark.anyio
async def test_one_committed_turn_counts_once_across_transcript_and_tool_continuations() -> None:
    runner, provider, _scheduled = _runner()
    for text in ("你们有", "知识库吗", "知识库怎么使用"):
        await runner._handle_user_transcript(
            "call-policy",
            provider,
            ProviderEvent(type="user_transcript_done", payload={"transcript": text}),
            datetime.now(timezone.utc),
        )
    assert runner._customer_turn_counts.get("call-policy", 0) == 0

    turn = runner._pending_turn("call-policy")
    turn.stopped_at = datetime.now(timezone.utc)
    await runner._request_response_from_turn("call-policy", provider, turn)
    assert runner._customer_turn_counts["call-policy"] == 1
    await runner._handle_user_transcript(
        "call-policy",
        provider,
        ProviderEvent(type="user_transcript_done", payload={"transcript": "知识库怎么使用"}),
        datetime.now(timezone.utc),
    )
    runner._queue_response_create("call-policy")
    await runner._complete_response_and_flush_pending("call-policy", provider)
    assert runner._customer_turn_counts["call-policy"] == 1


@pytest.mark.anyio
@pytest.mark.parametrize("barge_in_enabled", [True, False])
async def test_new_customer_speech_after_answer_counts_as_a_new_turn(barge_in_enabled) -> None:
    runner, provider, _scheduled = _runner()
    runner.registry.get("call-policy").effective_config["barge_in_enabled"] = barge_in_enabled
    for text in ("你们有知识库吗？", "那应该怎么使用呢？"):
        await runner._handle_user_speech_started(
            "call-policy", provider, datetime.now(timezone.utc)
        )
        await runner._handle_user_transcript(
            "call-policy",
            provider,
            ProviderEvent(type="user_transcript_done", payload={"transcript": text}),
            datetime.now(timezone.utc),
        )
        turn = runner._pending_turn("call-policy")
        turn.stopped_at = datetime.now(timezone.utc)
        runner._playback_guard("call-policy").user_speech_active = False
        await runner._request_response_from_turn("call-policy", provider, turn)
        await runner._complete_response_and_flush_pending("call-policy", provider)

    assert len(provider.created_responses) == 2
    assert runner._customer_turn_counts["call-policy"] == 2
    await runner._cancel_turn_response_task("call-policy")


@pytest.mark.anyio
async def test_opening_and_system_prompts_do_not_count_as_customer_turns() -> None:
    runner, provider, _scheduled = _runner()
    turn = runner._pending_turn("call-policy")
    turn.transcript_parts = ["你们有知识库吗？"]
    turn.stopped_at = datetime.now(timezone.utc)
    await runner._request_response("call-policy", provider, opening_response=True)
    await runner._complete_response_and_flush_pending("call-policy", provider)
    await runner._request_response("call-policy", provider, input_text="请简短收尾")
    assert runner._customer_turn_counts.get("call-policy", 0) == 0
    assert not turn.customer_turn_counted


@pytest.mark.anyio
async def test_duration_safety_limit_survives_interrupted_polite_closing(monkeypatch) -> None:
    runner, provider, scheduled = _runner()
    sleep_count = 0

    async def advance_clock(_seconds: float) -> None:
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count == 3:
            runner._interrupt_pending_call_end("call-policy", "new_customer_question")

    monkeypatch.setattr(agent_runner_module.asyncio, "sleep", advance_clock)
    await runner._run_call_policy("call-policy")

    assert scheduled == [("call-policy", "policy_duration_limit")]


class GatedPlayoutPublisher:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def wait_for_playout(self, _call_id: str) -> None:
        self.started.set()
        await self.release.wait()


@pytest.mark.anyio
async def test_duration_closing_waits_for_previous_and_final_playout() -> None:
    runner, provider, scheduled = _runner()
    publisher = GatedPlayoutPublisher()
    runner.audio_publisher = publisher
    runner.ai_speaking_tail_grace_seconds = 0
    call_id = "call-policy"

    async def emit(event_type, response_id):
        await runner._apply_provider_event(
            call_id, provider, event_type, datetime.now(timezone.utc),
            {"response_id": response_id},
        )

    try:
        await emit("model_response_started", "answer")
        await emit("model_audio_delta", "answer")
        await runner._begin_policy_call_end(call_id, provider, end_reason="policy_duration_limit")
        await emit("model_response_done", "answer")
        waiter = runner._playout_tasks[call_id]
        await asyncio.wait_for(publisher.started.wait(), 1)
        assert provider.created_responses == []
        assert scheduled == []

        publisher.release.set()
        await asyncio.wait_for(waiter, 1)
        assert provider.created_responses == [CALL_POLICY_FINAL_INPUT]
        assert scheduled == []
        # 新回复已请求但 response_started 尚未到达时，旧 done 也不能清掉新回复状态。
        await emit("model_response_done", "answer")
        assert runner._has_active_model_response(call_id)

        publisher.started.clear()
        publisher.release.clear()
        await emit("model_response_started", "goodbye")
        await emit("model_audio_delta", "goodbye")
        await emit("model_response_done", "goodbye")
        waiter = runner._playout_tasks[call_id]
        await asyncio.wait_for(publisher.started.wait(), 1)
        assert scheduled == []
        publisher.release.set()
        await asyncio.wait_for(waiter, 1)
        runner._schedule_pending_call_end_nowait(call_id)
        assert scheduled == [(call_id, "policy_duration_limit")]
        assert provider.created_responses == [CALL_POLICY_FINAL_INPUT]
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_stale_playout_and_done_cannot_end_new_goodbye() -> None:
    runner, provider, scheduled = _runner()
    publisher = GatedPlayoutPublisher()
    runner.audio_publisher = publisher
    runner.ai_speaking_tail_grace_seconds = 0
    call_id = "call-policy"

    async def emit(event_type, response_id):
        await runner._apply_provider_event(
            call_id, provider, event_type, datetime.now(timezone.utc),
            {"response_id": response_id},
        )

    try:
        await runner._begin_policy_call_end(call_id, provider, end_reason="policy_duration_limit")
        await emit("model_response_started", "old-goodbye")
        await emit("model_audio_delta", "old-goodbye")
        await emit("model_response_done", "old-goodbye")
        waiter = runner._playout_tasks[call_id]
        await asyncio.wait_for(publisher.started.wait(), 1)
        # 重现供应商新响应与旧播放完成回调交错到达。
        await emit("model_response_started", "new-goodbye")
        await emit("model_audio_delta", "new-goodbye")
        publisher.release.set()
        await asyncio.wait_for(waiter, 1)
        assert runner.registry.get(call_id).status == CallSessionStatus.AI_SPEAKING
        assert scheduled == []
        await emit("model_response_done", "old-goodbye")
        assert runner._has_active_model_response(call_id)
        assert scheduled == []

        await emit("model_response_done", "new-goodbye")
        await asyncio.wait_for(runner._playout_tasks[call_id], 1)
        assert scheduled == [(call_id, "policy_duration_limit")]
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
async def test_pending_hangup_cannot_skip_final_generation_before_first_audio() -> None:
    runner, provider, scheduled = _runner()
    await runner._begin_policy_call_end("call-policy", provider, end_reason="policy_duration_limit")
    runner._mark_response_started("call-policy", {"response_id": "goodbye"})
    runner._schedule_pending_call_end_nowait("call-policy")
    assert scheduled == []
    await runner._apply_provider_event(
        "call-policy", provider, "model_response_done", datetime.now(timezone.utc),
        {"response_id": "goodbye"},
    )
    assert scheduled == [("call-policy", "policy_duration_limit")]


@pytest.mark.anyio
async def test_accepted_closing_does_not_repeat_queued_duration_goodbye() -> None:
    runner, provider, scheduled = _runner()
    publisher = GatedPlayoutPublisher()
    runner.audio_publisher = publisher
    runner.ai_speaking_tail_grace_seconds = 0
    call_id = "call-policy"
    runner._mark_response_started(call_id, {"response_id": "current-goodbye"})
    runner.registry.transition(call_id, CallSessionStatus.AI_SPEAKING)
    runner._playback_guard(call_id).current_response_audio_published = True
    try:
        await runner._begin_policy_call_end(call_id, provider, end_reason="policy_duration_limit")
        await runner._handle_tool_call_done(call_id, provider, ProviderEvent(
            type="tool_call_done", payload={
                "call_id": "end-tool", "name": "schedule_call_end",
                "arguments": json.dumps({"reason": "customer_end"}),
            },
        ))
        await runner._apply_provider_event(
            call_id, provider, "model_response_done", datetime.now(timezone.utc),
            {"response_id": "current-goodbye"},
        )
        waiter = runner._playout_tasks[call_id]
        await asyncio.wait_for(publisher.started.wait(), 1)
        assert scheduled == []
        publisher.release.set()
        await asyncio.wait_for(waiter, 1)
        assert provider.created_responses == []
        assert scheduled == [(call_id, "policy_duration_limit")]
    finally:
        await runner.stop(call_id)


@pytest.mark.anyio
@pytest.mark.parametrize("stuck_stage", ["generation", "playout"])
async def test_duration_safety_end_bounds_stuck_closing(monkeypatch, stuck_stage) -> None:
    runner, provider, scheduled = _runner()
    runner.audio_publisher = GatedPlayoutPublisher()
    runner.ai_speaking_tail_grace_seconds = 0
    sleep = asyncio.sleep
    policy_sleeps = []

    async def advance_policy_clock(seconds):
        policy_sleeps.append(seconds)
        if len(policy_sleeps) != 3:
            return
        if stuck_stage == "playout":
            for event_type in ("model_response_started", "model_audio_delta", "model_response_done"):
                await runner._apply_provider_event(
                    "call-policy", provider, event_type, datetime.now(timezone.utc),
                    {"response_id": "goodbye"},
                )
            await sleep(0)
        assert scheduled == []

    monkeypatch.setattr(agent_runner_module.asyncio, "sleep", advance_policy_clock)
    try:
        await runner._run_call_policy("call-policy")
        # 业务上限为 300 秒，异常收尾最多再等 15 秒。
        assert sum(policy_sleeps) == 315
        assert scheduled == [("call-policy", "policy_duration_limit")]
    finally:
        await runner.stop("call-policy")


@pytest.mark.anyio
@pytest.mark.parametrize("entry_type,identity", [(None, "sip-customer"), ("direct_sip", "sip-customer"), ("sip_outbound", "sip-customer"), ("outbound", "caller-customer")])
async def test_third_silence_timeout_starts_polite_end(monkeypatch, entry_type, identity) -> None:
    monkeypatch.setattr(agent_runner_module, "CALL_POLICY_SILENCE_SECONDS", 0)
    runner, provider, _scheduled = _runner(entry_type=entry_type, participant_identity=identity)
    runner._silence_prompt_counts["call-policy"] = 2

    runner._arm_silence_watchdog("call-policy")
    task = runner._silence_watchdog_tasks["call-policy"]
    await task

    assert provider.created_responses == [CALL_POLICY_FINAL_INPUT]
    assert runner._pending_call_ends["call-policy"].end_reason == "policy_no_response"


@pytest.mark.anyio
@pytest.mark.parametrize("entry_type,identity,telephone", [
    ("outbound", "caller-customer", True),
    ("direct_sip", "sip-customer", True),
    ("sip_outbound", "sip-customer", True),
    ("web", "caller-customer", False),
    ("web", "sip-customer", False),
    (None, "sip-customer", True),
    (None, "browser-customer", False),
])
async def test_call_type_controls_duration_and_silence_protection(
    monkeypatch, entry_type, identity, telephone,
) -> None:
    monkeypatch.setattr(agent_runner_module, "CALL_POLICY_WRAP_UP_SECONDS", 0)
    monkeypatch.setattr(agent_runner_module, "CALL_POLICY_FINAL_RESPONSE_SECONDS", 0)
    monkeypatch.setattr(agent_runner_module, "CALL_POLICY_SAFETY_END_SECONDS", 0)
    runner, _provider, scheduled = _runner(entry_type=entry_type, participant_identity=identity)

    try:
        runner._arm_silence_watchdog("call-policy")
        assert ("call-policy" in runner._silence_watchdog_tasks) is telephone
        await runner.start_opening("call-policy")
        assert ("call-policy" in runner._call_policy_tasks) is telephone
        if telephone:
            await runner._call_policy_tasks["call-policy"]
        assert scheduled == ([("call-policy", "policy_duration_limit")] if telephone else [])
    finally:
        await runner.stop("call-policy")
