from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.services.ai_call import agent_runner as agent_runner_module
from app.services.ai_call.agent_runner import (
    CALL_POLICY_FINAL_INPUT,
    RealtimeCallAgentRunner,
)
from app.services.ai_call.event_store import InMemoryEventStore
from app.services.ai_call.providers.base import ProviderEvent
from app.services.ai_call.session_registry import (
    CallSession,
    CallSessionStatus,
    InMemorySessionRegistry,
)


class FakeProvider:
    def __init__(self) -> None:
        self.created_responses: list[str | None] = []

    async def create_response(self, input_text: str | None = None) -> None:
        self.created_responses.append(input_text)


def _runner() -> tuple[RealtimeCallAgentRunner, FakeProvider, list[tuple[str, str]]]:
    registry = InMemorySessionRegistry()
    registry.add(
        CallSession(
            call_id="call-policy",
            room_name="room-policy",
            participant_identity="sip-customer",
            status=CallSessionStatus.CONNECTED,
            effective_config={"barge_in_enabled": True},
        )
    )
    provider = FakeProvider()
    scheduled: list[tuple[str, str]] = []
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=InMemoryEventStore(),
        call_end_scheduler=lambda call_id, reason: scheduled.append((call_id, reason)),
    )
    runner._providers["call-policy"] = provider
    return runner, provider, scheduled


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


@pytest.mark.anyio
async def test_third_silence_timeout_starts_polite_end(monkeypatch) -> None:
    monkeypatch.setattr(agent_runner_module, "CALL_POLICY_SILENCE_SECONDS", 0)
    runner, provider, _scheduled = _runner()
    runner._silence_prompt_counts["call-policy"] = 2

    runner._arm_silence_watchdog("call-policy")
    task = runner._silence_watchdog_tasks["call-policy"]
    await task

    assert provider.created_responses == [CALL_POLICY_FINAL_INPUT]
    assert runner._pending_call_ends["call-policy"].end_reason == "policy_no_response"
