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
async def test_fifteenth_customer_turn_starts_polite_end() -> None:
    runner, provider, _scheduled = _runner()
    event = ProviderEvent(
        type="user_transcript_done",
        payload={"transcript": "请继续介绍产品。"},
    )

    for _ in range(15):
        await runner._handle_user_transcript(
            "call-policy",
            provider,
            event,
            datetime.now(timezone.utc),
        )

    assert runner._customer_turn_counts["call-policy"] == 15
    assert provider.created_responses[-1] == CALL_POLICY_FINAL_INPUT
    assert runner._pending_call_ends["call-policy"].end_reason == "policy_turn_limit"


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
