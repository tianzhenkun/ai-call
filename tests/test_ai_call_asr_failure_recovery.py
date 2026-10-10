from __future__ import annotations

import asyncio

import pytest
from test_ai_call_runtime_conversation_policy import SpeechClassifier, _runner

from app.services.ai_call.providers.base import ProviderEvent


def _event(event_type, item_id="failed", **payload):
    return ProviderEvent(type=event_type, payload={"item_id": item_id, **payload})


def _failure(item_id="failed"):
    return _event("user_transcript_failed", item_id, error={
        "code": "UNEXPECTED_ASR_ERROR",
        "type": "transcription_error",
        "message": "grpc error: statusCode=500, message=Receive batching backend response failed!",
    })


async def _consume(runner, provider, *events):
    async def receive_events():
        for event in events:
            yield event

    provider.receive_events = receive_events
    await runner._consume_provider_events("call-policy", provider)
    await asyncio.sleep(0)


@pytest.mark.anyio
@pytest.mark.parametrize("review_enabled", [False, True])
@pytest.mark.parametrize("failure_before_stop", [False, True])
async def test_asr_failure_clarifies_once_after_stop_without_silence_wait(review_enabled, failure_before_stop):
    classifier = SpeechClassifier() if review_enabled else None
    runner, provider, scheduled = _runner(classifier, entry_type="outbound")
    runner._customer_turn_counts["call-policy"] = 14
    failed, stopped = _failure(), _event("user_speech_stopped")
    first, second = (failed, stopped) if failure_before_stop else (stopped, failed)
    try:
        await _consume(runner, provider, _event("user_speech_started"), first)
        assert provider.created_responses == []
        await _consume(runner, provider, second)
        await asyncio.wait_for(runner.wait("call-policy"), timeout=1)
        assert len(provider.created_responses) == 1
        assert "再说一遍" in provider.created_responses[0]
        assert "call-policy" not in runner._silence_watchdog_tasks
        await _consume(runner, provider, failed, stopped)
        await asyncio.wait_for(runner.wait("call-policy"), timeout=1)
        assert len(provider.created_responses) == 1
        assert not runner._response_lifecycle("call-policy").pending_create
        assert runner._customer_turn_counts["call-policy"] == 14
        assert not runner._pending_call_ends and not scheduled
        if classifier:
            assert classifier.inputs == []
    finally:
        await runner.stop("call-policy")


@pytest.mark.anyio
@pytest.mark.parametrize("review_enabled", [False, True])
@pytest.mark.parametrize("failure_before_new_speech", [False, True])
async def test_old_asr_failure_and_late_transcript_do_not_replace_new_question(review_enabled, failure_before_new_speech):
    classifier = SpeechClassifier() if review_enabled else None
    runner, provider, scheduled = _runner(classifier)
    try:
        await _consume(runner, provider, _event("user_speech_started"))
        if failure_before_new_speech:
            await _consume(runner, provider, _failure())
        await _consume(runner, provider, _event("user_speech_started", "new"), _failure())
        await _consume(runner, provider,
                       _event("user_transcript_done", "failed", transcript="挂了吧。"),
                       _event("user_transcript_done", "new", transcript="怎么收费？"),
                       _event("user_speech_stopped", "new"))
        await asyncio.wait_for(runner.wait("call-policy"), timeout=1)
        assert len(provider.created_responses) == 1
        assert "再说一遍" not in (provider.created_responses[0] or "")
        assert runner._pending_turn("call-policy").transcript == "怎么收费？"
        assert runner._customer_turn_counts["call-policy"] == 1
        assert not runner._pending_call_ends and not scheduled
        assert not any(event.type == "call_end_intent_detected"
                       for event in runner.event_store.list_all("call-policy"))
    finally:
        await runner.stop("call-policy")


@pytest.mark.anyio
@pytest.mark.parametrize("review_enabled", [False, True])
async def test_resumed_speech_cancels_pending_failure_and_discards_partial_transcript(review_enabled):
    runner, provider, _ = _runner(SpeechClassifier() if review_enabled else None)
    try:
        # 同一批事件中重新开口，让失败恢复任务尚未来得及执行就被取消。
        await _consume(runner, provider, _event("user_speech_started"),
                       _event("user_transcript_delta", text="未识别完整的片段"), _failure(),
                       _event("user_speech_started", "new"))
        assert runner._pending_turn("call-policy").transcript == ""
        assert provider.created_responses == []
        await _consume(runner, provider, _event("user_transcript_done", "new", transcript="怎么收费？"),
                       _event("user_speech_stopped", "new"))
        await asyncio.wait_for(runner.wait("call-policy"), timeout=1)
        assert len(provider.created_responses) == 1
        assert "再说一遍" not in (provider.created_responses[0] or "")
        assert runner._pending_turn("call-policy").transcript == "怎么收费？"
        assert runner._customer_turn_counts["call-policy"] == 1
    finally:
        await runner.stop("call-policy")


@pytest.mark.anyio
@pytest.mark.parametrize("customer_resumes", [False, True])
async def test_asr_clarification_respects_active_playback_and_resumed_speech(customer_resumes):
    runner, provider, _ = _runner()
    runner.registry.get("call-policy").effective_config["barge_in_enabled"] = False
    runner._response_lifecycle("call-policy").active = True
    try:
        await _consume(runner, provider, _event("user_speech_started"), _failure(),
                       _event("user_speech_stopped"))
        await asyncio.wait_for(runner.wait("call-policy"), timeout=1)
        assert provider.created_responses == []
        assert "再说一遍" in (runner._response_lifecycle("call-policy").pending_input_text or "")
        if customer_resumes:
            await _consume(runner, provider, _event("user_speech_started", "new"))
        await runner._complete_response_and_flush_pending("call-policy", provider)
        if customer_resumes:
            assert provider.created_responses == []
            await _consume(runner, provider, _event("user_transcript_done", "new", transcript="怎么收费？"),
                           _event("user_speech_stopped", "new"))
            await asyncio.wait_for(runner.wait("call-policy"), timeout=1)
            assert provider.created_responses == [None]
            assert runner._customer_turn_counts["call-policy"] == 1
        else:
            assert len(provider.created_responses) == 1
            assert "再说一遍" in provider.created_responses[0]
            assert not runner._customer_turn_counts
    finally:
        await runner.stop("call-policy")
