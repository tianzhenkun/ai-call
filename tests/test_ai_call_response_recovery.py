import asyncio
import base64
from datetime import datetime, timezone

import pytest
from test_ai_call_phase_a_core import FakeAudioPublisher, QueueRealtimeProvider
from test_ai_call_runtime_conversation_policy import SpeechClassifier, _runner

from app.services.ai_call import agent_runner as module
from app.services.ai_call.audio_bridge import PcmAudioFrame
from app.services.ai_call.providers.base import ProviderEvent


async def _until(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(.001)


def _response(kind, response_id="old", **extra):
    return ProviderEvent(type=kind, payload={"response": {"id": response_id, **extra}})


async def _setup(monkeypatch):
    monkeypatch.setattr(module, "RESPONSE_STALL_SECONDS", .06, raising=False)
    monkeypatch.setattr(module, "RESPONSE_CANCEL_SECONDS", .03, raising=False)
    monkeypatch.setattr(module, "RESPONSE_RECONNECT_SECONDS", .2, raising=False)
    runner, _, scheduled = _runner(entry_type="outbound", participant_identity="caller-test")
    old, new = QueueRealtimeProvider(), QueueRealtimeProvider()
    providers = iter([old, new])
    runner.provider_factory = lambda _: next(providers)
    runner.audio_publisher = FakeAudioPublisher()
    await runner.start(runner.registry.get("call-policy"))
    return runner, old, new, scheduled


@pytest.mark.anyio
@pytest.mark.parametrize("cancel_before_started", [False, True], ids=[
    "call_367171092849741824_partial_then_cancel", "call_367191336657997824_cancel_before_created",
])
async def test_missing_done_recovers_once_and_isolates_late_events(monkeypatch, cancel_before_started):
    runner, old, new, scheduled = await _setup(monkeypatch)
    try:
        await runner._request_response("call-policy", old, input_text="我们主要想找客户")
        if cancel_before_started:
            await runner._invalidate_audio_for_interrupt_candidate(
                call_id="call-policy", provider=old, trigger_timestamp=datetime.now(timezone.utc),
                source="provider", reason="user_speech_started_during_ai_audio",
            )
        await old.emit(_response("model_response_started"))
        await _until(lambda: runner._playback_guard("call-policy").current_response_id == "old")
        if not cancel_before_started:
            await old.emit(ProviderEvent(type="ai_transcript_delta", payload={
                "response_id": "old", "delta": "好的",
            }))
            await runner._invalidate_audio_for_interrupt_candidate(
                call_id="call-policy", provider=old, trigger_timestamp=datetime.now(timezone.utc),
                source="provider", reason="user_speech_started_during_ai_audio",
            )
        await runner._request_response("call-policy", old, input_text="你们怎么找客户？")
        await _until(lambda: bool(new.created_responses))
        assert old.closed and new.connected
        assert len(old.created_responses) == len(new.created_responses) == 1
        assert "找客户" in new.created_responses[0]
        assert not scheduled
        await new.emit(_response("model_response_started", "new"))
        await new.emit(_response("model_response_done", "new", status="completed"))
        await _until(lambda: not runner._response_lifecycle("call-policy").active)
        # 已退出的连接及旧 response 迟到，不能重新激活旧回复或多答一遍。
        await runner._apply_provider_event("call-policy", old, "model_response_started",
                                           datetime.now(timezone.utc), {"response": {"id": "old"}})
        await new.emit(_response("model_response_started", "old"))
        await new.emit(_response("model_response_done", "old", status="cancelled"))
        await asyncio.sleep(.02)
        assert runner._playback_guard("call-policy").current_response_id == "new"
        assert not runner._response_lifecycle("call-policy").active
        assert len(new.created_responses) == 1
    finally:
        await runner.stop("call-policy")


@pytest.mark.anyio
@pytest.mark.parametrize("stop_during_connect", [False, True])
async def test_audio_waits_for_connection_and_stop_cleans_recovery(monkeypatch, stop_during_connect):
    runner, old, new, scheduled = await _setup(monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()

    async def connect():
        entered.set()
        await release.wait()
        new.connected = True

    new.connect = connect
    audio_task = None
    try:
        await runner._request_response("call-policy", old, input_text="业务问题")
        await old.emit(_response("model_response_started"))
        await asyncio.wait_for(entered.wait(), 1)
        frame = PcmAudioFrame(data=b"\x01\x02" * 320, sample_rate_hz=16000, channels=1, sample_width_bytes=2)
        audio_task = asyncio.create_task(runner.send_audio_frame("call-policy", frame))
        await asyncio.sleep(.01)
        assert not audio_task.done() and not new.sent_audio
        if stop_during_connect:
            await runner.stop("call-policy")
            assert new.closed and not new.created_responses
            assert not runner._response_watchdogs and not runner._provider_ready
        else:
            release.set()
            await asyncio.wait_for(audio_task, 1)
            assert b"".join(new.sent_audio) == frame.data
            await new.emit(_response("model_response_started", "new"))
            await new.emit(_response("model_response_done", "new", status="completed"))
            await _until(lambda: not runner._response_lifecycle("call-policy").active)
        assert not scheduled
    finally:
        release.set()
        if audio_task is not None:
            await asyncio.gather(audio_task, return_exceptions=True)
        await runner.stop("call-policy")


@pytest.mark.anyio
async def test_reconnect_waits_for_current_customer_and_uses_latest_question(monkeypatch):
    runner, old, new, scheduled = await _setup(monkeypatch)
    runner.user_turn_stability_delay_seconds = 0
    try:
        await runner._request_response("call-policy", old, input_text="旧问题")
        await old.emit(_response("model_response_started"))
        await old.emit(ProviderEvent(type="user_speech_started", payload={"item_id": "customer"}))
        await _until(lambda: new.connected)
        assert not new.created_responses
        await new.emit(ProviderEvent(type="user_transcript_done", payload={
            "item_id": "customer", "transcript": "你们通过什么方式找客户？",
        }))
        await new.emit(ProviderEvent(type="user_speech_stopped"))
        await _until(lambda: bool(new.created_responses))
        assert "你们通过什么方式找客户" in new.created_responses[0]
        assert len(new.created_responses) == 1 and not scheduled
        await new.emit(_response("model_response_started", "new"))
        await new.emit(_response("model_response_done", "new", status="completed"))
        await _until(lambda: not runner._response_lifecycle("call-policy").active)
    finally:
        await runner.stop("call-policy")


@pytest.mark.anyio
async def test_cancel_send_timeout_does_not_loop_forever(monkeypatch):
    runner, old, new, _ = await _setup(monkeypatch)

    async def stuck_cancel():
        await asyncio.Event().wait()

    old.cancel_response = stuck_cancel
    try:
        await runner._request_response("call-policy", old, input_text="怎么找客户？")
        await old.emit(_response("model_response_started"))
        await _until(lambda: bool(new.created_responses))
        assert old.closed and len(new.created_responses) == 1
        await new.emit(_response("model_response_started", "new"))
        await new.emit(_response("model_response_done", "new", status="completed"))
        await _until(lambda: not runner._response_lifecycle("call-policy").active)
    finally:
        await runner.stop("call-policy")


@pytest.mark.anyio
async def test_cancel_ack_recovers_on_same_connection(monkeypatch):
    runner, old, new, scheduled = await _setup(monkeypatch)
    try:
        await runner._request_response("call-policy", old, input_text="怎么收费？")
        await old.emit(_response("model_response_started"))
        await _until(lambda: old.cancelled_response_count == 1)
        await old.emit(_response("model_response_done", status="cancelled"))
        await _until(lambda: len(old.created_responses) == 2)
        assert "怎么收费" in old.created_responses[1]
        assert not old.closed and not new.connected and not scheduled
        await old.emit(_response("model_response_started", "retried"))
        await old.emit(ProviderEvent(type="model_audio_delta", payload={
            "response_id": "retried", "delta": base64.b64encode(b"\x01\x02" * 2400).decode(),
        }))
        await _until(lambda: bool(runner.audio_publisher.published))
        await old.emit(_response("model_response_done", "retried", status="completed"))
        await _until(lambda: not runner._response_lifecycle("call-policy").active)
    finally:
        await runner.stop("call-policy")


@pytest.mark.anyio
async def test_progressing_response_has_no_added_wait_or_false_timeout(monkeypatch):
    runner, old, new, scheduled = await _setup(monkeypatch)
    try:
        await runner._request_response("call-policy", old, input_text="介绍一下")
        assert len(old.created_responses) == 1
        await old.emit(_response("model_response_started"))
        for _ in range(8):
            await old.emit(ProviderEvent(type="ai_transcript_delta", payload={
                "response_id": "old", "delta": "继续介绍",
            }))
            await asyncio.sleep(.015)
        await old.emit(_response("model_response_done", status="completed"))
        await _until(lambda: not runner._response_lifecycle("call-policy").active)
        await asyncio.sleep(.08)
        assert old.cancelled_response_count == 0 and not new.connected and not scheduled
    finally:
        await runner.stop("call-policy")


@pytest.mark.anyio
async def test_failed_reconnect_plays_notice_and_ends_with_failure(monkeypatch):
    runner, old, new, scheduled = await _setup(monkeypatch)

    async def fail_connect():
        raise OSError("test connection failure")
    new.connect = fail_connect
    try:
        await runner._request_response("call-policy", old, input_text="介绍一下")
        await old.emit(_response("model_response_started"))
        await _until(lambda: bool(scheduled))
        assert scheduled == [("call-policy", "model_error")]
        assert runner.audio_publisher.published
        assert old.closed and new.closed
        assert not new.created_responses
    finally:
        await runner.stop("call-policy")


@pytest.mark.anyio
async def test_reconnect_preserves_inflight_speech_review(monkeypatch):
    runner, old, new, scheduled = await _setup(monkeypatch)
    release = asyncio.Event()
    classifier = SpeechClassifier()

    async def classify(**kwargs):
        await release.wait()
        return await classifier.classify(**kwargs)

    runner.customer_speech_classifier = SpeechClassifier()
    runner.customer_speech_classifier.classify = classify
    runner.user_turn_stability_delay_seconds = 0
    try:
        await runner._request_response("call-policy", old, input_text="旧问题")
        await old.emit(_response("model_response_started"))
        await old.emit(ProviderEvent(type="user_speech_started", payload={"item_id": "customer"}))
        await old.emit(ProviderEvent(type="user_transcript_done", payload={
            "item_id": "customer", "transcript": "现在请介绍一下价格",
        }))
        await old.emit(ProviderEvent(type="user_speech_stopped"))
        await _until(lambda: old.closed or "call-policy" in runner._provider_ready)
        await asyncio.sleep(.01)
        assert not new.created_responses
        release.set()
        await _until(lambda: bool(new.created_responses))
        assert not runner._pending_turn("call-policy").transcript_review_pending
        assert "现在请介绍一下价格" in new.created_responses[0]
        assert len(classifier.inputs) == 1 and not scheduled
        await new.emit(_response("model_response_started", "new"))
        await new.emit(_response("model_response_done", "new", status="completed"))
        await _until(lambda: not runner._response_lifecycle("call-policy").active)
    finally:
        release.set()
        await runner.stop("call-policy")


@pytest.mark.anyio
@pytest.mark.parametrize("confirm", [False, True])
async def test_late_cancel_send_does_not_mark_new_response_cancelled(monkeypatch, confirm):
    runner, old, _, _ = await _setup(monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()

    async def delayed_cancel():
        entered.set()
        await release.wait()

    old.cancel_response = delayed_cancel
    cancel = None
    try:
        await runner._request_response("call-policy", old, input_text="旧问题")
        await old.emit(_response("model_response_started"))
        await _until(lambda: runner._playback_guard("call-policy").current_response_id == "old")
        if confirm:
            coro = runner._confirm_interrupt("call-policy", old, datetime.now(timezone.utc))
        else:
            coro = runner._invalidate_audio_for_interrupt_candidate(
                call_id="call-policy", provider=old, trigger_timestamp=datetime.now(timezone.utc),
                source="test", reason="continued_speech",
            )
        cancel = asyncio.create_task(coro)
        await entered.wait()
        await runner._request_response("call-policy", old, input_text="新问题")
        await old.emit(_response("model_response_done", status="cancelled"))
        await _until(lambda: len(old.created_responses) == 2)
        release.set()
        await cancel
        assert not runner._playback_guard("call-policy").cancel_requested
        await old.emit(_response("model_response_started", "new"))
        await old.emit(_response("model_response_done", "new", status="completed"))
        await _until(lambda: not runner._response_lifecycle("call-policy").active)
    finally:
        release.set()
        if cancel is not None:
            await cancel
        await runner.stop("call-policy")


@pytest.mark.anyio
async def test_no_active_cancel_error_before_created_still_reconnects(monkeypatch):
    runner, old, new, _ = await _setup(monkeypatch)
    try:
        await runner._request_response("call-policy", old, input_text="怎么收费？")
        await runner._invalidate_audio_for_interrupt_candidate(
            call_id="call-policy", provider=old, trigger_timestamp=datetime.now(timezone.utc),
            source="test", reason="continued_speech",
        )
        await old.emit(ProviderEvent(type="model_error", payload={"error": {
            "message": "Conversation has none active response",
        }}))
        await asyncio.sleep(.005)
        assert runner._response_lifecycle("call-policy").cancel_pending
        assert len(old.created_responses) == 1
        await old.emit(_response("model_response_started"))
        await _until(lambda: bool(new.created_responses))
        await new.emit(_response("model_response_started", "new"))
        await new.emit(_response("model_response_done", "new", status="completed"))
        await _until(lambda: not runner._response_lifecycle("call-policy").active)
    finally:
        await runner.stop("call-policy")


@pytest.mark.anyio
@pytest.mark.parametrize("customer_hangs_up", [False, True])
async def test_second_stall_has_no_retry_loop_and_hangup_cancels_notice(monkeypatch, customer_hangs_up):
    runner, old, new, scheduled = await _setup(monkeypatch)
    prompt_started = asyncio.Event()

    async def wait_for_playout(_):
        prompt_started.set()
        if customer_hangs_up:
            await asyncio.Event().wait()

    runner.audio_publisher.wait_for_playout = wait_for_playout
    try:
        await runner._request_response("call-policy", old, input_text="怎么收费？")
        await old.emit(_response("model_response_started"))
        await _until(lambda: bool(new.created_responses))
        await new.emit(_response("model_response_started", "new"))
        await asyncio.wait_for(prompt_started.wait(), 1)
        assert runner._response_lifecycle("call-policy").recovery_count == 1
        if customer_hangs_up:
            await runner.stop("call-policy")
            assert not scheduled
        else:
            await _until(lambda: bool(scheduled))
            assert scheduled == [("call-policy", "model_error")]
        assert old.closed and len(new.created_responses) == 1
    finally:
        await runner.stop("call-policy")


@pytest.mark.anyio
async def test_old_audio_cleanup_failure_cannot_start_overlapping_recovery(monkeypatch):
    runner, old, new, scheduled = await _setup(monkeypatch)

    async def fail_stop(_):
        raise OSError("test audio transport unavailable")

    runner.audio_publisher.stop_audio = fail_stop
    try:
        await runner._request_response("call-policy", old, input_text="怎么收费？")
        await old.emit(_response("model_response_started"))
        await _until(lambda: bool(scheduled))
        assert scheduled == [("call-policy", "model_error")]
        assert not new.connected and not runner.audio_publisher.published
        assert any(e.type == "response_failure_prompt" and e.payload["phase"] == "failed"
                   for e in runner.event_store.list_all("call-policy"))
    finally:
        await runner.stop("call-policy")
