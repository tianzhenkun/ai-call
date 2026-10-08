from __future__ import annotations

import asyncio
import base64

import pytest

from app.config.setting import Settings
from app.services.ai_call.agent_runner import (
    CUSTOMER_HANDOFF_REJECTED_TOOL_RESULT,
    RealtimeCallAgentRunner,
)
from app.services.ai_call.event_store import InMemoryEventStore
from app.services.ai_call.handoff_trigger_service import (
    CompositeHandoffIntentClassifier,
    RuleBasedHandoffIntentClassifier,
)
from app.services.ai_call.orchestrator import AiCallOrchestrator, AiCallRuntimeConfig
from app.services.ai_call.providers import aliyun_qwen_realtime as qwen
from app.services.ai_call.providers.dashscope_qwen_tts_realtime import (
    DashScopeQwenTtsRealtimeProvider,
)
from app.services.ai_call.providers.guarded_qwen_speech import (
    CONTROL_SPEECH,
    REVIEW_UNAVAILABLE_SPEECH,
    GuardedQwenSpeechProvider,
)
from app.services.ai_call.session_registry import (
    CallSession,
    CallSessionStatus,
    InMemorySessionRegistry,
)
from app.services.ai_call.speech_output_review import SpeechOutputDecision


@pytest.fixture
def anyio_backend():
    return "asyncio"


class Socket:
    def __init__(self):
        self.incoming = asyncio.Queue()
        self.sent = []
        self.finish_after_response = False

    async def send_json(self, payload):
        self.sent.append(payload)
        item = payload.get("item", {})
        if payload["type"] == "conversation.item.create" and item.get("role") == "assistant":
            self.incoming.put_nowait({"type": "conversation.item.created", "item": {
                **item, "id": "server-spoken-item", "status": "completed",
            }})

    async def receive_json(self):
        event = await self.incoming.get()
        if event is None:
            raise StopAsyncIteration
        return event

    async def close(self):
        self.incoming.put_nowait(None)


@pytest.mark.anyio
async def test_default_provider_streams_audio_without_output_review(monkeypatch):
    socket = Socket()

    async def connect(_url, _headers):
        return socket

    monkeypatch.setattr(qwen, "_default_websocket_factory", connect)
    config = AiCallRuntimeConfig.from_settings(Settings(
        _env_file=None, DASHSCOPE_API_KEY="test-key", LLM_API_KEY="test-key",
    ))
    orchestrator = AiCallOrchestrator(config)
    session = CallSession(
        call_id="direct-audio", room_name="room", participant_identity="customer",
        status=CallSessionStatus.CONNECTED, effective_config={},
    )
    provider = orchestrator.agent_runner.provider_factory(session)
    await provider.connect()
    try:
        await provider.update_session(orchestrator.agent_runner._session_config(session))
        assert socket.sent[-1]["session"]["modalities"] == ["text", "audio"]
        audio = {"type": "response.audio.delta", "response_id": "response",
                 "delta": base64.b64encode(b"\x01\x02" * 240).decode()}
        socket.incoming.put_nowait(audio)
        socket.incoming.put_nowait(None)
        events = [event async for event in provider.receive_events()]
        assert [event.type for event in events] == ["model_audio_delta"]
        assert events[0].payload == audio
    finally:
        await provider.close()


@pytest.mark.anyio
async def test_isolated_guarded_provider_never_releases_unreviewed_audio_after_rejected_handoff():
    provider, socket, _store, _reviewer, _tts = await make_provider([])
    try:
        await provider.update_session(qwen.QwenRealtimeSessionConfig(
            voice="Tina", instructions="测试", vad_type="server_vad",
            vad_threshold=0.5, vad_silence_duration_ms=800,
        ))
        assert socket.sent[-1]["session"]["modalities"] == ["text"]
        await provider.submit_tool_result("tool-handoff", CUSTOMER_HANDOFF_REJECTED_TOOL_RESULT)
        for event in [
            {"type": "response.created", "response": {"id": "response-bad"}},
            {"type": "response.audio.delta", "response_id": "response-bad",
             "delta": base64.b64encode(b"\x01\x02" * 240).decode()},
            {"type": "response.audio_transcript.done", "response_id": "response-bad",
             "transcript": "请稍等，正在为您转接。"},
            {"type": "response.done", "response": {"id": "response-bad", "status": "completed"}},
            None,
        ]:
            socket.incoming.put_nowait(event)
        events = [event async for event in provider.receive_events()]
        assert not any(event.type == "model_audio_delta" for event in events)
        assert not any(event.type == "ai_transcript_done" for event in events)
    finally:
        await provider.close()


class Reviewer:
    TIMEOUT_SECONDS = 0.2
    model = "test-reviewer"

    def __init__(self, decisions):
        self.decisions = iter(decisions)
        self.texts = []
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()

    async def review(self, text, *, context):
        self.texts.append(text)
        self.started.set()
        await self.release.wait()
        result = next(self.decisions)
        if isinstance(result, Exception):
            raise result
        return result


class Tts:
    model = "test-tts"

    def __init__(self):
        self.texts = []
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()
        self.closed = False

    async def synthesize(self, text, *, voice):
        self.texts.append(text)
        self.started.set()
        try:
            yield b"\x01\x02" * 240
            await self.release.wait()
            yield b"\x03\x04" * 240
        finally:
            self.closed = True


async def make_provider(decisions):
    socket, store, reviewer, tts = Socket(), InMemoryEventStore(), Reviewer(decisions), Tts()

    async def connect(_url, _headers):
        return socket

    provider = GuardedQwenSpeechProvider(
        call_id="call", event_store=store, reviewer=reviewer, tts=tts, tts_voice="Cherry",
        realtime_url="wss://test/realtime", api_key="test-key", model="test-model", websocket_factory=connect,
    )
    await provider.connect()
    return provider, socket, store, reviewer, tts


def push_response(socket, text, *, response_id="response", finish_stream=True):
    for event in [
        {"type": "response.created", "response": {"id": response_id}},
        {"type": "response.text.delta", "response_id": response_id, "delta": text[:5]},
        {"type": "response.text.done", "response_id": response_id, "text": text},
        {"type": "response.done", "response": {
            "id": response_id, "status": "completed", "output": [{
                "id": "draft-item", "type": "message", "role": "assistant",
                "content": [{"type": "text", "text": text}],
            }],
        }},
    ]:
        socket.incoming.put_nowait(event)
    if finish_stream:
        socket.finish_after_response = True


async def collect(provider, events):
    async for event in provider.receive_events():
        events.append(event)
        if provider._websocket.finish_after_response and event.type in {"model_response_done", "model_error"}:
            await provider._websocket.close()


@pytest.mark.anyio
@pytest.mark.parametrize("draft,decisions,expected", [
    ("请稍等，正在为您转接。", [SpeechOutputDecision(False, "unsupported_handoff")], CONTROL_SPEECH["none"]),
    ("让我请同事接着和您聊，稍等他接入。", [SpeechOutputDecision(False, "implicit_handoff")], CONTROL_SPEECH["none"]),
    ("我是智能助手，可以继续为您解答。", [SpeechOutputDecision(True, "identity_answer")], "我是智能助手，可以继续为您解答。"),
    ("GEO 可以帮助分析品牌曝光。", [SpeechOutputDecision(True, "normal_answer")], "GEO 可以帮助分析品牌曝光。"),
    ("正在转接。", [TimeoutError()], REVIEW_UNAVAILABLE_SPEECH),
    ("正在转接。", [SpeechOutputDecision(False, "claim", "可以继续介绍产品。"), SpeechOutputDecision(True, "safe")], "可以继续介绍产品。"),
    ("正在转接。", [SpeechOutputDecision(False, "claim", "同事马上接听。"), SpeechOutputDecision(False, "still_claim")], CONTROL_SPEECH["none"]),
])
async def test_complete_reply_is_reviewed_before_tts_and_replaces_model_context(draft, decisions, expected):
    provider, socket, store, reviewer, tts = await make_provider(decisions)
    events = []
    push_response(socket, draft)
    try:
        await asyncio.wait_for(collect(provider, events), 1)
        assert tts.texts == [expected]
        assert [event.payload["transcript"] for event in events if event.type == "ai_transcript_done"] == [expected]
        assert any(event.type == "model_audio_delta" for event in events)
        assert {"type": "conversation.item.delete", "item_id": "draft-item"} in socket.sent
        context = [item["item"]["content"][0]["text"] for item in socket.sent if item["type"] == "conversation.item.create"]
        assert context == [expected]
        assert not any(event.type.startswith("handoff_") for event in store.list_all("call"))
        assert events[-1].type == "model_response_done"
    finally:
        await provider.close()


@pytest.mark.anyio
async def test_review_wait_does_not_block_customer_events_or_release_audio():
    provider, socket, _store, reviewer, tts = await make_provider([SpeechOutputDecision(True, "safe")])
    reviewer.release.clear()
    events = []
    task = asyncio.create_task(collect(provider, events))
    push_response(socket, "普通回复。", finish_stream=False)
    try:
        await asyncio.wait_for(reviewer.started.wait(), 1)
        socket.incoming.put_nowait({"type": "input_audio_buffer.speech_started", "item_id": "customer-turn"})
        for _ in range(20):
            await asyncio.sleep(0)
            if any(event.type == "user_speech_started" for event in events):
                break
        assert any(event.type == "user_speech_started" for event in events)
        assert not tts.texts
        assert not any(event.type == "model_audio_delta" for event in events)
        await provider.cancel_response()
        reviewer.release.set()
        socket.incoming.put_nowait(None)
        await asyncio.wait_for(task, 1)
        assert not tts.texts
        assert not any(event.type == "ai_transcript_done" for event in events)
    finally:
        await provider.close()


@pytest.mark.anyio
async def test_cancel_during_tts_closes_stream_and_drops_queued_late_audio():
    provider, socket, _store, _reviewer, tts = await make_provider([SpeechOutputDecision(True, "safe")])
    tts.release.clear()
    events = []
    task = asyncio.create_task(collect(provider, events))
    push_response(socket, "普通回复。", finish_stream=False)
    try:
        await asyncio.wait_for(tts.started.wait(), 1)
        await provider.cancel_response()
        count = sum(event.type == "model_audio_delta" for event in events)
        tts.release.set()
        socket.incoming.put_nowait(None)
        await asyncio.wait_for(task, 1)
        assert tts.closed
        assert sum(event.type == "model_audio_delta" for event in events) == count
        assert not any(event.type == "ai_transcript_done" for event in events)
        assert any(item["type"] == "conversation.item.delete" and item["item_id"] == "server-spoken-item" for item in socket.sent)
    finally:
        await provider.close()


@pytest.mark.anyio
@pytest.mark.parametrize("event_type,payload,state", [
    ("handoff_tool_ignored", {"reason": "customer_request_without_explicit_intent"}, "rejected"),
    ("handoff_tool_requested", {"reason": "customer_request", "confirmationRequired": True}, "confirmation"),
    ("handoff_auto_trigger_failed", {"stage": "create_handoff"}, "failed"),
    ("handoff_tool_result_timeout", {}, "unknown"),
    ("handoff_intent_ignored", {"reason": "active_handoff_exists"}, "unknown"),
])
async def test_handoff_tool_outcome_uses_controlled_speech_without_model_continuation(event_type, payload, state):
    provider, socket, store, reviewer, tts = await make_provider([])
    events = []
    task = asyncio.create_task(collect(provider, events))
    try:
        store.append("call", event_type, "handoff", {"toolCallId": "tool", **payload})
        await provider.submit_tool_result("tool", "工具执行结果")
        await provider.create_response()
        await asyncio.wait_for(provider._speech, 1)
        socket.incoming.put_nowait(None)
        await asyncio.wait_for(task, 1)
        assert tts.texts == [CONTROL_SPEECH[state]]
        assert not reviewer.texts
        assert not any(item["type"] == "response.create" for item in socket.sent)
        assert any(event.type == "ai_transcript_done" for event in events)
    finally:
        await provider.close()


@pytest.mark.anyio
async def test_tts_error_cannot_be_reported_as_successful_speech():
    socket = Socket()
    for event in [{"type": "session.updated"}, {"type": "response.done", "response": {"status": "failed"}}]:
        socket.incoming.put_nowait(event)

    async def connect(_url, _headers):
        return socket

    tts = DashScopeQwenTtsRealtimeProvider(realtime_url="wss://test", api_key="test-key", websocket_factory=connect)
    with pytest.raises(RuntimeError, match="未完成"):
        _ = [audio async for audio in tts.synthesize("确认话术。", voice="Cherry")]
    assert socket.incoming.qsize() == 1


@pytest.mark.anyio
@pytest.mark.parametrize("text,matched,reason", [
    ("转人工了吗？", False, "handoff_status_query"),
    ("已经转接人工了没有？", False, "handoff_status_query"),
    ("人工接通了吗？", False, "handoff_status_query"),
    ("请帮我转人工。", True, "customer_request"),
    ("可以帮我转一下人工吗？", True, "customer_request"),
    ("不用转人工。", False, "not_handoff"),
    ("你是人工吗？", False, "not_handoff"),
    ("你们有知识库吗？", False, "not_handoff"),
])
async def test_shared_handoff_classifier_distinguishes_status_from_authorization(text, matched, reason):
    classifier = CompositeHandoffIntentClassifier(None, RuleBasedHandoffIntentClassifier())
    result = await classifier.classify(transcript=text)
    assert result.matched is matched
    assert result.reason == reason


@pytest.mark.anyio
async def test_cancel_before_response_started_prevents_review_tts_and_keeps_draft_out_of_context():
    provider, socket, _store, reviewer, tts = await make_provider([])
    try:
        await provider.create_response()
        await provider.cancel_response()
        push_response(socket, "正在为您转接。")
        events = []
        await asyncio.wait_for(collect(provider, events), 1)
        assert not reviewer.texts and not tts.texts
        assert any(item.get("item_id") == "draft-item" for item in socket.sent if item["type"] == "conversation.item.delete")
        assert events[-1].payload["response"]["status"] == "cancelled"
    finally:
        await provider.close()


@pytest.mark.anyio
async def test_transaction_not_committed_does_not_announce_handoff():
    provider, socket, store, reviewer, tts = await make_provider([SpeechOutputDecision(True, "safe")])
    reviewer.release.clear()
    events = []
    task = asyncio.create_task(collect(provider, events))
    push_response(socket, "您好。", finish_stream=False)
    try:
        await asyncio.wait_for(reviewer.started.wait(), 1)
        store.append("call", "handoff_requested", "handoff", {"status": "requested"})
        reviewer.release.set()
        socket.incoming.put_nowait(None)
        await asyncio.wait_for(task, 1)
        assert not tts.texts
        assert not any(event.type == "model_audio_delta" for event in events)
    finally:
        await provider.close()


@pytest.mark.anyio
async def test_tts_transport_failure_emits_terminal_error_without_fake_transcript():
    provider, socket, _store, _reviewer, tts = await make_provider([SpeechOutputDecision(True, "safe")])

    async def broken_stream(_text, *, voice):
        raise TimeoutError("TTS stalled")
        yield

    tts.synthesize = broken_stream
    events = []
    push_response(socket, "您好。")
    try:
        await asyncio.wait_for(collect(provider, events), 1)
        assert any(event.type == "model_error" and event.payload["error"]["code"] == "speech_output_failed" for event in events)
        assert not any(event.type == "ai_transcript_done" for event in events)
    finally:
        await provider.close()


@pytest.mark.anyio
async def test_failed_text_generation_is_reported_instead_of_success_or_silent_cancel():
    provider, socket, _store, _reviewer, tts = await make_provider([])
    socket.incoming.put_nowait({"type": "response.done", "response": {"id": "failed", "status": "failed", "status_details": {"error": "provider_failure"}}})
    socket.incoming.put_nowait(None)
    events = []
    try:
        await asyncio.wait_for(collect(provider, events), 1)
        assert not tts.texts
        assert any(event.type == "model_error" and event.payload["error"]["code"] == "text_generation_failed" for event in events)
    finally:
        await provider.close()


@pytest.mark.anyio
async def test_guarded_output_reaches_existing_runner_playout_without_original_promise():
    provider, socket, store, _reviewer, tts = await make_provider([SpeechOutputDecision(False, "claim")])
    published = asyncio.Event()

    class Publisher:
        async def publish_audio(self, call_id, frame):
            assert frame.data
            published.set()

        async def stop_audio(self, call_id):
            pass

    registry = InMemorySessionRegistry()
    registry.add(CallSession(call_id="call", room_name="room", participant_identity="customer",
                             status=CallSessionStatus.AI_THINKING, effective_config={}))
    runner = RealtimeCallAgentRunner(provider_factory=lambda _session: provider, registry=registry,
                                    event_store=store, audio_publisher=Publisher())
    runner._providers["call"] = provider
    store.add_listener(lambda event: socket.incoming.put_nowait(None) if event.type in {"model_response_done", "model_error"} else None)
    push_response(socket, "请稍等，正在为您转接。", finish_stream=False)
    try:
        await asyncio.wait_for(runner._consume_provider_events("call", provider), 1)
        await asyncio.wait_for(published.wait(), 1)
        assert tts.texts == [CONTROL_SPEECH["none"]]
        transcripts = [e.payload["transcript"] for e in store.list_all("call") if e.type == "ai_transcript_done"]
        assert transcripts == [CONTROL_SPEECH["none"]]
        assert registry.get("call").status != CallSessionStatus.FAILED
    finally:
        await runner.stop("call")


@pytest.mark.anyio
async def test_cancel_during_context_ack_removes_actual_server_item_without_releasing_audio():
    provider, socket, _store, _reviewer, tts = await make_provider([SpeechOutputDecision(True, "safe")])
    context_sent = asyncio.Event()
    original_send = socket.send_json

    async def delayed_ack(payload):
        if payload.get("item", {}).get("role") == "assistant":
            socket.sent.append(payload)
            context_sent.set()
        else:
            await original_send(payload)

    socket.send_json = delayed_ack
    events = []
    task = asyncio.create_task(collect(provider, events))
    push_response(socket, "您好。", finish_stream=False)
    try:
        await asyncio.wait_for(context_sent.wait(), 1)
        await provider.cancel_response()
        socket.incoming.put_nowait({"type": "conversation.item.created", "item": {
            "id": "late-server-item", "role": "assistant", "status": "completed",
            "content": [{"type": "text", "text": "您好。"}],
        }})
        socket.incoming.put_nowait(None)
        await asyncio.wait_for(task, 1)
        assert tts.closed
        assert not any(e.type == "model_audio_delta" for e in events)
        assert {"type": "conversation.item.delete", "item_id": "late-server-item"} in socket.sent
    finally:
        await provider.close()
