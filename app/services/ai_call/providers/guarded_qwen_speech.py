from __future__ import annotations

import asyncio
import base64
import time
import uuid
from collections import deque
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import replace
from typing import Any

from app.services.ai_call.event_store import AiCallEvent, InMemoryEventStore
from app.services.ai_call.providers.aliyun_qwen_realtime import (
    AliyunQwenRealtimeProvider,
    QwenRealtimeSessionConfig,
)
from app.services.ai_call.providers.base import ProviderEvent
from app.services.ai_call.providers.dashscope_qwen_tts_realtime import (
    DashScopeQwenTtsRealtimeProvider,
)
from app.services.ai_call.speech_output_review import SpeechOutputReviewer
from app.services.ai_call.transcript_trust import is_realtime_transcript_semantically_rejected

CONTROL_SPEECH = {
    "confirmation": "您是希望转接人工客服吗？",
    "rejected": "目前没有发起转接，我可以继续为您解答。",
    "failed": "这次转接没有成功，我可以继续为您解答。",
    "unknown": "目前还无法确认转接结果，请稍候。",
    "requested": "正在为您联系人工，请稍候。",
    "accepted": "正在等待人工接入，请稍候。",
    "none": "目前没有发起转接，我可以继续为您解答。",
    "cancelled": "转接已取消，我可以继续为您解答。",
}
REVIEW_UNAVAILABLE_SPEECH = "抱歉，我暂时无法确认这个问题，请您稍后再试。"


class GuardedQwenSpeechProvider(AliyunQwenRealtimeProvider):
    """语音输入保持不变，只有已审核文字和服务端状态话术可以进入 TTS。"""

    def __init__(self, *, call_id: str, event_store: InMemoryEventStore,
                 reviewer: SpeechOutputReviewer, tts: DashScopeQwenTtsRealtimeProvider,
                 tts_voice: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.call_id = call_id
        self.event_store = event_store
        self.reviewer = reviewer
        self.tts = tts
        self.tts_voice = tts_voice
        self._events: asyncio.Queue[ProviderEvent | None] = asyncio.Queue(maxsize=256)
        self._reader: asyncio.Task | None = None
        self._speech: asyncio.Task | None = None
        self._active_response: str | None = None
        self._model_active = False
        self._cancel_before_started = False
        self._cancelled: set[str] = set()
        self._drafts: dict[str, str] = {}
        self._handoff_results: dict[str, str] = {}
        self._pending_control: str | None = None
        self._handoff_state = "none"
        self._state_version = 0
        self._closed = False
        self._context_creates: deque[tuple[str, str, asyncio.Future[str]]] = deque()

    async def connect(self) -> None:
        await super().connect()
        self.event_store.add_listener(self._observe_handoff)

    async def update_session(self, config: QwenRealtimeSessionConfig) -> None:
        await super().update_session(replace(config, modalities=("text",)))
        await self._emit("speech_output_configured", {
            "mode": "reviewed_text_streaming_tts", "inputModel": self.model,
            "ttsModel": self.tts.model, "ttsVoice": self.tts_voice, "reviewModel": self.reviewer.model,
        })

    async def submit_tool_result(self, tool_call_id: str, output: str) -> None:
        await super().submit_tool_result(tool_call_id, output)
        control = self._handoff_results.pop(tool_call_id, None)
        if control is not None:
            self._pending_control = control

    async def create_response(self, input_text: str | None = None) -> None:
        control = self._pending_control
        self._pending_control = None
        if control is None:
            self._active_response = None
            self._cancel_before_started = False
            self._model_active = True
            await super().create_response(input_text)
            return
        response_id = f"controlled_{uuid.uuid4().hex}"
        self._active_response = response_id
        await self._emit("model_response_started", {"response": {"id": response_id}})
        self._speech = asyncio.create_task(self._render(response_id, "", control=control))

    async def receive_events(self) -> AsyncIterator[ProviderEvent]:
        self._reader = asyncio.create_task(self._read_model())
        try:
            while True:
                event = await self._events.get()
                if event is None:
                    return
                if (event.type in {"model_audio_delta", "ai_transcript_delta", "ai_transcript_done", "model_audio_done"}
                        and event.payload.get("response_id") in self._cancelled):
                    continue
                yield event
        finally:
            if self._reader and not self._reader.done():
                self._reader.cancel()
                with suppress(asyncio.CancelledError):
                    await self._reader

    async def _read_model(self) -> None:
        try:
            async for event in super().receive_events():
                payload = event.payload
                response = payload.get("response") or {}
                response_id = str(payload.get("response_id") or response.get("id") or "")
                if event.type == "conversation_item_created" and self._context_creates:
                    item = payload.get("item") or {}
                    if item.get("role") == "assistant" and item.get("status") == "completed":
                        pending_id, expected, future = self._context_creates.popleft()
                        actual = "".join(part.get("text", "") for part in item.get("content", []))
                        if actual != expected or not item.get("id"):
                            future.set_exception(ValueError("模型上下文写入确认不匹配"))
                        else:
                            if pending_id in self._cancelled:
                                await self._send({"type": "conversation.item.delete", "item_id": item["id"]})
                                future.cancel()
                            else:
                                future.set_result(item["id"])
                if event.type == "model_response_started":
                    self._active_response = response_id
                    self._model_active = True
                    if self._cancel_before_started:
                        self._cancelled.add(response_id)
                        self._cancel_before_started = False
                if event.type in {"model_text_delta", "model_text_done"}:
                    text = str(payload.get("text") or payload.get("delta") or "")
                    self._drafts[response_id] = (
                        text if event.type == "model_text_done" else self._drafts.get(response_id, "") + text
                    )
                    continue
                if event.type in {"model_audio_delta", "ai_transcript_delta", "ai_transcript_done", "model_audio_done"}:
                    # 即使供应商未遵守 text 模态，也不能回退为直接播放。
                    self._cancelled.add(response_id)
                    if event.type == "model_audio_delta":
                        await self._emit("model_error", {"error": {
                            "code": "unexpected_unreviewed_audio", "message": "文本输出会话收到未经审核的音频",
                        }})
                    continue
                if event.type == "model_response_done":
                    self._model_active = False
                    items = [item for item in response.get("output", []) if item.get("type") == "message"]
                    text = "".join(part.get("text", "") for item in items for part in item.get("content", []))
                    text = text or self._drafts.pop(response_id, "")
                    self._drafts.pop(response_id, None)
                    # 所有草稿（包括取消和工具响应）都不能留作已经对客户说过的话。
                    for item in items:
                        await self._send({"type": "conversation.item.delete", "item_id": item["id"]})
                    if text:
                        await self._emit("speech_draft", {
                            "response_id": response_id, "text": text,
                            "status": response.get("status"), "usage": response.get("usage"),
                        })
                    if response.get("status") == "failed":
                        await self._emit("model_error", {"error": {
                            "code": "text_generation_failed", "message": str(response.get("status_details")),
                        }})
                    elif response_id in self._cancelled or response.get("status") != "completed":
                        await self._emit_done(response_id, "cancelled")
                    elif text and not any(item.get("type") == "function_call" for item in response.get("output", [])):
                        self._speech = asyncio.create_task(self._render(response_id, text))
                    else:
                        await self._emit_done(response_id, "completed")
                    continue
                if event.type == "provider_event_unmapped" and str(payload.get("rawType", "")).startswith(
                    ("response.content_part.", "response.output_item.")
                ):
                    continue
                await self._events.put(event)
            if self._speech:
                await self._speech
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._emit("model_error", {"error": {"code": "speech_provider_error", "message": str(exc)}})
        finally:
            if not self._closed and not asyncio.current_task().cancelling():
                await self._events.put(None)

    async def _render(self, response_id: str, draft: str,
                      *, control: str | None = None) -> None:
        started = time.monotonic()
        context_item: str | None = None
        created: asyncio.Future[str] | None = None
        completed = False
        try:
            version = self._state_version
            if control is not None:
                text = CONTROL_SPEECH.get(control, REVIEW_UNAVAILABLE_SPEECH)
                reason = "controlled_handoff_state"
            else:
                text, reason = await self._review(draft, response_id)
            if self._state_version != version:
                text = CONTROL_SPEECH.get(self._handoff_state, "")
                reason = "handoff_state_changed"
                version = self._state_version
            if self._closed or response_id in self._cancelled:
                return
            if self._handoff_state in {"pending", "connected"}:
                await self._emit_done(response_id, "cancelled")
                return
            if not text:
                await self._emit_done(response_id, "cancelled")
                return
            await self._emit("speech_output_selected", {
                "response_id": response_id, "text": text, "reason": reason,
                "handoffState": self._handoff_state, "reviewMs": round((time.monotonic() - started) * 1000),
                "ttsVoice": self.tts_voice, "ttsModel": self.tts.model,
            })
            first_audio = True
            stream = self.tts.synthesize(text, voice=self.tts_voice)
            try:
                async for audio in stream:
                    if self._closed or response_id in self._cancelled:
                        return
                    if self._state_version != version:
                        self._cancelled.add(response_id)
                        await self._emit_done(response_id, "cancelled")
                        return
                    if first_audio:
                        created = asyncio.get_running_loop().create_future()
                        self._context_creates.append((response_id, text, created))
                        await self._send({"type": "conversation.item.create", "item": {
                            "type": "message", "role": "assistant",
                            "content": [{"type": "text", "text": text}],
                        }})
                        # Qwen 会忽略客户端 item.id，取消时必须删除服务端实际创建的消息。
                        context_item = await asyncio.wait_for(asyncio.shield(created), timeout=3)
                        if self._closed or response_id in self._cancelled or self._state_version != version:
                            self._cancelled.add(response_id)
                            await self._emit_done(response_id, "cancelled")
                            return
                    await self._emit("model_audio_delta", {
                        "response_id": response_id, "delta": base64.b64encode(audio).decode(),
                        "item_id": context_item, "speechValidated": True,
                    })
                    if first_audio:
                        await self._emit("ai_transcript_delta", {
                            "response_id": response_id, "item_id": context_item, "delta": text,
                        })
                        await self._emit("speech_first_audio", {
                            "response_id": response_id, "elapsedMs": round((time.monotonic() - started) * 1000),
                        })
                        first_audio = False
            finally:
                await stream.aclose()
            if first_audio:
                raise RuntimeError("TTS 未返回任何音频")
            await self._emit("ai_transcript_done", {
                "response_id": response_id, "item_id": context_item, "transcript": text,
            })
            await self._emit("model_audio_done", {"response_id": response_id})
            await self._emit_done(response_id, "completed", text)
            completed = True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._emit("model_error", {"error": {
                "code": "speech_output_failed", "message": str(exc),
            }})
        finally:
            if context_item is None and created is not None and created.done() and not created.cancelled() and created.exception() is None:
                context_item = created.result()
            if context_item and not completed and not self._closed:
                await self._send({"type": "conversation.item.delete", "item_id": context_item})

    async def _review(self, draft: str, response_id: str) -> tuple[str, str]:
        text = draft
        try:
            async with asyncio.timeout(self.reviewer.TIMEOUT_SECONDS):
                for attempt in range(2):
                    decision = await self.reviewer.review(text, context=self._context())
                    await self._emit("speech_output_reviewed", {
                        "response_id": response_id, "attempt": attempt, "allowed": decision.allowed,
                        "reason": decision.reason, "text": text,
                    })
                    if decision.allowed:
                        return text, "approved" if attempt == 0 else "revised_and_approved"
                    if not decision.replacement:
                        break
                    text = decision.replacement
        except Exception as exc:
            await self._emit("speech_output_review_failed", {
                "response_id": response_id, "errorType": type(exc).__name__, "message": str(exc),
            })
            return REVIEW_UNAVAILABLE_SPEECH, "review_unavailable"
        return CONTROL_SPEECH.get(self._handoff_state, REVIEW_UNAVAILABLE_SPEECH), "review_rejected"

    def _context(self) -> dict[str, Any]:
        history = [
            {"role": "assistant" if event.type == "ai_transcript_done" else "customer",
             "text": str(event.payload.get("customerText") or event.payload.get("transcript") or event.payload.get("text") or "")}
            for event in self.event_store.list_all(self.call_id)
            if event.type in {"ai_transcript_done", "user_transcript_done"}
            and not is_realtime_transcript_semantically_rejected(event.payload)
        ][-6:]
        return {"handoff_state": self._handoff_state, "recent_dialogue": history}

    def _observe_handoff(self, event: AiCallEvent) -> None:
        if event.call_id != self.call_id:
            return
        payload = event.payload
        tool_id = payload.get("toolCallId")
        state = None
        if event.type == "handoff_requested":
            # AI 挂起发生在创建事务提交前，此时不能宣告已进入队列。
            state = "pending"
        elif event.type in {"handoff_auto_triggered", "handoff_accepted",
                          "handoff_connected", "handoff_canceled", "handoff_failed", "handoff_expired"}:
            state = payload.get("status")
            state = {"canceled": "cancelled", "expired": "failed"}.get(state, state)
        elif event.type == "handoff_confirmation_requested" or (
            event.type == "handoff_tool_requested" and (
                payload.get("confirmationRequired") or payload.get("reason") == "business_escalation"
            )
        ):
            state = "confirmation"
        elif event.type == "handoff_tool_ignored":
            state = self._handoff_state if payload.get("localDecisionReason") == "handoff_status_query" else "rejected"
        elif event.type == "handoff_tool_result_timeout":
            state = "unknown"
        elif event.type == "handoff_auto_trigger_failed":
            state = "failed"
        elif event.type == "handoff_confirmation_declined":
            state = "none"
        elif event.type == "handoff_intent_ignored" and tool_id:
            reason = payload.get("reason")
            if reason in {"duplicate_trigger", "active_handoff_exists"}:
                state = self._handoff_state if self._handoff_state in {"requested", "accepted", "connected"} else "unknown"
            elif reason == "handoff_confirmation_unresolved":
                state = "confirmation"
            else:
                state = "failed"
        if state:
            if self._handoff_state in {"pending", "requested", "accepted", "connected"} and state in {"confirmation", "rejected"}:
                state = self._handoff_state
            if state != self._handoff_state:
                self._state_version += 1
            self._handoff_state = str(state)
            if isinstance(tool_id, str):
                self._handoff_results[tool_id] = str(state)

    async def cancel_response(self) -> None:
        response_id = self._active_response
        if self._model_active and response_id is None:
            self._cancel_before_started = True
        if response_id:
            self._cancelled.add(response_id)
            self._drafts.pop(response_id, None)
        self._pending_control = None
        if self._speech and not self._speech.done():
            self._speech.cancel()
            with suppress(asyncio.CancelledError):
                await self._speech
            if response_id:
                await self._emit_done(response_id, "cancelled")
        if self._model_active:
            await super().cancel_response()

    async def close(self) -> None:
        self._closed = True
        self.event_store.remove_listener(self._observe_handoff)
        for task in (self._speech, self._reader):
            if task and task is not asyncio.current_task() and not task.done():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        for _, _, future in self._context_creates:
            future.cancel()
        self._context_creates.clear()
        await super().close()

    async def _emit(self, event_type: str, payload: dict) -> None:
        await self._events.put(ProviderEvent(event_type, payload))

    async def _emit_done(self, response_id: str, status: str, text: str = "") -> None:
        await self._emit("model_response_done", {"response": {
            "id": response_id, "status": status,
            "output": [{"type": "message", "content": [{"type": "audio", "transcript": text}]}] if text else [],
        }})
