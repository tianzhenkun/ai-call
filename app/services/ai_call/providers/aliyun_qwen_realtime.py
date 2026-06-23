from __future__ import annotations

import base64
import inspect
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.parse import urlencode

from app.services.ai_call.providers.base import ProviderEvent


class QwenWebSocketProtocol(Protocol):
    async def send_json(self, payload: dict[str, Any]) -> None: ...

    async def receive_json(self) -> dict[str, Any]: ...

    async def close(self) -> None: ...


WebSocketFactory = Callable[[str, dict[str, str]], Awaitable[QwenWebSocketProtocol]]

QWEN_SERVER_EVENT_MAPPING = {
    "session.created": "model_session_started",
    "session.updated": "model_session_updated",
    "conversation.item.created": "conversation_item_created",
    "input_audio_buffer.committed": "input_audio_committed",
    "input_audio_buffer.cleared": "input_audio_cleared",
    "input_audio_buffer.speech_started": "user_speech_started",
    "input_audio_buffer.speech_stopped": "user_speech_stopped",
    "conversation.item.input_audio_transcription.delta": "user_transcript_delta",
    "conversation.item.input_audio_transcription.completed": "user_transcript_done",
    "conversation.item.input_audio_transcription.failed": "user_transcript_failed",
    "response.created": "model_response_started",
    "response.audio.delta": "model_audio_delta",
    "response.audio.done": "model_audio_done",
    "response.audio_transcript.delta": "ai_transcript_delta",
    "response.audio_transcript.done": "ai_transcript_done",
    "response.function_call_arguments.done": "tool_call_done",
    "response.done": "model_response_done",
    "error": "model_error",
}


SCHEDULE_CALL_END_TOOL = {
    "type": "function",
    "function": {
        "name": "schedule_call_end",
        "description": (
            "仅当上下文明确表明通话已适合结束时，用于安排当前通话在最后一句回复播放完成后结束。"
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "reason": {
                    "type": "string",
                    "description": (
                        "结束原因。customer_end 表示用户侧明确不再继续；"
                        "task_completed 表示当前业务目标已完成且无需继续追问。"
                    ),
                    "enum": ["customer_end", "task_completed"],
                },
            },
            "required": ["reason"],
        },
    },
}


REQUEST_HANDOFF_TOOL = {
    "type": "function",
    "function": {
        "name": "request_handoff",
        "description": "当当前通话需要由人工坐席继续处理时，用于发起转人工请求。",
        "parameters": {
            "type": "object",
            "properties": {
                "reason": {
                    "type": "string",
                    "description": (
                        "转人工原因。customer_request 表示用户侧明确要求人工接入；"
                        "business_escalation 表示当前问题需要人工继续处理。"
                    ),
                    "enum": ["customer_request", "business_escalation"],
                },
            },
            "required": ["reason"],
        },
    },
}

DEFAULT_REALTIME_TOOLS = [SCHEDULE_CALL_END_TOOL, REQUEST_HANDOFF_TOOL]


@dataclass(frozen=True, slots=True)
class QwenRealtimeSessionConfig:
    voice: str
    instructions: str
    vad_type: str
    vad_threshold: float
    vad_silence_duration_ms: int
    input_audio_language: str = "zh"
    vad_create_response: bool = False
    vad_interrupt_response: bool = False
    temperature: float = 0.7
    tools: list[dict[str, Any]] = field(default_factory=list)


def build_session_update_event(config: QwenRealtimeSessionConfig) -> dict[str, Any]:
    session: dict[str, Any] = {
        "modalities": ["text", "audio"],
        "voice": config.voice,
        "input_audio_format": "pcm",
        "output_audio_format": "pcm",
        "input_audio_transcription": {
            "language": config.input_audio_language,
        },
        "instructions": config.instructions,
        "turn_detection": {
            "type": config.vad_type,
            "threshold": config.vad_threshold,
            "silence_duration_ms": config.vad_silence_duration_ms,
            "create_response": config.vad_create_response,
            "interrupt_response": config.vad_interrupt_response,
        },
        "temperature": config.temperature,
    }
    if config.tools:
        session["tools"] = config.tools

    return {
        "type": "session.update",
        "session": session,
    }


def map_qwen_server_event(event: dict[str, Any]) -> str | None:
    event_type = event.get("type")
    if not isinstance(event_type, str):
        return None
    return QWEN_SERVER_EVENT_MAPPING.get(event_type)


def build_unmapped_provider_event(event: dict[str, Any]) -> ProviderEvent:
    raw_type = event.get("type")
    return ProviderEvent(
        type="provider_event_unmapped",
        payload={
            "rawType": raw_type if isinstance(raw_type, str) else None,
            "raw": sanitize_qwen_event_payload(event),
        },
    )


def sanitize_qwen_event_payload(payload: Any) -> Any:
    if isinstance(payload, dict):
        return {str(key): _sanitize_qwen_value(str(key), value) for key, value in payload.items()}
    if isinstance(payload, list):
        return [sanitize_qwen_event_payload(value) for value in payload]
    if isinstance(payload, str):
        return _truncate_payload_string(payload)
    return payload


def _sanitize_qwen_value(key: str, value: Any) -> Any:
    key_lower = key.lower()
    if any(
        sensitive in key_lower
        for sensitive in ("authorization", "token", "api_key", "apikey", "secret", "password")
    ):
        return "<redacted>"
    if key_lower in {"audio"}:
        return "<redacted_audio>"
    if key_lower == "delta" and isinstance(value, str) and len(value) > 512:
        return "<redacted_large_delta>"
    return sanitize_qwen_event_payload(value)


def _truncate_payload_string(value: str, max_length: int = 500) -> str:
    if len(value) <= max_length:
        return value
    return f"{value[:max_length]}...<truncated:{len(value)}>"


class AliyunQwenRealtimeProvider:
    def __init__(
        self,
        realtime_url: str,
        api_key: str,
        model: str,
        websocket_factory: WebSocketFactory | None = None,
    ) -> None:
        self.realtime_url = realtime_url.rstrip("?")
        self.api_key = api_key
        self.model = model
        self.websocket_factory = websocket_factory or _default_websocket_factory
        self._websocket: QwenWebSocketProtocol | None = None

    async def connect(self) -> None:
        self._websocket = await self.websocket_factory(
            self._build_connect_url(),
            {"Authorization": f"Bearer {self.api_key}"},
        )

    async def update_session(self, config: QwenRealtimeSessionConfig) -> None:
        await self._send(build_session_update_event(config))

    async def send_audio(self, pcm_frame: bytes) -> None:
        await self._send({
            "type": "input_audio_buffer.append",
            "audio": base64.b64encode(pcm_frame).decode("ascii"),
        })

    async def create_response(self, input_text: str | None = None) -> None:
        if input_text:
            await self._send({
                "type": "conversation.item.create",
                "item": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": input_text}],
                },
            })
        await self._send({"type": "response.create"})

    async def submit_tool_result(self, tool_call_id: str, output: str) -> None:
        await self._send({
            "type": "conversation.item.create",
            "item": {
                "type": "function_call_output",
                "call_id": tool_call_id,
                "output": output,
            },
        })

    async def cancel_response(self) -> None:
        await self._send({"type": "response.cancel"})

    async def clear_input_audio(self) -> None:
        await self._send({"type": "input_audio_buffer.clear"})

    async def receive_events(self) -> AsyncIterator[ProviderEvent]:
        websocket = self._require_websocket()
        while True:
            try:
                payload = await websocket.receive_json()
            except StopAsyncIteration:
                return
            event_type = map_qwen_server_event(payload)
            if event_type is not None:
                yield ProviderEvent(type=event_type, payload=payload)
            else:
                yield build_unmapped_provider_event(payload)

    async def close(self) -> None:
        if self._websocket is None:
            return
        await self._websocket.close()
        self._websocket = None

    async def _send(self, payload: dict[str, Any]) -> None:
        await self._require_websocket().send_json(payload)

    def _require_websocket(self) -> QwenWebSocketProtocol:
        if self._websocket is None:
            raise RuntimeError("Qwen Realtime provider is not connected")
        return self._websocket

    def _build_connect_url(self) -> str:
        separator = "&" if "?" in self.realtime_url else "?"
        return f"{self.realtime_url}{separator}{urlencode({'model': self.model})}"


async def _default_websocket_factory(
    url: str,
    headers: dict[str, str],
) -> QwenWebSocketProtocol:
    try:
        import websockets
    except ImportError as exc:
        raise RuntimeError("缺少 websockets 依赖，无法连接 Qwen Realtime WebSocket") from exc

    connect_kwargs: dict[str, Any] = {"additional_headers": headers}
    try:
        if "proxy" in inspect.signature(websockets.connect).parameters:
            connect_kwargs["proxy"] = None
    except (TypeError, ValueError):
        pass

    try:
        websocket = await websockets.connect(url, **connect_kwargs)
    except TypeError:
        connect_kwargs.pop("additional_headers", None)
        connect_kwargs["extra_headers"] = headers
        websocket = await websockets.connect(url, **connect_kwargs)
    return _WebsocketsJsonConnection(websocket)


class _WebsocketsJsonConnection:
    def __init__(self, websocket: Any) -> None:
        self.websocket = websocket

    async def send_json(self, payload: dict[str, Any]) -> None:
        await self.websocket.send(json.dumps(payload, ensure_ascii=False))

    async def receive_json(self) -> dict[str, Any]:
        message = await self.websocket.recv()
        if isinstance(message, bytes):
            message = message.decode("utf-8")
        data = json.loads(message)
        if not isinstance(data, dict):
            raise ValueError("Qwen Realtime WebSocket 返回了非对象 JSON")
        return data

    async def close(self) -> None:
        await self.websocket.close()
