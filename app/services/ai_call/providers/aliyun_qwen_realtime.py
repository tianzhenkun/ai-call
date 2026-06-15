from __future__ import annotations

import base64
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
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
    "input_audio_buffer.speech_started": "user_speech_started",
    "input_audio_buffer.speech_stopped": "user_speech_stopped",
    "conversation.item.input_audio_transcription.delta": "user_transcript_delta",
    "conversation.item.input_audio_transcription.completed": "user_transcript_done",
    "response.created": "model_response_started",
    "response.audio.delta": "model_audio_delta",
    "response.audio.done": "model_audio_done",
    "response.audio_transcript.delta": "ai_transcript_delta",
    "response.audio_transcript.done": "ai_transcript_done",
    "response.done": "model_response_done",
    "error": "model_error",
}


@dataclass(frozen=True, slots=True)
class QwenRealtimeSessionConfig:
    voice: str
    instructions: str
    vad_type: str
    vad_threshold: float
    vad_silence_duration_ms: int
    auto_create_response: bool = False
    auto_interrupt_response: bool = False
    temperature: float = 0.7
    input_audio_transcription_model: str = "qwen3-asr-flash-realtime"
    input_audio_transcription_language: str = "zh"


def build_session_update_event(config: QwenRealtimeSessionConfig) -> dict[str, Any]:
    return {
        "type": "session.update",
        "session": {
            "modalities": ["text", "audio"],
            "voice": config.voice,
            "input_audio_format": "pcm",
            "output_audio_format": "pcm",
            "input_audio_transcription": {
                "model": config.input_audio_transcription_model,
                "language": config.input_audio_transcription_language,
            },
            "instructions": config.instructions,
            "turn_detection": {
                "type": config.vad_type,
                "threshold": config.vad_threshold,
                "silence_duration_ms": config.vad_silence_duration_ms,
                "create_response": config.auto_create_response,
                "interrupt_response": config.auto_interrupt_response,
            },
            "temperature": config.temperature,
        },
    }


def map_qwen_server_event(event: dict[str, Any]) -> str | None:
    event_type = event.get("type")
    if not isinstance(event_type, str):
        return None
    return QWEN_SERVER_EVENT_MAPPING.get(event_type)


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
        await self._send(
            {
                "type": "input_audio_buffer.append",
                "audio": base64.b64encode(pcm_frame).decode("ascii"),
            }
        )

    async def create_response(self, input_text: str | None = None) -> None:
        if input_text:
            await self._send(
                {
                    "type": "conversation.item.create",
                    "item": {
                        "type": "message",
                        "role": "user",
                        "content": [{"type": "input_text", "text": input_text}],
                    },
                }
            )
        await self._send({"type": "response.create"})

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

    try:
        websocket = await websockets.connect(url, additional_headers=headers)
    except TypeError:
        websocket = await websockets.connect(url, extra_headers=headers)
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
