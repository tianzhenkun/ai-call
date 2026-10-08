from __future__ import annotations

import asyncio
import base64
from collections.abc import AsyncIterator
from urllib.parse import urlencode

from app.services.ai_call.providers import aliyun_qwen_realtime as qwen

QWEN_TTS_DEFAULT_MODEL = "qwen3-tts-flash-realtime-2025-11-27"


class DashScopeQwenTtsRealtimeProvider:
    """复用候选实现的 commit 协议，每段已审核回复独立连接，取消时关闭连接。"""

    def __init__(self, *, realtime_url: str, api_key: str, model: str = QWEN_TTS_DEFAULT_MODEL,
                 websocket_factory: qwen.WebSocketFactory | None = None) -> None:
        if not realtime_url or not api_key or not model:
            raise ValueError("流式 TTS 缺少地址、模型或密钥配置")
        self.realtime_url = realtime_url.rstrip("?")
        self.api_key = api_key
        self.model = model
        self.websocket_factory = websocket_factory or qwen._default_websocket_factory

    async def synthesize(self, text: str, *, voice: str) -> AsyncIterator[bytes]:
        if not text.strip() or not voice:
            raise ValueError("流式 TTS 文本和音色不能为空")
        separator = "&" if "?" in self.realtime_url else "?"
        socket = await asyncio.wait_for(self.websocket_factory(
            f"{self.realtime_url}{separator}{urlencode({'model': self.model})}",
            {"Authorization": f"Bearer {self.api_key}"},
        ), timeout=10)
        try:
            async with asyncio.timeout(10):
                await socket.send_json({"type": "session.update", "session": {
                    "voice": voice, "mode": "commit", "language_type": "Chinese",
                    "response_format": "pcm", "sample_rate": 24000,
                }})
                while True:
                    event = await socket.receive_json()
                    self._raise_error(event)
                    if event.get("type") == "session.updated":
                        break
                await socket.send_json({"type": "input_text_buffer.append", "text": text})
                await socket.send_json({"type": "input_text_buffer.commit"})
            received_audio = False
            async with asyncio.timeout(30):
                while True:
                    event = await asyncio.wait_for(socket.receive_json(), timeout=10)
                    self._raise_error(event)
                    if event.get("type") == "response.audio.delta":
                        audio = base64.b64decode(event.get("delta", ""), validate=True)
                        if not audio or len(audio) % 2:
                            raise ValueError("流式 TTS 返回无效 PCM 音频")
                        received_audio = True
                        yield audio
                    elif event.get("type") == "response.done":
                        if event.get("response", {}).get("status") != "completed" or not received_audio:
                            raise RuntimeError("流式 TTS 未完成音频合成")
                        await socket.send_json({"type": "session.finish"})
                        return
                    elif event.get("type") == "session.finished":
                        raise RuntimeError("流式 TTS 在回复完成前结束")
        finally:
            await socket.close()

    @staticmethod
    def _raise_error(event: dict) -> None:
        if event.get("type") == "error":
            error = event.get("error") or {}
            raise RuntimeError(f"流式 TTS 失败: {error.get('code')}: {error.get('message')}")
