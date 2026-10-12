"""远端不回应关闭握手时，转人工仍须及时完成模型连接清理。"""

from __future__ import annotations

import asyncio
import base64
import hashlib

import pytest
from test_ai_call_handoff_tool_feedback import make_runner

from app.services.ai_call.providers.aliyun_qwen_realtime import _default_websocket_factory


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_handoff_releases_unresponsive_model_connection_within_two_seconds():
    disconnected = asyncio.Event()
    peers = set()

    async def unresponsive_peer(reader, writer):
        peers.add(writer)
        try:
            headers = (await reader.readuntil(b"\r\n\r\n")).decode()
            key = next(
                line.split(":", 1)[1].strip()
                for line in headers.splitlines()
                if line.lower().startswith("sec-websocket-key:")
            )
            accept = base64.b64encode(
                hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
            ).decode()
            writer.write(
                (
                    "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                    "Connection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n"
                ).encode()
            )
            await writer.drain()
            # 只模拟供应商缺失关闭响应；客户端和运行器均走真实实现。
            await reader.read()
        finally:
            writer.close()
            await writer.wait_closed()
            peers.discard(writer)
            disconnected.set()

    server = await asyncio.start_server(unresponsive_peer, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    runner, provider, _, _ = make_runner()
    connection = await _default_websocket_factory(f"ws://127.0.0.1:{port}/realtime", {})
    provider.close = connection.close
    try:
        await asyncio.wait_for(runner.suspend_for_handoff("call-handoff-result"), timeout=2)
        await asyncio.wait_for(disconnected.wait(), timeout=0.2)
        assert connection.websocket.state.name == "CLOSED"
        assert connection.websocket.transport.is_closing()
        assert "call-handoff-result" not in runner._providers
        assert "call-handoff-result" not in runner._response_lifecycles
    finally:
        for peer in tuple(peers):
            peer.close()
        server.close()
        await server.wait_closed()
        await connection.close()
        await runner.stop("call-handoff-result")
