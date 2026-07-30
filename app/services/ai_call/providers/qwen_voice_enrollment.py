from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

DEFAULT_QWEN_VOICE_ENROLLMENT_ENDPOINT = (
    "https://dashscope.aliyuncs.com/api/v1/services/audio/tts/customization"
)
QWEN_VOICE_ENROLLMENT_MODEL = "qwen-voice-enrollment"


class VoiceProviderError(RuntimeError):
    """Qwen 音色复刻 Provider 的基础错误。"""


class VoiceProviderRejectedError(VoiceProviderError):
    """请求被 Provider 明确拒绝。"""


class VoiceProviderRetryableError(VoiceProviderError):
    """请求可由上层按策略重试。"""


class VoiceProviderResultUnknownError(VoiceProviderError):
    """请求可能已被 Provider 执行，禁止直接重试创建。"""


class VoiceProviderProtocolError(VoiceProviderError):
    """Provider 响应不符合约定协议。"""


@dataclass(frozen=True, slots=True)
class VoiceCreateResult:
    voice: str
    request_id: str | None


@dataclass(frozen=True, slots=True)
class VoiceListItem:
    voice: str
    target_model: str | None
    gmt_create: str | None


class QwenVoiceEnrollmentProvider:
    def __init__(
        self,
        *,
        api_key: str,
        target_model: str,
        endpoint: str = DEFAULT_QWEN_VOICE_ENROLLMENT_ENDPOINT,
        timeout_seconds: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.api_key = api_key
        self.target_model = target_model
        self.endpoint = endpoint
        self.timeout_seconds = timeout_seconds
        self.transport = transport

    async def create(
        self,
        *,
        preferred_name: str,
        audio_data_url: str,
    ) -> VoiceCreateResult:
        body, request_id = await self._request(
            {
                "action": "create",
                "target_model": self.target_model,
                "preferred_name": preferred_name,
                "audio": {"data": audio_data_url},
            },
            result_unknown_on_timeout=True,
        )
        output = body.get("output")
        if not isinstance(output, dict):
            raise VoiceProviderProtocolError("Qwen 创建响应缺少 output")
        voice = str(output.get("voice") or "").strip()
        if not voice:
            raise VoiceProviderProtocolError("Qwen 创建响应缺少 voice")
        return VoiceCreateResult(voice=voice, request_id=request_id)

    async def list(
        self,
        *,
        page_index: int = 0,
        page_size: int = 1000,
    ) -> list[VoiceListItem]:
        body, _ = await self._request(
            {
                "action": "list",
                "page_index": page_index,
                "page_size": page_size,
            }
        )
        output = body.get("output")
        if not isinstance(output, dict):
            raise VoiceProviderProtocolError("Qwen 列表响应缺少 output")
        voice_list = output.get("voice_list")
        if not isinstance(voice_list, list):
            raise VoiceProviderProtocolError("Qwen 列表响应缺少 voice_list")

        result: list[VoiceListItem] = []
        for item in voice_list:
            if not isinstance(item, dict):
                raise VoiceProviderProtocolError("Qwen 列表响应 voice_list 格式异常")
            voice = str(item.get("voice") or "").strip()
            if not voice:
                raise VoiceProviderProtocolError("Qwen 列表响应缺少 voice")
            result.append(
                VoiceListItem(
                    voice=voice,
                    target_model=_optional_string(item.get("target_model")),
                    gmt_create=_optional_string(item.get("gmt_create")),
                )
            )
        return result

    async def delete(self, *, voice: str) -> str | None:
        _, request_id = await self._request({"action": "delete", "voice": voice})
        return request_id

    async def _request(
        self,
        input_values: dict[str, Any],
        *,
        result_unknown_on_timeout: bool = False,
    ) -> tuple[dict[str, Any], str | None]:
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout_seconds,
                transport=self.transport,
            ) as client:
                response = await client.post(
                    self.endpoint,
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": QWEN_VOICE_ENROLLMENT_MODEL,
                        "input": input_values,
                    },
                )
        except (httpx.ReadTimeout, httpx.WriteTimeout):
            if result_unknown_on_timeout:
                raise VoiceProviderResultUnknownError(
                    "Qwen 创建请求超时，执行结果未知"
                ) from None
            raise VoiceProviderRetryableError("Qwen 请求超时") from None
        except httpx.RequestError:
            raise VoiceProviderRetryableError("Qwen 请求连接失败") from None

        if response.status_code == 429 or response.status_code >= 500:
            raise VoiceProviderRetryableError(
                f"Qwen 返回可重试 HTTP 状态 {response.status_code}"
            )
        if 400 <= response.status_code < 500:
            raise VoiceProviderRejectedError(
                f"Qwen 拒绝请求，HTTP 状态 {response.status_code}"
            )
        if not 200 <= response.status_code < 300:
            raise VoiceProviderProtocolError(
                f"Qwen 返回非预期 HTTP 状态 {response.status_code}"
            )

        try:
            body = response.json()
        except ValueError:
            raise VoiceProviderProtocolError("Qwen 响应不是有效 JSON") from None
        if not isinstance(body, dict):
            raise VoiceProviderProtocolError("Qwen 响应不是 JSON 对象")
        return body, _request_id(body)


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _request_id(body: dict[str, Any]) -> str | None:
    return _optional_string(body.get("request_id") or body.get("requestId"))
