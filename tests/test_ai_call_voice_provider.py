from __future__ import annotations

from collections.abc import Callable
from traceback import format_exception
from typing import Any

import httpx
import pytest

from app.services.ai_call.providers.qwen_voice_enrollment import (
    QwenVoiceEnrollmentProvider,
    VoiceProviderProtocolError,
    VoiceProviderRejectedError,
    VoiceProviderResultUnknownError,
    VoiceProviderRetryableError,
)

API_KEY = "test-secret-key"
TARGET_MODEL = "qwen3.5-omni-plus-realtime"
ENDPOINT = "https://workspace.example.test/api/v1/services/audio/tts/customization"
AUDIO_DATA_URL = "data:audio/mpeg;base64,AA=="


def _provider(
    handler: Callable[[httpx.Request], httpx.Response],
) -> QwenVoiceEnrollmentProvider:
    return QwenVoiceEnrollmentProvider(
        api_key=API_KEY,
        target_model=TARGET_MODEL,
        endpoint=ENDPOINT,
        transport=httpx.MockTransport(handler),
    )


def _exception_chain_text(error: BaseException) -> str:
    pending = [error]
    seen: set[int] = set()
    messages: list[str] = []
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        messages.append(str(current))
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        if current.__context__ is not None:
            pending.append(current.__context__)
    return "\n".join(messages)


@pytest.mark.anyio
async def test_create_uses_server_owned_contract_and_bearer_key() -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["headers"] = dict(request.headers)
        seen["json"] = request.read()
        return httpx.Response(
            200,
            json={
                "output": {"voice": "qwen-omni-vc-demo"},
                "request_id": "req-create",
            },
        )

    result = await _provider(handler).create(
        preferred_name="vc123",
        audio_data_url=AUDIO_DATA_URL,
    )

    assert result.voice == "qwen-omni-vc-demo"
    assert result.request_id == "req-create"
    assert seen["url"] == ENDPOINT
    assert seen["headers"]["authorization"] == f"Bearer {API_KEY}"
    assert seen["headers"]["content-type"] == "application/json"
    assert httpx.Response(200, content=seen["json"]).json() == {
        "model": "qwen-voice-enrollment",
        "input": {
            "action": "create",
            "target_model": TARGET_MODEL,
            "preferred_name": "vc123",
            "audio": {"data": AUDIO_DATA_URL},
        },
    }


@pytest.mark.anyio
async def test_list_sends_pagination_and_parses_voice_list() -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["json"] = request.read()
        return httpx.Response(
            200,
            json={
                "output": {
                    "voice_list": [
                        {
                            "voice": "qwen-omni-vc-one",
                            "target_model": TARGET_MODEL,
                            "gmt_create": "2026-07-30 12:00:00",
                        },
                        {"voice": "qwen-omni-vc-two"},
                    ]
                },
                "request_id": "req-list",
            },
        )

    result = await _provider(handler).list(page_index=2, page_size=50)

    assert httpx.Response(200, content=seen["json"]).json() == {
        "model": "qwen-voice-enrollment",
        "input": {
            "action": "list",
            "page_index": 2,
            "page_size": 50,
        },
    }
    assert [(item.voice, item.target_model, item.gmt_create) for item in result] == [
        ("qwen-omni-vc-one", TARGET_MODEL, "2026-07-30 12:00:00"),
        ("qwen-omni-vc-two", None, None),
    ]


@pytest.mark.anyio
async def test_delete_sends_voice_and_accepts_camel_case_request_id() -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["json"] = request.read()
        return httpx.Response(200, json={"requestId": "req-delete"})

    request_id = await _provider(handler).delete(voice="qwen-omni-vc-one")

    assert request_id == "req-delete"
    assert httpx.Response(200, content=seen["json"]).json() == {
        "model": "qwen-voice-enrollment",
        "input": {
            "action": "delete",
            "voice": "qwen-omni-vc-one",
        },
    }


@pytest.mark.anyio
@pytest.mark.parametrize("status_code", [400, 401, 403, 404])
async def test_non_rate_limit_4xx_is_rejected(status_code: int) -> None:
    provider = _provider(
        lambda _: httpx.Response(status_code, json={"message": "invalid request"})
    )

    with pytest.raises(VoiceProviderRejectedError, match=str(status_code)):
        await provider.list()


@pytest.mark.anyio
@pytest.mark.parametrize("status_code", [429, 500, 503])
async def test_rate_limit_and_server_errors_are_retryable(status_code: int) -> None:
    provider = _provider(
        lambda _: httpx.Response(status_code, json={"message": "temporary failure"})
    )

    with pytest.raises(VoiceProviderRetryableError, match=str(status_code)):
        await provider.list()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "error_factory",
    [
        lambda request: httpx.ConnectError("connection failed", request=request),
        lambda request: httpx.ConnectTimeout("connection timed out", request=request),
        lambda request: httpx.PoolTimeout("pool timed out", request=request),
    ],
)
async def test_connection_failures_are_retryable(
    error_factory: Callable[[httpx.Request], httpx.RequestError],
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise error_factory(request)

    with pytest.raises(VoiceProviderRetryableError):
        await _provider(handler).list()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "error_factory",
    [
        lambda request: httpx.ReadTimeout("read timed out", request=request),
        lambda request: httpx.WriteTimeout("write timed out", request=request),
    ],
)
async def test_create_read_or_write_timeout_has_unknown_result(
    error_factory: Callable[[httpx.Request], httpx.RequestError],
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise error_factory(request)

    with pytest.raises(VoiceProviderResultUnknownError):
        await _provider(handler).create(
            preferred_name="vc123",
            audio_data_url=AUDIO_DATA_URL,
        )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "error_factory",
    [
        lambda request: httpx.ReadTimeout("read timed out", request=request),
        lambda request: httpx.WriteTimeout("write timed out", request=request),
        lambda request: httpx.RemoteProtocolError("protocol failed", request=request),
    ],
)
async def test_delete_errors_after_possible_send_have_unknown_result(
    error_factory: Callable[[httpx.Request], httpx.RequestError],
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise error_factory(request)

    with pytest.raises(VoiceProviderResultUnknownError):
        await _provider(handler).delete(voice="qwen-omni-vc-one")


@pytest.mark.anyio
@pytest.mark.parametrize(
    "error_factory",
    [
        lambda request: httpx.ReadError("read failed", request=request),
        lambda request: httpx.WriteError("write failed", request=request),
        lambda request: httpx.RemoteProtocolError("protocol failed", request=request),
        lambda request: httpx.RequestError("request failed", request=request),
    ],
)
async def test_create_errors_after_possible_send_have_unknown_result(
    error_factory: Callable[[httpx.Request], httpx.RequestError],
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise error_factory(request)

    with pytest.raises(VoiceProviderResultUnknownError):
        await _provider(handler).create(
            preferred_name="vc123",
            audio_data_url=AUDIO_DATA_URL,
        )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "error_factory",
    [
        lambda request: httpx.ConnectError("connection failed", request=request),
        lambda request: httpx.ConnectTimeout("connection timed out", request=request),
        lambda request: httpx.PoolTimeout("pool timed out", request=request),
    ],
)
async def test_create_errors_before_send_are_retryable(
    error_factory: Callable[[httpx.Request], httpx.RequestError],
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise error_factory(request)

    with pytest.raises(VoiceProviderRetryableError):
        await _provider(handler).create(
            preferred_name="vc123",
            audio_data_url=AUDIO_DATA_URL,
        )


@pytest.mark.anyio
async def test_non_create_read_timeout_is_retryable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("read timed out", request=request)

    with pytest.raises(VoiceProviderRetryableError):
        await _provider(handler).list()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "response_factory",
    [
        lambda: httpx.Response(200, text="not-json"),
        lambda: httpx.Response(200, json=[]),
        lambda: httpx.Response(200, json={"output": []}),
        lambda: httpx.Response(200, json={"output": {"voice_list": {}}}),
    ],
)
async def test_invalid_response_structure_is_protocol_error(
    response_factory: Callable[[], httpx.Response],
) -> None:
    with pytest.raises(VoiceProviderProtocolError):
        await _provider(lambda _: response_factory()).list()


@pytest.mark.anyio
async def test_create_without_voice_is_protocol_error() -> None:
    provider = _provider(
        lambda _: httpx.Response(
            200,
            json={"output": {}, "request_id": "req-without-voice"},
        )
    )

    with pytest.raises(VoiceProviderProtocolError, match="voice"):
        await provider.create(
            preferred_name="vc123",
            audio_data_url=AUDIO_DATA_URL,
        )


@pytest.mark.anyio
@pytest.mark.parametrize("voice", [1, True, {}, []])
async def test_create_voice_must_be_string(voice: object) -> None:
    provider = _provider(
        lambda _: httpx.Response(
            200,
            json={"output": {"voice": voice}, "request_id": "req-create"},
        )
    )

    with pytest.raises(VoiceProviderProtocolError, match="voice"):
        await provider.create(
            preferred_name="vc123",
            audio_data_url=AUDIO_DATA_URL,
        )


@pytest.mark.anyio
@pytest.mark.parametrize("voice", [1, True, {}, []])
async def test_list_item_voice_must_be_string(voice: object) -> None:
    provider = _provider(
        lambda _: httpx.Response(
            200,
            json={"output": {"voice_list": [{"voice": voice}]}},
        )
    )

    with pytest.raises(VoiceProviderProtocolError, match="voice"):
        await provider.list()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("target_model", 1),
        ("target_model", {}),
        ("gmt_create", True),
        ("gmt_create", []),
    ],
)
async def test_list_optional_fields_must_be_string_or_none(
    field: str,
    value: object,
) -> None:
    provider = _provider(
        lambda _: httpx.Response(
            200,
            json={
                "output": {
                    "voice_list": [
                        {
                            "voice": "qwen-omni-vc-one",
                            field: value,
                        }
                    ]
                }
            },
        )
    )

    with pytest.raises(VoiceProviderProtocolError, match=field):
        await provider.list()


@pytest.mark.anyio
@pytest.mark.parametrize("field", ["request_id", "requestId"])
@pytest.mark.parametrize("request_id", [1, True, {}, []])
async def test_request_id_must_be_string_or_none(
    field: str,
    request_id: object,
) -> None:
    provider = _provider(
        lambda _: httpx.Response(200, json={field: request_id})
    )

    with pytest.raises(VoiceProviderProtocolError, match=field):
        await provider.delete(voice="qwen-omni-vc-one")


@pytest.mark.anyio
async def test_error_and_logs_do_not_expose_api_key_or_audio_data_url(
    caplog: pytest.LogCaptureFixture,
) -> None:
    provider = _provider(
        lambda _: httpx.Response(
            400,
            json={
                "message": f"invalid key {API_KEY}",
                "audio": AUDIO_DATA_URL,
            },
        )
    )

    with pytest.raises(VoiceProviderRejectedError) as exc_info:
        await provider.create(
            preferred_name="vc123",
            audio_data_url=AUDIO_DATA_URL,
        )

    combined = f"{exc_info.value}\n{caplog.text}"
    assert API_KEY not in combined
    assert AUDIO_DATA_URL not in combined


@pytest.mark.anyio
async def test_network_error_traceback_does_not_expose_sensitive_values() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(
            f"connection failed for {API_KEY} with {AUDIO_DATA_URL}",
            request=request,
        )

    with pytest.raises(VoiceProviderRetryableError) as exc_info:
        await _provider(handler).create(
            preferred_name="vc123",
            audio_data_url=AUDIO_DATA_URL,
        )

    rendered_traceback = "".join(format_exception(exc_info.value))
    assert API_KEY not in rendered_traceback
    assert AUDIO_DATA_URL not in rendered_traceback


@pytest.mark.anyio
async def test_network_error_object_has_no_sensitive_exception_chain() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(
            f"connection failed for {API_KEY} with {AUDIO_DATA_URL}",
            request=request,
        )

    with pytest.raises(VoiceProviderRetryableError) as exc_info:
        await _provider(handler).create(
            preferred_name="vc123",
            audio_data_url=AUDIO_DATA_URL,
        )

    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    chain_text = _exception_chain_text(exc_info.value)
    assert API_KEY not in chain_text
    assert AUDIO_DATA_URL not in chain_text
