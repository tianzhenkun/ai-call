from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.api.v1.ai_call.schema import CreateSessionOut, CreateWebSessionRequest
from app.services.ai_call.runtime_control.command_repository import (
    CommandSnapshot,
    IdempotencyConflictError,
    start_call_request_fingerprint,
)

NOW = datetime(2026, 8, 1, 12, tzinfo=timezone.utc)


class _FakeRuntimeRepository:
    def __init__(self, _db) -> None:
        self.requests: list[object] = []
        self._commands: dict[str, tuple[str, CommandSnapshot]] = {}

    async def create_start_call(self, request):
        self.requests.append(request)
        fingerprint = start_call_request_fingerprint(request)
        existing = self._commands.get(request.idempotency_key)
        if existing is not None:
            existing_fingerprint, snapshot = existing
            if existing_fingerprint != fingerprint:
                raise IdempotencyConflictError("different request fingerprint")
            return snapshot
        command_number = len(self._commands) + 1
        snapshot = CommandSnapshot(
            command_id=100 + command_number,
            tenant_id=request.tenant_id,
            call_id=f"call_{command_number}",
            command_seq=1,
            command_type="START_CALL",
            idempotency_key=request.idempotency_key,
            request_fingerprint=fingerprint,
            status="PENDING",
            created_at=NOW,
        )
        self._commands[request.idempotency_key] = (fingerprint, snapshot)
        return snapshot


def _auth(tenant_id: str | None = "tenant-a") -> SimpleNamespace:
    return SimpleNamespace(
        db=object(),
        user=SimpleNamespace(tenant_id=tenant_id),
    )


def _request(**overrides) -> CreateWebSessionRequest:
    values = {
        "voice": "v1",
        "business_id": "biz-1",
        "scene_code": "collection",
        "business_params": {"customerName": "张三"},
    }
    values.update(overrides)
    return CreateWebSessionRequest(**values)


def _legacy_result() -> CreateSessionOut:
    return CreateSessionOut(
        call_id="legacy-call-1",
        room_name="legacy-room-1",
        livekit_url="wss://legacy.test",
        participant_token="legacy-token",
        participant_identity="legacy-caller-1",
        status="ready",
        effective_config={
            "model": "legacy-model",
            "voice": "v1",
            "promptHash": "prompt-hash",
            "openingMessageHash": "opening-hash",
            "promptSourceKey": "collection",
            "vadType": "server_vad",
            "vadThreshold": 0.5,
            "vadSilenceDurationMs": 500,
        },
        web_audio_constraints={
            "echoCancellation": True,
            "noiseSuppression": True,
            "autoGainControl": True,
        },
    )


def _install_owner_mode(monkeypatch: pytest.MonkeyPatch, controller):
    repository = _FakeRuntimeRepository(object())
    monkeypatch.setattr(
        controller,
        "settings",
        SimpleNamespace(
            AI_CALL_OWNER_COMMAND_V1_ENTRIES="web",
            AI_CALL_WEB_ALLOCATION_TIMEOUT_SECONDS=30.0,
        ),
    )
    monkeypatch.setattr(
        controller,
        "RuntimeCommandRepository",
        lambda _db: repository,
        raising=False,
    )
    return repository


@pytest.mark.anyio
async def test_web_owner_mode_returns_202_without_calling_legacy_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.api.v1.ai_call import controller

    repository = _install_owner_mode(monkeypatch, controller)
    legacy_service = SimpleNamespace(create_web_session=AsyncMock())

    response = await controller.create_session_controller(
        service=legacy_service,
        request=_request(),
        auth=_auth(),
        idempotency_key="web:biz-1:req-1",
    )

    body = json.loads(response.body)
    assert response.status_code == 202
    assert body["data"] == {
        "acceptanceStatus": "ACCEPTED",
        "commandId": "101",
        "callId": "call_1",
        "commandSeq": "1",
        "commandType": "START_CALL",
        "status": "PENDING",
    }
    legacy_service.create_web_session.assert_not_awaited()
    assert len(repository.requests) == 1
    assert repository.requests[0].tenant_id == "tenant-a"
    assert repository.requests[0].payload == {
        "voice": "v1",
        "business_id": "biz-1",
        "scene_code": "collection",
        "business_params": {"customerName": "张三"},
    }
    assert repository.requests[0].allocation_timeout_seconds == 30.0


@pytest.mark.anyio
async def test_web_legacy_mode_preserves_synchronous_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.api.v1.ai_call import controller

    monkeypatch.setattr(
        controller,
        "settings",
        SimpleNamespace(AI_CALL_OWNER_COMMAND_V1_ENTRIES=""),
    )
    legacy_result = _legacy_result()
    legacy_service = SimpleNamespace(
        create_web_session=AsyncMock(return_value=legacy_result)
    )

    response = await controller.create_session_controller(
        service=legacy_service,
        request=_request(),
        auth=_auth(),
        idempotency_key=None,
    )

    assert response.status_code == 200
    assert json.loads(response.body)["data"]["callId"] == "legacy-call-1"
    legacy_service.create_web_session.assert_awaited_once_with(
        voice="v1",
        prompt=None,
        business_id="biz-1",
        scene_code="collection",
        business_params={"customerName": "张三"},
    )


@pytest.mark.anyio
async def test_web_owner_mode_requires_authenticated_tenant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.api.v1.ai_call import controller

    repository = _install_owner_mode(monkeypatch, controller)

    with pytest.raises(controller.HTTPException) as exc_info:
        await controller.create_session_controller(
            service=SimpleNamespace(create_web_session=AsyncMock()),
            request=_request(),
            auth=_auth(None),
            idempotency_key="web:biz-1:req-1",
        )

    assert exc_info.value.status_code == 401
    assert repository.requests == []


@pytest.mark.anyio
@pytest.mark.parametrize("idempotency_key", [None, "", "   "])
async def test_web_owner_mode_requires_idempotency_key(
    monkeypatch: pytest.MonkeyPatch,
    idempotency_key: str | None,
) -> None:
    from app.api.v1.ai_call import controller

    repository = _install_owner_mode(monkeypatch, controller)

    with pytest.raises(controller.CustomException) as exc_info:
        await controller.create_session_controller(
            service=SimpleNamespace(create_web_session=AsyncMock()),
            request=_request(),
            auth=_auth(),
            idempotency_key=idempotency_key,
        )

    assert exc_info.value.status_code == 422
    assert exc_info.value.data == {"errorCode": "IDEMPOTENCY_KEY_REQUIRED"}
    assert repository.requests == []


@pytest.mark.anyio
async def test_web_owner_mode_reuses_same_idempotent_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.api.v1.ai_call import controller

    repository = _install_owner_mode(monkeypatch, controller)
    legacy_service = SimpleNamespace(create_web_session=AsyncMock())

    first = await controller.create_session_controller(
        service=legacy_service,
        request=_request(),
        auth=_auth(),
        idempotency_key="web:biz-1:same",
    )
    repeated = await controller.create_session_controller(
        service=legacy_service,
        request=_request(),
        auth=_auth(),
        idempotency_key="web:biz-1:same",
    )

    assert json.loads(first.body)["data"] == json.loads(repeated.body)["data"]
    assert len(repository._commands) == 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    "changed_request",
    [
        _request(voice="v2"),
        _request(business_id="biz-2"),
        _request(scene_code="marketing"),
        _request(business_params={"customerName": "李四"}),
    ],
    ids=["voice", "business_id", "scene_code", "business_params"],
)
async def test_web_owner_mode_rejects_same_key_with_changed_business_request(
    monkeypatch: pytest.MonkeyPatch,
    changed_request: CreateWebSessionRequest,
) -> None:
    from app.api.v1.ai_call import controller

    _install_owner_mode(monkeypatch, controller)
    legacy_service = SimpleNamespace(create_web_session=AsyncMock())
    await controller.create_session_controller(
        service=legacy_service,
        request=_request(),
        auth=_auth(),
        idempotency_key="web:biz-1:conflict",
    )

    with pytest.raises(controller.CustomException) as exc_info:
        await controller.create_session_controller(
            service=legacy_service,
            request=changed_request,
            auth=_auth(),
            idempotency_key="web:biz-1:conflict",
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.data == {"errorCode": "IDEMPOTENCY_CONFLICT"}
