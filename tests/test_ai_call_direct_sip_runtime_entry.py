from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from app.api.v1.ai_call import controller
from app.api.v1.ai_call.schema import CreateSipSessionRequest
from app.core.exceptions import CustomException
from app.services.ai_call.runtime_control.command_repository import (
    CommandSnapshot,
    IdempotencyConflictError,
    StartCallIntent,
    start_call_request_fingerprint,
)


class _FakeRuntimeRepository:
    def __init__(self) -> None:
        self.requests: list[StartCallIntent] = []
        self._accepted: dict[str, tuple[str, CommandSnapshot]] = {}

    async def create_start_call(self, request: StartCallIntent) -> CommandSnapshot:
        self.requests.append(request)
        fingerprint = start_call_request_fingerprint(request)
        accepted = self._accepted.get(request.idempotency_key)
        if accepted is not None:
            previous_fingerprint, snapshot = accepted
            if previous_fingerprint != fingerprint:
                raise IdempotencyConflictError("different request fingerprint")
            return snapshot

        sequence = len(self._accepted) + 1
        snapshot = CommandSnapshot(
            command_id=100 + sequence,
            tenant_id=request.tenant_id,
            call_id=f"call_{sequence}",
            command_seq=1,
            command_type="START_CALL",
            idempotency_key=request.idempotency_key,
            request_fingerprint=fingerprint,
            status="PENDING",
            created_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
        )
        self._accepted[request.idempotency_key] = (fingerprint, snapshot)
        return snapshot


def _settings(entries: str) -> SimpleNamespace:
    return SimpleNamespace(
        AI_CALL_OWNER_COMMAND_V1_ENTRIES=entries,
        AI_CALL_DIRECT_SIP_ALLOCATION_TIMEOUT_SECONDS=30.0,
    )


def _auth(tenant_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        db=object(),
        user=SimpleNamespace(tenant_id=tenant_id),
    )


def _request(phone: str = "13812345678") -> CreateSipSessionRequest:
    return CreateSipSessionRequest(
        callee_phone_number=phone,
        voice="voice-a",
        business_id="business-a",
        scene_code="collection",
        business_params={"customerName": "张三"},
        ringing_timeout_seconds=30,
    )


def _legacy_result() -> dict[str, object]:
    return {
        "call_id": "call_legacy",
        "room_name": "ai-call-call_legacy",
        "participant_identity": "sip-call_legacy",
        "status": "ready",
        "effective_config": {
            "model": "stub-model",
            "voice": "voice-a",
            "prompt_hash": "prompt-hash",
            "opening_message_hash": "opening-hash",
            "prompt_source_key": "scene:collection",
            "barge_in_enabled": False,
            "vad_type": "server_vad",
            "vad_threshold": 0.5,
            "vad_silence_duration_ms": 800,
        },
        "sip_call_id": "sip-legacy",
        "sip_trunk_id": "trunk-legacy",
        "sip_call_status": "active",
    }


@pytest.mark.anyio
async def test_direct_sip_owner_mode_returns_masked_202_without_legacy_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _FakeRuntimeRepository()
    legacy_service = SimpleNamespace(create_sip_session=AsyncMock())
    monkeypatch.setattr(controller, "settings", _settings("direct_sip"))
    monkeypatch.setattr(
        controller,
        "RuntimeCommandRepository",
        lambda _db: repository,
    )

    response = await controller.create_sip_session_controller(
        service=legacy_service,
        request=_request(),
        auth=_auth("tenant-a"),
        idempotency_key="sip:req:1",
    )

    body = json.loads(response.body)
    serialized = response.body.decode("utf-8")
    assert response.status_code == 202
    assert body["data"]["calleePhoneNumberMasked"] == "138****5678"
    assert "138****5678" in serialized
    assert "13812345678" not in serialized
    assert "callee_phone_number" not in serialized
    assert '"calleePhoneNumber":' not in serialized
    assert repository.requests[-1].callee_phone_number == "13812345678"
    assert repository.requests[-1].payload == {
        "voice": "voice-a",
        "business_id": "business-a",
        "scene_code": "collection",
        "business_params": {"customerName": "张三"},
        "ringing_timeout_seconds": 30,
    }
    legacy_service.create_sip_session.assert_not_awaited()


@pytest.mark.anyio
async def test_direct_sip_owner_mode_requires_tenant_and_idempotency_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _FakeRuntimeRepository()
    legacy_service = SimpleNamespace(create_sip_session=AsyncMock())
    monkeypatch.setattr(controller, "settings", _settings("direct_sip"))
    monkeypatch.setattr(
        controller,
        "RuntimeCommandRepository",
        lambda _db: repository,
    )

    with pytest.raises(HTTPException) as tenant_error:
        await controller.create_sip_session_controller(
            service=legacy_service,
            request=_request(),
            auth=_auth(""),
            idempotency_key="sip:req:tenant",
        )
    assert tenant_error.value.status_code == 401

    with pytest.raises(CustomException) as idempotency_error:
        await controller.create_sip_session_controller(
            service=legacy_service,
            request=_request(),
            auth=_auth("tenant-a"),
            idempotency_key=None,
        )
    assert idempotency_error.value.status_code == 400
    assert idempotency_error.value.data == {
        "errorCode": "IDEMPOTENCY_KEY_REQUIRED"
    }
    assert repository.requests == []
    legacy_service.create_sip_session.assert_not_awaited()


@pytest.mark.anyio
async def test_direct_sip_owner_mode_reuses_same_request_and_rejects_new_phone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _FakeRuntimeRepository()
    legacy_service = SimpleNamespace(create_sip_session=AsyncMock())
    monkeypatch.setattr(controller, "settings", _settings("direct_sip"))
    monkeypatch.setattr(
        controller,
        "RuntimeCommandRepository",
        lambda _db: repository,
    )

    first = await controller.create_sip_session_controller(
        service=legacy_service,
        request=_request(),
        auth=_auth("tenant-a"),
        idempotency_key="sip:req:idempotent",
    )
    repeated = await controller.create_sip_session_controller(
        service=legacy_service,
        request=_request(),
        auth=_auth("tenant-a"),
        idempotency_key="sip:req:idempotent",
    )
    assert json.loads(first.body)["data"]["commandId"] == json.loads(repeated.body)[
        "data"
    ]["commandId"]

    with pytest.raises(CustomException) as conflict:
        await controller.create_sip_session_controller(
            service=legacy_service,
            request=_request("13912345678"),
            auth=_auth("tenant-a"),
            idempotency_key="sip:req:idempotent",
        )
    assert conflict.value.status_code == 409
    assert conflict.value.data == {"errorCode": "IDEMPOTENCY_CONFLICT"}
    legacy_service.create_sip_session.assert_not_awaited()


@pytest.mark.anyio
async def test_direct_sip_legacy_mode_calls_only_existing_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _FakeRuntimeRepository()
    legacy_service = SimpleNamespace(
        create_sip_session=AsyncMock(return_value=_legacy_result())
    )
    monkeypatch.setattr(controller, "settings", _settings(""))
    monkeypatch.setattr(
        controller,
        "RuntimeCommandRepository",
        lambda _db: repository,
    )

    response = await controller.create_sip_session_controller(
        service=legacy_service,
        request=_request(),
        auth=None,
        idempotency_key=None,
    )

    assert response.status_code == 200
    legacy_service.create_sip_session.assert_awaited_once()
    assert repository.requests == []
