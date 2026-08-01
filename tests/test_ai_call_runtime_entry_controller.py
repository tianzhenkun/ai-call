from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from app.services.ai_call.runtime_control.command_repository import CommandSnapshot


class _FakeRepository:
    def __init__(self, _db) -> None:
        self.calls: list[object] = []

    async def create_start_call(self, request):
        self.calls.append(request)
        return CommandSnapshot(
            command_id=101,
            tenant_id=request.tenant_id,
            call_id="call_101",
            command_seq=1,
            command_type="START_CALL",
            idempotency_key=request.idempotency_key,
            request_fingerprint="fingerprint",
            status="PENDING",
            created_at=SimpleNamespace(),
        )


def _auth() -> SimpleNamespace:
    return SimpleNamespace(
        db=object(),
        user=SimpleNamespace(tenant_id="tenant-from-auth"),
    )


@pytest.mark.anyio
async def test_runtime_start_controller_returns_accepted_persistent_identifiers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.api.v1.ai_call import runtime_control_controller as controller

    repository = _FakeRepository(object())
    monkeypatch.setattr(controller, "RuntimeCommandRepository", lambda _db: repository)
    monkeypatch.setattr(
        controller,
        "settings",
        SimpleNamespace(AI_CALL_OWNER_COMMAND_V1_ENTRIES="web"),
    )

    response = await controller.create_runtime_start_call_controller(
        auth=_auth(),
        request=controller.RuntimeStartCallRequest(
            entry_type="web",
            idempotency_key="start:web:controller",
            payload={"business_id": "biz-1"},
            business_id="biz-1",
        ),
    )

    body = json.loads(response.body)
    assert body["data"] == {
        "commandId": "101",
        "callId": "call_101",
        "commandSeq": "1",
        "commandType": "START_CALL",
        "status": "PENDING",
    }
    assert repository.calls[0].tenant_id == "tenant-from-auth"


@pytest.mark.anyio
async def test_runtime_start_controller_rejects_disabled_entry_without_persisting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.api.v1.ai_call import runtime_control_controller as controller

    repository = _FakeRepository(object())
    monkeypatch.setattr(controller, "RuntimeCommandRepository", lambda _db: repository)
    monkeypatch.setattr(
        controller,
        "settings",
        SimpleNamespace(AI_CALL_OWNER_COMMAND_V1_ENTRIES="preview"),
    )

    with pytest.raises(controller.HTTPException) as exc_info:
        await controller.create_runtime_start_call_controller(
            auth=_auth(),
            request=controller.RuntimeStartCallRequest(
                entry_type="web",
                idempotency_key="start:web:disabled",
                payload={},
            ),
        )

    assert exc_info.value.status_code == 409
    assert repository.calls == []
