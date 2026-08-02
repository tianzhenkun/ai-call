from __future__ import annotations

import json
from types import SimpleNamespace

import pytest


@pytest.mark.anyio
async def test_owner_handoff_cancel_controller_returns_pending_command(
    monkeypatch,
) -> None:
    from app.api.v1.ai_call import controller

    class _Db:
        def __init__(self) -> None:
            self.commit_count = 0

        async def scalar(self, _statement):
            return "owner_command_v1"

        async def commit(self) -> None:
            self.commit_count += 1

    class _Repository:
        def __init__(self, _db) -> None:
            self.requests = []

        async def request_cancel(self, request):
            self.requests.append(request)
            return SimpleNamespace(
                handoff_id=request.handoff_id,
                call_id="call-1",
                handoff_status="accepted",
                command_id=202,
                command_seq=3,
                command_status="PENDING",
            )

    db = _Db()
    repository = _Repository(db)
    legacy_service = SimpleNamespace(cancel_handoff=_unexpected_async_call)
    monkeypatch.setattr(controller, "RuntimeHandoffRepository", lambda _db: repository)

    response = await controller.cancel_handoff_controller(
        handoff_id="handoff-1",
        service=legacy_service,
        auth=SimpleNamespace(
            db=db,
            user=SimpleNamespace(tenant_id="tenant-a"),
        ),
        request=controller.FinishHandoffRequest(reason="customer_cancelled"),
        idempotency_key="handoff:handoff-1:cancel:click-1",
    )

    assert response.status_code == 202
    assert json.loads(response.body)["data"] == {
        "acceptanceStatus": "ACCEPTED",
        "callId": "call-1",
        "commandId": "202",
        "commandSeq": "3",
        "commandStatus": "PENDING",
        "handoffId": "handoff-1",
        "handoffStatus": "accepted",
    }
    assert repository.requests[0].tenant_id == "tenant-a"
    assert repository.requests[0].reason == "customer_cancelled"
    assert db.commit_count == 1


@pytest.mark.anyio
async def test_handoff_cancel_controller_does_not_fall_back_across_tenants() -> None:
    from app.api.v1.ai_call import controller
    from app.core.exceptions import CustomException

    class _Db:
        async def scalar(self, _statement):
            return None

    with pytest.raises(CustomException) as exc_info:
        await controller.cancel_handoff_controller(
            handoff_id="handoff-other-tenant",
            service=SimpleNamespace(cancel_handoff=_unexpected_async_call),
            auth=SimpleNamespace(
                db=_Db(),
                user=SimpleNamespace(tenant_id="tenant-a"),
            ),
            request=None,
            idempotency_key="cancel:cross-tenant",
        )

    assert exc_info.value.status_code == 404


async def _unexpected_async_call(**_kwargs):
    raise AssertionError("legacy path must not run for owner mode")
