from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from app.services.ai_call.runtime_control.bootstrap_service import (
    RuntimeBootstrapLegacyError,
    RuntimeBootstrapNotFoundError,
    RuntimeBootstrapSnapshot,
)
from app.services.ai_call.runtime_control.command_repository import CommandSnapshot
from app.services.ai_call.runtime_control.runtime_token_service import (
    RuntimeIssuedToken,
    RuntimeTokenGateError,
)


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
    assert response.status_code == 202
    assert body["data"] == {
        "acceptanceStatus": "ACCEPTED",
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


class _FakeBootstrapService:
    def __init__(self, _db) -> None:
        self.call_ids: list[str] = []

    async def get(self, *, tenant_id: str, call_id: str):
        self.call_ids.append(f"{tenant_id}:{call_id}")
        return RuntimeBootstrapSnapshot(
            call_id=call_id,
            entry_type="web",
            phase="ready",
            room_name="ai-call-call-1",
            participant_identity="caller-call-1",
            runtime_fencing_token=7,
            agent_media_ready_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
            terminal_requested_at=None,
            token_available=False,
        )


@pytest.mark.anyio
async def test_runtime_bootstrap_controller_returns_readiness_without_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.api.v1.ai_call import runtime_control_controller as controller

    service = _FakeBootstrapService(object())
    monkeypatch.setattr(controller, "RuntimeBootstrapService", lambda db: service)

    response = await controller.get_runtime_bootstrap_controller(
        auth=_auth(),
        call_id="call-1",
    )

    body = json.loads(response.body)
    assert body["data"] == {
        "callId": "call-1",
        "entryType": "web",
        "phase": "ready",
        "roomName": "ai-call-call-1",
        "participantIdentity": "caller-call-1",
        "runtimeFencingToken": 7,
        "agentMediaReadyAt": "2026-08-01T00:00:00Z",
        "terminalRequestedAt": None,
        "tokenAvailable": False,
    }
    assert service.call_ids == ["tenant-from-auth:call-1"]


@pytest.mark.anyio
async def test_runtime_bootstrap_controller_does_not_fallback_to_legacy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.api.v1.ai_call import runtime_control_controller as controller

    class _LegacyService:
        def __init__(self, _db) -> None:
            pass

        async def get(self, *, tenant_id: str, call_id: str):
            raise RuntimeBootstrapLegacyError("legacy")

    monkeypatch.setattr(controller, "RuntimeBootstrapService", _LegacyService)

    with pytest.raises(controller.HTTPException) as exc_info:
        await controller.get_runtime_bootstrap_controller(
            auth=_auth(),
            call_id="call-1",
        )

    assert exc_info.value.status_code == 409


@pytest.mark.anyio
async def test_runtime_bootstrap_controller_returns_not_found_without_cross_tenant_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.api.v1.ai_call import runtime_control_controller as controller

    class _MissingService:
        def __init__(self, _db) -> None:
            pass

        async def get(self, *, tenant_id: str, call_id: str):
            raise RuntimeBootstrapNotFoundError("missing")

    monkeypatch.setattr(controller, "RuntimeBootstrapService", _MissingService)

    with pytest.raises(controller.HTTPException) as exc_info:
        await controller.get_runtime_bootstrap_controller(
            auth=_auth(),
            call_id="call-1",
        )

    assert exc_info.value.status_code == 404


@pytest.mark.anyio
async def test_runtime_token_controller_returns_locally_signed_owner_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.api.v1.ai_call import runtime_control_controller as controller

    class _TokenService:
        def __init__(self, **_kwargs) -> None:
            pass

        async def issue_browser_token(self, *, tenant_id: str, call_id: str):
            assert tenant_id == "tenant-from-auth"
            assert call_id == "call-1"
            return RuntimeIssuedToken(
                call_id=call_id,
                room_name="ai-call-call-1",
                livekit_url="wss://livekit.test",
                participant_token="signed-token",
                participant_identity="caller-call-1",
                expires_in_seconds=60,
            )

    monkeypatch.setattr(controller, "RuntimeTokenService", _TokenService)
    monkeypatch.setattr(
        controller,
        "settings",
        SimpleNamespace(
            LIVEKIT_URL="wss://livekit.test",
            LIVEKIT_API_KEY="livekit-key",
            LIVEKIT_API_SECRET="livekit-secret-that-is-long-enough",
            LIVEKIT_BROWSER_TOKEN_TTL_SECONDS=60,
        ),
    )

    response = await controller.create_runtime_token_controller(
        auth=_auth(),
        call_id="call-1",
    )

    assert json.loads(response.body)["data"] == {
        "callId": "call-1",
        "roomName": "ai-call-call-1",
        "livekitUrl": "wss://livekit.test",
        "participantToken": "signed-token",
        "participantIdentity": "caller-call-1",
        "expiresInSeconds": 60,
    }


@pytest.mark.anyio
async def test_runtime_token_controller_returns_stable_gate_error_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.api.v1.ai_call import runtime_control_controller as controller

    class _TokenService:
        def __init__(self, **_kwargs) -> None:
            pass

        async def issue_browser_token(self, *, tenant_id: str, call_id: str):
            raise RuntimeTokenGateError("OWNER_UNAVAILABLE", "owner unavailable")

    monkeypatch.setattr(controller, "RuntimeTokenService", _TokenService)
    monkeypatch.setattr(
        controller,
        "settings",
        SimpleNamespace(
            LIVEKIT_URL="wss://livekit.test",
            LIVEKIT_API_KEY="livekit-key",
            LIVEKIT_API_SECRET="livekit-secret-that-is-long-enough",
            LIVEKIT_BROWSER_TOKEN_TTL_SECONDS=60,
        ),
    )

    with pytest.raises(controller.CustomException) as exc_info:
        await controller.create_runtime_token_controller(
            auth=_auth(),
            call_id="call-1",
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.data == {"errorCode": "OWNER_UNAVAILABLE"}


@pytest.mark.anyio
async def test_runtime_end_controller_persists_web_evidence_and_returns_real_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.api.v1.ai_call import runtime_control_controller as controller

    class _EndRepository:
        def __init__(self, _db) -> None:
            self.requests = []

        async def request_end(self, request):
            self.requests.append(request)
            return SimpleNamespace(
                command_id=202,
                call_id=request.call_id,
                command_seq=2,
                command_status="PENDING",
            )

    repository = _EndRepository(object())
    monkeypatch.setattr(controller, "RuntimeCommandRepository", lambda _db: repository)

    response = await controller.create_runtime_end_call_controller(
        auth=_auth(),
        call_id="call-1",
        request=controller.RuntimeEndCallRequest(
            dedupe_key="call-1:web_client:click-1",
            end_reason="user_requested",
        ),
    )

    assert response.status_code == 202
    assert json.loads(response.body)["data"] == {
        "acceptanceStatus": "ACCEPTED",
        "callId": "call-1",
        "commandId": "202",
        "commandSeq": "2",
        "commandStatus": "PENDING",
    }
    persisted = repository.requests[0]
    assert persisted.tenant_id == "tenant-from-auth"
    assert persisted.call_id == "call-1"
    assert persisted.source == "web_client"
    assert persisted.end_reason == "user_requested"
    assert persisted.dedupe_key == "call-1:web_client:click-1"
