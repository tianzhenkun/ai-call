from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import jwt
import pytest

from app.services.ai_call.livekit_room import LiveKitRoomManager
from app.services.ai_call.runtime_control.runtime_token_service import (
    RuntimeTokenGateError,
    RuntimeTokenService,
    evaluate_runtime_token_gate,
)

UTC = timezone.utc
NOW = datetime(2026, 8, 1, 12, tzinfo=UTC)
LIVEKIT_SECRET = "livekit-secret-that-is-at-least-32-bytes-long"


def _record(**overrides):
    values = {
        "call_id": "call-1",
        "entry_type": "web",
        "runtime_control_mode": "owner_command_v1",
        "runtime_owner_id": "runtime-1",
        "runtime_fencing_token": 7,
        "runtime_lease_expires_at": NOW + timedelta(minutes=1),
        "room_name": "ai-call-call-1",
        "participant_identity": "caller-call-1",
        "agent_participant_identity": "agent-call-1-g7",
        "agent_resource_generation": 7,
        "agent_media_ready_at": NOW,
        "terminal_requested_at": None,
        "status": "ready",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_token_gate_uses_customer_identity_after_all_current_generation_gates() -> None:
    snapshot = evaluate_runtime_token_gate(
        _record(),
        owner_available=True,
        room_applied=True,
        agent_applied=True,
    )

    assert snapshot.call_id == "call-1"
    assert snapshot.participant_identity == "caller-call-1"
    assert snapshot.runtime_fencing_token == 7


@pytest.mark.parametrize(
    "record_overrides,owner_available,room_applied,agent_applied,error_code",
    [
        ({"terminal_requested_at": NOW}, False, False, False, "CALL_ENDING"),
        ({"runtime_control_mode": "legacy_local"}, True, True, True, "CALL_NOT_READY"),
        ({"entry_type": "preview"}, True, True, True, "CALL_NOT_READY"),
        ({}, False, True, True, "OWNER_UNAVAILABLE"),
        ({"status": "preparing"}, True, True, True, "CALL_NOT_READY"),
        ({"agent_media_ready_at": None}, True, True, True, "CALL_NOT_READY"),
        ({"agent_resource_generation": 6}, True, True, True, "CALL_NOT_READY"),
        ({}, True, False, True, "CALL_NOT_READY"),
        ({}, True, True, False, "CALL_NOT_READY"),
    ],
)
def test_token_gate_rejects_each_failed_contract_with_stable_error_code(
    record_overrides,
    owner_available,
    room_applied,
    agent_applied,
    error_code,
) -> None:
    with pytest.raises(RuntimeTokenGateError) as exc_info:
        evaluate_runtime_token_gate(
            _record(**record_overrides),
            owner_available=owner_available,
            room_applied=room_applied,
            agent_applied=agent_applied,
        )

    assert exc_info.value.error_code == error_code


class _FakeGateRepository:
    async def authorize(self, *, tenant_id: str, call_id: str):
        assert tenant_id == "tenant-a"
        assert call_id == "call-1"
        return evaluate_runtime_token_gate(
            _record(),
            owner_available=True,
            room_applied=True,
            agent_applied=True,
        )


class _FakeRoomManager:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, str]]] = []

    def issue_browser_token(self, room_name, participant_identity, *, metadata):
        self.calls.append((room_name, participant_identity, metadata))
        return SimpleNamespace(
            livekit_url="wss://livekit.test",
            participant_token="signed-token",
            participant_identity=participant_identity,
            expires_in_seconds=60,
        )


@pytest.mark.anyio
async def test_runtime_token_service_signs_only_after_gate_with_generation_metadata() -> None:
    manager = _FakeRoomManager()
    result = await RuntimeTokenService(
        repository=_FakeGateRepository(),
        room_manager=manager,
    ).issue_browser_token(tenant_id="tenant-a", call_id="call-1")

    assert result.call_id == "call-1"
    assert result.room_name == "ai-call-call-1"
    assert result.participant_token == "signed-token"
    assert manager.calls == [
        (
            "ai-call-call-1",
            "caller-call-1",
            {
                "call_id": "call-1",
                "resource_generation": "7",
                "participant_identity": "caller-call-1",
            },
        )
    ]


def test_livekit_browser_token_contains_controlled_runtime_metadata() -> None:
    manager = LiveKitRoomManager(
        livekit_url="wss://livekit.test",
        api_key="livekit-key",
        api_secret=LIVEKIT_SECRET,
        browser_token_ttl_seconds=60,
    )

    token = manager.issue_browser_token(
        "ai-call-call-1",
        "caller-call-1",
        metadata={
            "call_id": "call-1",
            "resource_generation": "7",
            "participant_identity": "caller-call-1",
        },
    )

    claims = jwt.decode(
        token.participant_token,
        LIVEKIT_SECRET,
        algorithms=["HS256"],
        issuer="livekit-key",
    )
    assert claims["sub"] == "caller-call-1"
    assert claims["video"]["room"] == "ai-call-call-1"
    assert claims["metadata"] == (
        '{"call_id":"call-1","participant_identity":"caller-call-1",'
        '"resource_generation":"7"}'
    )
