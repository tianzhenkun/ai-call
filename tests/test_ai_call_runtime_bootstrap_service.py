from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from app.services.ai_call.runtime_control.bootstrap_service import (
    RuntimeBootstrapLegacyError,
    build_runtime_bootstrap_snapshot,
)

UTC = timezone.utc


def _record(**overrides):
    values = {
        "call_id": "call-1",
        "entry_type": "web",
        "runtime_control_mode": "owner_command_v1",
        "runtime_owner_id": "runtime-1",
        "runtime_fencing_token": 7,
        "runtime_lease_expires_at": datetime(2026, 8, 1, 12, 1, tzinfo=UTC),
        "room_name": "ai-call-call-1",
        "participant_identity": "caller-call-1",
        "agent_participant_identity": "agent-call-1",
        "agent_resource_generation": 7,
        "agent_media_ready_at": datetime(2026, 8, 1, 12, 0, 30, tzinfo=UTC),
        "terminal_requested_at": None,
        "status": "preparing",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _effect(effect_type: str, status: str = "APPLIED", generation: int = 7):
    return SimpleNamespace(
        effect_type=effect_type,
        status=status,
        resource_generation=generation,
    )


def test_bootstrap_is_ready_only_after_room_agent_and_media_gates() -> None:
    snapshot = build_runtime_bootstrap_snapshot(
        _record(),
        [_effect("CREATE_ROOM"), _effect("ATTACH_AGENT_PARTICIPANT")],
        now=datetime(2026, 8, 1, 12, tzinfo=UTC),
    )

    assert snapshot.phase == "ready"
    assert snapshot.token_available is False
    assert snapshot.room_name == "ai-call-call-1"
    assert snapshot.participant_identity == "caller-call-1"


@pytest.mark.parametrize(
    "record_overrides,effects",
    [
        ({"runtime_lease_expires_at": datetime(2026, 8, 1, 11, 59, tzinfo=UTC)}, []),
        ({"agent_media_ready_at": None}, [_effect("CREATE_ROOM"), _effect("ATTACH_AGENT_PARTICIPANT")]),
        ({"agent_resource_generation": 6}, [_effect("CREATE_ROOM"), _effect("ATTACH_AGENT_PARTICIPANT")]),
        ({}, [_effect("CREATE_ROOM"), _effect("ATTACH_AGENT_PARTICIPANT", status="PENDING")]),
    ],
)
def test_bootstrap_stays_starting_when_any_readiness_gate_is_missing(
    record_overrides,
    effects,
) -> None:
    snapshot = build_runtime_bootstrap_snapshot(
        _record(**record_overrides),
        effects,
        now=datetime(2026, 8, 1, 12, tzinfo=UTC),
    )

    assert snapshot.phase == "starting"
    assert snapshot.token_available is False


def test_bootstrap_never_exposes_token_during_terminal_barrier() -> None:
    snapshot = build_runtime_bootstrap_snapshot(
        _record(
            terminal_requested_at=datetime(2026, 8, 1, 12, tzinfo=UTC),
            status="ending",
        ),
        [_effect("CREATE_ROOM"), _effect("ATTACH_AGENT_PARTICIPANT")],
        now=datetime(2026, 8, 1, 12, tzinfo=UTC),
    )

    assert snapshot.phase == "ending"
    assert snapshot.token_available is False


def test_bootstrap_rejects_legacy_record_instead_of_falling_back() -> None:
    with pytest.raises(RuntimeBootstrapLegacyError):
        build_runtime_bootstrap_snapshot(
            _record(runtime_control_mode="legacy_local"),
            [],
            now=datetime(2026, 8, 1, 12, tzinfo=UTC),
        )
