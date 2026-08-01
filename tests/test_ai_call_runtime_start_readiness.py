from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.services.ai_call.runtime_control.effect_repository import EffectSpec
from app.services.ai_call.runtime_control.owner_repository import OwnerLease
from app.services.ai_call.runtime_control.start_readiness_repository import (
    RuntimeStartReadinessRepository,
    StubStartReadiness,
    build_stub_start_readiness,
)

NOW = datetime(2026, 8, 1, 12, tzinfo=timezone.utc)


def _spec(effect_type: str, key: str) -> EffectSpec:
    return EffectSpec(
        effect_type=effect_type,
        idempotency_key=key,
        provider_namespace="stub:test",
        provider_idempotency_key=key,
        resource_key=f"{effect_type.lower()}:call-a:g7",
        resource_generation=7,
    )


def _effect(
    effect_type: str,
    key: str,
    *,
    generation: int = 7,
    status: str = "APPLIED",
    provider_reference: str | None = "stub-ref",
):
    return SimpleNamespace(
        effect_type=effect_type,
        idempotency_key=key,
        resource_generation=generation,
        status=status,
        provider_reference=provider_reference,
    )


def test_stub_start_readiness_is_reconstructed_from_persisted_effects() -> None:
    readiness = build_stub_start_readiness(
        call_id="call-a",
        fencing_token=7,
        specs=[
            _spec("CREATE_ROOM", "room-key"),
            _spec("ATTACH_AGENT_PARTICIPANT", "agent-key"),
        ],
        effects=[
            _effect("CREATE_ROOM", "room-key", provider_reference="room-sid"),
            _effect(
                "ATTACH_AGENT_PARTICIPANT",
                "agent-key",
                provider_reference="agent-sid",
            ),
        ],
    )

    assert readiness is not None
    assert readiness.applied_effect_count == 2
    assert readiness.agent_participant_identity == "agent-call-a-g7"
    assert readiness.agent_participant_sid == "agent-sid"
    assert readiness.agent_audio_track_sid == "stub-track-call-a-g7"


def test_stub_start_readiness_rejects_missing_or_stale_effect_evidence() -> None:
    specs = [
        _spec("CREATE_ROOM", "room-key"),
        _spec("ATTACH_AGENT_PARTICIPANT", "agent-key"),
    ]

    assert (
        build_stub_start_readiness(
            call_id="call-a",
            fencing_token=7,
            specs=specs,
            effects=[
                _effect("CREATE_ROOM", "room-key"),
                _effect(
                    "ATTACH_AGENT_PARTICIPANT",
                    "agent-key",
                    generation=6,
                ),
            ],
        )
        is None
    )
    assert (
        build_stub_start_readiness(
            call_id="call-a",
            fencing_token=7,
            specs=specs,
            effects=[
                _effect("CREATE_ROOM", "room-key"),
                _effect(
                    "ATTACH_AGENT_PARTICIPANT",
                    "agent-key",
                    provider_reference=None,
                ),
            ],
        )
        is None
    )


def test_direct_sip_readiness_requires_all_three_create_effects_applied() -> None:
    specs = [
        _spec("CREATE_ROOM", "room-key"),
        _spec("ATTACH_AGENT_PARTICIPANT", "agent-key"),
        _spec("CREATE_SIP_PARTICIPANT", "sip-key"),
    ]
    room_and_agent = [
        _effect("CREATE_ROOM", "room-key"),
        _effect("ATTACH_AGENT_PARTICIPANT", "agent-key"),
    ]

    assert (
        build_stub_start_readiness(
            call_id="call-a",
            fencing_token=7,
            specs=specs,
            effects=room_and_agent,
        )
        is None
    )
    assert (
        build_stub_start_readiness(
            call_id="call-a",
            fencing_token=7,
            specs=specs,
            effects=[
                *room_and_agent,
                _effect("CREATE_SIP_PARTICIPANT", "sip-key", status="PENDING"),
            ],
        )
        is None
    )

    readiness = build_stub_start_readiness(
        call_id="call-a",
        fencing_token=7,
        specs=specs,
        effects=[
            *room_and_agent,
            _effect("CREATE_SIP_PARTICIPANT", "sip-key"),
        ],
    )
    assert readiness is not None
    assert readiness.applied_effect_count == 3


@pytest.mark.anyio
async def test_stub_readiness_persistence_rejects_preview_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.services.ai_call.runtime_control import start_readiness_repository as module

    record = SimpleNamespace(
        entry_type="preview",
        runtime_control_mode="owner_command_v1",
        runtime_owner_id="runtime-a",
        runtime_fencing_token=7,
        runtime_lease_expires_at=NOW + timedelta(minutes=1),
        terminal_requested_at=None,
        status="preparing",
        agent_resource_generation=None,
        agent_media_ready_at=None,
    )
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=record),
        flush=AsyncMock(),
    )
    monkeypatch.setattr(module, "read_database_time", AsyncMock(return_value=NOW))

    persisted = await RuntimeStartReadinessRepository(session).persist_stub_ready(
        SimpleNamespace(
            command_type="START_CALL",
            tenant_id="tenant-a",
            call_id="call-a",
            processing_owner_id="runtime-a",
            processing_fencing_token=7,
        ),
        OwnerLease(
            tenant_id="tenant-a",
            call_id="call-a",
            owner_id="runtime-a",
            fencing_token=7,
            lease_expires_at=NOW + timedelta(minutes=1),
            capacity_class="active",
        ),
        StubStartReadiness(
            applied_effect_count=2,
            agent_participant_identity="agent-call-a-g7",
            agent_participant_sid="agent-sid",
            agent_audio_track_sid="track-sid",
        ),
    )

    assert persisted is False
    assert record.status == "preparing"
    session.flush.assert_not_awaited()


@pytest.mark.anyio
async def test_stub_readiness_persistence_accepts_direct_sip_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.services.ai_call.runtime_control import start_readiness_repository as module

    record = SimpleNamespace(
        entry_type="direct_sip",
        runtime_control_mode="owner_command_v1",
        runtime_owner_id="runtime-a",
        runtime_fencing_token=7,
        runtime_lease_expires_at=NOW + timedelta(minutes=1),
        terminal_requested_at=None,
        status="preparing",
        agent_resource_generation=None,
        agent_media_ready_at=None,
    )
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=record),
        flush=AsyncMock(),
    )
    monkeypatch.setattr(module, "read_database_time", AsyncMock(return_value=NOW))

    persisted = await RuntimeStartReadinessRepository(session).persist_stub_ready(
        SimpleNamespace(
            command_type="START_CALL",
            tenant_id="tenant-a",
            call_id="call-a",
            processing_owner_id="runtime-a",
            processing_fencing_token=7,
        ),
        OwnerLease(
            tenant_id="tenant-a",
            call_id="call-a",
            owner_id="runtime-a",
            fencing_token=7,
            lease_expires_at=NOW + timedelta(minutes=1),
            capacity_class="active",
        ),
        StubStartReadiness(
            applied_effect_count=3,
            agent_participant_identity="agent-call-a-g7",
            agent_participant_sid="agent-sid",
            agent_audio_track_sid="track-sid",
        ),
    )

    assert persisted is True
    assert record.status == "ready"
    session.flush.assert_awaited_once()
