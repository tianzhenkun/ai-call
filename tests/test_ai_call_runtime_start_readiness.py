from types import SimpleNamespace

from app.services.ai_call.runtime_control.effect_repository import EffectSpec
from app.services.ai_call.runtime_control.start_readiness_repository import (
    build_stub_start_readiness,
)


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
