from __future__ import annotations

from app.services.ai_call.runtime_control.effect_repository import (
    AUXILIARY_START_EFFECT_TYPES,
    CREATE_EFFECT_TYPES,
    DESTROY_EFFECT_TYPES,
    ProviderObservation,
    ProviderObservationKind,
    RuntimeEffectRepository,
)


def test_effect_type_sets_are_explicit_and_disjoint() -> None:
    assert CREATE_EFFECT_TYPES == {
        "CREATE_ROOM",
        "CREATE_SIP_PARTICIPANT",
        "ATTACH_AGENT_PARTICIPANT",
        "START_EGRESS",
        "START_TRACK_EGRESS",
    }
    assert DESTROY_EFFECT_TYPES == {
        "HANGUP_SIP",
        "DISCONNECT_AGENT_PARTICIPANT",
        "STOP_EGRESS",
        "STOP_TRACK_EGRESS",
        "DELETE_ROOM",
    }
    assert AUXILIARY_START_EFFECT_TYPES == {
        "START_EGRESS",
        "START_TRACK_EGRESS",
    }
    assert CREATE_EFFECT_TYPES.isdisjoint(DESTROY_EFFECT_TYPES)


def test_provider_observations_use_closed_enum() -> None:
    observation = ProviderObservation(kind=ProviderObservationKind.RESOURCE_ABSENT)

    assert observation.kind == "RESOURCE_ABSENT"
    assert callable(RuntimeEffectRepository.register)
    assert callable(RuntimeEffectRepository.register_end_graph)
    assert callable(RuntimeEffectRepository.claim_next)
    assert callable(RuntimeEffectRepository.submit)
    assert callable(RuntimeEffectRepository.mark_cleanup_clean)
