from __future__ import annotations

import hashlib
from collections import deque
from collections.abc import Mapping, Sequence
from enum import StrEnum

from app.services.ai_call.runtime_control.effect_repository import (
    EffectClaim,
    ProviderObservation,
    ProviderObservationKind,
)
from app.services.ai_call.runtime_control.handoff_handlers import (
    AgentMediaObservation,
)


class StubObservationKind(StrEnum):
    RESOURCE_PRESENT = "RESOURCE_PRESENT"
    NO_RESOURCE = "NO_RESOURCE"
    DESTROY_CONFIRMED = "DESTROY_CONFIRMED"
    REQUEST_ACCEPTED = "REQUEST_ACCEPTED"
    RESULT_UNKNOWN = "RESULT_UNKNOWN"
    RETRYABLE_NO_EFFECT = "RETRYABLE_NO_EFFECT"
    PERMANENT_NO_EFFECT = "PERMANENT_NO_EFFECT"


_OBSERVATION_MAPPING = {
    StubObservationKind.RESOURCE_PRESENT: ProviderObservationKind.RESOURCE_PRESENT,
    StubObservationKind.NO_RESOURCE: ProviderObservationKind.RESOURCE_ABSENT,
    StubObservationKind.DESTROY_CONFIRMED: ProviderObservationKind.TERMINAL_CONFIRMED,
    StubObservationKind.REQUEST_ACCEPTED: ProviderObservationKind.ACCEPTED,
    StubObservationKind.RESULT_UNKNOWN: ProviderObservationKind.UNCERTAIN,
    StubObservationKind.RETRYABLE_NO_EFFECT: ProviderObservationKind.RETRYABLE_FAILURE,
    StubObservationKind.PERMANENT_NO_EFFECT: (
        ProviderObservationKind.PERMANENT_NO_RESOURCE
    ),
}


class DeterministicWebProviderStub:
    """Deterministic DB-only Web provider; never performs network or SDK calls."""

    _CREATE_EFFECT_TYPES = frozenset(
        {
            "CREATE_ROOM",
            "ATTACH_AGENT_PARTICIPANT",
        }
    )
    _DESTROY_EFFECT_TYPES = frozenset(
        {
            "DISCONNECT_AGENT_PARTICIPANT",
            "DELETE_ROOM",
        }
    )

    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    async def apply(self, effect: EffectClaim) -> ProviderObservation:
        self.calls.append(
            {
                "provider_namespace": effect.provider_namespace,
                "effect_type": effect.effect_type,
                "resource_key": effect.resource_key,
            }
        )
        if effect.effect_type in self._CREATE_EFFECT_TYPES:
            return ProviderObservation(
                kind=ProviderObservationKind.RESOURCE_PRESENT,
                provider_reference=f"stub:{effect.resource_key}",
            )
        if effect.effect_type in self._DESTROY_EFFECT_TYPES:
            return ProviderObservation(
                kind=ProviderObservationKind.TERMINAL_CONFIRMED,
            )
        raise LookupError(
            f"unsupported deterministic Web effect type {effect.effect_type}"
        )

    async def query_agent_media(
        self,
        room_name: str,
        participant_identity: str,
    ) -> AgentMediaObservation:
        digest = hashlib.sha256(
            f"{room_name}|{participant_identity}".encode()
        ).hexdigest()
        return AgentMediaObservation(
            ready=True,
            participant_identity=participant_identity,
            participant_sid=f"PA_stub_{digest[:24]}",
            track_sid=f"TR_stub_{digest[24:48]}",
        )


class DeterministicDbOnlyProviderStub(DeterministicWebProviderStub):
    _CREATE_EFFECT_TYPES = DeterministicWebProviderStub._CREATE_EFFECT_TYPES | {
        "CREATE_SIP_PARTICIPANT"
    }
    _DESTROY_EFFECT_TYPES = DeterministicWebProviderStub._DESTROY_EFFECT_TYPES | {
        "HANGUP_SIP"
    }


class ScriptedProviderStub:
    """In-memory Provider fact source; never performs network or SDK calls."""

    def __init__(
        self,
        script: Mapping[
            str,
            Sequence[StubObservationKind | ProviderObservation],
        ],
    ) -> None:
        self._script = {key: deque(values) for key, values in script.items()}
        self.calls: list[dict[str, str]] = []

    async def apply(self, effect: EffectClaim) -> ProviderObservation:
        self.calls.append(
            {
                "provider_namespace": effect.provider_namespace,
                "effect_type": effect.effect_type,
                "resource_key": effect.resource_key,
            }
        )
        observations = self._script.get(effect.resource_key)
        if not observations:
            raise LookupError(
                f"no scripted observation for resource key {effect.resource_key}"
            )
        scripted = observations.popleft()
        if isinstance(scripted, ProviderObservation):
            return scripted
        observation = ProviderObservation(kind=_OBSERVATION_MAPPING[scripted])
        if scripted == StubObservationKind.RESOURCE_PRESENT:
            return ProviderObservation(
                kind=observation.kind,
                provider_reference=f"stub:{effect.resource_key}",
            )
        return observation

    async def query_agent_media(
        self,
        room_name: str,
        participant_identity: str,
    ) -> AgentMediaObservation:
        digest = hashlib.sha256(
            f"{room_name}|{participant_identity}".encode()
        ).hexdigest()
        return AgentMediaObservation(
            ready=True,
            participant_identity=participant_identity,
            participant_sid=f"PA_stub_{digest[:24]}",
            track_sid=f"TR_stub_{digest[24:48]}",
        )
