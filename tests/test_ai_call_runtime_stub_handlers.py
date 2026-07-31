from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from app.services.ai_call.runtime_control.command_repository import CommandClaim
from app.services.ai_call.runtime_control.effect_repository import EffectClaim, EffectSpec
from app.services.ai_call.runtime_control.handlers import EndCallHandler, StartCallHandler
from app.services.ai_call.runtime_control.owner_repository import OwnerLease
from app.services.ai_call.runtime_control.provider_stub import (
    ScriptedProviderStub,
    StubObservationKind,
)


def _effect_claim(
    *,
    effect_id: int = 1,
    effect_type: str = "CREATE_ROOM",
    resource_key: str = "room:call-a:g1",
) -> EffectClaim:
    now = datetime.now(timezone.utc)
    return EffectClaim(
        effect_id=effect_id,
        tenant_id="tenant-a",
        call_id="call-a",
        effect_type=effect_type,
        processing_owner_id="runtime-a",
        processing_fencing_token=1,
        processing_token="effect-token",
        processing_expires_at=now,
        source_create_effect_id=None,
        create_protection_deadline_at=None,
        attempt_count=1,
        reconcile_only=False,
        provider_namespace="stub:test",
        resource_key=resource_key,
    )


def _command_claim(command_type: str) -> CommandClaim:
    now = datetime.now(timezone.utc)
    return CommandClaim(
        command_id=10,
        tenant_id="tenant-a",
        call_id="call-a",
        command_seq=1 if command_type == "START_CALL" else 2,
        command_type=command_type,
        processing_owner_id="runtime-a",
        processing_fencing_token=1,
        processing_token="command-token",
        processing_expires_at=now,
        payload_json=None,
        attempt_count=1,
    )


def _owner_lease() -> OwnerLease:
    return OwnerLease(
        tenant_id="tenant-a",
        call_id="call-a",
        owner_id="runtime-a",
        fencing_token=1,
        lease_expires_at=datetime.now(timezone.utc),
        capacity_class="active",
    )


class _FakeSession:
    def __init__(self) -> None:
        self.record = SimpleNamespace(status="ending", ended_at=None)

    async def scalar(self, statement):
        if "clock_timestamp" in str(statement):
            return datetime.now(timezone.utc)
        return self.record


class _Transaction:
    def __init__(self, factory: _FakeSessionFactory) -> None:
        self.factory = factory

    async def __aenter__(self):
        self.factory.active_transactions += 1
        return self.factory.session

    async def __aexit__(self, exc_type, exc, traceback):
        self.factory.active_transactions -= 1


class _FakeSessionFactory:
    def __init__(self) -> None:
        self.session = _FakeSession()
        self.active_transactions = 0

    def begin(self):
        return _Transaction(self)


class _FakeEffectRepository:
    def __init__(self, claims: list[EffectClaim], *, clean: bool) -> None:
        self.claims = claims
        self.clean = clean
        self.registered: list[EffectSpec] = []
        self.submitted: list[tuple[EffectClaim, object]] = []
        self.end_graph_registered = False

    async def register(self, command_claim, spec):
        self.registered.append(spec)

    async def register_end_graph(self, command_claim):
        self.end_graph_registered = True
        return []

    async def claim_next(self, owner_lease):
        return self.claims.pop(0) if self.claims else None

    async def submit(self, claim, observation):
        self.submitted.append((claim, observation))
        return True

    async def mark_cleanup_clean(self, owner_lease):
        return self.clean


class _FakeCommandRepository:
    def __init__(self) -> None:
        self.decisions = []

    async def complete(self, claim, decision):
        self.decisions.append(decision)
        return True


class _FakeRecoveryOwnerRepository:
    async def park_attention(self, lease, retry_after) -> bool:
        return False


class _AssertingProvider(ScriptedProviderStub):
    def __init__(self, factory: _FakeSessionFactory, script) -> None:
        super().__init__(script)
        self.factory = factory

    async def apply(self, effect):
        assert self.factory.active_transactions == 0
        return await super().apply(effect)


@pytest.mark.anyio
async def test_scripted_provider_stub_returns_only_scripted_facts() -> None:
    stub = ScriptedProviderStub(
        {"room:call-a:g1": [StubObservationKind.RESOURCE_PRESENT]}
    )

    observation = await stub.apply(_effect_claim())

    assert observation.kind == "RESOURCE_PRESENT"
    assert stub.calls == [
        {
            "provider_namespace": "stub:test",
            "effect_type": "CREATE_ROOM",
            "resource_key": "room:call-a:g1",
        }
    ]


@pytest.mark.anyio
async def test_scripted_provider_stub_rejects_unscripted_network_like_calls() -> None:
    stub = ScriptedProviderStub({})

    with pytest.raises(LookupError):
        await stub.apply(_effect_claim())


@pytest.mark.anyio
async def test_start_handler_commits_before_stub_and_completes_after_effects() -> None:
    factory = _FakeSessionFactory()
    effect_repository = _FakeEffectRepository(
        [
            _effect_claim(),
            _effect_claim(
                effect_id=2,
                effect_type="ATTACH_AGENT_PARTICIPANT",
                resource_key="agent:call-a:g1",
            ),
        ],
        clean=False,
    )
    command_repository = _FakeCommandRepository()
    provider = _AssertingProvider(
        factory,
        {
            "room:call-a:g1": [StubObservationKind.RESOURCE_PRESENT],
            "agent:call-a:g1": [StubObservationKind.RESOURCE_PRESENT],
        },
    )
    specs = [
        EffectSpec(
            effect_type="CREATE_ROOM",
            idempotency_key="room",
            provider_namespace="stub:test",
            provider_idempotency_key="room",
            resource_key="room:call-a:g1",
            resource_generation=1,
        ),
        EffectSpec(
            effect_type="ATTACH_AGENT_PARTICIPANT",
            idempotency_key="agent",
            provider_namespace="stub:test",
            provider_idempotency_key="agent",
            resource_key="agent:call-a:g1",
            resource_generation=1,
        ),
    ]

    result = await StartCallHandler(
        factory,
        provider,
        effect_repository_factory=lambda session: effect_repository,
        command_repository_factory=lambda session: command_repository,
    ).handle(_command_claim("START_CALL"), _owner_lease(), specs)

    assert result.command_completed is True
    assert result.applied_effect_count == 2
    assert len(effect_repository.registered) == 2
    assert command_repository.decisions[0].status == "SUCCEEDED"


@pytest.mark.anyio
async def test_end_handler_separates_logical_end_from_cleanup_status() -> None:
    factory = _FakeSessionFactory()
    effect_repository = _FakeEffectRepository(
        [
            _effect_claim(
                effect_type="DELETE_ROOM",
                resource_key="room:call-a:g1",
            )
        ],
        clean=False,
    )
    command_repository = _FakeCommandRepository()
    provider = _AssertingProvider(
        factory,
        {"room:call-a:g1": [StubObservationKind.REQUEST_ACCEPTED]},
    )

    result = await EndCallHandler(
        factory,
        provider,
        effect_repository_factory=lambda session: effect_repository,
        command_repository_factory=lambda session: command_repository,
        recovery_owner_repository_factory=lambda session: _FakeRecoveryOwnerRepository(),
    ).handle(_command_claim("END_CALL"), _owner_lease())

    assert result.logical_end_completed is True
    assert result.resource_cleanup_status == "reconciling"
    assert effect_repository.end_graph_registered is True
    assert factory.session.record.status == "completed"
