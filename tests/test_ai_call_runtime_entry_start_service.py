from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from app.services.ai_call.runtime_control.command_repository import StartCallIntent
from app.services.ai_call.runtime_control.entry_start_service import (
    RuntimeEntryStartError,
    RuntimeEntryStartService,
    StartEntryRequest,
)


@dataclass
class _FakeRepository:
    requests: list[StartCallIntent]

    async def create_start_call(self, request: StartCallIntent):
        self.requests.append(request)
        return "command-snapshot"


def _settings(entries: str) -> SimpleNamespace:
    return SimpleNamespace(AI_CALL_OWNER_COMMAND_V1_ENTRIES=entries)


@pytest.mark.anyio
async def test_enabled_entry_persists_only_an_owner_command_start_intent() -> None:
    repository = _FakeRepository([])
    service = RuntimeEntryStartService(
        settings=_settings("web"),
        repository=repository,
    )

    result = await service.submit(
        StartEntryRequest(
            tenant_id="tenant-a",
            entry_type="web",
            idempotency_key="start:web:1",
            payload={"business_id": "biz-1", "voice": "v1"},
            business_id="biz-1",
            scene_code="collection",
            allocation_timeout_seconds=30.0,
        )
    )

    assert result == "command-snapshot"
    assert repository.requests == [
        StartCallIntent(
            tenant_id="tenant-a",
            entry_type="web",
            idempotency_key="start:web:1",
            payload={"business_id": "biz-1", "voice": "v1"},
            business_id="biz-1",
            scene_code="collection",
            allocation_timeout_seconds=30.0,
        )
    ]


@pytest.mark.anyio
async def test_disabled_entry_returns_legacy_signal_without_persisting() -> None:
    repository = _FakeRepository([])
    service = RuntimeEntryStartService(
        settings=_settings("web"),
        repository=repository,
    )

    result = await service.submit(
        StartEntryRequest(
            tenant_id="tenant-a",
            entry_type="direct_sip",
            idempotency_key="start:direct-sip:disabled",
            payload={},
        )
    )

    assert result is None
    assert repository.requests == []


@pytest.mark.anyio
async def test_enabled_entry_rejects_non_positive_allocation_timeout() -> None:
    service = RuntimeEntryStartService(
        settings=_settings("web"),
        repository=_FakeRepository([]),
    )

    with pytest.raises(RuntimeEntryStartError, match="排队超时"):
        await service.submit(
            StartEntryRequest(
                tenant_id="tenant-a",
                entry_type="web",
                idempotency_key="start:web:invalid-timeout",
                payload={},
                allocation_timeout_seconds=0,
            )
        )


@pytest.mark.anyio
async def test_preview_is_not_an_owner_command_entry() -> None:
    repository = _FakeRepository([])
    service = RuntimeEntryStartService(
        settings=_settings("web"),
        repository=repository,
    )

    with pytest.raises(RuntimeEntryStartError, match="不是合法"):
        await service.submit(
            StartEntryRequest(
                tenant_id="tenant-a",
                entry_type="preview",
                idempotency_key="start:preview:1",
                payload={"voice": "v1"},
            )
        )

    assert repository.requests == []


@pytest.mark.anyio
async def test_direct_sip_requires_encrypted_sensitive_payload_and_rejects_plain_number(
) -> None:
    repository = _FakeRepository([])
    service = RuntimeEntryStartService(
        settings=_settings("direct_sip"),
        repository=repository,
    )
    base = {
        "tenant_id": "tenant-a",
        "entry_type": "direct_sip",
        "idempotency_key": "start:sip:1",
        "business_type": "manual_sip",
        "business_id": "biz-1",
    }

    with pytest.raises(RuntimeEntryStartError, match="密文"):
        await service.submit(
            StartEntryRequest(
                **base,
                payload={"calleePhoneNumber": "13800000000"},
            )
        )

    with pytest.raises(RuntimeEntryStartError, match="明文号码"):
        await service.submit(
            StartEntryRequest(
                **base,
                payload={"callee_phone_number": "13800000000"},
                sensitive_payload_ciphertext="ciphertext",
                payload_key_version="v1",
            )
        )

    result = await service.submit(
        StartEntryRequest(
            **base,
            payload={"callee_phone_number_hash": "hash"},
            sensitive_payload_ciphertext="ciphertext",
            payload_key_version="v1",
            allocation_deadline_at=datetime.now(timezone.utc),
        )
    )

    assert result == "command-snapshot"
    assert repository.requests[-1].sensitive_payload_ciphertext == "ciphertext"
    assert repository.requests[-1].payload_key_version == "v1"


@pytest.mark.anyio
async def test_sip_inbound_is_not_an_owner_command_entry() -> None:
    service = RuntimeEntryStartService(
        settings=_settings("web"),
        repository=_FakeRepository([]),
    )

    with pytest.raises(RuntimeEntryStartError, match="合法"):
        await service.submit(
            StartEntryRequest(
                tenant_id="tenant-a",
                entry_type="sip_inbound",
                idempotency_key="start:inbound:1",
                payload={},
            )
        )
