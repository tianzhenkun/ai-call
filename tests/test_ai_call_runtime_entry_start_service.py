from __future__ import annotations

import json
from dataclasses import dataclass
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
async def test_direct_sip_builds_plain_record_fields_without_sensitive_command_payload(
) -> None:
    repository = _FakeRepository([])
    service = RuntimeEntryStartService(
        settings=_settings("direct_sip"),
        repository=repository,
    )

    result = await service.submit(
        StartEntryRequest(
            tenant_id="tenant-a",
            entry_type="direct_sip",
            idempotency_key="start:sip:1",
            payload={"voice": "v1", "business_params": {"customerName": "张三"}},
            callee_phone_number="13812345678",
        )
    )

    assert result == "command-snapshot"
    intent = repository.requests[-1]
    assert intent.callee_phone_number == "13812345678"
    assert intent.callee_phone_number_masked == "138****5678"
    assert intent.callee_phone_number_hash.startswith("sha256:")
    assert "13812345678" not in json.dumps(intent.payload, ensure_ascii=False)
    assert intent.sensitive_payload_ciphertext is None
    assert intent.payload_key_version is None


@pytest.mark.anyio
async def test_direct_sip_rejects_phone_repeated_in_nested_payload() -> None:
    repository = _FakeRepository([])
    service = RuntimeEntryStartService(
        settings=_settings("direct_sip"),
        repository=repository,
    )

    with pytest.raises(RuntimeEntryStartError, match="payload"):
        await service.submit(
            StartEntryRequest(
                tenant_id="tenant-a",
                entry_type="direct_sip",
                idempotency_key="start:sip:nested-phone",
                payload={"business_params": {"note": "联系 13812345678"}},
                callee_phone_number="13812345678",
            )
        )

    assert repository.requests == []


@pytest.mark.anyio
async def test_web_rejects_direct_sip_phone_field() -> None:
    repository = _FakeRepository([])
    service = RuntimeEntryStartService(
        settings=_settings("web"),
        repository=repository,
    )

    with pytest.raises(RuntimeEntryStartError, match="Web"):
        await service.submit(
            StartEntryRequest(
                tenant_id="tenant-a",
                entry_type="web",
                idempotency_key="start:web:phone",
                payload={},
                callee_phone_number="13812345678",
            )
        )

    assert repository.requests == []


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
