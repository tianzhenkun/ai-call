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
    return SimpleNamespace(
        AI_CALL_OWNER_COMMAND_V1_ENTRIES=entries,
        QWEN_REALTIME_MODEL="qwen3.5-omni-plus-realtime",
        QWEN_REALTIME_TURN_DETECTION_TYPE="server_vad",
        QWEN_REALTIME_VOICE="Tina",
        AI_CALL_CUSTOMER_SPEECH_REVIEW_ENABLED=False,
    )


@pytest.mark.anyio
@pytest.mark.parametrize("model", ["qwen3.5-omni-plus-realtime", "qwen3.5-omni-flash-realtime"])
@pytest.mark.parametrize("entry", ["web", "direct_sip"])
async def test_session_model_and_default_voice_are_persisted_without_changing_settings(model, entry) -> None:
    repository = _FakeRepository([])
    settings = _settings("web,direct_sip")
    await RuntimeEntryStartService(settings=settings, repository=repository).submit(
        StartEntryRequest(
            tenant_id="tenant-a", entry_type=entry, idempotency_key="test:model",
            payload={"model": model},
            callee_phone_number="19900001001" if entry == "direct_sip" else None,
        )
    )
    assert repository.requests[0].payload == {"model": model, "voice": "Tina"}
    assert settings.QWEN_REALTIME_MODEL == "qwen3.5-omni-plus-realtime"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "entry,model,overrides,error",
    [
        ("web", "arbitrary-model", {}, "仅支持"),
        ("web", "", {}, "仅支持"),
        ("web", {}, {}, "仅支持"),
        ("web", "qwen3.5-omni-flash-realtime", {"QWEN_REALTIME_MODEL": "qwen-audio-3.1-realtime-plus"}, "基线"),
        ("web", "qwen3.5-omni-flash-realtime", {"QWEN_REALTIME_TURN_DETECTION_TYPE": "smart_turn"}, "基线"),
        ("web", "qwen3.5-omni-flash-realtime", {"AI_CALL_CUSTOMER_SPEECH_REVIEW_ENABLED": True}, "基线"),
    ],
)
async def test_web_model_rejects_unsupported_models_entries_and_baselines(entry, model, overrides, error) -> None:
    repository = _FakeRepository([])
    settings = _settings("web,direct_sip")
    for key, value in overrides.items():
        setattr(settings, key, value)
    with pytest.raises(RuntimeEntryStartError, match=error):
        await RuntimeEntryStartService(settings=settings, repository=repository).submit(
            StartEntryRequest(
                tenant_id="tenant-a", entry_type=entry, idempotency_key="web:bad-model",
                payload={"model": model, "voice": "Tina"},
            )
        )
    assert repository.requests == []


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
