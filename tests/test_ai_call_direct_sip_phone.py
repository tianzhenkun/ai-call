from __future__ import annotations

import pytest

from app.services.ai_call.runtime_control.direct_sip_phone import (
    DirectSipPhoneError,
    payload_contains_phone,
    prepare_direct_sip_phone,
)


def test_prepare_direct_sip_phone_keeps_plaintext_and_builds_mask_and_hash() -> None:
    phone = prepare_direct_sip_phone(" 13812345678 ")

    assert phone.plaintext == "13812345678"
    assert phone.masked == "138****5678"
    assert phone.fingerprint.startswith("sha256:")
    assert "13812345678" not in phone.fingerprint


@pytest.mark.parametrize("value", ["", "138-1234-5678", "abc", "+12"])
def test_prepare_direct_sip_phone_rejects_non_canonical_number(value: str) -> None:
    with pytest.raises(DirectSipPhoneError, match="格式不合法"):
        prepare_direct_sip_phone(value)


def test_payload_contains_phone_checks_nested_keys_values_and_text() -> None:
    assert payload_contains_phone(
        {"business_params": {"contact": "13812345678"}},
        "13812345678",
    )
    assert payload_contains_phone(
        {"business_params": {"note": "请联系 13812345678"}},
        "13812345678",
    )
    assert payload_contains_phone(
        {"business_params": {"13812345678": "contact"}},
        "13812345678",
    )
    assert payload_contains_phone(
        {"business_params": ["张三", {"contact": "13812345678"}]},
        "13812345678",
    )
    assert not payload_contains_phone(
        {"business_params": {"customerName": "张三"}},
        "13812345678",
    )


@pytest.mark.parametrize(
    "payload",
    (
        {"phone": "8613812345678"},
        {"phone": "+86 138 1234 5678"},
        {"phone": 8613812345678},
    ),
)
def test_payload_contains_phone_rejects_equivalent_international_forms(
    payload: object,
) -> None:
    assert payload_contains_phone(payload, "+8613812345678")
