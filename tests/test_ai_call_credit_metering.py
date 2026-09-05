from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
from decimal import Decimal
from pathlib import Path

import pytest

from app.core.exceptions import CustomException
from app.services.ai_call.credit_metering import (
    CREDIT_ELIGIBILITY_PATH,
    CreditMeteringClient,
    _decimal_string,
    build_signed_headers,
    require_credit_eligible_for_request,
)


def test_reach_credit_signature_matches_platform_contract() -> None:
    body = '{"ownerId":"42"}'
    headers = build_signed_headers(
        method="POST",
        path=CREDIT_ELIGIBILITY_PATH,
        body=body,
        client_id="reach",
        secret="secret-value",
        timestamp="1700000000",
        nonce="nonce-1",
    )
    digest = hashlib.sha256(body.encode()).hexdigest()
    canonical = "\n".join(("POST", CREDIT_ELIGIBILITY_PATH, "1700000000", "nonce-1", digest))
    expected = base64.b64encode(
        hmac.new(b"secret-value", canonical.encode(), hashlib.sha256).digest()
    ).decode()
    assert headers["X-LC-Signature"] == expected


def test_disabled_reach_billing_is_free_without_business_switch() -> None:
    async def post(_path: str, _payload: dict[str, object]) -> dict[str, object]:
        return {"eligible": True, "meteringEnabled": False}

    asyncio.run(
        CreditMeteringClient(post=post).require_eligible(tenant_id="tenant-1", owner_id="user-1")
    )


def test_reach_decimal_string_does_not_drop_integer_zeroes() -> None:
    assert _decimal_string(Decimal("30")) == "30"
    assert _decimal_string(Decimal("30.25000000")) == "30.25"


def test_reach_insufficient_balance_maps_to_global_recharge_dialog_contract() -> None:
    async def post(_path: str, _payload: dict[str, object]) -> dict[str, object]:
        return {
            "eligible": False,
            "reasonCode": "INSUFFICIENT_START_BALANCE",
            "reasonMessage": "信用点余额低于任务最低启动点数",
            "availablePoints": "1",
            "minimumStartPoints": "10",
        }

    with pytest.raises(CustomException) as caught:
        asyncio.run(
            require_credit_eligible_for_request(
                CreditMeteringClient(post=post),
                tenant_id="tenant-1",
                owner_id="user-1",
            )
        )

    assert caught.value.status_code == 402
    assert caught.value.code == 10402
    assert caught.value.data == {
        "creditReason": "INSUFFICIENT_START_BALANCE",
        "availablePoints": "1",
        "minimumStartPoints": "10",
    }


def test_reach_credit_outbox_migration_matches_runtime_model() -> None:
    sql = (
        Path(__file__).parents[1]
        / "docs/livekit-ai-outbound/sql/phase-k2-credit-metering-postgres.sql"
    ).read_text()

    assert "CREATE TABLE IF NOT EXISTS reach_credit_usage_outbox" in sql
    assert "UNIQUE (tenant_id, idempotency_key)" in sql
