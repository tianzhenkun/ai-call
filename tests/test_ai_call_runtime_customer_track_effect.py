from __future__ import annotations

import hashlib
import importlib

import pytest


def _customer_track_module():
    return importlib.import_module(
        "app.services.ai_call.runtime_control.customer_track"
    )


def test_customer_identity_digest_hashes_raw_utf8_without_stripping() -> None:
    module = _customer_track_module()
    identity = "  customer-张三  "

    assert module.customer_identity_digest(identity) == hashlib.sha256(
        identity.encode("utf-8")
    ).hexdigest()


@pytest.mark.parametrize("identity", ["", " ", "\t\r\n"])
def test_customer_identity_digest_rejects_blank_identity(identity: str) -> None:
    module = _customer_track_module()

    with pytest.raises(ValueError, match="participant_identity"):
        module.customer_identity_digest(identity)


def test_customer_track_keys_are_stable_bounded_and_role_specific() -> None:
    module = _customer_track_module()
    digest = hashlib.sha256(b"customer-a").hexdigest()

    keys = module.customer_track_keys("call-a", "customer-a")

    assert keys == (
        f"start:call-a:ctr:{digest}",
        f"egress:ctr:call-a:{digest}",
        f"egress:track:call-a:customer:{digest}",
    )
    assert all(len(key) <= 160 for key in keys)
