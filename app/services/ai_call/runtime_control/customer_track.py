from __future__ import annotations

import hashlib


def customer_identity_digest(participant_identity: str) -> str:
    if not participant_identity.strip():
        raise ValueError("participant_identity must not be blank")
    return hashlib.sha256(participant_identity.encode("utf-8")).hexdigest()


def customer_track_keys(
    call_id: str,
    participant_identity: str,
) -> tuple[str, str, str]:
    digest = customer_identity_digest(participant_identity)
    keys = (
        f"start:{call_id}:ctr:{digest}",
        f"egress:ctr:{call_id}:{digest}",
        f"egress:track:{call_id}:customer:{digest}",
    )
    if any(len(key) > 160 for key in keys):
        raise ValueError("customer track key exceeds 160 characters")
    return keys
