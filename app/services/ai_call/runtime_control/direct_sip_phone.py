from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

_PHONE_PATTERN = re.compile(r"^\+?\d{5,20}$")


class DirectSipPhoneError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class DirectSipPhone:
    plaintext: str
    masked: str
    fingerprint: str


def prepare_direct_sip_phone(value: str) -> DirectSipPhone:
    plaintext = str(value or "").strip()
    if not _PHONE_PATTERN.fullmatch(plaintext):
        raise DirectSipPhoneError("Direct SIP 被叫号码格式不合法")
    digits = "".join(character for character in plaintext if character.isdigit())
    masked = "***" if len(digits) <= 7 else f"{digits[:3]}****{digits[-4:]}"
    digest = hashlib.sha256(digits.encode("utf-8")).hexdigest()
    return DirectSipPhone(
        plaintext=plaintext,
        masked=masked,
        fingerprint=f"sha256:{digest}",
    )


def payload_contains_phone(value: object, phone_number: str) -> bool:
    phone_digits = "".join(character for character in phone_number if character.isdigit())
    if not phone_digits:
        return False

    def _contains(candidate: object) -> bool:
        if isinstance(candidate, str):
            compact = re.sub(r"[\s()+.\-]+", "", candidate)
            return phone_number in candidate or phone_digits in compact
        if isinstance(candidate, int) and not isinstance(candidate, bool):
            return phone_digits in str(candidate)
        if isinstance(candidate, Mapping):
            return any(_contains(item) for pair in candidate.items() for item in pair)
        if isinstance(candidate, Sequence) and not isinstance(
            candidate,
            (str, bytes, bytearray),
        ):
            return any(_contains(item) for item in candidate)
        return False

    return _contains(value)
