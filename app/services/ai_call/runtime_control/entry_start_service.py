from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from app.services.ai_call.runtime_control.command_repository import (
    CommandSnapshot,
    RuntimeCommandRepository,
    StartCallIntent,
)
from app.services.ai_call.runtime_control.roles import (
    RuntimeRoleConfigurationError,
    runtime_control_mode_for_entry,
)
from app.services.ai_call.runtime_control.types import OwnerCommandEntry


class RuntimeEntryStartError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class StartEntryRequest:
    tenant_id: str
    entry_type: OwnerCommandEntry | str
    idempotency_key: str
    payload: Mapping[str, object]
    business_type: str | None = None
    business_id: str | None = None
    scene_code: str | None = None
    prompt_source_key: str | None = None
    allocation_deadline_at: datetime | None = None
    allocation_timeout_seconds: float | None = None
    sensitive_payload_ciphertext: str | None = None
    payload_key_version: str | None = None


class RuntimeEntryStartService:
    """把已授权的入口请求转换为持久 START_CALL 意图。"""

    def __init__(self, *, settings: Any, repository: RuntimeCommandRepository) -> None:
        self._settings = settings
        self._repository = repository

    async def submit(self, request: StartEntryRequest) -> CommandSnapshot | None:
        try:
            entry = OwnerCommandEntry(request.entry_type)
        except ValueError as exc:
            raise RuntimeEntryStartError(
                f"入口 {request.entry_type!s} 不是合法的 owner command entry"
            ) from exc

        try:
            mode = runtime_control_mode_for_entry(self._settings, entry)
        except RuntimeRoleConfigurationError as exc:
            raise RuntimeEntryStartError(str(exc)) from exc
        if mode != "owner_command_v1":
            return None

        tenant_id = request.tenant_id.strip()
        idempotency_key = request.idempotency_key.strip()
        if not tenant_id:
            raise RuntimeEntryStartError("owner command entry 必须有租户")
        if not idempotency_key:
            raise RuntimeEntryStartError("START_CALL 必须有幂等键")
        if (
            request.allocation_timeout_seconds is not None
            and request.allocation_timeout_seconds <= 0
        ):
            raise RuntimeEntryStartError("START_CALL 排队超时必须大于 0 秒")

        payload = dict(request.payload)
        if entry is OwnerCommandEntry.DIRECT_SIP:
            if not request.sensitive_payload_ciphertext or not request.payload_key_version:
                raise RuntimeEntryStartError(
                    "direct_sip 必须提供敏感参数密文和密钥版本"
                )
            if {
                "callee_phone_number",
                "calleePhoneNumber",
                "phone_number",
                "phoneNumber",
                "destination_phone",
                "destinationPhone",
            } & payload.keys():
                raise RuntimeEntryStartError(
                    "direct_sip payload 不得包含明文号码"
                )

        return await self._repository.create_start_call(
            StartCallIntent(
                tenant_id=tenant_id,
                entry_type=entry.value,
                idempotency_key=idempotency_key,
                payload=payload,
                business_type=request.business_type,
                business_id=request.business_id,
                scene_code=request.scene_code,
                prompt_source_key=request.prompt_source_key,
                allocation_deadline_at=request.allocation_deadline_at,
                allocation_timeout_seconds=request.allocation_timeout_seconds,
                sensitive_payload_ciphertext=request.sensitive_payload_ciphertext,
                payload_key_version=request.payload_key_version,
            )
        )
