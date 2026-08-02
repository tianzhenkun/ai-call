from __future__ import annotations

from collections.abc import Iterable
from enum import StrEnum
from typing import Any, TypeVar

from app.services.ai_call.runtime_control.types import OwnerCommandEntry, ProcessRole

EnumT = TypeVar("EnumT", bound=StrEnum)


class RuntimeRoleConfigurationError(ValueError):
    pass


def _parse_csv_enum(raw: str, enum_type: type[EnumT], label: str) -> frozenset[EnumT]:
    values = [value.strip() for value in raw.split(",") if value.strip()]
    parsed: set[EnumT] = set()
    invalid: list[str] = []
    for value in values:
        try:
            parsed.add(enum_type(value))
        except ValueError:
            invalid.append(value)
    if invalid:
        allowed = ",".join(item.value for item in enum_type)
        raise RuntimeRoleConfigurationError(
            f"{label} 包含非法值: {','.join(sorted(set(invalid)))}；合法值: {allowed}"
        )
    return frozenset(parsed)


def parse_process_roles(raw: str) -> frozenset[ProcessRole]:
    roles = _parse_csv_enum(raw, ProcessRole, "AI_CALL_PROCESS_ROLES")
    if not roles:
        raise RuntimeRoleConfigurationError("AI_CALL_PROCESS_ROLES 不能为空")
    if ProcessRole.LEGACY_RUNTIME in roles and ProcessRole.API not in roles:
        raise RuntimeRoleConfigurationError("legacy_runtime 必须与 api 同进程启用")
    if ProcessRole.LEGACY_RUNTIME in roles and ProcessRole.RUNTIME in roles:
        raise RuntimeRoleConfigurationError("legacy_runtime 不得与 runtime 同进程启用")
    return roles


def parse_owner_command_entries(raw: str) -> frozenset[OwnerCommandEntry]:
    return _parse_csv_enum(
        raw,
        OwnerCommandEntry,
        "AI_CALL_OWNER_COMMAND_V1_ENTRIES",
    )


def _environment_value(settings: Any) -> str | None:
    value = getattr(settings, "ENVIRONMENT", None)
    if value is None:
        return None
    return str(getattr(value, "value", value)).strip().lower() or None


def runtime_control_mode_for_entry(settings: Any, entry: OwnerCommandEntry | str) -> str:
    try:
        entry_value = OwnerCommandEntry(entry)
    except ValueError as exc:
        raise RuntimeRoleConfigurationError(
            f"入口 {entry!s} 不是合法的 owner command entry"
        ) from exc
    entries = parse_owner_command_entries(
        str(settings.AI_CALL_OWNER_COMMAND_V1_ENTRIES)
    )
    return "owner_command_v1" if entry_value in entries else "legacy_local"


def validate_runtime_role_settings(settings: Any) -> frozenset[ProcessRole]:
    roles = parse_process_roles(str(settings.AI_CALL_PROCESS_ROLES))
    if (
        _environment_value(settings) == "prod"
        and str(getattr(settings, "AI_CALL_RUNTIME_PROVIDER_MODE", "stub"))
        == "livekit"
    ):
        raise RuntimeRoleConfigurationError(
            "正式环境 AI Call Runtime Provider 必须保持 stub"
        )
    entries = parse_owner_command_entries(
        str(settings.AI_CALL_OWNER_COMMAND_V1_ENTRIES)
    )
    if entries:
        if _environment_value(settings) == "prod":
            raise RuntimeRoleConfigurationError(
                "正式环境 AI_CALL_OWNER_COMMAND_V1_ENTRIES 必须为空"
            )
        required_roles = {
            ProcessRole.API,
            ProcessRole.RUNTIME,
            ProcessRole.DISPATCHER,
        }
        missing_roles = required_roles - roles
        if missing_roles:
            raise RuntimeRoleConfigurationError(
                "owner command entry 必须同时启用 api、runtime、dispatcher；"
                f"缺少: {','.join(sorted(role.value for role in missing_roles))}"
            )
        if ProcessRole.LEGACY_RUNTIME in roles:
            raise RuntimeRoleConfigurationError(
                "owner command entry 不得与 legacy_runtime 同进程启用"
            )
        if (
            OwnerCommandEntry.OUTBOUND in entries
            and ProcessRole.OUTBOUND not in roles
        ):
            raise RuntimeRoleConfigurationError(
                "outbound entry 必须同时启用 outbound 角色"
            )
    runtime_roles: Iterable[ProcessRole] = (
        ProcessRole.RUNTIME,
        ProcessRole.DISPATCHER,
    )
    if any(role in roles for role in runtime_roles) and settings.DATABASE_TYPE != "postgres":
        raise RuntimeRoleConfigurationError(
            "runtime/dispatcher 角色只允许使用 PostgreSQL"
        )
    if ProcessRole.RUNTIME in roles and not str(
        settings.AI_CALL_RUNTIME_INSTANCE_ID
    ).strip():
        raise RuntimeRoleConfigurationError(
            "runtime 角色必须配置 AI_CALL_RUNTIME_INSTANCE_ID"
        )
    return roles
