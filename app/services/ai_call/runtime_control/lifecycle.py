from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.services.ai_call.runtime_control.dispatcher_service import (
    DispatcherControlService,
)
from app.services.ai_call.runtime_control.health import default_runtime_worker_health
from app.services.ai_call.runtime_control.owner_repository import build_worker_id
from app.services.ai_call.runtime_control.postgres_wakeup import (
    PostgresWakeupListener,
)
from app.services.ai_call.runtime_control.provider_stub import (
    DeterministicDbOnlyProviderStub,
)
from app.services.ai_call.runtime_control.recovery_service import RecoveryControlService
from app.services.ai_call.runtime_control.runtime_service import (
    RuntimeControlService,
    RuntimeRegistry,
)


@dataclass(frozen=True, slots=True)
class AiCallRuntimeTimingPolicy:
    end_scan_interval_seconds: float = 0.5
    command_scan_interval_seconds: float = 1.0


async def validate_db_only_runtime_database(
    session_factory: async_sessionmaker[AsyncSession],
) -> tuple[str, str]:
    async with session_factory() as session:
        row = (
            await session.execute(
                text(
                    "select current_database(), current_schema(), "
                    "current_setting('transaction_isolation')"
                )
            )
        ).one()
    if row[2] != "read committed":
        raise RuntimeError("AI Call DB-only control plane requires READ COMMITTED")
    return str(row[0]), str(row[1])


async def start_runtime_control_lifecycle(
    settings: Any,
    session_factory: async_sessionmaker[AsyncSession],
) -> RuntimeControlService:
    await validate_db_only_runtime_database(session_factory)
    worker_id = build_worker_id(
        str(settings.AI_CALL_RUNTIME_INSTANCE_ID),
        uuid4(),
    )
    registry = RuntimeRegistry()
    service = RuntimeControlService(
        worker_id=worker_id,
        registry=registry,
        session_factory=session_factory,
        provider=select_runtime_provider(
            settings,
            session_factory=session_factory,
            registry=registry,
        ),
        capacity=int(settings.AI_CALL_RUNTIME_CAPACITY),
        cleanup_capacity=int(settings.AI_CALL_RUNTIME_CLEANUP_CAPACITY),
        worker_lease_ttl=timedelta(
            seconds=float(settings.AI_CALL_RUNTIME_WORKER_LEASE_SECONDS)
        ),
        owner_lease_ttl=timedelta(
            seconds=float(settings.AI_CALL_RUNTIME_OWNER_LEASE_SECONDS)
        ),
        fail_closed_margin_seconds=float(
            settings.AI_CALL_RUNTIME_FAIL_CLOSED_MARGIN_SECONDS
        ),
        scan_interval_seconds=float(settings.AI_CALL_RUNTIME_END_SCAN_INTERVAL_SECONDS),
        wakeup_listener=PostgresWakeupListener(session_factory.kw["bind"]),
        health=default_runtime_worker_health,
    )
    await service.start()
    return service


def build_livekit_runtime_provider(
    *,
    settings: Any,
    session_factory: async_sessionmaker[AsyncSession],
    registry: RuntimeRegistry,
):
    from app.services.ai_call.runtime_control.livekit_provider import (
        build_livekit_runtime_provider as build_provider,
    )

    return build_provider(
        settings=settings,
        session_factory=session_factory,
        registry=registry,
    )


def select_runtime_provider(
    settings: Any,
    *,
    session_factory: async_sessionmaker[AsyncSession],
    registry: RuntimeRegistry,
):
    mode = str(getattr(settings, "AI_CALL_RUNTIME_PROVIDER_MODE", "stub"))
    if mode == "stub":
        return DeterministicDbOnlyProviderStub()
    if mode != "livekit":
        raise RuntimeError(f"未知 AI Call Runtime Provider 模式: {mode}")
    environment = str(
        getattr(
            getattr(settings, "ENVIRONMENT", ""),
            "value",
            getattr(settings, "ENVIRONMENT", ""),
        )
    ).lower()
    entries = str(getattr(settings, "AI_CALL_OWNER_COMMAND_V1_ENTRIES", ""))
    allowed_callee = str(
        getattr(settings, "AI_CALL_OUTBOUND_LINPHONE_ALLOWED_CALLEE", "")
    ).strip()
    allowed_callee_prefixes = str(
        getattr(settings, "AI_CALL_SIP_ALLOWED_CALLEE_PREFIXES", "")
    ).strip()
    allowed = (
        bool(getattr(settings, "AI_CALL_RUNTIME_REAL_PROVIDER_ALLOWED", False))
        and bool(entries.strip())
        and bool(
            getattr(settings, "AI_CALL_OUTBOUND_LINPHONE_TEST_ENABLED", False)
        )
        and bool(allowed_callee or allowed_callee_prefixes)
        and str(getattr(settings, "DATABASE_TYPE", "")) == "postgres"
        and environment != "prod"
    )
    if not allowed:
        raise RuntimeError(
            "AI Call 真实 Provider 门禁未满足：仅允许隔离 PostgreSQL、"
            "显式 opt-in、非正式环境和被叫号码边界"
        )
    return build_livekit_runtime_provider(
        settings=settings,
        session_factory=session_factory,
        registry=registry,
    )


async def start_dispatcher_control_lifecycle(
    settings: Any,
    session_factory: async_sessionmaker[AsyncSession],
) -> DispatcherControlService:
    await validate_db_only_runtime_database(session_factory)
    service = DispatcherControlService(
        session_factory,
        scan_interval_seconds=float(
            settings.AI_CALL_RUNTIME_COMMAND_SCAN_INTERVAL_SECONDS
        ),
        wakeup_listener=PostgresWakeupListener(session_factory.kw["bind"]),
    )
    await service.start()
    return service


async def start_recovery_control_lifecycle(
    settings: Any,
    session_factory: async_sessionmaker[AsyncSession],
) -> RecoveryControlService:
    await validate_db_only_runtime_database(session_factory)
    service = RecoveryControlService(
        session_factory,
        scan_interval_seconds=float(settings.AI_CALL_RUNTIME_END_SCAN_INTERVAL_SECONDS),
    )
    await service.start()
    return service
