from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Protocol
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.model import AiCallRecordModel
from app.core.logger import log
from app.services.ai_call.runtime_control.command_repository import (
    CommandDecision,
    RuntimeCommandRepository,
)
from app.services.ai_call.runtime_control.effect_repository import (
    EffectSpec,
    ProviderObservation,
    ProviderObservationKind,
    RuntimeEffectRepository,
)
from app.services.ai_call.runtime_control.handlers import EndCallHandler, StartCallHandler
from app.services.ai_call.runtime_control.models import AiCallRuntimeWorkerModel
from app.services.ai_call.runtime_control.owner_repository import (
    OwnerFailClosedWatchdog,
    OwnerLease,
    RuntimeOwnerRepository,
    WorkerLease,
    WorkerRegistration,
    WorkerRegistryRepository,
)
from app.services.ai_call.runtime_control.postgres_wakeup import WakeupListener
from app.services.ai_call.runtime_control.timing import read_database_time
from app.services.ai_call.runtime_control.types import CommandStatus


class RuntimeProvider(Protocol):
    async def apply(self, effect: object) -> ProviderObservation: ...


class RuntimeLocalHandle(Protocol):
    async def fail_closed(self) -> None: ...


@dataclass(slots=True)
class RuntimeRegistry:
    owner_watchdogs: dict[str, OwnerFailClosedWatchdog] = field(default_factory=dict)
    owner_fencing_tokens: dict[str, int] = field(default_factory=dict)
    fail_closed_timers: dict[str, asyncio.TimerHandle] = field(default_factory=dict)
    local_handles: dict[str, RuntimeLocalHandle] = field(default_factory=dict)


class _OwnerFailClosed(RuntimeError):
    pass


class _FailClosedProvider:
    def __init__(
        self,
        delegate: RuntimeProvider,
        watchdog: OwnerFailClosedWatchdog,
        fail_closed: Callable[[], Awaitable[None]],
    ) -> None:
        self._delegate = delegate
        self._watchdog = watchdog
        self._fail_closed = fail_closed

    async def apply(self, effect: object) -> ProviderObservation:
        remaining = self._watchdog.seconds_until_hard_deadline()
        if remaining <= 0:
            await self._fail_closed()
            raise _OwnerFailClosed("owner fail-closed deadline reached")
        try:
            async with asyncio.timeout(remaining):
                observation = await self._delegate.apply(effect)
        except TimeoutError as exc:
            if self._watchdog.creation_allowed():
                raise
            self._watchdog.trip()
            await self._fail_closed()
            raise _OwnerFailClosed("owner fail-closed deadline reached") from exc
        if self._watchdog.must_stop_media():
            await self._fail_closed()
            raise _OwnerFailClosed("owner fail-closed deadline reached")
        return observation


class RuntimeControlService:
    def __init__(
        self,
        *,
        worker_id: str,
        registry: RuntimeRegistry,
        session_factory: async_sessionmaker[AsyncSession] | None,
        provider: RuntimeProvider | None,
        capacity: int = 20,
        cleanup_capacity: int = 4,
        worker_lease_ttl: timedelta = timedelta(seconds=30),
        owner_lease_ttl: timedelta = timedelta(seconds=15),
        fail_closed_margin_seconds: float = 3.0,
        batch_size: int = 32,
        scan_interval_seconds: float = 0.5,
        wakeup_listener: WakeupListener | None = None,
    ) -> None:
        self.worker_id = worker_id
        self.registry = registry
        self._session_factory = session_factory
        self._provider = provider
        self._capacity = capacity
        self._cleanup_capacity = cleanup_capacity
        self._worker_lease_ttl = worker_lease_ttl
        self._owner_lease_ttl = owner_lease_ttl
        self._fail_closed_margin_seconds = fail_closed_margin_seconds
        self._batch_size = batch_size
        self._scan_interval_seconds = scan_interval_seconds
        self._wakeup_listener = wakeup_listener
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        session_factory, _provider = self._require_dependencies()
        deployment_id, startup_id = _parse_worker_id(self.worker_id)
        async with session_factory.begin() as session:
            lease = await WorkerRegistryRepository(
                session, lease_ttl=self._worker_lease_ttl
            ).register(
                WorkerRegistration(
                    deployment_instance_id=deployment_id,
                    startup_id=startup_id,
                    capacity=self._capacity,
                    cleanup_capacity=self._cleanup_capacity,
                )
            )
        if lease.worker_id != self.worker_id:
            raise RuntimeError("registered worker identity does not match service identity")
        self._stop_event.clear()
        if self._wakeup_listener is not None:
            try:
                await self._wakeup_listener.start()
            except Exception as exc:
                log.error(f"AI Call Runtime PostgreSQL 唤醒监听启动失败: {exc!s}")
        self._task = asyncio.create_task(
            self._run_loop(), name=f"ai-call-runtime:{self.worker_id}"
        )

    async def stop(self) -> None:
        self._stop_event.set()
        task = self._task
        self._task = None
        if task is not None:
            await task
        if self._wakeup_listener is not None:
            try:
                await self._wakeup_listener.stop()
            except Exception as exc:
                log.error(f"AI Call Runtime PostgreSQL 唤醒监听关闭失败: {exc!s}")
        for timer in self.registry.fail_closed_timers.values():
            timer.cancel()
        self.registry.fail_closed_timers.clear()
        for call_id in tuple(self.registry.local_handles):
            await self._fail_closed_local_handle(call_id)
        if self._session_factory is not None:
            async with self._session_factory.begin() as session:
                await session.execute(
                    update(AiCallRuntimeWorkerModel)
                    .where(AiCallRuntimeWorkerModel.worker_id == self.worker_id)
                    .values(status="DRAINING")
                )

    async def run_once(self) -> int:
        session_factory, provider = self._require_dependencies()
        async with session_factory.begin() as session:
            worker_alive = await WorkerRegistryRepository(
                session,
                lease_ttl=self._worker_lease_ttl,
            ).heartbeat(
                WorkerLease(
                    worker_id=self.worker_id,
                    lease_expires_at=await read_database_time(session),
                )
            )
        if not worker_alive:
            return 0
        async with session_factory() as session:
            now = await read_database_time(session)
            rows = (
                await session.execute(
                    select(
                        AiCallRecordModel.tenant_id,
                        AiCallRecordModel.call_id,
                        AiCallRecordModel.runtime_fencing_token,
                        AiCallRecordModel.runtime_lease_expires_at,
                        AiCallRecordModel.runtime_capacity_class,
                    )
                    .where(
                        AiCallRecordModel.runtime_owner_id == self.worker_id,
                        AiCallRecordModel.runtime_lease_expires_at > now,
                    )
                    .order_by(AiCallRecordModel.id)
                    .limit(self._batch_size)
                )
            ).all()

        processed = 0
        for row in rows:
            lease = OwnerLease(
                tenant_id=row.tenant_id,
                call_id=row.call_id,
                owner_id=self.worker_id,
                fencing_token=row.runtime_fencing_token,
                lease_expires_at=row.runtime_lease_expires_at,
                capacity_class=row.runtime_capacity_class,
            )
            watchdog = self.registry.owner_watchdogs.get(row.call_id)
            if (
                watchdog is None
                or self.registry.owner_fencing_tokens.get(row.call_id)
                != row.runtime_fencing_token
            ):
                previous_timer = self.registry.fail_closed_timers.pop(
                    row.call_id, None
                )
                if previous_timer is not None:
                    previous_timer.cancel()
                watchdog = OwnerFailClosedWatchdog(
                    lease_ttl_seconds=self._owner_lease_ttl.total_seconds(),
                    safety_margin_seconds=self._fail_closed_margin_seconds,
                )
                self.registry.owner_watchdogs[row.call_id] = watchdog
                self.registry.owner_fencing_tokens[row.call_id] = (
                    row.runtime_fencing_token
                )
            renewal_started = time.monotonic()
            async with session_factory.begin() as session:
                renewed_lease = await RuntimeOwnerRepository(
                    session,
                    lease_ttl=self._owner_lease_ttl,
                ).renew(lease)
            if renewed_lease is None:
                await self._trip_owner(row.call_id, watchdog)
                continue
            watchdog.observe_renewal(
                renewal_started_monotonic=renewal_started
            )
            self._arm_fail_closed_timer(row.call_id, watchdog)
            if await self._process_owned_call(
                session_factory, provider, renewed_lease, watchdog
            ):
                processed += 1
        return processed

    async def _process_owned_call(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        provider: RuntimeProvider,
        lease: OwnerLease,
        watchdog: OwnerFailClosedWatchdog,
    ) -> bool:
        if watchdog.must_stop_media():
            await self._trip_owner(lease.call_id, watchdog)
            return False
        guarded_provider = _FailClosedProvider(
            provider,
            watchdog,
            lambda: self._trip_owner(lease.call_id, watchdog),
        )
        async with session_factory.begin() as session:
            command_repository = RuntimeCommandRepository(session)
            end_claim = await command_repository.claim_pending_end(lease)
        if end_claim is not None:
            try:
                result = await EndCallHandler(
                    session_factory, guarded_provider
                ).handle(end_claim, lease)
            except _OwnerFailClosed:
                return False
            if result.resource_cleanup_status in {"clean", "attention_required"}:
                await self._clear_owner_tracking(lease.call_id)
            return True

        async with session_factory.begin() as session:
            effect_claim = await RuntimeEffectRepository(session).claim_next(lease)
        if effect_claim is not None:
            try:
                observation = await guarded_provider.apply(effect_claim)
            except asyncio.CancelledError:
                raise
            except _OwnerFailClosed:
                return False
            except Exception as exc:
                observation = ProviderObservation(
                    kind=ProviderObservationKind.UNCERTAIN,
                    error_message=str(exc),
                )
            async with session_factory.begin() as session:
                submitted = await RuntimeEffectRepository(session).submit(
                    effect_claim, observation
                )
            if submitted:
                async with session_factory.begin() as session:
                    clean = await RuntimeEffectRepository(
                        session
                    ).mark_cleanup_clean(lease)
                if clean:
                    await self._clear_owner_tracking(lease.call_id)
            return True

        async with session_factory.begin() as session:
            clean = await RuntimeEffectRepository(session).mark_cleanup_clean(lease)
        if clean:
            await self._clear_owner_tracking(lease.call_id)
            return True

        async with session_factory.begin() as session:
            command_claim = await RuntimeCommandRepository(
                session
            ).claim_next_for_owner(lease)
        if command_claim is None:
            return False
        if command_claim.command_type == "START_CALL":
            if not watchdog.creation_allowed():
                await self._trip_owner(lease.call_id, watchdog)
                return False
            specs = _default_start_specs(
                command_claim.call_id,
                lease,
                self.worker_id,
                entry_type=command_claim.entry_type,
            )
            try:
                await StartCallHandler(session_factory, guarded_provider).handle(
                    command_claim,
                    lease,
                    specs,
                )
            except _OwnerFailClosed:
                return False
        else:
            async with session_factory.begin() as session:
                await RuntimeCommandRepository(session).complete(
                    command_claim,
                    CommandDecision(status=CommandStatus.SUCCEEDED),
                )
        return True

    def _arm_fail_closed_timer(
        self,
        call_id: str,
        watchdog: OwnerFailClosedWatchdog,
    ) -> None:
        previous = self.registry.fail_closed_timers.pop(call_id, None)
        if previous is not None:
            previous.cancel()
        remaining = watchdog.seconds_until_hard_deadline()

        def trigger() -> None:
            self.registry.fail_closed_timers.pop(call_id, None)
            asyncio.create_task(
                self._trip_owner(call_id, watchdog),
                name=f"ai-call-fail-closed:{call_id}",
            )

        self.registry.fail_closed_timers[call_id] = (
            asyncio.get_running_loop().call_later(remaining, trigger)
        )

    async def _trip_owner(
        self,
        call_id: str,
        watchdog: OwnerFailClosedWatchdog,
    ) -> None:
        if self.registry.owner_watchdogs.get(call_id) is not watchdog:
            return
        watchdog.trip()
        timer = self.registry.fail_closed_timers.pop(call_id, None)
        if timer is not None:
            timer.cancel()
        await self._fail_closed_local_handle(call_id)

    async def _fail_closed_local_handle(self, call_id: str) -> None:
        handle = self.registry.local_handles.pop(call_id, None)
        if handle is None:
            return
        try:
            await handle.fail_closed()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error(f"AI Call 本地媒体 fail-closed 失败 call_id={call_id}: {exc!s}")

    async def _clear_owner_tracking(self, call_id: str) -> None:
        timer = self.registry.fail_closed_timers.pop(call_id, None)
        if timer is not None:
            timer.cancel()
        self.registry.owner_watchdogs.pop(call_id, None)
        self.registry.owner_fencing_tokens.pop(call_id, None)
        await self._fail_closed_local_handle(call_id)

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error(f"AI Call Runtime DB-only 扫描失败: {exc!s}")
            await self._wait_for_next_scan()

    async def _wait_for_next_scan(self) -> None:
        if self._wakeup_listener is not None:
            try:
                await self._wakeup_listener.wait(
                    timeout_seconds=self._scan_interval_seconds,
                    stop_event=self._stop_event,
                )
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error(f"AI Call Runtime PostgreSQL 唤醒等待失败: {exc!s}")
        try:
            await asyncio.wait_for(
                self._stop_event.wait(), timeout=self._scan_interval_seconds
            )
        except TimeoutError:
            return

    def _require_dependencies(
        self,
    ) -> tuple[async_sessionmaker[AsyncSession], RuntimeProvider]:
        if self._session_factory is None or self._provider is None:
            raise RuntimeError("RuntimeControlService dependencies are not configured")
        return self._session_factory, self._provider


def _parse_worker_id(worker_id: str) -> tuple[str, UUID]:
    deployment_id, separator, startup_id = worker_id.rpartition(":")
    if not separator or not deployment_id:
        raise ValueError("worker_id must contain deployment identity and startup UUID")
    return deployment_id, UUID(startup_id)


def _default_start_specs(
    call_id: str,
    lease: OwnerLease,
    worker_id: str,
    *,
    entry_type: str,
) -> list[EffectSpec]:
    if entry_type not in {"web", "direct_sip"}:
        raise ValueError(f"unsupported owner command entry: {entry_type}")
    namespace = f"stub:{worker_id}"
    specs = [
        EffectSpec(
            effect_type="CREATE_ROOM",
            idempotency_key=f"start:{call_id}:create-room:g{lease.fencing_token}",
            provider_namespace=namespace,
            provider_idempotency_key=f"room:{call_id}:g{lease.fencing_token}",
            resource_key=f"room:{call_id}:g{lease.fencing_token}",
            resource_generation=lease.fencing_token,
        ),
        EffectSpec(
            effect_type="ATTACH_AGENT_PARTICIPANT",
            idempotency_key=f"start:{call_id}:attach-agent:g{lease.fencing_token}",
            provider_namespace=namespace,
            provider_idempotency_key=f"agent:{call_id}:g{lease.fencing_token}",
            resource_key=f"agent:{call_id}:g{lease.fencing_token}",
            resource_generation=lease.fencing_token,
        ),
    ]
    if entry_type == "direct_sip":
        specs.append(
            EffectSpec(
                effect_type="CREATE_SIP_PARTICIPANT",
                idempotency_key=(
                    f"start:{call_id}:create-sip-participant:"
                    f"g{lease.fencing_token}"
                ),
                provider_namespace=namespace,
                provider_idempotency_key=(
                    f"sip:{call_id}:g{lease.fencing_token}"
                ),
                resource_key=f"sip:{call_id}:g{lease.fencing_token}",
                resource_generation=lease.fencing_token,
            )
        )
    return specs
