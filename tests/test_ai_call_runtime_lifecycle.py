from __future__ import annotations

import asyncio

import pytest

from app.services.ai_call.runtime_control.lifecycle import AiCallRuntimeTimingPolicy
from app.services.ai_call.runtime_control.owner_repository import OwnerFailClosedWatchdog
from app.services.ai_call.runtime_control.runtime_service import (
    RuntimeControlService,
    RuntimeRegistry,
    _FailClosedProvider,
)


def test_runtime_services_do_not_share_local_registry() -> None:
    runtime_a = RuntimeControlService(
        worker_id="pod-a:12345678-1234-5678-1234-567812345678",
        registry=RuntimeRegistry(),
        session_factory=None,
        provider=None,
    )
    runtime_b = RuntimeControlService(
        worker_id="pod-b:87654321-4321-8765-4321-876543218765",
        registry=RuntimeRegistry(),
        session_factory=None,
        provider=None,
    )

    assert runtime_a.registry is not runtime_b.registry


def test_db_only_timing_policy_freezes_scan_intervals() -> None:
    policy = AiCallRuntimeTimingPolicy()

    assert policy.end_scan_interval_seconds == 0.5
    assert policy.command_scan_interval_seconds == 1.0


@pytest.mark.anyio
async def test_provider_timeout_before_owner_deadline_does_not_trip_watchdog() -> None:
    class TimeoutProvider:
        async def apply(self, effect):
            raise TimeoutError("provider timeout")

    fail_closed = asyncio.Event()

    async def mark_fail_closed() -> None:
        fail_closed.set()

    watchdog = OwnerFailClosedWatchdog(
        lease_ttl_seconds=15,
        safety_margin_seconds=3,
    )
    watchdog.observe_renewal()

    with pytest.raises(TimeoutError, match="provider timeout"):
        await _FailClosedProvider(
            TimeoutProvider(), watchdog, mark_fail_closed
        ).apply(object())

    assert watchdog.creation_allowed() is True
    assert fail_closed.is_set() is False
