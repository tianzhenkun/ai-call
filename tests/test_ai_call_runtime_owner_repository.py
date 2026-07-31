from __future__ import annotations

from uuid import UUID

import pytest

from app.services.ai_call.runtime_control.owner_repository import (
    OwnerFailClosedWatchdog,
    RuntimeOwnerRepository,
    build_worker_id,
)


class _MonotonicClock:
    def __init__(self) -> None:
        self.value = 100.0

    def __call__(self) -> float:
        return self.value


def test_worker_id_contains_deployment_identity_and_startup_uuid() -> None:
    startup_id = UUID("12345678-1234-5678-1234-567812345678")

    assert (
        build_worker_id("runtime-a", startup_id)
        == "runtime-a:12345678-1234-5678-1234-567812345678"
    )

    with pytest.raises(ValueError):
        build_worker_id("", startup_id)


def test_runtime_repository_exposes_no_owner_claim_or_takeover_api() -> None:
    forbidden_names = {
        "claim_unowned_record",
        "assign_initial_owner",
        "assign_cleanup_owner",
        "takeover",
    }

    assert forbidden_names.isdisjoint(dir(RuntimeOwnerRepository))


def test_fail_closed_watchdog_uses_renewal_start_and_monotonic_deadline() -> None:
    clock = _MonotonicClock()
    watchdog = OwnerFailClosedWatchdog(
        lease_ttl_seconds=15,
        safety_margin_seconds=3,
        monotonic_clock=clock,
    )

    assert watchdog.creation_allowed() is False
    assert watchdog.must_stop_media() is True

    renewal_started = clock()
    clock.value = 105.0
    watchdog.observe_renewal(renewal_started_monotonic=renewal_started)

    clock.value = 111.999
    assert watchdog.creation_allowed() is True
    assert watchdog.must_stop_media() is False

    clock.value = 112.0
    assert watchdog.creation_allowed() is False
    assert watchdog.must_stop_media() is True


def test_failed_renewal_does_not_extend_watchdog_deadline() -> None:
    clock = _MonotonicClock()
    watchdog = OwnerFailClosedWatchdog(
        lease_ttl_seconds=15,
        safety_margin_seconds=3,
        monotonic_clock=clock,
    )
    watchdog.observe_renewal(renewal_started_monotonic=clock())

    clock.value = 111.0
    assert watchdog.creation_allowed() is True

    # 续租失败时调用方不会 observe；数据库超时不能延长本地硬截止。
    clock.value = 112.0
    assert watchdog.must_stop_media() is True


def test_expired_watchdog_cannot_be_revived_by_a_late_renewal() -> None:
    clock = _MonotonicClock()
    watchdog = OwnerFailClosedWatchdog(
        lease_ttl_seconds=15,
        safety_margin_seconds=3,
        monotonic_clock=clock,
    )
    watchdog.observe_renewal(renewal_started_monotonic=clock())

    clock.value = 112.0
    assert watchdog.must_stop_media() is True

    watchdog.observe_renewal(renewal_started_monotonic=clock())

    assert watchdog.creation_allowed() is False
    assert watchdog.must_stop_media() is True


def test_late_renewal_cannot_revive_watchdog_without_an_intermediate_poll() -> None:
    clock = _MonotonicClock()
    watchdog = OwnerFailClosedWatchdog(
        lease_ttl_seconds=15,
        safety_margin_seconds=3,
        monotonic_clock=clock,
    )
    watchdog.observe_renewal(renewal_started_monotonic=clock())

    clock.value = 112.0
    watchdog.observe_renewal(renewal_started_monotonic=clock())

    assert watchdog.creation_allowed() is False
    assert watchdog.must_stop_media() is True
