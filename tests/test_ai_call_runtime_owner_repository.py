from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest

from app.services.ai_call.runtime_control import owner_repository
from app.services.ai_call.runtime_control.owner_repository import (
    DispatcherOwnerRepository,
    OutboundStartRefs,
    OwnerFailClosedWatchdog,
    RuntimeOwnerRepository,
    build_worker_id,
    parse_outbound_start_refs,
)
from app.services.ai_call.runtime_control.types import CommandStatus


class _ScalarRows:
    def __init__(self, rows: list[str]) -> None:
        self._rows = rows

    def all(self) -> list[str]:
        return self._rows


class _DispatcherSession:
    def __init__(self, candidate_ids: list[str]) -> None:
        self.candidate_ids = candidate_ids
        self.flush_count = 0

    async def scalars(self, _statement: object) -> _ScalarRows:
        return _ScalarRows(self.candidate_ids)

    async def flush(self) -> None:
        self.flush_count += 1


async def _constant_time(value: datetime) -> datetime:
    return value


class _MonotonicClock:
    def __init__(self) -> None:
        self.value = 100.0

    def __call__(self) -> float:
        return self.value


def test_worker_id_contains_deployment_identity_and_startup_uuid() -> None:
    startup_id = UUID("12345678-1234-5678-1234-567812345678")

    assert (
        build_worker_id("runtime-a", startup_id) == "runtime-a:12345678-1234-5678-1234-567812345678"
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


def test_outbound_start_refs_accept_only_canonical_positive_identifiers() -> None:
    payload = {
        "attempt_id": "13",
        "attempt_no": 1,
        "line_code": "provider-a",
        "line_id": "14",
        "prompt_profile_id": "prompt-1",
        "scene_code": "intro_contract",
        "target_id": "12",
        "task_id": "11",
        "voice": "Tina",
    }

    assert parse_outbound_start_refs(json.dumps(payload)) == OutboundStartRefs(
        task_id=11,
        target_id=12,
        attempt_id=13,
        line_id=14,
    )

    invalid_payloads = [
        {**payload, "attempt_id": True},
        {**payload, "attempt_id": 13},
        {**payload, "attempt_id": "013"},
        {**payload, "attempt_no": True},
        {**payload, "attempt_no": 0},
        {**payload, "line_id": "0"},
        {key: value for key, value in payload.items() if key != "target_id"},
        {**payload, "unexpected": "value"},
    ]
    assert all(
        parse_outbound_start_refs(json.dumps(invalid)) is None for invalid in invalid_payloads
    )


@pytest.mark.anyio
async def test_initial_owner_assignment_publishes_one_transactional_wakeup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 8, 1, 12, tzinfo=timezone.utc)
    session = _DispatcherSession(["worker-a"])
    published: list[object] = []
    record = SimpleNamespace(
        tenant_id="tenant-a",
        call_id="call-1",
        runtime_control_mode="owner_command_v1",
        runtime_owner_id=None,
        runtime_fencing_token=0,
        runtime_lease_expires_at=None,
        runtime_heartbeat_at=None,
        runtime_capacity_class="none",
        terminal_requested_at=None,
    )
    worker = SimpleNamespace(
        worker_id="worker-a",
        status="READY",
        lease_expires_at=now + timedelta(seconds=30),
        active_call_count=0,
        capacity=2,
        updated_at=now,
    )
    command = SimpleNamespace(
        status=CommandStatus.PENDING,
        payload_json="{}",
        target_owner_id=None,
        expected_fencing_token=None,
        updated_at=now,
    )

    async def _publish(target: object) -> None:
        published.append(target)

    async def _lock_record(_tenant_id: str, _call_id: str):
        return record

    async def _has_provider_resource(_tenant_id: str, _call_id: str) -> bool:
        return False

    async def _read_start_command(_tenant_id: str, _call_id: str):
        return command

    async def _lock_worker(_worker_id: str):
        return worker

    async def _lock_command(
        _tenant_id: str,
        _call_id: str,
        _command_type: str,
    ):
        return command

    monkeypatch.setattr(owner_repository, "publish_control_wakeup", _publish)
    repository = DispatcherOwnerRepository(
        session,
        lease_ttl=timedelta(seconds=15),
        database_clock=lambda _session: _constant_time(now),
    )
    monkeypatch.setattr(repository, "_lock_record", _lock_record)
    monkeypatch.setattr(repository, "_has_provider_resource", _has_provider_resource)
    monkeypatch.setattr(repository, "_read_start_command", _read_start_command)
    monkeypatch.setattr(repository, "_lock_worker", _lock_worker)
    monkeypatch.setattr(repository, "_lock_command", _lock_command)

    lease = await repository.assign_initial_owner("tenant-a", "call-1")

    assert lease is not None
    assert lease.owner_id == "worker-a"
    assert record.runtime_fencing_token == 1
    assert worker.active_call_count == 1
    assert session.flush_count == 1
    assert published == [session]


@pytest.mark.anyio
async def test_initial_owner_capacity_miss_does_not_publish_wakeup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 8, 1, 12, tzinfo=timezone.utc)
    session = _DispatcherSession([])
    published: list[object] = []

    async def _publish(target: object) -> None:
        published.append(target)

    monkeypatch.setattr(owner_repository, "publish_control_wakeup", _publish)
    repository = DispatcherOwnerRepository(
        session,
        database_clock=lambda _session: _constant_time(now),
    )

    assert await repository.assign_initial_owner("tenant-a", "call-1") is None
    assert session.flush_count == 0
    assert published == []


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
