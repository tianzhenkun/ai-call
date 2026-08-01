from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.services.ai_call.runtime_control import command_repository
from app.services.ai_call.runtime_control.command_repository import (
    CommandDecision,
    EndCallIntent,
    RuntimeCommandRepository,
    StartCallIntent,
    canonical_request_fingerprint,
    end_call_request_fingerprint,
    start_call_request_fingerprint,
)
from app.services.ai_call.runtime_control.types import CommandStatus


def test_canonical_request_fingerprint_is_order_independent() -> None:
    left = canonical_request_fingerprint(
        {"tenant_id": "tenant-a", "payload": {"voice": "v1", "speed": 1}}
    )
    right = canonical_request_fingerprint(
        {"payload": {"speed": 1, "voice": "v1"}, "tenant_id": "tenant-a"}
    )

    assert left == right
    assert len(left) == 64


def test_start_fingerprint_excludes_server_generated_identifiers() -> None:
    request = StartCallIntent(
        tenant_id="tenant-a",
        entry_type="web",
        idempotency_key="start:business-1",
        payload={"business_id": "business-1", "voice": "v1"},
    )

    expected = canonical_request_fingerprint(
        {
            "command_type": "START_CALL",
            "entry_type": "web",
            "payload": {"business_id": "business-1", "voice": "v1"},
            "tenant_id": "tenant-a",
        }
    )

    assert start_call_request_fingerprint(request) == expected


def test_start_fingerprint_excludes_server_allocation_policy() -> None:
    first = StartCallIntent(
        tenant_id="tenant-a",
        entry_type="web",
        idempotency_key="start:business-1",
        payload={"business_id": "business-1", "voice": "v1"},
        allocation_timeout_seconds=30.0,
    )
    second = StartCallIntent(
        tenant_id="tenant-a",
        entry_type="web",
        idempotency_key="start:business-1",
        payload={"business_id": "business-1", "voice": "v1"},
        allocation_timeout_seconds=60.0,
    )

    assert start_call_request_fingerprint(first) == start_call_request_fingerprint(second)


class _NestedTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, _exc_type, _exc, _traceback) -> None:
        return None


class _FakeSession:
    def __init__(self) -> None:
        self.rows: list[object] = []
        self._bind = SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))

    async def scalar(self, _statement):
        return None

    def get_bind(self) -> object:
        return self._bind

    def begin_nested(self) -> _NestedTransaction:
        return _NestedTransaction()

    def add_all(self, rows) -> None:
        self.rows.extend(rows)

    def add(self, row: object) -> None:
        self.rows.append(row)

    async def flush(self) -> None:
        return None


@pytest.mark.anyio
async def test_start_deadline_uses_database_time_and_server_timeout() -> None:
    now = datetime(2026, 8, 1, 12, tzinfo=timezone.utc)
    ids = iter((101, 102, 103))
    session = _FakeSession()
    repository = RuntimeCommandRepository(
        session,
        id_generator=lambda: next(ids),
        database_clock=lambda _session: _constant_time(now),
    )

    await repository.create_start_call(
        StartCallIntent(
            tenant_id="tenant-a",
            entry_type="web",
            idempotency_key="start:database-deadline",
            payload={"voice": "v1"},
            allocation_timeout_seconds=30.0,
        )
    )

    command = session.rows[1]
    assert command.allocation_deadline_at == now + timedelta(seconds=30)


@pytest.mark.anyio
async def test_new_start_publishes_one_transactional_wakeup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 8, 1, 12, tzinfo=timezone.utc)
    ids = iter((101, 102, 103))
    session = _FakeSession()
    published: list[object] = []

    async def _publish(target: object) -> None:
        published.append(target)

    monkeypatch.setattr(command_repository, "publish_control_wakeup", _publish)
    repository = RuntimeCommandRepository(
        session,
        id_generator=lambda: next(ids),
        database_clock=lambda _session: _constant_time(now),
    )

    await repository.create_start_call(
        StartCallIntent(
            tenant_id="tenant-a",
            entry_type="web",
            idempotency_key="start:wakeup",
            payload={"voice": "v1"},
        )
    )

    assert published == [session]


@pytest.mark.anyio
async def test_idempotent_start_does_not_publish_another_wakeup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 8, 1, 12, tzinfo=timezone.utc)
    ids = iter((101, 102, 103))
    session = _FakeSession()
    published: list[object] = []

    async def _publish(target: object) -> None:
        published.append(target)

    monkeypatch.setattr(command_repository, "publish_control_wakeup", _publish)
    repository = RuntimeCommandRepository(
        session,
        id_generator=lambda: next(ids),
        database_clock=lambda _session: _constant_time(now),
    )
    request = StartCallIntent(
        tenant_id="tenant-a",
        entry_type="web",
        idempotency_key="start:wakeup-idempotent",
        payload={"voice": "v1"},
    )
    await repository.create_start_call(request)
    created_command = session.rows[1]

    async def _find_existing(**_kwargs):
        return created_command

    monkeypatch.setattr(repository, "_find_by_idempotency", _find_existing)
    await repository.create_start_call(request)

    assert published == [session]


@pytest.mark.anyio
async def test_end_barrier_publishes_one_transactional_wakeup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 8, 1, 12, tzinfo=timezone.utc)
    session = _FakeSession()
    published: list[object] = []
    record = SimpleNamespace(
        runtime_control_mode="owner_command_v1",
        next_command_seq=2,
        runtime_fencing_token=7,
        runtime_owner_id="worker-a",
        terminal_requested_at=None,
        status="running",
        resource_cleanup_status="not_started",
        end_reason=None,
        last_applied_command_seq=1,
    )

    async def _publish(target: object) -> None:
        published.append(target)

    async def _lock_record(_tenant_id: str, _call_id: str):
        return record

    async def _lock_end_command(_tenant_id: str, _call_id: str):
        return None

    async def _preempt_ordinary_commands(**_kwargs) -> None:
        return None

    async def _append_end_evidence(**_kwargs):
        return SimpleNamespace(id=301)

    monkeypatch.setattr(command_repository, "publish_control_wakeup", _publish)
    repository = RuntimeCommandRepository(
        session,
        id_generator=iter((201,)).__next__,
        database_clock=lambda _session: _constant_time(now),
    )
    monkeypatch.setattr(repository, "_lock_record", _lock_record)
    monkeypatch.setattr(repository, "_lock_end_command", _lock_end_command)
    monkeypatch.setattr(
        repository,
        "_preempt_ordinary_commands",
        _preempt_ordinary_commands,
    )
    monkeypatch.setattr(repository, "_append_end_evidence", _append_end_evidence)

    await repository.request_end(
        EndCallIntent(
            tenant_id="tenant-a",
            call_id="call-1",
            source="web",
            end_reason="user_requested",
            dedupe_key="end:wakeup",
        )
    )

    assert published == [session]
    assert record.terminal_requested_at == now


async def _constant_time(value: datetime) -> datetime:
    return value


def test_end_fingerprint_ignores_source_reason_and_provider_event() -> None:
    first = EndCallIntent(
        tenant_id="tenant-a",
        call_id="call-1",
        source="customer_sip",
        end_reason="customer_hangup",
        dedupe_key="livekit:cluster-a:event-1",
        provider="livekit",
        provider_namespace="cluster-a",
        provider_event_id="event-1",
    )
    second = EndCallIntent(
        tenant_id="tenant-a",
        call_id="call-1",
        source="agent",
        end_reason="agent_hangup",
        dedupe_key="agent:call-1:hangup-1",
    )

    assert end_call_request_fingerprint(first) == end_call_request_fingerprint(second)
    assert end_call_request_fingerprint(first) == canonical_request_fingerprint(
        {
            "call_id": "call-1",
            "command_type": "END_CALL",
            "tenant_id": "tenant-a",
        }
    )


def test_command_claim_and_completion_api_is_explicit() -> None:
    assert callable(RuntimeCommandRepository.claim_next_for_owner)
    assert callable(RuntimeCommandRepository.claim_pending_end)
    assert callable(RuntimeCommandRepository.complete)
    assert CommandDecision(status=CommandStatus.SUCCEEDED).status == "SUCCEEDED"
