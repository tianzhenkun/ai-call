from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.services.ai_call.runtime_control.owner_repository import OwnerLease
from app.services.ai_call.runtime_control.recovery_service import (
    RecoveryControlService,
)

NOW = datetime(2026, 8, 3, 4, 0, tzinfo=timezone.utc)


async def _database_clock(_session) -> datetime:
    return NOW


class _Rows:
    def __init__(self, rows) -> None:
        self._rows = rows

    def all(self):
        return self._rows


class _ReadSession:
    def __init__(self) -> None:
        self.execute_count = 0

    async def execute(self, _statement):
        self.execute_count += 1
        if self.execute_count == 1:
            return _Rows([("tenant-a", "call-a")])
        return _Rows([])


class _Context:
    def __init__(self, session) -> None:
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        return None


class _SessionFactory:
    def __init__(self) -> None:
        self.read_session = _ReadSession()
        self.transaction_session = SimpleNamespace()

    def __call__(self):
        return _Context(self.read_session)

    def begin(self):
        return _Context(self.transaction_session)


class _RecoveryOwnerRepository:
    def __init__(self, _session) -> None:
        pass

    async def assign_cleanup_owner(self, tenant_id: str, call_id: str):
        assert (tenant_id, call_id) == ("tenant-a", "call-a")
        return OwnerLease(
            tenant_id=tenant_id,
            call_id=call_id,
            owner_id="recovery-a",
            fencing_token=8,
            lease_expires_at=NOW + timedelta(seconds=15),
            capacity_class="cleanup",
        )


class _DialogueRepository:
    def __init__(self) -> None:
        self.finalized = []

    async def finalize(self, fence, *, status: str, error: str | None = None):
        self.finalized.append((fence, status, error))
        return True


class _StartupRecovery:
    async def run_once(self) -> int:
        return 0


@pytest.mark.anyio
async def test_recovery_takeover_marks_pending_dialogue_uncertain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.services.ai_call.runtime_control import recovery_service

    monkeypatch.setattr(
        recovery_service,
        "RecoveryOwnerRepository",
        _RecoveryOwnerRepository,
    )
    session_factory = _SessionFactory()
    dialogue_repository = _DialogueRepository()
    service = RecoveryControlService(
        session_factory,
        database_clock=_database_clock,
        dialogue_repository_factory=lambda _session: dialogue_repository,
    )
    service._startup_reconcile = _StartupRecovery()

    assert await service.run_once() == 1
    assert len(dialogue_repository.finalized) == 1
    fence, status, error = dialogue_repository.finalized[0]
    assert fence.owner_id == "recovery-a"
    assert fence.fencing_token == 8
    assert status == "uncertain"
    assert error == "recovery_owner_takeover"
