from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class RuntimeTaskState(StrEnum):
    NOT_CONFIGURED = "not_configured"
    STARTING = "starting"
    RUNNING = "running"
    FAILED = "failed"
    STOPPED = "stopped"


@dataclass(frozen=True, slots=True)
class RuntimeHealthSnapshot:
    state: RuntimeTaskState
    worker_id: str | None
    error_code: str | None


class RuntimeWorkerHealth:
    def __init__(self) -> None:
        self._snapshot = RuntimeHealthSnapshot(
            state=RuntimeTaskState.NOT_CONFIGURED,
            worker_id=None,
            error_code=None,
        )

    def snapshot(self) -> RuntimeHealthSnapshot:
        return self._snapshot

    def mark_starting(self, worker_id: str) -> None:
        self._set(RuntimeTaskState.STARTING, worker_id)

    def mark_running(self, worker_id: str) -> None:
        self._set(RuntimeTaskState.RUNNING, worker_id)

    def mark_failed(self, worker_id: str, error_code: str) -> None:
        self._set(RuntimeTaskState.FAILED, worker_id, error_code)

    def mark_stopped(self, worker_id: str) -> None:
        self._set(RuntimeTaskState.STOPPED, worker_id)

    def _set(
        self,
        state: RuntimeTaskState,
        worker_id: str,
        error_code: str | None = None,
    ) -> None:
        self._snapshot = RuntimeHealthSnapshot(state, worker_id, error_code)


default_runtime_worker_health = RuntimeWorkerHealth()
