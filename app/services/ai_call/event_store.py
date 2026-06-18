from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol

from app.utils.id_util import generate_snowflake_id


@dataclass(frozen=True, slots=True)
class AiCallEvent:
    event_id: str
    call_id: str
    type: str
    timestamp: datetime
    source: str
    payload: dict[str, Any] = field(default_factory=dict)


class AiCallEventListener(Protocol):
    def __call__(self, event: AiCallEvent) -> None: ...


class InMemoryEventStore:
    """Phase A 运行态事件存储；只服务延迟验证，不承诺进程重启后的追溯。"""

    def __init__(self) -> None:
        self._events: list[AiCallEvent] = []
        self._listeners: list[AiCallEventListener] = []

    def add_listener(self, listener: AiCallEventListener) -> None:
        if listener not in self._listeners:
            self._listeners.append(listener)

    def remove_listener(self, listener: AiCallEventListener) -> None:
        if listener in self._listeners:
            self._listeners.remove(listener)

    def append(
        self,
        call_id: str,
        type: str,
        source: str,
        payload: dict[str, Any] | None = None,
        timestamp: datetime | None = None,
    ) -> AiCallEvent:
        event = AiCallEvent(
            event_id=f"evt_{generate_snowflake_id()}",
            call_id=call_id,
            type=type,
            timestamp=timestamp or datetime.now(timezone.utc),
            source=source,
            payload=payload or {},
        )
        self._events.append(event)
        for listener in tuple(self._listeners):
            try:
                listener(event)
            except Exception:
                # 事件旁路不能反向影响实时通话路径。
                continue
        return event

    def list(
        self,
        call_id: str,
        limit: int = 200,
        after_event_id: str | None = None,
    ) -> list[AiCallEvent]:
        safe_limit = max(1, min(limit, 1000))
        rows = [event for event in self._events if event.call_id == call_id]
        if after_event_id:
            seen = False
            filtered: list[AiCallEvent] = []
            for event in rows:
                if seen:
                    filtered.append(event)
                elif event.event_id == after_event_id:
                    seen = True
            rows = filtered
        return rows[:safe_limit]

    def list_all(self, call_id: str) -> list[AiCallEvent]:
        return [event for event in self._events if event.call_id == call_id]
