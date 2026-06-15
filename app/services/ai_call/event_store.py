from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.utils.id_util import generate_snowflake_id


@dataclass(frozen=True, slots=True)
class AiCallEvent:
    event_id: str
    call_id: str
    type: str
    timestamp: datetime
    source: str
    payload: dict[str, Any] = field(default_factory=dict)


class InMemoryEventStore:
    """Phase A 运行态事件存储；只服务延迟验证，不承诺进程重启后的追溯。"""

    def __init__(self) -> None:
        self._events: list[AiCallEvent] = []

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
