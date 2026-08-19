from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from fastapi import status

from app.services.ai_call.exceptions import AiCallError


class CallSessionStatus(str, Enum):
    CREATED = "created"
    PREPARING = "preparing"
    READY = "ready"
    CONNECTED = "connected"
    USER_SPEAKING = "user_speaking"
    AI_THINKING = "ai_thinking"
    AI_SPEAKING = "ai_speaking"
    INTERRUPTED = "interrupted"
    WAITING = "waiting"
    ENDING = "ending"
    COMPLETED = "completed"
    FAILED = "failed"


RUNNING_STATUSES = {
    CallSessionStatus.READY,
    CallSessionStatus.CONNECTED,
    CallSessionStatus.USER_SPEAKING,
    CallSessionStatus.AI_THINKING,
    CallSessionStatus.AI_SPEAKING,
    CallSessionStatus.INTERRUPTED,
    CallSessionStatus.WAITING,
}

TERMINAL_STATUSES = {
    CallSessionStatus.COMPLETED,
    CallSessionStatus.FAILED,
}

ALLOWED_TRANSITIONS: dict[CallSessionStatus, set[CallSessionStatus]] = {
    CallSessionStatus.CREATED: {CallSessionStatus.PREPARING, CallSessionStatus.FAILED},
    CallSessionStatus.PREPARING: {CallSessionStatus.READY, CallSessionStatus.FAILED},
    CallSessionStatus.READY: {
        CallSessionStatus.CONNECTED,
        CallSessionStatus.WAITING,
        CallSessionStatus.ENDING,
        CallSessionStatus.FAILED,
    },
    CallSessionStatus.CONNECTED: {
        CallSessionStatus.USER_SPEAKING,
        CallSessionStatus.AI_THINKING,
        CallSessionStatus.AI_SPEAKING,
        CallSessionStatus.WAITING,
        CallSessionStatus.ENDING,
        CallSessionStatus.FAILED,
    },
    CallSessionStatus.USER_SPEAKING: {
        CallSessionStatus.AI_THINKING,
        CallSessionStatus.WAITING,
        CallSessionStatus.ENDING,
        CallSessionStatus.FAILED,
    },
    CallSessionStatus.AI_THINKING: {
        CallSessionStatus.AI_SPEAKING,
        CallSessionStatus.WAITING,
        CallSessionStatus.ENDING,
        CallSessionStatus.FAILED,
    },
    CallSessionStatus.AI_SPEAKING: {
        CallSessionStatus.CONNECTED,
        CallSessionStatus.INTERRUPTED,
        CallSessionStatus.WAITING,
        CallSessionStatus.ENDING,
        CallSessionStatus.FAILED,
    },
    CallSessionStatus.INTERRUPTED: {
        CallSessionStatus.USER_SPEAKING,
        CallSessionStatus.AI_THINKING,
        CallSessionStatus.WAITING,
        CallSessionStatus.ENDING,
        CallSessionStatus.FAILED,
    },
    CallSessionStatus.WAITING: {
        CallSessionStatus.CONNECTED,
        CallSessionStatus.USER_SPEAKING,
        CallSessionStatus.ENDING,
        CallSessionStatus.FAILED,
    },
    CallSessionStatus.ENDING: {CallSessionStatus.COMPLETED, CallSessionStatus.FAILED},
    CallSessionStatus.COMPLETED: set(),
    CallSessionStatus.FAILED: set(),
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class KnowledgeRuntimeContext:
    tenant_id: str
    task_id: int
    prompt_profile_id: int
    version_ids: tuple[int, ...]
    version_snapshot_hash: str
    retriever_version: str


@dataclass(slots=True)
class CallSession:
    call_id: str
    room_name: str
    participant_identity: str
    status: CallSessionStatus
    effective_config: Any
    knowledge_context: KnowledgeRuntimeContext | None = None
    local_participant_identity: str | None = None
    started_at: datetime = field(default_factory=utc_now)
    last_event_at: datetime = field(default_factory=utc_now)
    metrics: dict[str, Any] = field(default_factory=dict)


class InMemorySessionRegistry:
    """Phase A 会话注册表；正式会话持久化和跨进程恢复留给后续阶段。"""

    def __init__(self) -> None:
        self._sessions: dict[str, CallSession] = {}

    def add(self, session: CallSession) -> None:
        if session.call_id in self._sessions:
            raise AiCallError(
                error_id="session_already_exists",
                msg="会话已存在",
                status_code=status.HTTP_409_CONFLICT,
            )
        self._sessions[session.call_id] = session

    def get(self, call_id: str) -> CallSession:
        session = self._sessions.get(call_id)
        if session is None:
            raise AiCallError(
                error_id="session_not_found",
                msg="会话不存在",
                status_code=status.HTTP_404_NOT_FOUND,
            )
        return session

    def discard(self, call_id: str) -> None:
        self._sessions.pop(call_id, None)

    def transition(self, call_id: str, target: CallSessionStatus) -> CallSession:
        session = self.get(call_id)
        if target not in ALLOWED_TRANSITIONS[session.status]:
            raise AiCallError(
                error_id="invalid_session_state",
                msg="当前会话状态不允许该操作",
                status_code=status.HTTP_409_CONFLICT,
            )
        session.status = target
        session.last_event_at = utc_now()
        return session
