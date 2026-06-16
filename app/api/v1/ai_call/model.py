from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.core.base_model import MappedBase


class AiCallRecordModel(MappedBase):
    """AI Call B1 通话记录表。"""

    __tablename__ = "ai_call_record"
    __table_args__ = (
        UniqueConstraint("call_id", name="uk_ai_call_record_call_id"),
        Index("idx_ai_call_record_status_started", "status", "started_at"),
        Index("idx_ai_call_record_entry_started", "entry_type", "started_at"),
        Index("idx_ai_call_record_business", "business_type", "business_id"),
        Index("idx_ai_call_record_room_name", "room_name"),
        {"comment": "AI Call 通话记录表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="雪花主键",
    )
    call_id: Mapped[str] = mapped_column(String(64), nullable=False, comment="通话业务ID")
    business_type: Mapped[str | None] = mapped_column(
        String(32),
        nullable=True,
        comment="上游业务类型",
    )
    business_id: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="上游业务ID",
    )
    entry_type: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="web",
        comment="入口类型",
    )
    room_name: Mapped[str] = mapped_column(String(128), nullable=False, comment="LiveKit Room")
    participant_identity: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="用户侧 Participant identity",
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False, comment="会话状态")
    end_reason: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="结束原因",
    )
    failure_stage: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="失败阶段",
    )
    failure_message: Mapped[str | None] = mapped_column(
        String(500),
        nullable=True,
        comment="失败摘要",
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="会话创建时间",
    )
    answered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="用户侧进入可通话状态时间",
    )
    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="终态时间",
    )
    duration_ms: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="通话持续毫秒",
    )


class AiCallEventModel(MappedBase):
    """AI Call B1 关键事件表。"""

    __tablename__ = "ai_call_event"
    __table_args__ = (
        UniqueConstraint("event_id", name="uk_ai_call_event_event_id"),
        Index("idx_ai_call_event_call_id_id", "call_id", "id"),
        Index("idx_ai_call_event_call_time", "call_id", "event_time"),
        Index("idx_ai_call_event_call_type", "call_id", "event_type"),
        {"comment": "AI Call 关键事件表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="雪花主键",
    )
    call_id: Mapped[str] = mapped_column(String(64), nullable=False, comment="通话业务ID")
    event_id: Mapped[str] = mapped_column(String(64), nullable=False, comment="事件业务ID")
    event_type: Mapped[str] = mapped_column(String(80), nullable=False, comment="事件类型")
    source: Mapped[str] = mapped_column(String(32), nullable=False, comment="事件来源")
    event_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="事件发生时间",
    )
    payload_json: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="脱敏后的事件 payload JSON",
    )

    @property
    def payload(self) -> dict[str, Any]:
        if not self.payload_json:
            return {}
        try:
            payload = json.loads(self.payload_json)
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, dict) else {}
