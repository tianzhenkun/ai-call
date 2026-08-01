from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, Boolean, DateTime, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.core.base_model import MappedBase


class AiCallOutboundRuleModel(MappedBase):
    """租户级通用外呼规则。"""

    __tablename__ = "ai_call_outbound_rule"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "rule_name",
            name="uk_outbound_rule_tenant_name",
        ),
        Index(
            "idx_outbound_rule_tenant_enabled",
            "tenant_id",
            "deleted",
            "enabled",
            "updated_at",
        ),
        Index("idx_outbound_rule_tenant_id", "tenant_id", "id"),
        {"comment": "通用外呼呼叫规则"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    rule_name: Mapped[str] = mapped_column(String(100), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    call_windows_json: Mapped[str] = mapped_column(Text, nullable=False)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    retry_intervals_json: Mapped[str] = mapped_column(Text, nullable=False)
    retryable_results_json: Mapped[str] = mapped_column(Text, nullable=False)
    deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_by: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_by: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AiCallOutboundTaskModel(MappedBase):
    """正式通用外呼任务。"""

    __tablename__ = "ai_call_outbound_task"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "idempotency_key",
            name="uk_outbound_task_tenant_idempotency",
        ),
        Index(
            "idx_outbound_task_tenant_status",
            "tenant_id",
            "status",
            "created_at",
        ),
        Index(
            "idx_outbound_task_dispatch",
            "status",
            "next_dispatch_at",
            "last_dispatched_at",
            "id",
        ),
        Index("idx_outbound_task_tenant_id", "tenant_id", "id"),
        Index("idx_outbound_task_validation", "tenant_id", "validation_id"),
        {"comment": "通用外呼正式任务"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    validation_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    task_name: Mapped[str] = mapped_column(String(50), nullable=False)
    task_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    total_targets: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    completed_targets: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    connected_targets: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_targets: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    execution_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    scheduled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_dispatch_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_dispatched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    prompt_profile_id: Mapped[str | None] = mapped_column(String(64))
    prompt_name: Mapped[str] = mapped_column(String(100), nullable=False)
    scene_code: Mapped[str] = mapped_column(String(64), nullable=False)
    voice: Mapped[str] = mapped_column(String(128), nullable=False)
    voice_name: Mapped[str | None] = mapped_column(String(100))
    voice_type: Mapped[str | None] = mapped_column(String(32))
    voice_target_model: Mapped[str | None] = mapped_column(String(64))
    rule_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    rule_name: Mapped[str] = mapped_column(String(100), nullable=False)
    rule_summary: Mapped[str] = mapped_column(String(500), nullable=False)
    line_id: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        comment="任务绑定的SIP线路ID，无物理外键",
    )
    line_name: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
        comment="创建任务时的线路名称",
    )
    config_snapshot_json: Mapped[str] = mapped_column(Text, nullable=False)
    error_message: Mapped[str | None] = mapped_column(String(1000))
    created_by: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_by_name: Mapped[str | None] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AiCallOutboundTargetModel(MappedBase):
    """任务外呼对象快照；与任务、校验明细均为逻辑关联。"""

    __tablename__ = "ai_call_outbound_target"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "task_id",
            "source_validation_row_id",
            name="uk_outbound_target_source_row",
        ),
        Index(
            "idx_outbound_target_task_page",
            "tenant_id",
            "task_id",
            "source_row_number",
            "id",
        ),
        Index(
            "idx_outbound_target_task_status",
            "tenant_id",
            "task_id",
            "status",
            "next_attempt_at",
        ),
        {"comment": "通用外呼正式任务对象"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    task_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    validation_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_validation_row_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    phone_number: Mapped[str] = mapped_column(String(64), nullable=False)
    customer_name: Mapped[str | None] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    latest_result: Mapped[str | None] = mapped_column(String(128))
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AiCallOutboundAttemptModel(MappedBase):
    """一次拨打尝试；通过 call_id 与通话记录逻辑关联。"""

    __tablename__ = "ai_call_outbound_attempt"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "target_id",
            "attempt_no",
            name="uk_outbound_attempt_target_no",
        ),
        UniqueConstraint(
            "call_id",
            name="uk_outbound_attempt_call",
        ),
        UniqueConstraint(
            "tenant_id",
            "command_idempotency_key",
            name="uk_outbound_attempt_tenant_command",
        ),
        UniqueConstraint(
            "tenant_id",
            "active_slot",
            name="uk_outbound_attempt_tenant_active_slot",
        ),
        Index(
            "idx_outbound_attempt_task",
            "tenant_id",
            "task_id",
            "started_at",
        ),
        Index(
            "idx_outbound_attempt_target",
            "tenant_id",
            "target_id",
            "attempt_no",
        ),
        Index("idx_outbound_attempt_stale", "status", "started_at"),
        Index("idx_outbound_attempt_reconcile", "status", "reconcile_after"),
        Index("idx_outbound_attempt_reconcile_lease", "reconcile_expires_at"),
        {"comment": "通用外呼拨打尝试与通话记录关联"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    task_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    target_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    attempt_no: Mapped[int] = mapped_column(Integer, nullable=False)
    call_id: Mapped[str] = mapped_column(String(64), nullable=False)
    dialer_type: Mapped[str | None] = mapped_column(String(32))
    test_scenario: Mapped[str | None] = mapped_column(String(32))
    command_idempotency_key: Mapped[str | None] = mapped_column(String(128))
    active_slot: Mapped[str | None] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    call_result: Mapped[str | None] = mapped_column(String(64))
    error_message: Mapped[str | None] = mapped_column(String(1000))
    line_id: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        comment="本次拨打实际使用的线路ID，无物理外键",
    )
    line_code: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="本次拨打实际使用的线路编码",
    )
    provider_status_code: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="SIP Provider状态码",
    )
    provider_reason: Mapped[str | None] = mapped_column(
        String(500),
        nullable=True,
        comment="SIP Provider原始原因",
    )
    hangup_cause: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        comment="标准化挂机原因",
    )
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    reconcile_owner_id: Mapped[str | None] = mapped_column(String(128))
    reconcile_token: Mapped[str | None] = mapped_column(String(128))
    reconcile_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reconcile_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reconcile_attempt_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
