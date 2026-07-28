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
    """正式通用外呼任务；本阶段只落计划态，不负责调度执行。"""

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
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    prompt_profile_id: Mapped[str | None] = mapped_column(String(64))
    prompt_name: Mapped[str] = mapped_column(String(100), nullable=False)
    scene_code: Mapped[str] = mapped_column(String(64), nullable=False)
    voice: Mapped[str] = mapped_column(String(128), nullable=False)
    voice_name: Mapped[str | None] = mapped_column(String(100))
    rule_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    rule_name: Mapped[str] = mapped_column(String(100), nullable=False)
    rule_summary: Mapped[str] = mapped_column(String(500), nullable=False)
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
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
