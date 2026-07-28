from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, Boolean, DateTime, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.core.base_model import MappedBase


class AiCallOutboundValidationModel(MappedBase):
    """通用外呼批量名单校验任务。"""

    __tablename__ = "ai_call_outbound_validation"
    __table_args__ = (
        Index("idx_outbound_validation_tenant_status", "tenant_id", "status", "updated_at"),
        Index("idx_outbound_validation_tenant_id", "tenant_id", "id"),
        {"comment": "通用外呼批量名单校验任务"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="雪花主键",
    )
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, comment="租户ID")
    status: Mapped[str] = mapped_column(String(32), nullable=False, comment="校验状态")
    processing_stage: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="处理阶段",
    )
    original_filename: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="原始文件名",
    )
    temp_file_path: Mapped[str | None] = mapped_column(
        String(1000),
        nullable=True,
        comment="临时文件路径",
    )
    file_size: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        comment="上传文件字节数",
    )
    task_config_json: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="待创建任务配置JSON",
    )
    valid_target_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="有效名单数",
    )
    issue_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="问题行数",
    )
    issue_stats_json: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="问题类型统计JSON",
    )
    error_message: Mapped[str | None] = mapped_column(
        String(1000),
        nullable=True,
        comment="系统错误提示",
    )
    retryable: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        comment="是否允许基于validationId重试",
    )
    retry_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="系统校验重试次数",
    )
    created_by: Mapped[int] = mapped_column(BigInteger, nullable=False, comment="创建人")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="创建时间",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="更新时间",
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="终态时间",
    )


class AiCallOutboundValidationRowModel(MappedBase):
    """通用外呼名单校验明细，有效行和错误行共用本表。"""

    __tablename__ = "ai_call_outbound_validation_row"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "validation_id",
            "row_number",
            name="uk_outbound_validation_row_number",
        ),
        Index(
            "idx_outbound_validation_row_page",
            "tenant_id",
            "validation_id",
            "is_valid",
            "id",
        ),
        Index(
            "idx_outbound_validation_row_phone",
            "tenant_id",
            "validation_id",
            "normalized_phone",
        ),
        {"comment": "通用外呼名单校验明细"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="雪花主键",
    )
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, comment="租户ID")
    validation_id: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        comment="校验任务ID，无物理外键",
    )
    row_number: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="原Excel行号",
    )
    phone_number: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="原始手机号",
    )
    customer_name: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="原始客户名称",
    )
    normalized_phone: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="规范化手机号",
    )
    is_valid: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        comment="是否为有效行",
    )
    reasons_json: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="错误原因JSON数组",
    )
    duplicate_row_number: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="关联重复行号",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="创建时间",
    )
