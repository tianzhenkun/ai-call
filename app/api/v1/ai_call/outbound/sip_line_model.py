from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, Boolean, DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.core.base_model import MappedBase


class AiCallSipLineModel(MappedBase):
    """租户级 SIP 外呼线路；Provider 凭据不进入业务库。"""

    __tablename__ = "ai_call_sip_line"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "line_code",
            name="uk_ai_call_sip_line_tenant_code",
        ),
        UniqueConstraint(
            "tenant_id",
            "default_marker",
            name="uk_ai_call_sip_line_tenant_default",
        ),
        Index(
            "idx_ai_call_sip_line_tenant_enabled",
            "tenant_id",
            "deleted",
            "enabled",
            "updated_at",
        ),
        Index("idx_ai_call_sip_line_tenant_id", "tenant_id", "id"),
        {"comment": "AI Call 租户级 SIP 外呼线路"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="雪花主键",
    )
    tenant_id: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="租户ID",
    )
    line_code: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="租户内唯一线路编码",
    )
    line_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        comment="线路名称",
    )
    enabled: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        comment="是否允许新任务使用",
    )
    default_marker: Mapped[str | None] = mapped_column(
        String(32),
        nullable=True,
        comment="默认外呼线路标记，固定为OUTBOUND",
    )
    adapter_type: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="拨号适配器类型",
    )
    route_mode: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="managed_trunk_id或inline_hostname",
    )
    trunk_id: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        comment="LiveKit托管Outbound Trunk ID",
    )
    proxy_host: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        comment="内联SIP代理主机",
    )
    proxy_port: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="内联SIP代理端口",
    )
    auth_mode: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="managed_trunk或ip_allowlist",
    )
    caller_number: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="Provider授权主叫号码",
    )
    destination_country: Mapped[str] = mapped_column(
        String(8),
        nullable=False,
        default="CN",
        comment="目的国家",
    )
    max_concurrency: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        comment="线路最大并发",
    )
    originate_timeout_seconds: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=45,
        comment="建立呼叫超时秒数",
    )
    health_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="UNKNOWN",
        comment="线路健康状态",
    )
    health_message: Mapped[str | None] = mapped_column(
        String(500),
        nullable=True,
        comment="最近预检结论",
    )
    last_checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="最近预检时间",
    )
    deleted: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        comment="是否软删除",
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="删除时间",
    )
    created_by: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        comment="创建人",
    )
    updated_by: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        comment="更新人",
    )
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
