from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Index, Integer, String, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column

from app.core.base_model import MappedBase


class AiCallTenantVoiceProfileModel(MappedBase):
    """AI Call 租户自定义复刻音色档案。"""

    __tablename__ = "ai_call_tenant_voice_profile"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "target_model",
            "voice",
            name="uk_tenant_voice_model_voice",
        ),
        Index(
            "idx_tenant_voice_status_updated",
            "tenant_id",
            "status",
            "updated_at",
        ),
        Index("idx_tenant_voice_tenant_id", "tenant_id", "id"),
        {"comment": "AI Call 租户自定义复刻音色档案"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    display_name: Mapped[str] = mapped_column(String(100), nullable=False)
    voice: Mapped[str | None] = mapped_column(String(128), nullable=True)
    voice_type: Mapped[str] = mapped_column(String(32), nullable=False)
    gender: Mapped[str] = mapped_column(String(16), nullable=False)
    language: Mapped[str] = mapped_column(String(32), nullable=False)
    target_model: Mapped[str] = mapped_column(String(64), nullable=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    latest_enrollment_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    provider_created_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    error_message: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    created_by: Mapped[int] = mapped_column(BigInteger, nullable=False)
    deleted_by: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AiCallVoiceEnrollmentModel(MappedBase):
    """AI Call 自定义音色复刻申请。"""

    __tablename__ = "ai_call_voice_enrollment"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "idempotency_key",
            name="uk_voice_enrollment_tenant_key",
        ),
        Index("idx_voice_enrollment_claim", "status", "next_retry_at", "id"),
        Index(
            "idx_voice_enrollment_profile",
            "tenant_id",
            "voice_profile_id",
            "created_at",
        ),
        {"comment": "AI Call 自定义音色复刻申请"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    voice_profile_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    preferred_name: Mapped[str] = mapped_column(String(16), nullable=False)
    language: Mapped[str] = mapped_column(String(32), nullable=False)
    transcript: Mapped[str | None] = mapped_column(String(2000), nullable=True)
    sample_object_key: Mapped[str | None] = mapped_column(String(500), nullable=True)
    sample_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    provider_voice: Mapped[str | None] = mapped_column(String(128), nullable=True)
    provider_request_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    attempt_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    error_message: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    cleanup_error_message: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    consent_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    consent_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AiCallVoiceDeletionModel(MappedBase):
    """AI Call 自定义音色删除申请。"""

    __tablename__ = "ai_call_voice_deletion"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "idempotency_key",
            name="uk_voice_deletion_tenant_key",
        ),
        Index("idx_voice_deletion_claim", "status", "next_retry_at", "id"),
        Index(
            "idx_voice_deletion_profile",
            "tenant_id",
            "voice_profile_id",
            "created_at",
        ),
        {"comment": "AI Call 自定义音色删除申请"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    voice_profile_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    provider_request_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    attempt_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    historical_task_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    error_message: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    requested_by: Mapped[int] = mapped_column(BigInteger, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AiCallVoiceSampleCleanupModel(MappedBase):
    """数据库回滚后遗留的私有声音样本清理任务。"""

    __tablename__ = "ai_call_voice_sample_cleanup"
    __table_args__ = (
        UniqueConstraint(
            "object_key",
            name="uk_voice_sample_cleanup_object_key",
        ),
        Index(
            "idx_voice_sample_cleanup_claim",
            "status",
            "next_retry_at",
            "id",
        ),
        {"comment": "AI Call 孤儿声音样本清理任务"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    object_key: Mapped[str] = mapped_column(String(500), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    attempt_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    error_message: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
