from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.core.base_model import MappedBase


class AiCallRuntimeWorkerModel(MappedBase):
    __tablename__ = "ai_call_runtime_worker"
    __table_args__ = (
        Index("idx_runtime_worker_dispatch", "status", "lease_expires_at", "worker_id"),
        Index("idx_runtime_worker_stream_cleanup", "stream_cleanup_after"),
        CheckConstraint("capacity >= 0", name="ck_runtime_worker_capacity"),
        CheckConstraint(
            "active_call_count >= 0 and active_call_count <= capacity",
            name="ck_runtime_worker_active_count",
        ),
        CheckConstraint("cleanup_capacity >= 0", name="ck_runtime_worker_cleanup_capacity"),
        CheckConstraint(
            "active_cleanup_count >= 0 and active_cleanup_count <= cleanup_capacity",
            name="ck_runtime_worker_cleanup_count",
        ),
        {"comment": "AI Call Runtime Worker 注册与容量"},
    )
    __permission_strategy__ = None

    worker_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    capacity: Mapped[int] = mapped_column(Integer, nullable=False)
    active_call_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    cleanup_capacity: Mapped[int] = mapped_column(Integer, nullable=False)
    active_cleanup_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    lease_expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    stream_cleanup_owner_id: Mapped[str | None] = mapped_column(String(128))
    stream_cleanup_token: Mapped[str | None] = mapped_column(String(128))
    stream_cleanup_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    stream_cleanup_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AiCallRuntimeCommandModel(MappedBase):
    __tablename__ = "ai_call_runtime_command"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "idempotency_key", name="uk_runtime_command_tenant_idempotency"
        ),
        UniqueConstraint(
            "tenant_id",
            "call_id",
            "command_seq",
            name="uk_runtime_command_call_seq",
        ),
        Index("idx_runtime_command_retry", "status", "next_retry_at"),
        Index(
            "idx_runtime_command_allocation",
            "command_type",
            "status",
            "allocation_deadline_at",
        ),
        Index("idx_runtime_command_dispatch_lease", "status", "dispatch_expires_at"),
        Index("idx_runtime_command_published", "status", "published_at"),
        Index("idx_runtime_command_processing", "status", "processing_expires_at"),
        Index(
            "idx_runtime_command_owner_dispatch",
            "target_owner_id",
            "status",
            "dispatch_priority",
            "created_at",
        ),
        Index("idx_runtime_command_call_audit", "tenant_id", "call_id", "created_at"),
        {"comment": "AI Call Runtime 持久命令"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    tenant_id: Mapped[str] = mapped_column(String(20), nullable=False)
    call_id: Mapped[str] = mapped_column(String(64), nullable=False)
    command_seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    command_type: Mapped[str] = mapped_column(String(64), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    dispatch_priority: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, default=100, server_default=text("100")
    )
    allocation_deadline_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    payload_json: Mapped[str | None] = mapped_column(Text)
    sensitive_payload_ciphertext: Mapped[str | None] = mapped_column(Text)
    payload_key_version: Mapped[str | None] = mapped_column(String(64))
    expected_fencing_token: Mapped[int | None] = mapped_column(BigInteger)
    target_owner_id: Mapped[str | None] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    dispatch_token: Mapped[str | None] = mapped_column(String(128))
    dispatch_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    stream_message_id: Mapped[str | None] = mapped_column(String(128))
    processing_owner_id: Mapped[str | None] = mapped_column(String(128))
    processing_fencing_token: Mapped[int | None] = mapped_column(BigInteger)
    processing_token: Mapped[str | None] = mapped_column(String(128))
    processing_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cancel_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    preempted_by_command_id: Mapped[int | None] = mapped_column(BigInteger)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    result_json: Mapped[str | None] = mapped_column(Text)
    error_message: Mapped[str | None] = mapped_column(String(1000))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AiCallEndEvidenceModel(MappedBase):
    __tablename__ = "ai_call_end_evidence"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "dedupe_key", name="uk_end_evidence_tenant_dedupe"
        ),
        Index("idx_end_evidence_call", "tenant_id", "call_id", "received_at"),
        {"comment": "AI Call 多来源终止证据"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    tenant_id: Mapped[str] = mapped_column(String(20), nullable=False)
    call_id: Mapped[str] = mapped_column(String(64), nullable=False)
    command_id: Mapped[int | None] = mapped_column(BigInteger)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    end_reason: Mapped[str] = mapped_column(String(64), nullable=False)
    provider: Mapped[str | None] = mapped_column(String(32))
    provider_namespace: Mapped[str | None] = mapped_column(String(128))
    provider_event_id: Mapped[str | None] = mapped_column(String(160))
    event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    dedupe_key: Mapped[str] = mapped_column(String(160), nullable=False)
    evidence_json: Mapped[str | None] = mapped_column(Text)


class AiCallHandoffMediaEvidenceModel(MappedBase):
    __tablename__ = "ai_call_handoff_media_evidence"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "provider_namespace",
            "dedupe_key",
            name="uk_handoff_media_evidence_dedupe",
        ),
        UniqueConstraint(
            "tenant_id",
            "call_id",
            "handoff_id",
            "media_state_version",
            name="uk_handoff_media_evidence_version",
        ),
        Index(
            "idx_handoff_media_evidence_handoff_version",
            "tenant_id",
            "handoff_id",
            "media_state_version",
        ),
        {"comment": "AI Call 转人工媒体证据"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    tenant_id: Mapped[str] = mapped_column(String(20), nullable=False)
    handoff_id: Mapped[str] = mapped_column(String(64), nullable=False)
    call_id: Mapped[str] = mapped_column(String(64), nullable=False)
    provider_namespace: Mapped[str] = mapped_column(String(128), nullable=False)
    dedupe_key: Mapped[str] = mapped_column(String(160), nullable=False)
    participant_identity: Mapped[str] = mapped_column(String(255), nullable=False)
    participant_sid: Mapped[str | None] = mapped_column(String(255))
    track_sid: Mapped[str | None] = mapped_column(String(255))
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    media_state_version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    provider_event_id: Mapped[str | None] = mapped_column(String(160))
    event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    evidence_json: Mapped[str | None] = mapped_column(Text)


class AiCallWebhookInboxModel(MappedBase):
    __tablename__ = "ai_call_webhook_inbox"
    __table_args__ = (
        UniqueConstraint(
            "provider",
            "provider_namespace",
            "dedupe_key",
            name="uk_webhook_inbox_provider_dedupe",
        ),
        Index(
            "idx_webhook_inbox_retry",
            "status",
            "next_retry_at",
            "received_at",
        ),
        Index("idx_webhook_inbox_recovery", "status", "processing_expires_at"),
        Index(
            "idx_webhook_inbox_call",
            "tenant_id",
            "call_id",
            "received_at",
        ),
        {"comment": "AI Call LiveKit Webhook 持久收件箱"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    provider_namespace: Mapped[str] = mapped_column(String(128), nullable=False)
    dedupe_key: Mapped[str] = mapped_column(String(160), nullable=False)
    tenant_id: Mapped[str] = mapped_column(String(20), nullable=False)
    call_id: Mapped[str | None] = mapped_column(String(64))
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processing_owner_id: Mapped[str | None] = mapped_column(String(128))
    processing_token: Mapped[str | None] = mapped_column(String(128))
    processing_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    error_message: Mapped[str | None] = mapped_column(String(1000))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AiCallWebhookQuarantineModel(MappedBase):
    __tablename__ = "ai_call_webhook_quarantine"
    __table_args__ = (
        UniqueConstraint(
            "provider",
            "provider_namespace",
            "dedupe_key",
            name="uk_webhook_quarantine_provider_dedupe",
        ),
        Index(
            "idx_webhook_quarantine_retry",
            "status",
            "next_retry_at",
            "received_at",
        ),
        Index("idx_webhook_quarantine_recovery", "status", "processing_expires_at"),
        {"comment": "AI Call 未匹配 Webhook 隔离队列"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    provider_namespace: Mapped[str] = mapped_column(String(128), nullable=False)
    dedupe_key: Mapped[str] = mapped_column(String(160), nullable=False)
    room_name: Mapped[str | None] = mapped_column(String(255))
    participant_identity: Mapped[str | None] = mapped_column(String(255))
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processing_owner_id: Mapped[str | None] = mapped_column(String(128))
    processing_generation: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default=text("0")
    )
    processing_token: Mapped[str | None] = mapped_column(String(128))
    processing_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    resolved_tenant_id: Mapped[str | None] = mapped_column(String(20))
    resolved_call_id: Mapped[str | None] = mapped_column(String(64))
    error_message: Mapped[str | None] = mapped_column(String(1000))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AiCallRuntimeEffectModel(MappedBase):
    __tablename__ = "ai_call_runtime_effect"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "idempotency_key", name="uk_runtime_effect_tenant_idempotency"
        ),
        UniqueConstraint(
            "tenant_id",
            "provider_namespace",
            "effect_type",
            "resource_key",
            name="uk_runtime_effect_resource",
        ),
        UniqueConstraint(
            "tenant_id",
            "provider_namespace",
            "provider_idempotency_key",
            name="uk_runtime_effect_provider_idempotency",
        ),
        Index("idx_runtime_effect_reconcile", "status", "reconcile_after"),
        Index("idx_runtime_effect_processing", "status", "processing_expires_at"),
        Index(
            "idx_runtime_effect_call_audit",
            "tenant_id",
            "call_id",
            "status",
            "created_at",
        ),
        {"comment": "AI Call Provider 副作用日志"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    tenant_id: Mapped[str] = mapped_column(String(20), nullable=False)
    call_id: Mapped[str] = mapped_column(String(64), nullable=False)
    command_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    effect_type: Mapped[str] = mapped_column(String(64), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(160), nullable=False)
    fencing_token: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    processing_token: Mapped[str | None] = mapped_column(String(128))
    processing_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    provider_namespace: Mapped[str] = mapped_column(String(128), nullable=False)
    provider_idempotency_key: Mapped[str] = mapped_column(String(160), nullable=False)
    resource_key: Mapped[str] = mapped_column(String(255), nullable=False)
    resource_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_create_effect_id: Mapped[int | None] = mapped_column(BigInteger)
    create_protection_deadline_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    absence_observation_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    absence_confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    terminal_confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    provider_reference: Mapped[str | None] = mapped_column(String(255))
    execution_phase: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, default=0, server_default=text("0")
    )
    processing_owner_id: Mapped[str | None] = mapped_column(String(128))
    processing_fencing_token: Mapped[int | None] = mapped_column(BigInteger)
    reconcile_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reconcile_deadline_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    error_message: Mapped[str | None] = mapped_column(String(1000))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AiCallRuntimeEffectDependencyModel(MappedBase):
    __tablename__ = "ai_call_runtime_effect_dependency"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "effect_id",
            "prerequisite_effect_id",
            name="uk_runtime_effect_dependency",
        ),
        Index("idx_runtime_effect_dependency_effect", "tenant_id", "effect_id"),
        {"comment": "AI Call Provider Effect 依赖"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    tenant_id: Mapped[str] = mapped_column(String(20), nullable=False)
    effect_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    prerequisite_effect_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    required_status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="APPLIED", server_default=text("'APPLIED'")
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AiCallSipLineReservationModel(MappedBase):
    __tablename__ = "ai_call_sip_line_reservation"
    __table_args__ = (
        UniqueConstraint("call_id", name="uk_sip_line_reservation_call"),
        Index(
            "idx_sip_line_reservation_capacity",
            "tenant_id",
            "line_id",
            "status",
            "acquired_at",
        ),
        {"comment": "AI Call SIP 线路并发占用"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    tenant_id: Mapped[str] = mapped_column(String(20), nullable=False)
    line_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    call_id: Mapped[str] = mapped_column(String(64), nullable=False)
    attempt_id: Mapped[int | None] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    reservation_token: Mapped[str] = mapped_column(String(128), nullable=False)
    fencing_token: Mapped[int] = mapped_column(BigInteger, nullable=False)
    acquired_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    reconcile_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    released_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_message: Mapped[str | None] = mapped_column(String(1000))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
