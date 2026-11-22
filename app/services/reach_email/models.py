"""同库邮件表；UUID 字符串 ID，无物理外键。"""

import json
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import Boolean, DateTime, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.core.base_model import MappedBase
from app.services.ai_call.credit_metering import ReachCreditUsageOutboxModel
from app.services.reach_email.providers import mailbox_service


def now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def new_id():
    return uuid4().hex


class EmailRow(MappedBase):
    __abstract__ = True
    __permission_strategy__ = None
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    owner_id: Mapped[str] = mapped_column(String(64), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=now, onupdate=now)


class Account(EmailRow):
    __tablename__ = "reach_email_account"
    __table_args__ = (
        UniqueConstraint("tenant_id", "owner_id", "email", "service_key", name="uq_reach_email_account_service"),
    )
    name: Mapped[str] = mapped_column(String(100))
    email: Mapped[str] = mapped_column(String(254))
    service_key: Mapped[str] = mapped_column(String(253), default=lambda context:
        mailbox_service(json.loads(context.get_current_parameters()['config'])))
    config: Mapped[str] = mapped_column(Text)
    secret: Mapped[str] = mapped_column(Text)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    weight: Mapped[int] = mapped_column(Integer, default=1)
    current_weight: Mapped[int] = mapped_column(Integer, default=0)
    hourly_limit: Mapped[int] = mapped_column(Integer, default=50)
    daily_limit: Mapped[int] = mapped_column(Integer, default=100)
    interval_seconds: Mapped[int] = mapped_column(Integer, default=60)
    smtp_status: Mapped[str] = mapped_column(String(20), default="untested")
    imap_status: Mapped[str] = mapped_column(String(20), default="untested")
    smtp_test_result: Mapped[str | None] = mapped_column(Text)
    imap_test_result: Mapped[str | None] = mapped_column(Text)
    oauth_status: Mapped[str | None] = mapped_column(String(20))
    next_send_at: Mapped[datetime | None] = mapped_column(DateTime)
    uidvalidity: Mapped[str | None] = mapped_column(String(40))
    last_uid: Mapped[int] = mapped_column(Integer, default=0)
    last_sync_at: Mapped[datetime | None] = mapped_column(DateTime)
    version: Mapped[int] = mapped_column(Integer, default=1)


class OutlookAuthorization(EmailRow):
    """短时、单次使用的授权流程，PKCE 和表单内容均加密存储。"""

    __tablename__ = "reach_email_outlook_authorization"
    payload: Mapped[str] = mapped_column(Text)
    expires_at: Mapped[datetime] = mapped_column(DateTime, index=True)


class Import(EmailRow):
    __tablename__ = "reach_email_import"
    filename: Mapped[str] = mapped_column(String(255))
    report: Mapped[str] = mapped_column(Text)
    consumed: Mapped[bool] = mapped_column(Boolean, default=False)


class Task(EmailRow):
    __tablename__ = "reach_email_task"
    name: Mapped[str] = mapped_column(String(120))
    import_id: Mapped[str] = mapped_column(String(32))
    settings: Mapped[str] = mapped_column(Text)
    sender_allocation_state: Mapped[str | None] = mapped_column(Text, default="{}")
    content: Mapped[str] = mapped_column(Text, default="{}")
    status: Mapped[str] = mapped_column(String(20), default="unstarted", index=True)
    version: Mapped[int] = mapped_column(Integer, default=1)
    scheduled_at: Mapped[datetime | None] = mapped_column(DateTime)
    ended_reason: Mapped[str | None] = mapped_column(String(40))
    start_key: Mapped[str | None] = mapped_column(String(128))
    recipient_count: Mapped[int] = mapped_column(Integer, default=0)
    daily_limit: Mapped[int] = mapped_column(Integer, default=50)


class Lead(EmailRow):
    __tablename__ = "reach_email_lead"
    __table_args__ = (UniqueConstraint("task_id", "email", name="uq_reach_email_task_recipient"),)
    task_id: Mapped[str] = mapped_column(String(32), index=True)
    email: Mapped[str] = mapped_column(String(254))
    values: Mapped[str] = mapped_column(Text)
    classification: Mapped[str] = mapped_column(String(20), default="following")
    account_id: Mapped[str | None] = mapped_column(String(32))
    first_message_id: Mapped[str | None] = mapped_column(String(32))
    follow_up_count: Mapped[int] = mapped_column(Integer, default=0)
    next_follow_up_at: Mapped[datetime | None] = mapped_column(DateTime)
    stopped_reason: Mapped[str | None] = mapped_column(String(40))
    last_sent_at: Mapped[datetime | None] = mapped_column(DateTime)
    last_reply_at: Mapped[datetime | None] = mapped_column(DateTime)
    last_read_reply_at: Mapped[datetime | None] = mapped_column(DateTime)


class Message(EmailRow):
    __tablename__ = "reach_email_message"
    __table_args__ = (
        UniqueConstraint("tenant_id", "owner_id", "dedup_key", name="uq_reach_email_message_dedup"),
        Index("ix_reach_email_queue", "status", "due_at"),
    )
    task_id: Mapped[str | None] = mapped_column(String(32), index=True)
    lead_id: Mapped[str | None] = mapped_column(String(32), index=True)
    account_id: Mapped[str | None] = mapped_column(String(32), index=True)
    direction: Mapped[str] = mapped_column(String(10), default="outbound")
    kind: Mapped[str] = mapped_column(String(20), default="initial")
    status: Mapped[str] = mapped_column(String(20), default="queued")
    dedup_key: Mapped[str] = mapped_column(String(200))
    message_id: Mapped[str] = mapped_column(String(512))
    provider_message_id: Mapped[str | None] = mapped_column(String(512))
    in_reply_to: Mapped[str | None] = mapped_column(String(512))
    references: Mapped[str] = mapped_column(Text, default="")
    delivery_report: Mapped[str | None] = mapped_column(Text)
    from_email: Mapped[str] = mapped_column(String(254), default="")
    to_email: Mapped[str] = mapped_column(String(254))
    subject: Mapped[str] = mapped_column(String(512))
    html: Mapped[str] = mapped_column(Text)
    attachment_ids: Mapped[str] = mapped_column(Text, default="[]")
    due_at: Mapped[datetime] = mapped_column(DateTime, default=now)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime)
    error_code: Mapped[str | None] = mapped_column(String(100))
    send_diagnostic: Mapped[str | None] = mapped_column(Text)
    resolution_note: Mapped[str | None] = mapped_column(Text)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    lease_token: Mapped[str | None] = mapped_column(String(32))
    lease_until: Mapped[datetime | None] = mapped_column(DateTime)


class Attempt(EmailRow):
    __tablename__ = "reach_email_attempt"
    __table_args__ = (
        Index("ix_reach_email_attempt_account", "account_id", "created_at"),
        Index("ix_reach_email_attempt_task", "task_id", "created_at"),
    )
    message_id: Mapped[str] = mapped_column(String(32))
    account_id: Mapped[str] = mapped_column(String(32))
    task_id: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(20), default="sending")


class SendRestriction(EmailRow):
    """服务商明确拒绝后的等待状态；不把未公开额度存成推测值。"""

    __tablename__ = "reach_email_send_restriction"
    __table_args__ = (
        UniqueConstraint("tenant_id", "owner_id", "scope_key", name="uq_reach_email_send_restriction"),
    )
    account_id: Mapped[str] = mapped_column(String(32), index=True)
    scope: Mapped[str] = mapped_column(String(24))
    scope_key: Mapped[str] = mapped_column(String(64))
    target: Mapped[str] = mapped_column(String(512))
    failure_count: Mapped[int] = mapped_column(Integer, default=0)
    retry_at: Mapped[datetime | None] = mapped_column(DateTime)
    diagnostic: Mapped[str] = mapped_column(Text)


class Attachment(EmailRow):
    __tablename__ = "reach_email_attachment"
    name: Mapped[str] = mapped_column(String(255))
    size: Mapped[int] = mapped_column(Integer)
    content_type: Mapped[str] = mapped_column(String(100))
    object_key: Mapped[str] = mapped_column(String(500))
    sha256: Mapped[str] = mapped_column(String(64))


class Version(EmailRow):
    __tablename__ = "reach_email_version"
    task_id: Mapped[str] = mapped_column(String(32), index=True)
    content: Mapped[str] = mapped_column(Text)
    version: Mapped[int] = mapped_column(Integer)


class AIJob(EmailRow):
    __tablename__ = "reach_email_ai_job"
    __table_args__ = (UniqueConstraint("task_id", "source_hash", name="uq_reach_email_ai_source"),)
    task_id: Mapped[str] = mapped_column(String(32))
    action: Mapped[str] = mapped_column(String(20))
    source_hash: Mapped[str] = mapped_column(String(64))
    source_version: Mapped[int] = mapped_column(Integer)
    payload: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(20), default="queued")
    result: Mapped[str | None] = mapped_column(Text)
    attempts: Mapped[str | None] = mapped_column(Text)
    error_code: Mapped[str | None] = mapped_column(String(100))
    lease_until: Mapped[datetime | None] = mapped_column(DateTime)


class Suppression(EmailRow):
    __tablename__ = "reach_email_suppression"
    __table_args__ = (
        UniqueConstraint("tenant_id", "owner_id", "email", name="uq_reach_email_suppression"),
    )
    email: Mapped[str] = mapped_column(String(254))
    reason: Mapped[str] = mapped_column(String(40))


class WorkerLease(MappedBase):
    """跨进程租约：email 保证单消费者，邮箱 UUID 串行化微软凭据续期。"""

    __tablename__ = "reach_email_worker_lease"
    __permission_strategy__ = None
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    token: Mapped[str] = mapped_column(String(32))
    expires_at: Mapped[datetime] = mapped_column(DateTime)


TABLES = [
    x.__table__
    for x in (
        Account,
        OutlookAuthorization,
        Import,
        Task,
        Lead,
        Message,
        Attempt,
        SendRestriction,
        Attachment,
        Version,
        AIJob,
        Suppression,
        WorkerLease,
    )
]

# 邮件与通话共用信用点投递箱；邮件独立建表流程也须创建它。
TABLES.append(ReachCreditUsageOutboxModel.__table__)
