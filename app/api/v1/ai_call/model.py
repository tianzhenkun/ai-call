from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.core.base_model import MappedBase


def _default_dialogue_persistence_status(context: Any) -> str:
    parameters = context.get_current_parameters()
    if parameters.get("runtime_control_mode") == "owner_command_v1":
        return "pending"
    return "not_started"


class AiCallRecordModel(MappedBase):
    """AI Call B1 通话记录表。"""

    __tablename__ = "ai_call_record"
    __table_args__ = (
        UniqueConstraint("call_id", name="uk_ai_call_record_call_id"),
        UniqueConstraint("room_name", name="uk_ai_call_record_room_name"),
        Index("idx_ai_call_record_status_started", "status", "started_at"),
        Index("idx_ai_call_record_entry_started", "entry_type", "started_at"),
        Index("idx_ai_call_record_business", "business_type", "business_id"),
        Index("idx_ai_call_record_follow_up", "follow_up_id"),
        Index("idx_ai_call_record_room_name", "room_name"),
        Index(
            "idx_ai_call_record_runtime_owner_lease",
            "runtime_owner_id",
            "runtime_lease_expires_at",
        ),
        Index(
            "idx_ai_call_record_sip_callee_active",
            "entry_type",
            "callee_phone_number_hash",
            "status",
            "started_at",
        ),
        CheckConstraint(
            "runtime_control_mode in ('legacy_local', 'owner_command_v1')",
            name="ck_ai_call_record_runtime_control_mode",
        ),
        CheckConstraint(
            "runtime_control_mode = 'legacy_local' or tenant_id is not null",
            name="ck_ai_call_record_owner_mode_tenant",
        ),
        CheckConstraint(
            "runtime_capacity_class not in ('active', 'cleanup') or "
            "(runtime_owner_id is not null and runtime_lease_expires_at is not null)",
            name="ck_ai_call_record_owned_capacity",
        ),
        CheckConstraint(
            "runtime_capacity_class <> 'attention' or "
            "(runtime_owner_id is null and runtime_lease_expires_at is null "
            "and resource_cleanup_status = 'attention_required' "
            "and resource_cleanup_next_retry_at is not null)",
            name="ck_ai_call_record_attention_capacity",
        ),
        CheckConstraint(
            "resource_cleanup_status <> 'clean' or "
            "(runtime_capacity_class = 'none' and runtime_owner_id is null "
            "and runtime_lease_expires_at is null "
            "and resource_cleanup_completed_at is not null)",
            name="ck_ai_call_record_cleanup_clean",
        ),
        CheckConstraint(
            "dialogue_persistence_status in "
            "('not_started', 'pending', 'complete', 'uncertain')",
            name="ck_ai_call_record_dialogue_status",
        ),
        CheckConstraint(
            "runtime_control_mode <> 'owner_command_v1' "
            "or dialogue_persistence_status <> 'not_started'",
            name="ck_ai_call_record_owner_dialogue_started",
        ),
        CheckConstraint(
            "dialogue_persistence_status not in ('complete', 'uncertain') "
            "or dialogue_persistence_completed_at is not null",
            name="ck_ai_call_record_dialogue_completed_at",
        ),
        {"comment": "AI Call 通话记录表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="雪花主键",
    )
    tenant_id: Mapped[str | None] = mapped_column(String(20), nullable=True)
    call_id: Mapped[str] = mapped_column(String(64), nullable=False, comment="通话业务ID")
    follow_up_id: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        comment="人工回拨来源跟进任务ID",
    )
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
    scene_code: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="业务场景编码",
    )
    prompt_source_key: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="提示词来源键",
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
    callee_phone_number_hash: Mapped[str | None] = mapped_column(
        String(80),
        nullable=True,
        comment="SIP 被叫号码指纹",
    )
    callee_phone_number: Mapped[str | None] = mapped_column(
        String(32),
        nullable=True,
        comment="Direct SIP 被叫号码明文",
    )
    callee_phone_number_masked: Mapped[str | None] = mapped_column(
        String(32),
        nullable=True,
        comment="SIP 被叫号码脱敏展示",
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
    dialogue_persistence_status: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default=_default_dialogue_persistence_status,
        server_default=text("'not_started'"),
    )
    dialogue_persistence_error: Mapped[str | None] = mapped_column(
        String(500),
        nullable=True,
    )
    dialogue_persistence_completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    runtime_control_mode: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="legacy_local",
        server_default=text("'legacy_local'"),
    )
    runtime_owner_id: Mapped[str | None] = mapped_column(String(128))
    runtime_fencing_token: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    runtime_lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    runtime_heartbeat_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    runtime_capacity_class: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="none",
        server_default=text("'none'"),
    )
    startup_reconcile_deadline_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    startup_reconcile_policy_version: Mapped[str | None] = mapped_column(String(64))
    startup_reconcile_budget_json: Mapped[str | None] = mapped_column(Text)
    agent_participant_identity: Mapped[str | None] = mapped_column(String(255))
    agent_participant_sid: Mapped[str | None] = mapped_column(String(255))
    agent_audio_track_sid: Mapped[str | None] = mapped_column(String(255))
    agent_resource_generation: Mapped[int | None] = mapped_column(BigInteger)
    agent_media_ready_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    next_command_seq: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=1,
        server_default=text("1"),
    )
    last_applied_command_seq: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    terminal_requested_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    resource_cleanup_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="not_started",
        server_default=text("'not_started'"),
    )
    resource_cleanup_error: Mapped[str | None] = mapped_column(String(1000))
    resource_cleanup_next_retry_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    resource_cleanup_completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
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


class AiCallRecordingModel(MappedBase):
    """AI Call B2 通话录音表。"""

    __tablename__ = "ai_call_recording"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "call_id",
            name="uk_ai_call_recording_tenant_call",
        ),
        Index(
            "idx_ai_call_recording_tenant_started",
            "tenant_id",
            "status",
            "started_at",
        ),
        Index("idx_ai_call_recording_tenant_egress", "tenant_id", "egress_id"),
        Index("idx_ai_call_recording_tenant_oss", "tenant_id", "oss_id"),
        Index(
            "idx_ai_call_recording_tenant_verify_due",
            "tenant_id",
            "status",
            "next_verify_at",
        ),
        {"comment": "AI Call 通话录音表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="雪花主键",
    )
    tenant_id: Mapped[str] = mapped_column(String(20), nullable=False)
    call_id: Mapped[str] = mapped_column(String(64), nullable=False, comment="通话业务ID")
    room_name: Mapped[str] = mapped_column(String(128), nullable=False, comment="LiveKit Room")
    status: Mapped[str] = mapped_column(String(32), nullable=False, comment="录音状态")
    egress_id: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        comment="LiveKit Egress ID",
    )
    egress_generation: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        comment="Owner Runtime 主录音资源代次",
    )
    oss_id: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        comment="sys_oss 对象ID",
    )
    object_name: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        comment="对象存储文件名",
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="录音启动时间",
    )
    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="录音结束时间",
    )
    duration_ms: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="录音持续毫秒",
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
    stop_requested_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="发起停止录音时间",
    )
    verify_attempts: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="停止结果确认次数",
    )
    next_verify_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="下次停止结果确认时间",
    )
    verify_deadline_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="停止结果确认截止时间",
    )
    last_verify_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="最近停止结果确认时间",
    )
    last_verify_error: Mapped[str | None] = mapped_column(
        String(500),
        nullable=True,
        comment="最近停止结果确认错误",
    )


class AiCallRecordingTrackModel(MappedBase):
    """AI Call 分参与方录音明细表。"""

    __tablename__ = "ai_call_recording_track"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "call_id",
            "track_role",
            "participant_identity",
            name="uk_ai_call_recording_track_tenant_participant",
        ),
        Index(
            "idx_ai_call_recording_track_tenant_call_role",
            "tenant_id",
            "call_id",
            "track_role",
        ),
        Index(
            "idx_ai_call_recording_track_tenant_egress",
            "tenant_id",
            "egress_id",
        ),
        Index(
            "idx_ai_call_recording_track_tenant_oss",
            "tenant_id",
            "oss_id",
        ),
        Index(
            "idx_ai_call_recording_track_tenant_verify_due",
            "tenant_id",
            "status",
            "next_verify_at",
        ),
        Index(
            "idx_ai_call_recording_track_verify_due",
            "status",
            "next_verify_at",
            "id",
        ),
        {"comment": "AI Call 分参与方录音明细表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="雪花主键",
    )
    tenant_id: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        comment="租户ID",
    )
    call_id: Mapped[str] = mapped_column(String(64), nullable=False, comment="通话业务ID")
    room_name: Mapped[str] = mapped_column(String(128), nullable=False, comment="LiveKit Room")
    track_role: Mapped[str] = mapped_column(String(32), nullable=False, comment="轨道角色")
    participant_identity: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="LiveKit Participant identity",
    )
    handoff_id: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="关联转人工ID",
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False, comment="录音状态")
    egress_id: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        comment="LiveKit Egress ID",
    )
    egress_generation: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        comment="Egress 资源代次",
    )
    oss_id: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        comment="sys_oss 对象ID",
    )
    object_name: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        comment="对象存储文件名",
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="录音启动时间",
    )
    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="录音结束时间",
    )
    duration_ms: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="录音持续毫秒",
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
    stop_requested_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="发起停止录音时间",
    )
    verify_attempts: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="停止结果确认次数",
    )
    next_verify_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="下次停止结果确认时间",
    )
    verify_deadline_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="停止结果确认截止时间",
    )
    last_verify_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="最近停止结果确认时间",
    )
    last_verify_error: Mapped[str | None] = mapped_column(
        String(500),
        nullable=True,
        comment="最近停止结果确认错误",
    )


class AiCallDialogueSegmentModel(MappedBase):
    """AI Call B2.5 对话文本段表。"""

    __tablename__ = "ai_call_dialogue_segment"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "call_id",
            "segment_no",
            name="uk_ai_call_dialogue_call_no",
        ),
        UniqueConstraint(
            "tenant_id",
            "call_id",
            "speaker_type",
            "source",
            "source_segment_id",
            name="uk_ai_call_dialogue_source_segment",
        ),
        Index(
            "idx_ai_call_dialogue_speaker",
            "tenant_id",
            "call_id",
            "speaker_type",
            "segment_no",
        ),
        {"comment": "AI Call 对话文本段表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="雪花主键",
    )
    tenant_id: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="000000",
    )
    call_id: Mapped[str] = mapped_column(String(64), nullable=False, comment="通话业务ID")
    segment_no: Mapped[int] = mapped_column(Integer, nullable=False, comment="通话内段落序号")
    speaker_type: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="说话方类型",
    )
    speaker_identity: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        comment="说话方身份",
    )
    source: Mapped[str] = mapped_column(String(32), nullable=False, comment="文本来源")
    source_segment_id: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="来源侧文本段ID",
    )
    segment_text: Mapped[str] = mapped_column(Text, nullable=False, comment="对话文本")
    segment_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="段落状态",
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="段落开始时间",
    )
    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="段落结束时间",
    )
    duration_ms: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="段落持续毫秒",
    )
    audio_start_ms: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="录音内音频开始毫秒",
    )
    audio_end_ms: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="录音内音频结束毫秒",
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


class AiCallAsrJobModel(MappedBase):
    """AI Call 离线 ASR 任务表。"""

    __tablename__ = "ai_call_asr_job"
    __table_args__ = (
        UniqueConstraint("track_id", "provider", "model", name="uk_ai_call_asr_job_track_provider"),
        Index("idx_ai_call_asr_job_call_status", "call_id", "status"),
        Index("idx_ai_call_asr_job_track_id", "track_id"),
        Index("idx_ai_call_asr_job_task_id", "task_id"),
        {"comment": "AI Call 离线 ASR 任务表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="雪花主键",
    )
    call_id: Mapped[str] = mapped_column(String(64), nullable=False, comment="通话业务ID")
    track_id: Mapped[int] = mapped_column(BigInteger, nullable=False, comment="分轨录音ID")
    track_role: Mapped[str] = mapped_column(String(32), nullable=False, comment="轨道角色")
    participant_identity: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="LiveKit Participant identity",
    )
    provider: Mapped[str] = mapped_column(String(32), nullable=False, comment="ASR供应商")
    model: Mapped[str] = mapped_column(String(64), nullable=False, comment="ASR模型")
    status: Mapped[str] = mapped_column(String(32), nullable=False, comment="任务状态")
    task_id: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        comment="供应商异步任务ID",
    )
    source_url: Mapped[str | None] = mapped_column(
        String(1000),
        nullable=True,
        comment="ASR输入音频URL",
    )
    transcription_url: Mapped[str | None] = mapped_column(
        String(1000),
        nullable=True,
        comment="供应商转写结果URL",
    )
    submitted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="任务提交时间",
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="任务完成时间",
    )
    segment_count: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="写入文本段数",
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


class AiCallSemanticAnalysisModel(MappedBase):
    """AI Call 通话后语义分析记录表。"""

    __tablename__ = "ai_call_semantic_analysis"
    __table_args__ = (
        UniqueConstraint(
            "call_id",
            "analysis_scene_code",
            name="uk_ai_call_semantic_call_scene",
        ),
        Index("idx_ai_call_semantic_call_id", "call_id"),
        Index("idx_ai_call_semantic_status_updated", "analysis_status", "updated_at"),
        Index("idx_ai_call_semantic_scene_status", "scene_code", "analysis_status"),
        Index(
            "idx_ai_call_semantic_scene_intent",
            "analysis_scene_code",
            "customer_intent",
        ),
        Index(
            "idx_ai_call_semantic_scene_follow_up",
            "analysis_scene_code",
            "follow_up_suggested",
        ),
        {"comment": "AI Call 通话后语义分析记录表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="雪花主键",
    )
    call_id: Mapped[str] = mapped_column(String(64), nullable=False, comment="通话业务ID")
    scene_code: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="业务场景编码",
    )
    analysis_scene_code: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="分析场景编码",
    )
    analysis_status: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        comment="分析状态：0待分析/1分析中/2成功/3失败/4无有效用户输入",
    )
    analysis_result: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="结构化语义分析与话后结果 JSON",
    )
    customer_intent: Mapped[str | None] = mapped_column(
        String(16),
        nullable=True,
        comment="物化客户意向：positive/neutral/negative",
    )
    follow_up_suggested: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        comment="是否建议后续人工跟进",
    )
    follow_up_consent: Mapped[str | None] = mapped_column(
        String(16),
        nullable=True,
        comment="跟进同意状态：explicit/missing/refused",
    )
    follow_up_reason: Mapped[str | None] = mapped_column(
        String(500),
        nullable=True,
        comment="结构化跟进原因",
    )
    follow_up_preferred_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="客户期望跟进时间",
    )
    follow_up_confidence: Mapped[str | None] = mapped_column(
        String(16),
        nullable=True,
        comment="跟进证据置信度：high/medium/low",
    )
    analysis_error: Mapped[str | None] = mapped_column(
        String(1000),
        nullable=True,
        comment="分析错误或无需分析原因",
    )
    analysis_retry_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="分析失败重试次数",
    )
    analysis_started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="分析开始时间",
    )
    analysis_finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="分析结束时间",
    )
    transcript_hash: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        comment="转写快照哈希",
    )
    transcript_snapshot_json: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="本次分析使用的转写快照 JSON",
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

    @property
    def analysis_result_dict(self) -> dict[str, Any] | None:
        if not self.analysis_result:
            return None
        try:
            value = json.loads(self.analysis_result)
        except json.JSONDecodeError:
            return None
        return value if isinstance(value, dict) else None

    @property
    def transcript_snapshot_dict(self) -> dict[str, Any] | None:
        if not self.transcript_snapshot_json:
            return None
        try:
            value = json.loads(self.transcript_snapshot_json)
        except json.JSONDecodeError:
            return None
        return value if isinstance(value, dict) else None


class AiCallQualityScoreModel(MappedBase):
    """AI Call 自动评分记录表。"""

    __tablename__ = "ai_call_quality_score"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "call_id",
            "model_version",
            name="uk_ai_call_quality_score_call_model",
        ),
        Index(
            "idx_ai_call_quality_score_tenant_status",
            "tenant_id",
            "status",
            "updated_at",
        ),
        Index("idx_ai_call_quality_score_call", "call_id"),
        {"comment": "AI Call 自动评分记录表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="雪花主键",
    )
    tenant_id: Mapped[str] = mapped_column(String(20), nullable=False)
    call_id: Mapped[str] = mapped_column(String(64), nullable=False, comment="通话业务ID")
    status: Mapped[str] = mapped_column(String(16), nullable=False, comment="评分状态")
    score: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="AI评分")
    reason: Mapped[str | None] = mapped_column(Text, nullable=True, comment="AI评分理由")
    model_version: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        server_default=text("'quality-v1'"),
        comment="评分模型或提示词版本",
    )
    retry_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="评分失败重试次数",
    )
    error_message: Mapped[str | None] = mapped_column(
        String(500),
        nullable=True,
        comment="评分失败摘要",
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="评分开始时间",
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="评分结束时间",
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


class AiCallQualityReviewModel(MappedBase):
    """AI Call 人工质检记录表。"""

    __tablename__ = "ai_call_quality_review"
    __table_args__ = (
        UniqueConstraint("tenant_id", "call_id", name="uk_ai_call_quality_review_call"),
        Index(
            "idx_ai_call_quality_review_tenant_result",
            "tenant_id",
            "quality_result",
            "reviewed_at",
        ),
        {"comment": "AI Call 人工质检记录表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="雪花主键",
    )
    tenant_id: Mapped[str] = mapped_column(String(20), nullable=False)
    call_id: Mapped[str] = mapped_column(String(64), nullable=False, comment="通话业务ID")
    quality_result: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        comment="人工质检结论：excellent/good/pass/fail",
    )
    quality_reason: Mapped[str | None] = mapped_column(
        String(500),
        nullable=True,
        comment="人工质检原因",
    )
    reviewed_by: Mapped[str] = mapped_column(String(64), nullable=False, comment="质检员ID")
    reviewed_by_name: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="质检员姓名",
    )
    reviewed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="质检时间",
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


class AiCallHandoffModel(MappedBase):
    """AI Call B3 转人工记录表。"""

    __tablename__ = "ai_call_handoff"
    __table_args__ = (
        UniqueConstraint("tenant_id", "handoff_id", name="uk_ai_call_handoff_tenant_handoff"),
        Index("idx_ai_call_handoff_tenant_call", "tenant_id", "call_id", "requested_at"),
        Index("idx_ai_call_handoff_tenant_status", "tenant_id", "status", "requested_at"),
        {"comment": "AI Call 转人工记录表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="雪花主键",
    )
    tenant_id: Mapped[str] = mapped_column(
        String(20), nullable=False, default="000000", server_default="000000", comment="租户编号"
    )
    handoff_id: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="转人工业务ID",
    )
    call_id: Mapped[str] = mapped_column(String(64), nullable=False, comment="通话业务ID")
    room_name: Mapped[str] = mapped_column(String(128), nullable=False, comment="LiveKit Room")
    scene_code: Mapped[str] = mapped_column(
        String(64), nullable=False, default="default", server_default="default", comment="业务场景编码"
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False, comment="转人工状态")
    request_source: Mapped[str] = mapped_column(String(32), nullable=False, comment="请求来源")
    request_reason: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="请求原因",
    )
    request_message: Mapped[str | None] = mapped_column(
        String(500),
        nullable=True,
        comment="请求摘要",
    )
    human_agent_identity: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        comment="接管人工身份",
    )
    accepted_console_session_id: Mapped[str | None] = mapped_column(
        String(36), nullable=True, comment="认领浏览器会话UUID"
    )
    requested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="请求创建时间",
    )
    accepted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="坐席接管请求时间",
    )
    connected_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="坐席进入 Room 时间",
    )
    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="转人工终态时间",
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="等待或连接超时时间",
    )
    claim_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="首次媒体接入截止时间"
    )
    reconnect_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="媒体重连截止时间"
    )
    participant_identity: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="当前坐席参与方身份"
    )
    participant_sid: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="当前坐席参与方SID"
    )
    track_sid: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="当前坐席音频轨SID"
    )
    verified_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="当前坐席媒体验证时间"
    )
    evidence_source: Mapped[str | None] = mapped_column(
        String(64), nullable=True, comment="当前坐席媒体证据来源"
    )
    media_state_version: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=0,
        server_default=text("0"),
        comment="坐席媒体单调版本",
    )
    media_invalidated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="最近媒体失效时间"
    )
    last_media_event_key: Mapped[str | None] = mapped_column(
        String(160), nullable=True, comment="最近媒体事件去重键"
    )
    end_reason: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="终态原因",
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


class AiCallHandoffAgentModel(MappedBase):
    """AI Call 最小人工坐席状态表。"""

    __tablename__ = "ai_call_handoff_agent"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "agent_identity", name="uk_ai_call_handoff_agent_tenant_identity"
        ),
        Index("idx_ai_call_handoff_agent_tenant_status", "tenant_id", "status"),
        Index("idx_ai_call_handoff_agent_active", "active_handoff_id"),
        {"comment": "AI Call 人工坐席状态表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="雪花主键",
    )
    tenant_id: Mapped[str] = mapped_column(
        String(20), nullable=False, default="000000", server_default="000000", comment="租户编号"
    )
    agent_identity: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="坐席身份",
    )
    skill_group: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        default="default",
        comment="技能组",
    )
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="坐席状态",
    )
    active_handoff_id: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        comment="当前接管转人工ID",
    )
    active_call_id: Mapped[str | None] = mapped_column(
        String(64), nullable=True, comment="当前人工通话ID"
    )
    console_session_id: Mapped[str | None] = mapped_column(
        String(36), nullable=True, comment="媒体控制浏览器会话UUID"
    )
    last_seen_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="最近心跳时间",
    )
    status_updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="状态更新时间",
    )


class AiCallAgentProfileModel(MappedBase):
    """AI Call 坐席档案表。"""

    __tablename__ = "ai_call_agent_profile"
    __table_args__ = (
        UniqueConstraint("tenant_id", "agent_identity", name="uk_ai_call_agent_profile_identity"),
        UniqueConstraint("tenant_id", "user_id", name="uk_ai_call_agent_profile_user"),
        {"comment": "AI Call 坐席档案表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    tenant_id: Mapped[str] = mapped_column(String(20), nullable=False)
    agent_identity: Mapped[str] = mapped_column(String(128), nullable=False)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_by: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_by: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AiCallAgentSceneScopeModel(MappedBase):
    """AI Call 坐席场景授权表。"""

    __tablename__ = "ai_call_agent_scene_scope"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "agent_identity",
            "scene_code",
            name="uk_ai_call_agent_scene_scope",
        ),
        Index("idx_ai_call_agent_scene_scope_scene", "tenant_id", "scene_code"),
        {"comment": "AI Call 坐席场景授权表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    tenant_id: Mapped[str] = mapped_column(String(20), nullable=False)
    agent_identity: Mapped[str] = mapped_column(String(128), nullable=False)
    scene_code: Mapped[str] = mapped_column(String(64), nullable=False)
    created_by: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AiCallAfterCallWorkModel(MappedBase):
    """AI Call 快速话后处理表。"""

    __tablename__ = "ai_call_after_call_work"
    __table_args__ = (
        UniqueConstraint("tenant_id", "work_id", name="uk_ai_call_acw_work"),
        UniqueConstraint("tenant_id", "handoff_id", name="uk_ai_call_acw_handoff"),
        Index("idx_ai_call_acw_tenant_call", "tenant_id", "call_id"),
        {"comment": "AI Call 快速话后处理表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    work_id: Mapped[str] = mapped_column(String(64), nullable=False)
    tenant_id: Mapped[str] = mapped_column(String(20), nullable=False)
    call_id: Mapped[str] = mapped_column(String(64), nullable=False)
    handoff_id: Mapped[str] = mapped_column(String(64), nullable=False)
    agent_identity: Mapped[str] = mapped_column(String(128), nullable=False)
    disposition_code: Mapped[str] = mapped_column(String(32), nullable=False)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    needs_follow_up: Mapped[bool] = mapped_column(Boolean, nullable=False)
    submitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AiCallFollowUpTaskModel(MappedBase):
    """AI Call 人工跟进任务表。"""

    __tablename__ = "ai_call_follow_up_task"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "source_handoff_id", name="uk_ai_call_follow_up_source_handoff"
        ),
        UniqueConstraint(
            "tenant_id",
            "source_type",
            "source_key",
            name="uk_ai_call_follow_up_source_key",
        ),
        Index(
            "idx_ai_call_follow_up_owner_status",
            "tenant_id",
            "owner_agent_identity",
            "status",
        ),
        Index("idx_ai_call_follow_up_scene_status", "tenant_id", "scene_code", "status"),
        {"comment": "AI Call 人工跟进任务表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    tenant_id: Mapped[str] = mapped_column(String(20), nullable=False)
    source_type: Mapped[str] = mapped_column(String(32), nullable=False)
    source_key: Mapped[str] = mapped_column(String(160), nullable=False)
    source_call_id: Mapped[str] = mapped_column(String(64), nullable=False)
    source_handoff_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    scene_code: Mapped[str] = mapped_column(String(64), nullable=False)
    business_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    business_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    contact_ref: Mapped[str] = mapped_column(String(128), nullable=False)
    masked_contact: Mapped[str] = mapped_column(String(64), nullable=False)
    owner_agent_identity: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    follow_up_reason: Mapped[str] = mapped_column(String(500), nullable=False)
    customer_callback_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    closed_reason: Mapped[str | None] = mapped_column(String(32), nullable=True)
    closed_remark: Mapped[str | None] = mapped_column(String(500), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AiCallFollowUpAttemptModel(MappedBase):
    """AI Call 跟进联系尝试表。"""

    __tablename__ = "ai_call_follow_up_attempt"
    __table_args__ = (
        Index(
            "idx_ai_call_follow_up_attempt_time",
            "tenant_id",
            "follow_up_id",
            "contacted_at",
        ),
        Index("idx_ai_call_follow_up_attempt_call", "tenant_id", "related_call_id"),
        {"comment": "AI Call 跟进联系尝试表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    tenant_id: Mapped[str] = mapped_column(String(20), nullable=False)
    follow_up_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    agent_identity: Mapped[str] = mapped_column(String(128), nullable=False)
    contact_channel: Mapped[str] = mapped_column(String(32), nullable=False)
    attempt_result: Mapped[str] = mapped_column(String(32), nullable=False)
    related_call_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    ring_duration_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(500), nullable=True)
    remark: Mapped[str | None] = mapped_column(String(500), nullable=True)
    contacted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    customer_callback_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AiCallFollowUpHandlingResultModel(MappedBase):
    """AI Call 跟进处理结果表。"""

    __tablename__ = "ai_call_follow_up_handling_result"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "idempotency_key",
            name="uk_ai_call_follow_up_handling_key",
        ),
        UniqueConstraint(
            "tenant_id",
            "related_call_id",
            name="uk_ai_call_follow_up_handling_call",
        ),
        Index(
            "idx_ai_call_follow_up_handling_time",
            "tenant_id",
            "follow_up_id",
            "handled_at",
        ),
        {"comment": "AI Call 跟进处理结果表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    tenant_id: Mapped[str] = mapped_column(String(20), nullable=False)
    follow_up_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    related_call_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    contact_channel: Mapped[str] = mapped_column(String(32), nullable=False)
    contact_result: Mapped[str] = mapped_column(String(32), nullable=False)
    remark: Mapped[str] = mapped_column(String(500), nullable=False)
    next_action: Mapped[str] = mapped_column(String(16), nullable=False)
    next_follow_up_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    closed_reason: Mapped[str | None] = mapped_column(String(32), nullable=True)
    agent_identity: Mapped[str] = mapped_column(String(128), nullable=False)
    handled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AiCallPromptProfileModel(MappedBase):
    """AI Call B4 业务提示词配置表。"""

    __tablename__ = "ai_call_prompt_profile"
    __table_args__ = (
        UniqueConstraint("scene_code", name="uk_ai_call_prompt_profile_scene"),
        {"comment": "AI Call 业务提示词配置表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="雪花主键",
    )
    scene_code: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="场景编码",
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False, comment="配置名称")
    provider_key: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="提示词来源模式",
    )
    prompt_text: Mapped[str | None] = mapped_column(Text, nullable=True, comment="固定提示词")
    opening_message: Mapped[str | None] = mapped_column(
        String(1000),
        nullable=True,
        comment="固定开场白",
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


class AiCallVoiceProfileModel(MappedBase):
    """AI Call 端到端音色配置表。"""

    __tablename__ = "ai_call_voice_profile"
    __table_args__ = (
        UniqueConstraint("target_model", "voice", name="uk_ai_call_voice_model_voice"),
        Index("idx_ai_call_voice_model_sort", "target_model", "sort_order"),
        {"comment": "AI Call 端到端音色配置表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="雪花主键",
    )
    voice: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="Qwen Realtime voice 参数",
    )
    display_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        comment="音色展示名",
    )
    voice_type: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        comment="音色类型：内置/自定义复刻",
    )
    gender: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="未知",
        comment="音色性别：未知/女声/男声",
    )
    target_model: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="适用 Qwen Omni Realtime 模型",
    )
    description: Mapped[str | None] = mapped_column(
        String(500),
        nullable=True,
        comment="官方描述或备注说明",
    )
    sort_order: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="排序",
    )
    remark: Mapped[str | None] = mapped_column(
        String(500),
        nullable=True,
        default="",
        comment="备注",
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
