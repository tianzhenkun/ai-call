from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, Boolean, DateTime, Index, Integer, String, Text, UniqueConstraint
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


class AiCallRecordingModel(MappedBase):
    """AI Call B2 通话录音表。"""

    __tablename__ = "ai_call_recording"
    __table_args__ = (
        UniqueConstraint("call_id", name="uk_ai_call_recording_call_id"),
        Index("idx_ai_call_recording_status_started", "status", "started_at"),
        Index("idx_ai_call_recording_egress_id", "egress_id"),
        Index("idx_ai_call_recording_oss_id", "oss_id"),
        Index("idx_ai_call_recording_verify_due", "status", "next_verify_at"),
        {"comment": "AI Call 通话录音表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="雪花主键",
    )
    call_id: Mapped[str] = mapped_column(String(64), nullable=False, comment="通话业务ID")
    room_name: Mapped[str] = mapped_column(String(128), nullable=False, comment="LiveKit Room")
    status: Mapped[str] = mapped_column(String(32), nullable=False, comment="录音状态")
    egress_id: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
        comment="LiveKit Egress ID",
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
            "call_id",
            "track_role",
            "participant_identity",
            name="uk_ai_call_recording_track_participant",
        ),
        Index("idx_ai_call_recording_track_call_role", "call_id", "track_role"),
        Index("idx_ai_call_recording_track_egress_id", "egress_id"),
        Index("idx_ai_call_recording_track_oss_id", "oss_id"),
        Index("idx_ai_call_recording_track_verify_due", "status", "next_verify_at"),
        {"comment": "AI Call 分参与方录音明细表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="雪花主键",
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
        UniqueConstraint("call_id", "segment_no", name="uk_ai_call_dialogue_call_no"),
        UniqueConstraint(
            "call_id",
            "speaker_type",
            "source",
            "source_segment_id",
            name="uk_ai_call_dialogue_source_segment",
        ),
        Index("idx_ai_call_dialogue_speaker", "call_id", "speaker_type", "segment_no"),
        {"comment": "AI Call 对话文本段表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="雪花主键",
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


class AiCallHandoffModel(MappedBase):
    """AI Call B3 转人工记录表。"""

    __tablename__ = "ai_call_handoff"
    __table_args__ = (
        UniqueConstraint("handoff_id", name="uk_ai_call_handoff_handoff_id"),
        Index("idx_ai_call_handoff_call_requested", "call_id", "requested_at"),
        Index("idx_ai_call_handoff_status_requested", "status", "requested_at"),
        {"comment": "AI Call 转人工记录表"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="雪花主键",
    )
    handoff_id: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="转人工业务ID",
    )
    call_id: Mapped[str] = mapped_column(String(64), nullable=False, comment="通话业务ID")
    room_name: Mapped[str] = mapped_column(String(128), nullable=False, comment="LiveKit Room")
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
        UniqueConstraint("agent_identity", name="uk_ai_call_handoff_agent_identity"),
        Index("idx_ai_call_handoff_agent_status", "status", "skill_group"),
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
    barge_in_enabled: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default="false",
        comment="是否允许当前场景启用通话打断",
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
