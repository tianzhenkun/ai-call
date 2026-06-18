-- Phase B2/B2.5 PostgreSQL DDL
-- 约束：不使用 jsonb，不创建物理外键，bigint ID 由应用侧雪花算法生成。
-- 注意：本文件用于全新环境或表结构已符合当前方案的环境。
-- 如果库里存在旧版 ai_call_recording 表，create table if not exists 会跳过建表，
-- 后续索引可能因旧字段缺失而失败。测试库重建请使用 phase-b2-b25-postgres-reset-dev.sql。

create table if not exists ai_call_recording (
    id bigint not null,
    call_id varchar(64) not null,
    room_name varchar(128) not null,
    status varchar(32) not null,
    egress_id varchar(128),
    oss_id bigint,
    object_name varchar(255),
    started_at timestamp with time zone not null,
    ended_at timestamp with time zone,
    duration_ms integer,
    failure_stage varchar(64),
    failure_message varchar(500),
    stop_requested_at timestamp with time zone,
    verify_attempts integer,
    next_verify_at timestamp with time zone,
    verify_deadline_at timestamp with time zone,
    last_verify_at timestamp with time zone,
    last_verify_error varchar(500),
    constraint pk_ai_call_recording primary key (id),
    constraint uk_ai_call_recording_call_id unique (call_id)
);

create index if not exists idx_ai_call_recording_status_started
    on ai_call_recording (status, started_at);

create index if not exists idx_ai_call_recording_egress_id
    on ai_call_recording (egress_id);

create index if not exists idx_ai_call_recording_oss_id
    on ai_call_recording (oss_id);

create index if not exists idx_ai_call_recording_verify_due
    on ai_call_recording (status, next_verify_at);

create table if not exists ai_call_recording_track (
    id bigint not null,
    call_id varchar(64) not null,
    room_name varchar(128) not null,
    track_role varchar(32) not null,
    participant_identity varchar(128) not null,
    handoff_id varchar(64),
    status varchar(32) not null,
    egress_id varchar(128),
    oss_id bigint,
    object_name varchar(255),
    started_at timestamp with time zone not null,
    ended_at timestamp with time zone,
    duration_ms integer,
    failure_stage varchar(64),
    failure_message varchar(500),
    stop_requested_at timestamp with time zone,
    verify_attempts integer,
    next_verify_at timestamp with time zone,
    verify_deadline_at timestamp with time zone,
    last_verify_at timestamp with time zone,
    last_verify_error varchar(500),
    constraint pk_ai_call_recording_track primary key (id),
    constraint uk_ai_call_recording_track_participant
        unique (call_id, track_role, participant_identity)
);

create index if not exists idx_ai_call_recording_track_call_role
    on ai_call_recording_track (call_id, track_role);

create index if not exists idx_ai_call_recording_track_egress_id
    on ai_call_recording_track (egress_id);

create index if not exists idx_ai_call_recording_track_oss_id
    on ai_call_recording_track (oss_id);

create index if not exists idx_ai_call_recording_track_verify_due
    on ai_call_recording_track (status, next_verify_at);

create table if not exists ai_call_dialogue_segment (
    id bigint not null,
    call_id varchar(64) not null,
    segment_no integer not null,
    speaker_type varchar(32) not null,
    speaker_identity varchar(128),
    source varchar(32) not null,
    source_segment_id varchar(128) not null,
    segment_text text not null,
    segment_status varchar(32) not null,
    started_at timestamp with time zone,
    ended_at timestamp with time zone,
    duration_ms integer,
    audio_start_ms integer,
    audio_end_ms integer,
    failure_stage varchar(64),
    failure_message varchar(500),
    constraint pk_ai_call_dialogue_segment primary key (id),
    constraint uk_ai_call_dialogue_call_no unique (call_id, segment_no),
    constraint uk_ai_call_dialogue_source_segment unique (call_id, speaker_type, source, source_segment_id)
);

create index if not exists idx_ai_call_dialogue_speaker
    on ai_call_dialogue_segment (call_id, speaker_type, segment_no);

create table if not exists ai_call_asr_job (
    id bigint not null,
    call_id varchar(64) not null,
    track_id bigint not null,
    track_role varchar(32) not null,
    participant_identity varchar(128) not null,
    provider varchar(32) not null,
    model varchar(64) not null,
    status varchar(32) not null,
    task_id varchar(128),
    source_url varchar(1000),
    transcription_url varchar(1000),
    submitted_at timestamp with time zone,
    completed_at timestamp with time zone,
    segment_count integer,
    failure_stage varchar(64),
    failure_message varchar(500),
    constraint pk_ai_call_asr_job primary key (id),
    constraint uk_ai_call_asr_job_track_provider unique (track_id, provider, model)
);

create index if not exists idx_ai_call_asr_job_call_status
    on ai_call_asr_job (call_id, status);

create index if not exists idx_ai_call_asr_job_track_id
    on ai_call_asr_job (track_id);

create index if not exists idx_ai_call_asr_job_task_id
    on ai_call_asr_job (task_id);
