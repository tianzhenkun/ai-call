-- Phase B2/B2.5 离线 ASR 任务表增量 DDL
-- 约束：不使用 jsonb，不创建物理外键，bigint ID 由应用侧雪花算法生成。

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
