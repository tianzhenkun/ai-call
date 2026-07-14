-- Phase B5 AI Call semantic analysis PostgreSQL DDL.
-- 约束：不使用 jsonb，不创建物理外键，bigint ID 由应用侧雪花算法生成。

alter table if exists ai_call_record
    add column if not exists scene_code varchar(64);

alter table if exists ai_call_record
    add column if not exists prompt_source_key varchar(64);

comment on column ai_call_record.scene_code is '业务场景编码';
comment on column ai_call_record.prompt_source_key is '提示词来源键';

create table if not exists ai_call_semantic_analysis (
    id bigint not null,
    call_id varchar(64) not null,
    scene_code varchar(64),
    analysis_scene_code varchar(64) not null,
    analysis_status varchar(16) not null,
    analysis_result text,
    analysis_error varchar(1000),
    analysis_retry_count integer not null default 0,
    analysis_started_at timestamp with time zone,
    analysis_finished_at timestamp with time zone,
    transcript_hash varchar(128),
    transcript_snapshot_json text,
    created_at timestamp with time zone not null,
    updated_at timestamp with time zone not null,
    constraint pk_ai_call_semantic_analysis primary key (id),
    constraint uk_ai_call_semantic_call_scene unique (call_id, analysis_scene_code)
);

comment on table ai_call_semantic_analysis is 'AI Call 通话后语义分析记录表';
comment on column ai_call_semantic_analysis.call_id is '通话业务ID';
comment on column ai_call_semantic_analysis.scene_code is '业务场景编码';
comment on column ai_call_semantic_analysis.analysis_scene_code is '分析场景编码';
comment on column ai_call_semantic_analysis.analysis_status is '分析状态：0待分析/1分析中/2成功/3失败/4无有效用户输入';
comment on column ai_call_semantic_analysis.analysis_result is '五字段语义分析 JSON';
comment on column ai_call_semantic_analysis.analysis_error is '分析错误或无需分析原因';
comment on column ai_call_semantic_analysis.analysis_retry_count is '分析失败重试次数';
comment on column ai_call_semantic_analysis.transcript_hash is '转写快照哈希';
comment on column ai_call_semantic_analysis.transcript_snapshot_json is '本次分析使用的转写快照 JSON';

create index if not exists idx_ai_call_semantic_call_id
    on ai_call_semantic_analysis (call_id);

create index if not exists idx_ai_call_semantic_status_updated
    on ai_call_semantic_analysis (analysis_status, updated_at);

create index if not exists idx_ai_call_semantic_scene_status
    on ai_call_semantic_analysis (scene_code, analysis_status);
