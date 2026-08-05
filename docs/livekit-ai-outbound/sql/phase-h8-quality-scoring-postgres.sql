-- Phase H8 AI Call quality scoring migration for existing PostgreSQL databases.
-- Purpose: persist AI scoring and the human review of that score.

create table if not exists ai_call_quality_score (
    id bigint primary key,
    tenant_id varchar(20) not null,
    call_id varchar(64) not null,
    status varchar(16) not null,
    score integer,
    reason text,
    model_version varchar(64) not null default 'quality-v1',
    retry_count integer not null default 0,
    error_message varchar(500),
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    created_at timestamp with time zone not null,
    updated_at timestamp with time zone not null,
    constraint uk_ai_call_quality_score_call_model
        unique (tenant_id, call_id, model_version)
);

create index if not exists idx_ai_call_quality_score_tenant_status
    on ai_call_quality_score (tenant_id, status, updated_at);

create index if not exists idx_ai_call_quality_score_call
    on ai_call_quality_score (call_id);

create table if not exists ai_call_quality_review (
    id bigint primary key,
    tenant_id varchar(20) not null,
    call_id varchar(64) not null,
    quality_result varchar(16) not null,
    quality_reason varchar(500),
    reviewed_by varchar(64) not null,
    reviewed_by_name varchar(64),
    reviewed_at timestamp with time zone not null,
    created_at timestamp with time zone not null,
    updated_at timestamp with time zone not null,
    constraint uk_ai_call_quality_review_call unique (tenant_id, call_id)
);

create index if not exists idx_ai_call_quality_review_tenant_result
    on ai_call_quality_review (tenant_id, quality_result, reviewed_at);
