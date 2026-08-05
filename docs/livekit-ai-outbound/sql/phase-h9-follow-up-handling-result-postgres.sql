-- Phase H9: 跟进处理结果原子提交（PostgreSQL，可重复执行）

create table if not exists ai_call_follow_up_handling_result (
    id bigint primary key,
    tenant_id varchar(20) not null,
    follow_up_id bigint not null,
    idempotency_key varchar(128) not null,
    related_call_id varchar(64),
    contact_channel varchar(32) not null,
    contact_result varchar(32) not null,
    remark varchar(500) not null,
    next_action varchar(16) not null,
    next_follow_up_at timestamptz,
    closed_reason varchar(32),
    agent_identity varchar(128) not null,
    handled_at timestamptz not null,
    created_at timestamptz not null,
    constraint uk_ai_call_follow_up_handling_key
        unique (tenant_id, idempotency_key),
    constraint uk_ai_call_follow_up_handling_call
        unique (tenant_id, related_call_id)
);

create index if not exists idx_ai_call_follow_up_handling_time
    on ai_call_follow_up_handling_result (tenant_id, follow_up_id, handled_at);
