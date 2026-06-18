-- Phase B3 PostgreSQL DDL
-- 约束：不使用 jsonb，不创建物理外键，bigint ID 由应用侧雪花算法生成。

create table if not exists ai_call_handoff (
    id bigint not null,
    handoff_id varchar(64) not null,
    call_id varchar(64) not null,
    room_name varchar(128) not null,
    status varchar(32) not null,
    request_source varchar(32) not null,
    request_reason varchar(64),
    request_message varchar(500),
    human_agent_identity varchar(128),
    requested_at timestamp with time zone not null,
    accepted_at timestamp with time zone,
    connected_at timestamp with time zone,
    ended_at timestamp with time zone,
    expires_at timestamp with time zone,
    end_reason varchar(64),
    failure_stage varchar(64),
    failure_message varchar(500),
    constraint pk_ai_call_handoff primary key (id),
    constraint uk_ai_call_handoff_handoff_id unique (handoff_id)
);

create index if not exists idx_ai_call_handoff_call_requested
    on ai_call_handoff (call_id, requested_at);

create index if not exists idx_ai_call_handoff_status_requested
    on ai_call_handoff (status, requested_at);
