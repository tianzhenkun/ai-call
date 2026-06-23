-- Phase B3 handoff agent status migration for existing PostgreSQL databases.
-- Purpose: persist the minimal human-agent availability state used by handoff acceptance.

create table if not exists ai_call_handoff_agent (
    id bigint primary key,
    agent_identity varchar(128) not null,
    skill_group varchar(64) not null default 'default',
    status varchar(32) not null,
    active_handoff_id varchar(64),
    last_seen_at timestamp with time zone,
    status_updated_at timestamp with time zone not null,
    constraint uk_ai_call_handoff_agent_identity unique (agent_identity)
);

create index if not exists idx_ai_call_handoff_agent_status
    on ai_call_handoff_agent (status, skill_group);

create index if not exists idx_ai_call_handoff_agent_active
    on ai_call_handoff_agent (active_handoff_id);
