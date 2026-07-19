-- Phase G: 通用浏览器坐席中心数据契约（PostgreSQL，可重复执行）

create table if not exists ai_call_agent_profile (
    id bigint primary key,
    tenant_id varchar(20) not null,
    agent_identity varchar(128) not null,
    user_id bigint not null,
    enabled boolean not null default false,
    created_by bigint not null,
    created_at timestamptz not null,
    updated_by bigint not null,
    updated_at timestamptz not null,
    constraint uk_ai_call_agent_profile_identity unique (tenant_id, agent_identity),
    constraint uk_ai_call_agent_profile_user unique (tenant_id, user_id)
);

create table if not exists ai_call_agent_scene_scope (
    id bigint primary key,
    tenant_id varchar(20) not null,
    agent_identity varchar(128) not null,
    scene_code varchar(64) not null,
    created_by bigint not null,
    created_at timestamptz not null,
    constraint uk_ai_call_agent_scene_scope unique (tenant_id, agent_identity, scene_code)
);

create index if not exists idx_ai_call_agent_scene_scope_scene
    on ai_call_agent_scene_scope (tenant_id, scene_code);

create table if not exists ai_call_after_call_work (
    id bigint primary key,
    work_id varchar(64) not null,
    tenant_id varchar(20) not null,
    call_id varchar(64) not null,
    handoff_id varchar(64) not null,
    agent_identity varchar(128) not null,
    disposition_code varchar(32) not null,
    summary text,
    needs_follow_up boolean not null,
    submitted_at timestamptz not null,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    constraint uk_ai_call_acw_work unique (tenant_id, work_id),
    constraint uk_ai_call_acw_handoff unique (tenant_id, handoff_id)
);

create index if not exists idx_ai_call_acw_tenant_call
    on ai_call_after_call_work (tenant_id, call_id);

create table if not exists ai_call_follow_up_task (
    id bigint primary key,
    tenant_id varchar(20) not null,
    source_type varchar(32) not null,
    source_call_id varchar(64) not null,
    source_handoff_id varchar(64) not null,
    scene_code varchar(64) not null,
    business_type varchar(32),
    business_id varchar(64),
    contact_ref varchar(128) not null,
    masked_contact varchar(64) not null,
    owner_agent_identity varchar(128),
    status varchar(32) not null,
    follow_up_reason varchar(500) not null,
    customer_callback_at timestamptz,
    summary text,
    closed_reason varchar(32),
    closed_remark varchar(500),
    completed_at timestamptz,
    closed_at timestamptz,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    constraint uk_ai_call_follow_up_source_handoff unique (tenant_id, source_handoff_id)
);

create index if not exists idx_ai_call_follow_up_owner_status
    on ai_call_follow_up_task (tenant_id, owner_agent_identity, status);
create index if not exists idx_ai_call_follow_up_scene_status
    on ai_call_follow_up_task (tenant_id, scene_code, status);

create table if not exists ai_call_follow_up_attempt (
    id bigint primary key,
    tenant_id varchar(20) not null,
    follow_up_id bigint not null,
    agent_identity varchar(128) not null,
    contact_channel varchar(32) not null,
    attempt_result varchar(32) not null,
    related_call_id varchar(64),
    ring_duration_seconds integer,
    error_message varchar(500),
    remark varchar(500),
    contacted_at timestamptz not null,
    customer_callback_at timestamptz,
    created_at timestamptz not null
);

create index if not exists idx_ai_call_follow_up_attempt_time
    on ai_call_follow_up_attempt (tenant_id, follow_up_id, contacted_at);
create index if not exists idx_ai_call_follow_up_attempt_call
    on ai_call_follow_up_attempt (tenant_id, related_call_id);

alter table ai_call_handoff add column if not exists tenant_id varchar(20);
alter table ai_call_handoff add column if not exists scene_code varchar(64);
alter table ai_call_handoff add column if not exists accepted_console_session_id varchar(36);
alter table ai_call_handoff add column if not exists claim_expires_at timestamptz;
alter table ai_call_handoff add column if not exists reconnect_expires_at timestamptz;

update ai_call_handoff set tenant_id = '000000' where tenant_id is null;
update ai_call_handoff set scene_code = 'default' where scene_code is null;
alter table ai_call_handoff alter column tenant_id set default '000000';
alter table ai_call_handoff alter column tenant_id set not null;
alter table ai_call_handoff alter column scene_code set default 'default';
alter table ai_call_handoff alter column scene_code set not null;
alter table ai_call_handoff drop constraint if exists uk_ai_call_handoff_handoff_id;

alter table ai_call_handoff_agent add column if not exists tenant_id varchar(20);
alter table ai_call_handoff_agent add column if not exists active_call_id varchar(64);
alter table ai_call_handoff_agent add column if not exists console_session_id varchar(36);

update ai_call_handoff_agent set tenant_id = '000000' where tenant_id is null;
alter table ai_call_handoff_agent alter column tenant_id set default '000000';
alter table ai_call_handoff_agent alter column tenant_id set not null;
alter table ai_call_handoff_agent drop constraint if exists uk_ai_call_handoff_agent_identity;

create unique index if not exists uk_ai_call_handoff_tenant_handoff
    on ai_call_handoff (tenant_id, handoff_id);
create index if not exists idx_ai_call_handoff_tenant_call
    on ai_call_handoff (tenant_id, call_id, requested_at);
create index if not exists idx_ai_call_handoff_tenant_status
    on ai_call_handoff (tenant_id, status, requested_at);
create unique index if not exists uk_ai_call_handoff_agent_tenant_identity
    on ai_call_handoff_agent (tenant_id, agent_identity);
create index if not exists idx_ai_call_handoff_agent_tenant_status
    on ai_call_handoff_agent (tenant_id, status);
