create table if not exists ai_call_prompt_common_config (
    id bigint primary key,
    tenant_id varchar(20) not null,
    content text not null default '',
    updated_at timestamptz not null,
    constraint uk_ai_call_prompt_common_tenant unique (tenant_id)
);

comment on table ai_call_prompt_common_config is 'AI Call 租户通用业务提示词模板';
