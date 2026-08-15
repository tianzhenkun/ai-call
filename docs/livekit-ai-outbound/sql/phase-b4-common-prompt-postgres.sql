create table if not exists ai_call_prompt_common_config (
    id integer primary key,
    content text not null default '',
    updated_at timestamptz not null
);

comment on table ai_call_prompt_common_config is 'AI Call 全局通用业务提示词';
comment on column ai_call_prompt_common_config.content is '所有业务场景共同继承的提示词';
