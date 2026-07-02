create table if not exists ai_call_prompt_profile (
    id bigint primary key,
    scene_code varchar(64) not null,
    name varchar(100) not null,
    provider_key varchar(64) not null,
    prompt_text text null,
    opening_message varchar(1000) null,
    barge_in_enabled boolean not null default false,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    constraint uk_ai_call_prompt_profile_scene unique (scene_code)
);

alter table ai_call_prompt_profile
    add column if not exists barge_in_enabled boolean not null default false;

alter table ai_call_prompt_profile drop column if exists remark;
alter table ai_call_prompt_profile drop column if exists business_type;
alter table ai_call_prompt_profile drop column if exists profile_code;
alter table ai_call_prompt_profile drop column if exists status;
alter table ai_call_prompt_profile drop column if exists prompt_mode;
alter table ai_call_prompt_profile drop column if exists opening_mode;
alter table ai_call_prompt_profile drop column if exists opening_enabled;

comment on table ai_call_prompt_profile is 'AI Call 业务提示词配置表';
comment on column ai_call_prompt_profile.scene_code is '业务场景编码，全局唯一';
comment on column ai_call_prompt_profile.name is '配置名称';
comment on column ai_call_prompt_profile.provider_key is '提示词来源模式：static_profile 或 business_query';
comment on column ai_call_prompt_profile.prompt_text is '固定提示词';
comment on column ai_call_prompt_profile.opening_message is '固定开场白';
comment on column ai_call_prompt_profile.barge_in_enabled is '是否允许当前场景启用通话打断';
