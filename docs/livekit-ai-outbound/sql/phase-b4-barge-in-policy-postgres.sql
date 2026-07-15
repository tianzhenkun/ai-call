alter table ai_call_prompt_profile
    add column if not exists barge_in_enabled boolean not null default false;

comment on column ai_call_prompt_profile.barge_in_enabled is '是否允许当前场景启用通话打断';

update ai_call_prompt_profile
set barge_in_enabled = (scene_code = 'intro_collection')
where scene_code in (
    'intro_collection',
    'intro_contract',
    'intro_document',
    'intro_overseas',
    'intro_geo'
);
