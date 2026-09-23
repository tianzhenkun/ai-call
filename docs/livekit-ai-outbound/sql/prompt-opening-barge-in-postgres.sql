-- 部署 API 和运行时前执行；旧场景继续沿用原有打断行为。
begin;
alter table ai_call_prompt_profile
    add column if not exists opening_barge_in_enabled boolean not null default true;
comment on column ai_call_prompt_profile.opening_barge_in_enabled is
    '开场白允许打断，仍受系统总开关限制';
commit;
