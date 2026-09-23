-- 先执行迁移，再部署含表达风格字段的 API 与运行时。
begin;
alter table ai_call_tenant_voice_profile
    add column if not exists speaking_style varchar(32) not null default 'natural';
do $$
begin
    if not exists (
        select 1 from pg_constraint
        where conrelid = 'ai_call_tenant_voice_profile'::regclass
          and conname = 'ck_tenant_voice_speaking_style'
    ) then
        alter table ai_call_tenant_voice_profile
            add constraint ck_tenant_voice_speaking_style
            check (speaking_style in ('natural','gentle','professional','lively','serious'));
    end if;
end $$;
comment on column ai_call_tenant_voice_profile.speaking_style is
    '表达风格：natural 自然/gentle 温和/professional 专业/lively 活泼/serious 严肃';
commit;
