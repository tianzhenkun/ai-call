-- Phase H7: 结构化话后结果与幂等跟进来源（PostgreSQL，可重复执行）

alter table ai_call_semantic_analysis
    add column if not exists customer_intent varchar(16);
alter table ai_call_semantic_analysis
    add column if not exists follow_up_suggested boolean not null default false;
alter table ai_call_semantic_analysis
    add column if not exists follow_up_consent varchar(16);
alter table ai_call_semantic_analysis
    add column if not exists follow_up_reason varchar(500);
alter table ai_call_semantic_analysis
    add column if not exists follow_up_preferred_at timestamptz;
alter table ai_call_semantic_analysis
    add column if not exists follow_up_confidence varchar(16);

create index if not exists idx_ai_call_semantic_scene_intent
    on ai_call_semantic_analysis (analysis_scene_code, customer_intent);
create index if not exists idx_ai_call_semantic_scene_follow_up
    on ai_call_semantic_analysis (analysis_scene_code, follow_up_suggested);

alter table ai_call_follow_up_task
    add column if not exists source_key varchar(160);

update ai_call_follow_up_task
set source_key = 'handoff:' || source_handoff_id
where source_key is null
  and source_handoff_id is not null;

do $$
begin
    if exists (
        select 1
        from ai_call_follow_up_task
        where source_key is null
    ) then
        raise exception
            'ai_call_follow_up_task contains rows without a recoverable source_key';
    end if;

    if exists (
        select 1
        from ai_call_follow_up_task
        group by tenant_id, source_type, source_key
        having count(*) > 1
    ) then
        raise exception
            'ai_call_follow_up_task contains duplicate source keys';
    end if;
end
$$;

alter table ai_call_follow_up_task
    alter column source_key set not null;
alter table ai_call_follow_up_task
    alter column source_handoff_id drop not null;

create unique index if not exists uk_ai_call_follow_up_source_key
    on ai_call_follow_up_task (tenant_id, source_type, source_key);
