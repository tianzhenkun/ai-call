-- Phase H12 回滚：仅回滚本阶段结构，不删除既有回访任务和通话数据。

drop index if exists idx_ai_call_follow_up_handling_data_time;
drop index if exists uk_ai_call_follow_up_data_active_task;

do $$
begin
    if exists (
        select 1
        from ai_call_follow_up_handling_result
        where follow_up_id is null
    ) then
        raise exception
            'cannot roll back while follow-up-data-only handling results exist';
    end if;
end
$$;

alter table if exists ai_call_follow_up_handling_result
    drop constraint if exists ck_ai_call_follow_up_handling_parent,
    alter column follow_up_id set not null,
    drop column if exists request_fingerprint,
    drop column if exists follow_up_data_id;

alter table if exists ai_call_follow_up_task
    drop column if exists follow_up_data_id;

alter table if exists ai_call_semantic_analysis
    drop column if exists analysis_version;

drop index if exists idx_ai_call_record_operator_started;
drop index if exists idx_ai_call_record_follow_up_data;

alter table if exists ai_call_record
    drop column if exists operator_agent_identity,
    drop column if exists follow_up_data_id;

drop index if exists idx_ai_call_follow_up_history_data_time;
drop table if exists ai_call_follow_up_classification_history;

drop index if exists idx_ai_call_follow_up_data_review;
drop index if exists idx_ai_call_follow_up_data_classification;
drop table if exists ai_call_follow_up_data;
