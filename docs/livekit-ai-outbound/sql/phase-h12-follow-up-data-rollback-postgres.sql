-- Phase H12 回滚：仅回滚本阶段结构，不删除既有回访任务和通话数据。

drop index if exists idx_ai_call_follow_up_handling_data_time;
drop index if exists uk_ai_call_follow_up_data_active_task;
drop index if exists uk_ai_call_acw_idempotency;

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
    if exists (
        select 1
        from ai_call_follow_up_handling_result
        where length(remark) > 500
    ) then
        raise exception
            'cannot roll back while handling result remarks exceed 500 characters';
    end if;
end
$$;

alter table if exists ai_call_follow_up_handling_result
    drop constraint if exists ck_ai_call_follow_up_handling_parent,
    drop constraint if exists ck_ai_call_follow_up_handling_classification,
    drop constraint if exists ck_ai_call_follow_up_handling_low_value_reason,
    drop constraint if exists ck_ai_call_follow_up_handling_low_value_required,
    drop constraint if exists ck_ai_call_follow_up_handling_result_version,
    alter column follow_up_id set not null,
    alter column remark type varchar(500),
    drop column if exists result_version,
    drop column if exists low_value_reason,
    drop column if exists classification,
    drop column if exists request_fingerprint,
    drop column if exists follow_up_data_id;

do $$
begin
    if exists (
        select 1
        from ai_call_after_call_work
        where disposition_code is null or needs_follow_up is null
    ) then
        raise exception
            'cannot roll back while classification-based after-call work exists';
    end if;
end
$$;

alter table if exists ai_call_after_call_work
    drop constraint if exists ck_ai_call_acw_classification,
    drop constraint if exists ck_ai_call_acw_low_value_reason,
    drop constraint if exists ck_ai_call_acw_low_value_required,
    drop constraint if exists ck_ai_call_acw_idempotency,
    drop constraint if exists ck_ai_call_acw_result_version,
    alter column disposition_code set not null,
    alter column needs_follow_up set not null,
    drop column if exists result_version,
    drop column if exists next_follow_up_at,
    drop column if exists low_value_reason,
    drop column if exists classification,
    drop column if exists request_fingerprint,
    drop column if exists idempotency_key,
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
drop index if exists idx_ai_call_follow_up_schedule_data_time;
drop table if exists ai_call_follow_up_schedule_request;
drop table if exists ai_call_follow_up_classification_history;

drop index if exists idx_ai_call_follow_up_data_review;
drop index if exists idx_ai_call_follow_up_data_classification;
drop table if exists ai_call_follow_up_data;
