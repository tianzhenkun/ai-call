-- Phase H12: 跟进数据与回访任务解耦（PostgreSQL，可重复执行）

create table if not exists ai_call_follow_up_data (
    id bigint primary key,
    tenant_id varchar(20) not null,
    task_id bigint not null,
    target_id bigint not null,
    source_call_id varchar(64) not null,
    classification varchar(16),
    classification_reason varchar(500),
    classification_source varchar(16),
    classification_confidence varchar(16),
    suggest_review boolean not null default false,
    low_value_reason varchar(32),
    latest_conclusion text,
    last_contact_at timestamptz,
    blocking_human_call_id varchar(64),
    version integer not null default 1,
    classification_updated_at timestamptz,
    classification_updated_by varchar(128),
    created_at timestamptz not null,
    updated_at timestamptz not null,
    constraint uk_ai_call_follow_up_data_target
        unique (tenant_id, task_id, target_id),
    constraint ck_ai_call_follow_up_data_classification check (
        classification is null or classification in
            ('interested', 'nurturing', 'low_value', 'converted')
    ),
    constraint ck_ai_call_follow_up_data_source check (
        classification_source is null or classification_source in
            ('ai', 'human', 'system')
    ),
    constraint ck_ai_call_follow_up_data_confidence check (
        classification_confidence is null or classification_confidence in
            ('high', 'medium', 'low')
    ),
    constraint ck_ai_call_follow_up_data_low_value_reason check (
        low_value_reason is null or low_value_reason in
            ('explicit_rejection', 'no_current_need', 'customer_mismatch',
             'non_target_customer', 'invalid_contact', 'other')
    ),
    constraint ck_ai_call_follow_up_data_classification_source check (
        (classification is null and classification_source is null)
        or (classification is not null and classification_source is not null)
    ),
    constraint ck_ai_call_follow_up_data_low_value_required check (
        classification <> 'low_value' or low_value_reason is not null
    ),
    constraint ck_ai_call_follow_up_data_version check (version > 0)
);

create index if not exists idx_ai_call_follow_up_data_classification
    on ai_call_follow_up_data (tenant_id, classification, last_contact_at);
create index if not exists idx_ai_call_follow_up_data_review
    on ai_call_follow_up_data (tenant_id, suggest_review, updated_at);

create table if not exists ai_call_follow_up_classification_history (
    id bigint primary key,
    tenant_id varchar(20) not null,
    follow_up_data_id bigint not null,
    from_classification varchar(16),
    to_classification varchar(16) not null,
    change_reason varchar(500) not null,
    source varchar(32) not null,
    call_id varchar(64),
    semantic_analysis_id bigint,
    semantic_analysis_version integer,
    ai_suggested_classification varchar(16),
    ai_confidence varchar(16),
    ai_reason varchar(500),
    ai_evidence_json text,
    ai_conflict boolean,
    ai_adopted boolean,
    idempotency_key varchar(128),
    request_fingerprint varchar(64),
    result_version integer not null default 1,
    changed_by varchar(128),
    changed_by_name varchar(100),
    created_at timestamptz not null,
    constraint uk_ai_call_follow_up_history_analysis
        unique (tenant_id, semantic_analysis_id, semantic_analysis_version),
    constraint uk_ai_call_follow_up_history_idempotency
        unique (tenant_id, idempotency_key),
    constraint ck_ai_call_follow_up_history_from check (
        from_classification is null or from_classification in
            ('interested', 'nurturing', 'low_value', 'converted')
    ),
    constraint ck_ai_call_follow_up_history_to check (
        to_classification in ('interested', 'nurturing', 'low_value', 'converted')
    ),
    constraint ck_ai_call_follow_up_history_source check (
        source in ('ai_auto', 'handoff_after_call', 'manual_outbound',
                   'manual_adjustment', 'transfer_failed')
    ),
    constraint ck_ai_call_follow_up_history_ai_suggestion check (
        ai_suggested_classification is null or ai_suggested_classification in
            ('interested', 'nurturing', 'low_value')
    ),
    constraint ck_ai_call_follow_up_history_ai_confidence check (
        ai_confidence is null or ai_confidence in ('high', 'medium', 'low')
    ),
    constraint ck_ai_call_follow_up_history_idempotency check (
        (idempotency_key is null and request_fingerprint is null)
        or (idempotency_key is not null and request_fingerprint is not null)
    ),
    constraint ck_ai_call_follow_up_history_result_version check (
        result_version > 0
    )
);

alter table ai_call_follow_up_classification_history
    add column if not exists idempotency_key varchar(128),
    add column if not exists request_fingerprint varchar(64),
    add column if not exists result_version integer not null default 1;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'ck_ai_call_follow_up_history_idempotency'
          and conrelid = 'ai_call_follow_up_classification_history'::regclass
    ) then
        alter table ai_call_follow_up_classification_history
            add constraint ck_ai_call_follow_up_history_idempotency check (
                (idempotency_key is null and request_fingerprint is null)
                or (idempotency_key is not null and request_fingerprint is not null)
            );
    end if;
    if not exists (
        select 1
        from pg_constraint
        where conname = 'ck_ai_call_follow_up_history_result_version'
          and conrelid = 'ai_call_follow_up_classification_history'::regclass
    ) then
        alter table ai_call_follow_up_classification_history
            add constraint ck_ai_call_follow_up_history_result_version check (
                result_version > 0
            );
    end if;
end
$$;

create unique index if not exists uk_ai_call_follow_up_history_idempotency
    on ai_call_follow_up_classification_history (tenant_id, idempotency_key);

create index if not exists idx_ai_call_follow_up_history_data_time
    on ai_call_follow_up_classification_history
        (tenant_id, follow_up_data_id, created_at);

create table if not exists ai_call_follow_up_schedule_request (
    id bigint primary key,
    tenant_id varchar(20) not null,
    follow_up_data_id bigint not null,
    follow_up_id bigint not null,
    idempotency_key varchar(128) not null,
    request_fingerprint varchar(64) not null,
    result_version integer not null,
    changed_by varchar(128) not null,
    changed_by_name varchar(100),
    created_at timestamptz not null,
    constraint uk_ai_call_follow_up_schedule_key
        unique (tenant_id, idempotency_key),
    constraint ck_ai_call_follow_up_schedule_result_version check (
        result_version > 0
    )
);

create index if not exists idx_ai_call_follow_up_schedule_data_time
    on ai_call_follow_up_schedule_request
        (tenant_id, follow_up_data_id, created_at);

alter table ai_call_record
    add column if not exists follow_up_data_id bigint,
    add column if not exists operator_agent_identity varchar(128);

create index if not exists idx_ai_call_record_follow_up_data
    on ai_call_record (tenant_id, follow_up_data_id);
create index if not exists idx_ai_call_record_operator_started
    on ai_call_record (tenant_id, operator_agent_identity, started_at);

alter table ai_call_semantic_analysis
    add column if not exists analysis_version integer not null default 0;

update ai_call_semantic_analysis
set analysis_version = 1
where analysis_status = '2'
  and analysis_version = 0;

alter table ai_call_follow_up_task
    add column if not exists follow_up_data_id bigint;

alter table ai_call_after_call_work
    add column if not exists follow_up_data_id bigint,
    add column if not exists idempotency_key varchar(128),
    add column if not exists request_fingerprint varchar(64),
    add column if not exists classification varchar(16),
    add column if not exists low_value_reason varchar(32),
    add column if not exists next_follow_up_at timestamptz,
    add column if not exists result_version integer,
    alter column disposition_code drop not null,
    alter column needs_follow_up drop not null;

create unique index if not exists uk_ai_call_acw_idempotency
    on ai_call_after_call_work (tenant_id, idempotency_key);

alter table ai_call_follow_up_handling_result
    add column if not exists follow_up_data_id bigint,
    add column if not exists request_fingerprint varchar(64),
    add column if not exists classification varchar(16),
    add column if not exists low_value_reason varchar(32),
    add column if not exists result_version integer,
    alter column remark type text,
    alter column follow_up_id drop not null;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'ck_ai_call_acw_classification'
          and conrelid = 'ai_call_after_call_work'::regclass
    ) then
        alter table ai_call_after_call_work
            add constraint ck_ai_call_acw_classification check (
                classification is null or classification in
                    ('interested', 'nurturing', 'low_value')
            );
    end if;
    if not exists (
        select 1
        from pg_constraint
        where conname = 'ck_ai_call_acw_low_value_reason'
          and conrelid = 'ai_call_after_call_work'::regclass
    ) then
        alter table ai_call_after_call_work
            add constraint ck_ai_call_acw_low_value_reason check (
                low_value_reason is null or low_value_reason in
                    ('explicit_rejection', 'no_current_need', 'customer_mismatch',
                     'non_target_customer', 'other')
            );
    end if;
    if not exists (
        select 1
        from pg_constraint
        where conname = 'ck_ai_call_acw_low_value_required'
          and conrelid = 'ai_call_after_call_work'::regclass
    ) then
        alter table ai_call_after_call_work
            add constraint ck_ai_call_acw_low_value_required check (
                classification <> 'low_value' or low_value_reason is not null
            );
    end if;
    if not exists (
        select 1
        from pg_constraint
        where conname = 'ck_ai_call_acw_idempotency'
          and conrelid = 'ai_call_after_call_work'::regclass
    ) then
        alter table ai_call_after_call_work
            add constraint ck_ai_call_acw_idempotency check (
                (idempotency_key is null and request_fingerprint is null)
                or (idempotency_key is not null and request_fingerprint is not null)
            );
    end if;
    if not exists (
        select 1
        from pg_constraint
        where conname = 'ck_ai_call_acw_result_version'
          and conrelid = 'ai_call_after_call_work'::regclass
    ) then
        alter table ai_call_after_call_work
            add constraint ck_ai_call_acw_result_version check (
                result_version is null or result_version > 0
            );
    end if;
    if not exists (
        select 1
        from pg_constraint
        where conname = 'ck_ai_call_follow_up_handling_parent'
          and conrelid = 'ai_call_follow_up_handling_result'::regclass
    ) then
        alter table ai_call_follow_up_handling_result
            add constraint ck_ai_call_follow_up_handling_parent check (
                follow_up_id is not null or follow_up_data_id is not null
            );
    end if;
    if not exists (
        select 1
        from pg_constraint
        where conname = 'ck_ai_call_follow_up_handling_classification'
          and conrelid = 'ai_call_follow_up_handling_result'::regclass
    ) then
        alter table ai_call_follow_up_handling_result
            add constraint ck_ai_call_follow_up_handling_classification check (
                classification is null or classification in
                    ('interested', 'nurturing', 'low_value', 'converted')
            );
    end if;
    if not exists (
        select 1
        from pg_constraint
        where conname = 'ck_ai_call_follow_up_handling_low_value_reason'
          and conrelid = 'ai_call_follow_up_handling_result'::regclass
    ) then
        alter table ai_call_follow_up_handling_result
            add constraint ck_ai_call_follow_up_handling_low_value_reason check (
                low_value_reason is null or low_value_reason in
                    ('explicit_rejection', 'no_current_need', 'customer_mismatch',
                     'non_target_customer', 'invalid_contact', 'other')
            );
    end if;
    if not exists (
        select 1
        from pg_constraint
        where conname = 'ck_ai_call_follow_up_handling_low_value_required'
          and conrelid = 'ai_call_follow_up_handling_result'::regclass
    ) then
        alter table ai_call_follow_up_handling_result
            add constraint ck_ai_call_follow_up_handling_low_value_required check (
                classification <> 'low_value' or low_value_reason is not null
            );
    end if;
    if not exists (
        select 1
        from pg_constraint
        where conname = 'ck_ai_call_follow_up_handling_result_version'
          and conrelid = 'ai_call_follow_up_handling_result'::regclass
    ) then
        alter table ai_call_follow_up_handling_result
            add constraint ck_ai_call_follow_up_handling_result_version check (
                result_version is null or result_version > 0
            );
    end if;
end
$$;

do $$
begin
    if exists (
        select 1
        from ai_call_follow_up_task follow_up
        left join ai_call_outbound_attempt attempt
          on attempt.tenant_id = follow_up.tenant_id
         and attempt.call_id = follow_up.source_call_id
        where follow_up.status in ('pending', 'processing')
          and follow_up.follow_up_data_id is null
          and attempt.id is null
    ) then
        raise exception
            'active legacy follow-up task has no outbound task/target relation';
    end if;

    if exists (
        select 1
        from ai_call_follow_up_task follow_up
        join ai_call_outbound_attempt attempt
          on attempt.tenant_id = follow_up.tenant_id
         and attempt.call_id = follow_up.source_call_id
        where follow_up.status in ('pending', 'processing')
          and follow_up.follow_up_data_id is null
        group by follow_up.tenant_id, attempt.task_id, attempt.target_id
        having count(*) > 1
    ) then
        raise exception
            'multiple active legacy follow-up tasks resolve to one outbound target';
    end if;
end
$$;

insert into ai_call_follow_up_data (
    id,
    tenant_id,
    task_id,
    target_id,
    source_call_id,
    classification,
    classification_source,
    suggest_review,
    latest_conclusion,
    last_contact_at,
    version,
    created_at,
    updated_at
)
select
    follow_up.id,
    follow_up.tenant_id,
    attempt.task_id,
    attempt.target_id,
    follow_up.source_call_id,
    null,
    null,
    false,
    follow_up.summary,
    coalesce(record.ended_at, record.started_at),
    1,
    follow_up.created_at,
    follow_up.updated_at
from ai_call_follow_up_task follow_up
join ai_call_outbound_attempt attempt
  on attempt.tenant_id = follow_up.tenant_id
 and attempt.call_id = follow_up.source_call_id
left join ai_call_record record
  on record.tenant_id = follow_up.tenant_id
 and record.call_id = follow_up.source_call_id
where follow_up.status in ('pending', 'processing')
  and follow_up.follow_up_data_id is null
on conflict (tenant_id, task_id, target_id) do nothing;

update ai_call_follow_up_task follow_up
set follow_up_data_id = data.id
from ai_call_outbound_attempt attempt,
     ai_call_follow_up_data data
where follow_up.status in ('pending', 'processing')
  and follow_up.follow_up_data_id is null
  and attempt.tenant_id = follow_up.tenant_id
  and attempt.call_id = follow_up.source_call_id
  and data.tenant_id = follow_up.tenant_id
  and data.task_id = attempt.task_id
  and data.target_id = attempt.target_id;

update ai_call_record record
set follow_up_data_id = data.id
from ai_call_outbound_attempt attempt,
     ai_call_follow_up_data data
where record.follow_up_data_id is null
  and attempt.tenant_id = record.tenant_id
  and attempt.call_id = record.call_id
  and data.tenant_id = attempt.tenant_id
  and data.task_id = attempt.task_id
  and data.target_id = attempt.target_id;

update ai_call_record record
set follow_up_data_id = follow_up.follow_up_data_id
from ai_call_follow_up_task follow_up
where record.follow_up_data_id is null
  and record.tenant_id = follow_up.tenant_id
  and record.follow_up_id = follow_up.id
  and follow_up.follow_up_data_id is not null;

update ai_call_follow_up_handling_result result
set follow_up_data_id = follow_up.follow_up_data_id
from ai_call_follow_up_task follow_up
where result.follow_up_data_id is null
  and result.tenant_id = follow_up.tenant_id
  and result.follow_up_id = follow_up.id
  and follow_up.follow_up_data_id is not null;

create unique index if not exists uk_ai_call_follow_up_data_active_task
    on ai_call_follow_up_task (tenant_id, follow_up_data_id)
    where follow_up_data_id is not null
      and status in ('pending', 'processing');

create index if not exists idx_ai_call_follow_up_handling_data_time
    on ai_call_follow_up_handling_result
        (tenant_id, follow_up_data_id, handled_at);
