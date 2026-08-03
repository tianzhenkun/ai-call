begin;

alter table ai_call_recording
    add column if not exists tenant_id varchar(20),
    add column if not exists egress_generation bigint;

do $$
begin
    if exists (
        select 1
        from ai_call_recording recording
        left join ai_call_record record
          on record.call_id = recording.call_id
        where recording.tenant_id is null
          and (record.call_id is null or record.tenant_id is null)
    ) then
        raise exception 'ai_call_recording_tenant_backfill_failed';
    end if;
end
$$;

update ai_call_recording recording
set tenant_id = record.tenant_id
from ai_call_record record
where record.call_id = recording.call_id
  and recording.tenant_id is null;

alter table ai_call_recording
    alter column tenant_id set not null,
    drop constraint if exists uk_ai_call_recording_call_id;

drop index if exists uk_ai_call_recording_call_id;
drop index if exists idx_ai_call_recording_status_started;
drop index if exists idx_ai_call_recording_egress_id;
drop index if exists idx_ai_call_recording_oss_id;
drop index if exists idx_ai_call_recording_verify_due;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'uk_ai_call_recording_tenant_call'
          and conrelid = 'ai_call_recording'::regclass
    ) then
        alter table ai_call_recording
            add constraint uk_ai_call_recording_tenant_call
            unique (tenant_id, call_id);
    end if;
end
$$;

create index if not exists idx_ai_call_recording_tenant_started
    on ai_call_recording (tenant_id, status, started_at);

create index if not exists idx_ai_call_recording_tenant_egress
    on ai_call_recording (tenant_id, egress_id);

create index if not exists idx_ai_call_recording_tenant_oss
    on ai_call_recording (tenant_id, oss_id);

create index if not exists idx_ai_call_recording_tenant_verify_due
    on ai_call_recording (tenant_id, status, next_verify_at);

commit;
