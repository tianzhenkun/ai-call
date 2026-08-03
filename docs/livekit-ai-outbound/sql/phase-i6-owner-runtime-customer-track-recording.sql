begin;

alter table ai_call_recording_track
    add column if not exists tenant_id varchar(20),
    add column if not exists egress_generation bigint;

do $$
begin
    if exists (
        select track.id
        from ai_call_recording_track track
        left join ai_call_record record
          on record.call_id = track.call_id
        group by track.id
        having count(record.id) filter (
            where nullif(btrim(record.tenant_id), '') is not null
        ) <> 1
    ) then
        raise exception 'ai_call_recording_track_tenant_backfill_failed';
    end if;
end
$$;

update ai_call_recording_track track
set tenant_id = record.tenant_id
from ai_call_record record
where record.call_id = track.call_id
  and nullif(btrim(record.tenant_id), '') is not null
  and track.tenant_id is null;

do $$
begin
    if exists (
        select 1
        from ai_call_recording_track track
        join ai_call_record record
          on record.call_id = track.call_id
         and nullif(btrim(record.tenant_id), '') is not null
        where nullif(btrim(track.tenant_id), '') is null
           or track.tenant_id is distinct from record.tenant_id
    ) then
        raise exception 'ai_call_recording_track_tenant_backfill_failed';
    end if;
end
$$;

alter table ai_call_recording_track
    alter column tenant_id set not null,
    drop constraint if exists uk_ai_call_recording_track_participant;

drop index if exists uk_ai_call_recording_track_participant;
drop index if exists idx_ai_call_recording_track_call_role;
drop index if exists idx_ai_call_recording_track_egress_id;
drop index if exists idx_ai_call_recording_track_oss_id;
drop index if exists idx_ai_call_recording_track_verify_due;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'uk_ai_call_recording_track_tenant_participant'
          and conrelid = 'ai_call_recording_track'::regclass
    ) then
        alter table ai_call_recording_track
            add constraint uk_ai_call_recording_track_tenant_participant
            unique (tenant_id, call_id, track_role, participant_identity);
    end if;
end
$$;

create index if not exists idx_ai_call_recording_track_tenant_call_role
    on ai_call_recording_track (tenant_id, call_id, track_role);

create index if not exists idx_ai_call_recording_track_tenant_egress
    on ai_call_recording_track (tenant_id, egress_id);

create index if not exists idx_ai_call_recording_track_tenant_oss
    on ai_call_recording_track (tenant_id, oss_id);

create index if not exists idx_ai_call_recording_track_tenant_verify_due
    on ai_call_recording_track (tenant_id, status, next_verify_at);

create index if not exists idx_ai_call_recording_track_verify_due
    on ai_call_recording_track (status, next_verify_at, id);

commit;
