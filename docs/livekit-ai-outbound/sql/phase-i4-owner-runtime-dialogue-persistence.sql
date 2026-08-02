begin;

alter table ai_call_record
    add column if not exists dialogue_persistence_status varchar(16)
        not null default 'not_started',
    add column if not exists dialogue_persistence_error varchar(500),
    add column if not exists dialogue_persistence_completed_at timestamptz;

alter table ai_call_dialogue_segment
    add column if not exists tenant_id varchar(20);

do $$
begin
    if exists (
        select 1
        from ai_call_dialogue_segment dialogue
        left join ai_call_record record
          on record.call_id = dialogue.call_id
        where record.call_id is null
           or record.tenant_id is null
    ) then
        raise exception 'ai_call_dialogue_segment_tenant_backfill_failed';
    end if;
end
$$;

update ai_call_dialogue_segment dialogue
set tenant_id = record.tenant_id
from ai_call_record record
where record.call_id = dialogue.call_id
  and dialogue.tenant_id is null;

alter table ai_call_dialogue_segment
    alter column tenant_id set not null;

alter table ai_call_dialogue_segment
    drop constraint if exists uk_ai_call_dialogue_call_no,
    drop constraint if exists uk_ai_call_dialogue_source_segment;

drop index if exists uk_ai_call_dialogue_call_no;
drop index if exists uk_ai_call_dialogue_source_segment;
drop index if exists idx_ai_call_dialogue_speaker;

alter table ai_call_dialogue_segment
    add constraint uk_ai_call_dialogue_call_no
        unique (tenant_id, call_id, segment_no),
    add constraint uk_ai_call_dialogue_source_segment
        unique (tenant_id, call_id, speaker_type, source, source_segment_id);

create index idx_ai_call_dialogue_speaker
    on ai_call_dialogue_segment (
        tenant_id,
        call_id,
        speaker_type,
        segment_no
    );

update ai_call_record
set dialogue_persistence_status = 'pending',
    dialogue_persistence_error = null,
    dialogue_persistence_completed_at = null
where runtime_control_mode = 'owner_command_v1'
  and ended_at is null
  and dialogue_persistence_status = 'not_started';

update ai_call_record
set dialogue_persistence_status = 'uncertain',
    dialogue_persistence_error = 'pre_migration_runtime_state_unverifiable',
    dialogue_persistence_completed_at = clock_timestamp()
where runtime_control_mode = 'owner_command_v1'
  and ended_at is not null
  and dialogue_persistence_status = 'not_started';

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'ck_ai_call_record_dialogue_status'
          and conrelid = 'ai_call_record'::regclass
    ) then
        alter table ai_call_record
            add constraint ck_ai_call_record_dialogue_status
            check (
                dialogue_persistence_status in (
                    'not_started',
                    'pending',
                    'complete',
                    'uncertain'
                )
            );
    end if;
    if not exists (
        select 1
        from pg_constraint
        where conname = 'ck_ai_call_record_owner_dialogue_started'
          and conrelid = 'ai_call_record'::regclass
    ) then
        alter table ai_call_record
            add constraint ck_ai_call_record_owner_dialogue_started
            check (
                runtime_control_mode <> 'owner_command_v1'
                or dialogue_persistence_status <> 'not_started'
            );
    end if;
    if not exists (
        select 1
        from pg_constraint
        where conname = 'ck_ai_call_record_dialogue_completed_at'
          and conrelid = 'ai_call_record'::regclass
    ) then
        alter table ai_call_record
            add constraint ck_ai_call_record_dialogue_completed_at
            check (
                dialogue_persistence_status not in ('complete', 'uncertain')
                or dialogue_persistence_completed_at is not null
            );
    end if;
end
$$;

commit;
