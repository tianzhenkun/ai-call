-- Phase B2.5 dialogue source segment migration for existing PostgreSQL databases.
-- Purpose: add source-side dialogue segment identity so transcript callbacks can be idempotent.

alter table ai_call_dialogue_segment
    add column if not exists source_segment_id varchar(128);

update ai_call_dialogue_segment
set source_segment_id = 'legacy-' || segment_no::varchar
where source_segment_id is null;

alter table ai_call_dialogue_segment
    alter column source_segment_id set not null;

alter table ai_call_dialogue_segment
    drop constraint if exists uk_ai_call_dialogue_source_segment;

drop index if exists uk_ai_call_dialogue_source_segment;

create unique index if not exists uk_ai_call_dialogue_source_segment
    on ai_call_dialogue_segment (call_id, speaker_type, source, source_segment_id);

drop index if exists idx_ai_call_dialogue_call_id;

drop index if exists idx_ai_call_dialogue_speaker;

create index if not exists idx_ai_call_dialogue_speaker
    on ai_call_dialogue_segment (call_id, speaker_type, segment_no);
