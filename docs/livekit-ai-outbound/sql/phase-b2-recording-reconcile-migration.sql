-- Phase B2 recording reconcile migration
-- 只补结构，不回填历史数据。

alter table if exists ai_call_recording
    add column if not exists stop_requested_at timestamp with time zone,
    add column if not exists verify_attempts integer,
    add column if not exists next_verify_at timestamp with time zone,
    add column if not exists verify_deadline_at timestamp with time zone,
    add column if not exists last_verify_at timestamp with time zone,
    add column if not exists last_verify_error varchar(500);

create index if not exists idx_ai_call_recording_verify_due
    on ai_call_recording (status, next_verify_at);

alter table if exists ai_call_recording_track
    add column if not exists stop_requested_at timestamp with time zone,
    add column if not exists verify_attempts integer,
    add column if not exists next_verify_at timestamp with time zone,
    add column if not exists verify_deadline_at timestamp with time zone,
    add column if not exists last_verify_at timestamp with time zone,
    add column if not exists last_verify_error varchar(500);

create index if not exists idx_ai_call_recording_track_verify_due
    on ai_call_recording_track (status, next_verify_at);
