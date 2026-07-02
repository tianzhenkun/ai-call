-- Phase E SIP outbound callee target migration.
-- Purpose: prevent duplicate active SIP outbound calls to the same callee without
-- storing the raw phone number.

alter table if exists ai_call_record
    add column if not exists callee_phone_number_hash varchar(80);

alter table if exists ai_call_record
    add column if not exists callee_phone_number_masked varchar(32);

create index if not exists idx_ai_call_record_sip_callee_active
    on ai_call_record (entry_type, callee_phone_number_hash, status, started_at);

-- SQLite local dev equivalent:
-- alter table ai_call_record add column callee_phone_number_hash varchar(80);
-- alter table ai_call_record add column callee_phone_number_masked varchar(32);
-- create index if not exists idx_ai_call_record_sip_callee_active
--     on ai_call_record (entry_type, callee_phone_number_hash, status, started_at);
