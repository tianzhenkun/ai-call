begin;

alter table ai_call_record
    add column if not exists callee_phone_number varchar(32);

commit;
