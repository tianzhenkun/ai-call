alter table ai_call_voice_deletion
    add column if not exists reconcile_absent_count integer not null default 0;

update ai_call_voice_deletion
set status = case status
    when 'DELETED' then 'SUCCEEDED'
    when 'DELETE_FAILED' then 'FAILED'
    else status
end
where status in ('DELETED', 'DELETE_FAILED');

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'ck_voice_deletion_status'
          and conrelid = 'ai_call_voice_deletion'::regclass
    ) then
        alter table ai_call_voice_deletion
            add constraint ck_voice_deletion_status
            check (
                status in (
                    'PENDING',
                    'PROCESSING',
                    'RECONCILING',
                    'RETRY_WAIT',
                    'SUCCEEDED',
                    'FAILED'
                )
            );
    end if;
end
$$;

comment on column ai_call_voice_deletion.reconcile_absent_count
    is '连续完整确认音色不存在次数';
