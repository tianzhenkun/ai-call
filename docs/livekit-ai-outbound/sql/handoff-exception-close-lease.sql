begin;

alter table ai_call_handoff
    add column if not exists exception_close_token varchar(36),
    add column if not exists exception_close_expires_at timestamptz,
    add column if not exists exception_prompt_completed_at timestamptz;

comment on column ai_call_handoff.exception_close_token is '异常收尾执行令牌';
comment on column ai_call_handoff.exception_close_expires_at is '异常收尾执行租约截止时间';
comment on column ai_call_handoff.exception_prompt_completed_at is '异常提示已完成时间';

commit;
