-- Phase H4: 本机 Linphone 外呼测试门禁
-- PostgreSQL 幂等迁移；仅增加测试元数据与租户级唯一门禁，无物理外键。
-- Must run with autocommit enabled and outside any transaction block.
-- PostgreSQL does not allow CREATE INDEX CONCURRENTLY inside a transaction block.

ALTER TABLE ai_call_outbound_attempt
    ADD COLUMN IF NOT EXISTS dialer_type varchar(32);

ALTER TABLE ai_call_outbound_attempt
    ADD COLUMN IF NOT EXISTS test_scenario varchar(32);

ALTER TABLE ai_call_outbound_attempt
    ADD COLUMN IF NOT EXISTS command_idempotency_key varchar(128);

ALTER TABLE ai_call_outbound_attempt
    ADD COLUMN IF NOT EXISTS active_slot varchar(32);

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uk_outbound_attempt_tenant_command
    ON ai_call_outbound_attempt (tenant_id, command_idempotency_key);

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uk_outbound_attempt_tenant_active_slot
    ON ai_call_outbound_attempt (tenant_id, active_slot);
