-- Phase H2: 通用外呼呼叫规则、正式任务与外呼对象
-- PostgreSQL 迁移；仅新增表，无物理外键、无 jsonb，JSON 快照统一使用 text。

CREATE TABLE IF NOT EXISTS ai_call_outbound_rule (
    id bigint PRIMARY KEY,
    tenant_id varchar(64) NOT NULL,
    rule_name varchar(100) NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    call_windows_json text NOT NULL,
    retry_count integer NOT NULL DEFAULT 0,
    retry_intervals_json text NOT NULL,
    retryable_results_json text NOT NULL,
    deleted boolean NOT NULL DEFAULT false,
    deleted_at timestamptz,
    created_by bigint NOT NULL,
    updated_by bigint NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT uk_outbound_rule_tenant_name UNIQUE (tenant_id, rule_name)
);

COMMENT ON TABLE ai_call_outbound_rule IS '通用外呼呼叫规则';
COMMENT ON COLUMN ai_call_outbound_rule.call_windows_json
    IS '呼叫时段JSON数组文本';
COMMENT ON COLUMN ai_call_outbound_rule.retry_intervals_json
    IS '重试间隔JSON数组文本';
COMMENT ON COLUMN ai_call_outbound_rule.retryable_results_json
    IS '可重试结果JSON数组文本';

CREATE INDEX IF NOT EXISTS idx_outbound_rule_tenant_enabled
    ON ai_call_outbound_rule (tenant_id, deleted, enabled, updated_at);
CREATE INDEX IF NOT EXISTS idx_outbound_rule_tenant_id
    ON ai_call_outbound_rule (tenant_id, id);

CREATE TABLE IF NOT EXISTS ai_call_outbound_task (
    id bigint PRIMARY KEY,
    tenant_id varchar(64) NOT NULL,
    validation_id bigint NOT NULL,
    idempotency_key varchar(128) NOT NULL,
    request_fingerprint varchar(64) NOT NULL,
    task_name varchar(50) NOT NULL,
    task_mode varchar(16) NOT NULL,
    status varchar(32) NOT NULL,
    total_targets integer NOT NULL DEFAULT 0,
    completed_targets integer NOT NULL DEFAULT 0,
    connected_targets integer NOT NULL DEFAULT 0,
    failed_targets integer NOT NULL DEFAULT 0,
    execution_mode varchar(16) NOT NULL,
    scheduled_at timestamptz,
    started_at timestamptz,
    ended_at timestamptz,
    prompt_profile_id varchar(64),
    prompt_name varchar(100) NOT NULL,
    scene_code varchar(64) NOT NULL,
    voice varchar(128) NOT NULL,
    voice_name varchar(100),
    rule_id bigint NOT NULL,
    rule_name varchar(100) NOT NULL,
    rule_summary varchar(500) NOT NULL,
    config_snapshot_json text NOT NULL,
    error_message varchar(1000),
    created_by bigint NOT NULL,
    created_by_name varchar(100),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT uk_outbound_task_tenant_idempotency
        UNIQUE (tenant_id, idempotency_key)
);

COMMENT ON TABLE ai_call_outbound_task IS '通用外呼正式任务';
COMMENT ON COLUMN ai_call_outbound_task.validation_id
    IS '校验任务ID，仅逻辑关联，无物理外键';
COMMENT ON COLUMN ai_call_outbound_task.rule_id
    IS '创建时使用的规则ID，仅逻辑关联；删除规则不影响任务快照';
COMMENT ON COLUMN ai_call_outbound_task.config_snapshot_json
    IS '规则、提示词、音色及请求参数的完整JSON快照文本';

CREATE INDEX IF NOT EXISTS idx_outbound_task_tenant_status
    ON ai_call_outbound_task (tenant_id, status, created_at);
CREATE INDEX IF NOT EXISTS idx_outbound_task_tenant_id
    ON ai_call_outbound_task (tenant_id, id);
CREATE INDEX IF NOT EXISTS idx_outbound_task_validation
    ON ai_call_outbound_task (tenant_id, validation_id);

CREATE TABLE IF NOT EXISTS ai_call_outbound_target (
    id bigint PRIMARY KEY,
    tenant_id varchar(64) NOT NULL,
    task_id bigint NOT NULL,
    validation_id bigint NOT NULL,
    source_validation_row_id bigint NOT NULL,
    source_row_number integer NOT NULL,
    phone_number varchar(64) NOT NULL,
    customer_name varchar(255),
    status varchar(32) NOT NULL,
    attempt_count integer NOT NULL DEFAULT 0,
    latest_result varchar(128),
    next_attempt_at timestamptz,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT uk_outbound_target_source_row
        UNIQUE (tenant_id, task_id, source_validation_row_id)
);

COMMENT ON TABLE ai_call_outbound_target IS '通用外呼正式任务对象';
COMMENT ON COLUMN ai_call_outbound_target.task_id
    IS '正式任务ID，仅逻辑关联，无物理外键';
COMMENT ON COLUMN ai_call_outbound_target.source_validation_row_id
    IS '名单校验明细ID，仅逻辑关联，用于幂等复制';

CREATE INDEX IF NOT EXISTS idx_outbound_target_task_page
    ON ai_call_outbound_target (tenant_id, task_id, source_row_number, id);
CREATE INDEX IF NOT EXISTS idx_outbound_target_task_status
    ON ai_call_outbound_target (tenant_id, task_id, status, next_attempt_at);
