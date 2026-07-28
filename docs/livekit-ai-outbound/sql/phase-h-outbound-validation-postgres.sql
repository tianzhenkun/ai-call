-- Phase H: 通用外呼批量名单直接上传与校验
-- PostgreSQL 迁移；无物理外键、无 jsonb，所有 JSON 使用 text。

CREATE TABLE IF NOT EXISTS ai_call_outbound_validation (
    id bigint PRIMARY KEY,
    tenant_id varchar(64) NOT NULL,
    status varchar(32) NOT NULL,
    processing_stage varchar(32) NOT NULL,
    original_filename varchar(255) NOT NULL,
    temp_file_path varchar(1000),
    file_size bigint NOT NULL,
    task_config_json text NOT NULL,
    valid_target_count integer NOT NULL DEFAULT 0,
    issue_count integer NOT NULL DEFAULT 0,
    issue_stats_json text,
    error_message varchar(1000),
    retryable boolean NOT NULL DEFAULT false,
    retry_count integer NOT NULL DEFAULT 0,
    created_by bigint NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    finished_at timestamptz
);

COMMENT ON TABLE ai_call_outbound_validation IS '通用外呼批量名单校验任务';
COMMENT ON COLUMN ai_call_outbound_validation.task_config_json IS '待创建任务配置JSON文本';
COMMENT ON COLUMN ai_call_outbound_validation.temp_file_path IS '解析前使用的服务端临时文件路径';

CREATE INDEX IF NOT EXISTS idx_outbound_validation_tenant_status
    ON ai_call_outbound_validation (tenant_id, status, updated_at);
CREATE INDEX IF NOT EXISTS idx_outbound_validation_tenant_id
    ON ai_call_outbound_validation (tenant_id, id);

CREATE TABLE IF NOT EXISTS ai_call_outbound_validation_row (
    id bigint PRIMARY KEY,
    tenant_id varchar(64) NOT NULL,
    validation_id bigint NOT NULL,
    row_number integer NOT NULL,
    phone_number text,
    customer_name text,
    normalized_phone varchar(64),
    is_valid boolean NOT NULL,
    reasons_json text,
    duplicate_row_number integer,
    created_at timestamptz NOT NULL,
    CONSTRAINT uk_outbound_validation_row_number
        UNIQUE (tenant_id, validation_id, row_number)
);

COMMENT ON TABLE ai_call_outbound_validation_row IS '通用外呼名单校验明细';
COMMENT ON COLUMN ai_call_outbound_validation_row.validation_id
    IS '校验任务ID，仅逻辑关联，无物理外键';
COMMENT ON COLUMN ai_call_outbound_validation_row.reasons_json
    IS '错误原因JSON数组文本；错误行与有效行共用本表';

CREATE INDEX IF NOT EXISTS idx_outbound_validation_row_page
    ON ai_call_outbound_validation_row (tenant_id, validation_id, is_valid, id);
CREATE INDEX IF NOT EXISTS idx_outbound_validation_row_phone
    ON ai_call_outbound_validation_row (tenant_id, validation_id, normalized_phone);
