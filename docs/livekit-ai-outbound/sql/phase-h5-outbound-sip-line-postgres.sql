-- Phase H5: 租户级 SIP 外呼线路与拨号诊断字段
-- PostgreSQL 幂等迁移；不保存 Provider 凭据，不创建物理外键，不使用数据库专用 JSON 类型。

CREATE TABLE IF NOT EXISTS ai_call_sip_line (
    id bigint PRIMARY KEY,
    tenant_id varchar(64) NOT NULL,
    line_code varchar(64) NOT NULL,
    line_name varchar(100) NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    default_marker varchar(32),
    adapter_type varchar(32) NOT NULL,
    route_mode varchar(32) NOT NULL,
    trunk_id varchar(128),
    proxy_host varchar(255),
    proxy_port integer,
    auth_mode varchar(32) NOT NULL,
    caller_number varchar(64) NOT NULL,
    destination_country varchar(8) NOT NULL DEFAULT 'CN',
    max_concurrency integer NOT NULL DEFAULT 1,
    originate_timeout_seconds integer NOT NULL DEFAULT 45,
    health_status varchar(32) NOT NULL DEFAULT 'UNKNOWN',
    health_message varchar(500),
    last_checked_at timestamptz,
    deleted boolean NOT NULL DEFAULT false,
    deleted_at timestamptz,
    created_by bigint NOT NULL,
    updated_by bigint NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT uk_ai_call_sip_line_tenant_code
        UNIQUE (tenant_id, line_code),
    CONSTRAINT uk_ai_call_sip_line_tenant_default
        UNIQUE (tenant_id, default_marker)
);

COMMENT ON TABLE ai_call_sip_line IS 'AI Call 租户级 SIP 外呼线路';
COMMENT ON COLUMN ai_call_sip_line.trunk_id
    IS 'LiveKit托管Outbound Trunk ID，不是Provider凭据';
COMMENT ON COLUMN ai_call_sip_line.default_marker
    IS '默认外呼线路固定为OUTBOUND，非默认线路为空';

CREATE INDEX IF NOT EXISTS idx_ai_call_sip_line_tenant_enabled
    ON ai_call_sip_line (tenant_id, deleted, enabled, updated_at);
CREATE INDEX IF NOT EXISTS idx_ai_call_sip_line_tenant_id
    ON ai_call_sip_line (tenant_id, id);

ALTER TABLE ai_call_outbound_validation
    ADD COLUMN IF NOT EXISTS line_id bigint;
ALTER TABLE ai_call_outbound_validation
    ADD COLUMN IF NOT EXISTS line_snapshot_json text;

ALTER TABLE ai_call_outbound_task
    ADD COLUMN IF NOT EXISTS line_id bigint;
ALTER TABLE ai_call_outbound_task
    ADD COLUMN IF NOT EXISTS line_name varchar(100);

ALTER TABLE ai_call_outbound_attempt
    ADD COLUMN IF NOT EXISTS line_id bigint;
ALTER TABLE ai_call_outbound_attempt
    ADD COLUMN IF NOT EXISTS line_code varchar(64);
ALTER TABLE ai_call_outbound_attempt
    ADD COLUMN IF NOT EXISTS provider_status_code varchar(64);
ALTER TABLE ai_call_outbound_attempt
    ADD COLUMN IF NOT EXISTS provider_reason varchar(500);
ALTER TABLE ai_call_outbound_attempt
    ADD COLUMN IF NOT EXISTS hangup_cause varchar(128);

CREATE INDEX IF NOT EXISTS idx_outbound_validation_line
    ON ai_call_outbound_validation (tenant_id, line_id);
CREATE INDEX IF NOT EXISTS idx_outbound_task_line
    ON ai_call_outbound_task (tenant_id, line_id);
CREATE INDEX IF NOT EXISTS idx_outbound_attempt_line
    ON ai_call_outbound_attempt (tenant_id, line_id, started_at);
