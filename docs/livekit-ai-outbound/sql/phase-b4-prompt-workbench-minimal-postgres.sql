ALTER TABLE ai_call_prompt_profile
    ADD COLUMN IF NOT EXISTS product_info text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS variables_json text NOT NULL DEFAULT '[]';

CREATE TABLE IF NOT EXISTS ai_call_prompt_profile_version (
    id bigint PRIMARY KEY,
    tenant_id varchar(20) NOT NULL,
    profile_id bigint NOT NULL,
    version_no integer NOT NULL,
    snapshot_json text NOT NULL,
    creation_method varchar(32) NOT NULL,
    restored_from_version_id bigint,
    created_by bigint,
    created_by_name varchar(100),
    created_at timestamptz NOT NULL,
    deleted_at timestamptz,
    CONSTRAINT uk_ai_call_prompt_version_number
        UNIQUE (tenant_id, profile_id, version_no)
);

CREATE INDEX IF NOT EXISTS idx_ai_call_prompt_version_profile_created
    ON ai_call_prompt_profile_version (tenant_id, profile_id, created_at);

COMMENT ON COLUMN ai_call_prompt_profile.product_info IS '产品或服务信息';
COMMENT ON COLUMN ai_call_prompt_profile.variables_json IS '业务变量定义JSON';
COMMENT ON TABLE ai_call_prompt_profile_version IS 'AI Call 场景提示词版本快照';
