ALTER TABLE ai_call_prompt_profile
    ADD COLUMN IF NOT EXISTS current_version_id bigint;

UPDATE ai_call_prompt_profile AS profile
SET current_version_id = (
    SELECT version.id
    FROM ai_call_prompt_profile_version AS version
    WHERE version.tenant_id = profile.tenant_id
      AND version.profile_id = profile.id
      AND version.deleted_at IS NULL
    ORDER BY version.version_no DESC
    LIMIT 1
)
WHERE profile.current_version_id IS NULL;

COMMENT ON COLUMN ai_call_prompt_profile.current_version_id
    IS '当前使用的场景提示词版本ID';

CREATE TABLE IF NOT EXISTS ai_call_prompt_profile_version_application (
    id bigint PRIMARY KEY,
    tenant_id varchar(20) NOT NULL,
    profile_id bigint NOT NULL,
    from_version_id bigint,
    to_version_id bigint NOT NULL,
    applied_by bigint,
    applied_by_name varchar(100),
    applied_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ai_call_prompt_version_apply_profile_time
    ON ai_call_prompt_profile_version_application (tenant_id, profile_id, applied_at);

COMMENT ON TABLE ai_call_prompt_profile_version_application
    IS 'AI Call 场景提示词版本切换审计表';
