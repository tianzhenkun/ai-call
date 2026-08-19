-- 知识库场景绑定依赖提示词租户隔离。
-- 旧数据存在时，事务内先指定其唯一归属租户：
-- SET LOCAL ai_call.prompt_tenant_id = '实际租户ID';

ALTER TABLE ai_call_prompt_profile
    ADD COLUMN IF NOT EXISTS tenant_id varchar(20);

DO $$
DECLARE
    target_tenant text := nullif(current_setting('ai_call.prompt_tenant_id', true), '');
BEGIN
    IF EXISTS (
        SELECT 1 FROM ai_call_prompt_profile WHERE tenant_id IS NULL
    ) AND target_tenant IS NULL THEN
        RAISE EXCEPTION
            '旧提示词尚未指定租户，请先执行 SET LOCAL ai_call.prompt_tenant_id = ''实际租户ID''';
    END IF;

    UPDATE ai_call_prompt_profile
    SET tenant_id = target_tenant
    WHERE tenant_id IS NULL;
END $$;

ALTER TABLE ai_call_prompt_profile
    ALTER COLUMN tenant_id SET NOT NULL,
    DROP CONSTRAINT IF EXISTS uk_ai_call_prompt_profile_scene,
    DROP CONSTRAINT IF EXISTS uk_ai_call_prompt_profile_tenant_scene,
    ADD CONSTRAINT uk_ai_call_prompt_profile_tenant_scene
        UNIQUE (tenant_id, scene_code);

CREATE INDEX IF NOT EXISTS idx_ai_call_prompt_profile_tenant_updated
    ON ai_call_prompt_profile (tenant_id, updated_at);

COMMENT ON COLUMN ai_call_prompt_profile.tenant_id IS '租户ID';
