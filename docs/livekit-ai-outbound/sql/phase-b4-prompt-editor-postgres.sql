-- 与新编辑器、场景准入代码协同上线；先备份，再在同一事务执行。
ALTER TABLE ai_call_prompt_profile ADD COLUMN IF NOT EXISTS lifecycle_status varchar(16) NOT NULL DEFAULT 'READY';
ALTER TABLE ai_call_prompt_profile ADD COLUMN IF NOT EXISTS edit_revision integer NOT NULL DEFAULT 1;
ALTER TABLE ai_call_prompt_profile ADD COLUMN IF NOT EXISTS creation_key varchar(100);
ALTER TABLE ai_call_prompt_profile ADD COLUMN IF NOT EXISTS deleted_at timestamptz;
ALTER TABLE ai_call_prompt_profile ADD COLUMN IF NOT EXISTS deleted_by bigint;

-- 存量重名必须由业务确认处理，禁止自动改名、合并或删除。
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM ai_call_prompt_profile WHERE deleted_at IS NULL
               GROUP BY tenant_id, btrim(name) HAVING count(*) > 1) THEN
        RAISE EXCEPTION '存在同租户同名场景，请先检查 tenant_id、btrim(name)、id 后再迁移';
    END IF;
    IF EXISTS (SELECT 1 FROM ai_call_prompt_profile WHERE deleted_at IS NULL AND btrim(name) = '') THEN
        RAISE EXCEPTION '存在空名称场景，请先修正名称后再迁移';
    END IF;
END $$;
UPDATE ai_call_prompt_profile SET name = btrim(name) WHERE name <> btrim(name);
CREATE UNIQUE INDEX IF NOT EXISTS uk_ai_call_prompt_active_name
    ON ai_call_prompt_profile (tenant_id, name) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uk_ai_call_prompt_creation_key
    ON ai_call_prompt_profile (tenant_id, creation_key);

-- 当前版本指针核对由 tools/migrate_prompt_editor.py 在同一事务执行；不创建历史版本。
