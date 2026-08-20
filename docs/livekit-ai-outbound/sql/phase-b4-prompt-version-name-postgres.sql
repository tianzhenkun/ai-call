ALTER TABLE ai_call_prompt_profile_version
    ADD COLUMN IF NOT EXISTS version_name varchar(100);

UPDATE ai_call_prompt_profile_version
SET version_name = COALESCE(
    NULLIF(snapshot_json::jsonb ->> 'name', ''),
    '版本 v' || version_no
)
WHERE version_name IS NULL OR btrim(version_name) = '';

ALTER TABLE ai_call_prompt_profile_version
    ALTER COLUMN version_name SET NOT NULL;
