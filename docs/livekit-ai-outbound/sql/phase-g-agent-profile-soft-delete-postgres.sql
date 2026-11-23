ALTER TABLE ai_call_agent_profile
    ADD COLUMN IF NOT EXISTS deleted_at timestamptz;
