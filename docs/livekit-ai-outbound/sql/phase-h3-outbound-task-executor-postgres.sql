-- Phase H3: 通用外呼任务执行器
-- 为存量 PostgreSQL 数据库补充对象下一次可拨打时间。

ALTER TABLE ai_call_outbound_target
    ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz;

DROP INDEX IF EXISTS idx_outbound_target_task_status;

CREATE INDEX IF NOT EXISTS idx_outbound_target_task_status
    ON ai_call_outbound_target (tenant_id, task_id, status, next_attempt_at);
