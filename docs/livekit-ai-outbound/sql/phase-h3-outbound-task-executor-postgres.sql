-- Phase H3: 通用外呼任务执行器
-- 为存量 PostgreSQL 数据库补充对象下一次可拨打时间。

ALTER TABLE ai_call_outbound_target
    ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz;

ALTER TABLE ai_call_outbound_task
    ADD COLUMN IF NOT EXISTS next_dispatch_at timestamptz;

ALTER TABLE ai_call_outbound_task
    ADD COLUMN IF NOT EXISTS last_dispatched_at timestamptz;

DROP INDEX IF EXISTS idx_outbound_target_task_status;

CREATE INDEX IF NOT EXISTS idx_outbound_target_task_status
    ON ai_call_outbound_target (tenant_id, task_id, status, next_attempt_at);

CREATE INDEX IF NOT EXISTS idx_outbound_task_dispatch
    ON ai_call_outbound_task (status, next_dispatch_at, last_dispatched_at, id);

CREATE INDEX IF NOT EXISTS idx_outbound_task_scheduled_dispatch
    ON ai_call_outbound_task (next_dispatch_at, id)
    WHERE status = 'SCHEDULED';

CREATE INDEX IF NOT EXISTS idx_outbound_task_running_dispatch
    ON ai_call_outbound_task (next_dispatch_at, last_dispatched_at, id)
    WHERE status = 'RUNNING';

CREATE INDEX IF NOT EXISTS idx_outbound_attempt_stale
    ON ai_call_outbound_attempt (status, started_at);
