-- Phase H3: 通用外呼拨打尝试与通话记录关联
-- PostgreSQL 迁移；仅新增表，无物理外键。
-- 当前阶段提供关联结构和查询能力；真实拨打写入由后续任务执行器接入。

CREATE TABLE IF NOT EXISTS ai_call_outbound_attempt (
    id bigint PRIMARY KEY,
    tenant_id varchar(64) NOT NULL,
    task_id bigint NOT NULL,
    target_id bigint NOT NULL,
    attempt_no integer NOT NULL,
    call_id varchar(64) NOT NULL,
    status varchar(32) NOT NULL,
    call_result varchar(64),
    error_message varchar(1000),
    started_at timestamptz NOT NULL,
    ended_at timestamptz,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT uk_outbound_attempt_target_no
        UNIQUE (tenant_id, target_id, attempt_no),
    CONSTRAINT uk_outbound_attempt_call
        UNIQUE (call_id)
);

COMMENT ON TABLE ai_call_outbound_attempt
    IS '通用外呼一次拨打尝试；逻辑关联任务、对象和通话记录';
COMMENT ON COLUMN ai_call_outbound_attempt.task_id
    IS '正式任务ID，仅逻辑关联，无物理外键';
COMMENT ON COLUMN ai_call_outbound_attempt.target_id
    IS '外呼对象ID，仅逻辑关联，无物理外键';
COMMENT ON COLUMN ai_call_outbound_attempt.call_id
    IS '通话记录业务ID，仅逻辑关联，无物理外键';

CREATE INDEX IF NOT EXISTS idx_outbound_attempt_task
    ON ai_call_outbound_attempt (tenant_id, task_id, started_at);
CREATE INDEX IF NOT EXISTS idx_outbound_attempt_target
    ON ai_call_outbound_attempt (tenant_id, target_id, attempt_no);
CREATE INDEX IF NOT EXISTS idx_outbound_attempt_stale
    ON ai_call_outbound_attempt (status, started_at);
