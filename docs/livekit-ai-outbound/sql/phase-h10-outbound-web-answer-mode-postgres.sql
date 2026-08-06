-- Phase H10: 单个外呼任务的 Web 浏览器接听方式
-- PostgreSQL 幂等迁移；不创建新表、物理外键或数据库专用 JSON 类型。

ALTER TABLE ai_call_outbound_task
    ADD COLUMN IF NOT EXISTS answer_mode varchar(16) NOT NULL DEFAULT 'linphone';

ALTER TABLE ai_call_outbound_target
    ALTER COLUMN phone_number DROP NOT NULL;
