-- 入库生成的检索资料独立于已冻结的原文及其校验值。
alter table ai_call_knowledge_version
    add column if not exists retrieval_version varchar(64),
    add column if not exists retrieval_metadata text,
    add column if not exists retrieval_vectors bytea,
    add column if not exists retrieval_lease varchar(36),
    add column if not exists retrieval_retry_at timestamptz;
