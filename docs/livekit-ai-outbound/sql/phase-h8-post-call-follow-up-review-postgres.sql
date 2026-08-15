-- Phase H8: 人工确认 AI 话后跟进建议（PostgreSQL，可重复执行）

alter table ai_call_semantic_analysis
    add column if not exists follow_up_review_status varchar(16);
alter table ai_call_semantic_analysis
    add column if not exists follow_up_reviewed_by varchar(64);
alter table ai_call_semantic_analysis
    add column if not exists follow_up_reviewed_by_name varchar(64);
alter table ai_call_semantic_analysis
    add column if not exists follow_up_reviewed_at timestamptz;
