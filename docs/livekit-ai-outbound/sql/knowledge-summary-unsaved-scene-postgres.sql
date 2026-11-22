-- 新增场景可先提取知识，审计仍记录所用资料。此时尚未创建场景。
alter table ai_call_knowledge_usage alter column prompt_profile_id drop not null;
