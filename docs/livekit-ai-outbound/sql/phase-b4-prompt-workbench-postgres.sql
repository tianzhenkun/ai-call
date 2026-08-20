-- 场景提示词工作台：租户隔离、版本、产品信息、变量及名单变量快照。
-- 旧库存在全局提示词时，执行前先指定归属租户：
-- SET ai_call.prompt_tenant_id = '实际租户ID';

ALTER TABLE ai_call_prompt_profile
    ADD COLUMN IF NOT EXISTS tenant_id varchar(20),
    ADD COLUMN IF NOT EXISTS product_info text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS variables_json text NOT NULL DEFAULT '[]';

ALTER TABLE ai_call_prompt_common_config
    ALTER COLUMN id TYPE bigint,
    ADD COLUMN IF NOT EXISTS tenant_id varchar(20);

DO $$
DECLARE
    target_tenant text := nullif(current_setting('ai_call.prompt_tenant_id', true), '');
BEGIN
    IF (
        EXISTS (SELECT 1 FROM ai_call_prompt_profile WHERE tenant_id IS NULL)
        OR EXISTS (SELECT 1 FROM ai_call_prompt_common_config WHERE tenant_id IS NULL)
    ) AND target_tenant IS NULL THEN
        RAISE EXCEPTION
            '旧提示词尚未指定租户，请先执行 SET ai_call.prompt_tenant_id = ''实际租户ID''';
    END IF;
    UPDATE ai_call_prompt_profile SET tenant_id = target_tenant WHERE tenant_id IS NULL;
    UPDATE ai_call_prompt_common_config SET tenant_id = target_tenant WHERE tenant_id IS NULL;
END $$;

ALTER TABLE ai_call_prompt_profile
    ALTER COLUMN tenant_id SET NOT NULL,
    DROP CONSTRAINT IF EXISTS uk_ai_call_prompt_profile_scene,
    DROP CONSTRAINT IF EXISTS uk_ai_call_prompt_profile_tenant_scene,
    ADD CONSTRAINT uk_ai_call_prompt_profile_tenant_scene
        UNIQUE (tenant_id, scene_code);

CREATE INDEX IF NOT EXISTS idx_ai_call_prompt_profile_tenant_updated
    ON ai_call_prompt_profile (tenant_id, updated_at);

ALTER TABLE ai_call_prompt_common_config
    ALTER COLUMN tenant_id SET NOT NULL,
    DROP CONSTRAINT IF EXISTS uk_ai_call_prompt_common_tenant,
    ADD CONSTRAINT uk_ai_call_prompt_common_tenant UNIQUE (tenant_id);

CREATE TABLE IF NOT EXISTS ai_call_prompt_profile_version (
    id bigint PRIMARY KEY,
    tenant_id varchar(20) NOT NULL,
    profile_id bigint NOT NULL,
    version_no integer NOT NULL,
    version_name varchar(100) NOT NULL,
    snapshot_json text NOT NULL,
    creation_method varchar(32) NOT NULL,
    restored_from_version_id bigint,
    created_by bigint,
    created_by_name varchar(100),
    created_at timestamptz NOT NULL,
    deleted_at timestamptz,
    CONSTRAINT uk_ai_call_prompt_version_number
        UNIQUE (tenant_id, profile_id, version_no)
);

CREATE INDEX IF NOT EXISTS idx_ai_call_prompt_version_profile_created
    ON ai_call_prompt_profile_version (tenant_id, profile_id, created_at);

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

ALTER TABLE ai_call_outbound_validation_row
    ADD COLUMN IF NOT EXISTS business_params_json text NOT NULL DEFAULT '{}';

ALTER TABLE ai_call_outbound_target
    ADD COLUMN IF NOT EXISTS business_params_json text NOT NULL DEFAULT '{}';

UPDATE ai_call_prompt_profile
SET product_info = $product$
产品名称：GEO 生成式引擎优化服务。
产品定位：帮助企业观测并优化品牌在 AI 问答和智能搜索中的理解、提及、引用与推荐表现。
适用客户：品牌方，市场、增长、内容、公关和售前团队，SaaS 公司，以及希望提升 AI 搜索可见度的业务团队。
核心能力：
1. 使用统一问题集观测主流 AI 平台对品牌和产品的表述。
2. 整理官方资料、FAQ、案例和权威来源，统一品牌事实与产品口径。
3. 辅助生成或优化 FAQ、文章、售前资料和 PR 内容，经人工审核后发布。
4. 持续观察品牌提及率、推荐率、Top3/Top5 占比、引用来源、情感倾向和错误回答占比。
可评估方向：API/RPA/CRM/OA 对接、私有化、本地化、数据安全和合规方案，需由产品顾问结合客户环境确认。
不能承诺：不承诺一定排名、一定被模型推荐、固定周期见效或未经确认的客户案例和效果数据。
$product$
WHERE scene_code = 'intro_geo'
  AND btrim(product_info) = '';

COMMENT ON COLUMN ai_call_prompt_profile.product_info IS '产品或服务信息';
COMMENT ON COLUMN ai_call_prompt_profile.variables_json IS '业务变量定义JSON';
COMMENT ON TABLE ai_call_prompt_profile_version IS 'AI Call 场景提示词版本快照';
COMMENT ON COLUMN ai_call_outbound_validation_row.business_params_json
    IS '名单业务变量快照JSON';
COMMENT ON COLUMN ai_call_outbound_target.business_params_json
    IS '任务对象业务变量快照JSON';
