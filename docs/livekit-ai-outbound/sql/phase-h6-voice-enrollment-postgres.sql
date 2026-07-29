create table if not exists ai_call_tenant_voice_profile (
    id bigint primary key,
    tenant_id varchar(64) not null,
    display_name varchar(100) not null,
    voice varchar(128) null,
    voice_type varchar(32) not null,
    gender varchar(16) not null,
    language varchar(32) not null,
    target_model varchar(64) not null,
    provider varchar(32) not null,
    status varchar(32) not null,
    latest_enrollment_id bigint null,
    provider_created_at timestamptz null,
    error_message varchar(1000) null,
    created_by bigint not null,
    deleted_by bigint null,
    deleted_at timestamptz null,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    constraint uk_tenant_voice_model_voice unique (tenant_id, target_model, voice)
);

create index if not exists idx_tenant_voice_status_updated
    on ai_call_tenant_voice_profile (tenant_id, status, updated_at);
create index if not exists idx_tenant_voice_tenant_id
    on ai_call_tenant_voice_profile (tenant_id, id);

comment on table ai_call_tenant_voice_profile is 'AI Call 租户自定义复刻音色档案';
comment on column ai_call_tenant_voice_profile.id is '雪花主键';
comment on column ai_call_tenant_voice_profile.tenant_id is '租户ID';
comment on column ai_call_tenant_voice_profile.display_name is '音色展示名';
comment on column ai_call_tenant_voice_profile.voice is '服务商返回的音色标识';
comment on column ai_call_tenant_voice_profile.voice_type is '固定为自定义复刻';
comment on column ai_call_tenant_voice_profile.gender is '音色性别';
comment on column ai_call_tenant_voice_profile.language is '音色语言';
comment on column ai_call_tenant_voice_profile.target_model is '适用目标模型';
comment on column ai_call_tenant_voice_profile.provider is '音色服务商';
comment on column ai_call_tenant_voice_profile.status is '音色状态';
comment on column ai_call_tenant_voice_profile.latest_enrollment_id is '最近复刻申请逻辑ID';
comment on column ai_call_tenant_voice_profile.provider_created_at is '服务商创建时间';
comment on column ai_call_tenant_voice_profile.error_message is '最近失败原因';
comment on column ai_call_tenant_voice_profile.created_by is '创建用户ID';
comment on column ai_call_tenant_voice_profile.deleted_by is '删除用户ID';
comment on column ai_call_tenant_voice_profile.deleted_at is '删除时间';
comment on column ai_call_tenant_voice_profile.created_at is '创建时间';
comment on column ai_call_tenant_voice_profile.updated_at is '更新时间';

create table if not exists ai_call_voice_enrollment (
    id bigint primary key,
    tenant_id varchar(64) not null,
    voice_profile_id bigint not null,
    idempotency_key varchar(128) not null,
    request_hash varchar(64) not null,
    preferred_name varchar(16) not null,
    language varchar(32) not null,
    transcript varchar(2000) null,
    sample_object_key varchar(500) null,
    sample_sha256 varchar(64) not null,
    status varchar(32) not null,
    provider_voice varchar(128) null,
    provider_request_id varchar(128) null,
    attempt_count integer not null default 0,
    next_retry_at timestamptz null,
    lease_owner varchar(128) null,
    lease_expires_at timestamptz null,
    error_message varchar(1000) null,
    cleanup_error_message varchar(1000) null,
    consent_user_id bigint not null,
    consent_at timestamptz not null,
    started_at timestamptz null,
    finished_at timestamptz null,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    constraint uk_voice_enrollment_tenant_key unique (tenant_id, idempotency_key)
);

create index if not exists idx_voice_enrollment_claim
    on ai_call_voice_enrollment (status, next_retry_at, id);
create index if not exists idx_voice_enrollment_profile
    on ai_call_voice_enrollment (tenant_id, voice_profile_id, created_at);

comment on table ai_call_voice_enrollment is 'AI Call 自定义音色复刻申请';
comment on column ai_call_voice_enrollment.id is '雪花主键';
comment on column ai_call_voice_enrollment.tenant_id is '租户ID';
comment on column ai_call_voice_enrollment.voice_profile_id is '音色档案逻辑ID';
comment on column ai_call_voice_enrollment.idempotency_key is '请求幂等键';
comment on column ai_call_voice_enrollment.request_hash is '请求内容哈希';
comment on column ai_call_voice_enrollment.preferred_name is '期望音色名称';
comment on column ai_call_voice_enrollment.language is '样音语言';
comment on column ai_call_voice_enrollment.transcript is '样音文本';
comment on column ai_call_voice_enrollment.sample_object_key is '样音对象键';
comment on column ai_call_voice_enrollment.sample_sha256 is '样音文件哈希';
comment on column ai_call_voice_enrollment.status is '复刻状态';
comment on column ai_call_voice_enrollment.provider_voice is '服务商返回音色标识';
comment on column ai_call_voice_enrollment.provider_request_id is '服务商请求ID';
comment on column ai_call_voice_enrollment.attempt_count is '尝试次数';
comment on column ai_call_voice_enrollment.next_retry_at is '下次重试时间';
comment on column ai_call_voice_enrollment.lease_owner is '处理租约持有者';
comment on column ai_call_voice_enrollment.lease_expires_at is '处理租约过期时间';
comment on column ai_call_voice_enrollment.error_message is '失败原因';
comment on column ai_call_voice_enrollment.cleanup_error_message is '样音清理失败原因';
comment on column ai_call_voice_enrollment.consent_user_id is '授权用户ID';
comment on column ai_call_voice_enrollment.consent_at is '授权确认时间';
comment on column ai_call_voice_enrollment.started_at is '开始处理时间';
comment on column ai_call_voice_enrollment.finished_at is '完成处理时间';
comment on column ai_call_voice_enrollment.created_at is '创建时间';
comment on column ai_call_voice_enrollment.updated_at is '更新时间';

create table if not exists ai_call_voice_deletion (
    id bigint primary key,
    tenant_id varchar(64) not null,
    voice_profile_id bigint not null,
    idempotency_key varchar(128) not null,
    status varchar(32) not null,
    provider_request_id varchar(128) null,
    attempt_count integer not null default 0,
    next_retry_at timestamptz null,
    lease_owner varchar(128) null,
    lease_expires_at timestamptz null,
    historical_task_count integer not null default 0,
    error_message varchar(1000) null,
    requested_by bigint not null,
    started_at timestamptz null,
    finished_at timestamptz null,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    constraint uk_voice_deletion_tenant_key unique (tenant_id, idempotency_key)
);

create index if not exists idx_voice_deletion_claim
    on ai_call_voice_deletion (status, next_retry_at, id);
create index if not exists idx_voice_deletion_profile
    on ai_call_voice_deletion (tenant_id, voice_profile_id, created_at);

comment on table ai_call_voice_deletion is 'AI Call 自定义音色删除申请';
comment on column ai_call_voice_deletion.id is '雪花主键';
comment on column ai_call_voice_deletion.tenant_id is '租户ID';
comment on column ai_call_voice_deletion.voice_profile_id is '音色档案逻辑ID';
comment on column ai_call_voice_deletion.idempotency_key is '请求幂等键';
comment on column ai_call_voice_deletion.status is '删除状态';
comment on column ai_call_voice_deletion.provider_request_id is '服务商请求ID';
comment on column ai_call_voice_deletion.attempt_count is '尝试次数';
comment on column ai_call_voice_deletion.next_retry_at is '下次重试时间';
comment on column ai_call_voice_deletion.lease_owner is '处理租约持有者';
comment on column ai_call_voice_deletion.lease_expires_at is '处理租约过期时间';
comment on column ai_call_voice_deletion.historical_task_count is '关联历史任务数';
comment on column ai_call_voice_deletion.error_message is '失败原因';
comment on column ai_call_voice_deletion.requested_by is '请求用户ID';
comment on column ai_call_voice_deletion.started_at is '开始处理时间';
comment on column ai_call_voice_deletion.finished_at is '完成处理时间';
comment on column ai_call_voice_deletion.created_at is '创建时间';
comment on column ai_call_voice_deletion.updated_at is '更新时间';
