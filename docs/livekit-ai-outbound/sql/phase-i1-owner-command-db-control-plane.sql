begin;

do $$
begin
    if exists (
        select 1
        from ai_call_record
        group by room_name
        having count(*) > 1
    ) then
        raise exception 'ai_call_record.room_name 存在重复值，禁止创建全局唯一约束';
    end if;

    if exists (
        select 1
        from ai_call_record
        group by call_id
        having count(*) > 1
    ) then
        raise exception 'ai_call_record.call_id 存在重复值，禁止继续控制面迁移';
    end if;

    if exists (
        select 1
        from ai_call_outbound_attempt
        where command_idempotency_key is not null
        group by tenant_id, command_idempotency_key
        having count(*) > 1
    ) then
        raise exception 'ai_call_outbound_attempt 命令幂等键存在重复值';
    end if;
end
$$;

alter table ai_call_record
    add column if not exists tenant_id varchar(20),
    add column if not exists runtime_control_mode varchar(32) not null default 'legacy_local',
    add column if not exists runtime_owner_id varchar(128),
    add column if not exists runtime_fencing_token bigint not null default 0,
    add column if not exists runtime_lease_expires_at timestamptz,
    add column if not exists runtime_heartbeat_at timestamptz,
    add column if not exists runtime_capacity_class varchar(16) not null default 'none',
    add column if not exists startup_reconcile_deadline_at timestamptz,
    add column if not exists startup_reconcile_policy_version varchar(64),
    add column if not exists startup_reconcile_budget_json text,
    add column if not exists agent_participant_identity varchar(255),
    add column if not exists agent_participant_sid varchar(255),
    add column if not exists agent_audio_track_sid varchar(255),
    add column if not exists agent_resource_generation bigint,
    add column if not exists agent_media_ready_at timestamptz,
    add column if not exists next_command_seq bigint not null default 1,
    add column if not exists last_applied_command_seq bigint not null default 0,
    add column if not exists terminal_requested_at timestamptz,
    add column if not exists resource_cleanup_status varchar(32) not null default 'not_started',
    add column if not exists resource_cleanup_error varchar(1000),
    add column if not exists resource_cleanup_next_retry_at timestamptz,
    add column if not exists resource_cleanup_completed_at timestamptz;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'uk_ai_call_record_room_name'
          and conrelid = 'ai_call_record'::regclass
    ) then
        alter table ai_call_record
            add constraint uk_ai_call_record_room_name unique (room_name);
    end if;
    if not exists (
        select 1
        from pg_constraint
        where conname = 'ck_ai_call_record_runtime_control_mode'
          and conrelid = 'ai_call_record'::regclass
    ) then
        alter table ai_call_record
            add constraint ck_ai_call_record_runtime_control_mode
            check (runtime_control_mode in ('legacy_local', 'owner_command_v1'));
    end if;
    if not exists (
        select 1
        from pg_constraint
        where conname = 'ck_ai_call_record_owner_mode_tenant'
          and conrelid = 'ai_call_record'::regclass
    ) then
        alter table ai_call_record
            add constraint ck_ai_call_record_owner_mode_tenant
            check (runtime_control_mode = 'legacy_local' or tenant_id is not null);
    end if;
    if not exists (
        select 1
        from pg_constraint
        where conname = 'ck_ai_call_record_owned_capacity'
          and conrelid = 'ai_call_record'::regclass
    ) then
        alter table ai_call_record
            add constraint ck_ai_call_record_owned_capacity
            check (
                runtime_capacity_class not in ('active', 'cleanup')
                or (runtime_owner_id is not null and runtime_lease_expires_at is not null)
            );
    end if;
    if not exists (
        select 1
        from pg_constraint
        where conname = 'ck_ai_call_record_attention_capacity'
          and conrelid = 'ai_call_record'::regclass
    ) then
        alter table ai_call_record
            add constraint ck_ai_call_record_attention_capacity
            check (
                runtime_capacity_class <> 'attention'
                or (
                    runtime_owner_id is null
                    and runtime_lease_expires_at is null
                    and resource_cleanup_status = 'attention_required'
                    and resource_cleanup_next_retry_at is not null
                )
            );
    end if;
    if not exists (
        select 1
        from pg_constraint
        where conname = 'ck_ai_call_record_cleanup_clean'
          and conrelid = 'ai_call_record'::regclass
    ) then
        alter table ai_call_record
            add constraint ck_ai_call_record_cleanup_clean
            check (
                resource_cleanup_status <> 'clean'
                or (
                    runtime_capacity_class = 'none'
                    and runtime_owner_id is null
                    and runtime_lease_expires_at is null
                    and resource_cleanup_completed_at is not null
                )
            );
    end if;
end
$$;

create index if not exists idx_ai_call_record_runtime_owner_lease
    on ai_call_record (runtime_owner_id, runtime_lease_expires_at);

alter table ai_call_outbound_attempt
    add column if not exists reconcile_owner_id varchar(128),
    add column if not exists reconcile_token varchar(128),
    add column if not exists reconcile_expires_at timestamptz,
    add column if not exists reconcile_after timestamptz,
    add column if not exists reconcile_attempt_count integer not null default 0;

create index if not exists idx_outbound_attempt_reconcile
    on ai_call_outbound_attempt (status, reconcile_after);
create index if not exists idx_outbound_attempt_reconcile_lease
    on ai_call_outbound_attempt (reconcile_expires_at);

create table if not exists ai_call_runtime_worker (
    worker_id varchar(128) primary key,
    status varchar(32) not null,
    capacity integer not null,
    active_call_count integer not null default 0,
    cleanup_capacity integer not null,
    active_cleanup_count integer not null default 0,
    heartbeat_at timestamptz not null,
    lease_expires_at timestamptz not null,
    stream_cleanup_owner_id varchar(128),
    stream_cleanup_token varchar(128),
    stream_cleanup_expires_at timestamptz,
    stream_cleanup_after timestamptz,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    constraint ck_runtime_worker_status
        check (status in ('STARTING', 'READY', 'DRAINING', 'OFFLINE')),
    constraint ck_runtime_worker_capacity check (capacity >= 0),
    constraint ck_runtime_worker_active_count
        check (active_call_count >= 0 and active_call_count <= capacity),
    constraint ck_runtime_worker_cleanup_capacity check (cleanup_capacity >= 0),
    constraint ck_runtime_worker_cleanup_count
        check (
            active_cleanup_count >= 0
            and active_cleanup_count <= cleanup_capacity
        )
);

create index if not exists idx_runtime_worker_dispatch
    on ai_call_runtime_worker (status, lease_expires_at, worker_id);
create index if not exists idx_runtime_worker_stream_cleanup
    on ai_call_runtime_worker (stream_cleanup_after);

create table if not exists ai_call_runtime_command (
    id bigint primary key,
    tenant_id varchar(20) not null,
    call_id varchar(64) not null,
    command_seq bigint not null,
    command_type varchar(64) not null,
    idempotency_key varchar(128) not null,
    request_fingerprint varchar(64) not null,
    dispatch_priority smallint not null default 100,
    allocation_deadline_at timestamptz,
    payload_json text,
    sensitive_payload_ciphertext text,
    payload_key_version varchar(64),
    expected_fencing_token bigint,
    target_owner_id varchar(128),
    status varchar(32) not null,
    dispatch_token varchar(128),
    dispatch_expires_at timestamptz,
    attempt_count integer not null default 0,
    next_retry_at timestamptz,
    published_at timestamptz,
    stream_message_id varchar(128),
    processing_owner_id varchar(128),
    processing_fencing_token bigint,
    processing_token varchar(128),
    processing_expires_at timestamptz,
    claimed_at timestamptz,
    cancel_requested_at timestamptz,
    preempted_by_command_id bigint,
    finished_at timestamptz,
    result_json text,
    error_message varchar(1000),
    created_at timestamptz not null,
    updated_at timestamptz not null,
    constraint uk_runtime_command_tenant_idempotency
        unique (tenant_id, idempotency_key),
    constraint uk_runtime_command_call_seq
        unique (tenant_id, call_id, command_seq),
    constraint ck_runtime_command_status check (
        status in (
            'PENDING',
            'DISPATCHING',
            'PUBLISHED',
            'PROCESSING',
            'RETRY_WAIT',
            'SUCCEEDED',
            'DEAD',
            'SUPERSEDED',
            'CANCELED'
        )
    ),
    constraint ck_runtime_command_attempt_count check (attempt_count >= 0)
);

create index if not exists idx_runtime_command_retry
    on ai_call_runtime_command (status, next_retry_at);
create index if not exists idx_runtime_command_allocation
    on ai_call_runtime_command (command_type, status, allocation_deadline_at);
create index if not exists idx_runtime_command_dispatch_lease
    on ai_call_runtime_command (status, dispatch_expires_at);
create index if not exists idx_runtime_command_published
    on ai_call_runtime_command (status, published_at);
create index if not exists idx_runtime_command_processing
    on ai_call_runtime_command (status, processing_expires_at);
create index if not exists idx_runtime_command_owner_dispatch
    on ai_call_runtime_command (
        target_owner_id,
        status,
        dispatch_priority,
        created_at
    );
create index if not exists idx_runtime_command_call_audit
    on ai_call_runtime_command (tenant_id, call_id, created_at);

create table if not exists ai_call_end_evidence (
    id bigint primary key,
    tenant_id varchar(20) not null,
    call_id varchar(64) not null,
    command_id bigint,
    source varchar(32) not null,
    end_reason varchar(64) not null,
    provider varchar(32),
    provider_namespace varchar(128),
    provider_event_id varchar(160),
    event_at timestamptz,
    received_at timestamptz not null,
    dedupe_key varchar(160) not null,
    evidence_json text,
    constraint uk_end_evidence_tenant_dedupe unique (tenant_id, dedupe_key)
);

create index if not exists idx_end_evidence_call
    on ai_call_end_evidence (tenant_id, call_id, received_at);

create table if not exists ai_call_runtime_effect (
    id bigint primary key,
    tenant_id varchar(20) not null,
    call_id varchar(64) not null,
    command_id bigint not null,
    effect_type varchar(64) not null,
    idempotency_key varchar(160) not null,
    fencing_token bigint not null,
    status varchar(32) not null,
    processing_token varchar(128),
    processing_expires_at timestamptz,
    provider_namespace varchar(128) not null,
    provider_idempotency_key varchar(160) not null,
    resource_key varchar(255) not null,
    resource_generation bigint not null,
    source_create_effect_id bigint,
    create_protection_deadline_at timestamptz,
    absence_observation_count integer not null default 0,
    absence_confirmed_at timestamptz,
    terminal_confirmed_at timestamptz,
    provider_reference varchar(255),
    execution_phase smallint not null default 0,
    processing_owner_id varchar(128),
    processing_fencing_token bigint,
    reconcile_after timestamptz,
    reconcile_deadline_at timestamptz,
    attempt_count integer not null default 0,
    error_message varchar(1000),
    created_at timestamptz not null,
    updated_at timestamptz not null,
    constraint uk_runtime_effect_tenant_idempotency
        unique (tenant_id, idempotency_key),
    constraint uk_runtime_effect_resource
        unique (tenant_id, provider_namespace, effect_type, resource_key),
    constraint uk_runtime_effect_provider_idempotency
        unique (tenant_id, provider_namespace, provider_idempotency_key),
    constraint ck_runtime_effect_status check (
        status in (
            'PENDING',
            'APPLYING',
            'APPLIED',
            'RECONCILE_REQUIRED',
            'FAILED'
        )
    ),
    constraint ck_runtime_effect_attempt_count
        check (attempt_count >= 0 and absence_observation_count >= 0)
);

create index if not exists idx_runtime_effect_reconcile
    on ai_call_runtime_effect (status, reconcile_after);
create index if not exists idx_runtime_effect_processing
    on ai_call_runtime_effect (status, processing_expires_at);
create index if not exists idx_runtime_effect_call_audit
    on ai_call_runtime_effect (tenant_id, call_id, status, created_at);

create table if not exists ai_call_runtime_effect_dependency (
    id bigint primary key,
    tenant_id varchar(20) not null,
    effect_id bigint not null,
    prerequisite_effect_id bigint not null,
    required_status varchar(32) not null default 'APPLIED',
    created_at timestamptz not null,
    constraint uk_runtime_effect_dependency
        unique (tenant_id, effect_id, prerequisite_effect_id),
    constraint ck_runtime_effect_dependency_status
        check (required_status = 'APPLIED')
);

create index if not exists idx_runtime_effect_dependency_effect
    on ai_call_runtime_effect_dependency (tenant_id, effect_id);

create table if not exists ai_call_sip_line_reservation (
    id bigint primary key,
    tenant_id varchar(20) not null,
    line_id bigint not null,
    call_id varchar(64) not null,
    attempt_id bigint,
    status varchar(32) not null,
    reservation_token varchar(128) not null,
    fencing_token bigint not null,
    acquired_at timestamptz not null,
    reconcile_after timestamptz,
    released_at timestamptz,
    error_message varchar(1000),
    created_at timestamptz not null,
    updated_at timestamptz not null,
    constraint uk_sip_line_reservation_call unique (call_id),
    constraint ck_sip_line_reservation_status check (
        status in ('RESERVED', 'ACTIVE', 'RECONCILE_REQUIRED', 'RELEASED')
    )
);

create index if not exists idx_sip_line_reservation_capacity
    on ai_call_sip_line_reservation (tenant_id, line_id, status, acquired_at);

comment on column ai_call_record.tenant_id
    is '通话所属租户；历史 legacy_local 记录允许为空';
comment on column ai_call_record.runtime_control_mode
    is '不可变控制模式：legacy_local 或 owner_command_v1';
comment on column ai_call_record.runtime_fencing_token
    is 'Runtime Owner 版本；首次分配和接管递增';
comment on column ai_call_record.terminal_requested_at
    is '吸收性终态屏障，只允许 null 变为数据库时间';
comment on table ai_call_runtime_worker
    is 'AI Call Runtime Worker 注册、正常容量和 cleanup 容量';
comment on table ai_call_runtime_command
    is 'AI Call PostgreSQL 权威命令表';
comment on table ai_call_end_evidence
    is 'AI Call 多来源终止证据，不创建物理外键';
comment on table ai_call_runtime_effect
    is 'AI Call Provider 副作用独立租约与对账状态';
comment on table ai_call_runtime_effect_dependency
    is 'AI Call Provider Effect 持久依赖图';
comment on table ai_call_sip_line_reservation
    is 'AI Call SIP 线路并发占用；非 RELEASED 均计数';

commit;
