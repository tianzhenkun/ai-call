begin;

alter table ai_call_handoff
    add column if not exists participant_identity varchar(255),
    add column if not exists participant_sid varchar(255),
    add column if not exists track_sid varchar(255),
    add column if not exists verified_at timestamptz,
    add column if not exists evidence_source varchar(64),
    add column if not exists media_state_version bigint not null default 0,
    add column if not exists media_invalidated_at timestamptz,
    add column if not exists last_media_event_key varchar(160);

create table if not exists ai_call_handoff_media_evidence (
    id bigint primary key,
    tenant_id varchar(20) not null,
    call_id varchar(64) not null,
    handoff_id varchar(64) not null,
    provider_namespace varchar(128) not null,
    dedupe_key varchar(160) not null,
    participant_identity varchar(255) not null,
    participant_sid varchar(255),
    track_sid varchar(255),
    event_type varchar(64) not null,
    media_state_version bigint not null,
    provider_event_id varchar(160),
    event_at timestamptz,
    received_at timestamptz not null,
    evidence_json text
);

create unique index if not exists uk_handoff_media_evidence_dedupe
    on ai_call_handoff_media_evidence (
        tenant_id,
        provider_namespace,
        dedupe_key
    );

create unique index if not exists uk_handoff_media_evidence_version
    on ai_call_handoff_media_evidence (
        tenant_id,
        call_id,
        handoff_id,
        media_state_version
    );

create index if not exists idx_handoff_media_evidence_handoff_version
    on ai_call_handoff_media_evidence (
        tenant_id,
        handoff_id,
        media_state_version
    );

create table if not exists ai_call_webhook_inbox (
    id bigint primary key,
    provider varchar(32) not null,
    provider_namespace varchar(128) not null,
    dedupe_key varchar(160) not null,
    tenant_id varchar(20) not null,
    call_id varchar(64),
    event_type varchar(64) not null,
    payload_json text,
    status varchar(32) not null,
    attempt_count integer not null default 0,
    next_retry_at timestamptz,
    processing_owner_id varchar(128),
    processing_token varchar(128),
    processing_expires_at timestamptz,
    error_message varchar(1000),
    received_at timestamptz not null,
    claimed_at timestamptz,
    processed_at timestamptz
);

create unique index if not exists uk_webhook_inbox_provider_dedupe
    on ai_call_webhook_inbox (
        provider,
        provider_namespace,
        dedupe_key
    );

create index if not exists idx_webhook_inbox_retry
    on ai_call_webhook_inbox (
        status,
        next_retry_at,
        received_at
    );

create index if not exists idx_webhook_inbox_recovery
    on ai_call_webhook_inbox (
        status,
        processing_expires_at
    );

create index if not exists idx_webhook_inbox_call
    on ai_call_webhook_inbox (
        tenant_id,
        call_id,
        received_at
    );

create table if not exists ai_call_webhook_quarantine (
    id bigint primary key,
    provider varchar(32) not null,
    provider_namespace varchar(128) not null,
    dedupe_key varchar(160) not null,
    room_name varchar(255),
    participant_identity varchar(255),
    event_type varchar(64) not null,
    payload_json text,
    status varchar(32) not null,
    attempt_count integer not null default 0,
    next_retry_at timestamptz,
    processing_owner_id varchar(128),
    processing_generation bigint not null default 0,
    processing_token varchar(128),
    processing_expires_at timestamptz,
    claimed_at timestamptz,
    resolved_tenant_id varchar(20),
    resolved_call_id varchar(64),
    error_message varchar(1000),
    received_at timestamptz not null,
    resolved_at timestamptz
);

create unique index if not exists uk_webhook_quarantine_provider_dedupe
    on ai_call_webhook_quarantine (
        provider,
        provider_namespace,
        dedupe_key
    );

create index if not exists idx_webhook_quarantine_retry
    on ai_call_webhook_quarantine (
        status,
        next_retry_at,
        received_at
    );

create index if not exists idx_webhook_quarantine_recovery
    on ai_call_webhook_quarantine (
        status,
        processing_expires_at
    );

commit;
