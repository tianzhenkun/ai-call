-- Phase J1: AI Call 知识库核心表与 PostgreSQL 词法检索（可重复执行）

create or replace function ai_call_knowledge_normalize(input text)
returns text
language sql
immutable
parallel safe
return lower(
    regexp_replace(
        normalize(coalesce(input, ''), NFKC),
        '[[:space:][:punct:]]+',
        '',
        'g'
    )
);

create or replace function ai_call_knowledge_ngrams(input text, width integer)
returns text[]
language sql
immutable
parallel safe
as $$
    with normalized as (
        select ai_call_knowledge_normalize(input) as value
    )
    select coalesce(array_agg(gram order by gram), array[]::text[])
    from (
        select distinct substring(value from position for width) as gram
        from normalized
        cross join lateral generate_series(
            1,
            greatest(char_length(value) - width + 1, 0)
        ) as position
        where width between 2 and 4
    ) as distinct_grams;
$$;

create or replace function ai_call_knowledge_ngram_tsvector(input text)
returns tsvector
language sql
immutable
parallel safe
as $$
    select
        setweight(
            to_tsvector(
                'simple',
                array_to_string(ai_call_knowledge_ngrams(input, 4), ' ')
            ),
            'A'
        ) ||
        setweight(
            to_tsvector(
                'simple',
                array_to_string(ai_call_knowledge_ngrams(input, 3), ' ')
            ),
            'B'
        ) ||
        setweight(
            to_tsvector(
                'simple',
                array_to_string(ai_call_knowledge_ngrams(input, 2), ' ')
            ),
            'D'
        );
$$;

create or replace function ai_call_knowledge_ngram_tsquery(input text)
returns tsquery
language sql
immutable
parallel safe
as $$
    with normalized as (
        select ai_call_knowledge_normalize(input) as value
    ), grams as (
        select distinct gram
        from normalized
        cross join lateral unnest(
            ai_call_knowledge_ngrams(value, 2) ||
            ai_call_knowledge_ngrams(value, 3) ||
            ai_call_knowledge_ngrams(value, 4)
        ) as gram
    )
    select case
        when (select char_length(value) from normalized) < 2 then null
        else to_tsquery(
            'simple',
            (select string_agg(gram, ' | ' order by char_length(gram) desc, gram) from grams)
        )
    end;
$$;

create table if not exists ai_call_knowledge_item (
    id bigint primary key,
    tenant_id varchar(20) not null,
    display_name varchar(255) not null,
    content_category varchar(20) not null,
    note varchar(1000),
    current_ready_version_id bigint,
    created_by bigint,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    deleted_at timestamptz,
    constraint ck_ai_call_knowledge_item_category check (
        content_category in (
            'PRODUCT_SERVICE', 'FAQ', 'PROFESSIONAL', 'INDUSTRY', 'OTHER'
        )
    )
);

create index if not exists idx_ai_call_knowledge_item_tenant_updated
    on ai_call_knowledge_item (tenant_id, deleted_at, updated_at);

create table if not exists ai_call_knowledge_version (
    id bigint primary key,
    tenant_id varchar(20) not null,
    knowledge_item_id bigint not null,
    version_no integer not null,
    status varchar(20) not null default 'UPLOADING',
    source_object_key varchar(1000) not null,
    source_filename varchar(255) not null,
    extension varchar(20) not null,
    mime_type varchar(255) not null,
    byte_size bigint not null,
    sha256 varchar(64) not null,
    upload_operation varchar(32),
    upload_idempotency_key varchar(128),
    upload_request_fingerprint varchar(64),
    parser_name varchar(64),
    parser_version varchar(64),
    chunk_strategy_version varchar(64),
    chunk_count integer not null default 0,
    chunk_set_sha256 varchar(64),
    attempt_count integer not null default 0,
    next_attempt_at timestamptz,
    lease_owner varchar(128),
    lease_expires_at timestamptz,
    processing_warning_json text,
    failure_code varchar(64),
    failure_message varchar(1000),
    failure_retryable boolean not null default false,
    created_by bigint,
    created_at timestamptz not null,
    ready_at timestamptz,
    constraint uk_ai_call_knowledge_version_number
        unique (tenant_id, knowledge_item_id, version_no),
    constraint uk_ai_call_knowledge_version_upload_key
        unique (tenant_id, upload_operation, upload_idempotency_key),
    constraint ck_ai_call_knowledge_version_status check (
        status in ('UPLOADING', 'PROCESSING', 'READY', 'FAILED')
    ),
    constraint ck_ai_call_knowledge_version_number check (version_no > 0),
    constraint ck_ai_call_knowledge_version_attempt_count check (attempt_count >= 0),
    constraint ck_ai_call_knowledge_version_chunk_count check (chunk_count >= 0),
    constraint ck_ai_call_knowledge_version_ready check (
        status <> 'READY'
        or (chunk_count > 0 and chunk_set_sha256 is not null and ready_at is not null)
    )
);

create index if not exists idx_ai_call_knowledge_version_item_created
    on ai_call_knowledge_version (tenant_id, knowledge_item_id, created_at);
create index if not exists idx_ai_call_knowledge_version_claim
    on ai_call_knowledge_version (status, next_attempt_at, lease_expires_at);

create table if not exists ai_call_knowledge_chunk (
    id bigint primary key,
    tenant_id varchar(20) not null,
    knowledge_version_id bigint not null,
    chunk_index integer not null,
    content text not null,
    content_checksum varchar(64) not null,
    content_type varchar(32) not null,
    source_type varchar(32) not null,
    page_no integer,
    section_path varchar(1000),
    source_path varchar(1000),
    start_ms bigint,
    end_ms bigint,
    speaker_id varchar(128),
    token_count integer,
    created_at timestamptz not null,
    ngram_tsv tsvector generated always as (
        ai_call_knowledge_ngram_tsvector(content)
    ) stored,
    constraint uk_ai_call_knowledge_chunk_position
        unique (tenant_id, knowledge_version_id, chunk_index),
    constraint ck_ai_call_knowledge_chunk_index check (chunk_index >= 0),
    constraint ck_ai_call_knowledge_chunk_content check (length(content) > 0)
);

-- 通用 `MappedBase.create_all()` 只创建可移植业务列；即使表已存在，
-- PostgreSQL 迁移也必须补上专用检索列。
alter table ai_call_knowledge_chunk
    add column if not exists ngram_tsv tsvector generated always as (
        ai_call_knowledge_ngram_tsvector(content)
    ) stored;

create index if not exists idx_ai_call_knowledge_chunk_scope
    on ai_call_knowledge_chunk (tenant_id, knowledge_version_id, chunk_index);
create index if not exists idx_ai_call_knowledge_chunk_ngram_tsv
    on ai_call_knowledge_chunk using gin (ngram_tsv);

create table if not exists ai_call_prompt_knowledge_binding (
    id bigint primary key,
    tenant_id varchar(20) not null,
    prompt_profile_id bigint not null,
    knowledge_item_id bigint not null,
    created_by bigint,
    created_at timestamptz not null,
    constraint uk_ai_call_prompt_knowledge_binding
        unique (tenant_id, prompt_profile_id, knowledge_item_id)
);

create table if not exists ai_call_knowledge_usage (
    id bigint primary key,
    tenant_id varchar(20) not null,
    purpose varchar(32) not null,
    prompt_profile_id bigint not null,
    task_id bigint,
    call_id varchar(64),
    customer_transcript_event_id varchar(64),
    tool_call_id varchar(128),
    tool_result_event_id varchar(64),
    answer_event_id varchar(64),
    qwen_response_id varchar(128),
    query_hash varchar(64),
    query_excerpt_redacted varchar(500),
    knowledge_version_ids text not null,
    version_snapshot_hash varchar(64) not null,
    status varchar(20) not null,
    retriever_version varchar(64) not null,
    model_name varchar(128),
    evidence_json text,
    latency_ms bigint,
    created_at timestamptz not null,
    constraint ck_ai_call_knowledge_usage_purpose check (
        purpose in ('PRODUCT_SUMMARY', 'REALTIME_ANSWER')
    ),
    constraint ck_ai_call_knowledge_usage_status check (
        status in ('OK', 'NO_HIT', 'TIMEOUT', 'FAILED')
    )
);

create index if not exists idx_ai_call_knowledge_usage_tenant_created
    on ai_call_knowledge_usage (tenant_id, created_at);
create index if not exists idx_ai_call_knowledge_usage_call_created
    on ai_call_knowledge_usage (tenant_id, call_id, created_at);

comment on table ai_call_knowledge_item is 'AI Call 租户知识条目';
comment on table ai_call_knowledge_version is 'AI Call 知识不可变版本';
comment on table ai_call_knowledge_chunk is 'AI Call 知识证据切片';
comment on table ai_call_prompt_knowledge_binding is 'AI Call 提示词场景与知识条目绑定';
comment on table ai_call_knowledge_usage is 'AI Call 知识使用审计';
