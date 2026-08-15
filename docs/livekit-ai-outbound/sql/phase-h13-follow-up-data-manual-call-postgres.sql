BEGIN;

CREATE TABLE IF NOT EXISTS ai_call_follow_up_call_request (
    id BIGINT PRIMARY KEY,
    tenant_id VARCHAR(20) NOT NULL,
    follow_up_data_id BIGINT NOT NULL,
    follow_up_id BIGINT NULL,
    call_id VARCHAR(64) NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    request_fingerprint VARCHAR(64) NOT NULL,
    assignment_action VARCHAR(16) NOT NULL,
    previous_owner_agent_identity VARCHAR(128) NULL,
    new_owner_agent_identity VARCHAR(128) NOT NULL,
    takeover_reason VARCHAR(500) NULL,
    changed_by VARCHAR(128) NOT NULL,
    changed_by_name VARCHAR(100) NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uk_ai_call_follow_up_call_request_key
        UNIQUE (tenant_id, idempotency_key),
    CONSTRAINT uk_ai_call_follow_up_call_request_call
        UNIQUE (tenant_id, call_id),
    CONSTRAINT ck_ai_call_follow_up_call_request_action
        CHECK (assignment_action IN ('direct', 'claim', 'owned', 'takeover'))
);

CREATE INDEX IF NOT EXISTS idx_ai_call_follow_up_call_request_data_time
    ON ai_call_follow_up_call_request (tenant_id, follow_up_data_id, created_at);

COMMIT;
