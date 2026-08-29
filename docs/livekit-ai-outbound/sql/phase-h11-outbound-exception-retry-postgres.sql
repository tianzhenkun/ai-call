CREATE TABLE IF NOT EXISTS ai_call_outbound_exception_policy (
    id BIGINT PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    category VARCHAR(32) NOT NULL,
    interval_days INTEGER NOT NULL,
    max_retry_count INTEGER NOT NULL,
    created_by BIGINT NOT NULL,
    updated_by BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uk_outbound_exception_policy_tenant_category
        UNIQUE (tenant_id, category)
);

CREATE TABLE IF NOT EXISTS ai_call_outbound_exception_batch (
    id BIGINT PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    category VARCHAR(32) NOT NULL,
    status VARCHAR(16) NOT NULL,
    interval_days INTEGER NOT NULL,
    max_retry_count INTEGER NOT NULL,
    cutoff_at TIMESTAMPTZ NOT NULL,
    target_count INTEGER NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    request_fingerprint VARCHAR(64) NOT NULL,
    active_slot VARCHAR(32),
    created_by BIGINT NOT NULL,
    created_by_name VARCHAR(100),
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uk_outbound_exception_batch_tenant_idempotency
        UNIQUE (tenant_id, idempotency_key),
    CONSTRAINT uk_outbound_exception_batch_tenant_active
        UNIQUE (tenant_id, active_slot)
);

ALTER TABLE ai_call_outbound_exception_batch
    ADD COLUMN IF NOT EXISTS created_by_name VARCHAR(100);

ALTER TABLE ai_call_outbound_target
    ADD COLUMN IF NOT EXISTS exception_category VARCHAR(32),
    ADD COLUMN IF NOT EXISTS exception_source_result VARCHAR(64),
    ADD COLUMN IF NOT EXISTS exception_original_attempt_count INTEGER,
    ADD COLUMN IF NOT EXISTS exception_batch_id BIGINT,
    ADD COLUMN IF NOT EXISTS exception_entered_at TIMESTAMPTZ;

UPDATE ai_call_outbound_target
SET exception_category = CASE
        WHEN latest_result IN ('no_answer', 'busy') THEN 'no_answer'
        WHEN latest_result = 'rejected' THEN 'rejected'
        WHEN latest_result = 'invalid_number' THEN 'invalid_number'
    END,
    exception_source_result = latest_result,
    exception_original_attempt_count = attempt_count,
    exception_entered_at = updated_at
WHERE status = 'COMPLETED'
  AND next_attempt_at IS NULL
  AND exception_category IS NULL
  AND latest_result IN ('no_answer', 'busy', 'rejected', 'invalid_number');

WITH strict_early_hangup AS (
    SELECT DISTINCT ON (target.id)
        target.id AS target_id,
        target.attempt_count,
        target.updated_at
    FROM ai_call_outbound_target target
    JOIN ai_call_outbound_attempt attempt
      ON attempt.tenant_id = target.tenant_id
     AND attempt.target_id = target.id
     AND NOT EXISTS (
        SELECT 1
        FROM ai_call_outbound_attempt newer_attempt
        WHERE newer_attempt.tenant_id = attempt.tenant_id
          AND newer_attempt.target_id = attempt.target_id
          AND newer_attempt.attempt_no > attempt.attempt_no
     )
    JOIN ai_call_record record
      ON record.call_id = attempt.call_id
     AND (record.tenant_id IS NULL OR record.tenant_id = target.tenant_id)
    JOIN LATERAL (
        SELECT evidence.*
        FROM ai_call_end_evidence evidence
        WHERE evidence.tenant_id = target.tenant_id
          AND evidence.call_id = attempt.call_id
        ORDER BY COALESCE(evidence.event_at, evidence.received_at), evidence.received_at, evidence.id
        LIMIT 1
    ) first_evidence ON TRUE
    WHERE target.status = 'COMPLETED'
      AND target.next_attempt_at IS NULL
      AND target.exception_category IS NULL
      AND attempt.call_result = 'connected'
      AND record.entry_type = 'direct_sip'
      AND record.answered_at IS NOT NULL
      AND record.duration_ms BETWEEN 0 AND 5000
      AND first_evidence.source = 'livekit_webhook'
      AND first_evidence.end_reason = 'sip_participant_left'
      AND COALESCE(
          first_evidence.evidence_json::jsonb ->> 'disconnectReason',
          first_evidence.evidence_json::jsonb -> 'participant' ->> 'disconnectReason'
      ) = 'CLIENT_INITIATED'
    ORDER BY target.id, attempt.attempt_no DESC
)
UPDATE ai_call_outbound_target target
SET exception_category = 'early_hangup',
    exception_source_result = 'early_hangup',
    exception_original_attempt_count = strict_early_hangup.attempt_count,
    exception_entered_at = strict_early_hangup.updated_at,
    latest_result = 'early_hangup'
FROM strict_early_hangup
WHERE target.id = strict_early_hangup.target_id;

CREATE INDEX IF NOT EXISTS idx_outbound_exception_batch_tenant_category
    ON ai_call_outbound_exception_batch (tenant_id, category, created_at);

CREATE INDEX IF NOT EXISTS idx_outbound_target_exception
    ON ai_call_outbound_target (
        tenant_id,
        exception_category,
        exception_batch_id,
        exception_entered_at
    );
