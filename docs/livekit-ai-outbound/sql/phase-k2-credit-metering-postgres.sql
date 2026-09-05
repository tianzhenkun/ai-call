BEGIN;

CREATE TABLE IF NOT EXISTS reach_credit_usage_outbox (
  id bigint PRIMARY KEY,
  tenant_id varchar(64) NOT NULL,
  owner_id varchar(64) NOT NULL,
  product_code varchar(64) NOT NULL,
  scenario_code varchar(64) NOT NULL,
  meter_item_code varchar(64) NOT NULL,
  quantity numeric(28, 8) NOT NULL CHECK (quantity > 0),
  source_id varchar(128) NOT NULL,
  idempotency_key varchar(128) NOT NULL,
  occurred_at timestamptz NOT NULL,
  payload_json text NOT NULL DEFAULT '{}',
  status varchar(16) NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'sending', 'sent', 'failed')),
  attempt_count integer NOT NULL DEFAULT 0,
  next_attempt_at timestamptz,
  lease_owner varchar(128),
  lease_expires_at timestamptz,
  last_error text,
  sent_at timestamptz,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  CONSTRAINT uk_reach_credit_usage_outbox_idempotency
    UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_reach_credit_usage_outbox_dispatch
  ON reach_credit_usage_outbox (status, next_attempt_at, created_at);

COMMIT;
