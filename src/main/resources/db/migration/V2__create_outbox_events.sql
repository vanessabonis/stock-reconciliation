-- Transactional Outbox: written in the same DB transaction as the stock update.
-- The relay polls this table and publishes PENDING entries to Kafka.
-- status: PENDING → PUBLISHED | FAILED
CREATE TABLE outbox_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type  VARCHAR(50)  NOT NULL,                          -- 'STOCK'
    aggregate_id    VARCHAR(255) NOT NULL,                          -- accountId:sku (Kafka message key)
    event_type      VARCHAR(50)  NOT NULL,
    payload         JSONB        NOT NULL,
    status          VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMP WITH TIME ZONE,
    retry_count     INT          NOT NULL DEFAULT 0,
    last_error      TEXT
);

-- Partial index on PENDING entries only — the relay only reads this status.
-- Keeps the index small as PUBLISHED rows accumulate over time.
CREATE INDEX idx_outbox_pending ON outbox_events(status, created_at)
    WHERE status = 'PENDING';
