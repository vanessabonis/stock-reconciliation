-- Stock: aggregate root. One row per (accountId, sku) pair.
-- version column drives optimistic locking — never updated manually.
CREATE TABLE stocks (
    id              UUID        NOT NULL PRIMARY KEY,
    account_id      VARCHAR(255) NOT NULL,
    sku             VARCHAR(255) NOT NULL,
    available_quantity INT       NOT NULL DEFAULT 0,
    last_updated_at TIMESTAMPTZ  NOT NULL,
    version         BIGINT       NOT NULL DEFAULT 0,
    CONSTRAINT uk_stocks_account_sku UNIQUE (account_id, sku)
);

-- Full audit trail of every balance change.
CREATE TABLE stock_history (
    id               UUID         NOT NULL PRIMARY KEY,
    stock_id         UUID         NOT NULL REFERENCES stocks(id),
    event_id         VARCHAR(255) NOT NULL,
    event_type       VARCHAR(50)  NOT NULL,
    quantity_before  INT          NOT NULL,
    quantity_after   INT          NOT NULL,
    delta            INT          NOT NULL,
    occurred_at      TIMESTAMPTZ  NOT NULL,
    created_at       TIMESTAMPTZ  NOT NULL,
    marketplace      VARCHAR(255),
    external_order_id VARCHAR(255),
    reason           VARCHAR(500)
);

CREATE INDEX idx_stock_history_stock_id ON stock_history(stock_id, occurred_at);

-- Order lifecycle tracking for idempotency and out-of-order handling.
CREATE TABLE order_states (
    id                             UUID         NOT NULL PRIMARY KEY,
    marketplace                    VARCHAR(255) NOT NULL,
    account_id                     VARCHAR(255) NOT NULL,
    external_order_id              VARCHAR(255) NOT NULL,
    sku                            VARCHAR(255) NOT NULL,
    state                          VARCHAR(50)  NOT NULL,
    quantity                       INT          NOT NULL,
    pending_cancellation_event_id  VARCHAR(255),
    created_at                     TIMESTAMPTZ  NOT NULL,
    updated_at                     TIMESTAMPTZ  NOT NULL,
    CONSTRAINT uk_order_states_key UNIQUE (marketplace, account_id, external_order_id, sku)
);

-- Idempotency guard. The unique constraint on event_id is the TOCTOU-safe mechanism.
-- EventStatus: PROCESSED | IGNORED | PENDING | INCONSISTENT
CREATE TABLE processed_events (
    id           UUID         NOT NULL PRIMARY KEY,
    event_id     VARCHAR(255) NOT NULL,
    event_type   VARCHAR(50)  NOT NULL,
    status       VARCHAR(50)  NOT NULL,
    account_id   VARCHAR(255) NOT NULL,
    sku          VARCHAR(255) NOT NULL,
    occurred_at  TIMESTAMPTZ  NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL,
    details      VARCHAR(500),
    CONSTRAINT uk_processed_events_event_id UNIQUE (event_id)
);

CREATE INDEX idx_processed_events_status ON processed_events(status);
