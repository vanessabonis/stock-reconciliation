-- Outbox retention support.
-- PUBLISHED entries are purged after 30 days by OutboxPurgeService.
-- archived_at is set before deletion, enabling a grace-period archive
-- strategy (e.g. copy to cold storage before row removal) if needed later.

ALTER TABLE outbox_events
    ADD COLUMN IF NOT EXISTS archived_at TIMESTAMP WITH TIME ZONE;

-- Partial index covering only PUBLISHED rows ordered by published_at.
-- Keeps the purge DELETE fast regardless of how many rows accumulate,
-- without affecting the relay's PENDING index (idx_outbox_pending).
CREATE INDEX idx_outbox_published_cleanup
    ON outbox_events(status, published_at)
    WHERE status = 'PUBLISHED';
