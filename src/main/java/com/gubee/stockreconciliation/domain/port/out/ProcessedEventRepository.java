package com.gubee.stockreconciliation.domain.port.out;

import com.gubee.stockreconciliation.domain.model.ProcessedEvent;
import com.gubee.stockreconciliation.domain.model.enums.EventStatus;
import com.gubee.stockreconciliation.domain.model.enums.EventType;
import com.gubee.stockreconciliation.domain.model.vo.AccountId;
import com.gubee.stockreconciliation.domain.model.vo.EventId;
import com.gubee.stockreconciliation.domain.model.vo.Sku;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface ProcessedEventRepository {

    ProcessedEvent save(ProcessedEvent event);

    Optional<ProcessedEvent> findByEventId(EventId eventId);

    Optional<ProcessedEvent> findPendingCancellation(String marketplace, String accountId, String externalOrderId, String sku);

    List<ProcessedEvent> findByStatus(EventStatus status);

    long countByStatus(EventStatus status);

    /**
     * Atomically claims the eventId via INSERT ON CONFLICT DO NOTHING.
     * Returns true if the slot was newly inserted (first delivery), false if it already existed (duplicate).
     * No exception is thrown on duplicate.
     */
    boolean tryInsert(EventId eventId, EventType type, AccountId accountId, Sku sku, Instant occurredAt);

    /**
     * Writes the final business outcome into the row previously claimed by tryInsert.
     */
    void finalizeStatus(EventId eventId, EventStatus status, String details);
}
