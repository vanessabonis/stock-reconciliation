package com.gubee.stockreconciliation.domain.model;

import com.gubee.stockreconciliation.domain.model.enums.EventStatus;
import com.gubee.stockreconciliation.domain.model.enums.EventType;
import com.gubee.stockreconciliation.domain.model.vo.AccountId;
import com.gubee.stockreconciliation.domain.model.vo.EventId;
import com.gubee.stockreconciliation.domain.model.vo.Sku;

import java.time.Instant;
import java.util.UUID;

/**
 * Registro de idempotência. Uma linha por eventId processado (ou tentado).
 * A constraint única em eventId impede o processamento duplicado.
 */
public final class ProcessedEvent {

    private UUID id;
    private final EventId eventId;
    private final EventType eventType;
    private EventStatus status;
    private final AccountId accountId;
    private final Sku sku;
    private final Instant occurredAt;
    private final Instant processedAt;
    private String details;

    public ProcessedEvent(
            EventId eventId,
            EventType eventType,
            EventStatus status,
            AccountId accountId,
            Sku sku,
            Instant occurredAt,
            String details
    ) {
        this.id = UUID.randomUUID();
        this.eventId = eventId;
        this.eventType = eventType;
        this.status = status;
        this.accountId = accountId;
        this.sku = sku;
        this.occurredAt = occurredAt;
        this.processedAt = Instant.now();
        this.details = details;
    }

    public void updateStatus(EventStatus newStatus, String newDetails) {
        this.status = newStatus;
        this.details = newDetails;
    }

    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }
    public EventId getEventId() { return eventId; }
    public EventType getEventType() { return eventType; }
    public EventStatus getStatus() { return status; }
    public AccountId getAccountId() { return accountId; }
    public Sku getSku() { return sku; }
    public Instant getOccurredAt() { return occurredAt; }
    public Instant getProcessedAt() { return processedAt; }
    public String getDetails() { return details; }
}
