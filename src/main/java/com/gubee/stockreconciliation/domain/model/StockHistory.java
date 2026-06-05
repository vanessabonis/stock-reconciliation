package com.gubee.stockreconciliation.domain.model;

import com.gubee.stockreconciliation.domain.model.enums.EventType;
import com.gubee.stockreconciliation.domain.model.vo.EventId;
import com.gubee.stockreconciliation.domain.model.vo.Quantity;

import java.time.Instant;
import java.util.UUID;

/**
 * Registro de auditoria de uma única alteração de saldo de estoque.
 * Criado exclusivamente por Stock.apply() — nunca instanciado diretamente por services.
 * reconstitute() existe apenas para o mapper de persistência reconstruir objetos de domínio a partir de linhas do banco.
 */
public final class StockHistory {

    private UUID id;
    private final UUID stockId;
    private final EventId eventId;
    private final EventType eventType;
    private final Quantity quantityBefore;
    private final Quantity quantityAfter;
    private final int delta;
    private final Instant occurredAt;
    private Instant createdAt;
    private final String marketplace;
    private final String externalOrderId;
    private final String reason;

    /**
     * Reconstrói um StockHistory a partir de dados persistidos.
     * Somente o mapper de persistência deve chamar este método — objetos de domínio são criados via Stock.apply().
     */
    public static StockHistory reconstitute(
            UUID id, UUID stockId, EventId eventId, EventType eventType,
            Quantity quantityBefore, Quantity quantityAfter, int delta,
            Instant occurredAt, Instant createdAt,
            String marketplace, String externalOrderId, String reason) {
        StockHistory h = new StockHistory(stockId, eventId, eventType, quantityBefore, quantityAfter,
                occurredAt, marketplace, externalOrderId, reason);
        h.id = id;
        h.createdAt = createdAt;
        return h;
    }

    StockHistory(
            UUID stockId,
            EventId eventId,
            EventType eventType,
            Quantity quantityBefore,
            Quantity quantityAfter,
            Instant occurredAt,
            String marketplace,
            String externalOrderId,
            String reason
    ) {
        this.id = UUID.randomUUID();
        this.stockId = stockId;
        this.eventId = eventId;
        this.eventType = eventType;
        this.quantityBefore = quantityBefore;
        this.quantityAfter = quantityAfter;
        this.delta = quantityAfter.value() - quantityBefore.value();
        this.occurredAt = occurredAt;
        this.createdAt = Instant.now();
        this.marketplace = marketplace;
        this.externalOrderId = externalOrderId;
        this.reason = reason;
    }

    public UUID getId() { return id; }
    public UUID getStockId() { return stockId; }
    public EventId getEventId() { return eventId; }
    public EventType getEventType() { return eventType; }
    public Quantity getQuantityBefore() { return quantityBefore; }
    public Quantity getQuantityAfter() { return quantityAfter; }
    public int getDelta() { return delta; }
    public Instant getOccurredAt() { return occurredAt; }
    public Instant getCreatedAt() { return createdAt; }
    public String getMarketplace() { return marketplace; }
    public String getExternalOrderId() { return externalOrderId; }
    public String getReason() { return reason; }
}
