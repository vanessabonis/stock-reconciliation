package com.gubee.stockreconciliation.domain.model;

import com.gubee.stockreconciliation.domain.model.enums.EventType;
import com.gubee.stockreconciliation.domain.model.vo.AccountId;
import com.gubee.stockreconciliation.domain.model.vo.EventId;
import com.gubee.stockreconciliation.domain.model.vo.Quantity;
import com.gubee.stockreconciliation.domain.model.vo.Sku;

import java.time.Instant;

/**
 * Comando de domínio imutável que representa um evento de estoque ou pedido recebido.
 * Java puro — sem anotações de framework.
 */
public record StockEvent(
        EventId eventId,
        EventType type,
        Instant occurredAt,
        AccountId accountId,
        Sku sku,
        String marketplace,
        String externalOrderId,
        Quantity quantity,
        String reason
) {
    public boolean isOrderEvent() {
        return type == EventType.ORDER_CREATED || type == EventType.ORDER_CANCELLED
                || type == EventType.MARKETPLACE_STOCK_RESTORED;
    }
}
