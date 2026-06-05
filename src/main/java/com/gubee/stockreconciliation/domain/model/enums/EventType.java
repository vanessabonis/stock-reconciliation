package com.gubee.stockreconciliation.domain.model.enums;

import com.gubee.stockreconciliation.domain.model.vo.Quantity;

public enum EventType {
    ORDER_CREATED,
    ORDER_CANCELLED,
    STOCK_ADJUSTED,
    STOCK_SYNC_SENT,
    MARKETPLACE_STOCK_RESTORED;

    public Quantity resolveQuantity(Integer quantity, Integer available, Integer quantitySent) {
        return switch (this) {
            case STOCK_ADJUSTED  -> Quantity.of(available    != null ? available    : 0);
            case STOCK_SYNC_SENT -> Quantity.of(quantitySent != null ? quantitySent : 0);
            default              -> Quantity.of(quantity     != null ? quantity     : 0);
        };
    }
}
