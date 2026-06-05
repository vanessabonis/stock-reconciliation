package com.gubee.stockreconciliation.adapter.in.web.dto;

import java.time.Instant;

public record StockHistoryEntryResponse(
        String eventId,
        String eventType,
        int quantityBefore,
        int quantityAfter,
        int delta,
        Instant occurredAt,
        String marketplace,
        String externalOrderId,
        String reason
) {}
