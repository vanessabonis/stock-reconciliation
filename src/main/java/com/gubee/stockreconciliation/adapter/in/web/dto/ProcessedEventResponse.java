package com.gubee.stockreconciliation.adapter.in.web.dto;

import java.time.Instant;

public record ProcessedEventResponse(
        String eventId,
        String eventType,
        String status,
        String accountId,
        String sku,
        Instant occurredAt,
        Instant processedAt,
        String details
) {}
