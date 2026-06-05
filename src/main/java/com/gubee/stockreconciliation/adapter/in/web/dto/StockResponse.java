package com.gubee.stockreconciliation.adapter.in.web.dto;

import java.time.Instant;

public record StockResponse(
        String accountId,
        String sku,
        int availableQuantity,
        Instant lastUpdatedAt
) {}
