package com.gubee.stockreconciliation.adapter.in.web.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;

public record StockEventRequest(

        @NotBlank(message = "eventId is required")
        String eventId,

        @NotBlank(message = "type is required")
        String type,

        @NotNull(message = "occurredAt is required")
        Instant occurredAt,

        @NotBlank(message = "accountId is required")
        String accountId,

        @NotBlank(message = "sku is required")
        String sku,

        // Campos de eventos de pedido
        String marketplace,
        String externalOrderId,
        Integer quantity,

        // Campo específico de STOCK_ADJUSTED
        Integer available,
        String reason,

        // Campo específico de STOCK_SYNC_SENT
        Integer quantitySent
) {}
