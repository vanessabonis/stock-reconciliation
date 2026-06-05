package com.gubee.stockreconciliation.adapter.in.web.dto;

public record StockEventResponse(
        String eventId,
        String status,
        String message
) {
    public static StockEventResponse of(String eventId, String status, String message) {
        return new StockEventResponse(eventId, status, message);
    }
}
