package com.gubee.stockreconciliation.domain.model.vo;

public record EventId(String value) {

    public EventId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("eventId cannot be blank");
        }
    }

    public static EventId of(String value) {
        return new EventId(value);
    }
}
