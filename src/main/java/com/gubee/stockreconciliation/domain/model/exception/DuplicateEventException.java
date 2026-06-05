package com.gubee.stockreconciliation.domain.model.exception;

import com.gubee.stockreconciliation.domain.model.enums.EventStatus;

public class DuplicateEventException extends RuntimeException {

    private final EventStatus originalStatus;

    public DuplicateEventException(String eventId, EventStatus originalStatus) {
        super("Event already processed: " + eventId + " with status " + originalStatus);
        this.originalStatus = originalStatus;
    }

    public EventStatus getOriginalStatus() {
        return originalStatus;
    }
}
