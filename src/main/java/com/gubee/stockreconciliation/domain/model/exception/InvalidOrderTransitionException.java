package com.gubee.stockreconciliation.domain.model.exception;

import com.gubee.stockreconciliation.domain.model.enums.EventType;
import com.gubee.stockreconciliation.domain.model.enums.OrderLifecycleState;

public class InvalidOrderTransitionException extends RuntimeException {

    public InvalidOrderTransitionException(EventType eventType, OrderLifecycleState currentState) {
        super("Invalid transition: cannot apply " + eventType + " when order is in state " + currentState);
    }

    public InvalidOrderTransitionException(String message) {
        super(message);
    }
}
