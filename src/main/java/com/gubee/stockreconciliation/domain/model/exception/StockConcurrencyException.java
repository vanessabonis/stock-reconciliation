package com.gubee.stockreconciliation.domain.model.exception;

public class StockConcurrencyException extends RuntimeException {

    public StockConcurrencyException(String message) {
        super(message);
    }
}
