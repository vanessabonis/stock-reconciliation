package com.gubee.stockreconciliation.adapter.in.web.exception;

import com.gubee.stockreconciliation.domain.model.exception.InsufficientStockException;
import com.gubee.stockreconciliation.domain.model.exception.StockConcurrencyException;
import com.gubee.stockreconciliation.infrastructure.metrics.StockMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.util.Optional;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    private final StockMetrics stockMetrics;

    public GlobalExceptionHandler(StockMetrics stockMetrics) {
        this.stockMetrics = stockMetrics;
    }

    @ExceptionHandler(StockConcurrencyException.class)
    public ResponseEntity<ProblemDetail> handleConcurrency(StockConcurrencyException ex) {
        log.warn("Concurrency conflict: {}", ex.getMessage());
        stockMetrics.recordEventFailed(
                Optional.ofNullable(MDC.get("eventType")).orElse("UNKNOWN"),
                "optimistic_lock_exhausted");
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, ex.getMessage());
        pd.setType(URI.create("urn:problem:stock-concurrency-conflict"));
        pd.setTitle("Stock Concurrency Conflict");
        return ResponseEntity.status(HttpStatus.CONFLICT).body(pd);
    }

    @ExceptionHandler(InsufficientStockException.class)
    public ResponseEntity<ProblemDetail> handleInsufficientStock(InsufficientStockException ex) {
        log.warn("Insufficient stock: {}", ex.getMessage());
        stockMetrics.recordInsufficientStock(
                Optional.ofNullable(MDC.get("accountId")).orElse("UNKNOWN"),
                Optional.ofNullable(MDC.get("sku")).orElse("UNKNOWN"));
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_ENTITY, ex.getMessage());
        pd.setType(URI.create("urn:problem:insufficient-stock"));
        pd.setTitle("Insufficient Stock");
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(pd);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ProblemDetail> handleIllegalArgument(IllegalArgumentException ex) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
        pd.setTitle("Invalid Request");
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(pd);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ProblemDetail> handleValidation(MethodArgumentNotValidException ex) {
        stockMetrics.recordEventFailed(
                Optional.ofNullable(MDC.get("eventType")).orElse("UNKNOWN"),
                "validation_error");
        String detail = ex.getBindingResult().getFieldErrors().stream()
                .map(e -> e.getField() + ": " + e.getDefaultMessage())
                .reduce((a, b) -> a + "; " + b)
                .orElse("Validation failed");
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, detail);
        pd.setTitle("Validation Error");
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(pd);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ProblemDetail> handleGeneric(Exception ex) {
        log.error("Unexpected error", ex);
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR, "An unexpected error occurred");
        pd.setTitle("Internal Server Error");
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(pd);
    }
}
