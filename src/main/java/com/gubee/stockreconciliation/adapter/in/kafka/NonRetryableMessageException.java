package com.gubee.stockreconciliation.adapter.in.kafka;

/**
 * Thrown when a Kafka message cannot be processed regardless of retries.
 *
 * Covers payload-level failures: malformed JSON, unknown event type, invalid field values.
 * The bytes on the wire will never change, so retrying is pointless — it would only consume
 * backoff time and delay other messages on the same partition.
 *
 * Registered in DefaultErrorHandler.addNotRetryableExceptions() so Spring Kafka routes
 * these directly to the DLQ without any retry backoff.
 *
 * Contrast with plain RuntimeException from the same consumer, which is retryable:
 * transient DB errors, optimistic lock collisions, and network timeouts may resolve on retry.
 */
public class NonRetryableMessageException extends RuntimeException {

    public NonRetryableMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
