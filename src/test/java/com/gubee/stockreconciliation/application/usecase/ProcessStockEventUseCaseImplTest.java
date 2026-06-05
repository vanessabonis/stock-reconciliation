package com.gubee.stockreconciliation.application.usecase;

import com.gubee.stockreconciliation.domain.model.StockEvent;
import com.gubee.stockreconciliation.domain.model.enums.EventStatus;
import com.gubee.stockreconciliation.domain.model.enums.EventType;
import com.gubee.stockreconciliation.domain.model.exception.StockConcurrencyException;
import com.gubee.stockreconciliation.domain.model.vo.AccountId;
import com.gubee.stockreconciliation.domain.model.vo.EventId;
import com.gubee.stockreconciliation.domain.model.vo.Quantity;
import com.gubee.stockreconciliation.domain.model.vo.Sku;
import com.gubee.stockreconciliation.infrastructure.metrics.StockMetrics;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.orm.ObjectOptimisticLockingFailureException;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ProcessStockEventUseCaseImplTest {

    @Mock
    StockEventTransactionalProcessor processor;

    @Mock
    StockMetrics stockMetrics;

    ProcessStockEventUseCaseImpl useCase;

    private static StockEvent sampleEvent() {
        return new StockEvent(
                EventId.of("evt-001"),
                EventType.STOCK_ADJUSTED,
                Instant.now(),
                AccountId.of("acc-001"),
                Sku.of("SKU-A"),
                null, null,
                Quantity.of(10),
                "test"
        );
    }

    @BeforeEach
    void setUp() {
        useCase = new ProcessStockEventUseCaseImpl(processor, stockMetrics);
        // Timer.Sample is used via start() — mock startProcessingTimer to avoid NPE
        when(stockMetrics.startProcessingTimer()).thenReturn(Timer.start());
    }

    @Nested
    @DisplayName("Successful processing")
    class SuccessfulProcessing {

        @Test
        @DisplayName("Returns PROCESSED when processor returns PROCESSED")
        void returns_processed_when_processor_returns_processed() {
            when(processor.process(any())).thenReturn(EventStatus.PROCESSED);

            EventStatus result = useCase.process(sampleEvent());

            assertThat(result).isEqualTo(EventStatus.PROCESSED);
            verify(processor).process(any());
            verify(stockMetrics).recordEventProcessed("STOCK_ADJUSTED", "PROCESSED");
        }

        @Test
        @DisplayName("Returns IGNORED when processor returns IGNORED (duplicate event via tryInsert)")
        void returns_ignored_when_processor_returns_ignored() {
            when(processor.process(any())).thenReturn(EventStatus.IGNORED);

            EventStatus result = useCase.process(sampleEvent());

            assertThat(result).isEqualTo(EventStatus.IGNORED);
            verify(processor).process(any());
            verify(stockMetrics).recordEventProcessed("STOCK_ADJUSTED", "IGNORED");
        }

        @Test
        @DisplayName("Returns PENDING for out-of-order cancellation")
        void returns_pending_for_out_of_order_cancellation() {
            when(processor.process(any())).thenReturn(EventStatus.PENDING);

            EventStatus result = useCase.process(sampleEvent());

            assertThat(result).isEqualTo(EventStatus.PENDING);
        }

        @Test
        @DisplayName("Returns INCONSISTENT for duplicate business operation")
        void returns_inconsistent_for_duplicate_business_operation() {
            when(processor.process(any())).thenReturn(EventStatus.INCONSISTENT);

            EventStatus result = useCase.process(sampleEvent());

            assertThat(result).isEqualTo(EventStatus.INCONSISTENT);
        }
    }

    @Nested
    @DisplayName("Optimistic locking retry policy")
    class RetryPolicy {

        @Test
        @DisplayName("Succeeds on second attempt after single optimistic lock failure")
        void succeeds_on_second_attempt_after_single_lock_failure() {
            when(processor.process(any()))
                    .thenThrow(new ObjectOptimisticLockingFailureException("Stock", "stock-id"))
                    .thenReturn(EventStatus.PROCESSED);

            EventStatus result = useCase.process(sampleEvent());

            assertThat(result).isEqualTo(EventStatus.PROCESSED);
            verify(processor, times(2)).process(any());
            verify(stockMetrics, times(1)).recordOptimisticLockRetry(anyString());
        }

        @Test
        @DisplayName("Succeeds on third attempt after two optimistic lock failures")
        void succeeds_on_third_attempt_after_two_lock_failures() {
            when(processor.process(any()))
                    .thenThrow(new ObjectOptimisticLockingFailureException("Stock", "id"))
                    .thenThrow(new ObjectOptimisticLockingFailureException("Stock", "id"))
                    .thenReturn(EventStatus.PROCESSED);

            EventStatus result = useCase.process(sampleEvent());

            assertThat(result).isEqualTo(EventStatus.PROCESSED);
            verify(processor, times(3)).process(any());
            verify(stockMetrics, times(2)).recordOptimisticLockRetry(anyString());
        }

        @Test
        @DisplayName("Throws StockConcurrencyException after MAX_RETRIES (3) failures")
        void throws_stock_concurrency_exception_after_max_retries_exhausted() {
            when(processor.process(any()))
                    .thenThrow(new ObjectOptimisticLockingFailureException("Stock", "id"))
                    .thenThrow(new ObjectOptimisticLockingFailureException("Stock", "id"))
                    .thenThrow(new ObjectOptimisticLockingFailureException("Stock", "id"));

            assertThatThrownBy(() -> useCase.process(sampleEvent()))
                    .isInstanceOf(StockConcurrencyException.class)
                    .hasMessageContaining("evt-001");

            verify(processor, times(3)).process(any());
            verify(stockMetrics, times(3)).recordOptimisticLockRetry(anyString());
        }

        @Test
        @DisplayName("Records processing time even when StockConcurrencyException is thrown")
        void records_processing_time_on_concurrency_exception() {
            when(processor.process(any()))
                    .thenThrow(new ObjectOptimisticLockingFailureException("Stock", "id"))
                    .thenThrow(new ObjectOptimisticLockingFailureException("Stock", "id"))
                    .thenThrow(new ObjectOptimisticLockingFailureException("Stock", "id"));

            assertThatThrownBy(() -> useCase.process(sampleEvent()))
                    .isInstanceOf(StockConcurrencyException.class);

            verify(stockMetrics).recordProcessingTime(any(), anyString());
        }
    }
}
