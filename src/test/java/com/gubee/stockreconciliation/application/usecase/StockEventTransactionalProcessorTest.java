package com.gubee.stockreconciliation.application.usecase;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gubee.stockreconciliation.domain.model.StockEvent;
import com.gubee.stockreconciliation.domain.model.enums.EventStatus;
import com.gubee.stockreconciliation.domain.model.enums.EventType;
import com.gubee.stockreconciliation.domain.model.vo.AccountId;
import com.gubee.stockreconciliation.domain.model.vo.EventId;
import com.gubee.stockreconciliation.domain.model.vo.Quantity;
import com.gubee.stockreconciliation.domain.model.vo.Sku;
import com.gubee.stockreconciliation.domain.port.out.OrderStateRepository;
import com.gubee.stockreconciliation.domain.port.out.OutboxRepository;
import com.gubee.stockreconciliation.domain.port.out.ProcessedEventRepository;
import com.gubee.stockreconciliation.domain.port.out.StockRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for StockEventTransactionalProcessor.
 *
 * Key contracts:
 * 1. tryInsert() is ALWAYS called first — before any business logic.
 * 2. If tryInsert() returns false (duplicate), process() returns IGNORED immediately
 *    without touching stock, order state, or outbox.
 * 3. If tryInsert() returns true (new event), business logic runs and finalizeStatus() is called.
 */
@ExtendWith(MockitoExtension.class)
class StockEventTransactionalProcessorTest {

    @Mock StockRepository stockRepository;
    @Mock OrderStateRepository orderStateRepository;
    @Mock ProcessedEventRepository processedEventRepository;
    @Mock OutboxRepository outboxRepository;

    StockEventTransactionalProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new StockEventTransactionalProcessor(
                stockRepository, orderStateRepository,
                processedEventRepository, outboxRepository,
                new ObjectMapper()
        );
    }

    private StockEvent adjustEvent(String eventId, int available) {
        return new StockEvent(
                EventId.of(eventId),
                EventType.STOCK_ADJUSTED,
                Instant.now(),
                AccountId.of("acc-001"),
                Sku.of("SKU-A"),
                null, null,
                Quantity.of(available),
                "test"
        );
    }

    private StockEvent orderCreatedEvent(String eventId, int qty) {
        return new StockEvent(
                EventId.of(eventId),
                EventType.ORDER_CREATED,
                Instant.now(),
                AccountId.of("acc-001"),
                Sku.of("SKU-A"),
                "ML", "ML-1",
                Quantity.of(qty),
                null
        );
    }

    @Nested
    @DisplayName("Idempotency guard (tryInsert)")
    class IdempotencyGuard {

        @Test
        @DisplayName("tryInsert is called at the START of process(), before any repository reads")
        void tryInsert_called_first_before_any_business_logic() {
            StockEvent event = adjustEvent("evt-dup", 10);
            // Simulate duplicate: tryInsert returns false
            when(processedEventRepository.tryInsert(any(), any(), any(), any(), any()))
                    .thenReturn(false);

            processor.process(event);

            InOrder order = inOrder(processedEventRepository, stockRepository, orderStateRepository);
            order.verify(processedEventRepository).tryInsert(
                    eq(EventId.of("evt-dup")), eq(EventType.STOCK_ADJUSTED),
                    eq(AccountId.of("acc-001")), eq(Sku.of("SKU-A")), any(Instant.class));
            // Stock and order repositories must NOT be touched on duplicate
            order.verify(stockRepository, never()).findByKey(any());
            order.verify(orderStateRepository, never()).findByKey(any(), any(), any(), any());
        }

        @Test
        @DisplayName("When tryInsert returns false (duplicate), process() returns IGNORED immediately")
        void when_tryInsert_returns_false_process_returns_ignored() {
            StockEvent event = adjustEvent("evt-dup", 10);
            when(processedEventRepository.tryInsert(any(), any(), any(), any(), any()))
                    .thenReturn(false);

            EventStatus result = processor.process(event);

            assertThat(result).isEqualTo(EventStatus.IGNORED);
        }

        @Test
        @DisplayName("When tryInsert returns false, no stock mutation, no outbox write, no finalizeStatus")
        void when_duplicate_no_side_effects_occur() {
            StockEvent event = adjustEvent("evt-dup", 10);
            when(processedEventRepository.tryInsert(any(), any(), any(), any(), any()))
                    .thenReturn(false);

            processor.process(event);

            // No stock changes
            verify(stockRepository, never()).save(any());
            verify(stockRepository, never()).saveHistory(any());
            // No outbox entry written
            verify(outboxRepository, never()).save(any());
            // No finalizeStatus called — row was not claimed
            verify(processedEventRepository, never()).finalizeStatus(any(), any(), any());
        }

        @Test
        @DisplayName("When tryInsert returns true (new event), finalizeStatus is called after business logic")
        void when_new_event_finalizeStatus_called_with_business_outcome() {
            StockEvent event = adjustEvent("evt-new", 10);
            when(processedEventRepository.tryInsert(any(), any(), any(), any(), any()))
                    .thenReturn(true);
            when(stockRepository.findByKey(any())).thenReturn(java.util.Optional.empty());

            processor.process(event);

            verify(processedEventRepository).finalizeStatus(
                    eq(EventId.of("evt-new")), eq(EventStatus.PROCESSED), any());
        }
    }

    @Nested
    @DisplayName("STOCK_ADJUSTED handler")
    class StockAdjustedHandler {

        @Test
        @DisplayName("Creates stock when none exists and returns PROCESSED")
        void creates_new_stock_and_returns_processed() {
            StockEvent event = adjustEvent("evt-adj", 10);
            when(processedEventRepository.tryInsert(any(), any(), any(), any(), any()))
                    .thenReturn(true);
            when(stockRepository.findByKey(any())).thenReturn(java.util.Optional.empty());

            EventStatus result = processor.process(event);

            assertThat(result).isEqualTo(EventStatus.PROCESSED);
            verify(stockRepository).save(any());
            verify(stockRepository).saveHistory(any());
        }
    }

    @Nested
    @DisplayName("ORDER_CREATED handler — insufficient stock")
    class OrderCreatedHandler {

        @Test
        @DisplayName("Throws InsufficientStockException when stock is too low")
        void throws_when_stock_insufficient() {
            StockEvent event = orderCreatedEvent("evt-order", 5);
            when(processedEventRepository.tryInsert(any(), any(), any(), any(), any()))
                    .thenReturn(true);

            // Stock with only 2 units
            var stock = com.gubee.stockreconciliation.domain.model.Stock.create(
                    com.gubee.stockreconciliation.domain.model.vo.StockKey.of("acc-001", "SKU-A"));
            stock.apply(new StockEvent(EventId.of("adj"), EventType.STOCK_ADJUSTED, Instant.now(),
                    AccountId.of("acc-001"), Sku.of("SKU-A"), null, null, Quantity.of(2), null));

            when(stockRepository.findByKey(any())).thenReturn(java.util.Optional.of(stock));
            when(orderStateRepository.findByKey(any(), any(), any(), any()))
                    .thenReturn(java.util.Optional.empty());

            org.assertj.core.api.Assertions.assertThatThrownBy(() -> processor.process(event))
                    .isInstanceOf(com.gubee.stockreconciliation.domain.model.exception.InsufficientStockException.class);
        }
    }
}
