package com.gubee.stockreconciliation.adapter.in.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gubee.stockreconciliation.domain.model.enums.EventStatus;
import com.gubee.stockreconciliation.domain.model.enums.EventType;
import com.gubee.stockreconciliation.domain.port.in.ProcessStockEventUseCase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StockEventKafkaConsumerTest {

    @Mock
    ProcessStockEventUseCase processStockEventUseCase;

    StockEventKafkaConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new StockEventKafkaConsumer(processStockEventUseCase, new ObjectMapper());
    }

    @Test
    void consume_stockSyncSent_withQuantitySent_parsesCorrectly() {
        String json = """
                {"eventId":"e1","type":"STOCK_SYNC_SENT","occurredAt":"%s",
                 "accountId":"acc-1","sku":"SKU-1","quantitySent":7}
                """.formatted(Instant.now()).strip();
        ConsumerRecord<String, String> record = new ConsumerRecord<>("stock-events", 0, 0L, "acc-1:SKU-1", json);
        when(processStockEventUseCase.process(argThat(e -> e.type() == EventType.STOCK_SYNC_SENT)))
                .thenReturn(EventStatus.IGNORED);

        consumer.consume(record);

        verify(processStockEventUseCase).process(argThat(e ->
                e.type() == EventType.STOCK_SYNC_SENT && e.quantity().value() == 7));
    }

    @Test
    void consume_stockSyncSent_missingQuantitySentKey_defaultsToZero() {
        String json = """
                {"eventId":"e2","type":"STOCK_SYNC_SENT","occurredAt":"%s",
                 "accountId":"acc-1","sku":"SKU-1"}
                """.formatted(Instant.now()).strip();
        ConsumerRecord<String, String> record = new ConsumerRecord<>("stock-events", 0, 0L, "acc-1:SKU-1", json);
        when(processStockEventUseCase.process(argThat(e -> e.type() == EventType.STOCK_SYNC_SENT)))
                .thenReturn(EventStatus.IGNORED);

        consumer.consume(record);

        verify(processStockEventUseCase).process(argThat(e -> e.quantity().value() == 0));
    }

    @Test
    void consume_stockAdjusted_missingAvailableKey_defaultsToZero() {
        String json = """
                {"eventId":"e3","type":"STOCK_ADJUSTED","occurredAt":"%s",
                 "accountId":"acc-1","sku":"SKU-1"}
                """.formatted(Instant.now()).strip();
        ConsumerRecord<String, String> record = new ConsumerRecord<>("stock-events", 0, 0L, "acc-1:SKU-1", json);
        when(processStockEventUseCase.process(argThat(e -> e.type() == EventType.STOCK_ADJUSTED)))
                .thenReturn(EventStatus.PROCESSED);

        consumer.consume(record);

        verify(processStockEventUseCase).process(argThat(e -> e.quantity().value() == 0));
    }

    @Test
    void consume_orderCreated_missingQuantityKey_defaultsToZero() {
        String json = """
                {"eventId":"e4","type":"ORDER_CREATED","occurredAt":"%s",
                 "accountId":"acc-1","sku":"SKU-1","marketplace":"ML","externalOrderId":"ML-1"}
                """.formatted(Instant.now()).strip();
        ConsumerRecord<String, String> record = new ConsumerRecord<>("stock-events", 0, 0L, "acc-1:SKU-1", json);
        when(processStockEventUseCase.process(argThat(e -> e.type() == EventType.ORDER_CREATED)))
                .thenReturn(EventStatus.PROCESSED);

        consumer.consume(record);

        verify(processStockEventUseCase).process(argThat(e -> e.quantity().value() == 0));
    }

    @Test
    void consume_malformedJson_throwsRuntimeException() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("stock-events", 0, 0L, "key", "not-valid-json");

        assertThatThrownBy(() -> consumer.consume(record))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to process Kafka message");
    }
}
