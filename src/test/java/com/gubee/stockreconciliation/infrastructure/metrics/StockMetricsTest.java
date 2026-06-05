package com.gubee.stockreconciliation.infrastructure.metrics;

import com.gubee.stockreconciliation.domain.model.enums.EventStatus;
import com.gubee.stockreconciliation.domain.port.out.ProcessedEventRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StockMetricsTest {

    private SimpleMeterRegistry registry;
    private ProcessedEventRepository processedEventRepository;
    private StockMetrics stockMetrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        processedEventRepository = mock(ProcessedEventRepository.class);
        stockMetrics = new StockMetrics(registry, processedEventRepository);
    }

    @Test
    void recordEventProcessed_incrementsCounter() {
        stockMetrics.recordEventProcessed("ORDER_CREATED", "PROCESSED");

        double count = registry.find("gubee.events.processed")
                .tags("eventType", "ORDER_CREATED", "status", "PROCESSED")
                .counter().count();
        assertThat(count).isEqualTo(1.0);
    }

    @Test
    void recordEventFailed_incrementsCounter() {
        stockMetrics.recordEventFailed("ORDER_CREATED", "validation_error");

        double count = registry.find("gubee.events.failed")
                .tags("eventType", "ORDER_CREATED", "reason", "validation_error")
                .counter().count();
        assertThat(count).isEqualTo(1.0);
    }

    @Test
    void startProcessingTimerAndRecord_recordsElapsedTime() {
        Timer.Sample sample = stockMetrics.startProcessingTimer();
        assertThat(sample).isNotNull();

        stockMetrics.recordProcessingTime(sample, "ORDER_CREATED");

        assertThat(registry.find("gubee.event.processing.duration")
                .tag("eventType", "ORDER_CREATED").timer().count()).isEqualTo(1);
    }

    @Test
    void recordOptimisticLockRetry_incrementsCounter() {
        stockMetrics.recordOptimisticLockRetry("SKU-1");

        double count = registry.find("gubee.optimistic.lock.retries")
                .tag("sku", "SKU-1").counter().count();
        assertThat(count).isEqualTo(1.0);
    }

    @Test
    void recordInsufficientStock_incrementsCounter() {
        stockMetrics.recordInsufficientStock("acc-1", "SKU-1");

        double count = registry.find("gubee.insufficient.stock")
                .tags("accountId", "acc-1", "sku", "SKU-1").counter().count();
        assertThat(count).isEqualTo(1.0);
    }

    @Test
    void recordOutboxPublishLatency_recordsTimer() {
        stockMetrics.recordOutboxPublishLatency(Instant.now().minusSeconds(1));

        assertThat(registry.find("gubee.outbox.publish.latency").timer().count()).isEqualTo(1);
    }

    @Test
    void registerOutboxPendingGauge_registersLiveGauge() {
        stockMetrics.registerOutboxPendingGauge(() -> 42);

        Gauge gauge = registry.find("gubee.outbox.pending").gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(42.0);
    }

    @Test
    void recordPurge_incrementsCounterByDeletedRowCount() {
        stockMetrics.recordPurge(10);

        assertThat(registry.find("gubee.outbox.purged").counter().count()).isEqualTo(10.0);
    }

    @Test
    void recordDlqEvent_incrementsCounter() {
        stockMetrics.recordDlqEvent("STOCK_ADJUSTED", "max_retries_exceeded");

        double count = registry.find("gubee.dlq.events")
                .tags("eventType", "STOCK_ADJUSTED", "reason", "max_retries_exceeded")
                .counter().count();
        assertThat(count).isEqualTo(1.0);
    }

    @Test
    void registerGauges_registersEventPendingOrderstateGauge() {
        when(processedEventRepository.countByStatus(EventStatus.PENDING)).thenReturn(5L);

        stockMetrics.registerGauges();

        Gauge gauge = registry.find("gubee.events.pending.orderstate").gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(5.0);
    }

    @Test
    void registerGauges_whenRepositoryThrows_gaugeReturnsNaN() {
        when(processedEventRepository.countByStatus(EventStatus.PENDING))
                .thenThrow(new RuntimeException("DB unavailable"));

        stockMetrics.registerGauges();

        Gauge gauge = registry.find("gubee.events.pending.orderstate").gauge();
        assertThat(gauge).isNotNull();
        assertThat(Double.isNaN(gauge.value())).isTrue();
    }
}
