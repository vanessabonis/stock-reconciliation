package com.gubee.stockreconciliation.adapter.out.kafka;

import com.gubee.stockreconciliation.adapter.out.postgres.entity.OutboxEventJpaEntity;
import com.gubee.stockreconciliation.adapter.out.postgres.repository.OutboxEventJpaRepository;
import com.gubee.stockreconciliation.infrastructure.metrics.StockMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxRelayTest {

    @Mock
    OutboxEventJpaRepository outboxRepository;

    @Mock
    KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    StockMetrics stockMetrics;

    OutboxRelay relay;

    @BeforeEach
    void setUp() {
        relay = new OutboxRelay(outboxRepository, kafkaTemplate, stockMetrics, "stock-events-processed");
    }

    private OutboxEventJpaEntity pendingEntry() {
        OutboxEventJpaEntity e = new OutboxEventJpaEntity();
        e.setId(UUID.randomUUID());
        e.setAggregateId("acc-1:SKU-1");
        e.setPayload("{\"test\":true}");
        e.setEventType("STOCK_ADJUSTED");
        e.setRetryCount(0);
        e.setCreatedAt(Instant.now());
        e.setStatus("PENDING");
        return e;
    }

    @Test
    void processOutbox_emptyList_returnsWithoutSaving() {
        when(outboxRepository.findPendingWithLock()).thenReturn(List.of());

        relay.processOutbox();

        verify(outboxRepository, never()).save(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void processOutbox_success_marksPublishedAndRecordsLatency() {
        OutboxEventJpaEntity entry = pendingEntry();
        when(outboxRepository.findPendingWithLock()).thenReturn(List.of(entry));
        CompletableFuture<SendResult<String, String>> future = new CompletableFuture<>();
        future.complete(null);
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(future);

        relay.processOutbox();

        assertThat(entry.getStatus()).isEqualTo("PUBLISHED");
        assertThat(entry.getPublishedAt()).isNotNull();
        verify(stockMetrics).recordOutboxPublishLatency(any(Instant.class));
        verify(outboxRepository).save(entry);
    }

    @Test
    @SuppressWarnings("unchecked")
    void processOutbox_failure_belowMaxRetries_incrementsRetryCount() {
        OutboxEventJpaEntity entry = pendingEntry();
        when(outboxRepository.findPendingWithLock()).thenReturn(List.of(entry));
        CompletableFuture<SendResult<String, String>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("kafka timeout"));
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(failedFuture);

        relay.processOutbox();

        assertThat(entry.getRetryCount()).isEqualTo(1);
        assertThat(entry.getStatus()).isEqualTo("PENDING");
        assertThat(entry.getLastError()).isNotNull();
        verify(stockMetrics, never()).recordDlqEvent(any(), any());
        verify(outboxRepository).save(entry);
    }

    @Test
    @SuppressWarnings("unchecked")
    void processOutbox_failure_atMaxRetries_marksFailedAndRecordsDlq() {
        OutboxEventJpaEntity entry = pendingEntry();
        entry.setRetryCount(2);
        when(outboxRepository.findPendingWithLock()).thenReturn(List.of(entry));
        CompletableFuture<SendResult<String, String>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("broker unavailable"));
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(failedFuture);

        relay.processOutbox();

        assertThat(entry.getRetryCount()).isEqualTo(3);
        assertThat(entry.getStatus()).isEqualTo("FAILED");
        verify(stockMetrics).recordDlqEvent(eq("STOCK_ADJUSTED"), anyString());
        verify(outboxRepository).save(entry);
    }

    @Test
    void processOutbox_nullErrorMessage_setsUnknownAsLastError() {
        OutboxEventJpaEntity entry = pendingEntry();
        when(outboxRepository.findPendingWithLock()).thenReturn(List.of(entry));
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenThrow(new RuntimeException((String) null));

        relay.processOutbox();

        assertThat(entry.getLastError()).isEqualTo("unknown");
    }

    @Test
    void registerMetrics_registersOutboxPendingGauge() {
        relay.registerMetrics();

        verify(stockMetrics).registerOutboxPendingGauge(any());
    }
}
