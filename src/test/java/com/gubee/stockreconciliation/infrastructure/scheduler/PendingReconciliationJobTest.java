package com.gubee.stockreconciliation.infrastructure.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gubee.stockreconciliation.adapter.out.postgres.entity.ProcessedEventJpaEntity;
import com.gubee.stockreconciliation.adapter.out.postgres.repository.ProcessedEventJpaRepository;
import com.gubee.stockreconciliation.infrastructure.metrics.StockMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the two-level escalation strategy of PendingReconciliationJob.
 *
 * Escalation rules:
 *   WARN level  — event older than pendingThresholdMinutes (10) but younger than dlqThresholdMinutes (30)
 *   DLQ level   — event older than dlqThresholdMinutes (30)
 *
 * Idempotency: the job is read-only on the DB; no rows are modified.
 */
@ExtendWith(MockitoExtension.class)
class PendingReconciliationJobTest {

    @Mock ProcessedEventJpaRepository processedEventRepository;
    @Mock StockMetrics stockMetrics;
    @Mock KafkaTemplate<String, String> kafkaTemplate;

    PendingReconciliationJob job;

    private static final long WARN_THRESHOLD_MINUTES = 10L;
    private static final long DLQ_THRESHOLD_MINUTES  = 30L;
    private static final String DLQ_TOPIC            = "stock-events-dlq";

    @BeforeEach
    void setUp() {
        job = new PendingReconciliationJob(
                processedEventRepository, stockMetrics, kafkaTemplate,
                new ObjectMapper(),
                WARN_THRESHOLD_MINUTES, DLQ_THRESHOLD_MINUTES, DLQ_TOPIC
        );
    }

    private ProcessedEventJpaEntity pendingEntity(Instant processedAt) {
        ProcessedEventJpaEntity e = new ProcessedEventJpaEntity();
        e.setId(UUID.randomUUID());
        e.setEventId("evt-" + UUID.randomUUID());
        e.setEventType("ORDER_CANCELLED");
        e.setStatus("PENDING");
        e.setAccountId("acc-001");
        e.setSku("SKU-A");
        e.setOccurredAt(processedAt);
        e.setProcessedAt(processedAt);
        return e;
    }

    @Nested
    @DisplayName("When no stale pending events exist")
    class NoStaleEvents {

        @Test
        @DisplayName("Updates backlog gauge to 0 and returns without any further action")
        void updates_backlog_to_zero_and_returns() {
            when(processedEventRepository.findByStatusAndProcessedAtBefore(anyString(), any()))
                    .thenReturn(List.of());

            job.checkStalePending();

            verify(stockMetrics).updateReconciliationBacklog(0);
            verify(stockMetrics, never()).recordReconciliationAction(anyString(), anyString());
            verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
        }
    }

    @Nested
    @DisplayName("WARN level escalation (10–30 minutes stale)")
    class WarnLevelEscalation {

        @Test
        @DisplayName("Emits WARN log action and records pending age — no DLQ publish")
        void emits_warn_action_without_dlq_publish() {
            // Event processed 15 minutes ago: beyond WARN threshold but before DLQ threshold
            Instant fifteenMinutesAgo = Instant.now().minus(15, ChronoUnit.MINUTES);
            ProcessedEventJpaEntity entity = pendingEntity(fifteenMinutesAgo);

            when(processedEventRepository.findByStatusAndProcessedAtBefore(eq("PENDING"), any()))
                    .thenReturn(List.of(entity));

            job.checkStalePending();

            verify(stockMetrics).updateReconciliationBacklog(1);
            verify(stockMetrics).recordPendingAge(any());
            verify(stockMetrics).recordReconciliationAction(eq("ORDER_CANCELLED"), eq("warn"));
            // Must NOT publish to DLQ at WARN level
            verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
            verify(stockMetrics, never()).recordDlqEvent(anyString(), anyString());
        }
    }

    @Nested
    @DisplayName("DLQ level escalation (>30 minutes stale)")
    class DlqLevelEscalation {

        @Test
        @DisplayName("Publishes to DLQ and records DLQ metric for events older than dlq threshold")
        void publishes_to_dlq_for_very_stale_event() {
            // Event processed 45 minutes ago: beyond both thresholds
            Instant fortyFiveMinutesAgo = Instant.now().minus(45, ChronoUnit.MINUTES);
            ProcessedEventJpaEntity entity = pendingEntity(fortyFiveMinutesAgo);

            when(processedEventRepository.findByStatusAndProcessedAtBefore(eq("PENDING"), any()))
                    .thenReturn(List.of(entity));
            when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                    .thenReturn(new java.util.concurrent.CompletableFuture<>());

            job.checkStalePending();

            verify(stockMetrics).updateReconciliationBacklog(1);
            verify(stockMetrics).recordPendingAge(any());
            verify(stockMetrics).recordReconciliationAction(eq("ORDER_CANCELLED"), eq("dlq"));
            verify(kafkaTemplate).send(eq(DLQ_TOPIC), anyString(), anyString());
            verify(stockMetrics).recordDlqEvent(eq("ORDER_CANCELLED"), anyString());
        }

        @Test
        @DisplayName("DLQ payload contains required fields: eventId, eventType, accountId, sku, status, pendingSince")
        void dlq_payload_contains_required_fields() throws Exception {
            Instant fortyFiveMinutesAgo = Instant.now().minus(45, ChronoUnit.MINUTES);
            ProcessedEventJpaEntity entity = pendingEntity(fortyFiveMinutesAgo);
            String expectedEventId = entity.getEventId();

            when(processedEventRepository.findByStatusAndProcessedAtBefore(eq("PENDING"), any()))
                    .thenReturn(List.of(entity));
            when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                    .thenReturn(new java.util.concurrent.CompletableFuture<>());

            job.checkStalePending();

            ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(anyString(), anyString(), payloadCaptor.capture());

            String payload = payloadCaptor.getValue();
            assertThat(payload).contains("eventId");
            assertThat(payload).contains(expectedEventId);
            assertThat(payload).contains("eventType");
            assertThat(payload).contains("accountId");
            assertThat(payload).contains("sku");
            assertThat(payload).contains("pendingSince");
        }
    }

    @Nested
    @DisplayName("Mixed escalation levels")
    class MixedEscalation {

        @Test
        @DisplayName("Correctly separates warn-level and dlq-level events in a single run")
        void separates_warn_and_dlq_level_events() {
            Instant fifteenMinutesAgo = Instant.now().minus(15, ChronoUnit.MINUTES);
            Instant fortyFiveMinutesAgo = Instant.now().minus(45, ChronoUnit.MINUTES);

            ProcessedEventJpaEntity warnEvent = pendingEntity(fifteenMinutesAgo);
            ProcessedEventJpaEntity dlqEvent  = pendingEntity(fortyFiveMinutesAgo);

            when(processedEventRepository.findByStatusAndProcessedAtBefore(eq("PENDING"), any()))
                    .thenReturn(List.of(warnEvent, dlqEvent));
            when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                    .thenReturn(new java.util.concurrent.CompletableFuture<>());

            job.checkStalePending();

            verify(stockMetrics).updateReconciliationBacklog(2);
            verify(stockMetrics, times(2)).recordPendingAge(any());
            verify(stockMetrics).recordReconciliationAction(anyString(), eq("warn"));
            verify(stockMetrics).recordReconciliationAction(anyString(), eq("dlq"));
            // Only one DLQ publish (the dlqEvent)
            verify(kafkaTemplate, times(1)).send(eq(DLQ_TOPIC), anyString(), anyString());
        }
    }

    @Nested
    @DisplayName("Idempotency")
    class Idempotency {

        @Test
        @DisplayName("Job is strictly read-only on the DB — no rows modified between runs")
        void job_does_not_modify_db_rows() {
            Instant fortyFiveMinutesAgo = Instant.now().minus(45, ChronoUnit.MINUTES);
            when(processedEventRepository.findByStatusAndProcessedAtBefore(eq("PENDING"), any()))
                    .thenReturn(List.of(pendingEntity(fortyFiveMinutesAgo)));
            when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                    .thenReturn(new java.util.concurrent.CompletableFuture<>());

            job.checkStalePending();
            job.checkStalePending();  // second run — same event still "stale"

            // DB should never be modified by this job (no save, update, or delete calls)
            verify(processedEventRepository, never()).save(any());
            verify(processedEventRepository, never()).guardInsert(anyString(), anyString(),
                    anyString(), anyString(), anyString(), any());
            verify(processedEventRepository, never()).finalizeStatus(anyString(), anyString(), anyString());
        }
    }
}
