package com.gubee.stockreconciliation.adapter.out.health;

import com.gubee.stockreconciliation.domain.port.out.OutboxRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxHealthIndicatorTest {

    @Mock
    OutboxRepository outboxRepository;

    @InjectMocks
    OutboxHealthIndicator indicator;

    @Test
    void health_up_noIssues() {
        when(outboxRepository.countByStatus("PENDING")).thenReturn(0L);
        when(outboxRepository.countStaleEntries(5)).thenReturn(0L);
        when(outboxRepository.countByStatus("FAILED")).thenReturn(0L);

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("pendingCount", 0L);
    }

    @Test
    void health_down_whenStaleEntries() {
        when(outboxRepository.countByStatus("PENDING")).thenReturn(2L);
        when(outboxRepository.countStaleEntries(5)).thenReturn(1L);
        when(outboxRepository.countByStatus("FAILED")).thenReturn(0L);

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("staleCount", 1L);
        assertThat(health.getDetails()).containsKey("reason");
    }

    @Test
    void health_down_whenFailedEntries() {
        when(outboxRepository.countByStatus("PENDING")).thenReturn(0L);
        when(outboxRepository.countStaleEntries(5)).thenReturn(0L);
        when(outboxRepository.countByStatus("FAILED")).thenReturn(3L);

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("failedCount", 3L);
    }

    @Test
    void health_down_whenBothStaleAndFailed() {
        when(outboxRepository.countByStatus("PENDING")).thenReturn(10L);
        when(outboxRepository.countStaleEntries(5)).thenReturn(2L);
        when(outboxRepository.countByStatus("FAILED")).thenReturn(1L);

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    }

    @Test
    void health_down_whenBacklogExceedsThreshold() {
        when(outboxRepository.countByStatus("PENDING")).thenReturn(501L);
        when(outboxRepository.countStaleEntries(5)).thenReturn(0L);
        when(outboxRepository.countByStatus("FAILED")).thenReturn(0L);

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("pendingCount", 501L);
        assertThat(health.getDetails()).containsKey("reason");
    }

    @Test
    void health_up_whenBacklogExactlyAtThreshold() {
        when(outboxRepository.countByStatus("PENDING")).thenReturn(500L);
        when(outboxRepository.countStaleEntries(5)).thenReturn(0L);
        when(outboxRepository.countByStatus("FAILED")).thenReturn(0L);

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
    }
}
