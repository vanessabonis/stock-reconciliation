package com.gubee.stockreconciliation.adapter.out.postgres;

import com.gubee.stockreconciliation.adapter.out.postgres.repository.OutboxPurgeRepository;
import com.gubee.stockreconciliation.infrastructure.metrics.StockMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxPurgeServiceTest {

    @Mock
    OutboxPurgeRepository repository;

    @Mock
    StockMetrics metrics;

    @InjectMocks
    OutboxPurgeService service;

    @Test
    void purgePublishedEntries_deletesOldRowsAndRecordsMetrics() {
        when(repository.deletePublishedBefore(any(Instant.class))).thenReturn(42);

        service.purgePublishedEntries();

        verify(repository).deletePublishedBefore(any(Instant.class));
        verify(metrics).recordPurge(42);
    }

    @Test
    void purgePublishedEntries_whenNothingDeleted_recordsZero() {
        when(repository.deletePublishedBefore(any(Instant.class))).thenReturn(0);

        service.purgePublishedEntries();

        verify(metrics).recordPurge(0);
    }
}
