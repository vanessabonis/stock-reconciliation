package com.gubee.stockreconciliation.adapter.out.postgres;

import com.gubee.stockreconciliation.adapter.out.postgres.repository.OutboxPurgeRepository;
import com.gubee.stockreconciliation.infrastructure.metrics.StockMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Exclui entradas PUBLISHED do outbox com mais de 30 dias.
 * Executa diariamente às 02:00 para evitar janelas de tráfego intenso.
 *
 * Entradas FAILED nunca são excluídas — representam falhas operacionais
 * que requerem investigação manual antes do arquivamento.
 *
 * Desabilitado em testes via spring.task.scheduling.enabled=false (application-test.yml).
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxPurgeService {

    private static final int RETENTION_DAYS = 30;

    private final OutboxPurgeRepository repository;
    private final StockMetrics metrics;

    @Scheduled(cron = "0 0 2 * * *")
    @Transactional
    public void purgePublishedEntries() {
        Instant cutoff = Instant.now().minus(RETENTION_DAYS, ChronoUnit.DAYS);
        int deleted = repository.deletePublishedBefore(cutoff);
        log.info("outbox.purge.completed deletedRows={} cutoffDays={}", deleted, RETENTION_DAYS);
        metrics.recordPurge(deleted);
    }
}
