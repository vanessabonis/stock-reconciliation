package com.gubee.stockreconciliation.application.usecase;

import com.gubee.stockreconciliation.domain.model.StockEvent;
import com.gubee.stockreconciliation.domain.model.enums.EventStatus;
import com.gubee.stockreconciliation.domain.model.exception.StockConcurrencyException;
import com.gubee.stockreconciliation.domain.port.in.ProcessStockEventUseCase;
import com.gubee.stockreconciliation.infrastructure.metrics.StockMetrics;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Service;

/**
 * Ponto de entrada do processamento de eventos. Gerencia a política de retry para conflitos
 * de lock otimista e o curto-circuito de idempotência para eventIds duplicados.
 *
 * Delega o trabalho transacional ao StockEventTransactionalProcessor (bean separado do Spring
 * para que o AOP aplique corretamente o limite @Transactional a cada tentativa de retry).
 *
 * DataIntegrityViolationException é capturada FORA da transação (após rollback) para evitar
 * consultar uma transação PostgreSQL já abortada. Retorna IGNORED — nenhuma lógica de negócio
 * é executada duas vezes.
 */
@Service
public class ProcessStockEventUseCaseImpl implements ProcessStockEventUseCase {

    private static final Logger log = LoggerFactory.getLogger(ProcessStockEventUseCaseImpl.class);
    private static final int MAX_RETRIES = 3;

    private final StockEventTransactionalProcessor processor;
    private final StockMetrics stockMetrics;

    public ProcessStockEventUseCaseImpl(StockEventTransactionalProcessor processor,
                                         StockMetrics stockMetrics) {
        this.processor = processor;
        this.stockMetrics = stockMetrics;
    }

    @Override
    public EventStatus process(StockEvent event) {
        log.info("event.received");
        Timer.Sample timer = stockMetrics.startProcessingTimer();
        long delayMs = 50;

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                EventStatus status = processor.process(event);
                if (status == EventStatus.IGNORED) {
                    log.info("event.ignored.duplicate");
                }
                stockMetrics.recordEventProcessed(event.type().name(), status.name());
                stockMetrics.recordProcessingTime(timer, event.type().name());
                return status;
            } catch (DataIntegrityViolationException e) {
                log.info("event.ignored.duplicate");
                stockMetrics.recordEventProcessed(event.type().name(), EventStatus.IGNORED.name());
                stockMetrics.recordProcessingTime(timer, event.type().name());
                return EventStatus.IGNORED;
            } catch (ObjectOptimisticLockingFailureException e) {
                log.warn("event.optimistic_lock_retry attempt={} maxAttempts={}", attempt, MAX_RETRIES);
                stockMetrics.recordOptimisticLockRetry(event.sku().value());
                if (attempt == MAX_RETRIES) {
                    log.error("event.processing_failed_after_retries maxAttempts={}", MAX_RETRIES);
                    stockMetrics.recordProcessingTime(timer, event.type().name());
                    throw new StockConcurrencyException(
                            "Failed after " + MAX_RETRIES + " attempts for event " + event.eventId().value());
                }
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during retry", ie);
                }
                delayMs *= 2;
            }
        }
        throw new IllegalStateException("Unreachable");
    }
}
