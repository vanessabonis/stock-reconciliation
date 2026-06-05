package com.gubee.stockreconciliation.adapter.out.kafka;

import com.gubee.stockreconciliation.adapter.out.postgres.entity.OutboxEventJpaEntity;
import com.gubee.stockreconciliation.adapter.out.postgres.repository.OutboxEventJpaRepository;
import com.gubee.stockreconciliation.infrastructure.metrics.StockMetrics;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import org.slf4j.MDC;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Faz polling na tabela outbox_events a cada 500ms e publica entradas PENDING no Kafka.
 *
 * SELECT FOR UPDATE SKIP LOCKED é essencial para correção em múltiplas instâncias:
 * cada instância do relay adquire um lock de linha antes de processar. SKIP LOCKED significa
 * "se a linha já está bloqueada por outra instância, pule-a completamente" — não "aguarde".
 * Resultado: cada entrada do outbox é processada por exatamente uma instância do relay,
 * evitando publicações duplicadas no Kafka mesmo com deployments simultâneos.
 *
 * Chave da mensagem = aggregateId (accountId:sku) → todos os eventos do mesmo estoque vão para
 * a mesma partição Kafka → a ordenação por estoque é preservada dentro do consumer group.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int MAX_RETRIES = 3;

    private final String topic;
    private final OutboxEventJpaRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final StockMetrics stockMetrics;

    public OutboxRelay(OutboxEventJpaRepository outboxRepository,
                       KafkaTemplate<String, String> kafkaTemplate,
                       StockMetrics stockMetrics,
                       @Value("${spring.kafka.topics.stock-events-processed}") String topic) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.stockMetrics = stockMetrics;
        this.topic = topic;
    }

    @PostConstruct
    public void registerMetrics() {
        stockMetrics.registerOutboxPendingGauge(
                () -> outboxRepository.countByStatus("PENDING"));
    }

    @Scheduled(fixedDelay = 500)
    @Transactional
    public void processOutbox() {
        List<OutboxEventJpaEntity> pending = outboxRepository.findPendingWithLock();
        if (pending.isEmpty()) return;

        log.debug("OutboxRelay: processing {} pending entries", pending.size());

        for (OutboxEventJpaEntity entry : pending) {
            MDC.put("outboxEntryId", entry.getId().toString());
            try {
                // O Spring Kafka com ObservationRegistry injeta automaticamente o header W3C traceparent
                // em cada mensagem enviada — sem necessidade de injeção manual.
                // O consumer extrai esse header e continua o mesmo trace,
                // oferecendo visibilidade ponta a ponta no Zipkin através da fronteira assíncrona.
                //
                // CRÍTICO: use .get() para bloquear até o broker confirmar o recebimento.
                // Fire-and-forget (sem .get()) arrisca perder mensagens se o relay
                // cair após o retorno de send() mas antes de o broker persistir a mensagem.
                // É exatamente o padrão de race condition de offset-commit que o Outbox Pattern
                // foi criado para prevenir.
                kafkaTemplate.send(topic, entry.getAggregateId(), entry.getPayload())
                        .get(5, TimeUnit.SECONDS);

                entry.setStatus("PUBLISHED");
                entry.setPublishedAt(Instant.now());
                stockMetrics.recordOutboxPublishLatency(entry.getCreatedAt());
                log.info("outbox.relay.published outboxEntryId={} topic={}", entry.getId(), topic);
            } catch (Exception e) {
                int newCount = entry.getRetryCount() + 1;
                entry.setRetryCount(newCount);
                entry.setLastError(e.getMessage() != null ? e.getMessage().substring(0, Math.min(e.getMessage().length(), 500)) : "unknown");

                if (newCount >= MAX_RETRIES) {
                    entry.setStatus("FAILED");
                    stockMetrics.recordDlqEvent(entry.getEventType(), "max_retries_exceeded");
                    log.error("outbox.relay.entry_dead outboxEntryId={} reason=max retries exceeded", entry.getId());
                } else {
                    log.warn("outbox.relay.publish_failed outboxEntryId={} retryCount={} error={}",
                            entry.getId(), newCount, e.getMessage());
                }
            } finally {
                MDC.remove("outboxEntryId");
            }
            outboxRepository.save(entry);
        }
    }
}
