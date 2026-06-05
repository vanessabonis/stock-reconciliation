package com.gubee.stockreconciliation.infrastructure.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gubee.stockreconciliation.adapter.out.postgres.entity.ProcessedEventJpaEntity;
import com.gubee.stockreconciliation.adapter.out.postgres.repository.ProcessedEventJpaRepository;
import com.gubee.stockreconciliation.infrastructure.metrics.StockMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

/**
 * Identifica eventos ORDER_CANCELLED em estado PENDING que ultrapassaram o threshold de idade
 * e aplica a estratégia de ação correspondente ao tempo de espera.
 *
 * ESTRATÉGIA DE ESCALAÇÃO (dois níveis):
 *
 *   Nível 1 — warn threshold ({pendingThresholdMinutes}, padrão 10 min):
 *     Evento aguarda ORDER_CREATED além do esperado.
 *     Ação: WARN log + incremento em gubee.reconciliation.stale_pending.
 *     Alerta no Prometheus: qualquer valor > 0 por mais de uma janela.
 *
 *   Nível 2 — DLQ threshold ({dlqThresholdMinutes}, padrão 30 min):
 *     Evento provavelmente órfão — ORDER_CREATED nunca chegará.
 *     Ação: publica no tópico DLQ + ERROR log + métrica de DLQ.
 *     O time de plantão recebe alerta via gubee.dlq.events e investiga manualmente.
 *
 * IDEMPOTÊNCIA: este job é estritamente read-only no banco — não altera nenhuma linha.
 * A publicação no DLQ é a única side-effect externa. Execuções duplicadas (overlap de
 * instâncias, restart) produzem no máximo mensagens DLQ duplicadas para o mesmo eventId,
 * o que é aceitável para inspeção manual (o operador filtra por eventId).
 */
@Component
public class PendingReconciliationJob {

    private static final Logger log = LoggerFactory.getLogger(PendingReconciliationJob.class);

    private final ProcessedEventJpaRepository processedEventRepository;
    private final StockMetrics stockMetrics;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final long pendingThresholdMinutes;
    private final long dlqThresholdMinutes;
    private final String dlqTopic;

    public PendingReconciliationJob(ProcessedEventJpaRepository processedEventRepository,
                                    StockMetrics stockMetrics,
                                    KafkaTemplate<String, String> kafkaTemplate,
                                    ObjectMapper objectMapper,
                                    @Value("${reconciliation.pending-threshold-minutes:10}") long pendingThresholdMinutes,
                                    @Value("${reconciliation.dlq-threshold-minutes:30}") long dlqThresholdMinutes,
                                    @Value("${spring.kafka.topics.stock-events-dlq}") String dlqTopic) {
        this.processedEventRepository = processedEventRepository;
        this.stockMetrics = stockMetrics;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.pendingThresholdMinutes = pendingThresholdMinutes;
        this.dlqThresholdMinutes = dlqThresholdMinutes;
        this.dlqTopic = dlqTopic;
    }

    @Scheduled(fixedDelayString = "${reconciliation.check-interval-ms:300000}")
    public void checkStalePending() {
        Instant now = Instant.now();
        Instant warnThreshold = now.minus(pendingThresholdMinutes, ChronoUnit.MINUTES);
        Instant dlqThreshold = now.minus(dlqThresholdMinutes, ChronoUnit.MINUTES);

        List<ProcessedEventJpaEntity> stale = processedEventRepository
                .findByStatusAndProcessedAtBefore("PENDING", warnThreshold);

        // Gauge reflete o backlog real no momento desta execução, incluindo zero.
        // Crítico para detectar quando o backlog drena: gauge volta a 0 sem intervenção manual.
        stockMetrics.updateReconciliationBacklog(stale.size());

        if (stale.isEmpty()) {
            return;
        }

        int warnCount = 0;
        int dlqCount = 0;

        for (ProcessedEventJpaEntity event : stale) {
            // Histograma: idade do evento no momento da detecção.
            // Permite distinguir "backlog novo acumulando" de "evento preso há horas".
            Duration age = Duration.between(event.getProcessedAt(), now);
            stockMetrics.recordPendingAge(age);

            if (event.getProcessedAt().isBefore(dlqThreshold)) {
                publishToDlq(event);
                stockMetrics.recordReconciliationAction(event.getEventType(), "dlq");
                log.error("reconciliation.escalated_to_dlq eventId={} accountId={} sku={} pendingSince={} ageMinutes={}",
                        event.getEventId(), event.getAccountId(), event.getSku(),
                        event.getProcessedAt(), age.toMinutes());
                dlqCount++;
            } else {
                stockMetrics.recordReconciliationAction(event.getEventType(), "warn");
                log.warn("reconciliation.stale_pending eventId={} accountId={} sku={} eventType={} pendingSince={} ageMinutes={}",
                        event.getEventId(), event.getAccountId(), event.getSku(),
                        event.getEventType(), event.getProcessedAt(), age.toMinutes());
                warnCount++;
            }
        }

        log.warn("reconciliation.check.completed warnCount={} dlqCount={}", warnCount, dlqCount);
    }

    private void publishToDlq(ProcessedEventJpaEntity event) {
        try {
            String payload = objectMapper.writeValueAsString(Map.of(
                    "eventId", event.getEventId(),
                    "eventType", event.getEventType(),
                    "accountId", event.getAccountId(),
                    "sku", event.getSku(),
                    "status", event.getStatus(),
                    "pendingSince", event.getProcessedAt().toString(),
                    "reason", "PENDING event stale beyond dlq-threshold-minutes"
            ));
            kafkaTemplate.send(dlqTopic, event.getAccountId() + ":" + event.getSku(), payload);
            stockMetrics.recordDlqEvent(event.getEventType(), "stale_pending_escalation");
        } catch (JsonProcessingException e) {
            log.error("reconciliation.dlq_publish_failed eventId={} error={}", event.getEventId(), e.getMessage());
        }
    }
}