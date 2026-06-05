package com.gubee.stockreconciliation.infrastructure.metrics;

import com.gubee.stockreconciliation.domain.model.enums.EventStatus;
import com.gubee.stockreconciliation.domain.port.out.ProcessedEventRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

@Configuration
public class StockMetrics {

    private final MeterRegistry registry;
    private final ProcessedEventRepository processedEventRepository;

    public StockMetrics(MeterRegistry registry, ProcessedEventRepository processedEventRepository) {
        this.registry = registry;
        this.processedEventRepository = processedEventRepository;
    }

    // kafka.consumer.records-lag (por partição), fetch-rate e commit-rate são registrados
    // automaticamente pelo KafkaMetricsAutoConfiguration do Spring Boot quando spring-kafka
    // e spring-boot-starter-actuator estão no classpath. Nenhum @Bean explícito necessário.
    // Consumer lag = o quanto o consumer está atrasado em relação ao último offset.
    // Limite de alerta: lag > 1000 por mais de 2 minutos = consumer pode estar travado.

    // ─── Event processing ─────────────────────────────────────────────────────

    /**
     * Total de eventos processados, segmentados por tipo e resultado.
     * Query:  rate(gubee_events_processed_total[5m]) by (eventType, status)
     * Alerta: se a taxa de INCONSISTENT ultrapassar o limite configurado
     */
    public void recordEventProcessed(String eventType, String status) {
        Counter.builder("gubee.events.processed")
                .description("Total events processed by type and outcome")
                .tag("eventType", eventType)
                .tag("status", status)
                .register(registry)
                .increment();
    }

    /**
     * Registra eventos que não puderam ser processados após todos os retries.
     * Valores de reason: "optimistic_lock_exhausted", "validation_error"
     */
    public void recordEventFailed(String eventType, String reason) {
        Counter.builder("gubee.events.failed")
                .tag("eventType", eventType)
                .tag("reason", reason)
                .register(registry)
                .increment();
    }

    /**
     * Inicia um timer de processamento. Chame no ponto de entrada do use case.
     * Query:  histogram_quantile(0.95, gubee_event_processing_duration_seconds_bucket)
     * Alerta: p95 > 500ms
     */
    public Timer.Sample startProcessingTimer() {
        return Timer.start(registry);
    }

    /**
     * Registra o tempo decorrido de processamento. Chame imediatamente antes de retornar o status.
     */
    public void recordProcessingTime(Timer.Sample sample, String eventType) {
        sample.stop(Timer.builder("gubee.event.processing.duration")
                .description("Event processing latency")
                .tag("eventType", eventType)
                .register(registry));
    }

    /**
     * Conta colisões de lock otimista que exigiram retry.
     * Query:  rate(gubee_optimistic_lock_retries_total[5m])
     * Alerta: taxa sustentada > 10/min → contenção de escrita em um SKU quente
     */
    public void recordOptimisticLockRetry(String sku) {
        Counter.builder("gubee.optimistic.lock.retries")
                .description("Optimistic lock collisions requiring retry")
                .tag("sku", sku)
                .register(registry)
                .increment();
    }

    /**
     * Conta eventos ORDER_CREATED rejeitados por saldo insuficiente.
     * Query:  increase(gubee_insufficient_stock_total[1h])
     * Insight: taxa elevada indica que vendedores podem estar vendendo além do estoque.
     */
    public void recordInsufficientStock(String accountId, String sku) {
        Counter.builder("gubee.insufficient.stock")
                .description("ORDER_CREATED rejected due to insufficient balance")
                .tag("accountId", accountId)
                .tag("sku", sku)
                .register(registry)
                .increment();
    }

    // ─── Outbox relay ─────────────────────────────────────────────────────────

    /**
     * Registra a latência de publicação desde a criação da entrada no outbox até o ACK do Kafka.
     * Limite de alerta: p95 > 5 segundos = relay está com atraso.
     */
    public void recordOutboxPublishLatency(Instant createdAt) {
        Timer.builder("gubee.outbox.publish.latency")
                .description("Time from outbox entry creation to Kafka publish")
                .register(registry)
                .record(Duration.between(createdAt, Instant.now()));
    }

    /**
     * Registra um gauge alimentado pelo supplier fornecido pelo chamador.
     * Chame no @PostConstruct do OutboxRelay com um supplier de countByStatus("PENDING").
     * Query:  gubee_outbox_pending
     * Alerta: > 1000 por mais de 5 minutos → relay pode estar travado
     */
    public void registerOutboxPendingGauge(Supplier<Number> supplier) {
        Gauge.builder("gubee.outbox.pending", supplier)
                .description("Outbox entries waiting to be published to Kafka")
                .register(registry);
    }

    /**
     * Registra quantas linhas PUBLISHED do outbox foram removidas pelo job de purge.
     * Query:  increase(gubee_outbox_purged_total[1d])
     * Normal: valor não-zero uma vez por dia por volta das 02:00; zero nos demais horários.
     */
    public void recordPurge(int deletedRows) {
        Counter.builder("gubee.outbox.purged")
                .description("Outbox PUBLISHED entries deleted by purge job")
                .register(registry)
                .increment(deletedRows);
    }

    /**
     * Conta eventos roteados para o tópico de dead-letter após esgotar os retries.
     * Query:  increase(gubee_dlq_events_total[1h])
     * Alerta: qualquer valor > 0 → acionar plantão imediatamente
     */
    public void recordDlqEvent(String eventType, String reason) {
        Counter.builder("gubee.dlq.events")
                .description("Events routed to DLQ after exhausting retries")
                .tag("eventType", eventType)
                .tag("reason", reason)
                .register(registry)
                .increment();
    }

    // ─── Gauges registered at startup ─────────────────────────────────────────

    @PostConstruct
    public void registerGauges() {
        // Rastreia eventos ORDER_CANCELLED aguardando o ORDER_CREATED correspondente.
        // Limite de alerta: > 100 de forma sustentada indica que ORDER_CREATED pode estar se perdendo ou atrasando.
        Gauge.builder("gubee.events.pending.orderstate",
                        processedEventRepository,
                        repo -> {
                            try {
                                return repo.countByStatus(EventStatus.PENDING);
                            } catch (Exception e) {
                                return Double.NaN;
                            }
                        })
                .description("ORDER_CANCELLED events awaiting ORDER_CREATED")
                .register(registry);
    }
}
