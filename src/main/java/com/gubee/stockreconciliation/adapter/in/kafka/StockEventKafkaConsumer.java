package com.gubee.stockreconciliation.adapter.in.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gubee.stockreconciliation.domain.model.StockEvent;
import com.gubee.stockreconciliation.domain.model.enums.EventType;
import com.gubee.stockreconciliation.domain.model.vo.AccountId;
import com.gubee.stockreconciliation.domain.model.vo.EventId;
import com.gubee.stockreconciliation.domain.model.vo.Quantity;
import com.gubee.stockreconciliation.domain.model.vo.Sku;
import com.gubee.stockreconciliation.domain.port.in.ProcessStockEventUseCase;
import com.gubee.stockreconciliation.infrastructure.logging.EventProcessingContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;

/**
 * NOTA DE CONFIABILIDADE — segurança no commit de offset
 *
 * Este consumer intencionalmente NÃO publica no Kafka diretamente.
 * Ele apenas grava no banco de dados dentro de uma única transação ACID:
 *   - stocks (atualização de saldo)
 *   - outbox_events (entrada PENDING para o relay)
 *
 * O offset só é confirmado após o commit da transação no banco.
 * O OutboxRelay cuida da publicação no Kafka de forma independente, usando
 * envio síncrono (kafkaTemplate.send().get()) que aguarda o ACK do broker.
 *
 * Isso elimina a clássica race condition entre async-send e offset-commit,
 * onde uma queda do serviço entre o commit do offset e a conclusão do envio
 * causa perda silenciosa de mensagens.
 *
 * Trade-off: a latência de publicação aumenta pelo intervalo de polling do relay (500ms).
 * Em produção, o Debezium CDC eliminaria essa latência completamente ao
 * monitorar o WAL do PostgreSQL em vez de fazer polling.
 *
 * A idempotência é garantida pelo INSERT ON CONFLICT DO NOTHING em processed_events.event_id
 * (guardInsert) executado no início de cada transação de processamento. Se o eventId já existir,
 * guardInsert retorna 0 e process() retorna IGNORED sem executar lógica de negócio.
 * entrega at-least-once (Kafka) + ON CONFLICT DO NOTHING (banco) = efeito de negócio exactly-once.
 *
 * Tanto este consumer quanto o endpoint REST (POST /events) chamam o mesmo
 * ProcessStockEventUseCase. O use case não tem conhecimento de como o evento chegou —
 * aqui a arquitetura hexagonal se paga.
 */
@Component
public class StockEventKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(StockEventKafkaConsumer.class);

    private final ProcessStockEventUseCase processStockEventUseCase;
    private final ObjectMapper objectMapper;

    public StockEventKafkaConsumer(ProcessStockEventUseCase processStockEventUseCase,
                                    ObjectMapper objectMapper) {
        this.processStockEventUseCase = processStockEventUseCase;
        this.objectMapper = objectMapper;
    }

    // CONTRATO DE PROPAGAÇÃO DE TRACE:
    // Producer (OutboxRelay) → injeta o header W3C traceparent na mensagem Kafka
    // Consumer → extrai o header traceparent → cria span filho
    // Resultado: um trace único cobre gravação no banco + publicação Kafka + processamento do consumer
    // No Zipkin/Jaeger: visibilidade ponta a ponta através da fronteira assíncrona
    //
    // O Spring Kafka + Micrometer Tracing faz isso automaticamente quando
    // o ObservationRegistry está configurado. O traceId no header Kafka
    // torna-se o span pai para todo o processamento dentro de consume().
    @KafkaListener(
            topics = "${spring.kafka.topics.stock-events}",
            groupId = "gubee-stock-consumer",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, String> record) {
        try {
            // traceId e spanId já estão no MDC pela bridge do OTel tracing.
            // enrichKafka() adiciona campos específicos de transporte que não estão no contexto de trace.
            log.debug("payload={}", record.value());
            EventProcessingContext.enrichKafka(record);
            log.info("kafka.message.received partition={} offset={}", record.partition(), record.offset());

            StockEvent event = deserialize(record.value());
            EventProcessingContext.enrich(event);

            var status = processStockEventUseCase.process(event);
            log.info("kafka.event.processed status={}", status);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            log.error("kafka.message.parse_error partition={} offset={} error={}",
                    record.partition(), record.offset(), e.getMessage());
            throw new NonRetryableMessageException(
                    "Non-retryable parse failure at partition=" + record.partition() + " offset=" + record.offset(), e);
        } catch (Exception e) {
            // Retryable: transient failure (DB unavailable, optimistic lock, network timeout).
            // Spring Kafka will apply exponential backoff before routing to DLQ.
            // "routed_to_dlq" is logged by KafkaConsumerConfig recoverer only on final exhaustion.
            log.error("kafka.message.processing_error partition={} offset={} error={}",
                    record.partition(), record.offset(), e.getMessage());
            throw new RuntimeException(
                    "Retryable processing failure at partition=" + record.partition() + " offset=" + record.offset(), e);
        } finally {
            EventProcessingContext.clear();
        }
    }

    @SuppressWarnings("unchecked")
    private StockEvent deserialize(String json) throws Exception {
        Map<String, Object> m = objectMapper.readValue(json, Map.class);

        EventType type = EventType.valueOf((String) m.get("type"));
        Quantity qty = type.resolveQuantity(
                intOrNull(m, "quantity"),
                intOrNull(m, "available"),
                intOrNull(m, "quantitySent")
        );

        return new StockEvent(
                EventId.of((String) m.get("eventId")),
                type,
                Instant.parse((String) m.get("occurredAt")),
                AccountId.of((String) m.get("accountId")),
                Sku.of((String) m.get("sku")),
                (String) m.get("marketplace"),
                (String) m.get("externalOrderId"),
                qty,
                (String) m.get("reason")
        );
    }

    private Integer intOrNull(Map<String, Object> m, String key) {
        Object v = m.get(key);
        return v instanceof Number n ? n.intValue() : null;
    }
}
