package com.gubee.stockreconciliation.infrastructure.logging;

import com.gubee.stockreconciliation.domain.model.StockEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;

import java.util.UUID;

/**
 * Centraliza o enriquecimento do MDC para o processamento de eventos
 * em todos os canais de entrada da aplicação.
 *
 * Chame enrich() no ponto de entrada de qualquer fluxo que processe
 * um evento e clear() em um bloco finally após a conclusão do processamento.
 */
public final class EventProcessingContext {

    private EventProcessingContext() {}

    /**
     * Adiciona ao MDC os campos presentes em todas as linhas de log
     * relacionadas ao processamento de eventos.
     *
     * Gera um correlationId caso ele ainda não tenha sido definido
     * por um filtro de nível de requisição (por exemplo,
     * CorrelationIdFilter para requisições REST).
     */
    public static void enrich(StockEvent event) {
        if (MDC.get("correlationId") == null) {
            MDC.put("correlationId", UUID.randomUUID().toString());
        }
        MDC.put("eventId",   event.eventId().value());
        MDC.put("accountId", event.accountId().value());
        MDC.put("sku",       event.sku().value());
        MDC.put("eventType", event.type().name());
    }

    /**
     * Adiciona ao MDC metadados de transporte do Kafka.
     *
     * Deve ser chamado em conjunto com enrich() no fluxo
     * de consumo de mensagens Kafka.
     */
    public static void enrichKafka(ConsumerRecord<?, ?> record) {
        MDC.put("kafkaPartition", String.valueOf(record.partition()));
        MDC.put("kafkaOffset",    String.valueOf(record.offset()));
    }

    /**
     * Remove todas as chaves do MDC definidas por esta classe.
     *
     * Deve ser chamado em um bloco finally para evitar vazamento
     * de contexto entre requisições ou mensagens processadas pela
     * mesma thread.
     */    public static void clear() {
        MDC.remove("correlationId");
        MDC.remove("eventId");
        MDC.remove("accountId");
        MDC.remove("sku");
        MDC.remove("eventType");
        MDC.remove("kafkaPartition");
        MDC.remove("kafkaOffset");
    }
}
