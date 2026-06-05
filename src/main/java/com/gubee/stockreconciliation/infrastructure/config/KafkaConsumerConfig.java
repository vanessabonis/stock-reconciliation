package com.gubee.stockreconciliation.infrastructure.config;

import com.gubee.stockreconciliation.adapter.in.kafka.NonRetryableMessageException;
import com.gubee.stockreconciliation.infrastructure.metrics.StockMetrics;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Value("${spring.kafka.topics.stock-events-dlq}")
    private String dlqTopic;

    /**
     * Error handler com backoff exponencial, roteamento para DLQ, e métricas.
     *
     * Retryable (RuntimeException): backoff 500ms → 1000ms → 2000ms, depois DLQ.
     * Non-retryable (NonRetryableMessageException): sem backoff — vai direto ao DLQ.
     * Payload malformado nunca vai melhorar com retry; o backoff só atrasa a partição.
     *
     * O recoverer é o único ponto onde kafka.message.routed_to_dlq é logado e
     * a métrica gubee.dlq.events é incrementada — garantindo exatamente uma ocorrência
     * por mensagem, independente de quantas tentativas foram feitas antes.
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template,
                                             StockMetrics stockMetrics) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, exception) -> {
                    String reason = exception instanceof NonRetryableMessageException
                            ? "parse_error" : "processing_error";
                    stockMetrics.recordDlqEvent("UNKNOWN", reason);
                    log.error("kafka.message.routed_to_dlq topic={} partition={} offset={} reason={}",
                            record.topic(), record.partition(), record.offset(), reason);
                    return new TopicPartition(dlqTopic, 0);
                });

        ExponentialBackOffWithMaxRetries backoff = new ExponentialBackOffWithMaxRetries(3);
        backoff.setInitialInterval(500);
        backoff.setMultiplier(2.0);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backoff);
        // NonRetryableMessageException bypasses the retry backoff entirely — goes straight to DLQ.
        // This covers malformed JSON, unknown EventType enum values, and invalid field values.
        errorHandler.addNotRetryableExceptions(NonRetryableMessageException.class);
        return errorHandler;
    }

    /**
     * Container factory com modo de ack BATCH e o error handler customizado.
     *
     * Semântica do consumer group: groupId = "gubee-stock-consumer" significa que múltiplas
     * instâncias do serviço compartilham automaticamente as 3 partições de stock-events.
     * Com 3 partições e 3 instâncias, cada instância possui uma partição — processamento
     * paralelo completo com garantias de ordenação por partição. O Kafka faz rebalanceamento
     * automaticamente quando instâncias sobem ou descem.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        // Modo de ack BATCH: offset confirmado após todos os registros do lote serem processados.
        // Se o processamento falhar no meio do lote, os offsets não confirmados são reenviados —
        // combinado com idempotência em event_id, isso é seguro.
        //
        // Por que não MANUAL_IMMEDIATE: MANUAL_IMMEDIATE é necessário quando o consumer
        // publica no Kafka diretamente — o ack só ocorreria após a conclusão do envio.
        // Como este consumer apenas grava no banco (sem publicação downstream), BATCH é
        // suficiente e mais simples. O OutboxRelay cuida da publicação separadamente.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        // Habilita propagação automática do header W3C traceparent nos consumers Kafka.
        // O Spring Kafka extrai o header de cada mensagem e cria um span filho no trace OTel,
        // conectando visualmente o processamento do consumer ao producer que publicou o evento.
        factory.getContainerProperties().setObservationEnabled(true);
        return factory;
    }
}
