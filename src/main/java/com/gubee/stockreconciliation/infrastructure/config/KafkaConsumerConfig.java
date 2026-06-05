package com.gubee.stockreconciliation.infrastructure.config;

import org.apache.kafka.common.TopicPartition;
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

    @Value("${spring.kafka.topics.stock-events-dlq}")
    private String dlqTopic;

    /**
     * Error handler com backoff exponencial e roteamento para DLQ.
     * Após 3 tentativas falhas (500ms → 1000ms → 2000ms), a mensagem é
     * enviada para stock-events-dlq para inspeção manual.
     * Alerte para qualquer mensagem na DLQ — elas representam eventos que não puderam ser
     * processados após todas as tentativas (incompatibilidade de schema, erro persistente no banco, etc.).
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (r, e) -> new TopicPartition(dlqTopic, 0));
        ExponentialBackOffWithMaxRetries backoff = new ExponentialBackOffWithMaxRetries(3);
        backoff.setInitialInterval(500);
        backoff.setMultiplier(2.0);
        return new DefaultErrorHandler(recoverer, backoff);
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
        return factory;
    }
}
