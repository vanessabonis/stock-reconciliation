package com.gubee.stockreconciliation.infrastructure.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
public class KafkaTopicConfig {

    @Value("${spring.kafka.topics.stock-events}")
    private String stockEventsTopic;

    @Value("${spring.kafka.topics.stock-events-processed}")
    private String stockEventsProcessedTopic;

    @Value("${spring.kafka.topics.stock-events-dlq}")
    private String stockEventsDlqTopic;

    // 3 partições permitem até 3 instâncias de consumer em paralelo.
    // Chave de partição = accountId:sku garante ordenação por estoque.
    @Bean
    public NewTopic stockEventsTopicBean() {
        return TopicBuilder.name(stockEventsTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    // O outbox relay publica aqui após o commit bem-sucedido no banco.
    // Sistemas downstream (analytics, notificações) consomem deste tópico.
    @Bean
    public NewTopic stockEventsProcessedTopicBean() {
        return TopicBuilder.name(stockEventsProcessedTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    // Partição única — eventos de DLQ são raros e a ordenação é menos crítica.
    // Alerte para qualquer mensagem aqui (acionar pager imediatamente).
    @Bean
    public NewTopic stockEventsDlqTopicBean() {
        return TopicBuilder.name(stockEventsDlqTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
