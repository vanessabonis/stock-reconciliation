package com.gubee.stockreconciliation.integration;

import com.gubee.stockreconciliation.adapter.out.kafka.OutboxRelay;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Testes de integração Kafka. Usa embedded broker (spring-kafka-test) com PostgreSQL real.
 *
 * Ambos os caminhos — consumer Kafka e POST /events — chamam o mesmo ProcessStockEventUseCase.
 * O use case não sabe como o evento chegou: hexagonal architecture se paga aqui.
 *
 * at-least-once (Kafka) + unique constraint em event_id = efeito exactly-once de negócio.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@EmbeddedKafka(
        partitions = 3,
        topics = {"stock-events", "stock-events-processed", "stock-events-dlq"},
        brokerProperties = {"auto.create.topics.enable=true"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:postgresql://localhost:5432/stockdb",
        "spring.datasource.username=stockuser",
        "spring.datasource.password=stockpass",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.admin.fail-fast=false"
})
class StockEventKafkaConsumerIntegrationTest {

    @Autowired EmbeddedKafkaBroker embeddedKafka;
    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired JdbcTemplate jdbcTemplate;
    @Autowired TestRestTemplate restTemplate;
    @Autowired OutboxRelay outboxRelay;

    @LocalServerPort int port;
    private String baseUrl;

    private static final String VENDEDOR_NIKE  = "vendedor-nike";
    private static final String VENDEDOR_APPLE = "vendedor-apple";
    private static final String SKU            = "TENIS-NIKE-AIRMAX-41";

    @BeforeEach
    void setUp() {
        baseUrl = "http://localhost:" + port;
        jdbcTemplate.execute(
                "TRUNCATE TABLE outbox_events, stock_history, order_states, processed_events, stocks RESTART IDENTITY CASCADE");
    }

    // ─── Teste 1: Consumer processa STOCK_ADJUSTED ───────────────────────────

    @Test
    @DisplayName("Teste 1: consumer Kafka processa STOCK_ADJUSTED e atualiza saldo para 10")
    void kafkaConsumer_processaStockAdjusted() {
        String event = """
                {"eventId":"evt-001","type":"STOCK_ADJUSTED","occurredAt":"2026-05-28T10:00:00Z",
                 "accountId":"%s","sku":"%s","available":10,"reason":"inventario_inicial"}
                """.formatted(VENDEDOR_NIKE, SKU).strip();

        kafkaTemplate.send("stock-events", VENDEDOR_NIKE + ":" + SKU, event);

        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> {
                    Integer qty = jdbcTemplate.queryForObject(
                            "SELECT available_quantity FROM stocks WHERE account_id=? AND sku=?",
                            Integer.class, VENDEDOR_NIKE, SKU);
                    assertThat(qty).isEqualTo(10);
                });

        String status = jdbcTemplate.queryForObject(
                "SELECT status FROM processed_events WHERE event_id='evt-001'", String.class);
        assertThat(status).isEqualTo("PROCESSED");
    }

    // ─── Teste 2: Consumer é idempotente ─────────────────────────────────────

    @Test
    @DisplayName("Teste 2: mesmo eventId publicado duas vezes — saldo atualizado exatamente uma vez")
    void kafkaConsumer_idempotente() {
        String event = """
                {"eventId":"evt-001","type":"STOCK_ADJUSTED","occurredAt":"2026-05-28T10:00:00Z",
                 "accountId":"%s","sku":"%s","available":10,"reason":"inventario_inicial"}
                """.formatted(VENDEDOR_APPLE, SKU).strip();

        kafkaTemplate.send("stock-events", VENDEDOR_APPLE + ":" + SKU, event);
        kafkaTemplate.send("stock-events", VENDEDOR_APPLE + ":" + SKU, event);

        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> {
                    Integer qty = jdbcTemplate.queryForObject(
                            "SELECT available_quantity FROM stocks WHERE account_id=? AND sku=?",
                            Integer.class, VENDEDOR_APPLE, SKU);
                    assertThat(qty).isEqualTo(10);
                });

        // Unique constraint em event_id: exatamente uma linha em processed_events
        int count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_events WHERE event_id='evt-001'", Integer.class);
        assertThat(count).isEqualTo(1);
    }

    // ─── Teste 3: Roteamento para DLQ em evento malformado ───────────────────

    @Test
    @DisplayName("Teste 3: evento malformado é roteado para DLQ após retries esgotados")
    void kafkaConsumer_dlqRouting_eventoMalformado() {
        kafkaTemplate.send("stock-events", VENDEDOR_NIKE + ":" + SKU, "{ JSON INVALIDO }");

        KafkaConsumer<String, String> dlqConsumer = buildTestConsumer("stock-events-dlq");
        List<String> received = new ArrayList<>();

        try {
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        ConsumerRecords<String, String> records = dlqConsumer.poll(Duration.ofMillis(500));
                        records.forEach(r -> received.add(r.value()));
                        assertThat(received).isNotEmpty();
                    });
        } finally {
            dlqConsumer.close();
        }
    }

    // ─── Teste 4: Outbox relay publica eventos processados ───────────────────

    @Test
    @DisplayName("Teste 4: POST /events gera entrada no outbox; relay publica em stock-events-processed")
    void outboxRelay_publicaEventoProcessado() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        String body = """
                {"eventId":"evt-001","type":"STOCK_ADJUSTED","occurredAt":"2026-05-28T10:00:00Z",
                 "accountId":"%s","sku":"%s","available":10,"reason":"inventario_inicial"}
                """.formatted(VENDEDOR_NIKE, SKU).strip();

        restTemplate.postForEntity(baseUrl + "/events", new HttpEntity<>(body, headers), Map.class);

        // Entrada no outbox deve ser escrita atomicamente com a atualização do estoque
        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    int count = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id=?",
                            Integer.class, VENDEDOR_NIKE + ":" + SKU);
                    assertThat(count).isEqualTo(1);
                });

        // Força o relay a publicar (scheduling desabilitado em testes)
        outboxRelay.processOutbox();

        String outboxStatus = jdbcTemplate.queryForObject(
                "SELECT status FROM outbox_events WHERE aggregate_id=?",
                String.class, VENDEDOR_NIKE + ":" + SKU);
        assertThat(outboxStatus).isEqualTo("PUBLISHED");

        // Mensagem deve aparecer em stock-events-processed
        KafkaConsumer<String, String> consumer = buildTestConsumer("stock-events-processed");
        List<String> received = new ArrayList<>();
        try {
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .pollInterval(Duration.ofMillis(500))
                    .untilAsserted(() -> {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                        records.forEach(r -> received.add(r.value()));
                        assertThat(received).anyMatch(v -> v.contains("evt-001"));
                    });
        } finally {
            consumer.close();
        }
    }

    // ─── Teste 5: SKIP LOCKED evita publicação dupla ─────────────────────────

    @Test
    @DisplayName("Teste 5: duas instâncias do relay concorrentes — SKIP LOCKED garante exatamente uma publicação")
    void outboxRelay_skipLocked_evitaPublicacaoDupla() throws Exception {
        // Usa vendedor-apple para isolar a chave Kafka do teste 4 (vendedor-nike)
        // — o tópico embedded não é resetado entre testes, então chaves distintas evitam contagem errada
        jdbcTemplate.update(
                "INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) " +
                "VALUES ('STOCK', 'vendedor-apple:TENIS-NIKE-AIRMAX-41', 'STOCK_ADJUSTED', " +
                "'{\"eventId\":\"evt-011\",\"accountId\":\"vendedor-apple\",\"sku\":\"TENIS-NIKE-AIRMAX-41\"}', " +
                "'PENDING', NOW(), 0)");

        // Duas instâncias do relay concorrentes — SKIP LOCKED garante que apenas uma processa a linha
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);
        ExecutorService pool = Executors.newFixedThreadPool(2);

        for (int i = 0; i < 2; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    outboxRelay.processOutbox();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        done.await(15, TimeUnit.SECONDS);
        pool.shutdown();

        // A linha deve estar PUBLISHED exatamente uma vez
        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    String status = jdbcTemplate.queryForObject(
                            "SELECT status FROM outbox_events WHERE aggregate_id='vendedor-apple:TENIS-NIKE-AIRMAX-41'",
                            String.class);
                    assertThat(status).isEqualTo("PUBLISHED");
                });

        // Exatamente uma mensagem em stock-events-processed para este aggregate
        KafkaConsumer<String, String> consumer = buildTestConsumer("stock-events-processed");
        AtomicInteger msgCount = new AtomicInteger(0);
        try {
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .pollInterval(Duration.ofMillis(500))
                    .untilAsserted(() -> {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                        records.forEach(r -> {
                            if ((VENDEDOR_APPLE + ":" + SKU).equals(r.key())) msgCount.incrementAndGet();
                        });
                        assertThat(msgCount.get()).isEqualTo(1);
                    });
        } finally {
            consumer.close();
        }
    }

    // ─── Helper ──────────────────────────────────────────────────────────────

    private KafkaConsumer<String, String> buildTestConsumer(String topic) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + topic + "-" + System.nanoTime(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        ));
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }
}
