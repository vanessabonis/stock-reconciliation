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
import java.time.Instant;
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
 * Kafka integration tests. Uses an embedded Kafka broker (spring-kafka-test) paired with
 * the docker-compose PostgreSQL at localhost:5432.
 *
 * Design note: both this consumer path and POST /events call the same ProcessStockEventUseCase.
 * The use case has no knowledge of how the event arrived — this is hexagonal architecture paying off.
 *
 * Idempotency is handled by the unique constraint on processed_events.event_id.
 * at-least-once delivery (Kafka) + unique constraint = exactly-once business effect.
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

    @BeforeEach
    void setUp() {
        baseUrl = "http://localhost:" + port;
        jdbcTemplate.execute(
                "TRUNCATE TABLE outbox_events, stock_history, order_states, processed_events, stocks RESTART IDENTITY CASCADE");
    }

    // ─── Test 1: Consumer processes STOCK_ADJUSTED ───────────────────────────

    @Test
    @DisplayName("Test 1: Kafka consumer processes STOCK_ADJUSTED and updates stock balance")
    void kafkaConsumer_processesStockAdjusted() {
        String event = """
                {"eventId":"kafka-t1-001","type":"STOCK_ADJUSTED","occurredAt":"%s",
                 "accountId":"acc-k1","sku":"SKU-K1","available":15,"reason":"kafka-test"}
                """.formatted(Instant.now()).strip();

        kafkaTemplate.send("stock-events", "acc-k1:SKU-K1", event);

        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> {
                    Integer qty = jdbcTemplate.queryForObject(
                            "SELECT available_quantity FROM stocks WHERE account_id='acc-k1' AND sku='SKU-K1'",
                            Integer.class);
                    assertThat(qty).isEqualTo(15);
                });

        String status = jdbcTemplate.queryForObject(
                "SELECT status FROM processed_events WHERE event_id='kafka-t1-001'", String.class);
        assertThat(status).isEqualTo("PROCESSED");
    }

    // ─── Test 2: Consumer is idempotent ──────────────────────────────────────

    @Test
    @DisplayName("Test 2: Same eventId published twice — balance updated exactly once, second is IGNORED")
    void kafkaConsumer_idempotent() {
        String event = """
                {"eventId":"kafka-t2-001","type":"STOCK_ADJUSTED","occurredAt":"%s",
                 "accountId":"acc-k2","sku":"SKU-K2","available":10,"reason":"idempotent-test"}
                """.formatted(Instant.now()).strip();

        kafkaTemplate.send("stock-events", "acc-k2:SKU-K2", event);
        kafkaTemplate.send("stock-events", "acc-k2:SKU-K2", event);

        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> {
                    Integer qty = jdbcTemplate.queryForObject(
                            "SELECT available_quantity FROM stocks WHERE account_id='acc-k2' AND sku='SKU-K2'",
                            Integer.class);
                    assertThat(qty).isEqualTo(10);
                });

        // Unique constraint on event_id: only one processed_events row
        int count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_events WHERE event_id='kafka-t2-001'", Integer.class);
        assertThat(count).isEqualTo(1);
    }

    // ─── Test 3: DLQ routing on malformed event ───────────────────────────────

    @Test
    @DisplayName("Test 3: Malformed event is routed to DLQ after retries exhausted")
    void kafkaConsumer_dlqRouting_onMalformedEvent() {
        kafkaTemplate.send("stock-events", "acc-k3:SKU-K3", "{ INVALID JSON }");

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

    // ─── Test 4: Outbox relay publishes processed events ─────────────────────

    @Test
    @DisplayName("Test 4: REST event triggers outbox entry; relay publishes to stock-events-processed")
    void outboxRelay_publishesProcessedEvent() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        String body = """
                {"eventId":"kafka-t4-001","type":"STOCK_ADJUSTED","occurredAt":"%s",
                 "accountId":"acc-k4","sku":"SKU-K4","available":5,"reason":"outbox-test"}
                """.formatted(Instant.now()).strip();

        restTemplate.postForEntity(baseUrl + "/events", new HttpEntity<>(body, headers), Map.class);

        // Outbox entry must be written atomically with the stock update
        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    int count = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id='acc-k4:SKU-K4'",
                            Integer.class);
                    assertThat(count).isEqualTo(1);
                });

        // Force relay to publish
        outboxRelay.processOutbox();

        // Outbox entry must be PUBLISHED
        String outboxStatus = jdbcTemplate.queryForObject(
                "SELECT status FROM outbox_events WHERE aggregate_id='acc-k4:SKU-K4'", String.class);
        assertThat(outboxStatus).isEqualTo("PUBLISHED");

        // Message must appear in stock-events-processed
        KafkaConsumer<String, String> consumer = buildTestConsumer("stock-events-processed");
        List<String> received = new ArrayList<>();
        try {
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .pollInterval(Duration.ofMillis(500))
                    .untilAsserted(() -> {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                        records.forEach(r -> received.add(r.value()));
                        assertThat(received).anyMatch(v -> v.contains("kafka-t4-001"));
                    });
        } finally {
            consumer.close();
        }
    }

    // ─── Test 5: SKIP LOCKED prevents duplicate publishing ────────────────────

    @Test
    @DisplayName("Test 5: Two concurrent relay instances — SKIP LOCKED ensures exactly one publish")
    void outboxRelay_skipLocked_preventsDoublePublish() throws Exception {
        // Insert one PENDING outbox entry directly
        jdbcTemplate.update("""
                INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count)
                VALUES ('STOCK', 'acc-k5:SKU-K5', 'STOCK_ADJUSTED', '{"eventId":"kafka-t5-001"}', 'PENDING', NOW(), 0)
                """);

        // Run two relay instances concurrently — SKIP LOCKED ensures only one picks the row
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

        // The row must be PUBLISHED exactly once
        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    String status = jdbcTemplate.queryForObject(
                            "SELECT status FROM outbox_events WHERE aggregate_id='acc-k5:SKU-K5'",
                            String.class);
                    assertThat(status).isEqualTo("PUBLISHED");
                });

        // Exactly one message in stock-events-processed for this aggregate
        KafkaConsumer<String, String> consumer = buildTestConsumer("stock-events-processed");
        AtomicInteger msgCount = new AtomicInteger(0);
        try {
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .pollInterval(Duration.ofMillis(500))
                    .untilAsserted(() -> {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                        records.forEach(r -> {
                            if ("acc-k5:SKU-K5".equals(r.key())) msgCount.incrementAndGet();
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
