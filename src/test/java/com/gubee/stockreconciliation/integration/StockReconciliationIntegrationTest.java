package com.gubee.stockreconciliation.integration;

import com.gubee.stockreconciliation.adapter.out.kafka.OutboxRelay;
import com.gubee.stockreconciliation.domain.model.enums.EventStatus;
import com.gubee.stockreconciliation.domain.model.enums.EventType;
import com.gubee.stockreconciliation.domain.model.enums.OrderLifecycleState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:postgresql://localhost:5432/stockdb",
        "spring.datasource.username=stockuser",
        "spring.datasource.password=stockpass",
        "spring.kafka.listener.auto-startup=false"
})
class StockReconciliationIntegrationTest {

    // The OutboxRelay is mocked here because this test suite has no Kafka broker.
    // Without this mock, the @Scheduled relay would hold DB connections for 5s per entry
    // (waiting for Kafka sends to time out), starving the connection pool mid-test.
    // The relay's correctness is covered by StockEventKafkaConsumerIntegrationTest.
    @MockBean
    OutboxRelay outboxRelay;

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    JdbcTemplate jdbcTemplate;

    private String baseUrl;

    @BeforeEach
    void setUp() {
        baseUrl = "http://localhost:" + port;
        jdbcTemplate.execute("TRUNCATE TABLE stock_history, order_states, processed_events, stocks RESTART IDENTITY CASCADE");
    }

    // ─── Helpers ────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private ResponseEntity<Map<String, Object>> postEvent(Map<String, Object> body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return restTemplate.exchange(baseUrl + "/events", HttpMethod.POST,
                new HttpEntity<>(body, headers), (Class<Map<String, Object>>) (Class<?>) Map.class);
    }

    private String statusOf(ResponseEntity<Map<String, Object>> resp) {
        return (String) resp.getBody().get("status");
    }

    private Map stockAdjustedEvent(String eventId, String accountId, String sku, int available) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("eventId", eventId);
        event.put("type", "STOCK_ADJUSTED");
        event.put("occurredAt", Instant.now().toString());
        event.put("accountId", accountId);
        event.put("sku", sku);
        event.put("available", available);
        event.put("reason", "test");
        return event;
    }

    private Map orderCreatedEvent(String eventId, String accountId, String sku, int qty, String orderId) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("eventId", eventId);
        event.put("type", "ORDER_CREATED");
        event.put("occurredAt", Instant.now().toString());
        event.put("accountId", accountId);
        event.put("sku", sku);
        event.put("marketplace", "MERCADO_LIVRE");
        event.put("externalOrderId", orderId);
        event.put("quantity", qty);
        return event;
    }

    private Map orderCancelledEvent(String eventId, String accountId, String sku, int qty, String orderId) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("eventId", eventId);
        event.put("type", "ORDER_CANCELLED");
        event.put("occurredAt", Instant.now().toString());
        event.put("accountId", accountId);
        event.put("sku", sku);
        event.put("marketplace", "MERCADO_LIVRE");
        event.put("externalOrderId", orderId);
        event.put("quantity", qty);
        return event;
    }

    private Map marketplaceRestoredEvent(String eventId, String accountId, String sku, int qty, String orderId) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("eventId", eventId);
        event.put("type", "MARKETPLACE_STOCK_RESTORED");
        event.put("occurredAt", Instant.now().toString());
        event.put("accountId", accountId);
        event.put("sku", sku);
        event.put("marketplace", "MERCADO_LIVRE");
        event.put("externalOrderId", orderId);
        event.put("quantity", qty);
        return event;
    }

    private int currentStock(String accountId, String sku) {
        Integer qty = jdbcTemplate.queryForObject(
                "SELECT available_quantity FROM stocks WHERE account_id = ? AND sku = ?",
                Integer.class, accountId, sku);
        return qty != null ? qty : -1;
    }

    private String orderState(String marketplace, String accountId, String externalOrderId, String sku) {
        return jdbcTemplate.queryForObject(
                "SELECT state FROM order_states WHERE marketplace=? AND account_id=? AND external_order_id=? AND sku=?",
                String.class, marketplace, accountId, externalOrderId, sku);
    }

    private String eventStatus(String eventId) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM processed_events WHERE event_id = ?",
                String.class, eventId);
    }

    // ─── Scenario 1: Stock adjusted ─────────────────────────────────────────

    @Test
    @DisplayName("Scenario 1: STOCK_ADJUSTED available=10 → stock=10")
    void scenario1_stockAdjusted() {
        var resp = postEvent(stockAdjustedEvent("evt-s1-001", "acc-001", "SKU-A", 10));

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(statusOf(resp)).isEqualTo("PROCESSED");
        assertThat(currentStock("acc-001", "SKU-A")).isEqualTo(10);
    }

    // ─── Scenario 2: Order reduces stock ────────────────────────────────────

    @Test
    @DisplayName("Scenario 2: STOCK_ADJUSTED=10 + ORDER_CREATED qty=2 → stock=8")
    void scenario2_orderReducesStock() {
        postEvent(stockAdjustedEvent("evt-s2-001", "acc-001", "SKU-B", 10));
        var resp = postEvent(orderCreatedEvent("evt-s2-002", "acc-001", "SKU-B", 2, "ML-200"));

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(statusOf(resp)).isEqualTo("PROCESSED");
        assertThat(currentStock("acc-001", "SKU-B")).isEqualTo(8);
    }

    // ─── Scenario 3: Cancellation restores stock ────────────────────────────

    @Test
    @DisplayName("Scenario 3: ORDER_CREATED qty=2 + ORDER_CANCELLED → stock restores")
    void scenario3_cancellationRestoresStock() {
        postEvent(stockAdjustedEvent("evt-s3-001", "acc-001", "SKU-C", 10));
        postEvent(orderCreatedEvent("evt-s3-002", "acc-001", "SKU-C", 2, "ML-300"));
        assertThat(currentStock("acc-001", "SKU-C")).isEqualTo(8);

        var resp = postEvent(orderCancelledEvent("evt-s3-003", "acc-001", "SKU-C", 2, "ML-300"));

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(statusOf(resp)).isEqualTo("PROCESSED");
        assertThat(currentStock("acc-001", "SKU-C")).isEqualTo(10);
        assertThat(orderState("MERCADO_LIVRE", "acc-001", "ML-300", "SKU-C"))
                .isEqualTo("CANCELLED");
    }

    // ─── Scenario 4: Duplicate eventId ──────────────────────────────────────

    @Test
    @DisplayName("Scenario 4: Same eventId processed twice — stock changes only once")
    void scenario4_duplicateEventId() {
        postEvent(stockAdjustedEvent("evt-s4-001", "acc-001", "SKU-D", 10));

        postEvent(orderCreatedEvent("evt-s4-002", "acc-001", "SKU-D", 2, "ML-400"));
        assertThat(currentStock("acc-001", "SKU-D")).isEqualTo(8);

        // Send the exact same ORDER_CREATED again
        var secondResp = postEvent(orderCreatedEvent("evt-s4-002", "acc-001", "SKU-D", 2, "ML-400"));
        assertThat(secondResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(statusOf(secondResp)).isEqualTo("IGNORED");

        // Stock must still be 8, not 6
        assertThat(currentStock("acc-001", "SKU-D")).isEqualTo(8);

        // Only one processed_events row for this eventId
        int count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_events WHERE event_id = 'evt-s4-002'", Integer.class);
        assertThat(count).isEqualTo(1);
    }

    // ─── Scenario 5: Duplicate cancellation ────────────────────────────────

    @Test
    @DisplayName("Scenario 5: Two ORDER_CANCELLED for same order — stock restored only once")
    void scenario5_duplicateCancellation() {
        postEvent(stockAdjustedEvent("evt-s5-001", "acc-001", "SKU-E", 10));
        postEvent(orderCreatedEvent("evt-s5-002", "acc-001", "SKU-E", 2, "ML-500"));
        postEvent(orderCancelledEvent("evt-s5-003", "acc-001", "SKU-E", 2, "ML-500"));
        assertThat(currentStock("acc-001", "SKU-E")).isEqualTo(10);

        // Second cancellation for the same order (different eventId)
        var resp = postEvent(orderCancelledEvent("evt-s5-004", "acc-001", "SKU-E", 2, "ML-500"));

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(statusOf(resp)).isEqualTo("INCONSISTENT");

        // Stock must remain 10, not go to 12
        assertThat(currentStock("acc-001", "SKU-E")).isEqualTo(10);
        assertThat(eventStatus("evt-s5-004")).isEqualTo("INCONSISTENT");
    }

    // ─── Scenario 6: Out-of-order cancellation ──────────────────────────────

    @Nested
    @DisplayName("Scenario 6: ORDER_CANCELLED before ORDER_CREATED")
    class Scenario6OutOfOrder {

        @Test
        @DisplayName("6a: ORDER_CANCELLED is held as PENDING")
        void cancelledBeforeCreated_isHeldPending() {
            postEvent(stockAdjustedEvent("evt-s6-001", "acc-001", "SKU-F", 10));

            // ORDER_CANCELLED arrives first
            var cancelResp = postEvent(orderCancelledEvent("evt-s6-002", "acc-001", "SKU-F", 2, "ML-600"));
            assertThat(cancelResp.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
            assertThat(statusOf(cancelResp)).isEqualTo("PENDING");

            // Stock unchanged
            assertThat(currentStock("acc-001", "SKU-F")).isEqualTo(10);
            assertThat(eventStatus("evt-s6-002")).isEqualTo("PENDING");
        }

        @Test
        @DisplayName("6b: ORDER_CREATED resolves PENDING atomically — net stock change = 0")
        void createdAfterCancelled_resolvesAtomically() {
            postEvent(stockAdjustedEvent("evt-s6a-001", "acc-001", "SKU-G", 10));
            postEvent(orderCancelledEvent("evt-s6a-002", "acc-001", "SKU-G", 2, "ML-601"));

            // ORDER_CREATED arrives later
            var createdResp = postEvent(orderCreatedEvent("evt-s6a-003", "acc-001", "SKU-G", 2, "ML-601"));
            assertThat(createdResp.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(statusOf(createdResp)).isEqualTo("PROCESSED");

            // Net stock change = 0
            assertThat(currentStock("acc-001", "SKU-G")).isEqualTo(10);

            // Both events now PROCESSED
            assertThat(eventStatus("evt-s6a-002")).isEqualTo("PROCESSED");
            assertThat(eventStatus("evt-s6a-003")).isEqualTo("PROCESSED");

            // Order state = CANCELLED
            assertThat(orderState("MERCADO_LIVRE", "acc-001", "ML-601", "SKU-G")).isEqualTo("CANCELLED");

            // Audit trail: 3 entries (STOCK_ADJUSTED + ORDER_CREATED subtract + ORDER_CANCELLED add-back)
            int historyCount = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM stock_history sh " +
                    "JOIN stocks s ON s.id = sh.stock_id " +
                    "WHERE s.account_id = 'acc-001' AND s.sku = 'SKU-G'", Integer.class);
            assertThat(historyCount).isEqualTo(3);
        }
    }

    // ─── Scenario 7: Concurrent ORDER_CREATED — no negative stock ───────────

    @Test
    @DisplayName("Scenario 7: Concurrent ORDER_CREATED events — stock never goes negative")
    void scenario7_concurrentOrdersNoNegativeStock() throws InterruptedException {
        // Set stock to 5 units
        postEvent(stockAdjustedEvent("evt-s7-001", "acc-007", "SKU-H", 5));

        int threads = 10;
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger conflictOrErrorCount = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            final int idx = i;
            pool.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                Map<String, Object> event = orderCreatedEvent(
                        "evt-s7-order-" + idx, "acc-007", "SKU-H", 1, "ML-700-" + idx);
                var resp = postEvent(event);
                if (resp.getStatusCode() == HttpStatus.OK) {
                    successCount.incrementAndGet();
                } else {
                    conflictOrErrorCount.incrementAndGet();
                }
            });
        }

        ready.await();
        start.countDown();
        pool.shutdown();
        pool.awaitTermination(30, TimeUnit.SECONDS);

        int remaining = currentStock("acc-007", "SKU-H");
        assertThat(remaining).isGreaterThanOrEqualTo(0);
        assertThat(successCount.get()).isLessThanOrEqualTo(5);
        // successful orders + remaining stock must equal initial stock
        assertThat(successCount.get() + remaining).isEqualTo(5);
    }

    // ─── Scenario 8: Marketplace restoration + Cancellation ────────────────

    @Nested
    @DisplayName("Scenario 8: MARKETPLACE_STOCK_RESTORED and ORDER_CANCELLED — no double restoration")
    class Scenario8NoDoubleRestoration {

        @Test
        @DisplayName("8a: ORDER_CANCELLED after MARKETPLACE_STOCK_RESTORED → INCONSISTENT")
        void cancelledAfterRestored_isInconsistent() {
            postEvent(stockAdjustedEvent("evt-s8a-001", "acc-001", "SKU-I", 10));
            postEvent(orderCreatedEvent("evt-s8a-002", "acc-001", "SKU-I", 2, "ML-800"));
            assertThat(currentStock("acc-001", "SKU-I")).isEqualTo(8);

            // Marketplace restores first
            var restoreResp = postEvent(marketplaceRestoredEvent("evt-s8a-003", "acc-001", "SKU-I", 2, "ML-800"));
            assertThat(restoreResp.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(statusOf(restoreResp)).isEqualTo("PROCESSED");
            assertThat(currentStock("acc-001", "SKU-I")).isEqualTo(10);

            // ORDER_CANCELLED arrives after — should be rejected
            var cancelResp = postEvent(orderCancelledEvent("evt-s8a-004", "acc-001", "SKU-I", 2, "ML-800"));
            assertThat(cancelResp.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
            assertThat(statusOf(cancelResp)).isEqualTo("INCONSISTENT");

            // Stock must remain 10, not go to 12
            assertThat(currentStock("acc-001", "SKU-I")).isEqualTo(10);
        }

        @Test
        @DisplayName("8b: MARKETPLACE_STOCK_RESTORED after ORDER_CANCELLED → INCONSISTENT")
        void restoredAfterCancelled_isInconsistent() {
            postEvent(stockAdjustedEvent("evt-s8b-001", "acc-001", "SKU-J", 10));
            postEvent(orderCreatedEvent("evt-s8b-002", "acc-001", "SKU-J", 2, "ML-801"));
            postEvent(orderCancelledEvent("evt-s8b-003", "acc-001", "SKU-J", 2, "ML-801"));
            assertThat(currentStock("acc-001", "SKU-J")).isEqualTo(10);

            // Marketplace tries to restore after cancellation — should be rejected
            var resp = postEvent(marketplaceRestoredEvent("evt-s8b-004", "acc-001", "SKU-J", 2, "ML-801"));
            assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
            assertThat(statusOf(resp)).isEqualTo("INCONSISTENT");

            // Stock must remain 10, not go to 12
            assertThat(currentStock("acc-001", "SKU-J")).isEqualTo(10);
        }
    }

    // ─── Multi-account isolation ─────────────────────────────────────────────

    @Test
    @DisplayName("Multi-account: same SKU in different accounts is independent")
    void multiAccountIsolation() {
        postEvent(stockAdjustedEvent("evt-ma-001", "acc-A", "SKU-Z", 10));
        postEvent(stockAdjustedEvent("evt-ma-002", "acc-B", "SKU-Z", 20));

        postEvent(orderCreatedEvent("evt-ma-003", "acc-A", "SKU-Z", 3, "ML-A1"));
        postEvent(orderCreatedEvent("evt-ma-004", "acc-B", "SKU-Z", 5, "ML-B1"));

        assertThat(currentStock("acc-A", "SKU-Z")).isEqualTo(7);
        assertThat(currentStock("acc-B", "SKU-Z")).isEqualTo(15);
    }
}
