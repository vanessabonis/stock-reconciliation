package com.gubee.stockreconciliation.integration;

import com.gubee.stockreconciliation.adapter.out.kafka.OutboxRelay;
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

    // OutboxRelay is mocked — no Kafka broker in this suite.
    // Without this mock, the relay would hold DB connections waiting for Kafka sends to time out.
    // Relay correctness is covered by StockEventKafkaConsumerIntegrationTest.
    @MockBean
    OutboxRelay outboxRelay;

    @LocalServerPort int port;
    @Autowired TestRestTemplate restTemplate;
    @Autowired JdbcTemplate jdbcTemplate;

    private String baseUrl;

    // Realistic identifiers matching teste-local.http
    private static final String VENDEDOR_NIKE  = "vendedor-nike";
    private static final String VENDEDOR_APPLE = "vendedor-apple";
    private static final String SKU            = "TENIS-NIKE-AIRMAX-41";
    private static final String MARKETPLACE    = "MERCADO_LIVRE";

    @BeforeEach
    void setUp() {
        baseUrl = "http://localhost:" + port;
        jdbcTemplate.execute("TRUNCATE TABLE outbox_events, stock_history, order_states, processed_events, stocks RESTART IDENTITY CASCADE");
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

    private Map<String, Object> stockAdjustedEvent(String eventId, String accountId, String sku,
                                                    int available, String reason) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("eventId", eventId);
        e.put("type", "STOCK_ADJUSTED");
        e.put("occurredAt", "2026-05-28T10:00:00Z");
        e.put("accountId", accountId);
        e.put("sku", sku);
        e.put("available", available);
        e.put("reason", reason);
        return e;
    }

    private Map<String, Object> orderCreatedEvent(String eventId, String accountId, String sku,
                                                   int qty, String orderId) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("eventId", eventId);
        e.put("type", "ORDER_CREATED");
        e.put("occurredAt", "2026-05-28T10:05:00Z");
        e.put("accountId", accountId);
        e.put("sku", sku);
        e.put("marketplace", MARKETPLACE);
        e.put("externalOrderId", orderId);
        e.put("quantity", qty);
        return e;
    }

    private Map<String, Object> orderCancelledEvent(String eventId, String accountId, String sku,
                                                     int qty, String orderId) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("eventId", eventId);
        e.put("type", "ORDER_CANCELLED");
        e.put("occurredAt", "2026-05-28T10:10:00Z");
        e.put("accountId", accountId);
        e.put("sku", sku);
        e.put("marketplace", MARKETPLACE);
        e.put("externalOrderId", orderId);
        e.put("quantity", qty);
        return e;
    }

    private Map<String, Object> marketplaceRestoredEvent(String eventId, String accountId, String sku,
                                                          int qty, String orderId) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("eventId", eventId);
        e.put("type", "MARKETPLACE_STOCK_RESTORED");
        e.put("occurredAt", "2026-05-28T10:35:00Z");
        e.put("accountId", accountId);
        e.put("sku", sku);
        e.put("marketplace", MARKETPLACE);
        e.put("externalOrderId", orderId);
        e.put("quantity", qty);
        return e;
    }

    private int currentStock(String accountId, String sku) {
        Integer qty = jdbcTemplate.queryForObject(
                "SELECT available_quantity FROM stocks WHERE account_id = ? AND sku = ?",
                Integer.class, accountId, sku);
        return qty != null ? qty : -1;
    }

    private String orderState(String accountId, String orderId, String sku) {
        return jdbcTemplate.queryForObject(
                "SELECT state FROM order_states WHERE marketplace=? AND account_id=? AND external_order_id=? AND sku=?",
                String.class, MARKETPLACE, accountId, orderId, sku);
    }

    private String eventStatus(String eventId) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM processed_events WHERE event_id = ?",
                String.class, eventId);
    }

    // ─── Cenário 1: Ajuste inicial de estoque ───────────────────────────────

    @Test
    @DisplayName("Cenário 1: STOCK_ADJUSTED available=10 → saldo=10")
    void cenario1_ajusteInicialDeEstoque() {
        var resp = postEvent(stockAdjustedEvent("evt-001", VENDEDOR_NIKE, SKU, 10, "inventario_inicial"));

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(statusOf(resp)).isEqualTo("PROCESSED");
        assertThat(currentStock(VENDEDOR_NIKE, SKU)).isEqualTo(10);
    }

    // ─── Cenário 2: Pedido baixa o estoque ──────────────────────────────────

    @Test
    @DisplayName("Cenário 2: STOCK_ADJUSTED=10 + ORDER_CREATED qty=2 → saldo=8")
    void cenario2_pedidoBaixaEstoque() {
        postEvent(stockAdjustedEvent("evt-001", VENDEDOR_NIKE, SKU, 10, "inventario_inicial"));
        var resp = postEvent(orderCreatedEvent("evt-002", VENDEDOR_NIKE, SKU, 2, "PEDIDO-NIKE-001"));

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(statusOf(resp)).isEqualTo("PROCESSED");
        assertThat(currentStock(VENDEDOR_NIKE, SKU)).isEqualTo(8);
    }

    // ─── Cenário 3: Cancelamento devolve o estoque ──────────────────────────

    @Test
    @DisplayName("Cenário 3: ORDER_CREATED + ORDER_CANCELLED → saldo restaurado para 10")
    void cenario3_cancelamentoDevolveEstoque() {
        postEvent(stockAdjustedEvent("evt-001", VENDEDOR_NIKE, SKU, 10, "inventario_inicial"));
        postEvent(orderCreatedEvent("evt-002", VENDEDOR_NIKE, SKU, 2, "PEDIDO-NIKE-001"));
        assertThat(currentStock(VENDEDOR_NIKE, SKU)).isEqualTo(8);

        var resp = postEvent(orderCancelledEvent("evt-003", VENDEDOR_NIKE, SKU, 2, "PEDIDO-NIKE-001"));

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(statusOf(resp)).isEqualTo("PROCESSED");
        assertThat(currentStock(VENDEDOR_NIKE, SKU)).isEqualTo(10);
        assertThat(orderState(VENDEDOR_NIKE, "PEDIDO-NIKE-001", SKU)).isEqualTo("CANCELLED");
    }

    // ─── Cenário 4: Idempotência por eventId ────────────────────────────────

    @Test
    @DisplayName("Cenário 4: mesmo eventId enviado duas vezes → segundo retorna IGNORED, saldo inalterado")
    void cenario4_idempotenciaPorEventId() {
        postEvent(stockAdjustedEvent("evt-001", VENDEDOR_NIKE, SKU, 10, "inventario_inicial"));
        postEvent(orderCreatedEvent("evt-002", VENDEDOR_NIKE, SKU, 2, "PEDIDO-NIKE-001"));
        assertThat(currentStock(VENDEDOR_NIKE, SKU)).isEqualTo(8);

        // Reenvio exato do evt-001 (mesmo eventId, mesmo body)
        var segundaVez = postEvent(stockAdjustedEvent("evt-001", VENDEDOR_NIKE, SKU, 10, "inventario_inicial"));
        assertThat(segundaVez.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(statusOf(segundaVez)).isEqualTo("IGNORED");

        // Saldo não pode ter voltado para 10 — idempotência garantida pela unique constraint em event_id
        assertThat(currentStock(VENDEDOR_NIKE, SKU)).isEqualTo(8);

        int count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_events WHERE event_id = 'evt-001'", Integer.class);
        assertThat(count).isEqualTo(1);
    }

    // ─── Cenário 5: Cancelamento duplicado (duplicidade lógica) ─────────────

    @Test
    @DisplayName("Cenário 5: dois ORDER_CANCELLED para o mesmo pedido → segundo retorna INCONSISTENT, saldo=10")
    void cenario5_cancelamentoDuplicado() {
        postEvent(stockAdjustedEvent("evt-001", VENDEDOR_NIKE, SKU, 10, "inventario_inicial"));
        postEvent(orderCreatedEvent("evt-002", VENDEDOR_NIKE, SKU, 2, "PEDIDO-NIKE-001"));
        postEvent(orderCancelledEvent("evt-003", VENDEDOR_NIKE, SKU, 2, "PEDIDO-NIKE-001"));
        assertThat(currentStock(VENDEDOR_NIKE, SKU)).isEqualTo(10);

        // Segundo cancelamento do PEDIDO-NIKE-001 com eventId diferente (evt-004)
        var resp = postEvent(orderCancelledEvent("evt-004", VENDEDOR_NIKE, SKU, 2, "PEDIDO-NIKE-001"));

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(statusOf(resp)).isEqualTo("INCONSISTENT");
        // Saldo não pode ter ido para 12 — OrderState já era CANCELLED
        assertThat(currentStock(VENDEDOR_NIKE, SKU)).isEqualTo(10);
        assertThat(eventStatus("evt-004")).isEqualTo("INCONSISTENT");
    }

    // ─── Cenário 6: Cancelamento antes do pedido (fora de ordem) ────────────

    @Nested
    @DisplayName("Cenário 6: ORDER_CANCELLED chega antes do ORDER_CREATED")
    class Cenario6ForaDeOrdem {

        @Test
        @DisplayName("6a: ORDER_CANCELLED é mantido como PENDING — saldo inalterado")
        void cancelamento_antesDaPedicao_ficaPending() {
            postEvent(stockAdjustedEvent("evt-001", VENDEDOR_NIKE, SKU, 10, "inventario_inicial"));

            // Cancelamento do PEDIDO-NIKE-002 que ainda não existe no sistema
            var cancelResp = postEvent(orderCancelledEvent("evt-005", VENDEDOR_NIKE, SKU, 2, "PEDIDO-NIKE-002"));
            assertThat(cancelResp.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
            assertThat(statusOf(cancelResp)).isEqualTo("PENDING");

            // Saldo não muda — cancelamento suspenso aguardando ORDER_CREATED
            assertThat(currentStock(VENDEDOR_NIKE, SKU)).isEqualTo(10);
            assertThat(eventStatus("evt-005")).isEqualTo("PENDING");
        }

        @Test
        @DisplayName("6b: ORDER_CREATED resolve PENDING atomicamente — delta líquido = 0, saldo=10")
        void criacao_tardia_resolveAtomicamente() {
            postEvent(stockAdjustedEvent("evt-001", VENDEDOR_NIKE, SKU, 10, "inventario_inicial"));
            // Cancelamento chegou primeiro
            postEvent(orderCancelledEvent("evt-005", VENDEDOR_NIKE, SKU, 2, "PEDIDO-NIKE-002"));

            // Criação tardia do PEDIDO-NIKE-002 (que já havia sido cancelado)
            var createdResp = postEvent(orderCreatedEvent("evt-006", VENDEDOR_NIKE, SKU, 2, "PEDIDO-NIKE-002"));
            assertThat(createdResp.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(statusOf(createdResp)).isEqualTo("PROCESSED");

            // Delta líquido = 0: subtract (ORDER_CREATED) + add-back (ORDER_CANCELLED) na mesma transação
            assertThat(currentStock(VENDEDOR_NIKE, SKU)).isEqualTo(10);
            assertThat(eventStatus("evt-005")).isEqualTo("PROCESSED");
            assertThat(eventStatus("evt-006")).isEqualTo("PROCESSED");
            assertThat(orderState(VENDEDOR_NIKE, "PEDIDO-NIKE-002", SKU)).isEqualTo("CANCELLED");

            // Audit trail: STOCK_ADJUSTED + ORDER_CREATED subtract + ORDER_CANCELLED add-back
            int historyCount = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM stock_history sh " +
                    "JOIN stocks s ON s.id = sh.stock_id " +
                    "WHERE s.account_id = ? AND s.sku = ?", Integer.class, VENDEDOR_NIKE, SKU);
            assertThat(historyCount).isEqualTo(3);
        }
    }

    // ─── Cenário 7: ORDER_CREATED concorrentes — saldo nunca negativo ────────

    @Test
    @DisplayName("Cenário 7: 10 ORDER_CREATED concorrentes com estoque=5 → saldo ≥ 0 e sucesso+saldo=5")
    void cenario7_pedidosConcorrentesNaoNegativoEstoque() throws InterruptedException {
        postEvent(stockAdjustedEvent("evt-001", VENDEDOR_NIKE, SKU, 5, "inventario_inicial"));

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
                try { start.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
                var resp = postEvent(orderCreatedEvent(
                        "evt-conc-" + idx, VENDEDOR_NIKE, SKU, 1, "PEDIDO-NIKE-CONC-" + idx));
                if (resp.getStatusCode() == HttpStatus.OK) successCount.incrementAndGet();
                else conflictOrErrorCount.incrementAndGet();
            });
        }

        ready.await();
        start.countDown();
        pool.shutdown();
        pool.awaitTermination(30, TimeUnit.SECONDS);

        int remaining = currentStock(VENDEDOR_NIKE, SKU);
        assertThat(remaining).isGreaterThanOrEqualTo(0);
        assertThat(successCount.get()).isLessThanOrEqualTo(5);
        assertThat(successCount.get() + remaining).isEqualTo(5);
    }

    // ─── Cenário 8: Recomposição do marketplace + cancelamento tardio ─────────

    @Nested
    @DisplayName("Cenário 8: MARKETPLACE_STOCK_RESTORED e ORDER_CANCELLED — sem dupla restauração")
    class Cenario8SemDuplaRestauracao {

        @Test
        @DisplayName("8a: ORDER_CANCELLED após MARKETPLACE_STOCK_RESTORED → INCONSISTENT, saldo=10")
        void cancelamentoAposRestauracao_inconsistente() {
            postEvent(stockAdjustedEvent("evt-001", VENDEDOR_NIKE, SKU, 10, "inventario_inicial"));
            postEvent(orderCreatedEvent("evt-007", VENDEDOR_NIKE, SKU, 3, "PEDIDO-NIKE-003"));
            assertThat(currentStock(VENDEDOR_NIKE, SKU)).isEqualTo(7);

            // Marketplace restaura primeiro (evt-008)
            var restoreResp = postEvent(marketplaceRestoredEvent("evt-008", VENDEDOR_NIKE, SKU, 3, "PEDIDO-NIKE-003"));
            assertThat(restoreResp.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(statusOf(restoreResp)).isEqualTo("PROCESSED");
            assertThat(currentStock(VENDEDOR_NIKE, SKU)).isEqualTo(10);

            // Cancelamento tardio do mesmo pedido (evt-009) — deve ser bloqueado
            var cancelResp = postEvent(orderCancelledEvent("evt-009", VENDEDOR_NIKE, SKU, 3, "PEDIDO-NIKE-003"));
            assertThat(cancelResp.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
            assertThat(statusOf(cancelResp)).isEqualTo("INCONSISTENT");
            // Saldo não pode ter ido para 13 — segunda restauração bloqueada pela state machine
            assertThat(currentStock(VENDEDOR_NIKE, SKU)).isEqualTo(10);
        }

        @Test
        @DisplayName("8b: MARKETPLACE_STOCK_RESTORED após ORDER_CANCELLED → INCONSISTENT, saldo=10")
        void restauracaoAposCancelamento_inconsistente() {
            postEvent(stockAdjustedEvent("evt-001", VENDEDOR_NIKE, SKU, 10, "inventario_inicial"));
            postEvent(orderCreatedEvent("evt-007", VENDEDOR_NIKE, SKU, 3, "PEDIDO-NIKE-003"));
            postEvent(orderCancelledEvent("evt-003", VENDEDOR_NIKE, SKU, 3, "PEDIDO-NIKE-003"));
            assertThat(currentStock(VENDEDOR_NIKE, SKU)).isEqualTo(10);

            // Marketplace tenta restaurar após cancelamento — deve ser rejeitado
            var resp = postEvent(marketplaceRestoredEvent("evt-008", VENDEDOR_NIKE, SKU, 3, "PEDIDO-NIKE-003"));
            assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
            assertThat(statusOf(resp)).isEqualTo("INCONSISTENT");
            assertThat(currentStock(VENDEDOR_NIKE, SKU)).isEqualTo(10);
        }
    }

    // ─── Cenário 9: Isolamento multi-conta ──────────────────────────────────

    @Test
    @DisplayName("Cenário 9: mesmo SKU em vendedores diferentes tem saldos independentes")
    void cenario9_isolamentoMultiConta() {
        // vendedor-nike tem 10 unidades
        postEvent(stockAdjustedEvent("evt-001", VENDEDOR_NIKE, SKU, 10, "inventario_inicial"));
        // vendedor-apple tem 50 unidades do mesmo SKU — saldos são independentes
        postEvent(stockAdjustedEvent("evt-011", VENDEDOR_APPLE, SKU, 50, "estoque_vendedor_apple"));

        postEvent(orderCreatedEvent("evt-002", VENDEDOR_NIKE,  SKU, 2, "PEDIDO-NIKE-001"));
        postEvent(orderCreatedEvent("evt-apple-001", VENDEDOR_APPLE, SKU, 5, "PEDIDO-APPLE-001"));

        assertThat(currentStock(VENDEDOR_NIKE,  SKU)).isEqualTo(8);
        assertThat(currentStock(VENDEDOR_APPLE, SKU)).isEqualTo(45);
    }
}
