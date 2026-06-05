/**
 * GUBEE STOCK RECONCILIATION — k6 Stress Test
 *
 * Pré-requisito: docker compose up --build -d (API em http://localhost:8080)
 *
 * Execução (via Docker, sem instalação do k6):
 *
 *   # Todos os cenários de uma vez:
 *   docker run --rm -i --network host grafana/k6 run - < stress-test/k6-stress.js
 *
 *   # Cenário individual (substitua o valor de SCENARIO):
 *   docker run --rm -i --network host \
 *     -e BASE_URL=http://localhost:8080 \
 *     grafana/k6 run - < stress-test/k6-stress.js \
 *     --env SCENARIO=concurrent_same_sku
 *
 *   Valores válidos de SCENARIO:
 *     concurrent_same_sku   → Scenario A: optimistic locking
 *     different_skus        → Scenario B: throughput com SKUs distintos
 *     idempotency_flood     → Scenario C: unicidade de eventId
 *     realistic_load        → Scenario D: carga de produção simulada
 */

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

// ─── Configuração ─────────────────────────────────────────────────────────────

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

// ─── Métricas customizadas ────────────────────────────────────────────────────

// Conta quantas vezes o serviço retornou 409 (optimistic lock esgotado).
// Alert: se subir muito no Scenario A, o retry está sendo esgotado com frequência.
const optimisticLockRetries = new Counter('optimistic_lock_retries');

// Conta eventos INCONSISTENT (duplicidade lógica ou restauração dupla).
// No Scenario D esperamos algum — é comportamento correto.
const inconsistentEvents = new Counter('inconsistent_events');

// Conta eventos PENDING (ORDER_CANCELLED antes do ORDER_CREATED).
// No Scenario D pode ocorrer por timing de geração de IDs.
const pendingEvents = new Counter('pending_events');

// Latência de escrita por tipo de evento — use para identificar qual endpoint é mais lento.
const writeDuration = new Trend('write_duration_ms', true);

// ─── Definição de todos os cenários ──────────────────────────────────────────

const allScenarios = {

  /**
   * SCENARIO A — Concurrent ORDER_CREATED same SKU
   *
   * 50 VUs concorrentes fazendo ORDER_CREATED no MESMO accountId+sku.
   * Testa o optimistic locking sob contenção real.
   *
   * HTTP 409 é COMPORTAMENTO CORRETO aqui — significa que o retry foi esgotado
   * após 3 tentativas. O invariante que importa é: saldo >= 0 ao final.
   * O threshold 'rate<0.05' aceita até 5% de erros de rede (não inclui 409).
   */
  concurrent_same_sku: {
    executor: 'ramping-vus',
    exec: 'scenarioA',
    startVUs: 1,
    stages: [
      { duration: '10s', target: 50 },
      { duration: '20s', target: 50 },
      { duration: '10s', target: 0  },
    ],
    tags: { scenario: 'concurrent_same_sku' },
  },

  /**
   * SCENARIO B — High volume different SKUs
   *
   * 100 VUs, cada um com SKU único — zero contenção entre VUs.
   * Testa throughput puro: cada evento vai para uma partition Kafka diferente
   * (key = accountId:sku), sem colisões de optimistic locking.
   * Threshold mais restrito: p95 < 300ms esperado porque não há contenção.
   */
  different_skus: {
    executor: 'ramping-vus',
    exec: 'scenarioB',
    startVUs: 1,
    stages: [
      { duration: '10s', target: 100 },
      { duration: '30s', target: 100 },
      { duration: '10s', target: 0   },
    ],
    tags: { scenario: 'different_skus' },
  },

  /**
   * SCENARIO C — Idempotency flood
   *
   * 20 VUs enviando o mesmo eventId repetidamente por 30s.
   * O UNIQUE CONSTRAINT em processed_events.event_id deve absorver todas.
   * Invariante: todas as respostas devem ser PROCESSED ou IGNORED — nunca 5xx.
   * Ao final: saldo deve ser exatamente 10 (apenas o primeiro apply conta).
   */
  idempotency_flood: {
    executor: 'constant-vus',
    exec: 'scenarioC',
    vus: 20,
    duration: '30s',
    tags: { scenario: 'idempotency_flood' },
  },

  /**
   * SCENARIO D — Mixed realistic load
   *
   * Rampa até 1000 eventos/seg com distribuição realista de tipos:
   *   20% STOCK_ADJUSTED · 50% ORDER_CREATED · 20% ORDER_CANCELLED · 10% GET
   *
   * Observe no /actuator/prometheus:
   *   - Se gubee_optimistic_lock_retries sobe → DB write contention
   *   - Se kafka_consumer_records_lag sobe → consumer é o bottleneck
   *   - Se gubee_outbox_pending sobe → relay não está dando conta
   */
  realistic_load: {
    executor: 'ramping-arrival-rate',
    exec: 'scenarioD',
    startRate: 10,
    timeUnit: '1s',
    preAllocatedVUs: 50,
    maxVUs: 200,
    stages: [
      { duration: '20s', target: 100  },
      { duration: '30s', target: 1000 },
      { duration: '20s', target: 100  },
    ],
    tags: { scenario: 'realistic_load' },
  },
};

// ─── Seleção de cenário por env var ──────────────────────────────────────────

const selectedScenario = __ENV.SCENARIO;
const activeScenarios = selectedScenario
  ? { [selectedScenario]: allScenarios[selectedScenario] }
  : allScenarios;

if (selectedScenario && !allScenarios[selectedScenario]) {
  throw new Error(`Cenário inválido: "${selectedScenario}". Válidos: ${Object.keys(allScenarios).join(', ')}`);
}

// ─── Opções globais ───────────────────────────────────────────────────────────

export const options = {
  scenarios: activeScenarios,

  thresholds: {
    // saúde geral da API (o mais importante)
    http_req_failed: ['rate<0.02'],

    // performance global (p95 é suficiente)
    http_req_duration: ['p(95)<500'],

    // sanity check básico de volume (evita teste “morto”)
    http_reqs: ['rate>10'],
  },
};

// ─── Helper functions ─────────────────────────────────────────────────────────

/**
 * Gera um ID único baseado em VU, iteração e timestamp.
 * Evita colisões entre VUs no mesmo momento.
 */
function uniqueId() {
  return `${__VU}-${__ITER}-${Date.now()}`;
}

/**
 * Envia STOCK_ADJUSTED (absoluto — define o saldo, não soma).
 */
function stockAdjusted(accountId, sku, available) {
  const res = http.post(
    `${BASE_URL}/events`,
    JSON.stringify({
      eventId:    `evt-adj-${uniqueId()}`,
      type:       'STOCK_ADJUSTED',
      occurredAt: new Date().toISOString(),
      accountId,
      sku,
      available,
      reason:     'stress_test',
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );
  writeDuration.add(res.timings.duration);
  processResponse(res);
  return res;
}

/**
 * Envia ORDER_CREATED (debita quantity do saldo).
 */
function orderCreated(accountId, sku, orderId, quantity) {
  const res = http.post(
    `${BASE_URL}/events`,
    JSON.stringify({
      eventId:         `evt-create-${uniqueId()}`,
      type:            'ORDER_CREATED',
      occurredAt:      new Date().toISOString(),
      accountId,
      sku,
      marketplace:     'MERCADO_LIVRE',
      externalOrderId: orderId,
      quantity,
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );
  writeDuration.add(res.timings.duration);
  processResponse(res);
  return res;
}

/**
 * Envia ORDER_CANCELLED (devolve quantity ao saldo).
 */
function orderCancelled(accountId, sku, orderId, quantity) {
  const res = http.post(
    `${BASE_URL}/events`,
    JSON.stringify({
      eventId:         `evt-cancel-${uniqueId()}`,
      type:            'ORDER_CANCELLED',
      occurredAt:      new Date().toISOString(),
      accountId,
      sku,
      marketplace:     'MERCADO_LIVRE',
      externalOrderId: orderId,
      quantity,
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );
  writeDuration.add(res.timings.duration);
  processResponse(res);
  return res;
}

/**
 * Consulta o saldo atual de um accountId/sku.
 */
function getStock(accountId, sku) {
  return http.get(
    `${BASE_URL}/stocks/${accountId}/${sku}`,
    { headers: { 'Accept': 'application/json' } }
  );
}

/**
 * Inspeciona a resposta e incrementa métricas customizadas.
 * Chamado após cada request de escrita.
 */
function processResponse(res) {
  if (res.status === 409) {
    optimisticLockRetries.add(1);
    return;
  }
  try {
    const body = JSON.parse(res.body);
    if (body && body.status === 'INCONSISTENT') inconsistentEvents.add(1);
    if (body && body.status === 'PENDING')      pendingEvents.add(1);
  } catch (_) {}
}

// ─── Setup global (roda uma vez antes de todos os cenários) ──────────────────

export function setup() {
  // Scenario A: garante saldo alto para o SKU compartilhado
  // (10.000 unidades = headroom para 50 VUs × 200 iterações)
  const setupA = http.post(
    `${BASE_URL}/events`,
    JSON.stringify({
      eventId:    'evt-stress-setup-sku',
      type:       'STOCK_ADJUSTED',
      occurredAt: new Date().toISOString(),
      accountId:  'stress-account',
      sku:        'STRESS-SKU',
      available:  10000,
      reason:     'stress_test_initial_stock',
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );
  console.log(`Setup Scenario A: STRESS-SKU stock=10000 → ${setupA.status} ${setupA.body}`);

  // Scenario C: envia o evento inundado uma vez para que as réplicas retornem IGNORED
  const setupC = http.post(
    `${BASE_URL}/events`,
    JSON.stringify({
      eventId:    'evt-FLOOD-001',
      type:       'STOCK_ADJUSTED',
      occurredAt: '2026-05-28T10:00:00Z',
      accountId:  'flood-account',
      sku:        'FLOOD-SKU',
      available:  10,
      reason:     'flood_test_setup',
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );
  console.log(`Setup Scenario C: FLOOD-SKU stock=10 → ${setupC.status} ${setupC.body}`);

  return {};
}

// ─── Teardown global (roda uma vez após todos os cenários) ───────────────────

export function teardown(_data) {
  // Scenario A: saldo nunca pode ser negativo, independente de quantos 409s ocorreram
  const resA = getStock('stress-account', 'STRESS-SKU');
  if (resA.status === 200) {
    const stockA = JSON.parse(resA.body);
    const balanceA = stockA.availableQuantity;
    console.log(`\nScenario A final: STRESS-SKU balance = ${balanceA} (deve ser >= 0)`);
    check({ balance: balanceA }, {
      'Scenario A: saldo nunca ficou negativo': (s) => s.balance >= 0,
    });
  } else {
    console.log(`Scenario A teardown: stock não encontrado (${resA.status}) — o cenário pode não ter rodado`);
  }

  // Scenario C: saldo deve ser exatamente 10 — idempotência impediu duplicatas
  const resC = getStock('flood-account', 'FLOOD-SKU');
  if (resC.status === 200) {
    const stockC = JSON.parse(resC.body);
    const balanceC = stockC.availableQuantity;
    console.log(`Scenario C final: FLOOD-SKU balance = ${balanceC} (deve ser exatamente 10)`);
    check({ balance: balanceC }, {
      'Scenario C: saldo = 10 (constraint absorbeu todos os duplicados)': (s) => s.balance === 10,
    });
  } else {
    console.log(`Scenario C teardown: stock não encontrado (${resC.status}) — o cenário pode não ter rodado`);
  }
}

// ─── Funções de cenário exportadas ───────────────────────────────────────────

/**
 * SCENARIO A — Concurrent ORDER_CREATED same SKU
 *
 * 50 threads concorrentes no mesmo SKU.
 * Testa optimistic locking: cada ORDER_CREATED lê a versão, tenta salvar,
 * e pode colidir com outro thread que salvou antes. O serviço retenta até 3x.
 * Após 3 falhas → HTTP 409. Isso é CORRETO — o invariante é saldo >= 0.
 */
export function scenarioA() {
  // 409 (optimistic lock exhausted) e 422 (insufficient stock) são respostas CORRETAS aqui.
  // Sem este callback, k6 conta todo non-2xx como falha — distorcendo http_req_failed
  // e disparando o threshold global, mesmo com o sistema se comportando perfeitamente.
  http.setResponseCallback(http.expectedStatuses(200, 409, 422));

  const orderId = `order-A-${uniqueId()}`;
  const res = orderCreated('stress-account', 'STRESS-SKU', orderId, 1);

  check(res, {
    'não é erro de servidor (5xx)':             (r) => r.status < 500,
    'resposta esperada (200, 409 ou 422)':      (r) => [200, 409, 422].includes(r.status),
  });
}

/**
 * SCENARIO B — High volume different SKUs
 *
 * SKU único por VU+iteração — zero contenção entre threads.
 * Cada par (accountId, sku) vai para a mesma partition Kafka.
 * Testa throughput puro: sem lock contention, sem colisão de OrderState.
 * Se p95 > 300ms aqui, o bottleneck é I/O ou pool de conexões, não locking.
 */
export function scenarioB() {
  const sku       = `SKU-${__VU}-${__ITER}`;
  const accountId = `acc-${__VU}`;
  const orderId   = `order-B-${uniqueId()}`;

  // Garante saldo antes de criar o pedido
  stockAdjusted(accountId, sku, 100);

  const res = orderCreated(accountId, sku, orderId, 1);
  check(res, {
    'ORDER_CREATED processado': (r) => r.status === 200,
  });
}

/**
 * SCENARIO C — Idempotency flood
 *
 * 20 VUs enviando o mesmo eventId 'evt-FLOOD-001' em loop por 30s.
 * O UNIQUE CONSTRAINT em processed_events.event_id garante que apenas
 * o primeiro apply tem efeito. Os demais retornam IGNORED imediatamente.
 * Invariante: NUNCA retornar 5xx. Saldo ao final = exatamente 10.
 */
export function scenarioC() {
  const res = http.post(
    `${BASE_URL}/events`,
    JSON.stringify({
      eventId:    'evt-FLOOD-001',
      type:       'STOCK_ADJUSTED',
      occurredAt: '2026-05-28T10:00:00Z',
      accountId:  'flood-account',
      sku:        'FLOOD-SKU',
      available:  10,
      reason:     'flood_test',
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  processResponse(res);
  check(res, {
    'sem erro de servidor': (r) => r.status < 500,
    'PROCESSED ou IGNORED (nunca 5xx)': (r) => {
      if (r.status !== 200) return false;
      try {
        const body = JSON.parse(r.body);
        return body.status === 'PROCESSED' || body.status === 'IGNORED';
      } catch (_) { return false; }
    },
  });
}

/**
 * SCENARIO D — Mixed realistic load
 *
 * Distribuição baseada em __ITER % 10:
 *   0-1  → 20% STOCK_ADJUSTED
 *   2-6  → 50% ORDER_CREATED
 *   7-8  → 20% ORDER_CANCELLED
 *   9    → 10% GET /stocks
 *
 * Monitore em /actuator/prometheus enquanto este cenário roda:
 *   - gubee_optimistic_lock_retries_total ↑ → contenção no banco
 *   - kafka_consumer_records_lag ↑          → consumer é o bottleneck
 *   - gubee_outbox_pending ↑                → relay não está acompanhando
 *   - gubee_dlq_events_total > 0            → eventos sendo perdidos (alerta crítico)
 */
export function scenarioD() {
  const bucket    = __ITER % 10;
  const sku       = `SKU-D-${__VU % 10}`;
  const accountId = `acc-D-${__VU % 5}`;

  if (bucket < 2) {
    // 20%: ajuste de estoque
    const res = stockAdjusted(accountId, sku, 1000);
    check(res, { 'STOCK_ADJUSTED sem 5xx': (r) => r.status < 500 });

  } else if (bucket < 7) {
    // 50%: criação de pedido
    const orderId = `order-D-${uniqueId()}`;
    const res = orderCreated(accountId, sku, orderId, 1);
    check(res, { 'ORDER_CREATED sem 5xx': (r) => r.status < 500 });

  } else if (bucket < 9) {
    // 20%: cancelamento (pedido aleatório — pode ser INCONSISTENT se não existe)
    const orderId = `order-D-${__VU}-${Math.floor(__ITER / 2)}`;
    const res = orderCancelled(accountId, sku, orderId, 1);
    check(res, { 'ORDER_CANCELLED sem 5xx': (r) => r.status < 500 });

  } else {
    // 10%: leitura de saldo
    const res = getStock(accountId, sku);
    check(res, { 'GET stock sem 5xx': (r) => r.status < 500 });
  }
}
