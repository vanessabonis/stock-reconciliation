# gubee-stock-reconciliation

A aplicação recebe eventos de estoque e pedidos vindos de marketplaces, mantém uma visão consistente do 
estoque disponível por vendedor e SKU (Stock Keeping Unit) ou Identificador único de um produto, 
evita duplicidade de processamento através de idempotência, trata eventos fora de ordem
e gera uma trilha auditável completa das alterações realizadas.
---
Inventory reconciliation service for marketplace integration. Receives stock and order events,
maintains a reliable stock balance per account/SKU, and provides a full audit trail.

See [DECISIONS.md](DECISIONS.md) for technical decisions, assumptions, and trade-offs.

## Technology Stack

| Layer | Technology |
|-------|------------|
| Language | Java 21 |
| Framework | Spring Boot 3.3 |
| Build | Maven |
| Database | PostgreSQL 16 |
| Migrations | Flyway |
| Architecture | Hexagonal (Ports & Adapters) |
| API Docs | OpenAPI 3 / Swagger UI |
| Tests | JUnit 5 + Testcontainers |
| Logging | Logback + Logstash JSON encoder (structured, with MDC correlation) |
| Metrics | Micrometer + Prometheus (`/actuator/prometheus`) |
| Tracing | Micrometer Tracing (OTel bridge) + Jaeger (OTLP HTTP) |
| Messaging | Spring Kafka (producer reliability config) |
| Load testing | k6 (Docker, no local install required) |
| Containers | Docker Compose |

## Running Locally

### Prerequisites
- Docker and Docker Compose
- Java 21 + Maven (for running tests only)

### Start with Docker Compose

```bash
docker compose up --build
```

The service will be available at `http://localhost:8080`.

Swagger UI: `http://localhost:8080/swagger-ui.html`

### Run just the database (for local development)

```bash
docker compose up postgres -d
mvn spring-boot:run
```

## Running the Tests

```bash
mvn test
```

Tests use Testcontainers with a real PostgreSQL instance. Docker must be running.

## Endpoints

### Ingestion paths

The service has **two ingestion paths** that both call the same `ProcessStockEventUseCase`:

| Path | When to use |
|------|-------------|
| **Kafka** — topic `stock-events` | Primary path in production; guarantees ordering per `accountId:sku` partition key |
| **`POST /events`** | Secondary path for local testing, Swagger UI, and administrative injection without a running Kafka producer |

Both paths are functionally equivalent. The REST endpoint exists so you can exercise every scenario with plain `curl` without publishing to Kafka first.

### Available endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/events` | Submit a stock or order event (local testing / admin) |
| `GET` | `/stocks/{accountId}/{sku}` | Current stock balance |
| `GET` | `/stocks/{accountId}/{sku}/history` | Full audit history |
| `GET` | `/events?status=PENDING` | Query events by status |

Event statuses in response: `PROCESSED`, `IGNORED` (200 OK), `PENDING` (202 Accepted), `INCONSISTENT` (422 Unprocessable Entity).

## Event Examples (curl)

### Scenario 1 — Initial stock adjustment

```bash
curl -s -X POST http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{
    "eventId": "evt-001",
    "type": "STOCK_ADJUSTED",
    "occurredAt": "2026-05-28T10:00:00Z",
    "accountId": "account-001",
    "sku": "ABC-123",
    "available": 10,
    "reason": "initial_count"
  }' | jq
```

Expected: `{"status":"PROCESSED"}`

```bash
curl -s http://localhost:8080/stocks/account-001/ABC-123 | jq
# {"accountId":"account-001","sku":"ABC-123","availableQuantity":10,...}
```

### Scenario 2 — Order reduces stock

```bash
curl -s -X POST http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{
    "eventId": "evt-002",
    "type": "ORDER_CREATED",
    "occurredAt": "2026-05-28T10:01:00Z",
    "marketplace": "MERCADO_LIVRE",
    "accountId": "account-001",
    "externalOrderId": "ML-123456",
    "sku": "ABC-123",
    "quantity": 2
  }' | jq
```

Expected: `{"status":"PROCESSED"}` — stock becomes 8.

### Scenario 3 — Cancellation restores stock

```bash
curl -s -X POST http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{
    "eventId": "evt-003",
    "type": "ORDER_CANCELLED",
    "occurredAt": "2026-05-28T10:02:00Z",
    "marketplace": "MERCADO_LIVRE",
    "accountId": "account-001",
    "externalOrderId": "ML-123456",
    "sku": "ABC-123",
    "quantity": 2
  }' | jq
```

Expected: `{"status":"PROCESSED"}` — stock returns to 10.

### Scenario 4 — Duplicate eventId (idempotency)

Send the exact same `evt-002` again. Expected: `{"status":"IGNORED"}`, stock unchanged at 8.

### Scenario 5 — Duplicate cancellation (logical duplicate)

Send another ORDER_CANCELLED for `ML-123456` with a new eventId `evt-099`.
Expected: `{"status":"INCONSISTENT"}`, stock unchanged.

### Scenario 6 — ORDER_CANCELLED before ORDER_CREATED (out-of-order)

```bash
# Step 1: adjust stock to 10
curl -s -X POST http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"evt-oo-1","type":"STOCK_ADJUSTED","occurredAt":"2026-05-28T10:00:00Z",
       "accountId":"acc-oo","sku":"OO-SKU","available":10,"reason":"test"}' | jq

# Step 2: ORDER_CANCELLED arrives first
curl -s -X POST http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"evt-oo-2","type":"ORDER_CANCELLED","occurredAt":"2026-05-28T10:05:00Z",
       "marketplace":"MERCADO_LIVRE","accountId":"acc-oo","externalOrderId":"ML-OO",
       "sku":"OO-SKU","quantity":2}' | jq
# Expected: {"status":"PENDING"} — stock unchanged at 10

# Step 3: ORDER_CREATED arrives late
curl -s -X POST http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"evt-oo-3","type":"ORDER_CREATED","occurredAt":"2026-05-28T10:04:00Z",
       "marketplace":"MERCADO_LIVRE","accountId":"acc-oo","externalOrderId":"ML-OO",
       "sku":"OO-SKU","quantity":2}' | jq
# Expected: {"status":"PROCESSED"} — subtract+add back, net=0, stock remains 10

curl -s http://localhost:8080/stocks/acc-oo/OO-SKU | jq
# {"availableQuantity":10,...}

# Check audit trail — 3 entries
curl -s http://localhost:8080/stocks/acc-oo/OO-SKU/history | jq
```

### Scenario 7 — Concurrent orders (concurrency)

```bash
for i in $(seq 1 10); do
  curl -s -X POST http://localhost:8080/events \
    -H 'Content-Type: application/json' \
    -d "{\"eventId\":\"evt-conc-$i\",\"type\":\"ORDER_CREATED\",
         \"occurredAt\":\"2026-05-28T10:00:0${i}Z\",\"marketplace\":\"MERCADO_LIVRE\",
         \"accountId\":\"acc-conc\",\"externalOrderId\":\"ML-CONC-$i\",
         \"sku\":\"CONC-SKU\",\"quantity\":1}" &
done
wait
curl -s http://localhost:8080/stocks/acc-conc/CONC-SKU | jq
# availableQuantity >= 0 always
```

### Scenario 8 — Marketplace restoration + cancellation (no double restoration)

```bash
# After ORDER_CREATED, marketplace restores first
curl -s -X POST http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"evt-mr-1","type":"MARKETPLACE_STOCK_RESTORED","occurredAt":"2026-05-28T10:10:00Z",
       "marketplace":"MERCADO_LIVRE","accountId":"account-001","externalOrderId":"ML-123456",
       "sku":"ABC-123","quantity":2}' | jq
# Expected: PROCESSED — stock restored

# ORDER_CANCELLED arrives after (double restoration attempt)
curl -s -X POST http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"evt-mr-2","type":"ORDER_CANCELLED","occurredAt":"2026-05-28T10:15:00Z",
       "marketplace":"MERCADO_LIVRE","accountId":"account-001","externalOrderId":"ML-123456",
       "sku":"ABC-123","quantity":2}' | jq
# Expected: INCONSISTENT — double restoration prevented
```

### View audit history

```bash
curl -s http://localhost:8080/stocks/account-001/ABC-123/history | jq
```

### View pending / inconsistent events

```bash
curl -s "http://localhost:8080/events?status=PENDING" | jq
curl -s "http://localhost:8080/events?status=INCONSISTENT" | jq
```

## Observability

### Metrics (Prometheus)
`GET /actuator/prometheus`

| Metric | Type | Description | Alert threshold |
|--------|------|-------------|-----------------|
| `gubee_events_processed_total` | Counter | Events by type + status | — |
| `gubee_events_failed_total` | Counter | Failures by eventType + reason | > 0 per minute |
| `gubee_events_pending_orderstate` | Gauge | ORDER_CANCELLED awaiting ORDER_CREATED | > 100 |
| `gubee_event_processing_duration_seconds` | Timer | Processing latency per event type | p95 > 500ms |
| `gubee_outbox_pending` | Gauge | Outbox entries awaiting relay | > 500 |
| `gubee_outbox_publish_latency_seconds` | Timer | Relay publish latency | p95 > 5s |
| `gubee_outbox_purged_total` | Counter | PUBLISHED outbox rows deleted by purge job | — |
| `gubee_dlq_events_total` | Counter | Events routed to DLQ (parse error or retries exhausted) | > 0 (page immediately) |
| `gubee_optimistic_lock_retries_total` | Counter | Optimistic lock collision retries | > 10/min sustained |
| `gubee_reconciliation_pending_backlog` | Gauge | PENDING events stale beyond warn threshold (10 min) | > 0 for > 2 windows |
| `gubee_reconciliation_actions_total` | Counter | Reconciliation job actions by type (warn / dlq) | dlq > 0 (page) |
| `gubee_reconciliation_pending_age_seconds` | Timer/Histogram | Age of stale PENDING events at detection | p95 rising = events accumulating |
| `kafka_consumer_records_lag` | Gauge | Consumer lag per partition | > 1000 for > 2min |

### Distributed Tracing
`http://localhost:16686` — Jaeger UI (starts automatically with `docker compose up`)

Every event generates a trace spanning:
- HTTP/Kafka ingestion → DB write → outbox creation → relay publish

The service sends spans via OTLP HTTP to Jaeger. The W3C `traceparent` header is
automatically propagated in Kafka messages, so the consumer's span is linked to the
producer's span — the full async chain is visible as a single trace in Jaeger.

Search by `traceId` (present in every structured log line) to reconstruct the complete
processing timeline of any event across async boundaries.

### Structured Logs
JSON format. Every log line includes:
`traceId`, `spanId`, `correlationId`, `eventId`, `accountId`, `sku`, `eventType`

### Health
`GET /actuator/health`

## Stress Testing

Four k6 scenarios cover the main operational concerns. No local k6 installation needed — runs entirely via Docker:

| Scenario | What it validates | Key invariant |
|----------|------------------|---------------|
| A — Concurrent same SKU | Optimistic locking under contention | `availableQuantity >= 0` always |
| B — Different SKUs | Pure throughput with no lock contention | p95 < 300ms, error rate < 1% |
| C — Idempotency flood | `ON CONFLICT DO NOTHING` under 20 concurrent duplicates | Only `PROCESSED` or `IGNORED` — never 5xx |
| D — Realistic load | Ramp to 1,000 events/sec with a realistic event mix | Reveals the actual production bottleneck sequence |

```bash
# Run all scenarios
docker run --rm -i --network host \
  -e BASE_URL=http://localhost:8080 \
  grafana/k6 run - < stress-test/k6-stress.js

# Run a single scenario
docker run --rm -i --network host \
  -e BASE_URL=http://localhost:8080 \
  grafana/k6 run - < stress-test/k6-stress.js \
  --env SCENARIO=concurrent_same_sku
```

See [stress-test/STRESS_GUIDE.md](stress-test/STRESS_GUIDE.md) for the full guide: smoke test, per-bottleneck diagnosis, and how to interpret k6 output.

## Known Limitations

- **STOCK_SYNC_SENT with no prior stock**: The event is recorded as IGNORED but no StockHistory
  entry is created (no stock record exists to attach it to). This is safe and expected.
- **Stale PENDING events**: If ORDER_CREATED never arrives for a pending ORDER_CANCELLED,
  `PendingReconciliationJob` warns after 10 minutes and escalates to the DLQ after 30 minutes.
  The event is never auto-resolved — manual intervention is required for DLQ entries.
- **No authentication/authorization**: The API is open. In production, service-to-service auth
  (JWT, mTLS) would be required.
- **Single database**: No read replica or CQRS read model. Under heavy read load, a separate
  projection would be beneficial.
- **In-process retry for optimistic locking**: Works correctly under a single-process load;
  under extreme write contention (flash sale) some callers may receive HTTP 409 after 3 retries.
  A message queue (Kafka) would absorb the burst.
