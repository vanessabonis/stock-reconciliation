# Guia de Stress Test — gubee-stock-reconciliation

Este guia permite executar os testes de carga do zero, sem nenhuma instalação além do Docker.

---

## Pré-requisitos

- Docker Desktop instalado e rodando
- Portas livres: `8080` (API), `5432` (PostgreSQL), `9092` (Kafka), `16686` (Jaeger UI), `4318` (OTLP), `2181` (Zookeeper)
- Mínimo **4 GB de RAM** alocados para Docker (vá em Docker Desktop → Settings → Resources)
- Nenhuma outra aplicação usando as portas acima

---

## Passo 1 — Subir toda a infraestrutura

```bash
docker compose up --build -d
```

Aguarde até a API estar pronta (máx. 60 segundos):

```bash
until curl -sf http://localhost:8080/actuator/health | grep -q '"status":"UP"'; do
  echo "Aguardando API..."
  sleep 3
done
echo "API pronta!"
```

Verificar o que subiu:

```bash
docker compose ps
```

Esperado: `api`, `postgres`, `kafka`, `zookeeper`, `jaeger` — todos com status `Up`.

> **Atenção:** se algum container não subiu, verifique com `docker compose logs <nome>`.
> Kafka pode demorar 20–30s depois do Zookeeper estar Up.

---

## Passo 2 — Verificar saúde antes dos testes

```bash
# Health geral da aplicação
curl -s http://localhost:8080/actuator/health | jq

# Listar métricas gubee_ disponíveis
curl -s http://localhost:8080/actuator/metrics | jq '.names[]' | grep gubee

# Ver métricas brutas no formato Prometheus
curl -s http://localhost:8080/actuator/prometheus | grep gubee
```

Esperado: `health` retornando `"status":"UP"` e métricas `gubee_` visíveis.

---

## Passo 3 — Smoke test antes do stress

Execute um teste funcional para garantir que a API está respondendo corretamente antes de carregar.

```bash
# Ajuste inicial de estoque
curl -s -X POST http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{
    "eventId": "smoke-001",
    "type": "STOCK_ADJUSTED",
    "occurredAt": "2026-05-28T10:00:00Z",
    "accountId": "smoke",
    "sku": "SMOKE-SKU",
    "available": 1000,
    "reason": "smoke_test"
  }' | jq

# Esperado: {"status":"PROCESSED"}

# Verificar saldo
curl -s http://localhost:8080/stocks/smoke/SMOKE-SKU | jq
# Esperado: "availableQuantity": 1000
```

**Só prossiga para o stress test se o smoke test retornar `PROCESSED` e saldo = 1000.**

Alternativamente, abra `teste-local.http` no IntelliJ e execute os cenários 1–8 na ordem.

---

## Passo 4 — Executar os stress tests

O k6 roda inteiramente via Docker — não é necessário instalar nada.

### Cenário A — Mesmo SKU (testa optimistic locking)

```bash
docker run --rm -i --network host \
  -e BASE_URL=http://localhost:8080 \
  grafana/k6 run - < stress-test/k6-stress.js \
  --env SCENARIO=concurrent_same_sku
```

**O que faz:** 50 threads concorrentes criando pedidos no mesmo `STRESS-SKU`.
**HTTP 409 é esperado** — significa que o retry do optimistic locking se esgotou. Não é falha.
**Invariante:** saldo ao final deve ser `>= 0`.

---

### Cenário B — SKUs diferentes (testa throughput puro)

```bash
docker run --rm -i --network host \
  -e BASE_URL=http://localhost:8080 \
  grafana/k6 run - < stress-test/k6-stress.js \
  --env SCENARIO=different_skus
```

**O que faz:** 100 VUs, cada um com SKU único. Zero contenção entre VUs.
**Threshold:** p95 < 300ms, taxa de erro < 1%.
**Se falhar:** bottleneck é connection pool ou I/O, não locking.

---

### Cenário C — Flood de idempotência

```bash
docker run --rm -i --network host \
  -e BASE_URL=http://localhost:8080 \
  grafana/k6 run - < stress-test/k6-stress.js \
  --env SCENARIO=idempotency_flood
```

**O que faz:** 20 VUs enviando o mesmo `eventId` por 30s.
**Invariante:** todas as respostas devem ser `PROCESSED` ou `IGNORED` — nunca 5xx.
**Saldo ao final:** exatamente 10 (apenas o primeiro apply conta).

---

### Cenário D — Carga realista (o mais importante)

```bash
docker run --rm -i --network host \
  -e BASE_URL=http://localhost:8080 \
  grafana/k6 run - < stress-test/k6-stress.js \
  --env SCENARIO=realistic_load
```

**O que faz:** rampa até 1.000 eventos/seg com mix realista de tipos.
**É o cenário que revela os gargalos reais em produção.**

---

### Rodar todos os cenários de uma vez

```bash
docker run --rm -i --network host \
  -e BASE_URL=http://localhost:8080 \
  grafana/k6 run - < stress-test/k6-stress.js
```

> Os cenários rodam em paralelo. O relatório ao final consolida todos.

---

## Passo 5 — O que observar durante o teste

Abra 4 terminais simultâneos enquanto o k6 está rodando:

```bash
# Terminal 1: eventos processados em tempo real (atualiza a cada 2s)
watch -n 2 'curl -s http://localhost:8080/actuator/prometheus \
  | grep -E "gubee_events_processed|gubee_events_failed|gubee_optimistic"'

# Terminal 2: consumer lag do Kafka por partition
watch -n 2 'curl -s http://localhost:8080/actuator/prometheus \
  | grep kafka_consumer_records_lag'

# Terminal 3: health do outbox relay
watch -n 2 'curl -s http://localhost:8080/actuator/health | jq .components.outboxHealthIndicator'

# Terminal 4: logs da aplicação (mostra retries, 409s, INCONSISTENT)
docker compose logs -f api
```

**Jaeger:** acesse `http://localhost:16686` e busque pelo serviço `gubee-stock-reconciliation`
para ver traces completos de ponta a ponta (ingestão → DB → outbox → relay).

---

## Passo 6 — Interpretar os resultados do k6

O k6 imprime um resumo ao final. O que cada número significa:

### Métricas de HTTP

| Métrica k6 | O que significa | Ação se estiver alto |
|-----------|-----------------|----------------------|
| `http_req_duration p95` | 95% das requests completaram em menos de X ms | Investigar connection pool e query plan |
| `http_req_failed` | Taxa de erros HTTP reais (5xx) | Verificar logs da aplicação |
| `http_reqs` | Total de requests por segundo (throughput) | Referência de capacidade atingida |
| `iterations` | Quantas vezes cada VU executou sua função | Usado para calcular throughput efetivo |

> **Importante:** HTTP 409 e 422 **não contam** como `http_req_failed` no k6 por padrão.
> Eles são respostas válidas da aplicação (lock esgotado e inconsistência de negócio).

### Métricas customizadas do script

| Métrica | O que conta | Alerta |
|---------|------------|--------|
| `optimistic_lock_retries` | Quantas vezes o serviço retornou 409 | Se > 10% das writes → contenção excessiva no mesmo SKU |
| `inconsistent_events` | Respostas com `status=INCONSISTENT` | Normal no Scenario D; 0 esperado no Scenario B |
| `pending_events` | Respostas com `status=PENDING` | Normal quando ORDER_CANCELLED chega antes do ORDER_CREATED |
| `write_duration_ms p95` | Latência das escritas de evento | p95 > 500ms sugere bottleneck no banco |

### Exemplo de saída saudável (Scenario B)

```
✓ ORDER_CREATED processado

checks.........................: 99.87% ✓ 29962   ✗ 38
http_req_duration.............: avg=45ms p(95)=210ms p(99)=380ms
http_req_failed...............: 0.12%  ✓ 36      ✗ 29962
http_reqs.....................: 31.2/s  (total: 31000)
optimistic_lock_retries.......: 0
inconsistent_events...........: 38
```

### Exemplo com gargalo identificado (Scenario A sob alta contenção)

```
✗ não é erro de servidor (5xx)

http_req_duration p(95)=890ms   ← acima do threshold de 500ms
optimistic_lock_retries......: 3.847  ← alto: muitos 409s
```

Nesse caso, o bottleneck é o lock contention no mesmo SKU — comportamento esperado e documentado
em DECISIONS.md seção 15.1 (single-partition processing para o mesmo SKU).

---

## Passo 7 — Bottleneck: identificar e interpretar

### Gargalo 1: Optimistic locking (mesmo SKU, alta concorrência)

**Sintoma:** `optimistic_lock_retries` alto, muitos HTTP 409 no Scenario A.

**Diagnóstico:**
```bash
curl -s http://localhost:8080/actuator/prometheus | grep gubee_optimistic_lock_retries_total
```

**Significado:** 50 threads competindo pelo mesmo `version` em `stocks`. Cada retry
adiciona 50ms–100ms de latência. Documentado como comportamento esperado em DECISIONS.md §6.

**Solução em produção:** usar Kafka com partition key `accountId:sku` — eventos do mesmo SKU
chegam sequencialmente na mesma partition, eliminando o lock contention (já implementado via Kafka consumer).

---

### Gargalo 2: Consumer lag crescente (Kafka)

**Sintoma:** `kafka_consumer_records_lag` subindo durante o Scenario D.

**Diagnóstico:**
```bash
watch -n 1 'curl -s http://localhost:8080/actuator/prometheus | grep kafka_consumer_records_lag'
```

**Significado:** o consumer está processando mais lentamente do que os eventos chegam.
Ceiling de 1 partition = ~100 eventos/seg por thread (10ms/evento × 1 thread).

**Solução em produção:** aumentar o número de partitions. Cada partition adicional permite
uma instância adicional do serviço processando em paralelo.

---

### Gargalo 3: Outbox relay não acompanha

**Sintoma:** `gubee_outbox_pending` cresce e health retorna `DOWN`.

**Diagnóstico:**
```bash
curl -s http://localhost:8080/actuator/health | jq .components.outboxHealthIndicator
```

**Significado:** a fila de publicação para o Kafka está acumulando.
O relay roda a cada 500ms com batch de 50 — máximo de ~100 publishes/seg.

**Solução em produção:** deployar múltiplas instâncias do relay.
O `SELECT FOR UPDATE SKIP LOCKED` garante que não haverá publicação duplicada.

---

### Gargalo 4: Connection pool esgotado

**Sintoma:** latência alta no Scenario B (SKUs diferentes) — sem lock contention, mas lento.

**Diagnóstico:**
```bash
curl -s http://localhost:8080/actuator/prometheus | grep hikaricp_connections
```

**Significado:** HikariCP com `maximum-pool-size: 10`. A 1.000 eventos/seg × 10ms/query,
você precisa de ~10 conexões simultâneas — exatamente no limite.

**Solução em produção:** aumentar `maximumPoolSize` para 100 ou adicionar PgBouncer
como connection pooler (elimina o teto completamente).

---

## Passo 8 — Limpeza após os testes

```bash
# Para todos os containers e remove volumes (limpa o banco)
docker compose down -v

# Para sem limpar dados (para reiniciar a infraestrutura)
docker compose down
```

> Use `down -v` se quiser começar do zero para uma nova rodada de testes.
> Use `down` se quiser inspecionar os dados que ficaram no banco após o stress.

---

## Referência rápida — comandos mais usados

```bash
# Subir tudo
docker compose up --build -d

# Aguardar API
until curl -sf http://localhost:8080/actuator/health | grep -q '"status":"UP"'; do sleep 3; done

# Rodar cenário específico
docker run --rm -i --network host -e BASE_URL=http://localhost:8080 \
  grafana/k6 run - < stress-test/k6-stress.js --env SCENARIO=concurrent_same_sku

# Ver métricas em tempo real
watch -n 2 'curl -s http://localhost:8080/actuator/prometheus | grep gubee'

# Ver logs
docker compose logs -f api

# Limpar tudo
docker compose down -v
```

---

## Mapeamento: cenário → decisão de design

| Cenário | O que valida | Decisão documentada em |
|---------|-------------|------------------------|
| A — Mesmo SKU | Optimistic locking não deixa saldo negativo | DECISIONS.md §6 e §16.1 |
| B — SKUs diferentes | Throughput horizontal com partitioning | DECISIONS.md §13 e §16.2 |
| C — Flood idempotência | ON CONFLICT DO NOTHING absorve duplicatas | DECISIONS.md §4 |
| D — Carga realista | Bottleneck sequence real em produção | DECISIONS.md §16.3 |
