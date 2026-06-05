# DECISIONS.md — gubee-stock-reconciliation

## 1. Interpretação do Problema

O serviço gerencia um livro-razão de estoque para vendedores em marketplaces. Múltiplos sistemas
independentes (vendedores, Gubee, marketplaces) podem emitir eventos que afetam o mesmo estoque.
O desafio é garantir que o saldo corrente esteja sempre correto apesar de duplicatas, entrega fora
de ordem e escritas concorrentes.

A percepção central: estoque é um **recurso mutável compartilhado** e os problemas de corretude
mais difíceis ocorrem na fronteira de escrita, não no lado de leitura.

---

## 2. Resolução de Ambiguidades

### 2.1 `STOCK_ADJUSTED.available` é absoluto ou um delta?

**Decisão: absoluto (setar para N).**

Justificativa:
- O campo se chama `available`, não `delta` ou `adjustment`.
- O evento representa a visão autoritativa do estoque atual ("tenho 10 unidades"), não uma
  variação ("adicionar 3 unidades").
- Em integrações com marketplaces, ajustes manuais vêm de um vendedor que contou o estoque
  físico e registrou o que viu — ele define um valor, não calcula um delta.
- Tratar como delta sob concorrência amplificaria erros (dois deltas +5 simultâneos podem
  representar a mesma recontagem); tratar como absoluto significa que o último a escrever
  vence, o que é correto para um modelo de inventário baseado em contagem.

### 2.2 A chave do estoque é `(accountId + sku)` ou `(accountId + sku + marketplace)`?

**Decisão: `(accountId + sku)` — um saldo global por SKU do vendedor.**

Justificativa:
- `STOCK_ADJUSTED` não tem campo de marketplace. Se o estoque fosse por marketplace, seria
  impossível aplicar esse evento.
- `STOCK_SYNC_SENT` representa enviar o valor atual do estoque PARA um marketplace, não criar
  um pool separado. Você envia um número; o marketplace o reflete.
- O inventário físico é indivisível: 10 unidades de ABC-123 para account-001 existem uma
  única vez, independente de quantos marketplaces as listem.
- Marketplace é uma dimensão do PEDIDO (onde a venda ocorreu), não do ESTOQUE.
- Trade-off: não é possível rastrear reservas por marketplace diretamente no nível do estoque;
  isso é aceitável porque as alocações por marketplace são tratadas via `OrderState`.

### 2.3 O que `STOCK_SYNC_SENT` significa para o saldo interno?

**Decisão: `STOCK_SYNC_SENT` é apenas um registro de auditoria. Não altera o saldo.**

Justificativa:
- Enviar uma quantidade a um marketplace é uma notificação, não uma retirada. Os itens ainda
  estão fisicamente disponíveis; o marketplace apenas fica ciente deles.
- Se `STOCK_SYNC_SENT` reduzisse o saldo, vender o mesmo SKU em dois marketplaces reduziria
  o estoque duas vezes mesmo sem nenhuma venda ter ocorrido.
- O evento é registrado em `StockHistory` com delta=0 para que a linha do tempo mostre que
  a sincronização aconteceu.

### 2.4 `ORDER_CANCELLED` chegando antes de `ORDER_CREATED`

**Decisão: estado PENDING — não alterar o saldo; resolver atomicamente quando `ORDER_CREATED` chegar.**

Comportamento exato:
1. `ORDER_CANCELLED` chega, sem `OrderState` existente →
   - Criar `OrderState(state=PENDING)`
   - `ProcessedEvent.status = PENDING` (não `PROCESSED`)
   - Saldo do estoque inalterado
2. `ORDER_CREATED` chega depois, encontra `OrderState(state=PENDING)` →
   - Dentro de uma única transação:
     a. Subtrair quantidade do estoque (efeito do `ORDER_CREATED`)
     b. Devolver quantidade (efeito do `ORDER_CANCELLED`)
     c. Transicionar `OrderState` → `CANCELLED`
     d. Atualizar o `ProcessedEvent` pendente para `PROCESSED`
     e. Registrar `ProcessedEvent` do `ORDER_CREATED` como `PROCESSED`
   - Variação líquida no estoque = 0; audit trail mostra ambas as transições

Alternativas rejeitadas:
- Ignorar o `ORDER_CANCELLED`: perde o evento de negócio completamente; o `ORDER_CREATED`
  subtrairia o estoque depois sem nenhuma restauração eventual.
- Creditar o estoque imediatamente no `ORDER_CANCELLED`: infla o estoque antes de o pedido
  ser confirmado; cria um problema de correção posterior.

### 2.5 `MARKETPLACE_STOCK_RESTORED` vs `ORDER_CANCELLED` — qual prevalece?

**Decisão: a primeira restauração vence; a segunda é marcada como `INCONSISTENT`.**

O invariante é: exatamente uma restauração pode ser aplicada por pedido.

- Se `ORDER_CANCELLED` chega primeiro → estoque restaurado, `OrderState = CANCELLED` →
  `MARKETPLACE_STOCK_RESTORED` chega → `OrderState` já é `CANCELLED` → `INCONSISTENT`
- Se `MARKETPLACE_STOCK_RESTORED` chega primeiro → estoque restaurado, `OrderState = RESTORED` →
  `ORDER_CANCELLED` chega → `OrderState` já é `RESTORED` → `INCONSISTENT`

Ambos os caminhos produzem exatamente um crédito no estoque. O evento `INCONSISTENT` é
registrado com um motivo no `ProcessedEvent` para que operadores possam investigar.

---

## 3. Fonte da Verdade

**A tabela interna `stocks` é a única fonte da verdade para o saldo disponível atual.**

Eventos são entradas. O saldo é a saída. Nenhum evento é confiado cegamente — cada um é
validado contra o estado atual (ciclo de vida do pedido, saldo existente) antes de ser aplicado.

Hierarquia de confiança:
1. `STOCK_ADJUSTED` — maior autoridade; define o saldo diretamente
2. `ORDER_CREATED` / `ORDER_CANCELLED` — alterações transacionais, validadas via `OrderState`
3. `MARKETPLACE_STOCK_RESTORED` — sinal do marketplace, validado contra `OrderState`
4. `STOCK_SYNC_SENT` — apenas notificação; sem alteração de saldo

---

## 4. Idempotência

**Estratégia: unique constraint em `processed_events.event_id` + tratamento de `DataIntegrityViolationException`.**

O padrão SELECT-then-INSERT tem uma race condition TOCTOU sob requisições concorrentes: duas
threads podem fazer SELECT, não encontrar nenhuma linha, e ambas fazerem INSERT. A abordagem
de unique constraint é atômica no nível do banco.

Ao receber um `eventId` duplicado:
- O INSERT em `processed_events` falha com violação de unique constraint.
- A aplicação captura `DataIntegrityViolationException`, faz rollback e retorna
  `EventStatus.IGNORED` — sem query secundária ao banco, sem lógica de negócio executada duas vezes.
- A resposta HTTP é 200 OK com status=IGNORED.

---

## 5. Duplicatas Lógicas (mesma operação de negócio, `eventId`s diferentes)

Duplicatas lógicas (por exemplo, dois eventos `ORDER_CANCELLED` para o mesmo
marketplace/accountId/externalOrderId/sku) são tratadas pela state machine do `OrderState`.

- O primeiro `ORDER_CANCELLED` transiciona `OrderState` para `CANCELLED` e restaura o estoque.
- O segundo `ORDER_CANCELLED` encontra `OrderState = CANCELLED`, que é uma transição inválida →
  marcado como `INCONSISTENT` com o motivo "pedido já cancelado".

Esse é o comportamento correto de negócio, independente dos `eventId`s.

---

## 6. Concorrência

**Estratégia: optimistic locking em `stocks.version` + até 3 tentativas com exponential backoff.**

O campo `@Version` na entidade JPA de Stock faz o Spring Data JPA emitir
`UPDATE stocks ... WHERE version = ?`. Se duas threads carregam a mesma versão e ambas tentam
salvar, uma terá sucesso e a outra receberá `OptimisticLockingFailureException`.

Política de retry (aplicada no use case da aplicação):
- Tentativa 1: imediata
- Tentativa 2: aguardar 50ms
- Tentativa 3: aguardar 100ms
- Após 3 falhas: retornar HTTP 409 Conflict com um código de erro legível por máquina

Em deploy com duas instâncias: a corretude é garantida pela verificação de versão no banco.
Ambas as instâncias falam com o mesmo PostgreSQL; a constraint de versão é aplicada pelo banco,
não pelo estado em memória do processo. Sem race condition remanescente.

Para `STOCK_ADJUSTED` (valor absoluto): dois eventos `STOCK_ADJUSTED` simultâneos são tratados
pelo optimistic locking. Um terá sucesso; o outro fará retry, lerá a nova versão e sobrescreverá
com seu próprio valor absoluto. O último a escrever vence — que é a semântica correta para uma
recontagem manual absoluta.

---

## 7. State Machine do Ciclo de Vida do Pedido

```
         ORDER_CANCELLED (antecipado)
  [none] ────────────────────────────► PENDING
    │                                      │
    │ ORDER_CREATED                         │ ORDER_CREATED chega
    ▼                                      ▼
  CREATED ──────────────────────────► CANCELLED
    │         ORDER_CANCELLED               (atômico: subtrai + devolve)
    │
    │ MARKETPLACE_STOCK_RESTORED
    ▼
  RESTORED
```

Transições inválidas → evento `INCONSISTENT`, sem alteração de estoque:
- `ORDER_CREATED` quando estado é `CREATED`, `CANCELLED` ou `RESTORED`
- `ORDER_CANCELLED` quando estado é `CANCELLED` ou `RESTORED`
- `MARKETPLACE_STOCK_RESTORED` quando estado é `CANCELLED` ou `RESTORED`
- `MARKETPLACE_STOCK_RESTORED` quando não existe `OrderState`

---

## 8. Semântica do `EventStatus`

| Status         | Significado                                                                      |
|----------------|----------------------------------------------------------------------------------|
| `PROCESSED`    | Evento totalmente aplicado; estoque e/ou estado atualizado                       |
| `IGNORED`      | Evento recebido, mas sem ação tomada (`STOCK_SYNC_SENT` ou `eventId` duplicado)  |
| `PENDING`      | Evento recebido, aguardando um evento pré-requisito                              |
| `INCONSISTENT` | Evento conflita com o estado atual; registrado mas não aplicado                  |

---

## 9. Decisões de Arquitetura

### Arquitetura Hexagonal
O pacote de domínio tem zero dependências de Spring, JPA ou qualquer framework. Anotações de
framework existem apenas nos pacotes de adapter. Isso significa que a lógica de domínio pode
ser testada em unitário sem subir um contexto Spring.

### Stock como Aggregate Root
`Stock` possui seus invariantes. A única forma de alterar o saldo é através de `Stock.apply(event)`.
Esse método:
- Valida a mudança solicitada (sem saldo negativo)
- Retorna uma entrada de `StockHistory` (criada pelo próprio `Stock`, não por nenhum serviço)
- Atualiza o estado interno

Código externo não consegue definir `availableQuantity` diretamente. Não existem setters
públicos em `Stock` para esse campo.

### `Quantity` como Value Object
`Quantity` encapsula um `int` e rejeita valores negativos no momento da construção. Isso torna
impossível criar uma quantidade inválida em qualquer ponto do código, eliminando toda uma classe
de bugs. Toda quantidade de negócio no sistema é um `Quantity`, nunca um `int` primitivo.

### Value Objects de ID tipados
`EventId`, `AccountId`, `Sku` são wrappers tipados. Isso previne bugs de inversão de argumentos
(passar `sku` onde se espera `accountId`) em tempo de compilação.

### PostgreSQL
Escolhido por:
- Transações ACID (essenciais para optimistic locking e idempotência)
- Semântica de locking por linha
- Suporte nativo a constraints `UNIQUE` como guardas de idempotência
- Suporte maduro ao Flyway

### Flyway
Versionamento de schema é explícito e reproduzível. Sem `spring.jpa.hibernate.ddl-auto=create`.

---

## 10. Trade-offs e Simplificações

| Área | Decisão | Trade-off |
|------|---------|-----------|
| `STOCK_ADJUSTED` absoluto | Último a escrever vence | Correto para recontagens manuais; pode perder um ajuste concorrente |
| Retry em processo | Exponential backoff simples | Sob contenção muito alta, 409s são possíveis; Kafka+outbox elimina isso |
| REST síncrono | Simples, amigável para demo | Em produção: event streaming (Kafka) é melhor para throughput |
| Eventos `PENDING` como linhas no banco | Consultável, auditável | Exige um job em background para detectar eventos `PENDING` obsoletos (não implementado) |
| Sem modelo de leitura CQRS | Banco único para leituras e escritas | Sob carga pesada de leitura, uma read replica dedicada ou cache seria necessário |

---

## 11. O Que Faria Diferente em Produção

1. **Consumers Kafka** em vez de ingestão por REST — entrega at-least-once com offsets de
   consumer group; a unique constraint em `event_id` continua garantindo idempotência.
2. **Transactional Outbox** — em vez de escritas diretas no banco por evento, escrever eventos
   em uma tabela de outbox e publicar para sistemas downstream de forma atômica.
3. **Modelo de leitura CQRS** — uma projeção separada e desnormalizada para consultas de estoque,
   atualizada assincronamente; elimina contenção entre leituras e escritas.
4. **Job de reconciliação de `PENDING`** — identificar periodicamente eventos `PENDING` mais
   antigos que N minutos e alertar ou tentar reentrega do pré-requisito ausente.
5. **Distributed tracing** (OpenTelemetry) — substituir correlation IDs via MDC por W3C trace
   context para visibilidade entre serviços.
6. **Schema registry** para schemas de eventos — garante compatibilidade retroativa entre
   produtores.
7. **Dead letter queue** para eventos que falham após todos os retries — evita perda silenciosa
   de dados.

---

## 12. As 5 Principais Decisões de Design para a Entrevista Técnica

### 12.1 Granularidade da chave do estoque: `(accountId, sku)` vs `(accountId, sku, marketplace)`

Escolhi `(accountId, sku)` porque `STOCK_ADJUSTED` não tem campo de marketplace, tornando
pools de estoque por marketplace tecnicamente impossíveis sem ambiguidade. Mais importante,
o inventário físico é indivisível — você tem 10 unidades, ponto. O marketplace é onde a venda
aconteceu, não um pool separado de estoque. `OrderState` carrega a dimensão do marketplace para
rastreamento de pedidos sem fragmentar o saldo de inventário.

### 12.2 Absoluto vs delta para `STOCK_ADJUSTED`

Semântica absoluta é mais segura sob concorrência: dois eventos simultâneos "tenho 10" produzem
um resultado determinístico onde o último a escrever vence. Dois deltas "+5" simultâneos sobre
uma base de 10 produziriam 20, mesmo que ambos representassem a mesma recontagem física — uma
inflação fantasma. O absoluto torna a operação idempotente por natureza.

### 12.3 Estado PENDING para `ORDER_CANCELLED` fora de ordem

Em vez de ignorar cancelamentos antecipados ou creditar estoque especulativamente, mantenho o
evento em estado PENDING. Quando `ORDER_CREATED` chega, ambas as transições são aplicadas
atomicamente dentro de uma única transação: subtrai (`ORDER_CREATED`) e depois devolve
(`ORDER_CANCELLED`), net=0. Isso preserva a corretude, produz um audit trail limpo e evita
qualquer janela onde o estoque está incorreto.

### 12.4 Unique constraint para idempotência (não SELECT+INSERT)

SELECT-then-INSERT tem uma race condition TOCTOU: sob requisições concorrentes, duas threads
podem ambas ler "não encontrado" e ambas inserir. Uma unique constraint no nível do banco é
atômica — exatamente um insert vence, o outro recebe violação de constraint. A camada de
aplicação captura `DataIntegrityViolationException` e retorna `IGNORED`, tornando a operação
segura sob qualquer nível de concorrência.

### 12.5 Optimistic locking + retry vs pessimistic locking

Optimistic locking permite leituras concorrentes sem bloqueio, o que é importante para um
sistema de estoque com muitas leituras. Pessimistic locking (`SELECT FOR UPDATE`) serializaria
todas as escritas para o mesmo SKU, degradando o throughput. A política de retry (3 tentativas,
exponential backoff) trata a taxa realista de colisão. Sob contenção patológica (por exemplo,
uma promoção flash com centenas de `ORDER_CREATED` simultâneos), HTTP 409 sinaliza o problema
para que o caller possa enfileirar e retentar — que é o sinal correto de comportamento para um
recurso com limite de taxa.

---

## 13. Decisões de Arquitetura Kafka

### Chave de mensagem: `accountId:sku`

Todos os eventos para o mesmo estoque são publicados com a chave `accountId:sku`. O Kafka usa
a chave para determinar a partition: `hash(key) % numPartitions`. Como a chave é a mesma para
todos os eventos do mesmo SKU, eles sempre vão para a mesma partition. Dentro de uma partition,
o Kafka garante ordenação — ORDER_CREATED e ORDER_CANCELLED para o mesmo SKU nunca serão
processados por consumers diferentes em ordem invertida. Sem essa garantia, um ORDER_CANCELLED
poderia ser processado antes do ORDER_CREATED mesmo em produção com múltiplas instâncias.

### Transactional Outbox vs publicação direta no Kafka

Publicação direta no Kafka dentro do handler de evento tem um problema fundamental: se o banco
commita mas o Kafka publish falha, o estoque foi atualizado mas os sistemas downstream nunca
saberão. A janela de inconsistência pode durar horas. O Outbox elimina esse problema.

O relay lê da mesma base de dados que guarda o saldo de estoque. Se a transação de negócio
commita, a linha do outbox existe e o relay PUBLICARÁ eventualmente. Se a transação faz rollback,
a linha do outbox não existe e nada é publicado. Garantia at-least-once sem janela de
inconsistência.

### at-least-once delivery + unique constraint = efeito exactly-once

Kafka garante at-least-once: uma mensagem pode ser entregue mais de uma vez (rebalanceamento,
consumer crash antes de commitar o offset). A unique constraint em `processed_events.event_id`
garante que o efeito de negócio acontece exatamente uma vez mesmo que a mensagem seja entregue
duas vezes. O segundo processamento recebe `DataIntegrityViolationException` e retorna `IGNORED`.
A combinação elimina a necessidade de exactly-once ao nível do Kafka (que tem overhead significativo
de ~200–400ms por batch com transactional producers).

### `SELECT FOR UPDATE SKIP LOCKED` no outbox relay

Sob múltiplas instâncias deployadas, cada relay concorreria pelas mesmas linhas PENDING.
`SELECT FOR UPDATE` sem `SKIP LOCKED` faria as instâncias esperarem umas pelas outras (deadlock
potencial ou serialização total). `SKIP LOCKED` faz cada instância pular linhas já bloqueadas
e pegar apenas linhas disponíveis — sem espera, sem duplicata, processamento paralelo eficiente.
Resultado: cada linha do outbox é processada por exatamente uma instância do relay.

### Por que não SQS ou GCP Pub/Sub?

Para este desafio: Kafka é open-source e roda no Docker, tornando o demo autossuficiente. Em
um ambiente AWS real, Amazon MSK (Managed Kafka) ou SQS reduziria overhead operacional. SQS
tem semântica mais simples (sem partitions, sem consumer groups) mas perde garantias de
ordenação por chave. GCP Pub/Sub tem trade-offs similares. A escolha do Kafka aqui demonstra
os conceitos subjacentes; migrar para um serviço gerenciado seria uma decisão de deployment,
não uma decisão arquitetural.

### Kubernetes e escala horizontal

Cada pod roda um consumer no consumer group. O Kafka rebalanceia as partitions automaticamente
quando pods sobem ou descem. Com 3 partitions, o teto natural é 3 pods processando em paralelo
com garantias de ordenação completas. Além de 3 instâncias, aumentar a contagem de partitions
(decisão de criação do tópico — não pode ser reduzida retroativamente). Escala horizontal sem
nenhuma alteração de código na aplicação.

### Schema Registry (não implementado — evolução para produção)

Em produção, schemas de eventos seriam registrados no Confluent Schema Registry usando Avro ou
Protobuf. Isso garante compatibilidade retroativa — produtores não conseguem publicar uma
mudança de schema quebradora sem aprovação explícita do registry. Não implementado aqui para
manter o demo sem dependências adicionais. Caminho de migração: campo `version` em cada payload
+ handlers por versão no consumer.

### Debezium como alternativa ao outbox por polling (não implementado)

Debezium usa o WAL (Write-Ahead Log) do PostgreSQL para detectar mudanças na tabela outbox via
CDC (Change Data Capture) e publicá-las no Kafka. Isso elimina o delay de polling (atualmente
500ms) e reduz a carga no banco. Operacionalmente mais pesado — requer um deployment separado
do Debezium connector. Trade-off: latência menor vs complexidade operacional maior.

### Política de retenção do outbox

Entradas `PUBLISHED` são purgadas após 30 dias por `OutboxPurgeService`, que roda diariamente
às 02:00 via `@Scheduled(cron = "0 0 2 * * *")`. Isso evita crescimento ilimitado da tabela
enquanto retém histórico suficiente para investigação de incidentes.

Entradas `FAILED` **nunca são purgadas automaticamente** — representam falhas operacionais que
exigem investigação humana. Em produção, um processo separado moveria entradas `FAILED` para um
dead letter store (S3, tabela dedicada) antes do arquivamento.

O índice parcial `idx_outbox_pending` cobre apenas linhas `PENDING`, mantendo as queries do
relay rápidas independente do acúmulo de `PUBLISHED`. Um segundo índice parcial
`idx_outbox_published_cleanup` suporta o DELETE da purge sem full table scan.

Evolução para produção: particionamento da tabela por `created_at` (partições mensais)
permitiria `DROP PARTITION` instantâneo em vez de DELETE linha a linha, eliminando o overhead
de VACUUM completamente.

---

## 14. Decisões de Observabilidade

### Logs estruturados em JSON
Todos os logs são emitidos como JSON via `logstash-logback-encoder` em ambientes não-locais
(perfis diferentes de `local`). Cada evento de log carrega o contexto MDC completo:
`correlationId`, `eventId`, `accountId`, `sku`, `eventType`, `kafkaPartition`, `kafkaOffset`.
Isso permite reconstruir a linha do tempo completa de processamento de qualquer evento com um
único `grep` pelo `correlationId`, sem necessidade de infraestrutura de distributed tracing.

### Correlation IDs vs distributed tracing
MDC correlation IDs são suficientes para um serviço único. Em produção com múltiplos serviços,
substituir pelo W3C trace context do OpenTelemetry — o `correlationId` passa a ser o trace ID
propagado via headers HTTP e headers de mensagens Kafka entre serviços.

### Filosofia de design das métricas
Cada métrica responde a uma pergunta operacional específica:
- `gubee.events.processed` → o serviço está processando eventos?
- `gubee.event.processing.duration` → o processamento está ficando mais lento?
- `gubee.outbox.pending` → o relay está acompanhando o volume?
- `gubee.dlq.events` → estamos perdendo mensagens? (deve ser sempre zero)
- `gubee.optimistic.lock.retries` → há contenção de escrita em algum SKU?
- `gubee.insufficient.stock` → vendedores estão fazendo overselling?

O counter DLQ é um alerta crítico em qualquer valor acima de zero.
O gauge de pending do outbox é o sinal primário de backpressure.

### Por que Prometheus em vez de métricas push
O Prometheus faz scrape de `/actuator/prometheus` no próprio ciclo. O serviço não tem
dependência da infraestrutura de métricas — se o Prometheus estiver indisponível, o serviço
continua funcionando e as métricas são coletadas quando ele se recupera.

### O incidente de offset commit — causa raiz e correção

A causa raiz do bug de race condition com offset commit é o acoplamento do ciclo de vida do
consumer (commit do offset) a uma operação downstream assíncrona (publish no Kafka). A correção
tem duas formas:

1. **Send síncrono**: `kafkaTemplate.send().get()` — simples mas aumenta a latência do consumer
2. **Outbox Pattern**: desacopla consumer do publish completamente — o consumer só escreve no
   banco, um relay separado publica de forma síncrona

Esta implementação usa a abordagem 2. O consumer commita seu offset somente após o commit da
transação DB. O relay envia de forma síncrona com timeout de 5 segundos. Nenhum dos caminhos
tem uma janela onde dados podem ser perdidos silenciosamente.

---

## 15. Análise de Volume e Comportamento de Escala

### 15.1 Para o MESMO SKU

**100 eventos/seg:**
- Estratégia de partition: todos os eventos de um SKU são roteados para a mesma partition Kafka
  (key = `accountId:sku`). Uma thread de consumer processa ~100 eventos/seg.
- Taxa de colisão de optimistic locking: desprezível. Cada evento leva ~10ms de round trip no
  banco → 100/seg está dentro da capacidade confortável de thread única.
- 3 retries são suficientes para a janela de colisão realista.
- **Veredicto:** totalmente suportado, sem necessidade de alterações de configuração.

**1.000 eventos/seg:**
- Limite de partition única: uma partition = uma thread de consumer. A thread processa
  ~10ms/evento = máximo ~100 eventos/seg de forma síncrona. A 1.000/seg, o consumer lag começa
  a crescer.
- Optimistic locking: taxa de colisão aumenta proporcionalmente. ~50% das escritas precisarão
  de pelo menos um retry. O counter `gubee.optimistic_lock_retries_total` fica visível nas
  métricas.
- Gargalo principal: processamento single-threaded por partition, não o banco.
- Solução nessa escala: batch processing — acumular N eventos para o mesmo SKU dentro de um
  poll(), computar o efeito líquido, aplicar UMA escrita no banco. Reduz escritas N× enquanto
  preserva o rastreamento por `OrderState` por evento.
- **Veredicto:** arquitetura precisa de otimização de batch para SKUs quentes nessa escala.

**10.000 eventos/seg:**
- Partition única é o teto absoluto. Nenhuma escala horizontal (mais instâncias) ajuda —
  apenas um consumer lê uma partition.
- A partition key (`accountId:sku`) que garante ordenação é também o gargalo para SKUs quentes.
  Esse é um trade-off fundamental no ordenamento baseado em partition do Kafka.
- Soluções:
  a) Aumentar partitions para o tópico quente (perde garantia de ordenação estrita por SKU)
  b) Rotear SKUs quentes para um tópico dedicado de alto throughput com mais partitions
  c) Migrar para um store otimizado para escrita (Redis com scripts Lua para operações atômicas)
     com sincronização assíncrona com PostgreSQL para durabilidade
- Na prática, 10.000 `ORDER_CREATED`/seg para o mesmo SKU = 10.000 compradores simultâneos de
  um produto. A maioria das plataformas de marketplace lida com isso via padrões de reserva
  (soft-lock) em vez de atualizações de estoque em tempo real.
- **Veredicto:** arquitetura atual atinge seu limite. Limitação honesta documentada aqui.

### 15.2 Para SKUs DIFERENTES

**100 eventos/seg (entre N SKUs):**
- 3 partitions × 3 instâncias de consumer = ~33 eventos/seg por partition em média.
- Zero contenção entre SKUs diferentes.
- Pool de conexões (HikariCP: `maximum-pool-size: 10`) — a 10ms/query:
  10 conexões × 100 queries/seg = headroom confortável.
- **Veredicto:** trivialmente suportado.

**1.000 eventos/seg (entre N SKUs):**
- 3 partitions: ~333/seg por partition. Consumer processa confortavelmente.
- Conexões de banco: 1.000 eventos/seg × 10ms = ~10 conexões simultâneas. HikariCP suporta.
- Outbox relay: polling a cada 500ms + batch de 50 = até 100 publicações/seg.
  A 1.000/seg, o backlog do outbox cresce. Correção: reduzir intervalo do relay para 100ms ou
  aumentar tamanho do batch para 500. O gauge `gubee.outbox.pending` sinaliza isso antes de
  se tornar crítico.
- **Veredicto:** aumentar throughput do relay, fora isso sólido.

**10.000 eventos/seg (entre N SKUs):**
- Necessário: mínimo 10 partitions + 10 instâncias de consumer.
- Pool de conexões: 10.000/seg × 10ms = 100 conexões simultâneas necessárias.
  HikariCP padrão (10) é insuficiente. Definir `maximumPoolSize=100`.
  PgBouncer como connection pooler elimina esse teto completamente.
- Outbox relay: deployar 5 instâncias de relay com `SELECT ... FOR UPDATE SKIP LOCKED`
  (impede que instâncias de relay peguem as mesmas linhas). Cada uma processa ~2.000/seg.
- Throughput de escrita do PostgreSQL: 10.000 escritas/seg se aproxima do limite de uma
  instância única em hardware comum (~8.000–15.000 escritas/seg). Solução: read replica para
  consultas de estoque, primário otimizado para escrita.
- **Veredicto:** horizontalmente escalável com mudanças de configuração, sem alterações de código.
  `SKIP LOCKED` no outbox é o habilitador-chave para relay multi-instância.

### 15.3 A sequência de gargalos (em ordem de aparecimento)

| # | Gargalo | Aparece em |
|---|---------|------------|
| 1 | Processamento single-partition (mesmo SKU) | > 100 eventos/seg por SKU |
| 2 | Esgotamento do pool de conexões (SKUs diferentes, sem PgBouncer) | > 5.000 eventos/seg total |
| 3 | Throughput do outbox relay (sem escala do relay) | > 1.000 eventos/seg total |
| 4 | Teto de throughput de escrita do PostgreSQL | ~10.000–15.000 escritas/seg por instância |
| 5 | Contagem de partitions Kafka (fixada na criação do tópico) | planejar com antecedência — não pode ser reduzida |

### 15.4 Estratégia de evolução de schema

Implementação atual: JSON sem validação de schema.
Risco: um produtor pode adicionar/remover campos sem aviso, quebrando consumers silenciosamente.

Estratégia para produção:
- **Opção A — Confluent Schema Registry com Avro:** ID do schema embutido no header da mensagem.
  O Registry aplica regras de compatibilidade retroativa e prospectiva. Consumers recebem o schema
  junto com o payload — sem quebras surpresa. Melhor para equipes com múltiplos produtores.
- **Opção B — campo `version` em todo payload:** Consumer verifica a versão, roteia para o handler
  específico da versão. Handlers antigos permanecem ativos até todos os produtores migrarem.
  Mais simples operacionalmente; exige disciplina dos produtores.
- **Opção C — JSON com tratamento estrito de nulos (atual):** Consumers tratam campos ausentes
  como nulo (não como erro), então mudanças aditivas são seguras. Mudanças quebradoras (remoção
  de campo, alteração de tipo) exigem uma migração de tópico versionada.

Documentado como: implementado = JSON sem registry (aceitável para demo),
produção = Schema Registry com Avro, caminho de migração = campo `version` no payload.

### 15.5 Por que batch processing não foi implementado

Para o mesmo SKU acima de 1.000 eventos/segundo, a otimização natural seria agrupar eventos
dentro de um batch do poll e computar o efeito líquido com um único UPDATE na tabela `stocks`.
Essa abordagem foi avaliada e explicitamente rejeitada por cinco razões:

1. **Atomicidade por evento quebra**: Hoje cada evento tem sua própria transação. Com batching,
   uma `OptimisticLockingFailureException` no evento 30 de um batch de 50 força retry de todos —
   incluindo os 29 que teriam tido sucesso. O comportamento de falha torna-se não determinístico
   por evento.

2. **O audit trail não simplifica**: `StockHistory` ainda exige uma entrada por evento.
   `OrderState` ainda exige uma transição por evento. Apenas a tabela `stocks` se beneficia
   (um UPDATE em vez de N). Toda a complexidade é mantida por um ganho em uma única tabela.

3. **Dependências intra-batch criam um novo problema**: Um batch contendo
   `[ORDER_CREATED ML-001, ORDER_CANCELLED ML-001]` exige uma state machine intra-batch
   antes de tocar o banco, pois o cancelamento depende da criação que ainda não foi persistida.
   Isso é um mini reprocessador de eventos dentro do consumer.

4. **Atribuição sob falha torna-se ambígua**: Se o evento 7 de um batch de 50 causa estoque
   insuficiente, o que acontece com os eventos 8–50? `INCONSISTENT`? `PENDING`? O audit trail
   perde clareza — que é o requisito central deste desafio.

5. **Semântica de retry muda**: Hoje a unidade de reprocessamento é um único `eventId` — trivial
   de retentar isoladamente. Com batching, a unidade torna-se o batch inteiro. Operadores perdem
   a capacidade de reprocessar um único evento sem reexecutar o batch inteiro.

Para um domínio onde rastreabilidade é o requisito central, essas perdas superam o ganho de
throughput. A solução correta para 10.000 eventos/segundo no mesmo SKU é um pipeline dedicado
de alto throughput (operações atômicas em Redis + sincronização assíncrona com PostgreSQL),
não otimização de batch processing dentro do consumer existente.
