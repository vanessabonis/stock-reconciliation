package com.gubee.stockreconciliation.application.usecase;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gubee.stockreconciliation.domain.model.OrderState;
import com.gubee.stockreconciliation.domain.model.OutboxEvent;
import com.gubee.stockreconciliation.domain.model.ProcessedEvent;
import com.gubee.stockreconciliation.domain.model.Stock;
import com.gubee.stockreconciliation.domain.model.StockEvent;
import com.gubee.stockreconciliation.domain.model.StockHistory;
import com.gubee.stockreconciliation.domain.model.enums.EventStatus;
import com.gubee.stockreconciliation.domain.model.enums.EventType;
import com.gubee.stockreconciliation.domain.model.enums.OrderLifecycleState;
import com.gubee.stockreconciliation.domain.model.vo.EventId;
import com.gubee.stockreconciliation.domain.model.vo.StockKey;
import com.gubee.stockreconciliation.domain.port.out.OrderStateRepository;
import com.gubee.stockreconciliation.domain.port.out.OutboxRepository;
import com.gubee.stockreconciliation.domain.port.out.ProcessedEventRepository;
import com.gubee.stockreconciliation.domain.port.out.StockRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Contém toda a lógica de negócio transacional do processamento de eventos.
 * Existe como bean separado de ProcessStockEventUseCaseImpl para que o proxy AOP do Spring
 * aplique corretamente os limites transacionais (chamadas internas ao mesmo bean ignoram @Transactional).
 */
@Component
public class StockEventTransactionalProcessor {

    private static final Logger log = LoggerFactory.getLogger(StockEventTransactionalProcessor.class);

    private final StockRepository stockRepository;
    private final OrderStateRepository orderStateRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public StockEventTransactionalProcessor(StockRepository stockRepository,
                                             OrderStateRepository orderStateRepository,
                                             ProcessedEventRepository processedEventRepository,
                                             OutboxRepository outboxRepository,
                                             ObjectMapper objectMapper) {
        this.stockRepository = stockRepository;
        this.orderStateRepository = orderStateRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    // strategy
    // transação única no banco: tudo ou nada persiste
    @Transactional
    public EventStatus process(StockEvent event) {
        return switch (event.type()) {
            case STOCK_ADJUSTED -> handleStockAdjusted(event);
            case ORDER_CREATED -> handleOrderCreated(event);
            case ORDER_CANCELLED -> handleOrderCancelled(event);
            case STOCK_SYNC_SENT -> handleStockSyncSent(event);
            case MARKETPLACE_STOCK_RESTORED -> handleMarketplaceStockRestored(event);
        };
    }

    private EventStatus handleStockAdjusted(StockEvent event) {
        StockKey key = StockKey.of(event.accountId().value(), event.sku().value());
        Stock stock = stockRepository.findByKey(key).orElseGet(() -> Stock.create(key));

        StockHistory history = stock.apply(event);

        stockRepository.save(stock);
        stockRepository.saveHistory(history);

        saveProcessedEvent(event, EventStatus.PROCESSED, null);
        saveOutboxEvent(event, EventStatus.PROCESSED, stock.getAvailableQuantity().value());
        log.info("STOCK_ADJUSTED applied: accountId={} sku={} newQty={}",
                event.accountId().value(), event.sku().value(), stock.getAvailableQuantity().value());
        return EventStatus.PROCESSED;
    }

    private EventStatus handleOrderCreated(StockEvent event) {
        StockKey key = StockKey.of(event.accountId().value(), event.sku().value());
        Stock stock = stockRepository.findByKey(key).orElseGet(() -> Stock.create(key));

        Optional<OrderState> existingState = orderStateRepository.findByKey(
                event.marketplace(), event.accountId(), event.externalOrderId(), event.sku());

        if (existingState.isPresent()) {
            OrderState orderState = existingState.get();
            if (orderState.isPending()) {
                return resolveOutOfOrderCancellation(event, stock, orderState);
            }
            String reason = "ORDER_CREATED received but order is already in state " + orderState.getState();
            log.warn("Inconsistent ORDER_CREATED: eventId={} orderState={}", event.eventId().value(), orderState.getState());
            saveProcessedEvent(event, EventStatus.INCONSISTENT, reason);
            saveOutboxEvent(event, EventStatus.INCONSISTENT, null);
            return EventStatus.INCONSISTENT;
        }

        StockHistory history = stock.apply(event);
        stockRepository.save(stock);
        stockRepository.saveHistory(history);

        OrderState newState = OrderState.createCreated(event.marketplace(), event.accountId(),
                event.externalOrderId(), event.sku(), event.quantity());
        orderStateRepository.save(newState);

        saveProcessedEvent(event, EventStatus.PROCESSED, null);
        saveOutboxEvent(event, EventStatus.PROCESSED, stock.getAvailableQuantity().value());
        log.info("ORDER_CREATED applied: accountId={} sku={} orderId={} qty={} newBalance={}",
                event.accountId().value(), event.sku().value(), event.externalOrderId(),
                event.quantity().value(), stock.getAvailableQuantity().value());
        return EventStatus.PROCESSED;
    }

    /**
     * Cenário de evento fora de ordem: ORDER_CANCELLED chegou antes de ORDER_CREATED.
     * Aplica atomicamente subtract (ORDER_CREATED) e add-back (ORDER_CANCELLED). Delta líquido = 0.
     */
    private EventStatus resolveOutOfOrderCancellation(StockEvent orderCreatedEvent, Stock stock, OrderState orderState) {
        StockHistory createdHistory = stock.apply(orderCreatedEvent);
        stockRepository.saveHistory(createdHistory);

        StockEvent syntheticCancel = new StockEvent(
                EventId.of(orderState.getPendingCancellationEventId()),
                EventType.ORDER_CANCELLED,
                orderCreatedEvent.occurredAt(),
                orderCreatedEvent.accountId(),
                orderCreatedEvent.sku(),
                orderCreatedEvent.marketplace(),
                orderCreatedEvent.externalOrderId(),
                orderState.getQuantity(),
                "resolved out-of-order cancellation"
        );
        StockHistory cancelHistory = stock.apply(syntheticCancel);
        stockRepository.saveHistory(cancelHistory);

        stockRepository.save(stock);

        orderState.transitionTo(OrderLifecycleState.CANCELLED);
        orderStateRepository.save(orderState);

        processedEventRepository.findByEventId(EventId.of(orderState.getPendingCancellationEventId()))
                .ifPresent(pe -> {
                    pe.updateStatus(EventStatus.PROCESSED, "resolved: ORDER_CREATED arrived; net stock change = 0");
                    processedEventRepository.save(pe);
                });

        saveProcessedEvent(orderCreatedEvent, EventStatus.PROCESSED,
                "out-of-order: ORDER_CANCELLED had arrived first; net stock change = 0");
        saveOutboxEvent(orderCreatedEvent, EventStatus.PROCESSED, stock.getAvailableQuantity().value());
        log.info("Out-of-order ORDER_CANCELLED resolved: accountId={} sku={} orderId={} netDelta=0",
                orderCreatedEvent.accountId().value(), orderCreatedEvent.sku().value(), orderCreatedEvent.externalOrderId());
        return EventStatus.PROCESSED;
    }

    private EventStatus handleOrderCancelled(StockEvent event) {
        Optional<OrderState> existingState = orderStateRepository.findByKey(
                event.marketplace(), event.accountId(), event.externalOrderId(), event.sku());

        if (existingState.isEmpty()) {
            OrderState pendingState = OrderState.createPending(
                    event.marketplace(), event.accountId(), event.externalOrderId(),
                    event.sku(), event.quantity(), event.eventId().value());
            orderStateRepository.save(pendingState);

            saveProcessedEvent(event, EventStatus.PENDING, "ORDER_CREATED has not arrived yet; held in PENDING state");
            saveOutboxEvent(event, EventStatus.PENDING, null);
            log.info("ORDER_CANCELLED held as PENDING (out-of-order): accountId={} sku={} orderId={}",
                    event.accountId().value(), event.sku().value(), event.externalOrderId());
            return EventStatus.PENDING;
        }

        OrderState orderState = existingState.get();

        if (orderState.isCreated()) {
            StockKey key = StockKey.of(event.accountId().value(), event.sku().value());
            Stock stock = stockRepository.findByKey(key).orElseGet(() -> Stock.create(key));
            StockHistory history = stock.apply(event);
            stockRepository.save(stock);
            stockRepository.saveHistory(history);

            orderState.transitionTo(OrderLifecycleState.CANCELLED);
            orderStateRepository.save(orderState);

            saveProcessedEvent(event, EventStatus.PROCESSED, null);
            saveOutboxEvent(event, EventStatus.PROCESSED, stock.getAvailableQuantity().value());
            log.info("ORDER_CANCELLED applied: accountId={} sku={} orderId={} restoredQty={} newBalance={}",
                    event.accountId().value(), event.sku().value(), event.externalOrderId(),
                    event.quantity().value(), stock.getAvailableQuantity().value());
            return EventStatus.PROCESSED;
        }

        String reason = "ORDER_CANCELLED rejected: order is in state " + orderState.getState();
        log.warn("Inconsistent ORDER_CANCELLED: eventId={} currentState={}", event.eventId().value(), orderState.getState());
        saveProcessedEvent(event, EventStatus.INCONSISTENT, reason);
        saveOutboxEvent(event, EventStatus.INCONSISTENT, null);
        return EventStatus.INCONSISTENT;
    }

    private EventStatus handleStockSyncSent(StockEvent event) {
        StockKey key = StockKey.of(event.accountId().value(), event.sku().value());
        stockRepository.findByKey(key).ifPresent(stock -> {
            StockHistory history = stock.apply(event);
            stockRepository.saveHistory(history);
        });

        saveProcessedEvent(event, EventStatus.IGNORED, "STOCK_SYNC_SENT is audit-only; no balance change");
        saveOutboxEvent(event, EventStatus.IGNORED, null);
        log.info("STOCK_SYNC_SENT recorded: accountId={} sku={} qty={}",
                event.accountId().value(), event.sku().value(),
                event.quantity() != null ? event.quantity().value() : 0);
        return EventStatus.IGNORED;
    }

    private EventStatus handleMarketplaceStockRestored(StockEvent event) {
        Optional<OrderState> existingState = orderStateRepository.findByKey(
                event.marketplace(), event.accountId(), event.externalOrderId(), event.sku());

        if (existingState.isEmpty()) {
            String reason = "MARKETPLACE_STOCK_RESTORED rejected: no known order for externalOrderId=" + event.externalOrderId();
            log.warn("Inconsistent MARKETPLACE_STOCK_RESTORED: eventId={} reason={}", event.eventId().value(), reason);
            saveProcessedEvent(event, EventStatus.INCONSISTENT, reason);
            saveOutboxEvent(event, EventStatus.INCONSISTENT, null);
            return EventStatus.INCONSISTENT;
        }

        OrderState orderState = existingState.get();

        if (orderState.isCreated()) {
            StockKey key = StockKey.of(event.accountId().value(), event.sku().value());
            Stock stock = stockRepository.findByKey(key).orElseGet(() -> Stock.create(key));
            StockHistory history = stock.apply(event);
            stockRepository.save(stock);
            stockRepository.saveHistory(history);

            orderState.transitionTo(OrderLifecycleState.RESTORED);
            orderStateRepository.save(orderState);

            saveProcessedEvent(event, EventStatus.PROCESSED, null);
            saveOutboxEvent(event, EventStatus.PROCESSED, stock.getAvailableQuantity().value());
            log.info("MARKETPLACE_STOCK_RESTORED applied: accountId={} sku={} orderId={} newBalance={}",
                    event.accountId().value(), event.sku().value(), event.externalOrderId(),
                    stock.getAvailableQuantity().value());
            return EventStatus.PROCESSED;
        }

        String reason = "MARKETPLACE_STOCK_RESTORED rejected: order is already in state " + orderState.getState();
        log.warn("Inconsistent MARKETPLACE_STOCK_RESTORED: eventId={} orderId={} state={}",
                event.eventId().value(), event.externalOrderId(), orderState.getState());
        saveProcessedEvent(event, EventStatus.INCONSISTENT, reason);
        saveOutboxEvent(event, EventStatus.INCONSISTENT, null);
        return EventStatus.INCONSISTENT;
    }

    /**
     * Grava o registro de evento processado. Se a constraint única em event_id disparar (duplicata),
     * a DataIntegrityViolationException propaga para o chamador (process()), que a trata FORA desta
     * transação — após o rollback — para evitar consultar uma transação PostgreSQL já abortada.
     */
    private void saveProcessedEvent(StockEvent event, EventStatus status, String details) {
        ProcessedEvent pe = new ProcessedEvent(
                event.eventId(), event.type(), status,
                event.accountId(), event.sku(), event.occurredAt(), details);
        processedEventRepository.save(pe);
    }

    /**
     * Grava uma entrada no outbox DENTRO DA MESMA TRANSAÇÃO que a atualização do estoque.
     * Núcleo do Transactional Outbox Pattern:
     *   - DB comita → linha do outbox existe → o relay PUBLICARÁ no Kafka
     *   - DB reverte → linha do outbox não existe → nada é publicado
     * Sem problema de dual-write. Sem janela de inconsistência.
     *
     * aggregateId = accountId:sku é a chave da mensagem Kafka, garantindo que todos os eventos
     * do mesmo estoque roteem para a mesma partição (ordenação por estoque dentro do consumer group).
     */
    private void saveOutboxEvent(StockEvent event, EventStatus status, Integer resultingBalance) {
        try {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("eventId", event.eventId().value());
            payload.put("type", event.type().name());
            payload.put("accountId", event.accountId().value());
            payload.put("sku", event.sku().value());
            payload.put("status", status.name());
            payload.put("occurredAt", event.occurredAt().toString());
            if (resultingBalance != null) payload.put("resultingBalance", resultingBalance);
            if (event.marketplace() != null) payload.put("marketplace", event.marketplace());
            if (event.externalOrderId() != null) payload.put("externalOrderId", event.externalOrderId());

            String aggregateId = event.accountId().value() + ":" + event.sku().value();
            outboxRepository.save(new OutboxEvent("STOCK", aggregateId, event.type().name(),
                    objectMapper.writeValueAsString(payload)));
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize outbox payload for eventId={}: {}", event.eventId().value(), e.getMessage());
        }
    }

    @Transactional(readOnly = true)
    public Optional<ProcessedEvent> findByEventId(EventId eventId) {
        return processedEventRepository.findByEventId(eventId);
    }
}
