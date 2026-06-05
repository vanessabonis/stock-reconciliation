package com.gubee.stockreconciliation.domain.model;

import com.gubee.stockreconciliation.domain.model.enums.OrderLifecycleState;
import com.gubee.stockreconciliation.domain.model.vo.AccountId;
import com.gubee.stockreconciliation.domain.model.vo.Quantity;
import com.gubee.stockreconciliation.domain.model.vo.Sku;

import java.time.Instant;
import java.util.UUID;

/**
 * Rastreia o estado do ciclo de vida de um pedido do marketplace.
 *
 * pendingCancellationEventId é definido quando state=PENDING, permitindo que o handler de
 * ORDER_CREATED localize e atualize o ProcessedEvent salvo anteriormente para o
 * ORDER_CANCELLED que chegou fora de ordem.
 */
public final class OrderState {

    private UUID id;
    private final String marketplace;
    private final AccountId accountId;
    private final String externalOrderId;
    private final Sku sku;
    private OrderLifecycleState state;
    private final Quantity quantity;
    private String pendingCancellationEventId;
    private final Instant createdAt;
    private Instant updatedAt;

    public static OrderState createPending(
            String marketplace, AccountId accountId, String externalOrderId,
            Sku sku, Quantity quantity, String cancellationEventId) {
        OrderState os = new OrderState(marketplace, accountId, externalOrderId, sku,
                OrderLifecycleState.PENDING, quantity);
        os.pendingCancellationEventId = cancellationEventId;
        return os;
    }

    public static OrderState createCreated(
            String marketplace, AccountId accountId, String externalOrderId, Sku sku, Quantity quantity) {
        return new OrderState(marketplace, accountId, externalOrderId, sku, OrderLifecycleState.CREATED, quantity);
    }

    private OrderState(
            String marketplace, AccountId accountId, String externalOrderId,
            Sku sku, OrderLifecycleState state, Quantity quantity) {
        this.id = UUID.randomUUID();
        this.marketplace = marketplace;
        this.accountId = accountId;
        this.externalOrderId = externalOrderId;
        this.sku = sku;
        this.state = state;
        this.quantity = quantity;
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
    }

    public void transitionTo(OrderLifecycleState newState) {
        this.state = newState;
        this.updatedAt = Instant.now();
    }

    public boolean isPending() { return state == OrderLifecycleState.PENDING; }
    public boolean isCreated() { return state == OrderLifecycleState.CREATED; }
    public boolean isCancelled() { return state == OrderLifecycleState.CANCELLED; }
    public boolean isRestored() { return state == OrderLifecycleState.RESTORED; }
    public boolean isAlreadyRestored() { return isCancelled() || isRestored(); }

    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }
    public String getMarketplace() { return marketplace; }
    public AccountId getAccountId() { return accountId; }
    public String getExternalOrderId() { return externalOrderId; }
    public Sku getSku() { return sku; }
    public OrderLifecycleState getState() { return state; }
    public Quantity getQuantity() { return quantity; }
    public String getPendingCancellationEventId() { return pendingCancellationEventId; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}
