package com.gubee.stockreconciliation.domain.model;

import com.gubee.stockreconciliation.domain.model.vo.AccountId;
import com.gubee.stockreconciliation.domain.model.vo.Quantity;
import com.gubee.stockreconciliation.domain.model.vo.Sku;
import com.gubee.stockreconciliation.domain.model.vo.StockKey;

import java.time.Instant;
import java.util.UUID;

/**
 * Aggregate root do estoque. Protege a invariante: availableQuantity >= 0.
 *
 * Toda mutação de estoque passa por apply(StockEvent), que retorna uma entrada de StockHistory.
 * Não há setters públicos para availableQuantity. Código externo não pode contornar
 * as invariantes de domínio.
 */
public final class Stock {

    private UUID id;
    private final AccountId accountId;
    private final Sku sku;
    private Quantity availableQuantity;
    private Instant lastUpdatedAt;
    private long version;

    public static Stock create(StockKey key) {
        return new Stock(key.accountId(), key.sku(), Quantity.zero());
    }

    /**
     * Reconstrói um Stock a partir de dados persistidos. Somente o mapper de persistência deve chamar este método.
     */
    public static Stock reconstitute(UUID id, AccountId accountId, Sku sku,
                                     Quantity availableQuantity, Instant lastUpdatedAt, long version) {
        Stock stock = new Stock(accountId, sku, availableQuantity);
        stock.id = id;
        stock.lastUpdatedAt = lastUpdatedAt;
        stock.version = version;
        return stock;
    }

    private Stock(AccountId accountId, Sku sku, Quantity initialQuantity) {
        this.id = UUID.randomUUID();
        this.accountId = accountId;
        this.sku = sku;
        this.availableQuantity = initialQuantity;
        this.lastUpdatedAt = Instant.now();
        this.version = 0;
    }

    /**
     * Aplica um evento de estoque e retorna uma entrada de histórico imutável.
     * Este é o ÚNICO caminho para alterar o saldo do estoque.
     */
    public StockHistory apply(StockEvent event) {
        Quantity before = this.availableQuantity;
        Quantity after = computeNewQuantity(event);

        this.availableQuantity = after;
        this.lastUpdatedAt = Instant.now();

        return new StockHistory(
                this.id,
                event.eventId(),
                event.type(),
                before,
                after,
                event.occurredAt(),
                event.marketplace(),
                event.externalOrderId(),
                event.reason()
        );
    }

    private Quantity computeNewQuantity(StockEvent event) {
        return switch (event.type()) {
            case STOCK_ADJUSTED -> event.quantity();
            case ORDER_CREATED -> availableQuantity.subtract(event.quantity());
            case ORDER_CANCELLED, MARKETPLACE_STOCK_RESTORED -> availableQuantity.add(event.quantity());
            case STOCK_SYNC_SENT -> availableQuantity; // no balance change -auditoria
        };
    }

    public StockKey getKey() {
        return new StockKey(accountId, sku);
    }

    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }
    public AccountId getAccountId() { return accountId; }
    public Sku getSku() { return sku; }
    public Quantity getAvailableQuantity() { return availableQuantity; }
    public Instant getLastUpdatedAt() { return lastUpdatedAt; }
    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
}
