package com.gubee.stockreconciliation.domain.model.vo;

/**
 * Chave composta que identifica um registro de estoque único.
 * O uso de um value object previne inversão de argumentos (passar sku onde accountId é esperado).
 */
public record StockKey(AccountId accountId, Sku sku) {

    public StockKey {
        if (accountId == null) throw new IllegalArgumentException("accountId is required");
        if (sku == null) throw new IllegalArgumentException("sku is required");
    }

    public static StockKey of(String accountId, String sku) {
        return new StockKey(AccountId.of(accountId), Sku.of(sku));
    }

    @Override
    public String toString() {
        return accountId.value() + "/" + sku.value();
    }
}
