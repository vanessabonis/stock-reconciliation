package com.gubee.stockreconciliation.domain.model.vo;

public record Sku(String value) {

    public Sku {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("sku cannot be blank");
        }
    }

    public static Sku of(String value) {
        return new Sku(value);
    }
}
