package com.gubee.stockreconciliation.domain.model.vo;

public record AccountId(String value) {

    public AccountId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("accountId cannot be blank");
        }
    }

    public static AccountId of(String value) {
        return new AccountId(value);
    }
}
