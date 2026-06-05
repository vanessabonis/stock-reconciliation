package com.gubee.stockreconciliation.domain.model.vo;

import com.gubee.stockreconciliation.domain.model.exception.InsufficientStockException;

/**
 * Garante a invariante de que uma quantidade de negócio nunca pode ser negativa.
 * A construção falha imediatamente — nenhum Quantity inválido pode existir no sistema.
 */
public record Quantity(int value) {

    public Quantity {
        if (value < 0) {
            throw new IllegalArgumentException("Quantity cannot be negative: " + value);
        }
    }

    public static Quantity of(int value) {
        return new Quantity(value);
    }

    public static Quantity zero() {
        return new Quantity(0);
    }

    public Quantity add(Quantity other) {
        return new Quantity(this.value + other.value);
    }

    public Quantity subtract(Quantity other) {
        int result = this.value - other.value;
        if (result < 0) {
            throw new InsufficientStockException(
                    "Cannot subtract " + other.value + " from " + this.value + ": would result in negative stock");
        }
        return new Quantity(result);
    }

    public boolean isGreaterThanOrEqual(Quantity other) {
        return this.value >= other.value;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
