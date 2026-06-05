package com.gubee.stockreconciliation.adapter.out.postgres.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "stocks",
        uniqueConstraints = @UniqueConstraint(columnNames = {"account_id", "sku"}, name = "uk_stocks_account_sku"))
@Getter
@Setter
public class StockJpaEntity {

    @Id
    @Column(columnDefinition = "uuid")
    private UUID id;

    @Column(name = "account_id", nullable = false)
    private String accountId;

    @Column(name = "sku", nullable = false)
    private String sku;

    @Column(name = "available_quantity", nullable = false)
    private int availableQuantity;

    @Column(name = "last_updated_at", nullable = false)
    private Instant lastUpdatedAt;

    @Version
    @Column(name = "version", nullable = false)
    private long version;
}
