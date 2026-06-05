package com.gubee.stockreconciliation.adapter.out.postgres.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "stock_history")
@Getter
@Setter
public class StockHistoryJpaEntity {

    @Id
    @Column(columnDefinition = "uuid")
    private UUID id;

    @Column(name = "stock_id", nullable = false, columnDefinition = "uuid")
    private UUID stockId;

    @Column(name = "event_id", nullable = false)
    private String eventId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "quantity_before", nullable = false)
    private int quantityBefore;

    @Column(name = "quantity_after", nullable = false)
    private int quantityAfter;

    @Column(name = "delta", nullable = false)
    private int delta;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "marketplace")
    private String marketplace;

    @Column(name = "external_order_id")
    private String externalOrderId;

    @Column(name = "reason")
    private String reason;
}
