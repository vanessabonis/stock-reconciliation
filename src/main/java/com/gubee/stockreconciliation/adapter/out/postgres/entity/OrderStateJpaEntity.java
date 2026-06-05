package com.gubee.stockreconciliation.adapter.out.postgres.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "order_states",
        uniqueConstraints = @UniqueConstraint(
                columnNames = {"marketplace", "account_id", "external_order_id", "sku"},
                name = "uk_order_states_key"))
@Getter
@Setter
public class OrderStateJpaEntity {

    @Id
    @Column(columnDefinition = "uuid")
    private UUID id;

    @Column(name = "marketplace", nullable = false)
    private String marketplace;

    @Column(name = "account_id", nullable = false)
    private String accountId;

    @Column(name = "external_order_id", nullable = false)
    private String externalOrderId;

    @Column(name = "sku", nullable = false)
    private String sku;

    @Column(name = "state", nullable = false)
    private String state;

    @Column(name = "quantity", nullable = false)
    private int quantity;

    @Column(name = "pending_cancellation_event_id")
    private String pendingCancellationEventId;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
