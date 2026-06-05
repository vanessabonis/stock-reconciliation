package com.gubee.stockreconciliation.adapter.out.postgres.repository;

import com.gubee.stockreconciliation.adapter.out.postgres.entity.OrderStateJpaEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface OrderStateJpaRepository extends JpaRepository<OrderStateJpaEntity, UUID> {

    Optional<OrderStateJpaEntity> findByMarketplaceAndAccountIdAndExternalOrderIdAndSku(
            String marketplace, String accountId, String externalOrderId, String sku);
}
