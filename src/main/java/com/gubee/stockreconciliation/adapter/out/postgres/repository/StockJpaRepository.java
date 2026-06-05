package com.gubee.stockreconciliation.adapter.out.postgres.repository;

import com.gubee.stockreconciliation.adapter.out.postgres.entity.StockJpaEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface StockJpaRepository extends JpaRepository<StockJpaEntity, UUID> {

    Optional<StockJpaEntity> findByAccountIdAndSku(String accountId, String sku);
}
