package com.gubee.stockreconciliation.adapter.out.postgres.repository;

import com.gubee.stockreconciliation.adapter.out.postgres.entity.StockHistoryJpaEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface StockHistoryJpaRepository extends JpaRepository<StockHistoryJpaEntity, UUID> {

    List<StockHistoryJpaEntity> findByStockIdOrderByOccurredAtAsc(UUID stockId);
}
