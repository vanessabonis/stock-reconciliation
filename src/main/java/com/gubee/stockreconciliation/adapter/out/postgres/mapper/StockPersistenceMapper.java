package com.gubee.stockreconciliation.adapter.out.postgres.mapper;

import com.gubee.stockreconciliation.adapter.out.postgres.entity.StockHistoryJpaEntity;
import com.gubee.stockreconciliation.adapter.out.postgres.entity.StockJpaEntity;
import com.gubee.stockreconciliation.domain.model.Stock;
import com.gubee.stockreconciliation.domain.model.StockHistory;
import com.gubee.stockreconciliation.domain.model.enums.EventType;
import com.gubee.stockreconciliation.domain.model.vo.AccountId;
import com.gubee.stockreconciliation.domain.model.vo.EventId;
import com.gubee.stockreconciliation.domain.model.vo.Quantity;
import com.gubee.stockreconciliation.domain.model.vo.Sku;
import org.springframework.stereotype.Component;

@Component
public class StockPersistenceMapper {

    public StockJpaEntity toJpa(Stock stock) {
        StockJpaEntity entity = new StockJpaEntity();
        entity.setId(stock.getId());
        entity.setAccountId(stock.getAccountId().value());
        entity.setSku(stock.getSku().value());
        entity.setAvailableQuantity(stock.getAvailableQuantity().value());
        entity.setLastUpdatedAt(stock.getLastUpdatedAt());
        entity.setVersion(stock.getVersion());
        return entity;
    }

    public Stock toDomain(StockJpaEntity entity) {
        return Stock.reconstitute(
                entity.getId(),
                AccountId.of(entity.getAccountId()),
                Sku.of(entity.getSku()),
                Quantity.of(entity.getAvailableQuantity()),
                entity.getLastUpdatedAt(),
                entity.getVersion());
    }

    public StockHistoryJpaEntity historyToJpa(StockHistory history) {
        StockHistoryJpaEntity entity = new StockHistoryJpaEntity();
        entity.setId(history.getId());
        entity.setStockId(history.getStockId());
        entity.setEventId(history.getEventId().value());
        entity.setEventType(history.getEventType().name());
        entity.setQuantityBefore(history.getQuantityBefore().value());
        entity.setQuantityAfter(history.getQuantityAfter().value());
        entity.setDelta(history.getDelta());
        entity.setOccurredAt(history.getOccurredAt());
        entity.setCreatedAt(history.getCreatedAt());
        entity.setMarketplace(history.getMarketplace());
        entity.setExternalOrderId(history.getExternalOrderId());
        entity.setReason(history.getReason());
        return entity;
    }

    public StockHistory historyToDomain(StockHistoryJpaEntity entity) {
        return StockHistory.reconstitute(
                entity.getId(),
                entity.getStockId(),
                EventId.of(entity.getEventId()),
                EventType.valueOf(entity.getEventType()),
                Quantity.of(entity.getQuantityBefore()),
                Quantity.of(entity.getQuantityAfter()),
                entity.getDelta(),
                entity.getOccurredAt(),
                entity.getCreatedAt(),
                entity.getMarketplace(),
                entity.getExternalOrderId(),
                entity.getReason()
        );
    }
}
