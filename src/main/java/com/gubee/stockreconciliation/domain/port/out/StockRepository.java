package com.gubee.stockreconciliation.domain.port.out;

import com.gubee.stockreconciliation.domain.model.Stock;
import com.gubee.stockreconciliation.domain.model.StockHistory;
import com.gubee.stockreconciliation.domain.model.vo.StockKey;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface StockRepository {

    Optional<Stock> findByKey(StockKey key);

    Stock save(Stock stock);

    void saveHistory(StockHistory history);

    List<StockHistory> findHistoryByStockId(UUID stockId);
}
