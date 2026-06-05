package com.gubee.stockreconciliation.domain.port.in;

import com.gubee.stockreconciliation.domain.model.Stock;
import com.gubee.stockreconciliation.domain.model.vo.StockKey;

import java.util.Optional;

public interface GetStockUseCase {

    Optional<Stock> getStock(StockKey key);
}
