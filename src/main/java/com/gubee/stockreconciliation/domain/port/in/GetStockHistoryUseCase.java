package com.gubee.stockreconciliation.domain.port.in;

import com.gubee.stockreconciliation.domain.model.StockHistory;
import com.gubee.stockreconciliation.domain.model.vo.StockKey;

import java.util.List;

public interface GetStockHistoryUseCase {

    List<StockHistory> getHistory(StockKey key);
}
