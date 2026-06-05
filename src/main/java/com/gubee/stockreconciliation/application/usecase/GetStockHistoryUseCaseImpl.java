package com.gubee.stockreconciliation.application.usecase;

import com.gubee.stockreconciliation.domain.model.StockHistory;
import com.gubee.stockreconciliation.domain.model.vo.StockKey;
import com.gubee.stockreconciliation.domain.port.in.GetStockHistoryUseCase;
import com.gubee.stockreconciliation.domain.port.out.StockRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@Transactional(readOnly = true)
public class GetStockHistoryUseCaseImpl implements GetStockHistoryUseCase {

    private final StockRepository stockRepository;

    public GetStockHistoryUseCaseImpl(StockRepository stockRepository) {
        this.stockRepository = stockRepository;
    }

    @Override
    public List<StockHistory> getHistory(StockKey key) {
        return stockRepository.findByKey(key)
                .map(stock -> stockRepository.findHistoryByStockId(stock.getId()))
                .orElse(List.of());
    }
}
