package com.gubee.stockreconciliation.application.usecase;

import com.gubee.stockreconciliation.domain.model.Stock;
import com.gubee.stockreconciliation.domain.model.vo.StockKey;
import com.gubee.stockreconciliation.domain.port.in.GetStockUseCase;
import com.gubee.stockreconciliation.domain.port.out.StockRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
@Transactional(readOnly = true)
public class GetStockUseCaseImpl implements GetStockUseCase {

    private final StockRepository stockRepository;

    public GetStockUseCaseImpl(StockRepository stockRepository) {
        this.stockRepository = stockRepository;
    }

    @Override
    public Optional<Stock> getStock(StockKey key) {
        return stockRepository.findByKey(key);
    }
}
