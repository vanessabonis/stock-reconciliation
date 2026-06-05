package com.gubee.stockreconciliation.adapter.out.postgres.adapter;

import com.gubee.stockreconciliation.adapter.out.postgres.mapper.StockPersistenceMapper;
import com.gubee.stockreconciliation.adapter.out.postgres.repository.StockHistoryJpaRepository;
import com.gubee.stockreconciliation.adapter.out.postgres.repository.StockJpaRepository;
import com.gubee.stockreconciliation.domain.model.Stock;
import com.gubee.stockreconciliation.domain.model.StockHistory;
import com.gubee.stockreconciliation.domain.model.vo.StockKey;
import com.gubee.stockreconciliation.domain.port.out.StockRepository;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Component
public class StockRepositoryAdapter implements StockRepository {

    private final StockJpaRepository stockJpaRepository;
    private final StockHistoryJpaRepository historyJpaRepository;
    private final StockPersistenceMapper mapper;

    public StockRepositoryAdapter(StockJpaRepository stockJpaRepository,
                                   StockHistoryJpaRepository historyJpaRepository,
                                   StockPersistenceMapper mapper) {
        this.stockJpaRepository = stockJpaRepository;
        this.historyJpaRepository = historyJpaRepository;
        this.mapper = mapper;
    }

    @Override
    public Optional<Stock> findByKey(StockKey key) {
        return stockJpaRepository.findByAccountIdAndSku(key.accountId().value(), key.sku().value())
                .map(mapper::toDomain);
    }

    @Override
    public Stock save(Stock stock) {
        return mapper.toDomain(stockJpaRepository.save(mapper.toJpa(stock)));
    }

    @Override
    public void saveHistory(StockHistory history) {
        historyJpaRepository.save(mapper.historyToJpa(history));
    }

    @Override
    public List<StockHistory> findHistoryByStockId(UUID stockId) {
        return historyJpaRepository.findByStockIdOrderByOccurredAtAsc(stockId)
                .stream()
                .map(mapper::historyToDomain)
                .toList();
    }
}
