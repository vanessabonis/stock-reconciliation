package com.gubee.stockreconciliation.adapter.out.postgres.adapter;

import com.gubee.stockreconciliation.adapter.out.postgres.mapper.OrderStatePersistenceMapper;
import com.gubee.stockreconciliation.adapter.out.postgres.repository.OrderStateJpaRepository;
import com.gubee.stockreconciliation.domain.model.OrderState;
import com.gubee.stockreconciliation.domain.model.vo.AccountId;
import com.gubee.stockreconciliation.domain.model.vo.Sku;
import com.gubee.stockreconciliation.domain.port.out.OrderStateRepository;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class OrderStateRepositoryAdapter implements OrderStateRepository {

    private final OrderStateJpaRepository jpaRepository;
    private final OrderStatePersistenceMapper mapper;

    public OrderStateRepositoryAdapter(OrderStateJpaRepository jpaRepository, OrderStatePersistenceMapper mapper) {
        this.jpaRepository = jpaRepository;
        this.mapper = mapper;
    }

    @Override
    public Optional<OrderState> findByKey(String marketplace, AccountId accountId, String externalOrderId, Sku sku) {
        return jpaRepository.findByMarketplaceAndAccountIdAndExternalOrderIdAndSku(
                        marketplace, accountId.value(), externalOrderId, sku.value())
                .map(mapper::toDomain);
    }

    @Override
    public OrderState save(OrderState orderState) {
        return mapper.toDomain(jpaRepository.save(mapper.toJpa(orderState)));
    }
}
