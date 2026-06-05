package com.gubee.stockreconciliation.adapter.out.postgres.mapper;

import com.gubee.stockreconciliation.adapter.out.postgres.entity.OrderStateJpaEntity;
import com.gubee.stockreconciliation.domain.model.OrderState;
import com.gubee.stockreconciliation.domain.model.enums.OrderLifecycleState;
import com.gubee.stockreconciliation.domain.model.vo.AccountId;
import com.gubee.stockreconciliation.domain.model.vo.Quantity;
import com.gubee.stockreconciliation.domain.model.vo.Sku;
import org.springframework.stereotype.Component;

@Component
public class OrderStatePersistenceMapper {

    public OrderStateJpaEntity toJpa(OrderState domain) {
        OrderStateJpaEntity entity = new OrderStateJpaEntity();
        entity.setId(domain.getId());
        entity.setMarketplace(domain.getMarketplace());
        entity.setAccountId(domain.getAccountId().value());
        entity.setExternalOrderId(domain.getExternalOrderId());
        entity.setSku(domain.getSku().value());
        entity.setState(domain.getState().name());
        entity.setQuantity(domain.getQuantity().value());
        entity.setPendingCancellationEventId(domain.getPendingCancellationEventId());
        entity.setCreatedAt(domain.getCreatedAt());
        entity.setUpdatedAt(domain.getUpdatedAt());
        return entity;
    }

    public OrderState toDomain(OrderStateJpaEntity entity) {
        OrderLifecycleState state = OrderLifecycleState.valueOf(entity.getState());
        OrderState domain;
        if (state == OrderLifecycleState.PENDING) {
            domain = OrderState.createPending(
                    entity.getMarketplace(),
                    AccountId.of(entity.getAccountId()),
                    entity.getExternalOrderId(),
                    Sku.of(entity.getSku()),
                    Quantity.of(entity.getQuantity()),
                    entity.getPendingCancellationEventId());
        } else {
            domain = OrderState.createCreated(
                    entity.getMarketplace(),
                    AccountId.of(entity.getAccountId()),
                    entity.getExternalOrderId(),
                    Sku.of(entity.getSku()),
                    Quantity.of(entity.getQuantity()));
            domain.transitionTo(state);
        }
        domain.setId(entity.getId());
        return domain;
    }
}
