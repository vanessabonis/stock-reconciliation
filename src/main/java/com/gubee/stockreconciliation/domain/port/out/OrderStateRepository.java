package com.gubee.stockreconciliation.domain.port.out;

import com.gubee.stockreconciliation.domain.model.OrderState;
import com.gubee.stockreconciliation.domain.model.vo.AccountId;
import com.gubee.stockreconciliation.domain.model.vo.Sku;

import java.util.Optional;

public interface OrderStateRepository {

    Optional<OrderState> findByKey(String marketplace, AccountId accountId, String externalOrderId, Sku sku);

    OrderState save(OrderState orderState);
}
