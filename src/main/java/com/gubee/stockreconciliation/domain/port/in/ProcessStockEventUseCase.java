package com.gubee.stockreconciliation.domain.port.in;

import com.gubee.stockreconciliation.domain.model.StockEvent;
import com.gubee.stockreconciliation.domain.model.enums.EventStatus;

public interface ProcessStockEventUseCase {

    EventStatus process(StockEvent event);
}
