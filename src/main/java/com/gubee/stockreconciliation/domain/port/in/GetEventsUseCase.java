package com.gubee.stockreconciliation.domain.port.in;

import com.gubee.stockreconciliation.domain.model.ProcessedEvent;
import com.gubee.stockreconciliation.domain.model.enums.EventStatus;

import java.util.List;

public interface GetEventsUseCase {

    List<ProcessedEvent> getByStatus(EventStatus status);
}
