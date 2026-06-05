package com.gubee.stockreconciliation.domain.port.out;

import com.gubee.stockreconciliation.domain.model.ProcessedEvent;
import com.gubee.stockreconciliation.domain.model.enums.EventStatus;
import com.gubee.stockreconciliation.domain.model.vo.EventId;

import java.util.List;
import java.util.Optional;

public interface ProcessedEventRepository {

    ProcessedEvent save(ProcessedEvent event);

    Optional<ProcessedEvent> findByEventId(EventId eventId);

    Optional<ProcessedEvent> findPendingCancellation(String marketplace, String accountId, String externalOrderId, String sku);

    List<ProcessedEvent> findByStatus(EventStatus status);

    long countByStatus(EventStatus status);
}
