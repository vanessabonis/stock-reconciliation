package com.gubee.stockreconciliation.adapter.out.postgres.mapper;

import com.gubee.stockreconciliation.adapter.out.postgres.entity.ProcessedEventJpaEntity;
import com.gubee.stockreconciliation.domain.model.ProcessedEvent;
import com.gubee.stockreconciliation.domain.model.enums.EventStatus;
import com.gubee.stockreconciliation.domain.model.enums.EventType;
import com.gubee.stockreconciliation.domain.model.vo.AccountId;
import com.gubee.stockreconciliation.domain.model.vo.EventId;
import com.gubee.stockreconciliation.domain.model.vo.Sku;
import org.springframework.stereotype.Component;

@Component
public class ProcessedEventPersistenceMapper {

    public ProcessedEventJpaEntity toJpa(ProcessedEvent domain) {
        ProcessedEventJpaEntity entity = new ProcessedEventJpaEntity();
        entity.setId(domain.getId());
        entity.setEventId(domain.getEventId().value());
        entity.setEventType(domain.getEventType().name());
        entity.setStatus(domain.getStatus().name());
        entity.setAccountId(domain.getAccountId().value());
        entity.setSku(domain.getSku().value());
        entity.setOccurredAt(domain.getOccurredAt());
        entity.setProcessedAt(domain.getProcessedAt());
        entity.setDetails(domain.getDetails());
        return entity;
    }

    public ProcessedEvent toDomain(ProcessedEventJpaEntity entity) {
        ProcessedEvent domain = new ProcessedEvent(
                EventId.of(entity.getEventId()),
                EventType.valueOf(entity.getEventType()),
                EventStatus.valueOf(entity.getStatus()),
                AccountId.of(entity.getAccountId()),
                Sku.of(entity.getSku()),
                entity.getOccurredAt(),
                entity.getDetails()
        );
        domain.setId(entity.getId());
        return domain;
    }
}
