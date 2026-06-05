package com.gubee.stockreconciliation.adapter.out.postgres.adapter;

import com.gubee.stockreconciliation.adapter.out.postgres.mapper.ProcessedEventPersistenceMapper;
import com.gubee.stockreconciliation.adapter.out.postgres.repository.ProcessedEventJpaRepository;
import com.gubee.stockreconciliation.domain.model.ProcessedEvent;
import com.gubee.stockreconciliation.domain.model.enums.EventStatus;
import com.gubee.stockreconciliation.domain.model.enums.EventType;
import com.gubee.stockreconciliation.domain.model.vo.AccountId;
import com.gubee.stockreconciliation.domain.model.vo.EventId;
import com.gubee.stockreconciliation.domain.model.vo.Sku;
import com.gubee.stockreconciliation.domain.port.out.ProcessedEventRepository;
import org.springframework.stereotype.Component;

// EventStatus.PROCESSING is imported here to enforce that the initial-status string
// always tracks the enum value — changing the enum constant automatically propagates here.

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Component
public class ProcessedEventRepositoryAdapter implements ProcessedEventRepository {

    private final ProcessedEventJpaRepository jpaRepository;
    private final ProcessedEventPersistenceMapper mapper;

    public ProcessedEventRepositoryAdapter(ProcessedEventJpaRepository jpaRepository,
                                            ProcessedEventPersistenceMapper mapper) {
        this.jpaRepository = jpaRepository;
        this.mapper = mapper;
    }

    @Override
    public ProcessedEvent save(ProcessedEvent event) {
        return mapper.toDomain(jpaRepository.save(mapper.toJpa(event)));
    }

    @Override
    public Optional<ProcessedEvent> findByEventId(EventId eventId) {
        return jpaRepository.findByEventId(eventId.value()).map(mapper::toDomain);
    }

    @Override
    public Optional<ProcessedEvent> findPendingCancellation(String marketplace, String accountId,
                                                              String externalOrderId, String sku) {
        // Não utilizado diretamente — o use case usa findByEventId(orderState.getPendingCancellationEventId())
        return Optional.empty();
    }

    @Override
    public List<ProcessedEvent> findByStatus(EventStatus status) {
        return jpaRepository.findByStatus(status.name()).stream().map(mapper::toDomain).toList();
    }

    @Override
    public long countByStatus(EventStatus status) {
        return jpaRepository.countByStatus(status.name());
    }

    @Override
    public boolean tryInsert(EventId eventId, EventType type, AccountId accountId, Sku sku, Instant occurredAt) {
        // Passes EventStatus.PROCESSING.name() explicitly so that the enum is the single
        // source of truth for this transient sentinel value. Any rename of the enum constant
        // will surface as a compile error here rather than a runtime mismatch.
        return jpaRepository.guardInsert(
                eventId.value(), type.name(), EventStatus.PROCESSING.name(),
                accountId.value(), sku.value(), occurredAt) > 0;
    }

    @Override
    public void finalizeStatus(EventId eventId, EventStatus status, String details) {
        jpaRepository.finalizeStatus(eventId.value(), status.name(), details);
    }
}
