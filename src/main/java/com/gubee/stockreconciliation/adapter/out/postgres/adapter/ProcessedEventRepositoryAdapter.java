package com.gubee.stockreconciliation.adapter.out.postgres.adapter;

import com.gubee.stockreconciliation.adapter.out.postgres.mapper.ProcessedEventPersistenceMapper;
import com.gubee.stockreconciliation.adapter.out.postgres.repository.ProcessedEventJpaRepository;
import com.gubee.stockreconciliation.domain.model.ProcessedEvent;
import com.gubee.stockreconciliation.domain.model.enums.EventStatus;
import com.gubee.stockreconciliation.domain.model.vo.EventId;
import com.gubee.stockreconciliation.domain.port.out.ProcessedEventRepository;
import org.springframework.stereotype.Component;

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
}
