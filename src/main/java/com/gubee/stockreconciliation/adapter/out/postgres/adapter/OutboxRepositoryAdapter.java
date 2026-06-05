package com.gubee.stockreconciliation.adapter.out.postgres.adapter;

import com.gubee.stockreconciliation.adapter.out.postgres.entity.OutboxEventJpaEntity;
import com.gubee.stockreconciliation.adapter.out.postgres.repository.OutboxEventJpaRepository;
import com.gubee.stockreconciliation.domain.model.OutboxEvent;
import com.gubee.stockreconciliation.domain.port.out.OutboxRepository;
import org.springframework.stereotype.Component;

@Component
public class OutboxRepositoryAdapter implements OutboxRepository {

    private final OutboxEventJpaRepository jpaRepository;

    public OutboxRepositoryAdapter(OutboxEventJpaRepository jpaRepository) {
        this.jpaRepository = jpaRepository;
    }

    @Override
    public OutboxEvent save(OutboxEvent event) {
        OutboxEventJpaEntity entity = toJpa(event);
        jpaRepository.save(entity);
        return event;
    }

    @Override
    public long countByStatus(String status) {
        return jpaRepository.countByStatus(status);
    }

    @Override
    public long countStaleEntries(int minutes) {
        return jpaRepository.countStaleEntries(minutes);
    }

    private OutboxEventJpaEntity toJpa(OutboxEvent domain) {
        OutboxEventJpaEntity entity = new OutboxEventJpaEntity();
        entity.setId(domain.getId());
        entity.setAggregateType(domain.getAggregateType());
        entity.setAggregateId(domain.getAggregateId());
        entity.setEventType(domain.getEventType());
        entity.setPayload(domain.getPayload());
        entity.setStatus(domain.getStatus());
        entity.setCreatedAt(domain.getCreatedAt());
        entity.setPublishedAt(domain.getPublishedAt());
        entity.setRetryCount(domain.getRetryCount());
        entity.setLastError(domain.getLastError());
        return entity;
    }
}
