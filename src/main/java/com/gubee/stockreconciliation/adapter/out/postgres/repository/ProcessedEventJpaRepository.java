package com.gubee.stockreconciliation.adapter.out.postgres.repository;

import com.gubee.stockreconciliation.adapter.out.postgres.entity.ProcessedEventJpaEntity;
import com.gubee.stockreconciliation.domain.model.enums.EventStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ProcessedEventJpaRepository extends JpaRepository<ProcessedEventJpaEntity, UUID> {

    Optional<ProcessedEventJpaEntity> findByEventId(String eventId);

    List<ProcessedEventJpaEntity> findByStatus(String status);

    long countByStatus(String status);

    List<ProcessedEventJpaEntity> findByStatusAndProcessedAtBefore(String status, Instant threshold);

    /**
     * Atomically claims the eventId slot. Returns 1 if newly inserted, 0 if already exists.
     * No exception is thrown on duplicate — PostgreSQL handles the conflict silently.
     * Called at the START of processing to guard against concurrent duplicate delivery.
     *
     * The caller passes {@link EventStatus#PROCESSING}.name() as initialStatus — a transient
     * sentinel value that is always replaced by finalizeStatus() before the transaction commits.
     * Driving the value through the parameter (rather than a hard-coded literal) keeps the enum
     * as the single source of truth and makes refactoring safe.
     */
    @Modifying
    @Query(nativeQuery = true, value = """
            INSERT INTO processed_events (id, event_id, event_type, status, account_id, sku, occurred_at, processed_at)
            VALUES (gen_random_uuid(), :eventId, :eventType, :initialStatus, :accountId, :sku, :occurredAt, NOW())
            ON CONFLICT (event_id) DO NOTHING
            """)
    int guardInsert(@Param("eventId") String eventId,
                    @Param("eventType") String eventType,
                    @Param("initialStatus") String initialStatus,
                    @Param("accountId") String accountId,
                    @Param("sku") String sku,
                    @Param("occurredAt") Instant occurredAt);

    /**
     * Writes the final business outcome (status + details) into the row claimed by guardInsert.
     */
    @Modifying
    @Query(nativeQuery = true, value = """
            UPDATE processed_events SET status = :status, details = :details WHERE event_id = :eventId
            """)
    void finalizeStatus(@Param("eventId") String eventId,
                        @Param("status") String status,
                        @Param("details") String details);
}
