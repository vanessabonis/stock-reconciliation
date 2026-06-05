package com.gubee.stockreconciliation.adapter.out.postgres.repository;

import com.gubee.stockreconciliation.adapter.out.postgres.entity.OutboxEventJpaEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.UUID;

public interface OutboxPurgeRepository extends JpaRepository<OutboxEventJpaEntity, UUID> {

    /**
     * Exclui entradas PUBLISHED do outbox cujo published_at é anterior ao cutoff informado.
     * O índice parcial idx_outbox_published_cleanup torna este DELETE um index scan rápido.
     *
     * Entradas FAILED são intencionalmente excluídas do critério — requerem investigação
     * manual e nunca são removidas automaticamente.
     */
    @Modifying
    @Query(value = """
            DELETE FROM outbox_events
            WHERE status = 'PUBLISHED'
              AND published_at < :cutoff
            """, nativeQuery = true)
    int deletePublishedBefore(@Param("cutoff") Instant cutoff);
}
