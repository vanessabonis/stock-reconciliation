package com.gubee.stockreconciliation.adapter.out.postgres.repository;

import com.gubee.stockreconciliation.adapter.out.postgres.entity.OutboxEventJpaEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.UUID;

public interface OutboxEventJpaRepository extends JpaRepository<OutboxEventJpaEntity, UUID> {

    // SELECT FOR UPDATE SKIP LOCKED: com múltiplas instâncias do relay, cada instância adquire
    // linhas diferentes. Sem SKIP LOCKED, duas instâncias poderiam bloquear e processar a mesma
    // linha, publicando o mesmo evento duas vezes no Kafka.
    @Query(value = """
            SELECT * FROM outbox_events
            WHERE status = 'PENDING'
            ORDER BY created_at ASC
            LIMIT 50
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEventJpaEntity> findPendingWithLock();

    long countByStatus(String status);

    @Query(value = """
            SELECT COUNT(*) FROM outbox_events
            WHERE status = 'PENDING'
              AND created_at < NOW() - CAST(:minutes || ' minutes' AS INTERVAL)
            """, nativeQuery = true)
    long countStaleEntries(int minutes);
}
