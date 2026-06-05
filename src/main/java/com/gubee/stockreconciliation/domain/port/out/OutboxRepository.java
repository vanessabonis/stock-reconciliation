package com.gubee.stockreconciliation.domain.port.out;

import com.gubee.stockreconciliation.domain.model.OutboxEvent;

public interface OutboxRepository {

    OutboxEvent save(OutboxEvent event);

    long countByStatus(String status);

    /** Retorna a contagem de entradas que estão PENDING há mais de {@code minutes} minutos. */
    long countStaleEntries(int minutes);
}
