package com.gubee.stockreconciliation.domain.model.enums;

public enum EventStatus {
    /** Evento aplicado integralmente; estoque e/ou estado atualizados. */
    PROCESSED,

    /** Evento reconhecido, mas sem ação no estoque (ex.: STOCK_SYNC_SENT). */
    IGNORED,

    /** Evento recebido, aguardando um evento pré-requisito (fora de ordem). */
    PENDING,

    /** Evento conflita com o estado atual; registrado mas não aplicado. */
    INCONSISTENT,

    /**
     * Estado transiente: a linha foi reservada via INSERT ON CONFLICT DO NOTHING no início da
     * transação mas o resultado de negócio final ainda não foi escrito.
     *
     * Este status NUNCA deve aparecer em dados finais persistidos — ao final de qualquer
     * transação bem-sucedida, finalizeStatus() sempre o substitui por PROCESSED/IGNORED/PENDING/INCONSISTENT.
     *
     * Exposto neste enum para que a query nativa em ProcessedEventJpaRepository.guardInsert()
     * possa referenciar o valor via {@code EventStatus.PROCESSING.name()} ao invés de uma string
     * literal, eliminando o risco de divergência silenciosa entre o enum e o SQL.
     */
    PROCESSING
}
