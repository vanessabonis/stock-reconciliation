package com.gubee.stockreconciliation.domain.model.enums;

public enum EventStatus {
    /** Evento aplicado integralmente; estoque e/ou estado atualizados. */
    PROCESSED,

    /** Evento reconhecido, mas sem ação no estoque (ex.: STOCK_SYNC_SENT). */
    IGNORED,

    /** Evento recebido, aguardando um evento pré-requisito (fora de ordem). */
    PENDING,

    /** Evento conflita com o estado atual; registrado mas não aplicado. */
    INCONSISTENT
}
