package com.gubee.stockreconciliation.domain.model.enums;

public enum OrderLifecycleState {
    /**
     * ORDER_CANCELLED chegou antes de ORDER_CREATED.
     * Estoque inalterado. Aguardando ORDER_CREATED para resolução atômica.
     */
    PENDING,

    /** ORDER_CREATED foi processado; estoque foi debitado. */
    CREATED,

    /** ORDER_CANCELLED foi processado; estoque foi restaurado. */
    CANCELLED,

    /** MARKETPLACE_STOCK_RESTORED foi processado; estoque foi restaurado. */
    RESTORED
}
