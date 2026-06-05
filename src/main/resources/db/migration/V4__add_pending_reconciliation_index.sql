-- Índice composto para o job de reconciliação de PENDING.
-- A query findByStatusAndProcessedAtBefore("PENDING", threshold) filtra por status='PENDING'
-- e processed_at < threshold. Sem este índice, o banco faz full table scan em processed_events
-- a cada execução do job (a cada 5 minutos em produção), escalando linearmente com o volume.
-- O índice simples em status existente não é suficiente — não cobre o filtro de data.
CREATE INDEX IF NOT EXISTS idx_processed_events_status_processed_at
    ON processed_events (status, processed_at);
