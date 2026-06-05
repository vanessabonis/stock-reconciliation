package com.gubee.stockreconciliation.adapter.out.health;

import com.gubee.stockreconciliation.domain.port.out.OutboxRepository;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Reporta DOWN quando o relay do outbox apresenta sinais de travamento:
 * - Qualquer entrada PENDING com mais de 5 minutos (relay pode ter parado)
 * - Qualquer entrada FAILED (retries esgotados — requer investigação manual)
 * - Backlog de pendências acima de 500 (gargalo no relay)
 *
 * No Kubernetes: um health check DOWN impede que o tráfego alcance uma instância degradada.
 * Use como probe de liveness e readiness:
 *   GET /actuator/health
 */
@Component
public class OutboxHealthIndicator implements HealthIndicator {

    private static final int STALE_THRESHOLD_MINUTES = 5;
    private static final int CRITICAL_PENDING_COUNT   = 500;

    private final OutboxRepository outboxRepository;

    public OutboxHealthIndicator(OutboxRepository outboxRepository) {
        this.outboxRepository = outboxRepository;
    }

    @Override
    public Health health() {
        long pendingCount = outboxRepository.countByStatus("PENDING");
        long staleCount   = outboxRepository.countStaleEntries(STALE_THRESHOLD_MINUTES);
        long failedCount  = outboxRepository.countByStatus("FAILED");

        if (staleCount > 0 || failedCount > 0) {
            return Health.down()
                    .withDetail("pendingCount", pendingCount)
                    .withDetail("staleCount",   staleCount)
                    .withDetail("failedCount",  failedCount)
                    .withDetail("reason", "Outbox has stale or failed entries — relay may be stuck")
                    .build();
        }

        if (pendingCount > CRITICAL_PENDING_COUNT) {
            return Health.down()
                    .withDetail("pendingCount", pendingCount)
                    .withDetail("reason", "Outbox backlog exceeds threshold — possible relay bottleneck")
                    .build();
        }

        return Health.up()
                .withDetail("pendingCount", pendingCount)
                .build();
    }
}
