package com.gubee.stockreconciliation.infrastructure.logging;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/**
 * Mantém dois identificadores em paralelo durante a migração Brave → OTel:
 *
 *   correlationId — chave humana simples para logs e dashboards legados.
 *                   Fonte: X-Trace-Id (header legado) ou UUID gerado.
 *                   Permanece no MDC para sistemas que ainda não consomem traceparent.
 *
 *   traceId/spanId — populados automaticamente pelo Micrometer Tracing OTel bridge.
 *                    São a source of truth para distributed tracing (Jaeger/Tempo).
 *
 * O header W3C traceparent é propagado na resposta para que clientes novos possam
 * usar o trace context OTel. O X-Trace-Id é mantido na resposta para compatibilidade
 * com clientes legados que ainda correlacionam por esse header.
 */
@Component
public class CorrelationIdFilter extends OncePerRequestFilter {

    private static final String TRACEPARENT_HEADER = "traceparent";
    private static final String LEGACY_TRACE_HEADER = "X-Trace-Id";

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        // correlationId: chave humana simples — fonte preferida é o header legado para
        // manter compatibilidade; cai no UUID se nenhum header estiver presente.
        String legacyTraceId = request.getHeader(LEGACY_TRACE_HEADER);
        String correlationId = (legacyTraceId != null && !legacyTraceId.isBlank())
                ? legacyTraceId
                : UUID.randomUUID().toString();

        MDC.put("correlationId", correlationId);

        // Propaga ambos os headers na resposta para suportar clientes legados e OTel
        response.setHeader(LEGACY_TRACE_HEADER, correlationId);
        String traceparent = request.getHeader(TRACEPARENT_HEADER);
        if (traceparent != null && !traceparent.isBlank()) {
            response.setHeader(TRACEPARENT_HEADER, traceparent);
        }

        try {
            chain.doFilter(request, response);
        } finally {
            MDC.clear();
        }
    }
}
