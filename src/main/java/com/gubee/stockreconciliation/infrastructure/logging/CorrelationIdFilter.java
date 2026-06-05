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
 * Atribui um traceId a cada requisição recebida e o adiciona ao MDC,
 * permitindo que todos os logs gerados durante o processamento da mesma
 * requisição utilizem o mesmo correlationId.
 */
@Component
public class CorrelationIdFilter extends OncePerRequestFilter {

    private static final String TRACE_ID_HEADER = "X-Trace-Id";

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                     HttpServletResponse response,
                                     FilterChain chain) throws ServletException, IOException {
        String traceId = request.getHeader(TRACE_ID_HEADER);
        if (traceId == null || traceId.isBlank()) {
            traceId = UUID.randomUUID().toString();
        }
        // correlationId = X-Trace-Id fornecido pelo chamador para correlação entre serviços.
        // traceId + spanId são definidos automaticamente pela bridge do Micrometer Tracing (Brave).
        MDC.put("correlationId", traceId);
        response.setHeader(TRACE_ID_HEADER, traceId);
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.clear();
        }
    }
}
