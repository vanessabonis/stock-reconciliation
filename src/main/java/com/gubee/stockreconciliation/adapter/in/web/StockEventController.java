package com.gubee.stockreconciliation.adapter.in.web;

import com.gubee.stockreconciliation.adapter.in.web.dto.StockEventRequest;
import com.gubee.stockreconciliation.adapter.in.web.dto.StockEventResponse;
import com.gubee.stockreconciliation.domain.model.StockEvent;
import com.gubee.stockreconciliation.domain.model.enums.EventStatus;
import com.gubee.stockreconciliation.domain.model.enums.EventType;
import com.gubee.stockreconciliation.domain.model.vo.AccountId;
import com.gubee.stockreconciliation.domain.model.vo.EventId;
import com.gubee.stockreconciliation.domain.model.vo.Quantity;
import com.gubee.stockreconciliation.domain.model.vo.Sku;
import com.gubee.stockreconciliation.domain.port.in.ProcessStockEventUseCase;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import com.gubee.stockreconciliation.infrastructure.logging.EventProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Caminho de ingestão secundário. POST /events aceita eventos diretamente via REST.
 *
 * CAMINHO PRIMÁRIO:  Kafka consumer (StockEventKafkaConsumer) lê do tópico stock-events.
 * CAMINHO SECUNDÁRIO: este endpoint REST — usado para compatibilidade retroativa, injeção
 * administrativa e testes de integração sem Kafka.
 *
 * Ambos os caminhos chamam o mesmo ProcessStockEventUseCase. O use case não sabe como
 * o evento chegou — aqui a arquitetura hexagonal se paga. Adicionar um terceiro caminho
 * de ingestão (gRPC, SQS) não requer nenhuma alteração nas camadas de domínio ou aplicação.
 */
@RestController
@RequestMapping("/events")
@Tag(name = "Events", description = "Stock and order event ingestion")
public class StockEventController {

    private static final Logger log = LoggerFactory.getLogger(StockEventController.class);

    private final ProcessStockEventUseCase processStockEventUseCase;

    public StockEventController(ProcessStockEventUseCase processStockEventUseCase) {
        this.processStockEventUseCase = processStockEventUseCase;
    }

    //controller de entrada para teste local
    @PostMapping
    @Operation(summary = "Submit a stock or order event for processing")
    public ResponseEntity<StockEventResponse> receiveEvent(@Valid @RequestBody StockEventRequest request) {
        try {
            log.info("REQUEST RECEBIDO: {}", request); //debug de request do teste-local .http
            StockEvent event = toStockEvent(request);
            EventProcessingContext.enrich(event);
            log.info("event.received");
            EventStatus status = processStockEventUseCase.process(event);
            log.info("event.processed status={}", status);
            HttpStatus httpStatus = toHttpStatus(status);
            return ResponseEntity.status(httpStatus)
                    .body(StockEventResponse.of(request.eventId(), status.name(), toMessage(status)));
        } finally {
            EventProcessingContext.clear();
        }
    }

    private StockEvent toStockEvent(StockEventRequest r) {
        EventType type = EventType.valueOf(r.type());
        Quantity qty = type.resolveQuantity(r.quantity(), r.available(), r.quantitySent());
        return new StockEvent(
                EventId.of(r.eventId()),
                type,
                r.occurredAt(),
                AccountId.of(r.accountId()),
                Sku.of(r.sku()),
                r.marketplace(),
                r.externalOrderId(),
                qty,
                r.reason()
        );
    }

    private HttpStatus toHttpStatus(EventStatus status) {
        return switch (status) {
            case PROCESSED -> HttpStatus.OK;
            case IGNORED -> HttpStatus.OK;
            case PENDING -> HttpStatus.ACCEPTED;
            case INCONSISTENT -> HttpStatus.UNPROCESSABLE_ENTITY;
            // PROCESSING is a transient sentinel that must never reach this layer.
            // If it does, it indicates a serious bug (transaction rolled back mid-way).
            case PROCESSING -> HttpStatus.INTERNAL_SERVER_ERROR;
        };
    }

    private String toMessage(EventStatus status) {
        return switch (status) {
            case PROCESSED -> "Event processed successfully";
            case IGNORED -> "Event acknowledged; no balance change applied";
            case PENDING -> "Event received; waiting for prerequisite event";
            case INCONSISTENT -> "Event conflicts with current state; recorded but not applied";
            case PROCESSING -> "Internal error: event left in transient PROCESSING state";
        };
    }
}
