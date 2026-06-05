package com.gubee.stockreconciliation.adapter.in.web;

import com.gubee.stockreconciliation.adapter.in.web.dto.ProcessedEventResponse;
import com.gubee.stockreconciliation.adapter.in.web.dto.StockHistoryEntryResponse;
import com.gubee.stockreconciliation.adapter.in.web.dto.StockResponse;
import com.gubee.stockreconciliation.domain.model.ProcessedEvent;
import com.gubee.stockreconciliation.domain.model.Stock;
import com.gubee.stockreconciliation.domain.model.StockHistory;
import com.gubee.stockreconciliation.domain.model.enums.EventStatus;
import com.gubee.stockreconciliation.domain.model.vo.StockKey;
import com.gubee.stockreconciliation.domain.port.in.GetEventsUseCase;
import com.gubee.stockreconciliation.domain.port.in.GetStockHistoryUseCase;
import com.gubee.stockreconciliation.domain.port.in.GetStockUseCase;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@Tag(name = "Stocks", description = "Stock queries")
public class StockQueryController {

    private final GetStockUseCase getStockUseCase;
    private final GetStockHistoryUseCase getStockHistoryUseCase;
    private final GetEventsUseCase getEventsUseCase;

    public StockQueryController(GetStockUseCase getStockUseCase,
                                 GetStockHistoryUseCase getStockHistoryUseCase,
                                 GetEventsUseCase getEventsUseCase) {
        this.getStockUseCase = getStockUseCase;
        this.getStockHistoryUseCase = getStockHistoryUseCase;
        this.getEventsUseCase = getEventsUseCase;
    }

    @GetMapping("/stocks/{accountId}/{sku}")
    @Operation(summary = "Get current stock balance for an account and SKU")
    public ResponseEntity<StockResponse> getStock(@PathVariable String accountId, @PathVariable String sku) {
        return getStockUseCase.getStock(StockKey.of(accountId, sku))
                .map(this::toResponse)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/stocks/{accountId}/{sku}/history")
    @Operation(summary = "Get the full audit history for an account and SKU")
    public ResponseEntity<List<StockHistoryEntryResponse>> getHistory(@PathVariable String accountId,
                                                                       @PathVariable String sku) {
        List<StockHistoryEntryResponse> history = getStockHistoryUseCase.getHistory(StockKey.of(accountId, sku))
                .stream().map(this::toHistoryResponse).toList();
        return ResponseEntity.ok(history);
    }

    @GetMapping("/events")
    @Operation(summary = "Query processed events by status (PENDING, INCONSISTENT, PROCESSED, IGNORED)")
    public ResponseEntity<List<ProcessedEventResponse>> getEvents(
            @RequestParam(defaultValue = "PENDING") String status) {
        EventStatus eventStatus = EventStatus.valueOf(status.toUpperCase());
        List<ProcessedEventResponse> events = getEventsUseCase.getByStatus(eventStatus)
                .stream().map(this::toEventResponse).toList();
        return ResponseEntity.ok(events);
    }

    private StockResponse toResponse(Stock stock) {
        return new StockResponse(
                stock.getAccountId().value(),
                stock.getSku().value(),
                stock.getAvailableQuantity().value(),
                stock.getLastUpdatedAt()
        );
    }

    private StockHistoryEntryResponse toHistoryResponse(StockHistory h) {
        return new StockHistoryEntryResponse(
                h.getEventId().value(),
                h.getEventType().name(),
                h.getQuantityBefore().value(),
                h.getQuantityAfter().value(),
                h.getDelta(),
                h.getOccurredAt(),
                h.getMarketplace(),
                h.getExternalOrderId(),
                h.getReason()
        );
    }

    private ProcessedEventResponse toEventResponse(ProcessedEvent pe) {
        return new ProcessedEventResponse(
                pe.getEventId().value(),
                pe.getEventType().name(),
                pe.getStatus().name(),
                pe.getAccountId().value(),
                pe.getSku().value(),
                pe.getOccurredAt(),
                pe.getProcessedAt(),
                pe.getDetails()
        );
    }
}
