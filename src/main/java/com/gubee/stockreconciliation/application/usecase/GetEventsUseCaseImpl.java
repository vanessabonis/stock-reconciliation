package com.gubee.stockreconciliation.application.usecase;

import com.gubee.stockreconciliation.domain.model.ProcessedEvent;
import com.gubee.stockreconciliation.domain.model.enums.EventStatus;
import com.gubee.stockreconciliation.domain.port.in.GetEventsUseCase;
import com.gubee.stockreconciliation.domain.port.out.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@Transactional(readOnly = true)
public class GetEventsUseCaseImpl implements GetEventsUseCase {

    private final ProcessedEventRepository processedEventRepository;

    public GetEventsUseCaseImpl(ProcessedEventRepository processedEventRepository) {
        this.processedEventRepository = processedEventRepository;
    }

    @Override
    public List<ProcessedEvent> getByStatus(EventStatus status) {
        return processedEventRepository.findByStatus(status);
    }
}
