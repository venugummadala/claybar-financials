package com.claybar.application.messaging;

import com.claybar.application.domain.LoanApplicationRepository;
import com.claybar.application.domain.LoanStatus;
import com.claybar.events.CorrelationContext;
import com.claybar.events.EventCodec;
import com.claybar.events.EventEnvelope;
import com.claybar.events.EventTypes;
import com.claybar.events.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.stream.Stream;

/**
 * Projects workflow decisions onto the application's status (read model for the customer API).
 * Idempotent: the event id is recorded in the same transaction as the status change.
 */
@Component
public class LoanDecisionProjector {

    private static final Logger log = LoggerFactory.getLogger(LoanDecisionProjector.class);

    private final EventCodec codec;
    private final LoanApplicationRepository applications;
    private final ProcessedEventRepository processed;

    public LoanDecisionProjector(EventCodec codec, LoanApplicationRepository applications, ProcessedEventRepository processed) {
        this.codec = codec;
        this.applications = applications;
        this.processed = processed;
    }

    /**
     * Transactional here, not only on {@link #apply}: a self-invocation bypasses the Spring proxy, so
     * without this the status change would be made on a detached entity and silently lost.
     */
    @KafkaListener(topics = Topics.LOAN_DECISIONS)
    @Transactional
    public void onDecision(ConsumerRecord<String, String> record) {
        apply(codec.decode(record.value()));
    }

    @Transactional
    public void apply(EventEnvelope event) {
        LoanStatus next = switch (event.eventType()) {
            case EventTypes.LOAN_UNDER_REVIEW -> LoanStatus.UNDER_REVIEW;
            case EventTypes.LOAN_APPROVED -> LoanStatus.APPROVED;
            case EventTypes.LOAN_REJECTED -> LoanStatus.REJECTED;
            default -> null;
        };
        if (next == null) {
            return;
        }
        try (var ignored = CorrelationContext.open(event)) {
            if (processed.existsById(event.eventId())) {
                log.info("Skipping already-processed {} [{}]", event.eventType(), event.eventId());
                return;
            }
            applications.findById(event.correlationId()).ifPresentOrElse(
                    app -> {
                        if (app.transitionTo(next, reason(event))) {
                            log.info("Application {} -> {}", app.getId(), next);
                        }
                    },
                    () -> log.warn("Decision {} for unknown application {}", event.eventType(), event.correlationId()));
            processed.save(new ProcessedEvent(event.eventId(), event.eventType()));
        }
    }

    private static String reason(EventEnvelope e) {
        return Stream.of("underwriterComment", "kycReason")
                .map(e::payloadString)
                .filter(s -> s != null && !s.isBlank())
                .findFirst()
                .orElseGet(() -> e.payloadString("riskDecision") == null ? null : "Risk decision: " + e.payloadString("riskDecision"));
    }
}
