package com.claybar.workflow;

import com.claybar.events.EventEnvelope;
import io.micrometer.core.instrument.MeterRegistry;
import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Routes inbound Kafka events into Camunda.
 *
 * <ul>
 *   <li>Event types listed in {@code claybar.workflow.start-events} start a process through a message
 *       start event (message name = event type, business key = correlation id).</li>
 *   <li>All other events are correlated to the instance with that business key that is waiting for a
 *       message named after the event type.</li>
 * </ul>
 *
 * <p><b>Idempotency:</b> Kafka is at-least-once. Duplicate starts are detected through the runtime
 * and history business-key index. Duplicate replies find no subscription (the first delivery already
 * consumed it) and are acknowledged as {@link CorrelationOutcome#NO_MATCH}. Engine exceptions such as
 * optimistic locking propagate so the Kafka error handler retries the record.
 */
public class WorkflowEventCorrelator {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEventCorrelator.class);

    private final RuntimeService runtimeService;
    private final HistoryService historyService;
    private final ProcessVariableMapper mapper;
    private final Set<String> startEvents;
    private final MeterRegistry meterRegistry;

    public WorkflowEventCorrelator(RuntimeService runtimeService, HistoryService historyService,
                                   ProcessVariableMapper mapper, Set<String> startEvents, MeterRegistry meterRegistry) {
        this.runtimeService = runtimeService;
        this.historyService = historyService;
        this.mapper = mapper;
        this.startEvents = Set.copyOf(startEvents);
        this.meterRegistry = meterRegistry;
    }

    public CorrelationOutcome handle(EventEnvelope event) {
        CorrelationOutcome outcome = startEvents.contains(event.eventType()) ? start(event) : correlate(event);
        if (meterRegistry != null) {
            meterRegistry.counter("claybar.workflow.inbound", "eventType", event.eventType(),
                    "outcome", outcome.name()).increment();
        }
        return outcome;
    }

    private CorrelationOutcome start(EventEnvelope event) {
        String businessKey = event.correlationId();
        boolean exists = runtimeService.createProcessInstanceQuery().processInstanceBusinessKey(businessKey).count() > 0
                || historyService.createHistoricProcessInstanceQuery().processInstanceBusinessKey(businessKey).count() > 0;
        if (exists) {
            log.warn("Ignoring duplicate {} for businessKey={} (event {})", event.eventType(), businessKey, event.eventId());
            return CorrelationOutcome.DUPLICATE_START;
        }
        var instance = runtimeService.createMessageCorrelation(event.eventType())
                .processInstanceBusinessKey(businessKey)
                .setVariables(variables(event))
                .correlateStartMessage();
        log.info("Started process instance {} for businessKey={} from {}", instance.getId(), businessKey, event.eventType());
        return CorrelationOutcome.STARTED;
    }

    private CorrelationOutcome correlate(EventEnvelope event) {
        // correlateAllWithResult() returns an empty list instead of throwing when nothing waits; the
        // throwing variant makes the engine log ERROR (ENGINE-16004) on every harmless redelivery.
        var results = runtimeService.createMessageCorrelation(event.eventType())
                .processInstanceBusinessKey(event.correlationId())
                .setVariables(variables(event))
                .correlateAllWithResult();
        if (results.isEmpty()) {
            log.warn("No process waiting for {} with businessKey={} (event {}): duplicate or late reply - acknowledged",
                    event.eventType(), event.correlationId(), event.eventId());
            return CorrelationOutcome.NO_MATCH;
        }
        log.info("Correlated {} to businessKey={}", event.eventType(), event.correlationId());
        return CorrelationOutcome.CORRELATED;
    }

    private Map<String, Object> variables(EventEnvelope event) {
        Map<String, Object> vars = mapper.toVariables(event.payload());
        vars.remove("processInstanceId");
        vars.put(WorkflowVariables.LAST_INBOUND_EVENT_ID, event.eventId());
        return vars;
    }
}
