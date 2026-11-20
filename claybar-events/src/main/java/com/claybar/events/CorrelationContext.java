package com.claybar.events;

import org.slf4j.MDC;

/**
 * Puts the business correlation id into the logging MDC for the duration of a block, so every log
 * line written while handling an event can be joined across services.
 */
public final class CorrelationContext implements AutoCloseable {

    private final String previous;

    private CorrelationContext(String correlationId) {
        this.previous = MDC.get(EventHeaders.MDC_CORRELATION_ID);
        if (correlationId != null) {
            MDC.put(EventHeaders.MDC_CORRELATION_ID, correlationId);
        }
    }

    public static CorrelationContext open(String correlationId) {
        return new CorrelationContext(correlationId);
    }

    public static CorrelationContext open(EventEnvelope envelope) {
        return new CorrelationContext(envelope.correlationId());
    }

    @Override
    public void close() {
        if (previous == null) {
            MDC.remove(EventHeaders.MDC_CORRELATION_ID);
        } else {
            MDC.put(EventHeaders.MDC_CORRELATION_ID, previous);
        }
    }
}
