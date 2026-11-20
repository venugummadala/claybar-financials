package com.claybar.events;

/** Kafka record headers duplicated from the envelope so infrastructure can route/filter without parsing JSON. */
public final class EventHeaders {

    public static final String EVENT_ID = "claybar-event-id";
    public static final String EVENT_TYPE = "claybar-event-type";
    public static final String CORRELATION_ID = "claybar-correlation-id";
    public static final String SOURCE = "claybar-source";

    /** MDC key used for log correlation across services. */
    public static final String MDC_CORRELATION_ID = "correlationId";

    private EventHeaders() {
    }
}
