package com.claybar.workflow;

/** Result of routing one inbound event into the engine. */
public enum CorrelationOutcome {
    /** A new process instance was started. */
    STARTED,
    /** The event was delivered to a waiting process instance. */
    CORRELATED,
    /** A start event for a business key that already has (or had) an instance — redelivery. */
    DUPLICATE_START,
    /** No instance is waiting for this message: duplicate delivery or reply after a timeout. */
    NO_MATCH
}
