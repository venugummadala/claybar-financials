package com.claybar.workflow;

/** Well-known variable names used by the plug-in. */
public final class WorkflowVariables {

    /** Input parameter of a send task: Kafka topic to publish to. */
    public static final String TOPIC = "topic";
    /** Input parameter of a send task: event/command type to publish. */
    public static final String EVENT_TYPE = "eventType";
    /** Input parameter of a send task: comma-separated variable names forming the payload, or {@code *}. */
    public static final String PAYLOAD_VARIABLES = "payloadVariables";

    /** Process variable holding the id of the last inbound event (becomes causationId of the next command). */
    public static final String LAST_INBOUND_EVENT_ID = "lastInboundEventId";

    private WorkflowVariables() {
    }
}
