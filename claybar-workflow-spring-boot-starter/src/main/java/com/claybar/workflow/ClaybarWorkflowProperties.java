package com.claybar.workflow;

import com.claybar.events.Topics;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration of the Claybar workflow plug-in ({@code claybar.workflow.*}).
 *
 * <pre>
 * claybar:
 *   workflow:
 *     inbound-topics: claybar.loan.applications,claybar.kyc.events
 *     start-events: LoanApplicationSubmitted
 *     lifecycle-events:
 *       enabled: true
 * </pre>
 */
public class ClaybarWorkflowProperties {

    /** Master switch for the plug-in. */
    private boolean enabled = true;

    /**
     * Comma-separated topics whose events are routed into the engine. Leave empty to disable the
     * built-in inbound listener (e.g. when the host application consumes and calls
     * {@link WorkflowEventCorrelator} itself).
     */
    private String inboundTopics = "";

    /** Consumer group of the inbound listener. Defaults to spring.application.name. */
    private String consumerGroup;

    /**
     * Event types that <em>start</em> a process via a BPMN message start event whose message name
     * equals the event type. Every other inbound event is correlated to a waiting instance.
     */
    private List<String> startEvents = new ArrayList<>();

    private final LifecycleEvents lifecycleEvents = new LifecycleEvents();

    public static class LifecycleEvents {
        /** Stream process/task/incident lifecycle events to Kafka via the process engine plugin. */
        private boolean enabled = true;
        private String topic = Topics.WORKFLOW_EVENTS;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
    }

    public String[] inboundTopicArray() {
        return inboundTopics == null ? new String[0]
                : java.util.Arrays.stream(inboundTopics.split(",")).map(String::trim).filter(s -> !s.isEmpty())
                .toArray(String[]::new);
    }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public String getInboundTopics() { return inboundTopics; }
    public void setInboundTopics(String inboundTopics) { this.inboundTopics = inboundTopics; }
    public String getConsumerGroup() { return consumerGroup; }
    public void setConsumerGroup(String consumerGroup) { this.consumerGroup = consumerGroup; }
    public List<String> getStartEvents() { return startEvents; }
    public void setStartEvents(List<String> startEvents) { this.startEvents = startEvents; }
    public LifecycleEvents getLifecycleEvents() { return lifecycleEvents; }
}
