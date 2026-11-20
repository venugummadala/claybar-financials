package com.claybar.workflow;

import com.claybar.events.CorrelationContext;
import com.claybar.events.EventCodec;
import com.claybar.events.EventEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * Consumes the configured inbound topics and hands each event to {@link WorkflowEventCorrelator}.
 * Failures propagate to the shared Kafka error handler (retry with back-off, then dead-letter).
 */
public class WorkflowInboundListener {

    private final EventCodec codec;
    private final WorkflowEventCorrelator correlator;

    public WorkflowInboundListener(EventCodec codec, WorkflowEventCorrelator correlator) {
        this.codec = codec;
        this.correlator = correlator;
    }

    @KafkaListener(id = "claybar-workflow-inbound",
            topics = "#{@claybarWorkflowProperties.inboundTopicArray()}",
            groupId = "#{@claybarWorkflowProperties.consumerGroup ?: '${spring.application.name:claybar-workflow}'}")
    public void onEvent(ConsumerRecord<String, String> record) {
        EventEnvelope event = codec.decode(record.value());
        try (var ignored = CorrelationContext.open(event)) {
            correlator.handle(event);
        }
    }
}
