package com.claybar.workflow;

import com.claybar.events.EventEnvelope;
import com.claybar.events.EventPublisher;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Generic BPMN send-task implementation that publishes a command/event to Kafka.
 *
 * <p>Configured per activity through <b>input parameters</b> (not field injection, which is not
 * thread-safe on singleton beans):
 *
 * <pre>{@code
 * <bpmn:sendTask id="RequestKyc" camunda:asyncBefore="true" camunda:delegateExpression="${kafkaCommandDelegate}">
 *   <bpmn:extensionElements>
 *     <camunda:inputOutput>
 *       <camunda:inputParameter name="topic">claybar.kyc.commands</camunda:inputParameter>
 *       <camunda:inputParameter name="eventType">KycRequested</camunda:inputParameter>
 *       <camunda:inputParameter name="payloadVariables">applicantName,nationalId</camunda:inputParameter>
 *     </camunda:inputOutput>
 *   </bpmn:extensionElements>
 * </bpmn:sendTask>
 * }</pre>
 *
 * The process business key becomes the correlation id (and Kafka key). Publishing is deferred
 * until the engine transaction commits, so the reply can never arrive before the subscription
 * of the following receive task exists.
 */
public class KafkaCommandDelegate implements JavaDelegate {

    private static final Logger log = LoggerFactory.getLogger(KafkaCommandDelegate.class);

    private final EventPublisher publisher;
    private final ProcessVariableMapper mapper;
    private final String source;

    public KafkaCommandDelegate(EventPublisher publisher, ProcessVariableMapper mapper, String source) {
        this.publisher = publisher;
        this.mapper = mapper;
        this.source = source;
    }

    @Override
    public void execute(DelegateExecution execution) {
        String topic = required(execution, WorkflowVariables.TOPIC);
        String eventType = required(execution, WorkflowVariables.EVENT_TYPE);
        String payloadVariables = (String) execution.getVariableLocal(WorkflowVariables.PAYLOAD_VARIABLES);

        String businessKey = execution.getProcessBusinessKey();
        if (businessKey == null) {
            throw new IllegalStateException("Process instance " + execution.getProcessInstanceId()
                    + " has no business key; it is required as Kafka correlation id");
        }

        Map<String, Object> payload = new LinkedHashMap<>(mapper.select(execution.getVariables(), payloadVariables));
        payload.put("processInstanceId", execution.getProcessInstanceId());

        String causationId = (String) execution.getVariable(WorkflowVariables.LAST_INBOUND_EVENT_ID);
        EventEnvelope envelope = new EventEnvelope(UUID.randomUUID().toString(), eventType,
                EventEnvelope.CURRENT_VERSION, source, businessKey, causationId, Instant.now(), payload);

        log.info("Activity {} publishing {} for businessKey={}", execution.getCurrentActivityId(), eventType, businessKey);
        publisher.publish(topic, envelope);
    }

    private static String required(DelegateExecution execution, String name) {
        Object value = execution.getVariableLocal(name);
        if (value == null || value.toString().isBlank()) {
            throw new IllegalStateException("Activity " + execution.getCurrentActivityId()
                    + " is missing input parameter '" + name + "'");
        }
        return value.toString();
    }
}
