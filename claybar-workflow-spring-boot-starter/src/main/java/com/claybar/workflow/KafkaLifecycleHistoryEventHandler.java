package com.claybar.workflow;

import com.claybar.events.EventEnvelope;
import com.claybar.events.EventPublisher;
import com.claybar.events.EventTypes;
import org.camunda.bpm.engine.impl.context.Context;
import org.camunda.bpm.engine.impl.history.event.HistoricIncidentEventEntity;
import org.camunda.bpm.engine.impl.history.event.HistoricProcessInstanceEventEntity;
import org.camunda.bpm.engine.impl.history.event.HistoricTaskInstanceEventEntity;
import org.camunda.bpm.engine.impl.history.event.HistoryEvent;
import org.camunda.bpm.engine.impl.history.event.HistoryEventTypes;
import org.camunda.bpm.engine.impl.history.handler.HistoryEventHandler;
import org.camunda.bpm.engine.impl.interceptor.CommandContext;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Translates selected engine history events into Claybar lifecycle events on Kafka, giving every
 * downstream service (status projections, SLA dashboards, audit) a real-time view of the workflow
 * without polling the Camunda REST API.
 *
 * <p>Never throws: a broken event stream must not roll back business transactions in the engine.
 * Events are published after the engine transaction commits (see {@link EventPublisher}).
 */
public class KafkaLifecycleHistoryEventHandler implements HistoryEventHandler {

    private static final Logger log = LoggerFactory.getLogger(KafkaLifecycleHistoryEventHandler.class);

    private final EventPublisher publisher;
    private final String topic;
    private final String source;

    public KafkaLifecycleHistoryEventHandler(EventPublisher publisher, String topic, String source) {
        this.publisher = publisher;
        this.topic = topic;
        this.source = source;
    }

    @Override
    public void handleEvent(HistoryEvent event) {
        try {
            EventEnvelope envelope = toEnvelope(event);
            if (envelope != null) {
                publisher.publish(topic, envelope);
            }
        } catch (RuntimeException e) {
            log.error("Failed to publish lifecycle event {} for process instance {}", event.getEventType(),
                    event.getProcessInstanceId(), e);
        }
    }

    @Override
    public void handleEvents(List<HistoryEvent> events) {
        events.forEach(this::handleEvent);
    }

    EventEnvelope toEnvelope(HistoryEvent event) {
        String type = null;
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("processInstanceId", event.getProcessInstanceId());
        payload.put("processDefinitionKey", event.getProcessDefinitionKey());
        String businessKey = null;

        if (event instanceof HistoricProcessInstanceEventEntity pi) {
            if (event.isEventOfType(HistoryEventTypes.PROCESS_INSTANCE_START)) {
                type = EventTypes.WORKFLOW_PROCESS_STARTED;
            } else if (event.isEventOfType(HistoryEventTypes.PROCESS_INSTANCE_END)) {
                type = EventTypes.WORKFLOW_PROCESS_ENDED;
                payload.put("state", pi.getState());
                payload.put("endActivityId", pi.getEndActivityId());
                payload.put("durationMillis", pi.getDurationInMillis());
            }
            businessKey = pi.getBusinessKey();
        } else if (event instanceof HistoricTaskInstanceEventEntity task) {
            if (event.isEventOfType(HistoryEventTypes.TASK_INSTANCE_CREATE)) {
                type = EventTypes.WORKFLOW_TASK_CREATED;
            } else if (event.isEventOfType(HistoryEventTypes.TASK_INSTANCE_COMPLETE)) {
                type = EventTypes.WORKFLOW_TASK_COMPLETED;
                payload.put("assignee", task.getAssignee());
            }
            payload.put("taskId", task.getTaskId());
            payload.put("taskDefinitionKey", task.getTaskDefinitionKey());
            payload.put("taskName", task.getName());
        } else if (event instanceof HistoricIncidentEventEntity incident) {
            if (event.isEventOfType(HistoryEventTypes.INCIDENT_CREATE)) {
                type = EventTypes.WORKFLOW_INCIDENT_CREATED;
            } else if (event.isEventOfType(HistoryEventTypes.INCIDENT_RESOLVE)) {
                type = EventTypes.WORKFLOW_INCIDENT_RESOLVED;
            }
            payload.put("incidentType", incident.getIncidentType());
            payload.put("activityId", incident.getActivityId());
            payload.put("incidentMessage", incident.getIncidentMessage());
        }

        if (type == null) {
            return null;
        }
        if (businessKey == null) {
            businessKey = lookupBusinessKey(event.getProcessInstanceId());
        }
        payload.values().removeIf(java.util.Objects::isNull);
        return EventEnvelope.of(type, businessKey != null ? businessKey : String.valueOf(event.getProcessInstanceId()),
                source, payload);
    }

    private String lookupBusinessKey(String processInstanceId) {
        if (processInstanceId == null) {
            return null;
        }
        CommandContext ctx = Context.getCommandContext();
        if (ctx == null) {
            return null;
        }
        ExecutionEntity execution = ctx.getExecutionManager().findExecutionById(processInstanceId);
        return execution != null ? execution.getBusinessKey() : null;
    }
}
