package com.claybar.workflow;

import com.claybar.events.ClaybarKafkaAutoConfiguration;
import com.claybar.events.ClaybarKafkaProperties;
import com.claybar.events.EventCodec;
import com.claybar.events.EventPublisher;
import com.claybar.events.EventSource;
import io.micrometer.core.instrument.MeterRegistry;
import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.RuntimeService;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import java.util.HashSet;

/**
 * Auto-configuration of the Claybar workflow plug-in. Adding the starter to a Spring Boot + Camunda
 * application provides:
 * <ul>
 *   <li>{@code kafkaCommandDelegate} — generic BPMN send-task delegate publishing to Kafka</li>
 *   <li>{@link WorkflowEventCorrelator} + inbound listener — Kafka events start/continue processes</li>
 *   <li>{@link ClaybarKafkaEventsProcessEnginePlugin} — lifecycle events streamed to Kafka</li>
 * </ul>
 * Each piece can be replaced by declaring a bean of the same type, or switched off by property.
 */
@AutoConfiguration(after = ClaybarKafkaAutoConfiguration.class)
@ConditionalOnClass(ProcessEngine.class)
@ConditionalOnProperty(prefix = "claybar.workflow", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ClaybarWorkflowAutoConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "claybar.workflow")
    public ClaybarWorkflowProperties claybarWorkflowProperties() {
        return new ClaybarWorkflowProperties();
    }

    @Bean
    @ConditionalOnMissingBean
    public ProcessVariableMapper processVariableMapper() {
        return new ProcessVariableMapper();
    }

    @Bean("kafkaCommandDelegate")
    @ConditionalOnMissingBean(KafkaCommandDelegate.class)
    public KafkaCommandDelegate kafkaCommandDelegate(EventPublisher publisher, ProcessVariableMapper mapper,
                                                     ClaybarKafkaProperties kafkaProps, Environment env) {
        return new KafkaCommandDelegate(publisher, mapper, EventSource.resolve(kafkaProps, env));
    }

    @Bean
    @ConditionalOnMissingBean
    public WorkflowEventCorrelator workflowEventCorrelator(RuntimeService runtimeService, HistoryService historyService,
                                                           ProcessVariableMapper mapper, ClaybarWorkflowProperties props,
                                                           ObjectProvider<MeterRegistry> meterRegistry) {
        return new WorkflowEventCorrelator(runtimeService, historyService, mapper,
                new HashSet<>(props.getStartEvents()), meterRegistry.getIfAvailable());
    }

    @Bean
    @ConditionalOnExpression("'${claybar.workflow.inbound-topics:}'.trim().length() > 0")
    public WorkflowInboundListener workflowInboundListener(EventCodec codec, WorkflowEventCorrelator correlator) {
        return new WorkflowInboundListener(codec, correlator);
    }

    @Bean
    @ConditionalOnProperty(prefix = "claybar.workflow.lifecycle-events", name = "enabled", havingValue = "true", matchIfMissing = true)
    public static ClaybarKafkaEventsProcessEnginePlugin claybarKafkaEventsProcessEnginePlugin(
            ObjectProvider<EventPublisher> publisher, Environment env) {
        // static + lazy lookup: engine plugins are created early; avoid pulling the whole graph forward.
        String topic = env.getProperty("claybar.workflow.lifecycle-events.topic", com.claybar.events.Topics.WORKFLOW_EVENTS);
        String source = env.getProperty("claybar.kafka.service-name", env.getProperty("spring.application.name", "workflow"));
        return new ClaybarKafkaEventsProcessEnginePlugin(new LazyHistoryEventHandler(
                () -> new KafkaLifecycleHistoryEventHandler(publisher.getObject(), topic, source)));
    }
}
