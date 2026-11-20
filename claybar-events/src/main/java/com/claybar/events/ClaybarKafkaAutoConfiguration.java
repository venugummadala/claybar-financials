package com.claybar.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.ArrayList;
import java.util.List;

/**
 * Wires Claybar's Kafka conventions into any Spring Boot service that has this jar on the classpath:
 * <ul>
 *   <li>{@link EventPublisher} + {@link EventCodec}</li>
 *   <li>a {@link DefaultErrorHandler} with exponential back-off, dead-lettering to {@code <topic>.DLT}
 *       on the same partition, and immediate dead-lettering of poison messages</li>
 *   <li>topic provisioning (opt-out with {@code claybar.kafka.create-topics=false})</li>
 * </ul>
 */
@AutoConfiguration(after = KafkaAutoConfiguration.class)
@ConditionalOnClass(KafkaTemplate.class)
@EnableConfigurationProperties(ClaybarKafkaProperties.class)
public class ClaybarKafkaAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ClaybarKafkaAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean
    public EventCodec eventCodec(ObjectProvider<ObjectMapper> objectMapper) {
        return new EventCodec(objectMapper.getIfAvailable(ObjectMapper::new));
    }

    @Bean
    @ConditionalOnMissingBean
    public EventPublisher eventPublisher(KafkaTemplate<String, String> kafkaTemplate, EventCodec codec) {
        return new EventPublisher(kafkaTemplate, codec);
    }

    @Bean
    @ConditionalOnMissingBean(CommonErrorHandler.class)
    public DefaultErrorHandler claybarKafkaErrorHandler(KafkaOperations<?, ?> kafkaOperations, ClaybarKafkaProperties props) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaOperations,
                (record, ex) -> {
                    log.error("Dead-lettering record {}-{}@{} key={}: {}", record.topic(), record.partition(),
                            record.offset(), record.key(), rootMessage(ex));
                    return new TopicPartition(record.topic() + Topics.DLT_SUFFIX, record.partition());
                });

        ExponentialBackOff backOff = new ExponentialBackOff(props.getRetry().getInitialInterval().toMillis(), 2.0);
        backOff.setMaxInterval(props.getRetry().getMaxInterval().toMillis());
        backOff.setMaxAttempts((int) props.getRetry().getMaxAttempts());

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(NonRetryableEventException.class, DeserializationException.class,
                ConversionException.class);
        return handler;
    }

    @Bean
    @ConditionalOnProperty(prefix = "claybar.kafka", name = "create-topics", havingValue = "true", matchIfMissing = true)
    public KafkaAdmin.NewTopics claybarTopics(ClaybarKafkaProperties props) {
        List<NewTopic> topics = new ArrayList<>();
        for (String name : Topics.ALL) {
            topics.add(TopicBuilder.name(name).partitions(props.getPartitions()).replicas(props.getReplicationFactor()).build());
            topics.add(TopicBuilder.name(name + Topics.DLT_SUFFIX).partitions(props.getPartitions())
                    .replicas(props.getReplicationFactor()).build());
        }
        return new KafkaAdmin.NewTopics(topics.toArray(NewTopic[]::new));
    }

    private static String rootMessage(Throwable ex) {
        Throwable t = ex;
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return t.getClass().getSimpleName() + ": " + t.getMessage();
    }
}
