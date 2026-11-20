package com.claybar.events;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/**
 * Publishes {@link EventEnvelope}s to Kafka.
 *
 * <p><b>Transaction semantics:</b> when called inside a Spring-managed transaction the send is
 * deferred until <em>after commit</em>. This guarantees consumers never observe an event whose
 * originating state change was rolled back (e.g. a Camunda job that failed and will be retried),
 * and — for the workflow engine — that a reply can never overtake the commit that created the
 * message subscription waiting for it. The remaining gap (commit succeeded, send failed) is
 * covered by process-level timeouts; see ADR-0003.
 */
public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final EventCodec codec;

    public EventPublisher(KafkaTemplate<String, String> kafkaTemplate, EventCodec codec) {
        this.kafkaTemplate = kafkaTemplate;
        this.codec = codec;
    }

    public void publish(String topic, EventEnvelope envelope) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    send(topic, envelope);
                }
            });
            log.debug("Deferred {} [{}] until transaction commit", envelope.eventType(), envelope.eventId());
        } else {
            send(topic, envelope);
        }
    }

    /**
     * Sends immediately and blocks until the broker acknowledges. Use from Kafka listeners so that a
     * failed send fails the listener, which is then retried instead of the offset being committed.
     *
     * @throws IllegalStateException if the broker does not acknowledge within {@code timeout}
     */
    public void publishAndWait(String topic, EventEnvelope envelope, java.time.Duration timeout) {
        try {
            send(topic, envelope).get(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing " + envelope.eventType(), e);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("Broker did not acknowledge " + envelope.eventType()
                    + " [" + envelope.eventId() + "]", e);
        }
    }

    /** Sends immediately, regardless of any surrounding transaction. */
    public CompletableFuture<?> send(String topic, EventEnvelope envelope) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, envelope.correlationId(), codec.encode(envelope));
        header(record, EventHeaders.EVENT_ID, envelope.eventId());
        header(record, EventHeaders.EVENT_TYPE, envelope.eventType());
        header(record, EventHeaders.CORRELATION_ID, envelope.correlationId());
        header(record, EventHeaders.SOURCE, envelope.source());

        return kafkaTemplate.send(record).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish {} [{}] correlationId={} to {}", envelope.eventType(),
                        envelope.eventId(), envelope.correlationId(), topic, ex);
            } else {
                log.info("Published {} [{}] correlationId={} to {}-{}@{}", envelope.eventType(), envelope.eventId(),
                        envelope.correlationId(), topic, result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }

    private static void header(ProducerRecord<String, String> record, String key, String value) {
        if (value != null) {
            record.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
