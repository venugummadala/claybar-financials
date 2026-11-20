package com.claybar.application.outbox;

import com.claybar.events.EventCodec;
import com.claybar.events.EventPublisher;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Publishes pending outbox rows to Kafka in creation order. Stops at the first failure so events
 * for the same application are never reordered; the next poll retries from there.
 * Delivery is at-least-once (crash after send, before commit = resend); consumers are idempotent.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository repository;
    private final EventPublisher publisher;
    private final EventCodec codec;
    private final OutboxProperties props;

    public OutboxRelay(OutboxRepository repository, EventPublisher publisher, EventCodec codec,
                       OutboxProperties props, MeterRegistry meters) {
        this.repository = repository;
        this.publisher = publisher;
        this.codec = codec;
        this.props = props;
        Gauge.builder("claybar.outbox.pending", repository, OutboxRepository::countByPublishedAtIsNull)
                .description("Outbox rows not yet published to Kafka").register(meters);
    }

    /** @return number of rows published */
    @Transactional
    public int relayBatch() {
        List<OutboxEvent> batch = repository.claimPending(PageRequest.of(0, props.batchSize()));
        int published = 0;
        for (OutboxEvent row : batch) {
            try {
                publisher.publishAndWait(row.getTopic(), codec.decode(row.getEnvelope()), props.sendTimeout());
                row.markPublished();
                published++;
            } catch (RuntimeException e) {
                row.markFailed(e.getMessage());
                log.warn("Outbox relay stopped at {} ({}), attempt {}: {}", row.getId(), row.getEventType(),
                        row.getAttempts(), e.getMessage());
                break;
            }
        }
        return published;
    }

    @Transactional
    public int purgePublished() {
        return repository.deletePublishedBefore(Instant.now().minus(props.retention()));
    }
}
