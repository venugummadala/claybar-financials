# Stateful services: transactional outbox and idempotent consumers

Reference implementation: `services/application-service` (`outbox/`, `messaging/` packages, Flyway `V1__*.sql`).

## Why

Writing to the database and then sending to Kafka are two systems. Commit-then-send loses the event if the JVM
dies in between; send-then-commit publishes events for changes that rolled back. The outbox writes the event as a
row **in the same transaction** as the business change; a relay publishes rows afterwards.

## Schema (portable PostgreSQL/H2)

```sql
CREATE TABLE outbox_event (
    id            VARCHAR(36)  NOT NULL PRIMARY KEY,   -- = eventId
    topic         VARCHAR(200) NOT NULL,
    message_key   VARCHAR(100) NOT NULL,               -- = correlationId
    event_type    VARCHAR(100) NOT NULL,
    envelope      VARCHAR      NOT NULL,               -- JSON EventEnvelope
    created_at    TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    published_at  TIMESTAMP(6) WITH TIME ZONE,
    attempts      INTEGER      NOT NULL DEFAULT 0,
    last_error    VARCHAR(1000)
);
CREATE INDEX idx_outbox_pending ON outbox_event (published_at, created_at);

CREATE TABLE processed_event (
    event_id      VARCHAR(64)  NOT NULL PRIMARY KEY,
    event_type    VARCHAR(100) NOT NULL,
    processed_at  TIMESTAMP(6) WITH TIME ZONE NOT NULL
);
```

## Writer — must join the business transaction

```java
@Transactional(propagation = Propagation.MANDATORY)   // fails fast if someone calls it outside a tx
public void append(String topic, EventEnvelope envelope) {
    repository.save(new OutboxEvent(envelope.eventId(), topic, envelope.correlationId(), envelope.eventType(),
            codec.encode(envelope)));
}
```

## Relay

```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))   // SKIP LOCKED: replicas co-operate
@Query("select o from OutboxEvent o where o.publishedAt is null order by o.createdAt")
List<OutboxEvent> claimPending(Pageable page);

@Transactional
public int relayBatch() {
    int published = 0;
    for (OutboxEvent row : repository.claimPending(PageRequest.of(0, batchSize))) {
        try {
            publisher.publishAndWait(row.getTopic(), codec.decode(row.getEnvelope()), sendTimeout);
            row.markPublished();
            published++;
        } catch (RuntimeException e) {
            row.markFailed(e.getMessage());
            break;                      // stop: never publish a later event of the same key before an earlier one
        }
    }
    return published;
}
```
Schedule it with `@Scheduled(fixedDelayString = "${claybar.outbox.poll-interval:PT0.5S}")` in a separate
`@ConditionalOnProperty` configuration so tests can disable the scheduler and call `relayBatch()` directly.
Expose a gauge of pending rows and purge published rows after a retention period.

## Idempotent consumer

```java
@KafkaListener(topics = Topics.LOAN_DECISIONS)
@Transactional                                   // on the listener itself, not only on a helper it calls
public void onDecision(ConsumerRecord<String, String> record) {
    EventEnvelope event = codec.decode(record.value());
    if (processed.existsById(event.eventId())) return;
    // …apply the change (make state transitions monotonic: final states stay final)…
    processed.save(new ProcessedEvent(event.eventId(), event.eventType()));
}
```

## Client idempotency

Accept an `Idempotency-Key` header on create endpoints, store it in a unique column, and return the original
resource for a repeated key; map a unique-constraint race to 409 so the client retries.
