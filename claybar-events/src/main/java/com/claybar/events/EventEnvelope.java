package com.claybar.events;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Canonical envelope for every event and command exchanged over Kafka.
 *
 * <p>The envelope is deliberately schema-light: routing metadata is strongly typed while the
 * business payload is a map, so services can evolve payloads additively without lock-step
 * releases. Breaking payload changes must bump {@link #version()}.
 *
 * @param eventId       globally unique id, used by consumers for idempotency
 * @param eventType     logical type, see {@link EventTypes}
 * @param version       payload schema version
 * @param source        producing service name
 * @param correlationId business key that ties every event of one business transaction together
 *                      (for loans: the application id). Also used as the Kafka record key so all
 *                      events of one application stay ordered within a partition.
 * @param causationId   id of the event that caused this one, or {@code null} for root events
 * @param occurredAt    producer timestamp
 * @param payload       business data
 */
public record EventEnvelope(
        String eventId,
        String eventType,
        int version,
        String source,
        String correlationId,
        String causationId,
        Instant occurredAt,
        Map<String, Object> payload) {

    public static final int CURRENT_VERSION = 1;

    public EventEnvelope {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(eventType, "eventType");
        Objects.requireNonNull(correlationId, "correlationId");
        occurredAt = occurredAt == null ? Instant.now() : occurredAt;
        payload = payload == null ? Map.of() : Map.copyOf(payload);
    }

    /** Creates a root event (no causation). */
    public static EventEnvelope of(String eventType, String correlationId, String source, Map<String, Object> payload) {
        return new EventEnvelope(UUID.randomUUID().toString(), eventType, CURRENT_VERSION, source,
                correlationId, null, Instant.now(), payload);
    }

    /** Creates an event caused by {@code cause}, inheriting its correlation id. */
    public static EventEnvelope causedBy(EventEnvelope cause, String eventType, String source, Map<String, Object> payload) {
        return new EventEnvelope(UUID.randomUUID().toString(), eventType, CURRENT_VERSION, source,
                cause.correlationId(), cause.eventId(), Instant.now(), payload);
    }

    public String payloadString(String key) {
        Object value = payload.get(key);
        return value == null ? null : value.toString();
    }

    public Number payloadNumber(String key) {
        Object value = payload.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof Number n) {
            return n;
        }
        try {
            return new java.math.BigDecimal(value.toString());
        } catch (NumberFormatException e) {
            throw new NonRetryableEventException("Payload field '" + key + "' is not numeric: " + value, e);
        }
    }
}
