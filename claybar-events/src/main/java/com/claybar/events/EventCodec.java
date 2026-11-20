package com.claybar.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/** JSON (de)serialisation of {@link EventEnvelope}. Thread-safe. */
public class EventCodec {

    private final ObjectMapper mapper;

    public EventCodec() {
        this(new ObjectMapper());
    }

    public EventCodec(ObjectMapper base) {
        this.mapper = base.copy()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    public String encode(EventEnvelope envelope) {
        try {
            return mapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialise event " + envelope.eventId(), e);
        }
    }

    /**
     * @throws NonRetryableEventException if the record is not a valid envelope (poison message)
     */
    public EventEnvelope decode(String json) {
        if (json == null || json.isBlank()) {
            throw new NonRetryableEventException("Empty event record");
        }
        try {
            EventEnvelope envelope = mapper.readValue(json, EventEnvelope.class);
            if (envelope.version() > EventEnvelope.CURRENT_VERSION) {
                throw new NonRetryableEventException("Unsupported event version " + envelope.version()
                        + " for " + envelope.eventType());
            }
            return envelope;
        } catch (JsonProcessingException | NullPointerException e) {
            throw new NonRetryableEventException("Malformed event record: " + e.getMessage(), e);
        }
    }
}
