package com.claybar.application.messaging;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "processed_event")
public class ProcessedEvent {

    @Id
    private String eventId;
    private String eventType;
    private Instant processedAt;

    protected ProcessedEvent() {
    }

    public ProcessedEvent(String eventId, String eventType) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.processedAt = Instant.now();
    }

    public String getEventId() { return eventId; }
}
