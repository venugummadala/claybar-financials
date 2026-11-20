package com.claybar.application.outbox;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "outbox_event")
public class OutboxEvent {

    @Id
    private String id;
    private String topic;
    private String messageKey;
    private String eventType;
    private String envelope;
    private Instant createdAt;
    private Instant publishedAt;
    private int attempts;
    private String lastError;

    protected OutboxEvent() {
    }

    public OutboxEvent(String id, String topic, String messageKey, String eventType, String envelope) {
        this.id = id;
        this.topic = topic;
        this.messageKey = messageKey;
        this.eventType = eventType;
        this.envelope = envelope;
        this.createdAt = Instant.now();
    }

    void markPublished() {
        this.publishedAt = Instant.now();
        this.attempts++;
        this.lastError = null;
    }

    void markFailed(String error) {
        this.attempts++;
        this.lastError = error == null ? null : error.substring(0, Math.min(error.length(), 1000));
    }

    public String getId() { return id; }
    public String getTopic() { return topic; }
    public String getMessageKey() { return messageKey; }
    public String getEventType() { return eventType; }
    public String getEnvelope() { return envelope; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getPublishedAt() { return publishedAt; }
    public int getAttempts() { return attempts; }
    public String getLastError() { return lastError; }
}
