package com.claybar.events;

/**
 * Signals a poison message: retrying will never succeed (malformed JSON, missing mandatory fields,
 * unknown schema version). The Kafka error handler routes these straight to the dead-letter topic.
 */
public class NonRetryableEventException extends RuntimeException {

    public NonRetryableEventException(String message) {
        super(message);
    }

    public NonRetryableEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
