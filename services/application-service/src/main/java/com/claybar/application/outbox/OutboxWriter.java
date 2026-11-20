package com.claybar.application.outbox;

import com.claybar.events.EventCodec;
import com.claybar.events.EventEnvelope;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OutboxWriter {

    private final OutboxRepository repository;
    private final EventCodec codec;

    public OutboxWriter(OutboxRepository repository, EventCodec codec) {
        this.repository = repository;
        this.codec = codec;
    }

    /** Must join the caller's transaction: the event is only as durable as the change it describes. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void append(String topic, EventEnvelope envelope) {
        repository.save(new OutboxEvent(envelope.eventId(), topic, envelope.correlationId(), envelope.eventType(),
                codec.encode(envelope)));
    }
}
