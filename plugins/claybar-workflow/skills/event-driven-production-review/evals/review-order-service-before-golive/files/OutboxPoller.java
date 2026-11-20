package com.example.orders;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Component
public class OutboxPoller {

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;

    public OutboxPoller(OutboxRepository outbox, KafkaTemplate<String, String> kafka) {
        this.outbox = outbox;
        this.kafka = kafka;
    }

    @Scheduled(fixedDelayString = "500ms")
    @Transactional
    public void poll() {
        for (OutboxRow row : outbox.findTop100ByPublishedAtIsNull()) {
            try {
                kafka.send(row.getTopic(), row.getKey(), row.getPayload());
                row.setPublishedAt(Instant.now());
            } catch (Exception e) {
                // try the next one
            }
        }
    }
}
