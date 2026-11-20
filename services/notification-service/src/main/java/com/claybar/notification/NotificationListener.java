package com.claybar.notification;

import com.claybar.events.CorrelationContext;
import com.claybar.events.EventCodec;
import com.claybar.events.EventEnvelope;
import com.claybar.events.Topics;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationListener {

    private final EventCodec codec;
    private final NotificationComposer composer;
    private final NotificationSender sender;
    private final ProcessedEventCache processed;
    private final MeterRegistry meters;

    public NotificationListener(EventCodec codec, NotificationComposer composer, NotificationSender sender,
                                ProcessedEventCache processed, MeterRegistry meters) {
        this.codec = codec;
        this.composer = composer;
        this.sender = sender;
        this.processed = processed;
        this.meters = meters;
    }

    @KafkaListener(topics = {Topics.LOAN_DECISIONS, Topics.WORKFLOW_EVENTS})
    public void onEvent(ConsumerRecord<String, String> record) {
        EventEnvelope event = codec.decode(record.value());
        try (var ignored = CorrelationContext.open(event)) {
            composer.compose(event).ifPresent(notification -> {
                if (processed.markIfNew(event.eventId())) {
                    sender.send(notification);
                    meters.counter("claybar.notifications.sent", "channel", notification.channel().name(),
                            "eventType", event.eventType()).increment();
                }
            });
        }
    }
}
