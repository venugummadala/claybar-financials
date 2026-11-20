package com.claybar.kyc;

import com.claybar.events.CorrelationContext;
import com.claybar.events.EventCodec;
import com.claybar.events.EventEnvelope;
import com.claybar.events.EventPublisher;
import com.claybar.events.EventTypes;
import com.claybar.events.Topics;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class KycCommandListener {

    private static final Logger log = LoggerFactory.getLogger(KycCommandListener.class);
    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

    private final EventCodec codec;
    private final EventPublisher publisher;
    private final KycScreeningService screening;
    private final MeterRegistry meters;

    public KycCommandListener(EventCodec codec, EventPublisher publisher, KycScreeningService screening, MeterRegistry meters) {
        this.codec = codec;
        this.publisher = publisher;
        this.screening = screening;
        this.meters = meters;
    }

    @KafkaListener(topics = Topics.KYC_COMMANDS)
    public void onCommand(ConsumerRecord<String, String> record) {
        EventEnvelope command = codec.decode(record.value());
        if (!EventTypes.KYC_REQUESTED.equals(command.eventType())) {
            log.debug("Ignoring {} on {}", command.eventType(), record.topic());
            return;
        }
        try (var ignored = CorrelationContext.open(command)) {
            KycResult result = screening.screen(command.payloadString("applicantName"),
                    command.payloadString("nationalId"), command.payloadString("dateOfBirth"));

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("kycStatus", result.status().name());
            payload.put("kycReason", result.reason());

            // Block until acked: if the reply cannot be published the command is retried, not lost.
            publisher.publishAndWait(Topics.KYC_EVENTS,
                    EventEnvelope.causedBy(command, EventTypes.KYC_COMPLETED, "kyc-service", payload), SEND_TIMEOUT);
            meters.counter("claybar.kyc.screenings", "status", result.status().name()).increment();
            log.info("KYC for {} -> {} ({})", command.correlationId(), result.status(), result.reason());
        }
    }
}
