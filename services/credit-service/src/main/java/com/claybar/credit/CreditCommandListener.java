package com.claybar.credit;

import com.claybar.events.CorrelationContext;
import com.claybar.events.EventCodec;
import com.claybar.events.EventEnvelope;
import com.claybar.events.EventPublisher;
import com.claybar.events.EventTypes;
import com.claybar.events.NonRetryableEventException;
import com.claybar.events.Topics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class CreditCommandListener {

    private static final Logger log = LoggerFactory.getLogger(CreditCommandListener.class);
    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

    private final EventCodec codec;
    private final EventPublisher publisher;
    private final CreditBureauClient bureau;
    private final CreditProperties props;
    private final Timer bureauTimer;

    public CreditCommandListener(EventCodec codec, EventPublisher publisher, CreditBureauClient bureau,
                                 CreditProperties props, MeterRegistry meters) {
        this.codec = codec;
        this.publisher = publisher;
        this.bureau = bureau;
        this.props = props;
        this.bureauTimer = Timer.builder("claybar.credit.bureau.latency").publishPercentileHistogram().register(meters);
    }

    @KafkaListener(topics = Topics.CREDIT_COMMANDS)
    public void onCommand(ConsumerRecord<String, String> record) throws InterruptedException {
        EventEnvelope command = codec.decode(record.value());
        if (!EventTypes.CREDIT_CHECK_REQUESTED.equals(command.eventType())) {
            return;
        }
        try (var ignored = CorrelationContext.open(command)) {
            String nationalId = command.payloadString("nationalId");
            if (nationalId == null || nationalId.isBlank()) {
                throw new NonRetryableEventException("CreditCheckRequested without nationalId for " + command.correlationId());
            }
            double income = number(command, "annualIncome");
            double amount = number(command, "loanAmount");

            if (!props.simulatedLatency().isZero()) {
                Thread.sleep(props.simulatedLatency().toMillis());
            }
            CreditReport report = bureauTimer.record(() -> bureau.fetchReport(nationalId, income, amount));

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("creditStatus", "COMPLETED");
            payload.put("creditScore", report.creditScore());
            payload.put("riskBand", report.riskBand().name());
            payload.put("debtToIncome", report.debtToIncome());
            payload.put("bureauReference", report.bureauReference());

            publisher.publishAndWait(Topics.CREDIT_EVENTS,
                    EventEnvelope.causedBy(command, EventTypes.CREDIT_CHECK_COMPLETED, "credit-service", payload), SEND_TIMEOUT);
            log.info("Credit check for {} -> score {} ({})", command.correlationId(), report.creditScore(), report.riskBand());
        }
    }

    private static double number(EventEnvelope e, String key) {
        Number n = e.payloadNumber(key);
        return n == null ? 0d : n.doubleValue();
    }
}
