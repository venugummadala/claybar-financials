package com.claybar.events;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventCodecTest {

    private final EventCodec codec = new EventCodec();

    @Test
    void roundTripsEnvelope() {
        EventEnvelope original = EventEnvelope.of(EventTypes.KYC_REQUESTED, "APP-1", "test",
                Map.of("applicantName", "Ada Lovelace", "loanAmount", 25000));

        EventEnvelope decoded = codec.decode(codec.encode(original));

        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.payloadNumber("loanAmount").intValue()).isEqualTo(25000);
    }

    @Test
    void causedByInheritsCorrelationAndLinksCausation() {
        EventEnvelope root = EventEnvelope.of(EventTypes.KYC_REQUESTED, "APP-1", "orchestrator", Map.of());
        EventEnvelope reply = EventEnvelope.causedBy(root, EventTypes.KYC_COMPLETED, "kyc-service", Map.of());

        assertThat(reply.correlationId()).isEqualTo("APP-1");
        assertThat(reply.causationId()).isEqualTo(root.eventId());
    }

    @Test
    void malformedJsonIsPoison() {
        assertThatThrownBy(() -> codec.decode("{not json")).isInstanceOf(NonRetryableEventException.class);
        assertThatThrownBy(() -> codec.decode("")).isInstanceOf(NonRetryableEventException.class);
    }

    @Test
    void missingMandatoryFieldIsPoison() {
        assertThatThrownBy(() -> codec.decode("{\"eventType\":\"X\",\"correlationId\":\"A\"}"))
                .isInstanceOf(NonRetryableEventException.class);
    }

    @Test
    void futureSchemaVersionIsPoison() {
        String json = "{\"eventId\":\"1\",\"eventType\":\"X\",\"version\":99,\"correlationId\":\"A\"}";
        assertThatThrownBy(() -> codec.decode(json)).isInstanceOf(NonRetryableEventException.class)
                .hasMessageContaining("Unsupported event version");
    }
}
