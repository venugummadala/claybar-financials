package com.claybar.notification;

import com.claybar.events.EventEnvelope;
import com.claybar.events.EventTypes;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NotificationComposerTest {

    private final NotificationProperties props = new NotificationProperties("ops@test", 3);
    private final NotificationComposer composer = new NotificationComposer(props);

    @Test
    void approvalEmailsTheApplicant() {
        var n = composer.compose(EventEnvelope.of(EventTypes.LOAN_APPROVED, "APP-1", "t",
                Map.of("applicantName", "Ada", "email", "ada@example.com", "loanAmount", 20000, "termMonths", 36))).orElseThrow();

        assertThat(n.channel()).isEqualTo(Notification.Channel.EMAIL);
        assertThat(n.recipient()).isEqualTo("ada@example.com");
        assertThat(n.body()).contains("Ada", "APP-1", "20000", "36");
    }

    @Test
    void slaBreachAndIncidentsAlertOperations() {
        assertThat(composer.compose(EventEnvelope.of(EventTypes.UNDERWRITING_SLA_BREACHED, "APP-1", "t", Map.of())))
                .get().extracting(Notification::recipient).isEqualTo("ops@test");
        assertThat(composer.compose(EventEnvelope.of(EventTypes.WORKFLOW_INCIDENT_CREATED, "APP-1", "t",
                Map.of("incidentType", "failedJob")))).get().extracting(Notification::channel)
                .isEqualTo(Notification.Channel.OPS_ALERT);
    }

    @Test
    void ignoresUninterestingEventsAndMissingEmail() {
        assertThat(composer.compose(EventEnvelope.of(EventTypes.WORKFLOW_PROCESS_STARTED, "APP-1", "t", Map.of()))).isEmpty();
        assertThat(composer.compose(EventEnvelope.of(EventTypes.LOAN_REJECTED, "APP-1", "t", Map.of()))).isEmpty();
    }

    @Test
    void dedupeCacheIsBounded() {
        ProcessedEventCache cache = new ProcessedEventCache(props);
        assertThat(cache.markIfNew("a")).isTrue();
        assertThat(cache.markIfNew("a")).isFalse();
        cache.markIfNew("b");
        cache.markIfNew("c");
        cache.markIfNew("d"); // evicts "a"
        assertThat(cache.markIfNew("a")).isTrue();
    }
}
