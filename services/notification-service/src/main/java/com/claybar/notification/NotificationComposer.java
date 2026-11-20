package com.claybar.notification;

import com.claybar.events.EventEnvelope;
import com.claybar.events.EventTypes;
import org.springframework.stereotype.Component;

import java.util.Optional;

/** Maps domain events to customer / operations notifications. Pure function: easy to unit test. */
@Component
public class NotificationComposer {

    private final NotificationProperties props;

    public NotificationComposer(NotificationProperties props) {
        this.props = props;
    }

    public Optional<Notification> compose(EventEnvelope e) {
        String id = e.correlationId();
        String name = Optional.ofNullable(e.payloadString("applicantName")).orElse("Customer");
        String email = e.payloadString("email");
        return switch (e.eventType()) {
            case EventTypes.LOAN_APPROVED -> customer(email, id, "Your Claybar loan is approved",
                    "Dear %s, your loan application %s for %s over %s months has been approved."
                            .formatted(name, id, e.payloadString("loanAmount"), e.payloadString("termMonths")));
            case EventTypes.LOAN_REJECTED -> customer(email, id, "Update on your Claybar loan application",
                    "Dear %s, we are unable to offer you a loan for application %s at this time.".formatted(name, id));
            case EventTypes.LOAN_UNDER_REVIEW -> ops(id, "Loan " + id + " awaiting underwriting",
                    "Risk decision %s, credit score %s, KYC %s.".formatted(e.payloadString("riskDecision"),
                            e.payloadString("creditScore"), e.payloadString("kycStatus")));
            case EventTypes.UNDERWRITING_SLA_BREACHED -> ops(id, "SLA BREACH: underwriting of " + id,
                    "Underwriting for %s (%s) exceeded its SLA.".formatted(id, e.payloadString("loanAmount")));
            case EventTypes.WORKFLOW_INCIDENT_CREATED -> ops(id, "Workflow incident on " + id,
                    "%s at %s: %s".formatted(e.payloadString("incidentType"), e.payloadString("activityId"),
                            e.payloadString("incidentMessage")));
            default -> Optional.empty();
        };
    }

    private Optional<Notification> customer(String email, String id, String subject, String body) {
        if (email == null || email.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(new Notification(Notification.Channel.EMAIL, email, subject, body, id));
    }

    private Optional<Notification> ops(String id, String subject, String body) {
        return Optional.of(new Notification(Notification.Channel.OPS_ALERT, props.opsRecipient(), subject, body, id));
    }
}
