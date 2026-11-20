package com.claybar.application.domain;

import com.claybar.application.outbox.OutboxWriter;
import com.claybar.events.EventEnvelope;
import com.claybar.events.EventTypes;
import com.claybar.events.Topics;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
public class LoanApplicationService {

    private final LoanApplicationRepository repository;
    private final OutboxWriter outbox;

    public LoanApplicationService(LoanApplicationRepository repository, OutboxWriter outbox) {
        this.repository = repository;
        this.outbox = outbox;
    }

    /**
     * Persists the application and its {@code LoanApplicationSubmitted} event atomically.
     * Re-submitting with the same idempotency key returns the original application.
     */
    @Transactional
    public LoanApplication submit(SubmitLoanApplication cmd, String idempotencyKey) {
        if (idempotencyKey != null) {
            Optional<LoanApplication> existing = repository.findByIdempotencyKey(idempotencyKey);
            if (existing.isPresent()) {
                return existing.get();
            }
        }
        LoanApplication application = repository.save(new LoanApplication(newId(), idempotencyKey,
                cmd.applicantName(), cmd.nationalId(), cmd.email(), cmd.dateOfBirth(), cmd.loanAmount(),
                cmd.termMonths(), cmd.annualIncome(), cmd.purpose()));

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("applicantName", application.getApplicantName());
        payload.put("nationalId", application.getNationalId());
        payload.put("email", application.getEmail());
        payload.put("dateOfBirth", application.getDateOfBirth().toString());
        payload.put("loanAmount", application.getLoanAmount());
        payload.put("termMonths", application.getTermMonths());
        payload.put("annualIncome", application.getAnnualIncome());
        if (application.getPurpose() != null) {
            payload.put("purpose", application.getPurpose());
        }
        outbox.append(Topics.LOAN_APPLICATIONS, EventEnvelope.of(EventTypes.LOAN_APPLICATION_SUBMITTED,
                application.getId(), "application-service", payload));
        return application;
    }

    @Transactional(readOnly = true)
    public Optional<LoanApplication> find(String id) {
        return repository.findById(id);
    }

    private static String newId() {
        return "APP-" + LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE) + "-"
                + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }
}
