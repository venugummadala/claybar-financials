package com.claybar.application.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

@Entity
@Table(name = "loan_application")
public class LoanApplication {

    @Id
    private String id;
    @Column(name = "idempotency_key", unique = true)
    private String idempotencyKey;
    private String applicantName;
    private String nationalId;
    private String email;
    private LocalDate dateOfBirth;
    private BigDecimal loanAmount;
    private int termMonths;
    private BigDecimal annualIncome;
    private String purpose;
    @Enumerated(EnumType.STRING)
    private LoanStatus status;
    private String decisionReason;
    private Instant createdAt;
    private Instant updatedAt;
    @Version
    private long version;

    protected LoanApplication() {
    }

    public LoanApplication(String id, String idempotencyKey, String applicantName, String nationalId, String email,
                           LocalDate dateOfBirth, BigDecimal loanAmount, int termMonths, BigDecimal annualIncome,
                           String purpose) {
        this.id = id;
        this.idempotencyKey = idempotencyKey;
        this.applicantName = applicantName;
        this.nationalId = nationalId;
        this.email = email;
        this.dateOfBirth = dateOfBirth;
        this.loanAmount = loanAmount;
        this.termMonths = termMonths;
        this.annualIncome = annualIncome;
        this.purpose = purpose;
        this.status = LoanStatus.SUBMITTED;
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
    }

    /**
     * Applies a status change coming from the workflow. Final states are sticky: an out-of-order
     * "under review" arriving after "approved" must not reopen the application.
     *
     * @return true if the state changed
     */
    public boolean transitionTo(LoanStatus next, String reason) {
        if (status.isFinal() || status == next) {
            return false;
        }
        this.status = next;
        this.decisionReason = reason;
        this.updatedAt = Instant.now();
        return true;
    }

    public String getId() { return id; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getApplicantName() { return applicantName; }
    public String getNationalId() { return nationalId; }
    public String getEmail() { return email; }
    public LocalDate getDateOfBirth() { return dateOfBirth; }
    public BigDecimal getLoanAmount() { return loanAmount; }
    public int getTermMonths() { return termMonths; }
    public BigDecimal getAnnualIncome() { return annualIncome; }
    public String getPurpose() { return purpose; }
    public LoanStatus getStatus() { return status; }
    public String getDecisionReason() { return decisionReason; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}
