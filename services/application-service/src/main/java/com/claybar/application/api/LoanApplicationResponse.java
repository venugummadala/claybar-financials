package com.claybar.application.api;

import com.claybar.application.domain.LoanApplication;
import com.claybar.application.domain.LoanStatus;

import java.math.BigDecimal;
import java.time.Instant;

/** Public view of an application. Deliberately omits national id and date of birth (PII minimisation). */
public record LoanApplicationResponse(String id, String applicantName, BigDecimal loanAmount, int termMonths,
                                      LoanStatus status, String decisionReason, Instant createdAt, Instant updatedAt) {

    static LoanApplicationResponse from(LoanApplication a) {
        return new LoanApplicationResponse(a.getId(), a.getApplicantName(), a.getLoanAmount(), a.getTermMonths(),
                a.getStatus(), a.getDecisionReason(), a.getCreatedAt(), a.getUpdatedAt());
    }
}
