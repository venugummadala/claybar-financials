package com.claybar.orchestrator;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;

/**
 * Service-level agreements of the loan-origination process, referenced from BPMN timer
 * expressions as {@code ${loanSla.kycTimeout}} so operations can tune them per environment
 * without redeploying the process model.
 */
public class LoanSlaProperties {

    private Duration kycTimeout = Duration.ofMinutes(10);
    private Duration creditTimeout = Duration.ofMinutes(10);
    private Duration underwritingSla = Duration.ofDays(2);

    /** ISO-8601 duration consumed by BPMN timers. */
    public String getKycTimeout() { return kycTimeout.toString(); }
    public String getCreditTimeout() { return creditTimeout.toString(); }
    public String getUnderwritingSla() { return underwritingSla.toString(); }

    /** Due date shown in Tasklist for the underwriting task. */
    public Date underwritingDueDate() { return Date.from(Instant.now().plus(underwritingSla)); }

    public void setKycTimeout(Duration kycTimeout) { this.kycTimeout = kycTimeout; }
    public void setCreditTimeout(Duration creditTimeout) { this.creditTimeout = creditTimeout; }
    public void setUnderwritingSla(Duration underwritingSla) { this.underwritingSla = underwritingSla; }
}
