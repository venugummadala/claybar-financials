package com.claybar.credit;

/**
 * Port to an external credit bureau (Experian, Equifax, TransUnion...). The default implementation
 * is a deterministic simulator; a real adapter would add a circuit breaker and request timeouts.
 */
public interface CreditBureauClient {

    CreditReport fetchReport(String nationalId, double annualIncome, double requestedAmount);
}
