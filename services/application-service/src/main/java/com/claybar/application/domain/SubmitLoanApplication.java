package com.claybar.application.domain;

import java.math.BigDecimal;
import java.time.LocalDate;

public record SubmitLoanApplication(String applicantName, String nationalId, String email, LocalDate dateOfBirth,
                                    BigDecimal loanAmount, int termMonths, BigDecimal annualIncome, String purpose) {
}
