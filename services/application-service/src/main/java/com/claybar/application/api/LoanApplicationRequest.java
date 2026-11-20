package com.claybar.application.api;

import com.claybar.application.domain.SubmitLoanApplication;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Past;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;
import java.time.LocalDate;

public record LoanApplicationRequest(
        @NotBlank @Size(max = 200) @Schema(example = "Ada Lovelace") String applicantName,
        @NotBlank @Size(max = 50) @Schema(example = "PRIME-AB123456", description = "Demo: PRIME-/NEAR-/SUB- prefixes force a bureau score") String nationalId,
        @NotBlank @Email @Size(max = 320) @Schema(example = "ada@example.com") String email,
        @NotNull @Past @Schema(example = "1985-12-10") LocalDate dateOfBirth,
        @NotNull @DecimalMin("1000") @DecimalMax("1000000") @Schema(example = "20000") BigDecimal loanAmount,
        @Min(6) @Max(360) @Schema(example = "36") int termMonths,
        @NotNull @PositiveOrZero @Schema(example = "90000") BigDecimal annualIncome,
        @Size(max = 100) @Schema(example = "Home improvement") String purpose) {

    SubmitLoanApplication toCommand() {
        return new SubmitLoanApplication(applicantName.trim(), nationalId.trim(), email.trim(), dateOfBirth,
                loanAmount, termMonths, annualIncome, purpose);
    }
}
