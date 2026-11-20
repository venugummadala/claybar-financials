package com.claybar.kyc;

/** Outcome of identity / watch-list screening. */
public record KycResult(Status status, String reason) {

    public enum Status {
        /** Identity verified, no watch-list hit. */
        VERIFIED,
        /** Hard failure: sanctions hit, under age or missing identity. Loan must be rejected. */
        FAILED,
        /** Soft hit (PEP / adverse media): needs a human decision. */
        REFER
    }
}
