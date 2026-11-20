package com.claybar.application.domain;

public enum LoanStatus {
    SUBMITTED, UNDER_REVIEW, APPROVED, REJECTED;

    public boolean isFinal() {
        return this == APPROVED || this == REJECTED;
    }
}
