package com.claybar.application.api;

public class ApplicationNotFoundException extends RuntimeException {

    public ApplicationNotFoundException(String id) {
        super("Loan application " + id + " not found");
    }
}
