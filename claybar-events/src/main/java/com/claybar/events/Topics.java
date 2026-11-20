package com.claybar.events;

import java.util.List;

/**
 * Kafka topic names. Naming convention: {@code claybar.<bounded-context>.<commands|events>}.
 * Every topic has a companion dead-letter topic with the {@link #DLT_SUFFIX} suffix.
 */
public final class Topics {

    public static final String DLT_SUFFIX = ".DLT";

    public static final String LOAN_APPLICATIONS = "claybar.loan.applications";
    public static final String LOAN_DECISIONS = "claybar.loan.decisions";
    public static final String KYC_COMMANDS = "claybar.kyc.commands";
    public static final String KYC_EVENTS = "claybar.kyc.events";
    public static final String CREDIT_COMMANDS = "claybar.credit.commands";
    public static final String CREDIT_EVENTS = "claybar.credit.events";
    public static final String WORKFLOW_EVENTS = "claybar.workflow.events";

    public static final List<String> ALL = List.of(
            LOAN_APPLICATIONS, LOAN_DECISIONS, KYC_COMMANDS, KYC_EVENTS,
            CREDIT_COMMANDS, CREDIT_EVENTS, WORKFLOW_EVENTS);

    private Topics() {
    }
}
