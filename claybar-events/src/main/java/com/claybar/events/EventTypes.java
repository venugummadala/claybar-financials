package com.claybar.events;

/** Logical event and command types. Commands are imperative ("Requested"), events are past tense. */
public final class EventTypes {

    // Loan intake
    public static final String LOAN_APPLICATION_SUBMITTED = "LoanApplicationSubmitted";

    // KYC
    public static final String KYC_REQUESTED = "KycRequested";
    public static final String KYC_COMPLETED = "KycCompleted";

    // Credit bureau
    public static final String CREDIT_CHECK_REQUESTED = "CreditCheckRequested";
    public static final String CREDIT_CHECK_COMPLETED = "CreditCheckCompleted";

    // Decisions
    public static final String LOAN_UNDER_REVIEW = "LoanUnderReview";
    public static final String LOAN_APPROVED = "LoanApproved";
    public static final String LOAN_REJECTED = "LoanRejected";
    public static final String UNDERWRITING_SLA_BREACHED = "UnderwritingSlaBreached";

    // Workflow engine lifecycle (published by the workflow plug-in)
    public static final String WORKFLOW_PROCESS_STARTED = "WorkflowProcessStarted";
    public static final String WORKFLOW_PROCESS_ENDED = "WorkflowProcessEnded";
    public static final String WORKFLOW_TASK_CREATED = "WorkflowTaskCreated";
    public static final String WORKFLOW_TASK_COMPLETED = "WorkflowTaskCompleted";
    public static final String WORKFLOW_INCIDENT_CREATED = "WorkflowIncidentCreated";
    public static final String WORKFLOW_INCIDENT_RESOLVED = "WorkflowIncidentResolved";

    private EventTypes() {
    }
}
