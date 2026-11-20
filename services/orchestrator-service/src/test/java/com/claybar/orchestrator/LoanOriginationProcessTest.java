package com.claybar.orchestrator;

import com.claybar.events.EventEnvelope;
import com.claybar.events.EventPublisher;
import com.claybar.events.EventTypes;
import com.claybar.events.Topics;
import com.claybar.workflow.CorrelationOutcome;
import com.claybar.workflow.WorkflowEventCorrelator;
import org.camunda.bpm.engine.ManagementService;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.TaskService;
import org.camunda.bpm.engine.runtime.Job;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.engine.test.assertions.bpmn.BpmnAwareTests;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.camunda.bpm.engine.test.assertions.bpmn.BpmnAwareTests.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

/**
 * Runs the real BPMN + DMN on an in-memory engine. Kafka is replaced at the seam: inbound events are
 * fed straight into {@link WorkflowEventCorrelator} and outbound commands are captured from a mocked
 * {@link EventPublisher}. Async continuations are driven manually (job executor disabled) so every
 * transaction boundary of the model is exercised deterministically.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK, properties = {
        "camunda.bpm.job-execution.enabled=false",
        "claybar.kafka.create-topics=false",
        "claybar.workflow.inbound-topics=",
        "spring.datasource.url=jdbc:h2:mem:loan-process-test;DB_CLOSE_ON_EXIT=FALSE",
})
class LoanOriginationProcessTest {

    @Autowired ProcessEngine processEngine;
    @Autowired RuntimeService runtimeService;
    @Autowired ManagementService managementService;
    @Autowired TaskService taskService;
    @Autowired WorkflowEventCorrelator correlator;

    @MockBean EventPublisher publisher;

    private String applicationId;

    @BeforeEach
    void setUp() {
        BpmnAwareTests.init(processEngine);
        applicationId = "APP-" + UUID.randomUUID();
    }

    @Test
    void primeApplicantIsAutoApproved() {
        ProcessInstance pi = submit(20000, 90000);
        assertThat(pi).isWaitingAt("Task_RequestKyc", "Task_RequestCredit");

        runJobs();
        assertThat(pi).isWaitingAt("Task_AwaitKyc", "Task_AwaitCredit");
        assertThat(publishedTypes()).contains(EventTypes.KYC_REQUESTED, EventTypes.CREDIT_CHECK_REQUESTED);

        reply(EventTypes.KYC_COMPLETED, Map.of("kycStatus", "VERIFIED"));
        reply(EventTypes.CREDIT_CHECK_COMPLETED, Map.of("creditStatus", "COMPLETED", "creditScore", 780));
        runJobs();

        assertThat(pi).isEnded().hasPassed("Task_AssessRisk", "Task_PublishApproved", "End_Approved")
                .hasNotPassed("UserTask_Underwrite");
        assertThat(publishedTypes()).contains(EventTypes.LOAN_APPROVED);
    }

    @Test
    void nearPrimeApplicantGoesToUnderwriterWhoRejects() {
        ProcessInstance pi = submit(20000, 90000);
        runJobs();
        reply(EventTypes.KYC_COMPLETED, Map.of("kycStatus", "VERIFIED"));
        reply(EventTypes.CREDIT_CHECK_COMPLETED, Map.of("creditStatus", "COMPLETED", "creditScore", 650));
        runJobs();

        assertThat(pi).isWaitingAt("UserTask_Underwrite");
        assertThat(publishedTypes()).contains(EventTypes.LOAN_UNDER_REVIEW);
        assertThat(BpmnAwareTests.task(pi)).hasCandidateGroup("underwriters");

        BpmnAwareTests.complete(BpmnAwareTests.task(pi),
                Map.of("underwritingApproved", false, "underwriterComment", "Unstable employment history"));
        runJobs();

        assertThat(pi).isEnded().hasPassed("End_Rejected");
        assertThat(publishedTypes()).contains(EventTypes.LOAN_REJECTED);
    }

    @Test
    void kycTimeoutRoutesToManualReview() {
        ProcessInstance pi = submit(20000, 90000);
        runJobs();
        reply(EventTypes.CREDIT_CHECK_COMPLETED, Map.of("creditStatus", "COMPLETED", "creditScore", 800));
        runJobs();

        Job kycTimer = managementService.createJobQuery().processInstanceId(pi.getId()).timers()
                .activityId("Timer_KycTimeout").singleResult();
        assertThat(kycTimer).isNotNull();
        managementService.executeJob(kycTimer.getId());
        runJobs();

        assertThat(pi).isWaitingAt("UserTask_Underwrite").variables().containsEntry("kycStatus", "TIMEOUT");
    }

    @Test
    void failedKycIsRejectedWithoutUnderwriter() {
        ProcessInstance pi = submit(20000, 90000);
        runJobs();
        reply(EventTypes.KYC_COMPLETED, Map.of("kycStatus", "FAILED", "kycReason", "Sanctions list match"));
        reply(EventTypes.CREDIT_CHECK_COMPLETED, Map.of("creditStatus", "COMPLETED", "creditScore", 820));
        runJobs();

        assertThat(pi).isEnded().hasPassed("End_Rejected").hasNotPassed("UserTask_Underwrite");
    }

    @Test
    void redeliveredEventsAreIdempotent() {
        EventEnvelope submitted = submittedEvent(20000, 90000);
        assertThat(correlator.handle(submitted)).isEqualTo(CorrelationOutcome.STARTED);
        assertThat(correlator.handle(submitted)).isEqualTo(CorrelationOutcome.DUPLICATE_START);
        assertThat(runtimeService.createProcessInstanceQuery().processInstanceBusinessKey(applicationId).count()).isOne();

        runJobs();
        EventEnvelope kyc = event(EventTypes.KYC_COMPLETED, Map.of("kycStatus", "VERIFIED"));
        assertThat(correlator.handle(kyc)).isEqualTo(CorrelationOutcome.CORRELATED);
        assertThat(correlator.handle(kyc)).isEqualTo(CorrelationOutcome.NO_MATCH);
    }

    @Test
    void outboundCommandsCarryBusinessKeyAndSelectedPayload() {
        submit(20000, 90000);
        runJobs();

        ArgumentCaptor<EventEnvelope> captor = ArgumentCaptor.forClass(EventEnvelope.class);
        verify(publisher, atLeastOnce()).publish(anyString(), captor.capture());
        EventEnvelope creditRequest = captor.getAllValues().stream()
                .filter(e -> e.eventType().equals(EventTypes.CREDIT_CHECK_REQUESTED)).findFirst().orElseThrow();

        assertThat(creditRequest.correlationId()).isEqualTo(applicationId);
        assertThat(creditRequest.payload()).containsKeys("annualIncome", "loanAmount", "nationalId", "processInstanceId")
                .doesNotContainKey("email");
        verify(publisher).publish(org.mockito.ArgumentMatchers.eq(Topics.CREDIT_COMMANDS), any());
    }

    // ---------------------------------------------------------------- helpers

    private ProcessInstance submit(double amount, double income) {
        assertThat(correlator.handle(submittedEvent(amount, income))).isEqualTo(CorrelationOutcome.STARTED);
        return runtimeService.createProcessInstanceQuery().processInstanceBusinessKey(applicationId).singleResult();
    }

    private EventEnvelope submittedEvent(double amount, double income) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("applicantName", "Ada Lovelace");
        payload.put("nationalId", "AB123456C");
        payload.put("email", "ada@example.com");
        payload.put("dateOfBirth", "1985-12-10");
        payload.put("loanAmount", amount);
        payload.put("termMonths", 36);
        payload.put("annualIncome", income);
        return EventEnvelope.of(EventTypes.LOAN_APPLICATION_SUBMITTED, applicationId, "test", payload);
    }

    private void reply(String type, Map<String, Object> payload) {
        assertThat(correlator.handle(event(type, payload))).isEqualTo(CorrelationOutcome.CORRELATED);
    }

    private EventEnvelope event(String type, Map<String, Object> payload) {
        return EventEnvelope.of(type, applicationId, "test", payload);
    }

    /** Executes due async continuations until none are left (timers in the future stay untouched). */
    private void runJobs() {
        ProcessInstance pi = runtimeService.createProcessInstanceQuery().processInstanceBusinessKey(applicationId)
                .singleResult();
        if (pi == null) {
            return;
        }
        for (int guard = 0; guard < 50; guard++) {
            List<Job> jobs = managementService.createJobQuery().processInstanceId(pi.getId())
                    .executable().messages().list();
            if (jobs.isEmpty()) {
                return;
            }
            jobs.forEach(job -> managementService.executeJob(job.getId()));
        }
        throw new IllegalStateException("Job loop did not settle");
    }

    private List<String> publishedTypes() {
        ArgumentCaptor<EventEnvelope> captor = ArgumentCaptor.forClass(EventEnvelope.class);
        verify(publisher, atLeastOnce()).publish(anyString(), captor.capture());
        return captor.getAllValues().stream().map(EventEnvelope::eventType).toList();
    }
}
