# Testing processes and decisions

Two layers. Both run in `mvn verify` without Docker.

## DMN table test (pure, no Spring)

```java
class FraudRiskDecisionTest {
    private static DmnEngine dmnEngine;
    private static DmnDecision decision;

    @BeforeAll
    static void parse() throws Exception {
        dmnEngine = DmnEngineConfiguration.createDefaultDmnEngineConfiguration().buildEngine();
        try (InputStream in = FraudRiskDecisionTest.class.getResourceAsStream("/bpmn/loan-risk-assessment.dmn")) {
            decision = dmnEngine.parseDecision("loan-risk-assessment", in);
        }
    }

    @ParameterizedTest(name = "fraud={0} score={1} -> {2}")
    @CsvSource({
            "COMPLETED, 85, REJECT",   // one row per rule, plus boundaries
            "TIMEOUT,    0, REVIEW",
    })
    void decides(String fraudStatus, int fraudScore, String expected) {
        var result = dmnEngine.evaluateDecisionTable(decision, Variables.createVariables()
                .putValue("fraudStatus", fraudStatus)
                .putValue("fraudScore", fraudScore)
                /* …every other input the table reads, with "passing" values… */);
        assertThat((String) result.getSingleEntry()).isEqualTo(expected);
    }
}
```
When you add an input column, existing CSV rows need a value for it too — otherwise FEEL sees `null` and rows
that used to match may not.

## Process test (Spring Boot, in-memory engine, Kafka mocked at the seam)

Dependencies (test scope): `org.camunda.bpm:camunda-bpm-assert`, `spring-boot-starter-test`.

```java
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK, properties = {
        "camunda.bpm.job-execution.enabled=false",   // we drive async jobs ourselves
        "claybar.kafka.create-topics=false",          // no broker needed
        "claybar.workflow.inbound-topics=",           // no Kafka listener
        "spring.datasource.url=jdbc:h2:mem:process-test;DB_CLOSE_ON_EXIT=FALSE",
})
class LoanOriginationProcessTest {
    @Autowired ProcessEngine processEngine;
    @Autowired RuntimeService runtimeService;
    @Autowired ManagementService managementService;
    @Autowired WorkflowEventCorrelator correlator;
    @MockBean EventPublisher publisher;               // captures outbound commands

    private String businessKey;

    @BeforeEach void setUp() {
        BpmnAwareTests.init(processEngine);
        businessKey = "APP-" + UUID.randomUUID();
    }

    @Test
    void highFraudScoreIsRejected() {
        ProcessInstance pi = start();                                  // LoanApplicationSubmitted
        runJobs();                                                     // send tasks execute
        assertThat(publishedTypes()).contains("FraudCheckRequested");
        reply("FraudCheckCompleted", Map.of("fraudStatus", "COMPLETED", "fraudScore", 90));
        // …other replies the join needs…
        runJobs();
        assertThat(pi).isEnded().hasPassed("End_Rejected");
    }

    @Test
    void fraudTimeoutGoesToReview() {
        ProcessInstance pi = start();
        runJobs();
        Job timer = managementService.createJobQuery().processInstanceId(pi.getId())
                .timers().activityId("Timer_FraudTimeout").singleResult();
        managementService.executeJob(timer.getId());
        runJobs();
        assertThat(pi).variables().containsEntry("fraudStatus", "TIMEOUT");
    }

    // --- helpers ---
    private void reply(String type, Map<String, Object> payload) {
        assertThat(correlator.handle(EventEnvelope.of(type, businessKey, "test", payload)))
                .isEqualTo(CorrelationOutcome.CORRELATED);
    }

    /** Executes due async continuations until none are left; future timers are untouched. */
    private void runJobs() {
        ProcessInstance pi = runtimeService.createProcessInstanceQuery().processInstanceBusinessKey(businessKey).singleResult();
        if (pi == null) return;
        for (int guard = 0; guard < 50; guard++) {
            List<Job> jobs = managementService.createJobQuery().processInstanceId(pi.getId()).executable().messages().list();
            if (jobs.isEmpty()) return;
            jobs.forEach(j -> managementService.executeJob(j.getId()));
        }
        throw new IllegalStateException("Job loop did not settle");
    }

    private List<String> publishedTypes() {
        ArgumentCaptor<EventEnvelope> captor = ArgumentCaptor.forClass(EventEnvelope.class);
        verify(publisher, atLeastOnce()).publish(anyString(), captor.capture());
        return captor.getAllValues().stream().map(EventEnvelope::eventType).toList();
    }
}
```

Notes:
- `JobQuery` has no business-key filter — resolve the process instance id first.
- `webEnvironment = MOCK` is needed when the Camunda REST starter is on the classpath (it requires a servlet context).
- Use `assertThat(pi).isWaitingAt("Task_AwaitFraudCheck")` to prove the subscription exists before replying.
- Redelivery test: call `correlator.handle` twice with the same envelope → second returns `NO_MATCH`.
