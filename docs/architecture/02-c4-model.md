# 2. C4 model

Four zoom levels ([c4model.com](https://c4model.com)): context → containers → components → code.
Diagrams use Mermaid flowcharts with C4 notation: **blue** = our software, **grey** = external, cylinders = data stores,
`[Container: technology]` in each box.

```mermaid
flowchart LR
    classDef person fill:#08427b,color:#fff,stroke:#052e56
    classDef system fill:#1168bd,color:#fff,stroke:#0b4884
    classDef ext fill:#999,color:#fff,stroke:#6b6b6b
    P((Person)):::person --- S[Software system]:::system --- E[External system]:::ext
```

## Level 1 — System context

```mermaid
flowchart TB
    classDef person fill:#08427b,color:#fff,stroke:#052e56
    classDef system fill:#1168bd,color:#fff,stroke:#0b4884
    classDef ext fill:#999,color:#fff,stroke:#6b6b6b

    applicant(("Applicant<br/>[Person]")):::person
    channel["Customer channels<br/>[External: web, mobile, broker]"]:::ext
    underwriter(("Underwriter<br/>[Person]")):::person
    ops(("Loan operations<br/>[Person]")):::person
    risk(("Credit risk analyst<br/>[Person]")):::person

    claybar["Claybar Loan Origination<br/>[Software system]<br/>Takes applications, runs checks,<br/>decides, notifies"]:::system

    kycp["KYC / screening provider<br/>[External system]"]:::ext
    bureau["Credit bureau<br/>[External system]"]:::ext
    mail["E-mail / alerting<br/>[External system]"]:::ext

    applicant -->|applies via| channel
    channel -->|"submits application, polls status<br/>[HTTPS/JSON]"| claybar
    underwriter -->|"decides borderline cases<br/>[Camunda Tasklist]"| claybar
    ops -->|"monitors, resolves incidents<br/>[Camunda Cockpit]"| claybar
    risk -->|"maintains decision rules<br/>[DMN]"| claybar
    claybar -->|screens applicant| kycp
    claybar -->|requests credit report| bureau
    claybar -->|sends decisions and alerts| mail
    mail -->|e-mail| applicant
    mail -->|alerts| ops
```

> In this repository the KYC provider, credit bureau and e-mail gateway are simulated inside the KYC, credit and
> notification services behind ports (`KycScreeningService`, `CreditBureauClient`, `NotificationSender`).

## Level 2 — Containers

```mermaid
flowchart TB
    classDef person fill:#08427b,color:#fff,stroke:#052e56
    classDef container fill:#438dd5,color:#fff,stroke:#2e6295
    classDef db fill:#438dd5,color:#fff,stroke:#2e6295
    classDef bus fill:#85bbf0,color:#000,stroke:#5d82a8
    classDef ext fill:#999,color:#fff,stroke:#6b6b6b

    channel["Customer channels<br/>[External]"]:::ext
    staff(("Underwriter / Ops<br/>[Person]")):::person

    subgraph claybar["Claybar Loan Origination"]
        app["application-service<br/>[Container: Spring Boot 3.3]<br/>Intake API, system of record,<br/>outbox, status projection"]:::container
        appdb[("applications DB<br/>[PostgreSQL 16]<br/>loan_application, outbox_event,<br/>processed_event")]:::db
        orch["orchestrator-service<br/>[Container: Spring Boot + Camunda 7.22]<br/>BPMN/DMN engine, Tasklist,<br/>Cockpit, REST, workflow plug-in"]:::container
        camdb[("camunda DB<br/>[PostgreSQL 16]<br/>runtime + history")]:::db
        kafka{{"Event backbone<br/>[Apache Kafka 3.8, KRaft]<br/>7 topics + DLTs"}}:::bus
        kyc["kyc-service<br/>[Container: Spring Boot]<br/>Sanctions / PEP / age"]:::container
        credit["credit-service<br/>[Container: Spring Boot]<br/>Bureau adapter"]:::container
        notif["notification-service<br/>[Container: Spring Boot]<br/>Customer e-mails, ops alerts"]:::container
    end

    channel -->|"REST /api/v1/loan-applications"| app
    staff -->|"HTTPS /camunda"| orch
    app -->|"JPA / Flyway"| appdb
    orch -->|JDBC| camdb
    app <-->|"publishes LoanApplicationSubmitted<br/>consumes loan.decisions"| kafka
    orch <-->|"commands, decisions, lifecycle<br/>consumes applications, kyc/credit events"| kafka
    kyc <-->|"kyc.commands → kyc.events"| kafka
    credit <-->|"credit.commands → credit.events"| kafka
    notif -->|"consumes loan.decisions, workflow.events"| kafka
```

Rules visible at this level:

- **No service calls another service.** All interaction goes through Kafka; the only synchronous entry points are the
  intake API and the Camunda UIs/REST.
- **Database per service.** Only the application service and the orchestrator hold state; KYC, credit and
  notification are stateless.

### Topic ownership

| Topic | Producer | Consumers |
|---|---|---|
| `claybar.loan.applications` | application-service (outbox) | orchestrator |
| `claybar.kyc.commands` | orchestrator | kyc-service |
| `claybar.kyc.events` | kyc-service | orchestrator |
| `claybar.credit.commands` | orchestrator | credit-service |
| `claybar.credit.events` | credit-service | orchestrator |
| `claybar.loan.decisions` | orchestrator | application-service, notification-service |
| `claybar.workflow.events` | orchestrator (engine plug-in) | notification-service (+ any analytics) |
| `<topic>.DLT` | shared error handler | operators |

## Level 3 — Components

### orchestrator-service

```mermaid
flowchart TB
    classDef comp fill:#85bbf0,color:#000,stroke:#5d82a8
    classDef plugin fill:#c7e0f4,color:#000,stroke:#5d82a8,stroke-dasharray:4 3
    classDef ext fill:#999,color:#fff,stroke:#6b6b6b

    kin{{"Kafka: applications,<br/>kyc.events, credit.events"}}:::ext
    kout{{"Kafka: commands,<br/>decisions, workflow.events"}}:::ext
    db[("camunda DB")]:::ext
    users(("Underwriter / Ops")):::ext

    subgraph orch["orchestrator-service"]
        subgraph plug["claybar-workflow plug-in (starter)"]
            lis["WorkflowInboundListener<br/>[@KafkaListener]"]:::plugin
            cor["WorkflowEventCorrelator<br/>start or correlate, idempotent"]:::plugin
            map["ProcessVariableMapper<br/>payload ⇄ variables"]:::plugin
            del["KafkaCommandDelegate<br/>[JavaDelegate 'kafkaCommandDelegate']"]:::plugin
            hist["KafkaLifecycleHistoryEventHandler<br/>via ClaybarKafkaEventsProcessEnginePlugin"]:::plugin
        end
        pub["EventPublisher<br/>[claybar-events] after-commit send"]:::comp
        eng["Camunda process engine<br/>loan-origination.bpmn<br/>loan-risk-assessment.dmn<br/>job executor"]:::comp
        sla["LoanSlaProperties<br/>bean 'loanSla' (timer values)"]:::comp
        web["Camunda webapps + engine-rest<br/>EngineRestSecurityConfiguration (Basic)"]:::comp
        idi["UnderwritingIdentityInitializer<br/>(demo only)"]:::comp
    end

    kin --> lis --> cor --> eng
    cor --> map
    eng -->|send tasks| del --> pub --> kout
    del --> map
    eng -->|history events| hist --> pub
    eng -->|"${loanSla.*}"| sla
    eng --> db
    users --> web --> eng
    idi --> eng
```

### application-service

```mermaid
flowchart TB
    classDef comp fill:#85bbf0,color:#000,stroke:#5d82a8
    classDef ext fill:#999,color:#fff,stroke:#6b6b6b

    client(("Channel")):::ext
    db[("applications DB")]:::ext
    kout{{"Kafka: loan.applications"}}:::ext
    kin{{"Kafka: loan.decisions"}}:::ext

    subgraph app["application-service"]
        ctl["LoanApplicationController<br/>[REST, OpenAPI]"]:::comp
        err["ApiExceptionHandler<br/>RFC 9457 problem details"]:::comp
        svc["LoanApplicationService<br/>[@Transactional]"]:::comp
        repo["LoanApplicationRepository<br/>[Spring Data JPA]"]:::comp
        ow["OutboxWriter<br/>(MANDATORY tx)"]:::comp
        orepo["OutboxRepository<br/>claimPending SKIP LOCKED"]:::comp
        sch["OutboxScheduler<br/>[@Scheduled]"]:::comp
        rel["OutboxRelay"]:::comp
        proj["LoanDecisionProjector<br/>[@KafkaListener, @Transactional]"]:::comp
        prepo["ProcessedEventRepository"]:::comp
    end

    client --> ctl --> svc
    ctl -.-> err
    svc --> repo --> db
    svc --> ow --> orepo --> db
    sch --> rel --> orepo
    rel -->|publishAndWait| kout
    kin --> proj --> repo
    proj --> prepo --> db
```

### kyc / credit / notification services

Same shape, no database:

```mermaid
flowchart LR
    classDef comp fill:#85bbf0,color:#000,stroke:#5d82a8
    classDef ext fill:#999,color:#fff,stroke:#6b6b6b
    in{{"commands / events topic"}}:::ext --> L["*Listener<br/>[@KafkaListener]<br/>decode, MDC, idempotency"]:::comp
    L --> D["Domain logic<br/>KycScreeningService ·<br/>CreditBureauClient ·<br/>NotificationComposer"]:::comp
    D --> P["Port to outside world<br/>(simulated)"]:::comp
    L -->|"publishAndWait (kyc, credit)<br/>NotificationSender (notification)"| out{{"reply topic / e-mail"}}:::ext
```

## Level 4 — Code (the workflow plug-in)

```mermaid
classDiagram
    direction LR
    class ClaybarWorkflowAutoConfiguration {
        +claybarWorkflowProperties()
        +kafkaCommandDelegate()
        +workflowEventCorrelator()
        +workflowInboundListener()
        +claybarKafkaEventsProcessEnginePlugin()
    }
    class ClaybarWorkflowProperties {
        inboundTopics: String
        startEvents: List~String~
        lifecycleEvents: LifecycleEvents
        +inboundTopicArray() String[]
    }
    class WorkflowInboundListener {
        +onEvent(ConsumerRecord)
    }
    class WorkflowEventCorrelator {
        -startEvents: Set~String~
        +handle(EventEnvelope) CorrelationOutcome
        -start(EventEnvelope)
        -correlate(EventEnvelope)
    }
    class CorrelationOutcome {
        <<enumeration>>
        STARTED
        CORRELATED
        DUPLICATE_START
        NO_MATCH
    }
    class KafkaCommandDelegate {
        +execute(DelegateExecution)
    }
    class ProcessVariableMapper {
        +toVariables(Map) Map
        +select(Map, String) Map
    }
    class ClaybarKafkaEventsProcessEnginePlugin {
        +preInit(ProcessEngineConfigurationImpl)
    }
    class KafkaLifecycleHistoryEventHandler {
        +handleEvent(HistoryEvent)
        ~toEnvelope(HistoryEvent) EventEnvelope
    }
    class JavaDelegate {
        <<interface>>
    }
    class HistoryEventHandler {
        <<interface>>
    }
    class AbstractProcessEnginePlugin {
        <<abstract>>
    }
    class EventPublisher {
        +publish(topic, EventEnvelope)
        +publishAndWait(topic, EventEnvelope, Duration)
        +send(topic, EventEnvelope) CompletableFuture
    }

    ClaybarWorkflowAutoConfiguration ..> ClaybarWorkflowProperties
    ClaybarWorkflowAutoConfiguration ..> WorkflowInboundListener
    ClaybarWorkflowAutoConfiguration ..> KafkaCommandDelegate
    ClaybarWorkflowAutoConfiguration ..> ClaybarKafkaEventsProcessEnginePlugin
    WorkflowInboundListener --> WorkflowEventCorrelator
    WorkflowEventCorrelator --> ProcessVariableMapper
    WorkflowEventCorrelator ..> CorrelationOutcome
    KafkaCommandDelegate ..|> JavaDelegate
    KafkaCommandDelegate --> EventPublisher
    KafkaCommandDelegate --> ProcessVariableMapper
    ClaybarKafkaEventsProcessEnginePlugin --|> AbstractProcessEnginePlugin
    ClaybarKafkaEventsProcessEnginePlugin --> KafkaLifecycleHistoryEventHandler : registers (lazily)
    KafkaLifecycleHistoryEventHandler ..|> HistoryEventHandler
    KafkaLifecycleHistoryEventHandler --> EventPublisher
```
