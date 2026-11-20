# 3. 4+1 architectural views

Kruchten's 4+1 model: four views, each for a different audience, tied together by scenarios (+1).

| View | Question it answers | Audience |
|---|---|---|
| [Logical](#31-logical-view) | What are the main abstractions and their relationships? | Developers, analysts |
| [Process](#32-process-view) | What runs concurrently, where are the transaction and wait boundaries? | Developers, SRE |
| [Development](#33-development-view) | How is the code organised and built? | Developers |
| [Physical](#34-physical-view) | What runs where? | Platform / SRE |
| [Scenarios](#35-scenarios-1) | Which use cases validate the views? | Everyone |

## 3.1 Logical view

Domain objects per bounded context. The `EventEnvelope` is the only type shared between contexts.

```mermaid
classDiagram
    direction TB
    namespace Intake {
        class LoanApplication {
            id: String  APP-yyyyMMdd-XXXXXXXX
            idempotencyKey: String
            applicantName, nationalId, email
            dateOfBirth: LocalDate
            loanAmount: BigDecimal
            termMonths: int
            annualIncome: BigDecimal
            status: LoanStatus
            decisionReason: String
            version: long
            +transitionTo(LoanStatus, reason) boolean
        }
        class LoanStatus {
            <<enumeration>>
            SUBMITTED
            UNDER_REVIEW
            APPROVED
            REJECTED
            +isFinal() boolean
        }
        class OutboxEvent {
            id, topic, messageKey, eventType
            envelope: JSON
            publishedAt, attempts, lastError
        }
        class ProcessedEvent {
            eventId
            eventType
            processedAt
        }
    }
    namespace Screening {
        class KycResult {
            status: VERIFIED|FAILED|REFER
            reason: String
        }
        class CreditReport {
            creditScore: int
            riskBand: PRIME|NEAR_PRIME|SUBPRIME
            debtToIncome: double
            bureauReference: String
        }
    }
    namespace Messaging {
        class EventEnvelope {
            eventId, eventType, version
            source, correlationId, causationId
            occurredAt: Instant
            payload: Map
        }
        class Notification {
            channel: EMAIL|OPS_ALERT
            recipient, subject, body
        }
    }
    LoanApplication --> LoanStatus
    LoanApplication "1" ..> "1..*" OutboxEvent : emits via
    OutboxEvent ..> EventEnvelope : serialised
    KycResult ..> EventEnvelope : KycCompleted
    CreditReport ..> EventEnvelope : CreditCheckCompleted
    EventEnvelope ..> Notification : composed into
```

The **loan-origination process instance** is the fourth aggregate. It lives in Camunda, keyed by the same
application id, with these variables:

| Group | Variables | Set by |
|---|---|---|
| Application | `applicantName, nationalId, email, dateOfBirth, loanAmount, termMonths, annualIncome, purpose` | `LoanApplicationSubmitted` |
| KYC | `kycStatus, kycReason` | `KycCompleted` or KYC timeout |
| Credit | `creditStatus, creditScore, riskBand, debtToIncome, bureauReference` | `CreditCheckCompleted` or credit timeout |
| Decision | `riskDecision` | DMN |
| Underwriting | `underwritingApproved, underwriterComment` | Tasklist form |
| Tracing | `lastInboundEventId` | plug-in (becomes next `causationId`) |

## 3.2 Process view

### Transaction and wait-state boundaries in the BPMN

Every ⏸ is a committed wait state; every ⚡ is an async continuation executed by the job executor. Kafka
sends happen *after* the commit of the transaction they belong to.

```mermaid
flowchart LR
    S((Start msg)) --> F{{"+"}}
    F --> RK["⚡ Request KYC<br/>send task"] --> AK["⏸ Await KYC<br/>receive task ⚡ after"]
    F --> RC["⚡ Request credit<br/>send task"] --> AC["⏸ Await credit<br/>receive task ⚡ after"]
    AK -. "⏰ timer" .-> TK[KYC TIMEOUT]
    AC -. "⏰ timer" .-> TC[Credit TIMEOUT]
    AK --> J{{"+ join"}}
    TK --> J
    AC --> J
    TC --> J
    J --> R[DMN] --> G{decision}
    G --> PA["⚡ Publish approved"]
    G --> PR["⚡ Publish rejected"]
    G --> UR["⚡ Publish under review"] --> U["⏸ Underwriting<br/>user task"]
    U -. "⏰ non-interrupting" .-> E["⚡ Publish SLA breach"]
    U --> PA
    U --> PR
```

| Tx | Triggered by | Work done | Commit then send |
|---|---|---|---|
| T1 | Kafka `LoanApplicationSubmitted` (listener thread) | create instance, fork, create 2 jobs | – |
| T2/T3 | job executor | send task → reach receive task (subscription) | `KycRequested` / `CreditCheckRequested` |
| T4/T5 | Kafka reply (listener thread) | set variables, create async-after job | – |
| T6/T7 | job executor | arrive at join (one waits, the other continues: DMN, gateway, next send/user task) | decision event |
| T8 | Tasklist complete | gateway, create publish job | – |

The parallel join is reached by two jobs that may run concurrently; Camunda's optimistic locking makes one fail
and the job executor retries it. This is why the receive tasks are `asyncAfter`.

### Concurrency model

```mermaid
flowchart TB
    subgraph kafka["Kafka topic, 3 partitions, key = application id"]
        p0[partition 0]
        p1[partition 1]
        p2[partition 2]
    end
    subgraph svc["service instance(s), one consumer group"]
        c0[listener thread 0]
        c1[listener thread 1]
        c2[listener thread 2]
    end
    p0 --> c0
    p1 --> c1
    p2 --> c2
    c0 & c1 & c2 --> h["handler: decode → idempotency check → work → publishAndWait / commit"]
    h --> ok{success?}
    ok -->|yes| commit[commit offset]
    ok -->|"retryable"| retry["back-off 1s, 2s, 4s (max 3)"] --> h
    ok -->|"poison or retries exhausted"| dlt["&lt;topic&gt;.DLT same partition"] --> commit
```

- All events of one application land on one partition → processed in order by one thread.
- Parallelism = partitions (3 locally). Listener concurrency is 3 per service; more instances share the partitions.
- The orchestrator has a second pool: the Camunda **job executor** (async continuations, timers).
- The application service has a third: the **outbox scheduler** (single-threaded per instance; replicas
  co-operate through `SKIP LOCKED`).

## 3.3 Development view

```mermaid
flowchart BT
    classDef lib fill:#c7e0f4,stroke:#5d82a8
    classDef svc fill:#438dd5,color:#fff,stroke:#2e6295
    classDef ext fill:#eee,stroke:#999

    boot["spring-boot 3.3.5<br/>spring-kafka"]:::ext
    cam["camunda-bpm-spring-boot-starter 7.22"]:::ext
    ev["claybar-events<br/>contract + Kafka plumbing"]:::lib
    st["claybar-workflow-spring-boot-starter<br/>plug-in"]:::lib
    orch["orchestrator-service"]:::svc
    app["application-service"]:::svc
    kyc["kyc-service"]:::svc
    cr["credit-service"]:::svc
    no["notification-service"]:::svc

    ev --> boot
    st --> ev
    st --> cam
    orch --> st
    app --> ev
    kyc --> ev
    cr --> ev
    no --> ev
```

- Only `orchestrator-service` (through the starter) depends on Camunda. Domain services depend only on
  `claybar-events`.
- Each library ships a Spring Boot auto-configuration (`META-INF/spring/...AutoConfiguration.imports`).
- One Maven reactor, versions from the Spring Boot parent and the `camunda-bom`.

| Path | Contents |
|---|---|
| `claybar-events/` | `EventEnvelope`, `Topics`, `EventTypes`, `EventCodec`, `EventPublisher`, error handler |
| `claybar-workflow-spring-boot-starter/` | plug-in (see C4 level 4) |
| `services/<name>/src/main/java/com/claybar/<name>/` | service code; application-service is split into `api`, `domain`, `outbox`, `messaging` |
| `services/orchestrator-service/src/main/resources/bpmn/` | BPMN + DMN, auto-deployed |
| `services/application-service/src/main/resources/db/migration/` | Flyway |
| `scripts/` | demo, BPMN layout generator |

Test pyramid: DMN table tests (pure, ms) → BPMN process tests (in-memory engine, manual job execution) →
Spring MVC/JPA tests (H2) → embedded-Kafka integration test → `make up && make demo` against the real stack.
CI (`.github/workflows/ci.yml`) runs `mvn verify` and builds one image per service.

## 3.4 Physical view

### Local / demo (docker compose)

```mermaid
flowchart TB
    classDef node fill:#f5f5f5,stroke:#666
    subgraph host["Docker host (laptop / CI)"]
        direction TB
        kafka["kafka<br/>apache/kafka:3.8.0 KRaft<br/>:9092 host, :19092 internal"]
        pg[("postgres:16<br/>:5432<br/>DBs: camunda, applications")]
        ui["kafka-ui :8090"]
        orch["orchestrator-service :8080"]
        app["application-service :8081"]
        kyc["kyc-service :8082"]
        cr["credit-service :8083"]
        no["notification-service :8084"]
    end
    orch --> pg
    app --> pg
    orch & app & kyc & cr & no --> kafka
    ui --> kafka
```

Images: one `Dockerfile`, layered Spring Boot jar, JRE 21 Alpine, non-root user, `MaxRAMPercentage=75`.
Health: `/actuator/health/readiness` gates `docker compose up --wait`.

### Target production topology (proposed)

```mermaid
flowchart TB
    lb["API gateway / ingress<br/>TLS, OAuth2"]
    subgraph k8s["Kubernetes cluster"]
        subgraph ns["namespace: loans"]
            app["application-service<br/>Deployment ×2+, HPA"]
            orch["orchestrator-service<br/>Deployment ×2 (shared DB,<br/>job executor on all pods)"]
            kyc["kyc-service ×2"]
            cr["credit-service ×2"]
            no["notification-service ×2"]
        end
    end
    msk["Managed Kafka<br/>RF=3, min.insync=2, TLS+SASL, ACLs"]
    rds1[("PostgreSQL HA<br/>camunda")]
    rds2[("PostgreSQL HA<br/>applications")]
    sso["SSO / LDAP<br/>(Camunda identity)"]
    obs["Prometheus / Grafana / logs"]
    lb --> app
    lb --> orch
    orch --> rds1
    app --> rds2
    orch --> sso
    app & orch & kyc & cr & no --> msk
    app & orch & kyc & cr & no -.-> obs
```

- Camunda 7 scales horizontally on a shared database (the job executor uses optimistic locking).
- Partitions per topic ≥ the max replicas of the consuming service.
- Probes: `/actuator/health/liveness` and `/readiness`; graceful shutdown (30 s) is configured.

## 3.5 Scenarios (+1)

Each scenario exercises all four views; the right-hand column says where it is verified.

| # | Scenario | Views touched | Verified by |
|---|---|---|---|
| S1 | Prime applicant auto-approved | logical (status), process (T1–T7), physical (all containers) | `primeApplicantIsAutoApproved`, `make demo` |
| S2 | Borderline case decided by underwriter | process (user task wait state), logical (variables) | `nearPrimeApplicantGoesToUnderwriterWhoRejects`, demo + REST completion |
| S3 | KYC provider silent → review | process (timer job) | `kycTimeoutRoutesToManualReview` |
| S4 | Sanctions hit → reject | logical (KycResult), DMN rule 1 | `failedKycIsRejectedWithoutUnderwriter`, demo |
| S5 | Kafka redelivers events | process (idempotency) | `redeliveredEventsAreIdempotent`, `decisionsAreProjectedIdempotently…` |
| S6 | Broker down while submitting | development (outbox), process (relay ordering) | `relayPublishesInOrderAndStopsAtFirstFailure` |
| S7 | Poison message | process (DLT path) | `poisonMessageIsDeadLettered` (embedded Kafka) |
| S8 | Underwriting SLA breached | process (non-interrupting timer), physical (notification) | live stack (compose SLA 5 min) |

Business-level narratives for these scenarios are in [Business flows](01-business-flows.md#16-scenarios).
