---
name: event-driven-production-review
description: Production-readiness review of Spring Boot + Kafka + Camunda 7 code — finds lost/duplicated events, broken transactions, unsafe BPMN async boundaries, missing idempotency, dead-letter gaps, open Camunda REST, Java-serialised process variables and config that only fails at startup. Use this whenever the user asks to review, audit, harden or "check before go-live" an event-driven service, Kafka consumer/producer, outbox, BPMN/DMN model or orchestrator, or asks "why is my Kafka event lost / processed twice / my process stuck / my status not updating", even if they don't use the word "review".
---

# Event-driven production review

Each check below caught (or would have caught) a real defect in a system built with this pattern. Several of them
**pass unit tests and fail only in a running stack**, because tests disable schedulers, listeners and the job
executor — so read the code against the checklist rather than trusting a green build.

## How to run the review

1. Scope: the diff (`git diff <base>...HEAD`) if there is one, otherwise the module(s) the user names. Read
   `pom.xml`/`application.yml` first: profiles and properties change which code paths are live.
2. Walk the checklist in order; use the grep hints to find candidates quickly, then **read the code** to confirm —
   a grep hit is not a finding.
3. For each confirmed problem, write a concrete failure scenario (inputs/state → wrong outcome). If you cannot,
   it is not a finding; mention it as a question at most.
4. If a Docker/compose stack exists, offer to verify the riskiest finding live (scheduler, listener, outbox,
   timer paths).

## Checklist

### A. Transactions and delivery (highest impact)

| # | Check | Grep hint | Failure if wrong |
|---|---|---|---|
| A1 | A `@KafkaListener` / `@Scheduled` / controller method calling a `@Transactional` method **on the same class** | `@KafkaListener` and `@Transactional` in one file; `this.` or bare calls to the annotated method | Self-invocation bypasses the proxy → no transaction → entity changes on a detached object are silently lost (status never updates) |
| A2 | DB write + Kafka send in one method without an outbox or after-commit hook | `kafkaTemplate.send` / `publisher.send` inside `@Transactional` | Phantom events on rollback, lost events on crash |
| A3 | Listener replies fire-and-forget | `send(` without `.get(`/`publishAndWait` inside `@KafkaListener` | Offset committed, reply lost when the broker is unavailable |
| A4 | Outbox relay continues after a failed row, or is not ordered by creation time | relay loop without `break` | Events for one key published out of order |
| A5 | Outbox claim without `SKIP LOCKED`/row locking while running >1 replica | `claimPending`, `findBy…PublishedAtIsNull` | Double publishing |
| A6 | Producer `acks`/idempotence not set | `spring.kafka.producer` | Silent loss on leader failover / duplicates on retry |

### B. Consumers

| # | Check | Failure if wrong |
|---|---|---|
| B1 | No idempotency (no `processed_event`, no dedupe, non-deterministic side effect) | Duplicate e-mails/charges/state changes on redelivery |
| B2 | No error handler / DLT, or poison messages retried | One bad record blocks the partition forever |
| B3 | Business key not used as Kafka key | Events for one entity processed out of order across partitions |
| B4 | State transitions not monotonic (a late "under review" overwrites "approved") | Wrong customer-visible status |
| B5 | Listener throws on unknown event types sharing the topic | Healthy events dead-lettered |

### C. Camunda 7 / BPMN / DMN

| # | Check | Grep hint | Failure if wrong |
|---|---|---|---|
| C1 | Send task (or delegate that publishes) without `camunda:asyncBefore="true"`, or publish not deferred to after commit | `sendTask`, `delegateExpression` | Reply can arrive before the receive-task subscription is committed → `NO_MATCH`, process stuck until timeout |
| C2 | Receive task / message catch without a boundary timer | `receiveTask`, `intermediateCatchEvent` | Instance waits forever when a service or message is lost |
| C3 | Receive tasks feeding a parallel join without `asyncAfter` | `parallelGateway` | `OptimisticLockingException` surfaces in the Kafka listener instead of being retried by the job executor |
| C4 | Normal + timeout flows both entering a **parallel** join directly | | Join waits for tokens that never come |
| C5 | Message name ≠ event type / reply topic missing from inbound topics | `<bpmn:message name=`, `inbound-topics` | Reply never correlated |
| C6 | Field injection (`camunda:field`) on a singleton delegate bean | `camunda:field` + `delegateExpression` | Race between concurrent executions |
| C7 | Map/object variables (DMN `singleResult`, payload objects) with Java serialisation | `mapDecisionResult="singleResult"`, `javaSerializationFormatEnabled` | Serialisation errors, deserialisation attack surface, breaks on class changes |
| C8 | Correlation via a throwing API (`correlate()`, `correlateWithResult()`) on duplicate-prone paths | | ENGINE-16004 logged at **ERROR** on every redelivery → alert fatigue; use `correlateAllWithResult()` |
| C9 | Missing `historyTimeToLive` / history cleanup window | | Deployment rejected (enforced HTTL) or unbounded history tables |
| C10 | `/engine-rest` without an authentication filter | `ProcessEngineAuthenticationFilter` absent | Anyone on the network can start/complete/delete processes (open by default in the Spring Boot starter) |
| C11 | Hard-coded SLA durations in BPMN | `timeDuration` literals | Cannot tune per environment |
| C12 | Demo identities / default admin password in prod config | `admin-user`, seed initialisers | Trivial takeover |
| C13 | Camunda 7 **Community** Edition in production | `camunda-bom` version | CE reached end-of-life Oct 2025: no security fixes; needs Enterprise, Camunda 8 or Operaton |

### D. Configuration that only fails at runtime

| # | Check | Failure if wrong |
|---|---|---|
| D1 | `@Scheduled(fixedDelayString/fixedRateString)` values like `500ms` | `NumberFormatException` at startup — use ISO-8601 `PT0.5S` or millis |
| D2 | Code paths disabled in tests (`auto-startup=false`, scheduler off, job executor off) with no live verification | Startup or wiring bugs ship with a green build |
| D3 | `@KafkaListener` topics from properties that may be empty | Context fails to start |
| D4 | Health probes not exposed / no graceful shutdown | Rolling deploys drop in-flight messages |

### E. Data and observability

| # | Check | Failure if wrong |
|---|---|---|
| E1 | PII (national id, DOB) returned by APIs or logged | Compliance breach |
| E2 | No correlation id in logs (MDC) | Incidents cannot be traced across services |
| E3 | No metrics for outbox backlog, DLT depth, correlation outcomes | Silent degradation |

## Report format

Start with a one-line verdict (ship / ship after fixes / do not ship). Then a table, most severe first:

| Severity | Location | Finding | Failure scenario | Fix |
|---|---|---|---|---|
| High | `LoanDecisionProjector.java:<line>` | Listener self-invokes `@Transactional apply()` | LoanApproved consumed → status change on detached entity → API keeps showing SUBMITTED | Annotate `onDecision` with `@Transactional` and test through the listener entry point |

Severity: **High** = data loss, wrong business outcome, security exposure. **Medium** = stuck work recoverable by
ops, alert noise, scaling limits. **Low** = hygiene. Close with what you could not verify (e.g. "scheduler not
exercised — run the stack") and offer to fix the High findings.
