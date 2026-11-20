---
name: camunda-kafka-workflow
description: Design, extend and test Camunda 7 BPMN/DMN processes that orchestrate Spring Boot microservices over Kafka, using the claybar-workflow plug-in pattern (send task → Kafka command, receive task ← Kafka reply, SLA timers, DMN decisions, Tasklist user tasks). Use this whenever the user wants to add a step, check, approval, timeout, escalation or decision to a business process, create a new orchestrated process, wire a new microservice into a workflow, change a DMN table, or write process tests — even if they only say "workflow", "orchestration", "BPMN", "process", "saga" or describe a business flow like "add a fraud check before approval" in a Spring Boot + Camunda + Kafka codebase.
---

# Camunda 7 + Kafka orchestration

The orchestrator owns the long-running state (a Camunda process instance per business transaction); domain
services are stateless or own their own data and talk to the orchestrator **only through Kafka**. The glue is the
`claybar-workflow-spring-boot-starter` plug-in: a generic send-task delegate, an inbound event correlator, and an
engine plugin that streams process lifecycle events to Kafka.

Following the pattern exactly matters because its guarantees come from the *combination* of settings — e.g. a
send task without `asyncBefore` still works in a demo but can lose the race against its own reply in production.

## Step 0 — Orient before editing

1. Find the models: `**/src/main/resources/**/*.bpmn` and `*.dmn`.
2. Check the plug-in is on the orchestrator's classpath: grep `pom.xml`/`build.gradle` for
   `claybar-workflow-spring-boot-starter`. If it is missing, read `references/adopting-the-starter.md` first.
3. Read the orchestrator's `application.yml` → `claybar.workflow.inbound-topics` and `start-events`, and the
   shared `Topics` / `EventTypes` classes (in `claybar-events`). New events must be added there.
4. Find existing process tests (e.g. `*ProcessTest.java`, `*DecisionTest.java`) — extend them rather than
   starting new styles.

## Step 1 — Model the change

### Request/reply with a service (the core building block)

Always three elements plus a timeout path. Copy-ready XML is in `references/bpmn-snippets.md`.

| Element | Must have | Why |
|---|---|---|
| **Send task** | `camunda:delegateExpression="${kafkaCommandDelegate}"`, `camunda:asyncBefore="true"`, input parameters `topic`, `eventType`, `payloadVariables` | asyncBefore puts the send and the arrival at the receive task in one job transaction; the plug-in publishes **after commit**, so the reply can never arrive before the subscription exists |
| **Receive task** | `messageRef` whose message **name equals the reply's `eventType`**, `camunda:asyncAfter="true"` | the correlator matches by message name = eventType and business key; asyncAfter keeps the Kafka listener transaction small and lets the job executor retry optimistic-locking conflicts (e.g. at a parallel join) |
| **Interrupting boundary timer** on the receive task | duration from a Spring bean expression (e.g. `${loanSla.fraudTimeout}`), not a literal | a lost command or a dead service must degrade into a human decision, never a stuck instance; SLA values belong in config |
| **Timeout handler** | service task `camunda:expression="${true}"` with `camunda:outputParameter`s setting the same status variables the reply would set (e.g. `fraudStatus=TIMEOUT`) | downstream DMN/gateways then handle "timeout" explicitly |

Merge the normal and timeout paths with an **exclusive** gateway *before* any parallel join — a parallel join
counts incoming flows, so four flows into it would wait for four tokens forever.

### Parallel checks

Parallel gateway fork → one request/reply block per branch → per-branch XOR merge → parallel join. Each reply
event type must be distinct so correlation is unambiguous.

### Decisions

Put business rules in DMN (`businessRuleTask`, `camunda:decisionRef`, `camunda:mapDecisionResult="singleEntry"`,
`camunda:resultVariable="<name>"`). Use a single string output so the result is a scalar variable; a
`singleResult` map would be stored as a Java-serialised object, which these engines reject
(`javaSerializationFormatEnabled=false`). Hit policy FIRST, hard rejects and timeouts at the top, a catch-all
REVIEW row at the bottom. Every new rule gets a test row.

### Human work

`userTask` with `camunda:candidateGroups`, a `camunda:formData` (typed, `required` constraints), a due date from
config, and a **non-interrupting** boundary timer for SLA escalation that publishes an event (send task) — the
task stays with the human. Before the task, publish an "UnderReview"-style event so other services can react.

### Publishing outcomes

Final outcomes are send tasks (same delegate, `asyncBefore`) to a decisions topic, then an end event. Select
only the variables consumers need in `payloadVariables` — it is a data contract, and minimises PII on the wire.

### Starting a process from an event

Message start event whose message name equals the start event type, and add that type to
`claybar.workflow.start-events`. The event's `correlationId` becomes the business key; duplicates are ignored.

## Step 2 — Contracts outside the BPMN

- Add new event types to `EventTypes` and topics to `Topics` (`claybar.<context>.<commands|events>`), which also
  provisions the topic and its `.DLT`.
- Add reply topics to `claybar.workflow.inbound-topics` (comma-separated) or the reply is never consumed.
- SLA durations: add a property to the SLA bean (ISO-8601 getter) and a default in `application.yml`.
- Process variables must be scalars; the correlator stores nested payload values as JSON strings.
- The model needs `camunda:historyTimeToLive` (engines enforce it).
- If a new service is needed, use the `kafka-event-service` skill.

## Step 3 — Keep the diagram valid

The `.bpmn` must keep its `bpmndi` section consistent: every new flow node needs a `BPMNShape` and every
sequence flow a `BPMNEdge`, or Modeler/Cockpit render a broken diagram. Place new shapes near their neighbours
(tasks 100×80, events 36×36, gateways 50×50) and use orthogonal waypoints. If the repo has a layout generator
script (e.g. `scripts/generate-*-bpmn.py`), change the generator and regenerate instead of hand-editing XML.
Validate well-formedness with `xmllint --noout file.bpmn`.

## Step 4 — Test

Read `references/process-testing.md` for templates. Minimum per change:

- **DMN**: a parameterised row for every rule you add or change (pure DMN engine, milliseconds).
- **Process**: happy path through the new step, the timeout path (execute the timer job), and, where the step
  feeds a decision, the branch it changes. Drive async jobs manually with job execution disabled, feed events
  through `WorkflowEventCorrelator.handle`, and assert published commands via a mocked `EventPublisher`.
- Redelivery: the same reply twice → second outcome `NO_MATCH`.

Run the build (`mvn -B verify` or the module with `-pl <module> -am`). If a Docker stack exists
(`docker compose`), redeploy the orchestrator and exercise the path end-to-end — the unit tests disable the job
executor and Kafka, so wiring mistakes (missing inbound topic, wrong message name) only show up live.

## Done checklist

- [ ] Send task: delegate, `asyncBefore`, topic/eventType/payloadVariables
- [ ] Receive task: message name == reply eventType, `asyncAfter`, boundary timer from config, timeout handler
- [ ] XOR merge before any parallel join
- [ ] `EventTypes` / `Topics` / `inbound-topics` / SLA config updated
- [ ] DI shapes/edges for every new element; XML well-formed
- [ ] DMN rows + process tests for normal, timeout and redelivery; build green
- [ ] Docs that list events, topics or BPMN elements updated (search `docs/` for a sibling element id)
