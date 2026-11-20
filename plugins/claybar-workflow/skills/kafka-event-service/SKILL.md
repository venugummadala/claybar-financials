---
name: kafka-event-service
description: Scaffold or extend an event-driven Spring Boot microservice that consumes Kafka commands/events and replies with events, following the claybar conventions (EventEnvelope, application-id keys, idempotent consumers, publishAndWait replies, retry + dead-letter topics, transactional outbox for stateful services, embedded-Kafka tests, Docker/compose wiring). Use this whenever the user asks for a new microservice, worker, adapter, consumer or "service that listens to X and publishes Y", wants to integrate an external provider (bureau, fraud, payments, screening, notifications) behind Kafka, or needs to make an existing Kafka consumer idempotent/reliable — even if they don't say "Kafka" explicitly but the codebase communicates through events.
---

# Event-driven Spring Boot service

A service in this architecture does one thing: **react to an event, do domain work, emit an event.** It never
calls other services synchronously, and every guarantee it gives is at-least-once — so every handler must be
idempotent. The shared library `claybar-events` provides the envelope, codec, publisher, retry/DLT handler and
topic provisioning through Spring Boot auto-configuration; a new service mostly adds domain logic.

## Step 0 — Decide the shape

| Question | If yes |
|---|---|
| Does the service only compute an answer from the request? (screening, scoring, pricing) | **Stateless responder** — no DB, deterministic logic, reply via `publishAndWait`. Simplest and preferred. |
| Does it own data that other services need to hear about? | **Stateful service** — DB + Flyway, write events through the **transactional outbox** (`references/outbox-and-idempotency.md`). |
| Does it consume events to update its own data? | **Idempotent projection** — `processed_event` table in the same transaction. |
| Is it part of an orchestrated process? | Also follow the `camunda-kafka-workflow` skill to add the send/receive step. |

Look at an existing service in the repo of the same shape and mirror it (package layout, naming, config style)
rather than inventing a new one.

## Step 1 — Contract first

1. Name the command and reply: imperative command (`FraudCheckRequested`), past-tense event
   (`FraudCheckCompleted`). Add constants to `EventTypes`.
2. Topics `claybar.<context>.commands` and `claybar.<context>.events` → add to `Topics` and `Topics.ALL` (that
   provisions them and their `.DLT`).
3. Write the payload fields down (name, type, required?) — the reply's fields become process variables, so use
   scalar types and names that do not collide with existing variables.

## Step 2 — Generate the module

Templates for every file are in `references/service-templates.md`: `pom.xml`, application class, properties
record, listener, domain service + port, `application.yml`, unit test, embedded-Kafka integration test, compose
entry. Then:

- Register the module in the parent `pom.xml` `<modules>`.
- Pick the next free port; add the service to `docker-compose.yml` (the shared `Dockerfile` takes `MODULE`), the
  CI image matrix, and the module table in the README.

### Listener rules (each one prevents a real production failure)

- Decode with `EventCodec.decode` — malformed input throws `NonRetryableEventException` and goes to the DLT
  instead of blocking the partition with endless retries.
- Ignore event types you don't handle (topics can carry several types); don't throw.
- Wrap the work in `CorrelationContext.open(event)` so every log line carries the business id.
- Throw `NonRetryableEventException` for invalid business input (missing mandatory field) — retrying cannot fix it.
- Reply with `EventEnvelope.causedBy(command, …)` (keeps correlation id, sets causation id) and
  `publisher.publishAndWait(topic, reply, timeout)`. Waiting for the broker ack before returning means a failed
  send fails the listener and the command is redelivered; fire-and-forget would commit the offset and lose the
  reply.
- Keep the logic deterministic so a redelivered command produces the same reply (natural idempotency). If it is
  not (calls a paid external API, sends an e-mail), dedupe by `eventId` first.
- External providers sit behind an interface (port) with a simulated default implementation so tests and the
  local stack run without them; production adapters add timeouts and a circuit breaker.
- If the listener calls a `@Transactional` method on the same class, annotate the listener method itself: a
  self-invocation bypasses the Spring proxy and the transaction silently does not exist.

## Step 3 — Test

- Unit-test the domain logic directly (fast, covers the rules).
- One `@EmbeddedKafka` integration test proving: command in → reply out with the right key, type, causation id
  and payload; and a poison message lands on `<topic>.DLT`. Subscribe the test consumer with
  `consumer.subscribe(List.of(topic))` — `consumeFromAnEmbeddedTopic` only works for topics declared on the
  annotation.
- Anything disabled in tests (schedulers, listeners with `auto-startup=false`) must be exercised once on the real
  stack: `mvn -DskipTests package && docker compose up -d --build --wait <service>` and drive one request.

## Step 4 — Done checklist

- [ ] `EventTypes`, `Topics` (+`ALL`) updated; module in parent POM
- [ ] Listener follows the rules above; domain logic behind a port
- [ ] `application.yml`: `spring.application.name`, consumer `group-id`, `acks: all`, idempotent producer,
      `claybar.kafka.service-name`, actuator probes + prometheus, `%X{correlationId}` in the log pattern
- [ ] Unit tests + embedded-Kafka test green (`mvn -B -pl services/<name> -am verify`)
- [ ] compose service with healthcheck, CI matrix, README updated
- [ ] If orchestrated: reply topic added to the orchestrator's `claybar.workflow.inbound-topics`
