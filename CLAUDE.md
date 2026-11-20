# CLAUDE.md — working on claybar-financials

## Build and test
- `mvn -B verify` builds everything and runs all tests (no Docker needed; Kafka tests use an embedded broker).
- Single module: `mvn -B -pl services/<name> -am verify`.
- Full stack: `make up && make demo`. Stop it with `make down`.

## Conventions (do not break)
- Services communicate **only** via Kafka using `EventEnvelope`. Add topics to `Topics` and types to `EventTypes`.
- The Kafka key is always the application id (`correlationId`, the Camunda business key).
- A BPMN message name must equal the reply's `eventType`. Send tasks use `${kafkaCommandDelegate}` with
  `asyncBefore="true"`. Receive tasks use `asyncAfter="true"` and need an SLA boundary timer.
- New consumers must be idempotent. Throw `NonRetryableEventException` for poison input.
- Business-state changes that emit events in a service with a DB go through `OutboxWriter`.
- `@Transactional` methods must not be self-invoked from a `@KafkaListener` (the proxy is bypassed). Annotate the
  listener method itself.
- Process variables are scalars or JSON strings, never Java-serialised objects.
- Every BPMN/DMN needs `historyTimeToLive`.

## Where things are
- Process model: `services/orchestrator-service/src/main/resources/bpmn/`
- Process tests: `LoanOriginationProcessTest` (drive jobs manually, mock `EventPublisher`)
- DMN tests: `LoanRiskDecisionTest` (add a CSV row per rule change)
