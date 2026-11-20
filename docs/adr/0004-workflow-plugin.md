# ADR-0004: Workflow integration as a reusable plug-in

**Status:** accepted

## Decision
Package the Camunda ↔ Kafka integration as `claybar-workflow-spring-boot-starter`:
- **Spring Boot auto-configuration** with `@ConditionalOnMissingBean` / property switches. Adopting it means adding
  one dependency and a few lines of YAML.
- **`ClaybarKafkaEventsProcessEnginePlugin`**, a standard Camunda `ProcessEnginePlugin` that adds a
  `HistoryEventHandler` next to the default DB handler. It also works on a shared engine (`bpm-platform.xml`).
- **Input parameters, not field injection**, configure the delegate. Field injection on a singleton delegate is
  not thread-safe.
- The lifecycle handler **never throws**. Event streaming must not roll back business transactions.

## Consequences
New processes (payments, collections, onboarding) reuse the same send/receive/correlate contract. Engine-specific
code is in one module, which is the main lever for the migration described in ADR-0001.
