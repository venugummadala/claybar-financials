# Adopting the claybar-workflow plug-in in another application

Source: https://github.com/venugummadala/claybar-financials (modules `claybar-events` and
`claybar-workflow-spring-boot-starter`). Requires Java 21, Spring Boot 3.3.x, Camunda 7.22
(`camunda-bpm-spring-boot-starter`). The artifacts are not on Maven Central; install them locally or publish to
your own repository:

```bash
git clone https://github.com/venugummadala/claybar-financials.git
cd claybar-financials
mvn -B -pl claybar-events,claybar-workflow-spring-boot-starter -am install -DskipTests
```

```xml
<dependency>
  <groupId>com.claybar</groupId>
  <artifactId>claybar-workflow-spring-boot-starter</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

```yaml
claybar:
  kafka:
    service-name: my-orchestrator
  workflow:
    inbound-topics: my.domain.events,my.other.events   # comma-separated replies/start events
    start-events: SomethingRequested                     # event types that start a process
    lifecycle-events:
      enabled: true                                      # process/task/incident events to claybar.workflow.events
camunda:
  bpm:
    default-serialization-format: application/json
    generic-properties:
      properties:
        javaSerializationFormatEnabled: false
```

## What you get

| Bean | Purpose | Override |
|---|---|---|
| `kafkaCommandDelegate` | BPMN send-task delegate (after-commit publish, business key = Kafka key) | declare your own `KafkaCommandDelegate` |
| `WorkflowEventCorrelator` | start/correlate from `EventEnvelope`, idempotent, metric `claybar.workflow.inbound` | declare your own bean |
| `WorkflowInboundListener` | Kafka listener on `inbound-topics` | leave `inbound-topics` empty and call the correlator yourself |
| `ClaybarKafkaEventsProcessEnginePlugin` | lifecycle events to Kafka | `claybar.workflow.lifecycle-events.enabled=false` |
| `EventPublisher`, `EventCodec`, error handler, topic provisioning | from `claybar-events` | `claybar.kafka.*` |

Topic names live in `com.claybar.events.Topics`. For your own domain either add constants in a fork of
`claybar-events` or keep a `MyTopics` class and declare `NewTopic` beans (plus `.DLT` companions) yourself.

## If the starter cannot be used

Implement the same contract by hand — the guarantees, not the classes, are what matter:
1. Send-task delegate reads `topic`/`eventType`/`payloadVariables` input parameters and registers the Kafka send
   in `TransactionSynchronization.afterCommit`; Kafka key = business key.
2. Inbound listener: message name = eventType, correlate by business key with `correlateAllWithResult()` (an empty
   result means a duplicate — acknowledge it; the throwing variant logs ERROR on every redelivery).
3. Start events: check runtime **and** history for the business key before `correlateStartMessage()`.
4. Error handler: exponential back-off then `<topic>.DLT`; malformed input straight to the DLT.
