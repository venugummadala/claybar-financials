# ADR-0002: Event envelope, topics and naming

**Status:** accepted

## Decision
- Every record is a JSON `EventEnvelope`: `eventId, eventType, version, source, correlationId, causationId,
  occurredAt, payload`. Routing metadata is also copied into Kafka headers.
- **Kafka key = correlationId = application id = Camunda business key.** This gives per-application ordering and
  one join key across logs, Kafka and Cockpit.
- Topics: `claybar.<context>.<commands|events>`, plus `claybar.workflow.events` for the engine lifecycle. Each topic
  has a `.DLT` companion.
- Commands are imperative (`KycRequested`). Events are past tense (`KycCompleted`). The BPMN message name equals the
  reply's `eventType`. This convention is what makes correlation configuration-free.
- Payloads evolve additively. A breaking change bumps `version`. Consumers reject versions newer than they know
  and send them to the DLT rather than guessing.

## Consequences
A schema-light payload keeps the services decoupled at the cost of compile-time checks. If the number of teams grows,
introduce a schema registry (Avro/Protobuf) behind `EventCodec` without changing the envelope semantics.
