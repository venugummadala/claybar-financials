# ADR-0003: Delivery guarantees — outbox in services, after-commit in the engine

**Status:** accepted

## Problem
A database write and a Kafka send cannot be made atomic without XA. Two failures are possible:
- **Phantom event**: sent, then the transaction rolls back.
- **Lost event**: committed, then the send fails or the JVM dies.

## Decision
| Producer | Mechanism | Phantom | Lost |
|---|---|---|---|
| application-service | Transactional outbox (`outbox_event` written in the business transaction, relayed by a poller with `FOR UPDATE SKIP LOCKED`) | impossible | impossible (at-least-once) |
| orchestrator (BPMN send tasks) | `EventPublisher` defers the send to `afterCommit` of the engine transaction | impossible | possible on crash. Covered by BPMN **SLA timers** that route the case to a human |
| kyc / credit services | `publishAndWait` inside the listener: the offset is committed only after the broker acknowledges the reply | n/a | impossible (redelivery → recompute) |

After-commit has one more benefit in the engine: the receive task's message subscription is committed **before**
the command leaves, so a fast reply can never arrive before the process is waiting for it.

All consumers are idempotent, because every path above is at-least-once.

## Alternatives considered
- An outbox inside the engine (a Camunda variable or a custom table plus a job). It closes the remaining gap but
  adds a poller to the engine. This is the next step if SLA timeouts caused by lost commands ever show up in the
  `claybar.workflow.inbound` metrics or in timeout counts.
- Kafka transactions with a DB-first commit. This does not remove the dual-write problem with a JDBC database.
