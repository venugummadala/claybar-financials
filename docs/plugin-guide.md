# claybar-workflow plug-in — contract

## Outbound: `kafkaCommandDelegate`
| Input parameter | Required | Meaning |
|---|---|---|
| `topic` | yes | Kafka topic |
| `eventType` | yes | `EventEnvelope.eventType` |
| `payloadVariables` | no | Comma-separated variable names, or `*` for all. Missing variables are skipped. |

- `correlationId` (and the Kafka key) is the **process business key**. A missing business key is an error.
- `causationId` is the id of the last inbound event (`lastInboundEventId` variable).
- `processInstanceId` is always added to the payload for tracing.
- The send is deferred until the engine transaction commits. Always set `camunda:asyncBefore="true"` on the send task.

## Inbound: `WorkflowEventCorrelator`
| Event type in `start-events` | Other event types |
|---|---|
| Starts the process whose message start event is named after the type. Duplicate business key → `DUPLICATE_START` (ignored). | Correlated to the instance with that business key waiting on a message named after the type. Nothing waiting → `NO_MATCH` (acknowledged). |

- Payload fields become process variables. Scalars are kept typed; maps and lists are stored as JSON strings.
- Metric: `claybar.workflow.inbound{eventType,outcome}`.
- The built-in listener (`claybar.workflow.inbound-topics`) is optional. You can call the correlator yourself from
  any transport.

## Lifecycle events (`claybar.workflow.events`)
`WorkflowProcessStarted`, `WorkflowProcessEnded` (state, endActivityId, durationMillis), `WorkflowTaskCreated`,
`WorkflowTaskCompleted` (assignee), `WorkflowIncidentCreated`, `WorkflowIncidentResolved`. These are keyed by business key.

## Properties
| Property | Default |
|---|---|
| `claybar.workflow.enabled` | `true` |
| `claybar.workflow.inbound-topics` | empty (listener off) |
| `claybar.workflow.consumer-group` | `spring.application.name` |
| `claybar.workflow.start-events` | empty |
| `claybar.workflow.lifecycle-events.enabled` | `true` |
| `claybar.workflow.lifecycle-events.topic` | `claybar.workflow.events` |
| `claybar.kafka.retry.max-attempts` / `initial-interval` / `max-interval` | `3` / `1s` / `10s` |
| `claybar.kafka.create-topics` | `true` |
