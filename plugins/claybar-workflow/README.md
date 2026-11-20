# claybar-workflow — Claude Code plugin

Turns the patterns proven in this repository into reusable Claude Code skills.

| Skill | Use it to… | Invoke |
|---|---|---|
| `camunda-kafka-workflow` | add steps, timers, decisions, human tasks or whole processes to a Camunda 7 BPMN that orchestrates services over Kafka; write process/DMN tests | automatic, or `/claybar-workflow:camunda-kafka-workflow` |
| `kafka-event-service` | scaffold a new event-driven Spring Boot service (listener, reply, DLT, outbox, embedded-Kafka tests, compose/CI wiring) | automatic, or `/claybar-workflow:kafka-event-service` |
| `event-driven-production-review` | audit Kafka/Camunda code for lost or duplicate events, transaction bugs, unsafe BPMN, security and runtime-only config failures | automatic, or `/claybar-workflow:event-driven-production-review` |

Skills load automatically when a request matches their description (for example "add a fraud check to the loan
process", "create a service that scores fraud requests", "review this consumer before go-live").

## Install

```text
/plugin marketplace add venugummadala/claybar-financials
/plugin install claybar-workflow@claybar
```

For local development from a checkout: `/plugin marketplace add ./path/to/claybar-financials`.

## Works best with

The `claybar-events` and `claybar-workflow-spring-boot-starter` libraries in this repo (the skills detect them and
explain how to adopt them otherwise). Java 21, Spring Boot 3.3, Camunda 7.22, Kafka 3.x.

## Layout

```
plugins/claybar-workflow/
├── .claude-plugin/plugin.json
└── skills/
    ├── camunda-kafka-workflow/        SKILL.md + references/ (BPMN snippets, testing, adopting the starter)
    ├── kafka-event-service/           SKILL.md + references/ (service templates, outbox & idempotency)
    └── event-driven-production-review/SKILL.md
```
