# ADR-0001: Camunda 7 embedded in a dedicated orchestrator service

**Status:** accepted

## Context
Loan origination involves parallel checks, SLAs, human underwriting and an auditable decision. These are long-running
stateful concerns that do not belong in any single domain service. The requirement names "Camunda BPM", which is
Camunda 7.

## Decision
- Run the **Camunda 7 engine embedded** in one Spring Boot service (`orchestrator-service`) with its own database.
  Domain services never call the engine. They exchange commands and events over Kafka.
- **Orchestration, not choreography,** for the loan lifecycle. The BPMN is the single place that answers "where
  is application X and why". Choreography is still used for side effects (notifications, the status projection),
  which consume the decision events.
- Business rules live in **DMN**, owned by Credit Risk.
- All Kafka/engine glue lives in a **plug-in** (`claybar-workflow-spring-boot-starter`). See ADR-0004.

## Consequences
- Camunda 7.22 is used because it supports Spring Boot 3.3. **Camunda 7 Community Edition reached end-of-life in
  October 2025.** Production use requires either the Enterprise edition (extended support) or a migration path:
  - Camunda 8 (Zeebe). BPMN models port with tooling. The plug-in's send/receive pattern maps to job workers and
    message correlation.
  - Operaton (a community fork of Camunda 7). This is a near drop-in replacement.
  Because only the plug-in and the orchestrator depend on engine APIs, the domain services are unaffected either way.
