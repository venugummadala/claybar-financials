# Architecture documentation

Lightweight, diagram-first documentation of Claybar Loan Origination. All diagrams are Mermaid, so they render
on GitHub/GitLab and in most IDEs, and are diffed and reviewed like code.

| # | Document | Contents | Read it if you are… |
|---|---|---|---|
| 1 | [Business flows](01-business-flows.md) | Actors, end-to-end flow, status lifecycle, decision rules, SLAs, scenarios and exception paths | product, risk, operations, anyone new |
| 2 | [C4 model](02-c4-model.md) | L1 system context, L2 containers + topic ownership, L3 components (orchestrator, application-service, domain services), L4 plug-in classes | architects, new developers |
| 3 | [4+1 views](03-4plus1-views.md) | Logical (domain model, process variables), Process (transaction boundaries, concurrency), Development (modules, tests), Physical (compose, target prod), Scenarios mapped to tests | architects, SRE, reviewers |
| 4 | [Low-level design](04-lld.md) | Event catalogue, BPMN element spec, delegate/correlator/outbox algorithms, DB schema, REST API, error matrix, config, metrics, security | developers implementing or changing code |

Related: [ADRs](../adr) (why decisions were made) · [Plug-in guide](../plugin-guide.md) (contract for reusing the
workflow starter) · BPMN/DMN sources in `services/orchestrator-service/src/main/resources/bpmn/`.

**Keeping it current:** class, element and property names here match the code. When you rename one, search
`docs/architecture` for it; when you change the DMN, update the rules table in §1.4 and the CSV rows in
`LoanRiskDecisionTest`.
