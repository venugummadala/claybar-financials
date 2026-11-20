# 1. Business flows

How a loan application moves from submission to decision. This page is for product owners, credit risk
and operations; the technical views follow in [C4](02-c4-model.md), [4+1](03-4plus1-views.md) and [LLD](04-lld.md).

## 1.1 Actors

| Actor | Role in the flow |
|---|---|
| **Applicant** | Submits an application through a channel (web, mobile, broker) and receives the decision by e-mail. |
| **Underwriter** | Decides borderline cases in Camunda Tasklist (`underwriters` group). |
| **Loan operations** | Receives alerts: cases awaiting review, SLA breaches, workflow incidents. |
| **Credit risk** | Owns the decision table (DMN) that sets approve / review / reject rules. |
| **KYC provider**, **Credit bureau** | External checks (simulated in this repo). |

## 1.2 End-to-end flow

```mermaid
flowchart LR
    A([Applicant submits<br/>application]) --> B[Application recorded<br/>status SUBMITTED]
    B --> C{{Run checks in parallel}}
    C --> K[KYC screening<br/>sanctions, PEP, age]
    C --> CR[Credit bureau check<br/>score, risk band]
    K --> D{Risk decision<br/>DMN}
    CR --> D
    D -->|APPROVE| AP([Approved<br/>e-mail applicant])
    D -->|REJECT| RJ([Rejected<br/>e-mail applicant])
    D -->|REVIEW| UR[Manual underwriting<br/>status UNDER_REVIEW<br/>ops alerted]
    UR -->|approve| AP
    UR -->|decline| RJ
    UR -. "SLA exceeded" .-> ESC[Escalate to ops<br/>task stays open]
```

## 1.3 Application status lifecycle

The status the applicant (and the API) sees. Final states never change, even if a late event arrives.

```mermaid
stateDiagram-v2
    [*] --> SUBMITTED: POST /loan-applications
    SUBMITTED --> APPROVED: auto-approve
    SUBMITTED --> REJECTED: auto-reject
    SUBMITTED --> UNDER_REVIEW: rules say REVIEW
    UNDER_REVIEW --> APPROVED: underwriter approves
    UNDER_REVIEW --> REJECTED: underwriter declines
    APPROVED --> [*]
    REJECTED --> [*]
```

## 1.4 Decision rules (owned by Credit Risk)

Evaluated top to bottom; the first matching rule wins (`loan-risk-assessment.dmn`, hit policy FIRST).
Loan-to-income = loan amount ÷ annual income.

| # | KYC | Credit check | Score | Loan-to-income | Amount | Decision | Why |
|---|---|---|---|---|---|---|---|
| 1 | FAILED | – | – | – | – | **REJECT** | Sanctions hit, under age or missing identity |
| 2 | TIMEOUT | – | – | – | – | REVIEW | KYC provider did not answer in time |
| 3 | – | TIMEOUT | – | – | – | REVIEW | Bureau did not answer in time |
| 4 | – | – | < 550 | – | – | **REJECT** | Sub-prime |
| 5 | – | – | – | > 1.0 | – | **REJECT** | Loan exceeds annual income |
| 6 | VERIFIED | COMPLETED | ≥ 700 | ≤ 0.4 | ≤ 50,000 | **APPROVE** | Prime, affordable, within auto-approval limit |
| 7 | – | – | – | – | – | REVIEW | Everything else |

KYC outcomes: `VERIFIED` (clean), `FAILED` (hard stop), `REFER` (politically exposed person, falls through to rule 7 → human review).

## 1.5 Service levels

| SLA | Default | On breach |
|---|---|---|
| KYC answer | 10 min | Check marked `TIMEOUT` → case goes to an underwriter (never auto-rejected for a provider outage) |
| Credit bureau answer | 10 min | Same as above |
| Underwriting decision | 2 days | Ops alerted (`UnderwritingSlaBreached`); the task stays with the underwriter |

All three are configuration (`claybar.loan.sla.*`), not code. The local Docker stack shortens them to 2 / 2 / 5 minutes.

## 1.6 Scenarios

### Happy path — automatic approval

```mermaid
sequenceDiagram
    autonumber
    actor App as Applicant
    participant AS as Application service
    participant WF as Loan workflow
    participant KYC as KYC service
    participant CB as Credit service
    participant N as Notification service
    App->>AS: Submit application
    AS-->>App: 201 Created, status SUBMITTED
    AS-)WF: LoanApplicationSubmitted
    par KYC
        WF-)KYC: KycRequested
        KYC-)WF: KycCompleted VERIFIED
    and Credit
        WF-)CB: CreditCheckRequested
        CB-)WF: CreditCheckCompleted score 800
    end
    WF->>WF: Decision table: APPROVE
    WF-)AS: LoanApproved
    WF-)N: LoanApproved
    N-)App: Approval e-mail
    App->>AS: GET status
    AS-->>App: APPROVED
```

### Manual underwriting

```mermaid
sequenceDiagram
    autonumber
    participant WF as Loan workflow
    participant AS as Application service
    participant N as Notification service
    actor UW as Underwriter
    actor Ops as Loan operations
    WF->>WF: Decision table: REVIEW
    WF-)AS: LoanUnderReview
    WF-)N: LoanUnderReview
    N-)Ops: Case awaiting underwriting
    WF->>UW: Task "Manual underwriting" in Tasklist
    alt decided within SLA
        UW->>WF: Approve / decline + comment
        WF-)AS: LoanApproved or LoanRejected
        WF-)N: decision
    else SLA exceeded
        WF-)N: UnderwritingSlaBreached
        N-)Ops: SLA breach alert
        Note over UW,WF: Task stays open, the underwriter can still decide
    end
```

### Exception paths

| Situation | What happens | Applicant impact |
|---|---|---|
| Sanctions match | KYC `FAILED` → rule 1 | Rejected, no human time spent |
| KYC or bureau outage | SLA timer fires → `TIMEOUT` → REVIEW | Decision delayed, not lost |
| Same request sent twice (client retry) | `Idempotency-Key` returns the original application | One application |
| Message delivered twice (Kafka redelivery) | Ignored by every consumer | No duplicate e-mails or status changes |
| Malformed message | Parked on a dead-letter topic for investigation | If it was a KYC/credit reply, the SLA timer sends the case to review |
| Technical failure inside the workflow | Camunda incident, visible in Cockpit, ops alerted | Case pauses until ops retries it |
