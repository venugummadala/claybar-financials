-- Claybar Financials :: application-service schema (portable across PostgreSQL and H2)

CREATE TABLE loan_application (
    id               VARCHAR(40)  NOT NULL PRIMARY KEY,
    idempotency_key  VARCHAR(100) UNIQUE,
    applicant_name   VARCHAR(200) NOT NULL,
    national_id      VARCHAR(50)  NOT NULL,
    email            VARCHAR(320) NOT NULL,
    date_of_birth    DATE         NOT NULL,
    loan_amount      NUMERIC(15, 2) NOT NULL,
    term_months      INTEGER      NOT NULL,
    annual_income    NUMERIC(15, 2) NOT NULL,
    purpose          VARCHAR(100),
    status           VARCHAR(30)  NOT NULL,
    decision_reason  VARCHAR(500),
    created_at       TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    updated_at       TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    version          BIGINT       NOT NULL
);

CREATE INDEX idx_loan_application_status ON loan_application (status);

-- Transactional outbox: rows are written in the same transaction as the business change and
-- relayed to Kafka asynchronously, so no event is lost and none is published for a rolled-back change.
CREATE TABLE outbox_event (
    id            VARCHAR(36)  NOT NULL PRIMARY KEY,
    topic         VARCHAR(200) NOT NULL,
    message_key   VARCHAR(100) NOT NULL,
    event_type    VARCHAR(100) NOT NULL,
    envelope      VARCHAR      NOT NULL,
    created_at    TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    published_at  TIMESTAMP(6) WITH TIME ZONE,
    attempts      INTEGER      NOT NULL DEFAULT 0,
    last_error    VARCHAR(1000)
);

CREATE INDEX idx_outbox_pending ON outbox_event (published_at, created_at);

-- Idempotent consumer: ids of events already applied to the projection.
CREATE TABLE processed_event (
    event_id      VARCHAR(64)  NOT NULL PRIMARY KEY,
    event_type    VARCHAR(100) NOT NULL,
    processed_at  TIMESTAMP(6) WITH TIME ZONE NOT NULL
);
