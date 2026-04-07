-- Governance: append-only audit trail for molecular pipeline runs (MotherDuck + local).
-- No secrets. Apply with scripts/135_md_molecular_observability.py init-audit --md

CREATE SCHEMA IF NOT EXISTS qa;

-- DuckLake (MotherDuck production): no PRIMARY KEY — logical key is audit_id.
CREATE TABLE IF NOT EXISTS qa.molecular_pipeline_run_audit (
    audit_id            VARCHAR NOT NULL,
    recorded_at         TIMESTAMP NOT NULL DEFAULT current_timestamp,
    pipeline_name       VARCHAR NOT NULL,
    git_sha             VARCHAR,
    database_name       VARCHAR NOT NULL,
    schema_name         VARCHAR NOT NULL DEFAULT 'main',
    row_counts_json     VARCHAR,
    runtime_seconds     DOUBLE,
    validation_status   VARCHAR NOT NULL,
    custom_user_agent   VARCHAR,
    session_hint        VARCHAR,
    notes               VARCHAR
);
