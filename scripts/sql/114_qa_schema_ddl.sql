-- QA Schema DDL for MotherDuck "Thyroid 2026"
-- Creates the qa schema and its core tables for promotion governance.
-- Idempotent: safe to re-run.

CREATE SCHEMA IF NOT EXISTS qa;

-- Gate scorecard: one row per gate per run
CREATE TABLE IF NOT EXISTS qa.promotion_scorecard (
    run_label           VARCHAR NOT NULL,
    gate_id             VARCHAR NOT NULL,
    criterion           VARCHAR NOT NULL,
    status              VARCHAR NOT NULL,
    detail              VARCHAR,
    generated_at        TIMESTAMP NOT NULL DEFAULT current_timestamp,
    git_sha             VARCHAR,
    registry_version    VARCHAR
);

-- Manual review queue: persisted review decisions across gate runs
CREATE TABLE IF NOT EXISTS qa.promotion_review_decisions (
    review_id           INTEGER,
    run_label           VARCHAR NOT NULL,
    llm_entity_id       BIGINT,
    research_id         BIGINT,
    domain              VARCHAR NOT NULL,
    entity_type         VARCHAR,
    algorithm_status    VARCHAR,
    verification_status VARCHAR,
    reviewer            VARCHAR,
    reviewed_at         TIMESTAMP,
    waiver_reason       VARCHAR,
    created_at          TIMESTAMP NOT NULL DEFAULT current_timestamp
);

-- Concordance summary: per-domain concordance metrics by gate run
CREATE TABLE IF NOT EXISTS qa.concordance_summary (
    run_label               VARCHAR NOT NULL,
    comparison_domain       VARCHAR NOT NULL,
    algorithm_status        VARCHAR NOT NULL,
    llm_rows                BIGINT,
    unique_patients         BIGINT,
    structured_matches      BIGINT,
    fill_candidates         BIGINT,
    review_conflicts        BIGINT,
    generated_at            TIMESTAMP NOT NULL DEFAULT current_timestamp
);

-- Domain validation: schema compliance, dup rates, date coverage per gate run
CREATE TABLE IF NOT EXISTS qa.domain_validation (
    run_label               VARCHAR NOT NULL,
    domain_name             VARCHAR NOT NULL,
    parquet_stem            VARCHAR,
    total_rows              BIGINT,
    unique_patients         BIGINT,
    schema_ok               BOOLEAN,
    dup_rows                BIGINT,
    dup_rate                DOUBLE,
    provenance_cols_present INTEGER,
    entity_date_fill_pct    DOUBLE,
    note_date_fill_pct      DOUBLE,
    generated_at            TIMESTAMP NOT NULL DEFAULT current_timestamp
);

-- Tg lab ingestion QC: structured QC results from script 113
CREATE TABLE IF NOT EXISTS qa.tg_lab_ingestion_qc (
    run_label               VARCHAR NOT NULL,
    reconciliation_gap      INTEGER,
    numeric_parse_rate      DOUBLE,
    patients                INTEGER,
    tg_rows                 BIGINT,
    tgab_rows               BIGINT,
    ambiguous_remaining     INTEGER,
    dedup_status            VARCHAR,
    recurrence_linkage      BOOLEAN,
    generated_at            TIMESTAMP NOT NULL DEFAULT current_timestamp
);

-- Release manifest: tracks immutable point-in-time snapshots
CREATE TABLE IF NOT EXISTS qa.release_manifest (
    release_tag         VARCHAR NOT NULL,
    git_sha             VARCHAR,
    registry_version    VARCHAR,
    tables_included     VARCHAR,
    row_counts          VARCHAR,
    created_at          TIMESTAMP NOT NULL DEFAULT current_timestamp,
    created_by          VARCHAR
);
