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

-- Append-only batching + source traceability (final master release)
ALTER TABLE qa.promotion_review_decisions ADD COLUMN IF NOT EXISTS decision_batch_id VARCHAR;
ALTER TABLE qa.promotion_review_decisions ADD COLUMN IF NOT EXISTS source_object_id VARCHAR;
ALTER TABLE qa.promotion_review_decisions ADD COLUMN IF NOT EXISTS evidence_ref VARCHAR;

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

-- ═══════════════════════════════════════════════════════════════════════════
-- Manual review queue: rows flagged for human review during promotion gate
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS qa.manual_review_queue (
    review_row_id       INTEGER,
    run_label           VARCHAR NOT NULL,
    research_id         BIGINT,
    domain              VARCHAR NOT NULL,
    entity_type         VARCHAR,
    entity_value_norm   VARCHAR,
    algorithm_status    VARCHAR,
    review_reason       VARCHAR,
    verification_status VARCHAR,
    reviewer            VARCHAR,
    reviewed_at         TIMESTAMP,
    loaded_at           TIMESTAMP NOT NULL DEFAULT current_timestamp
);

-- Idempotent extension for v2 promotion gate CSV hydration (2026-04)
ALTER TABLE qa.manual_review_queue ADD COLUMN IF NOT EXISTS promotion_approved VARCHAR;
ALTER TABLE qa.manual_review_queue ADD COLUMN IF NOT EXISTS reviewer_evidence_span VARCHAR;
ALTER TABLE qa.manual_review_queue ADD COLUMN IF NOT EXISTS reviewer_comment VARCHAR;
ALTER TABLE qa.manual_review_queue ADD COLUMN IF NOT EXISTS reason_code VARCHAR;

-- ═══════════════════════════════════════════════════════════════════════════
-- Summary views for QA dashboards
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW qa.promotion_scorecard_summary_v AS
SELECT run_label,
       COUNT(*) AS total_gates,
       COUNT(*) FILTER (WHERE status = 'PASS') AS passed,
       COUNT(*) FILTER (WHERE status = 'FAIL') AS failed,
       COUNT(*) FILTER (WHERE status NOT IN ('PASS', 'FAIL')) AS conditional,
       MAX(generated_at) AS last_run
FROM qa.promotion_scorecard
GROUP BY run_label;

CREATE OR REPLACE VIEW qa.domain_validation_summary_v AS
SELECT run_label,
       COUNT(*) AS domains_checked,
       COUNT(*) FILTER (WHERE schema_ok) AS schema_ok_count,
       SUM(total_rows) AS total_rows_all_domains,
       SUM(unique_patients) AS total_patients_all_domains,
       AVG(dup_rate) AS avg_dup_rate,
       MAX(dup_rate) AS max_dup_rate,
       AVG(entity_date_fill_pct) AS avg_entity_date_fill,
       MIN(entity_date_fill_pct) AS min_entity_date_fill,
       AVG(note_date_fill_pct) AS avg_note_date_fill,
       MAX(generated_at) AS last_run
FROM qa.domain_validation
GROUP BY run_label;

CREATE OR REPLACE VIEW qa.date_provenance_completeness_v AS
SELECT domain_name,
       total_rows,
       unique_patients,
       entity_date_fill_pct,
       note_date_fill_pct,
       provenance_cols_present,
       CASE WHEN entity_date_fill_pct >= 50 AND note_date_fill_pct >= 50
            THEN 'adequate'
            WHEN entity_date_fill_pct >= 30 OR note_date_fill_pct >= 30
            THEN 'marginal'
            ELSE 'insufficient'
       END AS completeness_tier,
       run_label
FROM qa.domain_validation
WHERE run_label = (SELECT MAX(run_label) FROM qa.domain_validation);

CREATE OR REPLACE VIEW qa.manual_review_queue_summary_v AS
SELECT run_label,
       domain,
       COUNT(*) AS total_items,
       COUNT(*) FILTER (WHERE verification_status IS NOT NULL) AS reviewed,
       COUNT(*) FILTER (WHERE verification_status IS NULL) AS pending,
       COUNT(DISTINCT research_id) AS unique_patients
FROM qa.manual_review_queue
GROUP BY run_label, domain;
