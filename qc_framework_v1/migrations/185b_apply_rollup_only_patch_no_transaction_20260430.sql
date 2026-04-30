-- LOGAN RATIFIED 2026-04-30 (mig_192 patch); SUPERSEDES b641989 mig_185; READY FOR COWORK PATH-C APPLY
-- =============================================================================
-- Migration 185b — canonical_path_malignant_patient_rollup_v1 rollup-only patch
-- (no transaction wrapper; MotherDuck MCP one-statement-per-call)
-- =============================================================================
-- Date:   2026-04-30 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Batch:  mig192_patch / mig185b_apply_rollup_only_patch_no_transaction_20260430
-- Lane:    mig_192 / apply_readiness_patches_for_185_186_188
--
-- COWORK APPLY ORDER (ratified): mig_188b → mig_186b → mig_185b → mig_187
--   This patch runs third. It re-applies COUNT(DISTINCT grain) rollup semantics after
--   mig_186b (which may temporarily use COUNT(*) in §D2); final tumor grain matches 185b.
--
-- BEGIN TRANSACTION / COMMIT were removed: v10 §3.3 / MD MCP wrappers require standalone
-- statements only; Path-C applies one statement per RPC.
--
-- Logan-ratified rule (LOCKED):
--   Do NOT dedupe `main.canonical_path_malignant_events_v1`. The 533 excess
--   rows (within duplicate `(research_id, surgery_episode_id, tumor_ordinal)`
--   grains) are clinically / source-distinct; removing them loses information.
--
--   Patch `main.canonical_path_malignant_patient_rollup_v1` so tumor and
--   histology mode metrics use logical tumor-grain semantics
--   (`COUNT(DISTINCT …)` and `mode()` after one-row-per-grain collapse) while
--   preserving full source fidelity in the events table.
--
-- Optional: flag duplicate-grain rows on events with
--   `is_source_distinct_duplicate_grain` for explicit analyst grain choice.
--   CF-mig185-EVENT-GRAIN-SOURCE-DISTINCT-PRESERVED
--
-- Provenance: `manuscript_workspace.cpm_reconciliation_provenance_v1`
-- Registry:   `main.canonical_column_verification_registry_v1`
--
-- Target DB:  `thyroid_canonical_publication_v1_0`
-- Pre-snapshot DB/schema: `"Thyroid 2026 UPdated".archive_pub_v1_0`
--
-- TIMESTAMPTZ trap: use CAST(CURRENT_TIMESTAMP AS TIMESTAMP) for snapshot_ts.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- §0 Pre-flight invariants (fail closed if drift)
-- -----------------------------------------------------------------------------
-- Expected (connect_locked / 2026-04-30 live publication):
--   * canonical_patient_master: 10,871 rows / distinct research_id
--   * canonical_path_malignant_patient_rollup_v1: 4,137 rows
-- Sample inflated rid (largest n_tumors_total excess): 1294 (pre-fix n=7 vs distinct-grain n=2)
--
-- SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.canonical_patient_master;
-- SELECT COUNT(*) FROM main.canonical_path_malignant_patient_rollup_v1;

-- -----------------------------------------------------------------------------
-- §A Pre-snapshot rollup (full row copy + TIMESTAMP snapshot column)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS
    "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_patient_rollup_v1_pre_mig185b_rollup_only_patch_20260430
AS
SELECT
    *,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig185b_rollup_only_patch_snapshot_ts
FROM main.canonical_path_malignant_patient_rollup_v1;

-- -----------------------------------------------------------------------------
-- §B Rebuild rollup: fix `n_tumors_total`, fix `dominant_histology` mode grain;
--     preserve bethesda / POC merge columns from §A snapshot (`path_outcome_classification_v1`
--     is not live post–Script 361 — do not attempt to re-derive).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE main.canonical_path_malignant_patient_rollup_v1 AS
WITH ev AS (
    SELECT
        TRY_CAST(research_id AS BIGINT)       AS research_id,
        surgery_episode_id,
        tumor_ordinal,
        surgery_date,
        primary_histology,
        extrathyroidal_extension,
        gross_ete,
        stage_group_ajcc7,
        stage_group_ajcc8
    FROM main.canonical_path_malignant_events_v1
),
ev_dedup_hist AS (
    SELECT
        research_id,
        surgery_episode_id,
        tumor_ordinal,
        ANY_VALUE(primary_histology)          AS primary_histology
    FROM ev
    GROUP BY research_id, surgery_episode_id, tumor_ordinal
),
hist_mode AS (
    SELECT
        research_id,
        mode(primary_histology)               AS dominant_histology
    FROM ev_dedup_hist
    GROUP BY research_id
),
agg AS (
    SELECT
        research_id,
        TRUE                                  AS any_malignant_event,
        COUNT(DISTINCT surgery_episode_id)    AS n_malignant_surgeries,
        COUNT(DISTINCT (surgery_episode_id, tumor_ordinal)) AS n_tumors_total,
        MIN(surgery_date)                     AS earliest_malignant_path_date,
        MAX(surgery_date)                     AS latest_malignant_path_date,
        MAX(stage_group_ajcc8)                AS highest_stage_ajcc8,
        MAX(stage_group_ajcc7)                AS highest_stage_ajcc7,
        BOOL_OR(
            COALESCE(gross_ete, 0) = 1
            OR LOWER(COALESCE(CAST(extrathyroidal_extension AS VARCHAR), ''))
               IN ('present', 'minimal', 'microscopic', 'yes', 'c/a',
                   'gross', 'macroscopic')
        )                                     AS any_ett,
        FALSE                                 AS any_metastasis
    FROM ev
    GROUP BY research_id
),
rebuilt AS (
    SELECT
        a.research_id,
        a.any_malignant_event,
        a.n_malignant_surgeries,
        a.n_tumors_total,
        a.earliest_malignant_path_date,
        a.latest_malignant_path_date,
        a.highest_stage_ajcc8,
        a.highest_stage_ajcc7,
        a.any_ett,
        a.any_metastasis,
        h.dominant_histology
    FROM agg a
    INNER JOIN hist_mode h USING (research_id)
)
SELECT
    r.research_id,
    r.any_malignant_event,
    r.n_malignant_surgeries,
    r.n_tumors_total,
    r.earliest_malignant_path_date,
    r.latest_malignant_path_date,
    r.highest_stage_ajcc8,
    r.highest_stage_ajcc7,
    r.any_ett,
    r.any_metastasis,
    r.dominant_histology,
    pre.bethesda_final,
    pre.bethesda_final_name,
    pre.regex_path_outcome,
    pre.poc_tumor_1_histologic_type,
    'mig185b_apply_rollup_only_patch_no_transaction_20260430'::VARCHAR AS build_script,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP)               AS build_ts
FROM rebuilt r
LEFT JOIN "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_path_malignant_patient_rollup_v1_pre_mig185b_rollup_only_patch_20260430 pre
    ON r.research_id = pre.research_id;

-- -----------------------------------------------------------------------------
-- §C Events: optional duplicate-grain flag (533 rows TRUE on 2026-04-30 live)
-- -----------------------------------------------------------------------------
ALTER TABLE main.canonical_path_malignant_events_v1
    ADD COLUMN IF NOT EXISTS is_source_distinct_duplicate_grain BOOLEAN;

UPDATE main.canonical_path_malignant_events_v1 AS e
SET is_source_distinct_duplicate_grain = sub.flag
FROM (
    SELECT
        rowid,
        CASE
            WHEN n_grain > 1 AND rn > 1 THEN TRUE
            ELSE FALSE
        END AS flag
    FROM (
        SELECT
            rowid,
            ROW_NUMBER() OVER (
                PARTITION BY research_id, surgery_episode_id, tumor_ordinal
                ORDER BY synoptic_row_ix ASC NULLS LAST, build_ts ASC NULLS LAST
            ) AS rn,
            COUNT(*) OVER (
                PARTITION BY research_id, surgery_episode_id, tumor_ordinal
            ) AS n_grain
        FROM main.canonical_path_malignant_events_v1
    ) s
) AS sub
WHERE e.rowid = sub.rowid;

COMMENT ON COLUMN main.canonical_path_malignant_events_v1.is_source_distinct_duplicate_grain IS
    'CF-mig185-EVENT-GRAIN-SOURCE-DISTINCT-PRESERVED: TRUE when another row shares the same (research_id, surgery_episode_id, tumor_ordinal) and this row is not the first by (synoptic_row_ix, build_ts). Events remain un-deduped; use flag or COUNT(DISTINCT grain) for tumor-facet analytics.';

-- -----------------------------------------------------------------------------
-- §D Registry appendix — rollup columns affected by COUNT(*) / mode grain
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes, '')
    || ' | mig_185b (mig_192): rollup-only patch; COUNT(*)→COUNT(DISTINCT(surgery_episode_id,tumor_ordinal)) for n_tumors_total; dominant_histology mode() after one-row-per-grain collapse; events left untouched per source-fidelity; CF-mig185-ROLLUP-GRAIN-DEDUPE; no transaction wrapper.'
WHERE schema_name = 'main'
  AND table_name = 'canonical_path_malignant_patient_rollup_v1'
  AND column_name IN ('n_tumors_total', 'dominant_histology');

-- -----------------------------------------------------------------------------
-- §E cpm_reconciliation_provenance_v1
-- -----------------------------------------------------------------------------
-- Idempotency: Path-C must not double-insert the same run_id. If re-running,
-- delete the prior row for this run_id or skip §E after manual confirmation.
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
    (run_id, started_at, ended_at, phases_applied,
     critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES
    ('mig185b_apply_rollup_only_patch_no_transaction_20260430',
     CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
     CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
     'archive_rollup_snapshot_rebuild_rollup_events_flag_registry_notes',
     'CF-mig185-EVENT-GRAIN-SOURCE-DISTINCT-PRESERVED',
     'n_tumors_total_inflation_466_patients_533_excess_rows CF-mig185-ROLLUP-GRAIN-DEDUPE',
     'dominant_histology_mode_grain_pre_state_see_mig185_report',
     'none');

-- -----------------------------------------------------------------------------
-- §F Post-state probes (expect inflation cohort → 0)
-- -----------------------------------------------------------------------------
-- SELECT COUNT(*) AS rollup_rows FROM main.canonical_path_malignant_patient_rollup_v1;
--
-- WITH dedup AS (
--   SELECT TRY_CAST(research_id AS BIGINT) AS research_id,
--          COUNT(DISTINCT (CAST(research_id AS VARCHAR), surgery_episode_id, tumor_ordinal)) AS n_tumors_dedup
--   FROM main.canonical_path_malignant_events_v1
--   GROUP BY 1
-- ), live AS (
--   SELECT TRY_CAST(research_id AS BIGINT) AS research_id, n_tumors_total
--   FROM main.canonical_path_malignant_patient_rollup_v1
-- )
-- SELECT
--   SUM(CASE WHEN live.n_tumors_total > dedup.n_tumors_dedup THEN 1 ELSE 0 END) AS patients_still_inflated
-- FROM live INNER JOIN dedup USING (research_id);
--
-- SELECT is_source_distinct_duplicate_grain, COUNT(*) AS n
-- FROM main.canonical_path_malignant_events_v1
-- GROUP BY 1 ORDER BY 1;
--
-- SELECT research_id, n_tumors_total, dominant_histology
-- FROM main.canonical_path_malignant_patient_rollup_v1
-- WHERE research_id IN (1294, 593, 8894)
-- ORDER BY research_id;
--
-- Gate3 (signoff arithmetic): expect 0 rows
-- SELECT COUNT(*) AS gate3_violations
-- FROM main.canonical_table_signoff_registry_v1 t
-- WHERE t.table_status = 'verified'
--   AND (t.n_verified + t.n_na <> t.n_columns_total
--        OR t.n_not_started <> 0
--        OR COALESCE(t.n_failed, 0) <> 0);
