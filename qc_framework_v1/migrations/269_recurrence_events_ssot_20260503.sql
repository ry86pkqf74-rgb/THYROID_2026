-- mig_269 — canonical_recurrence_events_v1 SSOT + canonical_recurrence_patient_rollup_v1 + CPM recurrence repoint
-- Database: thyroid_canonical_publication_v1_0 (main.*)
-- Apply: .venv/bin/python scripts/mig_269_recurrence_events_ssot_apply.py --apply
--
-- DESIGN (publication-grounded 2026-05-03):
-- * Live MotherDuck recurrence_event_clean_v1 has 11 cols (research_id VARCHAR, TIMESTAMP date,
--   INTEGER recurrence_site all-NULL today, recurrence_definition tier, biochemical/structural flags).
-- * One row/patient → canonical_recurrence_events_v1 is Tier-2 naming SSOT aligned to Logan convention.
-- * Clinical recurrence_confirmed/first_recurrence_date for CPM stay anchored on canonical_recurrence_v1
--   (avoid inflating cohort from structural_date-unknown catalog tiers or biochemical-only tails).
-- * any_recurrence_flag is repointed := recurrence_confirmed (clears CF 46 pts where legacy any=TRUE only).
-- * Catalog ATA primary type merges event-grain biochemical vs structural where present.

-- ── §4a Archive snapshots ───────────────────────────────────────────────────
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_v1_pre_mig269_20260503 AS
SELECT * FROM main.canonical_recurrence_v1;

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_resolved_v1_pre_mig269_20260503 AS
SELECT * FROM main.canonical_recurrence_resolved_v1;

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.recurrence_event_clean_v1_pre_mig269_20260503 AS
SELECT * FROM main.recurrence_event_clean_v1;

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_recurrence_cols_pre_mig269_20260503 AS
SELECT research_id,
       any_recurrence_flag,
       first_recurrence_date,
       recurrence_type_primary,
       recurrence_site_primary,
       time_to_recurrence_days,
       recurrence_confirmed
FROM main.canonical_patient_master;

-- §4b — Event-grain Tier-2 SSOT from recurrence_event_clean_v1 ----------------
CREATE OR REPLACE TABLE main.canonical_recurrence_events_v1 AS
WITH dedup AS (
    SELECT c.*,
           ROW_NUMBER() OVER (
               PARTITION BY
                   CAST(c.research_id AS VARCHAR),
                   CAST(c.recurrence_date AS DATE),
                   COALESCE(CAST(c.recurrence_site AS VARCHAR), '__NULL_SITE__'),
                   COALESCE(CAST(c.recurrence_type AS VARCHAR), '__NULL_TYPE__'),
                   COALESCE(CAST(c.recurrence_definition AS VARCHAR), '__NULL_DEF__')
               ORDER BY CASE c.recurrence_definition
                            WHEN 'structural_confirmed' THEN 1
                            WHEN 'biochemical_tg_rise' THEN 2
                            WHEN 'biochemical_anti_tg_rise' THEN 3
                            WHEN 'structural_date_unknown' THEN 4
                            ELSE 9 END,
                        c.source_priority DESC NULLS LAST,
                        CAST(c.cleaned_at AS TIMESTAMP)
           ) AS dedup_rank
    FROM main.recurrence_event_clean_v1 c
),
seq AS (
    SELECT d.*,
           ROW_NUMBER() OVER (
               PARTITION BY CAST(d.research_id AS VARCHAR)
               ORDER BY CAST(d.recurrence_date AS DATE) ASC NULLS LAST,
                        dedup_rank
           ) AS recurrence_event_seq
    FROM dedup d
    WHERE dedup_rank = 1
)
SELECT CAST(s.research_id AS VARCHAR)                                    AS research_id,
       CAST(s.recurrence_event_seq AS INTEGER)                           AS recurrence_event_seq,
       CAST(s.recurrence_date AS DATE)                                 AS recurrence_date,
       CAST('tier1_recurrence_event_clean_v1' AS VARCHAR)                 AS recurrence_date_source,
       CAST(COALESCE(s.recurrence_type, '') AS VARCHAR)                AS recurrence_type,
       CAST(s.recurrence_site AS VARCHAR)                              AS recurrence_site,
       CAST(NULL AS VARCHAR)                                            AS recurrence_site_text,
       CAST(NULL AS VARCHAR)                                            AS recurrence_laterality,
       CAST(NULL AS VARCHAR)                                            AS recurrence_histology,
       CAST(CASE s.recurrence_definition
                WHEN 'structural_confirmed' THEN 'definitive'
                WHEN 'biochemical_tg_rise' THEN 'probable'
                WHEN 'biochemical_anti_tg_rise' THEN 'probable'
                WHEN 'structural_date_unknown' THEN 'possible'
                ELSE 'possible'
            END AS VARCHAR)                                             AS evidence_strength,
       CAST(CASE
                WHEN CAST(s.recurrence_definition AS VARCHAR) ILIKE '%biochemical%'
                     OR CAST(s.source_table AS VARCHAR) ILIKE '%lab%'
                THEN 'tg_lab'
                WHEN CAST(s.recurrence_definition AS VARCHAR) ILIKE '%structural%'
                THEN COALESCE(NULLIF(TRIM(CAST(s.source_table AS VARCHAR)), ''),
                              'clinical_structural')
                ELSE COALESCE(NULLIF(TRIM(CAST(s.source_table AS VARCHAR)), ''),
                              'clinical_note')
            END AS VARCHAR)                                             AS evidence_source,
       CAST(CONCAT_WS(' — ',
                     CAST(COALESCE(s.recurrence_definition, '') AS VARCHAR),
                     CAST(COALESCE(s.recurrence_type, '') AS VARCHAR)
              ) AS VARCHAR)                                              AS evidence_quote,
       CAST(
           CASE
               WHEN COALESCE(pm.pshx_nlp_prior_thyroidectomy, FALSE)
                    AND CAST(s.recurrence_site AS VARCHAR) IS NOT NULL THEN 'prior_thy_lnonly_upstage'
               ELSE 'recurrence_catalog_v1_derived'
           END AS VARCHAR)                                             AS attribution_basis,
       CAST(NULL AS VARCHAR)                                            AS note_row_id,
       CAST(NULL AS VARCHAR)                                            AS llm_model,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                            AS build_ts
FROM seq s
LEFT JOIN main.canonical_patient_master pm
       ON CAST(pm.research_id AS VARCHAR) = CAST(s.research_id AS VARCHAR);


-- ── @@ROLLUP_START (copied into scripts/mig_269_recurrence_events_ssot_apply.py dry-run)
CREATE OR REPLACE TABLE main.canonical_recurrence_patient_rollup_v1 AS
WITH agg AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id,
           COUNT(*)::BIGINT                                                      AS n_recurrence_catalog_events,
           MIN(recurrence_date)                                                  AS first_catalog_event_date,
           MAX(recurrence_date)                                                  AS last_catalog_event_date,
           COALESCE(MAX(CASE WHEN recurrence_type = 'structural'
                             THEN recurrence_type END),
                    MAX(CASE WHEN recurrence_type = 'biochemical'
                             THEN recurrence_type END))                           AS recurrence_type_primary_catalog,
           CAST(NULL AS VARCHAR)                                                AS recurrence_site_primary_catalog
    FROM main.canonical_recurrence_events_v1
    GROUP BY 1
),
spine AS (
    SELECT CAST(cr.research_id AS VARCHAR) AS research_id,
           CAST(cr.recurrence_confirmed AS BOOLEAN)                    AS recurrence_confirmed,
           CAST(cr.recurrence_date AS DATE)                              AS recurrence_date_clinical,
           CAST(cr.recurrence_type AS VARCHAR)                           AS recurrence_type_clinical,
           CAST(cr.recurrence_site AS VARCHAR)                           AS recurrence_site_clinical
    FROM main.canonical_recurrence_v1 cr
)
SELECT s.research_id,
       COALESCE(a.n_recurrence_catalog_events, 0::BIGINT)                    AS n_recurrence_catalog_events,
       s.recurrence_confirmed,
       CAST(s.recurrence_date_clinical AS DATE)                               AS first_recurrence_date,
       a.first_catalog_event_date,
       a.last_catalog_event_date,
       COALESCE(NULLIF(TRIM(a.recurrence_type_primary_catalog), ''),
                NULLIF(TRIM(s.recurrence_type_clinical), ''))
                                                                               AS recurrence_type_primary,
       COALESCE(NULLIF(TRIM(a.recurrence_site_primary_catalog), ''),
                NULLIF(TRIM(s.recurrence_site_clinical), ''))
                                                                               AS recurrence_site_primary,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                                  AS rollup_built_ts
FROM spine s
LEFT JOIN agg a ON a.research_id = s.research_id;
-- @@ROLLUP_END


-- ── §4d CPM recurrence columns (Publication Protocol v2) ─────────────────────
UPDATE main.canonical_patient_master pm
SET any_recurrence_flag        = CAST(r.recurrence_confirmed AS BOOLEAN),
    recurrence_confirmed       = CAST(r.recurrence_confirmed AS BOOLEAN),
    first_recurrence_date      = r.first_recurrence_date,
    recurrence_type_primary    = r.recurrence_type_primary,
    recurrence_site_primary    = r.recurrence_site_primary,
    time_to_recurrence_days =
        CASE
            WHEN r.first_recurrence_date IS NULL OR pm.first_surgery_date IS NULL THEN NULL
            WHEN DATE_DIFF('day', CAST(pm.first_surgery_date AS DATE), r.first_recurrence_date) < 0 THEN NULL
            WHEN pm.overall_survival_days IS NULL THEN DATE_DIFF(
                'day', CAST(pm.first_surgery_date AS DATE), r.first_recurrence_date)
            ELSE LEAST(
                DATE_DIFF('day', CAST(pm.first_surgery_date AS DATE), r.first_recurrence_date),
                CAST(pm.overall_survival_days AS BIGINT)
            )
        END,
    cpm_built_at               = CURRENT_TIMESTAMP
FROM main.canonical_recurrence_patient_rollup_v1 r
WHERE CAST(pm.research_id AS VARCHAR) = CAST(r.research_id AS VARCHAR);


-- §4g Signoff -----------------------------------------------------
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (
    'mig_269',
    CURRENT_TIMESTAMP::TIMESTAMP,
    'cursor_composer_mig269',
    'mig_269: Built canonical_recurrence_events_v1 from recurrence_event_clean_v1 (Tier-2 SSOT)'
    || ' plus canonical_recurrence_patient_rollup_v1 merging catalog ATA primary type'
    || ' with canonical_recurrence_v1 spine for recurrence_confirmed + first_recurrence_date.'
    || ' CPM recurrence columns repointed (any_recurrence_flag := recurrence_confirmed). Legacy'
    || ' canonical_recurrence_v1/canonical_recurrence_resolved_v1/recurrence_event_clean_v1 untouched'
    || ' pending mig_269b audit. CF-RECURRENCE-NAMING-CONVENTION + CF-RECURRENCE-EVENT-GRAIN-MISSING closed.'
);
