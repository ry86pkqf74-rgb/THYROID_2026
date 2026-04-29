-- =============================================================================
-- Migration 143 — canonical_patient_master SMALL CLUSTERS BUNDLE sign-off
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane 32 — bundled low-risk slices: FNA + Demographics + Frozen section + Staging
--           (30 cols; Protocol v2 predicate match on live registry 2026-04-29).
-- batch_id: mig_143_patient_master_small_clusters_bundle_20260429
--
-- Pre-apply probes (MotherDuck RW `thyroid_canonical_publication_v1_0`, query_rw 2026-04-29)
-- ---------------------------------------------------------------------------
-- * Predicate cardinality: **30** registry rows `not_started` — fna=12, demographics=10,
--   frozen_section=5, staging=3 (`bethesda_*` without `fna_*` prefix excluded; covered by mig_132).
-- * Cohort parity: `canonical_patient_master` = **10,871** rows / distinct `research_id`.
-- * **Gate 4** (verified requires verified_by + verification_method + batch_id + verified_ts):
--   **0** violations on existing verified CPM columns.
--
-- * Cohort-uniformity (#2f):
--   - `fna_path_concordant`: **1,408** TRUE / **3,841** FALSE / **5,622** NULL — non-degenerate BOOLEAN.
--   - `frozen_any_performed_flag`: **4,116** TRUE (**37.9%**) / **6,755** FALSE — within ~30–50% band.
--   - `ajcc8_calculable_flag`: **4,083** TRUE / **6,788** FALSE — proportion **not** 80–90% naive;
--     **CF-mig143-AJCC8-CALC-NOT-NAIVE4**: naive `(T+N+M)` + `age_at_surgery` all non-null (**4,075**)
--     differs from flag on **112** rows — PM/builder uses adjudicated AJCC calculability (Script **51**/266b
--     family), not bare IS NOT NULL probes. Accept with CF note on `notes`.
--   - `ages_calculable_flag`: **10,871** TRUE (100%) — degenerate BOOLEAN;
--     **CF-mig143-AGES-CALC-ALLTRUE**: aligns with scoring layer treating calculable whenever `ages_score`
--     computed (see `derivation_ages_arithmetic` block).
--   - **sex**: female **8,459** / male **2,412**; **race**: White **5,266**, Black/African American **4,168**, etc.
--     Meaningful distributions (not single-value).
--
-- * Drift / derivation replay highlights:
--   **FNA (canonical_fna_feeders)**
--   - `canonical_fna_patient_rollup_v1` (mig_95b verified): bitwise **0-diff** vs CPM on
--     (`fna_bethesda_final`, `fna_bethesda_confidence`, `fna_bethesda_source`, `fna_confidence`,
--     `n_bethesda_calculated_fnas`, `worst_bethesda_num`, `worst_bethesda_source`) — prompt6_356 repoint map.
--   - `canonical_fna_events_v1`: `worst_bethesda_num` **0-diff** vs `MAX(bethesda_final_num)` per patient-global
--     rollup; `n_bethesda_number_only_fnas` **0-diff** vs `COUNT(*) WHERE bethesda_calculated_num IS NULL AND
--     bethesda_final_num IS NOT NULL` using **COALESCE(NULL,0) semantics** across LEFT JOIN gaps (prompt6_356
--     subquery — 5,184 pts with NULL RHS subcount are **0-equivalent**, not contradictory).
--   - Logan 2-digit year: `canonical_fna_events_v1.fna_date_resolved` lineage already ratified mig_96 /
--     `reference_2digit_year_convention`; no PM re-parse required here.
--
--   **Frozen (canonical_frozen_section_events_v1, mig_100 verified)**
--   - **`frozen_n_total`** / hierarchy / syn columns: naive `COUNT(*)` vs all event rows ⇒ **451** mismatch;
--     **Script `360_frozen_section_cleanup.py` `refresh_cpm_frozen`** counts only rows where
--     (`frozen_section_result_raw IS NOT NULL OR source_of_data = 'synoptic_excel_parsed_column'`).
--     Replay with **that** filter ⇒ **0** mismatch vs PM; **`frozen_any_performed_flag` 0-diff** vs
--     `(COUNT(*)>0)` on canonical events filtered the same OR synoptic rollup OR NLP rollup per 360 —
--     aligns `project_frozen_section_mig_100_closeout` + gland/path family closeout (**2026-04-28**).
--
--   **Staging**
--   - **`ajcc7_missing_components` / `ajcc8_missing_components`**: deterministic STRING_AGG payloads;
--     calendar-safe comparison uses **`list_sort(string_split(trim(...),','))`** set-equal pattern per
--     `feedback_no_crossdomain_linkage_ids.md` (spot-check parity on representative patients;
--     full sort-order independence for carry-forwards preserved in builder).
--
--   **Demographics**
--   - **`ages_score`** on CPM matches **`scripts/51b_thyroid_scoring_python.py` `compute_ages`** semantics
--     (**0.1×** age-component for ≥40 bracket with zero-below-40-age-term, capped size term, gross ETE + mets —
--     **not** the 0.05× variant from some historical vignettes).
--   - **BMI**: multi-source BMI stack (operative / clinic / NLP note extraction) validated against PM build SSOT —
--     anesthesia vs clinic precedence per frozen Script **204**/205 consolidation lineage.
--
-- Active parallel lanes (registry-only; do not touch adjacent commits): mig_137, mig_138, mig_140,
-- mig_141, mig_142.
--
-- Applied on MotherDuck RW (`thyroid_canonical_publication_v1_0`).
-- =============================================================================

BEGIN TRANSACTION;


-- -----------------------------------------------------------------------------
-- 143a — 8 cols — FNA Bethesda + rollup passthrough columns (canonical_fna_patient_rollup_v1)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_fna_patient_rollup_v1',
    batch_id            = 'mig_143_patient_master_small_clusters_bundle_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_143 small-clusters Lane 32 (FNA/rollup shard). bitwise 0-drift replay vs '
                          || 'main.canonical_fna_patient_rollup_v1 (mig_95b) per prompt6_356 CPM_REPOINT_MAP; '
                          || 'feeder SSOT canonical_fna_events_v1 mig_78/mig_96 family.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'fna_bethesda_confidence',
    'fna_bethesda_final',
    'fna_bethesda_source',
    'fna_confidence',
    'n_bethesda_calculated_fnas',
    'worst_bethesda_num',
    'worst_bethesda_source'
  );


-- -----------------------------------------------------------------------------
-- 143b — 1 col — n_bethesda_number_only_fnas (canonical_fna_events_v1 aggregation)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_fna_events_v1',
    batch_id            = 'mig_143_patient_master_small_clusters_bundle_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_143 small-clusters Lane 32 (FNA/events shard). mechanical replay: '
                          || 'COUNT(*) WHERE bethesda_calculated_num IS NULL AND bethesda_final_num IS NOT NULL '
                          || 'LEFT JOIN-gap COALESCE(..,0)=0 semantics — 0-diff vs PM (prompt6_356 literal).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name = 'n_bethesda_number_only_fnas';


-- -----------------------------------------------------------------------------
-- 143c — 3 cols — FNA ↔ surgical pathology concordance + pathway (consolidated chain)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_fna_path_concordance_chain',
    batch_id            = 'mig_143_patient_master_small_clusters_bundle_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_143 small-clusters Lane 32 (concordance shard). Bethesda category vs '
                          || 'final histology adjudication taxonomy (Scripts 205/265 lineage; TP/TN/discord '
                          || 'lanes per surgical cohort enrichment); BOOL `fna_path_concordant` non-degenerate.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'fna_path_concordance_category',
    'fna_path_concordant',
    'fna_path_outcome',
    'fna_pathway_status'
  );


-- -----------------------------------------------------------------------------
-- 143d — 10 cols — demographics + AGES/BMI/note weight ladder
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_clinical_master_demographics',
    batch_id            = 'mig_143_patient_master_small_clusters_bundle_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_143 demographics shard. sex/race passthrough cohort spine + normalized enums; '
                          || 'combined with derivation_ages_arithmetic / derivation_bmi_hierarchy overlays.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'race',
    'sex'
  );


UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_bmi_hierarchy',
    batch_id            = 'mig_143_patient_master_small_clusters_bundle_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_143 BMI shard. anesthesia_vs_clinic_vs_NLP-note extraction ladder (+ '
                          || 'missingness reason stamping) per operative/clinic consolidation feeders.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'bmi_combined',
    'bmi_missingness_reason',
    'bmi_note_extracted',
    'bmi_note_source',
    'bmi_source',
    'weight_kg_note'
  );


UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_ages_arithmetic',
    batch_id            = 'mig_143_patient_master_small_clusters_bundle_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_143 AGES shard. matches thyroid_scoring_python `compute_ages` (**0.1** age scaling '
                          || 'tier, not obsolete 0.05 vignette); CF-mig143-AGES-CALC-ALLTRUE: flags all TRUE on cohort.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'ages_calculable_flag',
    'ages_score'
  );


-- -----------------------------------------------------------------------------
-- 143e — 5 cols — frozen section rollup vs canonical frozen events (filtered count)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_frozen_section_events_v1',
    batch_id            = 'mig_143_patient_master_small_clusters_bundle_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_143 frozen shard. Replay vs canonical_frozen_section_events_v1 mig_100 USING '
                          || 'filter (result_raw populated OR synoptic_excel parsed) matching Script**360** '
                          || '`frozen_n_total`; naive unfiltered COUNT≠PM by design (451-patient informational delta). '
                          || 'syn_* columns synoptic pathology slice paired with mig_119 rollup close-out.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'frozen_any_performed_flag',
    'frozen_n_total',
    'frozen_source_hierarchy',
    'syn_frozen_section',
    'syn_frozen_section_result'
  );


-- -----------------------------------------------------------------------------
-- 143f — 3 cols — AJCC missing-component strings + calculability flag (non-naive semantics)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_ajcc_calculability_check',
    batch_id            = 'mig_143_patient_master_small_clusters_bundle_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_143 AJCC-component shard STRING_AGG payloads; **`ajcc8_calculable_flag` NOT naive '
                          || '“(T+N+M+age) ALL non-null”** — see CF-mig143-AJCC8-CALC-NOT-NAIVE4 vs naive probe **112**. '
                          || 'Ordering-independent equality via list_sort/tokenizer on CSV-style missing lists.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'ajcc7_missing_components',
    'ajcc8_calculable_flag',
    'ajcc8_missing_components'
  );


-- -----------------------------------------------------------------------------
-- 143 — refresh canonical_patient_master row on canonical_table_signoff_registry_v1
-- -----------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1 AS ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes             = COALESCE(ts.notes,'')
                        || ' | mig_143: small-clusters bundle CLOSED (**30** cols verified: FNA+Demo+Frozen+Staging).'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'       THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'           THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;


COMMIT;


-- =============================================================================
-- end migration 143 — +30 cols verified (~881→911 thematic target on CPM rollup)
-- =============================================================================
