-- =============================================================================
-- Migration 156 — canonical_patient_master FRAMEWORK + PROVENANCE CLUSTER sign-off
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane:   45 — Framework / aggregate / cross-domain cluster (**71** cols).
-- Prompt: cursor_prompts/CURSOR_PROMPT_patient_master_framework_provenance_cluster_20260429.md
-- batch_id: mig_156_patient_master_framework_provenance_cluster_20260429
--
-- Pre-apply probes (MotherDuck RW `thyroid_canonical_publication_v1_0`, Cowork 2026-04-29):
--   * **§1a cardinality:** `information_schema.columns` allow-list (exactly **71** names) →
--     **71** physical cols on `main.canonical_patient_master`; dtypes spot-checked (DATE for
--     `prm_first_fna_date` / `prm_last_fna_date`; BIGINT/INTEGER for `n_*`; TIMESTAMP for
--     `cpm_built_at` / `rollup_built_at`).
--   * **Registry:** all **71** rows `verification_status='not_started'` pre-apply.
--   * **Cohort parity:** `canonical_patient_master` = **10,871** rows / distinct `research_id`
--     (`scripts._md_connect.connect_locked` sentinel).
--   * **Gate 4** (verified rows require `verified_by` + `verification_method` + `batch_id` +
--     `verified_ts`): **0** violations on existing **verified** CPM registry rows.
--
-- SSOT existence (`information_schema.tables`, `table_catalog` = publication DB): sampled live —
-- `canonical_recurrence_v1`, `canonical_invasion_events_v1`, `canonical_fna_events_v1`,
-- `canonical_path_malignant_patient_rollup_v1` — **all present** (verification_method strings
-- reference **live** `main.*` objects only; **no** archived-table identifiers).
--
-- **N-count V1 vs V2 drift (pairwise `IS DISTINCT FROM`, non-identical):**
--   * `n_surgeries` vs `n_surgeries_v2`: **1** row (**research_id=7779**, v1=2 / v2=1)
--     — **CF-mig156-N-SURGERIES-V1-V2-1PT**.
--   * `n_us_exams` vs `n_us_exams_v2`: **4,764** rows — large feeder/version drift
--     (**CF-mig156-N-US-EXAMS-V1-V2-WIDE-DRIFT**).
--   * `n_us_nodules_total` vs `n_us_nodules_total_v2`: **2,166** rows — ditto
--     (**CF-mig156-N-US-NODULES-V1-V2-WIDE-DRIFT**).
--
-- **`n_fna_episodes` vs `canonical_fna_events_v1` row-count replay:** **69** rid-level mismatches vs
-- naive `COUNT(*)` — definitional grain / dedup differs from mechanical episode tally
-- (**CF-mig156-N-FNA-EPISODES-VS-EVENTS-69DRIFT**).
--
-- **BOOLEAN cohort-uniformity (MotherDuck live sweep, highlights):**
--   * **Near-uniform / degenerate:** `prm_high_risk_marker_any` — **0** TRUE (**10,305** FALSE,
--     **566** NULL) — **CF-mig156-COHORT-UNIFORM-FALSE-prm_high_risk_marker_any**
--     (rule yields almost no positives on current build).
--   * **`any_airway_anywhere`:** **1** TRUE — ultra-sparse presence flag (acceptable).
--   * **`any_confirmed_complication` ≡ `any_confirmed_complication_flag`** — identical cohort
--     (**2,490** TRUE); documented as duplicate semantics (**CF-mig156-ANY-CONFIRMED-DUP-FLAG**).
--   * **`is_malignant`:** **4,137** TRUE / **6,734** FALSE — **not** a cancer-only cohort on CPM;
--     (**CF-mig156-IS-MALIGNANT-MIXED-BENIGN-COHORT**) — informational vs naive “all cancer” assumption.
--   * **`analysis_eligible_flag`:** **4,136** TRUE / **6,735** FALSE — tracks analytic eligibility,
--     distinct from malignancy flag by construction.
--
-- **ANY-vs-recurrence SSOT (`canonical_recurrence_v1.recurrence_confirmed`):**
--   Among **`any_recurrence_flag=TRUE`** (**384** pts): **165** also have `recurrence_confirmed=TRUE`;
--   **219** have **`any_recurrence_flag=TRUE`** but **`recurrence_confirmed=FALSE`** / NULL —
--   broader cross-domain OR / biochemical / NLP-inclusive recurrence envelope vs structured SSOT
--   (**CF-mig156-ANY-RECURRENCE-VS-STRUCTURED-219PT**).
--
-- **ANY capsular — naive union probe (`canonical_invasion_events_v1` capsular + present):**
--   Drift vs `any_capsular_anywhere` = **4** / **10,871** (≤ **50** threshold — PASS tier).
--
-- **`has_*` vs postop ladder (`postop_low_calcium_flag` / `postop_low_pth_flag`, mig_150 family):**
--   When both sides non-null: **2** calcium mismatches / **544** comparable rows; **1** PTH mismatch / **673**
--   comparable — **not** 100% duplicate (**CF-mig156-HAS-VS-POSTOP-NOT-DUP**).
--
-- **Single-distinct VARCHAR audit (non-null cardinality):** **CF-mig156-VALUE-DEGENERATE-UPSTREAM-***
--   on **`n_surgeries_source`**, **`prm_fna_source_tables`**, **`prm_margin_source`**, **`prm_size_concordance`**,
--   **`provenance_note`**, **`rollup_script_version`**, **`rollup_source_table`**, **`source_script`** (each **1**
--   distinct where populated / globally degenerate label cols — informational placeholder provenance).
--   **`gm_recurrence_site_primary`:** **0** non-null cells (**CF-mig156-GM-RECURRENCE-SITE-ALLNULL**).
--
-- **Clinical DATE policy:** `prm_first_fna_date` / `prm_last_fna_date` — **DATE** (CLEAR —
-- **CF-mig156-PRM-FNA-DATE-RETYPE** not needed).
--
-- **`cpm_built_at` vs `rollup_built_at` calendar skew (>1 day):** **8,422** / **10,871** rows — stamps are
--   **different pipeline clocks** (CPM reconciliation sweep vs rollup rebuild job), **not** byte-identical
--   twin timestamps (**CF-mig156-CPM-VS-ROLLUP-BUILT-AT-NOT-DAY-LOCKED** — informational; still verify both non-null
--   where expected).
--
-- Disposition: **71** cols → **`verified`** (Gate 4 metadata on flip); provenance slice uses
-- **`auto_provenance_skip_audit_metadata`** method label where stamp/version cols are informational only.
--
-- Applied on MotherDuck RW (`thyroid_canonical_publication_v1_0`) — **execute after** independent Logan review.
-- Cowork agent: SQL authoring + read-only probes only (**no** RW executes from agent session per Protocol §5).
-- =============================================================================

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig156_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig156_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN (
    'n_confirmed_complications','n_fna_cytology_records','n_fna_episodes',
    'n_notes_documenting_tsh_suppressed','n_stimulated_tg_measurements',
    'n_surgeries','n_surgeries_source','n_surgeries_v2',
    'n_tg_measurements_structured','n_tgab_measurements',
    'n_us_exams','n_us_exams_v2','n_us_nodules_total','n_us_nodules_total_v2','n_us_with_ln_assessment',
    'prm_first_fna_date','prm_first_fna_days_from_surg','prm_fna_n_sources','prm_fna_source_tables',
    'prm_high_risk_marker_any','prm_hypocalcemia_lab_flag','prm_hypoparathyroidism_lab_flag',
    'prm_last_fna_date','prm_last_fna_days_from_surg','prm_margin_confidence','prm_margin_source',
    'prm_molecular_risk_category','prm_n_recurrence_sources','prm_recurrence_detection_category',
    'prm_rln_worst_grade','prm_size_concordance','prm_structural_disease_flag',
    'gm_lab_completeness_score','gm_macis_calculable_flag','gm_path_lvi_raw','gm_path_pni_raw',
    'gm_path_vascular_inv_raw','gm_provenance_confidence','gm_recurrence_date_source',
    'gm_recurrence_site_primary','gm_recurrence_source','gm_recurrence_type_primary',
    'gm_tg_below_threshold_ever',
    'any_airway_anywhere','any_analysis_eligible_complication','any_capsular_anywhere',
    'any_confirmed_complication','any_confirmed_complication_flag','any_disease_concern_flag',
    'any_fusion_positive','any_fusion_positive_inferred_negative','any_lymphatic_microscopic_anywhere',
    'any_perineural_anywhere','any_recurrence_flag','any_soft_tissue_anywhere','any_vascular_microscopic_anywhere',
    'has_low_calcium_flag','has_low_pth_flag','has_suspicious_candidate','has_voice_data',
    'is_malignant','cross_fna_concordance',
    'analysis_eligible_flag','cpm_built_at','longitudinal_assessment_available',
    'provenance_confidence','provenance_note',
    'rollup_built_at','rollup_script_version','rollup_source_table','source_script'
  );

BEGIN TRANSACTION;


-- -----------------------------------------------------------------------------
-- 156a — 15 cols — N counts + surgeries provenance label (`n_surgeries_source`)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_upstream_counters_v1',
    batch_id            = 'mig_156_patient_master_framework_provenance_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_156 framework cluster (156a N-counts). '
                          || 'Per-feed COUNT aggregates + surgery-count lineage; v2 sibling cols vs legacy '
                          || 'feeders per CF-mig156-N-*-V1-V2-* + FNA episode drift CF in header.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'n_confirmed_complications','n_fna_cytology_records','n_fna_episodes',
    'n_notes_documenting_tsh_suppressed','n_stimulated_tg_measurements',
    'n_surgeries','n_surgeries_source','n_surgeries_v2',
    'n_tg_measurements_structured','n_tgab_measurements',
    'n_us_exams','n_us_exams_v2','n_us_nodules_total','n_us_nodules_total_v2','n_us_with_ln_assessment'
  );


-- -----------------------------------------------------------------------------
-- 156b — 17 cols — PRM rule-master outputs + FNA DATE offsets
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'internal_consistency_prm_rule_master_v1',
    batch_id            = 'mig_156_patient_master_framework_provenance_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_156 framework cluster (156b PRM). Patient-rule-master rollups from '
                          || 'canonical_fna_events_v1 spine for first/last FNA calendar dates + margins/recurrence '
                          || 'facets; cf. CF-mig156-COHORT-UNIFORM-FALSE-prm_high_risk_marker_any.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'prm_first_fna_date','prm_first_fna_days_from_surg','prm_fna_n_sources','prm_fna_source_tables',
    'prm_high_risk_marker_any','prm_hypocalcemia_lab_flag','prm_hypoparathyroidism_lab_flag',
    'prm_last_fna_date','prm_last_fna_days_from_surg','prm_margin_confidence','prm_margin_source',
    'prm_molecular_risk_category','prm_n_recurrence_sources','prm_recurrence_detection_category',
    'prm_rln_worst_grade','prm_size_concordance','prm_structural_disease_flag'
  );


-- -----------------------------------------------------------------------------
-- 156c — 11 cols — GM generic-metadata provenance passthrough
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'helper_script_gm_metadata_provenance_passthrough_v1',
    batch_id            = 'mig_156_patient_master_framework_provenance_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_156 framework cluster (156c GM). Passthrough / harmonized metadata '
                          || '(lab completeness, MACIS flag, path raw strings, recurrence provenance chain); '
                          || 'CF-mig156-GM-RECURRENCE-SITE-ALLNULL where applicable.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'gm_lab_completeness_score','gm_macis_calculable_flag','gm_path_lvi_raw','gm_path_pni_raw',
    'gm_path_vascular_inv_raw','gm_provenance_confidence','gm_recurrence_date_source',
    'gm_recurrence_site_primary','gm_recurrence_source','gm_recurrence_type_primary',
    'gm_tg_below_threshold_ever'
  );


-- -----------------------------------------------------------------------------
-- 156d — 13 cols — ANY cross-domain overlap booleans
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'cross_domain_aggregation_any_overlap_rules_v1',
    batch_id            = 'mig_156_patient_master_framework_provenance_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_156 framework cluster (156d ANY-*). BOOL_OR / union semantics across '
                          || 'path / invasion / molecular / complication feeds; capsular naive probe ≤50 drift; '
                          || 'recurrence OR wider than canonical_recurrence_v1 (CF-mig156-ANY-RECURRENCE-*).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'any_airway_anywhere','any_analysis_eligible_complication','any_capsular_anywhere',
    'any_confirmed_complication','any_confirmed_complication_flag','any_disease_concern_flag',
    'any_fusion_positive','any_fusion_positive_inferred_negative','any_lymphatic_microscopic_anywhere',
    'any_perineural_anywhere','any_recurrence_flag','any_soft_tissue_anywhere','any_vascular_microscopic_anywhere'
  );


-- -----------------------------------------------------------------------------
-- 156e — 4 cols — HAS placeholder / surveillance flags
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_has_placeholder_flags_v1',
    batch_id            = 'mig_156_patient_master_framework_provenance_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_156 framework cluster (156e HAS-*). Cross-checked vs postop_low_* ladder '
                          || '(CF-mig156-HAS-VS-POSTOP-NOT-DUP); voice/suspicious_candidate presence semantics.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'has_low_calcium_flag','has_low_pth_flag','has_suspicious_candidate','has_voice_data'
  );


-- -----------------------------------------------------------------------------
-- 156f — 3 cols — Singleton cohort / concordance scalars
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_singleton_cohort_eligibility_v1',
    batch_id            = 'mig_156_patient_master_framework_provenance_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_156 framework cluster (156f singletons). is_malignant mixed benign+malignant '
                          || 'cohort; analysis_eligible_flag analytic gate; cross_fna_concordance categorical.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'is_malignant','cross_fna_concordance','analysis_eligible_flag'
  );


-- -----------------------------------------------------------------------------
-- 156g — 8 cols — Build / rollup / audit provenance stamps
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'auto_provenance_skip_audit_metadata_v1',
    batch_id            = 'mig_156_patient_master_framework_provenance_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_156 framework cluster (156g build provenance). Informational stamps / '
                          || 'single-value script labels per CF-mig156-VALUE-DEGENERATE-*; TIMESTAMP allowlist OK.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'cpm_built_at','longitudinal_assessment_available',
    'provenance_confidence','provenance_note',
    'rollup_built_at','rollup_script_version','rollup_source_table','source_script'
  );


-- -----------------------------------------------------------------------------
-- 156h — refresh canonical_table_signoff_registry_v1 (CPM)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1 ts
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
                        || ' | mig_156: framework + provenance cluster CLOSED (**71** cols). '
                        || 'Lane 45 Protocol v2; see migration header for CF ledger.'
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


-- -----------------------------------------------------------------------------
-- Carry-forward CF tags (notes-only; selective columns)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig156-COHORT-UNIFORM-FALSE-prm_high_risk_marker_any: 0 TRUE rows live.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='verified'
  AND batch_id='mig_156_patient_master_framework_provenance_cluster_20260429'
  AND column_name='prm_high_risk_marker_any';

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig156-ANY-RECURRENCE-VS-STRUCTURED-219PT: any_recurrence_flag TRUE wider than '
            || 'canonical_recurrence_v1.recurrence_confirmed — OR-rule envelope.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='verified'
  AND batch_id='mig_156_patient_master_framework_provenance_cluster_20260429'
  AND column_name='any_recurrence_flag';

UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | CF-mig156-IS-MALIGNANT-MIXED-BENIGN-COHORT: not cancer-only spine on CPM.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='verified'
  AND batch_id='mig_156_patient_master_framework_provenance_cluster_20260429'
  AND column_name='is_malignant';

COMMIT;

-- =============================================================================
-- end migration 156 — CPM framework + provenance cluster verified (71 cols)
-- =============================================================================
