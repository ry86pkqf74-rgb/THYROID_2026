-- =============================================================================
-- Migration 150 -- canonical_patient_master PARATHYROID + POSTOP + TP CLUSTER sign-off
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane:   40 — parathyroid + postoperative labs + TP (pathology LN) thematic slice (~37 cols).
-- batch_id: mig_150_patient_master_parathyroid_postop_tp_cluster_20260429
--
-- Probe cardinality (documentation SSOT — repo grep of `data_dictionary.md` aligned to
-- `scripts/output/_cpm_tirads_audit_*` probe lists): **37** scoped columns —
-- **16** parathyroid (13 `para_*` + 3 `pth_*`) + **12** `postop_*` + **9** `tp_*`.
--
-- **≠ 38 discrepancy:** Prompt estimated 10 × `tp_*`; canonical CPM exposes **nine**
-- LN-pathology aggregates from `scripts/frozen/205_canonical_consolidation.py` block
-- `tp_ln` (PRIMARY LN fields from tumor_pathology / ln_master rollup; **NOT** thyroid-
-- hormone “perioperative TSH/T4” semantics). Naming is **tp_** = pathology **tumor**
-- primary LN rollup (cf. ln_crossval / 207 TP-vs-PS concordance), not abbreviation for
-- “thyroid perioperative hormones.” Tier-1 probe: `_us_v2_probe.json` agrees (9 cols).
--
-- Anchor SSOT tables (Cowork methodology):
--   * canonical_parathyroid_events_v1 + canonical_parathyroid_patient_rollup_v1 —
--     mig_102/mig_106 families (intent / gland-level counts fidelity).
--   * canonical_labs_pth_v1 — mig_115/script-347 lineage (mig_134 verified sibling
--     `lab_pth_*` wide rollups; **pth_nadir** trio + **postop_*** were explicitly deferred
--     in mig_134 header to a dedicated post-op slice — **this** migration).
--   * canonical_complications_* — mig_99/mig_108/mig_135; hypopara/hypocal **clinical**
--     phenotype is **NOT** interchangeable with biochemical `postop_low_*_flag`
--     (orthogonal evidence tiers; directional cross-check OK, ±5 pp rule not FAILED when
--     definitions differ — **CF-mig150-PARA-BIOCHEM-VS-COMPL-TIER**, informational).
--
-- Parathyroid `para_*` rollup — **scripts/221b_final_gap_resolution.py** parathyroid
-- keyed extract + patient-level aggregates (counts, cellularity, weight, categorical
-- intent/abnormality) vs upstream workbook rows.
--
-- Postoperative `postop_*` window — patient-level aggregates from NLP/structured expanded
-- post-op lab pools; representative builder **scripts/211_canonical_gap_fill.py**
-- (`build_postop_labs_sql` → extracted_postop_labs_expanded_v1 aggregates + days_postop join).
-- **postop_labs_has_data**: sentinel TRUE per 211 rollup group when labs present.
--
-- Biochemical LOW flags — **repository coding SSOT scripts/53_longitudinal_lab_hardening.py**
-- line 344–346: `postop_low_pth_flag` = BOOL_OR(pt < **15 pg/mL** AND days_post_surgery
-- BETWEEN 0 AND 30); **NOT** informal 12 pg/mL anecdotes — document as
-- **CF-mig150-BIOCHEM-PTH-THRESH-SSOT-15** vs prompt draft “12 pg/mL” wording.
--
-- BOOLEAN cohort uniformity (MotherDuck live row from `data_dictionary.md` circa 04-29):
--   * postop_low_pth_flag documented fill rate ~**1.2**% TRUE vs total rows — sparse but
--     NOT near-uniform-TRUE/FALSE degeneration; mirrors limited structured PTH coverage
--     (labs lane); flagged **informational CF-mig150-LAB-SPARSITY**.
--
-- Date-type policy: **pth_nadir_days_postop** is INTEGER elapsed days (ordinal offset),
-- no calendar DATE retype (CF-mig121 pattern); no TIMESTAMP-pretending-calendar columns
-- in this slice (**CF-mig150-date-retype-clear** OK).
--
-- Active parallel lanes (do NOT touch): mig_142 RAI blocked; mig_145/146/147 imaging;
-- mig_148 RAI upstream; sibling lane clusters 149/151/152 schedule.
--
-- Gate 4 (verified cols require verified_by + verification_method + batch_id + verified_ts):
-- **0** violations expected when applied to **not_started** rows only — pre-checked pattern
-- matches mig_149 sibling.
--
-- Executed on MotherDuck RW (`thyroid_canonical_publication_v1_0`).
-- =============================================================================

BEGIN TRANSACTION;


-- -----------------------------------------------------------------------------
-- 150a — 9 cols — parathyroid morphology / pathology counts aggregate
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'patient_level_aggregate_parathyroid_per_gland_221b',
    batch_id            = 'mig_150_patient_master_parathyroid_postop_tp_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_150 para/postop/TP cluster (Lane 40). '
                          || 'Gland enumeration + cellularity + weight extremes; '
                          || 'anchors canonical_parathyroid_events_v1 rollup semantics.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'para_n_glands_biopsied',
    'para_n_glands_excised',
    'para_n_glands_identified',
    'para_max_cellularity_pct',
    'para_min_cellularity_pct',
    'para_max_gland_weight_g',
    'para_has_pathologic_glands',
    'para_abnormality_type',
    'para_specimen_included'
  );


-- -----------------------------------------------------------------------------
-- 150b — 4 cols — parathyroid intent / provenance categorical + workbook lineage
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_parathyroid_patient_rollup_v1',
    batch_id            = 'mig_150_patient_master_parathyroid_postop_tp_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_150 para/postop/TP cluster (Lane 40). '
                          || 'Intent + incidental categorical + ingestion provenance; '
                          || 'cross-check complication hypopara tier is clinical not identical.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'para_removal_intent',
    'para_incidental_status_refined',
    'para_source_workbook',
    'para_source_script'
  );


-- -----------------------------------------------------------------------------
-- 150c — 3 cols — PTH nadir trilogy (canonical lab / PRM chain)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_labs_pth_v1',
    batch_id            = 'mig_150_patient_master_parathyroid_postop_tp_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_150 para/postop/TP cluster (Lane 40). '
                          || 'Nadir/min + 30d window + ordinal days-from-surgery to first Surg; '
                          || 'defer cross-footnote mig_134 lab_pth_* wide rollups sibling lane.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'pth_nadir',
    'pth_nadir_30d',
    'pth_nadir_days_postop'
  );


-- -----------------------------------------------------------------------------
-- 150d — 12 cols — postoperative inpatient/near-window lab aggregates + biochem flags
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_labs_postop_window_script211_53',
    batch_id            = 'mig_150_patient_master_parathyroid_postop_tp_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_150 para/postop/TP cluster (Lane 40). '
                          || 'postop_* from expanded labs + biochemical flags (<15 pg/mL PTH / '
                          || '<8 mg/dL Ca Script-53 lineage); ionized_ca min + presence sentinel.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'postop_calcium_min_days_postop',
    'postop_calcium_min_value',
    'postop_calcium_n_measurements',
    'postop_calcium_source_reliability',
    'postop_ionized_cal_min_value',
    'postop_labs_has_data',
    'postop_low_calcium_flag',
    'postop_low_pth_flag',
    'postop_pth_min_days_postop',
    'postop_pth_min_value',
    'postop_pth_n_measurements',
    'postop_pth_source_reliability'
  );


-- -----------------------------------------------------------------------------
-- 150e — 9 cols — TP primary pathology LN rollup (NOT thyroid-hormone “TP”!)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_tumor_pathology_primary_ln_205_consolidation',
    batch_id            = 'mig_150_patient_master_parathyroid_postop_tp_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_150 para/postop/TP cluster (Lane 40). '
                          || 'tp_* = LN examine/positive/ENE/size at PRIMARY tumor pathology '
                          || 'grain (205 consolidated block tp_ln — NOT perioperative thyroid '
                          || 'hormones). CF clarity vs informal “TP acronym.”'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'tp_central_examined',
    'tp_central_positive_total',
    'tp_ln_central_positive',
    'tp_ln_ene',
    'tp_ln_examined',
    'tp_ln_largest_deposit_cm',
    'tp_ln_lateral_positive',
    'tp_ln_levels_involved',
    'tp_ln_positive'
  );


-- -----------------------------------------------------------------------------
-- 150f — refresh canonical_table_signoff_registry_v1 for CPM (+37 n_verified)
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
    signoff_migration = 'qc_framework_v1/migrations/150_patient_master_parathyroid_postop_tp_cluster_signoff_20260429.sql',
    notes             = COALESCE(ts.notes,'')
                        || ' | mig_150: parathyroid + postop + TP(primary-LN) cluster CLOSED (37 cols verified).'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master'
  GROUP BY 1, 2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;


COMMIT;


-- =============================================================================
-- end migration 150 — CPM parathyroid + postop + TP(primary LN) (~37 cols not_started→verified)
-- =============================================================================
