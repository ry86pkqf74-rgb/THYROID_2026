-- =============================================================================
-- Migration 151 — canonical_patient_master MEDS + RADTX + PROCEDURES CLUSTER sign-off
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane:   41 — medications (`med_*` / `medications_*`) + external-beam / rad-treatment
--         NLP rollup (`radtx_*`) + procedure NLP rollup (`proc_*` / `procedure_*`).
-- batch_id: mig_151_patient_master_meds_radtx_proc_cluster_20260429
--
-- Live probe (MotherDuck RW `thyroid_canonical_publication_v1_0`, query_rw 2026-04-29):
--   Predicate matches workspace probe SQL (exclude registry rows where
--   verification_status <> 'not_started'). **37** columns remain —
--   medications **15**, procedures **14**, radtx **8**.
--
--   **≠ ~39 estimate:** gross thematic scope ~40 cols on CPM; **3** columns
--   matching the same LIKE predicates were already verified (`radtx_nlp_rai_ablation`,
--   `radtx_nlp_rai_ablation_n_mentions` — sibling RAI lineage; `proc_nlp_lateral_neck_dissection`).
--
-- * Cohort parity: `canonical_patient_master` = **10,871** rows / distinct `research_id`.
-- * **Gate 4** (verified requires verified_by + verification_method + batch_id + verified_ts):
--   updates touch **not_started** rows only — **0** pre-existing violations expected.
--
-- Methodology (Protocol v2):
--   * **Medications (`med_nlp_*`)** — Script **215** SOURCE 2 (`note_entities_medications`,
--     present-only regex aggregates: levothyroxine / calcium supplement / calcitriol + provenance +
--     `*_days_from_surg` join). Verification: deterministic replay **0-drift** vs upstream;
--     `med_nlp_note_types` / list aggregates: **set-equal** independence via `list_sort` pattern
--     per `feedback_no_crossdomain_linkage_ids.md`; STRING_AGG ordering CF **CF-mig58-STRING-AGG-ORDER**.
--     Cross-footnote: `canonical_medications_events_v1` + rollup (**mig_103** / **mig_105**) is the
--     **class/phenotype** SSOT for meds; `med_nlp_*` is **sparse note-regex** coverage — not interchangeable
--     denominators (**CF-mig151-MEDNLP-SPARSE-VS-ROLLUP**): e.g. `med_nlp_levothyroxine` TRUE **~13.7%**
--     vs naive “all post-thyroidectomy T4” expectation.
--
--   * **RadTx (`radtx_*`)** — Script **215** SOURCE 8 (`note_entities_llm_rad_treatment`, conf≥0.7,
--     present polarity). **No separate external-beam canonical table** — Tier-1 LLM SSOT acceptable
--     per Lane-41 brief (contrast mig_142 **structured** `rai_*` CPM cluster). This slice includes
--     NLP flags for **thyrogen / hormone withdrawal / post-tx scan** (RAI **prep/outcome** mentions in
--     the same LLM bundle) plus **`radtx_nlp_external_beam_radiation`** (rare); `radtx_nlp_rai_ablation*`
--     already verified in registry — do not duplicate.
--     **CF-mig151-DATE-RETYPE-clear:** no `radtx_*` **calendar** columns in this remainder; only
--     BOOLEAN / DOUBLE / VARCHAR / BIGINT provenance — aligns `feedback_clinical_dates_calendar_only.md`
--     (would apply if future `radtx_first_date` lands as TIMESTAMP).
--
--   * **Procedures (`proc_nlp_*`)** — Script **215** SOURCE 6 (`note_entities_procedures`, present-only
--     regex rollup). **`canonical_operative_procedure_codes_v1` (mig_118)** is a **different grain**
--     (per-mention LLM rows + linkage — Script 362); `proc_nlp_*` is **patient-level regex** rollup —
--     verify vs `note_entities_procedures`, not bitwise vs procedure_codes (**CF-mig151-PROC-NLP-VS-CODES-GRAIN**).
--
-- Cohort-uniformity (BOOLEAN sanity, 2026-04-29 live):
--   * `med_nlp_levothyroxine` TRUE **1,491** / 10,871 (~**13.7%**) — sparse NLP; **not** failed vs
--     rollup expectation (**CF-mig151-MEDNLP-SPARSE-VS-ROLLUP**).
--   * `med_nlp_calcium_supplement` TRUE **304** (~**2.8%**) — sparse; consistent with note-regex.
--   * `radtx_nlp_external_beam_radiation` TRUE **18** (~**0.17%**) — rare EBRT; expected.
--   * `proc_nlp_laryngoscopy` TRUE **270** (~**2.5%**) — low-moderate; non-degenerate.
--
-- Active parallel lanes (do not touch): mig_142 RAI blocked for **structured** `rai_*`; mig_145–147 imaging;
-- mig_148 RAI upstream; sibling clusters 149/150/152.
--
-- Executed on MotherDuck RW (`thyroid_canonical_publication_v1_0`).
-- =============================================================================

BEGIN TRANSACTION;

-- -----------------------------------------------------------------------------
-- 151a — 15 cols — medications (Script 215 regex / note_entities_medications)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'extraction_faithfulness_vs_note_entities_medications_script215',
    batch_id            = 'mig_151_patient_master_meds_radtx_proc_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_151 meds/radtx/proc cluster (Lane 41). med_nlp_*: '
                          || 'Script 215 SOURCE2 replay vs note_entities_medications present-only; '
                          || 'list_sort note_types for STRING_AGG order-independence (CF-mig58). '
                          || 'CF-mig151-MEDNLP-SPARSE-VS-ROLLUP: not canonical_medications_events tier.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'med_nlp_calcitriol',
    'med_nlp_calcitriol_date',
    'med_nlp_calcitriol_days_from_surg',
    'med_nlp_calcitriol_n_mentions',
    'med_nlp_calcium_supplement',
    'med_nlp_calcium_supplement_date',
    'med_nlp_calcium_supplement_days_from_surg',
    'med_nlp_calcium_supplement_n_mentions',
    'med_nlp_extraction_method',
    'med_nlp_levothyroxine',
    'med_nlp_levothyroxine_date',
    'med_nlp_levothyroxine_days_from_surg',
    'med_nlp_levothyroxine_n_mentions',
    'med_nlp_n_source_notes',
    'med_nlp_note_types'
  );

-- -----------------------------------------------------------------------------
-- 151b — 8 cols — rad treatment LLM rollup (note_entities_llm_rad_treatment / Script 215)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'extraction_faithfulness_vs_note_entities_llm_rad_treatment_script215',
    batch_id            = 'mig_151_patient_master_meds_radtx_proc_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_151 radtx remainder (Lane 41). Tier-1 LLM SSOT; '
                          || 'radtx_nlp_rai_ablation* already verified (sibling). '
                          || 'Includes RAI-prep NLP flags in same bundle as EBRT flag.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'radtx_llm_extraction_method',
    'radtx_llm_mean_confidence',
    'radtx_llm_n_source_notes',
    'radtx_nlp_external_beam_radiation',
    'radtx_nlp_has_data',
    'radtx_nlp_hormone_withdrawal',
    'radtx_nlp_post_tx_scan_negative',
    'radtx_nlp_thyrogen_prep'
  );

-- -----------------------------------------------------------------------------
-- 151c — 14 cols — procedures regex rollup (note_entities_procedures / Script 215)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'extraction_faithfulness_vs_note_entities_procedures_script215',
    batch_id            = 'mig_151_patient_master_meds_radtx_proc_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_151 proc_nlp_* (Lane 41). Regex patient rollup vs '
                          || 'note_entities_procedures; proc_nlp_lateral_neck_dissection '
                          || 'verified separately; canonical_operative_procedure_codes_v1 = '
                          || 'mention+linkage grain (CF-mig151-PROC-NLP-VS-CODES-GRAIN).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'proc_nlp_extraction_method',
    'proc_nlp_laryngoscopy',
    'proc_nlp_laryngoscopy_date',
    'proc_nlp_laryngoscopy_days_from_surg',
    'proc_nlp_laryngoscopy_n_mentions',
    'proc_nlp_mrnd',
    'proc_nlp_mrnd_n_mentions',
    'proc_nlp_n_source_notes',
    'proc_nlp_note_types',
    'proc_nlp_parathyroid_autotransplant',
    'proc_nlp_tracheostomy',
    'proc_nlp_tracheostomy_date',
    'proc_nlp_tracheostomy_days_from_surg',
    'proc_nlp_tracheostomy_n_mentions'
  );

-- -----------------------------------------------------------------------------
-- 151d — refresh canonical_table_signoff_registry_v1 for CPM (+37 n_verified)
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
    signoff_migration = 'qc_framework_v1/migrations/151_patient_master_meds_radtx_proc_cluster_signoff_20260429.sql',
    notes             = COALESCE(ts.notes,'')
                        || ' | mig_151: meds + radtx LLM remainder + proc_nlp cluster CLOSED (37 cols verified).'
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
-- end migration 151 — CPM meds + radtx + procedures NLP cluster (~37 cols not_started→verified)
-- =============================================================================
