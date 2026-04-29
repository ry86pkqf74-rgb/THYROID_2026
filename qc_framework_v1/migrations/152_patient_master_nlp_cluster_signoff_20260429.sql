-- =============================================================================
-- Migration 152 — canonical_patient_master NLP CLUSTER sign-off (Lane 42 / mig_152)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- batch_id: mig_152_patient_master_nlp_cluster_20260429
--
-- Live probe (MotherDuck RW `thyroid_canonical_publication_v1_0`, 2026-04-29):
--   * `canonical_patient_master` = **10,871** rows / distinct `research_id`.
--   * `information_schema` **nlp_%** on CPM = **128** cols; registry already **verified** = **12**
--     (`nlp_ne_procedures_*` mig_130; `nlp_path_ete_mentioned` + `nlp_path_histology_mentioned` mig_140/132;
--     `nlp_synoptic_*` mig_132; `nlp_raidetail_*` mig_142).
--   * **Remainder flipped here: 116** cols (`verification_status = 'not_started'` → `verified` or `na`).
--
-- Tier-1 inventory in **main** (note_entities_llm_*): **16** tables — several Script-212
-- tier-3 names (tg_kinetics, labs, physical_exam, survival_followup, imaging, rad_treatment,
-- patient_decision_adherence, functional_outcomes, us_nodule_dynamics) are **absent** from
-- publication `main` (CF-mig152-MISSING-TIER1-ROLLUP-TABLES). Non-LLM `note_entities_*`
-- rollups for `nlp_ne_complications_*`, `*_genetics_*`, `*_medications_*`, `*_problemlist_*`,
-- `*_staging_*` are also absent in `main` (complications/genetics/meds/problem_list live in
-- canonical event families or legacy attach — CF-mig152-MISSING-NEROLLUP-SOURCES).
--
-- Extraction replay (JSON `$.entities`, confidence ≥ 0.5, present_or_negated in ('present', NULL))
-- matches Script **212** / **215** / **369** / **382** / **384** lineage — **no `error` column**
-- on these tables; failed parses are filtered by `result_json` shape guards (212 template).
--
-- Evidence snapshots (live):
--   * **nlp_pmhx_* replay**: 0 drift vs `note_entities_llm_past_medical_hx` (counts + has_data).
--   * **nlp_ne_operative_* replay**: 0 drift vs `note_entities_operative_detail` row counts.
--   * **nlp_rec_earliest_days_from_surg**: 0 mismatch vs `DATE_DIFF('day', first_surgery_date,
--     nlp_rec_earliest_date)` where both anchors populated.
--   * **Structured vs NLP recurrence**: `canonical_recurrence_v1.recurrence_confirmed=TRUE` AND
--     `nlp_rec_any_mentioned` NOT TRUE on **473** patients (**4.35%**) — structured SSOT wins
--     (mig_123); below **5%** formal CF threshold — footnote **CF-mig152-NLP-REC-VS-STRUCTURAL-SPARSE**.
--   * **BOOLEAN uniformity** (BOOLEAN `nlp_*` not_started only): no all-TRUE / all-FALSE degeneracy;
--     `nlp_esoph_*` TRUE rates ~0.5% (expected rare domain).
--   * **TIRADS NLP vs structured imaging**: no comparable `max_tirads_ever` column on CPM in v1_0 —
--     **CF-mig152-NLP-TIRADS-VS-IMAGING-PENDING** (coordinate mig_145–147 imaging lane).
--   * **nlp_raidetail_* / mig_148**: already **verified** under mig_142 — do not duplicate; if mig_148
--     adds RAI canonical cross-checks later, append notes there (CF-mig152-NLP-RAIDETAIL-COVERED-MIG142).
--
-- NULL semantics: many `nlp_<D>_has_data` use **NULL** when absent (not FALSE) — cohort uniformity
-- treats NULL as “no Tier-1 signal,” distinct from explicit FALSE (e.g. `nlp_path_has_data` post-369).
--
-- Gate 4: `verified` rows require `verified_by`, `verification_method`, `batch_id`, `verified_ts`.
--         `na` rows use the same provenance columns per mig_140 pattern.
--
-- Active parallel lanes (do not touch in other commits): mig_142 RAI blocked slice; mig_145–147 imaging;
-- mig_148 RAI upstream; sibling clusters 149–151.
--
-- Executed on MotherDuck RW (`thyroid_canonical_publication_v1_0`).
-- =============================================================================

BEGIN TRANSACTION;

-- -----------------------------------------------------------------------------
-- 152a — 46 cols — Tier-1 / non-LLM rollup tables absent in publication `main` → na
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'na',
    verified_by           = 'logan',
    verification_method   = 'upstream_tier1_pending',
    batch_id                = 'mig_152_patient_master_nlp_cluster_20260429',
    verified_ts             = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                   = COALESCE(notes,'')
                              || ' | mig_152 NLP cluster (Lane 42). Tier-1 table absent in '
                              || 'thyroid_canonical_publication_v1_0.main — CF-mig152-MISSING-TIER1-'
                              || 'ROLLUP-TABLES (tg/labs/physexam/survfu/imaging/ptdecision/funcoutcome/'
                              || 'radtx/usnodule) OR CF-mig152-MISSING-NEROLLUP-SOURCES (ne complications/'
                              || 'genetics/meds/problemlist/staging). Re-verify when tables attach.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    -- Script-212 tier-3 LLM domains with no `note_entities_llm_*` mirror in main (36)
    'nlp_tg_has_data', 'nlp_tg_n_entities', 'nlp_tg_rising_mentioned', 'nlp_tg_undetectable_mentioned',
    'nlp_labs_has_data', 'nlp_labs_key_finding', 'nlp_labs_n_entities', 'nlp_labs_n_notes',
    'nlp_physexam_has_data', 'nlp_physexam_key_finding', 'nlp_physexam_n_entities', 'nlp_physexam_n_notes',
    'nlp_survfu_has_data', 'nlp_survfu_key_finding', 'nlp_survfu_n_entities', 'nlp_survfu_n_notes',
    'nlp_imaging_has_data', 'nlp_imaging_key_finding', 'nlp_imaging_n_entities', 'nlp_imaging_n_notes',
    'nlp_ptdecision_has_data', 'nlp_ptdecision_key_finding', 'nlp_ptdecision_n_entities', 'nlp_ptdecision_n_notes',
    'nlp_funcoutcome_has_data', 'nlp_funcoutcome_key_finding', 'nlp_funcoutcome_n_entities', 'nlp_funcoutcome_n_notes',
    'nlp_radtx_has_data', 'nlp_radtx_key_finding', 'nlp_radtx_n_entities', 'nlp_radtx_n_notes',
    'nlp_usnodule_has_data', 'nlp_usnodule_key_finding', 'nlp_usnodule_n_entities', 'nlp_usnodule_n_notes',
    -- Script-212 non-LLM `nlp_ne_*` row-count mirrors — source `note_entities_*` not in main (10)
    'nlp_ne_complications_has_data', 'nlp_ne_complications_n_rows',
    'nlp_ne_genetics_has_data', 'nlp_ne_genetics_n_rows',
    'nlp_ne_medications_has_data', 'nlp_ne_medications_n_rows',
    'nlp_ne_problemlist_has_data', 'nlp_ne_problemlist_n_rows',
    'nlp_ne_staging_has_data', 'nlp_ne_staging_n_rows'
  );

-- -----------------------------------------------------------------------------
-- 152b — 2 cols — `nlp_ne_operative_*` — non-LLM operative note_entities (Script 212 non-LLM rollup)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'extraction_faithfulness_vs_note_entities_operative_detail_rollup212',
    batch_id                = 'mig_152_patient_master_nlp_cluster_20260429',
    verified_ts             = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                   = COALESCE(notes,'')
                              || ' | mig_152 NLP cluster. Replay: COUNT(*) / EXISTS per RID vs '
                              || 'main.note_entities_operative_detail; 0 drift (live probe 2026-04-29).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN ('nlp_ne_operative_has_data', 'nlp_ne_operative_n_rows');

-- -----------------------------------------------------------------------------
-- 152c — 4 cols — past medical history LLM (Script 212 tier-3 template)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'extraction_faithfulness_vs_note_entities_llm_past_medical_hx',
    batch_id                = 'mig_152_patient_master_nlp_cluster_20260429',
    verified_ts             = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                   = COALESCE(notes,'')
                              || ' | mig_152 NLP cluster. JSON entity replay vs '
                              || 'note_entities_llm_past_medical_hx (confidence≥0.5; present/NULL polarity).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nlp_pmhx_has_data', 'nlp_pmhx_key_finding', 'nlp_pmhx_n_entities', 'nlp_pmhx_n_notes'
  );

-- -----------------------------------------------------------------------------
-- 152d — 4 cols — past surgical history LLM
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'extraction_faithfulness_vs_note_entities_llm_past_surgical_hx',
    batch_id                = 'mig_152_patient_master_nlp_cluster_20260429',
    verified_ts             = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                   = COALESCE(notes,'')
                              || ' | mig_152 NLP cluster. JSON entity replay vs '
                              || 'note_entities_llm_past_surgical_hx.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nlp_pshx_has_data', 'nlp_pshx_key_finding', 'nlp_pshx_n_entities', 'nlp_pshx_n_notes'
  );

-- -----------------------------------------------------------------------------
-- 152e — 4 cols — presenting symptoms LLM
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'extraction_faithfulness_vs_note_entities_llm_presenting_symptoms',
    batch_id                = 'mig_152_patient_master_nlp_cluster_20260429',
    verified_ts             = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                   = COALESCE(notes,'')
                              || ' | mig_152 NLP cluster. JSON entity replay vs '
                              || 'note_entities_llm_presenting_symptoms.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nlp_symptoms_has_data', 'nlp_symptoms_key_finding', 'nlp_symptoms_n_entities', 'nlp_symptoms_n_notes'
  );

-- -----------------------------------------------------------------------------
-- 152f — 4 cols — parathyroid LLM (v1 table suffix)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'extraction_faithfulness_vs_note_entities_llm_parathyroid_detail_v1',
    batch_id                = 'mig_152_patient_master_nlp_cluster_20260429',
    verified_ts             = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                   = COALESCE(notes,'')
                              || ' | mig_152 NLP cluster. JSON entity replay vs '
                              || 'note_entities_llm_parathyroid_detail_v1 (Script 284 lineage). '
                              || 'Cross-domain SSOT: canonical_parathyroid_events_v1 / mig_131 — '
                              || 'structured wins on discordance (CF-mig152-NLP-PARATHYROID-VS-CANONICAL).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nlp_parathyroid_has_data', 'nlp_parathyroid_key_finding',
    'nlp_parathyroid_n_entities', 'nlp_parathyroid_n_notes'
  );

-- -----------------------------------------------------------------------------
-- 152g — 4 cols — airway invasion LLM (v2 table)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'extraction_faithfulness_vs_note_entities_llm_airway_invasion_v2',
    batch_id                = 'mig_152_patient_master_nlp_cluster_20260429',
    verified_ts             = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                   = COALESCE(notes,'')
                              || ' | mig_152 NLP cluster. JSON entity replay vs '
                              || 'note_entities_llm_airway_invasion_v2.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nlp_airway_has_data', 'nlp_airway_key_finding', 'nlp_airway_n_entities', 'nlp_airway_n_notes'
  );

-- -----------------------------------------------------------------------------
-- 152h — 4 cols — dynamic risk response LLM
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'extraction_faithfulness_vs_note_entities_llm_dynamic_risk_response',
    batch_id                = 'mig_152_patient_master_nlp_cluster_20260429',
    verified_ts             = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                   = COALESCE(notes,'')
                              || ' | mig_152 NLP cluster. JSON entity replay vs '
                              || 'note_entities_llm_dynamic_risk_response; sparse TRUE (~25 RIDs) — '
                              || 'expected if DRR extraction shallow.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nlp_dynrisk_has_data', 'nlp_dynrisk_key_finding', 'nlp_dynrisk_n_entities', 'nlp_dynrisk_n_notes'
  );

-- -----------------------------------------------------------------------------
-- 152i — 4 cols — esophageal invasion LLM (Script 384 CPM surface + canonical esophageal family)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'extraction_faithfulness_vs_note_entities_llm_esophageal_invasion_script384',
    batch_id                = 'mig_152_patient_master_nlp_cluster_20260429',
    verified_ts             = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                   = COALESCE(notes,'')
                              || ' | mig_152 NLP cluster. JSON replay vs note_entities_llm_esophageal_invasion; '
                              || 'structured cross-check: canonical_esophageal_invasion_* (mig_93) — '
                              || 'precedence per Script 384 README (coarser op_esophageal_inv_* untouched).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nlp_esoph_has_data', 'nlp_esoph_n_entities', 'nlp_esoph_positive_mentioned', 'nlp_esoph_confidence_tier'
  );

-- -----------------------------------------------------------------------------
-- 152j — 4 cols — frozen section LLM
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'extraction_faithfulness_vs_note_entities_llm_frozen_section_detail',
    batch_id                = 'mig_152_patient_master_nlp_cluster_20260429',
    verified_ts             = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                   = COALESCE(notes,'')
                              || ' | mig_152 NLP cluster. JSON replay vs note_entities_llm_frozen_section_detail; '
                              || 'structured SSOT: canonical_frozen_section_events_v1 (mig_119) — precedence '
                              || 'documented in Script 360/369 guards.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nlp_frozensec_has_data', 'nlp_frozensec_key_finding', 'nlp_frozensec_n_entities', 'nlp_frozensec_n_notes'
  );

-- -----------------------------------------------------------------------------
-- 152k — 5 cols — cervical LN legacy rollup (`nlp_ln_*`, Script 212 tier-1 naming)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'extraction_faithfulness_vs_note_entities_llm_cervical_ln_detail_rollup212',
    batch_id                = 'mig_152_patient_master_nlp_cluster_20260429',
    verified_ts             = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                   = COALESCE(notes,'')
                              || ' | mig_152 NLP cluster. Legacy 212 rollup vs '
                              || 'note_entities_llm_cervical_ln_detail (distinct from Script 382 `nlp_cervln_*`).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nlp_ln_has_data', 'nlp_ln_n_entities', 'nlp_ln_n_notes',
    'nlp_ln_positive_mentioned', 'nlp_ln_levels_mentioned'
  );

-- -----------------------------------------------------------------------------
-- 152l — 4 cols — cervical LN Script 382 CPM Tier-2 surface (`nlp_cervln_*`)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'extraction_faithfulness_vs_note_entities_llm_cervical_ln_detail_script382',
    batch_id                = 'mig_152_patient_master_nlp_cluster_20260429',
    verified_ts             = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                   = COALESCE(notes,'')
                              || ' | mig_152 NLP cluster. CPM cols from Script 382 rollup vs same Tier-1 table; '
                              || 'structured cross-check: canonical_cervical_ln_clinical_events_v1 (mig_382).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nlp_cervln_has_data', 'nlp_cervln_n_entities', 'nlp_cervln_positive_mentioned', 'nlp_cervln_confidence_tier'
  );

-- -----------------------------------------------------------------------------
-- 152m — 4 cols — pathology Script 369 rollup quad (distinct from legacy 212 semantic flags)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'extraction_faithfulness_vs_canonical_pathology_clinical_rollups_script369',
    batch_id                = 'mig_152_patient_master_nlp_cluster_20260429',
    verified_ts             = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                   = COALESCE(notes,'')
                              || ' | mig_152 NLP cluster. CPM quad replayed from '
                              || 'canonical_pathology_clinical_patient_rollup_v1 + positive entity slice '
                              || '(Script 369) — not raw JSON MODE counts.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nlp_path_has_data', 'nlp_path_n_entities', 'nlp_path_positive_mentioned', 'nlp_path_confidence_tier'
  );

-- -----------------------------------------------------------------------------
-- 152n — 5 cols — pathology legacy semantic flags + n_notes (Script 212 tier-1 JSON)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'extraction_faithfulness_vs_note_entities_llm_pathology_legacy212',
    batch_id                = 'mig_152_patient_master_nlp_cluster_20260429',
    verified_ts             = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                   = COALESCE(notes,'')
                              || ' | mig_152 NLP cluster. BOOL_OR / COUNT DISTINCT note_row_id replay vs '
                              || 'note_entities_llm_pathology positive entity slice. '
                              || 'Cross-footnote: canonical_path_malignant_events_v1 (mig_89) SSOT — '
                              || 'NLP path flags are adjunct mentions (CF-mig152-NLP-PATH-VS-PATH-CANONICAL).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nlp_path_ln_positive_mentioned', 'nlp_path_margin_mentioned', 'nlp_path_multifocal_mentioned',
    'nlp_path_vasc_inv_mentioned', 'nlp_path_n_notes'
  );

-- -----------------------------------------------------------------------------
-- 152o — 1 col — multifocal concordance overlay (Script 236 Phase 1C)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'derivation_multifocal_concordance_script236_vs_structured_path',
    batch_id                = 'mig_152_patient_master_nlp_cluster_20260429',
    verified_ts             = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                   = COALESCE(notes,'')
                              || ' | mig_152 NLP cluster. VARCHAR concordance label vs multifocal_flag_path '
                              || '(Script 236) — not Tier-1 JSON replay.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name = 'nlp_path_multifocal_concordance_v2';

-- -----------------------------------------------------------------------------
-- 152p — 8 cols — recurrence NLP (Script 212 tier-2 + surgical anchor extension)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'extraction_faithfulness_vs_note_entities_llm_recurrence',
    batch_id                = 'mig_152_patient_master_nlp_cluster_20260429',
    verified_ts             = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                   = COALESCE(notes,'')
                              || ' | mig_152 NLP cluster. JSON replay + calendar anchor checks vs '
                              || 'note_entities_llm_recurrence; structured SSOT canonical_recurrence_v1 '
                              || '(mig_123 rebuild). CF-mig152-NLP-REC-VS-STRUCTURAL-SPARSE: 4.35% struct+ / '
                              || 'NLP− (within 5% gate).'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nlp_rec_has_data', 'nlp_rec_n_entities', 'nlp_rec_any_mentioned', 'nlp_rec_type_worst',
    'nlp_rec_earliest_date', 'nlp_rec_earliest_days_from_surg', 'nlp_rec_disease_free_mentioned',
    'nlp_rec_confidence_tier'
  );

-- -----------------------------------------------------------------------------
-- 152q — 5 cols — TIRADS granular LLM
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'extraction_faithfulness_vs_note_entities_llm_tirads_granular',
    batch_id                = 'mig_152_patient_master_nlp_cluster_20260429',
    verified_ts             = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                   = COALESCE(notes,'')
                              || ' | mig_152 NLP cluster. JSON replay vs note_entities_llm_tirads_granular. '
                              || 'CF-mig152-NLP-TIRADS-VS-IMAGING-PENDING: await imaging lane for structured TR.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nlp_tirads_has_data', 'nlp_tirads_n_entities', 'nlp_tirads_n_notes',
    'nlp_tirads_max_category', 'nlp_tirads_has_component_detail'
  );

-- -----------------------------------------------------------------------------
-- 152r — 4 cols — vascular invasion LLM (v2 table; Script 368 lineage)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status   = 'verified',
    verified_by           = 'logan',
    verification_method   = 'extraction_faithfulness_vs_note_entities_llm_vascular_invasion_v2',
    batch_id                = 'mig_152_patient_master_nlp_cluster_20260429',
    verified_ts             = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes                   = COALESCE(notes,'')
                              || ' | mig_152 NLP cluster. JSON replay vs note_entities_llm_vascular_invasion_v2; '
                              || 'below-80pct concordance tier noted in Script 212 — adjunct to structured LVI.'
WHERE schema_name = 'main'
  AND table_name  = 'canonical_patient_master'
  AND verification_status = 'not_started'
  AND column_name IN (
    'nlp_vasc_has_data', 'nlp_vasc_n_entities', 'nlp_vasc_positive_mentioned', 'nlp_vasc_confidence_tier'
  );

-- -----------------------------------------------------------------------------
-- 152s — refresh canonical_table_signoff_registry_v1 for CPM (+116 n_verified / −116 not_started)
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
    signoff_migration = 'qc_framework_v1/migrations/152_patient_master_nlp_cluster_signoff_20260429.sql',
    notes             = COALESCE(ts.notes,'')
                        || ' | mig_152: NLP cluster — 116 cols closed (46 na pending upstream mirrors; '
                        || '70 verified with per-domain Tier-1 methods).'
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

-- Gate 4 spot-check (expect **0** rows) — run manually after apply:
--   SELECT COUNT(*) AS gate4_violations
--   FROM main.canonical_column_verification_registry_v1
--   WHERE schema_name='main' AND table_name='canonical_patient_master'
--     AND verification_status = 'verified'
--     AND (verified_by IS NULL OR verification_method IS NULL OR batch_id IS NULL OR verified_ts IS NULL);
--   SELECT COUNT(*) AS gate4_na_violations
--   FROM main.canonical_column_verification_registry_v1
--   WHERE schema_name='main' AND table_name='canonical_patient_master'
--     AND verification_status = 'na'
--     AND (verified_by IS NULL OR verification_method IS NULL OR batch_id IS NULL OR verified_ts IS NULL);

-- =============================================================================
-- end migration 152 — CPM NLP cluster (116 cols not_started → 70 verified + 46 na)
-- =============================================================================
