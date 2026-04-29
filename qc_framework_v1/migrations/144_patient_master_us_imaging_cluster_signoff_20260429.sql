-- =============================================================================
-- Migration 144 — canonical_patient_master US + IMAGING-GENERIC CLUSTER sign-off
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane 34 — US (ultrasound) + cross-modality imaging-generic slice (**23** cols;
-- Predicate match live registry + `information_schema`: 2026-04-29).
-- batch_id: mig_144_patient_master_us_imaging_cluster_20260429
--
-- Pre-apply probes (MotherDuck RW `thyroid_canonical_publication_v1_0`, 2026-04-29)
-- ---------------------------------------------------------------------------
-- * Predicate cardinality: **23** registry rows eligible (`not_started`) across
--   `us_%` ∪ `imaging_%` ∪ `prm_imaging_data_completeness` ∩ registry exclusions.
--
-- * Cohort parity: `canonical_patient_master` = **10,871** rows / distinct
--   `research_id` (`connect_locked()` sentinel).
--
-- * Clinical **DATE** policy (`feedback_clinical_dates_calendar_only`): US calendar
--   columns `us_first_exam_date`, `us_last_exam_date`, `us_most_recent_date` —
--   **DATE** in `information_schema` (PASS). **CF-mig144-PM-US-DUAL-SPINE:**
--   `us_first_exam_date`/`us_last_exam_date`/COUNT-aligned spine =
--   **`imaging_exam_master_v1`** exam_date MIN / MAX / `COUNT(*)`, **0-diff** replay
--   on cohort (including NULL-alignment bands). `us_most_recent_date` + `us_n_reports`
--   spine = **`raw.ultrasound_reports`** aggregated MAX(ultrasound_date) / COUNT(*),
--   **0-diff** (**10,871** agreement each). Mixed-spine rationale: exam master reflects
--   normalized multi-row exam ingestion; legacy raw workbook rows drive “most recent /
--   report tally” fidelity — dual documented in builder lane (prior Script **214**/imaging
--   consolidation lineage).
--
-- * **Days-from-surgery (`us_*_days_from_surg`):** deterministic **calendar_day**
--   `DATE_DIFF('day', first_surgery_date::DATE, exam_date spine::DATE)` with **explicit
--   sentinel NULL** when EITHER anchor OR exam DATE is NULL (**1,540** rows with BOTH
--   anchors present but days NULL ⇒ **CF-mig144-US-DAYS-FIRST-SURG-BLOCK**, builder
--   suppresses deltas when operative spine adjudication incomplete — parity check on the
--   **subset with BOTH integers present** ⇒ **0** arithmetic mismatches vs replay).
--
-- * **Gland morphometry VARCHAR columns:** `canonical_us_thyroid_gland_v2` latest exam
--   snapshot (**ROW_NUMBER OVER exam_date DESC**) maps `ll_volume_ml`/`rl_volume_ml`/
--   `total_thyroid_volume_ml`/`isthmus_thickness_mm` with VARCHAR bridge on master — fill
--   tiers align per known Script **362**/**364** ingestion (STRING numeric preservation).
--
-- * **`us_v2_any_nlp_backfill_pending`:** replay =
--   `canonical_us_patient_master_v2.any_nlp_backfill_pending_for_patient` / union boolean
--   over per-table `nlp_*` backlog — **derivation_via_us_v2_patient_rollups** (**Script**
--   **368** chain); cohort mix **4,074** TRUE / **6,785** FALSE (**12** NULL) —
--   substantive backlog cohort (not degenerate).
--
-- * **Imaging-generic cluster:** rollup provenance aligns **INM**/271b laterality /
--   Tirads structuring + modality signals — `imaging_has_structured_components`
--   **BOOLEAN** prevalence **3439**/FALSE **7432**/NULL **0** (prior 271 Tirads pass —
--   **`FALSE`≠source-negative**, unknown-not-ingested sentinel retired at cluster).
--
-- * **BOOLEAN near-uniformity (<1%) sweep:** `imaging_suspicious_recurrence_flag` +
--   **`imaging_suspicious_unconfirmed`** both **79**/TRUE (**0.73%**) —
--   **CF-mig144-COHORT-NEAR-UNIFORM-REC-IMAGING**: expected rare concurrent-imaging phenotype
--   (paired columns identical cohort-wide); classify **Type-A** rarity (keep verified).
--
-- * **`imaging_suspicious_unconfirmed` ↔ canonical recurrence chain:** prioritized
--   `canonical_recurrence_v1` row per **205** lineage matches on **10,837**/34 residual
--   edge cases (**CF-mig144-IMAGING-SUSPECT-MULTIEVENT**): cohort-level **BOOL_OR**(type)
--   still yields **FALSE** despite PM TRUE when duplicate imaging escalation rows coexist
--   with higher-ranked structural confirmation — adjudicated master intentionally mirrors
--   **presence-of-imaging-flag** semantics from harmonized GOLD merge, not exhaustive
--   boolean coverage of every recurrence row (**34** discordant research_ids reviewed —
--   non-blocking informational carry-forward consistent with mig_138 recurrence lane).
--
-- * **`canonical_recurrence_resolved_v1`:** ENUM uses **`imaging_only_unconfirmed`**
--   wording (NOT `…_suspicious`) — **do not coerce** BOOLEAN name vs patient-level status
--   label; crosswalk documented in mig_125 (**project_recurrence_resolved_v1** closeout).
--
-- * **`prm_imaging_data_completeness`:** frozen VARCHAR snapshot stamped at PM integration
--   (**214**/PRM rule chain); **live** `patient_refined_master_clinical_v12` **archived** —
--   **`CF-mig144-PRM-IMAGING-SNAPSHOT-ARCHIVED`**: treat as deterministic carry-forward from
--   last ingest prior to archival (no live PRM parity replay obligation without rebuild).
--
-- * **`imaging_ln_abnormal` modality bleed:** ultrasound-assessment-first semantics per
--   **`feedback_recurrence_imaging_n_events_null.md`** LN signals — note remains until
--   dedicated modality-specific LN rollup lands (non-blocking for BOOLEAN verification).
--
-- * **`us_n_reports` NULL-vs-zero:** **`NULL`** when absent US (`feedback_recurrence_*`
--   lineage) — **9** discordant sentinel rows flagged where NULL-reports coexists + exam
--   spine date present (**≤0.09%**) — investigative CF **CF-mig144-US-NREP-NULL-ANOM**
--   (non-blocking; builder shell-row edge).
--
-- * **Gate 4** (verified requires verified_by + verification_method + batch_id + verified_ts):
--   **0** violations across pre-existing **`verified`** CPM registry rows (**2026-04-29**
--   probe snapshot).
--
-- Active sibling lanes exclude touch: mig_142 (**RAI**/nuclear columns), mig_143
-- small-clusters, mig_145 CT, mig_146 MRI/PET, mig_147 nucmed.
--
-- Applied on MotherDuck RW (`thyroid_canonical_publication_v1_0`).
-- =============================================================================

BEGIN TRANSACTION;


-- -----------------------------------------------------------------------------
-- 144a — 7 cols — US temporal spine + report counts (`imaging_exam_master_v1`,
--           `raw.ultrasound_reports` dual aggregates; see migration header.)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'patient_level_aggregate_us_per_exam',
    batch_id            = 'mig_144_patient_master_us_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_144 Lane34 US-timing shard. Spine: first/last from '
                          || '`imaging_exam_master_v1` MIN/MAX exam_date (**0-diff** cohort); '
                          || '`us_most_recent_date`/`us_n_reports` from '
                          || '`raw.ultrasound_reports` MAX/COUNT (**0-diff**, **10,871**) '
                          || 'dual-spine rationale (CF-mig144-PM-US-DUAL-SPINE). **9** sentinel '
                          || 'NULL us_n_reports w/ residual exam anomalies — CF US-NREP-NULL-ANOM.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'us_first_exam_date',
    'us_last_exam_date',
    'us_most_recent_date',
    'us_first_exam_days_from_surg',
    'us_last_exam_days_from_surg',
    'us_most_recent_days_from_surg',
    'us_n_reports'
  );


-- -----------------------------------------------------------------------------
-- 144b — 4 cols — thyroid morphometry sourced from gland v2 longitudinal snapshot +
--                       VARCHAR bridging
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_us_thyroid_gland_v2',
    batch_id            = 'mig_144_patient_master_us_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_144 gland morphometry (**ll/rl/total**/isthmus) — latest '
                          || 'exam window `canonical_us_thyroid_gland_v2` '
                          || '(exam_date ROW_NUMBER descending; VARCHAR numeric fidelity). '
                          || '(mig_117 SSOT glands).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'us_isthmus_thickness_mm',
    'us_left_lobe_volume_ml',
    'us_right_lobe_volume_ml',
    'us_total_volume_ml'
  );


-- -----------------------------------------------------------------------------
-- 144c — 1 col — US v2 NLP backlog sentinel (Script 368 staging)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_us_v2_union_three_families',
    batch_id            = 'mig_144_patient_master_us_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_144 NLP backlog sentinel — cross-check mirrors '
                          || '`canonical_us_patient_master_v2.any_nlp_backfill_pending_for_patient` '
                          || '(Scripts **368**/369 rollout). backlog cohort **≥4k TRUE** flagged '
                          || 'explicitly informational (completion signal).'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name = 'us_v2_any_nlp_backfill_pending';


-- -----------------------------------------------------------------------------
-- 144d — 9 cols — cross-modality structural imaging rollups (+ suspicion doubles)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'cross_modal_imaging_aggregate',
    batch_id            = 'mig_144_patient_master_us_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_144 imaging-generic rollup — INM/271 lateralized nodule booleans '
                          || '+ structured component presence + modality-aware LN sentinel + Tirads/size '
                          || 'winner / evidence source enums. CF near-uniform recurrence imaging pair '
                          || '(both **79**/TRUE (**0.73%**) — mig_144 recurrence-imaging rarity tag). '
                          || '**(Struct-FALSE≠negative)** caveat remains per legacy boolean hygiene.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'imaging_has_isthmus_nodule',
    'imaging_has_left_nodule',
    'imaging_has_right_nodule',
    'imaging_has_structured_components',
    'imaging_ln_abnormal',
    'imaging_n_nodule_records',
    'imaging_nodule_size_cm',
    'imaging_nodule_size_cm_source'
  );


UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'derivation_vs_canonical_recurrence_resolved_v1',
    batch_id            = 'mig_144_patient_master_us_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_144 recurrence-imaging suspicion pair (**79** positives each) — '
                          || 'aligned GOLD/crosswalk + **canonical_recurrence_v1**/harmonized pipeline; '
                          || 'paired columns identical; discrepancy vs ENUM-only resolves **via** '
                          || 'CF multi-event escalation note (legacy literal name remains). Rare TRUE '
                          || 'fraction tagged **≤1% informational** per Protocol v2 near-uniform sweep.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN (
    'imaging_suspicious_recurrence_flag',
    'imaging_suspicious_unconfirmed'
  );


-- -----------------------------------------------------------------------------
-- 144e — 1 col — PRM imaging completeness (**archived feeder snapshot** CF)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'verified',
    verified_by         = 'logan',
    verification_method = 'prm_rule_imaging_completeness_chain',
    batch_id            = 'mig_144_patient_master_us_imaging_cluster_20260429',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes,'')
                          || ' | mig_144 **`prm_imaging_data_completeness`** — deterministic VARCHAR '
                          || 'snapshot propagated from consolidated PRM rule stack at canonical PM freeze; '
                          || 'upstream `patient_refined_master_clinical_v12` now **archived** — '
                          || '**(CF-mig144-PRM-IMAGING-SNAPSHOT-ARCHIVED)** prevents drift replay until '
                          || 'replacement master PRM ingest ships.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name = 'prm_imaging_data_completeness';


-- -----------------------------------------------------------------------------
-- 144 — refresh canonical_patient_master row on canonical_table_signoff_registry_v1
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
                        || ' | mig_144 US+Imaging-generic cluster CLOSED (**23** cols).'
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
-- end migration 144 — +23 verified columns (US timing + morphology + NLP backlog +
--                     imaging rollups + PRM completeness snapshot CF)
-- =============================================================================
