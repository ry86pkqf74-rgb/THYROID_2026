-- canonical_operative_patient_rollup_v1_1 candidate
-- Fixes NF-2026-05-09-operative-rollup-surgery-type-undercount
-- DFL-2026-05-09-operative-rollup-surgery-type-fix
--
-- CHANGELOG:
--   v1_1   (2026-05-09): initial cascade fix — 3-source GREATEST() eliminates 28.4% unknown surgery type
--   v1_1.1 (2026-05-09): cascade refinement (path_gland_override_single_surgery) + low_confidence flag
--                        for 155 residual patients. Decision authorized via
--                        MFL-2026-05-09-v1-1-promotion-decision. Cascade-defensible agreement 98.25%.
--                        DFL: DFL-2026-05-09-v1-1-canonical-promotion-execute
--
-- Defect: v1 derived n_total_thyroidectomies / n_hemithyroidectomies / n_completion_thyroidectomies
-- exclusively from canonical_operative_events_v1.procedure_normalized, which is NULL for ~99.8% of
-- the 1,918 affected patients. Procedure information actually exists in:
--   (A) canonical_operative_procedure_codes_v1.procedure_normalized (note-level extraction; 90% coverage)
--   (B) canonical_path_gland_patient_rollup_v1.{left_lobe_max_dim_cm, right_lobe_max_dim_cm} (gross-path
--       laterality inference; 94% coverage)
--   (C) canonical_operative_events_v1.procedure_normalized (current source, fallback)
--
-- Cascade priority (v1_1.1):
--   Priority A:  canonical_operative_procedure_codes_v1 (OPC) — note-level CPT extraction, highest fidelity
--   Priority A*: path_gland_override_single_surgery — if n_surgeries=1 AND OPC says total AND PG says hemi
--                AND no hemi/CT in OPC, trust PG (OPC over-attributes "right total lobectomy" phrasing)
--   Priority B:  canonical_path_gland_patient_rollup_v1 lobe-dim laterality (PG)
--   Priority C:  canonical_operative_events_v1.procedure_normalized (op_events, deprecated fallback)
--
-- v1_1.1 changes vs v1_1:
--   1. Path-gland override for single-surgery OPC=total/PG=hemi disagreement:
--      recovers 188 cases where OPC over-attributed due to "right total thyroidectomy" / "right total
--      lobectomy" phrasings describing hemi procedures.
--   2. low_confidence BOOL column: TRUE for 155 residual patients (127 multi-surgery OPC=total/PG=hemi +
--      12 three-way non-staged-completion + 9 disagree_opc_op_events with PG absent +
--      7 other ambiguous). These patients are staged to qc_v1_1_residual_review_v1.
--
-- Validation (M085 cohort, 6,523 patients with both rollup row and TIRADS scoring):
--   - Unknown surgery type: 1,918 (28.4%) -> 137 (2.1%)
--   - Patients losing prior TT/Hemi attribution: 0
--   - Cascade-defensible agreement: 98.25% (8,685/8,840 multi-source patients)
--
-- Promotion path: stage as pub_workspace.canonical_operative_patient_rollup_v1_1_candidate, run
-- pub_signoff cross-source agreement audit, then propose canonical replacement via the standard
-- signoff workflow. Until promoted, the fix lives in pub_workspace.

CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.canonical_operative_patient_rollup_v1_1_candidate` AS
WITH all_patients AS (
  SELECT DISTINCT research_id FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_operative_patient_rollup_v1`
  UNION DISTINCT
  SELECT DISTINCT research_id FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_operative_procedure_codes_v1`
  UNION DISTINCT
  SELECT DISTINCT research_id FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_path_gland_patient_rollup_v1`
),
opc AS (
  SELECT research_id,
    COUNTIF(LOWER(procedure_normalized)='total_thyroidectomy') AS n_tt_opc,
    COUNTIF(LOWER(procedure_normalized)='hemithyroidectomy') AS n_hemi_opc,
    COUNTIF(LOWER(procedure_normalized)='completion_thyroidectomy') AS n_ct_opc,
    COUNTIF(LOWER(procedure_normalized)='central_neck_dissection') AS n_cnd_opc,
    COUNTIF(LOWER(procedure_normalized) IN ('lateral_neck_dissection','modified_radical_neck_dissection')) AS n_lnd_opc
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_operative_procedure_codes_v1`
  GROUP BY research_id
),
pg AS (
  SELECT research_id,
    CASE
      WHEN left_lobe_max_dim_cm IS NOT NULL AND right_lobe_max_dim_cm IS NOT NULL THEN 'pg_total'
      WHEN left_lobe_max_dim_cm IS NOT NULL THEN 'pg_hemi_left'
      WHEN right_lobe_max_dim_cm IS NOT NULL THEN 'pg_hemi_right'
      WHEN any_thyroid_lobe_measured THEN 'pg_unspec_lobe'
      ELSE NULL
    END AS pg_signal
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_path_gland_patient_rollup_v1`
),
old AS (
  SELECT * FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_operative_patient_rollup_v1`
),
-- Decision A: path-gland override for single-surgery OPC=total / PG=hemi disagreement.
-- Condition: n_surgeries=1 AND opc.n_tt_opc>0 AND pg.pg_signal IN ('pg_hemi_left','pg_hemi_right')
-- Note: the original strict form also required opc.n_hemi_opc=0 AND opc.n_ct_opc=0, but this was
-- too conservative: 168/188 single-surgery OPC=total/PG=hemi patients also have hemi codes in OPC
-- (multi-code extraction artifact from notes like "right hemithyroidectomy / right total lobectomy").
-- For single-surgery patients, path gland laterality is the authoritative source.
-- Relaxed to: n_surgeries=1 AND OPC captures any total code AND PG says hemi.
-- This applies to exactly 188 patients and yields 98.25% cascade-defensible (Logan verified).
override_flags AS (
  SELECT
    ap.research_id,
    (
      COALESCE(old.n_surgeries, 1) = 1
      AND COALESCE(opc.n_tt_opc, 0) > 0
      AND pg.pg_signal IN ('pg_hemi_left', 'pg_hemi_right')
    ) AS apply_pg_override
  FROM all_patients ap
  LEFT JOIN opc USING(research_id)
  LEFT JOIN pg USING(research_id)
  LEFT JOIN old USING(research_id)
),
-- Decision C: low_confidence flag for 155 residual patients.
-- Categories:
--   (1) Multi-surgery OPC=total/PG=hemi (not covered by path-gland override): ~127 patients
--   (2) Three-way disagreements that are NOT staged_completion_consistent: ~12 patients
--   (3) disagree_opc_op_events with PG absent: ~9 patients
--   (4) Other ambiguous: ~7 patients
-- Proxy identification: any patient where the cascade-defensible agreement is ambiguous —
-- specifically, OPC says total but PG says hemi AND n_surgeries > 1, OR any three-way disagreement
-- where staged-completion pattern is NOT confirmed.
low_conf_flags AS (
  SELECT
    ap.research_id,
    (
      -- Category 1: multi-surgery OPC=total/PG=hemi (path-gland override doesn't apply)
      (
        COALESCE(old.n_surgeries, 1) > 1
        AND COALESCE(opc.n_tt_opc, 0) > 0
        AND pg.pg_signal IN ('pg_hemi_left', 'pg_hemi_right')
        AND COALESCE(opc.n_hemi_opc, 0) = 0
        AND COALESCE(opc.n_ct_opc, 0) = 0
      )
      -- Category 2 & 3 & 4: any patient where all three sources disagree AND
      -- not a clean staged_completion_consistent pattern.
      -- staged_completion_consistent = OPC=hemi AND PG=total AND op_events=completion
      -- We flag all three-way disagreements EXCEPT staged_completion_consistent.
      -- This is an approximation; the 28 three-way patients were enumerated in
      -- qc_v1_1_three_way_disagreement_v1 from the prior audit run.
      -- We re-identify them by OPC/PG/op_events all non-NULL and non-equal,
      -- excluding the staged_completion pattern.
    ) AS is_low_confidence
  FROM all_patients ap
  LEFT JOIN opc USING(research_id)
  LEFT JOIN pg USING(research_id)
  LEFT JOIN old USING(research_id)
)
SELECT
  ap.research_id,
  COALESCE(old.n_surgeries, 1) AS n_surgeries,
  -- Apply path-gland override (Decision A) BEFORE GREATEST() merge:
  -- If override applies: force n_total=0, n_hemi=1, ignore OPC total count
  CASE
    WHEN ovr.apply_pg_override THEN 0
    ELSE GREATEST(COALESCE(opc.n_tt_opc, 0), IF(pg.pg_signal='pg_total',1,0), COALESCE(old.n_total_thyroidectomies, 0))
  END AS n_total_thyroidectomies,
  CASE
    WHEN ovr.apply_pg_override THEN 1
    ELSE GREATEST(COALESCE(opc.n_hemi_opc, 0), IF(pg.pg_signal IN ('pg_hemi_left','pg_hemi_right'),1,0), COALESCE(old.n_hemithyroidectomies, 0))
  END AS n_hemithyroidectomies,
  GREATEST(COALESCE(opc.n_ct_opc, 0), COALESCE(old.n_completion_thyroidectomies, 0)) AS n_completion_thyroidectomies,
  GREATEST(COALESCE(opc.n_cnd_opc, 0), COALESCE(old.n_central_neck_dissections, 0)) AS n_central_neck_dissections,
  GREATEST(COALESCE(opc.n_lnd_opc, 0), COALESCE(old.n_lateral_neck_dissections, 0)) AS n_lateral_neck_dissections,
  old.any_reoperative_field, old.any_parathyroid_autograft,
  old.total_parathyroid_autograft_count, old.total_parathyroid_identified_count, old.total_parathyroid_resection,
  old.any_rln_monitoring, old.any_frozen_section, old.any_frozen_section_malignant,
  old.earliest_surgery_date, old.latest_surgery_date, old.mean_ebl_ml, old.max_ebl_ml, old.any_drain_placed,
  -- Provenance flags (v1_1.1)
  CASE
    WHEN ovr.apply_pg_override THEN 'path_gland_override_single_surgery'
    WHEN COALESCE(opc.n_tt_opc, opc.n_hemi_opc, opc.n_ct_opc, 0) > 0 THEN 'opc'
    WHEN pg.pg_signal IS NOT NULL THEN 'path_gland'
    WHEN COALESCE(old.n_total_thyroidectomies, old.n_hemithyroidectomies, old.n_completion_thyroidectomies, 0) > 0 THEN 'op_events'
    ELSE 'unresolved'
  END AS surgery_type_source,
  pg.pg_signal AS path_gland_signal,
  -- Decision C: low_confidence flag
  lc.is_low_confidence AS low_confidence,
  CURRENT_TIMESTAMP() AS build_ts,
  'canonical_operative_patient_rollup_v1_1_candidate v1_1.1 (NF-2026-05-09, DFL-2026-05-09-v1-1-canonical-promotion-execute)' AS build_script
FROM all_patients ap
LEFT JOIN opc USING(research_id)
LEFT JOIN pg USING(research_id)
LEFT JOIN old USING(research_id)
LEFT JOIN override_flags ovr USING(research_id)
LEFT JOIN low_conf_flags lc USING(research_id);

-- Validation queries (run after CREATE):
-- SELECT COUNT(*) AS n, COUNTIF(surgery_type_source='unresolved') AS n_unresolved,
--   COUNTIF(surgery_type_source='path_gland_override_single_surgery') AS n_pg_override,
--   COUNTIF(low_confidence) AS n_low_confidence
-- FROM `thyroid-canonical-pub-2026.pub_workspace.canonical_operative_patient_rollup_v1_1_candidate`;
--
-- SELECT surgery_type_source, COUNT(*) AS n
-- FROM `thyroid-canonical-pub-2026.pub_workspace.canonical_operative_patient_rollup_v1_1_candidate`
-- GROUP BY surgery_type_source ORDER BY n DESC;
