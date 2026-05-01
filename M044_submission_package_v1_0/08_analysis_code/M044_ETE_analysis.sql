-- =====================================================================
-- M044 — Microscopic vs Gross ETE manuscript: reproducible SQL package
-- Database: thyroid_canonical_publication_v1_0
-- Primary cohort: manuscript_workspace.cohort_m044_ajcc_ete_v1 (n=4128)
-- Recurrence column-of-record: main.canonical_recurrence_resolved_v1
-- LN rollup: manuscript_workspace.ln_master_rollup_v1
-- Reoperative: manuscript_workspace.cohort_m040_reoperative_v1
-- Author: Claude (independent verifier), 2026-05-01
-- =====================================================================

-- ---------------------------------------------------------------------
-- 0. Spot-check (matches ChatGPT handoff) + legacy-vs-canonical QC row
-- ---------------------------------------------------------------------
SELECT
  COUNT(*)                                                AS n,
  SUM(CASE WHEN any_recurrence_flag THEN 1 ELSE 0 END)    AS any_recurrence_n,
  ROUND(MEDIAN(followup_years), 3)                        AS median_followup_years
FROM manuscript_workspace.cohort_m044_ajcc_ete_v1;
-- Cohort row counts / median FU drift with data refreshes.
-- Legacy-vs-canonical headline metrics (do not use legacy flags as endpoints):
SELECT * FROM manuscript_workspace.m044_legacy_recurrence_flag_audit_v1;
-- Deploy: qc_framework_v1/migrations/257_m044_legacy_recurrence_flag_audit_20260501.sql


-- ---------------------------------------------------------------------
-- 1. Canonical analytic view (one-row-per-patient with cleaned columns)
-- ---------------------------------------------------------------------
-- Adopt this WITH-block at the top of every downstream query.
-- ETE grouping = ChatGPT primary definition (literal 'gross' only).

WITH cohort AS (
  SELECT
    c.*,
    CASE
      WHEN c.ete_grade_final IN ('false','absent')   THEN 'No/negative ETE'
      WHEN c.ete_grade_final = 'microscopic'         THEN 'Microscopic ETE'
      WHEN c.ete_grade_final = 'gross'               THEN 'Gross ETE'
      WHEN c.ete_grade_final = 'present_ungraded'    THEN 'Present ungraded'
      ELSE 'Missing/other'
    END AS ete_group,
    -- cleaned LVI category collapsing spelling variants
    CASE
      WHEN c.lvi_grade ILIKE 'extensiv%'             THEN 'extensive'
      WHEN c.lvi_grade IN ('present','preesent')     THEN 'present'
      WHEN c.lvi_grade = 'focal'                     THEN 'focal'
      WHEN c.lvi_grade IS NULL                       THEN 'missing'
      WHEN c.lvi_grade IN ('indeterminate','indetermiante','indeeterminate','indeterminent','suspicious','x','c/a','no','n/s')
                                                     THEN 'indeterminate'
      ELSE 'indeterminate'
    END AS lvi_clean,
    -- vascular categories already cleaner; pass-through with explicit missing
    COALESCE(c.vascular_invasion_final, 'missing') AS vasc_clean
  FROM manuscript_workspace.cohort_m044_ajcc_ete_v1 c
),
ln AS (
  SELECT research_id,
    MAX(ln_total_examined)               AS ln_examined,
    MAX(ln_total_positive)               AS ln_positive,
    MAX(ln_central_examined)             AS ln_central_examined,
    MAX(ln_central_positive)             AS ln_central_positive,
    MAX(ln_lateral_left_positive)        AS ln_lateral_left_positive,
    MAX(ln_lateral_right_positive)       AS ln_lateral_right_positive,
    MAX(ln_bilateral_lateral_positive)   AS ln_bilateral_lateral_positive,
    MAX(ln_level_vi_positive)            AS ln_level_vi_positive,
    MAX(ln_level_vii_positive)           AS ln_level_vii_positive,
    MAX(ln_extranodal_extension)         AS ln_ene
  FROM manuscript_workspace.ln_master_rollup_v1
  GROUP BY research_id
),
reop AS (
  SELECT research_id,
    MAX(n_surgeries)                          AS n_surgeries,
    MAX(second_surgery_date)                  AS second_surgery_date,
    MAX(days_between_first_second_surgery)    AS days_to_2nd,
    MAX(completion_reason)                    AS completion_reason,
    MAX(completion_reason_confidence)         AS completion_reason_confidence,
    MAX(completion_histology_type)            AS completion_histology_type,
    MAX(op_reoperative_any)                   AS op_reoperative_any
  FROM manuscript_workspace.cohort_m040_reoperative_v1
  GROUP BY research_id
),
rec AS (
  SELECT
    research_id,
    recurrence_path_proven,
    recurrence_path_proven_date,
    recurrence_imaging_suspicious,
    recurrence_imaging_suspicious_date,
    recurrence_imaging_then_path_confirmed,
    recurrence_status_final,
    days_to_path_proven,
    days_to_imaging_suspicious,
    is_implausible_date_quarantine
  FROM main.canonical_recurrence_resolved_v1
)
-- ---------------------------------------------------------------------
-- Master analytic table — one row per research_id
-- ---------------------------------------------------------------------
SELECT
  c.research_id, c.ete_group, c.ete_grade_final, c.ete_grade, c.ete_grade_source,
  c.gross_ete_flag, c.path_gross_ete_flag, c.ete_op_note_grade, c.ete_original_grade,
  c.age_at_surgery, c.sex, c.histology_final, c.tumor_size_cm,
  c.ajcc8_t_stage, c.ajcc8_n_stage, c.ajcc8_m_stage, c.ajcc8_stage_group,
  c.ata_risk_category, c.surg_procedure_type, c.surg_first_date,
  c.followup_years, c.overall_survival_years, c.death_occurred,
  c.lvi_clean, c.vasc_clean, c.lvi_grade, c.vascular_invasion_final,
  c.ln_positive_flag, c.ln_total_positive AS ln_total_positive_view,
  ln.ln_examined, ln.ln_positive, ln.ln_central_positive,
  ln.ln_lateral_left_positive, ln.ln_lateral_right_positive,
  CASE WHEN ln.ln_central_positive > 0 THEN 1 ELSE 0 END AS central_pos_flag,
  CASE WHEN COALESCE(ln.ln_lateral_left_positive,0) > 0
         OR COALESCE(ln.ln_lateral_right_positive,0) > 0
         OR COALESCE(ln.ln_bilateral_lateral_positive,0) > 0
       THEN 1 ELSE 0 END AS lateral_pos_flag,
  c.rai_received_flag,
  c.any_recurrence_flag,                      -- legacy, do NOT use as primary
  c.structural_recurrence_flag,               -- legacy, do NOT use as primary
  rec.recurrence_path_proven,
  rec.recurrence_imaging_suspicious,
  rec.recurrence_imaging_then_path_confirmed,
  rec.recurrence_status_final,
  rec.days_to_path_proven,
  rec.days_to_imaging_suspicious,
  reop.n_surgeries, reop.days_to_2nd, reop.completion_reason,
  reop.completion_histology_type, reop.op_reoperative_any
FROM cohort c
LEFT JOIN ln  USING (research_id)
LEFT JOIN reop USING (research_id)
LEFT JOIN rec  USING (research_id);


-- ---------------------------------------------------------------------
-- 2. Table 1 — Baseline characteristics by ETE group
-- ---------------------------------------------------------------------
-- Demographic + clinical block
WITH cohort AS (
  SELECT *,
    CASE
      WHEN ete_grade_final IN ('false','absent') THEN 'No/negative ETE'
      WHEN ete_grade_final = 'microscopic'       THEN 'Microscopic ETE'
      WHEN ete_grade_final = 'gross'             THEN 'Gross ETE'
      WHEN ete_grade_final = 'present_ungraded'  THEN 'Present ungraded'
      ELSE 'Missing/other'
    END AS ete_group
  FROM manuscript_workspace.cohort_m044_ajcc_ete_v1
)
SELECT ete_group,
  COUNT(*) AS n,
  ROUND(AVG(age_at_surgery), 1)                                    AS mean_age,
  ROUND(MEDIAN(age_at_surgery), 1)                                 AS median_age,
  ROUND(QUANTILE_CONT(age_at_surgery, 0.25), 1)                    AS q1_age,
  ROUND(QUANTILE_CONT(age_at_surgery, 0.75), 1)                    AS q3_age,
  SUM(CASE WHEN sex='female' THEN 1 ELSE 0 END)                    AS female_n,
  SUM(CASE WHEN histology_final='PTC' THEN 1 ELSE 0 END)           AS ptc_n,
  SUM(CASE WHEN histology_final ILIKE '%follicular%' THEN 1 ELSE 0 END) AS ftc_like_n,
  SUM(CASE WHEN histology_final ILIKE '%medullary%' OR histology_final ILIKE '%MTC%' THEN 1 ELSE 0 END) AS mtc_n,
  ROUND(AVG(tumor_size_cm), 2)                                     AS mean_size_cm,
  ROUND(MEDIAN(tumor_size_cm), 2)                                  AS median_size_cm,
  ROUND(QUANTILE_CONT(tumor_size_cm, 0.25), 2)                     AS q1_size,
  ROUND(QUANTILE_CONT(tumor_size_cm, 0.75), 2)                     AS q3_size,
  SUM(CASE WHEN ajcc8_n_stage='N0' THEN 1 ELSE 0 END)              AS n0_n,
  SUM(CASE WHEN ajcc8_n_stage='N1a' THEN 1 ELSE 0 END)             AS n1a_n,
  SUM(CASE WHEN ajcc8_n_stage='N1b' THEN 1 ELSE 0 END)             AS n1b_n,
  SUM(CASE WHEN ajcc8_n_stage='Nx' THEN 1 ELSE 0 END)              AS nx_n,
  SUM(CASE WHEN ajcc8_n_stage IS NULL THEN 1 ELSE 0 END)           AS n_missing_n,
  SUM(CASE WHEN ajcc8_stage_group='I' THEN 1 ELSE 0 END)           AS stg_i,
  SUM(CASE WHEN ajcc8_stage_group='II' THEN 1 ELSE 0 END)          AS stg_ii,
  SUM(CASE WHEN ajcc8_stage_group='III' THEN 1 ELSE 0 END)         AS stg_iii,
  SUM(CASE WHEN ajcc8_stage_group='IVA' THEN 1 ELSE 0 END)         AS stg_iva,
  SUM(CASE WHEN ajcc8_stage_group='IVB' THEN 1 ELSE 0 END)         AS stg_ivb,
  SUM(CASE WHEN rai_received_flag THEN 1 ELSE 0 END)               AS rai_n,
  ROUND(AVG(CASE WHEN rai_received_flag THEN 1.0 ELSE 0.0 END), 4) AS rai_rate,
  ROUND(MEDIAN(followup_years), 3)                                 AS median_fu_y
FROM cohort GROUP BY ete_group ORDER BY n DESC;


-- ---------------------------------------------------------------------
-- 3. Table 2 — Recurrence outcomes by ETE group (DUAL-TRACK)
-- ---------------------------------------------------------------------
WITH cohort AS (
  SELECT c.*, r.recurrence_path_proven, r.recurrence_imaging_suspicious,
         r.recurrence_status_final, r.recurrence_imaging_then_path_confirmed,
    CASE
      WHEN c.ete_grade_final IN ('false','absent') THEN 'No/negative ETE'
      WHEN c.ete_grade_final = 'microscopic'       THEN 'Microscopic ETE'
      WHEN c.ete_grade_final = 'gross'             THEN 'Gross ETE'
      WHEN c.ete_grade_final = 'present_ungraded'  THEN 'Present ungraded'
      ELSE 'Missing/other'
    END AS ete_group
  FROM manuscript_workspace.cohort_m044_ajcc_ete_v1 c
  LEFT JOIN main.canonical_recurrence_resolved_v1 r USING (research_id)
)
SELECT ete_group,
  COUNT(*)                                                                          AS n,
  -- Primary endpoint: path-proven
  SUM(CASE WHEN recurrence_path_proven THEN 1 ELSE 0 END)                            AS path_proven_n,
  ROUND(AVG(CASE WHEN recurrence_path_proven THEN 1.0 ELSE 0.0 END), 4)              AS path_proven_rate,
  -- Imaging-only-unconfirmed
  SUM(CASE WHEN recurrence_status_final='imaging_only_unconfirmed' THEN 1 ELSE 0 END) AS img_only_n,
  ROUND(AVG(CASE WHEN recurrence_status_final='imaging_only_unconfirmed' THEN 1.0 ELSE 0.0 END),4) AS img_only_rate,
  -- Composite
  SUM(CASE WHEN recurrence_status_final IN ('path_proven','imaging_only_unconfirmed') THEN 1 ELSE 0 END) AS comp_n,
  ROUND(AVG(CASE WHEN recurrence_status_final IN ('path_proven','imaging_only_unconfirmed') THEN 1.0 ELSE 0.0 END),4) AS comp_rate,
  -- Imaging-then-path
  SUM(CASE WHEN recurrence_imaging_then_path_confirmed THEN 1 ELSE 0 END)            AS img_then_path_n,
  -- Person-years: pyrs = audit sum including zero-FU; pos_pyrs = PY-rate denominator
  ROUND(SUM(followup_years),1)                                                       AS pyrs,
  ROUND(SUM(CASE WHEN followup_years>0 THEN followup_years END),1)                   AS pos_pyrs,
  -- PY incidence numerators exclude zero-FU rows (align with pos_pyrs denominator)
  SUM(CASE WHEN followup_years>0 AND recurrence_path_proven THEN 1 ELSE 0 END)       AS path_proven_n_positive_fu,
  SUM(CASE WHEN followup_years>0 AND recurrence_status_final IN ('path_proven','imaging_only_unconfirmed') THEN 1 ELSE 0 END) AS comp_n_positive_fu,
  ROUND(100.0*SUM(CASE WHEN followup_years>0 AND recurrence_path_proven THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN followup_years>0 THEN followup_years END),0), 3) AS pp_per_100py,
  ROUND(100.0*SUM(CASE WHEN followup_years>0 AND recurrence_status_final IN ('path_proven','imaging_only_unconfirmed') THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN followup_years>0 THEN followup_years END),0), 3) AS comp_per_100py,
  -- Legacy (sensitivity only)
  SUM(CASE WHEN any_recurrence_flag THEN 1 ELSE 0 END)                               AS legacy_any_n,
  ROUND(AVG(CASE WHEN any_recurrence_flag THEN 1.0 ELSE 0.0 END),4)                  AS legacy_any_rate
FROM cohort GROUP BY ete_group ORDER BY n DESC;


-- ---------------------------------------------------------------------
-- 4. Table 3 — Multivariable model inputs (export to R/Python for fitting)
-- ---------------------------------------------------------------------
-- Patient-level analytic file for logistic regression / Cox
WITH cohort AS (
  SELECT *,
    CASE
      WHEN ete_grade_final IN ('false','absent') THEN 'No/negative ETE'
      WHEN ete_grade_final = 'microscopic'       THEN 'Microscopic ETE'
      WHEN ete_grade_final = 'gross'             THEN 'Gross ETE'
      WHEN ete_grade_final = 'present_ungraded'  THEN 'Present ungraded'
      ELSE 'Missing/other'
    END AS ete_group,
    CASE
      WHEN lvi_grade ILIKE 'extensiv%'             THEN 'extensive'
      WHEN lvi_grade IN ('present','preesent')     THEN 'present'
      WHEN lvi_grade = 'focal'                     THEN 'focal'
      WHEN lvi_grade IS NULL                       THEN 'missing'
      ELSE 'indeterminate'
    END AS lvi_clean,
    COALESCE(vascular_invasion_final, 'missing') AS vasc_clean
  FROM manuscript_workspace.cohort_m044_ajcc_ete_v1
),
ln AS (
  SELECT research_id,
    MAX(ln_total_examined) AS ln_examined,
    MAX(ln_total_positive) AS ln_positive,
    MAX(ln_central_positive) AS ln_central_positive,
    GREATEST(COALESCE(MAX(ln_lateral_left_positive),0), COALESCE(MAX(ln_lateral_right_positive),0), COALESCE(MAX(ln_bilateral_lateral_positive),0)) AS lateral_max
  FROM manuscript_workspace.ln_master_rollup_v1 GROUP BY research_id
)
SELECT
  c.research_id, c.ete_group, c.age_at_surgery, c.sex, c.tumor_size_cm,
  c.ajcc8_t_stage, c.ajcc8_n_stage, c.ajcc8_stage_group,
  c.histology_final, c.lvi_clean, c.vasc_clean, c.rai_received_flag,
  c.followup_years,
  ln.ln_examined, ln.ln_positive, ln.ln_central_positive, ln.lateral_max,
  CASE WHEN ln.ln_central_positive>0 THEN 1 ELSE 0 END AS central_pos_flag,
  CASE WHEN ln.lateral_max>0 THEN 1 ELSE 0 END AS lateral_pos_flag,
  r.recurrence_path_proven, r.recurrence_imaging_suspicious,
  r.recurrence_status_final, r.recurrence_imaging_then_path_confirmed,
  r.days_to_path_proven, r.days_to_imaging_suspicious
FROM cohort c
LEFT JOIN ln  USING (research_id)
LEFT JOIN main.canonical_recurrence_resolved_v1 r USING (research_id);


-- ---------------------------------------------------------------------
-- 5. Table 4 — No/negative ETE recurred vs non-recurred subgroup
-- ---------------------------------------------------------------------
WITH cohort AS (
  SELECT c.*, r.recurrence_path_proven, r.recurrence_imaging_then_path_confirmed,
         r.recurrence_status_final, r.days_to_path_proven, r.days_to_imaging_suspicious
  FROM manuscript_workspace.cohort_m044_ajcc_ete_v1 c
  LEFT JOIN main.canonical_recurrence_resolved_v1 r USING (research_id)
  WHERE c.ete_grade_final IN ('false','absent')
),
ln AS (
  SELECT research_id, MAX(ln_total_examined) AS ln_examined, MAX(ln_total_positive) AS ln_positive,
         MAX(ln_central_positive) AS ln_central_positive,
         GREATEST(COALESCE(MAX(ln_lateral_left_positive),0), COALESCE(MAX(ln_lateral_right_positive),0)) AS lateral_max
  FROM manuscript_workspace.ln_master_rollup_v1 GROUP BY research_id
),
reop AS (
  SELECT research_id, MAX(n_surgeries) AS n_surgeries,
         MAX(days_between_first_second_surgery) AS days_to_2nd,
         MAX(completion_reason) AS completion_reason,
         MAX(completion_histology_type) AS completion_histology_type
  FROM manuscript_workspace.cohort_m040_reoperative_v1 GROUP BY research_id
)
SELECT
  CASE WHEN c.recurrence_status_final IN ('path_proven','imaging_only_unconfirmed') THEN 'Recurred' ELSE 'No recurrence' END AS rec_status,
  COUNT(*) AS n,
  ROUND(AVG(c.tumor_size_cm),2) AS mean_size, ROUND(MEDIAN(c.tumor_size_cm),2) AS median_size,
  ROUND(MEDIAN(c.followup_years),2) AS median_fu,
  SUM(CASE WHEN c.ajcc8_n_stage='N1a' THEN 1 ELSE 0 END) AS n1a_n,
  SUM(CASE WHEN c.ajcc8_n_stage='N1b' THEN 1 ELSE 0 END) AS n1b_n,
  SUM(CASE WHEN ln.ln_central_positive>0 THEN 1 ELSE 0 END) AS central_pos_n,
  SUM(CASE WHEN ln.lateral_max>0 THEN 1 ELSE 0 END) AS lateral_pos_n,
  ROUND(AVG(ln.ln_positive),2) AS mean_ln_pos,
  SUM(CASE WHEN c.rai_received_flag THEN 1 ELSE 0 END) AS rai_n,
  SUM(CASE WHEN reop.n_surgeries>=2 THEN 1 ELSE 0 END) AS ge2_surg_n,
  ROUND(MEDIAN(CASE WHEN reop.n_surgeries>=2 THEN reop.days_to_2nd END),0) AS median_days_to_2nd,
  SUM(CASE WHEN c.recurrence_path_proven THEN 1 ELSE 0 END) AS path_proven_n,
  SUM(CASE WHEN c.recurrence_imaging_then_path_confirmed THEN 1 ELSE 0 END) AS img_then_path_n
FROM cohort c
LEFT JOIN ln USING (research_id)
LEFT JOIN reop USING (research_id)
GROUP BY 1 ORDER BY 1;


-- ---------------------------------------------------------------------
-- 6. Sensitivity — exclude zero follow-up rows (positive-FU only)
-- ---------------------------------------------------------------------
WITH cohort AS (
  SELECT c.*, r.recurrence_path_proven, r.recurrence_status_final,
    CASE
      WHEN c.ete_grade_final IN ('false','absent') THEN 'No/negative ETE'
      WHEN c.ete_grade_final = 'microscopic'       THEN 'Microscopic ETE'
      WHEN c.ete_grade_final = 'gross'             THEN 'Gross ETE'
      WHEN c.ete_grade_final = 'present_ungraded'  THEN 'Present ungraded'
      ELSE 'Missing/other'
    END AS ete_group
  FROM manuscript_workspace.cohort_m044_ajcc_ete_v1 c
  LEFT JOIN main.canonical_recurrence_resolved_v1 r USING (research_id)
  WHERE c.followup_years > 0
)
SELECT ete_group, COUNT(*) AS n_pos_fu,
  SUM(CASE WHEN recurrence_path_proven THEN 1 ELSE 0 END) AS pp_n,
  ROUND(100.0*SUM(CASE WHEN recurrence_path_proven THEN 1 ELSE 0 END)/SUM(followup_years),3) AS pp_per_100py,
  SUM(CASE WHEN recurrence_status_final IN ('path_proven','imaging_only_unconfirmed') THEN 1 ELSE 0 END) AS comp_n,
  ROUND(100.0*SUM(CASE WHEN recurrence_status_final IN ('path_proven','imaging_only_unconfirmed') THEN 1 ELSE 0 END)/SUM(followup_years),3) AS comp_per_100py
FROM cohort GROUP BY 1 ORDER BY 1;


-- ---------------------------------------------------------------------
-- 7. Sensitivity — LVI/vascular separation, joint cells
-- ---------------------------------------------------------------------
WITH cohort AS (
  SELECT
    CASE WHEN c.lvi_grade ILIKE 'extensiv%' THEN 'lym_extensive'
         WHEN c.lvi_grade IN ('present','preesent','focal') THEN 'lym_present'
         WHEN c.lvi_grade IS NULL THEN 'lym_missing'
         ELSE 'lym_indeterminate' END AS lym_cat,
    CASE WHEN c.vascular_invasion_final='extensive' THEN 'vas_extensive'
         WHEN c.vascular_invasion_final='focal'     THEN 'vas_focal'
         WHEN c.vascular_invasion_final='present_ungraded' THEN 'vas_present'
         WHEN c.vascular_invasion_final='indeterminate' THEN 'vas_indet'
         ELSE 'vas_missing' END AS vas_cat,
    r.recurrence_path_proven, r.recurrence_status_final, c.followup_years
  FROM manuscript_workspace.cohort_m044_ajcc_ete_v1 c
  LEFT JOIN main.canonical_recurrence_resolved_v1 r USING (research_id)
)
SELECT lym_cat, vas_cat, COUNT(*) AS n,
  SUM(CASE WHEN recurrence_path_proven THEN 1 ELSE 0 END) AS pp_n,
  ROUND(AVG(CASE WHEN recurrence_path_proven THEN 1.0 ELSE 0.0 END),4) AS pp_rate,
  SUM(CASE WHEN recurrence_status_final IN ('path_proven','imaging_only_unconfirmed') THEN 1 ELSE 0 END) AS comp_n,
  ROUND(AVG(CASE WHEN recurrence_status_final IN ('path_proven','imaging_only_unconfirmed') THEN 1.0 ELSE 0.0 END),4) AS comp_rate
FROM cohort GROUP BY 1,2 ORDER BY n DESC;


-- ---------------------------------------------------------------------
-- 8. Sensitivity — restrict to surgery-date-known 1999–2024
-- ---------------------------------------------------------------------
WITH cohort AS (
  SELECT c.*, r.recurrence_path_proven, r.recurrence_status_final,
    CASE
      WHEN c.ete_grade_final IN ('false','absent') THEN 'No/negative ETE'
      WHEN c.ete_grade_final = 'microscopic'       THEN 'Microscopic ETE'
      WHEN c.ete_grade_final = 'gross'             THEN 'Gross ETE'
      WHEN c.ete_grade_final = 'present_ungraded'  THEN 'Present ungraded'
      ELSE 'Missing/other'
    END AS ete_group
  FROM manuscript_workspace.cohort_m044_ajcc_ete_v1 c
  LEFT JOIN main.canonical_recurrence_resolved_v1 r USING (research_id)
  WHERE c.surg_first_date BETWEEN DATE '1999-01-01' AND DATE '2024-12-31'
)
SELECT ete_group, COUNT(*) AS n,
  SUM(CASE WHEN recurrence_path_proven THEN 1 ELSE 0 END) AS pp_n,
  ROUND(AVG(CASE WHEN recurrence_path_proven THEN 1.0 ELSE 0.0 END),4) AS pp_rate
FROM cohort GROUP BY 1 ORDER BY 1;


-- ---------------------------------------------------------------------
-- 9. AJCC T-stage cross-tab
-- ---------------------------------------------------------------------
WITH cohort AS (
  SELECT *,
    CASE
      WHEN ete_grade_final IN ('false','absent') THEN 'No/negative ETE'
      WHEN ete_grade_final = 'microscopic'       THEN 'Microscopic ETE'
      WHEN ete_grade_final = 'gross'             THEN 'Gross ETE'
      WHEN ete_grade_final = 'present_ungraded'  THEN 'Present ungraded'
      ELSE 'Missing/other'
    END AS ete_group
  FROM manuscript_workspace.cohort_m044_ajcc_ete_v1
)
SELECT ete_group, ajcc8_t_stage, COUNT(*) AS n
FROM cohort GROUP BY 1,2 ORDER BY 1, 2 NULLS LAST;


-- ---------------------------------------------------------------------
-- 10. Tumor-size strata × ETE group (path-proven recurrence)
-- ---------------------------------------------------------------------
WITH cohort AS (
  SELECT c.tumor_size_cm, r.recurrence_path_proven,
    CASE
      WHEN c.ete_grade_final IN ('false','absent') THEN 'No/negative ETE'
      WHEN c.ete_grade_final = 'microscopic'       THEN 'Microscopic ETE'
      WHEN c.ete_grade_final = 'gross'             THEN 'Gross ETE'
      ELSE 'Other'
    END AS ete_group
  FROM manuscript_workspace.cohort_m044_ajcc_ete_v1 c
  LEFT JOIN main.canonical_recurrence_resolved_v1 r USING (research_id)
)
SELECT ete_group,
  CASE WHEN tumor_size_cm <= 1 THEN '<=1 cm'
       WHEN tumor_size_cm <= 2 THEN '1.1-2 cm'
       WHEN tumor_size_cm <= 4 THEN '2.1-4 cm'
       WHEN tumor_size_cm > 4  THEN '>4 cm'
       ELSE 'unknown' END AS size_bin,
  COUNT(*) AS n,
  SUM(CASE WHEN recurrence_path_proven THEN 1 ELSE 0 END) AS pp_n,
  ROUND(AVG(CASE WHEN recurrence_path_proven THEN 1.0 ELSE 0.0 END),4) AS pp_rate
FROM cohort
WHERE ete_group IN ('No/negative ETE','Microscopic ETE','Gross ETE')
GROUP BY 1,2 ORDER BY 1,2;


-- ---------------------------------------------------------------------
-- 11. Reoperative interaction by ETE group
-- ---------------------------------------------------------------------
WITH cohort AS (
  SELECT c.research_id, c.ete_grade_final, r.recurrence_path_proven,
         reop.n_surgeries, reop.days_to_2nd, reop.completion_reason,
    CASE
      WHEN c.ete_grade_final IN ('false','absent') THEN 'No/negative ETE'
      WHEN c.ete_grade_final = 'microscopic'       THEN 'Microscopic ETE'
      WHEN c.ete_grade_final = 'gross'             THEN 'Gross ETE'
      WHEN c.ete_grade_final = 'present_ungraded'  THEN 'Present ungraded'
      ELSE 'Missing/other'
    END AS ete_group
  FROM manuscript_workspace.cohort_m044_ajcc_ete_v1 c
  LEFT JOIN main.canonical_recurrence_resolved_v1 r USING (research_id)
  LEFT JOIN (
    SELECT research_id, MAX(n_surgeries) AS n_surgeries,
           MAX(days_between_first_second_surgery) AS days_to_2nd,
           MAX(completion_reason) AS completion_reason
    FROM manuscript_workspace.cohort_m040_reoperative_v1 GROUP BY research_id
  ) reop USING (research_id)
)
SELECT ete_group, COUNT(*) AS n,
  SUM(CASE WHEN n_surgeries>=2 THEN 1 ELSE 0 END) AS ge2_n,
  SUM(CASE WHEN n_surgeries>=2 AND recurrence_path_proven THEN 1 ELSE 0 END) AS ge2_and_pp_n,
  SUM(CASE WHEN completion_reason IS NOT NULL THEN 1 ELSE 0 END) AS comp_reason_known
FROM cohort GROUP BY 1 ORDER BY 1;

-- End of M044_ETE_analysis.sql
