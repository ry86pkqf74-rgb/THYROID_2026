-- =============================================================================
-- M044 master validation — cohort_m044_ajcc_ete_v1 vs canonical sources
-- Database: thyroid_canonical_publication_v1_0 (USE main after connect_locked)
--
-- Expected snapshot (manuscript-frozen; runner compares programmatically):
--   n=4128, distinct_rid=4128, duplicate_rows=0
--   ETE: Microscopic 2576, Gross 1266, No/negative 192, Present-ungraded 29,
--        Missing/other 65
--   Recurrence (canonical_recurrence_resolved_v1): path_proven 145,
--        imaging_only_unconfirmed 195, composite (path + imaging-only) 340
--   Follow-up: followup_years<=0 OR NULL => 1400; followup_years>0 => 2728
--
-- Split markers: lines must match -- QUERY: <name> (see runner regex)
-- =============================================================================

-- QUERY: main_audit
WITH base AS (
  SELECT
    c.research_id,
    c.followup_years,
    r.recurrence_path_proven,
    r.recurrence_status_final,
    CASE
      WHEN c.ete_grade_final IN ('false', 'absent') THEN 'No/negative'
      WHEN c.ete_grade_final = 'microscopic' THEN 'Microscopic'
      WHEN c.ete_grade_final = 'gross' THEN 'Gross'
      WHEN c.ete_grade_final = 'present_ungraded' THEN 'Present-ungraded'
      ELSE 'Missing/other'
    END AS ete_bucket
  FROM manuscript_workspace.cohort_m044_ajcc_ete_v1 AS c
  LEFT JOIN main.canonical_recurrence_resolved_v1 AS r
    ON CAST(c.research_id AS VARCHAR) = CAST(r.research_id AS VARCHAR)
)
SELECT
  COUNT(*) AS n_rows,
  COUNT(DISTINCT research_id) AS distinct_research_id,
  (COUNT(*) - COUNT(DISTINCT research_id)) AS duplicate_extra_rows,
  SUM(CASE WHEN ete_bucket = 'Microscopic' THEN 1 ELSE 0 END) AS ete_microscopic,
  SUM(CASE WHEN ete_bucket = 'Gross' THEN 1 ELSE 0 END) AS ete_gross,
  SUM(CASE WHEN ete_bucket = 'No/negative' THEN 1 ELSE 0 END) AS ete_no_negative,
  SUM(CASE WHEN ete_bucket = 'Present-ungraded' THEN 1 ELSE 0 END) AS ete_present_ungraded,
  SUM(CASE WHEN ete_bucket = 'Missing/other' THEN 1 ELSE 0 END) AS ete_missing_other,
  SUM(CASE WHEN recurrence_path_proven IS TRUE THEN 1 ELSE 0 END) AS recurrence_path_proven_n,
  SUM(
    CASE WHEN recurrence_status_final = 'imaging_only_unconfirmed' THEN 1 ELSE 0 END
  ) AS recurrence_imaging_only_n,
  SUM(
    CASE
      WHEN recurrence_status_final IN ('path_proven', 'imaging_only_unconfirmed')
      THEN 1 ELSE 0 END
  ) AS recurrence_composite_n,
  SUM(CASE WHEN followup_years IS NULL OR followup_years <= 0 THEN 1 ELSE 0 END) AS fu_zero_n,
  SUM(CASE WHEN followup_years IS NOT NULL AND followup_years > 0 THEN 1 ELSE 0 END) AS fu_positive_n
FROM base;

-- QUERY: cohort_membership
WITH filt AS (
  SELECT research_id
  FROM main.canonical_patient_master
  WHERE is_malignant = TRUE
    AND ajcc8_stage_group IS NOT NULL
),
coh AS (
  SELECT research_id
  FROM manuscript_workspace.cohort_m044_ajcc_ete_v1
)
SELECT
  (SELECT COUNT(*) FROM filt) AS cpm_malignant_staged_n,
  (SELECT COUNT(*) FROM coh) AS cohort_n,
  (
    SELECT COUNT(*)
    FROM coh AS c
    LEFT JOIN filt AS f
      ON CAST(c.research_id AS VARCHAR) = CAST(f.research_id AS VARCHAR)
    WHERE f.research_id IS NULL
  ) AS cohort_rows_not_in_cpm_filter,
  (
    SELECT COUNT(*)
    FROM filt AS f
    LEFT JOIN coh AS c
      ON CAST(f.research_id AS VARCHAR) = CAST(c.research_id AS VARCHAR)
    WHERE c.research_id IS NULL
  ) AS cpm_filter_missing_from_cohort;

-- QUERY: cpm_ete_consistency
-- Cohort view should match CPM adjudicated ETE column (v2 is column-of-record).
SELECT
  COUNT(*) AS n_joined,
  SUM(
    CASE
      WHEN CAST(c.ete_grade_final AS VARCHAR) IS NOT DISTINCT FROM CAST(p.ete_grade_final_v2 AS VARCHAR)
      THEN 1 ELSE 0 END
  ) AS ete_match_n,
  SUM(
    CASE
      WHEN CAST(c.ete_grade_final AS VARCHAR) IS DISTINCT FROM CAST(p.ete_grade_final_v2 AS VARCHAR)
      THEN 1 ELSE 0 END
  ) AS ete_mismatch_n
FROM manuscript_workspace.cohort_m044_ajcc_ete_v1 AS c
INNER JOIN main.canonical_patient_master AS p
  ON CAST(c.research_id AS VARCHAR) = CAST(p.research_id AS VARCHAR);

-- QUERY: surgery_date_lineage
-- SSOT = CPM surg_first_date (DATE); mig_254 filled NULL gaps from first_surgery_date_v2.
-- Window flags support manuscript S2 (1999–2024) vs post-2024 and data-freeze sentinel 2024-06-04.
SELECT
  COUNT(*) AS n_cohort,
  SUM(CASE WHEN surg_first_date IS NOT NULL THEN 1 ELSE 0 END) AS surg_first_nonmissing,
  SUM(CASE WHEN surg_date_missing IS TRUE THEN 1 ELSE 0 END) AS surg_first_missing,
  SUM(CASE WHEN surg_date_pre_1999 IS TRUE THEN 1 ELSE 0 END) AS surg_date_pre_1999_n,
  SUM(CASE WHEN surg_date_1999_2024 IS TRUE THEN 1 ELSE 0 END) AS surg_date_1999_2024_n,
  SUM(CASE WHEN surg_date_post_2024 IS TRUE THEN 1 ELSE 0 END) AS surg_date_post_2024_n,
  SUM(CASE WHEN surg_date_after_2024_06_04 IS TRUE THEN 1 ELSE 0 END)
    AS surg_date_after_2024_06_04_n,
  SUM(
    CASE
      WHEN COALESCE(surg_date_missing, TRUE) IS NOT TRUE
        AND (
          CAST(surg_date_pre_1999 AS INTEGER)
          + CAST(surg_date_1999_2024 AS INTEGER)
          + CAST(surg_date_post_2024 AS INTEGER)
        ) <> 1
      THEN 1 ELSE 0 END
  ) AS calendar_partition_violations
FROM manuscript_workspace.cohort_m044_ajcc_ete_v1;

-- QUERY: surgery_date_vs_operative_v2_optional
-- Informational — expect 0 if CPM earliest surgery aligns with operative v2 DATE for cohort.
SELECT
  COUNT(*) AS n_compared,
  SUM(
    CASE
      WHEN CAST(c.surg_first_date AS DATE) IS DISTINCT FROM CAST(p.first_surgery_date_v2 AS DATE)
      THEN 1 ELSE 0 END
  ) AS n_mismatch_vs_first_surgery_date_v2,
  SUM(CASE WHEN p.first_surgery_date_v2 IS NULL THEN 1 ELSE 0 END) AS cohort_rows_with_null_first_surgery_date_v2
FROM manuscript_workspace.cohort_m044_ajcc_ete_v1 AS c
INNER JOIN main.canonical_patient_master AS p
  ON CAST(c.research_id AS VARCHAR) = CAST(p.research_id AS VARCHAR);

-- QUERY: recurrence_coherence
-- main.canonical_recurrence_resolved_v1: status-final must agree with BOOLEAN evidence flags
-- (builder SSOT: qc_framework_v1/migrations/62_canonical_recurrence_resolved_v1.sql).
-- Dual-track retained: path_proven rows may still have recurrence_imaging_suspicious=TRUE.
SELECT
  SUM(
    CASE
      WHEN recurrence_status_final = 'path_proven' AND recurrence_path_proven IS NOT TRUE
      THEN 1 ELSE 0 END
  ) AS v_path_status_missing_bool,
  SUM(
    CASE
      WHEN recurrence_status_final = 'imaging_only_unconfirmed'
        AND (recurrence_path_proven IS TRUE OR recurrence_imaging_suspicious IS NOT TRUE)
      THEN 1 ELSE 0 END
  ) AS v_imaging_only_incoherent,
  SUM(
    CASE
      WHEN recurrence_status_final = 'none'
        AND (recurrence_path_proven IS TRUE OR recurrence_imaging_suspicious IS TRUE)
      THEN 1 ELSE 0 END
  ) AS v_none_but_evidence_bool
FROM main.canonical_recurrence_resolved_v1;

-- QUERY: legacy_recurrence_audit
-- Legacy CPM flags vs canonical_recurrence_resolved_v1 (M044 cohort only).
-- Live metrics; not pass/fail — see manuscript_workspace.m044_legacy_recurrence_flag_audit_v1 (mig_257).
SELECT * FROM manuscript_workspace.m044_legacy_recurrence_flag_audit_v1;

-- QUERY: table1b_tt_ete_audit
-- Table 1B — Among M044-eligible patients with total thyroidectomy (canonical surgical-extent union).
-- SSOT extent rule: `surg_total_thyroidectomy IS TRUE` OR normalized procedure_type = total_thyroidectomy.
-- ETE buckets align with primary Table 1 / main_audit CASE logic (short labels).
WITH base AS (
  SELECT
    p.research_id,
    p.ete_grade_final,
    p.surg_total_thyroidectomy,
    p.surg_procedure_type
  FROM main.canonical_patient_master AS p
  WHERE p.is_malignant IS TRUE
    AND p.ajcc8_stage_group IS NOT NULL
),
tt AS (
  SELECT
    research_id,
    CASE
      WHEN ete_grade_final IN ('false', 'absent') THEN 'No/negative'
      WHEN ete_grade_final = 'microscopic' THEN 'Microscopic'
      WHEN ete_grade_final = 'gross' THEN 'Gross'
      WHEN ete_grade_final = 'present_ungraded' THEN 'Present-ungraded'
      ELSE 'Missing/other'
    END AS ete_bucket
  FROM base
  WHERE surg_total_thyroidectomy IS TRUE
    OR LOWER(TRIM(COALESCE(CAST(surg_procedure_type AS VARCHAR), ''))) = 'total_thyroidectomy'
)
SELECT
  COUNT(*) AS tt_n_total,
  SUM(CASE WHEN ete_bucket = 'No/negative' THEN 1 ELSE 0 END) AS tt_n_noneg,
  SUM(CASE WHEN ete_bucket = 'Microscopic' THEN 1 ELSE 0 END) AS tt_n_microscopic,
  SUM(CASE WHEN ete_bucket = 'Gross' THEN 1 ELSE 0 END) AS tt_n_gross,
  SUM(CASE WHEN ete_bucket = 'Present-ungraded' THEN 1 ELSE 0 END) AS tt_n_present_ungraded,
  SUM(CASE WHEN ete_bucket = 'Missing/other' THEN 1 ELSE 0 END) AS tt_n_missing_other
FROM tt;

-- QUERY: ete_grade_final_raw
-- Drift diagnostics: raw ete_grade_final distribution (not used for pass/fail).
SELECT
  CAST(ete_grade_final AS VARCHAR) AS ete_grade_final_raw,
  COUNT(*) AS n
FROM manuscript_workspace.cohort_m044_ajcc_ete_v1
GROUP BY 1
ORDER BY n DESC, 1;
