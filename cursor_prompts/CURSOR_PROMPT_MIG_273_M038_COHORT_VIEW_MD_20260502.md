# Cursor Composer Dispatch — mig_273: M038 Massive Goiter cohort view in MotherDuck

**Generated:** 2026-05-02 by Cowork.
**Lane:** mig_273 — Cowork built `COHORT_M038_MASSIVE_GOITER` in Snowflake (script 31). Mirror it to MotherDuck so M038 manuscript work, M044 cross-comparisons, and any tool with MD-only access can use the same definition.
**Recommended agent:** **Cursor Composer**.
**Estimated runtime:** 20 min
**Triggered by:** Round 9 M038 scaffold work.
**Severity:** MED (manuscript prerequisite for M038 + M252 audit).

---

## §0 — First message to paste into Cursor Composer

> mig_273 dispatch. Mirror Snowflake's COHORT_M038_MASSIVE_GOITER to MotherDuck as a manuscript_workspace view. Definition in §1; weight bucket = ≥200g (massive) / 50-199g (moderate) / <50g (small) / NULL (unknown).

---

## §1 — View definition

```sql
CREATE OR REPLACE VIEW main.cohort_m038_massive_goiter_v1 AS
SELECT
  research_id, age_at_surgery, sex, race,
  histology_final, is_malignant, first_surgery_date,
  ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage, ajcc8_stage_group,
  tumor_size_cm_max, ete_grade,
  gland_weight_final_g, gland_weight_source, multifocal_flag_path,
  syn_multinodular_goiter, ct_goiter_present_any,
  surg_procedure_type, rai_received_flag,
  any_recurrence_flag, overall_survival_years, followup_years,
  -- Derived
  CASE
    WHEN gland_weight_final_g IS NULL THEN 'unknown'
    WHEN gland_weight_final_g >= 200 THEN 'massive_200g_plus'
    WHEN gland_weight_final_g >= 50  THEN 'moderate_50_to_199g'
    ELSE 'small_under_50g'
  END AS weight_bucket,
  CASE WHEN gland_weight_final_g >= 200 THEN TRUE ELSE FALSE END AS is_massive_goiter
FROM main.canonical_patient_master;
```

## §2 — Verify

```sql
SELECT weight_bucket, COUNT(*) AS n
FROM main.cohort_m038_massive_goiter_v1
GROUP BY 1 ORDER BY 2 DESC;
-- Expected:
--   small_under_50g   ~6,188
--   moderate_50_to_199g ~2,467
--   unknown           ~1,741
--   massive_200g_plus  ~475
```

## §3 — Signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES ('mig_273', CURRENT_TIMESTAMP::TIMESTAMP, 'logan_via_cursor',
  'Built main.cohort_m038_massive_goiter_v1 view (≥200g threshold). Mirror of Snowflake COHORT_M038_MASSIVE_GOITER. Used by M038 Massive Goiter Definition Paper + downstream complications audit.');
```

## §4 — Surgical git add
```
qc_framework_v1/migrations/273_cohort_m038_view_20260502.sql
scripts/output/mig_273_apply_log.txt
```
