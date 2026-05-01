"""Prompt 10: Imaging coverage + date sanity (Snowflake mirror).

Note: live publication-DB CPM has only 5 NLP TIRADS columns. The 28+ TIRADS
cols (`tirads_best_category_v12` etc.) currently in Snowflake mirror will
disappear on next re-export. Switch to `canonical_us_patient_master_VIEW_v2`
(7 cols) or `views_readable.Patient_Master_Canonical` (42 cols) before re-run.
"""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor, md_table

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/10_imaging_validation.md")

ctx, cur = get_cursor()
report = ["# Snowflake Cortex Validation — Prompt 10: Imaging Coverage (Snowflake re-run)\n",
          f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
          "**Sources:** IMAGING_EXAM_MASTER_V1_FLAT, CANONICAL_US_PATIENT_MASTER_VIEW_V2_FLAT, CPM_FLAT\n\n---\n"]

# 1. Imaging master coverage by source
cur.execute("""
SELECT source, COUNT(*) AS n_exams,
       COUNT(DISTINCT research_id) AS n_pts,
       COUNT_IF(exam_date IS NULL) AS n_null_dates,
       MIN(exam_date) AS min_dt, MAX(exam_date) AS max_dt,
       COUNT_IF(max_tirads IS NOT NULL) AS n_with_tirads,
       AVG(n_nodules)::DECIMAL(6,2) AS avg_nodules
FROM IMAGING_EXAM_MASTER_V1_FLAT
GROUP BY 1 ORDER BY 2 DESC
""")
rows = cur.fetchall(); cols = [d[0] for d in cur.description]
report.append("## Imaging master coverage by source\n\n" + md_table(rows, cols) + "\n")

# 2. Implausible-date distribution
cur.execute("""
SELECT
  CASE
    WHEN exam_date IS NULL THEN '00_null'
    WHEN exam_date < DATE '1990-01-01' THEN '01_pre_1990'
    WHEN exam_date < DATE '2000-01-01' THEN '02_1990s'
    WHEN exam_date < DATE '2010-01-01' THEN '03_2000s'
    WHEN exam_date < DATE '2020-01-01' THEN '04_2010s'
    WHEN exam_date < CURRENT_DATE THEN '05_2020-now'
    WHEN exam_date < DATE '2030-01-01' THEN '06_post_today'
    ELSE '07_far_future'
  END AS bucket, COUNT(*) AS n
FROM IMAGING_EXAM_MASTER_V1_FLAT
GROUP BY 1 ORDER BY 1
""")
rows = cur.fetchall(); cols = [d[0] for d in cur.description]
report.append("## Date plausibility buckets\n\n" + md_table(rows, cols) + "\n")

# 3. US patient-master rollup
cur.execute("""
SELECT
  COUNT(*) AS n,
  COUNT_IF(has_any_us) AS n_with_us,
  COUNT_IF(preop_us_available_flag) AS n_preop_us,
  COUNT_IF(max_tirads_category_ever IS NOT NULL) AS n_with_tirads,
  COUNT_IF(bilateral_disease_flag_ever) AS n_bilateral,
  COUNT_IF(multifocal_flag_ever) AS n_multifocal,
  COUNT_IF(any_suspicious_us_ln_ever) AS n_susp_ln,
  COUNT_IF(any_nlp_backfill_pending_for_patient) AS n_nlp_backfill_pending
FROM CANONICAL_US_PATIENT_MASTER_VIEW_V2_FLAT
""")
rows = cur.fetchall(); cols = [d[0] for d in cur.description]
report.append("## US patient-master rollup coverage\n\n" + md_table(rows, cols) + "\n")

# 4. TIRADS × malignancy (post-mig265 column path)
cur.execute("""
SELECT v.max_tirads_category_ever AS tirads, COUNT(*) AS n_pts,
       COUNT_IF(c.is_malignant) AS n_malignant,
       ROUND(100.0 * COUNT_IF(c.is_malignant)/COUNT(*), 1) AS rom_pct
FROM CANONICAL_US_PATIENT_MASTER_VIEW_V2_FLAT v
JOIN CANONICAL_PATIENT_MASTER_FLAT c USING(research_id)
WHERE v.max_tirads_category_ever IS NOT NULL
GROUP BY 1 ORDER BY 1
""")
rows = cur.fetchall(); cols = [d[0] for d in cur.description]
report.append("## TIRADS × malignancy (max_tirads_category_ever)\n\n" + md_table(rows, cols) + "\n")
report.append("**ACR expected:** TR1 ~0%, TR2 <2%, TR3 <5%, TR4 5-20%, TR5 >20%\n\n")

OUT.write_text("".join(report))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
ctx.close()
