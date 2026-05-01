"""Prompt 4: RAI / Thyroglobulin kinetics validation."""
import sys, time, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor, md_table

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/04_rai_tg_validation.md")

ctx, cur = get_cursor()
report = ["# Snowflake Cortex Validation — Prompt 4: RAI / Tg Kinetics\n",
          f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
          "**Sources:** CANONICAL_PATIENT_MASTER_FLAT + CANONICAL_LABS_THYROGLOBULIN_V1_FLAT\n\n---\n"]

# 1. Tg lab availability vs RAI receipt
print("=== Tg lab availability × RAI ===")
cur.execute("""
WITH tg_count AS (
  SELECT RESEARCH_ID::VARCHAR AS rid, COUNT(*) AS n_tg_results
  FROM CANONICAL_LABS_THYROGLOBULIN_V1_FLAT
  GROUP BY 1
)
SELECT
  CASE WHEN cpm.RAI_RECEIVED_FLAG = TRUE THEN 'RAI_yes'
       WHEN cpm.RAI_RECEIVED_FLAG = FALSE THEN 'RAI_no'
       ELSE 'unknown' END AS rai_status,
  COUNT(*) AS n_pts,
  COUNT_IF(t.n_tg_results > 0) AS n_with_tg,
  ROUND(AVG(COALESCE(t.n_tg_results, 0)), 1) AS mean_tg_results
FROM CANONICAL_PATIENT_MASTER_FLAT cpm
LEFT JOIN tg_count t ON cpm.RESEARCH_ID::VARCHAR = t.rid
WHERE cpm.IS_MALIGNANT = TRUE
GROUP BY 1 ORDER BY 1
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
print(md_table(rows, cols))
report.append("## Tg availability × RAI receipt (malignant cohort)\n\n")
report.append(md_table(rows, cols) + "\n")

# 2. Tg trajectory characterization
print("=== Tg per-patient longitudinal pattern ===")
cur.execute("""
SELECT
  COUNT(DISTINCT RESEARCH_ID) AS n_pts,
  COUNT(*) AS n_total_results,
  ROUND(AVG(cnt), 1) AS mean_results_per_pt,
  MAX(cnt) AS max_results_per_pt
FROM (
  SELECT RESEARCH_ID, COUNT(*) AS cnt
  FROM CANONICAL_LABS_THYROGLOBULIN_V1_FLAT
  GROUP BY 1
)
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
report.append("## Per-patient Tg longitudinal coverage\n\n")
report.append(md_table(rows, cols) + "\n")

# 3. AI_FILTER: implausible Tg values (negative, or > 5000 ng/mL pre-RAI)
print("=== AI_FILTER: implausible Tg values ===")
cur.execute("""
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA='PUBLIC' AND TABLE_NAME='CANONICAL_LABS_THYROGLOBULIN_V1_FLAT'
ORDER BY ORDINAL_POSITION
""")
tg_cols = [r[0] for r in cur.fetchall()]
print(f"  Tg cols: {tg_cols[:10]}")

# VALUE_NUMERIC is TEXT (clinical labs use "<0.9" etc). Use TRY_TO_DOUBLE.
val_col = "VALUE_NUMERIC"
cur.execute(f"""
SELECT
  COUNT(*) AS n_total,
  COUNT_IF(TRY_TO_DOUBLE({val_col}) IS NULL AND {val_col} IS NOT NULL) AS n_non_numeric,
  COUNT_IF(IS_CENSORED) AS n_censored,
  COUNT_IF(TRY_TO_DOUBLE({val_col}) < 0) AS n_negative,
  COUNT_IF(TRY_TO_DOUBLE({val_col}) > 5000) AS n_over_5000,
  COUNT_IF(TRY_TO_DOUBLE({val_col}) = 0) AS n_zero,
  ROUND(AVG(TRY_TO_DOUBLE({val_col})), 2) AS mean_val,
  ROUND(MEDIAN(TRY_TO_DOUBLE({val_col})), 2) AS median_val,
  MIN(TRY_TO_DOUBLE({val_col})) AS min_val, MAX(TRY_TO_DOUBLE({val_col})) AS max_val
FROM CANONICAL_LABS_THYROGLOBULIN_V1_FLAT
WHERE {val_col} IS NOT NULL
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
report.append(f"## Tg value distribution (`{val_col}`, parsed via TRY_TO_DOUBLE)\n\n")
report.append(md_table(rows, cols) + "\n")

# 4. Anomaly detection on a single representative patient (full ML.ANOMALY_DETECTION
#    needs per-patient scaling; for this validation just spot-check)
print("=== Spot-check: 5 patients with most Tg results ===")
cur.execute(f"""
SELECT RESEARCH_ID, COUNT(*) AS n_results
FROM CANONICAL_LABS_THYROGLOBULIN_V1_FLAT
GROUP BY 1 ORDER BY 2 DESC LIMIT 5
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
report.append("## Top-5 most longitudinally tracked patients\n\n")
report.append(md_table(rows, cols) + "\n")

# 5. AI cross-validation: RAI-receivers should mostly have Tg results
print("=== AI_CLASSIFY consistency on RAI×Tg ===")
cur.execute("""
WITH tg_count AS (
  SELECT RESEARCH_ID::VARCHAR AS rid, COUNT(*) AS n_tg
  FROM CANONICAL_LABS_THYROGLOBULIN_V1_FLAT GROUP BY 1
)
SELECT
  cpm.RESEARCH_ID, cpm.RAI_RECEIVED_FLAG, COALESCE(t.n_tg, 0) AS n_tg_results,
  AI_CLASSIFY(
    CONCAT('RAI received: ', COALESCE(cpm.RAI_RECEIVED_FLAG::VARCHAR,'NULL'),
           '; Tg results count: ', COALESCE(t.n_tg::VARCHAR,'0')),
    ARRAY_CONSTRUCT('Concordant', 'Discordant - RAI without followup', 'Discordant - Tg without RAI', 'Insufficient data')
  ) AS verdict
FROM CANONICAL_PATIENT_MASTER_FLAT cpm
LEFT JOIN tg_count t ON cpm.RESEARCH_ID::VARCHAR = t.rid
WHERE cpm.IS_MALIGNANT
QUALIFY ROW_NUMBER() OVER (ORDER BY HASH(cpm.RESEARCH_ID)) <= 30
""")
ai_rows = cur.fetchall()
counts = {"Concordant":0,"Discordant - RAI without followup":0,"Discordant - Tg without RAI":0,"Insufficient data":0,"Other":0}
for r in ai_rows:
    try:
        d = json.loads(r[3])
        label = d.get("labels", ["Other"])[0]
    except Exception:
        label = "Other"
    counts[label] = counts.get(label, 0) + 1
report.append("## AI_CLASSIFY: RAI×Tg concordance (30-pt sample)\n\n")
for k, v in counts.items():
    report.append(f"- **{k}:** {v}\n")
report.append("\n")
print(f"  AI grades: {counts}")

OUT.write_text("".join(report))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
ctx.close()
