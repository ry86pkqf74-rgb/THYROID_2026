"""Prompt 11: Comorbidity / PMH / PSH coverage (Snowflake mirror).

Requires CANONICAL_PMH_PATIENT_ROLLUP_V1_FLAT, CANONICAL_PSH_EVENTS_V1_FLAT,
CANONICAL_PMH_EVENTS_V1_FLAT loaded into Snowflake first (not in current
9-table set; add to TABLES list in 01_export_md_to_parquet.py).
"""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor, md_table

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/11_comorbidity_validation.md")

ctx, cur = get_cursor()
report = ["# Snowflake Cortex Validation — Prompt 11: Comorbidity / PMH / PSH (Snowflake re-run)\n",
          f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n---\n"]

# 1. PMH rollup prevalences
conds = ['hypothyroidism','hypertension','diabetes','hyperthyroidism','prior_cancer_hx',
         'obesity','gerd','asthma','breast_cancer','depression','cad','ckd','afib',
         'lung_cancer','copd','autoimmune_thyroid_hx','radiation_exposure',
         'family_hx_thyroid','osteoporosis','family_hx_cancer','coagulopathy','men_syndrome']
union_parts = [
    f"SELECT '{c}' AS cond, COUNT_IF(pmh_{c}_any_evidence) AS any_n, "
    f"COUNT_IF(pmh_{c}_definitive) AS def_n FROM CANONICAL_PMH_PATIENT_ROLLUP_V1_FLAT"
    for c in conds]
union_parts.append("SELECT 'smoking_current', COUNT_IF(pmh_smoking_status_current), 0 FROM CANONICAL_PMH_PATIENT_ROLLUP_V1_FLAT")
union_parts.append("SELECT 'smoking_former', COUNT_IF(pmh_smoking_status_former), 0 FROM CANONICAL_PMH_PATIENT_ROLLUP_V1_FLAT")
union_parts.append("SELECT 'smoking_never', COUNT_IF(pmh_smoking_status_never), 0 FROM CANONICAL_PMH_PATIENT_ROLLUP_V1_FLAT")
sql = " UNION ALL ".join(union_parts)
cur.execute(f"""
WITH counts AS ({sql})
SELECT cond, any_n, def_n,
  ROUND(100.0 * any_n / 10871.0, 1) AS any_pct,
  ROUND(100.0 * def_n / 10871.0, 1) AS def_pct
FROM counts ORDER BY any_n DESC
""")
rows = cur.fetchall(); cols = [d[0] for d in cur.description]
report.append("## PMH rollup prevalences\n\n" + md_table(rows, cols) + "\n")

# 2. PSH event distribution
cur.execute("""
SELECT finding_value_norm, finding_status, COUNT(*) AS n_events,
       COUNT(DISTINCT research_id) AS n_pts
FROM CANONICAL_PSH_EVENTS_V1_FLAT
GROUP BY 1, 2 ORDER BY n_events DESC LIMIT 30
""")
rows = cur.fetchall(); cols = [d[0] for d in cur.description]
report.append("## PSH event distribution (top 30)\n\n" + md_table(rows, cols) + "\n")

OUT.write_text("".join(report))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
ctx.close()
