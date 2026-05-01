"""Prompt 3: Survival/recurrence integrity validation."""
import sys, time, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor, md_table

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/03_survival_validation.md")

ctx, cur = get_cursor()
report = ["# Snowflake Cortex Validation — Prompt 3: Survival/Recurrence Integrity\n",
          f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
          "**Source:** CANONICAL_PATIENT_MASTER_FLAT\n\n---\n"]

# 1. Vital status / death overview
print("=== Vital status overview ===")
cur.execute("""
SELECT VITAL_STATUS, DEATH_OCCURRED, COUNT(*) AS n
FROM CANONICAL_PATIENT_MASTER_FLAT
GROUP BY 1, 2 ORDER BY 1, 2
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
print(md_table(rows, cols))
report.append("## Vital status × death_occurred\n\n")
report.append(md_table(rows, cols) + "\n")

# 2. Recurrence overview by malignancy
print("=== Recurrence overview ===")
cur.execute("""
SELECT IS_MALIGNANT, ANY_RECURRENCE_FLAG, COUNT(*) AS n,
  ROUND(AVG(TIME_TO_RECURRENCE_DAYS), 0) AS mean_days_to_recur
FROM CANONICAL_PATIENT_MASTER_FLAT
GROUP BY 1, 2 ORDER BY 1, 2
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
print(md_table(rows, cols))
report.append("## Malignancy × recurrence flag\n\n")
report.append(md_table(rows, cols) + "\n")

# 3. Hard contradictions (deterministic SQL)
print("=== Deterministic contradiction probes ===")
probes = [
    ("alive but death_occurred=TRUE",
     "VITAL_STATUS = 'alive' AND DEATH_OCCURRED = TRUE"),
    ("deceased but death_occurred=FALSE",
     "VITAL_STATUS = 'deceased' AND DEATH_OCCURRED = FALSE"),
    ("any_recurrence_flag=TRUE but time_to_recurrence_days NULL",
     "ANY_RECURRENCE_FLAG = TRUE AND TIME_TO_RECURRENCE_DAYS IS NULL"),
    ("any_recurrence_flag=FALSE but time_to_recurrence_days NOT NULL",
     "ANY_RECURRENCE_FLAG = FALSE AND TIME_TO_RECURRENCE_DAYS IS NOT NULL"),
    ("benign but recurrence flagged",
     "IS_MALIGNANT = FALSE AND ANY_RECURRENCE_FLAG = TRUE"),
    ("followup_years > overall_survival_years (deceased pts)",
     "DEATH_OCCURRED = TRUE AND FOLLOWUP_YEARS > OVERALL_SURVIVAL_YEARS"),
]
probe_results = []
for label, where in probes:
    cur.execute(f"SELECT COUNT(*) FROM CANONICAL_PATIENT_MASTER_FLAT WHERE {where}")
    n = cur.fetchone()[0]
    probe_results.append((label, n))
    print(f"  {n:>4}  {label}")
report.append("## Deterministic contradiction probes\n\n")
report.append(md_table(probe_results, ["probe", "n_flagged"]) + "\n")

# 4. AI_CLASSIFY consistency grade on a stratified sample
print("=== AI_CLASSIFY: consistency grade (sample 50) ===")
cur.execute("""
SELECT
  RESEARCH_ID,
  VITAL_STATUS, DEATH_OCCURRED, OVERALL_SURVIVAL_YEARS,
  FOLLOWUP_YEARS, ANY_RECURRENCE_FLAG, TIME_TO_RECURRENCE_DAYS,
  AI_CLASSIFY(
    CONCAT(
      'vital=', COALESCE(VITAL_STATUS, 'NULL'),
      ', death=', COALESCE(DEATH_OCCURRED::VARCHAR, 'NULL'),
      ', surv_yrs=', COALESCE(OVERALL_SURVIVAL_YEARS::VARCHAR, 'NULL'),
      ', followup_yrs=', COALESCE(FOLLOWUP_YEARS::VARCHAR, 'NULL'),
      ', recur=', COALESCE(ANY_RECURRENCE_FLAG::VARCHAR, 'NULL'),
      ', time_to_recur_days=', COALESCE(TIME_TO_RECURRENCE_DAYS::VARCHAR, 'NULL')
    ),
    ARRAY_CONSTRUCT('Consistent', 'Minor discrepancy', 'Major contradiction', 'Insufficient data')
  ) AS grade
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE IS_MALIGNANT = TRUE
QUALIFY ROW_NUMBER() OVER (ORDER BY HASH(RESEARCH_ID)) <= 50
""")
ai_rows = cur.fetchall()
counts = {"Consistent": 0, "Minor discrepancy": 0, "Major contradiction": 0, "Insufficient data": 0, "Other": 0}
flagged = []
for r in ai_rows:
    try:
        d = json.loads(r[7])
        label = d.get("labels", ["Other"])[0]
    except Exception:
        label = "Other"
    counts[label] = counts.get(label, 0) + 1
    if label in ("Major contradiction",):
        flagged.append((r[0], label))
report.append("## AI_CLASSIFY: 50-sample consistency grading (malignant cohort)\n\n")
for k, v in counts.items():
    report.append(f"- **{k}:** {v}\n")
if flagged:
    report.append(f"\n### Flagged 'Major contradiction'\n\n")
    report.append(md_table(flagged, ["research_id", "ai_grade"]) + "\n")
print(f"  AI grades: {counts}")

OUT.write_text("".join(report))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
ctx.close()
