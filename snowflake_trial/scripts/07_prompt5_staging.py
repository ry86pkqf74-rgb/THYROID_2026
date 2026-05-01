"""Prompt 5: AJCC 8th edition staging consistency check via AI_COMPLETE."""
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import deploy_histology_lookup_ssot, get_cursor, md_table

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/05_staging_validation.md")

ctx, cur = get_cursor()
deploy_histology_lookup_ssot(cur)
report = ["# Snowflake Cortex Validation — Prompt 5: AJCC 8 Staging Consistency\n",
          f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
          "**Source:** CANONICAL_PATIENT_MASTER_FLAT (malignant subset)\n\n---\n"]

# 1. Stage distribution overview
print("=== Stage group distribution ===")
cur.execute("""
SELECT
  AJCC8_T_STAGE, AJCC8_N_STAGE, AJCC8_M_STAGE, AJCC8_STAGE_GROUP, COUNT(*) AS n
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE IS_MALIGNANT = TRUE AND AJCC8_STAGE_GROUP IS NOT NULL
GROUP BY 1, 2, 3, 4 ORDER BY n DESC LIMIT 25
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
report.append("## Top T/N/M × stage_group combinations\n\n")
report.append(md_table(rows, cols) + "\n")

# 2. AJCC 8 rule check: anyone with M1 should be Stage IVB
print("=== M1 stage rule probe ===")
cur.execute("""
SELECT AJCC8_STAGE_GROUP, COUNT(*) AS n
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE IS_MALIGNANT = TRUE AND AJCC8_M_STAGE = 'M1'
GROUP BY 1 ORDER BY 2 DESC
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
report.append("## Rule probe: M1 patients should all be Stage IVB\n\n")
report.append(md_table(rows, cols) + "\n")

# 3. Age-based stage rule: differentiated thyroid CA, age <55, can only be I or II
print("=== Age <55 + DTC stage rule ===")
cur.execute("""
SELECT cp.AJCC8_STAGE_GROUP, COUNT(*) AS n
FROM CANONICAL_PATIENT_MASTER_FLAT cp
LEFT JOIN CANONICAL_HISTOLOGY_LOOKUP_V1 lu
  ON cp.HISTOLOGY_FINAL = lu.HISTOLOGY_FINAL_RAW
WHERE cp.IS_MALIGNANT = TRUE
  AND cp.AGE_AT_SURGERY < 55
  AND lu.HISTOLOGY_GROUP = 'PTC'
GROUP BY 1 ORDER BY 2 DESC
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
report.append("## Rule probe: PTC + age <55 should be Stage I or II only\n\n")
report.append(md_table(rows, cols) + "\n")

# 4. AI_COMPLETE: ask llama3.1-8b to grade staging consistency on 100-row sample
print("=== AI_COMPLETE staging audit (100-pt sample, llama3.1-8b) ===")
t0 = time.time()
cur.execute("""
SELECT
  RESEARCH_ID,
  AJCC8_T_STAGE, AJCC8_N_STAGE, AJCC8_M_STAGE, AJCC8_STAGE_GROUP,
  AGE_AT_SURGERY, HISTOLOGY_FINAL, TUMOR_SIZE_CM_MAX, ETE_GRADE,
  AI_COMPLETE(
    'llama3.1-8b',
    CONCAT(
      'AJCC 8 thyroid staging check. Patient: T=', COALESCE(AJCC8_T_STAGE, 'NULL'),
      ', N=', COALESCE(AJCC8_N_STAGE, 'NULL'),
      ', M=', COALESCE(AJCC8_M_STAGE, 'NULL'),
      ', age=', COALESCE(AGE_AT_SURGERY::VARCHAR, 'NULL'),
      ', histology=', COALESCE(HISTOLOGY_FINAL, 'NULL'),
      ', size_cm=', COALESCE(TUMOR_SIZE_CM_MAX::VARCHAR, 'NULL'),
      ', ETE=', COALESCE(ETE_GRADE, 'NULL'),
      '. Recorded stage: ', COALESCE(AJCC8_STAGE_GROUP, 'NULL'),
      '. Reply with one word only: CONSISTENT, INCONSISTENT, or UNCERTAIN.'
    )
  ) AS ai_verdict
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE IS_MALIGNANT = TRUE AND AJCC8_STAGE_GROUP IS NOT NULL
QUALIFY ROW_NUMBER() OVER (ORDER BY HASH(RESEARCH_ID)) <= 100
""")
ai_rows = cur.fetchall()
print(f"  {len(ai_rows)} AI verdicts in {time.time()-t0:.1f}s")

counts = {"CONSISTENT": 0, "INCONSISTENT": 0, "UNCERTAIN": 0, "Other": 0}
flagged = []
for r in ai_rows:
    verdict = (r[9] or "").strip().upper()
    # Strip quotes & punctuation
    verdict = verdict.strip('"').strip("'").rstrip(".").strip()
    # Take first token if model added explanation
    verdict_word = verdict.split()[0] if verdict else "OTHER"
    bucket = verdict_word if verdict_word in counts else "Other"
    counts[bucket] += 1
    if bucket == "INCONSISTENT":
        flagged.append((r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], verdict[:100]))

report.append("## AI_COMPLETE staging audit (100-pt sample, llama3.1-8b)\n\n")
for k, v in counts.items():
    report.append(f"- **{k}:** {v}\n")
report.append("\n")
if flagged:
    report.append("### Patients flagged INCONSISTENT\n\n")
    report.append(md_table(
        flagged,
        ["rid", "T", "N", "M", "stage", "age", "histology", "size", "ete", "verdict"],
        max_rows=20) + "\n")

OUT.write_text("".join(report))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
ctx.close()
