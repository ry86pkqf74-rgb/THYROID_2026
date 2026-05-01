"""Build manuscript cohort views.

Creates:
  COHORT_M004_AUTOIMMUNE_CARCINOMA — Graves/Hashimoto + cancer
  COHORT_M037_LN_PREDICTORS        — predictors of nodal positivity
  COHORT_M032_25YR_DESCRIPTIVE     — 25-year descriptive
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import deploy_histology_lookup_ssot, get_cursor

ctx, cur = get_cursor()
deploy_histology_lookup_ssot(cur)

# Discover which Graves/Hashimoto columns exist
print("=== Discovering autoimmune-related columns ===")
cur.execute("""
SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_NAME = 'CANONICAL_PATIENT_MASTER_FLAT'
  AND (LOWER(COLUMN_NAME) LIKE '%graves%' OR LOWER(COLUMN_NAME) LIKE '%hashimoto%'
       OR LOWER(COLUMN_NAME) LIKE '%autoimmune%' OR LOWER(COLUMN_NAME) LIKE '%thyroiditis%')
ORDER BY COLUMN_NAME
""")
auto_cols = [r[0] for r in cur.fetchall()]
print(f"  Found {len(auto_cols)} autoimmune-related cols:")
for c in auto_cols[:15]:
    print(f"    {c}")

# Pick best candidates (prefer syn_* / pmh_* if present; else first match)
def pick(prefix):
    return next((c for c in auto_cols if c.upper().startswith(prefix)), None)

graves_col = pick("SYN_GRAVES") or pick("PMH_GRAVES") or pick("HX_GRAVES") or next((c for c in auto_cols if "GRAVES" in c.upper()), None)
hashi_col = pick("SYN_HASHIMOTO") or pick("PMH_HASHIMOTO") or pick("HX_HASHIMOTO") or next((c for c in auto_cols if "HASHIMOTO" in c.upper()), None)
print(f"\n  Using: GRAVES={graves_col}  HASHIMOTO={hashi_col}")

# === M004 ===
print("\n=== M004: Autoimmune + carcinoma cohort ===")
graves_expr = f"COALESCE({graves_col}, FALSE)" if graves_col else "FALSE"
hashi_expr = f"COALESCE({hashi_col}, FALSE)" if hashi_col else "FALSE"
m004_sql = f"""
CREATE OR REPLACE VIEW THYROID_VALIDATION.PUBLIC.COHORT_M004_AUTOIMMUNE_CARCINOMA AS
SELECT
  RESEARCH_ID,
  AGE_AT_SURGERY, SEX, RACE, BMI_COMBINED,
  HISTOLOGY_FINAL, IS_MALIGNANT, FIRST_SURGERY_DATE,
  AJCC8_STAGE_GROUP, AJCC8_T_STAGE, AJCC8_N_STAGE, AJCC8_M_STAGE,
  TUMOR_SIZE_CM_MAX, ETE_GRADE,
  LN_TOTAL_EXAMINED, LN_TOTAL_POSITIVE, LN_POSITIVE_FLAG,
  SURG_PROCEDURE_TYPE, RAI_RECEIVED_FLAG,
  ANY_RECURRENCE_FLAG, TIME_TO_RECURRENCE_DAYS, OVERALL_SURVIVAL_YEARS, FOLLOWUP_YEARS,
  MOLECULAR_TESTED_CONFIRMED, BRAF_POSITIVE_FINAL, RAS_POSITIVE_FINAL,
  {graves_expr} AS HAS_GRAVES,
  {hashi_expr} AS HAS_HASHIMOTO,
  CASE
    WHEN {graves_expr} THEN 'Graves'
    WHEN {hashi_expr} THEN 'Hashimoto'
    ELSE 'Neither'
  END AS AUTOIMMUNE_TYPE
FROM THYROID_VALIDATION.PUBLIC.CANONICAL_PATIENT_MASTER_FLAT
WHERE IS_MALIGNANT = TRUE
"""
cur.execute(m004_sql)
cur.execute("SELECT COUNT(*), COUNT_IF(HAS_GRAVES), COUNT_IF(HAS_HASHIMOTO) FROM COHORT_M004_AUTOIMMUNE_CARCINOMA")
n, ng, nh = cur.fetchone()
print(f"  COHORT_M004_AUTOIMMUNE_CARCINOMA: n={n:,}  Graves={ng:,}  Hashimoto={nh:,}")

# === M037: LN predictors ===
print("\n=== M037: LN predictors cohort ===")
m037_sql = """
CREATE OR REPLACE VIEW THYROID_VALIDATION.PUBLIC.COHORT_M037_LN_PREDICTORS AS
SELECT
  cp.RESEARCH_ID,
  cp.AGE_AT_SURGERY, cp.SEX, cp.RACE,
  cp.HISTOLOGY_FINAL,
  COALESCE(lu.HISTOLOGY_GROUP, 'Other') AS HISTOLOGY_GROUP,
  cp.TUMOR_SIZE_CM_MAX, cp.ETE_GRADE,
  cp.AJCC8_T_STAGE, cp.AJCC8_N_STAGE, cp.AJCC8_M_STAGE, cp.AJCC8_STAGE_GROUP,
  cp.LN_TOTAL_EXAMINED, cp.LN_TOTAL_POSITIVE, cp.LN_POSITIVE_FLAG,
  CASE WHEN cp.LN_POSITIVE_FLAG = 1 OR cp.LN_TOTAL_POSITIVE > 0 THEN TRUE
       ELSE FALSE
  END AS LN_POSITIVE,
  cp.SURG_PROCEDURE_TYPE,
  cp.MOLECULAR_TESTED_CONFIRMED, cp.BRAF_POSITIVE_FINAL, cp.RAS_POSITIVE_FINAL,
  cp.ANY_RECURRENCE_FLAG, cp.OVERALL_SURVIVAL_YEARS,
  cp.FIRST_SURGERY_DATE
FROM THYROID_VALIDATION.PUBLIC.CANONICAL_PATIENT_MASTER_FLAT cp
LEFT JOIN THYROID_VALIDATION.PUBLIC.CANONICAL_HISTOLOGY_LOOKUP_V1 lu
  ON cp.HISTOLOGY_FINAL = lu.HISTOLOGY_FINAL_RAW
WHERE cp.IS_MALIGNANT = TRUE AND cp.HISTOLOGY_FINAL IS NOT NULL
"""
cur.execute(m037_sql)
cur.execute("SELECT COUNT(*), COUNT_IF(LN_POSITIVE) FROM COHORT_M037_LN_PREDICTORS")
n, nlnp = cur.fetchone()
print(f"  COHORT_M037_LN_PREDICTORS: n={n:,}  LN+={nlnp:,}  ({100.0*nlnp/n:.1f}%)")

# === M032: 25-year descriptive ===
print("\n=== M032: 25-year descriptive cohort ===")
m032_sql = """
CREATE OR REPLACE VIEW THYROID_VALIDATION.PUBLIC.COHORT_M032_25YR_DESCRIPTIVE AS
SELECT
  RESEARCH_ID,
  AGE_AT_SURGERY, SEX, RACE,
  HISTOLOGY_FINAL, IS_MALIGNANT, FIRST_SURGERY_DATE,
  EXTRACT(YEAR FROM FIRST_SURGERY_DATE) AS SURGERY_YEAR,
  CASE
    WHEN EXTRACT(YEAR FROM FIRST_SURGERY_DATE) BETWEEN 1999 AND 2005 THEN 'Early (1999-2005)'
    WHEN EXTRACT(YEAR FROM FIRST_SURGERY_DATE) BETWEEN 2006 AND 2013 THEN 'Middle (2006-2013)'
    WHEN EXTRACT(YEAR FROM FIRST_SURGERY_DATE) BETWEEN 2014 AND 2019 THEN 'Modern (2014-2019)'
    WHEN EXTRACT(YEAR FROM FIRST_SURGERY_DATE) >= 2020 THEN 'Contemporary (2020+)'
    ELSE 'Unknown'
  END AS ERA,
  TUMOR_SIZE_CM_MAX, AJCC8_STAGE_GROUP, ETE_GRADE,
  SURG_PROCEDURE_TYPE, RAI_RECEIVED_FLAG,
  MOLECULAR_TESTED_CONFIRMED, BRAF_POSITIVE_FINAL,
  ANY_RECURRENCE_FLAG, OVERALL_SURVIVAL_YEARS, FOLLOWUP_YEARS
FROM THYROID_VALIDATION.PUBLIC.CANONICAL_PATIENT_MASTER_FLAT
WHERE FIRST_SURGERY_DATE IS NOT NULL
"""
cur.execute(m032_sql)
cur.execute("SELECT COUNT(*) FROM COHORT_M032_25YR_DESCRIPTIVE")
n = cur.fetchone()[0]
print(f"  COHORT_M032_25YR_DESCRIPTIVE: n={n:,}")

# Summarize all views
print("\n=== Summary: all manuscript cohort views ===")
cur.execute("""
SELECT TABLE_NAME, ROW_COUNT
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_NAME LIKE 'COHORT_%'
ORDER BY TABLE_NAME
""")
for row in cur.fetchall():
    print(f"  {row}")

ctx.close()
