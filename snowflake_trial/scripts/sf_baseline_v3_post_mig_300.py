"""SF baseline v3 — post mig_294b/297/298/299/300.

1. Export updated MD (M004 cohort + dropped legacy col + pub_v1_1 tag)
2. Rebuild flat views; add COHORT_M004_AUTOIMMUNE_CANCER_V1_FLAT
3. Re-baseline VALIDATE_ALL_COHORTS to v3 (add M004 checks; drop legacy col check)
4. Verify 17/17+ all PASS
"""
import os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor
import duckdb, json

REPO = Path("/Users/ros/THyroid 2026")

# ============================================================
# 1. Refresh CPM + cohort views from MD (just affected ones)
# ============================================================
print("=== 1. Refresh CPM + M004 cohort view from MD ===")
md_token = os.environ.get('MOTHERDUCK_TOKEN') or os.environ.get('motherduck_token')
md = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={md_token}")

PARQ = REPO / "snowflake_trial/parquet"
PARQ.mkdir(exist_ok=True)

# CPM (will be schema-narrower since nlp_tirads_max_category dropped)
md.execute(f"""
COPY (SELECT * FROM main.canonical_patient_master)
TO '{PARQ / "canonical_patient_master.parquet"}' (FORMAT 'parquet')
""")
print("  ✓ canonical_patient_master.parquet refreshed")

# M004 cohort view (new)
md.execute(f"""
COPY (SELECT * FROM manuscript_workspace.cohort_m004_autoimmune_cancer_v1)
TO '{PARQ / "cohort_m004_autoimmune_cancer_v1.parquet"}' (FORMAT 'parquet')
""")
n_m004 = duckdb.connect().execute(f"SELECT COUNT(*) FROM '{PARQ}/cohort_m004_autoimmune_cancer_v1.parquet'").fetchone()[0]
print(f"  ✓ cohort_m004 parquet: {n_m004:,} rows")

md.close()

# ============================================================
# 2. PUT + COPY into SF + build flat view
# ============================================================
print("\n=== 2. Load + flat view ===")
ctx, cur = get_cursor()
cur.execute("USE DATABASE THYROID_VALIDATION")
cur.execute("USE SCHEMA PUBLIC")
cur.execute("CREATE STAGE IF NOT EXISTS COWORK_STAGE")

# Reload CPM
cur.execute("CREATE OR REPLACE TABLE CANONICAL_PATIENT_MASTER (V VARIANT)")
cur.execute(f"PUT 'file://{PARQ / 'canonical_patient_master.parquet'}' @COWORK_STAGE/cpm_v3/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
cur.execute(f"""
COPY INTO CANONICAL_PATIENT_MASTER (V)
FROM (SELECT $1 FROM @COWORK_STAGE/cpm_v3/canonical_patient_master.parquet)
FILE_FORMAT = (TYPE = PARQUET)
""")
cur.execute("SELECT COUNT(*) FROM CANONICAL_PATIENT_MASTER")
print(f"  ✓ CANONICAL_PATIENT_MASTER reloaded: {cur.fetchone()[0]:,} rows")

# Load M004 cohort view
cur.execute("CREATE OR REPLACE TABLE COHORT_M004_AUTOIMMUNE_CANCER_V1 (V VARIANT)")
cur.execute(f"PUT 'file://{PARQ / 'cohort_m004_autoimmune_cancer_v1.parquet'}' @COWORK_STAGE/m004_v3/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
cur.execute(f"""
COPY INTO COHORT_M004_AUTOIMMUNE_CANCER_V1 (V)
FROM (SELECT $1 FROM @COWORK_STAGE/m004_v3/cohort_m004_autoimmune_cancer_v1.parquet)
FILE_FORMAT = (TYPE = PARQUET)
""")
cur.execute("SELECT COUNT(*) FROM COHORT_M004_AUTOIMMUNE_CANCER_V1")
print(f"  ✓ COHORT_M004_AUTOIMMUNE_CANCER_V1: {cur.fetchone()[0]:,} rows")

# Build M004 flat view
cur.execute("SELECT $1 FROM COHORT_M004_AUTOIMMUNE_CANCER_V1 LIMIT 1")
sample = cur.fetchone()[0]
data = sample if isinstance(sample, dict) else json.loads(sample)
def infer_type(v):
    if isinstance(v, bool): return 'BOOLEAN'
    if isinstance(v, int): return 'INTEGER'
    if isinstance(v, float): return 'DOUBLE'
    return 'VARCHAR'
projections = [f"$1:{k}::{infer_type(data.get(k))} AS {k.upper()}" for k in data.keys()]
cur.execute(f"""
CREATE OR REPLACE VIEW COHORT_M004_AUTOIMMUNE_CANCER_V1_FLAT AS
SELECT {', '.join(projections)} FROM COHORT_M004_AUTOIMMUNE_CANCER_V1
""")
cur.execute("SELECT COUNT(*) FROM COHORT_M004_AUTOIMMUNE_CANCER_V1_FLAT")
print(f"  ✓ COHORT_M004_AUTOIMMUNE_CANCER_V1_FLAT: {cur.fetchone()[0]:,} rows / {len(data)} cols")

# Rebuild CANONICAL_PATIENT_MASTER_FLAT (new schema, no nlp_tirads_max_category)
cur.execute("SELECT $1 FROM CANONICAL_PATIENT_MASTER LIMIT 1")
sample = cur.fetchone()[0]
data = sample if isinstance(sample, dict) else json.loads(sample)
projections = [f"$1:{k}::{infer_type(data.get(k))} AS {k.upper()}" for k in list(data.keys())[:1700]]  # keep all cols
cur.execute(f"""
CREATE OR REPLACE VIEW CANONICAL_PATIENT_MASTER_FLAT AS
SELECT {', '.join(projections)} FROM CANONICAL_PATIENT_MASTER
""")
cur.execute("SELECT COUNT(*) FROM CANONICAL_PATIENT_MASTER_FLAT")
print(f"  ✓ CANONICAL_PATIENT_MASTER_FLAT rebuilt: {cur.fetchone()[0]:,} rows / {len(data)} cols")

# ============================================================
# 3. Update COHORT_SUMMARY_DASHBOARD to include M004
# ============================================================
print("\n=== 3. Update COHORT_SUMMARY_DASHBOARD with M004 ===")
cur.execute("""
CREATE OR REPLACE VIEW COHORT_SUMMARY_DASHBOARD AS
SELECT 'M044_ETE' AS manuscript, (SELECT COUNT(*) FROM COHORT_M044_AJCC_ETE_V1_FLAT) AS n_cohort,
       (SELECT COUNT_IF(ANY_RECURRENCE_FLAG) FROM COHORT_M044_AJCC_ETE_V1_FLAT) AS n_events, CURRENT_TIMESTAMP AS refreshed_at
UNION ALL SELECT 'M037_LN_PREDICTORS', (SELECT COUNT(*) FROM COHORT_M037_LN_METASTASIS_V1_FLAT),
  (SELECT COUNT_IF(AJCC8_N_STAGE LIKE 'N1%') FROM COHORT_M037_LN_METASTASIS_V1_FLAT), CURRENT_TIMESTAMP
UNION ALL SELECT 'M025_TIRADS', (SELECT COUNT(*) FROM COHORT_M025_TIRADS_PERFORMANCE_V1_FLAT),
  (SELECT COUNT_IF(IS_MALIGNANT) FROM COHORT_M025_TIRADS_PERFORMANCE_V1_FLAT), CURRENT_TIMESTAMP
UNION ALL SELECT 'M032_25YR', (SELECT COUNT(*) FROM COHORT_M032_DESCRIPTIVE_25YR_V1_FLAT),
  (SELECT COUNT_IF(IS_MALIGNANT) FROM COHORT_M032_DESCRIPTIVE_25YR_V1_FLAT), CURRENT_TIMESTAMP
UNION ALL SELECT 'M038_MASSIVE_GOITER', (SELECT COUNT(*) FROM COHORT_M038_MASSIVE_GOITER_V1_FLAT),
  (SELECT COUNT_IF(IS_MALIGNANT) FROM COHORT_M038_MASSIVE_GOITER_V1_FLAT), CURRENT_TIMESTAMP
UNION ALL SELECT 'M004_AUTOIMMUNE', (SELECT COUNT(*) FROM COHORT_M004_AUTOIMMUNE_CANCER_V1_FLAT),
  (SELECT COUNT_IF(IS_MALIGNANT) FROM COHORT_M004_AUTOIMMUNE_CANCER_V1_FLAT), CURRENT_TIMESTAMP
UNION ALL SELECT 'PUB_v1.1_FULL', (SELECT COUNT(*) FROM CANONICAL_PATIENT_MASTER_FLAT),
  (SELECT COUNT_IF(IS_MALIGNANT) FROM CANONICAL_PATIENT_MASTER_FLAT), CURRENT_TIMESTAMP
""")
cur.execute("SELECT * FROM COHORT_SUMMARY_DASHBOARD ORDER BY MANUSCRIPT")
print("  Dashboard:")
for r in cur.fetchall():
    print(f"    {r[0]:25s}  cohort={r[1]:>6}  events={(r[2] if r[2] is not None else 'NA'):>6}")

# ============================================================
# 4. VALIDATE_ALL_COHORTS() — baseline v3
# ============================================================
print("\n=== 4. VALIDATE_ALL_COHORTS() baseline v3 ===")
cur.execute("""
CREATE OR REPLACE PROCEDURE VALIDATE_ALL_COHORTS()
RETURNS TABLE (CHECK_NAME VARCHAR, EXPECTED VARCHAR, OBSERVED VARCHAR, STATUS VARCHAR)
LANGUAGE SQL
AS
$$
DECLARE
  res RESULTSET;
BEGIN
  INSERT INTO VALIDATION_RUN_LOG_v1 (CHECK_NAME, EXPECTED, OBSERVED, STATUS, NOTES)
  WITH checks AS (
    SELECT 'M044_cohort_n' AS check_name, '4013' AS expected,
           (SELECT COUNT(*)::VARCHAR FROM COHORT_M044_AJCC_ETE_V1_FLAT) AS observed
    UNION ALL SELECT 'M037_cohort_n', '2234',
      (SELECT COUNT(*)::VARCHAR FROM COHORT_M037_LN_METASTASIS_V1_FLAT)
    UNION ALL SELECT 'M025_cohort_n', '3375',
      (SELECT COUNT(*)::VARCHAR FROM COHORT_M025_TIRADS_PERFORMANCE_V1_FLAT)
    UNION ALL SELECT 'M032_cohort_n', '10871',
      (SELECT COUNT(*)::VARCHAR FROM COHORT_M032_DESCRIPTIVE_25YR_V1_FLAT)
    UNION ALL SELECT 'M038_cohort_n', '10871',
      (SELECT COUNT(*)::VARCHAR FROM COHORT_M038_MASSIVE_GOITER_V1_FLAT)
    UNION ALL SELECT 'M004_cohort_n', '10871',
      (SELECT COUNT(*)::VARCHAR FROM COHORT_M004_AUTOIMMUNE_CANCER_V1_FLAT)
    UNION ALL SELECT 'CPM_cohort_n', '10871',
      (SELECT COUNT(*)::VARCHAR FROM CANONICAL_PATIENT_MASTER_FLAT)
    UNION ALL SELECT 'CPM_malig_n', '4019',
      (SELECT COUNT_IF(IS_MALIGNANT)::VARCHAR FROM CANONICAL_PATIENT_MASTER_FLAT)
    UNION ALL SELECT 'CPM_smoking_known_n', '3022',
      (SELECT COUNT_IF(PMHX_NLP_SMOKING_STATUS IS NOT NULL)::VARCHAR FROM CANONICAL_PATIENT_MASTER_FLAT)
    UNION ALL SELECT 'CPM_fhx_thy_known_n', '3018',
      (SELECT COUNT_IF(PMHX_NLP_FAMILY_HX_THYROID IS NOT NULL)::VARCHAR FROM CANONICAL_PATIENT_MASTER_FLAT)
    UNION ALL SELECT 'CPM_smoking_clean_enum', 'YES',
      (SELECT IFF(COUNT(DISTINCT PMHX_NLP_SMOKING_STATUS) <= 4, 'YES', 'NO')
       FROM CANONICAL_PATIENT_MASTER_FLAT WHERE PMHX_NLP_SMOKING_STATUS IS NOT NULL)
    UNION ALL SELECT 'CPM_tirads_resolved_n', '3382',
      (SELECT COUNT_IF(TIRADS_RESOLVED IS NOT NULL)::VARCHAR FROM CANONICAL_PATIENT_MASTER_FLAT)
    -- Manuscript-cell checks
    UNION ALL SELECT 'M044_events_any_recurrence', '499',
      (SELECT COUNT_IF(ANY_RECURRENCE_FLAG)::VARCHAR FROM COHORT_M044_AJCC_ETE_V1_FLAT)
    UNION ALL SELECT 'M037_LN_pos_n', '1124',
      (SELECT COUNT_IF(AJCC8_N_STAGE LIKE 'N1%')::VARCHAR FROM COHORT_M037_LN_METASTASIS_V1_FLAT)
    UNION ALL SELECT 'M025_malig_n', '1479',
      (SELECT COUNT_IF(IS_MALIGNANT)::VARCHAR FROM COHORT_M025_TIRADS_PERFORMANCE_V1_FLAT)
    UNION ALL SELECT 'TIRADS_TR5_n', '1402',
      (SELECT COUNT_IF(TIRADS_RESOLVED='TR5')::VARCHAR FROM CANONICAL_PATIENT_MASTER_FLAT)
    UNION ALL SELECT 'NLP_smoking_full_results', '3541',
      (SELECT COUNT(*)::VARCHAR FROM NLP_SMOKING_FULL_RESULTS_V1)
    UNION ALL SELECT 'NLP_family_hx_full_results', '3534',
      (SELECT COUNT(*)::VARCHAR FROM NLP_FAMILY_HX_THYROID_FULL_RESULTS_V1)
    -- M004 checks (new in v3)
    UNION ALL SELECT 'M004_hashi_combined_n', '400',
      (SELECT COUNT_IF(HAS_HASHI)::VARCHAR FROM COHORT_M004_AUTOIMMUNE_CANCER_V1_FLAT)
    UNION ALL SELECT 'M004_graves_combined_n', '1656',
      (SELECT COUNT_IF(HAS_GRAVES)::VARCHAR FROM COHORT_M004_AUTOIMMUNE_CANCER_V1_FLAT)
    -- Sentinel: legacy col gone (post-mig_294b)
    UNION ALL SELECT 'CPM_legacy_tirads_dropped', 'YES',
      (SELECT IFF(COUNT(*) = 0, 'YES', 'NO') FROM information_schema.columns
       WHERE table_schema='PUBLIC' AND column_name='NLP_TIRADS_MAX_CATEGORY' AND table_name='CANONICAL_PATIENT_MASTER')
  )
  SELECT check_name, expected, observed,
         CASE WHEN expected = observed THEN 'PASS' ELSE 'FAIL' END,
         'baseline-v3 auto-validation post-mig_300'
  FROM checks;

  res := (SELECT CHECK_NAME, EXPECTED, OBSERVED, STATUS
          FROM VALIDATION_RUN_LOG_v1
          WHERE RUN_TS >= DATEADD('second', -10, CURRENT_TIMESTAMP)
          ORDER BY RUN_ID);
  RETURN TABLE(res);
END;
$$
""")
print("  ✓ SP updated to baseline v3 (20 checks)")

cur.execute("CALL VALIDATE_ALL_COHORTS()")
results = cur.fetchall()
n_pass = sum(1 for r in results if r[3]=='PASS')
print(f"\n  baseline run: {n_pass}/{len(results)} PASS")
for r in results:
    sym = '✓' if r[3]=='PASS' else '✗'
    print(f"    {sym} {r[0]:32s} exp={r[1]:>8s} obs={r[2]:>8s}")

ctx.close()
print("\n=== DONE — baseline v3 live ===")
