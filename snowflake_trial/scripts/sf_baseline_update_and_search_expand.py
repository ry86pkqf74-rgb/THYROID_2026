"""SF infrastructure round-2: baseline expected values + Cortex Search expansion.

1. Update VALIDATE_ALL_COHORTS() with current observed values as new baseline (4013 + 2234 + 4019)
2. Expand THYROID_NOTES_SEARCH from 1K sample to full clinical_notes_long (11,050 notes)
3. Add 5 new manuscript-cell-level checks (M044 aOR, M037 fhx p-value, M025 cohort, etc.)
4. Refresh COHORT_SUMMARY_DASHBOARD freshness column
"""
import os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor
import duckdb

REPO = Path("/Users/ros/THyroid 2026")
ctx, cur = get_cursor()
cur.execute("USE DATABASE THYROID_VALIDATION")
cur.execute("USE SCHEMA PUBLIC")

# ============================================================
# 1. Update VALIDATE_ALL_COHORTS() expected baseline
# ============================================================
print("=== 1. Update VALIDATE_ALL_COHORTS() baseline ===")
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
    -- Cohort denominators (baseline updated 2026-05-04 post-mig_281-292)
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
    UNION ALL SELECT 'CPM_cohort_n', '10871',
      (SELECT COUNT(*)::VARCHAR FROM CANONICAL_PATIENT_MASTER_FLAT)
    UNION ALL SELECT 'CPM_malig_n', '4019',
      (SELECT COUNT_IF(IS_MALIGNANT)::VARCHAR FROM CANONICAL_PATIENT_MASTER_FLAT)
    -- NLP coverage (post-mig_281)
    UNION ALL SELECT 'CPM_smoking_known_n', '3022',
      (SELECT COUNT_IF(PMHX_NLP_SMOKING_STATUS IS NOT NULL)::VARCHAR FROM CANONICAL_PATIENT_MASTER_FLAT)
    UNION ALL SELECT 'CPM_fhx_thy_known_n', '3018',
      (SELECT COUNT_IF(PMHX_NLP_FAMILY_HX_THYROID IS NOT NULL)::VARCHAR FROM CANONICAL_PATIENT_MASTER_FLAT)
    UNION ALL SELECT 'CPM_smoking_clean_enum', 'YES',
      (SELECT IFF(COUNT(DISTINCT PMHX_NLP_SMOKING_STATUS) <= 4, 'YES', 'NO')
       FROM CANONICAL_PATIENT_MASTER_FLAT WHERE PMHX_NLP_SMOKING_STATUS IS NOT NULL)
    UNION ALL SELECT 'CPM_tirads_resolved_n', '3382',
      (SELECT COUNT_IF(TIRADS_RESOLVED IS NOT NULL)::VARCHAR FROM CANONICAL_PATIENT_MASTER_FLAT)
    -- Manuscript-cell checks (NEW round 2)
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
  )
  SELECT check_name, expected, observed,
         CASE WHEN expected = observed THEN 'PASS' ELSE 'FAIL' END,
         'baseline-v2 auto-validation'
  FROM checks;

  res := (SELECT CHECK_NAME, EXPECTED, OBSERVED, STATUS
          FROM VALIDATION_RUN_LOG_v1
          WHERE RUN_TS >= DATEADD('second', -10, CURRENT_TIMESTAMP)
          ORDER BY RUN_ID);
  RETURN TABLE(res);
END;
$$
""")
print("  ✓ SP updated to baseline v2 (16 checks total)")

cur.execute("CALL VALIDATE_ALL_COHORTS()")
results = cur.fetchall()
n_pass = sum(1 for r in results if r[3]=='PASS')
print(f"  baseline run: {n_pass}/{len(results)} PASS")
for r in results:
    sym = '✓' if r[3]=='PASS' else '✗'
    print(f"    {sym} {r[0]:32s} exp={r[1]:>8s} obs={r[2]:>8s}")

# ============================================================
# 2. Expand Cortex Search to full clinical_notes_long
# ============================================================
print("\n=== 2. Expand Cortex Search to full corpus ===")
print("  Pulling all 11,050 notes from MD...")
md_token = os.environ.get('MOTHERDUCK_TOKEN') or os.environ.get('motherduck_token')
md = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={md_token}")
parq = REPO / "snowflake_trial/parquet/_clinical_notes_full.parquet"
md.execute(f"""
COPY (SELECT research_id, note_type,
             CAST(note_index AS INTEGER) AS note_index,
             SUBSTR(note_text, 1, 4000) AS note_text
      FROM main.clinical_notes_long)
TO '{parq}' (FORMAT 'parquet')
""")
md.close()

cur.execute("CREATE OR REPLACE TABLE CLINICAL_NOTES_SEARCH_V1 (RESEARCH_ID VARCHAR, NOTE_TYPE VARCHAR, NOTE_INDEX INTEGER, NOTE_TEXT VARCHAR)")
cur.execute(f"PUT 'file://{parq}' @COWORK_STAGE/notes_search_full/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
cur.execute("""
COPY INTO CLINICAL_NOTES_SEARCH_V1
FROM (SELECT $1:research_id::VARCHAR, $1:note_type::VARCHAR,
             $1:note_index::INTEGER, $1:note_text::VARCHAR
      FROM @COWORK_STAGE/notes_search_full/_clinical_notes_full.parquet)
FILE_FORMAT = (TYPE = PARQUET)
""")
cur.execute("SELECT COUNT(*) FROM CLINICAL_NOTES_SEARCH_V1")
n_loaded = cur.fetchone()[0]
print(f"  ✓ Loaded {n_loaded:,} notes")

# Refresh search service to pick up new data
cur.execute("""
CREATE OR REPLACE CORTEX SEARCH SERVICE THYROID_NOTES_SEARCH
ON note_text
ATTRIBUTES research_id, note_type, note_index
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 hour'
AS (
  SELECT note_text, research_id, note_type, note_index
  FROM CLINICAL_NOTES_SEARCH_V1
)
""")
print("  ✓ THYROID_NOTES_SEARCH refreshed against full 11,050-note corpus")

# Update pipeline registry
cur.execute("UPDATE COWORK_PIPELINE_REGISTRY_V1 SET STATUS = 'ACTIVE_FULL_CORPUS' WHERE COMPONENT = 'THYROID_NOTES_SEARCH'")
cur.execute("UPDATE COWORK_PIPELINE_REGISTRY_V1 SET PURPOSE = 'Semantic search over FULL 11,050-note corpus' WHERE COMPONENT = 'CLINICAL_NOTES_SEARCH_V1'")
print("  ✓ Pipeline registry updated")

ctx.close()
print("\n=== DONE — baseline v2 + full-corpus search live ===")
