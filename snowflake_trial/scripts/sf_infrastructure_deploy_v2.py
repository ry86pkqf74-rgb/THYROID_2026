"""SF infrastructure deploy v2 — flat views + validation SP + Cortex Search.

Cortex Analyst semantic model already staged in v1; only re-stage if YAML changed.
This v2 builds proper FLAT views with explicit projection (cohort tables came
over as VARIANT $1 from mig_289 — same pattern as canonical_us_patient_master_VIEW_v2).
"""
import os, sys, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor

REPO = Path("/Users/ros/THyroid 2026")
ctx, cur = get_cursor()
cur.execute("USE DATABASE THYROID_VALIDATION")
cur.execute("USE SCHEMA PUBLIC")

# ============================================================
# 1. Build proper FLAT views with explicit projection
# ============================================================
print("=== 1. Building FLAT views from VARIANT $1 cohort tables ===")

cohort_views_to_flatten = [
    'COHORT_M037_LN_METASTASIS_V1',
    'COHORT_M025_TIRADS_PERFORMANCE_V1',
    'COHORT_M032_DESCRIPTIVE_25YR_V1',
    'COHORT_M044_AJCC_ETE_V1',
    'COHORT_M038_MASSIVE_GOITER_V1',
]

for tbl in cohort_views_to_flatten:
    # Sample one row to get col names + infer types
    cur.execute(f"SELECT $1 FROM {tbl} LIMIT 1")
    r = cur.fetchone()
    data = r[0] if isinstance(r[0], dict) else json.loads(r[0])
    keys = list(data.keys())

    def infer_type(v):
        if isinstance(v, bool): return 'BOOLEAN'
        if isinstance(v, int): return 'INTEGER'
        if isinstance(v, float): return 'DOUBLE'
        if v is None: return 'VARCHAR'
        return 'VARCHAR'

    projections = []
    for k in keys:
        sf_type = infer_type(data.get(k))
        projections.append(f"$1:{k}::{sf_type} AS {k.upper()}")

    flat_name = f"{tbl}_FLAT"
    sql = f"CREATE OR REPLACE VIEW {flat_name} AS\nSELECT\n  " + ",\n  ".join(projections) + f"\nFROM {tbl}"
    try:
        cur.execute(sql)
        cur.execute(f"SELECT COUNT(*) FROM {flat_name}")
        n = cur.fetchone()[0]
        print(f"  ✓ {flat_name:50s} {n:>6,} rows / {len(keys)} cols")
    except Exception as e:
        print(f"  ✗ {flat_name}: {e}")

# ============================================================
# 2. COHORT_SUMMARY_DASHBOARD view (rebuild with flat refs)
# ============================================================
print("\n=== 2. COHORT_SUMMARY_DASHBOARD ===")
try:
    cur.execute("""
CREATE OR REPLACE VIEW COHORT_SUMMARY_DASHBOARD AS
SELECT 'M044_ETE' AS manuscript,
  (SELECT COUNT(*) FROM COHORT_M044_AJCC_ETE_V1_FLAT) AS n_cohort,
  (SELECT COUNT_IF(ANY_RECURRENCE_FLAG) FROM COHORT_M044_AJCC_ETE_V1_FLAT) AS n_events,
  CURRENT_TIMESTAMP AS refreshed_at
UNION ALL SELECT 'M037_LN_PREDICTORS',
  (SELECT COUNT(*) FROM COHORT_M037_LN_METASTASIS_V1_FLAT),
  (SELECT COUNT_IF(AJCC8_N_STAGE LIKE 'N1%') FROM COHORT_M037_LN_METASTASIS_V1_FLAT),
  CURRENT_TIMESTAMP
UNION ALL SELECT 'M025_TIRADS',
  (SELECT COUNT(*) FROM COHORT_M025_TIRADS_PERFORMANCE_V1_FLAT),
  (SELECT COUNT_IF(IS_MALIGNANT) FROM COHORT_M025_TIRADS_PERFORMANCE_V1_FLAT),
  CURRENT_TIMESTAMP
UNION ALL SELECT 'M032_25YR',
  (SELECT COUNT(*) FROM COHORT_M032_DESCRIPTIVE_25YR_V1_FLAT),
  (SELECT COUNT_IF(IS_MALIGNANT) FROM COHORT_M032_DESCRIPTIVE_25YR_V1_FLAT),
  CURRENT_TIMESTAMP
UNION ALL SELECT 'M038_MASSIVE_GOITER',
  (SELECT COUNT(*) FROM COHORT_M038_MASSIVE_GOITER_V1_FLAT),
  (SELECT COUNT_IF(IS_MALIGNANT) FROM COHORT_M038_MASSIVE_GOITER_V1_FLAT),
  CURRENT_TIMESTAMP
UNION ALL SELECT 'PUB_v1.0_FULL',
  (SELECT COUNT(*) FROM CANONICAL_PATIENT_MASTER_FLAT),
  (SELECT COUNT_IF(IS_MALIGNANT) FROM CANONICAL_PATIENT_MASTER_FLAT),
  CURRENT_TIMESTAMP
""")
    print("  ✓ created")
    cur.execute("SELECT * FROM COHORT_SUMMARY_DASHBOARD")
    for r in cur.fetchall():
        print(f"    {r[0]:25s}  n_cohort={r[1]:>6}  n_events={(r[2] if r[2] is not None else 'NA'):>6}")
except Exception as e:
    print(f"  ✗ {e}")

# ============================================================
# 3. VALIDATION_RUN_LOG_v1 + VALIDATE_ALL_COHORTS() SP
# ============================================================
print("\n=== 3. Validation SP ===")
cur.execute("DROP TABLE IF EXISTS VALIDATION_RUN_LOG_v1")
cur.execute("""
CREATE TABLE VALIDATION_RUN_LOG_v1 (
  RUN_ID NUMBER AUTOINCREMENT,
  RUN_TS TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
  CHECK_NAME VARCHAR,
  EXPECTED VARCHAR,
  OBSERVED VARCHAR,
  STATUS VARCHAR,
  NOTES VARCHAR,
  PRIMARY KEY (RUN_ID)
)
""")
print("  ✓ VALIDATION_RUN_LOG_v1 ready")

cur.execute("""
CREATE OR REPLACE PROCEDURE VALIDATE_ALL_COHORTS()
RETURNS TABLE (CHECK_NAME VARCHAR, EXPECTED VARCHAR, OBSERVED VARCHAR, STATUS VARCHAR)
LANGUAGE SQL
AS
$$
DECLARE
  this_run_id NUMBER;
  res RESULTSET;
BEGIN
  -- Insert all checks into log
  INSERT INTO VALIDATION_RUN_LOG_v1 (CHECK_NAME, EXPECTED, OBSERVED, STATUS, NOTES)
  WITH checks AS (
    SELECT 'M044_cohort_n' AS check_name, '4012' AS expected,
           (SELECT COUNT(*)::VARCHAR FROM COHORT_M044_AJCC_ETE_V1_FLAT) AS observed
    UNION ALL SELECT 'M037_cohort_n', '2233',
      (SELECT COUNT(*)::VARCHAR FROM COHORT_M037_LN_METASTASIS_V1_FLAT)
    UNION ALL SELECT 'M025_cohort_n', '3375',
      (SELECT COUNT(*)::VARCHAR FROM COHORT_M025_TIRADS_PERFORMANCE_V1_FLAT)
    UNION ALL SELECT 'M032_cohort_n', '10871',
      (SELECT COUNT(*)::VARCHAR FROM COHORT_M032_DESCRIPTIVE_25YR_V1_FLAT)
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
  )
  SELECT check_name, expected, observed,
         CASE WHEN expected = observed THEN 'PASS' ELSE 'FAIL' END,
         'auto-validation run'
  FROM checks;

  -- Return latest run results
  res := (
    SELECT CHECK_NAME, EXPECTED, OBSERVED, STATUS
    FROM VALIDATION_RUN_LOG_v1
    WHERE RUN_TS >= DATEADD('second', -10, CURRENT_TIMESTAMP)
    ORDER BY RUN_ID
  );
  RETURN TABLE(res);
END;
$$
""")
print("  ✓ VALIDATE_ALL_COHORTS() created")

cur.execute("CALL VALIDATE_ALL_COHORTS()")
results = cur.fetchall()
print(f"\n  Test run: {len(results)} checks")
n_pass = sum(1 for r in results if r[3]=='PASS')
print(f"  {n_pass}/{len(results)} PASS")
for r in results:
    sym = '✓' if r[3]=='PASS' else '✗'
    print(f"    {sym} {r[0]:30s} expected={r[1]:>8s}  observed={r[2]:>8s}")

# ============================================================
# 4. Cortex Search service
# ============================================================
print("\n=== 4. Cortex Search service ===")

# Load 1K notes if not already there
cur.execute("SHOW TABLES LIKE 'CLINICAL_NOTES_SEARCH_V1' IN THYROID_VALIDATION.PUBLIC")
exists = cur.fetchall()

if not exists:
    print("  Loading 1,000 notes for search...")
    import duckdb
    md_token = os.environ.get('MOTHERDUCK_TOKEN') or os.environ.get('motherduck_token')
    md = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={md_token}")
    parq = REPO / "snowflake_trial/parquet/_clinical_notes_for_search.parquet"
    md.execute(f"""
COPY (SELECT research_id, note_type,
             CAST(note_index AS INTEGER) AS note_index,
             SUBSTR(note_text, 1, 4000) AS note_text
      FROM main.clinical_notes_long ORDER BY RANDOM() LIMIT 1000)
TO '{parq}' (FORMAT 'parquet')
""")
    md.close()
    cur.execute("CREATE OR REPLACE TABLE CLINICAL_NOTES_SEARCH_V1 (RESEARCH_ID VARCHAR, NOTE_TYPE VARCHAR, NOTE_INDEX INTEGER, NOTE_TEXT VARCHAR)")
    cur.execute("CREATE STAGE IF NOT EXISTS COWORK_STAGE")
    cur.execute(f"PUT 'file://{parq}' @COWORK_STAGE/notes_search/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
    cur.execute("""
COPY INTO CLINICAL_NOTES_SEARCH_V1
FROM (SELECT $1:research_id::VARCHAR, $1:note_type::VARCHAR,
             $1:note_index::INTEGER, $1:note_text::VARCHAR
      FROM @COWORK_STAGE/notes_search/_clinical_notes_for_search.parquet)
FILE_FORMAT = (TYPE = PARQUET)
""")
    cur.execute("SELECT COUNT(*) FROM CLINICAL_NOTES_SEARCH_V1")
    print(f"  ✓ Loaded {cur.fetchone()[0]} notes")
else:
    print("  ✓ CLINICAL_NOTES_SEARCH_V1 already loaded")

try:
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
    print("  ✓ THYROID_NOTES_SEARCH Cortex Search service created")
except Exception as e:
    print(f"  ⚠ Cortex Search error: {e}")
    print("  → Service may need account-level enablement; trial may not include Cortex Search")

# ============================================================
# 5. Pipeline registry (manifest of deployed components)
# ============================================================
print("\n=== 5. Pipeline registry ===")
cur.execute("""
CREATE OR REPLACE TABLE COWORK_PIPELINE_REGISTRY_V1 (
  COMPONENT VARCHAR, KIND VARCHAR, STATUS VARCHAR, PURPOSE VARCHAR, DEPLOYED_AT TIMESTAMP_LTZ
)
""")
components = [
    ('CANONICAL_PATIENT_MASTER_FLAT', 'view', 'ACTIVE', 'CPM flattened from VARIANT $1'),
    ('COHORT_M044_AJCC_ETE_V1_FLAT', 'view', 'ACTIVE', 'M044 ETE manuscript cohort'),
    ('COHORT_M037_LN_METASTASIS_V1_FLAT', 'view', 'ACTIVE', 'M037 LN predictors cohort'),
    ('COHORT_M025_TIRADS_PERFORMANCE_V1_FLAT', 'view', 'ACTIVE', 'M025 TIRADS cohort'),
    ('COHORT_M032_DESCRIPTIVE_25YR_V1_FLAT', 'view', 'ACTIVE', 'M032 25-yr descriptive cohort'),
    ('COHORT_M038_MASSIVE_GOITER_V1_FLAT', 'view', 'ACTIVE', 'M038 massive goiter cohort'),
    ('NLP_SMOKING_FULL_RESULTS_V1', 'table', 'ACTIVE', 'AI_CLASSIFY smoking (full corpus)'),
    ('NLP_FAMILY_HX_THYROID_FULL_RESULTS_V1', 'table', 'ACTIVE', 'AI_CLASSIFY family-hx (full corpus)'),
    ('NLP_VASC_INVASION_FULL_RESULTS_V1', 'table', 'ACTIVE', 'AI_CLASSIFY vasc invasion (full corpus)'),
    ('VALIDATION_RUN_LOG_v1', 'table', 'ACTIVE', 'Audit log for VALIDATE_ALL_COHORTS() runs'),
    ('VALIDATE_ALL_COHORTS()', 'procedure', 'ACTIVE', 'Repeatable manuscript denominator audit'),
    ('COHORT_SUMMARY_DASHBOARD', 'view', 'ACTIVE', 'Cross-manuscript cohort sizes at a glance'),
    ('CLINICAL_NOTES_SEARCH_V1', 'table', 'ACTIVE', 'Sampled notes for Cortex Search'),
    ('THYROID_NOTES_SEARCH', 'cortex_search', 'STAGED-OR-ACTIVE', 'Semantic search over notes'),
    ('@SEMANTIC_MODELS/thyroid_2026_semantic_model.yaml', 'cortex_analyst', 'STAGED', 'Bind via Snowsight UI'),
]
for c in components:
    cur.execute(f"INSERT INTO COWORK_PIPELINE_REGISTRY_V1 VALUES ('{c[0]}', '{c[1]}', '{c[2]}', '{c[3]}', CURRENT_TIMESTAMP)")
print(f"  ✓ Registered {len(components)} components")

ctx.close()
print("\n=== DEPLOYMENT COMPLETE ===")
