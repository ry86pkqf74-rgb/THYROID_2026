"""
Build flat views over the VARIANT $1 canonical tables.

For each loaded table, INFER_SCHEMA tells us the Parquet field names + types;
we generate a CREATE VIEW that projects $1:<col>::<TYPE> AS <col> for each.

Run from /Users/ros/THyroid 2026/ with .venv activated:
    SNOWFLAKE_PAT='...' python snowflake_trial/scripts/04_build_flat_views.py
"""
import os, json, time
import snowflake.connector
import snowflake.connector.network as _net

PAT = os.environ["SNOWFLAKE_PAT"]
DOTTED = "qcc02515.us-east-1"

_orig = _net.SnowflakeRestful._post_request
def _patched(self, url, headers, body, *args, **kwargs):
    if "/session/v1/login-request" in url:
        try:
            d = json.loads(body) if isinstance(body, str) else json.loads(body.decode())
            d["data"]["ACCOUNT_NAME"] = DOTTED
            if not d["data"].get("TOKEN"):
                d["data"]["TOKEN"] = PAT
            body = json.dumps(d)
        except Exception:
            pass
    return _orig(self, url, headers, body, *args, **kwargs)
_net.SnowflakeRestful._post_request = _patched

ctx = snowflake.connector.connect(
    account="qcc02515", host=f"{DOTTED}.snowflakecomputing.com",
    user="LGLOSSE13", password=PAT,
    authenticator="PROGRAMMATIC_ACCESS_TOKEN",
    warehouse="COMPUTE_WH", database="THYROID_VALIDATION",
    schema="PUBLIC", role="ACCOUNTADMIN")
cur = ctx.cursor()

TABLES = [
    "CANONICAL_PATIENT_MASTER",
    "CANONICAL_FNA_EVENTS_V1",
    "CANONICAL_INVASION_EVENTS_V1",
    "CANONICAL_LABS_THYROGLOBULIN_V1",
    "CANONICAL_MOLECULAR_GENETICS_V2",
    "CANONICAL_OPERATIVE_EVENTS_V1",
    "CANONICAL_PATH_GLAND_EVENTS_V1",
    "CANONICAL_PATH_MALIGNANT_EVENTS_V1",
    "CANONICAL_COMPLICATIONS_EVENTS_V1",
    # mig_260: flat TIRADS rollup for Prompt 7 / imaging validators (post-CPM TIRADS prune)
    "CANONICAL_US_PATIENT_MASTER_VIEW_V2",
]

# Map Parquet types -> Snowflake cast targets
def cast_for(p_type: str) -> str:
    p = p_type.upper()
    if "INT" in p or "BIGINT" in p:
        return "INT"
    if "DOUBLE" in p or "FLOAT" in p or "DECIMAL" in p or "NUMBER" in p:
        return "DOUBLE"
    if p == "BOOLEAN" or "BOOL" in p:
        return "BOOLEAN"
    if "TIMESTAMP" in p:
        return "TIMESTAMP"
    if "DATE" in p:
        return "DATE"
    if "TIME" in p:
        return "TIME"
    return "VARCHAR"  # default

for table in TABLES:
    print(f"\n=== {table} ===")
    parquet_name = table.lower() + ".parquet"
    cur.execute(f"""
SELECT COLUMN_NAME, TYPE
FROM TABLE(INFER_SCHEMA(
  LOCATION => '@THYROID_VALIDATION.PUBLIC.thyroid_stage/{parquet_name}',
  FILE_FORMAT => 'THYROID_VALIDATION.PUBLIC.parquet_fmt'
)) ORDER BY ORDER_ID
    """)
    schema = cur.fetchall()
    print(f"  {len(schema)} columns")

    projections = []
    for col_name, col_type in schema:
        cast = cast_for(col_type)
        # quote column names with special chars or that are reserved
        safe = f'"{col_name}"' if not col_name.isidentifier() or col_name.lower() in {"order"} else col_name
        projections.append(f'$1:"{col_name}"::{cast} AS {safe}')

    view_name = table + "_FLAT"
    proj_sql = ",\n  ".join(projections)
    create_view_sql = f"""
CREATE OR REPLACE VIEW THYROID_VALIDATION.PUBLIC.{view_name} AS
SELECT
  {proj_sql}
FROM THYROID_VALIDATION.PUBLIC.{table}
""".strip()

    t0 = time.time()
    try:
        cur.execute(create_view_sql)
        cur.execute(f"SELECT COUNT(*) FROM THYROID_VALIDATION.PUBLIC.{view_name}")
        n = cur.fetchone()[0]
        print(f"  created {view_name} -> {n:,} rows  t={time.time()-t0:.1f}s")
    except Exception as e:
        print(f"  FAIL {view_name}: {str(e)[:200]}")

# Quick sanity: same query as Prompt 1 but against the flat view
print("\n=== Sanity check: same demographics on the flat view ===")
cur.execute("""
SELECT
  COUNT(*) AS n_total,
  COUNT_IF(IS_MALIGNANT) AS n_malignant,
  ROUND(AVG(AGE_AT_SURGERY), 1) AS mean_age,
  COUNT_IF(SEX = 'female') AS n_female
FROM THYROID_VALIDATION.PUBLIC.CANONICAL_PATIENT_MASTER_FLAT
""")
print(cur.fetchone())

ctx.close()
