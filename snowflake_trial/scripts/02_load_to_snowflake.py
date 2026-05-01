"""
Load Parquet stage files into Snowflake tables via CTAS.

Connector workarounds (v4.4.0):
  - PROGRAMMATIC_ACCESS_TOKEN auth leaves TOKEN body field empty
  - ACCOUNT_NAME body strips region from dotted form
"""
import os, sys, time, json
from pathlib import Path
import snowflake.connector
import snowflake.connector.network as _net

PARQUET_DIR = Path("/Users/ros/THyroid 2026/snowflake_trial/parquet")
PAT = os.environ["SNOWFLAKE_PAT"]
DOTTED = "qcc02515.us-east-1"

_orig_post = _net.SnowflakeRestful._post_request
def _patched_post(self, url, headers, body, *args, **kwargs):
    if "/session/v1/login-request" in url:
        try:
            d = json.loads(body) if isinstance(body, str) else json.loads(body.decode())
            d["data"]["ACCOUNT_NAME"] = DOTTED
            if not d["data"].get("TOKEN"):
                d["data"]["TOKEN"] = PAT
            body = json.dumps(d)
        except Exception:
            pass
    return _orig_post(self, url, headers, body, *args, **kwargs)
_net.SnowflakeRestful._post_request = _patched_post

ctx = snowflake.connector.connect(
    account="qcc02515", host=f"{DOTTED}.snowflakecomputing.com",
    user="LGLOSSE13", password=PAT,
    authenticator="PROGRAMMATIC_ACCESS_TOKEN",
    warehouse="COMPUTE_WH", database="THYROID_VALIDATION",
    schema="PUBLIC", role="ACCOUNTADMIN",
)
cur = ctx.cursor()
print(f"[connect] OK -> {cur.execute('SELECT CURRENT_USER(), CURRENT_ACCOUNT()').fetchone()}")

cur.execute("CREATE STAGE IF NOT EXISTS THYROID_VALIDATION.PUBLIC.thyroid_stage")
cur.execute("""
CREATE OR REPLACE FILE FORMAT THYROID_VALIDATION.PUBLIC.parquet_fmt
    TYPE = PARQUET
    USE_VECTORIZED_SCANNER = TRUE
""")
print("[setup] stage + file format ready")

files = sorted(PARQUET_DIR.glob("*.parquet"))
# PUT (idempotent — overwrite=true)
print(f"[put] uploading {len(files)} file(s)")
for f in files:
    t0 = time.time()
    cur.execute(
        f"PUT 'file://{f}' @THYROID_VALIDATION.PUBLIC.thyroid_stage "
        f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )
    rows = cur.fetchall()
    status = rows[0][6] if rows else "?"
    print(f"  {f.name:50s}  {status}  t={time.time()-t0:5.1f}s")

# CTAS — Snowflake natively reads Parquet via SELECT from stage with file_format
results = {"loaded": [], "failed": []}
for f in files:
    table = f.stem.upper()
    t0 = time.time()
    try:
        cur.execute(f"DROP TABLE IF EXISTS THYROID_VALIDATION.PUBLIC.{table}")
        cur.execute(f"""
CREATE TABLE THYROID_VALIDATION.PUBLIC.{table}
AS SELECT *
FROM @THYROID_VALIDATION.PUBLIC.thyroid_stage/{f.name}
(FILE_FORMAT => 'THYROID_VALIDATION.PUBLIC.parquet_fmt')
        """)
        cur.execute(f"SELECT COUNT(*) FROM THYROID_VALIDATION.PUBLIC.{table}")
        n = cur.fetchone()[0]
        cur.execute(f"DESC TABLE THYROID_VALIDATION.PUBLIC.{table}")
        n_cols = len(cur.fetchall())
        print(f"  OK   {table:50s}  rows={n:>7,}  cols={n_cols:>4}  t={time.time()-t0:5.1f}s")
        results["loaded"].append((table, n, n_cols))
    except Exception as e:
        msg = str(e)[:160]
        print(f"  FAIL {table:50s}  {msg}")
        results["failed"].append((table, msg))

print(f"\n[summary] loaded={len(results['loaded'])} failed={len(results['failed'])}")
if results["failed"]:
    print("Failed tables:")
    for t, m in results["failed"]:
        print(f"  {t}: {m}")
ctx.close()
