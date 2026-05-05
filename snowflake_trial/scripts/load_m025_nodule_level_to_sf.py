"""
Load manuscript_workspace.cohort_m025_nodule_level_v1 (MotherDuck) to
THYROID_VALIDATION.PUBLIC.COHORT_M025_NODULE_LEVEL_V1_FLAT (Snowflake).

mig_311 — bridges the M025 v2.0 nodule-level analytic spine into Snowflake
so that the Cortex Analyst semantic model can resolve. Run after any
mig that rebuilds the MD nodule-level cohort.

Reproducibility:
    cd /Users/loganglosser/THYROID_2026
    .venv/bin/python snowflake_trial/scripts/load_m025_nodule_level_to_sf.py

After load, verify with:
    cortex analyst query "what is the per-tr ROM in the strict eligible cohort, with counts" \\
      --connection thyroid_2026 \\
      --model snowflake_trial/semantic_models/m025_nodule_level_semantic_model.yaml

Expected: TR2 12.90% / TR3 9.13% / TR4 18.72% / TR5 26.11%.
"""
import os, json, sys, time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
from _md_connect import connect_locked  # noqa: E402

from dotenv import load_dotenv
load_dotenv(REPO / ".env")

import snowflake.connector
import snowflake.connector.network as _net

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


def main() -> None:
    # 1) Export MD view to parquet
    parquet_dir = REPO / "snowflake_trial" / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = parquet_dir / "cohort_m025_nodule_level_v1.parquet"

    md = connect_locked()
    t0 = time.time()
    md.sql(
        f"COPY (SELECT * FROM manuscript_workspace.cohort_m025_nodule_level_v1) "
        f"TO '{parquet_path}' (FORMAT PARQUET)"
    )
    n_md = md.sql(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()[0]
    print(f"[md->parquet] {n_md:,} rows -> {parquet_path} ({parquet_path.stat().st_size:,} bytes)  t={time.time()-t0:.1f}s")

    # 2) PUT + CTAS into Snowflake
    ctx = snowflake.connector.connect(
        account="qcc02515",
        host=f"{DOTTED}.snowflakecomputing.com",
        user="LGLOSSE13",
        password=PAT,
        warehouse="COMPUTE_WH",
        database="THYROID_VALIDATION",
        schema="PUBLIC",
        role="ACCOUNTADMIN",
        authenticator="PROGRAMMATIC_ACCESS_TOKEN",
    )
    cur = ctx.cursor()
    cur.execute("CREATE STAGE IF NOT EXISTS THYROID_VALIDATION.PUBLIC.thyroid_stage")
    cur.execute(
        "CREATE OR REPLACE FILE FORMAT THYROID_VALIDATION.PUBLIC.parquet_fmt "
        "TYPE = PARQUET USE_VECTORIZED_SCANNER = TRUE"
    )

    cur.execute(
        f"PUT 'file://{parquet_path}' @THYROID_VALIDATION.PUBLIC.thyroid_stage "
        "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )
    print(f"[put] {cur.fetchall()[0][6]}")

    TABLE = "COHORT_M025_NODULE_LEVEL_V1_FLAT"
    cur.execute(f"DROP VIEW IF EXISTS THYROID_VALIDATION.PUBLIC.{TABLE}")
    cur.execute(f"DROP TABLE IF EXISTS THYROID_VALIDATION.PUBLIC.{TABLE}")

    cur.execute(
        f"""
CREATE TABLE THYROID_VALIDATION.PUBLIC.{TABLE}
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  WITHIN GROUP (ORDER BY ORDER_ID)
  FROM TABLE(INFER_SCHEMA(
    LOCATION => '@THYROID_VALIDATION.PUBLIC.thyroid_stage/cohort_m025_nodule_level_v1.parquet',
    FILE_FORMAT => 'THYROID_VALIDATION.PUBLIC.parquet_fmt'
  ))
)
"""
    )
    cur.execute(
        f"""
COPY INTO THYROID_VALIDATION.PUBLIC.{TABLE}
FROM @THYROID_VALIDATION.PUBLIC.thyroid_stage/cohort_m025_nodule_level_v1.parquet
FILE_FORMAT = (FORMAT_NAME = 'THYROID_VALIDATION.PUBLIC.parquet_fmt')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
"""
    )

    # 3) Rename quoted-lowercase columns to bare-uppercase so unquoted SQL resolves
    cur.execute(f"DESC TABLE THYROID_VALIDATION.PUBLIC.{TABLE}")
    cols = cur.fetchall()
    renamed = 0
    for c in cols:
        name = c[0]
        if name != name.upper():
            cur.execute(
                f'ALTER TABLE THYROID_VALIDATION.PUBLIC.{TABLE} '
                f'RENAME COLUMN "{name}" TO {name.upper()}'
            )
            renamed += 1
    print(f"[rename] {renamed}/{len(cols)} columns -> uppercase")

    # 4) Smoke test against locked Wilson-CI numbers
    cur.execute(
        f"""
SELECT acr2017_tirads_category AS tr,
       COUNT(*) AS n,
       COUNT_IF(nodule_path_proven_malignant) AS n_malig,
       ROUND(100.0 * COUNT_IF(nodule_path_proven_malignant) / NULLIF(COUNT(*), 0), 2) AS rom_pct
FROM THYROID_VALIDATION.PUBLIC.{TABLE}
WHERE analytic_eligible_strict_acr_pernodule = TRUE
GROUP BY 1 ORDER BY 1
"""
    )
    locked = {"TR2": 12.9, "TR3": 9.1, "TR4": 18.7, "TR5": 26.1}
    print(f"\n[smoke] strict-cohort per-TR ROM:")
    print(f"{'TR':6s} {'N':>10s} {'malig':>8s} {'ROM%':>8s}  expected")
    all_pass = True
    for tr, n, m, rom in cur.fetchall():
        exp = locked.get(tr)
        ok = exp is not None and abs(float(rom) - exp) < 0.5
        all_pass = all_pass and (exp is None or ok)
        mark = "OK" if ok else ("--" if exp is None else "FAIL")
        print(f"{tr or 'NULL':6s} {n:>10,} {m:>8,} {rom:>8}  {mark} (exp {exp})")

    print("\nALL PASS" if all_pass else "\nSMOKE TEST FAIL")
    ctx.close()


if __name__ == "__main__":
    main()
