"""Phase D.1.b — CTAS rebuild canonical_us_nodule_v2 with validated columns.

Adds two new columns via CTAS-rebuild (preserves CLUSTER BY research_id):
  - tirads_reported_system_validated: cleaned system name per date-aware rule
  - tirads_reported_system_inference_method: documents how each row got its value

D.1.a findings addressed:
  - 13,016 rows: tirads_reported_system=NULL but has TR text → apply date rule
  - 8,073 rows: tirads_reported_system='unspecified' but NO TR text → flip to NULL
  - 1,152 rows: tirads_reported_system='null' (string) → treat as Python NULL
  - 1 row: tirads_reported_system='ATA' → preserve as explicit_named_system

Snapshot: pub_workspace.canonical_us_nodule_v2_pre_phase_d_snapshot_<yyyymmdd>
"""
from __future__ import annotations
import json
from datetime import datetime
from google.cloud import bigquery

PROJECT = "thyroid-canonical-pub-2026"
client = bigquery.Client(project=PROJECT)
TODAY = datetime.now().strftime("%Y%m%d")
SNAPSHOT = f"pub_workspace.canonical_us_nodule_v2_pre_phase_d_snapshot_{TODAY}"

EXPLICIT_SYSTEMS = ("'Kwak', 'EU', 'KTIRADS', 'CTIRADS', 'BTA', 'AACE', 'ATA', 'SRU'")


def run(sql: str, label: str) -> None:
    print(f"\n--- {label} ---")
    job = client.query(sql)
    job.result()
    print(f"  done — bytes={job.estimated_bytes_processed or 'N/A'}")


# ── Step 0: snapshot current table ──────────────────────────────────────────
print(f"\nSnapshotting to {SNAPSHOT} ...")
run(
    f"""
    CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.{SNAPSHOT}`
    CLUSTER BY research_id AS
    SELECT * FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_v2`
    """,
    "snapshot",
)
print("  snapshot done.")

# ── Step 1: CTAS rebuild with two new validated columns ─────────────────────
# NOTE: BQ CTAS cannot use ALTER TABLE to add columns then CTAS; instead we
# just include the new columns in the SELECT and it auto-creates them.
# Because the table already exists, we use CREATE OR REPLACE TABLE.
#
# Validation logic (per Logan v0.2 spec + D.1.a anomalies):
#   1. If tirads_reported_system is a real named system (not null, not 'null', not ACR2017/unspecified)
#      AND it's a recognized non-ACR system → keep as explicit_named_system
#   2. If TR text IS present AND post-2017 → ACR2017
#   3. If TR text IS present AND pre-2017 → unspecified
#   4. If TR text IS NULL → NULL (no TR in report at all)

CTAS_SQL = """
CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_v2`
CLUSTER BY research_id AS
SELECT
  *,

  -- NEW: validated system
  CASE
    -- Explicit non-ACR system named by radiologist → preserve
    WHEN NULLIF(t.tirads_reported_system, 'null') IN ({explicit})
      THEN NULLIF(t.tirads_reported_system, 'null')
    -- TR text present, post-2017 → default ACR2017
    WHEN t.tirads_reported_in_text IS NOT NULL AND t.exam_date >= '2017-01-01'
      THEN 'ACR2017'
    -- TR text present, pre-2017 → cannot assume system
    WHEN t.tirads_reported_in_text IS NOT NULL AND t.exam_date < '2017-01-01'
      THEN 'unspecified'
    -- No TR text at all → NULL
    ELSE NULL
  END AS tirads_reported_system_validated,

  -- NEW: inference method provenance
  CASE
    WHEN NULLIF(t.tirads_reported_system, 'null') IN ({explicit})
      THEN 'explicit_named_system'
    WHEN t.tirads_reported_in_text IS NOT NULL AND t.exam_date >= '2017-01-01'
      THEN 'date_aware_acr2017_default'
    WHEN t.tirads_reported_in_text IS NOT NULL AND t.exam_date < '2017-01-01'
      THEN 'pre_2017_unspecified'
    WHEN t.tirads_reported_in_text IS NULL
      THEN 'no_tr_in_report'
    ELSE 'edge_case'
  END AS tirads_reported_system_inference_method

FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_v2` t
""".format(explicit=EXPLICIT_SYSTEMS)

run(CTAS_SQL, "D.1.b CTAS rebuild canonical_us_nodule_v2")

# ── Step 2: quick sanity check ───────────────────────────────────────────────
sanity_rows = list(client.query("""
  SELECT
    tirads_reported_system_validated,
    tirads_reported_system_inference_method,
    COUNT(*) AS n
  FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_v2`
  GROUP BY 1, 2
  ORDER BY 3 DESC
""").result())

print("\n=== D.1.b post-CTAS sanity ===")
for r in sanity_rows:
    print(" ", dict(r))

total_non_null = sum(r["n"] for r in sanity_rows if r["tirads_reported_system_validated"] is not None)
total_null = sum(r["n"] for r in sanity_rows if r["tirads_reported_system_validated"] is None)
print(f"\n  Non-null validated: {total_non_null:,}")
print(f"  Null (no TR in report): {total_null:,}")
print(f"  Expected non-null ~27,885, null ~9,694")
print("\nD.1.b complete.")
