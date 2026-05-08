"""Phase D.1.c — Add tirads_reported_system_validated + inference_method to
canonical_us_nodule_tirads_multisystem_v1 by joining to canonical_us_nodule_v2.

D.1.d audit also runs at the end of this script.
"""
from __future__ import annotations
from google.cloud import bigquery

PROJECT = "thyroid-canonical-pub-2026"
client = bigquery.Client(project=PROJECT)


def run(sql: str, label: str) -> None:
    print(f"\n--- {label} ---")
    job = client.query(sql)
    job.result()
    print("  done")


def qrows(sql: str, label: str) -> list[dict]:
    print(f"\n=== {label} ===")
    rows = [dict(r) for r in client.query(sql).result()]
    for r in rows:
        print(" ", r)
    return rows


# ── Step 0: check if multisystem validated cols already exist ────────────────
existing = qrows(
    """
    SELECT column_name
    FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'canonical_us_nodule_tirads_multisystem_v1'
      AND column_name IN ('tirads_reported_system_validated', 'tirads_reported_system_inference_method')
    """,
    "D.1.c — existing validated cols in multisystem table",
)
existing_names = {r["column_name"] for r in existing}

# ── Step 1: CTAS rebuild joining to canonical_us_nodule_v2 validated cols ───
# The multisystem table has nodule_id; canonical_us_nodule_v2 also has nodule_id.
# Join on nodule_id to pull the two validated columns across.

# First confirm the join key
qrows(
    """
    SELECT COUNT(*) AS n_multisys, COUNT(DISTINCT nodule_id) AS n_distinct
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_tirads_multisystem_v1`
    """,
    "D.1.c — multisystem table baseline",
)

EXCEPT_CLAUSE = ""
if "tirads_reported_system_validated" in existing_names:
    EXCEPT_CLAUSE = "EXCEPT (tirads_reported_system_validated, tirads_reported_system_inference_method)"

run(
    f"""
    CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_tirads_multisystem_v1`
    CLUSTER BY research_id AS
    SELECT
      m.* {EXCEPT_CLAUSE},
      n.tirads_reported_system_validated,
      n.tirads_reported_system_inference_method
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_tirads_multisystem_v1` m
    LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_v2` n
      USING (nodule_id)
    """,
    "D.1.c CTAS multisystem with validated cols",
)

# ── Step 2: D.1.d Audit ─────────────────────────────────────────────────────
print("\n\n##### D.1.d — Distribution audit (canonical_us_nodule_v2) #####")
dist_v2 = qrows(
    """
    SELECT
      tirads_reported_system_validated,
      tirads_reported_system_inference_method,
      COUNT(*) AS n
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_v2`
    GROUP BY 1, 2
    ORDER BY 3 DESC
    """,
    "D.1.d — canonical_us_nodule_v2 validated distribution",
)

print("\n##### D.1.d — Distribution audit (multisystem table) #####")
dist_ms = qrows(
    """
    SELECT
      tirads_reported_system_validated,
      tirads_reported_system_inference_method,
      COUNT(*) AS n
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_tirads_multisystem_v1`
    GROUP BY 1, 2
    ORDER BY 3 DESC
    """,
    "D.1.d — multisystem validated distribution",
)

# ── Sanity checks ────────────────────────────────────────────────────────────
total_v2 = sum(r["n"] for r in dist_v2)
acr_v2 = sum(r["n"] for r in dist_v2 if r["tirads_reported_system_validated"] == "ACR2017")
unspec_v2 = sum(r["n"] for r in dist_v2 if r["tirads_reported_system_validated"] == "unspecified")
explicit_v2 = sum(r["n"] for r in dist_v2 if r.get("tirads_reported_system_inference_method") == "explicit_named_system")
no_tr_v2 = sum(r["n"] for r in dist_v2 if r["tirads_reported_system_validated"] is None)

print(f"\n=== D.1.d SANITY CHECKS (canonical_us_nodule_v2) ===")
print(f"  Total rows:                  {total_v2:,}  (expected 37,579)")
print(f"  ACR2017 (date-aware default):{acr_v2:,}  (expected ~24,394)")
print(f"  unspecified (pre-2017):      {unspec_v2:,}  (expected ~3,491)")
print(f"  explicit_named_system:       {explicit_v2:,}  (expected <5%={0.05*total_v2:.0f})")
print(f"  no_tr_in_report (NULL):      {no_tr_v2:,}  (expected ~9,694)")
print(f"  Non-null fraction:           {(total_v2-no_tr_v2)/total_v2:.1%}")

assert total_v2 == 37579, f"Row count mismatch! {total_v2} != 37,579"
assert abs(acr_v2 - 24394) < 200, f"ACR2017 count unexpected: {acr_v2}"
assert explicit_v2 / total_v2 < 0.05, f"explicit_named_system > 5%: {explicit_v2/total_v2:.1%}"
print("\nAll D.1.d sanity checks PASS")

total_ms = sum(r["n"] for r in dist_ms)
acr_ms = sum(r["n"] for r in dist_ms if r["tirads_reported_system_validated"] == "ACR2017")
print(f"\n=== D.1.d SANITY CHECKS (multisystem) ===")
print(f"  Total rows: {total_ms:,}  (expected 37,579)")
print(f"  ACR2017:    {acr_ms:,}")
print("D.1.c+d complete.")
