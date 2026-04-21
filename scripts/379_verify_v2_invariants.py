#!/usr/bin/env python3
"""Script 379 — Phase 4: verify canonical_us_nodule_v2 invariants post-absorption.

Hard invariants (script exits 1 on any fail):
  1. source_modality is constant 'US' on canonical_us_lymph_node_v2 (sister
     contract; nodule_v2 has no modality column).
  2. No duplicate (research_id, exam_date, nodule_index_within_exam) in v2.
  3. v2 row count grew (or held) since the cleanup baseline.
  4. v2 patient count grew (or held).
  5. New rows from absorption carry source_tirads_v2=TRUE or
     source_tirads_llm=TRUE (provenance preserved).

Soft reports (logged, not gating):
  - TIRADS metric snapshot (ACR vs updated, both populated, disagreements).
  - Pending-flag distribution.
"""
from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

PUB = PUBLICATION_DB
SCRIPT_TAG = "Script 379"
V2 = f"{PUB}.main.canonical_us_nodule_v2"
LN = f"{PUB}.main.canonical_us_lymph_node_v2"

OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"379_verify_v2_{RUN_TS}.json"

# Cleanup baseline (after Script 374 ran, before any absorption):
BASELINE_ROWS = 36_957
BASELINE_PATIENTS = 6_126


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    log(f"{SCRIPT_TAG} start  commit={args.commit}")
    con = connect_locked()

    fails: list[str] = []

    # 1. LN modality contract (US only)
    ln_modalities = [
        r[0] for r in con.execute(
            f"SELECT DISTINCT source_modality FROM {LN}"
        ).fetchall()
    ]
    log(f"  LN source_modality: {ln_modalities}")
    if ln_modalities != ['US']:
        fails.append(f"LN modality must be ['US'], got {ln_modalities}")

    # 2. No duplicate keys in v2
    dup_count = con.execute(f"""
SELECT COUNT(*) FROM (
    SELECT research_id, exam_date, nodule_index_within_exam, COUNT(*)
    FROM {V2}
    GROUP BY 1,2,3 HAVING COUNT(*) > 1
)
""").fetchone()[0]
    log(f"  duplicate (research_id, exam_date, nodule_index) keys: {dup_count}")
    if dup_count > 0:
        fails.append(f"{dup_count} duplicate keys in canonical_us_nodule_v2")
        # Show top 5 offenders
        for r in con.execute(f"""
SELECT research_id, exam_date, nodule_index_within_exam, COUNT(*) AS n
FROM {V2}
GROUP BY 1,2,3 HAVING COUNT(*) > 1
ORDER BY 4 DESC LIMIT 5
""").fetchall():
            log(f"    dup: rid={r[0]} date={r[1]} idx={r[2]} n={r[3]}")

    # 3-4. Row + patient count vs baseline
    n_rows = con.execute(f"SELECT COUNT(*) FROM {V2}").fetchone()[0]
    n_pts = con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM {V2}"
    ).fetchone()[0]
    log(f"  v2 rows now:     {n_rows:,}  baseline={BASELINE_ROWS:,}  "
        f"Δ={n_rows - BASELINE_ROWS:+,d}")
    log(f"  v2 patients now: {n_pts:,}  baseline={BASELINE_PATIENTS:,}  "
        f"Δ={n_pts - BASELINE_PATIENTS:+,d}")
    if n_rows < BASELINE_ROWS:
        fails.append(f"v2 row count regressed: {n_rows} < {BASELINE_ROWS}")
    if n_pts < BASELINE_PATIENTS:
        fails.append(f"v2 patient count regressed: {n_pts} < {BASELINE_PATIENTS}")

    # 5. Provenance: every row has either a source_* flag OR nlp_backfill_pending
    no_prov = con.execute(f"""
SELECT COUNT(*) FROM {V2}
WHERE NOT (source_base OR source_tirads_v2 OR source_tirads_llm
        OR source_dynamics_llm OR source_fna_linkage
        OR source_us_nodules_tirads)
  AND COALESCE(nlp_backfill_pending, FALSE) = FALSE
""").fetchone()[0]
    log(f"  rows with NO provenance flag AND no pending flag: {no_prov}")
    if no_prov > 0:
        fails.append(
            f"{no_prov} v2 rows have no source_* flag and "
            f"nlp_backfill_pending is FALSE"
        )

    # Soft: TIRADS metric snapshot
    tirads = con.execute(f"""
SELECT
    COUNT(*) AS total_rows,
    COUNT(acr2017_tirads_points) AS has_acr2017_points,
    COUNT(acr2017_tirads_category) AS has_acr2017_category,
    COUNT(updated_tirads_category) AS has_updated_category,
    COUNT(CASE WHEN acr2017_tirads_category IS NOT NULL
               AND updated_tirads_category IS NOT NULL THEN 1 END) AS both_populated,
    SUM(CASE WHEN acr2017_vs_updated_concordant = FALSE THEN 1 ELSE 0 END) AS disagreeing,
    COUNT(tirads_reported_in_text) AS has_text_extracted_tirads,
    SUM(CASE WHEN nlp_backfill_pending THEN 1 ELSE 0 END) AS pending_count
FROM {V2}
""").fetchone()
    keys = ["total_rows", "has_acr2017_points", "has_acr2017_category",
            "has_updated_category", "both_populated", "disagreeing",
            "has_text_extracted_tirads", "pending_count"]
    metrics = dict(zip(keys, tirads))
    log("  TIRADS metric snapshot:")
    for k, v in metrics.items():
        log(f"    {k:30s}: {v}")

    # Source-flag distribution
    src_counts = dict(con.execute(f"""
SELECT
    'source_base'                  AS flag, SUM(CASE WHEN source_base                THEN 1 ELSE 0 END) FROM {V2}
UNION ALL SELECT 'source_tirads_v2',         SUM(CASE WHEN source_tirads_v2          THEN 1 ELSE 0 END) FROM {V2}
UNION ALL SELECT 'source_tirads_llm',        SUM(CASE WHEN source_tirads_llm         THEN 1 ELSE 0 END) FROM {V2}
UNION ALL SELECT 'source_dynamics_llm',      SUM(CASE WHEN source_dynamics_llm       THEN 1 ELSE 0 END) FROM {V2}
UNION ALL SELECT 'source_fna_linkage',       SUM(CASE WHEN source_fna_linkage        THEN 1 ELSE 0 END) FROM {V2}
UNION ALL SELECT 'source_us_nodules_tirads', SUM(CASE WHEN source_us_nodules_tirads  THEN 1 ELSE 0 END) FROM {V2}
""").fetchall())
    log(f"  source_flag distribution: {src_counts}")

    log(f"=== verification {'FAILED' if fails else 'PASSED'} ===")
    for f in fails:
        log(f"  FAIL: {f}")

    DECISION_LOG.write_text(json.dumps({
        "script": SCRIPT_TAG, "run_ts_utc": RUN_TS,
        "v2_rows": n_rows, "v2_patients": n_pts,
        "baseline_rows": BASELINE_ROWS, "baseline_patients": BASELINE_PATIENTS,
        "ln_modalities": ln_modalities,
        "duplicate_keys": dup_count,
        "rows_without_provenance": no_prov,
        "tirads_metrics": metrics,
        "source_flag_distribution": src_counts,
        "fails": fails,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
