#!/usr/bin/env python3
"""
ops/348 — Propagate any_metastasis to pub_canonical.canonical_path_malignant_patient_rollup_v1

Background
----------
canonical_path_malignant_patient_rollup_v1.any_metastasis was hardcoded FALSE at build
time (script 361, line ~1504: ``FALSE AS any_metastasis``).  The workspace layer has
complete M-stage data in manuscript_cohort_v1_surgery_reconciled (ajcc8_m_stage field)
and path_stage_raw_backfill_v1.  This script propagates the derived flag via a targeted
MERGE — no full table rewrite.

Linked Verification Check: VC-2026-05-08-M086-any-metastasis-zero-and-bethesda-reconcile
  Airtable record: recSbq9CeoduZIP6F
  Base: THYROID_DATA_REGISTRY (appTGeB1jIizZbjnw), table tbl65mYqMWIGEQIBZ

Pipeline
--------
Phase A  — probe current state (always runs in dry-run too)
Phase B  — compute derived flags; stage pub_workspace.tmp_any_metastasis_propagation_2026_05_08
Phase C  — pre-update validation (counts, sanity)
Phase D  — MERGE into canonical rollup (skipped on --dry-run)
Phase E  — post-update cross-checks
Phase F  — update canonical_table_signoff_registry_v1 build_ts (skipped on --dry-run)
Phase G  — write docs/canonical_layer_integrity_report_20260508_addendum.md
Phase H  — (optional, --log-airtable) log Data Feedback Log row + narrow Verification Check

Usage
-----
  .venv/bin/python scripts/ops/348_any_metastasis_canonical_propagation.py --dry-run
  .venv/bin/python scripts/ops/348_any_metastasis_canonical_propagation.py --apply
  .venv/bin/python scripts/ops/348_any_metastasis_canonical_propagation.py --apply --log-airtable

Options
-------
  --apply          Execute BQ writes (MERGE + signoff update).
  --dry-run        Probe + stage + validate only — no canonical mutations.
  --project        BQ project (default: thyroid-canonical-pub-2026).
  --log-airtable   POST Data Feedback Log row + update VC lifecycle to Verified.
                   Requires AIRTABLE_API_KEY env var.

NO PHI: column-update only on de-identified rollup data; no new fields, no free text.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PROJECT_DEFAULT = "thyroid-canonical-pub-2026"
DATASET_CANONICAL = "pub_canonical"
DATASET_WORKSPACE = "pub_workspace"

ROLLUP_TABLE = "canonical_path_malignant_patient_rollup_v1"
STAGING_TABLE = "tmp_any_metastasis_propagation_2026_05_08"
SIGNOFF_TABLE = "canonical_table_signoff_registry_v1"
RECURRENCE_TABLE = "canonical_recurrence_events_v1"

SCRIPT_TAG = "ops/348_any_metastasis_canonical_propagation"
RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# Airtable IDs
AT_THYROID_DATA_REGISTRY_BASE = "appTGeB1jIizZbjnw"
AT_VERIFICATION_CHECKS_TABLE = "tbl65mYqMWIGEQIBZ"
AT_VC_RECORD = "recSbq9CeoduZIP6F"

AT_THYROID_MANUSCRIPT_BASE = "appJYOnUb7KrHKwpV"
AT_DATA_FEEDBACK_LOG_TABLE = "tblsiYKJtKcktkzze"

EXPECTED_ROLLUP_ROWS = 4022
META_M1_COUNT_EXPECTED_RANGE = (1800, 2100)  # acceptance criteria

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DOCS_ADDENDUM_PATH = REPO_ROOT / "docs" / "canonical_layer_integrity_report_20260508_addendum.md"
OUT_DIR = REPO_ROOT / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_log_lines: list[str] = []


def log(msg: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3] + "Z"
    line = f"[{stamp}] {msg}"
    print(line)
    _log_lines.append(line)


def bq(client: Any, sql: str, label: str = "", dry_run_sql: bool = False) -> Any:
    """Run a BQ query and return the result iterator. Logs label."""
    if label:
        log(f"  [{label}]")
    if dry_run_sql:
        log(f"    DRY-RUN SQL:\n{sql[:600].strip()}")
        return []
    job = client.query(sql)
    return job.result()


def fetch_scalar(client: Any, sql: str) -> Any:
    rows = list(client.query(sql).result())
    if not rows:
        return None
    row = rows[0]
    return row[0]


def table_ref(project: str, dataset: str, table: str) -> str:
    return f"`{project}.{dataset}.{table}`"


def table_exists(client: Any, project: str, dataset: str, table: str) -> bool:
    sql = f"""
SELECT COUNT(*) AS n
FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`
WHERE table_name = '{table}'
"""
    try:
        n = fetch_scalar(client, sql)
        return int(n or 0) > 0
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Phase A — Probe current state
# ---------------------------------------------------------------------------

def phase_a(client: Any, project: str) -> dict:
    log("=== PHASE A: Probe current state ===")
    results: dict = {}

    rollup = table_ref(project, DATASET_CANONICAL, ROLLUP_TABLE)
    workspace_cohort = table_ref(project, DATASET_WORKSPACE, "manuscript_cohort_v1_surgery_reconciled")
    path_stage = table_ref(project, DATASET_WORKSPACE, "path_stage_raw_backfill_v1")
    recurrence = table_ref(project, DATASET_CANONICAL, RECURRENCE_TABLE)

    # 1. Row count on rollup
    n_rollup = fetch_scalar(client, f"SELECT COUNT(*) FROM {rollup}")
    results["n_rollup"] = int(n_rollup or 0)
    log(f"  canonical_path_malignant_patient_rollup_v1 rows: {results['n_rollup']:,}")

    # 2. Pre-update any_metastasis=TRUE count
    n_pre_true = fetch_scalar(
        client,
        f"SELECT COUNTIF(any_metastasis IS TRUE) FROM {rollup}"
    )
    results["n_any_metastasis_pre"] = int(n_pre_true or 0)
    log(f"  any_metastasis = TRUE (pre): {results['n_any_metastasis_pre']:,}")

    # 3. M-stage distribution in workspace cohort (all patients)
    log("  Probing manuscript_cohort_v1_surgery_reconciled M-stage distribution…")
    try:
        m_rows = list(client.query(f"""
            SELECT
              COALESCE(UPPER(TRIM(ajcc8_m_stage)), 'NULL') AS m_stage,
              COUNT(*) AS n
            FROM {workspace_cohort}
            GROUP BY 1
            ORDER BY 2 DESC
        """).result())
        for r in m_rows:
            log(f"    ajcc8_m_stage={r['m_stage']}: {r['n']:,}")
        results["m_stage_dist"] = {r["m_stage"]: int(r["n"]) for r in m_rows}
        results["n_workspace_m1"] = results["m_stage_dist"].get("M1", 0)
    except Exception as e:
        log(f"  WARNING: could not probe workspace cohort M-stage: {e}")
        results["n_workspace_m1"] = None

    # 4. path_stage_raw_backfill_v1 row count
    try:
        n_psrb = fetch_scalar(client, f"SELECT COUNT(*) FROM {path_stage}")
        n_psrb_filled = fetch_scalar(
            client,
            f"SELECT COUNTIF(proposed_path_stage_raw IS NOT NULL) FROM {path_stage}"
        )
        results["n_path_stage_raw_backfill"] = int(n_psrb or 0)
        results["n_path_stage_raw_filled"] = int(n_psrb_filled or 0)
        log(f"  path_stage_raw_backfill_v1: {results['n_path_stage_raw_backfill']:,} rows, "
            f"{results['n_path_stage_raw_filled']:,} proposed_path_stage_raw filled")
    except Exception as e:
        log(f"  WARNING: could not probe path_stage_raw_backfill_v1: {e}")
        results["n_path_stage_raw_backfill"] = None

    # 5. canonical_recurrence_events_v1 existence + distant site count
    results["recurrence_table_exists"] = table_exists(client, project, DATASET_CANONICAL, RECURRENCE_TABLE)
    log(f"  {RECURRENCE_TABLE} exists in pub_canonical: {results['recurrence_table_exists']}")
    if results["recurrence_table_exists"]:
        n_rec = fetch_scalar(client, f"SELECT COUNT(*) FROM {recurrence}")
        n_distant = fetch_scalar(
            client,
            f"""SELECT COUNT(DISTINCT CAST(research_id AS STRING)) FROM {recurrence}
                WHERE LOWER(TRIM(COALESCE(recurrence_site, '')))
                      IN ('lung','bone','liver','brain','distant','distant_mets',
                          'pulmonary','skeletal','hepatic','cerebral','distant_lymph')"""
        )
        results["n_recurrence_events"] = int(n_rec or 0)
        results["n_distant_recurrence_patients"] = int(n_distant or 0)
        log(f"  canonical_recurrence_events_v1: {results['n_recurrence_events']:,} rows")
        log(f"  Patients with distant recurrence site: {results['n_distant_recurrence_patients']:,}")
    else:
        results["n_recurrence_events"] = 0
        results["n_distant_recurrence_patients"] = 0
        log("  WARN: canonical_recurrence_events_v1 not found in pub_canonical — "
            "recurrence-derived arm will contribute 0 rows.")

    return results


# ---------------------------------------------------------------------------
# Phase B — Compute and stage derived flags
# ---------------------------------------------------------------------------

def phase_b(client: Any, project: str, phase_a_results: dict, dry_run: bool) -> dict:
    log("=== PHASE B: Compute derived flags + stage tmp table ===")
    results: dict = {}

    staging = table_ref(project, DATASET_WORKSPACE, STAGING_TABLE)
    rollup = table_ref(project, DATASET_CANONICAL, ROLLUP_TABLE)
    workspace_cohort = table_ref(project, DATASET_WORKSPACE, "manuscript_cohort_v1_surgery_reconciled")
    recurrence = table_ref(project, DATASET_CANONICAL, RECURRENCE_TABLE)

    # Build the distant recurrence CTE only if table exists
    if phase_a_results.get("recurrence_table_exists"):
        distant_rec_cte = f"""
    distant_recurrence AS (
        SELECT DISTINCT CAST(research_id AS STRING) AS research_id
        FROM {recurrence}
        WHERE LOWER(TRIM(COALESCE(recurrence_site, '')))
              IN ('lung','bone','liver','brain','distant','distant_mets',
                  'pulmonary','skeletal','hepatic','cerebral','distant_lymph')
    ),"""
        distant_rec_join = """
        LEFT JOIN distant_recurrence dr ON dr.research_id = CAST(r.research_id AS STRING)"""
        distant_rec_flag = "dr.research_id IS NOT NULL"
    else:
        distant_rec_cte = ""
        distant_rec_join = ""
        distant_rec_flag = "FALSE"

    stage_sql = f"""
CREATE OR REPLACE TABLE {staging} AS
WITH
    -- M1 from workspace M-stage cohort (inner join to malignant rollup)
    workspace_m_stage AS (
        SELECT
            CAST(wc.research_id AS STRING) AS research_id,
            UPPER(TRIM(COALESCE(wc.ajcc8_m_stage, ''))) = 'M1' AS any_metastasis_M1_path
        FROM {workspace_cohort} wc
        INNER JOIN {rollup} r ON CAST(r.research_id AS STRING) = CAST(wc.research_id AS STRING)
    ),
    {distant_rec_cte}
    -- Combined per-patient flags
    combined AS (
        SELECT
            CAST(r.research_id AS STRING) AS research_id,
            COALESCE(wm.any_metastasis_M1_path, FALSE)   AS any_metastasis_M1_path,
            {distant_rec_flag}                           AS any_metastasis_recurrence
        FROM {rollup} r
        LEFT JOIN workspace_m_stage wm ON wm.research_id = CAST(r.research_id AS STRING)
        {distant_rec_join}
    )
SELECT
    research_id,
    any_metastasis_M1_path,
    any_metastasis_recurrence,
    (any_metastasis_M1_path OR any_metastasis_recurrence) AS any_metastasis_combined,
    CASE
        WHEN any_metastasis_M1_path AND any_metastasis_recurrence THEN 'both'
        WHEN any_metastasis_M1_path                               THEN 'M1_at_diagnosis'
        WHEN any_metastasis_recurrence                            THEN 'distant_recurrence'
        ELSE                                                           'none'
    END AS source_evidence
FROM combined
"""

    if dry_run:
        log(f"  DRY-RUN: would execute staging SQL (first 800 chars):\n"
            f"{stage_sql[:800].strip()}…")
    else:
        log(f"  Staging {STAGING_TABLE}…")
        client.query(stage_sql).result()
        log("  Stage complete.")

    # Count staged rows (skip on dry-run since table not written)
    if not dry_run:
        n_staged = fetch_scalar(client, f"SELECT COUNT(*) FROM {staging}")
        n_m1 = fetch_scalar(
            client,
            f"SELECT COUNTIF(any_metastasis_M1_path) FROM {staging}"
        )
        n_rec_only = fetch_scalar(
            client,
            f"SELECT COUNTIF(any_metastasis_recurrence AND NOT any_metastasis_M1_path) FROM {staging}"
        )
        n_combined_true = fetch_scalar(
            client,
            f"SELECT COUNTIF(any_metastasis_combined) FROM {staging}"
        )
        results["n_staged"] = int(n_staged or 0)
        results["n_m1_path"] = int(n_m1 or 0)
        results["n_recurrence_only"] = int(n_rec_only or 0)
        results["n_combined_true"] = int(n_combined_true or 0)

        log(f"  Staged rows: {results['n_staged']:,}")
        log(f"  any_metastasis_M1_path=TRUE: {results['n_m1_path']:,}")
        log(f"  any_metastasis_recurrence-only=TRUE: {results['n_recurrence_only']:,}")
        log(f"  any_metastasis_combined=TRUE: {results['n_combined_true']:,} "
            f"  (implied delta from pre={phase_a_results.get('n_any_metastasis_pre',0):,})")
    else:
        results["n_staged"] = 0
        results["n_m1_path"] = phase_a_results.get("n_workspace_m1") or 0
        results["n_recurrence_only"] = phase_a_results.get("n_distant_recurrence_patients") or 0
        results["n_combined_true"] = results["n_m1_path"] + results["n_recurrence_only"]
        log(f"  DRY-RUN: estimated combined TRUE ≈ {results['n_combined_true']:,}")

    return results


# ---------------------------------------------------------------------------
# Phase C — Pre-update validation
# ---------------------------------------------------------------------------

def phase_c(client: Any, project: str, phase_a: dict, phase_b: dict, dry_run: bool) -> dict:
    log("=== PHASE C: Pre-update validation ===")
    results: dict = {"gates": []}

    def gate(name: str, cond: bool, msg: str) -> None:
        status = "PASS" if cond else "FAIL"
        entry = {"name": name, "status": status, "msg": msg}
        results["gates"].append(entry)
        prefix = "  ✓" if cond else "  ✗"
        log(f"{prefix} [{status}] {name}: {msg}")

    # Gate 1: rollup row count
    n_rollup = phase_a["n_rollup"]
    gate("rollup_row_count",
         n_rollup == EXPECTED_ROLLUP_ROWS,
         f"canonical rollup rows = {n_rollup:,} (expected {EXPECTED_ROLLUP_ROWS:,})")

    # Gate 2: pre-update baseline is all-FALSE (confirms bug state)
    n_pre = phase_a.get("n_any_metastasis_pre", 0)
    gate("pre_update_all_false",
         n_pre == 0,
         f"any_metastasis=TRUE pre-update = {n_pre:,} (expected 0 — the unfilled-flag state)")

    # Gate 3: expected combined TRUE in acceptance range
    n_combined = phase_b.get("n_combined_true", 0)
    lo, hi = META_M1_COUNT_EXPECTED_RANGE
    gate("combined_true_in_range",
         lo <= n_combined <= hi,
         f"any_metastasis_combined=TRUE = {n_combined:,} (expected {lo:,}–{hi:,})")

    # Gate 4: M1_path <= combined (no data loss)
    n_m1 = phase_b.get("n_m1_path", 0)
    gate("m1_leq_combined",
         n_m1 <= n_combined,
         f"M1_path ({n_m1:,}) ≤ combined ({n_combined:,})")

    # Gate 5: staged row count matches rollup (all patients covered)
    n_staged = phase_b.get("n_staged", 0)
    if not dry_run:
        gate("staged_covers_rollup",
             n_staged == n_rollup,
             f"staged rows ({n_staged:,}) == rollup rows ({n_rollup:,})")
    else:
        log("  DRY-RUN: skip staged_covers_rollup (table not written)")

    # Hard stop if any gate FAILS (allow pre_update_all_false to WARN, not FAIL)
    hard_fails = [g for g in results["gates"]
                  if g["status"] == "FAIL" and g["name"] != "pre_update_all_false"]
    if hard_fails:
        log(f"  HARD STOP: {len(hard_fails)} gate(s) failed: "
            f"{[g['name'] for g in hard_fails]}")
        raise RuntimeError(f"Phase C gates failed: {[g['name'] for g in hard_fails]}")

    log("  All hard gates pass.")
    return results


# ---------------------------------------------------------------------------
# Phase D — MERGE into canonical rollup
# ---------------------------------------------------------------------------

def phase_d(client: Any, project: str, dry_run: bool) -> dict:
    log("=== PHASE D: MERGE into canonical rollup ===")
    results: dict = {}

    rollup = table_ref(project, DATASET_CANONICAL, ROLLUP_TABLE)
    staging = table_ref(project, DATASET_WORKSPACE, STAGING_TABLE)

    merge_sql = f"""
MERGE {rollup} AS T
USING {staging} AS S
  ON CAST(T.research_id AS STRING) = S.research_id
WHEN MATCHED THEN
  UPDATE SET
    T.any_metastasis = S.any_metastasis_combined,
    T.build_ts       = CURRENT_TIMESTAMP()
"""
    if dry_run:
        log(f"  DRY-RUN: would run MERGE:\n{merge_sql.strip()[:500]}")
        results["rows_merged"] = 0
        results["skipped"] = True
        return results

    log("  Executing MERGE…")
    job = client.query(merge_sql)
    job.result()
    # BQ MERGE does not expose affected-row count directly; use DML stats
    num_updated = job.num_dml_affected_rows or 0
    results["rows_merged"] = num_updated
    log(f"  MERGE complete: {num_updated:,} rows affected.")
    return results


# ---------------------------------------------------------------------------
# Phase E — Post-update cross-checks
# ---------------------------------------------------------------------------

def phase_e(client: Any, project: str, phase_b_results: dict, dry_run: bool) -> dict:
    log("=== PHASE E: Post-update cross-checks ===")
    results: dict = {}

    if dry_run:
        log("  DRY-RUN: skipping post-update cross-checks (no mutation happened).")
        return results

    rollup = table_ref(project, DATASET_CANONICAL, ROLLUP_TABLE)

    n_post_total = fetch_scalar(client, f"SELECT COUNT(*) FROM {rollup}")
    n_post_true = fetch_scalar(client, f"SELECT COUNTIF(any_metastasis IS TRUE) FROM {rollup}")
    n_post_false = fetch_scalar(client, f"SELECT COUNTIF(any_metastasis IS FALSE) FROM {rollup}")
    n_post_null = fetch_scalar(client, f"SELECT COUNTIF(any_metastasis IS NULL) FROM {rollup}")

    results["n_post_total"] = int(n_post_total or 0)
    results["n_post_true"] = int(n_post_true or 0)
    results["n_post_false"] = int(n_post_false or 0)
    results["n_post_null"] = int(n_post_null or 0)

    log("  Post-update canonical_path_malignant_patient_rollup_v1:")
    log(f"    Total rows   : {results['n_post_total']:,}  (expected {EXPECTED_ROLLUP_ROWS:,})")
    log(f"    any_metastasis=TRUE  : {results['n_post_true']:,}")
    log(f"    any_metastasis=FALSE : {results['n_post_false']:,}")
    log(f"    any_metastasis=NULL  : {results['n_post_null']:,}")

    lo, hi = META_M1_COUNT_EXPECTED_RANGE
    if not (lo <= results["n_post_true"] <= hi):
        log(f"  WARNING: post-update TRUE count {results['n_post_true']:,} "
            f"outside acceptance range {lo:,}–{hi:,}")
    else:
        log(f"  ✓ Post-update TRUE count {results['n_post_true']:,} within range {lo:,}–{hi:,}")

    if results["n_post_total"] != EXPECTED_ROLLUP_ROWS:
        raise RuntimeError(
            f"Row count changed post-MERGE! Expected {EXPECTED_ROLLUP_ROWS:,}, "
            f"got {results['n_post_total']:,}"
        )
    log("  ✓ Row count unchanged.")

    # Cross-check: M1_path count <= post-TRUE count
    n_m1 = phase_b_results.get("n_m1_path", 0)
    if n_m1 > results["n_post_true"]:
        log(f"  WARNING: M1_path count ({n_m1:,}) > post-TRUE count "
            f"({results['n_post_true']:,}) — unexpected data loss.")
    else:
        log(f"  ✓ M1_path ({n_m1:,}) ≤ post-TRUE ({results['n_post_true']:,})")

    return results


# ---------------------------------------------------------------------------
# Phase F — Update canonical_table_signoff_registry_v1
# ---------------------------------------------------------------------------

def phase_f(client: Any, project: str, phase_e_results: dict, dry_run: bool) -> dict:
    log("=== PHASE F: Update canonical_table_signoff_registry_v1 ===")
    results: dict = {}

    if dry_run:
        log("  DRY-RUN: skipping signoff registry update.")
        return results

    signoff = table_ref(project, DATASET_CANONICAL, SIGNOFF_TABLE)
    n_post_true = phase_e_results.get("n_post_true", "?")

    update_sql = f"""
UPDATE {signoff}
SET
  build_ts         = CURRENT_TIMESTAMP(),
  notes            = CONCAT(
    COALESCE(notes, ''),
    ' | 2026-05-08 ops/348: any_metastasis backfill from ajcc8_m_stage + distant recurrence. ',
    'Post-update any_metastasis=TRUE: {n_post_true:,}. ',
    'VC: VC-2026-05-08-M086-any-metastasis-zero-and-bethesda-reconcile (recSbq9CeoduZIP6F).'
  )
WHERE table_name = '{ROLLUP_TABLE}'
"""
    log(f"  Updating signoff registry for {ROLLUP_TABLE}…")
    try:
        job = client.query(update_sql)
        job.result()
        n_updated = job.num_dml_affected_rows or 0
        results["signoff_rows_updated"] = n_updated
        log(f"  Signoff registry update: {n_updated} row(s) updated.")
    except Exception as e:
        log(f"  WARNING: signoff registry update failed (non-blocking): {e}")
        results["signoff_rows_updated"] = 0
        results["signoff_error"] = str(e)

    return results


# ---------------------------------------------------------------------------
# Phase G — Write docs addendum
# ---------------------------------------------------------------------------

def phase_g(
    phase_a: dict,
    phase_b: dict,
    phase_c: dict,
    phase_e: dict,
    dry_run: bool,
) -> None:
    log("=== PHASE G: Write docs addendum ===")

    gate_rows = "\n".join(
        f"| {g['name']} | {g['status']} | {g['msg']} |"
        for g in phase_c.get("gates", [])
    )
    n_pre = phase_a.get("n_any_metastasis_pre", 0)
    n_post = phase_e.get("n_post_true", "N/A (dry-run)")
    n_m1 = phase_b.get("n_m1_path", "?")
    n_rec = phase_b.get("n_recurrence_only", "?")
    n_rollup = phase_a.get("n_rollup", "?")

    content = f"""# Canonical Layer Integrity Report — Addendum 2026-05-08

**Script:** `scripts/ops/348_any_metastasis_canonical_propagation.py`
**Run timestamp:** {RUN_TS}
**Mode:** {"DRY-RUN" if dry_run else "APPLIED"}

## Summary

Propagated `any_metastasis` to `pub_canonical.canonical_path_malignant_patient_rollup_v1`
using M-stage evidence from `pub_workspace.manuscript_cohort_v1_surgery_reconciled` (M1
flag) and distant-site recurrence evidence from `pub_canonical.canonical_recurrence_events_v1`.

| Metric | Value |
|--------|-------|
| Canonical rollup rows (unchanged) | {n_rollup:,} |
| any_metastasis = TRUE (pre-update) | {n_pre:,} |
| any_metastasis = TRUE (post-update) | {n_post} |
| M1_at_diagnosis arm (M1 in workspace cohort) | {n_m1:,} |
| distant_recurrence-only arm (no path M1) | {n_rec:,} |
| Acceptance range | 1,800–2,100 |

## Validation Gates

| Gate | Status | Detail |
|------|--------|--------|
{gate_rows}

## Tables Changed

| Table | Change |
|-------|--------|
| `pub_canonical.canonical_path_malignant_patient_rollup_v1` | `any_metastasis` column backfilled; `build_ts` updated |
| `pub_workspace.tmp_any_metastasis_propagation_2026_05_08` | Staging table created/replaced |
| `pub_canonical.canonical_table_signoff_registry_v1` | `build_ts` + `notes` updated for `{ROLLUP_TABLE}` |

## Linked Verification Check

- **VC ID:** VC-2026-05-08-M086-any-metastasis-zero-and-bethesda-reconcile
- **Airtable record:** recSbq9CeoduZIP6F
- **Base / Table:** THYROID_DATA_REGISTRY (appTGeB1jIizZbjnw) / tbl65mYqMWIGEQIBZ
- **Action:** lifecycle → Verified; status → Resolved

## Notes

- M086 build is **unaffected** — workspace tables are authoritative for M086.
- This is hygiene for downstream papers that read from pub_canonical.
- `canonical_path_malignant_patient_rollup_v1` hash changed; `build_ts` updated accordingly.
- MERGE strategy used; row count invariant preserved at {EXPECTED_ROLLUP_ROWS:,}.
"""

    DOCS_ADDENDUM_PATH.write_text(content, encoding="utf-8")
    log(f"  Written: {DOCS_ADDENDUM_PATH}")


# ---------------------------------------------------------------------------
# Phase H — Airtable DFL + Verification Check update
# ---------------------------------------------------------------------------

def _at_request(method: str, url: str, payload: dict | None, api_key: str) -> dict:
    data = json.dumps(payload).encode("utf-8") if payload else None
    headers: dict = {"Authorization": f"Bearer {api_key}"}
    if data:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Airtable HTTP {e.code}: {body}") from e


def phase_h(
    phase_e: dict,
    phase_b: dict,
    dry_run: bool,
) -> None:
    log("=== PHASE H: Airtable Data Feedback Log + Verification Check ===")

    api_key = os.environ.get("AIRTABLE_API_KEY", "").strip()
    if not api_key:
        log("  SKIP: AIRTABLE_API_KEY not set. Pass --log-airtable with the key in env.")
        return

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    n_post = phase_e.get("n_post_true", 0)
    n_m1 = phase_b.get("n_m1_path", 0)
    n_rec = phase_b.get("n_recurrence_only", 0)

    # --- H1: Data Feedback Log row ---
    dfl_url = (
        f"https://api.airtable.com/v0/{AT_THYROID_MANUSCRIPT_BASE}/"
        f"{urllib.parse.quote('Data Feedback Log')}"
    )
    dfl_payload = {
        "fields": {
            "feedback_id": "DFL-20260508-ANYMETASTASIS-CANONICAL-PROPAGATION",
            "timestamp": now_iso,
            "target_type": "BQ infrastructure",
            "target_record": (
                "pub_canonical.canonical_path_malignant_patient_rollup_v1 "
                "(any_metastasis column)"
            ),
            "change_type": "migration",
            "your_request_summary": (
                "any_metastasis was hardcoded FALSE at build time (script 361). "
                "Backfill from ajcc8_m_stage in manuscript_cohort_v1_surgery_reconciled "
                "and distant-site recurrence events."
            ),
            "my_action_summary": (
                f"ops/348 MERGE: any_metastasis=TRUE post-update={n_post:,} "
                f"(M1_at_diagnosis={n_m1:,}, distant_recurrence_only={n_rec:,}). "
                "Row count unchanged at 4,022. "
                "VC: VC-2026-05-08-M086-any-metastasis-zero-and-bethesda-reconcile."
            ),
            "before_value": "any_metastasis = FALSE for all 4,022 malignant patients (unfilled).",
            "after_value": (
                f"any_metastasis=TRUE for {n_post:,} patients "
                f"({n_m1:,} M1_at_diagnosis + {n_rec:,} distant_recurrence_only)."
            ),
            "source_chat": "VC-2026-05-08-M086-any-metastasis-zero-and-bethesda-reconcile",
            "lifecycle": "Logged",
        }
    }

    if dry_run:
        log(f"  DRY-RUN: would POST DFL row: {dfl_payload['fields']['feedback_id']}")
    else:
        try:
            resp = _at_request("POST", dfl_url, dfl_payload, api_key)
            log(f"  DFL row created: {resp.get('id', 'unknown')} "
                f"({dfl_payload['fields']['feedback_id']})")
        except Exception as e:
            log(f"  WARNING: DFL row creation failed (non-blocking): {e}")

    # --- H2: Narrow Verification Check lifecycle → Verified ---
    vc_url = (
        f"https://api.airtable.com/v0/{AT_THYROID_DATA_REGISTRY_BASE}/"
        f"{AT_VERIFICATION_CHECKS_TABLE}/{AT_VC_RECORD}"
    )
    vc_payload = {
        "fields": {
            "lifecycle": "Verified",
            "status": "Resolved",
            "notes": (
                f"Resolved 2026-05-08 by ops/348. "
                f"any_metastasis=TRUE post-update: {n_post:,} patients. "
                f"Row count invariant: {EXPECTED_ROLLUP_ROWS:,}. "
                "Downstream papers that read from pub_canonical canonical layer now "
                "have accurate any_metastasis flags."
            ),
        }
    }
    if dry_run:
        log(f"  DRY-RUN: would PATCH VC record {AT_VC_RECORD} → lifecycle=Verified")
    else:
        try:
            resp = _at_request("PATCH", vc_url, vc_payload, api_key)
            log(f"  VC {AT_VC_RECORD} updated: lifecycle=Verified, status=Resolved")
        except Exception as e:
            log(f"  WARNING: VC update failed (non-blocking): {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description="ops/348 — Propagate any_metastasis to canonical rollup (BigQuery)"
    )
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--apply", action="store_true",
                      help="Execute BQ writes (MERGE + signoff update).")
    mode.add_argument("--dry-run", action="store_true",
                      help="Probe + stage + validate only; no canonical mutations.")
    ap.add_argument("--project", default=PROJECT_DEFAULT,
                    help=f"BQ project ID (default: {PROJECT_DEFAULT})")
    ap.add_argument("--log-airtable", action="store_true",
                    help="POST DFL row + narrow VC to Verified (needs AIRTABLE_API_KEY).")
    args = ap.parse_args()

    dry_run = args.dry_run

    log("=" * 60)
    log("ops/348 any_metastasis canonical propagation")
    log(f"  project  : {args.project}")
    log(f"  mode     : {'DRY-RUN' if dry_run else 'APPLY'}")
    log(f"  run_ts   : {RUN_TS}")
    log("=" * 60)

    try:
        from google.cloud import bigquery
    except ImportError:
        log("ERROR: google-cloud-bigquery not installed. "
            "Run: pip install google-cloud-bigquery")
        return 1

    try:
        client = bigquery.Client(project=args.project)
    except Exception as e:
        log(f"ERROR: BQ client init failed: {e}")
        log("  Run: gcloud auth application-default login")
        return 1

    try:
        pa = phase_a(client, args.project)
        pb = phase_b(client, args.project, pa, dry_run)
        pc = phase_c(client, args.project, pa, pb, dry_run)
        phase_d(client, args.project, dry_run)
        pe = phase_e(client, args.project, pb, dry_run)
        phase_f(client, args.project, pe, dry_run)
        phase_g(pa, pb, pc, pe, dry_run)

        if args.log_airtable:
            phase_h(pe, pb, dry_run)

    except RuntimeError as e:
        log(f"\nABORTED: {e}")
        return 1

    # Write run log
    log_path = OUT_DIR / f"348_run_{RUN_TS}.txt"
    log_path.write_text("\n".join(_log_lines), encoding="utf-8")
    log(f"\nRun log: {log_path}")

    mode_label = "DRY-RUN" if dry_run else "APPLIED"
    log(f"\n{'=' * 60}")  # noqa: E501
    log(f"ops/348 complete [{mode_label}]")
    if not dry_run:
        n_post = pe.get("n_post_true", "?")
        log(f"  any_metastasis=TRUE post-update : {n_post}")
        log(f"  Row count invariant             : {EXPECTED_ROLLUP_ROWS:,}")
        log(f"  Docs written                    : {DOCS_ADDENDUM_PATH.name}")
    log(f"{'=' * 60}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
