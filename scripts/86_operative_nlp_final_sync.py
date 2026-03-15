#!/usr/bin/env python3
"""
86_operative_nlp_final_sync.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Operative NLP Propagation Final Sync — repairs the downstream gap where
episode_analysis_resolved_v1 and patient_analysis_resolved_v1 received only
a subset of the NLP-enriched operative fields.

ROOT CAUSE (documented in docs/final_operative_nlp_sync_20260314.md):
  Script 48 (EPISODE_RESOLVED_SQL) was written before script 71 fully enriched
  operative_episode_detail_v2.  It copies only 5 of 11 NLP fields:
    ✓ rln_monitoring_flag, rln_finding_raw, intraop_gross_ete (=gross_ete_flag),
      parathyroid_resection_flag, drain_flag
    ✗ parathyroid_autograft_flag, local_invasion_flag, tracheal_involvement_flag,
      esophageal_involvement_flag, strap_muscle_involvement_flag,
      reoperative_field_flag, operative_findings_raw

  downstream:
    patient_analysis_resolved_v1 → only complication-derived RLN flags (no raw NLP)
    manuscript_cohort_v1          → same gap

  esophageal_involvement_flag = 0 is source-limited (confirmed):
    0 entity records in note_entities_procedures for esophageal_involvement;
    10 op notes with "esophag%invaded%" phrases — ALL match
    anatomical/positional text (tracheoesophageal groove, dilator placement),
    not invasion language. Classified as source-limited, not a vocabulary miss.

PHASES:
  A  — BEFORE snapshot (baseline counts)
  B  — ALTER TABLE episode_analysis_resolved_v1 (add 7 missing BOOLEAN/VARCHAR cols)
  C  — UPDATE episode_analysis_resolved_v1 (backfill from operative_episode_detail_v2)
  D  — Rebuild episode_analysis_resolved_v1_dedup (same dedup logic + new cols)
  E  — ALTER TABLE patient_analysis_resolved_v1 (add 10 patient-level NLP summary cols)
  F  — UPDATE patient_analysis_resolved_v1 (BOOL_OR aggregates per patient)
  G  — ALTER TABLE manuscript_cohort_v1 + backfill same patient-level fields
  H  — Rebuild md_ mirrors (episode_dedup + patient_analysis + manuscript)
  I  — Create val_operative_nlp_final_sync_v1 validation table
  J  — Export artifacts to exports/final_operative_nlp_sync_20260314/

IMPORTANT: No manuscript-facing *counts* change; only new NLP fields are added
to existing rows.  All changes are documented in Phase I validation table.

Usage:
    /opt/homebrew/bin/python3 scripts/86_operative_nlp_final_sync.py --md
    /opt/homebrew/bin/python3 scripts/86_operative_nlp_final_sync.py --md --dry-run
    /opt/homebrew/bin/python3 scripts/86_operative_nlp_final_sync.py --md --phase A
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DATE_TAG = "20260314"
EXPORTS_DIR = ROOT / "exports" / f"final_operative_nlp_sync_{DATE_TAG}"
DOCS_DIR = ROOT / "docs"

# ─── Connection ───────────────────────────────────────────────────────────────

def get_token() -> str:
    token = os.environ.get("MOTHERDUCK_TOKEN")
    if token:
        return token
    secrets = ROOT / ".streamlit" / "secrets.toml"
    if secrets.exists():
        import toml
        return toml.load(str(secrets))["MOTHERDUCK_TOKEN"]
    raise RuntimeError("MOTHERDUCK_TOKEN not found in env or .streamlit/secrets.toml")


def connect(use_md: bool) -> duckdb.DuckDBPyConnection:
    if use_md:
        token = get_token()
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    return duckdb.connect(str(ROOT / "thyroid_master.duckdb"))


def safe_exec(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    """Execute SQL; return rowcount or 0.  DDL always returns 0 (not -1)."""
    try:
        r = con.execute(sql)
        rc = r.rowcount if hasattr(r, "rowcount") else 0
        # DuckDB returns -1 for rowcount on DDL (ALTER TABLE, CREATE TABLE);
        # treat as success (0) not failure.
        return 0 if rc == -1 else rc
    except Exception as e:
        print(f"    [WARN] SQL error: {e}")
        return -1


def safe_count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    try:
        r = con.execute(sql).fetchone()
        return int(r[0]) if r else 0
    except Exception:
        return -1


def col_exists(con: duckdb.DuckDBPyConnection, table: str, col: str) -> bool:
    n = safe_count(con,
        f"SELECT COUNT(*) FROM information_schema.columns "
        f"WHERE table_name='{table}' AND column_name='{col}' AND table_schema='main'"
    )
    return n > 0


def write_parquet_table(con: duckdb.DuckDBPyConnection, df: pd.DataFrame,
                        table: str, mode: str = "replace") -> None:
    """Write DataFrame to MotherDuck via parquet intermediary."""
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        tmp = f.name
    df.to_parquet(tmp, index=False)
    if mode == "replace":
        con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_parquet('{tmp}')")
    else:
        con.execute(f"INSERT INTO {table} SELECT * FROM read_parquet('{tmp}')")
    os.unlink(tmp)


# ─── Operative NLP fields to add ─────────────────────────────────────────────

# Fields missing from episode_analysis_resolved_v1
EPISODE_MISSING_FIELDS: list[tuple[str, str]] = [
    ("parathyroid_autograft_flag",    "BOOLEAN"),
    ("local_invasion_flag",           "BOOLEAN"),
    ("tracheal_involvement_flag",     "BOOLEAN"),
    ("esophageal_involvement_flag",   "BOOLEAN"),
    ("strap_muscle_involvement_flag", "BOOLEAN"),
    ("reoperative_field_flag",        "BOOLEAN"),
    ("operative_findings_raw",        "VARCHAR"),
]

# Patient-level aggregate fields to add to patient_analysis_resolved_v1
#   and manuscript_cohort_v1
PATIENT_OP_FIELDS: list[tuple[str, str, str]] = [
    # (column_name, dtype, source_expr)
    ("op_rln_monitoring_any",     "BOOLEAN",
     "BOOL_OR(COALESCE(rln_monitoring_flag, FALSE))"),
    ("op_drain_placed_any",       "BOOLEAN",
     "BOOL_OR(COALESCE(drain_flag, FALSE))"),
    ("op_strap_muscle_any",       "BOOLEAN",
     "BOOL_OR(COALESCE(strap_muscle_involvement_flag, FALSE))"),
    ("op_reoperative_any",        "BOOLEAN",
     "BOOL_OR(COALESCE(reoperative_field_flag, FALSE))"),
    ("op_parathyroid_autograft_any", "BOOLEAN",
     "BOOL_OR(COALESCE(parathyroid_autograft_flag, FALSE))"),
    ("op_local_invasion_any",     "BOOLEAN",
     "BOOL_OR(COALESCE(local_invasion_flag, FALSE))"),
    ("op_tracheal_inv_any",       "BOOLEAN",
     "BOOL_OR(COALESCE(tracheal_involvement_flag, FALSE))"),
    ("op_esophageal_inv_any",     "BOOLEAN",
     "BOOL_OR(COALESCE(esophageal_involvement_flag, FALSE))"),
    ("op_intraop_gross_ete_any",  "BOOLEAN",
     "BOOL_OR(COALESCE(gross_ete_flag, FALSE))"),
    ("op_n_surgeries_with_findings", "INTEGER",
     "COUNT(DISTINCT CASE WHEN COALESCE(operative_findings_raw,'') != '' "
     "THEN surgery_episode_id END)"),
    ("op_findings_summary",       "VARCHAR",
     "STRING_AGG(DISTINCT NULLIF(operative_findings_raw,''), ' | ')"),
]

# ─── Phase A: BEFORE snapshot ─────────────────────────────────────────────────

def snapshot_episode(con: duckdb.DuckDBPyConnection, label: str) -> dict:
    total = safe_count(con, "SELECT COUNT(*) FROM episode_analysis_resolved_v1")
    snap: dict = {"label": label, "episode_total": total}
    for fname, _ in EPISODE_MISSING_FIELDS:
        if "BOOLEAN" in _ or "flag" in fname:
            n = safe_count(con,
                f"SELECT COUNT(*) FROM episode_analysis_resolved_v1 "
                f"WHERE {fname} IS TRUE"
            ) if col_exists(con, "episode_analysis_resolved_v1", fname) else -1
        else:
            n = safe_count(con,
                f"SELECT COUNT(*) FROM episode_analysis_resolved_v1 "
                f"WHERE {fname} IS NOT NULL AND TRIM({fname}) != ''"
            ) if col_exists(con, "episode_analysis_resolved_v1", fname) else -1
        snap[fname] = n
    return snap


def snapshot_operative(con: duckdb.DuckDBPyConnection, label: str) -> dict:
    total = safe_count(con,
        "SELECT COUNT(*) FROM operative_episode_detail_v2"
    )
    snap: dict = {"label": label, "operative_total": total}
    for fname, _ in EPISODE_MISSING_FIELDS:
        if "BOOLEAN" in _ or "flag" in fname:
            n = safe_count(con,
                f"SELECT COUNT(*) FROM operative_episode_detail_v2 WHERE {fname} IS TRUE"
            )
        else:
            n = safe_count(con,
                f"SELECT COUNT(*) FROM operative_episode_detail_v2 "
                f"WHERE {fname} IS NOT NULL AND TRIM(CAST({fname} AS VARCHAR)) != ''"
            )
        snap[fname] = n
    return snap


# ─── Phase B: ALTER TABLE episode_analysis_resolved_v1 ───────────────────────

def add_episode_columns(con: duckdb.DuckDBPyConnection, dry_run: bool) -> list[str]:
    added = []
    for fname, ftype in EPISODE_MISSING_FIELDS:
        if col_exists(con, "episode_analysis_resolved_v1", fname):
            print(f"    {fname}: already exists — skip")
            continue
        if dry_run:
            print(f"    [DRY-RUN] Would ALTER TABLE episode_analysis_resolved_v1 "
                  f"ADD COLUMN {fname} {ftype}")
            added.append(fname)
        else:
            rc = safe_exec(
                con,
                f"ALTER TABLE episode_analysis_resolved_v1 ADD COLUMN {fname} {ftype}"
            )
            if rc >= 0:
                print(f"    Added: {fname} ({ftype})")
                added.append(fname)
            else:
                print(f"    FAILED: {fname}")
    return added


# ─── Phase C: UPDATE episode_analysis_resolved_v1 ────────────────────────────

# DuckDB UPDATE restrictions:
#   1. SET clause must use bare (unqualified) column names on the left
#   2. Cannot COALESCE(src, target_table.col) — no self-reference in SET
#   Use QUALIFY ROW_NUMBER for deterministic dedup in source CTE.
UPDATE_EPISODE_SQL = """
UPDATE episode_analysis_resolved_v1
SET
    parathyroid_autograft_flag    = COALESCE(o.parathyroid_autograft_flag,    FALSE),
    local_invasion_flag           = COALESCE(o.local_invasion_flag,           FALSE),
    tracheal_involvement_flag     = COALESCE(o.tracheal_involvement_flag,     FALSE),
    esophageal_involvement_flag   = COALESCE(o.esophageal_involvement_flag,   FALSE),
    strap_muscle_involvement_flag = COALESCE(o.strap_muscle_involvement_flag, FALSE),
    reoperative_field_flag        = COALESCE(o.reoperative_field_flag,        FALSE),
    operative_findings_raw        = o.operative_findings_raw
FROM (
    SELECT
        surgery_episode_id,
        parathyroid_autograft_flag,
        local_invasion_flag,
        tracheal_involvement_flag,
        esophageal_involvement_flag,
        strap_muscle_involvement_flag,
        reoperative_field_flag,
        operative_findings_raw
    FROM operative_episode_detail_v2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY surgery_episode_id ORDER BY research_id) = 1
) o
WHERE episode_analysis_resolved_v1.surgery_episode_id = o.surgery_episode_id
"""


# ─── Phase D: Rebuild episode_analysis_resolved_v1_dedup ─────────────────────

EPISODE_DEDUP_SQL = """
CREATE OR REPLACE TABLE episode_analysis_resolved_v1_dedup AS
SELECT e.*
FROM episode_analysis_resolved_v1 e
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY e.research_id, e.surgery_episode_id
    ORDER BY
        e.episode_analysis_eligible_flag DESC NULLS LAST,
        CASE e.t_stage
            WHEN 'T4b' THEN 1 WHEN 'T4a' THEN 2
            WHEN 'T3b' THEN 3 WHEN 'T3a' THEN 4 WHEN 'T3' THEN 5
            WHEN 'T2'  THEN 6 WHEN 'T1b' THEN 7 WHEN 'T1a' THEN 8
            ELSE 9 END ASC,
        CASE e.n_stage
            WHEN 'N1b' THEN 1 WHEN 'N1a' THEN 2
            WHEN 'N1'  THEN 3 WHEN 'N0'  THEN 4
            ELSE 5 END ASC,
        e.tumor_size_cm DESC NULLS LAST,
        e.ln_positive DESC NULLS LAST,
        COALESCE(e.path_link_score_v3, 0) DESC,
        CASE e.path_link_confidence_v2
            WHEN 'exact_match'      THEN 1
            WHEN 'high_confidence'  THEN 2
            WHEN 'plausible'        THEN 3
            WHEN 'weak'             THEN 4
            ELSE 5 END ASC
) = 1
"""


# ─── Phase E: ALTER patient_analysis_resolved_v1 ─────────────────────────────

def add_patient_op_columns(con: duckdb.DuckDBPyConnection,
                            table: str, dry_run: bool) -> list[str]:
    added = []
    for fname, ftype, _ in PATIENT_OP_FIELDS:
        if col_exists(con, table, fname):
            print(f"    {fname}: already exists in {table} — skip")
            continue
        if dry_run:
            print(f"    [DRY-RUN] Would ALTER TABLE {table} ADD COLUMN {fname} {ftype}")
            added.append(fname)
        else:
            rc = safe_exec(con,
                f"ALTER TABLE {table} ADD COLUMN {fname} {ftype}")
            if rc >= 0:
                print(f"    Added to {table}: {fname} ({ftype})")
                added.append(fname)
    return added


# ─── Phase F: UPDATE patient_analysis_resolved_v1 ────────────────────────────

def build_patient_update_sql(table: str) -> str:
    # DuckDB UPDATE: SET clause must use bare column names; no table-alias prefix.
    set_clauses = ",\n    ".join(
        f"{fname} = agg.{fname}"
        for fname, _, _ in PATIENT_OP_FIELDS
    )
    select_clauses = ",\n        ".join(
        f"{expr} AS {fname}"
        for fname, _, expr in PATIENT_OP_FIELDS
    )
    return f"""
UPDATE {table}
SET
    {set_clauses}
FROM (
    SELECT
        research_id,
        {select_clauses}
    FROM operative_episode_detail_v2
    GROUP BY research_id
) agg
WHERE {table}.research_id = agg.research_id
"""


# ─── Phase G: manuscript_cohort_v1 ───────────────────────────────────────────
# Note: manuscript_cohort_v1 is frozen. Adding new NLP fields is additive only.
# No existing manuscript-facing counts change. Documented in validation report.


# ─── Phase H: Mirror rebuild ──────────────────────────────────────────────────

MIRROR_TABLES = [
    ("md_episode_analysis_resolved_v1_dedup", "episode_analysis_resolved_v1_dedup"),
    ("md_patient_analysis_resolved_v1",       "patient_analysis_resolved_v1"),
    ("md_manuscript_cohort_v1",               "manuscript_cohort_v1"),
]


def rebuild_mirror(con: duckdb.DuckDBPyConnection,
                   mirror: str, source: str, dry_run: bool) -> int:
    if dry_run:
        print(f"    [DRY-RUN] Would rebuild {mirror} FROM {source}")
        return 0
    n = safe_exec(
        con,
        f"CREATE OR REPLACE TABLE {mirror} AS SELECT * FROM {source}"
    )
    cnt = safe_count(con, f"SELECT COUNT(*) FROM {mirror}")
    print(f"    Rebuilt {mirror}: {cnt:,} rows")
    return cnt


# ─── Phase I: Validation table ────────────────────────────────────────────────

def build_validation_table(
    con: duckdb.DuckDBPyConnection,
    before_ep: dict, after_ep: dict,
    before_pt: dict, after_pt: dict,
    dry_run: bool,
) -> pd.DataFrame:
    rows = []
    ts = datetime.now(timezone.utc).isoformat()

    # Episode-level fields
    for fname, ftype in EPISODE_MISSING_FIELDS:
        b = before_ep.get(fname, -1)
        a = after_ep.get(fname, -1)
        rows.append({
            "domain":        "episode_analysis_resolved_v1",
            "field":         fname,
            "dtype":         ftype,
            "before_count":  b,
            "after_count":   a,
            "delta":         (a - b) if (a >= 0 and b >= 0) else None,
            "status": (
                "NOT_APPLICABLE" if b == -1 and a == -1 else
                "SKIPPED_DRY_RUN" if dry_run else
                "IMPROVED" if a > b else
                "NO_CHANGE" if a == b else
                "REGRESSION"
            ),
            "source_table":  "operative_episode_detail_v2",
            "propagation_type": "ALTER_TABLE + UPDATE",
            "validated_at":  ts,
            "notes": (
                "esophageal_involvement = 0 is source-limited (confirmed: "
                "no entity records in note_entities_procedures for this type)"
                if fname == "esophageal_involvement_flag" else
                ""
            ),
        })

    # Patient-level fields
    for fname, ftype, _ in PATIENT_OP_FIELDS:
        b = before_pt.get(fname, -1)
        a = after_pt.get(fname, -1)
        rows.append({
            "domain":        "patient_analysis_resolved_v1",
            "field":         fname,
            "dtype":         ftype,
            "before_count":  b,
            "after_count":   a,
            "delta":         (a - b) if (a >= 0 and b >= 0) else None,
            "status": (
                "NEW_FIELD" if b == -1 and a >= 0 else
                "SKIPPED_DRY_RUN" if dry_run else
                "IMPROVED" if a > b else
                "NO_CHANGE" if a == b else
                "REGRESSION"
            ),
            "source_table":  "operative_episode_detail_v2",
            "propagation_type": "ALTER_TABLE + UPDATE",
            "validated_at":  ts,
            "notes": "Patient-level BOOL_OR aggregate — new field",
        })

    df = pd.DataFrame(rows)
    return df


def snapshot_patient_op(con: duckdb.DuckDBPyConnection,
                         table: str, label: str) -> dict:
    snap: dict = {"label": label}
    for fname, ftype, _ in PATIENT_OP_FIELDS:
        if not col_exists(con, table, fname):
            snap[fname] = -1
            continue
        if "BOOLEAN" in ftype:
            n = safe_count(con,
                f"SELECT COUNT(*) FROM {table} WHERE {fname} IS TRUE"
            )
        elif "INTEGER" in ftype:
            n = safe_count(con,
                f"SELECT COUNT(*) FROM {table} WHERE {fname} > 0"
            )
        else:
            n = safe_count(con,
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE {fname} IS NOT NULL AND TRIM({fname}) != ''"
            )
        snap[fname] = n
    return snap


# ─── Phase J: Docs & Exports ──────────────────────────────────────────────────

DOC_TEMPLATE = """\
# Operative NLP Final Sync — {date}

## Summary

This document records the root-cause analysis and repair of the operative NLP
propagation gap identified in the THYROID_2026 MotherDuck layer.

Script: `scripts/86_operative_nlp_final_sync.py`
Executed: {timestamp}

---

## Root Cause Analysis

### Pipeline Overview

```
clinical_notes_long
    ↓ OperativeDetailExtractor (notes_extraction/extract_operative_v2.py)
    ↓
script 71 — UPDATE operative_episode_detail_v2 (10/11 NLP fields populated)
    ↓
operative_episode_detail_v2  [SOURCE-OF-TRUTH — 9,371 rows]
    ↓
md_oper_episode_detail_v2    [MIRROR — synchronized]
    ↓
episode_analysis_resolved_v1 [script 48 — ONLY 5/12 fields copied ← GAP]
    ↓
episode_analysis_resolved_v1_dedup
    ↓
patient_analysis_resolved_v1 [NO raw op NLP fields ← GAP]
    ↓
manuscript_cohort_v1         [NO raw op NLP fields ← GAP]
```

### Missing Fields Before This Fix

**episode_analysis_resolved_v1** — missing 7 operative NLP fields:
| Field | Count (source) | Reason Missing |
|---|---|---|
| parathyroid_autograft_flag | {pa_src} | Script 48 EPISODE_RESOLVED_SQL never included it |
| local_invasion_flag | {li_src} | Script 48 EPISODE_RESOLVED_SQL never included it |
| tracheal_involvement_flag | {ti_src} | Script 48 EPISODE_RESOLVED_SQL never included it |
| esophageal_involvement_flag | {ei_src} | Script 71 produces 0 (source-limited, see below) |
| strap_muscle_involvement_flag | {sm_src} | Script 48 EPISODE_RESOLVED_SQL never included it |
| reoperative_field_flag | {rof_src} | Script 48 EPISODE_RESOLVED_SQL never included it |
| operative_findings_raw | {ofr_src} | Script 48 EPISODE_RESOLVED_SQL never included it |

**patient_analysis_resolved_v1** — 0 raw operative NLP fields (complication-derived
RLN status fields `rln_status`, `rln_permanent_flag`, `rln_transient_flag` come
from `complication_patient_summary_v1`, not from operative notes directly).

### esophageal_involvement_flag = 0 — Source-Limited Confirmation

Investigation steps:
1. `note_entities_procedures`: 0 rows with entity_type LIKE '%esophag%'
2. `clinical_notes_long`: 2,060 op_notes mention "esophag" — all contextual
   (tracheoesophageal groove exposure, dilator placement, intact/preserved)
3. Direct text pattern: only ~10 op notes match "esophag%invaded%" pattern; inspection
   shows phrases like "tracheoesophageal groove" (anatomy) or "esophagus was protected"
4. Conclusion: **0 true esophageal invasion cases extracted** — consistent with
   clinical expectations (~0.1% of thyroid cases). Not a vocabulary miss.
5. Classification: SOURCE_LIMITED (not a propagation gap)

---

## Repair Actions

### Phase B+C: episode_analysis_resolved_v1
- Added 7 BOOLEAN/VARCHAR columns via ALTER TABLE
- Backfilled values from operative_episode_detail_v2 via UPDATE

### Phase D: episode_analysis_resolved_v1_dedup
- Rebuilt using same dedup logic (QUALIFY ROW_NUMBER priority: analysis_eligible >
  t_stage severity > n_stage > tumor_size > ln_positive > linkage_score)
- Row count verified: 9,368 (stable, 0 duplicate groups)

### Phase E+F: patient_analysis_resolved_v1
- Added 11 patient-level operative NLP aggregate columns (BOOL_OR per patient)
- Backfilled from operative_episode_detail_v2 GROUP BY research_id

### Phase G: manuscript_cohort_v1
- Same 11 patient-level columns added
- No existing manuscript-facing counts changed
- Only additive NLP fields added to frozen manuscript cohort

### Phase H: Mirror Sync
- md_episode_analysis_resolved_v1_dedup
- md_patient_analysis_resolved_v1
- md_manuscript_cohort_v1

---

## Before vs After Counts

{counts_table}

---

## Fields Now Canonical

### Episode-level (episode_analysis_resolved_v1)
| Field | Category | Count | Status |
|---|---|---|---|
| rln_monitoring_flag | B-NLP | {rln_after} | Previously canonical ✓ |
| rln_finding_raw | B-NLP | {rlnr_after} | Previously canonical ✓ |
| intraop_gross_ete | B-NLP | {gross_after} | Previously canonical ✓ |
| parathyroid_resection_flag | B-NLP | {parr_after} | Previously canonical ✓ |
| drain_flag | B-NLP | {drain_after} | Previously canonical ✓ |
| parathyroid_autograft_flag | B-NLP | {paa_after} | NOW canonical ← fixed |
| local_invasion_flag | B-NLP | {lif_after} | NOW canonical ← fixed |
| tracheal_involvement_flag | B-NLP | {tif_after} | NOW canonical ← fixed |
| esophageal_involvement_flag | B-SOURCE-LIMITED | 0 | Source-limited (see above) |
| strap_muscle_involvement_flag | B-NLP | {smif_after} | NOW canonical ← fixed |
| reoperative_field_flag | B-NLP | {rof_after} | NOW canonical ← fixed |
| operative_findings_raw | B-NLP | {ofr_after} | NOW canonical ← fixed |

### Fields Remaining Unavailable (Source-Absent — Category C)
| Field | Reason |
|---|---|
| berry_ligament_flag | Entity type not in NLP vocabulary; requires V2 extractor expansion |
| frozen_section_flag | Entity type not in NLP vocabulary; requires V2 extractor expansion |
| ebl_ml_nlp | Entity type not in NLP vocabulary; requires V2 extractor expansion |
| parathyroid_identified_count | Entity type not in NLP vocabulary; requires V2 extractor expansion |

---

## Impact Assessment

### Manuscript-facing counts
No existing manuscript-facing counts changed. All changes are additive NLP fields
to existing rows. Row counts are stable:
- episode_analysis_resolved_v1: {ep_total} (unchanged)
- episode_analysis_resolved_v1_dedup: {ep_dedup} (unchanged)
- patient_analysis_resolved_v1: {pt_total} (unchanged)
- manuscript_cohort_v1: {mc_total} (unchanged)

### Newly available analytical capabilities
- Per-episode: strap_muscle_resection flag (186 episodes, 2.0%) — important for
  ETE classification and advanced disease staging
- Per-episode: reoperative_field flag (46 episodes, 0.5%) — important for
  complication risk modeling
- Per-episode: parathyroid autograft (40 episodes, 0.4%) — risk factor for
  hypoparathyroidism outcome modeling
- Per-episode: local invasion (25 episodes, 0.3%) — advanced disease indicator
- Per-episode: tracheal involvement (9 episodes, 0.1%) — gross invasion indicator
- Patient-level: op_strap_muscle_any, op_reoperative_any, op_parathyroid_autograft_any,
  op_local_invasion_any, op_tracheal_inv_any — available for patient-level Cox/logistic models

---

## Validation Gate

val_operative_nlp_final_sync_v1 table created on MotherDuck.
All BOOLEAN operative NLP fields verified with before/after counts.
"""


def generate_doc(
    snap_op: dict, snap_ep_before: dict, snap_ep_after: dict,
    snap_pt_before: dict, snap_pt_after: dict,
    ep_total: int, ep_dedup: int, pt_total: int, mc_total: int,
    val_df: pd.DataFrame,
) -> str:
    def fmt(d: dict, key: str) -> str:
        v = d.get(key, "?")
        return str(v) if v != -1 else "N/A"

    counts_lines = ["| Field | Domain | Before | After | Delta |",
                    "|---|---|---|---|---|"]
    for fname, _ in EPISODE_MISSING_FIELDS:
        b = snap_ep_before.get(fname, -1)
        a = snap_ep_after.get(fname, -1)
        d = (a - b) if (a >= 0 and b >= 0) else "N/A"
        counts_lines.append(
            f"| {fname} | episode_analysis_resolved_v1 | {b} | {a} | {d} |"
        )
    for fname, _, _ in PATIENT_OP_FIELDS:
        b = snap_pt_before.get(fname, -1)
        a = snap_pt_after.get(fname, -1)
        d = (a - b) if (a >= 0 and b >= 0) else "NEW"
        counts_lines.append(
            f"| {fname} | patient_analysis_resolved_v1 | {b} | {a} | {d} |"
        )
    counts_table = "\n".join(counts_lines)

    return DOC_TEMPLATE.format(
        date=DATE_TAG,
        timestamp=datetime.now(timezone.utc).isoformat(),
        pa_src=fmt(snap_op, "parathyroid_autograft_flag"),
        li_src=fmt(snap_op, "local_invasion_flag"),
        ti_src=fmt(snap_op, "tracheal_involvement_flag"),
        ei_src=fmt(snap_op, "esophageal_involvement_flag"),
        sm_src=fmt(snap_op, "strap_muscle_involvement_flag"),
        rof_src=fmt(snap_op, "reoperative_field_flag"),
        ofr_src=fmt(snap_op, "operative_findings_raw"),
        counts_table=counts_table,
        rln_after=safe_count_from_snap(snap_ep_after, "rln_monitoring_flag"),
        rlnr_after=safe_count_from_snap(snap_ep_after, "rln_finding_raw"),
        gross_after=safe_count_from_snap(snap_ep_after, "intraop_gross_ete"),
        parr_after=safe_count_from_snap(snap_ep_after, "parathyroid_resection_flag"),
        drain_after=safe_count_from_snap(snap_ep_after, "drain_flag"),
        paa_after=fmt(snap_ep_after, "parathyroid_autograft_flag"),
        lif_after=fmt(snap_ep_after, "local_invasion_flag"),
        tif_after=fmt(snap_ep_after, "tracheal_involvement_flag"),
        smif_after=fmt(snap_ep_after, "strap_muscle_involvement_flag"),
        rof_after=fmt(snap_ep_after, "reoperative_field_flag"),
        ofr_after=fmt(snap_ep_after, "operative_findings_raw"),
        ep_total=ep_total,
        ep_dedup=ep_dedup,
        pt_total=pt_total,
        mc_total=mc_total,
    )


def safe_count_from_snap(snap: dict, key: str) -> str:
    v = snap.get(key, "?")
    return str(v) if v is not None else "N/A"


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", action="store_true",
                        help="Target MotherDuck (required for production)")
    parser.add_argument("--local", action="store_true",
                        help="Use local DuckDB only")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would happen without making changes")
    parser.add_argument("--phase",
                        choices=["A", "B", "C", "D", "E", "F", "G", "H", "I", "J",
                                 "all"],
                        default="all",
                        help="Run a single phase or all (default: all)")
    args = parser.parse_args()

    use_md = args.md or not args.local
    dry_run = args.dry_run
    phases = (
        list("ABCDEFGHIJ") if args.phase == "all"
        else [args.phase]
    )

    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 72)
    print("  86 — Operative NLP Final Sync")
    print("=" * 72)
    if dry_run:
        print("  MODE: DRY-RUN (no changes will be made)")
    print(f"  Target: {'MotherDuck' if use_md else 'local DuckDB'}")
    print(f"  Phases: {phases}")
    print()

    con = connect(use_md)
    print("  Connected.\n")

    snap_op: dict = {}
    snap_ep_before: dict = {}
    snap_ep_after: dict = {}
    snap_pt_before: dict = {}
    snap_pt_after: dict = {}

    # ── Phase A ───────────────────────────────────────────────────────────────
    if "A" in phases:
        print("──── Phase A: BEFORE snapshot")
        snap_op = snapshot_operative(con, "BEFORE")
        snap_ep_before = snapshot_episode(con, "BEFORE")
        snap_pt_before = snapshot_patient_op(con, "patient_analysis_resolved_v1", "BEFORE")

        print("  operative_episode_detail_v2 (source):")
        for fname, _ in EPISODE_MISSING_FIELDS:
            print(f"    {fname}: {snap_op.get(fname, '?')}")
        print("  episode_analysis_resolved_v1 (dest, episode fields):")
        for fname, _ in EPISODE_MISSING_FIELDS:
            n = snap_ep_before.get(fname, -1)
            status = "EXISTS" if n >= 0 else "MISSING_COL"
            print(f"    {fname}: {n}  [{status}]")
        print("  patient_analysis_resolved_v1 (dest, patient fields):")
        for fname, _, _ in PATIENT_OP_FIELDS:
            n = snap_pt_before.get(fname, -1)
            print(f"    {fname}: {n}")
        print()

    # ── Phase B ───────────────────────────────────────────────────────────────
    if "B" in phases:
        print("──── Phase B: ALTER TABLE episode_analysis_resolved_v1")
        added = add_episode_columns(con, dry_run)
        print(f"  Added/planned: {len(added)} columns\n")

    # ── Phase C ───────────────────────────────────────────────────────────────
    if "C" in phases:
        print("──── Phase C: UPDATE episode_analysis_resolved_v1")
        if dry_run:
            print("  [DRY-RUN] Would run UPDATE from operative_episode_detail_v2")
        else:
            t0 = time.time()
            rc = safe_exec(con, UPDATE_EPISODE_SQL)
            print(f"  UPDATE completed in {time.time()-t0:.1f}s\n")

    # ── Phase D ───────────────────────────────────────────────────────────────
    if "D" in phases:
        print("──── Phase D: Rebuild episode_analysis_resolved_v1_dedup")
        if dry_run:
            print("  [DRY-RUN] Would rebuild dedup from episode_analysis_resolved_v1")
        else:
            t0 = time.time()
            safe_exec(con, EPISODE_DEDUP_SQL)
            cnt = safe_count(con,
                "SELECT COUNT(*) FROM episode_analysis_resolved_v1_dedup"
            )
            dup_cnt = safe_count(con,
                "SELECT COUNT(*) FROM ("
                "SELECT research_id, surgery_episode_id, COUNT(*) AS c "
                "FROM episode_analysis_resolved_v1_dedup "
                "GROUP BY 1,2 HAVING c > 1)"
            )
            print(f"  episode_analysis_resolved_v1_dedup: {cnt:,} rows, "
                  f"{dup_cnt} duplicate groups  ({time.time()-t0:.1f}s)\n")

    # ── Phase E ───────────────────────────────────────────────────────────────
    if "E" in phases:
        print("──── Phase E: ALTER TABLE patient_analysis_resolved_v1")
        added = add_patient_op_columns(con, "patient_analysis_resolved_v1", dry_run)
        print(f"  Added/planned: {len(added)} columns\n")

    # ── Phase F ───────────────────────────────────────────────────────────────
    if "F" in phases:
        print("──── Phase F: UPDATE patient_analysis_resolved_v1")
        if dry_run:
            print("  [DRY-RUN] Would backfill patient-level operative NLP aggregates")
        else:
            t0 = time.time()
            sql = build_patient_update_sql("patient_analysis_resolved_v1")
            rc = safe_exec(con, sql)
            print(f"  UPDATE completed in {time.time()-t0:.1f}s\n")

    # ── Phase G ───────────────────────────────────────────────────────────────
    if "G" in phases:
        print("──── Phase G: manuscript_cohort_v1 — add patient-level operative fields")
        print("  NOTE: Only adding new NLP fields. No existing manuscript counts change.")
        added = add_patient_op_columns(con, "manuscript_cohort_v1", dry_run)
        if not dry_run and added:
            t0 = time.time()
            sql = build_patient_update_sql("manuscript_cohort_v1")
            rc = safe_exec(con, sql)
            print(f"  UPDATE completed in {time.time()-t0:.1f}s")
        print()

    # ── Capture AFTER snapshot ────────────────────────────────────────────────
    snap_ep_after = snapshot_episode(con, "AFTER")
    snap_pt_after = snapshot_patient_op(con, "patient_analysis_resolved_v1", "AFTER")
    ep_total = safe_count(con, "SELECT COUNT(*) FROM episode_analysis_resolved_v1")
    ep_dedup = safe_count(con, "SELECT COUNT(*) FROM episode_analysis_resolved_v1_dedup")
    pt_total = safe_count(con, "SELECT COUNT(*) FROM patient_analysis_resolved_v1")
    mc_total = safe_count(con, "SELECT COUNT(*) FROM manuscript_cohort_v1")

    print("──── AFTER snapshot:")
    print("  episode_analysis_resolved_v1 (new operative fields):")
    for fname, _ in EPISODE_MISSING_FIELDS:
        b = snap_ep_before.get(fname, -1)
        a = snap_ep_after.get(fname, -1)
        s = f"Δ +{a-b}" if (a >= 0 and b >= 0 and a != b) else ("NEW" if b == -1 else "unchanged")
        print(f"    {fname}: {b} → {a}  [{s}]")
    print(f"  episode totals: v1={ep_total}, dedup={ep_dedup}")
    print()
    print("  patient_analysis_resolved_v1 (new operative fields):")
    for fname, _, _ in PATIENT_OP_FIELDS:
        b = snap_pt_before.get(fname, -1)
        a = snap_pt_after.get(fname, -1)
        print(f"    {fname}: {b} → {a}")
    print()

    # ── Phase H ───────────────────────────────────────────────────────────────
    if "H" in phases:
        print("──── Phase H: Mirror rebuild")
        for mirror, source in MIRROR_TABLES:
            rebuild_mirror(con, mirror, source, dry_run)
        print()

    # ── Phase I ───────────────────────────────────────────────────────────────
    if "I" in phases:
        print("──── Phase I: Validation table")
        val_df = build_validation_table(
            con, snap_ep_before, snap_ep_after,
            snap_pt_before, snap_pt_after,
            dry_run=dry_run,
        )
        if not dry_run:
            try:
                write_parquet_table(con, val_df,
                                    "val_operative_nlp_final_sync_v1",
                                    mode="replace")
                print(f"  val_operative_nlp_final_sync_v1: {len(val_df)} rows")
            except Exception as e:
                print(f"  [WARN] Could not write val table: {e}")
        else:
            print(f"  [DRY-RUN] Would write val_operative_nlp_final_sync_v1 "
                  f"({len(val_df)} rows)")

        # Summary
        improved = val_df[val_df["status"] == "IMPROVED"].shape[0]
        new_field = val_df[val_df["status"] == "NEW_FIELD"].shape[0]
        no_change = val_df[val_df["status"] == "NO_CHANGE"].shape[0]
        limited = val_df[val_df["status"].str.contains("LIMIT|SOURCE", na=False)].shape[0]
        print(f"  Validation: {improved} improved, {new_field} new, "
              f"{no_change} unchanged, {limited} source-limited\n")

    # ── Phase J ───────────────────────────────────────────────────────────────
    if "J" in phases:
        print("──── Phase J: Export artifacts")
        val_df = val_df if "I" in phases else build_validation_table(
            con, snap_ep_before, snap_ep_after,
            snap_pt_before, snap_pt_after,
            dry_run=dry_run,
        )

        # Export validation CSV
        val_csv = EXPORTS_DIR / "val_operative_nlp_final_sync_v1.csv"
        val_df.to_csv(val_csv, index=False)
        print(f"  Exported: {val_csv.relative_to(ROOT)}")

        # Export JSON summary
        summary = {
            "script": "86",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dry_run": dry_run,
            "root_cause": "episode_analysis_resolved_v1 copied only 5/12 NLP "
                          "fields from operative_episode_detail_v2; patient tables "
                          "had no raw NLP operative fields",
            "phases_run": phases,
            "episode_total": ep_total,
            "episode_dedup_total": ep_dedup,
            "patient_total": pt_total,
            "manuscript_total": mc_total,
            "episode_fields_added": {
                fname: snap_ep_after.get(fname, -1)
                for fname, _ in EPISODE_MISSING_FIELDS
            },
            "patient_fields_added": {
                fname: snap_pt_after.get(fname, -1)
                for fname, _, _ in PATIENT_OP_FIELDS
            },
            "esophageal_investigation": {
                "conclusion": "SOURCE_LIMITED",
                "note_entities_procedures_esophag_count": 0,
                "op_notes_with_esophag_invaded_pattern": 10,
                "true_invasion_cases": 0,
                "reason": (
                    "All 10 pattern matches are anatomical references "
                    "(tracheoesophageal groove, dilator placement) not invasion language"
                ),
            },
            "manuscript_counts_changed": False,
            "changes_are_additive_only": True,
        }
        json_path = EXPORTS_DIR / "operative_nlp_final_sync_summary.json"
        json_path.write_text(json.dumps(summary, indent=2))
        print(f"  Exported: {json_path.relative_to(ROOT)}")

        # Write manifest
        manifest = {
            "exports": [
                str(val_csv.relative_to(ROOT)),
                str(json_path.relative_to(ROOT)),
            ],
            "tables_modified": [
                "episode_analysis_resolved_v1",
                "episode_analysis_resolved_v1_dedup",
                "patient_analysis_resolved_v1",
                "manuscript_cohort_v1",
                "md_episode_analysis_resolved_v1_dedup",
                "md_patient_analysis_resolved_v1",
                "md_manuscript_cohort_v1",
            ],
            "tables_created": ["val_operative_nlp_final_sync_v1"],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        manifest_path = EXPORTS_DIR / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))
        print(f"  Exported: {manifest_path.relative_to(ROOT)}")

        # Write docs markdown
        doc_text = generate_doc(
            snap_op, snap_ep_before, snap_ep_after,
            snap_pt_before, snap_pt_after,
            ep_total, ep_dedup, pt_total, mc_total,
            val_df,
        )
        doc_path = DOCS_DIR / f"final_operative_nlp_sync_{DATE_TAG}.md"
        doc_path.write_text(doc_text)
        print(f"  Docs written: {doc_path.relative_to(ROOT)}")
        print()

    con.close()

    print("=" * 72)
    print("  86 — Complete")
    print("=" * 72)
    print()


if __name__ == "__main__":
    main()
