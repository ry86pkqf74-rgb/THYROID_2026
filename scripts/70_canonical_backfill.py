#!/usr/bin/env python3
"""
70_canonical_backfill.py -- Backfill canonical episode tables from refined/linkage sources

Targeted remediation script that propagates already-solved values from sidecar
tables into the canonical episode layer:

  1. RAI dose:        extracted_rai_dose_refined_v1 -> rai_treatment_episode_v2.dose_mci
  2. RAS flag:        extracted_ras_patient_summary_v1 -> molecular_test_episode_v2.ras_flag
  3. Linkage IDs:     V3 linkage tables (score_rank=1) -> canonical episode linkage columns

All updates are idempotent (IS NULL guard) and use only deterministic joins.
Ambiguous cases (multiple V3 candidates at rank=1) route to a review table.

Supports --md flag (required for MotherDuck deployment).
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
sys.path.insert(0, str(ROOT))

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")
EXPORT_DIR = ROOT / "exports" / f"canonical_backfill_{TIMESTAMP}"


def section(title: str) -> None:
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}\n")


def table_available(con: duckdb.DuckDBPyConnection, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        return True
    except Exception:
        return False


def count_filled(con: duckdb.DuckDBPyConnection, tbl: str, col: str) -> tuple[int, int]:
    """Return (filled_count, total_count) for a column."""
    row = con.execute(
        f"SELECT COUNT({col}), COUNT(*) FROM {tbl} WHERE {col} IS NOT NULL"
    ).fetchone()
    assert row is not None
    total_row = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()
    assert total_row is not None
    return (row[0], total_row[0])


def count_true(con: duckdb.DuckDBPyConnection, tbl: str, col: str) -> tuple[int, int]:
    """Return (true_count, total_count) for a boolean column."""
    row = con.execute(
        f"SELECT SUM(CASE WHEN LOWER(CAST({col} AS VARCHAR))='true' THEN 1 ELSE 0 END), "
        f"COUNT(*) FROM {tbl}"
    ).fetchone()
    assert row is not None
    return (row[0] or 0, row[1])


# ── SQL: Backfill RAI dose ──────────────────────────────────────────────────
BACKFILL_RAI_DOSE_SQL = """
UPDATE rai_treatment_episode_v2 AS r
SET dose_mci = d.dose_mci
FROM (
    SELECT research_id, rai_date, dose_mci,
           ROW_NUMBER() OVER (
               PARTITION BY research_id, rai_date
               ORDER BY source_reliability DESC, dose_mci DESC
           ) AS rn
    FROM extracted_rai_dose_refined_v1
    WHERE dose_mci IS NOT NULL
) d
WHERE r.research_id = d.research_id
  AND r.resolved_rai_date = d.rai_date
  AND d.rn = 1
  AND r.dose_mci IS NULL
"""

# ── SQL: Backfill RAS flag ──────────────────────────────────────────────────
BACKFILL_RAS_FLAG_SQL = """
UPDATE molecular_test_episode_v2 AS m
SET ras_flag = TRUE
FROM extracted_ras_patient_summary_v1 s
WHERE m.research_id = s.research_id
  AND s.ras_positive IS TRUE
  AND (m.ras_flag IS NULL OR LOWER(CAST(m.ras_flag AS VARCHAR)) != 'true')
"""

# ── SQL: Propagate linkage IDs ──────────────────────────────────────────────

BACKFILL_MOLECULAR_FNA_LINK_SQL = """
UPDATE molecular_test_episode_v2 AS m
SET linked_fna_episode_id = CAST(l.fna_episode_id AS VARCHAR)
FROM fna_molecular_linkage_v3 l
WHERE m.research_id = l.research_id
  AND m.molecular_episode_id = l.molecular_episode_id
  AND l.score_rank = 1
  AND m.linked_fna_episode_id IS NULL
"""

BACKFILL_FNA_MOLECULAR_LINK_SQL = """
UPDATE fna_episode_master_v2 AS f
SET linked_molecular_episode_id = CAST(l.molecular_episode_id AS VARCHAR)
FROM fna_molecular_linkage_v3 l
WHERE f.research_id = l.research_id
  AND f.fna_episode_id = l.fna_episode_id
  AND l.score_rank = 1
  AND f.linked_molecular_episode_id IS NULL
"""

BACKFILL_RAI_SURGERY_LINK_SQL = """
UPDATE rai_treatment_episode_v2 AS r
SET linked_surgery_episode_id = CAST(l.surgery_episode_id AS VARCHAR)
FROM pathology_rai_linkage_v3 l
WHERE r.research_id = l.research_id
  AND r.rai_episode_id = l.rai_episode_id
  AND l.score_rank = 1
  AND r.linked_surgery_episode_id IS NULL
"""

BACKFILL_IMAGING_FNA_LINK_SQL = """
UPDATE imaging_nodule_long_v2 AS i
SET linked_fna_episode_id = CAST(l.fna_episode_id AS VARCHAR)
FROM imaging_fna_linkage_v3 l
WHERE i.research_id = l.research_id
  AND i.nodule_id = l.nodule_id
  AND l.score_rank = 1
  AND i.linked_fna_episode_id IS NULL
"""

# ── SQL: Ambiguity review table ─────────────────────────────────────────────
AMBIGUITY_REVIEW_SQL = """
CREATE OR REPLACE TABLE canonical_backfill_ambiguity_review AS
SELECT
    'fna_molecular' AS linkage_type,
    CAST(research_id AS VARCHAR) AS research_id,
    CAST(fna_episode_id AS VARCHAR) AS source_episode_id,
    CAST(molecular_episode_id AS VARCHAR) AS target_episode_id,
    linkage_score,
    score_rank,
    n_candidates
FROM fna_molecular_linkage_v3
WHERE n_candidates > 1

UNION ALL

SELECT
    'pathology_rai',
    CAST(research_id AS VARCHAR),
    CAST(surgery_episode_id AS VARCHAR),
    CAST(rai_episode_id AS VARCHAR),
    linkage_score,
    score_rank,
    n_candidates
FROM pathology_rai_linkage_v3
WHERE n_candidates > 1

UNION ALL

SELECT
    'imaging_fna',
    CAST(research_id AS VARCHAR),
    CAST(nodule_id AS VARCHAR),
    CAST(fna_episode_id AS VARCHAR),
    linkage_score,
    score_rank,
    n_candidates
FROM imaging_fna_linkage_v3
WHERE n_candidates > 1
"""


BACKFILL_STEPS: list[tuple[str, str, str, str]] = [
    ("RAI dose backfill", BACKFILL_RAI_DOSE_SQL,
     "rai_treatment_episode_v2", "dose_mci"),
    ("RAS flag backfill", BACKFILL_RAS_FLAG_SQL,
     "molecular_test_episode_v2", "ras_flag"),
    ("Molecular -> FNA linkage", BACKFILL_MOLECULAR_FNA_LINK_SQL,
     "molecular_test_episode_v2", "linked_fna_episode_id"),
    ("FNA -> Molecular linkage", BACKFILL_FNA_MOLECULAR_LINK_SQL,
     "fna_episode_master_v2", "linked_molecular_episode_id"),
    ("RAI -> Surgery linkage", BACKFILL_RAI_SURGERY_LINK_SQL,
     "rai_treatment_episode_v2", "linked_surgery_episode_id"),
    ("Imaging -> FNA linkage", BACKFILL_IMAGING_FNA_LINK_SQL,
     "imaging_nodule_long_v2", "linked_fna_episode_id"),
]


def run_backfill(con: duckdb.DuckDBPyConnection) -> dict:
    """Execute all backfill steps and return before/after audit dict."""
    audit: dict = {"timestamp": TIMESTAMP, "steps": []}

    for name, sql, tbl, col in BACKFILL_STEPS:
        if not table_available(con, tbl):
            print(f"  SKIP  {name}: {tbl} not found")
            audit["steps"].append({
                "name": name, "status": "skipped", "reason": f"{tbl} not found"
            })
            continue

        is_bool = col == "ras_flag"
        before = count_true(con, tbl, col) if is_bool else count_filled(con, tbl, col)
        print(f"  BEFORE {name}: {before[0]:,}/{before[1]:,} filled")

        source_tbl = sql.split("FROM ")[-1].split()[0].strip()
        if not table_available(con, source_tbl.replace("(", "")):
            parts = sql.split("FROM ")
            if len(parts) >= 3:
                source_tbl = parts[2].split()[0].strip()

        try:
            con.execute(sql)
            after = count_true(con, tbl, col) if is_bool else count_filled(con, tbl, col)
            delta = after[0] - before[0]
            print(f"  AFTER  {name}: {after[0]:,}/{after[1]:,} filled (+{delta:,})")
            audit["steps"].append({
                "name": name, "table": tbl, "column": col,
                "before": before[0], "after": after[0], "total": after[1],
                "delta": delta, "status": "ok",
            })
        except Exception as e:
            print(f"  ERROR  {name}: {e}")
            audit["steps"].append({
                "name": name, "status": "error", "error": str(e)
            })

    # Ambiguity review table
    try:
        con.execute(AMBIGUITY_REVIEW_SQL)
        cnt_row = con.execute(
            "SELECT COUNT(*) FROM canonical_backfill_ambiguity_review"
        ).fetchone()
        assert cnt_row is not None
        cnt = cnt_row[0]
        print(f"\n  Ambiguity review table: {cnt:,} rows")
        audit["ambiguity_review_rows"] = cnt
    except Exception as e:
        print(f"  WARN ambiguity review: {e}")
        audit["ambiguity_review_error"] = str(e)

    return audit


def export_audit(con: duckdb.DuckDBPyConnection, audit: dict) -> None:
    """Export audit results to CSV + JSON."""
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    manifest_path = EXPORT_DIR / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(audit, f, indent=2, default=str)
    print(f"  Manifest: {manifest_path}")

    try:
        df = con.execute(
            "SELECT * FROM canonical_backfill_ambiguity_review"
        ).fetchdf()
        csv_path = EXPORT_DIR / "ambiguity_review.csv"
        df.to_csv(csv_path, index=False)
        print(f"  Ambiguity CSV: {csv_path} ({len(df)} rows)")
    except Exception:
        pass

    for step in audit.get("steps", []):
        if step.get("status") == "ok":
            tbl = step["table"]
            col = step["column"]
            try:
                sample = con.execute(
                    f"SELECT research_id, {col} FROM {tbl} "
                    f"WHERE {col} IS NOT NULL LIMIT 20"
                ).fetchdf()
                csv_path = EXPORT_DIR / f"sample_{tbl}_{col}.csv"
                sample.to_csv(csv_path, index=False)
            except Exception:
                pass


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", action="store_true",
                        help="Execute against MotherDuck (required for production)")
    args = parser.parse_args()

    section("70 -- Canonical Episode Backfill")

    if args.md:
        try:
            import toml  # type: ignore[import-untyped]
            token = toml.load(str(ROOT / ".streamlit" / "secrets.toml"))["MOTHERDUCK_TOKEN"]
        except Exception:
            import os
            token = os.environ.get("MOTHERDUCK_TOKEN")
        if not token:
            print("ERROR: MOTHERDUCK_TOKEN not found")
            sys.exit(1)
        con = duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
        print("  Connected to MotherDuck (thyroid_research_2026)")
    else:
        con = duckdb.connect(str(DB_PATH))
        print(f"  Connected to local DB: {DB_PATH}")

    audit = run_backfill(con)

    section("Export audit artifacts")
    export_audit(con, audit)

    section("Summary")
    ok_count = sum(1 for s in audit["steps"] if s.get("status") == "ok")
    skip_count = sum(1 for s in audit["steps"] if s.get("status") == "skipped")
    err_count = sum(1 for s in audit["steps"] if s.get("status") == "error")
    total_delta = sum(s.get("delta", 0) for s in audit["steps"])
    print(f"  Steps: {ok_count} OK, {skip_count} skipped, {err_count} errors")
    print(f"  Total cells backfilled: {total_delta:,}")

    con.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
