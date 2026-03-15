#!/usr/bin/env python3
"""
105_manuscript_freeze_v1.py — Manuscript Publication Freeze Workflow

Creates an immutable, reproducible snapshot of all manuscript-critical
MotherDuck tables and exports a versioned freeze artifact bundle.

Workflow
────────
  Phase A  Connect to MotherDuck prod; verify source environment
  Phase B  Inventory all manuscript-critical tables; collect row counts
  Phase C  Export each critical table to Parquet + CSV (small tables)
  Phase D  Compute SHA-256 checksums on every export artifact
  Phase E  Write freeze manifest (JSON) with git SHA, timestamps, row
           counts, checksums, MotherDuck DB name, and freeze version
  Phase F  Create frozen-suffix copies on MotherDuck (optional --stamp)
  Phase G  Verify freeze integrity (re-read manifest, compare checksums)

Outputs
───────
  exports/manuscript_freeze_<version>/
    manifest.json              — full provenance + checksums
    table_inventory.csv        — table name, row count, checksum
    rowcount_summary.json      — compact row counts
    metadata.json              — git SHA, timestamp, python version, env
    data/                      — per-table Parquet + small-table CSV
    verification_report.json   — integrity check results

Usage
─────
  # Standard freeze (reads MotherDuck prod)
  .venv/bin/python scripts/105_manuscript_freeze_v1.py --md

  # Dry-run (inventory only, no data export)
  .venv/bin/python scripts/105_manuscript_freeze_v1.py --md --dry-run

  # Stamp frozen copies in MotherDuck (e.g. table_freeze_v1)
  .venv/bin/python scripts/105_manuscript_freeze_v1.py --md --stamp

  # Custom version tag
  .venv/bin/python scripts/105_manuscript_freeze_v1.py --md --version v2

  # Local DuckDB fallback
  .venv/bin/python scripts/105_manuscript_freeze_v1.py

Flags
─────
  --md          Read from MotherDuck prod (recommended for publication)
  --dry-run     Inventory + validation only; no Parquet/CSV export
  --stamp       Create versioned TABLE copies in MotherDuck (e.g. _freeze_v1)
  --version TAG Freeze version tag (default: v1)
  --skip-data   Skip Parquet export; manifest + inventory only
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import platform
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

PROD_DB = "thyroid_research_2026"
TIMESTAMP_FMT = "%Y%m%d_%H%M"

# ═══════════════════════════════════════════════════════════════════════════
# Manuscript-critical table inventory
# ═══════════════════════════════════════════════════════════════════════════
# Tier 1: Primary manuscript tables (fail-closed if missing)
# Tier 2: Supporting analysis tables (warn if missing)
# Tier 3: Validation / QA tables (informational)

TIER1_TABLES: list[tuple[str, int | None, str]] = [
    # (table_name, expected_rows_or_None, description)
    ("manuscript_cohort_v1",                   10871, "Primary manuscript cohort (1 row/patient)"),
    ("patient_analysis_resolved_v1",           10871, "Resolved patient-level table"),
    ("episode_analysis_resolved_v1_dedup",      9368, "Deduplicated episode-level table"),
    ("lesion_analysis_resolved_v1",            None,  "Lesion-level resolved table"),
    ("thyroid_scoring_py_v1",                  10871, "AJCC8/ATA/MACIS/AGES/AMES scoring"),
    ("analysis_cancer_cohort_v1",               4136, "Cancer-eligible analytic subset"),
    ("complication_phenotype_v1",               None,  "Complication phenotyping (long format)"),
    ("complication_patient_summary_v1",         None,  "Wide complication flags per patient"),
    ("recurrence_event_clean_v1",               1946, "Cleaned recurrence events"),
    ("longitudinal_lab_canonical_v1",          None,  "Canonical longitudinal labs"),
    ("survival_cohort_enriched",               None,  "Survival analysis cohort"),
]

TIER2_TABLES: list[tuple[str, int | None, str]] = [
    ("analysis_molecular_subset_v1",           10025, "Molecular-tested analytic subset"),
    ("analysis_tirads_subset_v1",               3474, "TIRADS-scored analytic subset"),
    ("analysis_recurrence_subset_v1",           1946, "Recurrence analytic subset"),
    ("patient_refined_master_clinical_v12",    12886, "Master clinical table (all phases)"),
    ("extracted_tirads_validated_v1",           3474, "Validated TIRADS scores"),
    ("extracted_braf_recovery_v1",             None,  "BRAF recovery audit"),
    ("extracted_ras_patient_summary_v1",       None,  "RAS per-patient summary"),
    ("extracted_rln_injury_refined_v2",        None,  "Refined RLN injury"),
    ("extracted_complications_refined_v5",     None,  "Refined complications union"),
    ("operative_episode_detail_v2",             9371, "Operative episodes"),
    ("rai_treatment_episode_v2",                1857, "RAI treatment episodes"),
    ("molecular_test_episode_v2",              None,  "Molecular test episodes"),
    ("tumor_episode_master_v2",                None,  "Tumor episode master"),
    ("imaging_nodule_master_v1",               None,  "Imaging nodule per-exam"),
    ("extracted_ete_subgraded_v1",             None,  "ETE sub-grading"),
    ("extracted_postop_labs_expanded_v1",       None,  "Post-op lab expansion"),
]

TIER3_TABLES: list[tuple[str, int | None, str]] = [
    ("val_dataset_integrity_summary_v1",       None,  "Dataset integrity summary"),
    ("val_episode_linkage_v2_scorecard",        None,  "Episode linkage scorecard"),
    ("val_operative_truth_state_v1",            None,  "Operative truth state"),
    ("val_phase12_tirads_validation",           None,  "TIRADS validation"),
    ("val_complication_refinement",             None,  "Complication refinement audit"),
    ("val_provenance_hardening_summary_v1",     None,  "Provenance hardening"),
]

ALL_TIERS = [
    ("tier1", TIER1_TABLES),
    ("tier2", TIER2_TABLES),
    ("tier3", TIER3_TABLES),
]

# Row count drift tolerance
DRIFT_PCT = 1.0


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def log(msg: str, level: str = "INFO") -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{level:5s}] {msg}")


def git_sha(short: bool = False) -> str:
    try:
        flag = "--short" if short else ""
        r = subprocess.run(
            ["git", "rev-parse"] + ([flag] if flag else []) + ["HEAD"],
            capture_output=True, text=True, cwd=str(ROOT),
        )
        return r.stdout.strip() if r.returncode == 0 else "unknown"
    except Exception:
        return "unknown"


def git_dirty() -> bool:
    try:
        r = subprocess.run(
            ["git", "status", "--porcelain"],
            capture_output=True, text=True, cwd=str(ROOT),
        )
        return bool(r.stdout.strip())
    except Exception:
        return True


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def get_connection(use_md: bool):
    import duckdb

    if use_md:
        token = os.environ.get("MOTHERDUCK_TOKEN")
        if not token:
            try:
                import toml
                token = toml.load(str(ROOT / ".streamlit" / "secrets.toml"))["MOTHERDUCK_TOKEN"]
                os.environ["MOTHERDUCK_TOKEN"] = token
            except Exception:
                pass
        if not token:
            log("MOTHERDUCK_TOKEN not found — falling back to local", "WARN")
            return duckdb.connect(str(ROOT / "thyroid_master.duckdb"), read_only=True)
        return duckdb.connect(
            f"md:{PROD_DB}?motherduck_token={token}", read_only=True
        )
    return duckdb.connect(str(ROOT / "thyroid_master.duckdb"), read_only=True)


def get_rw_connection():
    """Read-write connection for --stamp mode."""
    import duckdb

    token = os.environ.get("MOTHERDUCK_TOKEN")
    if not token:
        try:
            import toml
            token = toml.load(str(ROOT / ".streamlit" / "secrets.toml"))["MOTHERDUCK_TOKEN"]
            os.environ["MOTHERDUCK_TOKEN"] = token
        except Exception:
            pass
    if not token:
        raise RuntimeError("MOTHERDUCK_TOKEN required for --stamp mode")
    return duckdb.connect(f"md:{PROD_DB}?motherduck_token={token}")


# ═══════════════════════════════════════════════════════════════════════════
# Phase A: Environment verification
# ═══════════════════════════════════════════════════════════════════════════

def verify_environment(con, use_md: bool) -> dict:
    log("Phase A: Verify source environment")
    env_info: dict[str, Any] = {
        "source_type": "motherduck_prod" if use_md else "local_duckdb",
        "expected_db": PROD_DB,
        "actual_db": "unknown",
        "is_prod": False,
    }
    try:
        row = con.execute("SELECT current_database()").fetchone()
        env_info["actual_db"] = str(row[0]) if row else "unknown"
        env_info["is_prod"] = env_info["actual_db"] == PROD_DB
    except Exception as e:
        env_info["error"] = str(e)

    if use_md and not env_info["is_prod"]:
        log(f"  WARNING: Expected {PROD_DB}, got {env_info['actual_db']}", "WARN")
    else:
        log(f"  Connected to: {env_info['actual_db']} (prod={env_info['is_prod']})")
    return env_info


# ═══════════════════════════════════════════════════════════════════════════
# Phase B: Inventory tables
# ═══════════════════════════════════════════════════════════════════════════

def inventory_tables(con) -> list[dict]:
    log("Phase B: Inventory manuscript-critical tables")
    inventory: list[dict] = []

    for tier_name, tables in ALL_TIERS:
        for tbl_name, expected_rows, description in tables:
            entry: dict[str, Any] = {
                "table": tbl_name,
                "tier": tier_name,
                "description": description,
                "expected_rows": expected_rows,
                "actual_rows": None,
                "status": "MISSING",
                "drift_pct": None,
            }
            # Try canonical name, then md_ prefix fallback
            for candidate in [tbl_name, f"md_{tbl_name}"]:
                try:
                    row = con.execute(f"SELECT COUNT(*) FROM {candidate}").fetchone()
                    entry["actual_rows"] = row[0]
                    entry["resolved_name"] = candidate
                    if expected_rows is not None and expected_rows > 0:
                        drift = abs(row[0] - expected_rows) / expected_rows * 100
                        entry["drift_pct"] = round(drift, 2)
                        if drift > DRIFT_PCT and tier_name == "tier1":
                            entry["status"] = "DRIFT"
                        else:
                            entry["status"] = "OK"
                    else:
                        entry["status"] = "OK"
                    break
                except Exception:
                    continue

            tier_label = tier_name.upper()
            if entry["status"] == "MISSING":
                level = "ERROR" if tier_name == "tier1" else "WARN"
                log(f"  [{tier_label}] {tbl_name}: MISSING", level)
            elif entry["status"] == "DRIFT":
                log(
                    f"  [{tier_label}] {tbl_name}: {entry['actual_rows']} rows "
                    f"(expected {expected_rows}, drift {entry['drift_pct']}%)",
                    "WARN",
                )
            else:
                log(f"  [{tier_label}] {tbl_name}: {entry['actual_rows']} rows — OK")

            inventory.append(entry)

    return inventory


# ═══════════════════════════════════════════════════════════════════════════
# Phase C: Export data
# ═══════════════════════════════════════════════════════════════════════════

CSV_ROW_THRESHOLD = 50_000  # also export CSV for tables under this many rows


def export_tables(con, inventory: list[dict], data_dir: Path) -> list[dict]:
    log("Phase C: Export table data to Parquet (+CSV for small tables)")
    import pandas as pd

    data_dir.mkdir(parents=True, exist_ok=True)
    export_records: list[dict] = []

    for entry in inventory:
        tbl = entry.get("resolved_name", entry["table"])
        if entry["status"] == "MISSING":
            continue

        safe_name = entry["table"].replace(".", "_")
        pq_path = data_dir / f"{safe_name}.parquet"

        try:
            df = con.execute(f"SELECT * FROM {tbl}").fetchdf()
            df.to_parquet(pq_path, index=False)
            rec: dict[str, Any] = {
                "table": entry["table"],
                "parquet": str(pq_path.name),
                "parquet_sha256": sha256_file(pq_path),
                "rows": len(df),
                "columns": len(df.columns),
            }

            if len(df) <= CSV_ROW_THRESHOLD:
                csv_path = data_dir / f"{safe_name}.csv"
                df.to_csv(csv_path, index=False)
                rec["csv"] = str(csv_path.name)
                rec["csv_sha256"] = sha256_file(csv_path)

            export_records.append(rec)
            log(f"  Exported {entry['table']}: {len(df)} rows, {len(df.columns)} cols")
        except Exception as e:
            log(f"  FAILED to export {entry['table']}: {e}", "ERROR")
            export_records.append({"table": entry["table"], "error": str(e)})

    return export_records


# ═══════════════════════════════════════════════════════════════════════════
# Phase D: Checksums (computed inline during Phase C)
# ═══════════════════════════════════════════════════════════════════════════

# Checksums are computed in export_tables() during write.


# ═══════════════════════════════════════════════════════════════════════════
# Phase E: Write manifest
# ═══════════════════════════════════════════════════════════════════════════

def write_manifest(
    out_dir: Path,
    version: str,
    env_info: dict,
    inventory: list[dict],
    export_records: list[dict],
    use_md: bool,
    stamp_applied: bool,
) -> Path:
    log("Phase E: Write freeze manifest")

    sha_full = git_sha(short=False)
    sha_short = git_sha(short=True)
    is_dirty = git_dirty()

    manifest = {
        "freeze_version": version,
        "freeze_type": "manuscript_publication_freeze",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": "scripts/105_manuscript_freeze_v1.py",
        "source": {
            "type": env_info.get("source_type", "unknown"),
            "database": env_info.get("actual_db", "unknown"),
            "is_prod": env_info.get("is_prod", False),
        },
        "git": {
            "sha_full": sha_full,
            "sha_short": sha_short,
            "dirty": is_dirty,
        },
        "python_version": platform.python_version(),
        "duckdb_version": _duckdb_version(),
        "table_count": {
            "total": len(inventory),
            "tier1": sum(1 for e in inventory if e["tier"] == "tier1"),
            "tier2": sum(1 for e in inventory if e["tier"] == "tier2"),
            "tier3": sum(1 for e in inventory if e["tier"] == "tier3"),
            "present": sum(1 for e in inventory if e["status"] != "MISSING"),
            "missing": sum(1 for e in inventory if e["status"] == "MISSING"),
            "drifted": sum(1 for e in inventory if e["status"] == "DRIFT"),
        },
        "stamp_applied": stamp_applied,
        "stamp_suffix": f"_freeze_{version}" if stamp_applied else None,
        "refresh_notes": (
            "This freeze captures the current manuscript state. "
            "When the pending external lab/medication extract arrives, "
            "re-run with --version v2 to create a separate freeze point. "
            "Compare v1 vs v2 manifests to quantify data drift."
        ),
        "exports": export_records,
        "inventory": inventory,
    }

    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str))
    log(f"  Manifest: {manifest_path.relative_to(ROOT)}")

    # ── Row count summary ──────────────────────────────────────────────
    rowcounts = {
        e["table"]: e["actual_rows"]
        for e in inventory
        if e["actual_rows"] is not None
    }
    rc_path = out_dir / "rowcount_summary.json"
    rc_path.write_text(json.dumps(rowcounts, indent=2))
    log(f"  Row counts: {rc_path.name}")

    # ── Table inventory CSV ────────────────────────────────────────────
    inv_path = out_dir / "table_inventory.csv"
    with open(inv_path, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "table", "tier", "description", "expected_rows",
                "actual_rows", "drift_pct", "status",
            ],
        )
        writer.writeheader()
        for entry in inventory:
            writer.writerow({
                "table": entry["table"],
                "tier": entry["tier"],
                "description": entry["description"],
                "expected_rows": entry.get("expected_rows", ""),
                "actual_rows": entry.get("actual_rows", ""),
                "drift_pct": entry.get("drift_pct", ""),
                "status": entry["status"],
            })
    log(f"  Inventory: {inv_path.name}")

    # ── Metadata JSON ──────────────────────────────────────────────────
    meta = {
        "freeze_version": version,
        "git_sha_full": sha_full,
        "git_sha_short": sha_short,
        "git_dirty": is_dirty,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "python_version": platform.python_version(),
        "duckdb_version": _duckdb_version(),
        "source_database": env_info.get("actual_db", "unknown"),
        "source_is_prod": env_info.get("is_prod", False),
        "total_tables_frozen": sum(
            1 for e in inventory if e["status"] != "MISSING"
        ),
        "total_rows_frozen": sum(
            e.get("actual_rows", 0) or 0
            for e in inventory
            if e["status"] != "MISSING"
        ),
    }
    meta_path = out_dir / "metadata.json"
    meta_path.write_text(json.dumps(meta, indent=2))
    log(f"  Metadata: {meta_path.name}")

    return manifest_path


def _duckdb_version() -> str:
    try:
        import duckdb
        return duckdb.__version__
    except Exception:
        return "unknown"


# ═══════════════════════════════════════════════════════════════════════════
# Phase F: Stamp frozen copies in MotherDuck (optional)
# ═══════════════════════════════════════════════════════════════════════════

def stamp_frozen_copies(
    inventory: list[dict], version: str
) -> list[dict]:
    log(f"Phase F: Stamp frozen copies in MotherDuck (suffix=_freeze_{version})")
    con = get_rw_connection()
    stamp_results: list[dict] = []

    # Only stamp tier1 + tier2 tables that are present
    for entry in inventory:
        if entry["status"] == "MISSING":
            continue
        if entry["tier"] == "tier3":
            continue

        src = entry.get("resolved_name", entry["table"])
        dst = f"{entry['table']}_freeze_{version}"

        try:
            con.execute(f"DROP TABLE IF EXISTS {dst}")
            con.execute(f"CREATE TABLE {dst} AS SELECT * FROM {src}")
            row = con.execute(f"SELECT COUNT(*) FROM {dst}").fetchone()
            stamp_results.append({
                "source": src,
                "frozen_as": dst,
                "rows": row[0],
                "status": "OK",
            })
            log(f"  Stamped: {dst} ({row[0]} rows)")
        except Exception as e:
            stamp_results.append({
                "source": src,
                "frozen_as": dst,
                "rows": 0,
                "status": f"ERROR: {e}",
            })
            log(f"  FAILED: {dst} — {e}", "ERROR")

    con.close()
    return stamp_results


# ═══════════════════════════════════════════════════════════════════════════
# Phase G: Verify freeze integrity
# ═══════════════════════════════════════════════════════════════════════════

def verify_freeze(out_dir: Path) -> dict:
    log("Phase G: Verify freeze integrity")
    manifest_path = out_dir / "manifest.json"
    if not manifest_path.exists():
        log("  Manifest not found — cannot verify", "ERROR")
        return {"status": "FAIL", "reason": "manifest_missing"}

    manifest = json.loads(manifest_path.read_text())
    checks: list[dict] = []

    # Check exported files exist and checksums match
    data_dir = out_dir / "data"
    for rec in manifest.get("exports", []):
        if "error" in rec:
            checks.append({
                "table": rec["table"],
                "check": "export",
                "status": "SKIP",
                "reason": rec["error"],
            })
            continue

        pq_name = rec.get("parquet")
        if pq_name:
            pq_path = data_dir / pq_name
            if pq_path.exists():
                actual = sha256_file(pq_path)
                expected = rec.get("parquet_sha256", "")
                ok = actual == expected
                checks.append({
                    "table": rec["table"],
                    "check": "parquet_checksum",
                    "status": "PASS" if ok else "FAIL",
                    "expected": expected[:16] + "…",
                    "actual": actual[:16] + "…",
                })
                if not ok:
                    log(f"  FAIL: {pq_name} checksum mismatch", "ERROR")
            else:
                checks.append({
                    "table": rec["table"],
                    "check": "parquet_exists",
                    "status": "FAIL",
                })
                log(f"  FAIL: {pq_name} not found", "ERROR")

        csv_name = rec.get("csv")
        if csv_name:
            csv_path = data_dir / csv_name
            if csv_path.exists():
                actual = sha256_file(csv_path)
                expected = rec.get("csv_sha256", "")
                ok = actual == expected
                checks.append({
                    "table": rec["table"],
                    "check": "csv_checksum",
                    "status": "PASS" if ok else "FAIL",
                })
                if not ok:
                    log(f"  FAIL: {csv_name} checksum mismatch", "ERROR")
            else:
                checks.append({
                    "table": rec["table"],
                    "check": "csv_exists",
                    "status": "FAIL",
                })

    n_pass = sum(1 for c in checks if c["status"] == "PASS")
    n_fail = sum(1 for c in checks if c["status"] == "FAIL")
    n_skip = sum(1 for c in checks if c["status"] == "SKIP")

    result = {
        "status": "PASS" if n_fail == 0 else "FAIL",
        "checks_passed": n_pass,
        "checks_failed": n_fail,
        "checks_skipped": n_skip,
        "details": checks,
    }

    report_path = out_dir / "verification_report.json"
    report_path.write_text(json.dumps(result, indent=2))
    log(f"  Verification: {result['status']} ({n_pass} pass, {n_fail} fail, {n_skip} skip)")
    return result


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Manuscript publication freeze — snapshot critical tables"
    )
    parser.add_argument("--md", action="store_true", help="Read from MotherDuck prod")
    parser.add_argument("--dry-run", action="store_true", help="Inventory only, no export")
    parser.add_argument("--stamp", action="store_true",
                        help="Create frozen TABLE copies in MotherDuck with version suffix")
    parser.add_argument("--version", default="v1",
                        help="Freeze version tag (default: v1)")
    parser.add_argument("--skip-data", action="store_true",
                        help="Skip Parquet/CSV export; manifest + inventory only")
    args = parser.parse_args()

    ts_tag = datetime.now().strftime(TIMESTAMP_FMT)
    version = args.version
    out_dir = ROOT / "exports" / f"manuscript_freeze_{version}"

    log(f"═══ Manuscript Publication Freeze — {version} ═══")
    log(f"  Source: {'MotherDuck prod' if args.md else 'local DuckDB'}")
    log(f"  Mode: {'DRY RUN' if args.dry_run else 'FULL FREEZE'}")
    log(f"  Output: {out_dir.relative_to(ROOT)}")
    log(f"  Git SHA: {git_sha(short=True)} (dirty={git_dirty()})")
    log("")

    # Phase A
    con = get_connection(args.md)
    env_info = verify_environment(con, args.md)
    log("")

    # Phase B
    inventory = inventory_tables(con)
    log("")

    # Gate check: tier1 tables must all be present
    tier1_missing = [
        e["table"] for e in inventory
        if e["tier"] == "tier1" and e["status"] == "MISSING"
    ]
    tier1_drifted = [
        e["table"] for e in inventory
        if e["tier"] == "tier1" and e["status"] == "DRIFT"
    ]

    if tier1_missing:
        log(f"ABORT: {len(tier1_missing)} Tier-1 tables missing: {tier1_missing}", "ERROR")
        con.close()
        sys.exit(1)
    if tier1_drifted:
        log(f"WARNING: {len(tier1_drifted)} Tier-1 tables drifted: {tier1_drifted}", "WARN")

    if args.dry_run:
        con.close()
        log("")
        log("═══ DRY RUN COMPLETE ═══")
        log(f"  Tables found: {sum(1 for e in inventory if e['status'] != 'MISSING')}")
        log(f"  Tables missing: {sum(1 for e in inventory if e['status'] == 'MISSING')}")
        log(f"  Tier-1 OK: {sum(1 for e in inventory if e['tier'] == 'tier1' and e['status'] == 'OK')}")
        sys.exit(0)

    # Phase C: Export
    out_dir.mkdir(parents=True, exist_ok=True)
    export_records: list[dict] = []
    if not args.skip_data:
        data_dir = out_dir / "data"
        export_records = export_tables(con, inventory, data_dir)
        log("")
    else:
        log("Phase C: SKIPPED (--skip-data)")
        log("")

    con.close()

    # Phase D: Checksums computed inline in Phase C

    # Phase E: Manifest
    stamp_applied = False
    write_manifest(out_dir, version, env_info, inventory, export_records, args.md, stamp_applied)
    log("")

    # Phase F: Stamp (optional)
    stamp_results: list[dict] = []
    if args.stamp:
        stamp_results = stamp_frozen_copies(inventory, version)
        stamp_applied = True
        # Re-write manifest with stamp info
        write_manifest(out_dir, version, env_info, inventory, export_records, args.md, stamp_applied)
        # Write stamp log
        stamp_path = out_dir / "stamp_results.json"
        stamp_path.write_text(json.dumps(stamp_results, indent=2, default=str))
        log(f"  Stamp log: {stamp_path.name}")
        log("")

    # Phase G: Verify
    verification = verify_freeze(out_dir) if not args.skip_data else {"status": "SKIP"}
    log("")

    # ── Summary ────────────────────────────────────────────────────────
    present = sum(1 for e in inventory if e["status"] != "MISSING")
    total_rows = sum(e.get("actual_rows", 0) or 0 for e in inventory if e["status"] != "MISSING")
    n_files = sum(1 for _ in out_dir.rglob("*") if _.is_file())

    log("═══════════════════════════════════════════════════════")
    log(f"  MANUSCRIPT FREEZE {version.upper()} COMPLETE")
    log(f"  Tables frozen: {present}/{len(inventory)}")
    log(f"  Total rows: {total_rows:,}")
    log(f"  Output files: {n_files}")
    log(f"  Output dir: {out_dir.relative_to(ROOT)}")
    log(f"  Verification: {verification.get('status', 'N/A')}")
    if stamp_applied:
        ok_stamps = sum(1 for s in stamp_results if s["status"] == "OK")
        log(f"  MD stamps: {ok_stamps} tables frozen with _freeze_{version} suffix")
    log(f"  Git SHA: {git_sha(short=True)}")
    log("═══════════════════════════════════════════════════════")

    if verification.get("status") == "FAIL":
        log("  ⚠ Verification failures detected — review verification_report.json", "WARN")
        sys.exit(1)


if __name__ == "__main__":
    main()
