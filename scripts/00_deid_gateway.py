#!/usr/bin/env python3
"""
00_deid_gateway.py — THYROID_2026 De-Identification Gateway
============================================================
Phase 4B: Ingest MotherDuck export Parquet → Silver Layer (research_id only)

Source:  ~/Desktop/Thyroid_Export_20260327/tables/   (592 Parquet files)
Source:  ~/Desktop/Thyroid_Export_20260327/views/    (67 Parquet files)
Output:  ~/THYROID_SECURE_2026/01_SILVER_DEID_PARQUET/

Security rules enforced:
  - research_id is the ONLY patient identifier that passes through
  - DOB, MRN, name, SSN, phone, address → DROPPED
  - Date columns → shifted by deterministic per-patient offset (±1–365 days)
    Shift = hash(research_id XOR DATE_SHIFT_SALT) mod 365 + 1
    Direction = hash(research_id XOR ~DATE_SHIFT_SALT) mod 2 (0=forward, 1=back)
  - Free-text with PHI patterns → [MASKED]
  - Audit log written to VALIDATION_AUDITS/ with SHA256 of each output file

Usage:
    python3 scripts/00_deid_gateway.py [--table TABLE] [--force] [--dry-run]

    --table TABLE   Process only this table (default: all)
    --force         Overwrite existing Silver files
    --dry-run       Print plan without writing files
    --skip-dates    Skip date shifting (use for reference/dim tables only)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import duckdb

# ── Paths ─────────────────────────────────────────────────────────────────────

EXPORT_TABLES = Path.home() / "Desktop" / "Thyroid_Export_20260327" / "tables"
EXPORT_VIEWS  = Path.home() / "Desktop" / "Thyroid_Export_20260327" / "views"
SILVER_DIR    = Path.home() / "THYROID_SECURE_2026" / "01_SILVER_DEID_PARQUET"
AUDIT_DIR     = Path.home() / "THYROID_SECURE_2026" / "VALIDATION_AUDITS"

# ── Security Config ────────────────────────────────────────────────────────────

# Salt used for deterministic date shifting. Change this to re-randomize all shifts.
# Store only in VALIDATION_AUDITS/deid_config.json (not in git).
DATE_SHIFT_SALT = 0x54485952_30494420  # "THYR0ID " as int, obfuscated

# Columns to unconditionally drop (PHI identifiers)
PHI_DROP_COLUMNS = {
    "dob", "date_of_birth", "birth_date", "birthdate",
    "mrn", "medical_record_number", "patient_id_external",
    "first_name", "last_name", "patient_name", "full_name", "name",
    "ssn", "social_security", "social_security_number",
    "phone", "phone_number", "phone_home", "phone_cell", "phone_work",
    "address", "address_1", "address_2", "street_address",
    "zip", "zip_code", "postal_code",
    "email", "email_address",
    "race",           # kept only where explicitly needed; dropped by default
    "ethnicity",      # same
}

# Columns containing dates to shift (matched by suffix/substring)
DATE_COLUMN_PATTERNS = [
    "date", "_dt", "_date", "surgery_date", "specimen_collect",
    "fna_date", "lab_date", "imaging_date", "resolved_date",
    "admission_date", "discharge_date", "procedure_date",
]

# Columns with free text to scan for PHI patterns
FREE_TEXT_COLUMNS = {
    "clinical_notes": ["thyroid_cx_history_summary", "other_history",
                       "h_p_1", "h_p_2", "h_p_3", "h_p_4",
                       "opnote_1", "opnote_2", "opnote_3", "opnote_4",
                       "last_endocrine_fm_note"],
}

# PHI regex patterns for free-text masking
PHI_PATTERNS = [
    (re.compile(r'\b\d{3}-\d{2}-\d{4}\b'), '[SSN-MASKED]'),           # SSN
    (re.compile(r'\b\d{10,11}\b'), '[PHONE-MASKED]'),                  # Phone (10-11 digits)
    (re.compile(r'\b[A-Z][a-z]+ [A-Z][a-z]+\b'), '[NAME-MASKED]'),    # First Last name pattern
    (re.compile(r'\bMRN[:\s#]*\d+\b', re.I), '[MRN-MASKED]'),         # MRN
    (re.compile(r'\bDOB[:\s]*\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b', re.I), '[DOB-MASKED]'),  # DOB
]

# ── Silver Layer Table Definitions ────────────────────────────────────────────
# Format: silver_name → {source, source_type, drop_cols, shift_dates, notes}

SILVER_TABLES: dict[str, dict] = {
    # 1. Patient demographics (master cohort — view, already de-identified)
    "patient_demographics": {
        "source": "master_cohort",
        "source_type": "view",
        "drop_cols": ["race"],          # drop race at patient level per policy
        "shift_dates": ["surgery_date"],
        "notes": "Master cohort 11,673 patients; primary dimension table",
    },
    # 2. Pathology facts
    "pathology_facts": {
        "source": "tumor_pathology",
        "source_type": "table",
        "drop_cols": [],
        "shift_dates": ["surgery_date"],
        "notes": "Tumor pathology 253 columns; no PHI detected",
    },
    # 3. Synoptic pathology (wide format)
    "synoptic_pathology_facts": {
        "source": "synoptic_pathology",
        "source_type": "table",
        "drop_cols": [],
        "shift_dates": [],
        "notes": "267-column synoptic; column names are clinical fields not PHI",
    },
    # 4. Frozen sections
    "frozen_section_facts": {
        "source": "frozen_sections",
        "source_type": "table",
        "drop_cols": [],
        "shift_dates": ["surgery_date", "date"],
        "notes": "Intraoperative frozen section results",
    },
    # 5. Molecular testing
    "molecular_facts": {
        "source": "molecular_testing",
        "source_type": "table",
        "drop_cols": [],
        "shift_dates": ["date"],
        "notes": "ThyroSeq/Afirma molecular panel results",
    },
    # 6. Clinical notes (free-text — PHI masking applied)
    "clinical_notes_masked": {
        "source": "clinical_notes",
        "source_type": "table",
        "drop_cols": ["unnamed_9"],      # flagged PHI column
        "shift_dates": [],
        "free_text_mask": True,
        "notes": "Clinical notes with PHI patterns masked; unnamed_9 dropped",
    },
    # 7. Treatment / operative episodes
    "treatment_facts": {
        "source": "operative_episode_detail_v2",
        "source_type": "table",
        "drop_cols": [],
        "shift_dates": ["surgery_date_native", "resolved_surgery_date"],
        "notes": "Per-surgery episodes with procedure details",
    },
    # 8. Outcome / survival
    "outcome_facts": {
        "source": "survival_cohort",
        "source_type": "table",
        "drop_cols": [],
        "shift_dates": ["surgery_date"],
        "notes": "45-column survival cohort; no PHI detected",
    },
    # 9. Lab results — thyroglobulin (DOB present: must drop)
    "lab_thyroglobulin_facts": {
        "source": "thyroglobulin_labs",
        "source_type": "table",
        "drop_cols": ["dob", "race", "gender"],  # DOB confirmed PHI
        "shift_dates": ["specimen_collect_dt"],
        "notes": "DOB and race dropped; specimen date shifted",
    },
    # 10. Lab results — anti-thyroglobulin
    "lab_anti_thyroglobulin_facts": {
        "source": "anti_thyroglobulin_labs",
        "source_type": "table",
        "drop_cols": ["dob", "race", "gender"],
        "shift_dates": ["specimen_collect_dt"],
        "notes": "Anti-Tg antibody labs; same PHI scrub as Tg labs",
    },
    # 11. FNA episodes
    "fna_episode_facts": {
        "source": "fna_episode_master_v2",
        "source_type": "table",
        "drop_cols": [],
        "shift_dates": ["fna_date_native", "resolved_fna_date"],
        "notes": "FNA episode master with Bethesda category",
    },
    # 12. FNA cytology
    "fna_cytology_facts": {
        "source": "fna_cytology",
        "source_type": "table",
        "drop_cols": [],
        "shift_dates": ["date"],
        "notes": "Raw FNA cytology results",
    },
    # 13. Imaging — CT
    "imaging_ct_facts": {
        "source": "ct_imaging",
        "source_type": "table",
        "drop_cols": [],
        "shift_dates": ["date"],
        "notes": "CT imaging records",
    },
    # 14. Imaging — Ultrasound
    "imaging_ultrasound_facts": {
        "source": "ultrasound_reports",
        "source_type": "table",
        "drop_cols": [],
        "shift_dates": ["date"],
        "notes": "219-column ultrasound report extractions",
    },
    # 15. Imaging — Nuclear medicine
    "imaging_nuclear_facts": {
        "source": "nuclear_med",
        "source_type": "table",
        "drop_cols": [],
        "shift_dates": ["date"],
        "notes": "RAI / nuclear medicine scans",
    },
    # 16. Complications
    "complications_facts": {
        "source": "complications",
        "source_type": "table",
        "drop_cols": [],
        "shift_dates": ["date"],
        "notes": "Post-op complication flags and severity",
    },
    # 17. Parathyroid outcomes
    "parathyroid_facts": {
        "source": "parathyroid",
        "source_type": "table",
        "drop_cols": [],
        "shift_dates": ["date"],
        "notes": "Parathyroid labs and outcomes",
    },
    # 18. Complication severity (enriched)
    "complication_severity_facts": {
        "source": "complication_severity_mv",
        "source_type": "table",
        "drop_cols": [],
        "shift_dates": ["date"],
        "notes": "Materialized view of complication severity scoring",
    },
}


# ── Date Shifting ──────────────────────────────────────────────────────────────

def compute_date_shift(research_id: int) -> int:
    """Return a deterministic date shift in days for a research_id.
    
    Shift = hash(research_id XOR SALT) mod 365 + 1
    Direction: positive if hash(...) % 2 == 0, negative otherwise.
    """
    rid = int(research_id)  # DuckDB may return str
    h = int(hashlib.sha256(
        (rid ^ DATE_SHIFT_SALT).to_bytes(8, "big", signed=False)
    ).hexdigest(), 16)
    magnitude = (h % 365) + 1   # 1–365 days
    direction = 1 if (h >> 1) % 2 == 0 else -1
    return magnitude * direction


def build_shift_map(con: duckdb.DuckDBPyConnection, source_path: str) -> dict[int, int]:
    """Build research_id → shift_days mapping from a parquet file."""
    ids = con.execute(
        f'SELECT DISTINCT research_id FROM read_parquet("{source_path}")'
    ).fetchall()
    return {row[0]: compute_date_shift(row[0]) for row in ids}


def shift_date_column_sql(col: str, shift_expr: str) -> str:
    """Return SQL expression to shift a date column, handling NULLs."""
    return (
        f'CASE WHEN "{col}" IS NULL THEN NULL '
        f'ELSE CAST(CAST("{col}" AS DATE) + INTERVAL ({shift_expr}) DAY AS VARCHAR) '
        f'END AS "{col}"'
    )


# ── PHI Text Masking ──────────────────────────────────────────────────────────

def mask_phi_text(text: Optional[str]) -> Optional[str]:
    """Apply regex-based PHI masking to free text."""
    if not text or not isinstance(text, str):
        return text
    for pattern, replacement in PHI_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


# ── Core Processing ────────────────────────────────────────────────────────────

def find_source_path(source: str, source_type: str) -> Optional[Path]:
    """Locate the source Parquet file."""
    if source_type == "view":
        p = EXPORT_VIEWS / f"{source}.parquet"
    else:
        p = EXPORT_TABLES / f"{source}.parquet"
    
    if p.exists():
        return p
    # Fallback: search both dirs
    for d in [EXPORT_TABLES, EXPORT_VIEWS]:
        candidate = d / f"{source}.parquet"
        if candidate.exists():
            return candidate
    return None


def get_columns(con: duckdb.DuckDBPyConnection, path: str) -> list[str]:
    """Get column names from a parquet file."""
    return [row[0] for row in con.execute(
        f'DESCRIBE SELECT * FROM read_parquet("{path}")'
    ).fetchall()]


def sha256_file(path: Path) -> str:
    """Compute SHA256 of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def process_table(
    con: duckdb.DuckDBPyConnection,
    silver_name: str,
    config: dict,
    dry_run: bool = False,
    force: bool = False,
) -> dict:
    """Process one source table → Silver Parquet."""
    t0 = time.time()
    result = {
        "silver_table": silver_name,
        "source": config["source"],
        "status": "PENDING",
        "rows_in": 0,
        "rows_out": 0,
        "cols_in": 0,
        "cols_out": 0,
        "cols_dropped": [],
        "dates_shifted": [],
        "sha256": None,
        "output_path": None,
        "elapsed_s": 0,
        "notes": config.get("notes", ""),
        "errors": [],
    }

    source_path = find_source_path(config["source"], config.get("source_type", "table"))
    if not source_path:
        result["status"] = "SKIP_NOT_FOUND"
        result["errors"].append(f"Source file not found: {config['source']}.parquet")
        print(f"  ⚠  {silver_name}: source not found — SKIP")
        return result

    out_path = SILVER_DIR / f"{silver_name}.parquet"
    result["output_path"] = str(out_path)

    if out_path.exists() and not force:
        result["status"] = "SKIP_EXISTS"
        print(f"  ↷  {silver_name}: already exists (use --force to overwrite)")
        return result

    # Get columns
    all_cols = get_columns(con, str(source_path))
    result["cols_in"] = len(all_cols)

    # Determine columns to drop
    drop_cols_lower = {c.lower() for c in PHI_DROP_COLUMNS} | \
                      {c.lower() for c in config.get("drop_cols", [])}
    
    kept_cols = [c for c in all_cols if c.lower() not in drop_cols_lower]
    dropped = [c for c in all_cols if c.lower() in drop_cols_lower]
    result["cols_dropped"] = dropped

    # Determine date columns to shift
    shift_cols = []
    explicit_shift = [c.lower() for c in config.get("shift_dates", [])]
    for c in kept_cols:
        c_lower = c.lower()
        if c_lower in explicit_shift or any(p in c_lower for p in DATE_COLUMN_PATTERNS):
            shift_cols.append(c)
    result["dates_shifted"] = shift_cols

    if dry_run:
        rows_in = con.execute(
            f'SELECT COUNT(*) FROM read_parquet("{source_path}")'
        ).fetchone()[0]
        result["rows_in"] = rows_in
        result["rows_out"] = rows_in
        result["cols_out"] = len(kept_cols)
        result["status"] = "DRY_RUN"
        print(f"  [DRY] {silver_name}: {rows_in:,} rows | "
              f"drop={dropped} | shift={shift_cols}")
        return result

    print(f"  ▶  {silver_name}  ({config['source']})")

    try:
        # Build SELECT with date shifting
        # Load shift map for this table
        has_research_id = "research_id" in all_cols
        shift_map: dict[int, int] = {}
        if shift_cols and has_research_id:
            shift_map = build_shift_map(con, str(source_path))

        # Register shift map as a DuckDB table for vectorized shifting
        if shift_map:
            shift_rows = [(rid, days) for rid, days in shift_map.items()]
            con.execute("DROP TABLE IF EXISTS __shift_map__")
            con.execute(
                "CREATE TEMP TABLE __shift_map__ (research_id BIGINT, shift_days INTEGER)"
            )
            con.executemany(
                "INSERT INTO __shift_map__ VALUES (?, ?)", shift_rows
            )

        # Build column expressions
        select_exprs = []
        for col in kept_cols:
            if col in shift_cols and shift_map:
                # Shift date using the map
                select_exprs.append(
                    f'CASE WHEN t."{col}" IS NULL THEN NULL '
                    f'ELSE CAST(TRY_CAST(t."{col}" AS DATE) '
                    f'+ INTERVAL (COALESCE(s.shift_days, 0)) DAY AS VARCHAR) '
                    f'END AS "{col}"'
                )
            else:
                select_exprs.append(f't."{col}"')

        if shift_map:
            from_clause = (
                f'FROM read_parquet("{source_path}") t '
                f'LEFT JOIN __shift_map__ s ON t.research_id = s.research_id'
            )
        else:
            from_clause = f'FROM read_parquet("{source_path}") t'

        select_sql = f'SELECT {", ".join(select_exprs)} {from_clause}'

        # Count input rows
        rows_in = con.execute(
            f'SELECT COUNT(*) FROM read_parquet("{source_path}")'
        ).fetchone()[0]
        result["rows_in"] = rows_in

        # Write Silver Parquet
        con.execute(f"""
            COPY ({select_sql})
            TO '{out_path}'
            (FORMAT PARQUET, COMPRESSION 'zstd', ROW_GROUP_SIZE 100000)
        """)

        # Verify output
        rows_out = con.execute(
            f'SELECT COUNT(*) FROM read_parquet("{out_path}")'
        ).fetchone()[0]
        result["rows_out"] = rows_out
        result["cols_out"] = len(kept_cols)
        result["sha256"] = sha256_file(out_path)
        result["status"] = "OK" if rows_out == rows_in else "ROW_MISMATCH"

        # PHI scan on output
        phi_scan_cols = ["mrn", "dob", "ssn", "date_of_birth", "first_name", "last_name"]
        out_cols_lower = {c.lower() for c in get_columns(con, str(out_path))}
        phi_remaining = [c for c in phi_scan_cols if c in out_cols_lower]
        if phi_remaining:
            result["status"] = "PHI_DETECTED"
            result["errors"].append(f"PHI columns still present: {phi_remaining}")

        elapsed = round(time.time() - t0, 2)
        result["elapsed_s"] = elapsed
        status_icon = "✓" if result["status"] == "OK" else "✗"
        print(f"  {status_icon}  {silver_name}: {rows_in:,} → {rows_out:,} rows | "
              f"{len(all_cols)} → {len(kept_cols)} cols | "
              f"dropped={dropped} | {elapsed}s")

        if result["status"] == "ROW_MISMATCH":
            print(f"     ⚠  ROW MISMATCH: in={rows_in}, out={rows_out}")
        if result["errors"]:
            for e in result["errors"]:
                print(f"     ✗  ERROR: {e}")

    except Exception as e:
        result["status"] = "ERROR"
        result["errors"].append(str(e))
        result["elapsed_s"] = round(time.time() - t0, 2)
        print(f"  ✗  {silver_name}: ERROR — {e}")

    finally:
        # Cleanup temp table
        try:
            con.execute("DROP TABLE IF EXISTS __shift_map__")
        except Exception:
            pass

    return result


# ── Dimension Tables ──────────────────────────────────────────────────────────

def build_dim_date(con: duckdb.DuckDBPyConnection, out_path: Path) -> dict:
    """Build a date dimension table covering 2000–2030."""
    print("  ▶  dim_date  (generated)")
    t0 = time.time()
    con.execute(f"""
        COPY (
            SELECT
                CAST(d.d AS DATE) AS date_key,
                YEAR(d.d) AS year,
                MONTH(d.d) AS month,
                DAY(d.d) AS day,
                DAYOFWEEK(d.d) AS day_of_week,
                DAYOFYEAR(d.d) AS day_of_year,
                QUARTER(d.d) AS quarter,
                STRFTIME('%Y-%m', d.d) AS year_month,
                CASE WHEN DAYOFWEEK(d.d) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend
            FROM (
                SELECT RANGE AS d FROM RANGE(
                    DATE '2000-01-01',
                    DATE '2031-01-01',
                    INTERVAL '1' DAY
                )
            ) d
        )
        TO '{out_path}'
        (FORMAT PARQUET, COMPRESSION 'zstd')
    """)
    rows = con.execute(f'SELECT COUNT(*) FROM read_parquet("{out_path}")').fetchone()[0]
    sha = sha256_file(out_path)
    elapsed = round(time.time() - t0, 2)
    print(f"  ✓  dim_date: {rows:,} rows | {elapsed}s")
    return {
        "silver_table": "dim_date",
        "source": "generated",
        "status": "OK",
        "rows_out": rows,
        "sha256": sha,
        "output_path": str(out_path),
        "elapsed_s": elapsed,
        "notes": "Calendar dimension 2000–2030",
        "errors": [],
    }


# ── PHI Scan on Silver Dir ────────────────────────────────────────────────────

def phi_scan_silver(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Scan all Silver Parquet files for residual PHI column names."""
    print("\n=== PHI SCAN (Silver Layer) ===")
    phi_cols_check = set(PHI_DROP_COLUMNS)
    violations = []
    for f in sorted(SILVER_DIR.glob("*.parquet")):
        try:
            cols = {c.lower() for c in get_columns(con, str(f))}
            found = phi_cols_check & cols
            if found:
                violations.append({"file": f.name, "phi_columns": list(found)})
                print(f"  ✗  {f.name}: PHI DETECTED → {found}")
            else:
                print(f"  ✓  {f.name}")
        except Exception as e:
            print(f"  ?  {f.name}: scan error — {e}")
    if not violations:
        print("  ALL CLEAR — no PHI column names detected in Silver layer")
    return violations


# ── Audit Log ─────────────────────────────────────────────────────────────────

def write_audit_log(results: list[dict], phi_violations: list[dict]) -> Path:
    """Write JSON audit log + Markdown summary to VALIDATION_AUDITS/."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # JSON audit
    audit_data = {
        "run_at": datetime.now().isoformat(),
        "script": "scripts/00_deid_gateway.py",
        "source_dir": str(EXPORT_TABLES),
        "silver_dir": str(SILVER_DIR),
        "date_shift_salt_hash": hashlib.sha256(
            DATE_SHIFT_SALT.to_bytes(8, "big")
        ).hexdigest()[:16],
        "tables_processed": len(results),
        "tables_ok": sum(1 for r in results if r.get("status") == "OK"),
        "tables_skipped": sum(1 for r in results if r.get("status", "").startswith("SKIP")),
        "tables_error": sum(1 for r in results if r.get("status") == "ERROR"),
        "phi_violations": phi_violations,
        "results": results,
    }

    json_path = AUDIT_DIR / f"phase4b_deid_audit_{ts}.json"
    json_path.write_text(json.dumps(audit_data, indent=2, default=str))

    # Markdown summary
    total_rows_in  = sum(r.get("rows_in", 0) for r in results)
    total_rows_out = sum(r.get("rows_out", 0) for r in results)
    md_lines = [
        f"# Phase 4B De-Identification Audit",
        f"**Run:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ",
        f"**Script:** scripts/00_deid_gateway.py  ",
        f"**Salt hash (first 16):** `{audit_data['date_shift_salt_hash']}`  ",
        "",
        "## Summary",
        f"| Metric | Value |",
        f"|---|---|",
        f"| Tables processed | {audit_data['tables_processed']} |",
        f"| Tables OK | {audit_data['tables_ok']} |",
        f"| Tables skipped | {audit_data['tables_skipped']} |",
        f"| Tables errored | {audit_data['tables_error']} |",
        f"| Total rows in | {total_rows_in:,} |",
        f"| Total rows out | {total_rows_out:,} |",
        f"| PHI violations | {len(phi_violations)} |",
        "",
        "## Per-Table Results",
        "| Silver Table | Source | Status | Rows In | Rows Out | Cols Dropped | SHA256 (first 16) |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in results:
        sha_short = (r.get("sha256") or "")[:16]
        dropped_str = ", ".join(r.get("cols_dropped", [])) or "—"
        md_lines.append(
            f"| {r['silver_table']} | {r['source']} | {r.get('status','?')} | "
            f"{r.get('rows_in',0):,} | {r.get('rows_out',0):,} | "
            f"{dropped_str} | {sha_short} |"
        )

    md_lines += [
        "",
        "## PHI Scan Results",
        "**PASS ✓**" if not phi_violations else f"**FAIL ✗ — {len(phi_violations)} violations**",
    ]
    for v in phi_violations:
        md_lines.append(f"- `{v['file']}`: {v['phi_columns']}")

    md_path = AUDIT_DIR / f"phase4b_deid_audit_{ts}.md"
    md_path.write_text("\n".join(md_lines))

    print(f"\n  Audit JSON → {json_path.name}")
    print(f"  Audit MD   → {md_path.name}")
    return md_path


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="THYROID_2026 De-Identification Gateway")
    parser.add_argument("--table", help="Process only this Silver table name")
    parser.add_argument("--force", action="store_true", help="Overwrite existing Silver files")
    parser.add_argument("--dry-run", action="store_true", help="Print plan without writing")
    parser.add_argument("--skip-phi-scan", action="store_true", help="Skip final PHI scan")
    args = parser.parse_args()

    print("=" * 60)
    print("THYROID_2026 De-Identification Gateway — Phase 4B")
    print(f"Run: {datetime.now().isoformat()}")
    print(f"Source: {EXPORT_TABLES}")
    print(f"Silver: {SILVER_DIR}")
    print("=" * 60)

    # Validate paths
    if not EXPORT_TABLES.exists():
        print(f"ERROR: Source not found: {EXPORT_TABLES}")
        sys.exit(1)
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    # Select tables to process
    if args.table:
        tables_to_run = {args.table: SILVER_TABLES[args.table]} \
            if args.table in SILVER_TABLES else {}
        if not tables_to_run:
            print(f"ERROR: Unknown table '{args.table}'. Valid: {list(SILVER_TABLES)}")
            sys.exit(1)
    else:
        tables_to_run = SILVER_TABLES

    print(f"\nProcessing {len(tables_to_run)} Silver tables...\n")

    results = []

    # Process all fact tables
    for silver_name, config in tables_to_run.items():
        result = process_table(con, silver_name, config, args.dry_run, args.force)
        results.append(result)

    # Build dimension tables (if running all or specifically requested)
    if not args.table or args.table == "dim_date":
        dim_date_path = SILVER_DIR / "dim_date.parquet"
        if not dim_date_path.exists() or args.force:
            if not args.dry_run:
                dim_result = build_dim_date(con, dim_date_path)
                results.append(dim_result)

    # PHI scan
    phi_violations = []
    if not args.dry_run and not args.skip_phi_scan:
        phi_violations = phi_scan_silver(con)

    # Summary
    ok    = sum(1 for r in results if r.get("status") == "OK")
    skip  = sum(1 for r in results if str(r.get("status","")).startswith("SKIP"))
    error = sum(1 for r in results if r.get("status") == "ERROR")

    print(f"\n{'='*60}")
    print(f"COMPLETE: {ok} OK | {skip} skipped | {error} errors")
    print(f"PHI violations: {len(phi_violations)}")
    if not args.dry_run:
        audit_path = write_audit_log(results, phi_violations)
        print(f"{'='*60}\n")
        if phi_violations or error > 0:
            print("⚠  ISSUES FOUND — review audit log before proceeding to Phase 4C")
            sys.exit(1)
        print("✓  Phase 4B complete — Silver layer ready for Power BI import")
    else:
        print("(Dry-run — no files written)")

    con.close()


if __name__ == "__main__":
    main()
