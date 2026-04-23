#!/usr/bin/env python3
"""
Back up the live MotherDuck 'Thyroid 2026' database to a local .duckdb file.

Why: MotherDuck trial ends today. DuckDB (the engine) is free and identical.
This preserves every table, view, and schema locally so you lose nothing on
trial expiry and can continue running the exact same SQL against the local copy.

Usage (from repo root):
    python3 qc_framework_v1/01_backup_motherduck_to_local.py

Output:
    <repo_root>/backups/thyroid_2026_full_backup_<timestamp>.duckdb

Credentials:
    Reads token from motherduck.local.toml at repo root (keys tried in order:
    MD_SA_TOKEN, MOTHERDUCK_TOKEN, motherduck_token), or from env vars with the
    same names. Matches the resolution order in motherduck_client.py.

Verifies:
    After copy, runs a row-count comparison on a handful of canonical tables
    and aborts with nonzero exit if any count differs.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore

try:
    import duckdb
except ModuleNotFoundError:
    print("ERROR: duckdb not installed. Run: pip3 install duckdb", file=sys.stderr)
    sys.exit(1)


REPO_ROOT = Path(__file__).resolve().parent.parent
BACKUP_DIR = REPO_ROOT / "backups"
DB_NAME = "thyroid_canonical_publication_v1_0"

# Tables to sanity-check after copy. Add to taste.
VERIFY_TABLES = [
    "main.manuscript_cohort_v1",
    "main.canonical_us_nodule_v2",
    "main.canonical_path_malignant_events_v1",
    "main.canonical_path_malignant_patient_rollup_v1",
    "main.canonical_fna_events_v1",
    "main.recurrence_event_clean_v1",
    "main.canonical_cervical_ln_clinical_events_v1",
    "main.specimen_master_v1",
    "main.specimen_tumor_focus_v1",
]


def load_token() -> str:
    for env_key in ("MD_SA_TOKEN", "MOTHERDUCK_TOKEN", "motherduck_token"):
        v = os.environ.get(env_key)
        if v:
            print(f"[token] using env var {env_key}")
            return v

    toml_path = REPO_ROOT / "motherduck.local.toml"
    if toml_path.exists():
        with open(toml_path, "rb") as f:
            cfg = tomllib.load(f)
        for key in ("MD_SA_TOKEN", "MOTHERDUCK_TOKEN", "motherduck_token"):
            if key in cfg and cfg[key]:
                print(f"[token] using motherduck.local.toml key {key}")
                return cfg[key]

    print(
        "ERROR: no MotherDuck token found. Expected one of MD_SA_TOKEN / "
        "MOTHERDUCK_TOKEN / motherduck_token in env or motherduck.local.toml.",
        file=sys.stderr,
    )
    sys.exit(2)


def main() -> int:
    token = load_token()
    os.environ["motherduck_token"] = token  # duckdb reads this automatically

    BACKUP_DIR.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = BACKUP_DIR / f"thyroid_2026_full_backup_{stamp}.duckdb"

    print(f"[backup] target: {out_path}")

    # MotherDuck does NOT support aliases on ATTACH ('md:X AS src').
    # So we attach with the database's natural name and reference it as <DB_NAME>.*
    # We still alias the local file as 'local' (plain DuckDB supports aliases).
    con = duckdb.connect()
    try:
        print(f"[backup] attaching MotherDuck database '{DB_NAME}' (read-only)…")
        con.execute(f"ATTACH 'md:{DB_NAME}' (READ_ONLY)")

        print(f"[backup] attaching local target file…")
        con.execute(f"ATTACH '{out_path}' AS local")

        # Pre-count source side (reference by real DB name, not alias)
        src_counts = {}
        for fq in VERIFY_TABLES:
            try:
                n = con.execute(f'SELECT COUNT(*) FROM {DB_NAME}.{fq}').fetchone()[0]
                src_counts[fq] = n
                print(f"[source] {DB_NAME}.{fq} rows = {n:,}")
            except Exception as e:
                print(f"[source] {DB_NAME}.{fq} SKIP ({e})")

        print(f"[backup] COPY FROM DATABASE {DB_NAME} TO local …  (this can take a few minutes)")
        con.execute(f"COPY FROM DATABASE {DB_NAME} TO local")

        # Post-count local side and compare
        print("\n[verify] comparing row counts:")
        mismatches = []
        for fq, src_n in src_counts.items():
            try:
                loc_n = con.execute(f'SELECT COUNT(*) FROM local.{fq}').fetchone()[0]
            except Exception as e:
                print(f"  {fq}: LOCAL MISSING ({e})")
                mismatches.append(fq)
                continue
            ok = "OK" if loc_n == src_n else "MISMATCH"
            print(f"  {fq}: src={src_n:,}  local={loc_n:,}  {ok}")
            if loc_n != src_n:
                mismatches.append(fq)

        # Object inventory
        n_tables = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_catalog='local' AND table_type='BASE TABLE'"
        ).fetchone()[0]
        n_views = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_catalog='local' AND table_type='VIEW'"
        ).fetchone()[0]
        print(f"\n[verify] local inventory: {n_tables} tables, {n_views} views")

        size_mb = out_path.stat().st_size / (1024 * 1024)
        print(f"[done]   file size: {size_mb:,.1f} MB")
        print(f"[done]   path: {out_path}")

        if mismatches:
            print(f"\nFAIL: {len(mismatches)} table(s) failed row-count verification:")
            for m in mismatches:
                print(f"  - {m}")
            return 3

        print("\nSUCCESS. To query the local backup:")
        print(f"    duckdb '{out_path}'")
        print("    # then:")
        print("    SELECT COUNT(*) FROM main.gold_master_patient_facts_v1;")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
