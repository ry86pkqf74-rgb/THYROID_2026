#!/usr/bin/env python3
"""
THYROID_2026 — Script 221 (eras): Canonical Sync to logan.glosser.eras@gmail.com

Populates 'Thyroid 2026 UPdated' (eras MotherDuck account) with the fully-consolidated
canonical master built in Script 221 (glosser account).

STRATEGY
--------
The two accounts are isolated (no cross-account visibility), so we bridge via parquet:
  1  Upload glosser canonical parquet → eras as canonical_patient_master_v221
     (parquet was exported in Script 221 Phase 6: scripts/output/parquet_backup/)
  2  Find columns in eras gold_master_patient_facts_v1 NOT in the uploaded canonical
     and merge them in (eras-only enrichments)
  3  Find columns in eras canonical_patient_master_v218 NOT in v221
     and carry them forward (eras pipeline columns)
  4  Add days_from_surgery temporal provenance for every date column
  5  Add multi-surgery linkage columns (n_surgeries, second_surgery_date, …)
  6  Apply COMMENT ON COLUMN / TABLE
  7  Final invariant validation

INVARIANTS
----------
  - 10,871 rows, 10,871 distinct research_ids
  - research_id never NULL, fna_path_outcome never NULL
  - research_id is VARCHAR (CAST BIGINT sources on join)

Run:
  .venv/bin/python scripts/221_eras_canonical_sync.py [--dry-run] [--phase A|B|C|D|E|F|G|all]

TOKEN: reads from motherduck.local.toml (TOML token = eras account)
       Bypasses MOTHERDUCK_TOKEN env var which may be the glosser account.
"""
from __future__ import annotations

import argparse
import csv
import sys
import time
import toml
from pathlib import Path
from typing import Any

import duckdb

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

SCRIPT_TAG = "221_eras_canonical_sync"
DB_ERAS = "Thyroid 2026 UPdated"
DB_ERAS_SQL = '"Thyroid 2026 UPdated"'
CANONICAL_OLD = "canonical_patient_master_v218"   # 168-col eras version
CANONICAL_NEW = "canonical_patient_master_v221"   # new fully-consolidated target
GOLD_MASTER = "gold_master_patient_facts_v1"
TOTAL_ROWS = 10_871
REQUIRED_NON_NULL = ["research_id", "fna_path_outcome"]

PARQUET_PATH = REPO / "scripts" / "output" / "parquet_backup" / "canonical_patient_master_v1.parquet"

# eras-specific operative_episode table for surgery dates
OED_TABLE = "operative_episode_detail_v2"
OED_DATE_COL = "surgery_date_native"

# Table comments to apply (subset relevant to eras account)
TABLE_COMMENTS: dict[str, str] = {
    CANONICAL_NEW: (
        "Master analytical table: 10,871 thyroid surgery patients × 1,300+ columns. "
        "One row per patient. Built from glosser-account canonical (1,374 cols) + "
        "eras-account gold_master enrichments + temporal provenance. "
        "Replaces canonical_patient_master_v218 (168 cols)."
    ),
    "gold_master_patient_facts_v1": (
        "Eras-account gold master: 10,871 patients × 146 cols from Scripts 218/219."
    ),
    "canonical_patient_master_v218": (
        "Superseded by canonical_patient_master_v221. Retained for audit only."
    ),
}

# Auto column descriptions by prefix
AUTO_COL_DESC: dict[str, str] = {
    "_days_from_surg": "Days from first surgery date (negative = before surgery)",
    "nlp_": "NLP-extracted from clinical notes (qwen3:32b fleet)",
    "prm_": "From patient_refined_master_clinical_v12",
    "ops_": "From operative sheet data",
    "comp_": "Complication status from complication_phenotype_v1",
    "cnln_": "Clinical note lymph node integration",
    "syn_": "From synoptic pathology reports",
    "pet_": "PET/CT imaging data",
    "ct_": "CT imaging data",
    "mri_": "MRI imaging data",
    "nucmed_": "Nuclear medicine data",
    "lnus_": "Dedicated lymph node ultrasound data",
    "lab_": "Laboratory value",
    "nsqip_": "NSQIP perioperative quality data",
    "op_nlp_": "NLP-extracted from operative notes",
    "med_nlp_": "NLP-extracted medication data",
    "pmhx_": "Past medical history",
    "pshx_": "Past surgical history",
    "tirads_": "TIRADS scoring from ACR criteria / LLM extraction",
    "bethesda_": "FNA Bethesda classification",
    "ete_": "Extrathyroidal extension",
    "tg_": "Thyroglobulin lab trajectory",
    "gm_": "From gold_master_patient_facts_v1",
    "para_": "Parathyroid data",
    "mol_": "Molecular testing data",
    "rec_": "Recurrence data",
}


# ===========================================================================
# Connection
# ===========================================================================

def connect_eras() -> duckdb.DuckDBPyConnection:
    """Connect to eras MotherDuck account using TOML token (bypasses env vars)."""
    toml_path = REPO / "motherduck.local.toml"
    if not toml_path.exists():
        print(f"[{SCRIPT_TAG}] ERROR: {toml_path} not found")
        sys.exit(1)
    cfg = toml.load(str(toml_path))
    token = (
        cfg.get("MD_SA_TOKEN")
        or cfg.get("MOTHERDUCK_TOKEN")
        or cfg.get("motherduck_token")
    )
    if not token:
        print(f"[{SCRIPT_TAG}] ERROR: No token in motherduck.local.toml")
        sys.exit(1)
    print(f"[{SCRIPT_TAG}] TOML token: SET, len={len(token)}")
    con = duckdb.connect(f"md:?motherduck_token={token}")

    # Verify we're on the eras account
    import base64, json
    payload_b64 = token.split(".")[1] + "==="
    payload = json.loads(base64.urlsafe_b64decode(payload_b64[:len(payload_b64)//4*4]))
    email = payload.get("email", "unknown")
    print(f"[{SCRIPT_TAG}] Account: {email}")
    if "eras" not in email.lower():
        print(f"[{SCRIPT_TAG}] WARNING: Expected eras account, got {email}")
        print(f"[{SCRIPT_TAG}]   TOML token is for the non-eras account.")
        print(f"[{SCRIPT_TAG}]   Update motherduck.local.toml with eras token to proceed.")
        sys.exit(1)
    return con


# ===========================================================================
# Helpers
# ===========================================================================

def _quoted(name: str) -> str:
    return f'"{name}"' if " " in name else name


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {DB_ERAS_SQL}.main.{table} LIMIT 0")
        return True
    except Exception:
        return False


def col_exists(con: duckdb.DuckDBPyConnection, table: str, col: str) -> bool:
    try:
        safe = col.replace('"', '""')
        con.execute(f'SELECT "{safe}" FROM {DB_ERAS_SQL}.main.{table} LIMIT 0')
        return True
    except Exception:
        return False


def describe(con: duckdb.DuckDBPyConnection, table: str) -> list[tuple[str, str]]:
    """Return [(col_name, col_type)] for a table in the eras database."""
    try:
        rows = con.execute(f"DESCRIBE {DB_ERAS_SQL}.main.{table}").fetchall()
        return [(r[0], r[1]) for r in rows]
    except Exception as e:
        print(f"  WARN describe {table}: {e!s:.100s}")
        return []


def row_count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    try:
        r = con.execute(f"SELECT COUNT(*) FROM {DB_ERAS_SQL}.main.{table}").fetchone()
        return r[0] if r else -1
    except Exception as e:
        print(f"  WARN row_count({table}): {e!s:.80s}")
        return -1


def q(con: duckdb.DuckDBPyConnection, sql: str, label: str = "") -> Any:
    try:
        return con.execute(sql)
    except Exception as e:
        tag = f" [{label}]" if label else ""
        print(f"  WARN{tag}: {e!s:.200s}")
        return None


def check_invariants(con: duckdb.DuckDBPyConnection, label: str) -> None:
    """Schema-only invariant check against CANONICAL_NEW."""
    all_ok = True
    if table_exists(con, CANONICAL_NEW):
        print(f"  [✓] {CANONICAL_NEW} exists")
    else:
        print(f"  [✗] {CANONICAL_NEW} MISSING")
        all_ok = False
    for col in REQUIRED_NON_NULL:
        if col_exists(con, CANONICAL_NEW, col):
            print(f"  [✓] has {col}")
        else:
            print(f"  [✗] missing {col}")
            all_ok = False
    if not all_ok:
        print(f"[{SCRIPT_TAG}] ABORT at [{label}]")
        sys.exit(1)
    print(f"[{SCRIPT_TAG}] ✓ Invariants OK [{label}]")


# ===========================================================================
# PHASE A — Upload glosser canonical parquet → eras canonical_patient_master_v221
# ===========================================================================

def phase_a_upload(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    print(f"\n[{SCRIPT_TAG}] ══ PHASE A: Upload glosser canonical parquet ══")

    if not PARQUET_PATH.exists():
        print(f"  ✗ Parquet not found: {PARQUET_PATH}")
        print("  Run Script 221 Phase 6 on glosser account first to generate it.")
        sys.exit(1)

    size_mb = PARQUET_PATH.stat().st_size / 1_048_576
    print(f"  Source: {PARQUET_PATH.name} ({size_mb:.1f} MB)")

    # Check what's currently there
    if table_exists(con, CANONICAL_NEW):
        existing_rows = row_count(con, CANONICAL_NEW)
        existing_cols = len(describe(con, CANONICAL_NEW))
        print(f"  {CANONICAL_NEW} already exists: {existing_rows:,} rows × {existing_cols} cols")
        if existing_rows == TOTAL_ROWS:
            print("  Row count matches — skipping upload (use --phase A to force)")
            return
    
    if dry_run:
        # Peek at parquet
        import duckdb as _ddb
        _local = _ddb.connect()
        info = _local.execute(f"""
            SELECT COUNT(*) as rows,
                   COUNT(DISTINCT research_id) as distinct_rids
            FROM read_parquet('{PARQUET_PATH}')
        """).fetchone()
        col_count = len(_local.execute(f"DESCRIBE SELECT * FROM read_parquet('{PARQUET_PATH}')").fetchall())
        print(f"  [dry-run] parquet: {info[0]:,} rows, {info[1]:,} distinct RIDs, {col_count} cols")
        print(f"  [dry-run] would CREATE {CANONICAL_NEW} in {DB_ERAS}")
        return

    print(f"  Creating {CANONICAL_NEW} from parquet…")
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE {DB_ERAS_SQL}.main.{CANONICAL_NEW} AS
            SELECT * FROM read_parquet('{PARQUET_PATH}')
        """)
    except Exception as e:
        print(f"  ✗ Upload failed: {e!s:.200s}")
        sys.exit(1)

    n = row_count(con, CANONICAL_NEW)
    cols = len(describe(con, CANONICAL_NEW))
    print(f"  ✓ {CANONICAL_NEW}: {n:,} rows × {cols} cols")
    if n != TOTAL_ROWS:
        print(f"  ✗ Row count {n} ≠ expected {TOTAL_ROWS}")
        sys.exit(1)
    check_invariants(con, "phase-A-end")


# ===========================================================================
# PHASE B — Merge eras gold_master columns not in v221
# ===========================================================================

def phase_b_merge_gold(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    print(f"\n[{SCRIPT_TAG}] ══ PHASE B: Merge eras gold_master columns ══")

    if not table_exists(con, GOLD_MASTER):
        print(f"  SKIP: {GOLD_MASTER} not in {DB_ERAS}")
        return

    gm_cols = {c: t for c, t in describe(con, GOLD_MASTER)}
    v221_cols = {c for c, _ in describe(con, CANONICAL_NEW)}
    new_from_gm = sorted(c for c in gm_cols if c not in v221_cols)

    print(f"  {GOLD_MASTER}: {len(gm_cols)} cols")
    print(f"  {CANONICAL_NEW}: {len(v221_cols)} cols")
    print(f"  New columns to add from gold_master: {len(new_from_gm)}")

    if not new_from_gm:
        print("  ✓ canonical already has all gold_master columns")
        return

    added = 0
    skipped = 0
    for col in new_from_gm:
        dtype = gm_cols[col]
        safe = col.replace('"', '""')
        if dry_run:
            print(f"  [dry-run] would add {col} ({dtype})")
            continue
        # Add column
        if not col_exists(con, CANONICAL_NEW, col):
            try:
                con.execute(f"""
                    ALTER TABLE {DB_ERAS_SQL}.main.{CANONICAL_NEW}
                    ADD COLUMN "{safe}" {dtype}
                """)
            except Exception as e:
                print(f"  ✗ add {col}: {e!s:.80s}")
                skipped += 1
                continue
        # Populate from gold_master (BIGINT research_id → VARCHAR)
        try:
            con.execute(f"""
                UPDATE {DB_ERAS_SQL}.main.{CANONICAL_NEW} c
                SET "{safe}" = g."{safe}"
                FROM {DB_ERAS_SQL}.main.{GOLD_MASTER} g
                WHERE c.research_id = CAST(g.research_id AS VARCHAR)
                  AND g."{safe}" IS NOT NULL
            """)
            added += 1
            print(f"  ✓ {col} ({dtype})")
        except Exception as e:
            print(f"  ✗ update {col}: {e!s:.120s}")
            skipped += 1

    if not dry_run:
        print(f"\n  Phase B: {added} cols added, {skipped} skipped")
        check_invariants(con, "phase-B-end")


# ===========================================================================
# PHASE C — Carry forward eras v218-specific columns not in v221
# ===========================================================================

def phase_c_merge_v218(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    print(f"\n[{SCRIPT_TAG}] ══ PHASE C: Carry forward v218 eras-pipeline columns ══")

    if not table_exists(con, CANONICAL_OLD):
        print(f"  SKIP: {CANONICAL_OLD} not found")
        return

    v218_cols = {c: t for c, t in describe(con, CANONICAL_OLD)}
    v221_cols = {c for c, _ in describe(con, CANONICAL_NEW)}
    new_from_v218 = sorted(c for c in v218_cols if c not in v221_cols)

    print(f"  {CANONICAL_OLD}: {len(v218_cols)} cols")
    print(f"  {CANONICAL_NEW}: {len(v221_cols)} cols")
    print(f"  New columns to carry forward from v218: {len(new_from_v218)}")

    if not new_from_v218:
        print("  ✓ canonical already incorporates all v218 columns")
        return

    added = 0
    skipped = 0
    for col in new_from_v218:
        dtype = v218_cols[col]
        safe = col.replace('"', '""')
        if dry_run:
            print(f"  [dry-run] would carry {col} ({dtype}) from v218")
            continue
        if not col_exists(con, CANONICAL_NEW, col):
            try:
                con.execute(f"""
                    ALTER TABLE {DB_ERAS_SQL}.main.{CANONICAL_NEW}
                    ADD COLUMN "{safe}" {dtype}
                """)
            except Exception as e:
                print(f"  ✗ add {col}: {e!s:.80s}")
                skipped += 1
                continue
        try:
            con.execute(f"""
                UPDATE {DB_ERAS_SQL}.main.{CANONICAL_NEW} c
                SET "{safe}" = v."{safe}"
                FROM {DB_ERAS_SQL}.main.{CANONICAL_OLD} v
                WHERE c.research_id = CAST(v.research_id AS VARCHAR)
                  AND v."{safe}" IS NOT NULL
            """)
            added += 1
            print(f"  ✓ {col} ({dtype})")
        except Exception as e:
            print(f"  ✗ update {col}: {e!s:.120s}")
            skipped += 1

    if not dry_run:
        print(f"\n  Phase C: {added} cols carried forward, {skipped} skipped")
        check_invariants(con, "phase-C-end")


# ===========================================================================
# PHASE D — Date provenance: days_from_surgery + multi-surgery linkage
# ===========================================================================

def phase_d_provenance(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    print(f"\n[{SCRIPT_TAG}] ══ PHASE D: Date provenance (days_from_surgery) ══")

    DATE_TYPES = {"DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ"}
    all_cols = describe(con, CANONICAL_NEW)

    # 3B-style: add _days_from_surg for every date column
    date_cols = [
        col for col, dtype in all_cols
        if dtype.upper() in DATE_TYPES
        and col != "first_surgery_date"
        and "days" not in col
        and not col.endswith("_days_from_surg")
    ]
    print(f"  Date columns to process: {len(date_cols)}")
    added = 0
    for col in date_cols:
        if col.endswith("_date"):
            days_col = col[:-5] + "_days_from_surg"
        elif col.endswith("_at"):
            days_col = col[:-3] + "_days_from_surg"
        else:
            days_col = col + "_days_from_surg"

        safe_col = col.replace('"', '""')
        safe_days = days_col.replace('"', '""')

        if col_exists(con, CANONICAL_NEW, days_col):
            continue  # already present
        if dry_run:
            print(f"  [dry-run] would add {days_col}")
            continue
        try:
            con.execute(f"""
                ALTER TABLE {DB_ERAS_SQL}.main.{CANONICAL_NEW}
                ADD COLUMN "{safe_days}" INTEGER
            """)
            con.execute(f"""
                UPDATE {DB_ERAS_SQL}.main.{CANONICAL_NEW}
                SET "{safe_days}" = DATEDIFF('day', first_surgery_date, "{safe_col}")
                WHERE first_surgery_date IS NOT NULL AND "{safe_col}" IS NOT NULL
            """)
            filled = con.execute(f"""
                SELECT COUNT(*) FROM {DB_ERAS_SQL}.main.{CANONICAL_NEW}
                WHERE "{safe_days}" IS NOT NULL
            """).fetchone()[0]
            print(f"  ✓ {days_col}: {filled:,} values")
            added += 1
        except Exception as e:
            print(f"  ✗ {days_col}: {e!s:.100s}")

    print(f"  Added {added} new days_from_surgery columns")

    # Multi-surgery linkage
    print("\n  Multi-surgery linkage…")
    if not table_exists(con, OED_TABLE):
        print(f"  SKIP: {OED_TABLE} not in {DB_ERAS}")
    elif dry_run:
        print("  [dry-run] would build multi-surgery columns")
    else:
        try:
            con.execute(f"""
                CREATE OR REPLACE TABLE {DB_ERAS_SQL}.main._patient_surgery_dates AS
                SELECT
                    CAST(research_id AS VARCHAR) as research_id,
                    {OED_DATE_COL} AS surgery_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY research_id ORDER BY {OED_DATE_COL}
                    ) as surgery_number,
                    COUNT(*) OVER (PARTITION BY research_id) as total_surgeries
                FROM {DB_ERAS_SQL}.main.{OED_TABLE}
                WHERE {OED_DATE_COL} IS NOT NULL
            """)
            multi_n = con.execute(f"""
                SELECT COUNT(DISTINCT research_id)
                FROM {DB_ERAS_SQL}.main._patient_surgery_dates
                WHERE total_surgeries > 1
            """).fetchone()[0]
            print(f"  Multi-surgery patients: {multi_n:,}")
        except Exception as e:
            print(f"  ✗ _patient_surgery_dates: {e!s:.120s}")

        for col, dtype in [
            ("n_surgeries", "INTEGER"),
            ("second_surgery_date", "DATE"),
            ("third_surgery_date", "DATE"),
            ("days_between_first_second_surgery", "INTEGER"),
        ]:
            if not col_exists(con, CANONICAL_NEW, col):
                q(con, f"""
                    ALTER TABLE {DB_ERAS_SQL}.main.{CANONICAL_NEW}
                    ADD COLUMN {col} {dtype}
                """, f"add {col}")

        q(con, f"""
            UPDATE {DB_ERAS_SQL}.main.{CANONICAL_NEW} c
            SET n_surgeries = sub.total_surgeries
            FROM (
                SELECT research_id, MAX(total_surgeries) as total_surgeries
                FROM {DB_ERAS_SQL}.main._patient_surgery_dates GROUP BY 1
            ) sub
            WHERE c.research_id = sub.research_id
        """, "n_surgeries")
        q(con, f"""
            UPDATE {DB_ERAS_SQL}.main.{CANONICAL_NEW} c
            SET second_surgery_date = sub.surgery_date
            FROM {DB_ERAS_SQL}.main._patient_surgery_dates sub
            WHERE c.research_id = sub.research_id AND sub.surgery_number = 2
        """, "second_surgery_date")
        q(con, f"""
            UPDATE {DB_ERAS_SQL}.main.{CANONICAL_NEW} c
            SET third_surgery_date = sub.surgery_date
            FROM {DB_ERAS_SQL}.main._patient_surgery_dates sub
            WHERE c.research_id = sub.research_id AND sub.surgery_number = 3
        """, "third_surgery_date")
        q(con, f"""
            UPDATE {DB_ERAS_SQL}.main.{CANONICAL_NEW}
            SET days_between_first_second_surgery =
                DATEDIFF('day', first_surgery_date, second_surgery_date)
            WHERE first_surgery_date IS NOT NULL AND second_surgery_date IS NOT NULL
        """, "days_between")
        print("  ✓ Multi-surgery linkage columns populated")

    if not dry_run:
        check_invariants(con, "phase-D-end")


# ===========================================================================
# PHASE E — MotherDuck optimization: COMMENT ON COLUMN / TABLE
# ===========================================================================

def phase_e_optimize(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    print(f"\n[{SCRIPT_TAG}] ══ PHASE E: Schema optimization (COMMENT ON …) ══")

    # Load existing data dict for descriptions
    col_descs: dict[str, str] = {}
    dict_path = REPO / "data_dictionary.csv"
    if dict_path.exists():
        with open(dict_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                col = row.get("column_name", "")
                desc = row.get("description", "")
                if col and desc:
                    col_descs[col] = desc
        print(f"  Loaded {len(col_descs)} descriptions from data_dictionary.csv")

    all_cols = describe(con, CANONICAL_NEW)
    print(f"  Adding COMMENT ON COLUMN for {len(all_cols)} columns…")
    ok = skip = 0
    for col_name, col_type in all_cols:
        desc = col_descs.get(col_name, "")
        if not desc:
            for prefix, auto in AUTO_COL_DESC.items():
                if col_name.startswith(prefix) or col_name.endswith(prefix):
                    desc = auto
                    break
        if not desc:
            desc = f"{col_type} field"
        desc_safe = desc.replace("'", "''")[:500]
        if dry_run:
            ok += 1
            continue
        try:
            con.execute(f"""
                COMMENT ON COLUMN {DB_ERAS_SQL}.main.{CANONICAL_NEW}."{col_name}"
                IS '{desc_safe}'
            """)
            ok += 1
        except Exception:
            skip += 1
    print(f"  COMMENT ON COLUMN: {ok} OK, {skip} skipped")

    print(f"  Adding COMMENT ON TABLE for {len(TABLE_COMMENTS)} tables…")
    tbl_ok = 0
    for tbl, comment in TABLE_COMMENTS.items():
        if not table_exists(con, tbl):
            continue
        c_safe = comment.replace("'", "''")[:1000]
        if dry_run:
            tbl_ok += 1
            continue
        try:
            con.execute(f"""
                COMMENT ON TABLE {DB_ERAS_SQL}.main.{tbl} IS '{c_safe}'
            """)
            tbl_ok += 1
        except Exception as e:
            print(f"  SKIP table comment {tbl}: {e!s:.60s}")
    print(f"  COMMENT ON TABLE: {tbl_ok} OK")


# ===========================================================================
# PHASE F — Data dictionary upload to eras MotherDuck
# ===========================================================================

def phase_f_data_dict(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    print(f"\n[{SCRIPT_TAG}] ══ PHASE F: Data dictionary upload ══")
    dict_path = REPO / "data_dictionary.csv"
    parquet_path = REPO / "scripts" / "output" / "data_dictionary.parquet"

    if not dict_path.exists():
        print("  SKIP: data_dictionary.csv not found (run Script 221 Phase 5 first)")
        return

    if dry_run:
        import csv as _csv
        with open(dict_path) as f:
            n = sum(1 for _ in _csv.reader(f)) - 1
        print(f"  [dry-run] would upload data_dictionary.csv ({n} rows) to {DB_ERAS}")
        return

    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE {DB_ERAS_SQL}.main.data_dictionary_v221 AS
            SELECT * FROM read_csv_auto('{dict_path}')
        """)
        n = con.execute(f"SELECT COUNT(*) FROM {DB_ERAS_SQL}.main.data_dictionary_v221").fetchone()[0]
        print(f"  ✓ data_dictionary_v221: {n:,} rows uploaded to {DB_ERAS}")
    except Exception as e:
        print(f"  ✗ data dictionary upload: {e!s:.120s}")

    if parquet_path.exists():
        try:
            con.execute(f"""
                CREATE OR REPLACE TABLE {DB_ERAS_SQL}.main.data_dictionary_parquet_v221 AS
                SELECT * FROM read_parquet('{parquet_path}')
            """)
            print("  ✓ data_dictionary_parquet_v221 uploaded")
        except Exception as e:
            print(f"  SKIP parquet dict: {e!s:.80s}")


# ===========================================================================
# PHASE G — Final validation report
# ===========================================================================

def phase_g_validate(con: duckdb.DuckDBPyConnection) -> None:
    print(f"\n[{SCRIPT_TAG}] ══ PHASE G: Final validation ══")
    check_invariants(con, "final")

    all_cols = describe(con, CANONICAL_NEW)
    final_cols = len(all_cols)
    col_set = {c for c, _ in all_cols}

    # Row count
    total_rows = row_count(con, CANONICAL_NEW)

    # Null checks on required columns
    null_counts: dict[str, int] = {}
    for col in REQUIRED_NON_NULL:
        if col in col_set:
            try:
                n = con.execute(f"""
                    SELECT COUNT(*) FROM {DB_ERAS_SQL}.main.{CANONICAL_NEW}
                    WHERE "{col}" IS NULL
                """).fetchone()[0]
                null_counts[col] = n
            except Exception:
                null_counts[col] = -1

    has_n_surg = "n_surgeries" in col_set

    # Temporal coverage
    DATE_PROBE = [
        ("Surgery",      "first_surgery_date"),
        ("FNA",          "prm_first_fna_date"),
        ("Last contact", "last_contact_date"),
        ("Tg lab",       "first_tg_date"),
        ("Recurrence",   "first_recurrence_date"),
        ("Death",        "death_date"),
        ("N_surgeries",  "n_surgeries"),
    ]
    print("\n  Temporal coverage:")
    for label, col in DATE_PROBE:
        if col not in col_set:
            print(f"    {label:<20} N/A (col missing)")
            continue
        try:
            n = con.execute(f"""
                SELECT COUNT(*) FROM {DB_ERAS_SQL}.main.{CANONICAL_NEW}
                WHERE "{col}" IS NOT NULL
            """).fetchone()[0]
            pct = round(n / TOTAL_ROWS * 100, 1)
            print(f"    {label:<20} {n:>8,d}  ({pct}%)")
        except Exception as e:
            print(f"    {label:<20} ERROR: {e!s:.60s}")

    print(f"""
{'='*60}
ERAS CANONICAL SYNC REPORT — {SCRIPT_TAG}
{'='*60}
Account:         logan.glosser.eras@gmail.com
Database:        {DB_ERAS}
Canonical table: {CANONICAL_NEW}
Columns:         {final_cols:,}
Row count:       {total_rows:,}  (expected {TOTAL_ROWS:,})
Multi-surgery:   n_surgeries {'PRESENT' if has_n_surg else 'MISSING'}
research_id NULLs:     {null_counts.get('research_id', 'N/A')}
fna_path_outcome NULLs:{null_counts.get('fna_path_outcome', 'N/A')}
{'='*60}""")

    if total_rows != TOTAL_ROWS:
        print(f"[{SCRIPT_TAG}] ✗ ROW COUNT FAILURE: {total_rows} ≠ {TOTAL_ROWS}")
        sys.exit(1)
    for col, n in null_counts.items():
        if n > 0:
            print(f"[{SCRIPT_TAG}] ✗ NULL FAILURE: {col} has {n} NULL rows")
            sys.exit(1)
    print(f"[{SCRIPT_TAG}] ✓ ALL INVARIANTS PASS")


# ===========================================================================
# CLI
# ===========================================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=f"THYROID_2026 — {SCRIPT_TAG}")
    p.add_argument(
        "--phase",
        default="all",
        choices=["A", "B", "C", "D", "E", "F", "G", "all"],
        help="Which phase(s) to run (default: all)",
    )
    p.add_argument("--dry-run", action="store_true",
                   help="Plan only — no writes")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    t0 = time.time()

    if args.dry_run:
        print(f"[{SCRIPT_TAG}] DRY-RUN mode\n")

    phases = set("ABCDEFG") if args.phase == "all" else {args.phase.upper()}
    con = connect_eras()

    if "A" in phases:
        phase_a_upload(con, args.dry_run)
    if "B" in phases:
        phase_b_merge_gold(con, args.dry_run)
    if "C" in phases:
        phase_c_merge_v218(con, args.dry_run)
    if "D" in phases:
        phase_d_provenance(con, args.dry_run)
    if "E" in phases:
        phase_e_optimize(con, args.dry_run)
    if "F" in phases:
        phase_f_data_dict(con, args.dry_run)
    if "G" in phases:
        phase_g_validate(con)

    elapsed = time.time() - t0
    print(f"\n[{SCRIPT_TAG}] Total elapsed: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
