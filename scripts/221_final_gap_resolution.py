#!/usr/bin/env python3
"""
THYROID_2026 — Script 221: Final Gap Resolution
  - NSQIP Thyroidectomy Enrichment (1,261 patients, 86+ new columns)
  - Parathyroid Notes Intent Integration (3,878 patients, ~15 para_* columns)

Database: thyroid_ete_fix_20260413 on MotherDuck
Canonical: canonical_patient_master_v1 (10,871 rows)

Run:
  .venv/bin/python scripts/221_final_gap_resolution.py [--dry-run] [--phase 1|2|3|all]
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from motherduck_client import get_token  # noqa: E402

DB = "thyroid_ete_fix_20260413"
CANONICAL = "canonical_patient_master_v1"
TOTAL_ROWS = 10871
SCRIPT_TAG = "221_final_gap_resolution"

PHI_COLS = {"nsqip_dob", "nsqip_death_date", "nsqip_case_number"}

# ============================================================
# Connection + utilities
# ============================================================

def connect() -> duckdb.DuckDBPyConnection:
    token = get_token()
    if not token:
        print("[221] ERROR: No MotherDuck token found.")
        sys.exit(1)
    print(f"[221] Token: SET, len={len(token)}")
    con = duckdb.connect(f"md:{DB}?motherduck_token={token}")
    print(f"[221] Connected to {DB}")
    return con


def check_invariants_light(con: duckdb.DuckDBPyConnection, label: str) -> bool:
    """
    Lightweight invariant check using metadata-only queries.
    Avoids scanning the 1000+ column canonical table (which hangs on MotherDuck for wide tables).
    """
    # Use information_schema for column existence (fast metadata)
    has_rid = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_name = '{CANONICAL}' AND table_schema = 'main'
          AND column_name = 'research_id'
    """).fetchone()[0]
    has_fna = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_name = '{CANONICAL}' AND table_schema = 'main'
          AND column_name = 'fna_path_outcome'
    """).fetchone()[0]
    tbl_exists = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name = '{CANONICAL}' AND table_schema = 'main'
    """).fetchone()[0]

    ok = tbl_exists > 0 and has_rid > 0 and has_fna > 0
    status = "✓" if ok else "✗ FAIL"
    print(f"[221] {status} Schema invariants [{label}]: "
          f"table_exists={tbl_exists}, has_research_id={has_rid}, has_fna_path_outcome={has_fna}")
    if not ok:
        print("[221] ERROR: Schema invariants broken — aborting")
        sys.exit(1)
    return ok


def check_invariants(con: duckdb.DuckDBPyConnection, label: str) -> bool:
    """Full invariant check — scans research_id and fna_path_outcome columns.
    May be slow on very wide tables (1000+ cols). Use check_invariants_light() as alternative."""
    total = con.execute(f"SELECT COUNT(research_id) FROM {CANONICAL}").fetchone()[0]
    distinct = con.execute(f"SELECT COUNT(DISTINCT research_id) FROM {CANONICAL}").fetchone()[0]
    null_fna = con.execute(
        f"SELECT COUNT(*) FROM (SELECT fna_path_outcome FROM {CANONICAL} WHERE fna_path_outcome IS NULL)"
    ).fetchone()[0]

    ok = total == TOTAL_ROWS and distinct == TOTAL_ROWS and null_fna == 0
    status = "✓" if ok else "✗ FAIL"
    print(f"[221] {status} Invariants [{label}]: "
          f"rows={total}, distinct_rids={distinct}, null_fna={null_fna}")
    if not ok:
        print("[221] ERROR: Invariants broken — aborting")
        sys.exit(1)
    return ok


def get_existing_columns(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(f"""
        SELECT DISTINCT column_name
        FROM information_schema.columns
        WHERE table_name = '{CANONICAL}' AND table_schema = 'main'
    """).fetchall()
    return {r[0] for r in rows}


def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    r = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name = '{name}' AND table_schema = 'main'
    """).fetchone()
    return r[0] > 0


def run_sql(con: duckdb.DuckDBPyConnection, sql: str, label: str, dry_run: bool = False) -> None:
    if dry_run:
        print(f"[221] DRY-RUN — would execute: {label}")
        return
    t0 = time.time()
    con.execute(sql)
    print(f"[221] {label} — done in {time.time()-t0:.1f}s")


def check_orphans(con: duckdb.DuckDBPyConnection, staging_table: str, label: str) -> int:
    r = con.execute(f"""
        SELECT COUNT(*) FROM {staging_table}
        WHERE research_id NOT IN (SELECT research_id FROM {CANONICAL})
    """).fetchone()[0]
    if r > 0:
        print(f"[221] WARNING: {r} orphan research_ids in {label} (not in canonical spine)")
    else:
        print(f"[221] OK: 0 orphan research_ids in {label}")
    return r


def backup_canonical(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    """Create backup of canonical — using ALTER TABLE + CREATE approach for narrow clone."""
    backup_name = f"{CANONICAL}_pre221"
    if dry_run:
        print(f"[221] DRY-RUN — would create backup: {backup_name}")
        return
    # Use CREATE TABLE AS with research_id check only (fast metadata approach)
    run_sql(con, f"CREATE OR REPLACE TABLE {backup_name} AS SELECT * FROM {CANONICAL}",
            f"Backup → {backup_name}")
    # Verify backup via research_id count only (narrow scan)
    n = con.execute(f"SELECT COUNT(research_id) FROM {backup_name}").fetchone()[0]
    assert n == TOTAL_ROWS, f"Backup has {n} rows, expected {TOTAL_ROWS}"
    print(f"[221] ✓ Backup created: {backup_name} ({n} rows)")


def _pd_dtype_to_duckdb(dtype: str) -> str:
    """Map pandas dtype string to a safe DuckDB column type."""
    dtype = str(dtype).lower()
    if dtype in ("int64", "int32", "int16", "int8", "uint64", "uint32", "uint16", "uint8"):
        return "BIGINT"
    if dtype in ("float64", "float32"):
        return "DOUBLE"
    if dtype == "bool":
        return "BOOLEAN"
    # object/string/datetime → VARCHAR (safest for heterogeneous data)
    return "VARCHAR"


def safe_add_column(con: duckdb.DuckDBPyConnection, col: str, dtype: str) -> None:
    try:
        con.execute(f'ALTER TABLE {CANONICAL} ADD COLUMN "{col}" {dtype}')
        print(f"[221]   + added column {col} ({dtype})")
    except Exception:
        pass  # column already exists


def integrate_staging_into_canonical(
    con: duckdb.DuckDBPyConnection,
    staging_table: str,
    new_cols: list[str],
    col_dtypes: dict[str, str],  # col -> duckdb type
    extra_sentinel_cols: list[tuple[str, str, str]],  # (col_name, dtype, expr)
    label: str,
) -> None:
    """
    Integrate staging_table into canonical using ALTER TABLE + UPDATE pattern.
    This avoids full-table recreation (much faster for 1000+ column tables).
    """
    if not new_cols and not extra_sentinel_cols:
        print(f"[221] No new columns from {staging_table} — skipping")
        return

    # Step A: ADD new columns
    all_new = [(c, col_dtypes.get(c, "VARCHAR")) for c in new_cols]
    all_new += [(name, dtype) for name, dtype, _ in extra_sentinel_cols]
    for col, dtype in all_new:
        safe_add_column(con, col, dtype)

    # Step B: UPDATE from staging for data columns
    if new_cols:
        set_clauses = ",\n            ".join(f'"{c}" = s."{c}"' for c in new_cols)
        update_sql = f"""
UPDATE {CANONICAL}
SET
    {set_clauses}
FROM {staging_table} s
WHERE {CANONICAL}.research_id = s.research_id
"""
        t0 = time.time()
        con.execute(update_sql)
        rc = con.execute(f"SELECT COUNT(*) FROM {CANONICAL} WHERE \"{new_cols[0]}\" IS NOT NULL").fetchone()[0]
        print(f"[221]   {label} UPDATE done in {time.time()-t0:.1f}s — {rc} rows filled for {new_cols[0]}")

    # Step C: UPDATE sentinels (constant expressions, applied to matched rows)
    for col_name, _, expr in extra_sentinel_cols:
        # Sentinel values: set for all rows (e.g., has_data flag, source_script)
        if "research_id" in expr.lower() or staging_table in expr.lower():
            # Set TRUE/value where staging has a match
            update_sentinel = f"""
UPDATE {CANONICAL}
SET "{col_name}" = {expr}
WHERE research_id IN (SELECT research_id FROM {staging_table})
"""
        else:
            # Literal constant: set for all rows
            update_sentinel = f'UPDATE {CANONICAL} SET "{col_name}" = {expr}'
        try:
            con.execute(update_sentinel)
            print(f"[221]   Sentinel {col_name} updated")
        except Exception as e:
            print(f"[221]   Sentinel {col_name} update failed: {e}")


# ============================================================
# TASK 1: NSQIP THYROIDECTOMY ENRICHMENT
# ============================================================

STAGING_NSQIP = "_nsqip_thyroidectomy_enrichment_v1"


def phase1_nsqip(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    print("\n" + "=" * 70)
    print("[221] TASK 1: NSQIP THYROIDECTOMY ENRICHMENT")
    print("=" * 70)

    # ------------------------------------------------------------------
    # Step 1.1: Load parquet, drop PHI, cast research_id, upload
    # ------------------------------------------------------------------
    nsqip_path = REPO / "exports" / "nsqip" / "nsqip_patient_summary.parquet"
    if not nsqip_path.exists():
        csv_path = REPO / "exports" / "nsqip" / "nsqip_patient_summary.csv"
        if not csv_path.exists():
            print(f"[221] ERROR: Cannot find NSQIP enrichment at {nsqip_path}")
            sys.exit(1)
        print("[221] Parquet not found — loading CSV fallback")
        df = pd.read_csv(csv_path)
    else:
        df = pd.read_parquet(nsqip_path)

    print(f"[221] Loaded NSQIP enrichment: {len(df)} rows × {len(df.columns)} columns")
    print(f"[221] research_id dtype: {df['research_id'].dtype}, nunique: {df['research_id'].nunique()}")

    # Validate
    assert len(df) == df["research_id"].nunique(), "FAIL: duplicate research_ids in NSQIP"
    assert df["research_id"].notna().all(), "FAIL: null research_ids in NSQIP"

    # Drop PHI
    drop_cols = [c for c in PHI_COLS if c in df.columns]
    df = df.drop(columns=drop_cols)
    print(f"[221] Dropped PHI columns: {drop_cols}")
    print(f"[221] After PHI removal: {len(df.columns)} columns")

    # Cast research_id to string for canonical compatibility
    df["research_id"] = df["research_id"].astype(str)

    if dry_run:
        print(f"[221] DRY-RUN — would upload {len(df)} rows to {STAGING_NSQIP}")
    else:
        # Write to temp parquet and upload
        tmp = REPO / "scripts" / "output" / "_nsqip_staging_221.parquet"
        tmp.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(str(tmp), index=False)
        con.execute(f"""
            CREATE OR REPLACE TABLE {STAGING_NSQIP} AS
            SELECT * FROM read_parquet('{tmp}')
        """)
        try:
            tmp.unlink()
        except Exception:
            pass
        verify = con.execute(f"""
            SELECT COUNT(*) as n_rows, COUNT(DISTINCT research_id) as n_rids
            FROM {STAGING_NSQIP}
        """).fetchone()
        print(f"[221] ✓ Uploaded {STAGING_NSQIP}: {verify[0]} rows, {verify[1]} patients")
        assert verify[0] == 1261 and verify[1] == 1261, f"FAIL: expected (1261, 1261), got {verify}"

    # ------------------------------------------------------------------
    # Step 1.2: Validate research_ids against canonical spine
    # ------------------------------------------------------------------
    print("\n[221] Step 1.2: Validating research_ids against canonical spine...")
    if not dry_run:
        orphans = check_orphans(con, STAGING_NSQIP, "NSQIP enrichment")
        if orphans > 0:
            print(f"[221] Dropping {orphans} orphan rows from {STAGING_NSQIP}")
            con.execute(f"""
                CREATE OR REPLACE TABLE {STAGING_NSQIP} AS
                SELECT * FROM {STAGING_NSQIP}
                WHERE research_id IN (SELECT research_id FROM {CANONICAL})
            """)
            n_after = con.execute(f"SELECT COUNT(*) FROM {STAGING_NSQIP}").fetchone()[0]
            print(f"[221] After orphan removal: {n_after} rows")

    # ------------------------------------------------------------------
    # Step 1.3: Cross-validate overlapping columns
    # ------------------------------------------------------------------
    print("\n[221] Step 1.3: Cross-validating overlapping columns...")
    if not dry_run:
        overlap_checks = [
            ("nsqip_asa_class", "CAST(c.nsqip_asa_class AS VARCHAR) != CAST(n.nsqip_asa_class AS VARCHAR)"),
            ("nsqip_bmi", "ABS(TRY_CAST(c.nsqip_bmi AS DOUBLE) - TRY_CAST(n.nsqip_bmi AS DOUBLE)) > 0.5"),
            ("nsqip_diabetes", "CAST(c.nsqip_diabetes AS VARCHAR) != CAST(n.nsqip_diabetes AS VARCHAR)"),
            ("nsqip_hypertension", "CAST(c.nsqip_hypertension AS VARCHAR) != CAST(n.nsqip_hypertension AS VARCHAR)"),
            ("nsqip_functional_status", "CAST(c.nsqip_functional_status AS VARCHAR) != CAST(n.nsqip_functional_status AS VARCHAR)"),
        ]

        canonical_cols = get_existing_columns(con)
        print(f"[221] {'Field':<25} {'Both non-null':>14} {'Discordant':>12} {'Rate':>8}")
        print("[221] " + "-" * 62)

        for field, discord_expr in overlap_checks:
            if field not in canonical_cols:
                print(f"[221]   {field:<23} (not in canonical yet — skipped)")
                continue
            row = con.execute(f"""
                SELECT
                    COUNT(*) FILTER (WHERE c.{field} IS NOT NULL AND n.{field} IS NOT NULL) AS both_nn,
                    COUNT(*) FILTER (WHERE c.{field} IS NOT NULL AND n.{field} IS NOT NULL
                                     AND {discord_expr}) AS discordant
                FROM {CANONICAL} c
                JOIN {STAGING_NSQIP} n ON c.research_id = n.research_id
            """).fetchone()
            both_nn, discord = row
            rate = discord / both_nn * 100 if both_nn > 0 else 0
            flag = " ⚠" if rate > 5 else ""
            print(f"[221]   {field:<25} {both_nn:>14} {discord:>12} {rate:>7.1f}%{flag}")

    # ------------------------------------------------------------------
    # Step 1.4: Cross-validate staging vs canonical pathology staging
    # ------------------------------------------------------------------
    print("\n[221] Step 1.4: Cross-validating NSQIP staging vs canonical pathology staging...")
    if not dry_run:
        canonical_cols = get_existing_columns(con)
        staging_result = con.execute(f"""
            SELECT
                COUNT(*) FILTER (WHERE n.nsqip_t_classification IS NOT NULL
                                 AND c.t_stage_ajcc8 IS NOT NULL) AS both_have_t,
                COUNT(*) FILTER (WHERE n.nsqip_t_classification IS NOT NULL
                                 AND c.t_stage_ajcc8 IS NOT NULL
                                 AND UPPER(CAST(n.nsqip_t_classification AS VARCHAR))
                                     != UPPER(CAST(c.t_stage_ajcc8 AS VARCHAR))) AS t_discordant,
                COUNT(*) FILTER (WHERE n.nsqip_n_classification IS NOT NULL
                                 AND c.n_stage_ajcc8 IS NOT NULL) AS both_have_n,
                COUNT(*) FILTER (WHERE n.nsqip_n_classification IS NOT NULL
                                 AND c.n_stage_ajcc8 IS NOT NULL
                                 AND UPPER(CAST(n.nsqip_n_classification AS VARCHAR))
                                     != UPPER(CAST(c.n_stage_ajcc8 AS VARCHAR))) AS n_discordant,
                COUNT(*) FILTER (WHERE n.nsqip_nodes_removed IS NOT NULL
                                 AND c.ln_total_examined IS NOT NULL) AS both_have_ln,
                COUNT(*) FILTER (WHERE n.nsqip_nodes_removed IS NOT NULL
                                 AND c.ln_total_examined IS NOT NULL
                                 AND ABS(TRY_CAST(n.nsqip_nodes_removed AS INT)
                                       - TRY_CAST(c.ln_total_examined AS INT)) > 2) AS ln_discordant
            FROM {CANONICAL} c
            JOIN {STAGING_NSQIP} n ON c.research_id = n.research_id
        """).fetchone()
        t_both, t_disc, n_both, n_disc, ln_both, ln_disc = staging_result
        print(f"[221]   T-stage: {t_both} comparable, {t_disc} discordant "
              f"({t_disc/t_both*100:.1f}%)" if t_both else "[221]   T-stage: no overlap")
        print(f"[221]   N-stage: {n_both} comparable, {n_disc} discordant "
              f"({n_disc/n_both*100:.1f}%)" if n_both else "[221]   N-stage: no overlap")
        print(f"[221]   LN count: {ln_both} comparable, {ln_disc} discordant "
              f"(>2 node diff, {ln_disc/ln_both*100:.1f}%)" if ln_both else "[221]   LN count: no overlap")

    # ------------------------------------------------------------------
    # Step 1.5: Cross-validate hypocalcemia
    # ------------------------------------------------------------------
    print("\n[221] Step 1.5: Cross-validating NSQIP hypocalcemia vs canonical...")
    if not dry_run:
        canonical_cols = get_existing_columns(con)
        comp_hypo = "comp_hypoparathyroidism_confirmed" if "comp_hypoparathyroidism_confirmed" in canonical_cols else None
        comp_hypo2 = "comp_hypocalcemia_confirmed" if "comp_hypocalcemia_confirmed" in canonical_cols else None
        if comp_hypo or comp_hypo2:
            filter_parts = []
            if comp_hypo:
                filter_parts.append(f"c.{comp_hypo} IS NOT NULL")
            if comp_hypo2:
                filter_parts.append(f"c.{comp_hypo2} IS NOT NULL")
            canonical_has_hypo = " OR ".join(filter_parts)
            r = con.execute(f"""
                SELECT
                    COUNT(*) FILTER (WHERE TRY_CAST(n.nsqip_hypocalcemia_flag AS INT) = 1) AS nsqip_yes,
                    COUNT(*) FILTER (WHERE {canonical_has_hypo}) AS canonical_yes,
                    COUNT(*) FILTER (WHERE TRY_CAST(n.nsqip_hypocalcemia_flag AS INT) = 1
                                     AND ({canonical_has_hypo})) AS both_yes,
                    COUNT(*) FILTER (WHERE TRY_CAST(n.nsqip_hypocalcemia_flag AS INT) = 1
                                     AND NOT ({canonical_has_hypo})) AS nsqip_only,
                    COUNT(*) FILTER (WHERE (n.nsqip_hypocalcemia_flag IS NULL
                                            OR TRY_CAST(n.nsqip_hypocalcemia_flag AS INT) = 0)
                                     AND ({canonical_has_hypo})) AS canonical_only
                FROM {CANONICAL} c
                JOIN {STAGING_NSQIP} n ON c.research_id = n.research_id
            """).fetchone()
            print(f"[221]   NSQIP(30d) hypo: {r[0]}, Canonical(any-time): {r[1]}, "
                  f"Both: {r[2]}, NSQIP-only: {r[3]}, Canonical-only: {r[4]}")
        else:
            print("[221]   Hypocalcemia columns not found in canonical — skipping")

    # ------------------------------------------------------------------
    # Step 1.6: Cross-validate RLN injury
    # ------------------------------------------------------------------
    print("\n[221] Step 1.6: Cross-validating NSQIP RLN injury vs canonical...")
    if not dry_run:
        canonical_cols = get_existing_columns(con)
        if "comp_rln_injury_confirmed" in canonical_cols:
            r = con.execute(f"""
                SELECT
                    COUNT(*) FILTER (WHERE TRY_CAST(n.nsqip_rln_injury_flag AS INT) = 1) AS nsqip_yes,
                    COUNT(*) FILTER (WHERE c.comp_rln_injury_confirmed IS NOT NULL) AS canonical_yes,
                    COUNT(*) FILTER (WHERE TRY_CAST(n.nsqip_rln_injury_flag AS INT) = 1
                                     AND c.comp_rln_injury_confirmed IS NOT NULL) AS both_yes,
                    COUNT(*) FILTER (WHERE TRY_CAST(n.nsqip_rln_injury_flag AS INT) = 1
                                     AND c.comp_rln_injury_confirmed IS NULL) AS nsqip_only,
                    COUNT(*) FILTER (WHERE (n.nsqip_rln_injury_flag IS NULL
                                            OR TRY_CAST(n.nsqip_rln_injury_flag AS INT) = 0)
                                     AND c.comp_rln_injury_confirmed IS NOT NULL) AS canonical_only
                FROM {CANONICAL} c
                JOIN {STAGING_NSQIP} n ON c.research_id = n.research_id
            """).fetchone()
            print(f"[221]   NSQIP(30d) RLN: {r[0]}, Canonical: {r[1]}, "
                  f"Both: {r[2]}, NSQIP-only: {r[3]}, Canonical-only: {r[4]}")
        else:
            print("[221]   comp_rln_injury_confirmed not in canonical — skipping")

    # ------------------------------------------------------------------
    # Step 1.7: Cross-validate CND/LND/drain/RLN monitoring
    # ------------------------------------------------------------------
    print("\n[221] Step 1.7: Cross-validating NSQIP surgical indicators vs canonical...")
    if not dry_run:
        canonical_cols = get_existing_columns(con)
        checks = []
        if "ln_central_examined" in canonical_cols:
            checks.append(("CND", "nsqip_central_neck_dissection", "'Yes'",
                           "TRY_CAST(c.ln_central_examined AS INT) > 0",
                           "c.ln_central_examined"))
        if "ln_lateral_examined" in canonical_cols:
            checks.append(("LND", "nsqip_lateral_neck_dissection", "'Yes'",
                           "TRY_CAST(c.ln_lateral_examined AS INT) > 0",
                           "c.ln_lateral_examined"))
        for check_name, nsqip_col, nsqip_val, canonical_cond, _ in checks:
            r = con.execute(f"""
                SELECT
                    COUNT(*) FILTER (WHERE n.{nsqip_col} = {nsqip_val}) AS nsqip_yes,
                    COUNT(*) FILTER (WHERE {canonical_cond}) AS canonical_yes
                FROM {CANONICAL} c
                JOIN {STAGING_NSQIP} n ON c.research_id = n.research_id
            """).fetchone()
            print(f"[221]   {check_name}: NSQIP={r[0]}, Canonical={r[1]}")

    # ------------------------------------------------------------------
    # Step 1.8: Determine new columns and integrate via ALTER TABLE + UPDATE
    # ------------------------------------------------------------------
    print("\n[221] Step 1.8: Integrating NSQIP columns into canonical...")
    if not dry_run:
        existing_cols = get_existing_columns(con)
        # Get NSQIP staging columns with dtypes from pandas
        staging_col_list = [r[0] for r in con.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = '{STAGING_NSQIP}' AND table_schema = 'main'
            ORDER BY ordinal_position
        """).fetchall()]

        # New columns = in staging but not in canonical, not PHI, not research_id
        exclude = PHI_COLS | {"research_id"}
        new_cols = [c for c in staging_col_list if c not in existing_cols and c not in exclude]
        print(f"[221] New NSQIP columns to add: {len(new_cols)}")
        for c in new_cols:
            print(f"[221]   + {c}")

        # Build dtype map from pandas df
        col_dtypes = {c: _pd_dtype_to_duckdb(str(df[c].dtype)) for c in new_cols if c in df.columns}

        # Sentinel columns (not in staging table — computed)
        sentinels: list[tuple[str, str, str]] = []
        if "nsqip_thyroidectomy_has_data" not in existing_cols:
            sentinels.append(("nsqip_thyroidectomy_has_data", "BOOLEAN", "TRUE"))
        if "nsqip_thyroidectomy_source_script" not in existing_cols:
            sentinels.append(("nsqip_thyroidectomy_source_script", "VARCHAR",
                               f"'{SCRIPT_TAG}'"))

        integrate_staging_into_canonical(
            con, STAGING_NSQIP, new_cols, col_dtypes, sentinels, "NSQIP"
        )
    else:
        df_cols = set(df.columns) - PHI_COLS - {"research_id"}
        print(f"[221] DRY-RUN — would add ~{len(df_cols)} NSQIP columns")

    # ------------------------------------------------------------------
    # Step 1.9: Verify invariants
    # ------------------------------------------------------------------
    print("\n[221] Step 1.9: Verifying canonical invariants...")
    if not dry_run:
        check_invariants_light(con, "post-NSQIP rebuild")

    # ------------------------------------------------------------------
    # Step 1.10: Coverage report
    # ------------------------------------------------------------------
    print("\n[221] Step 1.10: NSQIP coverage report...")
    if not dry_run:
        cols_to_report = sorted([c for c in get_existing_columns(con)
                                 if c.startswith("nsqip_") and c not in PHI_COLS])
        # Key columns first
        key_cols = [
            "nsqip_thyroidectomy_has_data", "nsqip_primary_indication",
            "nsqip_central_neck_dissection", "nsqip_lateral_neck_dissection",
            "nsqip_hypocalcemia_flag", "nsqip_rln_injury_flag",
            "nsqip_t_classification", "nsqip_n_classification",
            "nsqip_asa_class", "nsqip_operative_duration_min",
            "nsqip_hospital_los_days", "nsqip_albumin", "nsqip_creatinine",
        ]
        print("\n[221] NSQIP Coverage (10,871 canonical patients):")
        print(f"[221]   {'Column':<50} {'Non-null':>8}  {'Pct':>6}")
        print("[221]   " + "-" * 68)
        for col in key_cols:
            if col not in get_existing_columns(con):
                continue
            try:
                n = con.execute(f"""
                    SELECT COUNT(*) FILTER (WHERE "{col}" IS NOT NULL)
                    FROM {CANONICAL}
                """).fetchone()[0]
                pct = n / TOTAL_ROWS * 100
                print(f"[221]   {col:<50} {n:>8}  {pct:>5.1f}%")
            except Exception as e:
                print(f"[221]   {col:<50} ERROR: {e}")

        total_nsqip_cols = len(cols_to_report)
        print(f"\n[221] Total nsqip_* columns in canonical: {total_nsqip_cols}")


# ============================================================
# TASK 2: PARATHYROID NOTES INTENT INTEGRATION
# ============================================================

STAGING_PARA = "_parathyroid_patient_rollup_v1"
PARA_SOURCE = "parathyroid_notes_intent_v1"  # fallback; primary is 'parathyroid'


PARATHYROID_ROLLUP_SQL = """
CREATE OR REPLACE TABLE {staging} AS
WITH source AS (
    SELECT
        CAST("{rid_col}" AS VARCHAR) AS research_id,
        "{incl_col}" AS parathyroid_included,
        "{incid_col}" AS incidental_gland_excision,
        "{pathg_col}" AS pathologic_glands,
        "{para_abn_col}" AS parathyroid_abnormality,
        "{intent_col}" AS removal_intent,
        "{intent_ev_col}" AS removal_intent_evidence,
        "{inc_ref_col}" AS incidental_status_refined,
        "{note_int_col}" AS note_intent_inferred,
        g1_location, g1_biopsy, g1_excision, g1_cellularity, g1_weight, g1_size,
        g2_location, g2_biopsy, g2_excision, g2_cellularity, g2_weight, g2_size,
        g3_location, g3_biopsy, g3_excision, g3_cellularity, g3_weight, g3_size,
        g4_location, g4_biopsy, g4_excision, g4_cellularity, g4_weight, g4_size,
        g5_location, g5_biopsy, g5_excision, g5_cellularity, g5_weight, g5_size,
        g6_location, g6_biopsy, g6_excision, g6_cellularity, g6_weight, g6_size
    FROM {source_tbl}
    WHERE "{rid_col}" IS NOT NULL
      AND CAST("{rid_col}" AS VARCHAR) != ''
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY
                (CASE WHEN g1_location IS NOT NULL AND CAST(g1_location AS VARCHAR) != '' THEN 1 ELSE 0 END
               + CASE WHEN g2_location IS NOT NULL AND CAST(g2_location AS VARCHAR) != '' THEN 1 ELSE 0 END
               + CASE WHEN g3_location IS NOT NULL AND CAST(g3_location AS VARCHAR) != '' THEN 1 ELSE 0 END
               + CASE WHEN g4_location IS NOT NULL AND CAST(g4_location AS VARCHAR) != '' THEN 1 ELSE 0 END
               + CASE WHEN g5_location IS NOT NULL AND CAST(g5_location AS VARCHAR) != '' THEN 1 ELSE 0 END
               + CASE WHEN g6_location IS NOT NULL AND CAST(g6_location AS VARCHAR) != '' THEN 1 ELSE 0 END
                ) DESC
        ) AS rn
    FROM source
)
SELECT
    research_id,

    -- Parathyroid included in specimen
    CASE WHEN LOWER(CAST(parathyroid_included AS VARCHAR)) IN ('yes', 'x') THEN TRUE
         WHEN LOWER(CAST(parathyroid_included AS VARCHAR)) IN ('no') THEN FALSE
         ELSE NULL END AS para_specimen_included,

    -- Removal intent (prefer note_intent_inferred for best coverage)
    CASE WHEN note_intent_inferred IS NOT NULL
              AND LOWER(CAST(note_intent_inferred AS VARCHAR)) NOT IN ('', 'nan', 'none', 'null')
         THEN LOWER(CAST(note_intent_inferred AS VARCHAR))
         WHEN removal_intent IS NOT NULL
              AND LOWER(CAST(removal_intent AS VARCHAR)) NOT IN ('', 'nan', 'none', 'null')
         THEN LOWER(CAST(removal_intent AS VARCHAR))
         ELSE NULL END AS para_removal_intent,

    -- Incidental status refined
    NULLIF(CAST(incidental_status_refined AS VARCHAR), '') AS para_incidental_status_refined,

    -- Pathologic glands flag
    CASE WHEN pathologic_glands IS NOT NULL
              AND CAST(pathologic_glands AS VARCHAR) NOT IN ('', 'nan', 'none', 'null', 'no', 'NO')
         THEN TRUE ELSE FALSE END AS para_has_pathologic_glands,

    -- Abnormality type (e.g. adenoma)
    NULLIF(CAST(parathyroid_abnormality AS VARCHAR), '') AS para_abnormality_type,

    -- Gland count: number of glands with a non-empty location field
    (CASE WHEN g1_location IS NOT NULL AND CAST(g1_location AS VARCHAR) NOT IN ('', 'nan', 'none') THEN 1 ELSE 0 END
   + CASE WHEN g2_location IS NOT NULL AND CAST(g2_location AS VARCHAR) NOT IN ('', 'nan', 'none') THEN 1 ELSE 0 END
   + CASE WHEN g3_location IS NOT NULL AND CAST(g3_location AS VARCHAR) NOT IN ('', 'nan', 'none') THEN 1 ELSE 0 END
   + CASE WHEN g4_location IS NOT NULL AND CAST(g4_location AS VARCHAR) NOT IN ('', 'nan', 'none') THEN 1 ELSE 0 END
   + CASE WHEN g5_location IS NOT NULL AND CAST(g5_location AS VARCHAR) NOT IN ('', 'nan', 'none') THEN 1 ELSE 0 END
   + CASE WHEN g6_location IS NOT NULL AND CAST(g6_location AS VARCHAR) NOT IN ('', 'nan', 'none') THEN 1 ELSE 0 END
    ) AS para_n_glands_identified,

    -- Biopsy count
    (CASE WHEN g1_biopsy IS NOT NULL AND LOWER(CAST(g1_biopsy AS VARCHAR)) NOT IN ('', 'no', 'nan', 'none') THEN 1 ELSE 0 END
   + CASE WHEN g2_biopsy IS NOT NULL AND LOWER(CAST(g2_biopsy AS VARCHAR)) NOT IN ('', 'no', 'nan', 'none') THEN 1 ELSE 0 END
   + CASE WHEN g3_biopsy IS NOT NULL AND LOWER(CAST(g3_biopsy AS VARCHAR)) NOT IN ('', 'no', 'nan', 'none') THEN 1 ELSE 0 END
   + CASE WHEN g4_biopsy IS NOT NULL AND LOWER(CAST(g4_biopsy AS VARCHAR)) NOT IN ('', 'no', 'nan', 'none') THEN 1 ELSE 0 END
   + CASE WHEN g5_biopsy IS NOT NULL AND LOWER(CAST(g5_biopsy AS VARCHAR)) NOT IN ('', 'no', 'nan', 'none') THEN 1 ELSE 0 END
   + CASE WHEN g6_biopsy IS NOT NULL AND LOWER(CAST(g6_biopsy AS VARCHAR)) NOT IN ('', 'no', 'nan', 'none') THEN 1 ELSE 0 END
    ) AS para_n_glands_biopsied,

    -- Excision count
    (CASE WHEN g1_excision IS NOT NULL AND LOWER(CAST(g1_excision AS VARCHAR)) NOT IN ('', 'no', 'nan', 'none') THEN 1 ELSE 0 END
   + CASE WHEN g2_excision IS NOT NULL AND LOWER(CAST(g2_excision AS VARCHAR)) NOT IN ('', 'no', 'nan', 'none') THEN 1 ELSE 0 END
   + CASE WHEN g3_excision IS NOT NULL AND LOWER(CAST(g3_excision AS VARCHAR)) NOT IN ('', 'no', 'nan', 'none') THEN 1 ELSE 0 END
   + CASE WHEN g4_excision IS NOT NULL AND LOWER(CAST(g4_excision AS VARCHAR)) NOT IN ('', 'no', 'nan', 'none') THEN 1 ELSE 0 END
   + CASE WHEN g5_excision IS NOT NULL AND LOWER(CAST(g5_excision AS VARCHAR)) NOT IN ('', 'no', 'nan', 'none') THEN 1 ELSE 0 END
   + CASE WHEN g6_excision IS NOT NULL AND LOWER(CAST(g6_excision AS VARCHAR)) NOT IN ('', 'no', 'nan', 'none') THEN 1 ELSE 0 END
    ) AS para_n_glands_excised,

    -- Cellularity stats (%)
    GREATEST(
        TRY_CAST(NULLIF(CAST(g1_cellularity AS VARCHAR), '') AS DOUBLE),
        TRY_CAST(NULLIF(CAST(g2_cellularity AS VARCHAR), '') AS DOUBLE),
        TRY_CAST(NULLIF(CAST(g3_cellularity AS VARCHAR), '') AS DOUBLE),
        TRY_CAST(NULLIF(CAST(g4_cellularity AS VARCHAR), '') AS DOUBLE),
        TRY_CAST(NULLIF(CAST(g5_cellularity AS VARCHAR), '') AS DOUBLE),
        TRY_CAST(NULLIF(CAST(g6_cellularity AS VARCHAR), '') AS DOUBLE)
    ) AS para_max_cellularity_pct,

    LEAST(
        TRY_CAST(NULLIF(CAST(g1_cellularity AS VARCHAR), '') AS DOUBLE),
        TRY_CAST(NULLIF(CAST(g2_cellularity AS VARCHAR), '') AS DOUBLE),
        TRY_CAST(NULLIF(CAST(g3_cellularity AS VARCHAR), '') AS DOUBLE),
        TRY_CAST(NULLIF(CAST(g4_cellularity AS VARCHAR), '') AS DOUBLE),
        TRY_CAST(NULLIF(CAST(g5_cellularity AS VARCHAR), '') AS DOUBLE),
        TRY_CAST(NULLIF(CAST(g6_cellularity AS VARCHAR), '') AS DOUBLE)
    ) AS para_min_cellularity_pct,

    -- Max gland weight (g)
    GREATEST(
        TRY_CAST(NULLIF(CAST(g1_weight AS VARCHAR), '') AS DOUBLE),
        TRY_CAST(NULLIF(CAST(g2_weight AS VARCHAR), '') AS DOUBLE),
        TRY_CAST(NULLIF(CAST(g3_weight AS VARCHAR), '') AS DOUBLE),
        TRY_CAST(NULLIF(CAST(g4_weight AS VARCHAR), '') AS DOUBLE),
        TRY_CAST(NULLIF(CAST(g5_weight AS VARCHAR), '') AS DOUBLE),
        TRY_CAST(NULLIF(CAST(g6_weight AS VARCHAR), '') AS DOUBLE)
    ) AS para_max_gland_weight_g,

    -- Source provenance
    'parathyroid_notes_intent.xlsx' AS para_source_workbook,
    '{script_tag}' AS para_source_script

FROM deduped
WHERE rn = 1
"""


def _get_para_source(con: duckdb.DuckDBPyConnection) -> str | None:
    """Return the parathyroid source table name if it exists."""
    for tbl in ("parathyroid", "parathyroid_notes_intent_v1"):
        if table_exists(con, tbl):
            return tbl
    return None


def _get_para_col(cols: list[str], candidates: list[str], label: str) -> str:
    """Return first matching column from candidates list."""
    col_set = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in col_set:
            return col_set[cand.lower()]
    raise ValueError(f"Cannot find column for {label} in: {cols[:20]}")


def phase2_parathyroid(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    print("\n" + "=" * 70)
    print("[221] TASK 2: PARATHYROID NOTES INTENT INTEGRATION")
    print("=" * 70)

    # ------------------------------------------------------------------
    # Step 2.1: Verify / ingest source table
    # ------------------------------------------------------------------
    print("\n[221] Step 2.1: Checking parathyroid source table...")

    if not dry_run:
        source_tbl = _get_para_source(con)
    else:
        source_tbl = "parathyroid"  # assume for dry-run

    if source_tbl is None:
        print("[221] No parathyroid table found — ingesting from Excel...")
        if dry_run:
            print("[221] DRY-RUN — would ingest parathyroid_notes_intent.xlsx")
            source_tbl = "parathyroid_notes_intent_v1"
        else:
            excel_path = REPO / "raw" / "parathyroid_notes_intent.xlsx"
            if not excel_path.exists():
                print(f"[221] ERROR: Cannot find {excel_path}")
                sys.exit(1)
            pt_df = pd.read_excel(str(excel_path), sheet_name=0)
            # Normalize column names (keep originals for now, just strip whitespace)
            pt_df.columns = [str(c).strip() for c in pt_df.columns]
            # Rename research_id column
            rid_col_raw = next(c for c in pt_df.columns if "research" in c.lower() and "id" in c.lower())
            pt_df = pt_df.rename(columns={rid_col_raw: "research_id"})
            pt_df["research_id"] = pt_df["research_id"].astype(str)
            tmp = REPO / "scripts" / "output" / "_para_ingest_221.parquet"
            tmp.parent.mkdir(parents=True, exist_ok=True)
            pt_df.to_parquet(str(tmp), index=False)
            con.execute(f"CREATE OR REPLACE TABLE parathyroid_notes_intent_v1 AS SELECT * FROM read_parquet('{tmp}')")
            try:
                tmp.unlink()
            except Exception:
                pass
            source_tbl = "parathyroid_notes_intent_v1"
            n = con.execute(f"SELECT COUNT(*) FROM {source_tbl}").fetchone()[0]
            print(f"[221] ✓ Ingested {source_tbl}: {n} rows")
    else:
        if not dry_run:
            n = con.execute(f"SELECT COUNT(*) FROM {source_tbl}").fetchone()[0]
            print(f"[221] Found {source_tbl}: {n} rows")

    # ------------------------------------------------------------------
    # Inspect actual column names in the source table
    # ------------------------------------------------------------------
    if not dry_run:
        actual_cols = [r[0] for r in con.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = '{source_tbl}' AND table_schema = 'main'
            ORDER BY ordinal_position
        """).fetchall()]
        print(f"[221] Source table columns ({len(actual_cols)}): {actual_cols[:10]}...")

        # Map column names to their actual names
        rid_col = _get_para_col(actual_cols,
            ["research_id", "Research ID number", "research_id_number"], "research_id")
        incl_col = _get_para_col(actual_cols,
            ["parathyroid_included",
             "Parathyroid Gland &/oR tissue included in resected specimen?",
             "Parathyroid Gland  &/oR tissue included in resected specimen?",
             "parathyroid_gland_included"], "parathyroid_included")
        incid_col = _get_para_col(actual_cols,
            ["incidental_gland_excision", "incidental gland excision"], "incidental_gland_excision")
        pathg_col = _get_para_col(actual_cols,
            ["pathologic_glands", "pathologic glands"], "pathologic_glands")
        para_abn_col = _get_para_col(actual_cols,
            ["parathyroid_abnormality", "parathyroid abnormality"], "parathyroid_abnormality")
        intent_col = _get_para_col(actual_cols,
            ["removal_intent", "removal intent"], "removal_intent")
        intent_ev_col = _get_para_col(actual_cols,
            ["removal_intent_evidence", "removal intent evidence"], "removal_intent_evidence")
        inc_ref_col = _get_para_col(actual_cols,
            ["incidental_status_refined", "incidental status refined"], "incidental_status_refined")
        note_int_col = _get_para_col(actual_cols,
            ["note_intent_inferred", "note intent inferred"], "note_intent_inferred")

        print(f"[221] Column mapping confirmed: research_id='{rid_col}', "
              f"included='{incl_col}', intent='{intent_col}'")

    # ------------------------------------------------------------------
    # Step 2.2: Build patient-level rollup
    # ------------------------------------------------------------------
    print("\n[221] Step 2.2: Building patient-level parathyroid rollup...")
    if not dry_run:
        rollup_sql = PARATHYROID_ROLLUP_SQL.format(
            staging=STAGING_PARA,
            source_tbl=source_tbl,
            rid_col=rid_col,
            incl_col=incl_col,
            incid_col=incid_col,
            pathg_col=pathg_col,
            para_abn_col=para_abn_col,
            intent_col=intent_col,
            intent_ev_col=intent_ev_col,
            inc_ref_col=inc_ref_col,
            note_int_col=note_int_col,
            script_tag=SCRIPT_TAG,
        )
        run_sql(con, rollup_sql, "Parathyroid patient rollup")
        n_rollup = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {STAGING_PARA}").fetchone()
        print(f"[221] Rollup: {n_rollup[0]} rows, {n_rollup[1]} unique research_ids")
        assert n_rollup[0] == n_rollup[1], f"FAIL: duplicates in rollup ({n_rollup[0]} rows, {n_rollup[1]} rids)"

    # ------------------------------------------------------------------
    # Step 2.3: Validate rollup
    # ------------------------------------------------------------------
    print("\n[221] Step 2.3: Validating parathyroid rollup...")
    if not dry_run:
        orphans = check_orphans(con, STAGING_PARA, "parathyroid rollup")
        if orphans > 0:
            print(f"[221] Dropping {orphans} orphan rows from {STAGING_PARA}")
            con.execute(f"""
                CREATE OR REPLACE TABLE {STAGING_PARA} AS
                SELECT * FROM {STAGING_PARA}
                WHERE research_id IN (SELECT research_id FROM {CANONICAL})
            """)

        coverage = con.execute(f"""
            SELECT
                COUNT(*) AS total_patients,
                COUNT(*) FILTER (WHERE para_specimen_included IS NOT NULL) AS has_specimen_flag,
                COUNT(*) FILTER (WHERE para_removal_intent IS NOT NULL
                                 AND para_removal_intent NOT IN ('unsure', 'nan', '')) AS has_clear_intent,
                COUNT(*) FILTER (WHERE para_removal_intent = 'intentional') AS intentional,
                COUNT(*) FILTER (WHERE para_removal_intent = 'incidental') AS incidental,
                COUNT(*) FILTER (WHERE para_removal_intent = 'mixed') AS mixed,
                COUNT(*) FILTER (WHERE para_has_pathologic_glands = TRUE) AS has_pathologic,
                COUNT(*) FILTER (WHERE para_n_glands_identified > 0) AS has_gland_data,
                COUNT(*) FILTER (WHERE para_max_cellularity_pct IS NOT NULL) AS has_cellularity,
                COUNT(*) FILTER (WHERE para_max_gland_weight_g IS NOT NULL) AS has_weight
            FROM {STAGING_PARA}
        """).fetchone()
        print(f"[221] Coverage: {coverage[0]} patients, "
              f"specimen_flag={coverage[1]}, clear_intent={coverage[2]}, "
              f"intentional={coverage[3]}, incidental={coverage[4]}, mixed={coverage[5]}, "
              f"pathologic={coverage[6]}, gland_data={coverage[7]}, "
              f"cellularity={coverage[8]}, weight={coverage[9]}")

    # ------------------------------------------------------------------
    # Step 2.4: Cross-validate vs existing canonical para fields
    # ------------------------------------------------------------------
    print("\n[221] Step 2.4: Cross-validating rollup vs canonical parathyroid fields...")
    if not dry_run:
        canonical_cols = get_existing_columns(con)
        para_fields = [c for c in canonical_cols if "parathyroid" in c.lower() or c.startswith("op_para")]
        if para_fields:
            for field in para_fields[:5]:  # check up to 5
                try:
                    r = con.execute(f"""
                        SELECT p.para_removal_intent,
                               COUNT(*) as n,
                               COUNT(*) FILTER (WHERE c."{field}" IS NOT NULL) as has_field
                        FROM {STAGING_PARA} p
                        JOIN {CANONICAL} c ON p.research_id = c.research_id
                        GROUP BY p.para_removal_intent
                        ORDER BY n DESC LIMIT 6
                    """).fetchall()
                    print(f"[221]   {field} by intent: "
                          + ", ".join(f"{row[0]}({row[2]}/{row[1]})" for row in r))
                except Exception as e:
                    print(f"[221]   {field}: {e}")
        else:
            print("[221]   No existing parathyroid fields in canonical to cross-validate")

    # ------------------------------------------------------------------
    # Step 2.5: Integrate into canonical via ALTER TABLE + UPDATE
    # ------------------------------------------------------------------
    print("\n[221] Step 2.5: Integrating parathyroid columns into canonical...")
    if not dry_run:
        existing_cols = get_existing_columns(con)
        para_staging_cols = [r[0] for r in con.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = '{STAGING_PARA}' AND table_schema = 'main'
            ORDER BY ordinal_position
        """).fetchall()]
        new_para_cols = [c for c in para_staging_cols if c not in existing_cols and c != "research_id"]
        print(f"[221] New parathyroid columns to add: {len(new_para_cols)}")
        for c in new_para_cols:
            print(f"[221]   + {c}")

        # Infer dtypes from rollup column names (all outputs are VARCHAR/BIGINT/DOUBLE/BOOLEAN)
        para_dtype_hints = {
            "para_specimen_included": "BOOLEAN",
            "para_removal_intent": "VARCHAR",
            "para_incidental_status_refined": "VARCHAR",
            "para_has_pathologic_glands": "BOOLEAN",
            "para_abnormality_type": "VARCHAR",
            "para_n_glands_identified": "BIGINT",
            "para_n_glands_biopsied": "BIGINT",
            "para_n_glands_excised": "BIGINT",
            "para_max_cellularity_pct": "DOUBLE",
            "para_min_cellularity_pct": "DOUBLE",
            "para_max_gland_weight_g": "DOUBLE",
            "para_source_workbook": "VARCHAR",
            "para_source_script": "VARCHAR",
        }
        col_dtypes = {c: para_dtype_hints.get(c, "VARCHAR") for c in new_para_cols}

        integrate_staging_into_canonical(
            con, STAGING_PARA, new_para_cols, col_dtypes, [], "Parathyroid"
        )
    else:
        print("[221] DRY-RUN — would add ~15 para_* columns")

    # ------------------------------------------------------------------
    # Step 2.6: Verify invariants
    # ------------------------------------------------------------------
    print("\n[221] Step 2.6: Verifying canonical invariants post-parathyroid...")
    if not dry_run:
        check_invariants_light(con, "post-parathyroid rebuild")


# ============================================================
# TASK 3: FINAL COLUMN COUNT + COVERAGE REPORT
# ============================================================

def phase3_report(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    print("\n" + "=" * 70)
    print("[221] TASK 3: FINAL COLUMN COUNT + COVERAGE REPORT")
    print("=" * 70)

    if dry_run:
        print("[221] DRY-RUN — would print final column count and coverage report")
        return

    all_cols = [r[0] for r in con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = '{CANONICAL}' AND table_schema = 'main'
        ORDER BY ordinal_position
    """).fetchall()]
    total_cols = len(all_cols)

    print(f"\n[221] {'=' * 70}")
    print(f"[221] FINAL {CANONICAL}: {TOTAL_ROWS:,} patients × {total_cols} columns")
    print(f"[221] {'=' * 70}")

    domain_prefixes = [
        ("nsqip_", "NSQIP (30-day outcomes)"),
        ("para_", "Parathyroid Notes Intent"),
        ("ct_", "CT Imaging"),
        ("mri_", "MRI Imaging"),
        ("pet_", "PET/CT"),
        ("nucmed_", "Nuclear Med"),
        ("lnus_", "LN Ultrasound"),
        ("cnln_", "Clinical Note LN"),
        ("lab_", "Labs"),
        ("nlp_", "NLP Entities"),
        ("op_nlp_", "Operative NLP"),
        ("comp_", "Complications"),
        ("ops_", "OP Sheet"),
        ("tg_", "Thyroglobulin"),
        ("rai_", "RAI Treatment"),
        ("ln_", "Lymph Node Pathology"),
        ("ete_", "ETE"),
    ]

    accounted: set[str] = set()
    print("\n[221] Domain breakdown:")
    print(f"[221]   {'Domain':<30} {'Cols':>6}  {'Sample'}")
    print("[221]   " + "-" * 70)
    for prefix, label in domain_prefixes:
        cols = [c for c in all_cols if c.startswith(prefix)]
        accounted.update(cols)
        sample = cols[0] if cols else ""
        print(f"[221]   {label:<30} {len(cols):>6}  {sample}")

    remaining = [c for c in all_cols if c not in accounted]
    print(f"[221]   {'Other / Core':<30} {len(remaining):>6}")
    print(f"[221]   {'TOTAL':<30} {total_cols:>6}")

    # ETE paper key variables
    print("\n[221] ETE Paper Key Variable Counts:")
    ete_checks = [
        ("research_id", "IS NOT NULL"),
        ("fna_path_outcome", "= 'malignant'"),
        ("ete_grade", "IS NOT NULL"),
        ("t_stage_ajcc8", "IS NOT NULL"),
        ("n_stage_ajcc8", "IS NOT NULL"),
        ("ln_total_examined", "IS NOT NULL"),
        ("followup_years", "> 0"),
        ("recurrence_confirmed", "= TRUE"),
        ("age_at_surgery", "IS NOT NULL"),
        ("diagnosis_primary", "IS NOT NULL"),
        ("nsqip_thyroidectomy_has_data", "= TRUE"),
        ("para_removal_intent", "IS NOT NULL"),
        ("nsqip_hypocalcemia_flag", "= 1"),
        ("nsqip_rln_injury_flag", "= 1"),
        ("nsqip_central_neck_dissection", "= 'Yes'"),
    ]
    print(f"[221]   {'Variable':<45} {'Count':>7} {'Pct':>7}")
    print("[221]   " + "-" * 62)
    existing_set = set(all_cols)
    for var, cond in ete_checks:
        if var not in existing_set:
            print(f"[221]   {var:<45} {'MISSING':>7}")
            continue
        try:
            n = con.execute(f'SELECT COUNT(*) FROM {CANONICAL} WHERE "{var}" {cond}').fetchone()[0]
            pct = n / TOTAL_ROWS * 100
            print(f"[221]   {var:<45} {n:>7} {pct:>6.1f}%")
        except Exception as e:
            print(f"[221]   {var:<45} ERROR: {str(e)[:40]}")

    # Data dictionary refresh
    print("\n[221] Exporting data dictionary CSV...")
    col_info = con.execute(f"""
        SELECT
            column_name,
            data_type,
            CASE
                WHEN column_name LIKE 'nsqip_%' THEN 'NSQIP'
                WHEN column_name LIKE 'para_%'  THEN 'Parathyroid'
                WHEN column_name LIKE 'ct_%'    THEN 'CT Imaging'
                WHEN column_name LIKE 'mri_%'   THEN 'MRI'
                WHEN column_name LIKE 'pet_%'   THEN 'PET/CT'
                WHEN column_name LIKE 'nucmed_%' THEN 'Nuclear Med'
                WHEN column_name LIKE 'cnln_%'  THEN 'Clinical Note LN'
                WHEN column_name LIKE 'lnus_%'  THEN 'LN Ultrasound'
                WHEN column_name LIKE 'lab_%'   THEN 'Labs'
                WHEN column_name LIKE 'nlp_%'   THEN 'NLP Entities'
                WHEN column_name LIKE 'op_nlp_%' THEN 'Operative NLP'
                WHEN column_name LIKE 'comp_%'  THEN 'Complications'
                WHEN column_name LIKE 'ops_%'   THEN 'OP Sheet'
                WHEN column_name LIKE 'tg_%'    THEN 'Thyroglobulin'
                WHEN column_name LIKE 'rai_%'   THEN 'RAI Treatment'
                WHEN column_name LIKE 'ln_%'    THEN 'LN Pathology'
                WHEN column_name LIKE 'ete_%'   THEN 'ETE'
                ELSE 'Core/Other'
            END AS domain,
            ordinal_position
        FROM information_schema.columns
        WHERE table_name = '{CANONICAL}' AND table_schema = 'main'
        ORDER BY ordinal_position
    """).df()

    dd_path = REPO / "scripts" / "output" / "data_dictionary.csv"
    dd_path.parent.mkdir(parents=True, exist_ok=True)
    col_info.to_csv(str(dd_path), index=False)
    # Also write to repo root (matching Script 213 pattern)
    root_dd = REPO / "data_dictionary.csv"
    col_info.to_csv(str(root_dd), index=False)
    print(f"[221] ✓ Data dictionary exported: {len(col_info)} columns → {dd_path}")
    print(f"[221] ✓ Also written to: {root_dd}")


# ============================================================
# Main
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Script 221: Final Gap Resolution")
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate and report without writing to canonical")
    parser.add_argument("--phase", choices=["1", "2", "3", "all"], default="all",
                        help="Which phase to run (default: all)")
    parser.add_argument("--full-check", action="store_true",
                        help="Use full table scan for invariants (slow on 1000+ col tables)")
    args = parser.parse_args()

    inv_fn = check_invariants if args.full_check else check_invariants_light

    print(f"\n[221] {'DRY-RUN MODE — ' if args.dry_run else ''}Script 221 starting — phase={args.phase}")
    if not args.full_check:
        print("[221] Using schema-only invariant checks (fast). Pass --full-check for row-level scans.")

    con = connect()

    if not args.dry_run:
        inv_fn(con, "pre-script baseline")

    run_phases = {"1", "2", "3"} if args.phase == "all" else {args.phase}

    if "1" in run_phases:
        phase1_nsqip(con, args.dry_run)

    if "2" in run_phases:
        phase2_parathyroid(con, args.dry_run)

    if "3" in run_phases:
        phase3_report(con, args.dry_run)

    if not args.dry_run:
        inv_fn(con, "post-script final")

    print("\n[221] ✓ Script 221 complete")


if __name__ == "__main__":
    main()
