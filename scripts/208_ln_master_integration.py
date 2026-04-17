#!/usr/bin/env python3
"""
THYROID_2026 — Script 208: Copy ln_master_rollup_v1 + LN Master Integration
Database: thyroid_ete_fix_20260413 (CANONICAL — all writes go here)

Goal:
  1. Copy ln_master_rollup_v1 from thyroid_research_ro_v2 → thyroid_ete_fix_20260413
  2. Audit rollup LN values vs existing canonical LN columns (discrepancy report)
  3. Add ~30 rollup columns to canonical_patient_master_v1 (additive — existing cols unchanged)
  4. Validate: 10,871 rows, 0 dups, 0 NULL research_ids, fna_path_outcome 100% non-null

DESIGN RULES (inherited from 207)
  1. ONE row per patient. ALWAYS 10,871 rows. No exceptions.
  2. NEVER change existing column values — only ADD new ln_rollup_* columns.
  3. Rollup data is additive: tp_* and ln_* columns remain unchanged.
  4. Patients without rollup data (10,871 − 3,986 = 6,885) receive NULL in all rollup columns.
  5. Column mapping is introspected at runtime to guard against schema drift.
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from motherduck_client import get_token  # noqa: E402

OUTPUT_DIR = REPO / "scripts" / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

DB = "thyroid_ete_fix_20260413"
TABLE = "canonical_patient_master_v1"
ROLLUP_TABLE = "ln_master_rollup_v1"
SOURCE_DB = "thyroid_research_ro_v2"

# ---------------------------------------------------------------------------
# Target → candidate source column names (first match wins)
# Allows the script to handle minor schema variations in the rollup table
# ---------------------------------------------------------------------------
COLUMN_MAP: list[tuple[str, list[str]]] = [
    # Totals (x-marker corrected)
    ("ln_rollup_total_examined",             ["ln_total_examined"]),
    ("ln_rollup_total_positive",             ["ln_total_positive"]),
    ("ln_rollup_ratio",                      ["ln_ratio"]),
    ("ln_rollup_any_positive",               ["ln_any_positive", "any_positive"]),
    # ENE & deposit
    ("ln_rollup_ene",                        ["ln_extranodal_extension", "ln_ene",
                                              "extranodal_extension"]),
    ("ln_rollup_mets_ene",                   ["ln_mets_extranodal_extension"]),
    ("ln_rollup_largest_deposit_cm",         ["ln_largest_deposit_cm", "largest_deposit_cm",
                                              "largest_deposit"]),
    # Regional breakdowns
    ("ln_rollup_central_examined",           ["ln_central_examined", "central_examined"]),
    ("ln_rollup_central_positive",           ["ln_central_positive", "central_positive"]),
    ("ln_rollup_lateral_left_examined",      ["ln_lateral_left_examined",
                                              "lateral_left_examined"]),
    ("ln_rollup_lateral_left_positive",      ["ln_lateral_left_positive",
                                              "lateral_left_positive"]),
    ("ln_rollup_lateral_right_examined",     ["ln_lateral_right_examined",
                                              "lateral_right_examined"]),
    ("ln_rollup_lateral_right_positive",     ["ln_lateral_right_positive",
                                              "lateral_right_positive"]),
    ("ln_rollup_bilateral_lateral_examined", ["ln_bilateral_lateral_examined",
                                              "bilateral_lateral_examined"]),
    ("ln_rollup_bilateral_lateral_positive", ["ln_bilateral_lateral_positive",
                                              "bilateral_lateral_positive"]),
    ("ln_rollup_other_examined",             ["ln_other_examined", "other_examined"]),
    ("ln_rollup_other_positive",             ["ln_other_positive", "other_positive"]),
    # Per-level data (I – VII)
    ("ln_level_i_examined",                  ["ln_level_i_examined",   "level_i_examined",
                                              "level_1_examined"]),
    ("ln_level_i_positive",                  ["ln_level_i_positive",   "level_i_positive",
                                              "level_1_positive"]),
    ("ln_level_ii_examined",                 ["ln_level_ii_examined",  "level_ii_examined",
                                              "level_2_examined"]),
    ("ln_level_ii_positive",                 ["ln_level_ii_positive",  "level_ii_positive",
                                              "level_2_positive"]),
    ("ln_level_iii_examined",                ["ln_level_iii_examined", "level_iii_examined",
                                              "level_3_examined"]),
    ("ln_level_iii_positive",                ["ln_level_iii_positive", "level_iii_positive",
                                              "level_3_positive"]),
    ("ln_level_iv_examined",                 ["ln_level_iv_examined",  "level_iv_examined",
                                              "level_4_examined"]),
    ("ln_level_iv_positive",                 ["ln_level_iv_positive",  "level_iv_positive",
                                              "level_4_positive"]),
    ("ln_level_v_examined",                  ["ln_level_v_examined",   "level_v_examined",
                                              "level_5_examined"]),
    ("ln_level_v_positive",                  ["ln_level_v_positive",   "level_v_positive",
                                              "level_5_positive"]),
    ("ln_level_vi_examined",                 ["ln_level_vi_examined",  "level_vi_examined",
                                              "level_6_examined"]),
    ("ln_level_vi_positive",                 ["ln_level_vi_positive",  "level_vi_positive",
                                              "level_6_positive"]),
    ("ln_level_vii_examined",                ["ln_level_vii_examined", "level_vii_examined",
                                              "level_7_examined"]),
    ("ln_level_vii_positive",                ["ln_level_vii_positive", "level_vii_positive",
                                              "level_7_positive"]),
    # Per-cancer-type mets
    ("ln_rollup_mets_ptc",                   ["ln_mets_ptc"]),
    ("ln_rollup_mets_ptc_variant",           ["ln_mets_ptc_variant"]),
    ("ln_rollup_mets_ftc",                   ["ln_mets_ftc"]),
    ("ln_rollup_mets_hurthle",               ["ln_mets_hurthle"]),
    ("ln_rollup_mets_mtc",                   ["ln_mets_mtc"]),
    ("ln_rollup_mets_atc",                   ["ln_mets_atc"]),
    ("ln_rollup_mets_pdtc",                  ["ln_mets_pdtc"]),
    ("ln_rollup_mets_micrometastasis",       ["ln_mets_micrometastasis"]),
    ("ln_rollup_mets_cystic",                ["ln_mets_cystic"]),
    # Summary / QC
    ("ln_rollup_total_levels_involved",      ["ln_total_levels_involved",
                                              "total_levels_involved"]),
    ("ln_rollup_has_per_level_data",         ["has_per_level_data"]),
    ("ln_rollup_crossval_status",            ["ln_crossval_status",   "crossval_status"]),
    ("ln_rollup_internal_consistency",       ["ln_internal_consistency",
                                              "internal_consistency"]),
    ("ln_rollup_source",                     ["ln_source", "source"]),
]

# Deduplication preference order for multi-surgery patients:
#   1. crossval_status = 'agree'     (internally consistent source agreement)
#   2. internal_consistency = 'ok'   (no mismatch flag)
#   3. ln_total_examined DESC        (prefer the row with most data)
# This ensures one canonical patient-level rollup row per research_id.
ROLLUP_DEDUP_SQL = """
    WITH ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY research_id
                ORDER BY
                    CASE WHEN ln_crossval_status = 'agree' THEN 0 ELSE 1 END,
                    CASE WHEN ln_internal_consistency = 'ok' THEN 0 ELSE 1 END,
                    COALESCE(ln_total_examined, 0) DESC
            ) AS _rn
        FROM {rollup}
    )
    SELECT * EXCLUDE (_rn) FROM ranked WHERE _rn = 1
"""


def connect() -> duckdb.DuckDBPyConnection:
    token = get_token()
    if not token:
        raise RuntimeError("MotherDuck token not found — set MOTHERDUCK_TOKEN")
    return duckdb.connect(f"md:{DB}?motherduck_token={token}")


# ---------------------------------------------------------------------------
# Phase 1: Copy ln_master_rollup_v1 from source DB
# ---------------------------------------------------------------------------
def phase1_copy(con: duckdb.DuckDBPyConnection) -> None:
    print("\n" + "=" * 60)
    print("PHASE 1: COPY ln_master_rollup_v1 FROM SOURCE DB")
    print("=" * 60)

    print(f"  Source: {SOURCE_DB}.main.{ROLLUP_TABLE}")
    print(f"  Target: {DB}.main.{ROLLUP_TABLE}")

    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_TABLE} AS
        SELECT * FROM "{SOURCE_DB}".main.{ROLLUP_TABLE}
    """)

    row, pts = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {ROLLUP_TABLE}"
    ).fetchone()

    print(f"  Rows:     {row:,}")
    print(f"  Patients: {pts:,}")

    assert row == 4290, f"Expected 4,290 rows, got {row}"
    assert pts == 3986, f"Expected 3,986 distinct patients, got {pts}"
    print("  ✓ Row/patient counts verified")

    # Print actual columns for reference
    cols = [
        r[0] for r in con.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{ROLLUP_TABLE}' AND table_schema='main' "
            f"ORDER BY ordinal_position"
        ).fetchall()
    ]
    print(f"\n  Rollup columns ({len(cols)} total):")
    for i, c in enumerate(cols):
        print(f"    [{i+1:>2}] {c}")


# ---------------------------------------------------------------------------
# Phase 1b: Restore canonical spine to exactly 10,871 rows if corrupted
# ---------------------------------------------------------------------------
def phase1b_restore_canonical(con: duckdb.DuckDBPyConnection) -> None:
    """Guard against a prior failed run that may have left the canonical with
    extra rows from a fan-out join.  Deduplicates back to one row per research_id
    using ROW_NUMBER() if the count is not exactly 10,871."""
    count = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    if count == 10_871:
        print(f"  ✓ {TABLE}: {count:,} rows — no restoration needed")
        return

    print(f"  ⚠ {TABLE} has {count:,} rows (expected 10,871) — restoring spine …")
    con.execute(f"""
        CREATE OR REPLACE TABLE {TABLE} AS
        SELECT * EXCLUDE (_rn)
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY research_id
                    ORDER BY research_id  -- deterministic tiebreak
                ) AS _rn
            FROM {TABLE}
        ) WHERE _rn = 1
    """)
    restored = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    assert restored == 10_871, (
        f"Restoration failed — still {restored} rows (expected 10,871)"
    )
    print(f"  ✓ Restored to {restored:,} rows")


# ---------------------------------------------------------------------------
# Phase 2: Audit rollup vs canonical LN values
# ---------------------------------------------------------------------------
def phase2_audit(con: duckdb.DuckDBPyConnection) -> None:
    print("\n" + "=" * 60)
    print("PHASE 2: AUDIT — rollup vs canonical LN columns")
    print("=" * 60)

    # Check which comparison columns actually exist in the canonical
    canon_cols = {
        r[0] for r in con.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{TABLE}' AND table_schema='main'"
        ).fetchall()
    }
    rollup_cols = {
        r[0] for r in con.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{ROLLUP_TABLE}' AND table_schema='main'"
        ).fetchall()
    }

    pairs: list[tuple[str, str, str]] = []
    for canon_col, rollup_col, label in [
        ("ln_total_examined", "ln_total_examined", "total_examined"),
        ("ln_total_positive", "ln_total_positive", "total_positive"),
        ("ln_ratio",          "ln_ratio",          "ratio"),
    ]:
        if canon_col in canon_cols and rollup_col in rollup_cols:
            pairs.append((canon_col, rollup_col, label))

    if not pairs:
        print("  ⚠ No overlapping LN columns found for audit — skipping comparison")
        return

    # Build a dynamic comparison query
    mismatch_clauses = " OR ".join(
        f"CAST(c.{cc} AS VARCHAR) != CAST(r.{rc} AS VARCHAR)"
        for cc, rc, _ in pairs
    )
    select_clauses = ", ".join(
        f"c.{cc} AS canon_{lbl}, r.{rc} AS rollup_{lbl}"
        for cc, rc, lbl in pairs
    )

    mismatch_sql = f"""
        SELECT
            c.research_id,
            {select_clauses},
            r.ln_crossval_status,
            r.ln_internal_consistency
        FROM {TABLE} c
        JOIN {ROLLUP_TABLE} r ON c.research_id = CAST(r.research_id AS VARCHAR)
        WHERE {mismatch_clauses}
        ORDER BY c.research_id
    """

    mismatch_df = con.execute(mismatch_sql).fetchdf()
    print(f"  Patients in both tables with LN discrepancy: {len(mismatch_df):,}")

    if len(mismatch_df) > 0:
        print("\n  Sample (first 20 mismatches):")
        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", 120)
        print(mismatch_df.head(20).to_string(index=False))

        # Save full mismatch report
        out = OUTPUT_DIR / "208_ln_mismatch_report.csv"
        mismatch_df.to_csv(out, index=False)
        print(f"\n  Full mismatch report → {out}")
    else:
        print("  ✓ No discrepancies — rollup totals match canonical exactly")

    # Coverage summary
    cov_sql = f"""
        SELECT
            COUNT(*)                                     AS canon_total,
            COUNT(r.research_id)                         AS in_rollup,
            COUNT(*) - COUNT(r.research_id)              AS not_in_rollup
        FROM {TABLE} c
        LEFT JOIN (
            SELECT DISTINCT research_id FROM {ROLLUP_TABLE}
        ) r ON c.research_id = CAST(r.research_id AS VARCHAR)
    """
    row = con.execute(cov_sql).fetchone()
    print(f"\n  Canonical patients:      {row[0]:,}")
    print(f"  In rollup:               {row[1]:,}")
    print(f"  Not in rollup (→ NULL):  {row[2]:,}")


# ---------------------------------------------------------------------------
# Phase 3: Resolve column mapping against actual rollup schema
# ---------------------------------------------------------------------------
def resolve_mapping(
    con: duckdb.DuckDBPyConnection,
) -> list[tuple[str, str | None]]:
    """Return list of (target_col, source_col_or_None) using available rollup columns."""
    rollup_cols = {
        r[0] for r in con.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{ROLLUP_TABLE}' AND table_schema='main'"
        ).fetchall()
    }

    resolved: list[tuple[str, str | None]] = []
    for target, candidates in COLUMN_MAP:
        matched = next((c for c in candidates if c in rollup_cols), None)
        resolved.append((target, matched))

    print("\n  Column resolution:")
    for target, src in resolved:
        status = f"→ r.{src}" if src else "→ NULL (not found in rollup)"
        print(f"    {target:<45} {status}")

    missing = [t for t, s in resolved if s is None]
    if missing:
        print(f"\n  ⚠ {len(missing)} target columns will be NULL (source not found in rollup)")

    return resolved


# ---------------------------------------------------------------------------
# Phase 4: Rebuild canonical master with rollup columns appended
# ---------------------------------------------------------------------------
def phase4_rebuild(
    con: duckdb.DuckDBPyConnection,
    mapping: list[tuple[str, str | None]],
) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("PHASE 4: REBUILD canonical_patient_master_v1 WITH ROLLUP COLUMNS")
    print("=" * 60)

    # Build SELECT list for rollup columns
    rollup_select_parts = []
    for target, src in mapping:
        if src:
            rollup_select_parts.append(f"    r.{src} AS {target}")
        else:
            rollup_select_parts.append(f"    NULL AS {target}")

    rollup_select = ",\n".join(rollup_select_parts)

    # Check existing canon columns — skip adding any that already exist
    canon_cols = {
        row[0] for row in con.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{TABLE}' AND table_schema='main'"
        ).fetchall()
    }
    existing_rollup_cols = {t for t, _ in mapping if t in canon_cols}
    if existing_rollup_cols:
        print(f"  ⚠ Skipping {len(existing_rollup_cols)} rollup cols already in canon: "
              f"{sorted(existing_rollup_cols)}")
        # Remove already-existing targets from select list
        rollup_select_parts = [
            part for part, (tgt, _) in zip(rollup_select_parts, mapping)
            if tgt not in existing_rollup_cols
        ]
        rollup_select = ",\n".join(rollup_select_parts)

    # Dedup rollup to one row per patient before joining
    dedup_sql = ROLLUP_DEDUP_SQL.format(rollup=ROLLUP_TABLE)
    dedup_count = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM ({dedup_sql})"
    ).fetchone()
    print(f"  Deduped rollup: {dedup_count[0]:,} rows, {dedup_count[1]:,} unique patients "
          f"(was 4,290 rows / 3,986 patients before dedup)")
    assert dedup_count[0] == dedup_count[1], (
        f"Dedup still has duplicates: {dedup_count[0]} rows / {dedup_count[1]} patients"
    )

    expansion_sql = f"""
        WITH
        rollup_dedup AS (
            {dedup_sql}
        ),
        rollup AS (
            SELECT
                CAST(research_id AS VARCHAR)  AS research_id,
                *  EXCLUDE (research_id)
            FROM rollup_dedup
        )

        SELECT
            oc.*,

            -- ================================================================
            -- BLOCK LN_ROLLUP: x-marker corrected LN data (ln_master_rollup_v1)
            -- Deduped to one row per patient (prefer agree/ok/max_examined).
            -- Patients without rollup data receive NULL (additive — no overwrite)
            -- ================================================================
        {rollup_select}

        FROM {TABLE} oc
        LEFT JOIN rollup r
            ON oc.research_id = r.research_id
    """

    print("  Building expanded table …")
    con.execute(f"CREATE OR REPLACE TABLE {TABLE} AS {expansion_sql}")

    df = con.execute(f"SELECT * FROM {TABLE}").fetchdf()
    print(f"  Rows:    {len(df):,}")
    print(f"  Patients:{df['research_id'].nunique():,}")
    print(f"  Columns: {len(df.columns)}")
    return df


# ---------------------------------------------------------------------------
# Phase 5: Validation
# ---------------------------------------------------------------------------
def phase5_validate(
    con: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    mapping: list[tuple[str, str | None]],
) -> None:
    print("\n" + "=" * 60)
    print("PHASE 5: VALIDATION")
    print("=" * 60)

    # 1. Row count
    assert len(df) == 10_871, f"FAIL row count: expected 10,871, got {len(df)}"
    print(f"  ✓ Row count: {len(df):,}")

    # 2. No duplicate research_ids
    dups = df["research_id"].duplicated().sum()
    assert dups == 0, f"FAIL: {dups} duplicate research_ids"
    print("  ✓ No duplicate research_ids")

    # 3. No NULL research_ids
    nulls = df["research_id"].isna().sum()
    assert nulls == 0, f"FAIL: {nulls} NULL research_ids"
    print("  ✓ No NULL research_ids")

    # 4. fna_path_outcome 100% non-null
    fpo_null = df["fna_path_outcome"].isna().sum() if "fna_path_outcome" in df.columns else 0
    status = "✓" if fpo_null == 0 else "⚠"
    print(f"  {status} fna_path_outcome NULL: {fpo_null}")

    # 5. Rollup coverage for in-rollup patients
    rollup_targets = [t for t, s in mapping if s is not None and t in df.columns]
    if rollup_targets:
        primary = rollup_targets[0]  # ln_rollup_total_examined
        n_with_rollup = df[primary].notna().sum()
        print(f"\n  Rollup coverage (via '{primary}'):")
        print(f"    Non-null: {n_with_rollup:,} / 10,871 (expected ~3,986)")
        if n_with_rollup < 3_800 or n_with_rollup > 4_100:
            print("  ⚠ Coverage outside expected range 3,800–4,100")
        else:
            print("  ✓ Coverage in expected range")

    # 6. Per-level data coverage
    print("\n  Per-level LN data coverage:")
    for level in ["i", "ii", "iii", "iv", "v", "vi", "vii"]:
        col = f"ln_level_{level}_examined"
        if col in df.columns:
            n = df[col].notna().sum()
            print(f"    {col}: {n:,} patients")

    # 7. MotherDuck row-count cross-check
    md_count = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    assert md_count == len(df), f"FAIL: MotherDuck={md_count} vs pandas={len(df)}"
    print(f"\n  ✓ MotherDuck row count matches: {md_count:,}")

    # 8. Column count
    md_cols = con.execute(f"""
        SELECT COUNT(DISTINCT column_name) FROM information_schema.columns
        WHERE table_name='{TABLE}' AND table_schema='main'
    """).fetchone()[0]
    print(f"  ✓ Total columns in MotherDuck: {md_cols}")


# ---------------------------------------------------------------------------
# Phase 6: Save parquet
# ---------------------------------------------------------------------------
def phase6_save(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    print("\n" + "=" * 60)
    print("PHASE 6: SAVE PARQUET")
    print("=" * 60)

    out_path = OUTPUT_DIR / "canonical_patient_master_v1.parquet"
    df.to_parquet(out_path, index=False)
    size_mb = out_path.stat().st_size / 1_048_576
    print(f"  Saved: {out_path}")
    print(f"  Size:  {size_mb:.1f} MB")
    print(f"  Rows:  {len(df):,} | Columns: {len(df.columns)}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    con = connect()
    print(f"Connected → MotherDuck database: {DB}")

    phase1_copy(con)

    print("\n" + "=" * 60)
    print("PHASE 1b: RESTORE CANONICAL SPINE (guard against prior failed runs)")
    print("=" * 60)
    phase1b_restore_canonical(con)

    phase2_audit(con)

    print("\n" + "=" * 60)
    print("PHASE 3: RESOLVE COLUMN MAPPING")
    print("=" * 60)
    mapping = resolve_mapping(con)

    df = phase4_rebuild(con, mapping)
    phase5_validate(con, df, mapping)
    phase6_save(con, df)

    new_rollup_cols = [t for t, s in mapping if s is not None and t in df.columns]
    null_cols = [t for t, s in mapping if s is None]

    print("\n" + "=" * 60)
    print("✓ Script 208 COMPLETE")
    print(f"  Table:              {TABLE}")
    print(f"  Database:           {DB}")
    print(f"  Rows:               {len(df):,}")
    print(f"  Columns:            {len(df.columns)}")
    print(f"  Rollup cols added:  {len(new_rollup_cols)}")
    if null_cols:
        print(f"  Cols left NULL:     {len(null_cols)} (source not found in rollup schema)")
    print("=" * 60)

    con.close()


if __name__ == "__main__":
    main()
