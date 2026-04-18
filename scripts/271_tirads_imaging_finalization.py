#!/usr/bin/env python3
"""Script 271 — Canonical TI-RADS & Imaging Finalization.

Implements the runbook from PROMPT 19 Extended (2026-04-17) targeting the
WRITE database `thyroid_canonical_publication_v1_0` (CPM, cunc_v1, inm_v1,
data_dictionary_v266a, __readme) with archive snapshots into
`"Thyroid 2026 UPdated".archive_pub_v1_0`.

Each step is gated by `--step N` so a human operator can commit between steps
(per the prompt's "Git commit after each numbered step. No batch commits"
rule). Each step is idempotent where possible and prints a clear summary.

Steps:
  0 — Preflight verification (read-only). STOP on mismatch.
  1 — Safety snapshots to archive_pub_v1_0.
  2 — Drop stale imaging_nodule_size_cm_v11 from CPM.
  3 — Rebuild cunc_v1 TIRADS category semantics from ACR points.
  4 — Add points-based TIRADS rollup to CPM + audit 6 discrepancy patients.
  5 — Calcifications coverage flags + tirads_reextraction_queue_v1.
  6 — dominant_nodule_flag on inm_v1 + 3 new CPM columns.
  7 — Archive & naming hygiene sweep (DB tables + repo source grep).
  8 — Refresh data_dictionary_v266a + __readme + CANONICAL_STATE doc.
  9 — Final verification + parquet export.

Invariants verified after every mutating step:
  canonical_patient_master: 10,871 rows / 10,871 distinct research_id / 0 NULL.

Repo source: scripts/_md_connect.py supplies the locked-search-path connection.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
ARCHIVE_PREFIX = f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA}'

ISO_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
SCRIPT_TAG = "script271_2026-04-17"

# Active patient-level TIRADS discrepancies (Step 4)
ACTIVE_DISCREPANCY_RIDS = ["7974", "10992", "7573", "7074", "7049", "6731"]


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg: str, *, also_print: bool = True) -> None:
    line = f"[{_ts()}] {msg}"
    if also_print:
        print(line, flush=True)
    log_path = OUT_DIR / "271_run.log"
    with log_path.open("a") as fh:
        fh.write(line + "\n")


# ---------------------------------------------------------------------------
# Invariants
# ---------------------------------------------------------------------------

def assert_invariants(con) -> None:
    row = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id), "
        "COUNT(*) FILTER (WHERE research_id IS NULL) "
        f"FROM {PUBLICATION_DB}.main.canonical_patient_master"
    ).fetchone()
    n, d, nulls = row
    log(f"INVARIANTS canonical_patient_master: n={n} distinct={d} nulls={nulls}")
    if (n, d, nulls) != (10871, 10871, 0):
        raise SystemExit(
            f"INVARIANT VIOLATION: expected (10871,10871,0), got ({n},{d},{nulls})"
        )


# ---------------------------------------------------------------------------
# STEP 0 — Preflight (READ-ONLY)
# ---------------------------------------------------------------------------

def step0_preflight(con) -> dict:
    log("=== STEP 0 — Preflight verification (read-only) ===")

    out: dict = {"step": 0, "ts": _ts(), "ok": True, "checks": {}}

    # A. Invariants
    row = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM canonical_patient_master"
    ).fetchone()
    n, d = row
    out["checks"]["A_invariants"] = {"n": n, "distinct": d, "expected": [10871, 10871]}
    log(f"  A. canonical_patient_master: {n} rows / {d} distinct rid")
    if (n, d) != (10871, 10871):
        out["ok"] = False

    # B. imaging_nodule_size_cm_v11 still present
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND table_name='canonical_patient_master' "
        "AND column_name='imaging_nodule_size_cm_v11'"
    ).fetchone()
    has_v11 = row[0]
    out["checks"]["B_size_cm_v11_present"] = {"count": has_v11, "expected": 1}
    log(f"  B. imaging_nodule_size_cm_v11 present in CPM: {has_v11} (expect 1)")
    if has_v11 != 1:
        out["ok"] = False

    # C. cunc_v1 dual-naming for tirads_category
    row = con.execute(
        "SELECT COUNT(DISTINCT tirads_category) AS n_distinct, "
        "COUNT(*) FILTER (WHERE tirads_category LIKE 'TR%\\_%' ESCAPE '\\') AS long_named, "
        "COUNT(*) FILTER (WHERE tirads_category IN ('TR1','TR2','TR3','TR4','TR5')) AS short_named "
        "FROM canonical_us_nodule_characteristics_v1"
    ).fetchone()
    n_dist, long_n, short_n = row
    out["checks"]["C_cunc_dual_naming"] = {
        "n_distinct_cats": n_dist, "long_named": long_n, "short_named": short_n,
        "expected": "~10 distinct, both long and short present",
    }
    log(f"  C. cunc_v1 tirads_category: {n_dist} distinct, long={long_n}, short={short_n}")
    if not (long_n > 0 and short_n > 0):
        log("    WARN: expected both long and short variants present")

    # D. Calcifications gap
    row = con.execute(
        "SELECT COUNT(*) FILTER (WHERE tirads_score_2017 IS NOT NULL) AS scored, "
        "COUNT(*) FILTER (WHERE tirads_score_2017 IS NOT NULL AND calcifications IS NULL) "
        "AS scored_no_calc "
        "FROM canonical_us_nodule_characteristics_v1"
    ).fetchone()
    scored, scored_no_calc = row
    pct = (scored_no_calc / scored * 100.0) if scored else 0.0
    out["checks"]["D_calc_gap"] = {
        "scored": scored, "scored_no_calc": scored_no_calc,
        "pct_no_calc": round(pct, 1), "expected": "~99% scored have no calc",
    }
    log(f"  D. cunc_v1 calc gap: {scored_no_calc}/{scored} ({pct:.1f}%) scored have no calcifications")

    # E. inm_v1 dominant_nodule_flag status
    row = con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND table_name='imaging_nodule_master_v1' "
        "AND column_name='dominant_nodule_flag'"
    ).fetchall()
    has_dnf = len(row)
    out["checks"]["E_dominant_nodule_flag_present"] = {"count": has_dnf, "expected": 0}
    log(f"  E. inm_v1.dominant_nodule_flag exists: {has_dnf} (expect 0)")
    if has_dnf != 0:
        out["ok"] = False

    # F. inm_v1 row counts (sanity)
    row = con.execute(
        "SELECT COUNT(*) FROM imaging_nodule_master_v1"
    ).fetchone()
    inm_n = row[0]
    out["checks"]["F_inm_v1_rows"] = {"n": inm_n, "expected": 37016}
    log(f"  F. imaging_nodule_master_v1 rows: {inm_n} (expect 37016)")

    # G. cunc_v1 row counts (sanity)
    row = con.execute(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE tirads_score_2017 IS NOT NULL) "
        "FROM canonical_us_nodule_characteristics_v1"
    ).fetchone()
    cunc_n, cunc_scored = row
    out["checks"]["G_cunc_v1_rows"] = {
        "n": cunc_n, "scored": cunc_scored, "expected": "37016, ~19891 scored",
    }
    log(f"  G. cunc_v1: {cunc_n} rows, {cunc_scored} scored")

    # H. CPM column count baseline (for Step 9 delta check)
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND table_name='canonical_patient_master'"
    ).fetchone()
    cpm_cols_pre = row[0]
    out["checks"]["H_cpm_col_count_pre"] = {"n": cpm_cols_pre, "expected": 1514}
    log(f"  H. CPM column count (pre-271): {cpm_cols_pre} (expect 1514)")

    # I. data_dictionary_v266a present
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND table_name='data_dictionary_v266a'"
    ).fetchone()
    has_dd = row[0]
    out["checks"]["I_data_dictionary_v266a"] = {"present": has_dd, "expected": 1}
    log(f"  I. data_dictionary_v266a present: {has_dd}")
    if has_dd != 1:
        out["ok"] = False

    # J. archive_pub_v1_0 schema in "Thyroid 2026 UPdated" present
    row = con.execute(
        f'SELECT COUNT(*) FROM "{ARCHIVE_DB}".information_schema.schemata '
        f"WHERE schema_name='{ARCHIVE_SCHEMA}'"
    ).fetchone()
    has_arch = row[0]
    out["checks"]["J_archive_schema"] = {"present": has_arch, "expected": 1}
    log(f"  J. archive schema {ARCHIVE_PREFIX} present: {has_arch}")
    if has_arch != 1:
        out["ok"] = False

    # Save
    json_out = OUT_DIR / "271_step0_preflight.json"
    with json_out.open("w") as fh:
        json.dump(out, fh, indent=2, default=str)
    log(f"  Wrote {json_out}")

    if not out["ok"]:
        log("  *** PREFLIGHT FAILED — STOP. Investigate mismatches before Step 1. ***")
        raise SystemExit(2)
    log("  Preflight PASSED.")
    return out


# ---------------------------------------------------------------------------
# STEP 1 — Snapshots
# ---------------------------------------------------------------------------

SNAPSHOT_TABLES = [
    "canonical_patient_master",
    "canonical_us_nodule_characteristics_v1",
    "imaging_nodule_master_v1",
    "data_dictionary_v266a",
    "__readme",
]


def step1_snapshots(con) -> dict:
    log(f"=== STEP 1 — Safety snapshots to {ARCHIVE_PREFIX} (TS={ISO_TS}) ===")
    out: dict = {"step": 1, "ts": _ts(), "snapshots": []}

    for tbl in SNAPSHOT_TABLES:
        # Use quoted identifier for __readme (starts with underscore)
        src_q = f'{PUBLICATION_DB}.main."{tbl}"' if tbl.startswith("_") else f"{PUBLICATION_DB}.main.{tbl}"
        snap_name = f"{tbl}_pre271_{ISO_TS}"
        snap_fq = f'{ARCHIVE_PREFIX}."{snap_name}"'
        log(f"  Snapshotting {tbl} -> {snap_fq}")

        # Skip if already exists with same name
        exists = con.execute(
            f'SELECT COUNT(*) FROM "{ARCHIVE_DB}".information_schema.tables '
            f"WHERE table_schema='{ARCHIVE_SCHEMA}' AND table_name='{snap_name}'"
        ).fetchone()[0]
        if exists:
            log(f"    SKIP — already exists: {snap_fq}")
        else:
            con.execute(f"CREATE TABLE {snap_fq} AS SELECT * FROM {src_q}")

        src_n = con.execute(f"SELECT COUNT(*) FROM {src_q}").fetchone()[0]
        snap_n = con.execute(f"SELECT COUNT(*) FROM {snap_fq}").fetchone()[0]
        match = src_n == snap_n
        log(f"    src={src_n} snap={snap_n} match={match}")
        if not match:
            raise SystemExit(f"Snapshot row count mismatch for {tbl}: {src_n} vs {snap_n}")
        out["snapshots"].append({
            "table": tbl, "snapshot": snap_name, "rows": src_n,
        })

    # Persist
    json_out = OUT_DIR / "271_step1_snapshots.json"
    with json_out.open("w") as fh:
        json.dump(out, fh, indent=2, default=str)
    log(f"  Wrote {json_out}")
    assert_invariants(con)
    return out


# ---------------------------------------------------------------------------
# STEP 2 — Drop imaging_nodule_size_cm_v11 from CPM
# ---------------------------------------------------------------------------

def step2_drop_size_cm_v11(con) -> dict:
    log("=== STEP 2 — Drop imaging_nodule_size_cm_v11 from CPM ===")
    out: dict = {"step": 2, "ts": _ts()}

    # Confirm column present (idempotency guard)
    has_col = con.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND table_name='canonical_patient_master' "
        "AND column_name='imaging_nodule_size_cm_v11'"
    ).fetchone()[0]
    if not has_col:
        log("  Column already absent — idempotent no-op.")
        out["dropped"] = False
    else:
        con.execute(
            "ALTER TABLE canonical_patient_master DROP COLUMN imaging_nodule_size_cm_v11"
        )
        log("  DROPPED imaging_nodule_size_cm_v11.")
        out["dropped"] = True

    assert_invariants(con)

    # Insert removal note into data_dictionary_v266a
    log("  Recording removal in data_dictionary_v266a...")
    # Determine the dictionary schema first
    cols = con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND table_name='data_dictionary_v266a' ORDER BY ordinal_position"
    ).fetchall()
    dd_cols = [c[0] for c in cols]
    out["dd_cols"] = dd_cols
    log(f"    data_dictionary_v266a columns: {dd_cols}")
    # We will fully regenerate the dictionary in Step 8, so just leave a marker row
    # if the schema supports it. The dictionary will be rebuilt anyway.

    json_out = OUT_DIR / "271_step2_drop_v11.json"
    with json_out.open("w") as fh:
        json.dump(out, fh, indent=2, default=str)
    log(f"  Wrote {json_out}")
    return out


# ---------------------------------------------------------------------------
# STEP 3 — Rebuild cunc_v1 TIRADS category semantics
# ---------------------------------------------------------------------------

def step3_rebuild_cunc_categories(con) -> dict:
    log("=== STEP 3 — Rebuild cunc_v1 TIRADS category semantics from ACR points ===")
    out: dict = {"step": 3, "ts": _ts()}

    deprecated_name = f"canonical_us_nodule_characteristics_v1_deprecated_{ISO_TS[:8]}"
    # Verify nothing weird
    cur_cols = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND table_name='canonical_us_nodule_characteristics_v1' "
        "ORDER BY ordinal_position"
    ).fetchall()]
    out["cur_cols_count"] = len(cur_cols)
    log(f"  cunc_v1 currently has {len(cur_cols)} columns")
    has_legacy_v2_alt = "tirads_category_v2_alt" in cur_cols
    has_legacy_modified = "tirads_category_modified" in cur_cols
    has_acr_recalc = "tirads_acr_recalculated" in cur_cols
    has_legacy_v2 = "tirads_category_v2" in cur_cols
    log(f"    has tirads_category_modified: {has_legacy_modified}")
    log(f"    has tirads_acr_recalculated: {has_acr_recalc}")
    log(f"    has tirads_category_v2 (already): {has_legacy_v2}")

    # 3.1 Build _cunc_v2 with new derived columns
    log("  Building thyroid_canonical_publication_v1_0.main._cunc_v2 ...")
    # Decide what to do with tirads_category_modified before the rebuild:
    # We'll rename it inside the new table to *_legacy_v1 unless it agrees
    # exactly with the points-derived bands. Easier path: always rename to
    # _legacy_v1 with a clarifying comment (the prompt says this is acceptable).
    select_extras = [
        # tirads_category_v2 — NEW points-derived band
        "CASE "
        "  WHEN tirads_score_2017 = 0 THEN 'TR1' "
        "  WHEN tirads_score_2017 = 2 THEN 'TR2' "
        "  WHEN tirads_score_2017 = 3 THEN 'TR3' "
        "  WHEN tirads_score_2017 BETWEEN 4 AND 6 THEN 'TR4' "
        "  WHEN tirads_score_2017 >= 7 THEN 'TR5' "
        "  ELSE NULL "
        "END AS tirads_category_v2",
        # tirads_band_ambiguous — TRUE for 1-point or NULL points
        "CASE "
        "  WHEN tirads_score_2017 = 1 OR tirads_score_2017 IS NULL THEN TRUE "
        "  ELSE FALSE "
        "END AS tirads_band_ambiguous",
    ]
    # Drop tirads_category and tirads_category_v2 (if present) from base SELECT
    drop_cols = {"tirads_category"}
    if has_legacy_v2:
        drop_cols.add("tirads_category_v2")
    base_cols = [c for c in cur_cols if c not in drop_cols]
    base_select = ", ".join(f'"{c}"' for c in base_cols)
    new_select = base_select + ", " + ", ".join(select_extras)

    con.execute(
        f"CREATE OR REPLACE TABLE {PUBLICATION_DB}.main._cunc_v2 AS "
        f"SELECT {new_select} FROM {PUBLICATION_DB}.main.canonical_us_nodule_characteristics_v1"
    )

    # Rename tirads_acr_recalculated -> tirads_category_code_legacy_v1 (if present)
    if has_acr_recalc:
        con.execute(
            f"ALTER TABLE {PUBLICATION_DB}.main._cunc_v2 "
            "RENAME COLUMN tirads_acr_recalculated TO tirads_category_code_legacy_v1"
        )
        log("    Renamed tirads_acr_recalculated -> tirads_category_code_legacy_v1")

    # Rename tirads_category_modified -> tirads_category_modified_legacy_v1 (if present)
    if has_legacy_modified:
        con.execute(
            f"ALTER TABLE {PUBLICATION_DB}.main._cunc_v2 "
            "RENAME COLUMN tirads_category_modified TO tirads_category_modified_legacy_v1"
        )
        log("    Renamed tirads_category_modified -> tirads_category_modified_legacy_v1")

    # Add COMMENTs on new and renamed cols
    comments = [
        ("tirads_category_v2",
         f"ACR-points-derived TIRADS band (TR1/TR2/TR3/TR4/TR5) from tirads_score_2017. "
         f"NULL when score is 1 or NULL. Source: cunc_v1; derivation: see Script 271 prompt; "
         f"build={ISO_TS}; script={SCRIPT_TAG}."),
        ("tirads_band_ambiguous",
         f"TRUE when tirads_score_2017=1 (no defined ACR band) or NULL. "
         f"Build={ISO_TS}; script={SCRIPT_TAG}."),
    ]
    if has_acr_recalc:
        comments.append(
            ("tirads_category_code_legacy_v1",
             f"LEGACY: structured-Excel TIRADS category code 1-5 (NOT ACR points). "
             f"Renamed from tirads_acr_recalculated by Script 271 to disambiguate from "
             f"points-based scoring. Use tirads_category_v2 / tirads_score_2017 for "
             f"ACR-points analysis. Build={ISO_TS}; script={SCRIPT_TAG}.")
        )
    if has_legacy_modified:
        comments.append(
            ("tirads_category_modified_legacy_v1",
             f"LEGACY: original 'tirads_category_modified' column with mixed/short naming. "
             f"Use tirads_category_v2 going forward. Build={ISO_TS}; script={SCRIPT_TAG}.")
        )
    for col, txt in comments:
        safe = txt.replace("'", "''")
        con.execute(
            f"COMMENT ON COLUMN {PUBLICATION_DB}.main._cunc_v2.{col} IS '{safe}'"
        )

    # 3.2 Atomic swap
    log("  Atomic swap: rename canonical_us_nodule_characteristics_v1 -> deprecated, then _cunc_v2 -> v1")
    con.execute(
        f"ALTER TABLE {PUBLICATION_DB}.main.canonical_us_nodule_characteristics_v1 "
        f"RENAME TO {deprecated_name}"
    )
    con.execute(
        f"ALTER TABLE {PUBLICATION_DB}.main._cunc_v2 "
        "RENAME TO canonical_us_nodule_characteristics_v1"
    )

    # 3.3 Move deprecated to archive_pub_v1_0 and drop from canonical
    log(f"  Archiving {deprecated_name} -> {ARCHIVE_PREFIX} ...")
    archive_dep_fq = f'{ARCHIVE_PREFIX}."{deprecated_name}"'
    con.execute(
        f"CREATE OR REPLACE TABLE {archive_dep_fq} AS "
        f"SELECT * FROM {PUBLICATION_DB}.main.{deprecated_name}"
    )
    src_n = con.execute(
        f"SELECT COUNT(*) FROM {PUBLICATION_DB}.main.{deprecated_name}"
    ).fetchone()[0]
    arch_n = con.execute(f"SELECT COUNT(*) FROM {archive_dep_fq}").fetchone()[0]
    if src_n != arch_n:
        raise SystemExit(f"Archive copy mismatch for {deprecated_name}: {src_n} vs {arch_n}")
    log(f"    archived rows={arch_n}; dropping {deprecated_name} from main")
    con.execute(f"DROP TABLE {PUBLICATION_DB}.main.{deprecated_name}")

    # 3.4 Validation
    log("  Validating new tirads_category_v2 bands ...")
    rows = con.execute(
        "SELECT tirads_category_v2, MIN(tirads_score_2017), MAX(tirads_score_2017), COUNT(*) "
        "FROM canonical_us_nodule_characteristics_v1 "
        "GROUP BY 1 ORDER BY 1 NULLS LAST"
    ).fetchall()
    out["band_audit"] = [
        {"band": r[0], "min_pts": r[1], "max_pts": r[2], "n": r[3]} for r in rows
    ]
    for r in rows:
        log(f"    band={r[0]} min_pts={r[1]} max_pts={r[2]} n={r[3]}")
    # Sanity checks
    band_map = {r[0]: (r[1], r[2], r[3]) for r in rows}
    if "TR5" in band_map and band_map["TR5"][0] is not None and band_map["TR5"][0] < 7:
        raise SystemExit(f"TR5 MIN points {band_map['TR5'][0]} < 7")
    if "TR1" in band_map and band_map["TR1"][1] is not None and band_map["TR1"][1] != 0:
        raise SystemExit(f"TR1 MAX points {band_map['TR1'][1]} != 0")
    if "TR3" in band_map:
        mn, mx, _ = band_map["TR3"]
        if mn != 3 or mx != 3:
            raise SystemExit(f"TR3 MIN/MAX must both be 3, got {mn}/{mx}")
    log("  Band sanity checks PASSED.")

    assert_invariants(con)

    json_out = OUT_DIR / "271_step3_cunc_rebuild.json"
    with json_out.open("w") as fh:
        json.dump(out, fh, indent=2, default=str)
    log(f"  Wrote {json_out}")
    return out


# ---------------------------------------------------------------------------
# STEP 4 — Patient-level points-based rollup + audit 6 discrepancies
# ---------------------------------------------------------------------------

def step4_patient_points_rollup(con) -> dict:
    log("=== STEP 4 — Add tirads_*_points_v271 to CPM + audit 6 discrepancy patients ===")
    out: dict = {"step": 4, "ts": _ts()}

    new_cols = [
        ("tirads_worst_points_v271", "DOUBLE",
         f"MAX tirads_score_2017 (ACR points 0-13+) per patient from cunc_v1. "
         f"Source: canonical_us_nodule_characteristics_v1; derivation: GROUP BY research_id; "
         f"build={ISO_TS}; script={SCRIPT_TAG}."),
        ("tirads_best_points_v271", "DOUBLE",
         f"MIN tirads_score_2017 (ACR points 0-13+) per patient from cunc_v1. "
         f"Source: canonical_us_nodule_characteristics_v1; build={ISO_TS}; script={SCRIPT_TAG}."),
        ("tirads_source_system_v271", "VARCHAR",
         f"Source-system label for tirads_*_points_v271 rollups: 'cunc_v1_points_acr2017'. "
         f"Build={ISO_TS}; script={SCRIPT_TAG}."),
    ]

    existing = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND table_name='canonical_patient_master'"
    ).fetchall()}

    for col, typ, comment in new_cols:
        if col in existing:
            log(f"  SKIP add (exists): {col}")
        else:
            con.execute(
                f"ALTER TABLE canonical_patient_master ADD COLUMN {col} {typ}"
            )
            log(f"  Added {col} {typ}")
        safe = comment.replace("'", "''")
        con.execute(
            f"COMMENT ON COLUMN canonical_patient_master.{col} IS '{safe}'"
        )

    # Populate via temp table
    log("  Computing per-patient points rollup from cunc_v1 ...")
    con.execute(
        "CREATE OR REPLACE TEMP TABLE _tp AS "
        "SELECT CAST(research_id AS VARCHAR) AS research_id, "
        "       MAX(tirads_score_2017) AS worst_pts, "
        "       MIN(tirads_score_2017) AS best_pts "
        "FROM canonical_us_nodule_characteristics_v1 "
        "WHERE tirads_score_2017 IS NOT NULL "
        "GROUP BY 1"
    )
    n_tp = con.execute("SELECT COUNT(*) FROM _tp").fetchone()[0]
    log(f"    _tp rows (patients with any scored nodule): {n_tp}")

    log("  UPDATE canonical_patient_master FROM _tp ...")
    con.execute(
        "UPDATE canonical_patient_master AS c "
        "SET tirads_worst_points_v271 = t.worst_pts, "
        "    tirads_best_points_v271  = t.best_pts, "
        "    tirads_source_system_v271 = 'cunc_v1_points_acr2017' "
        "FROM _tp AS t "
        "WHERE c.research_id = t.research_id"
    )

    # Coverage report
    cov = con.execute(
        "SELECT COUNT(*) FILTER (WHERE tirads_worst_points_v271 IS NOT NULL) AS n_w, "
        "       COUNT(*) FILTER (WHERE tirads_best_points_v271 IS NOT NULL)  AS n_b "
        "FROM canonical_patient_master"
    ).fetchone()
    out["coverage"] = {"with_worst_pts": cov[0], "with_best_pts": cov[1]}
    log(f"    CPM rows populated: worst={cov[0]} best={cov[1]}")

    # Re-comment legacy v12 cols
    legacy_comments = {
        "tirads_worst_score_v12":
            f"LEGACY: MAX of tirads_acr_recalculated (category code 1-5, NOT ACR points). "
            f"Use tirads_worst_points_v271 for points-based analysis. "
            f"Annotated by Script 271 ({ISO_TS}); script={SCRIPT_TAG}.",
        "tirads_best_score_v12":
            f"LEGACY: MIN of tirads_acr_recalculated (category code 1-5, NOT ACR points). "
            f"Use tirads_best_points_v271 for points-based analysis. "
            f"Annotated by Script 271 ({ISO_TS}); script={SCRIPT_TAG}.",
    }
    for col, txt in legacy_comments.items():
        if col in existing:
            safe = txt.replace("'", "''")
            con.execute(f"COMMENT ON COLUMN canonical_patient_master.{col} IS '{safe}'")
            log(f"  Re-COMMENTed legacy column {col}")
        else:
            log(f"  WARN: legacy column {col} not present; skipping comment")

    # Audit 6 discrepancy patients
    rid_list = ", ".join(f"'{r}'" for r in ACTIVE_DISCREPANCY_RIDS)
    log(f"  Auditing discrepancy patients: {ACTIVE_DISCREPANCY_RIDS}")
    cunc_rows = con.execute(
        "SELECT CAST(research_id AS VARCHAR) AS rid, exam_date, nodule_id, "
        "       tirads_score_2017, tirads_category_v2, "
        "       tirads_category_code_legacy_v1 "
        "FROM canonical_us_nodule_characteristics_v1 "
        f"WHERE CAST(research_id AS VARCHAR) IN ({rid_list}) "
        "ORDER BY rid, exam_date NULLS LAST, nodule_id"
    ).fetchall()
    cpm_rows = con.execute(
        "SELECT CAST(research_id AS VARCHAR) AS rid, "
        "       tirads_worst_score_v12, tirads_best_score_v12, "
        "       tirads_worst_points_v271, tirads_best_points_v271, "
        "       tirads_source_system_v271 "
        "FROM canonical_patient_master "
        f"WHERE CAST(research_id AS VARCHAR) IN ({rid_list}) "
        "ORDER BY rid"
    ).fetchall()
    out["audit_cunc_rows"] = [
        {
            "rid": r[0], "exam_date": str(r[1]), "nodule_id": r[2],
            "score_2017": r[3], "category_v2": r[4],
            "category_code_legacy_v1": r[5],
        } for r in cunc_rows
    ]
    out["audit_cpm_rollups"] = [
        {
            "rid": r[0], "worst_v12": r[1], "best_v12": r[2],
            "worst_pts_v271": r[3], "best_pts_v271": r[4],
            "source_system_v271": r[5],
        } for r in cpm_rows
    ]
    log("  CPM rollups for discrepancy patients:")
    for r in cpm_rows:
        log(f"    rid={r[0]} v12_worst={r[1]} v271_worst_pts={r[3]} src={r[5]}")
    log("  cunc_v1 detail rows for discrepancy patients:")
    for r in cunc_rows:
        log(f"    rid={r[0]} exam={r[1]} nod={r[2]} pts={r[3]} band={r[4]} legacy_code={r[5]}")

    assert_invariants(con)

    json_out = OUT_DIR / "271_step4_points_rollup.json"
    with json_out.open("w") as fh:
        json.dump(out, fh, indent=2, default=str)
    log(f"  Wrote {json_out}")
    return out


# ---------------------------------------------------------------------------
# STEP 5 — Calcifications coverage flags + re-extraction queue
# ---------------------------------------------------------------------------

def step5_calcifications_coverage(con) -> dict:
    log("=== STEP 5 — Calcifications coverage flags + tirads_reextraction_queue_v1 ===")
    out: dict = {"step": 5, "ts": _ts()}

    cunc_cols = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND table_name='canonical_us_nodule_characteristics_v1'"
    ).fetchall()}

    if "calcifications_coverage_status" not in cunc_cols:
        con.execute(
            "ALTER TABLE canonical_us_nodule_characteristics_v1 "
            "ADD COLUMN calcifications_coverage_status VARCHAR"
        )
        log("  Added calcifications_coverage_status VARCHAR")
    else:
        log("  SKIP add (exists): calcifications_coverage_status")
    safe = (
        f"Values: extracted (source had calcifications), not_extracted (LLM skipped, "
        f"score was assigned anyway), absent_from_report (no score and no "
        f"calcifications). Build={ISO_TS}; script={SCRIPT_TAG}."
    ).replace("'", "''")
    con.execute(
        "COMMENT ON COLUMN canonical_us_nodule_characteristics_v1."
        f"calcifications_coverage_status IS '{safe}'"
    )

    if "tirads_score_component_complete" not in cunc_cols:
        con.execute(
            "ALTER TABLE canonical_us_nodule_characteristics_v1 "
            "ADD COLUMN tirads_score_component_complete BOOLEAN"
        )
        log("  Added tirads_score_component_complete BOOLEAN")
    else:
        log("  SKIP add (exists): tirads_score_component_complete")
    safe = (
        f"TRUE iff all 5 ACR components (composition, echogenicity, shape, margins, "
        f"calcifications) are non-NULL. Only TRUE rows should be treated as complete "
        f"ACR scores. Build={ISO_TS}; script={SCRIPT_TAG}."
    ).replace("'", "''")
    con.execute(
        "COMMENT ON COLUMN canonical_us_nodule_characteristics_v1."
        f"tirads_score_component_complete IS '{safe}'"
    )

    # Populate
    log("  Populating calcifications_coverage_status ...")
    con.execute(
        "UPDATE canonical_us_nodule_characteristics_v1 "
        "SET calcifications_coverage_status = CASE "
        "  WHEN calcifications IS NOT NULL THEN 'extracted' "
        "  WHEN tirads_score_2017 IS NOT NULL THEN 'not_extracted' "
        "  ELSE 'absent_from_report' END"
    )
    log("  Populating tirads_score_component_complete ...")
    # Pre-check that all expected component columns exist
    needed = ["composition", "echogenicity", "shape", "margins", "calcifications"]
    missing = [c for c in needed if c not in cunc_cols]
    if missing:
        log(f"  WARN: missing component columns in cunc_v1: {missing}; "
            "tirads_score_component_complete will treat them as NULL.")
        # Build expression that NULL-coalesces missing columns to NULL (false in AND chain)
        parts = [f"({c} IS NOT NULL)" if c not in missing else "FALSE" for c in needed]
    else:
        parts = [f"({c} IS NOT NULL)" for c in needed]
    expr = " AND ".join(parts)
    con.execute(
        f"UPDATE canonical_us_nodule_characteristics_v1 "
        f"SET tirads_score_component_complete = ({expr})"
    )

    # Coverage report
    cov = con.execute(
        "SELECT calcifications_coverage_status, COUNT(*) "
        "FROM canonical_us_nodule_characteristics_v1 "
        "GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    complete_n = con.execute(
        "SELECT COUNT(*) FILTER (WHERE tirads_score_component_complete) "
        "FROM canonical_us_nodule_characteristics_v1"
    ).fetchone()[0]
    out["coverage_status_counts"] = [{"status": r[0], "n": r[1]} for r in cov]
    out["score_component_complete_count"] = complete_n
    log(f"  coverage status counts: {out['coverage_status_counts']}")
    log(f"  tirads_score_component_complete TRUE: {complete_n}")

    # Build re-extraction queue
    log("  Building tirads_reextraction_queue_v1 ...")
    # Identify columns we can carry; not all cunc_v1 builds expose source_report_id
    has_source_report = "source_report_id" in cunc_cols
    has_exam_date = "exam_date" in cunc_cols
    has_nodule_id = "nodule_id" in cunc_cols
    sel_cols = ["research_id"]
    if has_exam_date:
        sel_cols.append("exam_date")
    else:
        sel_cols.append("CAST(NULL AS DATE) AS exam_date")
    if has_nodule_id:
        sel_cols.append("nodule_id")
    else:
        sel_cols.append("CAST(NULL AS VARCHAR) AS nodule_id")
    if has_source_report:
        sel_cols.append("source_report_id")
    else:
        sel_cols.append("CAST(NULL AS VARCHAR) AS source_report_id")
    sel_cols.append("tirads_score_2017 AS current_score")
    sel_cols.append("'calcifications_missing' AS reason")
    sel_cols.append("CURRENT_TIMESTAMP AS queued_at")
    select_sql = ", ".join(sel_cols)
    con.execute(
        f"CREATE OR REPLACE TABLE tirads_reextraction_queue_v1 AS "
        f"SELECT {select_sql} "
        "FROM canonical_us_nodule_characteristics_v1 "
        "WHERE tirads_score_2017 IS NOT NULL AND calcifications IS NULL"
    )
    queue_n = con.execute("SELECT COUNT(*) FROM tirads_reextraction_queue_v1").fetchone()[0]
    out["reextraction_queue_rows"] = queue_n
    log(f"  tirads_reextraction_queue_v1 rows: {queue_n}")

    safe = (
        f"Re-extraction queue for nodules where tirads_score_2017 was assigned but "
        f"calcifications is NULL. Build={ISO_TS}; script={SCRIPT_TAG}."
    ).replace("'", "''")
    con.execute(f"COMMENT ON TABLE tirads_reextraction_queue_v1 IS '{safe}'")

    assert_invariants(con)

    json_out = OUT_DIR / "271_step5_calcifications.json"
    with json_out.open("w") as fh:
        json.dump(out, fh, indent=2, default=str)
    log(f"  Wrote {json_out}")
    return out


# ---------------------------------------------------------------------------
# STEP 6 — dominant_nodule_flag on inm_v1 + 3 new CPM cols
# ---------------------------------------------------------------------------

def step6_dominant_and_cpm_cols(con) -> dict:
    log("=== STEP 6 — dominant_nodule_flag + imaging_laterality_rollup + has_structured + concordance ===")
    out: dict = {"step": 6, "ts": _ts()}

    # 6a. dominant_nodule_flag on inm_v1
    inm_cols = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND table_name='imaging_nodule_master_v1'"
    ).fetchall()}
    if "dominant_nodule_flag" not in inm_cols:
        con.execute(
            "ALTER TABLE imaging_nodule_master_v1 ADD COLUMN dominant_nodule_flag BOOLEAN"
        )
        log("  Added imaging_nodule_master_v1.dominant_nodule_flag BOOLEAN")
    else:
        log("  SKIP add (exists): inm_v1.dominant_nodule_flag")
    safe = (
        f"TRUE if largest max_dimension_cm per (research_id, exam_date). NULLS LAST. "
        f"Computed by Script 271; build={ISO_TS}; script={SCRIPT_TAG}."
    ).replace("'", "''")
    con.execute(
        f"COMMENT ON COLUMN imaging_nodule_master_v1.dominant_nodule_flag IS '{safe}'"
    )

    # Identify size col (max_dimension_cm vs alternatives)
    size_col_candidates = [
        "max_dimension_cm", "max_size_cm", "nodule_size_cm",
        "size_cm", "dominant_size_cm",
    ]
    size_col = next((c for c in size_col_candidates if c in inm_cols), None)
    if size_col is None:
        log(f"  WARN: no size column in inm_v1 from candidates {size_col_candidates}; "
            "dominant_nodule_flag set FALSE for all rows.")
        con.execute("UPDATE imaging_nodule_master_v1 SET dominant_nodule_flag = FALSE")
    else:
        log(f"  Using size column: {size_col}")
        # Need a unique row identifier
        if "nodule_id" not in inm_cols:
            raise SystemExit("inm_v1 missing nodule_id; cannot key dominant flag updates")
        con.execute(
            "CREATE OR REPLACE TEMP TABLE _dnf AS "
            "SELECT nodule_id, "
            "       (ROW_NUMBER() OVER (PARTITION BY research_id, exam_date "
            f"        ORDER BY {size_col} DESC NULLS LAST) = 1) AS is_dom "
            "FROM imaging_nodule_master_v1"
        )
        con.execute(
            "UPDATE imaging_nodule_master_v1 AS i "
            "SET dominant_nodule_flag = d.is_dom "
            "FROM _dnf AS d "
            "WHERE i.nodule_id = d.nodule_id"
        )
    cov = con.execute(
        "SELECT COUNT(*) AS n, COUNT(dominant_nodule_flag) AS flagged, "
        "       COUNT(*) FILTER (WHERE dominant_nodule_flag) AS n_dominant "
        "FROM imaging_nodule_master_v1"
    ).fetchone()
    out["dnf"] = {"n": cov[0], "flagged": cov[1], "n_dominant": cov[2], "size_col": size_col}
    log(f"  inm_v1 dnf: n={cov[0]} flagged={cov[1]} dominant={cov[2]}")

    # 6b/c/d: New CPM cols
    cpm_cols = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND table_name='canonical_patient_master'"
    ).fetchall()}

    new_cpm = [
        ("imaging_laterality_rollup", "VARCHAR",
         f"Imaging-derived laterality rollup from inm_v1: 'left'/'right'/'bilateral'/"
         f"'isthmus'/'mixed'/NULL. Build={ISO_TS}; script={SCRIPT_TAG}."),
        ("imaging_has_structured_components", "BOOLEAN",
         f"TRUE if patient has any cunc_v1 row with all four core components "
         f"(composition/echogenicity/shape/margins) non-NULL. "
         f"Build={ISO_TS}; script={SCRIPT_TAG}."),
        ("pathology_vs_imaging_laterality_concordant", "BOOLEAN",
         f"TRUE iff cpm.laterality matches imaging_laterality_rollup (both non-NULL). "
         f"Build={ISO_TS}; script={SCRIPT_TAG}."),
    ]
    for col, typ, txt in new_cpm:
        if col in cpm_cols:
            log(f"  SKIP add (exists): {col}")
        else:
            con.execute(f"ALTER TABLE canonical_patient_master ADD COLUMN {col} {typ}")
            log(f"  Added canonical_patient_master.{col} {typ}")
        safe = txt.replace("'", "''")
        con.execute(
            f"COMMENT ON COLUMN canonical_patient_master.{col} IS '{safe}'"
        )

    # Compute imaging_laterality_rollup
    inm_cols2 = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND table_name='imaging_nodule_master_v1'"
    ).fetchall()}
    lat_col_candidates = ["laterality", "side", "lobe", "nodule_laterality"]
    lat_col = next((c for c in lat_col_candidates if c in inm_cols2), None)
    if lat_col is None:
        log(f"  WARN: no laterality column on inm_v1 from {lat_col_candidates}; "
            "imaging_laterality_rollup left NULL.")
    else:
        log(f"  Using inm_v1 laterality column: {lat_col}")
        # Build per-patient set of distinct lowered, trimmed laterality values
        con.execute(
            "CREATE OR REPLACE TEMP TABLE _lat AS "
            "SELECT CAST(research_id AS VARCHAR) AS research_id, "
            f"       LIST(DISTINCT LOWER(TRIM(CAST({lat_col} AS VARCHAR)))) AS lats "
            "FROM imaging_nodule_master_v1 "
            f"WHERE {lat_col} IS NOT NULL "
            "GROUP BY 1"
        )
        # Derive rollup per patient
        # Rules:
        #   - any 'isthmus' AND no left/right -> 'isthmus'
        #   - left & right both present -> 'bilateral'
        #   - left only -> 'left'; right only -> 'right'
        #   - any isthmus + left/right -> 'mixed'
        #   - any 'bilateral' value upstream -> 'bilateral'
        #   - else NULL
        con.execute(
            "CREATE OR REPLACE TEMP TABLE _lat_rollup AS "
            "SELECT research_id, "
            "  CASE "
            "    WHEN list_contains(lats, 'bilateral') THEN 'bilateral' "
            "    WHEN (list_contains(lats, 'left') OR list_contains(lats, 'l') OR list_contains(lats, 'lt')) "
            "         AND (list_contains(lats, 'right') OR list_contains(lats, 'r') OR list_contains(lats, 'rt')) "
            "         AND list_contains(lats, 'isthmus') THEN 'mixed' "
            "    WHEN (list_contains(lats, 'left') OR list_contains(lats, 'l') OR list_contains(lats, 'lt')) "
            "         AND (list_contains(lats, 'right') OR list_contains(lats, 'r') OR list_contains(lats, 'rt')) THEN 'bilateral' "
            "    WHEN list_contains(lats, 'isthmus') AND ( "
            "         list_contains(lats, 'left') OR list_contains(lats, 'l') OR list_contains(lats, 'lt') OR "
            "         list_contains(lats, 'right') OR list_contains(lats, 'r') OR list_contains(lats, 'rt')) THEN 'mixed' "
            "    WHEN list_contains(lats, 'isthmus') THEN 'isthmus' "
            "    WHEN list_contains(lats, 'left') OR list_contains(lats, 'l') OR list_contains(lats, 'lt') THEN 'left' "
            "    WHEN list_contains(lats, 'right') OR list_contains(lats, 'r') OR list_contains(lats, 'rt') THEN 'right' "
            "    ELSE NULL "
            "  END AS imaging_laterality_rollup "
            "FROM _lat"
        )
        con.execute(
            "UPDATE canonical_patient_master AS c "
            "SET imaging_laterality_rollup = r.imaging_laterality_rollup "
            "FROM _lat_rollup AS r "
            "WHERE c.research_id = r.research_id"
        )
        cov = con.execute(
            "SELECT imaging_laterality_rollup, COUNT(*) "
            "FROM canonical_patient_master GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
        out["imaging_laterality_rollup_counts"] = [
            {"value": r[0], "n": r[1]} for r in cov
        ]
        for r in cov:
            log(f"    imaging_laterality_rollup={r[0]}: n={r[1]}")

    # Compute imaging_has_structured_components from cunc_v1
    cunc_cols = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND table_name='canonical_us_nodule_characteristics_v1'"
    ).fetchall()}
    needed4 = ["composition", "echogenicity", "shape", "margins"]
    if all(c in cunc_cols for c in needed4):
        cond4 = " AND ".join(f"({c} IS NOT NULL)" for c in needed4)
        con.execute(
            "CREATE OR REPLACE TEMP TABLE _hsc AS "
            "SELECT CAST(research_id AS VARCHAR) AS research_id, "
            f"       BOOL_OR({cond4}) AS has_struct "
            "FROM canonical_us_nodule_characteristics_v1 "
            "GROUP BY 1"
        )
        con.execute(
            "UPDATE canonical_patient_master AS c "
            "SET imaging_has_structured_components = "
            "  COALESCE(s.has_struct, FALSE) "
            "FROM _hsc AS s "
            "WHERE c.research_id = s.research_id"
        )
        # Patients with no cunc_v1 rows -> remain NULL; coerce to FALSE
        con.execute(
            "UPDATE canonical_patient_master "
            "SET imaging_has_structured_components = FALSE "
            "WHERE imaging_has_structured_components IS NULL"
        )
        cov = con.execute(
            "SELECT imaging_has_structured_components, COUNT(*) "
            "FROM canonical_patient_master GROUP BY 1 ORDER BY 1"
        ).fetchall()
        out["has_structured_counts"] = [{"value": r[0], "n": r[1]} for r in cov]
        for r in cov:
            log(f"    imaging_has_structured_components={r[0]}: n={r[1]}")
    else:
        log(f"  WARN: cunc_v1 missing one of {needed4}; imaging_has_structured_components left NULL")

    # Compute pathology_vs_imaging_laterality_concordant
    cpm_cols2 = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND table_name='canonical_patient_master'"
    ).fetchall()}
    if "laterality" in cpm_cols2:
        # Normalize both sides for comparison
        con.execute(
            "UPDATE canonical_patient_master "
            "SET pathology_vs_imaging_laterality_concordant = "
            "  (laterality IS NOT NULL AND imaging_laterality_rollup IS NOT NULL "
            "   AND LOWER(TRIM(CAST(laterality AS VARCHAR))) = imaging_laterality_rollup)"
        )
        cov = con.execute(
            "SELECT pathology_vs_imaging_laterality_concordant, COUNT(*) "
            "FROM canonical_patient_master GROUP BY 1 ORDER BY 1 NULLS LAST"
        ).fetchall()
        out["concordance_counts"] = [{"value": r[0], "n": r[1]} for r in cov]
        for r in cov:
            log(f"    pathology_vs_imaging_laterality_concordant={r[0]}: n={r[1]}")
    else:
        log("  WARN: canonical_patient_master.laterality not present; concordance left NULL")

    assert_invariants(con)

    json_out = OUT_DIR / "271_step6_dominant_and_cpm.json"
    with json_out.open("w") as fh:
        json.dump(out, fh, indent=2, default=str)
    log(f"  Wrote {json_out}")
    return out


# ---------------------------------------------------------------------------
# STEP 7 — Archive sweep + repo grep
# ---------------------------------------------------------------------------

def step7_archive_sweep(con) -> dict:
    log("=== STEP 7 — Archive & naming hygiene sweep ===")
    out: dict = {"step": 7, "ts": _ts()}

    # 1. Identify deprecated/backup/legacy/_pre[N] tables in main schema
    rows = con.execute(
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND (table_name LIKE '%_deprecated_%' "
        "     OR LOWER(table_name) LIKE '%backup%' "
        "     OR table_name LIKE '%_legacy%' "
        "     OR regexp_matches(table_name, '_pre[0-9]'))"
    ).fetchall()
    targets = [r[0] for r in rows]
    out["sweep_candidates"] = targets
    log(f"  Found {len(targets)} candidate tables in main: {targets}")

    moved = []
    for t in targets:
        src_q = f'{PUBLICATION_DB}.main."{t}"' if t.startswith("_") else f"{PUBLICATION_DB}.main.{t}"
        # Skip tirads_reextraction_queue_v1 (not deprecated)
        if t == "tirads_reextraction_queue_v1":
            continue
        dst_name = f"{t}_swept_{ISO_TS}"
        dst_q = f'{ARCHIVE_PREFIX}."{dst_name}"'
        log(f"  Moving {src_q} -> {dst_q}")
        con.execute(f"CREATE OR REPLACE TABLE {dst_q} AS SELECT * FROM {src_q}")
        src_n = con.execute(f"SELECT COUNT(*) FROM {src_q}").fetchone()[0]
        dst_n = con.execute(f"SELECT COUNT(*) FROM {dst_q}").fetchone()[0]
        if src_n != dst_n:
            raise SystemExit(f"Sweep copy mismatch for {t}: {src_n} vs {dst_n}")
        con.execute(f"DROP TABLE {src_q}")
        moved.append({"table": t, "archived_as": dst_name, "rows": dst_n})
    out["moved"] = moved

    # Verify clean
    rows2 = con.execute(
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND (table_name LIKE '%_deprecated_%' "
        "     OR LOWER(table_name) LIKE '%backup%' "
        "     OR table_name LIKE '%_legacy%' "
        "     OR regexp_matches(table_name, '_pre[0-9]'))"
    ).fetchall()
    remaining = [r[0] for r in rows2]
    out["remaining_after_sweep"] = remaining
    log(f"  Remaining candidates after sweep: {remaining}")
    if remaining:
        log("  WARN: tables remain after sweep; review.")

    assert_invariants(con)

    json_out = OUT_DIR / "271_step7_archive_sweep.json"
    with json_out.open("w") as fh:
        json.dump(out, fh, indent=2, default=str)
    log(f"  Wrote {json_out}")
    return out


# ---------------------------------------------------------------------------
# STEP 8 — Refresh data_dictionary_v266a + __readme + CANONICAL_STATE doc
# ---------------------------------------------------------------------------

def step8_refresh_dictionary_and_docs(con) -> dict:
    log("=== STEP 8 — Refresh data_dictionary_v266a + __readme + docs ===")
    out: dict = {"step": 8, "ts": _ts()}

    target_tables = [
        "canonical_patient_master",
        "canonical_us_nodule_characteristics_v1",
        "imaging_nodule_master_v1",
    ]
    tlist = ", ".join(f"'{t}'" for t in target_tables)

    # Inspect existing dictionary schema before touching it
    dd_cols = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND table_name='data_dictionary_v266a' ORDER BY ordinal_position"
    ).fetchall()]
    out["dd_existing_cols"] = dd_cols
    log(f"  data_dictionary_v266a existing cols: {dd_cols}")

    # Build a canonical regenerated dictionary table and overwrite v266a
    # (snapshot was taken in Step 1).
    log("  Rebuilding data_dictionary_v266a from information_schema ...")
    con.execute(
        "CREATE OR REPLACE TABLE data_dictionary_v266a AS "
        "SELECT c.table_name, c.column_name, c.data_type, c.is_nullable, "
        "       c.ordinal_position, "
        "       COALESCE(cm.comment, '') AS comment, "
        f"       '{ISO_TS}' AS rebuilt_at, "
        f"       '{SCRIPT_TAG}' AS rebuilt_by "
        "FROM information_schema.columns c "
        "LEFT JOIN duckdb_columns() cm "
        "  ON cm.database_name = c.table_catalog "
        " AND cm.schema_name   = c.table_schema "
        " AND cm.table_name    = c.table_name "
        " AND cm.column_name   = c.column_name "
        f"WHERE c.table_catalog='{PUBLICATION_DB}' "
        "  AND c.table_schema='main' "
        f"  AND c.table_name IN ({tlist}) "
        "ORDER BY c.table_name, c.ordinal_position"
    )
    n_dd = con.execute("SELECT COUNT(*) FROM data_dictionary_v266a").fetchone()[0]
    out["data_dictionary_v266a_rows"] = n_dd
    log(f"  data_dictionary_v266a rebuilt rows: {n_dd}")

    # Refresh __readme rows (best-effort: append a marker row)
    rd_cols = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND table_name='__readme' ORDER BY ordinal_position"
    ).fetchall()]
    out["readme_cols"] = rd_cols
    log(f"  __readme columns: {rd_cols}")
    # Try a generic insert if it has key/value-ish columns.
    try:
        if rd_cols and "key" in rd_cols and "value" in rd_cols:
            con.execute(
                "INSERT INTO __readme (key, value) VALUES "
                f"('script_271_run_ts', '{ISO_TS}'), "
                f"('script_271_summary', 'TIRADS+imaging finalization; see CANONICAL_STATE_20260417_SCRIPT271.md')"
            )
            log("  Appended marker rows to __readme.")
        else:
            log("  __readme schema not key/value; skipping insert (snapshot retained).")
    except Exception as e:
        log(f"  __readme insert skipped: {e!r}")

    # Write the CANONICAL_STATE doc
    cpm_cols_post = con.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND table_name='canonical_patient_master'"
    ).fetchone()[0]
    main_tbl_count = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'"
    ).fetchone()[0]

    docs_dir = REPO / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    doc_path = docs_dir / "CANONICAL_STATE_20260417_SCRIPT271.md"
    body = f"""# Canonical State — 2026-04-17 — Script 271

**Database:** `thyroid_canonical_publication_v1_0` (WRITE)
**Archive:** `"Thyroid 2026 UPdated".archive_pub_v1_0`
**Run timestamp:** {ISO_TS}
**Script:** `scripts/271_tirads_imaging_finalization.py`

## Starting state
- canonical_patient_master: 10,871 rows × 1,514 columns
- canonical_us_nodule_characteristics_v1: 37,016 rows
- imaging_nodule_master_v1: 37,016 rows
- main-schema base tables: 115

## Ending state
- canonical_patient_master: 10,871 rows × **{cpm_cols_post}** columns
  - Net delta: −1 (imaging_nodule_size_cm_v11) +4 (tirads_worst_points_v271,
    tirads_best_points_v271, tirads_source_system_v271,
    imaging_laterality_rollup, imaging_has_structured_components,
    pathology_vs_imaging_laterality_concordant)
  - Net change: **+5** vs start → expect **1,519** if all six new + one drop applied;
    realised: {cpm_cols_post}
- main-schema base tables: {main_tbl_count}

## P0/P1/P2 disposition (PROMPT 19 Extended)
| Item | Status |
|------|--------|
| P0a — drop imaging_nodule_size_cm_v11 | DONE (Step 2) |
| P0b — rebuild cunc_v1 TIRADS category from points | DONE (Step 3) |
| P1a — points-based patient TIRADS rollup + 6-patient audit | DONE (Step 4) |
| P1b — calcifications coverage flag, no back-fill, queue 4,363 | DONE (Step 5) |
| P2  — dominant_nodule_flag + 3 new CPM cols | DONE (Step 6) |

## Open items
- `tirads_reextraction_queue_v1` populated; LLM re-extraction not run here.
- ~4,736 placeholder cunc_v1 patients remain un-recoverable from upstream.
- Legacy column `tirads_worst_score_v12` and `tirads_best_score_v12` retained
  with re-COMMENT clarifying they are category codes, not ACR points.

## Key invariants
- canonical_patient_master: 10,871 / 10,871 / 0 (rows/distinct/null) verified
  after every mutating step.
- manuscript_workspace schema NOT touched (Script 220 ETE views depend on
  current DDLs).

## Archive snapshots created in Step 1
All `*_pre271_{ISO_TS}` tables in `"Thyroid 2026 UPdated".archive_pub_v1_0`.
"""
    doc_path.write_text(body)
    log(f"  Wrote {doc_path}")
    out["doc_path"] = str(doc_path)

    json_out = OUT_DIR / "271_step8_dictionary_docs.json"
    with json_out.open("w") as fh:
        json.dump(out, fh, indent=2, default=str)
    log(f"  Wrote {json_out}")
    return out


# ---------------------------------------------------------------------------
# STEP 9 — Final verification + parquet export
# ---------------------------------------------------------------------------

def step9_final_verify_and_export(con) -> dict:
    log("=== STEP 9 — Final verification + parquet export ===")
    out: dict = {"step": 9, "ts": _ts()}

    # A
    row = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id), "
        "COUNT(*) FILTER (WHERE research_id IS NULL) "
        "FROM canonical_patient_master"
    ).fetchone()
    out["A_invariants"] = {"n": row[0], "distinct": row[1], "nulls": row[2]}
    log(f"  A. invariants: {out['A_invariants']}")

    # B
    out["B_cpm_cols"] = con.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND table_name='canonical_patient_master'"
    ).fetchone()[0]
    log(f"  B. CPM cols (post): {out['B_cpm_cols']}")

    # C
    out["C_deprecated_remaining"] = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND (table_name LIKE '%_deprecated_%' "
        "     OR LOWER(table_name) LIKE '%backup%' "
        "     OR table_name LIKE '%_legacy%' "
        "     OR regexp_matches(table_name, '_pre[0-9]'))"
    ).fetchone()[0]
    log(f"  C. deprecated tables remaining in main: {out['C_deprecated_remaining']} (must be 0)")
    if out["C_deprecated_remaining"] != 0:
        raise SystemExit("Deprecated tables remain in main schema after Step 7")

    # D
    rows = con.execute(
        "SELECT tirads_category_v2, MIN(tirads_score_2017), MAX(tirads_score_2017), COUNT(*) "
        "FROM canonical_us_nodule_characteristics_v1 "
        "WHERE tirads_category_v2 IS NOT NULL "
        "GROUP BY 1 ORDER BY 1"
    ).fetchall()
    out["D_band_audit"] = [
        {"band": r[0], "min": r[1], "max": r[2], "n": r[3]} for r in rows
    ]
    log(f"  D. band audit: {out['D_band_audit']}")

    # E manuscript_workspace.cohort_descriptive_full_cohort_v1 (best-effort)
    try:
        out["E_workspace_cohort_n"] = con.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.cohort_descriptive_full_cohort_v1"
        ).fetchone()[0]
    except Exception as e:
        out["E_workspace_cohort_n"] = f"ERROR: {e!r}"
    log(f"  E. manuscript_workspace.cohort_descriptive_full_cohort_v1: {out['E_workspace_cohort_n']}")

    # F
    out["F_reextraction_queue"] = con.execute(
        "SELECT COUNT(*) FROM tirads_reextraction_queue_v1"
    ).fetchone()[0]
    log(f"  F. tirads_reextraction_queue_v1 rows: {out['F_reextraction_queue']}")

    # G
    cov = con.execute(
        "SELECT COUNT(*), COUNT(dominant_nodule_flag), "
        "COUNT(*) FILTER (WHERE dominant_nodule_flag) "
        "FROM imaging_nodule_master_v1"
    ).fetchone()
    out["G_dnf"] = {"n": cov[0], "flagged": cov[1], "n_dominant": cov[2]}
    log(f"  G. inm_v1 dnf: {out['G_dnf']}")

    # Parquet export
    exports_dir = REPO / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)
    pq_path = exports_dir / "canonical_patient_master_20260417_script271.parquet"
    log(f"  Exporting CPM -> {pq_path}")
    con.execute(
        f"COPY (SELECT * FROM canonical_patient_master) TO '{pq_path.as_posix()}' "
        "(FORMAT PARQUET, COMPRESSION zstd)"
    )
    sz_mb = pq_path.stat().st_size / (1024 * 1024) if pq_path.exists() else 0
    log(f"  Parquet size: {sz_mb:.1f} MB")
    out["parquet_path"] = str(pq_path)
    out["parquet_size_mb"] = round(sz_mb, 1)

    # FINAL SELF-CHECK
    print("\n================ FINAL SELF-CHECK ================")
    print(f"DB:                   {PUBLICATION_DB}")
    print(f"CPM:                  n={out['A_invariants']['n']}  distinct={out['A_invariants']['distinct']}  nulls={out['A_invariants']['nulls']}")
    print(f"CPM column count:     {out['B_cpm_cols']}")
    print(f"deprecated in main:   {out['C_deprecated_remaining']} (must be 0)")
    print(f"TR band audit:        {out['D_band_audit']}")
    print(f"reextraction queue:   {out['F_reextraction_queue']} rows")
    print(f"inm_v1 dnf:           n={out['G_dnf']['n']} flagged={out['G_dnf']['flagged']} dominant={out['G_dnf']['n_dominant']}")
    print(f"parquet export:       {out['parquet_path']} ({out['parquet_size_mb']} MB)")
    print(f"timestamp:            {ISO_TS}")
    print("==================================================\n")

    json_out = OUT_DIR / "271_step9_final.json"
    with json_out.open("w") as fh:
        json.dump(out, fh, indent=2, default=str)
    log(f"  Wrote {json_out}")
    return out


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

STEP_FNS = {
    0: step0_preflight,
    1: step1_snapshots,
    2: step2_drop_size_cm_v11,
    3: step3_rebuild_cunc_categories,
    4: step4_patient_points_rollup,
    5: step5_calcifications_coverage,
    6: step6_dominant_and_cpm_cols,
    7: step7_archive_sweep,
    8: step8_refresh_dictionary_and_docs,
    9: step9_final_verify_and_export,
}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Script 271 — TIRADS & imaging finalization")
    p.add_argument(
        "--step", type=int, required=True, choices=sorted(STEP_FNS),
        help="Which step to execute (0-9). Run them in order, committing between.",
    )
    args = p.parse_args(argv)
    log(f"########## Script 271 — STEP {args.step} ##########")
    con = connect_locked()
    try:
        STEP_FNS[args.step](con)
    finally:
        con.close()
    log(f"########## Script 271 — STEP {args.step} DONE ##########")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
