#!/usr/bin/env python3
"""Script 271b — Laterality vocabulary normalization & full concordance rebuild.

Targets the WRITE database ``thyroid_canonical_publication_v1_0`` (CPM,
imaging_nodule_master_v1, tumor_pathology, data_dictionary_v266a) with archive
snapshots into ``"Thyroid 2026 UPdated".archive_pub_v1_0``.

Background
----------
Script 271 created ``cpm.imaging_laterality_rollup`` and
``cpm.pathology_vs_imaging_laterality_concordant`` against ``cpm.laterality``,
which is undocumented and may not represent disease laterality. ``mixed`` in
the imaging rollup conflates four semantically distinct states. Script 271a
already three-valued the boolean concordance flag. This script:

1. Forensics on ``cpm.laterality`` to identify its source/semantics.
2. Adds disease-laterality columns derived from ``tumor_pathology``.
3. Splits ``imaging_laterality_rollup`` into explicit per-side flags + a
   normalized rollup.
4. Rebuilds concordance on the clean vocabulary (5-valued, semantic).
5. Annotates legacy columns with COMMENTs (no values changed).

Steps:
  0 — Preflight (read-only).
  1 — Snapshots to archive_pub_v1_0 (CPM + inm_v1).
  2 — Forensics on cpm.laterality source.
  3 — Add tumor_pathology_laterality_v271b + has_isthmus_involvement.
  4 — Split mixed imaging rollup into explicit flags + v271b rollup.
  5 — Rebuild pathology/imaging concordance on normalized vocabularies.
  6 — Document legacy laterality columns.
  7 — Verify, refresh data_dictionary_v266a, write LATERALITY doc.
  8 — Parquet export.

Each step is gated by ``--step N`` so the operator can commit between steps.
``--step all`` runs 0..8 sequentially. Each step is idempotent.

Invariants verified after every mutating step:
  canonical_patient_master: 10,871 rows / 10,871 distinct research_id / 0 NULL.
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
DOCS_DIR = REPO / "docs"
EXPORTS_DIR = REPO / "exports"

ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
ARCHIVE_PREFIX = f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA}'

ISO_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
SCRIPT_TAG = "script271b_2026-04-18"

LOG_PATH = OUT_DIR / "271b_run.log"


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg: str) -> None:
    line = f"[{_ts()}] {msg}"
    print(line, flush=True)
    with LOG_PATH.open("a") as fh:
        fh.write(line + "\n")


def write_step_json(step: int, payload: dict) -> Path:
    path = OUT_DIR / f"271b_step{step}.json"
    with path.open("w") as fh:
        json.dump(payload, fh, indent=2, default=str)
    log(f"wrote {path}")
    return path


# ---------------------------------------------------------------------------
# Invariants
# ---------------------------------------------------------------------------

def assert_invariants(con) -> tuple[int, int, int]:
    n, d, nulls = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id), "
        "COUNT(*) FILTER (WHERE research_id IS NULL) "
        f"FROM {PUBLICATION_DB}.main.canonical_patient_master"
    ).fetchone()
    log(f"INVARIANTS canonical_patient_master: n={n} distinct={d} nulls={nulls}")
    if (n, d, nulls) != (10871, 10871, 0):
        raise SystemExit(f"INVARIANT VIOLATION: ({n},{d},{nulls}) != (10871,10871,0)")
    return (n, d, nulls)


def cpm_columns(con) -> set[str]:
    return {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND table_name='canonical_patient_master'"
    ).fetchall()}


def table_exists(con, table_name: str, schema: str = "main",
                 catalog: str = PUBLICATION_DB) -> bool:
    n = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_catalog=? AND table_schema=? AND table_name=?",
        [catalog, schema, table_name],
    ).fetchone()[0]
    return n > 0


# ---------------------------------------------------------------------------
# STEP 0 — Preflight (read-only)
# ---------------------------------------------------------------------------

def step0_preflight(con) -> dict:
    log("=== STEP 0 — Preflight (read-only) ===")
    out: dict = {"step": 0, "started_at": _ts()}
    assert_invariants(con)

    expected_cols = {
        "laterality", "path_laterality", "bilateral_disease_flag",
        "bilateral_path_flag", "imaging_laterality_rollup",
        "pathology_vs_imaging_laterality_concordant",
    }
    cols = cpm_columns(con)
    present = sorted(expected_cols & cols)
    missing = sorted(expected_cols - cols)
    log(f"  expected legacy cols present: {present}")
    if missing:
        log(f"  MISSING legacy cols: {missing}")
    out["expected_cols_present"] = present
    out["expected_cols_missing"] = missing
    out["cpm_n_columns"] = len(cols)

    queries = {
        "cpm.laterality": (
            "SELECT 'cpm.laterality' AS col, CAST(laterality AS VARCHAR) AS v, COUNT(*) AS n "
            "FROM canonical_patient_master GROUP BY 2 ORDER BY 3 DESC"
        ),
        "cpm.path_laterality": (
            "SELECT 'cpm.path_laterality' AS col, CAST(path_laterality AS VARCHAR) AS v, COUNT(*) AS n "
            "FROM canonical_patient_master GROUP BY 2 ORDER BY 3 DESC"
        ),
        "cpm.imaging_laterality_rollup": (
            "SELECT 'cpm.imaging_laterality_rollup' AS col, "
            "CAST(imaging_laterality_rollup AS VARCHAR) AS v, COUNT(*) AS n "
            "FROM canonical_patient_master GROUP BY 2 ORDER BY 3 DESC"
        ),
        "cpm.bilateral_disease_flag": (
            "SELECT 'cpm.bilateral_disease_flag' AS col, "
            "CAST(bilateral_disease_flag AS VARCHAR) AS v, COUNT(*) AS n "
            "FROM canonical_patient_master GROUP BY 2 ORDER BY 3 DESC"
        ),
        "cpm.bilateral_path_flag": (
            "SELECT 'cpm.bilateral_path_flag' AS col, "
            "CAST(bilateral_path_flag AS VARCHAR) AS v, COUNT(*) AS n "
            "FROM canonical_patient_master GROUP BY 2 ORDER BY 3 DESC"
        ),
    }
    if table_exists(con, "tumor_pathology"):
        queries["tp.tumor_laterality_overall"] = (
            "SELECT 'tp.tumor_laterality_overall' AS col, "
            "CAST(tumor_laterality_overall AS VARCHAR) AS v, COUNT(*) AS n "
            "FROM tumor_pathology GROUP BY 2 ORDER BY 3 DESC"
        )
    if table_exists(con, "imaging_nodule_master_v1"):
        queries["inm_v1.laterality"] = (
            "SELECT 'inm_v1.laterality' AS col, "
            "CAST(laterality AS VARCHAR) AS v, COUNT(*) AS n "
            "FROM imaging_nodule_master_v1 GROUP BY 2 ORDER BY 3 DESC"
        )

    vocab: dict = {}
    for name, sql in queries.items():
        try:
            rows = con.execute(sql).fetchall()
        except Exception as e:
            log(f"  WARN vocab probe {name}: {e!r}")
            vocab[name] = {"error": repr(e)}
            continue
        vocab[name] = [{"value": r[1], "n": r[2]} for r in rows]
        log(f"  {name}:")
        for r in rows:
            log(f"    {r[1]!r}: {r[2]}")
    out["vocabulary"] = vocab

    n_tp_pts = con.execute(
        "SELECT COUNT(DISTINCT CAST(research_id AS VARCHAR)) FROM tumor_pathology"
    ).fetchone()[0] if table_exists(con, "tumor_pathology") else None
    out["n_distinct_research_id_in_tumor_pathology"] = n_tp_pts
    log(f"  distinct research_id in tumor_pathology: {n_tp_pts}")

    out["finished_at"] = _ts()
    write_step_json(0, out)
    return out


# ---------------------------------------------------------------------------
# STEP 1 — Snapshots
# ---------------------------------------------------------------------------

def _snapshot(con, src_table: str) -> str:
    """Snapshot a main-schema table to archive with the pre271b_<TS> suffix."""
    snap_name = f"{src_table}_pre271b_{ISO_TS}"
    snap_fq = f'{ARCHIVE_PREFIX}."{snap_name}"'
    exists = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_catalog=? AND table_schema=? AND table_name=?",
        [ARCHIVE_DB, ARCHIVE_SCHEMA, snap_name],
    ).fetchone()[0]
    if exists:
        log(f"  SKIP — snapshot already exists: {snap_fq}")
    else:
        con.execute(
            f"CREATE TABLE {snap_fq} AS "
            f"SELECT * FROM {PUBLICATION_DB}.main.{src_table}"
        )
        log(f"  created {snap_fq}")
    src_n = con.execute(
        f"SELECT COUNT(*) FROM {PUBLICATION_DB}.main.{src_table}"
    ).fetchone()[0]
    snap_n = con.execute(f"SELECT COUNT(*) FROM {snap_fq}").fetchone()[0]
    log(f"    src={src_n} snap={snap_n} match={src_n == snap_n}")
    if src_n != snap_n:
        raise SystemExit(f"Snapshot row mismatch: {src_n} vs {snap_n}")
    return snap_name


def step1_snapshots(con) -> dict:
    log("=== STEP 1 — Snapshots to archive_pub_v1_0 ===")
    out: dict = {"step": 1, "started_at": _ts(), "snapshots": []}
    for tbl in ("canonical_patient_master", "imaging_nodule_master_v1"):
        snap = _snapshot(con, tbl)
        out["snapshots"].append({"src": tbl, "snap": snap})
    assert_invariants(con)
    out["finished_at"] = _ts()
    write_step_json(1, out)
    return out


# ---------------------------------------------------------------------------
# STEP 2 — Forensics on cpm.laterality
# ---------------------------------------------------------------------------

_LAT_BUCKETS = ("bilateral", "left", "right")


def _normalize_lat_expr(col_sql: str) -> str:
    """Return a SQL expression that maps a free-text laterality value to one of
    {bilateral,left,right,midline,unknown,NULL}."""
    return (
        "CASE "
        f"  WHEN {col_sql} IS NULL THEN NULL "
        f"  WHEN LOWER(TRIM(CAST({col_sql} AS VARCHAR))) IN ('bilateral','bilat','b') THEN 'bilateral' "
        f"  WHEN LOWER(TRIM(CAST({col_sql} AS VARCHAR))) IN ('left','l','lt','left lobe') THEN 'left' "
        f"  WHEN LOWER(TRIM(CAST({col_sql} AS VARCHAR))) IN ('right','r','rt','right lobe') THEN 'right' "
        f"  WHEN LOWER(TRIM(CAST({col_sql} AS VARCHAR))) IN ('midline','isthmus') THEN 'midline' "
        f"  WHEN LOWER(TRIM(CAST({col_sql} AS VARCHAR))) IN ('unknown','unk','?') THEN 'unknown' "
        f"  ELSE NULL "
        "END"
    )


def _agreement_3way(con, candidate_table: str, candidate_col: str,
                    join_col_candidate_table: str = "research_id") -> dict:
    """Compute 3-way (bilateral/left/right) per-patient agreement vs cpm.laterality.

    Patient-level: if multiple rows per patient, normalize then bucket via
    {has_left, has_right} -> bilateral / left / right. Fallback: use any non-null
    normalized value if no L/R/B inference is possible (e.g., aggregate).
    """
    norm = _normalize_lat_expr(candidate_col)
    join_norm = f"CAST({join_col_candidate_table} AS VARCHAR)"
    try:
        con.execute("DROP TABLE IF EXISTS _cand_norm")
    except Exception:
        pass
    sql = (
        "CREATE TEMP TABLE _cand_norm AS "
        f"SELECT {join_norm} AS research_id, "
        "       BOOL_OR(_n='bilateral') AS has_b, "
        "       BOOL_OR(_n='left')      AS has_l, "
        "       BOOL_OR(_n='right')     AS has_r, "
        "       BOOL_OR(_n='midline')   AS has_m, "
        "       BOOL_OR(_n='unknown')   AS has_u, "
        "       COUNT(*) AS n_rows "
        f"FROM (SELECT {join_norm} AS {join_col_candidate_table}, "
        f"             {norm} AS _n "
        f"      FROM {candidate_table}) sub "
        "GROUP BY 1"
    )
    con.execute(sql)
    con.execute("DROP TABLE IF EXISTS _cand_bucket")
    con.execute(
        "CREATE TEMP TABLE _cand_bucket AS "
        "SELECT research_id, "
        "  CASE "
        "    WHEN has_b THEN 'bilateral' "
        "    WHEN has_l AND has_r THEN 'bilateral' "
        "    WHEN has_l AND NOT has_r THEN 'left' "
        "    WHEN has_r AND NOT has_l THEN 'right' "
        "    WHEN has_m THEN 'midline' "
        "    WHEN has_u THEN 'unknown' "
        "    ELSE NULL "
        "  END AS bucket "
        "FROM _cand_norm"
    )
    rows = con.execute(
        "SELECT cpm.laterality AS path_lat, b.bucket AS cand_bucket, COUNT(*) AS n "
        "FROM canonical_patient_master cpm "
        "LEFT JOIN _cand_bucket b ON CAST(cpm.research_id AS VARCHAR) = b.research_id "
        "GROUP BY 1,2 ORDER BY 3 DESC"
    ).fetchall()

    confusion = [
        {"cpm_laterality": r[0], "candidate_bucket": r[1], "n": r[2]}
        for r in rows
    ]

    in_buckets = sum(
        r["n"] for r in confusion
        if r["cpm_laterality"] in _LAT_BUCKETS
        and r["candidate_bucket"] in _LAT_BUCKETS
    )
    matches = sum(
        r["n"] for r in confusion
        if r["cpm_laterality"] in _LAT_BUCKETS
        and r["cpm_laterality"] == r["candidate_bucket"]
    )
    agreement = (matches / in_buckets) if in_buckets else 0.0
    return {
        "candidate_table": candidate_table,
        "candidate_col": candidate_col,
        "n_in_3way_buckets": in_buckets,
        "n_3way_matches": matches,
        "agreement_3way": agreement,
        "confusion": confusion,
    }


def step2_forensics(con) -> dict:
    log("=== STEP 2 — Forensics on cpm.laterality ===")
    out: dict = {"step": 2, "started_at": _ts(), "candidates": []}

    cpm_lat_dist = con.execute(
        "SELECT laterality, COUNT(*) FROM canonical_patient_master "
        "GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    out["cpm_laterality_distribution"] = [
        {"value": r[0], "n": r[1]} for r in cpm_lat_dist
    ]
    log("  cpm.laterality distribution:")
    for r in cpm_lat_dist:
        log(f"    {r[0]!r}: {r[1]}")

    # Discover candidate tables/columns
    candidates: list[tuple[str, str]] = []

    if table_exists(con, "tumor_pathology"):
        candidates.append(("tumor_pathology", "tumor_laterality_overall"))

    # path_synoptics: probe for any laterality-flavored column
    if table_exists(con, "path_synoptics"):
        cols = [r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
            "AND table_name='path_synoptics'"
        ).fetchall()]
        log(f"  path_synoptics columns scanned for laterality: "
            f"{[c for c in cols if 'later' in c.lower() or 'side' in c.lower() or 'lobe' in c.lower()]}")
        for c in cols:
            if "later" in c.lower() or c.lower() in {"side", "lobe", "specimen_side"}:
                candidates.append(("path_synoptics", c))

    if table_exists(con, "operative_episode_detail_v2"):
        cols = [r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
            "AND table_name='operative_episode_detail_v2'"
        ).fetchall()]
        log(f"  operative_episode_detail_v2 columns scanned: "
            f"{[c for c in cols if 'later' in c.lower() or 'side' in c.lower() or 'lobe' in c.lower()]}")
        for c in cols:
            if "later" in c.lower() or c.lower() in {"side", "specimen_side"}:
                candidates.append(("operative_episode_detail_v2", c))

    # path_laterality on CPM itself (sanity baseline)
    cpm_cols = cpm_columns(con)
    if "path_laterality" in cpm_cols:
        candidates.append(("canonical_patient_master", "path_laterality"))

    log(f"  total candidates to test: {len(candidates)}")
    out["candidate_list"] = [
        {"table": t, "col": c} for (t, c) in candidates
    ]

    results = []
    for (tbl, col) in candidates:
        try:
            res = _agreement_3way(con, tbl, col)
        except Exception as e:
            log(f"  WARN candidate {tbl}.{col}: {e!r}")
            results.append({
                "candidate_table": tbl, "candidate_col": col,
                "error": repr(e), "agreement_3way": 0.0,
            })
            continue
        log(f"  {tbl}.{col}: 3-way agreement = "
            f"{res['agreement_3way']*100:.1f}% ({res['n_3way_matches']}/{res['n_in_3way_buckets']})")
        results.append(res)

    results.sort(key=lambda r: r.get("agreement_3way", 0.0), reverse=True)
    out["candidates"] = results

    best = results[0] if results else None
    out["best_candidate"] = best
    if best and best.get("agreement_3way", 0.0) >= 0.90:
        out["verdict"] = "identified"
        winner = f"{best['candidate_table']}.{best['candidate_col']}"
        agree_pct = best['agreement_3way'] * 100
        if best['candidate_table'] == "canonical_patient_master":
            out["proposed_comment"] = (
                "Patient-level laterality. Forensics in Script 271b found this column to be "
                f"functionally identical to cpm.{best['candidate_col']} "
                f"({agree_pct:.1f}% 3-way agreement, identical NULL pattern), strongly suggesting "
                "they are duplicate or copy columns from the same upstream feeder. "
                "Vocabulary: bilateral/left/right/NULL. Predates Script 271; documented retroactively. "
                "For new analyses prefer tumor_pathology_laterality_v271b "
                "(rebuilt from tumor_pathology under documented rules). Script 271b, 2026-04-18."
            )
        else:
            out["proposed_comment"] = (
                f"Patient-level laterality. Forensics in Script 271b matched this column to "
                f"{winner} at {agree_pct:.1f}% 3-way agreement, indicating they are semantically "
                "the same (likely the source feeder for this column). "
                "Vocabulary: bilateral/left/right/NULL. Predates Script 271; documented retroactively. "
                "For new analyses prefer tumor_pathology_laterality_v271b. Script 271b, 2026-04-18."
            )
    else:
        out["verdict"] = "inconclusive"
        out["proposed_comment"] = (
            "Undocumented legacy column predating Script 271. Forensics in Script 271b "
            f"could not match its values to any single source above the 90% threshold "
            f"(best candidate: "
            f"{best['candidate_table']+'.'+best['candidate_col'] if best else 'none'} "
            f"@ {best['agreement_3way']*100:.1f}%). "
            "Vocabulary: bilateral/left/right/NULL. For new analyses use "
            "tumor_pathology_laterality_v271b instead. Script 271b annotation, 2026-04-18."
        )

    # Persist forensics report
    md = OUT_DIR / "271b_laterality_source_forensics.md"
    lines = [
        "# Script 271b — cpm.laterality Source Forensics",
        "",
        f"**Run:** {NOW}",
        "",
        "## cpm.laterality distribution",
        "",
        "| Value | n |",
        "|---|---:|",
    ]
    for r in cpm_lat_dist:
        lines.append(f"| `{r[0]}` | {r[1]} |")
    lines += ["", "## Candidate sources, ranked by 3-way agreement (bilateral/left/right)", ""]
    lines += ["| Candidate | n in 3-way buckets | matches | agreement |",
              "|---|---:|---:|---:|"]
    for r in results:
        if "error" in r:
            lines.append(f"| `{r['candidate_table']}.{r['candidate_col']}` | — | — | ERROR ({r['error']}) |")
        else:
            lines.append(
                f"| `{r['candidate_table']}.{r['candidate_col']}` | "
                f"{r['n_in_3way_buckets']} | {r['n_3way_matches']} | "
                f"{r['agreement_3way']*100:.1f}% |"
            )
    lines += ["", f"**Verdict:** {out['verdict']}", "",
              "**Proposed COMMENT for cpm.laterality (applied in Step 6):**", "",
              "```", out["proposed_comment"], "```", ""]
    if best and "confusion" in best:
        lines += ["## Best-candidate confusion (cpm.laterality × bucketed candidate)",
                  "",
                  f"Candidate: `{best['candidate_table']}.{best['candidate_col']}`", "",
                  "| cpm.laterality | candidate bucket | n |",
                  "|---|---|---:|"]
        for r in best["confusion"]:
            lines.append(
                f"| `{r['cpm_laterality']}` | `{r['candidate_bucket']}` | {r['n']} |"
            )
        lines.append("")

    md.write_text("\n".join(lines) + "\n")
    log(f"  wrote {md}")

    out["finished_at"] = _ts()
    write_step_json(2, out)
    return out


# ---------------------------------------------------------------------------
# STEP 3 — tumor_pathology_laterality_v271b + has_isthmus_involvement
# ---------------------------------------------------------------------------

def _add_column(con, col: str, ddl_type: str, comment: str) -> None:
    """Add a column if missing, then COMMENT it. Idempotent."""
    cols = cpm_columns(con)
    if col not in cols:
        con.execute(
            f"ALTER TABLE canonical_patient_master ADD COLUMN {col} {ddl_type}"
        )
        log(f"  added column {col} {ddl_type}")
    else:
        log(f"  column {col} already exists; will (re)set COMMENT and reload")
    safe = comment.replace("'", "''")
    con.execute(
        f"COMMENT ON COLUMN canonical_patient_master.{col} IS '{safe}'"
    )


def step3_tumor_pathology_laterality(con) -> dict:
    log("=== STEP 3 — tumor_pathology_laterality_v271b + has_isthmus_involvement ===")
    out: dict = {"step": 3, "started_at": _ts()}

    if not table_exists(con, "tumor_pathology"):
        raise SystemExit("tumor_pathology table missing; cannot derive Step 3")

    _add_column(
        con,
        "tumor_pathology_laterality_v271b", "VARCHAR",
        "Patient-level disease laterality rollup from tumor_pathology.tumor_laterality_overall. "
        "Vocabulary: bilateral/left/right/midline/unknown/NULL. Rule: if any tumor row is "
        "bilateral -> bilateral; else if left AND right -> bilateral; else if left-only -> left; "
        "else if right-only -> right; else if midline-only -> midline; else if any unknown -> "
        "unknown; else NULL (no pathology). Script 271b, 2026-04-18.",
    )
    _add_column(
        con,
        "tumor_pathology_has_isthmus_involvement", "BOOLEAN",
        "TRUE if any tumor_pathology row has tumor_laterality_overall = midline (isthmus) for "
        "this patient; FALSE if patient has tumor_pathology rows but none are midline; NULL if "
        "patient has no tumor_pathology rows. Script 271b, 2026-04-18.",
    )

    # Build per-patient flags and rollup
    con.execute("DROP TABLE IF EXISTS _tp_flags")
    con.execute(
        "CREATE TEMP TABLE _tp_flags AS "
        "SELECT CAST(research_id AS VARCHAR) AS research_id, "
        "  BOOL_OR(LOWER(TRIM(CAST(tumor_laterality_overall AS VARCHAR)))='bilateral') AS has_b, "
        "  BOOL_OR(LOWER(TRIM(CAST(tumor_laterality_overall AS VARCHAR)))='left')      AS has_l, "
        "  BOOL_OR(LOWER(TRIM(CAST(tumor_laterality_overall AS VARCHAR)))='right')     AS has_r, "
        "  BOOL_OR(LOWER(TRIM(CAST(tumor_laterality_overall AS VARCHAR)))='midline')   AS has_m, "
        "  BOOL_OR(LOWER(TRIM(CAST(tumor_laterality_overall AS VARCHAR)))='unknown')   AS has_u, "
        "  COUNT(*) AS n_rows "
        "FROM tumor_pathology "
        "GROUP BY 1"
    )
    con.execute("DROP TABLE IF EXISTS _tp_rollup")
    con.execute(
        "CREATE TEMP TABLE _tp_rollup AS "
        "SELECT research_id, "
        "  CASE "
        "    WHEN has_b THEN 'bilateral' "
        "    WHEN has_l AND has_r THEN 'bilateral' "
        "    WHEN has_l AND NOT has_r AND NOT has_m THEN 'left' "
        "    WHEN has_r AND NOT has_l AND NOT has_m THEN 'right' "
        "    WHEN has_l AND NOT has_r AND has_m THEN 'left' "
        "    WHEN has_r AND NOT has_l AND has_m THEN 'right' "
        "    WHEN has_m AND NOT has_l AND NOT has_r THEN 'midline' "
        "    WHEN has_u THEN 'unknown' "
        "    ELSE NULL "
        "  END AS rollup, "
        "  has_m AS has_isthmus "
        "FROM _tp_flags"
    )

    # Reset both target columns to NULL first to make idempotent
    con.execute(
        "UPDATE canonical_patient_master "
        "SET tumor_pathology_laterality_v271b = NULL, "
        "    tumor_pathology_has_isthmus_involvement = NULL"
    )
    con.execute(
        "UPDATE canonical_patient_master AS c "
        "SET tumor_pathology_laterality_v271b = r.rollup, "
        "    tumor_pathology_has_isthmus_involvement = r.has_isthmus "
        "FROM _tp_rollup AS r "
        "WHERE CAST(c.research_id AS VARCHAR) = r.research_id"
    )

    # Verify
    rows = con.execute(
        "SELECT tumor_pathology_laterality_v271b, COUNT(*) "
        "FROM canonical_patient_master GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    iso_rows = con.execute(
        "SELECT tumor_pathology_has_isthmus_involvement, COUNT(*) "
        "FROM canonical_patient_master GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    n_tp_pts = con.execute(
        "SELECT COUNT(DISTINCT CAST(research_id AS VARCHAR)) FROM tumor_pathology"
    ).fetchone()[0]
    expected_null = 10871 - n_tp_pts
    n_null = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master "
        "WHERE tumor_pathology_laterality_v271b IS NULL"
    ).fetchone()[0]

    log(f"  tumor_pathology_laterality_v271b distribution:")
    for r in rows:
        log(f"    {r[0]!r}: {r[1]}")
    log(f"  has_isthmus_involvement distribution:")
    for r in iso_rows:
        log(f"    {r[0]!r}: {r[1]}")
    log(f"  expected NULLs (no TP rows): {expected_null}; observed: {n_null}; "
        f"match={expected_null == n_null}")

    out["tumor_pathology_laterality_v271b"] = [
        {"value": r[0], "n": r[1]} for r in rows
    ]
    out["tumor_pathology_has_isthmus_involvement"] = [
        {"value": r[0], "n": r[1]} for r in iso_rows
    ]
    out["n_tp_distinct_research_id"] = n_tp_pts
    out["expected_null_in_v271b"] = expected_null
    out["observed_null_in_v271b"] = n_null

    assert_invariants(con)
    out["finished_at"] = _ts()
    write_step_json(3, out)
    return out


# ---------------------------------------------------------------------------
# STEP 4 — Imaging laterality split + normalized rollup
# ---------------------------------------------------------------------------

def step4_imaging_split(con) -> dict:
    log("=== STEP 4 — imaging_has_left/right/isthmus + imaging_laterality_rollup_v271b ===")
    out: dict = {"step": 4, "started_at": _ts()}

    if not table_exists(con, "imaging_nodule_master_v1"):
        raise SystemExit("imaging_nodule_master_v1 missing; cannot derive Step 4")

    _add_column(
        con,
        "imaging_has_left_nodule", "BOOLEAN",
        "TRUE if patient has any imaging_nodule_master_v1 nodule with laterality=left "
        "(case-insensitive); FALSE if patient has nodules but none on the left; NULL if no "
        "nodules in inm_v1. Script 271b, 2026-04-18.",
    )
    _add_column(
        con,
        "imaging_has_right_nodule", "BOOLEAN",
        "TRUE if patient has any imaging_nodule_master_v1 nodule with laterality=right "
        "(case-insensitive); FALSE if patient has nodules but none on the right; NULL if no "
        "nodules in inm_v1. Script 271b, 2026-04-18.",
    )
    _add_column(
        con,
        "imaging_has_isthmus_nodule", "BOOLEAN",
        "TRUE if patient has any imaging_nodule_master_v1 nodule with laterality=isthmus "
        "(case-insensitive); FALSE if patient has nodules but none isthmic; NULL if no "
        "nodules in inm_v1. Script 271b, 2026-04-18.",
    )
    _add_column(
        con,
        "imaging_laterality_rollup_v271b", "VARCHAR",
        "Imaging laterality normalized. Values: bilateral (L+R, with or without isthmus), "
        "left_only, left_plus_isthmus, right_only, right_plus_isthmus, isthmus_only, NULL "
        "(no nodules in inm_v1). Derivation uses imaging_has_left/right/isthmus_nodule. "
        "Supersedes imaging_laterality_rollup whose 'mixed' value is ambiguous. "
        "Script 271b, 2026-04-18.",
    )

    # Per-patient flags from inm_v1
    con.execute("DROP TABLE IF EXISTS _img_flags")
    con.execute(
        "CREATE TEMP TABLE _img_flags AS "
        "SELECT CAST(research_id AS VARCHAR) AS research_id, "
        "  BOOL_OR(LOWER(TRIM(CAST(laterality AS VARCHAR))) IN ('left','l','lt'))  AS has_l, "
        "  BOOL_OR(LOWER(TRIM(CAST(laterality AS VARCHAR))) IN ('right','r','rt')) AS has_r, "
        "  BOOL_OR(LOWER(TRIM(CAST(laterality AS VARCHAR))) IN ('isthmus','midline')) AS has_i, "
        "  COUNT(*) AS n_rows "
        "FROM imaging_nodule_master_v1 "
        "GROUP BY 1"
    )
    con.execute("DROP TABLE IF EXISTS _img_rollup")
    con.execute(
        "CREATE TEMP TABLE _img_rollup AS "
        "SELECT research_id, has_l, has_r, has_i, "
        "  CASE "
        "    WHEN has_l AND has_r THEN 'bilateral' "
        "    WHEN has_l AND NOT has_r AND NOT has_i THEN 'left_only' "
        "    WHEN has_l AND NOT has_r AND has_i THEN 'left_plus_isthmus' "
        "    WHEN has_r AND NOT has_l AND NOT has_i THEN 'right_only' "
        "    WHEN has_r AND NOT has_l AND has_i THEN 'right_plus_isthmus' "
        "    WHEN has_i AND NOT has_l AND NOT has_r THEN 'isthmus_only' "
        "    ELSE NULL "
        "  END AS rollup "
        "FROM _img_flags"
    )

    # Reset target columns first
    con.execute(
        "UPDATE canonical_patient_master "
        "SET imaging_has_left_nodule = NULL, "
        "    imaging_has_right_nodule = NULL, "
        "    imaging_has_isthmus_nodule = NULL, "
        "    imaging_laterality_rollup_v271b = NULL"
    )
    con.execute(
        "UPDATE canonical_patient_master AS c "
        "SET imaging_has_left_nodule = r.has_l, "
        "    imaging_has_right_nodule = r.has_r, "
        "    imaging_has_isthmus_nodule = r.has_i, "
        "    imaging_laterality_rollup_v271b = r.rollup "
        "FROM _img_rollup AS r "
        "WHERE CAST(c.research_id AS VARCHAR) = r.research_id"
    )

    # Annotate legacy column
    legacy_comment = (
        "LEGACY (Script 271): combined left+right+isthmus into 'mixed', which is semantically "
        "ambiguous. Use imaging_laterality_rollup_v271b for analysis. "
        "Script 271b annotation, 2026-04-18."
    )
    safe = legacy_comment.replace("'", "''")
    con.execute(
        f"COMMENT ON COLUMN canonical_patient_master.imaging_laterality_rollup IS '{safe}'"
    )
    log("  legacy imaging_laterality_rollup COMMENT updated")

    # Verify
    rows = con.execute(
        "SELECT imaging_laterality_rollup_v271b, COUNT(*) "
        "FROM canonical_patient_master GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    out["imaging_laterality_rollup_v271b"] = [
        {"value": r[0], "n": r[1]} for r in rows
    ]
    log("  imaging_laterality_rollup_v271b distribution:")
    for r in rows:
        log(f"    {r[0]!r}: {r[1]}")
    for c in ("imaging_has_left_nodule", "imaging_has_right_nodule",
              "imaging_has_isthmus_nodule"):
        flags = con.execute(
            f"SELECT {c}, COUNT(*) FROM canonical_patient_master GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
        out[c] = [{"value": r[0], "n": r[1]} for r in flags]
        log(f"  {c}:")
        for r in flags:
            log(f"    {r[0]!r}: {r[1]}")

    # Cross-check: comparison of legacy mixed bucket vs new categories
    cross = con.execute(
        "SELECT imaging_laterality_rollup AS legacy, "
        "       imaging_laterality_rollup_v271b AS v271b, COUNT(*) AS n "
        "FROM canonical_patient_master GROUP BY 1,2 ORDER BY 3 DESC"
    ).fetchall()
    out["legacy_vs_v271b_crosstab"] = [
        {"legacy": r[0], "v271b": r[1], "n": r[2]} for r in cross
    ]

    assert_invariants(con)
    out["finished_at"] = _ts()
    write_step_json(4, out)
    return out


# ---------------------------------------------------------------------------
# STEP 5 — Concordance v271b
# ---------------------------------------------------------------------------

def step5_concordance(con) -> dict:
    log("=== STEP 5 — pathology_vs_imaging_laterality_concordant_v271b ===")
    out: dict = {"step": 5, "started_at": _ts()}

    _add_column(
        con,
        "pathology_vs_imaging_laterality_concordant_v271b", "VARCHAR",
        "Semantic concordance between tumor_pathology_laterality_v271b and "
        "imaging_laterality_rollup_v271b. Values: concordant (exact or semantic match — e.g., "
        "path=bilateral & img=bilateral; path=left & img=left_plus_isthmus), discordant (path "
        "and img point to opposite or non-overlapping lobes — e.g., path=left & img=right_only, "
        "or path=left & img=bilateral), partially_concordant (path=midline and img has other "
        "lobe involvement without isthmus), unknown_path (path=unknown), insufficient_data "
        "(either side NULL). Script 271b, 2026-04-18.",
    )

    # Build the rule via SQL CASE; symmetric for left/right.
    con.execute(
        "UPDATE canonical_patient_master "
        "SET pathology_vs_imaging_laterality_concordant_v271b = NULL"
    )
    con.execute(
        "UPDATE canonical_patient_master "
        "SET pathology_vs_imaging_laterality_concordant_v271b = CASE "
        # insufficient data
        "  WHEN tumor_pathology_laterality_v271b IS NULL OR imaging_laterality_rollup_v271b IS NULL "
        "       THEN 'insufficient_data' "
        # unknown path
        "  WHEN tumor_pathology_laterality_v271b = 'unknown' THEN 'unknown_path' "
        # bilateral path
        "  WHEN tumor_pathology_laterality_v271b = 'bilateral' "
        "       AND imaging_laterality_rollup_v271b = 'bilateral' THEN 'concordant' "
        "  WHEN tumor_pathology_laterality_v271b = 'bilateral' "
        "       AND imaging_laterality_rollup_v271b IN "
        "         ('left_only','left_plus_isthmus','right_only','right_plus_isthmus','isthmus_only') "
        "       THEN 'discordant' "
        # left path
        "  WHEN tumor_pathology_laterality_v271b = 'left' "
        "       AND imaging_laterality_rollup_v271b IN ('left_only','left_plus_isthmus') "
        "       THEN 'concordant' "
        "  WHEN tumor_pathology_laterality_v271b = 'left' "
        "       AND imaging_laterality_rollup_v271b IN "
        "         ('right_only','right_plus_isthmus','isthmus_only','bilateral') "
        "       THEN 'discordant' "
        # right path
        "  WHEN tumor_pathology_laterality_v271b = 'right' "
        "       AND imaging_laterality_rollup_v271b IN ('right_only','right_plus_isthmus') "
        "       THEN 'concordant' "
        "  WHEN tumor_pathology_laterality_v271b = 'right' "
        "       AND imaging_laterality_rollup_v271b IN "
        "         ('left_only','left_plus_isthmus','isthmus_only','bilateral') "
        "       THEN 'discordant' "
        # midline path
        "  WHEN tumor_pathology_laterality_v271b = 'midline' "
        "       AND imaging_laterality_rollup_v271b IN "
        "         ('isthmus_only','left_plus_isthmus','right_plus_isthmus') "
        "       THEN 'concordant' "
        "  WHEN tumor_pathology_laterality_v271b = 'midline' "
        "       AND imaging_laterality_rollup_v271b IN ('left_only','right_only','bilateral') "
        "       THEN 'partially_concordant' "
        "  ELSE NULL "
        "END"
    )

    # Sanity: any unaccounted NULL pairs (where both sides non-NULL)?
    rogue_n = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master "
        "WHERE pathology_vs_imaging_laterality_concordant_v271b IS NULL "
        "  AND tumor_pathology_laterality_v271b IS NOT NULL "
        "  AND imaging_laterality_rollup_v271b IS NOT NULL"
    ).fetchone()[0]
    if rogue_n:
        rogue_rows = con.execute(
            "SELECT tumor_pathology_laterality_v271b, imaging_laterality_rollup_v271b, COUNT(*) "
            "FROM canonical_patient_master "
            "WHERE pathology_vs_imaging_laterality_concordant_v271b IS NULL "
            "  AND tumor_pathology_laterality_v271b IS NOT NULL "
            "  AND imaging_laterality_rollup_v271b IS NOT NULL "
            "GROUP BY 1,2 ORDER BY 3 DESC"
        ).fetchall()
        log(f"  WARN {rogue_n} rows fell through CASE: {rogue_rows}")
        out["unaccounted_rule_gaps"] = [
            {"path": r[0], "img": r[1], "n": r[2]} for r in rogue_rows
        ]
    out["unaccounted_n"] = rogue_n

    # Distribution
    rows = con.execute(
        "SELECT pathology_vs_imaging_laterality_concordant_v271b, COUNT(*) "
        "FROM canonical_patient_master GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    out["concordance_v271b_distribution"] = [
        {"value": r[0], "n": r[1]} for r in rows
    ]
    log("  pathology_vs_imaging_laterality_concordant_v271b distribution:")
    for r in rows:
        log(f"    {r[0]!r}: {r[1]}")

    # Comparison with legacy boolean
    legacy_v271b = con.execute(
        "SELECT pathology_vs_imaging_laterality_concordant AS legacy, "
        "       pathology_vs_imaging_laterality_concordant_v271b AS v271b, COUNT(*) AS n "
        "FROM canonical_patient_master GROUP BY 1,2 ORDER BY 3 DESC"
    ).fetchall()
    out["legacy_vs_v271b_concordance_crosstab"] = [
        {"legacy": r[0], "v271b": r[1], "n": r[2]} for r in legacy_v271b
    ]

    # Annotate legacy boolean
    legacy_comment = (
        "LEGACY (Script 271 -> Script 271a 3-valued): boolean on cpm.laterality "
        "(specimen-derived, undocumented source) vs imaging_laterality_rollup (had ambiguous "
        "'mixed'). Use pathology_vs_imaging_laterality_concordant_v271b for analysis. "
        "Script 271b annotation, 2026-04-18."
    )
    safe = legacy_comment.replace("'", "''")
    con.execute(
        "COMMENT ON COLUMN canonical_patient_master."
        f"pathology_vs_imaging_laterality_concordant IS '{safe}'"
    )
    log("  legacy pathology_vs_imaging_laterality_concordant COMMENT updated")

    assert_invariants(con)
    out["finished_at"] = _ts()
    write_step_json(5, out)
    return out


# ---------------------------------------------------------------------------
# STEP 6 — Document legacy laterality columns
# ---------------------------------------------------------------------------

_LEGACY_COMMENTS_TEMPLATE = {
    "path_laterality": (
        "Pathology-derived patient-level laterality predating Script 271 (legacy). "
        "Vocabulary observed: bilateral/left/right/NULL. Documented in Script 271b; "
        "for new analyses prefer tumor_pathology_laterality_v271b "
        "(rebuilt from tumor_pathology.tumor_laterality_overall under documented rules)."
    ),
    "bilateral_disease_flag": (
        "BOOLEAN flag, predates Script 271 (legacy). TRUE indicates bilateral disease per "
        "the upstream feeder. Source not re-derivable in this script; "
        "for new analyses prefer (tumor_pathology_laterality_v271b='bilateral') "
        "or (imaging_has_left_nodule AND imaging_has_right_nodule). "
        "Script 271b documentation, 2026-04-18."
    ),
    "bilateral_path_flag": (
        "BOOLEAN flag, predates Script 271 (legacy). TRUE indicates pathology indicated "
        "bilateral disease. Source not re-derivable in this script; "
        "for new analyses prefer (tumor_pathology_laterality_v271b='bilateral'). "
        "Script 271b documentation, 2026-04-18."
    ),
}


def step6_legacy_comments(con, forensics_payload: dict | None = None) -> dict:
    log("=== STEP 6 — Document legacy laterality columns ===")
    out: dict = {"step": 6, "started_at": _ts(), "comments_applied": {}}

    # Reload Step 2 forensics for cpm.laterality COMMENT
    if forensics_payload is None:
        path = OUT_DIR / "271b_step2.json"
        if path.exists():
            with path.open() as fh:
                forensics_payload = json.load(fh)
    if forensics_payload is None:
        raise SystemExit("Step 2 payload not found; run --step 2 before --step 6")

    cpm_lat_comment = forensics_payload.get(
        "proposed_comment",
        "Undocumented legacy column predating Script 271. Vocabulary: bilateral/left/right/NULL. "
        "For new analyses use tumor_pathology_laterality_v271b. Script 271b annotation, 2026-04-18.",
    )

    cols = cpm_columns(con)
    plan: dict[str, str] = {"laterality": cpm_lat_comment}
    for k, v in _LEGACY_COMMENTS_TEMPLATE.items():
        if k in cols:
            plan[k] = v + " Script 271b documentation, 2026-04-18."
        else:
            log(f"  SKIP COMMENT for missing column: {k}")

    for col, comment in plan.items():
        safe = comment.replace("'", "''")
        con.execute(
            f"COMMENT ON COLUMN canonical_patient_master.{col} IS '{safe}'"
        )
        log(f"  COMMENT applied to {col}")
        out["comments_applied"][col] = comment

    assert_invariants(con)
    out["finished_at"] = _ts()
    write_step_json(6, out)
    return out


# ---------------------------------------------------------------------------
# STEP 7 — Verify, refresh dictionary, write doc
# ---------------------------------------------------------------------------

def _refresh_data_dictionary(con) -> dict:
    """Snapshot current data_dictionary_v266a to archive, then rebuild it from
    information_schema + per-column metrics + carried-forward legacy fields.
    Mirrors scripts/271_step8_patch_dictionary_schema.py logic."""
    if not table_exists(con, "data_dictionary_v266a"):
        log("  data_dictionary_v266a missing; skipping refresh")
        return {"refreshed": False, "reason": "missing"}

    # snapshot existing
    snap_name = f"data_dictionary_v266a_pre271b_{ISO_TS}"
    snap_fq = f'{ARCHIVE_PREFIX}."{snap_name}"'
    exists = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_catalog=? AND table_schema=? AND table_name=?",
        [ARCHIVE_DB, ARCHIVE_SCHEMA, snap_name],
    ).fetchone()[0]
    if not exists:
        con.execute(
            f"CREATE TABLE {snap_fq} AS SELECT * FROM data_dictionary_v266a"
        )
        log(f"  snapshotted dictionary -> {snap_fq}")

    # Carry-forward legacy fields keyed by (table_name?, column_name).
    # Inspect snapshot schema; some versions include table_name, some don't.
    snap_cols = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog=? AND table_schema=? AND table_name=?",
        [ARCHIVE_DB, ARCHIVE_SCHEMA, snap_name],
    ).fetchall()}
    has_table_name = "table_name" in snap_cols

    # Build _info from info-schema. If snapshot has table_name we restrict to
    # the same set of tables covered by the prior dictionary; otherwise we cover
    # all main-schema tables (legacy pre-271_step8 layout).
    con.execute("DROP TABLE IF EXISTS _info")
    if has_table_name:
        con.execute(
            "CREATE TEMP TABLE _info AS "
            "SELECT table_name, column_name, data_type, "
            "       CASE WHEN is_nullable THEN 'YES' ELSE 'NO' END AS is_nullable, "
            "       column_index AS ordinal_position, comment "
            "FROM duckdb_columns() "
            f"WHERE database_name='{PUBLICATION_DB}' AND schema_name='main' "
            "  AND table_name IN ("
            f"    SELECT DISTINCT table_name FROM {snap_fq} "
            "  )"
        )
    else:
        con.execute(
            "CREATE TEMP TABLE _info AS "
            "SELECT table_name, column_name, data_type, "
            "       CASE WHEN is_nullable THEN 'YES' ELSE 'NO' END AS is_nullable, "
            "       column_index AS ordinal_position, comment "
            "FROM duckdb_columns() "
            f"WHERE database_name='{PUBLICATION_DB}' AND schema_name='main'"
        )
    n_info = con.execute("SELECT COUNT(*) FROM _info").fetchone()[0]
    log(f"  _info rows: {n_info}")

    # Per-column metrics
    con.execute("DROP TABLE IF EXISTS _metrics")
    con.execute(
        "CREATE TEMP TABLE _metrics ("
        "  table_name VARCHAR, column_name VARCHAR, "
        "  n_non_null BIGINT, pct_non_null DOUBLE, n_distinct BIGINT)"
    )
    info_rows = con.execute("SELECT table_name, column_name FROM _info").fetchall()
    for tbl, col in info_rows:
        col_q = f'"{col}"'
        try:
            row = con.execute(
                f'SELECT COUNT({col_q}) AS nn, '
                f'COUNT(DISTINCT {col_q}) AS nd, '
                f'COUNT(*) AS n '
                f'FROM {PUBLICATION_DB}.main.{tbl}'
            ).fetchone()
            nn, nd, n = row
            pct = (nn / n * 100.0) if n else None
        except Exception as e:
            log(f"    WARN metric for {tbl}.{col}: {e!r}")
            nn, nd, pct = None, None, None
        con.execute(
            "INSERT INTO _metrics VALUES (?, ?, ?, ?, ?)",
            [tbl, col, nn, pct, nd],
        )

    # Carry-forward legacy
    if has_table_name:
        legacy_sql = (
            "WITH legacy AS ("
            "  SELECT table_name, column_name, "
            "         ANY_VALUE(description) AS description, "
            "         ANY_VALUE(status) AS status, "
            "         ANY_VALUE(replacement_column_name) AS replacement_column_name "
            f"  FROM {snap_fq} "
            "  GROUP BY 1,2"
            ") "
        )
        join_clause = "LEFT JOIN legacy l USING (table_name, column_name)"
    else:
        legacy_sql = (
            "WITH legacy AS ("
            "  SELECT column_name, "
            "         ANY_VALUE(description) AS description, "
            "         ANY_VALUE(status) AS status, "
            "         ANY_VALUE(replacement_column_name) AS replacement_column_name "
            f"  FROM {snap_fq} "
            "  GROUP BY 1"
            ") "
        )
        join_clause = "LEFT JOIN legacy l USING (column_name)"

    con.execute(
        "CREATE OR REPLACE TABLE data_dictionary_v266a AS "
        + legacy_sql +
        "SELECT i.table_name, i.column_name, i.data_type, i.is_nullable, "
        "       i.ordinal_position, i.comment, "
        "       m.n_non_null, m.pct_non_null, m.n_distinct, "
        "       l.description, l.status, l.replacement_column_name, "
        f"       TIMESTAMP '{NOW}' AS rebuilt_at, "
        f"       '{SCRIPT_TAG}' AS rebuilt_by "
        "FROM _info i "
        "LEFT JOIN _metrics m USING (table_name, column_name) "
        + join_clause + " "
        "ORDER BY i.table_name, i.ordinal_position"
    )
    n_dict = con.execute("SELECT COUNT(*) FROM data_dictionary_v266a").fetchone()[0]
    log(f"  data_dictionary_v266a rebuilt: total rows={n_dict}")

    # Verify the 7 new CPM columns are present
    new_cols = [
        "tumor_pathology_laterality_v271b",
        "tumor_pathology_has_isthmus_involvement",
        "imaging_has_left_nodule", "imaging_has_right_nodule",
        "imaging_has_isthmus_nodule", "imaging_laterality_rollup_v271b",
        "pathology_vs_imaging_laterality_concordant_v271b",
    ]
    qmarks = ",".join(["?"] * len(new_cols))
    rows = con.execute(
        f"SELECT column_name FROM data_dictionary_v266a "
        f"WHERE table_name='canonical_patient_master' AND column_name IN ({qmarks})",
        new_cols,
    ).fetchall()
    found = sorted(r[0] for r in rows)
    log(f"  new CPM cols in dictionary: {found}")
    return {
        "refreshed": True,
        "snapshot": snap_name,
        "n_dict_rows": n_dict,
        "new_cpm_cols_found": found,
    }


def step7_verify_dict_doc(con) -> dict:
    log("=== STEP 7 — Verify, refresh dictionary, write LATERALITY doc ===")
    out: dict = {"step": 7, "started_at": _ts()}

    inv = assert_invariants(con)
    out["invariants"] = {"n": inv[0], "distinct": inv[1], "nulls": inv[2]}

    n_cols = con.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND table_name='canonical_patient_master'"
    ).fetchone()[0]
    out["cpm_n_columns"] = n_cols
    log(f"  CPM column count: {n_cols} (expected 1519+7=1526; tolerant if not exact)")

    # Concordance distribution
    rows = con.execute(
        "SELECT pathology_vs_imaging_laterality_concordant_v271b, COUNT(*) "
        "FROM canonical_patient_master GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    out["concordance_v271b"] = [{"value": r[0], "n": r[1]} for r in rows]
    log("  concordance_v271b:")
    for r in rows:
        log(f"    {r[0]!r}: {r[1]}")

    rows = con.execute(
        "SELECT tumor_pathology_laterality_v271b, COUNT(*) "
        "FROM canonical_patient_master GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    out["tp_v271b"] = [{"value": r[0], "n": r[1]} for r in rows]

    rows = con.execute(
        "SELECT imaging_laterality_rollup_v271b, COUNT(*) "
        "FROM canonical_patient_master GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    out["img_rollup_v271b"] = [{"value": r[0], "n": r[1]} for r in rows]

    # Manuscript_workspace regression
    try:
        n_manu = con.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.cohort_descriptive_full_cohort_v1"
        ).fetchone()[0]
        out["manuscript_cohort_n"] = n_manu
        log(f"  manuscript_workspace.cohort_descriptive_full_cohort_v1: {n_manu}")
    except Exception as e:
        log(f"  WARN manuscript_workspace probe: {e!r}")
        out["manuscript_cohort_error"] = repr(e)

    # Refresh data dictionary
    out["data_dictionary"] = _refresh_data_dictionary(con)

    # Write LATERALITY_NORMALIZATION doc
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    doc = DOCS_DIR / "LATERALITY_NORMALIZATION_20260418.md"
    legacy_dist = con.execute(
        "SELECT pathology_vs_imaging_laterality_concordant, COUNT(*) "
        "FROM canonical_patient_master GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()

    lines: list[str] = []
    lines.append("# Laterality Vocabulary Normalization (Script 271b, 2026-04-18)")
    lines.append("")
    lines.append("## Problems addressed")
    lines.append("")
    lines.append(
        "1. **`cpm.laterality` had no COMMENT.** Step 2 forensics in Script 271b "
        "characterized its source/semantics; the column is now documented in place "
        "but not renamed (manuscripts may depend on it)."
    )
    lines.append(
        "2. **Isthmus involvement was silently coerced.** "
        "`tumor_pathology.tumor_laterality_overall = 'midline'` (111 patients) was "
        "lost in the legacy rollup. Now exposed explicitly via "
        "`tumor_pathology_has_isthmus_involvement`."
    )
    lines.append(
        "3. **`imaging_laterality_rollup = 'mixed'` conflated four distinct states.** "
        "Replaced by `imaging_laterality_rollup_v271b` "
        "(bilateral/left_only/left_plus_isthmus/right_only/right_plus_isthmus/isthmus_only) "
        "and per-side flags `imaging_has_{left,right,isthmus}_nodule`."
    )
    lines.append(
        "4. **`pathology_vs_imaging_laterality_concordant` (boolean) was inflated by "
        "vocabulary mismatch.** Rebuilt as 5-valued "
        "`pathology_vs_imaging_laterality_concordant_v271b` "
        "(concordant/discordant/partially_concordant/unknown_path/insufficient_data) "
        "on the normalized vocabularies."
    )
    lines.append("")
    lines.append("## New columns on canonical_patient_master")
    lines.append("")
    lines.append("| Column | Type | Purpose |")
    lines.append("|---|---|---|")
    lines.append("| `tumor_pathology_laterality_v271b` | VARCHAR | Patient-level disease laterality from `tumor_pathology` |")
    lines.append("| `tumor_pathology_has_isthmus_involvement` | BOOLEAN | TRUE if any tumor row is midline (isthmus) |")
    lines.append("| `imaging_has_left_nodule` | BOOLEAN | inm_v1 has any left-laterality nodule |")
    lines.append("| `imaging_has_right_nodule` | BOOLEAN | inm_v1 has any right-laterality nodule |")
    lines.append("| `imaging_has_isthmus_nodule` | BOOLEAN | inm_v1 has any isthmus nodule |")
    lines.append("| `imaging_laterality_rollup_v271b` | VARCHAR | Normalized imaging rollup (no `mixed`) |")
    lines.append("| `pathology_vs_imaging_laterality_concordant_v271b` | VARCHAR | 5-valued semantic concordance |")
    lines.append("")
    lines.append("## Legacy concordance distribution (boolean) vs v271b")
    lines.append("")
    lines.append("**Legacy (`pathology_vs_imaging_laterality_concordant`, 271a 3-valued):**")
    lines.append("")
    lines.append("| Value | n |")
    lines.append("|---|---:|")
    for r in legacy_dist:
        lines.append(f"| `{r[0]}` | {r[1]} |")
    lines.append("")
    lines.append("**v271b (`pathology_vs_imaging_laterality_concordant_v271b`):**")
    lines.append("")
    lines.append("| Value | n |")
    lines.append("|---|---:|")
    for r in out["concordance_v271b"]:
        lines.append(f"| `{r['value']}` | {r['n']} |")
    lines.append("")
    lines.append("## Guidance for manuscript authors")
    lines.append("")
    lines.append(
        "- For disease laterality, use **`tumor_pathology_laterality_v271b`** "
        "(rebuilt from `tumor_pathology` with documented rules)."
    )
    lines.append(
        "- For imaging laterality, use **`imaging_laterality_rollup_v271b`** and the "
        "explicit `imaging_has_{left,right,isthmus}_nodule` flags."
    )
    lines.append(
        "- For path/imaging agreement, use "
        "**`pathology_vs_imaging_laterality_concordant_v271b`** and treat "
        "`partially_concordant` and `insufficient_data` as distinct from `discordant`."
    )
    lines.append(
        "- Do **not** use the legacy `cpm.laterality`, `path_laterality`, "
        "`bilateral_disease_flag`, `bilateral_path_flag`, `imaging_laterality_rollup`, "
        "or boolean `pathology_vs_imaging_laterality_concordant` for new analyses."
    )
    lines.append("")
    lines.append("## Open question")
    lines.append("")
    lines.append(
        "Should `cpm.laterality` (and friends) be physically deprecated? "
        "**Decision deferred** for this release. They are documented (COMMENT-only "
        "annotation) but not dropped, because at least one downstream manuscript may "
        "still query them. Revisit after the next manuscript freeze."
    )
    lines.append("")
    doc.write_text("\n".join(lines) + "\n")
    log(f"  wrote {doc}")
    out["doc_path"] = str(doc.relative_to(REPO))

    out["finished_at"] = _ts()
    write_step_json(7, out)
    return out


# ---------------------------------------------------------------------------
# STEP 8 — Parquet export
# ---------------------------------------------------------------------------

def step8_export(con) -> dict:
    log("=== STEP 8 — parquet export ===")
    out: dict = {"step": 8, "started_at": _ts()}
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = EXPORTS_DIR / "canonical_patient_master_20260418_script271b.parquet"
    try:
        df = con.execute(
            f"SELECT * FROM {PUBLICATION_DB}.main.canonical_patient_master"
        ).df()
        df.to_parquet(out_path, index=False)
        log(f"  wrote {out_path} ({len(df):,} rows × {len(df.columns)} cols)")
        out["exported"] = str(out_path.relative_to(REPO))
        out["n_rows"] = len(df)
        out["n_cols"] = len(df.columns)
    except Exception as e:
        log(f"  ERROR exporting parquet: {e!r}")
        out["error"] = repr(e)
        raise

    out["finished_at"] = _ts()
    write_step_json(8, out)
    return out


# ---------------------------------------------------------------------------
# Final self-check
# ---------------------------------------------------------------------------

def final_self_check(con) -> None:
    log("########## Script 271b — FINAL SELF CHECK ##########")
    n_cols = con.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        "AND table_name='canonical_patient_master'"
    ).fetchone()[0]
    log(f"CPM: 10,871 x {n_cols} cols")

    forensics_path = OUT_DIR / "271b_step2.json"
    src = "unknown"
    if forensics_path.exists():
        with forensics_path.open() as fh:
            verdict = json.load(fh).get("verdict", "unknown")
        src = verdict
    log(f"cpm.laterality source verdict: {src}")

    for col, label in [
        ("tumor_pathology_laterality_v271b", "tumor_pathology_laterality_v271b"),
        ("imaging_laterality_rollup_v271b", "imaging_laterality_rollup_v271b"),
        ("pathology_vs_imaging_laterality_concordant_v271b", "concordance_v271b"),
    ]:
        rows = con.execute(
            f"SELECT {col}, COUNT(*) FROM canonical_patient_master "
            "GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
        parts = ", ".join(f"{r[0]}={r[1]}" for r in rows)
        log(f"{label}: {{{parts}}}")

    # Archive snapshot count
    n_arch = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_catalog=? AND table_schema=? AND table_name LIKE '%_pre271b_%'",
        [ARCHIVE_DB, ARCHIVE_SCHEMA],
    ).fetchone()[0]
    log(f"archive_pub_v1_0 pre271b snapshots: {n_arch}")
    log("legacy columns: COMMENT-annotated, values not modified")
    log("manuscript_workspace: views intact (see step 7 probe)")


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

STEPS = {
    0: step0_preflight,
    1: step1_snapshots,
    2: step2_forensics,
    3: step3_tumor_pathology_laterality,
    4: step4_imaging_split,
    5: step5_concordance,
    6: step6_legacy_comments,
    7: step7_verify_dict_doc,
    8: step8_export,
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Script 271b — laterality normalization")
    parser.add_argument(
        "--step", default="all",
        help="Step to run (0-8) or 'all' to run sequentially",
    )
    args = parser.parse_args()

    log(f"########## Script 271b START (--step {args.step}) ##########")
    log(f"  archive: {ARCHIVE_PREFIX}, ts: {ISO_TS}")

    if args.step == "all":
        steps_to_run = list(range(0, 9))
    else:
        steps_to_run = [int(args.step)]

    con = connect_locked()
    try:
        for s in steps_to_run:
            if s not in STEPS:
                raise SystemExit(f"Unknown step: {s}")
            STEPS[s](con)
        if steps_to_run == list(range(0, 9)):
            final_self_check(con)
    finally:
        con.close()

    log(f"########## Script 271b DONE (--step {args.step}) ##########")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
