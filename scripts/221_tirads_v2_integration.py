#!/usr/bin/env python3
"""
Script 221 — Integrate TIRADS v2 granular extraction into canonical.

Lands the Qwen2.5-32B-Instruct-AWQ TIRADS extraction (vLLM on Vast.ai H200,
commit d6ca339, 2026-04-18) into ``thyroid_canonical_publication_v1_0`` as
two raw tables and two patient-level rollups, and adds 10 ``tirads_v2_*``
columns to ``main.canonical_patient_master`` *alongside* the legacy v12 /
preop_tirads_best columns (no overwrite).

Phase gates (all gated by CLI; default runs Phase 0 only):

  --phase 0    Pre-flight audit (READ-ONLY)
  --phase 1    Load nodules_clean.parquet + reports_clean.parquet as raw
  --phase 2    Build patient-level rollups (1 row / RID each)
  --phase 3    ALTER canonical_patient_master + UPDATE from rollups
  --phase 4    Archive deprecated tirads_llm_extracted_v2 (RID-coverage gated)
  --phase 5    Update detail_table_registry_v1 + main.__readme
  --phase 6    Final validation (invariants + concordance)
  --phase all  Run 0→6 in sequence, halting on any failed gate.

Hard rules (per integration prompt):
  * NEVER write to ``"Thyroid 2026 UPdated"`` except for the Phase 4 archival
    CREATE.
  * NEVER overwrite preop_tirads_best, tirads_best_score_v12, or any existing
    legacy TIRADS column. v2 sits alongside.
  * DROP of legacy ``tirads_llm_extracted_v2`` is conditional on Phase 0
    confirming zero RID loss.
  * ``research_id`` is VARCHAR in both canonical and the parquets — no CAST.
  * If any phase fails an invariant, STOP and report. Do not continue.

Output JSON (audit + per-phase decisions): scripts/output/221_tirads_v2_integration.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from motherduck_client import get_token, token_mode  # noqa: E402

# ── constants ────────────────────────────────────────────────────────────────

CANONICAL_DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_DB = "Thyroid 2026 UPdated"
LEGACY_TABLE = "tirads_llm_extracted_v2"
ARCHIVE_TABLE = "tirads_llm_extracted_v2_deprecated_20260418"

NOD_PARQUET = REPO_ROOT / "runs/tirads_granular/full_v2_output/nodules_clean.parquet"
REP_PARQUET = REPO_ROOT / "runs/tirads_granular/full_v2_output/reports_clean.parquet"

NOD_TABLE = "tirads_v2_nodules_raw"
REP_TABLE = "tirads_v2_reports_raw"
NOD_ROLLUP = "tirads_v2_nodule_patient_rollup_v1"
REP_ROLLUP = "tirads_v2_report_patient_rollup_v1"

# 10 new canonical columns (name, DuckDB type) — all nullable, prefix tirads_v2_
NEW_CPM_COLS: list[tuple[str, str]] = [
    ("tirads_v2_n_nodules_scored",         "BIGINT"),
    ("tirads_v2_worst_category",           "VARCHAR"),
    ("tirads_v2_max_points",               "DOUBLE"),
    ("tirads_v2_largest_nodule_cm",        "DOUBLE"),
    ("tirads_v2_any_ete_on_us",            "BOOLEAN"),
    ("tirads_v2_any_interval_growth",      "BOOLEAN"),
    ("tirads_v2_any_fna_recommended",      "BOOLEAN"),
    ("tirads_v2_n_reports",                "BIGINT"),
    ("tirads_v2_any_suspicious_ln_on_us",  "BOOLEAN"),
    ("tirads_v2_shortest_followup_months", "DOUBLE"),
]

EXPECTED_NOD_ROWS, EXPECTED_NOD_RIDS = 11_914, 3_021
EXPECTED_REP_ROWS, EXPECTED_REP_RIDS = 8_810, 4_073
EXPECTED_CPM_ROWS = 10_871

SCRIPT_TAG = "scripts/221_tirads_v2_integration.py"
RUN_TS_ISO = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
RUN_DATE = RUN_TS_ISO[:10]
SOURCE_COMMIT = "d6ca339"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
DECISIONS_PATH = OUTPUT_DIR / "221_tirads_v2_integration.json"
LOG_PATH = OUTPUT_DIR / "221_tirads_v2_integration.log"


# ── helpers ──────────────────────────────────────────────────────────────────

_log_buf: list[str] = []


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).strftime('%H:%M:%S.%f')[:-3]}Z] {msg}"
    print(line, flush=True)
    _log_buf.append(line)


def _flush_log() -> None:
    LOG_PATH.write_text("\n".join(_log_buf) + "\n")


def connect() -> duckdb.DuckDBPyConnection:
    tok = get_token()
    if not tok:
        raise SystemExit(
            f"No MotherDuck RW token available (token_mode={token_mode()}). "
            "Set MD_SA_TOKEN / MOTHERDUCK_TOKEN or populate motherduck.local.toml."
        )
    log(f"connecting to MotherDuck '{CANONICAL_DB}' (token_mode={token_mode()})")
    return duckdb.connect(f"md:{CANONICAL_DB}?motherduck_token={tok}")


def col_exists(con: duckdb.DuckDBPyConnection, table: str, column: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_catalog=? AND table_schema='main' "
        "AND table_name=? AND column_name=?",
        [CANONICAL_DB, table, column],
    ).fetchone()
    return row is not None


def table_exists(con: duckdb.DuckDBPyConnection, table: str, schema: str = "main") -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_catalog=? AND table_schema=? AND table_name=?",
        [CANONICAL_DB, schema, table],
    ).fetchone()
    return row is not None


# ── PHASE 0 — pre-flight audit (READ-ONLY) ───────────────────────────────────

def phase_0(con: duckdb.DuckDBPyConnection) -> dict:
    log("=== PHASE 0 — pre-flight audit (READ-ONLY) ===")
    out: dict = {"phase": 0, "started_at": RUN_TS_ISO, "ok": True, "blockers": []}

    # 0A — TIRADS-related tables in canonical main (with row counts via duckdb_tables())
    rows = con.execute(
        "SELECT table_name, estimated_size "
        "FROM duckdb_tables() "
        f"WHERE database_name='{CANONICAL_DB}' AND schema_name='main' "
        "AND LOWER(table_name) LIKE '%tirads%' "
        "ORDER BY table_name"
    ).fetchall()
    out["existing_tirads_tables"] = [(r[0], int(r[1]) if r[1] is not None else None) for r in rows]
    log(f"  0A: {len(rows)} TIRADS-named tables in main:")
    for name, sz in rows:
        log(f"        {name:50s}  est_rows={sz}")

    # 0B — old tirads_llm_extracted_v2 stats (if present)
    legacy_present = table_exists(con, LEGACY_TABLE)
    out["legacy_table_present"] = legacy_present
    if legacy_present:
        n_legacy, r_legacy = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {LEGACY_TABLE}"
        ).fetchone()
        out["legacy_rows"] = int(n_legacy)
        out["legacy_rids"] = int(r_legacy)
        log(f"  0B: {LEGACY_TABLE}: {n_legacy:,} rows, {r_legacy:,} RIDs")
    else:
        out["legacy_rows"] = 0
        out["legacy_rids"] = 0
        log(f"  0B: {LEGACY_TABLE} not present in main — nothing to archive in Phase 4.")

    # 0C — canonical_patient_master TIRADS coverage (current state)
    coverage_cols = [
        "preop_tirads_best",
        "tirads_best_score_v12",
        "tirads_worst_combined",
    ]
    cov_present = {
        c: col_exists(con, "canonical_patient_master", c) for c in coverage_cols
    }
    out["cpm_legacy_cols_present"] = cov_present
    parts = [
        f"COUNT(*) FILTER (WHERE {c} IS NOT NULL) AS has_{c}"
        for c in coverage_cols if cov_present[c]
    ]
    if parts:
        sql = (
            "SELECT COUNT(*) AS n_cpm, COUNT(DISTINCT research_id) AS n_dist, "
            + ", ".join(parts) + " FROM canonical_patient_master"
        )
        row = con.execute(sql).fetchone()
        cols = [d[0] for d in con.description]
        cpm_summary = dict(zip(cols, row))
    else:
        row = con.execute(
            "SELECT COUNT(*) AS n_cpm, COUNT(DISTINCT research_id) AS n_dist "
            "FROM canonical_patient_master"
        ).fetchone()
        cpm_summary = {"n_cpm": row[0], "n_dist": row[1]}
    out["cpm_summary"] = {k: int(v) if v is not None else None for k, v in cpm_summary.items()}
    log(f"  0C: canonical_patient_master coverage: {cpm_summary}")
    if cpm_summary.get("n_cpm") != EXPECTED_CPM_ROWS:
        out["ok"] = False
        out["blockers"].append(
            f"canonical_patient_master rows={cpm_summary.get('n_cpm')} != {EXPECTED_CPM_ROWS}"
        )

    # 0D — local parquet stats + RID overlap with legacy
    log("  0D: inspecting source parquets locally ...")
    loc = duckdb.connect()
    n_nod, r_nod = loc.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM read_parquet('{NOD_PARQUET}')"
    ).fetchone()
    n_rep, r_rep = loc.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM read_parquet('{REP_PARQUET}')"
    ).fetchone()
    out["nod_parquet"] = {"rows": int(n_nod), "rids": int(r_nod)}
    out["rep_parquet"] = {"rows": int(n_rep), "rids": int(r_rep)}
    log(f"        nodules_clean.parquet : {n_nod:,} rows, {r_nod:,} RIDs "
        f"(expected {EXPECTED_NOD_ROWS:,}/{EXPECTED_NOD_RIDS:,})")
    log(f"        reports_clean.parquet : {n_rep:,} rows, {r_rep:,} RIDs "
        f"(expected {EXPECTED_REP_ROWS:,}/{EXPECTED_REP_RIDS:,})")
    if (n_nod, r_nod) != (EXPECTED_NOD_ROWS, EXPECTED_NOD_RIDS):
        out["ok"] = False
        out["blockers"].append(
            f"nodules parquet stats {n_nod}/{r_nod} != expected {EXPECTED_NOD_ROWS}/{EXPECTED_NOD_RIDS}"
        )
    if (n_rep, r_rep) != (EXPECTED_REP_ROWS, EXPECTED_REP_RIDS):
        out["ok"] = False
        out["blockers"].append(
            f"reports parquet stats {n_rep}/{r_rep} != expected {EXPECTED_REP_ROWS}/{EXPECTED_REP_RIDS}"
        )

    # Report-level vs nodule-level RID delta (callout from prompt)
    rids_only_in_reports = loc.execute(
        f"SELECT COUNT(DISTINCT r.research_id) "
        f"FROM read_parquet('{REP_PARQUET}') r "
        f"LEFT JOIN ("
        f"  SELECT DISTINCT research_id FROM read_parquet('{NOD_PARQUET}')"
        f") n USING (research_id) "
        f"WHERE n.research_id IS NULL"
    ).fetchone()[0]
    rids_only_in_nodules = loc.execute(
        f"SELECT COUNT(DISTINCT n.research_id) "
        f"FROM read_parquet('{NOD_PARQUET}') n "
        f"LEFT JOIN ("
        f"  SELECT DISTINCT research_id FROM read_parquet('{REP_PARQUET}')"
        f") r USING (research_id) "
        f"WHERE r.research_id IS NULL"
    ).fetchone()[0]
    out["rids_only_in_reports"] = int(rids_only_in_reports)
    out["rids_only_in_nodules"] = int(rids_only_in_nodules)
    log(f"        report-only RIDs (no scorable nodule): {rids_only_in_reports:,} "
        f"(expected ~1,052; benign/cystic/post-op surveillance)")
    log(f"        nodule-only RIDs (no report row):      {rids_only_in_nodules:,}")

    # 0E — RID overlap with legacy table (used as gate for Phase 4 DROP).
    # NOTE: legacy.research_id is BIGINT, new parquet is VARCHAR — compare as strings.
    rids_lost = 0
    if legacy_present:
        n_legacy_set = con.execute(
            f"SELECT COUNT(DISTINCT CAST(research_id AS VARCHAR)) FROM {LEGACY_TABLE}"
        ).fetchone()[0]
        n_new_union = loc.execute(
            f"SELECT COUNT(DISTINCT research_id) FROM ("
            f"  SELECT research_id FROM read_parquet('{NOD_PARQUET}') "
            f"  UNION SELECT research_id FROM read_parquet('{REP_PARQUET}')"
            f")"
        ).fetchone()[0]
        # Use MotherDuck-side intersection so type coercion is consistent.
        con.execute("CREATE OR REPLACE TEMP TABLE _new_rid_union AS "
                    f"SELECT DISTINCT research_id FROM read_parquet('{NOD_PARQUET}') "
                    f"UNION SELECT DISTINCT research_id FROM read_parquet('{REP_PARQUET}')")
        n_intersect = con.execute(
            f"SELECT COUNT(*) FROM ("
            f"  SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid FROM {LEGACY_TABLE}"
            f") l JOIN _new_rid_union n ON n.research_id = l.rid"
        ).fetchone()[0]
        n_only_legacy = con.execute(
            f"SELECT COUNT(*) FROM ("
            f"  SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid FROM {LEGACY_TABLE}"
            f") l LEFT JOIN _new_rid_union n ON n.research_id = l.rid "
            "WHERE n.research_id IS NULL"
        ).fetchone()[0]
        rids_lost = int(n_only_legacy)
        out["legacy_rid_set_size"] = int(n_legacy_set)
        out["new_rid_union_size"] = int(n_new_union)
        out["legacy_rid_intersect_new"] = int(n_intersect)
        out["legacy_rids_lost_in_new"] = rids_lost
        log(f"  0E: legacy RID set ({n_legacy_set:,}) ∩ new union "
            f"({n_new_union:,}) = {n_intersect:,}")
        log(f"        RIDs in legacy but NOT in new extraction: {rids_lost:,}")
        if rids_lost > 0:
            sample_rows = con.execute(
                f"SELECT DISTINCT CAST(l.research_id AS VARCHAR) AS rid "
                f"FROM {LEGACY_TABLE} l "
                "LEFT JOIN _new_rid_union n ON n.research_id = CAST(l.research_id AS VARCHAR) "
                "WHERE n.research_id IS NULL ORDER BY 1 LIMIT 10"
            ).fetchall()
            sample = [r[0] for r in sample_rows]
            out["legacy_rids_lost_sample"] = sample
            log(f"        sample lost RIDs (first 10): {sample}")
            log("        ⚠ Phase 4 DROP will be SKIPPED — coverage gap "
                "(legacy is COMPLEMENTARY to new, not superseded).")
        else:
            log("        ✓ zero RID loss — Phase 4 DROP is safe.")
    out["phase4_drop_safe"] = legacy_present and rids_lost == 0

    # 0F — cohort coverage diagnostic vs us_nodules_tirads (LLM extraction gap)
    if table_exists(con, "us_nodules_tirads"):
        con.execute("CREATE OR REPLACE TEMP TABLE _new_rep_only AS "
                    f"SELECT DISTINCT research_id FROM read_parquet('{REP_PARQUET}')")
        cov_row = con.execute("""
            SELECT
              COUNT(DISTINCT u.research_id)                                              AS us_nodules_rids_total,
              COUNT(DISTINCT u.research_id) FILTER (
                WHERE u.research_id IN (SELECT research_id FROM _new_rep_only)
              )                                                                          AS rids_in_new_v2_reports,
              COUNT(DISTINCT u.research_id) FILTER (
                WHERE u.research_id NOT IN (SELECT research_id FROM _new_rep_only)
              )                                                                          AS rids_missing_from_new_v2_reports
            FROM us_nodules_tirads u
        """).fetchone()
        out["us_nodules_tirads_coverage"] = {
            "us_nodules_rids_total": int(cov_row[0]),
            "rids_in_new_v2_reports": int(cov_row[1]),
            "rids_missing_from_new_v2_reports": int(cov_row[2]),
        }
        log(f"  0F: us_nodules_tirads cohort gap diagnostic:")
        log(f"        us_nodules_tirads RIDs total          : {cov_row[0]:,}")
        log(f"        of which in new v2 reports.parquet    : {cov_row[1]:,}")
        log(f"        of which MISSING from new v2 reports  : {cov_row[2]:,}")
        log("        (large 'missing' value = follow-on Script 222+ for re-extracting these patients)")
    else:
        log("  0F: us_nodules_tirads table absent — cohort gap diagnostic skipped.")

    # Already-applied check (idempotency hint)
    out["already_applied"] = {
        "tirads_v2_nodules_raw": table_exists(con, NOD_TABLE),
        "tirads_v2_reports_raw": table_exists(con, REP_TABLE),
        "tirads_v2_nodule_patient_rollup_v1": table_exists(con, NOD_ROLLUP),
        "tirads_v2_report_patient_rollup_v1": table_exists(con, REP_ROLLUP),
        "cpm_v2_cols_present": {
            c: col_exists(con, "canonical_patient_master", c) for c, _ in NEW_CPM_COLS
        },
    }

    # Summary
    log("")
    log("──── PHASE 0 AUDIT SUMMARY ────")
    log(f"  blockers:           {out['blockers'] or 'none'}")
    log(f"  legacy table:       {'present' if legacy_present else 'absent'} "
        f"({out.get('legacy_rows', 0):,} rows / {out.get('legacy_rids', 0):,} RIDs)")
    log(f"  legacy RID loss:    {rids_lost:,} (Phase 4 drop {'OK' if out['phase4_drop_safe'] else 'BLOCKED'})")
    log(f"  parquets OK:        {(n_nod, r_nod) == (EXPECTED_NOD_ROWS, EXPECTED_NOD_RIDS) and (n_rep, r_rep) == (EXPECTED_REP_ROWS, EXPECTED_REP_RIDS)}")
    log(f"  rids_only_in_reports (no scorable nodule, expected ~1,052): {rids_only_in_reports}")
    log(f"  already_applied:    {out['already_applied']}")
    log("──────────────────────────────")
    return out


# ── PHASE 1 — load raw tables ────────────────────────────────────────────────

def phase_1(con: duckdb.DuckDBPyConnection) -> dict:
    log("=== PHASE 1 — load raw tables into canonical ===")
    out: dict = {"phase": 1, "ok": True}
    nod_path = str(NOD_PARQUET)
    rep_path = str(REP_PARQUET)

    con.execute(
        f"CREATE OR REPLACE TABLE {NOD_TABLE} AS SELECT * FROM read_parquet('{nod_path}')"
    )
    con.execute(
        f"CREATE OR REPLACE TABLE {REP_TABLE} AS SELECT * FROM read_parquet('{rep_path}')"
    )

    for tbl, exp_rows, exp_rids in (
        (NOD_TABLE, EXPECTED_NOD_ROWS, EXPECTED_NOD_RIDS),
        (REP_TABLE, EXPECTED_REP_ROWS, EXPECTED_REP_RIDS),
    ):
        n, r = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {tbl}"
        ).fetchone()
        log(f"  {tbl}: {n:,} rows, {r:,} RIDs (expected {exp_rows:,}/{exp_rids:,})")
        out[tbl] = {"rows": int(n), "rids": int(r)}
        if (n, r) != (exp_rows, exp_rids):
            out["ok"] = False
            out.setdefault("blockers", []).append(
                f"{tbl}: {n}/{r} != expected {exp_rows}/{exp_rids}"
            )

    nod_comment = (
        "Per-nodule ACR/ATA/Kwak/EU TIRADS extraction by Qwen2.5-32B-Instruct-AWQ "
        "(vLLM on Vast.ai H200) from 8,810 US reports. 11,914 rows / 3,021 RIDs "
        "after sanitize (LN-leak filter, fossa filter, unit rescaler). SUBSUMES "
        "legacy main.tirads_llm_extracted_v2 RID set (1,429 RIDs / 1,429 confirmed "
        "subset present in this table). Methodological cross-run concordance: "
        f"manuscript_workspace.tirads_llm_haiku_vs_qwen_v1. Commit {SOURCE_COMMIT} "
        f"{RUN_DATE} ({SCRIPT_TAG})."
    ).replace("'", "''")
    rep_comment = (
        "Per-report TIRADS metadata (overall_recommendation, suspicious_ln_present, "
        "dominant_nodule_id_by_radiologist, report_impression_text) from same v2 "
        f"extraction. Commit {SOURCE_COMMIT} {RUN_DATE}."
    ).replace("'", "''")
    try:
        con.execute(f"COMMENT ON TABLE {NOD_TABLE} IS '{nod_comment}'")
        con.execute(f"COMMENT ON TABLE {REP_TABLE} IS '{rep_comment}'")
        log("  COMMENT ON TABLE applied to both raw tables.")
    except Exception as e:
        log(f"  COMMENT ON TABLE skipped (driver issue): {e!r}")
        out["comment_warning"] = repr(e)

    return out


# ── PHASE 2 — patient-level rollups ─────────────────────────────────────────

def phase_2(con: duckdb.DuckDBPyConnection) -> dict:
    log("=== PHASE 2 — build patient-level rollups ===")
    out: dict = {"phase": 2, "ok": True}

    con.execute(f"""
        CREATE OR REPLACE TABLE {NOD_ROLLUP} AS
        WITH scored AS (
          SELECT * FROM {NOD_TABLE}
          WHERE tirads_category IS NOT NULL
        ),
        ranked AS (
          SELECT
            research_id, tirads_category,
            CASE tirads_category
              WHEN 'TR1' THEN 1 WHEN 'TR2' THEN 2 WHEN 'TR3' THEN 3
              WHEN 'TR4' THEN 4 WHEN 'TR5' THEN 5
            END AS tr_rank,
            tirads_total_points, size_cm_max, composition, echogenicity,
            shape, margin, echogenic_foci, extrathyroidal_extension_on_us,
            interval_growth_flag, fna_recommended_this_nodule
          FROM scored
        )
        SELECT
          research_id,
          COUNT(*)                                             AS tirads_v2_n_nodules_scored,
          MAX(tr_rank)                                         AS tirads_v2_worst_rank,
          MAX(tirads_category)                                 AS tirads_v2_worst_category,
          MAX(tirads_total_points)                             AS tirads_v2_max_points,
          MAX(size_cm_max)                                     AS tirads_v2_largest_nodule_cm,
          MAX(CASE WHEN extrathyroidal_extension_on_us IN ('suspected','definite') THEN 1 ELSE 0 END)::BOOLEAN AS tirads_v2_any_ete_on_us,
          MAX(CASE WHEN interval_growth_flag = TRUE THEN 1 ELSE 0 END)::BOOLEAN                                AS tirads_v2_any_interval_growth,
          MAX(CASE WHEN fna_recommended_this_nodule = TRUE THEN 1 ELSE 0 END)::BOOLEAN                         AS tirads_v2_any_fna_recommended
        FROM ranked
        GROUP BY research_id
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE {REP_ROLLUP} AS
        SELECT
          research_id,
          COUNT(*)                                                                                                   AS tirads_v2_n_reports,
          MAX(CASE WHEN suspicious_ln_present = TRUE THEN 1 ELSE 0 END)::BOOLEAN                                     AS tirads_v2_any_suspicious_ln_on_us,
          MAX(CASE WHEN overall_recommendation = 'fna' THEN 1 ELSE 0 END)::BOOLEAN                                   AS tirads_v2_any_fna_recommended_report,
          MIN(follow_up_interval_months)                                                                             AS tirads_v2_shortest_followup_months
        FROM {REP_TABLE}
        GROUP BY research_id
    """)

    for tbl in (NOD_ROLLUP, REP_ROLLUP):
        n_rows, n_dist = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {tbl}"
        ).fetchone()
        log(f"  {tbl}: {n_rows:,} rows, {n_dist:,} distinct RIDs")
        out[tbl] = {"rows": int(n_rows), "distinct_rids": int(n_dist)}
        if n_rows != n_dist:
            out["ok"] = False
            out.setdefault("blockers", []).append(
                f"{tbl}: rows ({n_rows}) != distinct rids ({n_dist}) — rollup is not 1/RID."
            )

    n_nod_rollup = out[NOD_ROLLUP]["rows"]
    n_rep_rollup = out[REP_ROLLUP]["rows"]
    # Nodule rollup filters tirads_category IS NOT NULL (per prompt SQL), so its row
    # count is patients-with-≥1-scored-nodule, NOT raw nodule RID count (3,021).
    # Real invariant is 1-row-per-RID (already checked above) and a sane lower bound.
    if n_nod_rollup > EXPECTED_NOD_RIDS:
        out["ok"] = False
        out.setdefault("blockers", []).append(
            f"{NOD_ROLLUP} rows={n_nod_rollup} > raw RID count {EXPECTED_NOD_RIDS} (impossible)"
        )
    n_unscored_rids = EXPECTED_NOD_RIDS - n_nod_rollup
    log(f"  unscored-nodule-only RIDs (in raw, missing from rollup): {n_unscored_rids:,} "
        "(nodules with ZERO TR1-TR5 categories — expected: spongiform/indeterminate/etc)")
    out["nodule_rollup_unscored_only_rids"] = int(n_unscored_rids)

    # Reports rollup has no NOT NULL filter — must hit the full RID count.
    if n_rep_rollup != EXPECTED_REP_RIDS:
        out["ok"] = False
        out.setdefault("blockers", []).append(
            f"{REP_ROLLUP} rows={n_rep_rollup} != expected {EXPECTED_REP_RIDS}"
        )
    return out


# ── PHASE 3 — augment canonical_patient_master ──────────────────────────────

def phase_3(con: duckdb.DuckDBPyConnection) -> dict:
    log("=== PHASE 3 — add tirads_v2_* columns to canonical_patient_master ===")
    out: dict = {"phase": 3, "ok": True, "added": [], "preexisting": []}

    legacy_protected = (
        "preop_tirads_best", "tirads_best_score_v12", "tirads_worst_combined",
        "tirads_best_category_v12", "tirads_worst_category_v12",
    )
    log("  legacy TIRADS columns are PROTECTED — not touched by this script: "
        + ", ".join(legacy_protected))

    for col, dtype in NEW_CPM_COLS:
        if col_exists(con, "canonical_patient_master", col):
            out["preexisting"].append(col)
            log(f"  · {col} already present, skipping ADD.")
            continue
        con.execute(
            f"ALTER TABLE canonical_patient_master ADD COLUMN IF NOT EXISTS {col} {dtype}"
        )
        out["added"].append(col)
        log(f"  + ADD {col} {dtype}")

    log("  populating from rollups (LEFT JOIN on research_id, both VARCHAR) ...")
    con.execute(f"""
        UPDATE canonical_patient_master AS m
        SET
          tirads_v2_n_nodules_scored    = n.tirads_v2_n_nodules_scored,
          tirads_v2_worst_category      = n.tirads_v2_worst_category,
          tirads_v2_max_points          = n.tirads_v2_max_points,
          tirads_v2_largest_nodule_cm   = n.tirads_v2_largest_nodule_cm,
          tirads_v2_any_ete_on_us       = n.tirads_v2_any_ete_on_us,
          tirads_v2_any_interval_growth = n.tirads_v2_any_interval_growth,
          tirads_v2_any_fna_recommended = n.tirads_v2_any_fna_recommended
        FROM {NOD_ROLLUP} AS n
        WHERE m.research_id = n.research_id
    """)
    n_nod_touched = con.execute(
        f"SELECT COUNT(*) FROM canonical_patient_master m "
        f"JOIN {NOD_ROLLUP} n USING (research_id)"
    ).fetchone()[0]
    log(f"    nodule-rollup join coverage: {n_nod_touched:,} CPM rows (of {EXPECTED_CPM_ROWS:,})")
    out["nodule_rollup_join_rows"] = int(n_nod_touched)

    con.execute(f"""
        UPDATE canonical_patient_master AS m
        SET
          tirads_v2_n_reports                = r.tirads_v2_n_reports,
          tirads_v2_any_suspicious_ln_on_us  = r.tirads_v2_any_suspicious_ln_on_us,
          tirads_v2_shortest_followup_months = r.tirads_v2_shortest_followup_months
        FROM {REP_ROLLUP} AS r
        WHERE m.research_id = r.research_id
    """)
    n_rep_touched = con.execute(
        f"SELECT COUNT(*) FROM canonical_patient_master m "
        f"JOIN {REP_ROLLUP} r USING (research_id)"
    ).fetchone()[0]
    log(f"    report-rollup join coverage: {n_rep_touched:,} CPM rows (of {EXPECTED_CPM_ROWS:,})")
    out["report_rollup_join_rows"] = int(n_rep_touched)

    # Provenance: stamp cpm_built_at for every CPM row touched by this run.
    if col_exists(con, "canonical_patient_master", "cpm_built_at"):
        con.execute(f"""
            UPDATE canonical_patient_master AS m
            SET cpm_built_at = CURRENT_TIMESTAMP
            WHERE m.research_id IN (
              SELECT research_id FROM {NOD_ROLLUP}
              UNION
              SELECT research_id FROM {REP_ROLLUP}
            )
        """)
        n_stamped = con.execute(
            f"SELECT COUNT(*) FROM canonical_patient_master m "
            f"WHERE m.research_id IN ("
            f"  SELECT research_id FROM {NOD_ROLLUP} "
            f"  UNION SELECT research_id FROM {REP_ROLLUP})"
        ).fetchone()[0]
        log(f"  cpm_built_at stamped on {n_stamped:,} rows touched by v2 rollups.")
        out["cpm_built_at_stamped"] = int(n_stamped)
    else:
        log("  cpm_built_at column absent — provenance stamp skipped.")
        out["cpm_built_at_stamped"] = 0

    # Insert per-engagement provenance row (per AGENTS.md cleanup convention).
    if table_exists(con, "cpm_reconciliation_provenance_v1", schema="manuscript_workspace"):
        try:
            run_id = f"tirads_v2_integration_{RUN_DATE.replace('-', '')}_phase3"
            con.execute(
                """
                INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
                  (run_id, started_at, ended_at, phases_applied,
                   critical_findings_cleared, high_findings_cleared,
                   med_findings_cleared, held_for_adjudication)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    RUN_TS_ISO,
                    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "tirads_v2_integration:cpm_augment",
                    "0", "0", "0",
                    "v2_v_legacy_concordance_review",
                ],
            )
            log(f"  cpm_reconciliation_provenance_v1: inserted '{run_id}'")
            out["provenance_run_id"] = run_id
        except Exception as e:
            log(f"  provenance insert skipped: {e!r}")
            out["provenance_warning"] = repr(e)
    else:
        log("  manuscript_workspace.cpm_reconciliation_provenance_v1 absent — provenance row skipped.")

    # Final invariants
    n_cpm, n_dist = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM canonical_patient_master"
    ).fetchone()
    log(f"  invariants: rows={n_cpm:,} distinct={n_dist:,}")
    if n_cpm != EXPECTED_CPM_ROWS or n_dist != EXPECTED_CPM_ROWS:
        out["ok"] = False
        out.setdefault("blockers", []).append(
            f"CPM invariants violated: rows={n_cpm} distinct={n_dist}"
        )
    out["cpm_rows"] = int(n_cpm)
    out["cpm_distinct_rids"] = int(n_dist)
    return out


# ── PHASE 4 — archive deprecated legacy table ───────────────────────────────

def phase_4(con: duckdb.DuckDBPyConnection, audit: dict) -> dict:
    log("=== PHASE 4 — archive deprecated tirads_llm_extracted_v2 ===")
    out: dict = {"phase": 4, "ok": True}

    if not audit.get("legacy_table_present"):
        log("  legacy table absent — nothing to archive. SKIP.")
        out["action"] = "skip_legacy_absent"
        return out

    if not audit.get("phase4_drop_safe"):
        msg = (
            f"Phase 4 DROP refused: {audit.get('legacy_rids_lost_in_new', '?')} RIDs "
            "from legacy tirads_llm_extracted_v2 are NOT covered by the new extraction. "
            "Manual review required (per HARD RULES)."
        )
        log("  ⚠ " + msg)
        out["ok"] = False
        out["action"] = "skip_rid_loss"
        out.setdefault("blockers", []).append(msg)
        return out

    # Idempotency — if archive already exists, do not re-create / drop again.
    archive_already = con.execute(
        "SELECT 1 FROM information_schema.tables "
        f"WHERE table_catalog='{ARCHIVE_DB}' AND table_schema='main' "
        f"AND table_name='{ARCHIVE_TABLE}'"
    ).fetchone() is not None
    if archive_already:
        log(f"  archive table {ARCHIVE_DB!r}.main.{ARCHIVE_TABLE} already exists — "
            "leaving as-is and re-attempting DROP if legacy still present.")
        out["archive_preexisting"] = True
    else:
        log(f"  CREATE archive table {ARCHIVE_DB!r}.main.{ARCHIVE_TABLE} from legacy ...")
        con.execute(
            f'CREATE TABLE "{ARCHIVE_DB}".main.{ARCHIVE_TABLE} AS '
            f'SELECT * FROM {CANONICAL_DB}.main.{LEGACY_TABLE}'
        )
        n_arch = con.execute(
            f'SELECT COUNT(*) FROM "{ARCHIVE_DB}".main.{ARCHIVE_TABLE}'
        ).fetchone()[0]
        log(f"  archive populated: {n_arch:,} rows")
        out["archive_rows"] = int(n_arch)

    log(f"  DROP {CANONICAL_DB}.main.{LEGACY_TABLE} ...")
    con.execute(f"DROP TABLE IF EXISTS {CANONICAL_DB}.main.{LEGACY_TABLE}")
    still_there = table_exists(con, LEGACY_TABLE)
    if still_there:
        out["ok"] = False
        out.setdefault("blockers", []).append(
            f"DROP of {LEGACY_TABLE} did not remove the table"
        )
    else:
        log(f"  ✓ {LEGACY_TABLE} dropped from canonical.")
    out["legacy_dropped"] = not still_there
    return out


# ── PHASE 5 — registry + __readme sync ──────────────────────────────────────

REGISTRY_DESC_NOD = (
    "Per-nodule ACR/ATA/Kwak/EU TIRADS extraction by Qwen2.5-32B-Instruct-AWQ "
    "(vLLM/Vast.ai H200; 0 parse errors across 8,810 US reports). Drill-down "
    "for tirads_v2_worst_category / tirads_v2_max_points / tirads_v2_largest_nodule_cm / "
    "tirads_v2_any_ete_on_us / tirads_v2_any_interval_growth / tirads_v2_any_fna_recommended."
)
REGISTRY_DESC_REP = (
    "Per-US-report TIRADS metadata (overall_recommendation, suspicious_ln_present, "
    "dominant_nodule_id_by_radiologist, report_impression_text). Drill-down for "
    "tirads_v2_n_reports / tirads_v2_any_suspicious_ln_on_us / tirads_v2_shortest_followup_months."
)


COMPARISON_VIEW = "tirads_llm_haiku_vs_qwen_v1"

LEGACY_COMMENT = (
    "EARLY-EXTRACTION partial run (1,429 RIDs) via Anthropic Haiku 4.5, 2026-04, "
    "from raw_imaging_12_slots_v1. research_id is BIGINT. STRICT SUBSET of "
    "tirads_v2_nodules_raw RID coverage (audit 2026-04-18 Script 221: 1,429/1,429 "
    "RIDs present in v2 / 3,021 total). Retained for Haiku-vs-Qwen methodological "
    "comparison via manuscript_workspace.tirads_llm_haiku_vs_qwen_v1. NOT a "
    "canonical clinical signal — for CPM-bound TIRADS use tirads_v2_* columns."
)


def _phase_5_haiku_vs_qwen_view(con: duckdb.DuckDBPyConnection, out: dict) -> None:
    """Phase 5 sub-step: build a side-by-side per-patient comparison view in
    ``manuscript_workspace`` joining the legacy Haiku rollup against the new
    Qwen v2 nodule rollup. ANALYTICAL ONLY — does NOT feed any CPM column
    (legacy is a strict subset of v2; an ``any_worst_category`` CPM column
    would duplicate ``tirads_v2_worst_category``).
    """
    legacy_present = table_exists(con, LEGACY_TABLE)
    v2_present = table_exists(con, NOD_ROLLUP)
    if not (legacy_present and v2_present):
        log(f"  Haiku-vs-Qwen view skipped: legacy_present={legacy_present} "
            f"v2_present={v2_present} (need both).")
        return

    con.execute(
        f"""CREATE OR REPLACE VIEW manuscript_workspace.{COMPARISON_VIEW} AS
        WITH haiku AS (
          SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            COUNT(*) FILTER (WHERE tirads_level_2017 IS NOT NULL)             AS n_nodules_scored_haiku,
            MAX(CASE tirads_level_2017
                  WHEN 'TR1' THEN 1 WHEN 'TR2' THEN 2 WHEN 'TR3' THEN 3
                  WHEN 'TR4' THEN 4 WHEN 'TR5' THEN 5
                END)                                                          AS haiku_worst_rank,
            MAX(tirads_level_2017)                                            AS haiku_worst_category
          FROM main.{LEGACY_TABLE}
          GROUP BY 1
        ),
        qwen AS (
          SELECT
            research_id,
            tirads_v2_n_nodules_scored AS n_nodules_scored_qwen,
            tirads_v2_worst_rank       AS qwen_worst_rank,
            tirads_v2_worst_category   AS qwen_worst_category
          FROM main.{NOD_ROLLUP}
        )
        SELECT
          COALESCE(h.research_id, q.research_id) AS research_id,
          h.n_nodules_scored_haiku,
          h.haiku_worst_category,
          h.haiku_worst_rank,
          q.n_nodules_scored_qwen,
          q.qwen_worst_category,
          q.qwen_worst_rank,
          CASE
            WHEN h.research_id IS NULL                                                THEN 'qwen_only'
            WHEN q.research_id IS NULL                                                THEN 'haiku_only'
            WHEN h.haiku_worst_rank IS NULL OR q.qwen_worst_rank IS NULL              THEN 'one_run_unscored'
            WHEN h.haiku_worst_category = q.qwen_worst_category                       THEN 'agree'
            ELSE 'disagree'
          END                                                                         AS concordance_class,
          COALESCE(q.qwen_worst_rank, 0) - COALESCE(h.haiku_worst_rank, 0)            AS qwen_minus_haiku_rank
        FROM haiku h
        FULL OUTER JOIN qwen q USING (research_id)
        """
    )
    n_view = con.execute(
        f"SELECT COUNT(*) FROM manuscript_workspace.{COMPARISON_VIEW}"
    ).fetchone()[0]
    cls_rows = con.execute(
        f"SELECT concordance_class, COUNT(*) FROM manuscript_workspace.{COMPARISON_VIEW} "
        "GROUP BY 1 ORDER BY 1"
    ).fetchall()
    log(f"  view manuscript_workspace.{COMPARISON_VIEW}: {n_view:,} rows")
    for cls, n in cls_rows:
        log(f"        concordance_class={cls!s:18s}  n={n:,}")
    out["haiku_vs_qwen_view"] = {
        "name": f"manuscript_workspace.{COMPARISON_VIEW}",
        "rows": int(n_view),
        "concordance_breakdown": [{"class": c, "n": int(n)} for c, n in cls_rows],
    }
    try:
        con.execute(
            f"COMMENT ON VIEW manuscript_workspace.{COMPARISON_VIEW} IS "
            f"'Per-patient side-by-side TIRADS comparison: legacy Haiku 4.5 "
            f"(tirads_llm_extracted_v2) FULL OUTER JOIN new Qwen2.5-32B "
            f"({NOD_ROLLUP}). Analytical view for Haiku-vs-Qwen methodological "
            f"concordance — NOT a canonical clinical signal. Built {RUN_DATE} ({SCRIPT_TAG}).'"
        )
    except Exception as e:
        log(f"  COMMENT ON VIEW skipped: {e!r}")


def phase_5(con: duckdb.DuckDBPyConnection) -> dict:
    log("=== PHASE 5 — registry + __readme sync + unified-rollup view ===")
    out: dict = {"phase": 5, "ok": True}

    # 5.0 — refresh COMMENT ON TABLE on legacy + new (clarifier wording)
    if table_exists(con, LEGACY_TABLE):
        try:
            safe = LEGACY_COMMENT.replace("'", "''")
            con.execute(f"COMMENT ON TABLE {LEGACY_TABLE} IS '{safe}'")
            log(f"  COMMENT ON TABLE {LEGACY_TABLE} (early-cohort clarifier) applied.")
        except Exception as e:
            log(f"  COMMENT on legacy skipped: {e!r}")
            out["legacy_comment_warning"] = repr(e)
    if table_exists(con, NOD_TABLE):
        try:
            con.execute(
                f"COMMENT ON TABLE {NOD_TABLE} IS "
                f"'Per-nodule ACR/ATA/Kwak/EU TIRADS extraction by Qwen2.5-32B-Instruct-AWQ "
                f"(vLLM, Vast.ai H200), commit {SOURCE_COMMIT} {RUN_DATE}. "
                f"11,914 rows / 3,021 RIDs after sanitize. SUBSUMES legacy "
                f"main.tirads_llm_extracted_v2 RID set (1,429/1,429 in v2 union; "
                f"~1,110 also scored by v2 nodule rollup). Cross-run concordance: "
                f"manuscript_workspace.{COMPARISON_VIEW}.'"
            )
        except Exception as e:
            log(f"  COMMENT refresh on {NOD_TABLE} skipped: {e!r}")

    # 5.1 — Haiku-vs-Qwen analytical view (manuscript_workspace; NO CPM writes).
    _phase_5_haiku_vs_qwen_view(con, out)

    # 5A — detail_table_registry_v1 (manuscript_workspace)
    has_registry = con.execute(
        "SELECT 1 FROM information_schema.tables "
        f"WHERE table_catalog='{CANONICAL_DB}' AND table_schema='manuscript_workspace' "
        "AND table_name='detail_table_registry_v1'"
    ).fetchone() is not None
    if not has_registry:
        log("  manuscript_workspace.detail_table_registry_v1 absent — registry sync skipped.")
        out["registry_skipped_no_table"] = True
    else:
        n_nod_rows, n_nod_pts = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {NOD_TABLE}"
        ).fetchone()
        n_rep_rows, n_rep_pts = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {REP_TABLE}"
        ).fetchone()

        registry_rows = [
            (
                NOD_TABLE, "main",
                "research_id; note_row_id; nodule_id",
                "one row per US nodule per report (post-sanitize)",
                int(n_nod_rows), int(n_nod_pts),
                "Imaging",
                "feeds canonical_patient_master cols tirads_v2_n_nodules_scored, "
                "tirads_v2_worst_category, tirads_v2_max_points, tirads_v2_largest_nodule_cm, "
                "tirads_v2_any_ete_on_us, tirads_v2_any_interval_growth, "
                "tirads_v2_any_fna_recommended (via tirads_v2_nodule_patient_rollup_v1)",
                REGISTRY_DESC_NOD + f" Built {RUN_DATE} ({SCRIPT_TAG}; commit {SOURCE_COMMIT}).",
                "v1_0",
            ),
            (
                REP_TABLE, "main",
                "research_id; note_row_id",
                "one row per US report",
                int(n_rep_rows), int(n_rep_pts),
                "Imaging",
                "feeds canonical_patient_master cols tirads_v2_n_reports, "
                "tirads_v2_any_suspicious_ln_on_us, tirads_v2_shortest_followup_months "
                "(via tirads_v2_report_patient_rollup_v1)",
                REGISTRY_DESC_REP + f" Built {RUN_DATE} ({SCRIPT_TAG}; commit {SOURCE_COMMIT}).",
                "v1_0",
            ),
        ]
        # Haiku-vs-Qwen analytical view registry entry (audit-only, no CPM feed).
        if table_exists(con, COMPARISON_VIEW, schema="manuscript_workspace"):
            n_view_rows, n_view_pts = con.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT research_id) "
                f"FROM manuscript_workspace.{COMPARISON_VIEW}"
            ).fetchone()
            registry_rows.append((
                COMPARISON_VIEW, "manuscript_workspace",
                "research_id",
                "one row per RID present in either run (FULL OUTER JOIN haiku ⨝ qwen)",
                int(n_view_rows), int(n_view_pts),
                "Imaging/Audit",
                "(audit only, no canonical column) — Haiku-vs-Qwen TIRADS methodological "
                "comparison; legacy is a strict subset of v2 RID coverage so this view "
                "exists for cross-run concordance, NOT for CPM population.",
                "Per-patient side-by-side legacy Haiku vs new Qwen TIRADS comparison "
                f"({LEGACY_TABLE} ⨝ {NOD_ROLLUP}); built {RUN_DATE} ({SCRIPT_TAG}; "
                f"commit {SOURCE_COMMIT}). Concordance classes: agree / disagree / "
                "haiku_only / qwen_only / one_run_unscored.",
                "v1_0",
            ))

        # Idempotent upsert: DELETE then INSERT.
        con.execute(
            "DELETE FROM manuscript_workspace.detail_table_registry_v1 "
            "WHERE detail_table_name IN (?, ?, ?)",
            [NOD_TABLE, REP_TABLE, COMPARISON_VIEW],
        )
        con.executemany(
            "INSERT INTO manuscript_workspace.detail_table_registry_v1 "
            "(detail_table_name, schema_name, join_key, grain, total_rows, total_patients, "
            " domain, feeds_master_columns, description, canonical_version) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            registry_rows,
        )
        # Do NOT delete legacy registry pointer — legacy table still in canonical
        # (Phase 4 did not run; legacy retained for Haiku-vs-Qwen cross-run audit).
        n_reg = con.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.detail_table_registry_v1 "
            "WHERE detail_table_name IN (?, ?, ?)",
            [NOD_TABLE, REP_TABLE, COMPARISON_VIEW],
        ).fetchone()[0]
        log(f"  registry: upserted {n_reg} entries (raw nodules, raw reports, comparison view).")
        out["registry_upserted"] = int(n_reg)

    # 5B — main.__readme inventory
    has_readme = table_exists(con, "__readme")
    if not has_readme:
        log("  main.__readme absent — readme sync skipped.")
        out["readme_skipped_no_table"] = True
        return out

    rd_cols = [
        r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_catalog='{CANONICAL_DB}' AND table_schema='main' "
            "AND table_name='__readme' ORDER BY ordinal_position"
        ).fetchall()
    ]
    out["readme_cols"] = rd_cols
    log(f"  __readme columns: {rd_cols}")

    expected_readme_cols = {"table_name", "n_rows", "n_distinct_research_id",
                            "description", "inventoried_at"}
    if expected_readme_cols.issubset(set(rd_cols)):
        try:
            keys = [
                NOD_TABLE,
                REP_TABLE,
                NOD_ROLLUP,
                REP_ROLLUP,
                COMPARISON_VIEW,           # manuscript_workspace view, just table name
                LEGACY_TABLE,
            ]
            con.execute(
                f'DELETE FROM "__readme" WHERE table_name IN ({",".join(["?"] * len(keys))})',
                keys,
            )

            def _meta(table: str, schema: str = "main") -> tuple[int, int]:
                row = con.execute(
                    f'SELECT COUNT(*), COUNT(DISTINCT research_id) FROM "{schema}"."{table}"'
                ).fetchone()
                return int(row[0] or 0), int(row[1] or 0)

            n_nod_rows, n_nod_rids = _meta(NOD_TABLE)
            n_rep_rows, n_rep_rids = _meta(REP_TABLE)
            n_nod_rl_rows, n_nod_rl_rids = _meta(NOD_ROLLUP)
            n_rep_rl_rows, n_rep_rl_rids = _meta(REP_ROLLUP)
            n_view_rows, n_view_rids = (0, 0)
            if table_exists(con, COMPARISON_VIEW, schema="manuscript_workspace"):
                n_view_rows, n_view_rids = _meta(COMPARISON_VIEW, schema="manuscript_workspace")
            n_leg_rows, n_leg_rids = (0, 0)
            if table_exists(con, LEGACY_TABLE):
                row = con.execute(
                    f"SELECT COUNT(*), COUNT(DISTINCT CAST(research_id AS VARCHAR)) "
                    f"FROM {LEGACY_TABLE}"
                ).fetchone()
                n_leg_rows, n_leg_rids = int(row[0]), int(row[1])

            rows_to_insert = [
                (NOD_TABLE, n_nod_rows, n_nod_rids,
                 (f"Raw per-nodule v2 TIRADS extraction (Qwen2.5-32B-Instruct-AWQ, "
                  f"vLLM, Vast.ai H200, commit {SOURCE_COMMIT}); SUBSUMES legacy "
                  "tirads_llm_extracted_v2 RID set. Built " + RUN_DATE + " by " + SCRIPT_TAG + "."),
                 RUN_TS_ISO),
                (REP_TABLE, n_rep_rows, n_rep_rids,
                 (f"Raw per-US-report v2 TIRADS metadata (overall_recommendation, "
                  "suspicious_ln_present, dominant_nodule_id_by_radiologist, "
                  f"report_impression_text). Built {RUN_DATE} by {SCRIPT_TAG} "
                  f"(commit {SOURCE_COMMIT})."),
                 RUN_TS_ISO),
                (NOD_ROLLUP, n_nod_rl_rows, n_nod_rl_rids,
                 (f"Patient-level rollup of {NOD_TABLE} (filtered to scorable nodules); "
                  "feeds canonical_patient_master.tirads_v2_n_nodules_scored, "
                  "tirads_v2_worst_category, tirads_v2_max_points, "
                  "tirads_v2_largest_nodule_cm, tirads_v2_any_ete_on_us, "
                  "tirads_v2_any_interval_growth, tirads_v2_any_fna_recommended."),
                 RUN_TS_ISO),
                (REP_ROLLUP, n_rep_rl_rows, n_rep_rl_rids,
                 (f"Patient-level rollup of {REP_TABLE}; feeds canonical_patient_master."
                  "tirads_v2_n_reports, tirads_v2_any_suspicious_ln_on_us, "
                  "tirads_v2_shortest_followup_months."),
                 RUN_TS_ISO),
                (COMPARISON_VIEW, n_view_rows, n_view_rids,
                 ("ANALYTICAL VIEW (manuscript_workspace) — per-patient side-by-side "
                  "legacy Haiku 4.5 vs new Qwen2.5-32B TIRADS comparison; "
                  "concordance_class in {agree, disagree, haiku_only, qwen_only, "
                  "one_run_unscored}. Audit-only, no CPM feed."),
                 RUN_TS_ISO),
                (LEGACY_TABLE, n_leg_rows, n_leg_rids,
                 ("EARLY-EXTRACTION partial run (1,429 RIDs) via Anthropic Haiku 4.5, "
                  "2026-04. research_id is BIGINT. STRICT SUBSET of tirads_v2_nodules_raw "
                  f"RID coverage (audit {RUN_DATE} {SCRIPT_TAG}). Retained for "
                  f"Haiku-vs-Qwen methodological comparison via manuscript_workspace."
                  f"{COMPARISON_VIEW}; NOT a canonical clinical signal."),
                 RUN_TS_ISO),
            ]
            con.executemany(
                'INSERT INTO "__readme" '
                "(table_name, n_rows, n_distinct_research_id, description, inventoried_at) "
                "VALUES (?, ?, ?, ?, ?)",
                rows_to_insert,
            )
            log(f"  __readme rows upserted (deleted+inserted): {len(rows_to_insert)} entries.")
            out["readme_rows_refreshed"] = len(rows_to_insert)
        except Exception as e:
            log(f"  __readme update failed: {e!r}")
            out["readme_warning"] = repr(e)
    else:
        log(f"  __readme schema mismatch (got {rd_cols}, expected {sorted(expected_readme_cols)}) — skipping.")
        out["readme_skipped_schema"] = True

    return out


# ── PHASE 6 — final validation ──────────────────────────────────────────────

def phase_6(con: duckdb.DuckDBPyConnection) -> dict:
    log("=== PHASE 6 — final validation ===")
    out: dict = {"phase": 6, "ok": True}

    inv = con.execute("""
        SELECT
          COUNT(*)                                                          AS total,
          COUNT(DISTINCT research_id)                                       AS distinct_rids,
          COUNT(*) FILTER (WHERE research_id IS NULL)                       AS null_rids,
          COUNT(*) FILTER (WHERE tirads_v2_worst_category IS NOT NULL)      AS has_v2_tirads_cat,
          COUNT(*) FILTER (WHERE tirads_v2_any_suspicious_ln_on_us IS NOT NULL) AS has_v2_ln_flag
        FROM canonical_patient_master
    """).fetchone()
    cols = [d[0] for d in con.description]
    inv_d = dict(zip(cols, inv))
    out["invariants"] = {k: int(v) for k, v in inv_d.items()}
    log(f"  invariants: {inv_d}")
    if inv_d["total"] != EXPECTED_CPM_ROWS or inv_d["distinct_rids"] != EXPECTED_CPM_ROWS:
        out["ok"] = False
        out.setdefault("blockers", []).append(
            f"CPM invariants: total={inv_d['total']} distinct={inv_d['distinct_rids']}"
        )
    if inv_d["null_rids"] != 0:
        out["ok"] = False
        out.setdefault("blockers", []).append(
            f"CPM has {inv_d['null_rids']} NULL research_id rows."
        )
    # has_v2_tirads_cat = patients-with-≥1-scored-nodule (filtered rollup); should
    # equal NOD_ROLLUP row count, NOT the raw 3,021 RIDs in nodules.parquet.
    if table_exists(con, NOD_ROLLUP):
        n_nod_rollup_rows = con.execute(
            f"SELECT COUNT(*) FROM {NOD_ROLLUP}"
        ).fetchone()[0]
        if inv_d["has_v2_tirads_cat"] != n_nod_rollup_rows:
            log(f"  ⚠ has_v2_tirads_cat={inv_d['has_v2_tirads_cat']} != nodule-rollup rows "
                f"({n_nod_rollup_rows}); investigate.")
            out.setdefault("warnings", []).append(
                f"has_v2_tirads_cat ({inv_d['has_v2_tirads_cat']}) != nodule-rollup rows "
                f"({n_nod_rollup_rows})"
            )
    if inv_d["has_v2_ln_flag"] != EXPECTED_REP_RIDS:
        log(f"  ⚠ has_v2_ln_flag={inv_d['has_v2_ln_flag']} (expected {EXPECTED_REP_RIDS}) "
            "— may indicate RIDs in reports.parquet absent from canonical CPM.")
        out.setdefault("warnings", []).append(
            f"has_v2_ln_flag={inv_d['has_v2_ln_flag']} != expected {EXPECTED_REP_RIDS}"
        )

    # Haiku-vs-Qwen analytical view spot-check
    if table_exists(con, COMPARISON_VIEW, schema="manuscript_workspace"):
        cls_rows = con.execute(
            f"SELECT concordance_class, COUNT(*) "
            f"FROM manuscript_workspace.{COMPARISON_VIEW} "
            "GROUP BY 1 ORDER BY 1"
        ).fetchall()
        log(f"  manuscript_workspace.{COMPARISON_VIEW} concordance breakdown:")
        for cls, n in cls_rows:
            log(f"        {cls!s:18s}  n={n:,}")
        out["haiku_vs_qwen_concordance"] = [
            {"class": c, "n": int(n)} for c, n in cls_rows
        ]
        # Every legacy RID should land in agree / disagree / haiku_only / one_run_unscored.
        n_legacy_in_view = con.execute(
            f"SELECT COUNT(*) FROM manuscript_workspace.{COMPARISON_VIEW} "
            "WHERE concordance_class IN ('agree', 'disagree', 'haiku_only', 'one_run_unscored')"
        ).fetchone()[0]
        log(f"  legacy RIDs accounted for in view: {n_legacy_in_view:,} (expected 1,429)")
        out["haiku_rids_in_view"] = int(n_legacy_in_view)
        if n_legacy_in_view != 1429:
            out.setdefault("warnings", []).append(
                f"haiku_rids_in_view={n_legacy_in_view} != 1429"
            )

    # Concordance vs legacy preop_tirads_best
    if col_exists(con, "canonical_patient_master", "preop_tirads_best"):
        rows = con.execute("""
            SELECT
              tirads_v2_worst_category, preop_tirads_best, COUNT(*) AS n
            FROM canonical_patient_master
            WHERE tirads_v2_worst_category IS NOT NULL
              AND preop_tirads_best IS NOT NULL
            GROUP BY 1, 2
            ORDER BY 1, 2
        """).fetchall()
        out["concordance_v2_vs_preop_tirads_best"] = [
            {"v2": r[0], "legacy": r[1], "n": int(r[2])} for r in rows
        ]
        log(f"  concordance v2 vs preop_tirads_best: {len(rows)} cells")
        for r in rows[:25]:
            log(f"        v2={r[0]:5s}  legacy={str(r[1]):20s}  n={int(r[2]):>5d}")
        if len(rows) > 25:
            log(f"        … ({len(rows) - 25} more cells; see JSON output)")
    else:
        log("  preop_tirads_best column absent — concordance check skipped.")
        out["concordance_skipped"] = True

    return out


# ── orchestrator ─────────────────────────────────────────────────────────────

PHASES = ("0", "1", "2", "3", "4", "5", "6", "all")


def main() -> int:
    ap = argparse.ArgumentParser(description="Script 221 — TIRADS v2 integration into canonical")
    ap.add_argument("--phase", choices=PHASES, default="0",
                    help="phase to run (0=audit, 1=raw load, …, all=0→6 sequential)")
    ap.add_argument("--skip-phase", action="append", default=[],
                    help="phase to skip when --phase=all (repeatable, e.g. --skip-phase 4)")
    args = ap.parse_args()
    skip = set(args.skip_phase or [])

    con = connect()
    decisions: dict = {
        "script": SCRIPT_TAG,
        "run_ts": RUN_TS_ISO,
        "source_commit": SOURCE_COMMIT,
        "phase_arg": args.phase,
        "phases": {},
    }

    try:
        audit = phase_0(con)
        decisions["phases"]["0"] = audit
        if not audit["ok"]:
            log(f"⛔ Phase 0 blockers: {audit['blockers']}")
            if args.phase != "0":
                log("Halting: cannot proceed past Phase 0 with active blockers.")
                return 2
        if args.phase == "0":
            return 0 if audit["ok"] else 2

        if args.phase in ("1", "all"):
            r = phase_1(con); decisions["phases"]["1"] = r
            if not r["ok"]:
                log(f"⛔ Phase 1 blockers: {r.get('blockers')}")
                return 2
            if args.phase == "1":
                return 0

        if args.phase in ("2", "all"):
            r = phase_2(con); decisions["phases"]["2"] = r
            if not r["ok"]:
                log(f"⛔ Phase 2 blockers: {r.get('blockers')}")
                return 2
            if args.phase == "2":
                return 0

        if args.phase in ("3", "all"):
            r = phase_3(con); decisions["phases"]["3"] = r
            if not r["ok"]:
                log(f"⛔ Phase 3 blockers: {r.get('blockers')}")
                return 2
            if args.phase == "3":
                return 0

        if args.phase in ("4", "all") and "4" not in skip:
            r = phase_4(con, audit); decisions["phases"]["4"] = r
            if not r["ok"]:
                log(f"⛔ Phase 4 blockers: {r.get('blockers')}")
                return 2
            if args.phase == "4":
                return 0
        elif "4" in skip:
            log("=== PHASE 4 — SKIPPED (per --skip-phase 4) ===")
            decisions["phases"]["4"] = {"phase": 4, "ok": True, "action": "skipped_by_flag"}

        if args.phase in ("5", "all"):
            r = phase_5(con); decisions["phases"]["5"] = r
            if not r["ok"]:
                log(f"⛔ Phase 5 blockers: {r.get('blockers')}")
                return 2
            if args.phase == "5":
                return 0

        if args.phase in ("6", "all"):
            r = phase_6(con); decisions["phases"]["6"] = r
            if not r["ok"]:
                log(f"⛔ Phase 6 blockers: {r.get('blockers')}")
                return 2

        return 0
    finally:
        DECISIONS_PATH.write_text(json.dumps(decisions, indent=2, default=str))
        _flush_log()
        try:
            con.close()
        except Exception:
            pass
        log(f"decisions written to {DECISIONS_PATH}")
        log(f"log written to {LOG_PATH}")


if __name__ == "__main__":
    sys.exit(main())
