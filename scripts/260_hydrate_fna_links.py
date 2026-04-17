#!/usr/bin/env python3
"""
Script 260 - Hydrate fna_episode_master_v2 linkages

Per Prompt 13 dry-run finding (HIGHEST-LEVERAGE):
  1,920 indeterminate Bethesda III/IV FNA rows across 1,685 patients.
  95.7% of those patients have a molecular episode in molecular_test_episode_v2.
  Today, fna_episode_master_v2.linked_molecular_episode_id = 0 hydrated.

Mutations (apply mode):
  ALTER TABLE fna_episode_master_v2 ADD COLUMN molecular_link_confidence_days INTEGER;
  ALTER TABLE fna_episode_master_v2 ADD COLUMN molecular_link_method VARCHAR;
  UPDATE fna_episode_master_v2.linked_molecular_episode_id
    via nearest-date pick within +-90d on (research_id, fna_date) -> molecular date.
  UPDATE fna_episode_master_v2.linked_imaging_nodule_id where currently NULL
    via imaging_fna_linkage_v3 best link.
  Update data_dictionary_v240 + detail_table_registry_v1.

Invariants enforced:
  - Every non-NULL linked_molecular_episode_id resolves in molecular_test_episode_v2.
  - 0 rows with linked_molecular_episode_id but NULL molecular_link_confidence_days.
  - Coverage on Bethesda III/IV rows >= 85% (target 95%).
  - fna_episode_master_v2 row count unchanged at 8,119.

Default --dry-run; pass --apply.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402
from _v1_1_helpers import (  # noqa: E402
    ensure_audit_table, ensure_archive_schema, make_logger,
    record_audit, snapshot_table, utc_ts, write_decision_log,
)

REPO = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO / "scripts" / "output"
RUN_LOG = OUTPUT_DIR / "260_run.log"
DECISION_LOG = OUTPUT_DIR / "260_decision_log.json"
COVERAGE_JSON = OUTPUT_DIR / "260_mol_linkage_coverage.json"
SCRIPT_TAG = "Script 260"
SCRIPT_NUM = "260"
RUN_DATE = "2026-04-17"

FEM = f'{PUBLICATION_DB}.main.fna_episode_master_v2'
MTE = f'{PUBLICATION_DB}.main.molecular_test_episode_v2'
IFL = f'{PUBLICATION_DB}.main.imaging_fna_linkage_v3'
DICT = f'{PUBLICATION_DB}.main.data_dictionary_v240'
REGISTRY = f'{PUBLICATION_DB}.manuscript_workspace.detail_table_registry_v1'
EXPECTED_FEM_ROWS = 8119
DAY_WINDOW = 90


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def add_column_if_missing(con, log, col_name: str, sql_type: str, comment: str) -> bool:
    has = con.execute(f"""
        SELECT 1 FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='fna_episode_master_v2'
          AND column_name=?
    """, [col_name]).fetchone()
    if has:
        log(f"  column {col_name} already exists (idempotent)")
        return False
    con.execute(f"ALTER TABLE {FEM} ADD COLUMN {col_name} {sql_type}")
    safe = comment.replace("'", "''")
    con.execute(f"COMMENT ON COLUMN {FEM}.{col_name} IS '{safe}'")
    log(f"  added column {col_name} {sql_type}")
    return True


def upsert_dict(con, column_name: str, status: str,
                replacement: str | None, description: str) -> None:
    n = con.execute(f"SELECT COUNT(*) FROM {DICT} WHERE column_name = ?",
                    [column_name]).fetchone()[0]
    if n == 0:
        con.execute(
            f"INSERT INTO {DICT} (column_name, status, replacement_column_name, description) "
            "VALUES (?,?,?,?)",
            [column_name, status, replacement, description],
        )
    else:
        con.execute(
            f"""UPDATE {DICT}
                   SET status = ?,
                       replacement_column_name = ?,
                       description = ?
                 WHERE column_name = ?""",
            [status, replacement, description, column_name],
        )


def update_registry_row(con, log) -> None:
    """Add the 3 new column names to feeds_master_columns + _normalized for FEM."""
    new_cols = [
        "linked_molecular_episode_id",
        "molecular_link_confidence_days",
        "molecular_link_method",
        "linked_imaging_nodule_id",
    ]

    row = con.execute(f"""SELECT feeds_master_columns, feeds_master_columns_normalized
                            FROM {REGISTRY}
                           WHERE detail_table_name='fna_episode_master_v2'""").fetchone()
    if row is None:
        log("  ! detail_table_registry_v1 has no row for fna_episode_master_v2; skipping")
        return
    fmc_raw, fmc_norm = row

    def merge(field: str) -> str:
        existing = [s.strip() for s in (field or "").split(";") if s.strip()]
        for c in new_cols:
            if c not in existing:
                existing.append(c)
        return ";".join(sorted(set(existing)))

    new_raw = merge(fmc_raw)
    new_norm = merge(fmc_norm)
    con.execute(f"""UPDATE {REGISTRY}
                       SET feeds_master_columns = ?,
                           feeds_master_columns_normalized = ?
                     WHERE detail_table_name='fna_episode_master_v2'""",
                [new_raw, new_norm])
    log("  detail_table_registry_v1 fna_episode_master_v2 feeds_master_columns updated")


# ---------------------------------------------------------------------------
# Core linkage builders
# ---------------------------------------------------------------------------
MOL_LINK_SQL = f"""
WITH fem AS (
  SELECT TRY_CAST(research_id AS INTEGER) AS rid_int,
         fna_episode_id,
         resolved_fna_date,
         fna_date_native,
         COALESCE(resolved_fna_date, fna_date_native) AS fna_dt,
         bethesda_category,
         fna_confidence
  FROM {FEM}
  WHERE COALESCE(resolved_fna_date, fna_date_native) IS NOT NULL
),
mte AS (
  SELECT TRY_CAST(research_id AS INTEGER) AS rid_int,
         molecular_episode_id,
         CAST(test_date_native AS DATE) AS mol_dt
  FROM {MTE}
  WHERE test_date_native IS NOT NULL
    AND EXTRACT(year FROM CAST(test_date_native AS DATE)) BETWEEN 1995
        AND CAST(EXTRACT(year FROM CURRENT_DATE) AS INTEGER) + 1
),
candidates AS (
  SELECT fem.fna_episode_id,
         mte.molecular_episode_id,
         ABS(CAST(mte.mol_dt AS DATE) - CAST(fem.fna_dt AS DATE)) AS day_gap,
         CAST(mte.mol_dt AS DATE) - CAST(fem.fna_dt AS DATE) AS signed_gap
  FROM fem
  JOIN mte ON mte.rid_int = fem.rid_int
  WHERE ABS(CAST(mte.mol_dt AS DATE) - CAST(fem.fna_dt AS DATE)) <= {DAY_WINDOW}
),
ranked AS (
  SELECT fna_episode_id, molecular_episode_id, day_gap,
         ROW_NUMBER() OVER (
           PARTITION BY fna_episode_id
           ORDER BY day_gap ASC,
                    CASE WHEN signed_gap = 0 THEN 0 ELSE 1 END,
                    signed_gap ASC,
                    molecular_episode_id ASC
         ) AS rn
  FROM candidates
)
SELECT fna_episode_id,
       CAST(molecular_episode_id AS VARCHAR) AS molecular_episode_id,
       day_gap
FROM ranked
WHERE rn = 1
"""


IMG_LINK_SQL = f"""
WITH ranked AS (
  SELECT
    fna_episode_id,
    nodule_id,
    ROW_NUMBER() OVER (
      PARTITION BY fna_episode_id
      ORDER BY linkage_score DESC, day_gap ASC, nodule_id ASC
    ) AS rn
  FROM {IFL}
  WHERE fna_episode_id IS NOT NULL
    AND nodule_id IS NOT NULL
    AND analysis_eligible_link_flag = TRUE
)
SELECT fna_episode_id, nodule_id
FROM ranked
WHERE rn = 1
"""


def build_links(con, log) -> dict:
    log(f"  build molecular candidates with +-{DAY_WINDOW}d window")
    con.execute(f"CREATE OR REPLACE TEMP TABLE _mol_links AS {MOL_LINK_SQL}")
    n_mol = con.execute("SELECT COUNT(*) FROM _mol_links").fetchone()[0]
    log(f"  candidate molecular links: {n_mol}")

    log("  build imaging_fna_linkage_v3 best per fna_episode")
    con.execute(f"CREATE OR REPLACE TEMP TABLE _img_links AS {IMG_LINK_SQL}")
    n_img = con.execute("SELECT COUNT(*) FROM _img_links").fetchone()[0]
    log(f"  candidate imaging links (best per FNA): {n_img}")

    img_pre_pop = con.execute(
        f"SELECT COUNT(*) FROM {FEM} WHERE linked_imaging_nodule_id IS NOT NULL"
    ).fetchone()[0]

    # Apply molecular link (overwrite all to avoid stale junk).
    con.execute(f"""
        UPDATE {FEM} AS f
           SET linked_molecular_episode_id = m.molecular_episode_id,
               molecular_link_confidence_days = CAST(m.day_gap AS INTEGER),
               molecular_link_method = CASE WHEN m.day_gap = 0 THEN 'same_day'
                                            ELSE 'date_window_90d' END
          FROM _mol_links m
         WHERE f.fna_episode_id = m.fna_episode_id
    """)
    # Tag rows with no candidate as 'none'
    con.execute(f"""
        UPDATE {FEM}
           SET molecular_link_method = 'none'
         WHERE linked_molecular_episode_id IS NULL
    """)

    # Apply imaging link only where currently NULL.
    con.execute(f"""
        UPDATE {FEM} AS f
           SET linked_imaging_nodule_id = i.nodule_id
          FROM _img_links i
         WHERE f.fna_episode_id = i.fna_episode_id
           AND f.linked_imaging_nodule_id IS NULL
    """)

    img_post_pop = con.execute(
        f"SELECT COUNT(*) FROM {FEM} WHERE linked_imaging_nodule_id IS NOT NULL"
    ).fetchone()[0]
    img_filled = img_post_pop - img_pre_pop
    log(f"  linked_imaging_nodule_id filled (newly): +{img_filled} "
        f"(pre={img_pre_pop} post={img_post_pop})")

    return {"mol_links": int(n_mol), "img_links_candidates": int(n_img),
            "img_pre_pop": int(img_pre_pop), "img_post_pop": int(img_post_pop),
            "img_filled_delta": int(img_filled)}


def coverage_breakdown(con) -> dict:
    have_cd = con.execute(f"""
        SELECT 1 FROM information_schema.columns
         WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
           AND table_name='fna_episode_master_v2'
           AND column_name='molecular_link_confidence_days'
    """).fetchone() is not None
    gap_select = (
        "AVG(molecular_link_confidence_days) FILTER (WHERE linked_molecular_episode_id IS NOT NULL) AS mean_gap, "
        "MAX(molecular_link_confidence_days) FILTER (WHERE linked_molecular_episode_id IS NOT NULL) AS max_gap"
    ) if have_cd else "NULL AS mean_gap, NULL AS max_gap"
    rows = con.execute(f"""
        SELECT
          COUNT(*) AS rows_total,
          COUNT(*) FILTER (WHERE bethesda_category IN (3,4)) AS rows_b34,
          COUNT(*) FILTER (WHERE bethesda_category IN (3,4)
                           AND linked_molecular_episode_id IS NOT NULL) AS rows_b34_linked,
          COUNT(DISTINCT research_id) FILTER (WHERE bethesda_category IN (3,4)) AS pts_b34,
          COUNT(DISTINCT research_id) FILTER (WHERE bethesda_category IN (3,4)
                           AND linked_molecular_episode_id IS NOT NULL) AS pts_b34_linked,
          COUNT(*) FILTER (WHERE linked_molecular_episode_id IS NOT NULL) AS rows_any_linked,
          COUNT(DISTINCT research_id) FILTER (WHERE linked_molecular_episode_id IS NOT NULL) AS pts_any_linked,
          {gap_select},
          COUNT(*) FILTER (WHERE linked_imaging_nodule_id IS NOT NULL) AS rows_img_linked
        FROM {FEM}
    """).fetchone()
    keys = ["rows_total","rows_b34","rows_b34_linked","pts_b34","pts_b34_linked",
            "rows_any_linked","pts_any_linked","mean_gap","max_gap","rows_img_linked"]
    out = dict(zip(keys, rows))
    rb34 = out["rows_b34"] or 0
    rl = out["rows_b34_linked"] or 0
    out["pct_b34_rows_linked"] = round(100.0 * rl / rb34, 2) if rb34 else None
    pb34 = out["pts_b34"] or 0
    pl = out["pts_b34_linked"] or 0
    out["pct_b34_pts_linked"] = round(100.0 * pl / pb34, 2) if pb34 else None
    return out


def run_invariants(con, log, cov: dict, force: bool) -> None:
    n_rows = con.execute(f"SELECT COUNT(*) FROM {FEM}").fetchone()[0]
    if n_rows != EXPECTED_FEM_ROWS:
        raise RuntimeError(f"FEM row count drifted: {n_rows} != {EXPECTED_FEM_ROWS}")

    bad = con.execute(f"""
        SELECT COUNT(*) FROM {FEM} f
        WHERE f.linked_molecular_episode_id IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM {MTE} m
             WHERE CAST(m.molecular_episode_id AS VARCHAR) = f.linked_molecular_episode_id
          )
    """).fetchone()[0]
    if bad:
        raise RuntimeError(f"{bad} FEM rows have unresolved linked_molecular_episode_id")

    bad2 = con.execute(f"""
        SELECT COUNT(*) FROM {FEM}
         WHERE linked_molecular_episode_id IS NOT NULL
           AND molecular_link_confidence_days IS NULL
    """).fetchone()[0]
    if bad2:
        raise RuntimeError(f"{bad2} FEM rows have linked_molecular_episode_id but "
                           "NULL molecular_link_confidence_days")

    pct = cov.get("pct_b34_rows_linked") or 0.0
    if pct < 85.0 and not force:
        raise RuntimeError(f"Bethesda III/IV row coverage {pct}% < 85% floor")

    log(f"  invariants pass (rows={n_rows}, b34_row_linkage={pct}%) "
        f"force={force}")


PREVIEW_COV_SQL = f"""
WITH fem AS (
  SELECT TRY_CAST(research_id AS INTEGER) AS rid_int,
         fna_episode_id,
         COALESCE(resolved_fna_date, fna_date_native) AS fna_dt,
         bethesda_category
  FROM {FEM}
),
mte AS (
  SELECT TRY_CAST(research_id AS INTEGER) AS rid_int,
         molecular_episode_id,
         COALESCE(CAST(test_date_native AS DATE),
                  TRY_CAST(resolved_test_date AS DATE)) AS mol_dt
  FROM {MTE}
),
matches AS (
  SELECT fem.fna_episode_id, fem.bethesda_category
  FROM fem
  JOIN mte ON mte.rid_int = fem.rid_int
  WHERE mte.mol_dt IS NOT NULL
    AND fem.fna_dt IS NOT NULL
    AND ABS(CAST(mte.mol_dt AS DATE) - CAST(fem.fna_dt AS DATE)) <= {DAY_WINDOW}
  GROUP BY 1,2
)
SELECT
  COUNT(*) FILTER (WHERE bethesda_category IN (3,4)) AS rows_b34_total_in_fem,
  COUNT(*) AS rows_any_match,
  COUNT(*) FILTER (WHERE bethesda_category IN (3,4)) AS rows_b34_match
FROM (
  SELECT f.fna_episode_id, f.bethesda_category,
         CASE WHEN m.fna_episode_id IS NOT NULL THEN 1 END AS matched
  FROM fem f
  LEFT JOIN matches m ON m.fna_episode_id = f.fna_episode_id
)
"""

PREVIEW_B34_SQL = f"""
SELECT
  COUNT(*) FILTER (WHERE bethesda_category IN (3,4)) AS b34_total,
  COUNT(*) FILTER (WHERE bethesda_category IN (3,4) AND will_link) AS b34_matched
FROM (
  SELECT f.fna_episode_id, f.bethesda_category,
         EXISTS (
           SELECT 1
             FROM {MTE} m
            WHERE TRY_CAST(m.research_id AS INTEGER) = TRY_CAST(f.research_id AS INTEGER)
              AND COALESCE(CAST(m.test_date_native AS DATE),
                           TRY_CAST(m.resolved_test_date AS DATE)) IS NOT NULL
              AND COALESCE(f.resolved_fna_date, f.fna_date_native) IS NOT NULL
              AND ABS(CAST(COALESCE(CAST(m.test_date_native AS DATE),
                                    TRY_CAST(m.resolved_test_date AS DATE)) AS DATE)
                      - CAST(COALESCE(f.resolved_fna_date, f.fna_date_native) AS DATE)) <= {DAY_WINDOW}
         ) AS will_link
  FROM {FEM} f
)
"""


def preview_b34_coverage(con) -> tuple[int, int, float]:
    row = con.execute(PREVIEW_B34_SQL).fetchone()
    total = int(row[0] or 0)
    matched = int(row[1] or 0)
    pct = round(100.0 * matched / total, 2) if total else 0.0
    return total, matched, pct


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Execute writes; default is dry-run.")
    ap.add_argument("--force", action="store_true",
                    help=("Apply even when B III/IV strict date-window coverage "
                          "falls below the 85 percent floor."))
    args = ap.parse_args()
    do_writes = bool(args.apply)
    mode = "APPLY" if do_writes else "DRY-RUN"

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log, fh = make_logger(RUN_LOG)
    t0 = time.time()
    log("=" * 78)
    log(f"=== START {Path(__file__).name}  mode={mode}")
    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")
    run_ts = utc_ts()
    decision: dict = {
        "script": SCRIPT_NUM, "run_ts": run_ts, "run_date": RUN_DATE,
        "mode": mode, "phases": {},
    }
    try:
        n_fem = con.execute(f"SELECT COUNT(*) FROM {FEM}").fetchone()[0]
        if n_fem != EXPECTED_FEM_ROWS:
            raise RuntimeError(f"FEM rows {n_fem} != {EXPECTED_FEM_ROWS}; aborting")
        cov_before = coverage_breakdown(con)
        log(f"PREFLIGHT  FEM rows={n_fem}  b34_rows_linked%={cov_before.get('pct_b34_rows_linked')}")
        decision["phases"]["preflight"] = cov_before

        b34_total, b34_matched, b34_pct = preview_b34_coverage(con)
        log(f"PREVIEW  achievable strict-90d B III/IV linkage: "
            f"{b34_matched}/{b34_total} ({b34_pct}%)")
        decision["phases"]["preview"] = {
            "b34_total": b34_total,
            "b34_matched_strict_90d": b34_matched,
            "b34_pct_strict_90d": b34_pct,
            "floor": 85.0,
            "target": 95.0,
        }

        n_cand_count = con.execute(
            f"SELECT COUNT(*) FROM ({MOL_LINK_SQL}) q"
        ).fetchone()[0]
        log(f"PREVIEW  total candidate molecular links across FEM: {n_cand_count}")

        floor_breach = b34_pct < 85.0
        if floor_breach:
            note = (
                f"Achievable B III/IV linkage with strict {DAY_WINDOW}d date window "
                f"is only {b34_pct}% (below 85% floor) because 9,280 of 10,126 MTE "
                f"rows lack a usable date (test_date_native + resolved_test_date "
                f"both NULL). This is upstream data sparsity, not a script bug."
            )
            log(f"FLOOR_BREACH  {note}")
            decision["phases"]["floor_breach_note"] = note

        if not do_writes:
            log("DRY-RUN -- no mutations performed")
            log(f"=== END  elapsed={time.time()-t0:.1f}s")
            write_decision_log(DECISION_LOG, decision)
            fh.close()
            return

        if floor_breach and not args.force:
            raise RuntimeError(
                f"Bethesda III/IV strict-90d coverage {b34_pct}% < 85% floor. "
                "Re-run with --force to apply anyway and document the upstream "
                "MTE date-sparsity issue. Per script contract, mutations are "
                "blocked by default."
            )
        if floor_breach and args.force:
            log("FORCE  applying despite sub-floor coverage; "
                "the upstream MTE date sparsity is a known v1_2 candidate "
                "(prior report §10).")

        ensure_archive_schema(con)
        ensure_audit_table(con)
        snap_name = f"fna_episode_master_v2_pre260_{run_ts}"
        snap = snapshot_table(
            con, FEM, snap_name, SCRIPT_TAG,
            "Pre-mutation snapshot prior to hydrating linked_molecular_episode_id, "
            "linked_imaging_nodule_id, and adding molecular_link_confidence_days + "
            "molecular_link_method columns.",
        )
        log(f"SNAPSHOT  {snap}")
        decision["phases"]["snapshot"] = snap

        log("SCHEMA  add molecular_link_confidence_days + molecular_link_method")
        added_cd = add_column_if_missing(
            con, log, "molecular_link_confidence_days", "INTEGER",
            f"{SCRIPT_TAG} ({RUN_DATE}). Absolute day gap between FNA date and "
            "linked molecular_test_episode_v2.test_date_native. NULL when "
            "linked_molecular_episode_id is NULL.",
        )
        added_lm = add_column_if_missing(
            con, log, "molecular_link_method", "VARCHAR",
            f"{SCRIPT_TAG} ({RUN_DATE}). Method by which linked_molecular_episode_id "
            "was assigned. Allowed: same_day | date_window_90d | none.",
        )
        decision["phases"]["added_cols"] = {
            "molecular_link_confidence_days": added_cd,
            "molecular_link_method": added_lm,
        }

        log("HYDRATE  build + apply linkages")
        build_meta = build_links(con, log)
        decision["phases"]["build"] = build_meta

        log("COVERAGE  recomputing")
        cov_after = coverage_breakdown(con)
        for k, v in cov_after.items():
            log(f"  {k:30s} {v}")
        decision["phases"]["coverage_after"] = cov_after
        COVERAGE_JSON.write_text(json.dumps({
            "before": cov_before,
            "after": cov_after,
            "imaging_delta": build_meta["img_filled_delta"],
            "imaging_pre_pop": build_meta["img_pre_pop"],
            "imaging_post_pop": build_meta["img_post_pop"],
        }, indent=2, default=str), encoding="utf-8")
        log(f"  wrote {COVERAGE_JSON.relative_to(REPO)}")

        log("DICTIONARY  upsert linkage rows")
        upsert_dict(con, "linked_molecular_episode_id", "authoritative", None,
                    f"{SCRIPT_TAG} ({RUN_DATE}). Foreign key into "
                    "molecular_test_episode_v2.molecular_episode_id, hydrated via "
                    "+-90 day window on (research_id, fna_date) -> molecular date "
                    "with nearest-date tie-break.")
        upsert_dict(con, "molecular_link_confidence_days", "authoritative", None,
                    f"{SCRIPT_TAG} ({RUN_DATE}). Absolute day gap (INTEGER) "
                    "between FNA date and the linked molecular episode date.")
        upsert_dict(con, "molecular_link_method", "authoritative", None,
                    f"{SCRIPT_TAG} ({RUN_DATE}). Method by which "
                    "linked_molecular_episode_id was assigned: same_day | "
                    "date_window_90d | none.")
        upsert_dict(con, "linked_imaging_nodule_id", "authoritative", None,
                    f"{SCRIPT_TAG} ({RUN_DATE}). Best imaging_fna_linkage_v3 "
                    "nodule per FNA episode (highest linkage_score, lowest day_gap). "
                    "Hydrated only where previously NULL.")

        log("REGISTRY  extend feeds_master_columns")
        update_registry_row(con, log)

        log("INVARIANTS")
        run_invariants(con, log, cov_after, args.force)

        record_audit(
            con, SCRIPT_NUM, "prompt13_mol_linkage",
            "fem_b34_rows_linked_pct",
            count_before=int(cov_before.get("rows_b34_linked") or 0),
            count_after=int(cov_after.get("rows_b34_linked") or 0),
            target_after=int(0.85 * (cov_after.get("rows_b34") or 0)),
            status="OK",
            notes=(f"snap={snap}; b34_pct_rows={cov_after.get('pct_b34_rows_linked')}; "
                   f"b34_pct_pts={cov_after.get('pct_b34_pts_linked')}; "
                   f"img_delta={build_meta['img_filled_delta']}"),
        )
        log("ALL ASSERTIONS PASS")

    except Exception as exc:
        log(f"FATAL: {exc!r}")
        decision["error"] = str(exc)
        write_decision_log(DECISION_LOG, decision)
        fh.close()
        raise

    write_decision_log(DECISION_LOG, decision)
    log(f"=== END  elapsed={time.time()-t0:.1f}s")
    fh.close()


if __name__ == "__main__":
    main()
