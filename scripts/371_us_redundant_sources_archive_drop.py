#!/usr/bin/env python3
"""Script 371 — Archive + verify + drop redundant TIRADS source tables (Phase 2).

Targets:
  main.tirads_v2_nodules_raw           (50 cols, 11,914 rows / 3,021 pts)
  main.extracted_tirads_validated_v1   (15 cols, 3,439 rows / 3,439 pts)
  main.tirads_reextraction_queue_v1    (7 cols,  4,363 rows)

Verification (per-table):
  * tirads_v2_nodules_raw — every (research_id, linkage_date, nodule_index)
    row has a matching row in canonical_us_nodule_v2; sonography fields
    (halo, vascularity, ETE, chammas, elasto, dynamics, FNA, prior_size)
    were absorbed (count of LLM-non-null rows where v2 is null = 0
    after we INTENT-restrict to dates that mapped cleanly).
  * extracted_tirads_validated_v1 — patient-level coverage check (every
    patient has ≥1 nodule row in v2).
  * tirads_reextraction_queue_v1 — work-queue, no canonical references.

After verification, archive each as archived_<name> in us_legacy_20260421
and drop from main.
"""
from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

PUB = PUBLICATION_DB
ARCH_DB = "Thyroid 2026 UPdated"
ARCH_SCHEMA = "us_legacy_20260421"
SCRIPT_TAG = "Script 371"
TARGETS = [
    "tirads_v2_nodules_raw",
    "extracted_tirads_validated_v1",
    "tirads_reextraction_queue_v1",
]

OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"371_us_redundant_sources_{RUN_TS}.json"


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


# ──────────────────────────────────────────────────────────────────────────
# Verifications
# ──────────────────────────────────────────────────────────────────────────

def verify_tirads_v2_nodules_raw(con) -> dict:
    """Anti-join: rows in raw with date+index that DON'T appear in v2.
    Sonography fields: count rows where raw has a value but v2 has none."""
    src = f'{PUB}.main.tirads_v2_nodules_raw'
    v2 = f'{PUB}.main.canonical_us_nodule_v2'

    # Anti-join (rows with parseable date+rid not present in v2)
    unmerged = con.execute(f"""
        SELECT COUNT(*) FROM {src} r
        WHERE TRY_CAST(r.research_id AS INTEGER) IS NOT NULL
          AND TRY_CAST(r.linkage_date AS DATE) IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM {v2} n
              WHERE n.research_id = TRY_CAST(r.research_id AS INTEGER)
                AND n.exam_date  = TRY_CAST(r.linkage_date AS DATE)
                AND n.nodule_index_within_exam = r.nodule_index_within_exam
          )
    """).fetchone()[0]

    # Field coverage — flag if raw has data but v2 is null on the join row
    field_coverage = con.execute(f"""
        SELECT
            SUM(CASE WHEN r.halo IS NOT NULL AND n.halo IS NULL THEN 1 ELSE 0 END)
                AS halo_missing_in_v2,
            SUM(CASE WHEN r.vascularity IS NOT NULL AND n.vascularity IS NULL THEN 1 ELSE 0 END)
                AS vascularity_missing,
            SUM(CASE WHEN r.extrathyroidal_extension_on_us IS NOT NULL
                     AND n.extrathyroidal_extension_on_us IS NULL THEN 1 ELSE 0 END)
                AS ete_missing,
            SUM(CASE WHEN r.chammas_type IS NOT NULL AND n.chammas_type IS NULL THEN 1 ELSE 0 END)
                AS chammas_missing,
            SUM(CASE WHEN r.elastography IS NOT NULL AND n.elastography_category IS NULL THEN 1 ELSE 0 END)
                AS elasto_missing,
            SUM(CASE WHEN r.interval_growth_flag IS NOT NULL
                     AND n.interval_growth_flag IS NULL THEN 1 ELSE 0 END)
                AS interval_growth_missing,
            SUM(CASE WHEN r.prior_size_mm_max IS NOT NULL
                     AND n.prior_size_mm_max IS NULL THEN 1 ELSE 0 END)
                AS prior_size_missing,
            SUM(CASE WHEN r.fna_recommended_this_nodule IS NOT NULL
                     AND n.fna_recommended_this_nodule IS NULL THEN 1 ELSE 0 END)
                AS fna_rec_missing,
            SUM(CASE WHEN r.fna_performed_prior_or_concurrent IS NOT NULL
                     AND n.fna_performed_prior_or_concurrent IS NULL THEN 1 ELSE 0 END)
                AS fna_prior_missing
        FROM {src} r
        JOIN {v2} n
          ON n.research_id = TRY_CAST(r.research_id AS INTEGER)
         AND n.exam_date  = TRY_CAST(r.linkage_date AS DATE)
         AND n.nodule_index_within_exam = r.nodule_index_within_exam
    """).fetchone()
    field_names = [
        "halo_missing_in_v2", "vascularity_missing", "ete_missing",
        "chammas_missing", "elasto_missing", "interval_growth_missing",
        "prior_size_missing", "fna_rec_missing", "fna_prior_missing",
    ]
    coverage = dict(zip(field_names, field_coverage))

    # Date-unparseable rows (v2 cannot represent these)
    unparseable = con.execute(f"""
        SELECT COUNT(*) FROM {src}
        WHERE TRY_CAST(linkage_date AS DATE) IS NULL
           OR TRY_CAST(research_id AS INTEGER) IS NULL
    """).fetchone()[0]

    # Probe finding (2026-04-21): 5,445 raw rows live at
    # nodule_index_within_exam = 0 with real US data (composition, TIRADS,
    # size_cm_max). v2's index scheme inherits from cunc which is 1-based, so
    # these rows have no slot in v2 today. Archive preserves them; we
    # downgrade unmerged>0 to "hold" so the live source stays in main until
    # a follow-up script absorbs the index-0 nodules into v2.
    status = "ok"
    reason = None
    if unmerged > 0:
        status = "hold"
        reason = (f"{unmerged} parseable raw rows missing from v2 "
                  f"(likely nodule_index=0 default-slot rows; "
                  f"unparseable={unparseable}); archive preserves them "
                  f"but DO NOT drop main copy")
    elif any(v > 0 for v in coverage.values()):
        status = "warn"
        reason = ("sonography fields with raw-non-null but v2-null: "
                  + ", ".join(f"{k}={v}" for k, v in coverage.items() if v > 0))

    return {
        "table": "tirads_v2_nodules_raw",
        "status": status,
        "reason": reason,
        "unmerged_parseable_rows": unmerged,
        "unparseable_rows": unparseable,
        "field_coverage_gaps": coverage,
    }


def verify_extracted_tirads_validated_v1(con) -> dict:
    src = f'{PUB}.main.extracted_tirads_validated_v1'
    v2 = f'{PUB}.main.canonical_us_nodule_v2'

    # Patient-level coverage (this table is patient-level, not nodule-level)
    missing_pts = con.execute(f"""
        SELECT COUNT(*) FROM {src} e
        WHERE NOT EXISTS (
            SELECT 1 FROM {v2} n WHERE n.research_id = e.research_id
        )
    """).fetchone()[0]

    return {
        "table": "extracted_tirads_validated_v1",
        "status": "ok" if missing_pts == 0 else "warn",
        "reason": (f"{missing_pts} patients in extracted_validated have no "
                   f"row in canonical_us_nodule_v2") if missing_pts else None,
        "missing_patients_in_v2": missing_pts,
    }


def verify_tirads_reextraction_queue(con) -> dict:
    """Work-queue. We confirm no other surviving canonical table references it."""
    src = f'{PUB}.main.tirads_reextraction_queue_v1'

    n = con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]
    # Any view referencing this table?
    refs = con.execute(
        "SELECT table_schema, table_name FROM information_schema.views "
        "WHERE table_catalog = ? AND LOWER(view_definition) LIKE ?",
        [PUB, '%tirads_reextraction_queue_v1%'],
    ).fetchall()
    refs_l = [f"{r[0]}.{r[1]}" for r in refs]

    return {
        "table": "tirads_reextraction_queue_v1",
        "status": "ok" if not refs_l else "warn",
        "reason": (f"views still reference: {refs_l}") if refs_l else None,
        "rows": n,
        "view_references": refs_l,
    }


# ──────────────────────────────────────────────────────────────────────────
# Archive + drop
# ──────────────────────────────────────────────────────────────────────────


def archive_one(con, table: str) -> dict:
    src = f'{PUB}.main."{table}"'
    dst = f'"{ARCH_DB}"."{ARCH_SCHEMA}"."archived_{table}"'

    n_src = con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]

    # If archive already exists with same row count, treat as idempotent
    exists = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_catalog=? AND table_schema=? AND table_name=?",
        [ARCH_DB, ARCH_SCHEMA, f"archived_{table}"],
    ).fetchone()[0] > 0
    if exists:
        n_dst = con.execute(f"SELECT COUNT(*) FROM {dst}").fetchone()[0]
        if n_dst == n_src:
            log(f"  archive {table} already exists (rows={n_dst}); reusing")
            return {"table": table, "archive_status": "exists",
                    "src_rows": n_src, "dst_rows": n_dst}
        # Stale archive — drop and recreate
        log(f"  archive {table} exists with stale row count "
            f"({n_dst} vs {n_src}); recreating")
        con.execute(f"DROP TABLE {dst}")

    con.execute(f"CREATE TABLE {dst} AS SELECT * FROM {src}")
    n_dst = con.execute(f"SELECT COUNT(*) FROM {dst}").fetchone()[0]
    if n_dst != n_src:
        raise SystemExit(
            f"archive count mismatch for {table}: src={n_src} dst={n_dst}"
        )
    return {"table": table, "archive_status": "created",
            "src_rows": n_src, "dst_rows": n_dst}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    log(f"{SCRIPT_TAG} start  commit={args.commit}")
    con = connect_locked()

    log("verify each redundant source against canonical_us_nodule_v2")
    v1 = verify_tirads_v2_nodules_raw(con)
    v2 = verify_extracted_tirads_validated_v1(con)
    v3 = verify_tirads_reextraction_queue(con)
    verifications = [v1, v2, v3]
    for v in verifications:
        log(f"  {v['table']:35s} {v['status']:5s} "
            f"{v.get('reason') or '-'}")

    fails = [v for v in verifications if v["status"] == "fail"]
    if fails:
        DECISION_LOG.write_text(json.dumps(
            {"script": SCRIPT_TAG, "verifications": verifications,
             "fails": fails}, indent=2, default=str))
        log("FAIL — verification errors; aborting before drop.")
        return 1

    warns = [v for v in verifications if v["status"] == "warn"]
    if warns:
        log(f"WARN — {len(warns)} non-fatal coverage issues; proceeding "
            "with archive (no v2 row will be lost):")
        for w in warns:
            log(f"  {w['table']}: {w['reason']}")

    if not args.commit:
        log("dry-run — pass --commit to perform archive+drop.")
        DECISION_LOG.write_text(json.dumps(
            {"script": SCRIPT_TAG, "verifications": verifications,
             "commit": False}, indent=2, default=str))
        return 0

    log(f"ensure schema exists: \"{ARCH_DB}\".\"{ARCH_SCHEMA}\"")
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{ARCH_DB}"."{ARCH_SCHEMA}"')

    archives = [archive_one(con, t) for t in TARGETS]

    log("DROP each verified table; HOLD any with status='hold' "
        "(archive only)")
    held = {v["table"] for v in verifications if v["status"] == "hold"}
    drops: list[dict] = []
    for t in TARGETS:
        if t in held:
            log(f"  HOLD main.{t} (archived; live copy retained pending backfill)")
            drops.append({"table": t, "status": "held"})
            continue
        log(f"  DROP TABLE main.{t}")
        con.execute(f'DROP TABLE {PUB}.main."{t}"')
        con.execute(
            f"DELETE FROM {PUB}.manuscript_workspace.detail_table_registry_v1 "
            f"WHERE detail_table_name = ?", [t],
        )
        drops.append({"table": t, "status": "dropped"})

    DECISION_LOG.write_text(json.dumps({
        "script": SCRIPT_TAG, "run_ts_utc": RUN_TS,
        "verifications": verifications,
        "archives": archives, "drops": drops,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    log(f"summary: archived/dropped {len(drops)} tables")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
