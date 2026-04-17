"""Canonical cleanup 20260417 follow-up — release Phase 3.2 and Phase 4.6 holds.

Phase 3.2 (Logan-approved 2026-04-17):
  - Snapshot the 3 placeholder rows in main.us_nodules_tirads (rids 2332,
    2445, 7744) into manuscript_workspace.us_nodules_tirads_placeholder_archive_v1
    for reversibility.
  - DELETE them from main.us_nodules_tirads.

Phase 4.6 (Logan-approved 2026-04-17):
  - For each of the 9 manuscript_workspace cohort views referencing bare
    `ajcc8_t_stage`, apply CREATE OR REPLACE VIEW with bare ->
    `ajcc8_t_stage_corrected` (regex with negative-lookahead semantics
    implemented via Python re).
  - Re-run the Phase 4.6 PRE-GATE; assert 0 bare references remain.
  - (The actual ALTER RENAME is run separately via
    scripts/274b_canonical_cleanup_phase4_6_rename.py — invoked here as a
    function call after the migration.)

Writes only to thyroid_canonical_publication_v1_0.
"""
from __future__ import annotations

import csv
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
HERE = REPO / "studies" / "canonical_cleanup_20260417"
sys.path.insert(0, str(REPO / "scripts"))

from _md_connect import connect_locked  # type: ignore

LOG_PATH = HERE / "phase3_2_and_4_6_run.log"
DECISIONS_PATH = HERE / "phase3_2_and_4_6_decision_log.json"
DECISIONS: list[dict] = []

VIEWS = [
    "cohort_descriptive_full_cohort_v1",
    "cohort_m007_rss_reclassification_v1",
    "cohort_m036_ata_risk_comparison_v1",
    "cohort_m043_ln_predictors_v1",
    "cohort_m044_ajcc_ete_v1",
    "cohort_m048_tnm_multifocal_v1",
    "cohort_m050_tumor_size_volume_v1",
    "cohort_m051_ete_ln_v1",
    "cohort_m059_prognostic_scoring_v1",
]

PLACEHOLDER_RIDS = ["2332", "2445", "7744"]


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def record(entry: dict) -> None:
    DECISIONS.append(entry)
    DECISIONS_PATH.write_text(json.dumps(DECISIONS, indent=2, default=str))


def stop(msg: str) -> None:
    log(f"STOP: {msg}")
    DECISIONS_PATH.write_text(json.dumps(DECISIONS, indent=2, default=str))
    raise SystemExit(2)


def assert_invariants(con) -> None:
    n_rows, n_distinct = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) "
        "FROM main.canonical_patient_master"
    ).fetchone()
    if n_rows != 10871 or n_distinct != 10871:
        stop(f"Invariant breach: rows={n_rows} distinct={n_distinct}")
    log("invariants OK")


# ---------------------------------------------------------------------------
# Phase 3.2 — DELETE the 3 us_nodules_tirads placeholders
# ---------------------------------------------------------------------------

def phase_3_2_delete(con) -> None:
    log("=== Phase 3.2 release — DELETE 3 placeholder rows from us_nodules_tirads ===")

    pre_n = con.execute(
        "SELECT COUNT(*) FROM main.us_nodules_tirads "
        f"WHERE CAST(research_id AS VARCHAR) IN ({','.join(repr(r) for r in PLACEHOLDER_RIDS)})"
    ).fetchone()[0]
    log(f"[3.2-del] pre rows matching: {pre_n} (expected 3)")
    if pre_n == 0:
        # Already deleted in a prior invocation; verify archive has the 3 rows.
        try:
            n_arch = con.execute(
                "SELECT COUNT(*) FROM "
                "manuscript_workspace.us_nodules_tirads_placeholder_archive_v1 "
                f"WHERE CAST(research_id AS VARCHAR) IN ({','.join(repr(r) for r in PLACEHOLDER_RIDS)})"
            ).fetchone()[0]
        except Exception:
            n_arch = -1
        if n_arch == 3:
            log("[3.2-del] already applied (3 rows in archive, 0 in main); no-op")
            record({"step": "3.2-delete", "status": "ALREADY_APPLIED_NOOP",
                    "archive_rows": n_arch})
            return
        stop(f"[3.2-del] expected 3 source rows or 3 archive rows; found "
             f"src=0 archive={n_arch}")
    if pre_n != 3:
        stop(f"[3.2-del] expected 3 rows, found {pre_n}")

    pre_total = con.execute("SELECT COUNT(*) FROM main.us_nodules_tirads").fetchone()[0]
    log(f"[3.2-del] us_nodules_tirads pre-total: {pre_total}")

    # Snapshot for reversibility
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS
          manuscript_workspace.us_nodules_tirads_placeholder_archive_v1 AS
        SELECT *, CURRENT_TIMESTAMP AS archived_at,
               'phase_3_2_canonical_cleanup_20260417' AS archive_reason
        FROM main.us_nodules_tirads WHERE 1=0
        """
    )
    # Insert the 3 placeholder rows (idempotent: only if not already archived)
    n_already = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.us_nodules_tirads_placeholder_archive_v1 "
        f"WHERE CAST(research_id AS VARCHAR) IN ({','.join(repr(r) for r in PLACEHOLDER_RIDS)})"
    ).fetchone()[0]
    if n_already < 3:
        con.execute(
            f"""
            INSERT INTO manuscript_workspace.us_nodules_tirads_placeholder_archive_v1
            SELECT *, CURRENT_TIMESTAMP AS archived_at,
                   'phase_3_2_canonical_cleanup_20260417' AS archive_reason
            FROM main.us_nodules_tirads
            WHERE CAST(research_id AS VARCHAR) IN ({','.join(repr(r) for r in PLACEHOLDER_RIDS)})
              AND CAST(research_id AS VARCHAR) NOT IN (
                SELECT CAST(research_id AS VARCHAR)
                FROM manuscript_workspace.us_nodules_tirads_placeholder_archive_v1
              )
            """
        )
    n_archived = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.us_nodules_tirads_placeholder_archive_v1 "
        f"WHERE CAST(research_id AS VARCHAR) IN ({','.join(repr(r) for r in PLACEHOLDER_RIDS)})"
    ).fetchone()[0]
    log(f"[3.2-del] archive table now holds {n_archived} placeholder rows")
    if n_archived != 3:
        stop(f"[3.2-del] archive insert failed: {n_archived} != 3")

    con.execute(
        f"""
        DELETE FROM main.us_nodules_tirads
        WHERE CAST(research_id AS VARCHAR) IN ({','.join(repr(r) for r in PLACEHOLDER_RIDS)})
        """
    )
    post_n = con.execute(
        "SELECT COUNT(*) FROM main.us_nodules_tirads "
        f"WHERE CAST(research_id AS VARCHAR) IN ({','.join(repr(r) for r in PLACEHOLDER_RIDS)})"
    ).fetchone()[0]
    post_total = con.execute("SELECT COUNT(*) FROM main.us_nodules_tirads").fetchone()[0]
    log(f"[3.2-del] post rows matching: {post_n} (expected 0); us_nodules_tirads total: {post_total}")
    if post_n != 0:
        stop(f"[3.2-del] DELETE failed: {post_n} matches still present")
    if post_total != pre_total - 3:
        stop(f"[3.2-del] table delta unexpected: {pre_total} -> {post_total} (expected -3)")

    record({
        "step": "3.2-delete",
        "pre_matching": pre_n,
        "post_matching": post_n,
        "pre_total": pre_total,
        "post_total": post_total,
        "archive_table": "manuscript_workspace.us_nodules_tirads_placeholder_archive_v1",
        "archived_rows": n_archived,
        "rids": PLACEHOLDER_RIDS,
    })


# ---------------------------------------------------------------------------
# Phase 4.6 — view migration + re-run PRE-GATE + rename
# ---------------------------------------------------------------------------

_CREATE_VIEW_PREFIX = re.compile(
    r"^\s*CREATE\s+(OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+)?VIEW\s+"
    r"(?:[\w\".]+)\s+AS\s+",
    re.IGNORECASE | re.DOTALL,
)


def strip_create_prefix(d: str) -> str:
    """Strip leading 'CREATE [OR REPLACE] VIEW <ident> AS ' from a DuckDB
    information_schema.views.view_definition string."""
    m = _CREATE_VIEW_PREFIX.match(d)
    return d[m.end():] if m else d


def migrate_definition(d: str) -> str:
    """Replace bare 'ajcc8_t_stage' with 'ajcc8_t_stage_corrected'."""
    pattern = re.compile(r"ajcc8_t_stage(?!_)", re.IGNORECASE)
    return pattern.sub("ajcc8_t_stage_corrected", d)


def phase_4_6_view_migration(con) -> None:
    log("=== Phase 4.6 — view migration (9 cohort views) ===")
    per_view: list[dict] = []
    for v in VIEWS:
        row = con.execute(
            "SELECT view_definition FROM information_schema.views "
            "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
            "AND table_schema='manuscript_workspace' AND table_name=?",
            [v],
        ).fetchone()
        if not row:
            stop(f"[4.6-mig] view {v} not found")
        d_old_full = row[0]
        # DuckDB info_schema returns 'CREATE VIEW <ident> AS <body>'; strip
        # the prefix so we can prepend our own CREATE OR REPLACE.
        d_old_body = strip_create_prefix(d_old_full)
        d_new_body = migrate_definition(d_old_body)
        if d_new_body == d_old_body:
            log(f"[4.6-mig] {v}: already migrated (no bare refs); skipping")
            per_view.append({"view": v, "action": "skip-already-migrated"})
            continue
        bare_count = len(re.findall(r"ajcc8_t_stage(?!_)", d_old_body, flags=re.IGNORECASE))
        log(f"[4.6-mig] {v}: replacing {bare_count} bare ref(s)")
        con.execute(
            f'CREATE OR REPLACE VIEW manuscript_workspace."{v}" AS {d_new_body}'
        )
        # Verify post
        check_full = con.execute(
            "SELECT view_definition FROM information_schema.views "
            "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
            "AND table_schema='manuscript_workspace' AND table_name=?",
            [v],
        ).fetchone()[0]
        check_body = strip_create_prefix(check_full)
        post_bare = len(re.findall(r"ajcc8_t_stage(?!_)", check_body, flags=re.IGNORECASE))
        per_view.append({
            "view": v,
            "action": "rewritten",
            "bare_refs_before": bare_count,
            "bare_refs_after": post_bare,
        })
        if post_bare != 0:
            stop(f"[4.6-mig] {v} still has {post_bare} bare refs after rewrite")
    record({"step": "4.6-view-migration", "per_view": per_view})


def phase_4_6_pre_gate_recheck(con) -> int:
    """Re-run the Phase 4.6 PRE-GATE; identical SQL to script 274's PRE-GATE."""
    log("=== Phase 4.6 — PRE-GATE recheck after view migration ===")
    rows = con.execute(
        """
        WITH all_views AS (
          SELECT table_schema, table_name, LOWER(view_definition) AS d
          FROM information_schema.views
          WHERE table_catalog = 'thyroid_canonical_publication_v1_0'
            AND view_definition IS NOT NULL
            AND LOWER(view_definition) LIKE '%ajcc8_t_stage%'
        ),
        masked AS (
          SELECT table_schema, table_name, d,
                 REPLACE(REPLACE(d, 'ajcc8_t_stage_corrected', ''),
                                  'ajcc8_t_stage_v2', '') AS d_masked
          FROM all_views
        )
        SELECT table_schema, table_name,
               (LENGTH(d_masked) - LENGTH(REPLACE(d_masked, 'ajcc8_t_stage', '')))
                 / 13 AS bare_count
        FROM masked
        WHERE (LENGTH(d_masked) - LENGTH(REPLACE(d_masked, 'ajcc8_t_stage', ''))) > 0
        ORDER BY 1, 2
        """
    ).fetchall()
    log(f"[4.6-recheck] views with bare refs: {len(rows)}")
    if rows:
        # Refresh the migration list CSV
        with (HERE / "ajcc8_t_stage_migration_needed.csv").open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["table_schema", "table_name", "bare_ajcc8_t_stage_count"])
            for r in rows:
                w.writerow(r)
                log(f"[4.6-recheck]   - {r[0]}.{r[1]} bare={r[2]}")
    record({
        "step": "4.6-pregate-recheck",
        "n_views_with_bare_refs": len(rows),
        "rows": [list(r) for r in rows],
    })
    return len(rows)


def phase_4_6_rename(con) -> None:
    log("=== Phase 4.6 — RENAME ajcc8_t_stage_corrected -> ajcc8_t_stage ===")
    cpm_cols = {
        r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
            "AND table_schema='main' AND table_name='canonical_patient_master'"
        ).fetchall()
    }
    needed = {"ajcc8_t_stage", "ajcc8_t_stage_corrected"}
    if not needed.issubset(cpm_cols):
        stop(f"[4.6-rename] missing columns; have {sorted(cpm_cols & needed)}")
    if "ajcc8_t_stage_with_microete_t3b_DEPRECATED" in cpm_cols:
        log("[4.6-rename] DEPRECATED column already exists; rename appears already applied")
        record({"step": "4.6-rename", "status": "already-applied", "note": (
            "ajcc8_t_stage_with_microete_t3b_DEPRECATED already exists; not re-renaming"
        )})
        return

    con.execute(
        "ALTER TABLE main.canonical_patient_master "
        "RENAME COLUMN ajcc8_t_stage "
        "TO ajcc8_t_stage_with_microete_t3b_DEPRECATED"
    )
    con.execute(
        "ALTER TABLE main.canonical_patient_master "
        "RENAME COLUMN ajcc8_t_stage_corrected TO ajcc8_t_stage"
    )
    con.execute(
        "COMMENT ON COLUMN main.canonical_patient_master.ajcc8_t_stage_with_microete_t3b_DEPRECATED "
        "IS 'Do not use. AJCC 8 rule preserved for audit only. Superseded "
        "2026-04-17 by canonical cleanup script 274/278 rename of "
        "ajcc8_t_stage_corrected -> ajcc8_t_stage.'"
    )
    log("[4.6-rename] rename + COMMENT applied")
    record({"step": "4.6-rename", "status": "OK"})


# ---------------------------------------------------------------------------

def main() -> int:
    HERE.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("")
    DECISIONS_PATH.write_text("[]")
    log("=== Phase 3.2 + 4.6 release driver start ===")
    con = connect_locked()
    assert_invariants(con)

    phase_3_2_delete(con)
    assert_invariants(con)

    phase_4_6_view_migration(con)
    assert_invariants(con)

    n_bare = phase_4_6_pre_gate_recheck(con)
    if n_bare != 0:
        stop(f"[4.6] PRE-GATE recheck still finds {n_bare} bare refs")

    phase_4_6_rename(con)
    assert_invariants(con)

    log("=== Phase 3.2 + 4.6 release driver end ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
